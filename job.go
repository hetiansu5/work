package work

import (
	"sync"
	"errors"
	"time"
	"context"
	"fmt"
	"sync/atomic"
)

const (
	//默认worker的并发数
	defaultConcurrency = 5
)

const (
	Trace = uint8(iota)
	Debug
	Info
	Warn
	Error
	Fatal
	None
)

type queueManger struct {
	queue Queue
	//队列服务负责的主题
	topics []string
}

type Job struct {
	//上下文
	ctx context.Context

	//workers及map锁
	workers map[string]*Worker
	//map操作锁
	wLock sync.RWMutex

	//并发控制通道
	concurrency map[string]chan struct{}
	cLock       sync.RWMutex

	//队列数据通道
	tasksChan map[string]chan Task
	tLock     sync.RWMutex

	enabledTopics []string

	//work并发处理的等待暂停
	wg sync.WaitGroup
	//启动状态
	running bool
	//异常状态时需要sleep时间
	sleepy time.Duration
	//通道定时器超时时间
	timer time.Duration
	//默认的worker并发数
	con int

	//Queue服务 - 依赖外部注入
	queueMangers []queueManger
	//默认Queue服务 - 依赖外部注入
	defaultQueue Queue
	//topic与queue的映射关系
	queueMap map[string]Queue
	//map操作锁
	qLock sync.RWMutex

	//标准输出等级
	consoleLevel uint8

	//日志服务 - 依赖外部注入
	logger Logger
	//日记等级
	level uint8

	//统计
	pullCount      int64
	pullEmptyCount int64
	pullErrCount   int64
	taskCount      int64
	taskErrCount   int64
	handleCount    int64
	handleErrCount int64
}

func New() *Job {
	j := new(Job)
	j.ctx = context.Background()
	j.workers = make(map[string]*Worker)
	j.concurrency = make(map[string]chan struct{})
	j.tasksChan = make(map[string]chan Task)
	j.queueMap = make(map[string]Queue)
	j.level = Info
	j.consoleLevel = Info
	j.sleepy = time.Millisecond * 10
	j.timer = time.Millisecond * 10
	j.con = defaultConcurrency
	return j
}

func (j *Job) Start() {
	if j.running {
		return
	}

	j.running = true
	j.initJob()
	j.runQueues()
	j.processJob()
}

func (j *Job) initJob() {
	for topic, w := range j.workers {
		if !j.isTopicEnable(topic) {
			continue
		}

		if w.MaxConcurrency <= 0 {
			w.MaxConcurrency = j.con
		}

		//用来控制workers的并发数
		j.concurrency[topic] = make(chan struct{}, w.MaxConcurrency)
		for i := 0; i < w.MaxConcurrency; i++ {
			j.concurrency[topic] <- struct{}{}
		}

		//存放消息数据的通道
		j.tasksChan[topic] = make(chan Task, 0)
	}
}

//设置worker默认并发数
func (j *Job) SetConcurrency(concurrency int) {
	if concurrency <= 0 {
		return
	}
	j.con = concurrency
}

//设置标准输出日志等级
func (j *Job) SetSleepy(sleepy time.Duration) {
	j.sleepy = sleepy
}

//设置标准输出日志等级
func (j *Job) SetConsoleLevel(level uint8) {
	j.consoleLevel = level
}

//设置文件输出日志等级
func (j *Job) SetLevel(level uint8) {
	j.level = level
}

//设置日志服务
func (j *Job) SetLogger(logger Logger) {
	j.logger = logger
}

//针对性开启topics
func (j *Job) SetEnableTopics(topics ...string) {
	j.enabledTopics = topics
}

//topic是否开启 备注：空的时候默认启用全部
func (j *Job) isTopicEnable(topic string) bool {
	if len(j.enabledTopics) == 0 {
		return true
	}

	for _, t := range j.enabledTopics {
		if t == topic {
			return true
		}
	}
	return false
}

//启动拉取队列数据服务
func (j *Job) runQueues() {
	topicMap := make(map[string]bool)

	for topic, _ := range j.workers {
		if j.isTopicEnable(topic) {
			topicMap[topic] = true
		}
	}
	j.println(Debug, "topicMap", topicMap)

	for _, qm := range j.queueMangers {
		for _, topic := range qm.topics {
			validTopics := make([]string, 0)
			if _, ok := topicMap[topic]; ok {
				validTopics = append(validTopics, topic)
				delete(topicMap, topic)
			}
			j.println(Debug, "validTopics", validTopics)
			if len(validTopics) > 0 {
				for _, topic := range validTopics {
					go j.watchQueueTopic(qm.queue, topic)
				}
			}
		}
	}

	if j.defaultQueue == nil {
		return
	}

	remainTopics := make([]string, 0)
	for topic, ok := range topicMap {
		if ok == true {
			remainTopics = append(remainTopics, topic)
		}
	}
	j.println(Debug, "remainTopics", remainTopics)
	if len(remainTopics) > 0 {
		for _, topic := range remainTopics {
			go j.watchQueueTopic(j.defaultQueue, topic)
		}
	}
}

//监听队列某个topic
func (j *Job) watchQueueTopic(q Queue, topic string) {
	j.setQueueMap(q, topic)
	j.println(Info, "watch queue topic", topic)

	for {
		if !j.running {
			j.println(Info, "stop watch queue topic", topic)
			return
		}

		j.pullTask(q, topic)
	}
}

//topic与queue的map映射关系表，主要是ack通过Topic获取
func (j *Job) setQueueMap(q Queue, topic string) {
	j.qLock.Lock()
	j.queueMap[topic] = q
	j.qLock.Unlock()
}

//获取topic对应的queue服务
func (j *Job) getQueueByTopic(topic string) Queue {
	j.qLock.RLock()
	q := j.queueMap[topic]
	j.qLock.RUnlock()
	return q
}

//拉取队列消息
func (j *Job) pullTask(q Queue, topic string) {
	j.wg.Add(1)
	defer j.wg.Done()

	message, token, err := q.Dequeue(j.ctx, topic)
	atomic.AddInt64(&j.pullCount, 1)
	if err != nil {
		atomic.AddInt64(&j.pullErrCount, 1)
		j.logAndPrintln(Error, "dequeue_error", err, message)
		time.Sleep(j.sleepy)
		return
	}

	//无消息时，sleep
	if message == "" {
		j.println(Trace, "empty message", topic)
		atomic.AddInt64(&j.pullEmptyCount, 1)
		time.Sleep(j.sleepy)
		return
	}
	atomic.AddInt64(&j.taskCount, 1)

	task, err := DecodeStringTask(message)
	if err != nil {
		atomic.AddInt64(&j.taskErrCount, 1)
		j.logAndPrintln(Error, "decode_task_error", err, message)
		time.Sleep(j.sleepy)
		return
	} else if task.Topic != "" {
		task.Token = token
	}

	j.tLock.RLock()
	tc := j.tasksChan[topic]
	j.tLock.RUnlock()

	for {
		select {
		case tc <- task:
			j.println(Debug, "taskChan push after", task, time.Now())
			return

		case <-time.After(j.timer):
			//如果队列暂停了，先紧急处理任务
			if !j.running {
				j.processTask(topic, task)
				fmt.Println("stop handle", topic, task, j.taskCount)
				return
			}
			continue
		}
	}
}

/**
 * 往Job注入Queue服务
 */
func (j *Job) AddQueue(q Queue, topics ...string) {
	if len(topics) > 0 {
		qm := queueManger{
			queue:  q,
			topics: topics,
		}
		j.queueMangers = append(j.queueMangers, qm)
	} else {
		j.defaultQueue = q
	}
}

/**
 * 暂停Job
 */
func (j *Job) Stop() {
	if !j.running {
		return
	}
	j.running = false
}

/**
 * 等待队列任务消费完成，可设置超时时间返回
 * @param timeout 如果小于0则默认10秒
 */
func (j *Job) WaitStop(timeout time.Duration) error {
	ch := make(chan struct{})

	time.Sleep((j.timer + j.sleepy) * 2)
	if timeout <= 0 {
		timeout = time.Second * 10
	}

	go func() {
		j.wg.Wait()
		close(ch)
	}()

	select {
	case <-ch:
		return nil
	case <-time.After(timeout):
		return errors.New("timeout")
	}

	return nil
}

func (j *Job) AddFunc(topic string, f func(task Task) (TaskResult), args ...interface{}) error {
	//worker并发数
	var concurrency int
	if len(args) > 0 {
		if c, ok := args[0].(int); ok {
			concurrency = c
		}
	}
	w := &Worker{Call: MyWorkerFunc(f), MaxConcurrency: concurrency}
	return j.AddWorker(topic, w)
}

func (j *Job) AddWorker(topic string, w *Worker) error {
	j.wLock.Lock()
	defer j.wLock.Unlock()

	if _, ok := j.workers[topic]; ok {
		return errors.New("the key had been registered")
	}

	j.workers[topic] = w

	j.printf(Info, "topic(%s) concurrency %d\n", topic, w.MaxConcurrency)
	return nil
}

//获取统计数据
func (j *Job) Stats() map[string]int64 {
	return map[string]int64{
		"pull":       j.pullCount,
		"pull_err":   j.pullErrCount,
		"pull_empty": j.pullEmptyCount,
		"task":       j.taskCount,
		"task_err":   j.taskErrCount,
		"handle":     j.handleCount,
		"handle_err": j.handleErrCount,
	}
}

func (j *Job) processJob() {
	for topic, taskChan := range j.tasksChan {
		go j.processWork(topic, taskChan)
	}
}

//读取通道数据分发到各个topic对应的worker进行处理
func (j *Job) processWork(topic string, taskChan <-chan Task) {
	j.cLock.RLock()
	c := j.concurrency[topic]
	j.cLock.RUnlock()

	for {
		select {
		case <-c:
			select {
			case task := <-taskChan:
				go j.processTask(topic, task)
			case <-time.After(j.timer):
				c <- struct{}{}
			}
		case <-time.After(j.timer):
			continue
		}
	}
}

//处理task任务
func (j *Job) processTask(topic string, task Task) TaskResult {
	j.wg.Add(1)
	defer func() {
		j.wg.Done()
		j.concurrency[topic] <- struct{}{}

		if e := recover(); e != nil {
			j.logAndPrintln(Fatal, "task_recover", task, e)
		}
	}()

	j.wLock.RLock()
	w := j.workers[topic]
	j.wLock.RUnlock()

	result := w.Call.Run(task)
	//多线程安全加减
	atomic.AddInt64(&j.handleCount, 1)

	if task.Token != "" {
		if result.State == StateSucceed || result.State == StateFailedWithAck {
			_, err := j.getQueueByTopic(topic).AckMsg(j.ctx, topic, task.Token)
			if err != nil {
				j.logAndPrintln(Error, "ack_error", topic, task)
			}
		}

		if result.State == StateFailedWithAck || result.State == StateFailed {
			j.handleErrCount++
		}
	}

	return result
}

//是否达到标准输出等级
func (j *Job) reachConsoleLevel(level uint8) bool {
	return level >= j.consoleLevel
}

//标准输出
func (j *Job) println(level uint8, a ...interface{}) {
	if !j.reachConsoleLevel(level) {
		return
	}
	fmt.Println(a...)
}

//格式化标准输出
func (j *Job) printf(level uint8, format string, a ...interface{}) {
	if !j.reachConsoleLevel(level) {
		return
	}
	fmt.Printf(format, a...)
}

//是否达到输出日志等级
func (j *Job) reachLevel(level uint8) bool {
	return level >= j.level
}

//打印日志
func (j *Job) log(level uint8, a ...interface{}) {
	if j.logger == nil {
		return
	}
	if !j.reachLevel(level) {
		return
	}
	switch level {
	case Trace:
		j.logger.Trace(a...)
	case Debug:
		j.logger.Debug(a...)
	case Info:
		j.logger.Info(a...)
	case Warn:
		j.logger.Warn(a...)
	case Error:
		j.logger.Error(a...)
	case Fatal:
		j.logger.Fatal(a...)
	}
}

//格式化打印日志
func (j *Job) logf(level uint8, format string, a ...interface{}) {
	if j.logger == nil {
		return
	}
	if !j.reachLevel(level) {
		return
	}
	switch level {
	case Trace:
		j.logger.Tracef(format, a...)
	case Debug:
		j.logger.Debugf(format, a...)
	case Info:
		j.logger.Infof(format, a...)
	case Warn:
		j.logger.Warnf(format, a...)
	case Error:
		j.logger.Errorf(format, a...)
	case Fatal:
		j.logger.Fatalf(format, a...)
	}
}

//日志和标准输出
func (j *Job) logAndPrintln(level uint8, a ...interface{}) {
	j.log(level, a...)
	j.println(level, a...)
}

func (j *Job) LogfAndPrintf(level uint8, format string, a ...interface{}) {
	j.logf(level, format, a...)
	j.printf(level, format, a...)
}
