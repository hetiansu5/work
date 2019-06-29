package work

import (
	"sync"
	"errors"
	"time"
	"context"
	"fmt"
)

const (
	//默认worker的并发数
	DefaultConcurrency = 5
)

const (
	Trace = uint8(iota)
	Debug
	Info
	Warn
	Error
	Fatal
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
	wLock   sync.RWMutex

	//并发控制通道
	concurrency map[string]chan struct{}

	//队列数据通道
	tasksChan map[string]chan Task

	wg      sync.WaitGroup
	running bool
	sleepy  time.Duration
	timer   time.Duration

	//Queue服务 - 依赖外部注入
	queueMangers []queueManger
	//默认Queue服务 - 依赖外部注入
	defaultQueue Queue

	//标准输出的日志登记
	consoleLevel uint8

	//日志服务 - 依赖外部注入
	logger Logger
	level  uint8
}

func New() *Job {
	j := new(Job)
	j.ctx = context.Background()
	j.workers = make(map[string]*Worker)
	j.concurrency = make(map[string]chan struct{})
	j.tasksChan = make(map[string]chan Task)
	j.level = Info
	j.sleepy = time.Microsecond * 10
	j.timer = time.Millisecond * 10
	return j
}

func (j *Job) Start() {
	if j.running {
		return
	}

	j.running = true
	j.processJob()
	j.watchQueues()
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

func (j *Job) watchQueues() {
	topicMap := make(map[string]bool)

	for topic, _ := range j.workers {
		topicMap[topic] = true
	}

	for _, qm := range j.queueMangers {
		for _, topic := range qm.topics {
			validTopics := make([]string, 0)
			if _, ok := topicMap[topic]; ok {
				validTopics = append(validTopics, topic)
				delete(topicMap, topic)
			}
			if len(validTopics) > 0 {
				go j.watchQueue(qm.queue, validTopics...)
			}
		}
	}

	remainTopics := make([]string, 0)
	for topic, _ := range topicMap {
		remainTopics = append(remainTopics, topic)
	}
	if len(remainTopics) > 0 {
		go j.watchQueue(j.defaultQueue, remainTopics...)
	}
}

func (j *Job) watchQueue(q Queue, topics ...string) {
	var freeTopics []string
	var tpLock sync.RWMutex
	var task Task
	var topic string

	start := time.Now().Unix()
	topicMap := arrayToMap(topics)

	for {
		if !j.running {
			return
		}

		//没有task任务阻塞在通道的topic
		freeTopics = getFreeTopics(topicMap)
		if len(freeTopics) == 0 {
			time.Sleep(j.sleepy)
			continue
		}

		val, err := q.Dequeue(j.ctx, freeTopics...)
		if err != nil {
			j.log(Error, "watch_queue_error", err)
			j.println(Error, "watch_queue_error", err)
			time.Sleep(j.sleepy)
			continue
		}

		if j.reachConsoleLevel(Debug) {
			end := time.Now().Unix()
			if end-start >= 1 {
				start = end
				j.println(Debug, "dequeue", freeTopics, val)
			}
		}

		tasks := make([]Task, 0)
		switch val.(type) {
		case []interface{}:
			for _, v := range val.([]interface{}) {
				switch v.(type) {
				case string:
					task, err = DecodeStringTask(v.(string))
				case []byte:
					task, err = DecodeBytesTask(v.([]byte))
				case nil:
					task = Task{}
					err = nil
				default:
					task = Task{}
					err = errors.New("no support data type")
				}

				if err != nil {
					j.log(Error, "decode_task_error", err)
					j.println(Error, "decode_task_error", err)
				} else if task.Topic != "" {
					tasks = append(tasks, task)
				}
			}
		default:
			j.log(Warn, "unknown_task_format", val)
			j.println(Warn, "unknown_task_format", val)
			continue
		}

		for _, task = range tasks {
			topic = task.Topic

			if _, ok := j.tasksChan[topic]; !ok {
				j.log(Error, "task topic is not match", task)
				continue
			}

			tpLock.Lock()
			topicMap[topic] = false
			tpLock.Unlock()

			j.println(Debug, "taskChan push before", task, time.Now())

			go func(tp string, tk Task) {
				for {
					select {
					case j.tasksChan[tp] <- tk:
						tpLock.Lock()
						topicMap[tp] = true
						tpLock.Unlock()
						j.println(Debug, "taskChan push after", tk, time.Now())
						return

					case time.After(j.timer):
						//如果队列暂停了，先紧急处理任务
						if !j.running {
							go j.processTask(tp, tk)
							return
						}
						continue
					}
				}
			}(topic, task)
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
 * @param timeout 毫秒
 */
func (j *Job) WaitStop(timeout time.Duration) error {
	ch := make(chan struct{})

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

func (j *Job) AddFunc(topic string, f func(task Task) (TaskResult), args ...interface{}) {
	w := &Worker{Call: MyWorkerFunc(f)}
	j.AddWorker(topic, w, args...)
}

func (j *Job) AddWorker(topic string, w *Worker, args ...interface{}) error {
	//worker并发数
	var concurrency int
	if len(args) > 0 {
		if c, ok := args[0].(int); ok {
			concurrency = c
		}
	}
	if concurrency <= 0 {
		concurrency = DefaultConcurrency
	}
	w.MaxConcurrency = concurrency

	j.wLock.Lock()
	defer j.wLock.Unlock()

	if _, ok := j.workers[topic]; ok {
		return errors.New("the key had been registered")
	}

	j.workers[topic] = w
	j.tasksChan[topic] = make(chan Task, 0)
	j.concurrency[topic] = make(chan struct{}, concurrency)
	for i := 0; i < concurrency; i++ {
		j.concurrency[topic] <- struct{}{}
	}

	j.println(Info, "topic(%s) concurrency %d\n", topic, concurrency)
	return nil
}

func (j *Job) processJob() {
	for topic, taskChan := range j.tasksChan {
		go j.processWork(topic, taskChan)
	}
}

//读取通道数据分发到各个topic对应的worker进行处理
func (j *Job) processWork(topic string, taskChan <-chan Task) {
	for {
		if !j.running {
			return
		}

		select {
		case <-j.concurrency[topic]:
			select {
			case task := <-taskChan:
				go j.processTask(topic, task)

			case <-time.After(j.timer):
				j.concurrency[topic] <- struct{}{}
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

		if err := recover(); err != nil {
			j.log(Fatal, "task_recover", task, err)
			j.println(Fatal, "task_recover", task, err)
		}
	}()

	j.wLock.RLock()
	w := j.workers[topic]
	j.wLock.RUnlock()

	return w.Call.Run(task)
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
