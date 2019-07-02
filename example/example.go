package main

import (
	"github.com/hetiansu5/work"
	"fmt"
	"context"
	"sync"
	"strconv"
	"time"
)

var (
	queues map[string][]string
	lock   sync.RWMutex
)

func init() {
	queues = make(map[string][]string)
}

func main() {
	stop := make(chan int, 0)
	q := new(LocalQueue)
	q2 := new(LocalQueue)

	job := work.New()
	RegisterWorker2(job)
	job.AddQueue(q)
	job.AddQueue(q2, "kxy1")
	job.SetLogger(new(MyLogger))
	job.SetConsoleLevel(work.Info)
	job.SetEnableTopics("kxy1", "hts2")

	job.Enqueue(nil, "kxy1", "who am i")

	pushQueueData(job, "hts1", 10)
	pushQueueData(job, "hts2", 10, 100)
	pushQueueData(job, "kxy1", 10, 1000)

	job.Start()
	go jobStats(job)

	//job.Stop()
	//job.WaitStop(0)

	//time.Sleep(time.Millisecond * 1000)
	//stat := job.Stats()
	//fmt.Println(stat)
	//fmt.Println(len(queues["hts1"]) + len(queues["kxy1"]))

	<-stop
}

func pushQueueData(job *work.Job, topic string, args ...int) {
	ctx := context.Background()
	start := 1
	length := 1
	if len(args) > 0 {
		length = args[0]
		if len(args) > 1 {
			start = args[1]
		}
	}

	strs := make([]string, 0)
	for i := start; i < start+length; i++ {
		strs = append(strs, strconv.Itoa(i))
	}
	if len(args) > 0 {
		job.BatchEnqueue(ctx, topic, strs)
	}
}

func jobStats(job *work.Job) {
	lastStat := job.Stats()
	var stat map[string]int64
	var count int64
	var str string
	for {
		stat = job.Stats()

		str = ""
		for k, v := range stat {
			count = v - lastStat[k]
			if count > 0 {
				str += k + ":" + strconv.FormatInt(count, 10) + "|"
			}
		}
		fmt.Println(time.Now(), str)

		lastStat = stat
		time.Sleep(time.Second)
	}
}

/**
 * 配置队列任务
 */
func RegisterWorker2(job *work.Job) {
	job.AddFunc("hts1", Me, 10)
	job.AddFunc("hts2", Me, 6)
	job.AddWorker("kxy1", &work.Worker{Call: work.MyWorkerFunc(Me), MaxConcurrency: 30})
}

func Me(task work.Task) (work.TaskResult) {
	time.Sleep(time.Millisecond * 5)
	s, _ := work.JsonEncode(task)
	fmt.Println("do task", s)
	return work.TaskResult{Id: task.Id}
}

type LocalQueue struct{}

func (q *LocalQueue) Enqueue(ctx context.Context, key string, message string, args ...interface{}) (ok bool, err error) {
	lock.Lock()
	defer lock.Unlock()

	if _, ok = queues[key]; !ok {
		queues[key] = make([]string, 0)
	}

	queues[key] = append(queues[key], message)
	return true, nil
}

func (q *LocalQueue) BatchEnqueue(ctx context.Context, key string, messages []string, args ...interface{}) (ok bool, err error) {
	lock.Lock()
	defer lock.Unlock()

	if _, ok = queues[key]; !ok {
		queues[key] = make([]string, 0)
	}

	queues[key] = append(queues[key], messages...)
	return true, nil
}

func (q *LocalQueue) Dequeue(ctx context.Context, key string) (message string, token string, err error) {
	lock.Lock()
	defer lock.Unlock()

	if len(queues[key]) > 0 {
		message = queues[key][0]
		queues[key] = queues[key][1:]
	}

	return
}

func (q *LocalQueue) AckMsg(ctx context.Context, key string, token string) (bool, error) {
	return true, nil
}

type MyLogger struct{}

func (logger *MyLogger) Trace(v ...interface{}) {

}

func (logger *MyLogger) Tracef(format string, args ...interface{}) {

}

func (logger *MyLogger) Debug(v ...interface{}) {

}

func (logger *MyLogger) Debugf(format string, args ...interface{}) {

}

func (logger *MyLogger) Info(v ...interface{}) {

}

func (logger *MyLogger) Infof(format string, args ...interface{}) {

}

func (logger *MyLogger) Warn(v ...interface{}) {

}

func (logger *MyLogger) Warnf(format string, args ...interface{}) {

}

func (logger *MyLogger) Error(v ...interface{}) {

}

func (logger *MyLogger) Errorf(format string, args ...interface{}) {

}

func (logger *MyLogger) Fatal(v ...interface{}) {

}

func (logger *MyLogger) Fatalf(format string, args ...interface{}) {

}
