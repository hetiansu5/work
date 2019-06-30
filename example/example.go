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
	RegisterWorker(job)
	job.AddQueue(q)
	job.AddQueue(q2, "kxy1")
	job.SetLogger(new(MyLogger))
	job.SetConsoleLevel(work.Info)

	pushQueueData(q, "hts1")
	pushQueueData(q, "hts2")
	pushQueueData(q2, "kxy1")

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

func pushQueueData(q *LocalQueue, topic string) {
	ctx := context.Background()
	start := 1
	length := 5000

	strs := make([]string, 0)
	for i := start; i < start+length; i++ {
		task := work.Task{Id: strconv.Itoa(i), Topic: topic}
		str, _ := work.JsonEncode(task)
		strs = append(strs, str)
	}
	q.Enqueue(ctx, topic, strs...)
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
func RegisterWorker(job *work.Job) {
	job.AddFunc("hts1", Me, 10)
	job.AddFunc("hts2", Me, 6)
	job.AddWorker("kxy1", &work.Worker{Call: work.MyWorkerFunc(Me), MaxConcurrency: 30})
}

func Me(task work.Task) (work.TaskResult) {
	time.Sleep(time.Millisecond * 5)
	//s, _ := work.JsonEncode(task)
	return work.TaskResult{Id: task.Id}
}

type LocalQueue struct{}

func (q *LocalQueue) Enqueue(ctx context.Context, key string, values ...string) (ok bool, err error) {
	lock.Lock()
	defer lock.Unlock()

	if _, ok = queues[key]; !ok {
		queues[key] = make([]string, 0)
	}

	queues[key] = append(queues[key], values...)
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
