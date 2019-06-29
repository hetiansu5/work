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
	queues map[string][]interface{}
	lock   sync.RWMutex
)

func init() {
	queues = make(map[string][]interface{})
}

func main() {
	ctx := context.Background()
	stop := make(chan int, 0)
	q := new(LocalQueue)

	job := work.New()
	RegisterWorker(job)
	job.AddQueue(q)
	job.SetLogger(new(MyLogger))
	job.Start()

	go func() {
		for i := 1; i <= 10; i++ {
			time.Sleep(time.Millisecond * 300)
			topic := "hts"
			task := work.Task{Id: strconv.Itoa(i), Topic: topic}
			str, _ := work.JsonEncode(task)
			q.Enqueue(ctx, topic, str)
			fmt.Println("push", str)
		}
	}()

	<-stop
}

/**
 * 配置队列任务
 */
func RegisterWorker(job *work.Job) {
	job.AddFunc("hts", Me, 1)
}

func Me(task work.Task) (work.TaskResult) {
	time.Sleep(time.Second * 1)
	s, _ := work.JsonEncode(task)
	fmt.Println("handle", s, time.Now())
	if task.Id == "5" {
		panic("interrupt")
	}
	return work.TaskResult{Id: "1"}
}

type LocalQueue struct{}

func (q *LocalQueue) Enqueue(ctx context.Context, key string, values ...interface{}) (bool, error) {
	lock.Lock()
	defer lock.Unlock()

	if _, ok := queues[key]; !ok {
		queues[key] = make([]interface{}, 0)
	}

	queues[key] = append(queues[key], values...)
	return true, nil
}

func (q *LocalQueue) Dequeue(ctx context.Context, keys ...string) (interface{}, error) {
	lock.Lock()
	defer lock.Unlock()

	arr := make([]interface{}, len(keys))
	for k, key := range keys {
		arr[k] = nil
		if _, ok := queues[key]; ok {
			if len(queues[key]) > 0 {
				arr[k] = queues[key][0]
				queues[key] = queues[key][1:]
			}
		}
	}

	return arr, nil
}

func (q *LocalQueue) AckMsg(ctx context.Context, key string, args ...interface{}) (bool, error) {
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
