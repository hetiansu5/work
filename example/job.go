package main

import (
	"github.com/hetiansu5/work"
	"context"
	"time"
	"fmt"
)

func main() {
	//实例化一个队列调度服务
	job := work.New()
	//注册worker
	RegisterWorker(job)
	//设置Queue驱动
	AddQueue(job)
	//设置参数
	SetOptions(job)
	//启动服务
	job.Start()
}

/**
 * 注册worker
 */
func RegisterWorker(job *work.Job) {
	//设置worker的任务投递回调函数
	job.AddFunc("topic:test", test)
	//设置worker的任务投递回调函数，和并发数
	job.AddFunc("topic:test1", test, 2)
	//使用worker结构进行注册
	job.AddWorker("topic:test2", &work.Worker{Call: work.MyWorkerFunc(test), MaxConcurrency: 1})
}

/**
 * 给topic注册对应的Queue驱动
 */
func AddQueue(job *work.Job) {
	//针对topic设置相关的queue,需要实现work.Queue接口的方法
	job.AddQueue(&LocalQueue{}, "topic:test1", "topic:test2")
	//设置默认的queue, 没有设置过的topic会使用默认的queue
	job.AddQueue(&LocalQueue{})
}

/**
 * 设置配置参数
 */
func SetOptions(job *work.Job) {
	//设置logger，需要实现work.Logger接口的方法
	job.SetLogger(&MyLogger{})

	//设置启用的topic，未设置表示启用全部注册过topic
	job.SetEnableTopics("topic:test1", "topic:test2")
}

/**
 * 任务投递回调函数
 * 备注：任务处理逻辑不要异步化，否则无法控制worker并发数，需要异步化的需要阻塞等待异步化结束，如wg.Wait()
 */
func test(task work.Task) (work.TaskResult) {
	time.Sleep(time.Millisecond * 5)
	s, err := work.JsonEncode(task)
	if err != nil {
		//work.StateFailed 不会进行ack确认
		//work.StateFailedWithAck 会进行act确认
		//return work.TaskResult{Id: task.Id, State: work.StateFailed}
		return work.TaskResult{Id: task.Id, State: work.StateFailedWithAck}
	} else {
		//work.StateSucceed 会进行ack确认
		fmt.Println("do task", s)
		return work.TaskResult{Id: task.Id, State: work.StateSucceed}
	}

}

var jb *work.Job

func getJob() *work.Job {
	if jb == nil {
		jb = work.New()
		RegisterWorker(jb)
		AddQueue(jb)
	}
	return jb
}

/**
 * 消息入队 -- 原始message
 */
func Enqueue(ctx context.Context, topic string, message string, args ...interface{}) (isOk bool, err error) {
	return getJob().Enqueue(ctx, topic, message, args...)
}

/**
 * 消息入队 -- Task数据结构
 */
func EnqueueWithTask(ctx context.Context, topic string, task work.Task, args ...interface{}) (isOk bool, err error) {
	return getJob().EnqueueWithTask(ctx, topic, task, args...)
}

/**
 * 消息批量入队 -- 原始message
 */
func BatchEnqueue(ctx context.Context, topic string, messages []string, args ...interface{}) (isOk bool, err error) {
	return getJob().BatchEnqueue(ctx, topic, messages, args...)
}

/**
 * 消息批量入队 -- Task数据结构
 */
func BatchEnqueueWithTask(ctx context.Context, topic string, tasks []work.Task, args ...interface{}) (isOk bool, err error) {
	return getJob().BatchEnqueueWithTask(ctx, topic, tasks, args...)
}