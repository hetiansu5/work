package main

import (
	"github.com/hetiansu5/work"
	"context"
	"time"
	"fmt"
)

func main() {
	//实例化一个队列worker服务
	job := work.New()
	//注册worker
	RegisterWorker(job)
	//设置queue驱动
	RegisterQueueDriver(job)
	//设置参数
	SetOptions(job)
	//启动服务
	job.Start()

	//获取运行态统计数据
	//job.Stats()

	//停止服务
	//job.Stop()
	//等待服务停止，调用停止只是将服务设置为停止状态，但是可能有任务还在跑，waitStop会等待worker任务跑完后停止当前服务。
	//第一个参数为超时时间，如果无法获取到worker全部停止状态，在超时时间后会return一个超时错误
	//job.WaitStop(time.Second * 3)
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
func RegisterQueueDriver(job *work.Job) {
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
	//设置logger日志等级，默认work.Info
	job.SetLevel(work.Warn)
	//设置console输出等级,默认work.Warn
	job.SetConsoleLevel(work.Warn)

	//设置worker默认的并发数，默认为5
	job.SetConcurrency(10)

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
		RegisterQueueDriver(jb)
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