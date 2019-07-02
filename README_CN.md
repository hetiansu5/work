## 简介
基于Go语言实现的队列Worker服务

## 示例
```golang

function main(){
    //实例化一个队列Worker服务
    job := work.New()
    //注册worker
    RegisterWorker(job)
    //设置queue驱动
    RegisterQueueDriver(job)
    //设置参数
	SetOptions(job)
    //启动服务
    job.Start()
}

/**
 * 注册任务worker
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
 * 给topic注册对应的队列服务
 */
func RegisterQueueDriver(job *work.Job) {
	//针对topic设置相关的queue,需要实现work.Queue接口的方法
	job.AddQueue(queue1, "topic:test1", "topic:test2")
	//设置默认的queue, 没有设置过的topic会使用默认的queue
	job.AddQueue(queue2)
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
		//work.StateFailedWithAck 会进行ack确认
		//return work.TaskResult{Id: task.Id, State: work.StateFailed}
		return work.TaskResult{Id: task.Id, State: work.StateFailedWithAck}
	} else {
        //work.StateSucceed 会进行ack确认
		fmt.Println("do task", s)
		return work.TaskResult{Id: task.Id, State: work.StateSucceed}
	}

}
```

## 更多
```
example/job.go 是一个详细的示例文件
example/example.go 是一个可以跑起来的测试案例，可以通过go run example/example.go运行
```