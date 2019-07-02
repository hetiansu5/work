## go-workers
Processes background queue's jobs in Go.

## example
```golang
//示例化一个Job服务
job := work.New()
registerWorker(job)
//启动服务
job.Start()


/**
 * 配置队列任务
 */
func RegisterWorker(job *work.Job) {
	//worker未设置并发数，使用默认值
	job.AddFunc("topic:test", test)
	//worker并发数
	job.AddFunc("topic:test1", test, 2)
	//使用worker结构进行注册
	job.AddWorker("topic:test2", &work.Worker{Call: work.MyWorkerFunc(test), MaxConcurrency: 1})

	AddQueue(job)

	SetOptions(job)
}

/**
 * 给topic注册对应的队列服务
 */
func AddQueue(job *work.Job) {
	//设置队列服务，需要实现work.Queue接口的方法

	//针对topic设置相关的queue
	job.AddQueue(queue1, "topic:test1", "topic:test2")
	//设置默认的queue, 没有设置过的topic会使用默认的queue
	job.AddQueue(queue2)
}

/**
 * 设置配置参数
 */
func SetOptions(job *work.Job) {
	//设置logger，需要实现work.Logger接口的方法
	job.SetLogger(Logger)

	//设置启用的topic，未设置表示启用全部注册过topic
	job.SetEnableTopics(topics...)
}

var jb *work.Job

func getJob() *work.Job {
	if jb == nil {
		jb = work.New()
		RegisterWorker(jb)
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
```