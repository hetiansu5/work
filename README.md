## Job Worker
Queue scheduling service in Go.

## Example
```golang

function main(){
    //new a job worker
    job := work.New()
    //register worker
    RegisterWorker(job)
    //add 1ueue driver
    AddQueue(job)
    //set options
	SetOptions(job)
    //start service
    job.Start()
}

/**
 * Register worker
 */
func RegisterWorker(job *work.Job) {
	//register a worker with a callback function.
	job.AddFunc("topic:test", test)
	//register a worker with a callback function and a concurrency control param.
	job.AddFunc("topic:test1", test, 2)
	//register a worker with worker data structure.
	job.AddWorker("topic:test2", &work.Worker{Call: work.MyWorkerFunc(test), MaxConcurrency: 1})
}

/**
 * Register queue driver for topic
 */
func AddQueue(job *work.Job) {
	//you can register a queue driver for one or more topics. For queue driver, you must implement interface of work.Queue 
	job.AddQueue(queue1, "topic:test1", "topic:test2")
	//you can set a default queue driver, that will be available for the remain topics
	job.AddQueue(queue2)
}

/**
 * Task callback function
 * Remark：process is best not to be asynchronous，otherwise job service can control concurrency of worker.
 *  If you need an asynchronous process, you need to block util the process finish, such as wg.Wait()
 */
func test(task work.Task) (work.TaskResult) {
	time.Sleep(time.Millisecond * 5)
	s, err := work.JsonEncode(task)
	if err != nil {
		//work.StateFailed  will not execute ACK confirm
		//work.StateFailedWithAck will execute ACK confirm
		//return work.TaskResult{Id: task.Id, State: work.StateFailed}
		return work.TaskResult{Id: task.Id, State: work.StateFailedWithAck}
	} else {
        //work.StateSucceed will execute ACK confirm
		fmt.Println("do task", s)
		return work.TaskResult{Id: task.Id, State: work.StateSucceed}
	}

}
```