package work

type Worker struct {
	Call           WorkerFunc
	MaxConcurrency int
}

type WorkerFunc interface {
	Run(task Task) TaskResult
}

type MyWorkerFunc func(task Task) (TaskResult)

func (f MyWorkerFunc) Run(task Task) (TaskResult) {
	return f(task)
}
