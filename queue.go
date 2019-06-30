package work

import "context"

type Queue interface {
	Enqueue(ctx context.Context, key string, message string, args ...interface{}) (isOk bool, err error)
	Dequeue(ctx context.Context, key string) (message string, token string, err error)
	AckMsg(ctx context.Context, key string, token string) (ok bool, err error)
}
