package work

import "context"

type Queue interface {
	Enqueue(ctx context.Context, key string, values ...interface{}) (bool, error)
	Dequeue(ctx context.Context, keys ...string) (interface{}, error)
	AckMsg(ctx context.Context, key string, args ...interface{}) (bool, error)
}
