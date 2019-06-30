package work

import "context"

type Queue interface {
	Dequeue(ctx context.Context, key string) (message string, token string, err error)
	AckMsg(ctx context.Context, key string, token string) (ok bool, err error)
}
