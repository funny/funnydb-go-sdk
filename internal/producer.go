package internal

import (
	"context"
	"errors"
)

var ErrProducerClosed = errors.New("producer has been closed")

const (
	running int32 = 1
	stop    int32 = 0
)

type Producer interface {
	Add(ctx context.Context, data map[string]interface{}) error
	Close(ctx context.Context) error
}
