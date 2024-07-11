package funnydb

import (
	"context"
)

type producer interface {
	Add(ctx context.Context, data M) error
	Flush(ctx context.Context) error
	Close(ctx context.Context) error
}
