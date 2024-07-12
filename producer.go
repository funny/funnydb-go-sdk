package funnydb

import (
	"context"
)

type producer interface {
	Add(ctx context.Context, data M) error
	Close(ctx context.Context) error
}
