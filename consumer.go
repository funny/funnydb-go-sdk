package funnydb

import "context"

type Consumer interface {
	Add(ctx context.Context, data Reportable) error
	Flush(ctx context.Context) error
	Close(ctx context.Context) error
}
