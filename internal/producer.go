package internal

import (
	"context"
)

type Producer interface {
	Add(ctx context.Context, data map[string]interface{}) error
	Close(ctx context.Context) error
}
