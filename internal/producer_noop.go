package internal

import (
	"context"
)

type NoopProducer struct {
}

func NewNoopProducer() (Producer, error) {
	producer := NoopProducer{}
	DefaultLogger.Info("ModeNoop starting")
	return &producer, nil
}

func (c *NoopProducer) Add(ctx context.Context, data map[string]interface{}) error {
	return nil
}

func (c *NoopProducer) Close(ctx context.Context) error {
	return nil
}
