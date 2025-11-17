package internal

import (
	"context"
	"fmt"
)

type ConsoleProducer struct {
}

func NewConsoleProducer() (Producer, error) {
	producer := ConsoleProducer{}
	DefaultLogger.Info("ModeDebug starting")
	return &producer, nil
}

func (c *ConsoleProducer) Add(ctx context.Context, data map[string]interface{}) error {
	jsonStr, err := marshalToString(data)
	if err != nil {
		return err
	}

	fmt.Printf("%s\n", jsonStr)
	return nil
}

func (c *ConsoleProducer) Close(ctx context.Context) error {
	return nil
}
