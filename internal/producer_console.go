package internal

import (
	"context"
	"fmt"
	"log"
)

type ConsoleProducer struct {
}

func NewConsoleProducer() (Producer, error) {
	producer := ConsoleProducer{}
	return &producer, nil
}

func (c *ConsoleProducer) Add(ctx context.Context, data map[string]interface{}) error {
	jsonStr, err := marshalToString(data)
	if err != nil {
		log.Printf("ConsoleProducer Add MarshalToString Error: %s \n", err)
		return nil
	}

	fmt.Printf("%s\n", jsonStr)
	return nil
}

func (c *ConsoleProducer) Close(ctx context.Context) error {
	return nil
}
