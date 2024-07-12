package funnydb

import (
	"context"
	"fmt"
	"log"
)

type consoleProducer struct {
}

func newConsoleProducer(config Config) (producer, error) {
	producer := consoleProducer{}
	return &producer, nil
}

func (c *consoleProducer) Add(ctx context.Context, data M) error {
	jsonStr, err := marshalToString(data)
	if err != nil {
		log.Printf("consoleProducer Add MarshalToString Error: %s \n", err)
		return nil
	}

	fmt.Printf("%s\n", jsonStr)
	return nil
}

func (c *consoleProducer) Close(ctx context.Context) error {
	return nil
}
