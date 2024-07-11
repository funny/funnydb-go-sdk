package funnydb

import (
	"context"
	"fmt"
	"log"
)

type ConsoleProducer struct {
}

func newConsoleProducer(config *ClientConfig) (Producer, error) {
	producer := ConsoleProducer{}
	return &producer, nil
}

func (c *ConsoleProducer) Add(ctx context.Context, data Reportable) error {
	props, err := data.transformToReportableData()
	if err != nil {
		log.Printf("ConsoleProducer Add ToProps Error: %s \n", err)
		return nil
	}
	jsonStr, err := marshalToString(props)
	if err != nil {
		log.Printf("ConsoleProducer Add MarshalToString Error: %s \n", err)
		return nil
	}

	fmt.Printf("%s\n", jsonStr)
	return nil
}

func (c *ConsoleProducer) Flush(ctx context.Context) error {
	return nil
}

func (c *ConsoleProducer) Close(ctx context.Context) error {
	return nil
}
