package consumer

import (
	"fmt"
	"git.sofunny.io/data-analysis/funnydb-go-sdk/src/utils"
	"log"
)

type ConsoleConsumer struct {
}

type ConsoleConsumerConfig struct {
}

func NewConsoleConsumer(config *ConsoleConsumerConfig) (Consumer, error) {
	consumer := ConsoleConsumer{}
	return &consumer, nil
}

func (c *ConsoleConsumer) Add(data Reportable) error {
	props, err := data.TransformToReportableData()
	if err != nil {
		log.Printf("ConsoleConsumer Add ToProps Error: %s \n", err)
		return nil
	}
	jsonStr, err := utils.MarshalToString(props)
	if err != nil {
		log.Printf("ConsoleConsumer Add MarshalToString Error: %s \n", err)
		return nil
	}

	fmt.Printf("%s\n", jsonStr)
	return nil
}

func (c *ConsoleConsumer) Flush() error {
	return nil
}

func (c *ConsoleConsumer) Close() error {
	return nil
}
