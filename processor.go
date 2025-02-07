package main

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Listener interface {
	process(message *kafka.Message)
}

type Processor struct {
}

func newProcessor(config *Config) (Listener, error) {
	return &Processor{}, nil
}

func (processor *Processor) process(message *kafka.Message) {
	fmt.Printf("Received message with key %s from partition %d[%s]\n", message.Key, message.TopicPartition.Partition, message.TopicPartition.Offset)
}
