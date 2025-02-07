package main

import (
	"context"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"golang.org/x/sync/errgroup"
)

type Consumer interface {
	waitMessages(ctx context.Context, listener Listener)
	close()
}

type SimpleConsumer struct {
	kafkaConsumer *kafka.Consumer
}

type ConsumerConfig struct {
	Topic       string
	Partitioned bool
}

type PartitionedConsumer struct {
	kafkaConsumers []*kafka.Consumer
}

func newConsumer(config *Config) (Consumer, error) {
	if config.Consumer.Partitioned {
		return newPartitionedConsumer(config)
	}
	return newSimpleConsumer(config)
}

func newSimpleConsumer(config *Config) (*SimpleConsumer, error) {
	kafkaConsumer, err := kafka.NewConsumer(&config.Kafka)
	if err != nil {
		return nil, err
	}

	err = kafkaConsumer.Subscribe(config.Consumer.Topic, nil)
	if err != nil {
		return nil, err
	}

	return &SimpleConsumer{
		kafkaConsumer,
	}, nil
}

func (consumer *SimpleConsumer) close() {
	consumer.kafkaConsumer.Close()
}

func (consumer *SimpleConsumer) waitMessages(ctx context.Context, listener Listener) {
	err := consumerWaitMessages(ctx, consumer.kafkaConsumer, listener)
	if err != nil {
		fmt.Println("error waiting messages:", err)
	}
}

func consumerWaitMessages(ctx context.Context, consumer *kafka.Consumer, listener Listener) error {

	fmt.Println("Running consumer " + consumer.String())
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Done, exiting consumer loop")
			return nil
		default:
		}

		message, err := consumer.ReadMessage(time.Second * 60)
		if err != nil {
			return err
		}

		listener.enqueue(message)

	}
}

func newPartitionedConsumer(config *Config) (Consumer, error) {

	kafkaAdmin, err := kafka.NewAdminClient(&config.Kafka)
	if err != nil {
		return nil, err
	}
	defer kafkaAdmin.Close()

	metadata, err := kafkaAdmin.GetMetadata(&config.Consumer.Topic, false, 10000)
	if err != nil {
		return nil, err
	}

	kafkaErr := metadata.Topics[config.Consumer.Topic].Error
	if kafkaErr.Code() != kafka.ErrNoError {
		return nil, kafkaErr
	}

	partitions := metadata.Topics[config.Consumer.Topic].Partitions
	fmt.Printf("Subscribing to topic %s %d partitions: %v...\n", config.Consumer.Topic, len(partitions), partitions)

	consumers := make([]*kafka.Consumer, len(partitions))
	for i, partition := range partitions {
		consumer, err := kafka.NewConsumer(&config.Kafka)
		if err != nil {
			return nil, err
		}
		err = consumer.Assign([]kafka.TopicPartition{{
			Topic:     &config.Consumer.Topic,
			Partition: partition.ID,
			Offset:    kafka.OffsetStored,
		}})
		if err != nil {
			return nil, err
		}
		err = consumer.Subscribe(config.Consumer.Topic, nil)
		if err != nil {
			return nil, err
		}
		consumers[i] = consumer
	}

	fmt.Printf("Assigned %d consumers to %s topic\n", len(consumers), config.Consumer.Topic)

	return &PartitionedConsumer{
		kafkaConsumers: consumers,
	}, nil
}

func (consumer *PartitionedConsumer) close() {
	for _, c := range consumer.kafkaConsumers {
		err := c.Close()
		if err != nil {
			fmt.Println("Error closing consumer: " + err.Error())
		}
	}
}

func (consumer *PartitionedConsumer) waitMessages(ctx context.Context, listener Listener) {
	var wg errgroup.Group
	for _, c := range consumer.kafkaConsumers {
		c := c
		wg.Go(func() error {
			return consumerWaitMessages(ctx, c, listener)
		})
	}
	wg.Wait()
}
