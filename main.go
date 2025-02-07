package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"gopkg.in/yaml.v3"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Config struct {
	Kafka     kafka.ConfigMap
	Consumer  ConsumerConfig
	Processor ProcessorConfig
}

func main() {

	file, err := os.Open("config.yml")
	if err != nil {
		panic(fmt.Errorf("error opening config file: %v, copy original file config_example.yml to config.yml and edit it", err))
	}
	defer file.Close()

	bytes, err := io.ReadAll(file)
	if err != nil {
		panic(err)
	}

	var config Config
	err = yaml.Unmarshal(bytes, &config)
	if err != nil {
		panic(err)
	}

	consumer, err := newConsumer(&config)
	if err != nil {
		panic(err)
	}
	defer consumer.close()

	processor, err := newProcessor(&config)
	if err != nil {
		panic(err)
	}
	defer processor.close()

	ctx, cancel := context.WithCancel(context.Background())
	fmt.Println("press Ctrl-C to exit")
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		select {
		case <-signalCh:
			cancel()
			fmt.Println("received Ctrl-C, finishing jobs...")
			return
		}
	}()

	processor.start(ctx)
	consumer.waitMessages(ctx, processor)
}
