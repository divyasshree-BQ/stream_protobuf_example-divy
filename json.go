package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type JsonMessage struct {
	Block struct {
		Hash      string `json:"Hash"`
		Height    uint64 `json:"Height"`
		Slot      uint64 `json:"Slot"`
		Timestamp int64  `json:"Timestamp"`
	} `json:"Block"`
	Transaction struct {
		Index uint32 `json:"Index"`
	} `json:"Transaction"`
}

func (processor *Processor) jsonMessageHandler(ctx context.Context, message *kafka.Message, worker int) error {
	processingTime := time.Now()
	processor.stat.record(message.Timestamp, processingTime)

	var batch []JsonMessage

	err := json.Unmarshal(message.Value, &batch)
	if err != nil {
		return err
	}

	for _, t := range batch {
		processor.stat.add(t.Block.Timestamp, t.Block.Slot, t.Transaction.Index, message.Timestamp, processingTime)
	}

	fmt.Printf("slot %d processed with lag %d msec %d txs from partition %d[%s] in worker %d\n",
		batch[0].Block.Slot, processingTime.Sub(message.Timestamp).Milliseconds(),
		len(batch), message.TopicPartition.Partition, message.TopicPartition.Offset, worker)

	return nil
}
