package main

import (
	"context"
	"fmt"
	"time"

	evm_messages "github.com/bitquery/streaming_protobuf/v2/evm/messages"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/golang/protobuf/proto"
)

func (processor *Processor) tokensMessageHandlerBSC(ctx context.Context, message *kafka.Message, worker int) error {
	processingTime := time.Now()
	processor.stat.record(message.Timestamp, processingTime)

	var batch evm_messages.TokenBlockMessage
	err := proto.Unmarshal(message.Value, &batch)
	if err != nil {
		return fmt.Errorf("failed to unmarshal TokenBlockMessage: %w", err)
	}

	transfers := len(batch.Transfers)

	fmt.Printf("block %d processed with lag %d ms (%d token transfers) from partition %d[%s] in worker %d\n",
		batch.Header.Number,
		processingTime.Sub(message.Timestamp).Milliseconds(),
		transfers,
		message.TopicPartition.Partition,
		message.TopicPartition.Offset,
		worker,
	)

	// Optional: Print detailed info for each transfer (disable for high throughput topics)
	verbose := true
	if verbose {
		for i, transfer := range batch.Transfers {
			fmt.Printf("  [%d] %s -> %s | amount: %x | token: %s (%s)\n",
				i,
				fmt.Sprintf("%x", transfer.Sender),
				fmt.Sprintf("%x", transfer.Receiver),
				transfer.Amount,
				transfer.Currency.Name,
				transfer.Currency.Symbol,
			)
		}
	}

	return nil
}
