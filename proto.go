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

	//Set it to false if you don't want to print the transfers
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

func (processor *Processor) transactionsMessageHandlerBSC(ctx context.Context, message *kafka.Message, worker int) error {
	processingTime := time.Now()
	processor.stat.record(message.Timestamp, processingTime)

	var batch evm_messages.ParsedAbiBlockMessage
	err := proto.Unmarshal(message.Value, &batch)
	if err != nil {
		return fmt.Errorf("failed to unmarshal ParsedAbiBlockMessage: %w", err)
	}

	txCount := len(batch.Transactions)

	for _, tx := range batch.Transactions {
		// TransactionHeader.Index gives per-block order
		index := tx.TransactionHeader.Index

		// Optional debug print: signature, success/failure
		// This will only print the first call in the call list (if any)
		if len(tx.Calls) > 0 {
			call := tx.Calls[0]
			Method := call.Header.Signature.Name
			success := tx.TransactionStatus.Success
			fmt.Printf("  tx[%d] %x -> %x | sig: %s | success: %t\n",
				index,
				tx.TransactionHeader.From,
				tx.TransactionHeader.To,
				Method,
				success,
			)
		}
	}

	fmt.Printf("block %d processed with lag %d ms (%d transactions) from partition %d[%s] in worker %d\n",
		batch.Header.Number,
		processingTime.Sub(message.Timestamp).Milliseconds(),
		txCount,
		message.TopicPartition.Partition,
		message.TopicPartition.Offset,
		worker,
	)

	return nil
}
