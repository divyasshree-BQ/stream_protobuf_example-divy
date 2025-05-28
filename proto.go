package main

import (
	"context"

	solana_messages "github.com/bitquery/streaming_protobuf/v2/solana/messages"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/golang/protobuf/proto"
)

func (p *Processor) dexTradesMessageHandler(ctx context.Context, message *kafka.Message, worker int) error {
	var batch solana_messages.DexParsedBlockMessage
	if err := proto.Unmarshal(message.Value, &batch); err != nil {
		return err
	}
	p.recordFirstSeen(batch.Header.Slot, message.Timestamp)
	return nil
}

func (p *Processor) transactionsMessageHandler(ctx context.Context, message *kafka.Message, worker int) error {
	var batch solana_messages.ParsedIdlBlockMessage
	if err := proto.Unmarshal(message.Value, &batch); err != nil {
		return err
	}
	p.recordFirstSeen(batch.Header.Slot, message.Timestamp)
	return nil
}

func (p *Processor) tokensMessageHandler(ctx context.Context, message *kafka.Message, worker int) error {
	var batch solana_messages.TokenBlockMessage
	if err := proto.Unmarshal(message.Value, &batch); err != nil {
		return err
	}
	p.recordFirstSeen(batch.Header.Slot, message.Timestamp)
	return nil
}
