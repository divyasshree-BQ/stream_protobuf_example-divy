package main

import (
	"context"
	"fmt"
	"time"

	solana_messages "github.com/bitquery/streaming_protobuf/v2/solana/messages"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/golang/protobuf/proto"
)

func (processor *Processor) dexTradesMessageHandler(ctx context.Context, message *kafka.Message, worker int) error {
	processingTime := time.Now()
	processor.stat.record(message.Timestamp, processingTime)

	var batch solana_messages.DexParsedBlockMessage
	err := proto.Unmarshal(message.Value, &batch)
	if err != nil {
		return err
	}
	// Call the bridging logic to get USD PRICE
	// bridgeTradePrices(&batch)
	count := 0
	for _, t := range batch.Transactions {
		count += len(t.Trades)
		processor.stat.add(batch.Header.Timestamp, batch.Header.Slot, t.Index, message.Timestamp, processingTime)
		for _, t := range batch.Transactions {
			count += len(t.Trades)
			processor.stat.add(batch.Header.Timestamp, batch.Header.Slot, t.Index, message.Timestamp, processingTime)

			fmt.Printf("  Transaction Index: %d, Signature: %x, Trades: %d\n", t.Index, t.Signature, len(t.Trades))
			for _, trade := range t.Trades {
				fmt.Printf("    Trade - Protocol: %s, Market: %x\n", trade.Dex.ProtocolName, trade.Market.MarketAddress)
				fmt.Printf("      Buy Amount: %d, Sell Amount: %d\n", trade.Buy.Amount, trade.Sell.Amount)
			}
		} //uncomment this block to print detailed transaction info

	}

	fmt.Printf("slot %d processed with lag %d msec %d txs (%d trades) from partition %d[%s] in worker %d\n",
		batch.Header.Slot, processingTime.Sub(message.Timestamp).Milliseconds(),
		len(batch.Transactions), count, message.TopicPartition.Partition, message.TopicPartition.Offset, worker)

	return nil
}

func (processor *Processor) transactionsMessageHandler(ctx context.Context, message *kafka.Message, worker int) error {
	processingTime := time.Now()
	processor.stat.record(message.Timestamp, processingTime)

	var batch solana_messages.ParsedIdlBlockMessage
	err := proto.Unmarshal(message.Value, &batch)
	if err != nil {
		return err
	}
	for _, t := range batch.Transactions {
		processor.stat.add(batch.Header.Timestamp, batch.Header.Slot, t.Index, message.Timestamp, processingTime)
		// Print the parsed data for debugging

	}

	fmt.Printf("slot %d processed with lag %d msec %d txs from partition %d[%s] in worker %d\n",
		batch.Header.Slot, processingTime.Sub(message.Timestamp).Milliseconds(),
		len(batch.Transactions), message.TopicPartition.Partition, message.TopicPartition.Offset, worker)
	return nil
}

func (processor *Processor) tokensMessageHandler(ctx context.Context, message *kafka.Message, worker int) error {
	processingTime := time.Now()
	processor.stat.record(message.Timestamp, processingTime)

	var batch solana_messages.TokenBlockMessage
	err := proto.Unmarshal(message.Value, &batch)
	if err != nil {
		return err
	}

	transfers := 0
	balanceUpdates := 0
	instructionBalanceUpdates := 0
	instructionTokenSupplyUpdates := 0

	for _, t := range batch.Transactions {
		processor.stat.add(batch.Header.Timestamp, batch.Header.Slot, t.Index, message.Timestamp, processingTime)
		transfers += len(t.Transfers)
		balanceUpdates += len(t.BalanceUpdates)
		for _, instr := range t.InstructionBalanceUpdates {
			instructionBalanceUpdates += len(instr.OwnCurrencyBalanceUpdates)
			instructionTokenSupplyUpdates += len(instr.TokenSupplyUpdates)
		}
	}

	fmt.Printf("slot %d processed with lag %d msec %d txs (%d transfers %d balanceUpdates %d instructionBalanceUpdates %d instructionTokenSupplyUpdates) from partition %d[%s] in worker %d\n",
		batch.Header.Slot, processingTime.Sub(message.Timestamp).Milliseconds(),
		len(batch.Transactions),
		transfers, balanceUpdates, instructionBalanceUpdates, instructionTokenSupplyUpdates,
		message.TopicPartition.Partition, message.TopicPartition.Offset, worker)

	return nil
}
