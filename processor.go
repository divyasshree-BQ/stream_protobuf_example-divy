package main

import (
	"context"
	"fmt"

	"github.com/bitquery/streaming_protobuf/v2/solana/messages"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/golang/protobuf/proto"
	"golang.org/x/sync/errgroup"
)

type ProcessorConfig struct {
	Buffer  int
	Workers int
}

type Listener interface {
	enqueue(message *kafka.Message)
}

type processFn func(context.Context, *kafka.Message, int) error

type Processor struct {
	queue     chan (*kafka.Message)
	wg        errgroup.Group
	config    ProcessorConfig
	processFn processFn
}

func newProcessor(config *Config) (*Processor, error) {
	processor := &Processor{
		queue:  make(chan *kafka.Message, config.Processor.Buffer),
		config: config.Processor,
	}

	var processFn processFn
	switch config.Consumer.Topic {
	case "solana.dextrades.proto":
		processFn = processor.dexTradesMessageHandler
	case "solana.transactions.proto":
		processFn = processor.transactionsMessageHandler
	case "solana.tokens.proto":
		processFn = processor.tokensMessageHandler
	default:
		processFn = processor.unknownMessageHandler
	}

	processor.processFn = processFn
	return processor, nil
}

func (processor *Processor) enqueue(message *kafka.Message) {
	processor.queue <- message
}

func (processor *Processor) start(ctx context.Context) {
	for i := 0; i < processor.config.Workers; i++ {
		processor.wg.Go(func() error {
			i := i
			fmt.Println("Starting worker ", i)
			for {
				select {
				case <-ctx.Done():
					fmt.Println("Done, exiting processor loop worker ", i)
					return nil
				case message := <-processor.queue:
					err := processor.processFn(ctx, message, i)
					if err != nil {
						fmt.Println("Error processing message", err)
					}
				}
			}
			return nil
		})
	}
}

func (processor *Processor) close() {
	fmt.Println("Shutting down processor...")
	processor.wg.Wait()
	fmt.Println("Processor stopped")
}

func (processor *Processor) unknownMessageHandler(ctx context.Context, message *kafka.Message, worker int) error {
	fmt.Printf("Received message with key %s from partition %d[%s] in worker %d\n",
		message.Key, message.TopicPartition.Partition, message.TopicPartition.Offset, worker)
	return nil
}

func (processor *Processor) dexTradesMessageHandler(ctx context.Context, message *kafka.Message, worker int) error {
	var batch solana_messages.DexParsedBlockMessage
	err := proto.Unmarshal(message.Value, &batch)
	if err != nil {
		return err
	}

	count := 0
	for _, t := range batch.Transactions {
		count += len(t.Trades)
	}

	fmt.Printf("slot %d processed %d txs (%d trades) from partition %d[%s] in worker %d\n",
		batch.Header.Slot, len(batch.Transactions), count, message.TopicPartition.Partition, message.TopicPartition.Offset, worker)

	return nil
}

func (processor *Processor) transactionsMessageHandler(ctx context.Context, message *kafka.Message, worker int) error {
	var batch solana_messages.ParsedIdlBlockMessage
	err := proto.Unmarshal(message.Value, &batch)
	if err != nil {
		return err
	}
	fmt.Printf("slot %d processed %d txs from partition %d[%s] in worker %d\n",
		batch.Header.Slot, len(batch.Transactions), message.TopicPartition.Partition, message.TopicPartition.Offset, worker)
	return nil
}

func (processor *Processor) tokensMessageHandler(ctx context.Context, message *kafka.Message, worker int) error {
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
		transfers += len(t.Transfers)
		balanceUpdates += len(t.BalanceUpdates)
		for _, instr := range t.InstructionBalanceUpdates {
			instructionBalanceUpdates += len(instr.OwnCurrencyBalanceUpdates)
			instructionTokenSupplyUpdates += len(instr.TokenSupplyUpdates)
		}
	}

	fmt.Printf("slot %d processed %d txs (%d transfers %d balanceUpdates %d instructionBalanceUpdates %d instructionTokenSupplyUpdates) from partition %d[%s] in worker %d\n",
		batch.Header.Slot, len(batch.Transactions),
		transfers, balanceUpdates, instructionBalanceUpdates, instructionTokenSupplyUpdates,
		message.TopicPartition.Partition, message.TopicPartition.Offset, worker)

	return nil
}
