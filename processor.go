package main

import (
	"context"
	"fmt"
	"time"

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
	stat      *Statistics
}

func newProcessor(config *Config) (*Processor, error) {
	processor := &Processor{
		queue:  make(chan *kafka.Message, config.Processor.Buffer),
		config: config.Processor,
		stat:   newStatistics(),
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
					if time.Now().Unix()%10 == 0 {
						processor.stat.report()
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
	processor.stat.report()
}

func (processor *Processor) unknownMessageHandler(ctx context.Context, message *kafka.Message, worker int) error {
	processingTime := time.Now()
	fmt.Printf("Received message with key %s with lag %d msec from partition %d[%s] in worker %d\n",
		message.Key, processingTime.Sub(message.Timestamp).Milliseconds(),
		message.TopicPartition.Partition, message.TopicPartition.Offset, worker)
	return nil
}

func (processor *Processor) dexTradesMessageHandler(ctx context.Context, message *kafka.Message, worker int) error {
	processingTime := time.Now()
	var batch solana_messages.DexParsedBlockMessage
	err := proto.Unmarshal(message.Value, &batch)
	if err != nil {
		return err
	}

	count := 0
	for _, t := range batch.Transactions {
		count += len(t.Trades)
		processor.stat.add(batch.Header, t.Index, message.Timestamp, processingTime)
	}

	fmt.Printf("slot %d processed with lag %d msec %d txs (%d trades) from partition %d[%s] in worker %d\n",
		batch.Header.Slot, processingTime.Sub(message.Timestamp).Milliseconds(),
		len(batch.Transactions), count, message.TopicPartition.Partition, message.TopicPartition.Offset, worker)

	return nil
}

func (processor *Processor) transactionsMessageHandler(ctx context.Context, message *kafka.Message, worker int) error {
	processingTime := time.Now()
	var batch solana_messages.ParsedIdlBlockMessage
	err := proto.Unmarshal(message.Value, &batch)
	if err != nil {
		return err
	}
	for _, t := range batch.Transactions {
		processor.stat.add(batch.Header, t.Index, message.Timestamp, processingTime)
	}
	fmt.Printf("slot %d processed with lag %d msec %d txs from partition %d[%s] in worker %d\n",
		batch.Header.Slot, processingTime.Sub(message.Timestamp).Milliseconds(),
		len(batch.Transactions), message.TopicPartition.Partition, message.TopicPartition.Offset, worker)
	return nil
}

func (processor *Processor) tokensMessageHandler(ctx context.Context, message *kafka.Message, worker int) error {
	processingTime := time.Now()
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
		processor.stat.add(batch.Header, t.Index, message.Timestamp, processingTime)
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
