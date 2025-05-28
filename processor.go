package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
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
	queue     chan *kafka.Message
	wg        errgroup.Group
	config    ProcessorConfig
	processFn processFn
	stat      *Statistics
	firstSeen map[uint64]time.Time
	mu        sync.Mutex // 🔒 Protects firstSeen map
}

func newProcessor(config *Config) (*Processor, error) {
	processor := &Processor{
		queue:     make(chan *kafka.Message, config.Processor.Buffer),
		config:    config.Processor,
		stat:      newStatistics(),
		firstSeen: make(map[uint64]time.Time),
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
		processFn = processor.transactionsMessageHandler
	}

	processor.processFn = processFn
	return processor, nil
}

func (processor *Processor) enqueue(message *kafka.Message) {
	processor.queue <- message
}

func (processor *Processor) start(ctx context.Context) {
	counter := 0
	for i := 0; i < processor.config.Workers; i++ {
		workerID := i // Capture loop variable
		processor.wg.Go(func() error {
			fmt.Println("Starting worker", workerID)
			for {
				select {
				case <-ctx.Done():
					fmt.Println("Done, exiting processor loop worker", workerID)
					return nil
				case message := <-processor.queue:
					err := processor.processFn(ctx, message, workerID)
					if err != nil {
						fmt.Println("Error processing message:", err)
					}
					counter++
					if counter%100 == 0 {
						processor.stat.report()
					}
				}
			}
		})
	}
}

// Records first time a slot is seen
func (p *Processor) recordFirstSeen(slot uint64, received time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, exists := p.firstSeen[slot]; !exists {
		p.firstSeen[slot] = received
	}
}

func (p *Processor) writeCSVOnExit(filePath string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	f, err := os.Create(filePath)
	if err != nil {
		fmt.Printf("Error creating CSV file: %v\n", err)
		return
	}
	defer f.Close()

	writer := csv.NewWriter(f)
	defer writer.Flush()

	writer.Write([]string{"Slot", "FirstSeenTimestamp (UTC)"})
	for slot, ts := range p.firstSeen {
		writer.Write([]string{
			strconv.FormatUint(slot, 10),
			ts.Format(time.RFC3339Nano),
		})
	}
	fmt.Printf("Wrote %d unique blocks to %s\n", len(p.firstSeen), filePath)
}

// Clean shutdown and final reporting
func (processor *Processor) close() {
	fmt.Println("Shutting down processor...")
	processor.wg.Wait()
	fmt.Println("Processor stopped")
	processor.stat.report()
}
