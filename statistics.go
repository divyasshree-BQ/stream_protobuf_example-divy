package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/bitquery/streaming_protobuf/v2/solana/messages"
)

type Statistics struct {
	duplicates      map[string]int
	blockTimestamps map[uint64]int64
	txTimestamps    map[uint64][]int64

	processorLagSum   int64
	processorLagMax   int64
	processorLagCount int64

	mu sync.Mutex
}

func newStatistics() *Statistics {
	return &Statistics{
		duplicates:      make(map[string]int),
		blockTimestamps: make(map[uint64]int64),
		txTimestamps:    make(map[uint64][]int64),
	}
}

func (s *Statistics) add(header *solana_messages.BlockHeader, txIndex uint32, timestamp time.Time, processingTime time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := fmt.Sprintf("%d-%d", header.Slot, txIndex)
	s.duplicates[key] += 1

	if header.Timestamp > 0 {
		s.blockTimestamps[header.Slot] = header.Timestamp * 1000
	}

	s.txTimestamps[header.Slot] = append(s.txTimestamps[header.Slot], timestamp.UnixMilli())

}

func (s *Statistics) report() {
	s.mu.Lock()
	defer s.mu.Unlock()

	duplicatedTxs := 0
	totalTxs := 0
	for _, v := range s.duplicates {
		totalTxs += 1
		duplicatedTxs += v - 1
	}

	percent := float64(duplicatedTxs) * 100 / float64(totalTxs)

	fmt.Printf("-----------------------------------------------------------\n")
	fmt.Printf("total txs processed: %d duplicate transactions: %d (%.1f %%) \n", totalTxs, duplicatedTxs, percent)

	count := int64(0)
	sumLag := int64(0)
	maxLag := int64(0)
	for slot, txts := range s.txTimestamps {
		slotTs, hasTx := s.blockTimestamps[slot]
		if !hasTx {
			continue
		}
		for _, ts := range txts {
			lag := ts - slotTs
			if lag > maxLag {
				maxLag = lag
			}
			sumLag += lag
			count++
		}
	}

	if count > 0 {
		fmt.Printf("Average lag to block time %d msec, max lag %d msec\n", sumLag/count, maxLag)
	}

	if s.processorLagCount > 0 {
		fmt.Printf("Average lag to message time %d msec, max lag %d msec\n", s.processorLagSum/s.processorLagCount, s.processorLagMax)
	}
	fmt.Printf("-----------------------------------------------------------\n")
}

func (s *Statistics) record(timestamp time.Time, processingTime time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	lag := processingTime.Sub(timestamp).Milliseconds()

	s.processorLagCount++
	s.processorLagSum += lag

	if lag > s.processorLagMax {
		s.processorLagMax = lag
	}

}
