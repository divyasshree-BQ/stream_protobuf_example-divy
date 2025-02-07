package main

import (
	"fmt"
	"sync"
)

type Statistics struct {
	duplicates map[string]int
	mu         sync.Mutex
}

func newStatistics() *Statistics {
	return &Statistics{
		duplicates: make(map[string]int),
	}
}

func (s *Statistics) add(slot uint64, txIndex uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := fmt.Sprintf("%d-%d", slot, txIndex)
	s.duplicates[key] += 1

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

	fmt.Printf("total txs processed: %d duplicate transactions: %d (%.1f %%) \n", totalTxs, duplicatedTxs, percent)

}
