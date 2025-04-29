package main

import (
	"encoding/hex"
	"fmt"
	"sync"

	solana_messages "github.com/bitquery/streaming_protobuf/v2/solana/messages"
)

var (
	wsolMintAddress         = "So11111111111111111111111111111111111111112" // base58
	lastWSOLPrice   float64 = 1.0
	priceLock       sync.RWMutex
)

func bytesToBase58(b []byte) string {
	if b == nil {
		return ""
	}
	return hex.EncodeToString(b)
}

func bridgeTradePrices(block *solana_messages.DexParsedBlockMessage) {
	for _, tx := range block.Transactions {
		for _, trade := range tx.Trades {
			if trade.Market == nil || trade.Buy == nil || trade.Sell == nil {
				fmt.Printf("Skipping trade: missing market or sides\n")
				continue
			}

			baseMint := bytesToBase58(trade.Market.BaseCurrency.MintAddress)
			quoteMint := bytesToBase58(trade.Market.QuoteCurrency.MintAddress)

			baseAmount := float64(trade.Sell.Amount)
			quoteAmount := float64(trade.Buy.Amount)

			if baseAmount == 0 || quoteAmount == 0 {
				fmt.Printf("Skipping trade: zero base or quote amount\n")
				continue
			}

			signature := bytesToBase58(tx.Signature)

			if baseMint == wsolMintAddress {
				price := baseAmount / quoteAmount
				setWSOLPrice(price)
				fmt.Printf("Updated WSOL price: %.8f from trade %s\n", price, signature)
			} else if quoteMint == wsolMintAddress {
				price := quoteAmount / baseAmount
				setWSOLPrice(price)
				fmt.Printf("Updated WSOL price: %.8f from trade %s\n", price, signature)
			} else {
				priceLock.RLock()
				wsolPrice := lastWSOLPrice
				priceLock.RUnlock()

				estimated := (quoteAmount / baseAmount) * wsolPrice
				fmt.Printf("Estimated token price from trade %s: %.8f (in WSOL)\n", signature, estimated)
			}
		}
	}
}

func setWSOLPrice(price float64) {
	priceLock.Lock()
	defer priceLock.Unlock()
	lastWSOLPrice = price
}
