package main

import (
	"fmt"
	"sync"

	solana_messages "github.com/bitquery/streaming_protobuf/v2/solana/messages"
	"github.com/mr-tron/base58"
)

var (
	wsolMintAddress         = "So11111111111111111111111111111111111111112"
	usdcMintAddress         = "Es9vMFrzaCERGF3yp3r7dcDRcfzVfQdW2M3s9yHgjQcV"
	lastWSOLPrice   float64 = 1.0
	priceLock       sync.RWMutex
)

func bytesToBase58(b []byte) string {
	if b == nil {
		return ""
	}
	return base58.Encode(b)
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

			if baseMint == wsolMintAddress && quoteMint == usdcMintAddress {
				price := quoteAmount / baseAmount
				setWSOLPrice(price)
				fmt.Printf("Updated WSOL price from WSOL/USDC: %.8f (trade %s)\n", price, signature)

			} else if quoteMint == wsolMintAddress && baseMint == usdcMintAddress {
				price := baseAmount / quoteAmount
				setWSOLPrice(price)
				fmt.Printf("Updated WSOL price from USDC/WSOL: %.8f (trade %s)\n", price, signature)

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
