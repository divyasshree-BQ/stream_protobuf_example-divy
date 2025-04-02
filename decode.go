package main

import (
	"github.com/mr-tron/base58"
)

// TransactionData represents the incoming transaction payload.
type TransactionData struct {
	Address   []byte `json:"address"`
	Signature []byte `json:"signature"`
	Mint      []byte `json:"mint"`
	Data      []byte `json:"data"`
}

// EncodedTransaction represents the encoded transaction response.
type EncodedTransaction struct {
	Address   string `json:"address"`
	Signature string `json:"signature"`
	Mint      string `json:"mint"`
	Data      string `json:"data"`
}

// encodeTransactionData encodes binary fields to Base58.
func encodeTransactionData(tx TransactionData) EncodedTransaction {
	return EncodedTransaction{
		Address:   base58.Encode(tx.Address),
		Signature: base58.Encode(tx.Signature),
		Mint:      base58.Encode(tx.Mint),
		Data:      base58.Encode(tx.Data),
	}
}
