package main

import (
	"context"
	"fmt"
	"time"

	evm_messages "github.com/bitquery/streaming_protobuf/v2/evm/messages"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/golang/protobuf/proto"
)

func (processor *Processor) tokensMessageHandlerBSC(ctx context.Context, message *kafka.Message, worker int) error {
	processingTime := time.Now()
	processor.stat.record(message.Timestamp, processingTime)

	var batch evm_messages.TokenBlockMessage
	err := proto.Unmarshal(message.Value, &batch)
	if err != nil {
		return fmt.Errorf("failed to unmarshal TokenBlockMessage: %w", err)
	}

	transfers := len(batch.Transfers)

	fmt.Printf("block %d processed with lag %d ms (%d token transfers) from partition %d[%s] in worker %d\n",
		batch.Header.Number,
		processingTime.Sub(message.Timestamp).Milliseconds(),
		transfers,
		message.TopicPartition.Partition,
		message.TopicPartition.Offset,
		worker,
	)
	fmt.Printf("Chain ID: %x\n", batch.Chain.ChainId)

	//Set it to false if you don't want to print the transfers
	verbose := false
	if verbose {
		for i, transfer := range batch.Transfers {
			fmt.Printf("  [%d] %s -> %s | amount: %x | token: %s (%s)\n",
				i,
				fmt.Sprintf("%x", transfer.Sender),
				fmt.Sprintf("%x", transfer.Receiver),
				transfer.Amount,
				transfer.Currency.Name,
				transfer.Currency.Symbol,
			)
		}
	}

	return nil
}
func printArgumentValue(val *evm_messages.ArgumentValue, indent string) {

	switch v := val.Value.(type) {
	// case *evm_messages.ArgumentValue_string:
	// 	fmt.Printf("%s\"%s\"\n", indent, v.String)
	case *evm_messages.ArgumentValue_Bytes:
		fmt.Printf("%s%x\n", indent, v.Bytes)
	case *evm_messages.ArgumentValue_UInt:
		fmt.Printf("%s%d (uint)\n", indent, v.UInt)
	case *evm_messages.ArgumentValue_Int:
		fmt.Printf("%s%d (int)\n", indent, v.Int)
	case *evm_messages.ArgumentValue_Bool:
		fmt.Printf("%s%t\n", indent, v.Bool)
	case *evm_messages.ArgumentValue_Array:
		fmt.Printf("%sArray:\n", indent)
		for i, elem := range v.Array.Elements {
			fmt.Printf("%s  [%d]: ", indent, i)
			printArgumentValue(elem, indent+"    ")
		}
	case *evm_messages.ArgumentValue_Tuple:
		fmt.Printf("%sTuple (%s):\n", indent, v.Tuple.Name)
		for i, elem := range v.Tuple.Elements {
			fmt.Printf("%s  [%d]: ", indent, i)
			printArgumentValue(elem, indent+"    ")
		}
	default:
		fmt.Printf("%s<unknown or unset>\n", indent)
	}
}

func (processor *Processor) transactionsMessageHandlerBSC(ctx context.Context, message *kafka.Message, worker int) error {
	processingTime := time.Now()
	processor.stat.record(message.Timestamp, processingTime)

	var batch evm_messages.ParsedAbiBlockMessage
	err := proto.Unmarshal(message.Value, &batch)
	if err != nil {
		return fmt.Errorf("failed to unmarshal ParsedAbiBlockMessage: %w", err)
	}

	txCount := len(batch.Transactions)

	for _, tx := range batch.Transactions {
		index := tx.TransactionHeader.Index

		for callIdx, call := range tx.Calls {
			method := call.Header.Signature.Name
			success := call.Header.Success

			fmt.Printf("  tx[%d] call[%d]: %x -> %x | sig: %s | success: %t\n",
				index,
				callIdx,
				call.Header.From,
				call.Header.To,
				method,
				success,
			)

			// Print all arguments
			for _, arg := range call.Arguments {
				// check for nil before getting the actual value
				if arg == nil || arg.Value == nil {
					fmt.Printf("    Arg: <nil or unset>\n")
					continue
				}
				fmt.Printf("    Arg: name=%s, value=", arg.Name)
				printArgumentValue(arg.Value, "    ")
			}
		}
	}

	fmt.Printf("block %d processed with lag %d ms (%d transactions) from partition %d[%s] in worker %d\n",
		batch.Header.Number,
		processingTime.Sub(message.Timestamp).Milliseconds(),
		txCount,
		message.TopicPartition.Partition,
		message.TopicPartition.Offset,
		worker,
	)

	return nil
}
