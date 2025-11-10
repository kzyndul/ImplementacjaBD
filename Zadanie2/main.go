package main

import (
	"Zadanie2/utils"
	"fmt"
)

const (
	TypeInt    byte = 0
	TypeString byte = 1
	HeaderSize      = 16 // 4 + 4 + 8

	BatchSize = 8192
)

type FileHeader struct {
	BatchSize    int32 // Number of rows per batch
	NumColumns   int32 // Number of columns
	FooterOffset int64 // Offset where footer starts
}

type ColumnFooter struct {
	Type       byte  // 0 = int, 1 = string
	Delta      int64 // min value for delta compression
	Offset     int64 // Offset of the column
	DataOffset int64 // Offset where data starts (0 for int, offset for string data)
}

func printBatch(batch *Batch) {
	for i, _ := range batch.Data {
		switch batch.Data[i][0].(type) {
		case int64:
			println("Column", i, "Type: int64 Values:", fmt.Sprint(batch.Data[i]))
		case string:
			println("Column", i, "Type: string Values:", fmt.Sprint(batch.Data[i]))
		default:
			println("Column", i, "Type: unknown")
		}
	}
}

func main() {
	// Existing internal tests
	utils.TestVariableLengthEncoding()
	RunAllTests()

	// Example: create 3 columns and serialize/deserialize
	examplePath := "./example_table"

	intCol1 := []int64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
	intCol2 := []int64{3, 6, 9, 12, 15, 18, 21, 24, 27, 30}
	strCol := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta", "iota", "kappa"}

	batches, err := NewBatch(6, intCol1, intCol2, strCol)
	if err != nil {
		panic(err)
	}

	for idx, batch := range batches {
		serializer, err := NewSerializer(examplePath, batch.BatchSize, batch.NumColumns)
		if err != nil {
			panic(err)
		}
		if err := serializer.WriteBatch(idx+1, batch); err != nil {
			panic(err)
		}
	}

	for idx := range batches {
		des, err := NewBatchDeserializer(examplePath)
		if err != nil {
			panic(err)
		}
		readBatch, err := des.ReadBatch(idx + 1)
		if err != nil {
			panic(err)
		}

		printBatch(readBatch)

		fmt.Printf("\n=== Batch %d ===\n", idx+1)
		// Compute means for int columns and histogram for string column
		for i := 0; i < int(readBatch.NumColumns); i++ {
			if readBatch.ColumnTypes[i] == TypeInt {
				var sum int64
				for r := 0; r < int(readBatch.BatchSize); r++ {
					sum += readBatch.Data[i][r].(int64)
				}
				mean := float64(sum) / float64(readBatch.BatchSize)
				fmt.Printf("Column %d (int) mean: %.2f\n", i, mean)
			} else if readBatch.ColumnTypes[i] == TypeString {
				freq := map[rune]int{}
				for r := 0; r < int(readBatch.BatchSize); r++ {
					val := readBatch.Data[i][r].(string)
					for _, ch := range val {
						freq[ch]++
					}
				}
				fmt.Printf("Column %d (string) letter histogram:\n", i)
				for k, v := range freq {
					fmt.Printf("  %c: %d\n", k, v)
				}
			}
		}
	}
}
