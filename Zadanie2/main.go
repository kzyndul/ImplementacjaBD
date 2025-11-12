package main

import (
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
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
	println("Batch Size:", batch.BatchSize, "Num Columns:", batch.NumColumns)
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

func getBatchFiles(tablePath string) ([]int, error) {
	files, err := os.ReadDir(tablePath)
	if err != nil {
		return nil, err
	}

	var batchIndices []int
	batchRegex := regexp.MustCompile(`^batch_(\d+)\.dat$`)

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		matches := batchRegex.FindStringSubmatch(file.Name())
		if len(matches) == 2 {
			index, err := strconv.Atoi(matches[1])
			if err != nil {
				continue
			}
			batchIndices = append(batchIndices, index)
		}
	}

	sort.Ints(batchIndices)

	return batchIndices, nil
}

func main() {
	RunAllTests()

	// Test data creation (uncomment to create test data)
	// createTestDataSimple()
	createTestDataRandom()
	// Read table from given path
	tablePath := "./test_data"
	readTableAndComputeStatistics(tablePath)
	defer os.RemoveAll(tablePath)
}

// createTestData creates sample data for testing
func createTestDataSimple() {
	examplePath := "./test_data"

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
}

func createTestDataRandom() {
	examplePath := "./test_data"

	numberOfRows := 100000
	batchSize := int32(8192)

	// Generate random data
	intCol1 := make([]int64, numberOfRows)
	intCol2 := make([]int64, numberOfRows)
	strCol1 := make([]string, numberOfRows)
	strCol2 := make([]string, numberOfRows)

	for i := 0; i < numberOfRows; i++ {
		intCol1[i] = rand.Int63n(10000000)
		intCol2[i] = rand.Int63n(10000000)
		strCol1[i] = fmt.Sprintf("str_%d", i)
		strCol2[i] = fmt.Sprintf("str_%d", i)
	}

	sum1 := int64(0)
	sum2 := int64(0)
	charFreqs1 := make(map[rune]int)
	charFreqs2 := make(map[rune]int)
	for i := 0; i < numberOfRows; i++ {
		sum1 += intCol1[i]
		sum2 += intCol2[i]
		for _, ch := range strCol1[i] {
			charFreqs1[ch]++
		}
		for _, ch := range strCol2[i] {
			charFreqs2[ch]++
		}
	}

	fmt.Printf("Mean intCol1: %.2f\n", float64(sum1)/float64(numberOfRows))
	fmt.Printf("Mean intCol2: %.2f\n", float64(sum2)/float64(numberOfRows))
	println("Character frequencies in strCol1:")
	for ch, count := range charFreqs1 {
		println(string(ch), count)
	}
	println("Character frequencies in strCol2:")
	for ch, count := range charFreqs2 {
		println(string(ch), count)
	}

	batches, err := NewBatch(batchSize, intCol1, intCol2, strCol1, strCol2)
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
}

func readTableAndComputeStatistics(tablePath string) {
	batchFiles, err := getBatchFiles(tablePath)
	if err != nil {
		fmt.Printf("Error finding batch files: %v\n", err)
		return
	}

	if len(batchFiles) == 0 {
		fmt.Printf("No batch files found in %s\n", tablePath)
		return
	}

	fmt.Printf("Found %d batch files in %s\n", len(batchFiles), tablePath)

	var totalRows int32
	var numColumns int32
	var columnTypes []byte
	var intSums []int64          // Sum for each int column
	var charFreqs []map[rune]int // Character frequency for each string column

	for i, batchIndex := range batchFiles {
		des, err := NewBatchDeserializer(tablePath)
		if err != nil {
			fmt.Printf("Error creating deserializer: %v\n", err)
			continue
		}

		batch, err := des.ReadBatch(batchIndex)
		if err != nil {
			fmt.Printf("Error reading batch %d: %v\n", batchIndex, err)
			continue
		}

		fmt.Printf("\n=== Processing Batch %d (file: batch_%d.dat) ===\n", i+1, batchIndex)
		// printBatch(batch)

		if i == 0 {
			numColumns = batch.NumColumns
			columnTypes = make([]byte, numColumns)
			copy(columnTypes, batch.ColumnTypes)

			intSums = make([]int64, numColumns)
			charFreqs = make([]map[rune]int, numColumns)

			for j := int32(0); j < numColumns; j++ {
				if batch.ColumnTypes[j] == TypeString {
					charFreqs[j] = make(map[rune]int)
				}
			}
		}

		totalRows += batch.BatchSize

		for colIdx := int32(0); colIdx < batch.NumColumns; colIdx++ {
			if batch.ColumnTypes[colIdx] == TypeInt {
				for rowIdx := int32(0); rowIdx < batch.BatchSize; rowIdx++ {
					value := batch.Data[colIdx][rowIdx].(int64)
					intSums[colIdx] += value
				}
			} else if batch.ColumnTypes[colIdx] == TypeString {
				for rowIdx := int32(0); rowIdx < batch.BatchSize; rowIdx++ {
					value := batch.Data[colIdx][rowIdx].(string)
					for _, ch := range value {
						charFreqs[colIdx][ch]++
					}
				}
			}
		}
	}

	fmt.Printf("\n" + strings.Repeat("=", 50))
	fmt.Printf("\n=== OVERALL TABLE STATISTICS ===\n")
	fmt.Printf("Total rows across all batches: %d\n", totalRows)
	fmt.Printf("Number of columns: %d\n", numColumns)
	fmt.Printf("Number of batches: %d\n", len(batchFiles))

	for colIdx := int32(0); colIdx < numColumns; colIdx++ {
		if columnTypes[colIdx] == TypeInt {
			if totalRows > 0 {
				mean := float64(intSums[colIdx]) / float64(totalRows)
				fmt.Printf("Column %d (int) - Total sum: %d, total rows: %d, Mean: %.2f\n",
					colIdx, intSums[colIdx], totalRows, mean)
			}
		} else if columnTypes[colIdx] == TypeString {
			fmt.Printf("Column %d (string) - Overall character histogram:\n", colIdx)
			for ch, count := range charFreqs[colIdx] {
				fmt.Printf("  %c: %d\n", ch, count)
			}
		}
	}
}
