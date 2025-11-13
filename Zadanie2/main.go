package main

import (
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"unicode"
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

type BatchFooter struct {
	DeltaDelta    int64
	DeltaOffset   int64
	Offset1       int64   // Where ColumnsType ends
	Offset2       int64   // Where ColumnsOffset ends
	StringOffset  int64   // Where ColumnsDelta ends
	StringSize    int64   // Size of the compressed string
	ColumnsType   []byte  // 0 = int, 1 = string
	ColumnsDelta  []int64 // min value for delta compression
	ColumnsOffset []int64 // Offset of the column
}

type Batch struct {
	BatchSize   int32
	NumColumns  int32
	ColumnTypes []byte
	Data        [][]int64
	String      string
}

// func printBatch(batch *Batch) {
// 	println("Batch Size:", batch.BatchSize, "Num Columns:", batch.NumColumns)
// 	for i, _ := range batch.Data {
// 		switch batch.Data[i][0].(type) {
// 		case int64:
// 			println("Column", i, "Type: int64 Values:", fmt.Sprint(batch.Data[i]))
// 		case string:
// 			println("Column", i, "Type: string Values:", fmt.Sprint(batch.Data[i]))
// 		default:
// 			println("Column", i, "Type: unknown")
// 		}
// 	}
// }

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

	// sort.Ints(batchIndices)

	return batchIndices, nil
}

func LoadAllBatches(tablePath string) ([]*Batch, error) {
	indices, err := getBatchFiles(tablePath)
	if err != nil {
		return nil, err
	}
	if len(indices) == 0 {
		return nil, fmt.Errorf("no batch_*.dat files found in %q", tablePath)
	}

	d, err := NewBatchDeserializer(tablePath)
	if err != nil {
		return nil, err
	}

	batches := make([]*Batch, 0, len(indices))
	for _, idx := range indices {
		b, err := d.ReadBatch(idx)
		if err != nil {
			return nil, fmt.Errorf("failed to read batch %d: %w", idx, err)
		}
		batches = append(batches, b)
	}
	return batches, nil
}

func ComputeMeansAndLetterHistograms(tablePath string) (map[int]float64, map[int]map[rune]int, error) {
	batches, err := LoadAllBatches(tablePath)
	if err != nil {
		return nil, nil, err
	}
	if len(batches) == 0 {
		return map[int]float64{}, map[int]map[rune]int{}, nil
	}

	first := batches[0]
	numCols := int(first.NumColumns)
	colTypes := first.ColumnTypes

	sums := make([]float64, numCols)
	counts := make([]int64, numCols)
	hists := make(map[int]map[rune]int, numCols)

	for _, b := range batches {
		for i := 0; i < numCols; i++ {
			switch colTypes[i] {
			case TypeInt:
				col := b.Data[i]
				for _, v := range col {
					sums[i] += float64(v)
				}
				counts[i] += int64(len(col))
			case TypeString:
				if hists[i] == nil {
					hists[i] = make(map[rune]int)
				}

				start := int(b.Data[i][0])
				end := int(b.Data[i][len(b.Data[i])-1])

				for _, r := range b.String[start:end] {
					if unicode.IsLetter(r) {
						r = unicode.ToLower(r)
						hists[i][r]++
					}
				}
			default:
			}
		}
	}

	means := make(map[int]float64, numCols)
	for i := 0; i < numCols; i++ {
		if colTypes[i] == TypeInt && counts[i] > 0 {
			means[i] = sums[i] / float64(counts[i])
		}
	}

	return means, hists, nil
}

func main() {
	// RunAllTests()

	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "usage: %s <table_folder>\n", os.Args[0])
		os.Exit(2)
	}
	tablePath := os.Args[1]

	means, hists, err := ComputeMeansAndLetterHistograms(tablePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Means:")
	if len(means) == 0 {
		fmt.Println("  (none)")
	} else {
		var keys []int
		for k := range means {
			keys = append(keys, k)
		}
		sort.Ints(keys)
		for _, k := range keys {
			fmt.Printf("  column %d: %f\n", k, means[k])
		}
	}

	fmt.Println("Histograms:")
	if len(hists) == 0 {
		fmt.Println("  (none)")
	} else {
		var cols []int
		for c := range hists {
			cols = append(cols, c)
		}
		sort.Ints(cols)
		for _, c := range cols {
			fmt.Printf("  column %d:\n", c)
			var runes []rune
			for r := range hists[c] {
				runes = append(runes, r)
			}
			sort.Slice(runes, func(i, j int) bool { return runes[i] < runes[j] })
			for _, r := range runes {
				fmt.Printf("    %q: %d\n", r, hists[c][r])
			}
		}
	}
}
