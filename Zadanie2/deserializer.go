package main

import (
	"Zadanie2/utils"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
)

type Deserializer struct {
	tablePath string
}

type Column struct {
	Type    byte     // TypeInt or TypeString
	IntData []int64  // Used if Type == TypeInt
	StrData []string // Used if Type == TypeString
}

// type Batch struct {
// 	NumRows int32
// 	Columns []Column
// }

type Batch struct {
	BatchSize   int32
	NumColumns  int32
	ColumnTypes []byte
	Data        [][]interface{} // rows × columns
}

// NewBatch creates batches from columns, splitting them into chunks of batchSize.
// Each argument should be either []int64 or []string. All columns must have the same length.
// Returns an array of batches where each batch has batchSize rows (except the last which may be smaller).
func NewBatch(batchSize int32, cols ...interface{}) ([]*Batch, error) {
	if len(cols) == 0 {
		return nil, fmt.Errorf("no columns provided")
	}
	if batchSize <= 0 {
		return nil, fmt.Errorf("batch size must be positive")
	}

	// Determine total number of rows from first column
	var totalRows int32
	switch v := cols[0].(type) {
	case []int64:
		totalRows = int32(len(v))
	case []string:
		totalRows = int32(len(v))
	default:
		return nil, fmt.Errorf("unsupported column type %T", v)
	}

	// Validate all columns same length and determine types
	columnTypes := make([]byte, len(cols))
	for i, c := range cols {
		var colLen int32
		switch col := c.(type) {
		case []int64:
			colLen = int32(len(col))
			columnTypes[i] = TypeInt
		case []string:
			colLen = int32(len(col))
			columnTypes[i] = TypeString
		default:
			return nil, fmt.Errorf("unsupported column type at %d: %T", i, c)
		}
		if colLen != totalRows {
			return nil, fmt.Errorf("column %d length mismatch: %d != %d", i, colLen, totalRows)
		}
	}

	// Calculate number of batches needed
	numBatches := (totalRows + batchSize - 1) / batchSize
	batches := make([]*Batch, 0, numBatches)

	// Create batches
	for batchIdx := int32(0); batchIdx < numBatches; batchIdx++ {
		startRow := batchIdx * batchSize
		endRow := startRow + batchSize
		if endRow > totalRows {
			endRow = totalRows
		}
		currentBatchSize := endRow - startRow

		// Allocate data for this batch
		data := make([][]interface{}, len(cols))
		for colIdx, c := range cols {
			data[colIdx] = make([]interface{}, currentBatchSize)
			switch col := c.(type) {
			case []int64:
				for r := int32(0); r < currentBatchSize; r++ {
					data[colIdx][r] = col[startRow+r]
				}
			case []string:
				for r := int32(0); r < currentBatchSize; r++ {
					data[colIdx][r] = col[startRow+r]
				}
			}
		}

		batches = append(batches, &Batch{
			BatchSize:   int32(currentBatchSize),
			NumColumns:  int32(len(cols)),
			ColumnTypes: columnTypes,
			Data:        data,
		})
	}

	return batches, nil
}

func NewBatchDeserializer(tablePath string) (*Deserializer, error) {
	return &Deserializer{
		tablePath: tablePath,
	}, nil
}

func (d *Deserializer) ReadBatch(batchIndex int) (*Batch, error) {
	batchFile := filepath.Join(d.tablePath, fmt.Sprintf("batch_%d.dat", batchIndex))
	file, err := os.Open(batchFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	header, err := d.readHeader(file)
	if err != nil {
		return nil, err
	}

	columnFooters, err := d.readFooter(file, header)
	if err != nil {
		return nil, err
	}

	numRows := header.BatchSize
	numCols := header.NumColumns
	data := make([][]interface{}, numCols)
	for r := int32(0); r < numCols; r++ {
		data[r] = make([]interface{}, numRows)
	}

	columnTypes := make([]byte, numCols)

	currentOffset := int64(HeaderSize)

	for i, cf := range columnFooters {
		columnTypes[i] = cf.Type

		var nextOffset int64
		if i == len(columnFooters)-1 {
			nextOffset = header.FooterOffset
		} else {
			nextOffset = columnFooters[i+1].Offset
		}

		switch cf.Type {
		case TypeInt:
			compressedSize := nextOffset - currentOffset
			compressedData := make([]byte, compressedSize)

			if _, err := file.ReadAt(compressedData, currentOffset); err != nil {
				return nil, err
			}

			values := utils.DecompressIntegers(compressedData, cf.Delta)
			for r := range values {
				data[i][r] = values[r]
			}

		case TypeString:
			offsetsSize := cf.DataOffset - currentOffset
			compressedOffsets := make([]byte, offsetsSize)
			if _, err := file.ReadAt(compressedOffsets, currentOffset); err != nil {
				return nil, err
			}
			offsets := utils.DecompressIntegers(compressedOffsets, cf.Delta)

			compressedStringsSize := nextOffset - cf.DataOffset
			compressedStrings := make([]byte, compressedStringsSize)
			if _, err := file.ReadAt(compressedStrings, cf.DataOffset); err != nil {
				return nil, err
			}

			decompressedStrings, err := utils.DecompressLZ4(compressedStrings)
			if err != nil {
				return nil, err
			}

			strs := utils.DecompressStrings(decompressedStrings, offsets)

			for r := range strs {
				data[i][r] = strs[r]
			}

		default:
			return nil, fmt.Errorf("unknown column type %d", cf.Type)
		}

		currentOffset = nextOffset
	}

	return &Batch{
		BatchSize:   numRows,
		NumColumns:  numCols,
		ColumnTypes: columnTypes,
		Data:        data,
	}, nil
}

func (d *Deserializer) readHeader(file *os.File) (*FileHeader, error) {
	header := &FileHeader{}

	if _, err := file.Seek(0, 0); err != nil {
		return nil, err
	}

	if err := binary.Read(file, binary.LittleEndian, &header.BatchSize); err != nil {
		return nil, err
	}
	if err := binary.Read(file, binary.LittleEndian, &header.NumColumns); err != nil {
		return nil, err
	}
	if err := binary.Read(file, binary.LittleEndian, &header.FooterOffset); err != nil {
		return nil, err
	}

	return header, nil
}

func (d *Deserializer) readFooter(file *os.File, header *FileHeader) ([]ColumnFooter, error) {
	if _, err := file.Seek(header.FooterOffset, 0); err != nil {
		return nil, err
	}

	footers := make([]ColumnFooter, header.NumColumns)
	for i := 0; i < int(header.NumColumns); i++ {
		if err := binary.Read(file, binary.LittleEndian, &footers[i].Type); err != nil {
			return nil, err
		}
		if err := binary.Read(file, binary.LittleEndian, &footers[i].Delta); err != nil {
			return nil, err
		}
		if err := binary.Read(file, binary.LittleEndian, &footers[i].Offset); err != nil {
			return nil, err
		}
		if err := binary.Read(file, binary.LittleEndian, &footers[i].DataOffset); err != nil {
			return nil, err
		}
	}

	return footers, nil
}
