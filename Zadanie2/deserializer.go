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

type Batch struct {
	NumRows int32
	Columns []Column
}

// type Batch struct {
// 	NumRows     int32        // Number of rows in the batch
// 	NumColumns  int32        // Number of columns
// 	ColumnTypes []byte       // Type of each column (TypeInt or TypeString)
// 	Data        [][]byte     // 2D array where each column is a contiguous byte slice
// }

func (b *Batch) GetIntColumn(colIndex int) ([]int64, error) {
	if colIndex < 0 || colIndex >= len(b.Columns) {
		return nil, fmt.Errorf("column index %d out of bounds", colIndex)
	}
	if b.Columns[colIndex].Type != TypeInt {
		return nil, fmt.Errorf("column %d is not an integer column", colIndex)
	}
	return b.Columns[colIndex].IntData, nil
}

func (b *Batch) GetStringColumn(colIndex int) ([]string, error) {
	if colIndex < 0 || colIndex >= len(b.Columns) {
		return nil, fmt.Errorf("column index %d out of bounds", colIndex)
	}
	if b.Columns[colIndex].Type != TypeString {
		return nil, fmt.Errorf("column %d is not a string column", colIndex)
	}
	return b.Columns[colIndex].StrData, nil
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

	batch := &Batch{
		NumRows: header.BatchSize,
		Columns: make([]Column, header.NumColumns),
	}

	currentOffset := int64(HeaderSize)

	for i, cf := range columnFooters {
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

			batch.Columns[i] = Column{
				Type:    TypeInt,
				IntData: values,
			}

		case TypeString:

			var dataOffset int64 = cf.DataOffset

			arrayOffsetsSize := dataOffset - currentOffset
			compressedOffsets := make([]byte, arrayOffsetsSize)

			// offsetsSize := cf.DataOffset - currentOffset
			// compressedOffsets := make([]byte, offsetsSize)

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

			strings := utils.DecompressStrings(decompressedStrings, offsets)

			batch.Columns[i] = Column{
				Type:    TypeString,
				StrData: strings,
			}

		default:
			return nil, fmt.Errorf("unknown column type: %d", cf.Type)
		}
		currentOffset = nextOffset

	}

	return batch, nil
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
