package main

import (
	"Zadanie2/utils"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
)

type Serializer struct {
	tablePath  string // Path to the table directory
	numRows    int32  // Number of rows per batch
	numColumns int32  // Number of columns
}

func NewSerializer(tablePath string, numRows int32, numColumns int32) (*Serializer, error) {
	if err := os.MkdirAll(tablePath, 0755); err != nil {
		return nil, err
	}

	return &Serializer{
		tablePath:  tablePath,
		numRows:    numRows,
		numColumns: numColumns,
	}, nil
}

func (s *Serializer) WriteBatch(batchIndex int, batch *Batch) error {

	var batchFile string
	var index int = batchIndex
	if index == 0 {
		index = utils.GenerateRandomNumber()
	}
	batchFile = filepath.Join(s.tablePath, fmt.Sprintf("batch_%d.dat", index))

	file, err := os.Create(batchFile)
	if err != nil {
		return err
	}
	defer file.Close()

	header := FileHeader{
		BatchSize:    batch.BatchSize,
		NumColumns:   batch.NumColumns,
		FooterOffset: 0,
	}
	if err := s.writeHeader(file, header); err != nil {
		return err
	}

	currentOffset := int64(HeaderSize)
	columnFooters := make([]ColumnFooter, batch.NumColumns)

	for i := 0; i < int(batch.NumColumns); i++ {

		colData := make([]interface{}, batch.BatchSize)
		for r := 0; r < int(batch.BatchSize); r++ {
			colData[r] = batch.Data[i][r]
		}

		switch colData[0].(type) {
		case int64:

			ints := make([]int64, len(colData))
			for j, val := range colData {
				ints[j] = val.(int64)
			}

			compressed, min := utils.CompressIntegers(ints)

			n, err := file.Write(compressed)
			if err != nil {
				return err
			}

			columnFooters[i] = ColumnFooter{
				Type:       TypeInt,
				Delta:      min,
				Offset:     currentOffset,
				DataOffset: 0,
			}

			currentOffset += int64(n)

		case string:

			strs := make([]string, len(colData))
			for j, val := range colData {
				strs[j] = val.(string)
			}

			blob, offsets := utils.BuildConcatenatedBlob(strs)
			compressedOffsets, min := utils.CompressIntegers(offsets)

			// write compressed offsets
			n1, err := file.Write(compressedOffsets)
			if err != nil {
				return err
			}
			columnOffset := currentOffset
			currentOffset += int64(n1)

			dataStart := currentOffset

			compressedBlob, err := utils.CompressLZ4(blob)
			if err != nil {
				return err
			}

			n2, err := file.Write(compressedBlob)
			if err != nil {
				return err
			}

			columnFooters[i] = ColumnFooter{
				Type:       TypeString,
				Delta:      min,
				Offset:     columnOffset,
				DataOffset: dataStart,
			}

			currentOffset += int64(n2)

		default:
			return fmt.Errorf("column %d: unsupported type %T", i, colData[0])
		}
	}

	footerOffset := currentOffset
	header.FooterOffset = footerOffset

	if err := s.writeFooter(file, columnFooters); err != nil {
		return err
	}

	if err := s.writeHeader(file, header); err != nil {
		return err
	}

	return nil
}

func (s *Serializer) writeHeader(file *os.File, h FileHeader) error {
	if _, err := file.Seek(0, 0); err != nil {
		return err
	}
	if err := binary.Write(file, binary.LittleEndian, h.BatchSize); err != nil {
		return err
	}
	if err := binary.Write(file, binary.LittleEndian, h.NumColumns); err != nil {
		return err
	}
	if err := binary.Write(file, binary.LittleEndian, h.FooterOffset); err != nil {
		return err
	}
	return nil
}

func (s *Serializer) writeFooter(file *os.File, footers []ColumnFooter) error {
	for _, cf := range footers {
		if err := binary.Write(file, binary.LittleEndian, cf.Type); err != nil {
			return err
		}
		if err := binary.Write(file, binary.LittleEndian, cf.Delta); err != nil {
			return err
		}
		if err := binary.Write(file, binary.LittleEndian, cf.Offset); err != nil {
			return err
		}
		if err := binary.Write(file, binary.LittleEndian, cf.DataOffset); err != nil {
			return err
		}
	}
	return nil
}
