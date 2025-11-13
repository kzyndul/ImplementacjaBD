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
	// columnFooters := make([]ColumnFooter, batch.NumColumns)

	// footerColumnsType := make([]byte, batch.NumColumns)
	footerColumnsOffset := make([]int64, batch.NumColumns+1)
	footerColumnsDelta := make([]int64, batch.NumColumns)

	for i := range batch.NumColumns {

		if batch.ColumnTypes[i] != TypeInt && batch.ColumnTypes[i] != TypeString {
			return fmt.Errorf("unsupported column type %d", batch.ColumnTypes[i])
		}

		row := make([]int64, len(batch.Data[i]))
		copy(row, batch.Data[i])

		compressed, min := utils.CompressIntegers(row)

		// fmt.Print("Writing column ", i, " with min ", min, " and compressed :", compressed, "\n")

		n, err := file.Write(compressed)
		if err != nil {
			return err
		}

		// if batch.ColumnTypes[i] == TypeInt {
		// 	footerColumnsType[i] = TypeInt
		// } else {
		// 	footerColumnsType[i] = TypeString
		// }

		// footerColumnsType[i] = TypeInt
		footerColumnsOffset[i] = currentOffset
		footerColumnsDelta[i] = min

		currentOffset += int64(n)
	}

	footerColumnsOffset[batch.NumColumns] = currentOffset

	compressedStrings, err := utils.CompressLZ4([]byte(batch.String))
	if err != nil {
		return err
	}

	footerOffset := currentOffset
	header.FooterOffset = footerOffset

	if err := s.writeFooter(file, currentOffset, int64(len(compressedStrings)), batch.ColumnTypes, footerColumnsOffset, footerColumnsDelta); err != nil {
		return err
	}

	_, err = file.Write(compressedStrings)
	if err != nil {
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

func (s *Serializer) writeFooter(file *os.File, currentOffset int64, stringSize int64, footerColumnsType []byte, footerColumnsOffset []int64, footerColumnsDelta []int64) error {

	compressedDelta, deltaDelta := utils.CompressIntegers(footerColumnsDelta)

	compressedOffsets, deltaOffset := utils.CompressIntegers(footerColumnsOffset)

	currentOffset += 48 // 6 int64

	currentOffset += int64(len(footerColumnsType))
	offset1 := currentOffset

	currentOffset += int64(len(compressedDelta))
	offset2 := currentOffset

	currentOffset += int64(len(compressedOffsets))

	if err := binary.Write(file, binary.LittleEndian, deltaDelta); err != nil {
		return err
	}

	if err := binary.Write(file, binary.LittleEndian, deltaOffset); err != nil {
		return err
	}

	if err := binary.Write(file, binary.LittleEndian, offset1); err != nil {
		return err
	}

	if err := binary.Write(file, binary.LittleEndian, offset2); err != nil {
		return err
	}

	if err := binary.Write(file, binary.LittleEndian, currentOffset); err != nil {
		return err
	}

	if err := binary.Write(file, binary.LittleEndian, stringSize); err != nil {
		return err
	}

	_, err := file.Write(footerColumnsType)
	if err != nil {
		return err
	}

	_, err = file.Write(compressedDelta)
	if err != nil {
		return err
	}

	_, err = file.Write(compressedOffsets)
	if err != nil {
		return err
	}
	// fmt.Println("Footer read =============================: ")
	// fmt.Println("deltaDelta", deltaDelta)
	// fmt.Println("deltaOffset", deltaOffset)
	// fmt.Println("offset1", offset1)
	// fmt.Println("offset2", offset2)
	// fmt.Println("stringOffset", currentOffset)
	// fmt.Println("stringSize", stringSize)
	// fmt.Println("footerColumnsType", footerColumnsType)
	// fmt.Println("footerColumnsOffset", footerColumnsOffset)
	// fmt.Println("footerColumnsDelta", footerColumnsDelta)

	return nil
}
