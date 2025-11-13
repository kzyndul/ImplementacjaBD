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

	batchFooters, err := d.readFooter(file, header)
	if err != nil {
		return nil, err
	}

	data := make([][]int64, header.NumColumns)
	for r := range header.NumColumns {
		data[r] = make([]int64, header.BatchSize)
	}

	currentOffset := int64(HeaderSize)

	for i := range header.NumColumns {

		endOfColumn := batchFooters.ColumnsOffset[i+1]
		compressedSize := endOfColumn - currentOffset
		compressedData := make([]byte, compressedSize)

		if _, err := file.ReadAt(compressedData, currentOffset); err != nil {
			return nil, err
		}

		values := utils.DecompressIntegers(compressedData, batchFooters.ColumnsDelta[i])

		data[i] = values
		currentOffset = endOfColumn
	}

	compressedStrings := make([]byte, batchFooters.StringSize)
	if _, err := file.ReadAt(compressedStrings, batchFooters.StringOffset); err != nil {
		return nil, err
	}

	stringData, err := utils.DecompressLZ4(compressedStrings)
	if err != nil {
		return nil, err
	}

	return &Batch{
		BatchSize:   header.BatchSize,
		NumColumns:  header.NumColumns,
		ColumnTypes: batchFooters.ColumnsType,
		Data:        data,
		String:      string(stringData),
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

func (d *Deserializer) readFooter(file *os.File, header *FileHeader) (*BatchFooter, error) {
	if _, err := file.Seek(header.FooterOffset, 0); err != nil {
		return nil, err
	}

	deltaDelta := int64(0)
	deltaOffset := int64(0)
	offset1 := int64(0)
	offset2 := int64(0)
	stringOffset := int64(0)
	stringSize := int64(0)

	if err := binary.Read(file, binary.LittleEndian, &deltaDelta); err != nil {
		return nil, err
	}
	if err := binary.Read(file, binary.LittleEndian, &deltaOffset); err != nil {
		return nil, err
	}
	if err := binary.Read(file, binary.LittleEndian, &offset1); err != nil {
		return nil, err
	}
	if err := binary.Read(file, binary.LittleEndian, &offset2); err != nil {
		return nil, err
	}
	if err := binary.Read(file, binary.LittleEndian, &stringOffset); err != nil {
		return nil, err
	}
	if err := binary.Read(file, binary.LittleEndian, &stringSize); err != nil {
		return nil, err
	}

	compressedTypes := make([]byte, header.NumColumns)
	if _, err := file.Read(compressedTypes); err != nil {
		return nil, err
	}

	compressedDeltas := make([]byte, offset2-offset1)
	if _, err := file.Read(compressedDeltas); err != nil {
		return nil, err
	}
	deltas := utils.DecompressIntegers(compressedDeltas, deltaDelta)

	compressedOffsets := make([]byte, stringOffset-offset2)
	if _, err := file.Read(compressedOffsets); err != nil {
		return nil, err
	}
	offsets := utils.DecompressIntegers(compressedOffsets, deltaOffset)

	// fmt.Println("Footer read=====================: ")
	// fmt.Println(" deltaDelta:", deltaDelta)
	// fmt.Println(" deltaOffset:", deltaOffset)
	// fmt.Println(" offset1:", offset1)
	// fmt.Println(" offset2:", offset2)
	// fmt.Println(" stringOffset:", stringOffset)
	// fmt.Println(" stringSize:", stringSize)
	// fmt.Println(" ColumnsType:", compressedTypes)
	// fmt.Println(" ColumnsDelta:", deltas)
	// fmt.Println(" ColumnsOffset:", offsets)

	return &BatchFooter{
		DeltaDelta:    deltaDelta,
		DeltaOffset:   deltaOffset,
		Offset1:       offset1,
		Offset2:       offset2,
		StringOffset:  stringOffset,
		StringSize:    stringSize,
		ColumnsType:   compressedTypes,
		ColumnsOffset: offsets,
		ColumnsDelta:  deltas,
	}, nil
}
