package deserializer

import (
	"Zadanie2/utils"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
)

const (
	TypeInt    byte = 0
	TypeString byte = 1
	HeaderSize      = 13 // 1 + 4 + 8

	BatchSize = 1
)

type ColumnFileHeader struct {
	ColumnType   byte  // 0 = int, 1 = string
	NumBatches   int32 // Number of batches in this column
	FooterOffset int64 // Offset where footer starts
}

type ColumnFooter struct {
	BatchOffsets []int64 // Offset where each batch starts (length = NumBatches + 1)
	BatchDeltas  []int64 // Delta values for each batch (length = NumBatches)
	StringSizes  []int64 // Size of compressed string for each batch (length = NumBatches, only for string columns)
}

type Serializer struct {
	tablePath  string
	numRows    int32
	numColumns int32
}

type Batch struct {
	BatchSize   int32
	NumColumns  int32
	ColumnTypes []byte
	Data        [][]int64
	String      map[int]string
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
	for colIdx := int32(0); colIdx < batch.NumColumns; colIdx++ {
		if err := s.writeColumnBatch(colIdx, batchIndex, batch); err != nil {
			return fmt.Errorf("failed to write column %d: %w", colIdx, err)
		}
	}
	return nil
}

func (s *Serializer) writeColumnBatch(colIdx int32, batchIndex int, batch *Batch) error {
	columnFile := filepath.Join(s.tablePath, fmt.Sprintf("column_%d.dat", colIdx))

	// Check if file exists
	fileExists := false
	if _, err := os.Stat(columnFile); err == nil {
		fileExists = true
	}

	var file *os.File
	var err error
	var header ColumnFileHeader
	var footer ColumnFooter

	if fileExists {
		// Open existing file and read current header/footer
		file, err = os.OpenFile(columnFile, os.O_RDWR, 0644)
		if err != nil {
			return err
		}
		defer file.Close()

		header, err = s.readColumnHeader(file)
		if err != nil {
			return err
		}

		footer, err = s.readColumnFooter(file, &header)
		if err != nil {
			return err
		}
	} else {
		file, err = os.Create(columnFile)
		if err != nil {
			return err
		}
		defer file.Close()

		header = ColumnFileHeader{
			ColumnType:   batch.ColumnTypes[colIdx],
			NumBatches:   0,
			FooterOffset: HeaderSize,
		}
		footer = ColumnFooter{
			BatchOffsets: []int64{HeaderSize},
			BatchDeltas:  []int64{},
			StringSizes:  []int64{},
		}
	}

	row := make([]int64, len(batch.Data[colIdx]))
	copy(row, batch.Data[colIdx])

	compressed, minValue := utils.CompressIntegers(row)

	currentOffset := footer.BatchOffsets[len(footer.BatchOffsets)-1]
	if _, err := file.Seek(currentOffset, 0); err != nil {
		return err
	}

	n, err := file.Write(compressed)
	if err != nil {
		return err
	}
	currentOffset += int64(n)

	var stringSize int64 = 0
	if batch.ColumnTypes[colIdx] == TypeString {
		compressedString, err := utils.CompressLZ4([]byte(batch.String[int(colIdx)]))
		if err != nil {
			return err
		}
		stringSize = int64(len(compressedString))

		// Write string size
		// if err := binary.Write(file, binary.LittleEndian, stringSize); err != nil {
		// return err
		// }
		// currentOffset += 8 // int64 size

		// Write compressed string data
		nString, err := file.Write(compressedString)
		if err != nil {
			return err
		}
		currentOffset += int64(nString)
	}

	footer.BatchOffsets = append(footer.BatchOffsets, currentOffset)
	footer.BatchDeltas = append(footer.BatchDeltas, minValue)
	footer.StringSizes = append(footer.StringSizes, stringSize)
	header.NumBatches++

	header.FooterOffset = currentOffset
	if err := s.writeColumnFooter(file, &header, &footer); err != nil {
		return err
	}

	if err := s.writeColumnHeader(file, &header); err != nil {
		return err
	}

	return nil
}

func (s *Serializer) writeColumnHeader(file *os.File, h *ColumnFileHeader) error {
	if _, err := file.Seek(0, 0); err != nil {
		return err
	}
	if err := binary.Write(file, binary.LittleEndian, h.ColumnType); err != nil {
		return err
	}
	if err := binary.Write(file, binary.LittleEndian, h.NumBatches); err != nil {
		return err
	}
	if err := binary.Write(file, binary.LittleEndian, h.FooterOffset); err != nil {
		return err
	}
	return nil
}

func (s *Serializer) writeColumnFooter(file *os.File, h *ColumnFileHeader, f *ColumnFooter) error {
	if _, err := file.Seek(h.FooterOffset, 0); err != nil {
		return err
	}

	// Write batch offsets
	for _, offset := range f.BatchOffsets {
		if err := binary.Write(file, binary.LittleEndian, offset); err != nil {
			return err
		}
	}

	// Write batch deltas
	for _, delta := range f.BatchDeltas {
		if err := binary.Write(file, binary.LittleEndian, delta); err != nil {
			return err
		}
	}

	// Write string sizes for string columns
	// if h.ColumnType == TypeString {
	for _, size := range f.StringSizes {
		if err := binary.Write(file, binary.LittleEndian, size); err != nil {
			return err
		}
	}
	// }

	return nil
}

func (s *Serializer) readColumnHeader(file *os.File) (ColumnFileHeader, error) {
	header := ColumnFileHeader{}
	if _, err := file.Seek(0, 0); err != nil {
		return header, err
	}
	if err := binary.Read(file, binary.LittleEndian, &header.ColumnType); err != nil {
		return header, err
	}
	if err := binary.Read(file, binary.LittleEndian, &header.NumBatches); err != nil {
		return header, err
	}
	if err := binary.Read(file, binary.LittleEndian, &header.FooterOffset); err != nil {
		return header, err
	}
	return header, nil
}

func (s *Serializer) readColumnFooter(file *os.File, h *ColumnFileHeader) (ColumnFooter, error) {
	footer := ColumnFooter{}
	if _, err := file.Seek(h.FooterOffset, 0); err != nil {
		return footer, err
	}

	// Read batch offsets
	footer.BatchOffsets = make([]int64, h.NumBatches+1)
	for i := range footer.BatchOffsets {
		if err := binary.Read(file, binary.LittleEndian, &footer.BatchOffsets[i]); err != nil {
			return footer, err
		}
	}

	// Read batch deltas
	footer.BatchDeltas = make([]int64, h.NumBatches)
	for i := range footer.BatchDeltas {
		if err := binary.Read(file, binary.LittleEndian, &footer.BatchDeltas[i]); err != nil {
			return footer, err
		}
	}

	// Read string sizes if string column
	// if h.ColumnType == TypeString {
	footer.StringSizes = make([]int64, h.NumBatches)
	for i := range footer.StringSizes {
		if err := binary.Read(file, binary.LittleEndian, &footer.StringSizes[i]); err != nil {
			return footer, err
		}
	}
	// }

	return footer, nil
}
