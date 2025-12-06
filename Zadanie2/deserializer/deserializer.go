package deserializer

import (
	"Zadanie2/utils"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
)

type Deserializer struct {
	tablePath string
}

func NewBatchDeserializer(tablePath string) (*Deserializer, error) {
	return &Deserializer{
		tablePath: tablePath,
	}, nil
}

func (d *Deserializer) ReadTableData() ([][]int64, map[int]string, error) {
	columnFiles, err := d.getColumnFiles()
	if err != nil {
		return nil, nil, err
	}

	if len(columnFiles) == 0 {
		return nil, nil, fmt.Errorf("no column files found")
	}

	firstColPath := filepath.Join(d.tablePath, fmt.Sprintf("column_%d.dat", columnFiles[0]))
	firstFile, err := os.Open(firstColPath)
	if err != nil {
		return nil, nil, err
	}
	defer firstFile.Close()

	firstHeader, err := d.ReadColumnHeader(firstFile)
	if err != nil {
		return nil, nil, err
	}

	data := make([][]int64, len(columnFiles))
	stringData := make(map[int]string)

	for _, colIdx := range columnFiles {
		for batchIdx := 0; batchIdx < int(firstHeader.NumBatches); batchIdx++ {
			colPath := filepath.Join(d.tablePath, fmt.Sprintf("column_%d.dat", colIdx))
			values, colType, strData, err := d.readColumnBatch(colPath, batchIdx)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to read column %d: %w", colIdx, err)
			}

			if len(data[colIdx]) == 0 {
				data[colIdx] = make([]int64, 0)
			}

			if colType == TypeString {
				if _, ok := stringData[colIdx]; !ok {
					stringData[colIdx] = ""
				}
				stringData[colIdx] += strData
				lastElement := int64(0)
				if len(data[colIdx]) > 0 {
					lastElement = data[colIdx][len(data[colIdx])-1]
					values = values[1:]
				}

				for i := range values {
					values[i] += lastElement
				}
				data[colIdx] = append(data[colIdx], values...)

			} else {
				data[colIdx] = append(data[colIdx], values...)
			}
		}
	}

	return data, stringData, nil
}

func (d *Deserializer) ReadBatch(batchIndex int) (*Batch, error) {
	columnFiles, err := d.getColumnFiles()
	if err != nil {
		return nil, err
	}

	if len(columnFiles) == 0 {
		return nil, fmt.Errorf("no column files found")
	}

	firstColPath := filepath.Join(d.tablePath, fmt.Sprintf("column_%d.dat", columnFiles[0]))
	firstFile, err := os.Open(firstColPath)
	if err != nil {
		return nil, err
	}

	firstHeader, err := d.ReadColumnHeader(firstFile)
	if err != nil {
		firstFile.Close()
		return nil, err
	}
	firstFile.Close()

	if batchIndex >= int(firstHeader.NumBatches) {
		return nil, fmt.Errorf("batch index %d out of range (total batches: %d)", batchIndex, firstHeader.NumBatches)
	}

	batch := &Batch{
		NumColumns:  int32(len(columnFiles)),
		ColumnTypes: make([]byte, len(columnFiles)),
		Data:        make([][]int64, len(columnFiles)),
		String:      make(map[int]string),
	}

	for i, colIdx := range columnFiles {
		colPath := filepath.Join(d.tablePath, fmt.Sprintf("column_%d.dat", colIdx))
		data, columnType, stringData, err := d.readColumnBatch(colPath, batchIndex)
		if err != nil {
			return nil, fmt.Errorf("failed to read column %d batch %d: %w", colIdx, batchIndex, err)
		}

		batch.ColumnTypes[i] = columnType
		batch.Data[i] = data

		if columnType == TypeString {
			batch.String[i] = stringData
		}

		if i == 0 {
			batch.BatchSize = int32(len(data))
		}
	}
	return batch, nil
}

func (d *Deserializer) readColumnBatch(columnPath string, batchIndex int) ([]int64, byte, string, error) {
	file, err := os.Open(columnPath)
	if err != nil {
		return nil, 0, "", err
	}
	defer file.Close()

	header, err := d.ReadColumnHeader(file)
	if err != nil {
		return nil, 0, "", err
	}

	footer, err := d.readColumnFooter(file, &header)
	if err != nil {
		return nil, 0, "", err
	}

	if batchIndex >= len(footer.BatchDeltas) {
		return nil, 0, "", fmt.Errorf("batch index %d out of range", batchIndex)
	}

	startOffset := footer.BatchOffsets[batchIndex]
	endOffset := footer.BatchOffsets[batchIndex+1]
	stringSize := footer.StringSizes[batchIndex]

	intsSize := endOffset - startOffset - stringSize
	compressedData := make([]byte, intsSize)
	if _, err := file.ReadAt(compressedData, startOffset); err != nil {
		return nil, 0, "", err
	}

	values := utils.DecompressIntegers(compressedData, footer.BatchDeltas[batchIndex])

	var stringData string

	if header.ColumnType == TypeString {

		stringDataOffset := startOffset + intsSize
		compressedString := make([]byte, stringSize)
		if _, err := file.ReadAt(compressedString, stringDataOffset); err != nil {
			return nil, 0, "", err
		}

		decompressedString, err := utils.DecompressLZ4(compressedString)
		if err != nil {
			return nil, 0, "", err
		}
		stringData = string(decompressedString)

	}
	return values, header.ColumnType, stringData, nil

}

func (d *Deserializer) ReadColumnHeader(file *os.File) (ColumnFileHeader, error) {
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

func (d *Deserializer) readColumnFooter(file *os.File, h *ColumnFileHeader) (ColumnFooter, error) {
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

func (d *Deserializer) getColumnFiles() ([]int, error) {
	files, err := os.ReadDir(d.tablePath)
	if err != nil {
		return nil, err
	}

	var columnIndices []int
	columnRegex := regexp.MustCompile(`^column_(\d+)\.dat$`)

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		matches := columnRegex.FindStringSubmatch(file.Name())
		if len(matches) == 2 {
			index, err := strconv.Atoi(matches[1])
			if err != nil {
				continue
			}
			columnIndices = append(columnIndices, index)
		}
	}

	// Sort to ensure consistent ordering
	sort.Ints(columnIndices)

	return columnIndices, nil
}

func (d *Deserializer) GetNumBatches() (int, error) {
	columnFiles, err := d.getColumnFiles()
	if err != nil {
		return 0, err
	}

	if len(columnFiles) == 0 {
		return 0, nil
	}

	colPath := filepath.Join(d.tablePath, fmt.Sprintf("column_%d.dat", columnFiles[0]))
	file, err := os.Open(colPath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	header, err := d.ReadColumnHeader(file)
	if err != nil {
		return 0, err
	}

	return int(header.NumBatches), nil
}
