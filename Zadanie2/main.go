package main

import (
	"Zadanie2/utils"
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

// const (
// 	TypeInt    byte = 0
// 	TypeString byte = 1
// 	HeaderSize      = 21 // 1 + 4 + 8 + 8
// )

// type FileHeader struct {
// 	Type         byte  // 0 = int, 1 = string
// 	NumRows      int32 // Number of rows in batch
// 	FooterOffset int64 // Offset where footer starts
// 	DataOffset   int64 // Offset where data starts (0 for int type)
// }

// type BatchFooter struct {
// 	BatchOffset int64 // Where this batch starts in file
// 	DataSize    int32 // Size of compressed data in bytes
// }

// // Serializer handles writing data to file
// type Serializer struct {
// 	file           *os.File
// 	currentOffset  int64
// 	batches        []BatchFooter
// 	dataType       byte
// 	footerStartPos int64
// }

// // NewSerializer creates a new serializer for given file path
// func NewSerializer(filePath string, dataType byte) (*Serializer, error) {
// 	file, err := os.Create(filePath)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return &Serializer{
// 		file:          file,
// 		currentOffset: 0,
// 		batches:       make([]BatchFooter, 0),
// 		dataType:      dataType,
// 	}, nil
// }

// // WriteIntBatch writes a batch of integers
// func (s *Serializer) WriteIntBatch(values []int64) error {
// 	if s.dataType != TypeInt {
// 		return fmt.Errorf("serializer is not configured for int type")
// 	}

// 	batchOffset := s.currentOffset

// 	// Compress integers using delta + variable length encoding
// 	compressed, err := compressIntegers(values)
// 	if err != nil {
// 		return err
// 	}

// 	// Write header
// 	header := FileHeader{
// 		Type:         TypeInt,
// 		NumRows:      int32(len(values)),
// 		FooterOffset: 0, // Will be updated in Finalize
// 		DataOffset:   0, // Not used for int type
// 	}

// 	if err := s.writeHeader(header); err != nil {
// 		return err
// 	}

// 	// Write compressed data
// 	n, err := s.file.Write(compressed)
// 	if err != nil {
// 		return err
// 	}

// 	s.currentOffset += int64(n)

// 	// Record batch info
// 	s.batches = append(s.batches, BatchFooter{
// 		BatchOffset: batchOffset,
// 		// NumRows:     int32(len(values)),
// 		DataSize: int32(len(compressed)),
// 	})

// 	// s.firstBatch = false
// 	return nil
// }

// // WriteStringBatch writes a batch of strings
// func (s *Serializer) WriteStringBatch(values []string) error {
// 	if s.dataType != TypeString {
// 		return fmt.Errorf("serializer is not configured for string type")
// 	}

// 	batchOffset := s.currentOffset

// 	// Build concatenated blob and offsets
// 	blob, offsets := utils.BuildConcatenatedBlob(values)

// 	// Compress the blob
// 	compressedBlob, err := utils.CompressLZ4(blob)
// 	if err != nil {
// 		return err
// 	}

// 	// Compress offsets using delta + variable length encoding
// 	compressedOffsets, err := compressOffsets(offsets)
// 	if err != nil {
// 		return err
// 	}

// 	// Calculate data offset (after header + compressed offsets)
// 	dataOffset := s.currentOffset + HeaderSize + int64(len(compressedOffsets))

// 	// Write header
// 	header := FileHeader{
// 		Type:         TypeString,
// 		NumRows:      int32(len(values)),
// 		FooterOffset: 0, // Will be updated in Finalize
// 		DataOffset:   dataOffset,
// 	}

// 	if err := s.writeHeader(header); err != nil {
// 		return err
// 	}

// 	// Write compressed offsets
// 	n1, err := s.file.Write(compressedOffsets)
// 	if err != nil {
// 		return err
// 	}
// 	s.currentOffset += int64(n1)

// 	// Write compressed blob
// 	n2, err := s.file.Write(compressedBlob)
// 	if err != nil {
// 		return err
// 	}
// 	s.currentOffset += int64(n2)

// 	// Record batch info
// 	totalDataSize := len(compressedOffsets) + len(compressedBlob)
// 	s.batches = append(s.batches, BatchFooter{
// 		BatchOffset: batchOffset,
// 		DataSize:    int32(totalDataSize),
// 	})

// 	return nil
// }

// func (s *Serializer) Finalize() error {
// 	s.footerStartPos = s.currentOffset

// 	// Write all batches information
// 	for _, batch := range s.batches {
// 		if err := binary.Write(s.file, binary.LittleEndian, batch.BatchOffset); err != nil {
// 			return err
// 		}
// 		if err := binary.Write(s.file, binary.LittleEndian, batch.DataSize); err != nil {
// 			return err
// 		}
// 	}

// 	for _, batch := range s.batches {
// 		if _, err := s.file.Seek(batch.BatchOffset+5, 0); err != nil {
// 			return err
// 		}
// 		if err := binary.Write(s.file, binary.LittleEndian, s.footerStartPos); err != nil {
// 			return err
// 		}
// 	}

// 	return s.file.Close()
// }

// func (s *Serializer) writeHeader(h FileHeader) error {
// 	if err := binary.Write(s.file, binary.LittleEndian, h.Type); err != nil {
// 		return err
// 	}
// 	if err := binary.Write(s.file, binary.LittleEndian, h.NumRows); err != nil {
// 		return err
// 	}
// 	if err := binary.Write(s.file, binary.LittleEndian, h.FooterOffset); err != nil {
// 		return err
// 	}
// 	if err := binary.Write(s.file, binary.LittleEndian, h.DataOffset); err != nil {
// 		return err
// 	}

// 	s.currentOffset += HeaderSize
// 	return nil
// }

// // func CompressStringArrayLZ4(strs []string) ([]byte, []uint32, error) {
// // 	blob, offsets := utils.BuildConcatenatedBlob(strs)
// // 	compressed, err := utils.CompressLZ4(blob)
// // 	if err != nil {
// // 		return nil, nil, err
// // 	}
// // 	return compressed, offsets, nil
// // }

// // func GetStringAtIndex(data []byte, offsets []uint32, index int) (string, error) {
// // 	if index < 0 || index >= len(offsets) {
// // 		return "", fmt.Errorf("index out of bounds")
// // 	}
// // 	start := offsets[index]
// // 	end := start
// // 	for end < uint32(len(data)) && data[end] != 0 {
// // 		end++
// // 	}
// // 	return string(data[start:end]), nil
// // }

// // // Deserializer handles reading data from file
// // type Deserializer struct {
// // 	file         *os.File
// // 	batches      []BatchFooter
// // 	dataType     byte
// // 	footerOffset int64
// // 	batchSize    int32
// // 	totalBatches int
// // }

// // // NewDeserializer creates a new deserializer for given file path
// // func NewDeserializer(filePath string) (*Deserializer, error) {
// // 	file, err := os.Open(filePath)
// // 	if err != nil {
// // 		return nil, err
// // 	}

// // 	d := &Deserializer{
// // 		file:    file,
// // 		batches: make([]BatchFooter, 0),
// // 	}

// // 	// Read first header to get footer offset and data type
// // 	header, err := d.readHeaderAt(0)
// // 	if err != nil {
// // 		file.Close()
// // 		return nil, err
// // 	}

// // 	d.dataType = header.Type
// // 	d.footerOffset = header.FooterOffset
// // 	d.batchSize = header.NumRows

// // 	// Read footer to get all batch information
// // 	if err := d.readFooter(); err != nil {
// // 		file.Close()
// // 		return nil, err
// // 	}

// // 	d.totalBatches = len(d.batches)

// // 	return d, nil
// // }

// // // readFooter reads all batch information from footer
// // func (d *Deserializer) readFooter() error {
// // 	if _, err := d.file.Seek(d.footerOffset, 0); err != nil {
// // 		return err
// // 	}

// // 	// Read batches until EOF
// // 	for {
// // 		var batch BatchFooter
// // 		if err := binary.Read(d.file, binary.LittleEndian, &batch.BatchOffset); err != nil {
// // 			if err == io.EOF {
// // 				break
// // 			}
// // 			return err
// // 		}
// // 		// if err := binary.Read(d.file, binary.LittleEndian, &batch.NumRows); err != nil {
// // 		// 	return err
// // 		// }
// // 		if err := binary.Read(d.file, binary.LittleEndian, &batch.DataSize); err != nil {
// // 			return err
// // 		}
// // 		d.batches = append(d.batches, batch)
// // 	}

// // 	return nil
// // }

// // // readHeaderAt reads and returns the file header at given offset
// // func (d *Deserializer) readHeaderAt(offset int64) (*FileHeader, error) {
// // 	header := &FileHeader{}

// // 	buf := make([]byte, HeaderSize)
// // 	if _, err := d.file.ReadAt(buf, offset); err != nil {
// // 		return nil, err
// // 	}

// // 	header.Type = buf[0]
// // 	header.NumRows = int32(binary.LittleEndian.Uint32(buf[1:5]))
// // 	header.FooterOffset = int64(binary.LittleEndian.Uint64(buf[5:13]))
// // 	header.DataOffset = int64(binary.LittleEndian.Uint64(buf[13:21]))

// // 	return header, nil
// // }

// // // ReadIntBatch reads and decompresses a batch of integers by batch index
// // func (d *Deserializer) ReadIntBatch(batchIndex int) ([]int64, error) {
// // 	if d.dataType != TypeInt {
// // 		return nil, fmt.Errorf("file contains string type, not int")
// // 	}

// // 	if batchIndex < 0 || batchIndex >= len(d.batches) {
// // 		return nil, fmt.Errorf("batch index %d out of bounds", batchIndex)
// // 	}

// // 	batch := d.batches[batchIndex]
// // 	header, err := d.readHeaderAt(batch.BatchOffset)
// // 	if err != nil {
// // 		return nil, err
// // 	}

// // 	// Read compressed data
// // 	compressedData := make([]byte, batch.DataSize)
// // 	if _, err := d.file.ReadAt(compressedData, batch.BatchOffset+HeaderSize); err != nil {
// // 		return nil, err
// // 	}

// // 	// Decompress integers
// // 	values, err := decompressIntegers(compressedData, int(header.NumRows))
// // 	if err != nil {
// // 		return nil, err
// // 	}

// // 	return values, nil
// // }

// // // ReadStringBatch reads and decompresses a batch of strings by batch index
// // func (d *Deserializer) ReadStringBatch(batchIndex int) ([]string, error) {
// // 	if d.dataType != TypeString {
// // 		return nil, fmt.Errorf("file contains int type, not string")
// // 	}

// // 	if batchIndex < 0 || batchIndex >= len(d.batches) {
// // 		return nil, fmt.Errorf("batch index %d out of bounds", batchIndex)
// // 	}

// // 	batch := d.batches[batchIndex]
// // 	header, err := d.readHeaderAt(batch.BatchOffset)
// // 	if err != nil {
// // 		return nil, err
// // 	}

// // 	// Calculate sizes
// // 	offsetsSize := header.DataOffset - (batch.BatchOffset + HeaderSize)
// // 	blobSize := int64(batch.DataSize) - offsetsSize

// // 	// Read compressed offsets
// // 	compressedOffsets := make([]byte, offsetsSize)
// // 	if _, err := d.file.ReadAt(compressedOffsets, batch.BatchOffset+HeaderSize); err != nil {
// // 		return nil, err
// // 	}

// // 	// Decompress offsets
// // 	offsets, err := decompressOffsets(compressedOffsets, int(header.NumRows))
// // 	if err != nil {
// // 		return nil, err
// // 	}

// // 	// Read compressed blob
// // 	compressedBlob := make([]byte, blobSize)
// // 	if _, err := d.file.ReadAt(compressedBlob, header.DataOffset); err != nil {
// // 		return nil, err
// // 	}

// // 	// Decompress blob
// // 	blob, err := utils.DecompressLZ4(compressedBlob)
// // 	if err != nil {
// // 		return nil, err
// // 	}

// // 	// Extract strings using offsets
// // 	strings := make([]string, len(offsets))
// // 	for i := range offsets {
// // 		str, err := GetStringAtIndex(blob, offsets, i)
// // 		if err != nil {
// // 			return nil, err
// // 		}
// // 		strings[i] = str
// // 	}

// // 	return strings, nil
// // }

// // // GetBatchCount returns the total number of batches
// // func (d *Deserializer) GetBatchCount() int {
// // 	return d.totalBatches
// // }

// // // GetDataType returns the data type (TypeInt or TypeString)
// // func (d *Deserializer) GetDataType() byte {
// // 	return d.dataType
// // }

// // // Close closes the file
// // func (d *Deserializer) Close() error {
// // 	return d.file.Close()
// // }

// // // Helper functions for compression/decompression

// // compressIntegers uses delta encoding + variable length encoding
// func compressIntegers(values []int64) ([]byte, error) {
// 	if len(values) == 0 {
// 		return []byte{}, nil
// 	}

// 	buf := make([]byte, 0, len(values)*10)

// 	// Write first value
// 	buf = appendVarint(buf, values[0])

// 	// Write deltas
// 	for i := 1; i < len(values); i++ {
// 		delta := values[i] - values[i-1]
// 		buf = appendVarint(buf, delta)
// 	}

// 	return buf, nil
// }

// // decompressIntegers decodes delta + variable length encoded integers
// func decompressIntegers(data []byte, count int) ([]int64, error) {
// 	values := make([]int64, 0, count)

// 	offset := 0

// 	// Read first value
// 	val, n := readVarint(data[offset:])
// 	if n <= 0 {
// 		return nil, fmt.Errorf("failed to read first value")
// 	}
// 	values = append(values, val)
// 	offset += n

// 	// Read deltas and reconstruct values
// 	for i := 1; i < count; i++ {
// 		delta, n := readVarint(data[offset:])
// 		if n <= 0 {
// 			return nil, fmt.Errorf("failed to read delta at index %d", i)
// 		}
// 		values = append(values, values[i-1]+delta)
// 		offset += n
// 	}

// 	return values, nil
// }

// // compressOffsets compresses uint32 offsets using delta + varint encoding
// func compressOffsets(offsets []uint32) ([]byte, error) {
// 	if len(offsets) == 0 {
// 		return []byte{}, nil
// 	}

// 	buf := make([]byte, 0, len(offsets)*5)

// 	// Write first offset
// 	buf = appendUvarint(buf, uint64(offsets[0]))

// 	// Write deltas
// 	for i := 1; i < len(offsets); i++ {
// 		delta := offsets[i] - offsets[i-1]
// 		buf = appendUvarint(buf, uint64(delta))
// 	}

// 	return buf, nil
// }

// // // decompressOffsets decodes delta + varint encoded offsets
// // func decompressOffsets(data []byte, count int) ([]uint32, error) {
// // 	offsets := make([]uint32, 0, count)

// // 	offset := 0

// // 	// Read first offset
// // 	val, n := readUvarint(data[offset:])
// // 	if n <= 0 {
// // 		return nil, fmt.Errorf("failed to read first offset")
// // 	}
// // 	offsets = append(offsets, uint32(val))
// // 	offset += n

// // 	// Read deltas and reconstruct offsets
// // 	for i := 1; i < count; i++ {
// // 		delta, n := readUvarint(data[offset:])
// // 		if n <= 0 {
// // 			return nil, fmt.Errorf("failed to read delta at index %d", i)
// // 		}
// // 		offsets = append(offsets, offsets[i-1]+uint32(delta))
// // 		offset += n
// // 	}

// // 	return offsets, nil
// // }

// // Variable length encoding helpers
// func appendVarint(buf []byte, v int64) []byte {
// 	tmp := make([]byte, binary.MaxVarintLen64)
// 	n := binary.PutVarint(tmp, v)
// 	return append(buf, tmp[:n]...)
// }

// func readVarint(buf []byte) (int64, int) {
// 	return binary.Varint(buf)
// }

// func appendUvarint(buf []byte, v uint64) []byte {
// 	tmp := make([]byte, binary.MaxVarintLen64)
// 	n := binary.PutUvarint(tmp, v)
// 	return append(buf, tmp[:n]...)
// }

// func readUvarint(buf []byte) (uint64, int) {
// 	return binary.Uvarint(buf)
// }

func main() {
	// testDeltaEncoding()
	utils.TestVariableLengthEncoding()

	RunAllTests()

}
