package utils

import (
	"bytes"
	"math/rand"
)

// GenerateRandomNumber generates a random number for file naming
func GenerateRandomNumber() int {
	return rand.Intn(1 << 30)
}

// TODO uints?
func BuildConcatenatedBlob(strs []string) ([]byte, []int64) {
	var buf bytes.Buffer
	offsets := make([]int64, len(strs))
	var currentOffset int64 = 0

	for i, s := range strs {
		offsets[i] = currentOffset
		buf.WriteString(s)
		buf.WriteByte(0)
		currentOffset += int64(len(s)) + 1
	}

	offsets = append(offsets, currentOffset)

	return buf.Bytes(), offsets
}

func DecompressStrings(data []byte, offsets []int64) []string {
	strings := make([]string, len(offsets)-1)
	for i := range strings {
		start := offsets[i]
		end := offsets[i+1]
		strings[i] = string(data[start : end-1])
	}
	return strings
}

func CompressIntegers(ints []int64) ([]byte, int64) {
	deltaEncoded, min := DeltaEncode(ints)
	variableLengthEncoded := VariableLengthEncode(deltaEncoded)
	return variableLengthEncoded, min
}

func DecompressIntegers(data []byte, min int64) []int64 {
	variableLengthDecoded := VariableLengthDecode(data)
	deltaDecoded := DeltaDecode(variableLengthDecoded, min)
	return deltaDecoded
}
