package utils

import (
	"math/rand"
)

func GenerateRandomNumber() int {
	return rand.Intn(1 << 30)
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
