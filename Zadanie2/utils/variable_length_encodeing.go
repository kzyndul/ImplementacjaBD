package utils

import "fmt"

// TODO uints?
func VariableLengthEncode(integers []int64) []byte {
	result := make([]byte, 0, len(integers)*2)

	for _, n := range integers {
		for {
			b := byte(n & 0x7F)
			n >>= 7

			if n != 0 {
				b |= 0x80
			}

			result = append(result, b)

			if n == 0 {
				break
			}
		}
	}

	return result
}

// TODO uints?
func VariableLengthDecode(data []byte) []int64 {
	result := make([]int64, 0)
	var current int64
	var shift uint

	for _, currentByte := range data {

		current |= int64(currentByte&0x7F) << shift
		shift += 7

		if currentByte&0x80 == 0 {
			result = append(result, current)
			current = 0
			shift = 0
		}
	}

	return result
}

func TestVariableLengthEncoding() {

	test := []int64{1, 200, 31212, 4, 5232323}

	encoded := VariableLengthEncode([]int64{1, 200, 31212, 4, 5232323})

	decoded := VariableLengthDecode(encoded)

	println("Original:", fmt.Sprint(test))
	println("Encoded:", fmt.Sprint(encoded))
	println("Decoded:", fmt.Sprint(decoded))
}
