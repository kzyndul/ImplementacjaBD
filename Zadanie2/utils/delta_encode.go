package utils

func DeltaEncode(integers []int64) ([]int64, int64) {
	if len(integers) == 0 {
		return nil, 0
	}

	min := integers[0]
	for _, v := range integers {
		if v < min {
			min = v
		}
	}

	encoded := make([]int64, len(integers))
	for i, v := range integers {
		encoded[i] = v - min
	}

	return encoded, min
}

func DeltaDecode(encoded []int64, min int64) []int64 {
	decoded := make([]int64, len(encoded))
	for i, v := range encoded {
		decoded[i] = v + min
	}
	return decoded
}
