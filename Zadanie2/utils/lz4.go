package utils

import (
	"bytes"
	"io"

	"github.com/pierrec/lz4/v4"
)

func CompressLZ4(data []byte) ([]byte, error) {
	var out bytes.Buffer
	w := lz4.NewWriter(&out)
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func DecompressLZ4(compressed []byte) ([]byte, error) {
	r := bytes.NewReader(compressed)
	reader := lz4.NewReader(r)
	var out bytes.Buffer
	if _, err := io.Copy(&out, reader); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}
