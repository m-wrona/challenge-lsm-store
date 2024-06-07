package sstable

import (
	"fmt"
	"io"
)

type Writer struct {
	cfg Config

	// writers
	dataWriter        io.Writer
	indexWriter       io.Writer
	sparseIndexWriter io.Writer

	// state
	dataPos  int
	indexPos int
	keys     int
}

func (t *Writer) Write(key, value []byte) error {
	dataBytes, err := encode(key, value, t.dataWriter)
	if err != nil {
		return fmt.Errorf("data write error: %w", err)
	}

	indexBytes, err := encodeKeyOffset(key, t.dataPos, t.indexWriter)
	if err != nil {
		return fmt.Errorf("index write error: %w", err)
	}

	if t.keys%t.cfg.SparseKeyDistance == 0 {
		if _, err := encodeKeyOffset(key, t.indexPos, t.sparseIndexWriter); err != nil {
			return fmt.Errorf("sparse index write error: %w", err)
		}
	}

	t.dataPos += dataBytes
	t.indexPos += indexBytes
	t.keys += 1

	return nil
}
