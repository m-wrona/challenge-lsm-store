package sstable

import (
	"fmt"
	"io"
)

const (
	sparseKeyDistance = 5 // TODO move it to config with optionals
)

type Writer struct {
	// writers
	dataWriter        io.WriteCloser
	indexWriter       io.WriteCloser
	sparseIndexWriter io.WriteCloser

	// state
	dataPos  int
	indexPos int
	keys     int
}

func NewWriter(
	dataWriter io.WriteCloser,
	indexWriter io.WriteCloser,
	sparseIndexWriter io.WriteCloser,
) *Writer {
	return &Writer{
		dataWriter:        dataWriter,
		indexWriter:       indexWriter,
		sparseIndexWriter: sparseIndexWriter,
	}
}

func (w *Writer) Write(key, value []byte) error {
	dataBytes, err := encode(key, value, w.dataWriter)
	if err != nil {
		return fmt.Errorf("data write error: %w", err)
	}

	indexBytes, err := encodeKeyOffset(key, w.dataPos, w.indexWriter)
	if err != nil {
		return fmt.Errorf("index write error: %w", err)
	}

	if w.keys%sparseKeyDistance == 0 {
		if _, err := encodeKeyOffset(key, w.indexPos, w.sparseIndexWriter); err != nil {
			return fmt.Errorf("sparse index write error: %w", err)
		}
	}

	w.dataPos += dataBytes
	w.indexPos += indexBytes
	w.keys += 1

	return nil
}

func (w *Writer) Close() error {
	if err := w.dataWriter.Close(); err != nil {
		return err
	}
	if err := w.indexWriter.Close(); err != nil {
		return err
	}
	if err := w.sparseIndexWriter.Close(); err != nil {
		return err
	}
	return nil
}
