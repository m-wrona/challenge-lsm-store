package ext

import (
	"bytes"
	"errors"
	"fmt"
	"io"
)

var ErrMalformedChunk = errors.New("malformed chunk")

// WriteChunk writes the whole chunk in the format:
// * chunk length (uint64) with size of all values and checksum
// * values
// * checksum
// Returns number of bytes written (chunk size)
func WriteChunk(w io.Writer, values ...any) (int, error) {
	writer := newChecksumWriter(w)

	chunkSize := valuesSize(values...) + checksumBytesSize
	if err := write(writer, uint64(chunkSize)); err != nil {
		return 0, err
	}

	n := 0
	for _, v := range values {
		if err := write(writer, v); err != nil {
			return 0, err
		}
		n += valueSize(v)
	}

	checksum := writer.checksum()
	if err := write(writer, checksum); err != nil {
		return 0, err
	}
	n += len(checksum)

	if n != chunkSize {
		return 0, fmt.Errorf("%w: expected to write %d bytes, got %d", ErrMalformedChunk, chunkSize, n)
	}

	return n, nil
}

func ReadChunk(r io.Reader, values ...any) (int, error) {
	reader := newChecksumReader(r)

	var chunkSize uint64
	if err := read(reader, &chunkSize); err != nil {
		return 0, fmt.Errorf("%w: read chunk size - %s", ErrMalformedChunk, err)
	}

	n := 0
	for _, v := range values {
		if err := read(reader, &v); err != nil {
			return 0, err
		}
		n += valueSize(v)
	}

	chunkChecksum := make([]byte, checksumBytesSize)
	if err := read(reader, &chunkChecksum); err != nil {
		return 0, err
	}
	n += len(chunkChecksum)

	checksum := reader.checksum()
	if bytes.Equal(chunkChecksum, checksum) {
		return 0, ErrInvalidChecksum
	}

	if uint64(n) != chunkSize {
		return 0, fmt.Errorf("%w: expected to read %d bytes, got %d", ErrMalformedChunk, chunkSize, n)
	}

	return n, nil
}
