package storage

import (
	"hash"
	"hash/crc32"
	"io"
)

// checksumWriter plain wrapper to be able to trace written bytes to count hash when needed.
type checksumWriter struct {
	hash   hash.Hash
	writer io.Writer
}

func newChecksumWriter(writer io.Writer) checksumWriter {
	return checksumWriter{
		writer: writer,
		hash:   crc32.NewIEEE(),
	}
}

func (w checksumWriter) Write(p []byte) (int, error) {
	n, err := w.writer.Write(p)
	_, _ = w.hash.Write(p[:n])
	return n, err
}

func (w checksumWriter) checksum() []byte {
	return w.hash.Sum(nil)
}

// writeWithChecksum writes all values in a row with checksum in the end.
func writeWithChecksum(w io.Writer, values ...any) (int, error) {
	writer := newChecksumWriter(w)
	n := 0

	for _, v := range values {
		if err := write(writer, v); err != nil {
			return 0, err
		}
		n += intDataSize(v)
	}

	checksum := writer.checksum()
	if err := write(writer, checksum); err != nil {
		return 0, err
	}
	n += len(checksum)

	return n, nil
}

// intDataSize returns the size of the data required to represent the data when encoded.
// It returns zero if the type cannot be implemented by the fast path in Read or Write.
func intDataSize(data any) int {
	switch data := data.(type) {
	case bool, int8, uint8, *bool, *int8, *uint8:
		return 1
	case []bool:
		return len(data)
	case []int8:
		return len(data)
	case []uint8:
		return len(data)
	case int16, uint16, *int16, *uint16:
		return 2
	case []int16:
		return 2 * len(data)
	case []uint16:
		return 2 * len(data)
	case int32, uint32, *int32, *uint32:
		return 4
	case []int32:
		return 4 * len(data)
	case []uint32:
		return 4 * len(data)
	case int64, uint64, *int64, *uint64:
		return 8
	case []int64:
		return 8 * len(data)
	case []uint64:
		return 8 * len(data)
	case float32, *float32:
		return 4
	case float64, *float64:
		return 8
	case []float32:
		return 4 * len(data)
	case []float64:
		return 8 * len(data)
	default:
		return 0 // TODO check if error should be returned here
	}
}
