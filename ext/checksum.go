package ext

import (
	"errors"
	"hash"
	"hash/crc32"
	"io"
)

const checksumBytesSize = 4

var ErrInvalidChecksum = errors.New("invalid checksum")

// checksumWriter plain wrapper to be able to trace written bytes to count hash when needed.
type checksumWriter struct {
	hash   hash.Hash
	writer io.Writer
}

// checksumWriter plain wrapper to be able to trace written bytes to count hash when needed.
type checksumReader struct {
	hash   hash.Hash
	reader io.Reader
}

func checksumAlgorithm() hash.Hash {
	return crc32.NewIEEE()
}

func newChecksumWriter(writer io.Writer) *checksumWriter {
	return &checksumWriter{
		writer: writer,
		hash:   checksumAlgorithm(),
	}
}

func newChecksumReader(reader io.Reader) *checksumReader {
	return &checksumReader{
		reader: reader,
		hash:   checksumAlgorithm(),
	}
}

func (w *checksumWriter) Write(p []byte) (int, error) {
	n, err := w.writer.Write(p)
	_, _ = w.hash.Write(p[:n])
	return n, err
}

func (w *checksumWriter) checksum() []byte {
	return w.hash.Sum(nil)
}

func (r *checksumReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.hash.Write(p[:n])
	return n, err
}

func (r *checksumReader) checksum() []byte {
	return r.hash.Sum(nil)
}
