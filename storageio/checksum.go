package storageio

import (
	"errors"
	"hash"
	"hash/crc32"
	"io"
)

const ChecksumBytesSize = 4

var ErrInvalidChecksum = errors.New("invalid checksum")

// ChecksumWriter plain wrapper to be able to trace written bytes to count hash when needed.
type ChecksumWriter struct {
	hash   hash.Hash
	writer io.Writer
}

// ChecksumReader plain wrapper to be able to trace read bytes to count hash when needed.
type ChecksumReader struct {
	hash   hash.Hash
	reader io.Reader
}

func checksumAlgorithm() hash.Hash {
	return crc32.NewIEEE()
}

func NewChecksumWriter(writer io.Writer) *ChecksumWriter {
	return &ChecksumWriter{
		writer: writer,
		hash:   checksumAlgorithm(),
	}
}

func NewChecksumReader(reader io.Reader) *ChecksumReader {
	return &ChecksumReader{
		reader: reader,
		hash:   checksumAlgorithm(),
	}
}

func (w *ChecksumWriter) Write(p []byte) (int, error) {
	n, err := w.writer.Write(p)
	_, _ = w.hash.Write(p[:n])
	return n, err
}

func (w *ChecksumWriter) Checksum() []byte {
	return w.hash.Sum(nil)
}

func (w *ChecksumWriter) Clear() {
	w.hash.Reset()
}

func (r *ChecksumReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.hash.Write(p[:n])
	return n, err
}

func (r *ChecksumReader) Checksum() []byte {
	return r.hash.Sum(nil)
}

func (r *ChecksumReader) Clear() {
	r.hash.Reset()
}
