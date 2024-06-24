package sstable_test

import (
	"bytes"
)

type closeableWriter struct {
	buff     *bytes.Buffer
	writeErr error
}

type closeableReader struct {
	reader *bytes.Reader
}

func (b *closeableWriter) Write(p []byte) (n int, err error) {
	if b.writeErr != nil {
		return 0, b.writeErr
	}
	return b.buff.Write(p)
}

func (b *closeableWriter) Close() error {
	return nil
}

func (b *closeableWriter) Bytes() []byte {
	return b.buff.Bytes()
}

func (b *closeableWriter) Reader() *closeableReader {
	return &closeableReader{reader: bytes.NewReader(b.Bytes())}
}

func (b *closeableReader) Read(p []byte) (n int, err error) {
	return b.reader.Read(p)
}

func (b *closeableReader) Seek(offset int64, whence int) (int64, error) {
	return b.reader.Seek(offset, whence)
}

func (b *closeableReader) Close() error {
	return nil
}
