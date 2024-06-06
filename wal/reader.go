package wal

import (
	"bytes"
	"encoding/binary"
	"io"
	"segments-disk-writer/storageio"
)

type Reader struct {
	reader         io.ReadCloser
	checksumReader *storageio.ChecksumReader
	fileChecksum   []byte
}

func NewReader(reader io.ReadCloser) *Reader {
	return &Reader{
		reader:         reader,
		checksumReader: storageio.NewChecksumReader(reader),
		fileChecksum:   make([]byte, storageio.ChecksumBytesSize),
	}
}

func (r *Reader) Read() ([]byte, error) {
	defer r.checksumReader.Clear()

	var dataLen uint32
	if err := binary.Read(r.checksumReader, binary.LittleEndian, &dataLen); err != nil {
		return nil, err
	}

	buff := make([]byte, int(dataLen)) // TODO get this buffer from some pool
	if _, err := io.ReadFull(r.checksumReader, buff); err != nil {
		return nil, err
	}
	checksum := r.checksumReader.Checksum()

	if _, err := io.ReadFull(r.checksumReader, r.fileChecksum); err != nil {
		return nil, err
	}

	if !bytes.Equal(r.fileChecksum, checksum) {
		return nil, storageio.ErrInvalidChecksum
	}

	return buff, nil
}

func (r *Reader) Close() error {
	r.checksumReader.Clear()
	return r.reader.Close()
}
