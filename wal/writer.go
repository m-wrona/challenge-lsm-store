package wal

import (
	"challenge-lsm-store/storageio"
	"encoding/binary"
	"io"
)

type WriterSync = func() error
type WriterDelete = func() error

type Writer struct {
	writer         io.WriteCloser
	checksumWriter *storageio.ChecksumWriter
	sync           WriterSync
	delete         WriterDelete
}

func NewWriter(w io.WriteCloser, sync WriterSync, delete WriterDelete) *Writer {
	return &Writer{
		writer:         w,
		checksumWriter: storageio.NewChecksumWriter(w),
		sync:           sync,
		delete:         delete,
	}
}

func (w *Writer) Write(bytes []byte) error {
	defer w.checksumWriter.Clear()

	if err := binary.Write(w.checksumWriter, binary.LittleEndian, uint32(len(bytes))); err != nil {
		return err
	}

	if _, err := w.checksumWriter.Write(bytes); err != nil {
		return err
	}

	checksum := w.checksumWriter.Checksum()
	if _, err := w.checksumWriter.Write(checksum); err != nil {
		return err
	}

	return w.sync()
}

func (w *Writer) Close() error {
	w.checksumWriter.Clear()
	return w.writer.Close()
}

func (w *Writer) Delete() error {
	return w.delete()
}
