package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
)

// version marks type of read WAL entries
type version uint8

const (
	v1 version = 1
)

var ErrInvalidVersion = errors.New("invalid entry version")
var ErrInvalidEmptyKey = errors.New("empty key")

// EntryV1 keeps basic change information
type EntryV1 struct {
	Key   []byte
	Value []byte
}

func (e *EntryV1) Encode(buff *bytes.Buffer) error {
	if err := e.Validate(); err != nil {
		return err
	}

	// type / version
	if err := binary.Write(buff, binary.LittleEndian, v1); err != nil {
		return err
	}

	// key
	if err := binary.Write(buff, binary.LittleEndian, uint16(len(e.Key))); err != nil {
		return err
	}
	if _, err := buff.Write(e.Key); err != nil {
		return err
	}

	// value
	if err := binary.Write(buff, binary.LittleEndian, uint16(len(e.Value))); err != nil {
		return err
	}
	if _, err := buff.Write(e.Value); err != nil {
		return err
	}

	return nil
}

func (e *EntryV1) Decode(buff *bytes.Buffer) error {
	// type / version
	var v version
	if err := binary.Read(buff, binary.LittleEndian, &v); err != nil {
		return err
	}

	if v != v1 {
		return ErrInvalidVersion
	}

	// key
	var blockLength uint16
	if err := binary.Read(buff, binary.LittleEndian, &blockLength); err != nil {
		return err
	}

	e.Key = make([]byte, blockLength)
	if _, err := io.ReadFull(buff, e.Key); err != nil {
		return err
	}

	// value
	if err := binary.Read(buff, binary.LittleEndian, &blockLength); err != nil {
		return err
	}

	// TODO check whether empty and nil values should be handled here
	e.Value = make([]byte, blockLength)
	if _, err := io.ReadFull(buff, e.Value); err != nil {
		return err
	}

	return nil
}

func (e *EntryV1) Validate() error {
	if len(e.Key) == 0 {
		return ErrInvalidEmptyKey
	}
	return nil
}
