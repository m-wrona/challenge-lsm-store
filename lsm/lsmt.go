package lsm

import (
	"bytes"
	"challenge-lsm-store/memtable"
	"challenge-lsm-store/wal"
)

type LSMTSettings struct {
	MemoryThreshold int
}

type LSMT struct {
	memory   *memtable.Memtable
	flush    *memtable.Memtable
	wal      *wal.Writer
	settings LSMTSettings
	buff     *bytes.Buffer
}

func NewLSMT(wal *wal.Writer, settings LSMTSettings) *LSMT {
	return &LSMT{
		wal:  wal,
		buff: bytes.NewBuffer(nil),
	}
}

func (t *LSMT) Put(key []byte, value []byte) error {
	// WAL
	defer t.buff.Reset()
	walEntry := wal.EntryV1{
		Key:   key,
		Value: value,
	}
	if err := walEntry.Encode(t.buff); err != nil {
		return err
	}
	if err := t.wal.Write(t.buff.Bytes()); err != nil {
		return err
	}

	// memory
	t.memory.Upsert(key, value)
	if t.memory.GetSize() > t.settings.MemoryThreshold {
		//TODO flush
	}

	return nil
}

func (t *LSMT) Get(key []byte) ([]byte, error) {
	value, found := t.memory.Get(key)
	if found {
		return value, nil
	}

	return nil, nil
}
