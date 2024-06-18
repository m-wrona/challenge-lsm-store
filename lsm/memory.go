package lsm

import (
	"bytes"
	"challenge-lsm-store/memtable"
	"challenge-lsm-store/sstable"
	"challenge-lsm-store/wal"
	"sync"
)

// memoryStorage represents data kept only in memory for now but backed-up using WAL
type memoryStorage struct {
	memory *memtable.Memtable
	wal    *wal.Writer
	buff   *bytes.Buffer
	mu     sync.RWMutex
}

func (t *memoryStorage) Size() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.memory.Size()
}

func (t *memoryStorage) Get(key []byte) ([]byte, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.memory.Get(key)
}

func (t *memoryStorage) Put(key []byte, value []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()

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
	return nil
}

func (t *memoryStorage) Write(writer *sstable.Writer) error {
	t.mu.RLock() // because it only reads from memory
	defer t.mu.RUnlock()

	for e := range t.memory.GetAll() {
		if err := writer.Write(e.GetKey(), e.GetValue()); err != nil {
			return err
		}
	}

	return nil
}

func (t *memoryStorage) Clear() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if err := t.wal.Close(); err != nil {
		return err
	}
	if err := t.wal.Delete(); err != nil {
		return err
	}

	t.memory.Clear()

	return nil
}
