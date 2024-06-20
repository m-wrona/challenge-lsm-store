package lsm

import (
	"bytes"
	"challenge-lsm-store/memtable"
	"challenge-lsm-store/sstable"
	"challenge-lsm-store/wal"
	"sync"
)

// MemoryStorage represents data kept only in memory for now but backed-up using WAL
type MemoryStorage struct {
	memory *memtable.Memtable
	wal    *wal.Writer
	buff   *bytes.Buffer
	mu     sync.RWMutex
}

func (s *MemoryStorage) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.memory.Size()
}

func (s *MemoryStorage) Get(key []byte) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.memory.Get(key)
}

// Put loads value into a memory and updates WAL about given change
func (s *MemoryStorage) Put(key []byte, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	defer s.buff.Reset()
	walEntry := wal.EntryV1{
		Key:   key,
		Value: value,
	}
	if err := walEntry.Encode(s.buff); err != nil {
		return err
	}
	if err := s.wal.Write(s.buff.Bytes()); err != nil {
		return err
	}

	// memory
	s.memory.Upsert(key, value)
	return nil
}

// Load loads (imports) value into a memory without keeping WAL about it
func (s *MemoryStorage) Load(key []byte, value []byte) {
	s.memory.Upsert(key, value)
}

func (s *MemoryStorage) Write(writer *sstable.Writer) error {
	s.mu.RLock() // because it only reads from memory
	defer s.mu.RUnlock()

	for e := range s.memory.GetAll() {
		if err := writer.Write(e.GetKey(), e.GetValue()); err != nil {
			return err
		}
	}

	return nil
}

func (s *MemoryStorage) Clear() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.wal.Close(); err != nil {
		return err
	}
	if err := s.wal.Delete(); err != nil {
		return err
	}

	s.memory.Clear()

	return nil
}
