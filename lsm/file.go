package lsm

import (
	"challenge-lsm-store/sstable"
	"sync"
)

// fileStorage represents data kept in a single file
type fileStorage struct {
	reader *sstable.Reader
	mu     sync.Mutex
}

func (s *fileStorage) Find(key []byte) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// TODO some common cross-file cache could appear here
	return s.reader.Find(key)
}
