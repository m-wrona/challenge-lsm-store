package lsm

import (
	"bytes"
	"challenge-lsm-store/memtable"
	"challenge-lsm-store/sstable"
	"challenge-lsm-store/wal"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	walDir    = "wal"
	tablesDir = "tables"

	dirPerm = 0755
)

type OSStorageProvider struct {
	cfg     Config
	buff    *bytes.Buffer
	counter atomic.Uint32

	// TODO tables dir kept created directories for tables.
	// in the future it may be cache or should be removed since it's workaround for now */
	tablesDir []string
	mu        sync.RWMutex //needed for now for tables dir
}

func NewOSStorageProvider(cfg Config) (*OSStorageProvider, error) {
	for _, subDir := range []string{walDir, tablesDir} {
		if err := os.MkdirAll(fmt.Sprintf("%s/%s", cfg.Dir, subDir), dirPerm); err != nil {
			return nil, err
		}
	}

	return &OSStorageProvider{
		cfg:  cfg,
		buff: bytes.NewBuffer(nil),
	}, nil
}

func (s *OSStorageProvider) NewMemoryStorage() (*MemoryStorage, error) {
	writer, err := wal.NewFileWriter(
		fmt.Sprintf("%s/%s/%d-%d.wal", s.cfg.Dir, walDir, s.counter.Add(1), time.Now().Unix()),
	)
	if err != nil {
		return nil, err
	}
	return &MemoryStorage{
		memory: memtable.NewMemtable(),
		wal:    writer,
		buff:   s.buff,
	}, nil
}

func (s *OSStorageProvider) NewSSTableWriter() (*sstable.Writer, error) {
	dir := fmt.Sprintf("%s/%s/%d-%d", s.cfg.Dir, tablesDir, s.counter.Add(1), time.Now().Unix())
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return nil, err
	}

	s.mu.Lock()
	s.tablesDir = append(s.tablesDir, dir)
	s.mu.Unlock()

	return sstable.NewFileWriter(dir)
}

func (s *OSStorageProvider) FilesStorage() ([]*fileStorage, error) {
	// TODO check OS dir to get tables dirs
	s.mu.RLock()
	defer s.mu.RUnlock()
	files := make([]*fileStorage, 0)

	for _, dir := range s.tablesDir {
		reader, err := sstable.NewFileReader(dir)
		if err != nil {
			return nil, err
		}
		files = append(files, &fileStorage{
			reader: reader,
		})
	}

	return files, nil
}
