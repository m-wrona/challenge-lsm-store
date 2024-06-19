package lsm

import (
	"bytes"
	"challenge-lsm-store/memtable"
	"challenge-lsm-store/sstable"
	"challenge-lsm-store/wal"
	"fmt"
	"os"
	"time"
)

const (
	walDir    = "wal"
	tablesDir = "tables"

	dirPerm = 0755
)

type osStorageProvider struct {
	cfg  Config
	buff *bytes.Buffer

	// TODO tables dir kept created directories for tables.
	// in the future it may be cache or should be removed since it's workaround for now */
	tablesDir []string
}

func NewOSStorageProvider(cfg Config) (*osStorageProvider, error) {
	for _, subDir := range []string{walDir, tablesDir} {
		if err := os.MkdirAll(fmt.Sprintf("%s/%s", cfg.Dir, subDir), dirPerm); err != nil {
			return nil, err
		}
	}

	return &osStorageProvider{
		cfg:  cfg,
		buff: bytes.NewBuffer(nil),
	}, nil
}

func (s *osStorageProvider) NewMemoryStorage() (*memoryStorage, error) {
	writer, err := wal.NewFileWriter(
		fmt.Sprintf("%s/%s/%d.wal", s.cfg.Dir, walDir, time.Now().Unix()),
	)
	if err != nil {
		return nil, err
	}
	return &memoryStorage{
		memory: memtable.NewMemtable(),
		wal:    writer,
		buff:   s.buff,
	}, nil
}

func (s *osStorageProvider) NewSSTableWriter() (*sstable.Writer, error) {
	dir := fmt.Sprintf("%s/%s/%d", s.cfg.Dir, tablesDir, time.Now().Unix())
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return nil, err
	}
	s.tablesDir = append(s.tablesDir, dir)
	return sstable.NewFileWriter(dir)
}

func (s *osStorageProvider) FilesStorage() ([]*fileStorage, error) {
	// TODO check OS dir to get tables dirs
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
