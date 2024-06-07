package sstable

import (
	"errors"
	"os"
	"path/filepath"
)

const (
	fileWriteFlags = os.O_WRONLY | os.O_CREATE | os.O_APPEND | os.O_TRUNC
	fileReadFlags  = os.O_RDONLY

	fileWriteReadMode = 0o666
	fileReadOnlyMode  = 0o444

	dataFileName        = "data.db"
	indexFileName       = "index.db"
	sparseIndexFileName = "sparse.db"
)

var ErrFileNotDirectory = errors.New("file is not a directory")

func NewFileWriter(dirPath string) (*Writer, error) {
	dir, err := os.OpenFile(dirPath, fileReadFlags, fileReadOnlyMode)
	if err != nil {
		return nil, err
	}
	stat, err := dir.Stat()
	if err != nil {
		return nil, err
	}
	if !stat.IsDir() {
		return nil, ErrFileNotDirectory
	}

	data, index, sparse, err := openDBFiles(dirPath, fileWriteFlags, fileWriteReadMode)
	if err != nil {
		return nil, err
	}

	return NewWriter(data, index, sparse), nil
}

func NewFileReader(dirPath string) (*Reader, error) {
	dir, err := os.OpenFile(dirPath, fileReadFlags, fileReadOnlyMode)
	if err != nil {
		return nil, err
	}
	stat, err := dir.Stat()
	if err != nil {
		return nil, err
	}
	if !stat.IsDir() {
		return nil, ErrFileNotDirectory
	}

	data, index, sparse, err := openDBFiles(dirPath, fileReadFlags, fileReadOnlyMode)
	if err != nil {
		return nil, err
	}

	return NewReader(data, index, sparse), nil
}

func openDBFiles(dirPath string, flags int, mode os.FileMode) (data *os.File, index *os.File, sparse *os.File, err error) {
	data, err = os.OpenFile(filepath.Join(dirPath, dataFileName), flags, mode)
	if err != nil {
		return
	}

	index, err = os.OpenFile(filepath.Join(dirPath, indexFileName), flags, mode)
	if err != nil {
		return
	}

	sparse, err = os.OpenFile(filepath.Join(dirPath, sparseIndexFileName), flags, mode)
	if err != nil {
		return
	}

	return
}
