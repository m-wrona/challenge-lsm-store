package wal

import "os"

const (
	fileWriteFlags = os.O_WRONLY | os.O_CREATE | os.O_APPEND
	fileReadFlags  = os.O_RDONLY

	fileWriteOnlyMode = 0o222
	fileReadOnlyMode  = 0o444

	FileExtension = "wal"
)

func NewFileReader(path string) (*Reader, error) {
	file, err := os.OpenFile(path, fileReadFlags, fileReadOnlyMode)
	if err != nil {
		return nil, err
	}
	return NewReader(file), nil
}

func NewFileWriter(path string) (*Writer, error) {
	file, err := os.OpenFile(path, fileWriteFlags, fileWriteOnlyMode)
	if err != nil {
		return nil, err
	}
	return NewWriter(file, file.Sync), nil
}
