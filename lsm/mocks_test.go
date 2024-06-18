package lsm

import (
	"bytes"
	"challenge-lsm-store/memtable"
	"challenge-lsm-store/sstable"
	"challenge-lsm-store/wal"
)

type closeableBuffer struct {
	buff     *bytes.Buffer
	writeErr error
}

type mockStorageProvider struct {
	memoryStorageErr error

	walBuffers   []*closeableBuffer
	memoryTables []*memtable.Memtable

	tableWriters   []*closeableBuffer
	tableWriterErr error

	files    []*fileStorage
	filesErr error
}

func (b *closeableBuffer) Write(p []byte) (n int, err error) {
	if b.writeErr != nil {
		return 0, b.writeErr
	}
	return b.buff.Write(p)
}

func (b *closeableBuffer) Read(p []byte) (n int, err error) {
	return b.buff.Read(p)
}

func (b *closeableBuffer) Close() error {
	return nil
}

func (b *closeableBuffer) Bytes() []byte {
	return b.buff.Bytes()
}

func fnStub() error {
	return nil
}

func (m *mockStorageProvider) NewMemoryStorage() (*memoryStorage, error) {
	if m.memoryStorageErr != nil {
		return nil, m.memoryStorageErr
	}

	buff := &closeableBuffer{buff: bytes.NewBuffer(nil)}
	m.walBuffers = append(m.walBuffers, buff)
	table := memtable.NewMemtable()
	m.memoryTables = append(m.memoryTables, table)

	return &memoryStorage{
		memory: table,
		wal:    wal.NewWriter(buff, fnStub, fnStub),
		buff:   bytes.NewBuffer(nil),
	}, nil
}

func (m *mockStorageProvider) NewSSTableWriter() (*sstable.Writer, error) {
	if m.tableWriterErr != nil {
		return nil, m.tableWriterErr
	}

	buff := &closeableBuffer{buff: bytes.NewBuffer(nil)}
	m.tableWriters = append(m.tableWriters, buff)

	return sstable.NewWriter(
		buff,
		&closeableBuffer{buff: bytes.NewBuffer(nil)},
		&closeableBuffer{buff: bytes.NewBuffer(nil)},
	), nil
}

func (m *mockStorageProvider) FilesStorage() ([]*fileStorage, error) {
	return m.files, m.filesErr
}
