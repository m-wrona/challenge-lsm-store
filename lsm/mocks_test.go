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

// TODO simplify mock to make it more handy during test case preparation phase
type mockStorageProvider struct {
	memoryStorageErr error

	walBuffers   []*closeableBuffer
	memoryTables []*memtable.Memtable

	tableDataWriters        []*closeableBuffer
	tableIndexWriters       []*closeableBuffer
	tableSparseIndexWriters []*closeableBuffer
	tableWriterErr          error

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

func (b *closeableBuffer) Seek(offset int64, whence int) (int64, error) {
	return bytes.NewReader(b.Bytes()).Seek(offset, whence)
}

func fnStub() error {
	return nil
}

func (m *mockStorageProvider) NewMemoryStorage() (*MemoryStorage, error) {
	if m.memoryStorageErr != nil {
		return nil, m.memoryStorageErr
	}

	buff := &closeableBuffer{buff: bytes.NewBuffer(nil)}
	m.walBuffers = append(m.walBuffers, buff)
	table := memtable.NewMemtable()
	m.memoryTables = append(m.memoryTables, table)

	return &MemoryStorage{
		memory: table,
		wal:    wal.NewWriter(buff, fnStub, fnStub),
		buff:   bytes.NewBuffer(nil),
	}, nil
}

func (m *mockStorageProvider) NewSSTableWriter() (*sstable.Writer, error) {
	if m.tableWriterErr != nil {
		return nil, m.tableWriterErr
	}

	dataBuff := &closeableBuffer{buff: bytes.NewBuffer(nil)}
	m.tableDataWriters = append(m.tableDataWriters, dataBuff)

	indexBuff := &closeableBuffer{buff: bytes.NewBuffer(nil)}
	m.tableIndexWriters = append(m.tableIndexWriters, indexBuff)

	sparseIndexBuff := &closeableBuffer{buff: bytes.NewBuffer(nil)}
	m.tableSparseIndexWriters = append(m.tableSparseIndexWriters, sparseIndexBuff)

	return sstable.NewWriter(
		dataBuff,
		indexBuff,
		sparseIndexBuff,
	), nil
}

func (m *mockStorageProvider) FilesStorage() ([]*fileStorage, error) {
	return m.files, m.filesErr
}

func (m *mockStorageProvider) MoveSSTablesToFiles() {
	for idx, _ := range m.tableDataWriters {
		f := &fileStorage{
			reader: sstable.NewReader(
				m.tableDataWriters[idx],
				m.tableIndexWriters[idx],
				m.tableSparseIndexWriters[idx],
			),
		}
		m.files = append(m.files, f)
	}

	m.tableDataWriters = nil
	m.tableIndexWriters = nil
	m.tableSparseIndexWriters = nil
}
