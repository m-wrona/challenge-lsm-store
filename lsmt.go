package segments_disk_writer

import (
	"io"
	"segments-disk-writer/memtable"
)

type LSMTSettings struct {
	MemoryThreshold int
}

type LSMT struct {
	memory   *memtable.Memtable
	flush    *memtable.Memtable
	wal      io.Writer
	settings LSMTSettings
}

func NewLSMT(wal io.Writer, settings LSMTSettings) *LSMT {
	return &LSMT{
		wal: wal,
	}
}

func (t *LSMT) Put(key []byte, value []byte) error {
	//if err := t.wal.Append(wal.Entry{
	//	SystemVersion: version,
	//}); err != nil {
	//	return err
	//}

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
