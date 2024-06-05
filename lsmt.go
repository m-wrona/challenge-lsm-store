package segments_disk_writer

import "segments-disk-writer/memtable"

type LSMTSettings struct {
	MemoryThreshold int
}

type logger interface {
	Append(key Key, value Value) error
	Tombstone(key Key) error
}

type LSMT struct {
	memory   *memtable.Memtable
	flush    *memtable.Memtable
	wal      logger
	settings LSMTSettings
}

func NewLSMT(wal logger, settings LSMTSettings) *LSMT {
	return &LSMT{
		wal: wal,
	}
}

func (t *LSMT) Put(key Key, value Value) error {
	if err := t.wal.Append(key, value); err != nil {
		return err
	}

	t.memory.Upsert(key, value)
	if t.memory.GetSize() > t.settings.MemoryThreshold {
		//TODO flush
	}

	return nil
}

func (t *LSMT) Get(key Key) (Value, error) {
	value, found := t.memory.Get(key)
	if found {
		return value, nil
	}

	return nil, nil
}
