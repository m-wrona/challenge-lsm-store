package segments_disk_writer

import (
	"bytes"
	"github.com/google/btree"
)

const btreeDegree = 3

type (
	Key   []byte
	Value []byte

	memtableEntry struct {
		key   Key
		value Value
	}

	Memtable struct {
		// TODO check other implementations with nicer generic types which will not require
		// additional entry struct. Or just implement it.
		// From checked libs this is officially supported.
		// Note: skip list could be used here as well
		tree *btree.BTreeG[memtableEntry]
		size int
	}
)

func NewMemtable() *Memtable {
	return &Memtable{
		tree: btree.NewG[memtableEntry](btreeDegree, func(a, b memtableEntry) bool {
			return bytes.Compare(a.key, b.key) == -1
		}),
	}
}

func (m *Memtable) Upsert(key Key, value Value) bool {
	_, isNew := m.tree.ReplaceOrInsert(memtableEntry{key, value})
	m.size += len(key) + len(value)
	return isNew
}

func (m *Memtable) Get(key Key) (Value, bool) {
	v, found := m.tree.Get(memtableEntry{key, nil})
	if found {
		return v.value, true
	}
	return nil, false
}

func (m *Memtable) Delete(key Key) (Value, bool) {
	v, found := m.tree.Delete(memtableEntry{key, nil})
	if found {
		return v.value, true
	}
	return nil, false
}
