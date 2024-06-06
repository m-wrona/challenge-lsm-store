package memtable

import (
	"bytes"
	"github.com/google/btree"
)

const btreeDegree = 3

type (
	memtableEntry struct {
		key   []byte
		value []byte
	}

	Memtable struct {
		// TODO check other implementations with nicer generic types which will not require
		// additional entry struct. Or just implement it.
		// From checked libs this is officially supported and seems most pro/official one.
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

func (m *Memtable) Upsert(key, value []byte) bool {
	_, isNew := m.tree.ReplaceOrInsert(memtableEntry{key, value})
	m.size += len(key) + len(value)
	return isNew
}

func (m *Memtable) Get(key []byte) ([]byte, bool) {
	v, found := m.tree.Get(memtableEntry{key, nil})
	if found {
		return v.value, true
	}
	return nil, false
}

func (m *Memtable) Delete(key []byte) ([]byte, bool) {
	v, found := m.tree.Delete(memtableEntry{key, nil})
	if found {
		return v.value, true
	}
	return nil, false
}

func (m *Memtable) GetSize() int {
	return m.size
}

func (m *Memtable) Clear() {
	m.tree.Clear(true)
	m.size = 0
}
