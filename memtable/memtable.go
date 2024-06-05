package memtable

import (
	"bytes"
	"github.com/google/btree"
	"segments-disk-writer"
)

const btreeDegree = 3

type (
	memtableEntry struct {
		key   segments_disk_writer.Key
		value segments_disk_writer.Value
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

func (m *Memtable) Upsert(key segments_disk_writer.Key, value segments_disk_writer.Value) bool {
	_, isNew := m.tree.ReplaceOrInsert(memtableEntry{key, value})
	m.size += len(key) + len(value)
	return isNew
}

func (m *Memtable) Get(key segments_disk_writer.Key) (segments_disk_writer.Value, bool) {
	v, found := m.tree.Get(memtableEntry{key, nil})
	if found {
		return v.value, true
	}
	return nil, false
}

func (m *Memtable) Delete(key segments_disk_writer.Key) (segments_disk_writer.Value, bool) {
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
