package memtable

import (
	"bytes"
	"github.com/google/btree"
)

const btreeDegree = 3

type (
	Entry struct {
		key   []byte
		value []byte
	}

	//Memtable implements basic memory structure for keeping key-value pairs.
	//Memtable is not thread-safe.
	Memtable struct {
		// TODO check other implementations with nicer generic types which will not require
		// additional entry struct. Or just implement it.
		// From checked libs this is officially supported and seems most pro/official one.
		// Note: skip list could be used here as well
		tree *btree.BTreeG[Entry]
		size int
	}
)

func NewMemtable() *Memtable {
	return &Memtable{
		tree: btree.NewG[Entry](btreeDegree, func(a, b Entry) bool {
			return bytes.Compare(a.key, b.key) == -1
		}),
	}
}

func (m *Memtable) Upsert(key, value []byte) bool {
	_, isNew := m.tree.ReplaceOrInsert(Entry{key, value})
	m.size += len(key) + len(value)
	return isNew
}

func (m *Memtable) Get(key []byte) ([]byte, bool) {
	v, found := m.tree.Get(Entry{key, nil})
	if found {
		return v.value, true
	}
	return nil, false
}

func (m *Memtable) Delete(key []byte) ([]byte, bool) {
	v, found := m.tree.Delete(Entry{key, nil})
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

func (m *Memtable) GetAll() <-chan Entry {
	// Note: sage of channel and routine is a drawback and consequence of used BTree implementation.
	// TODO Simplify it by using other implementation or structure that could introduce here iterator approach
	c := make(chan Entry)

	go func() {
		defer close(c)

		m.tree.Ascend(func(item Entry) bool {
			c <- item
			return true
		})
	}()

	return c
}

func (e *Entry) GetKey() []byte {
	return e.key
}

func (e *Entry) GetValue() []byte {
	return e.value
}
