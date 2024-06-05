package memtable

import (
	"github.com/stretchr/testify/assert"
	"segments-disk-writer"
	"testing"
)

func TestMemtable_UpsertAndGet(t *testing.T) {
	type entry struct {
		key   segments_disk_writer.Key
		value segments_disk_writer.Value
	}
	tests := []struct {
		name    string
		upserts []entry
		delete  []segments_disk_writer.Key
		expGet  []entry
	}{
		{
			name: "insert key value",
			upserts: []entry{
				{
					key:   []byte("key1"),
					value: []byte("value1"),
				},
				{
					key:   []byte("key2"),
					value: []byte("value2"),
				},
			},
			expGet: []entry{
				{
					key:   []byte("key1"),
					value: []byte("value1"),
				},
				{
					key:   []byte("key2"),
					value: []byte("value2"),
				},
				{
					key:   []byte("key3"),
					value: nil,
				},
			},
		},
		{
			name: "update key value",
			upserts: []entry{
				{
					key:   []byte("key1"),
					value: []byte("value1"),
				},
				{
					key:   []byte("key2"),
					value: []byte("value2"),
				},
				{
					key:   []byte("key1"),
					value: []byte("value11"),
				},
			},
			expGet: []entry{
				{
					key:   []byte("key1"),
					value: []byte("value11"),
				},
				{
					key:   []byte("key2"),
					value: []byte("value2"),
				},
				{
					key:   []byte("key3"),
					value: nil,
				},
			},
		},
		{
			name: "delete key value",
			upserts: []entry{
				{
					key:   []byte("key1"),
					value: []byte("value1"),
				},
				{
					key:   []byte("key2"),
					value: []byte("value2"),
				},
			},
			delete: []segments_disk_writer.Key{
				[]byte("key2"),
			},
			expGet: []entry{
				{
					key:   []byte("key1"),
					value: []byte("value1"),
				},
				{
					key:   []byte("key2"),
					value: nil,
				},
				{
					key:   []byte("key3"),
					value: nil,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewMemtable()
			for _, entry := range tt.upserts {
				m.Upsert(entry.key, entry.value)
			}

			for _, key := range tt.delete {
				m.Delete(key)
			}

			for idx, expGet := range tt.expGet {
				value, found := m.Get(expGet.key)
				assert.Equalf(t, tt.expGet[idx].value != nil, found, "key not found at index: %d", idx)
				assert.Equalf(t, tt.expGet[idx].value, value, "unexpected value at index: %d", idx)
			}
		})
	}
}
