package lsm

import (
	"bytes"
	"challenge-lsm-store/memtable"
	"challenge-lsm-store/wal"
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_LSM_MemoryStorage_Put(t *testing.T) {
	type pair struct {
		key   []byte
		value []byte
	}
	tests := []struct {
		name   string
		buff   *closeableBuffer
		input  pair
		expErr error
	}{
		{
			name: "should write to memory and WAL",
			buff: &closeableBuffer{buff: bytes.NewBuffer(nil)},
			input: pair{
				key:   []byte("key1"),
				value: []byte("value1"),
			},
		},
		{
			name: "should not write to memory if WAL write fails",
			buff: &closeableBuffer{
				buff:     bytes.NewBuffer(nil),
				writeErr: errors.New("WAL write error"),
			},
			input: pair{
				key:   []byte("key1"),
				value: []byte("value1"),
			},
			expErr: errors.New("WAL write error"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writer := wal.NewWriter(tt.buff, fnStub, fnStub)
			s := &memoryStorage{
				memory: memtable.NewMemtable(),
				wal:    writer,
				buff:   bytes.NewBuffer(nil),
			}

			err := s.Put(tt.input.key, tt.input.value)
			if tt.expErr != nil {
				assert.Equal(t, tt.expErr, err, "unexpected put error")
			} else {
				require.NoError(t, err, "put error")

				// check WAL
				r := wal.NewReader(tt.buff)
				d, err := r.Read()
				require.Nil(t, err, "read error")
				e := wal.EntryV1{}
				err = e.Decode(bytes.NewBuffer(d))
				require.Nil(t, err, "wal decode error")

				assert.Equal(t, tt.input.key, e.Key, "unexpected WAL key")
				assert.Equal(t, tt.input.value, e.Value, "unexpected WAL value")
			}

			// check memory
			value, ok := s.memory.Get(tt.input.key)
			if tt.expErr == nil {
				assert.True(t, ok, "no entry found in memory")
				assert.Equal(t, tt.input.value, value, "unexpected memory value")
			} else {
				assert.False(t, ok, "no entry must be found")
				assert.Nil(t, value, "no memory value must be found")

			}
		})
	}
}

func Test_LSM_MemoryStorage_Write(t *testing.T) {
	t.Skip("check if memory can be dumped into a file")
}

func Test_LSM_MemoryStorage_Clear(t *testing.T) {
	t.Skip("check if WAL is deleted once memory is cleared")
}
