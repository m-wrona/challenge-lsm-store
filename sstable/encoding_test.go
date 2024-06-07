package sstable

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_SSTable_EncodeDecode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		key   []byte
		value []byte
	}{
		{
			name:  "non-empty value",
			key:   []byte("key"),
			value: []byte("value"),
		},
		{
			name:  "empty value",
			key:   []byte("key"),
			value: []byte(""),
		},
		{
			name:  "nil value",
			key:   []byte("key"),
			value: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := bytes.NewBuffer(nil)
			_, err := encode(w, tt.key, tt.value)
			require.Nil(t, err, "encode error")

			r := bytes.NewReader(w.Bytes())
			key, value, err := decode(r)
			require.Nil(t, err, "decode error")
			assert.Equal(t, tt.key, key, "unexpected key")
			if len(tt.value) == 0 {
				assert.Empty(t, value, "unexpected value")
			} else {
				assert.Equal(t, tt.value, value, "unexpected value")
			}
		})
	}
}

func Test_SSTable_EncodeDecodeOffset(t *testing.T) {
	tests := []struct {
		name   string
		key    []byte
		offset int
	}{
		{
			name:   "non-empty value",
			key:    []byte("key"),
			offset: 123,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := bytes.NewBuffer(nil)
			_, err := encodeKeyOffset(w, tt.key, tt.offset)
			require.Nil(t, err, "encode error")

			r := bytes.NewReader(w.Bytes())
			key, offset, err := decodeKeyOffset(r)
			require.Nil(t, err, "decode error")
			assert.Equal(t, tt.key, key, "unexpected key")
			assert.Equal(t, tt.offset, offset, "unexpected offset")
		})
	}
}
