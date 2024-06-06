package wal

import (
	"bytes"
	"encoding/binary"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_WAL_V1_EncodeDecode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		entry        EntryV1
		expEncodeErr error
	}{
		{
			name: "key & non-null value",
			entry: EntryV1{
				Key:   []byte("key1"),
				Value: []byte("value1"),
			},
		},
		{
			name: "key & empty value",
			entry: EntryV1{
				Key:   []byte("key1"),
				Value: []byte(""),
			},
		},
		{
			name:         "invalid entry",
			entry:        EntryV1{},
			expEncodeErr: ErrInvalidEmptyKey,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := bytes.NewBuffer(nil)
			err := tt.entry.Encode(w)
			if tt.expEncodeErr != nil {
				assert.Equal(t, tt.expEncodeErr, err, "unexpected encode error")
				return
			} else {
				require.Nil(t, err, "encode error")
			}

			r := bytes.NewBuffer(w.Bytes())
			e := EntryV1{}
			err = e.Decode(r)
			require.Nil(t, err, "decode error")

			assert.Equal(t, tt.entry, e, "unexpected entry")
		})
	}
}

func Test_WAL_V1_DecodeWrongEntry(t *testing.T) {
	w := bytes.NewBuffer(nil)

	const v2 version = 2
	err := binary.Write(w, binary.LittleEndian, v2)
	require.Nil(t, err, "encode error")

	r := bytes.NewBuffer(w.Bytes())
	e := EntryV1{}
	err = e.Decode(r)
	assert.Equal(t, ErrInvalidVersion, err)
}
