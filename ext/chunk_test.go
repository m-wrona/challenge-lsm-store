package ext

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_IO_ChunkWriteRead(t *testing.T) {
	t.Parallel()

	type entry struct {
		Version  uint8
		Type     int
		SomeFlag bool
		//Bytes    []byte
	}

	tests := []struct {
		name  string
		entry entry
	}{
		{
			"entry - 0",
			entry{
				Version:  0,
				Type:     1,
				SomeFlag: true,
				//Bytes:    []byte("some value"),
			},
		},
		{
			"entry - 0",
			entry{
				Version:  1,
				Type:     2,
				SomeFlag: false,
				//Bytes:    []byte("some other value"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &bytes.Buffer{}

			sizeOut, err := WriteChunk(w, tt.entry.Version, tt.entry.Type, tt.entry.SomeFlag)
			require.Nil(t, err, "write failed")
			assert.NotEqual(t, 0, sizeOut)

			out := entry{}
			_, err = ReadChunk(bytes.NewReader(w.Bytes()), out.Version, out.Type, out.SomeFlag)
			require.Nil(t, err, "read failed")

			assert.Equal(t, tt.entry, out, "unexpected entry")
		})
	}
}
