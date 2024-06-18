package wal_test

import (
	"bytes"
	"challenge-lsm-store/wal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"testing"
)

type testWriter struct {
	buff *bytes.Buffer
	sync int
}

type testRead struct {
	reader io.Reader
}

func Test_WAL_WriteRead(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content [][]byte
	}{
		{
			name: "single entry",
			content: [][]byte{
				[]byte("hello world"),
			},
		},
		{
			name: "many entries",
			content: [][]byte{
				[]byte("line 1"),
				[]byte("line 2"),
				[]byte("line 3"),
				[]byte(""),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// write
			writer := testWriter{
				buff: bytes.NewBuffer(nil),
			}
			w := wal.NewWriter(&writer, writer.Sync, nil)
			for _, content := range tt.content {
				err := w.Write(content)
				require.Nil(t, err, "write error")
			}
			require.Nil(t, w.Close(), "write close error")
			assert.Equal(t, len(tt.content), writer.sync, "unexpected sync")

			// read
			reader := testRead{
				reader: bytes.NewReader(writer.buff.Bytes()),
			}
			r := wal.NewReader(&reader)
			data := make([][]byte, 0)
			for {
				d, err := r.Read()
				if err == io.EOF {
					break
				}
				require.Nil(t, err, "read error")

				data = append(data, d)
			}

			// verify read content against original one
			require.Equal(t, len(tt.content), len(data), "content read differs from expected")
			for idx, content := range tt.content {
				assert.Equalf(t, content, data[idx], "read row differs from expected at: %d", idx)
			}
		})
	}
}

func (m *testWriter) Write(p []byte) (n int, err error) {
	return m.buff.Write(p)
}

func (m *testWriter) Close() error {
	return nil
}

func (m *testWriter) Sync() error {
	m.sync += 1
	return nil
}

func (m *testRead) Read(p []byte) (n int, err error) {
	return m.reader.Read(p)
}

func (m *testRead) Close() error {
	return nil
}
