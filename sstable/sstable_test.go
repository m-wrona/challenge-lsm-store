package sstable_test

import (
	"challenge-lsm-store/sstable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

type pair struct {
	Key   []byte
	Value []byte
}

type result struct {
	Key   []byte
	Value []byte
	Found bool
}

func Test_SSTable_WriteRead(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   []pair
		exp  []result
	}{
		{
			name: "find existing key",
			in: []pair{
				{Key: []byte("key1"), Value: []byte("value1")},
				{Key: []byte("key2"), Value: []byte("value2")},
			},
			exp: []result{
				{Key: []byte("key1"), Value: []byte("value1"), Found: true},
			},
		},
		{
			name: "find last key",
			in: []pair{
				{Key: []byte("key1"), Value: []byte("value1")},
				{Key: []byte("key2"), Value: []byte("value2")},
			},
			exp: []result{
				{Key: []byte("key2"), Value: []byte("value2"), Found: true},
			},
		},
		{
			name: "find non-existing key",
			in: []pair{
				{Key: []byte("key1"), Value: []byte("value1")},
				{Key: []byte("key2"), Value: []byte("value2")},
			},
			exp: []result{
				{Key: []byte("xxx"), Value: []byte("yyy"), Found: false},
			},
		},
		{
			name: "mix search order in bigger batch",
			in: []pair{
				{Key: []byte("key1"), Value: []byte("value1")},
				{Key: []byte("key10"), Value: []byte("value10")},
				{Key: []byte("key2"), Value: []byte("value2")},
				{Key: []byte("key20"), Value: []byte("value20")},
				{Key: []byte("key3"), Value: []byte("value3")},
				{Key: []byte("key4"), Value: []byte("value4")},
				{Key: []byte("key5"), Value: []byte("value5")},
				{Key: []byte("key6"), Value: []byte("value6")},
				{Key: []byte("key7"), Value: []byte("value7")},
				{Key: []byte("key8"), Value: []byte("value8")},
				{Key: []byte("key9"), Value: []byte("value9")},
			},
			exp: []result{
				{Key: []byte("key1"), Value: []byte("value1"), Found: true},
				{Key: []byte("key9"), Value: []byte("value9"), Found: true},
				{Key: []byte("key10"), Value: []byte("value10"), Found: true},
				{Key: []byte("xxx1"), Value: []byte("yyy1"), Found: false},
				{Key: []byte("key20"), Value: []byte("value20"), Found: true},
				{Key: []byte("key3"), Value: []byte("value3"), Found: true},
				{Key: []byte("xxx2"), Value: []byte("yyy2"), Found: false},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dirPath, err := os.MkdirTemp(t.TempDir(), "*")
			require.NoError(t, err, "create temp dir")

			writer, err := sstable.NewFileWriter(dirPath)
			require.NoError(t, err, "could not create file writer")
			defer writer.Close()

			for _, pair := range tt.in {
				err := writer.Write(pair.Key, pair.Value)
				require.NoError(t, err, "could not write to file")
			}

			reader, err := sstable.NewFileReader(dirPath)
			require.NoError(t, err, "could not create file reader")
			defer reader.Close()

			for _, result := range tt.exp {
				v, ok, err := reader.Find(result.Key)
				require.NoError(t, err, "could not read from file")
				assert.Equalf(t, result.Found, ok, "unexpected found key: %s", result.Key)
				if result.Found {
					assert.Equalf(t, result.Value, v, "value not equal for key: %s", result.Key)
				} else {
					assert.Nilf(t, v, "value must be nil for key: %s", result.Key)
				}
			}
		})
	}
}
