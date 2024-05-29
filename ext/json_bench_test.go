package ext

import (
	"bytes"
	"encoding/json"
	"errors"
	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/require"
	"io"
	"os"
	"testing"
)

func Benchmark_JSON_Read(b *testing.B) {
	// TODO add overriding by env vars
	const jsonFilePath = "../segments.json"

	if _, err := os.Stat(jsonFilePath); errors.Is(err, os.ErrNotExist) {
		b.Skipf("skipping benchmark because segments.json does not exist: %s", err)
	}

	f, err := os.Open(jsonFilePath)
	require.Nilf(b, err, "json file not found: %s", jsonFilePath)
	fileBytes, err := io.ReadAll(f)
	require.Nilf(b, err, "json read error: %w", err)

	b.Run("standard json", func(b *testing.B) {
		s := &segments{}
		for i := 0; i < b.N; i++ {
			r := bytes.NewReader(fileBytes)
			_ = Read(r, json.Unmarshal, s)
		}
	})

	b.Run("jsoniter", func(b *testing.B) {
		s := &segments{}
		for i := 0; i < b.N; i++ {
			r := bytes.NewReader(fileBytes)
			_ = Read(r, jsoniter.ConfigCompatibleWithStandardLibrary.Unmarshal, s)
		}
	})

}
