package ext

import (
	"encoding/json"
	"errors"
	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	"io"
	"strings"
	"testing"
)

type segments struct {
	Entries [][]entry `json:"segments"`
}

type entry struct {
	Term          string `json:"term"`
	DocId         uint64 `json:"doc_id"`
	TermFrequency uint32 `json:"term_frequency"`
}

func Test_JSON_Read(t *testing.T) {
	t.Parallel()

	const testJson = `
{
    "segments": [
        [
            {
                "term": "great",
                "doc_id": 1,
                "term_frequency": 11
            },
            {
                "term": "poor",
                "doc_id": 2,
                "term_frequency": 22
            }
		]
	]
}
`

	var expJsonSegments = segments{
		Entries: [][]entry{
			{
				{
					Term:          "great",
					TermFrequency: 11,
					DocId:         1,
				},
				{
					Term:          "poor",
					TermFrequency: 22,
					DocId:         2,
				},
			},
		},
	}

	tests := []struct {
		name      string
		in        io.Reader
		unmarshal Unmarshal
		want      segments
		wantErr   error
	}{
		{
			name:      "empty file",
			in:        strings.NewReader(""),
			unmarshal: json.Unmarshal,
			wantErr:   errors.New("unexpected end of JSON input"),
		},
		{
			name:      "empty doc",
			in:        strings.NewReader("{}"),
			unmarshal: json.Unmarshal,
		},
		{
			name:      "use standard json",
			in:        strings.NewReader(testJson),
			unmarshal: json.Unmarshal,
			want:      expJsonSegments,
		},
		// just for fun checking performance of other libs against bigger files
		{
			name:      "use jsoniter",
			in:        strings.NewReader(testJson),
			unmarshal: jsoniter.ConfigCompatibleWithStandardLibrary.Unmarshal,
			want:      expJsonSegments,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := &segments{}
			err := Read(tt.in, tt.unmarshal, &got)
			if tt.wantErr != nil {
				assert.NotNilf(t, err, "must return error: %w", tt.wantErr)
				if err != nil {
					assert.Equalf(t, err.Error(), tt.wantErr.Error(), "unexpected error: %w", err)
				}
			} else {
				assert.Nilf(t, err, "mustn't return error, got: %w", err)
				if err == nil {
					assert.Equal(t, tt.want, *got, "unexpected file read")
				}
			}
		})
	}
}
