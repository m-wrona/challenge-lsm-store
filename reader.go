package segments_disk_writer

import (
	"io"
)

type Unmarshal func(data []byte, v any) error

type Segments struct {
	Entries [][]Entry `json:"segments"`
}

type Entry struct {
	Term          string `json:"term"`
	DocId         uint64 `json:"doc_id"`
	TermFrequency uint32 `json:"term_frequency"`
}

func Read(in io.Reader, unmarshal Unmarshal) (*Segments, error) {
	bytes, err := io.ReadAll(in)
	if err != nil {
		return nil, err
	}

	var f Segments
	if err := unmarshal(bytes, &f); err != nil {
		return nil, err
	}
	return &f, nil
}
