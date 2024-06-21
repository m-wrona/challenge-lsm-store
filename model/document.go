package model

import (
	"encoding/binary"
)

type (
	DocumentID   = uint64
	DocumentFreq = uint32
	Term         = string

	Document struct {
		Term          Term         `json:"term"`
		DocId         DocumentID   `json:"doc_id"`
		TermFrequency DocumentFreq `json:"term_frequency"`
	}
	Segments struct {
		Entries [][]Document `json:"segments"`
	}
)

func (d *Document) Key() []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, d.DocId)
	return b
}
