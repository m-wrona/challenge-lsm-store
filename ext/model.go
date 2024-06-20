package ext

import (
	"encoding/binary"
)

type (
	DocumentID = uint64
	Document   struct {
		Term          string     `json:"term"`
		DocId         DocumentID `json:"doc_id"`
		TermFrequency uint32     `json:"term_frequency"`
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
