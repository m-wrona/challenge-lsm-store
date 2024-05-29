package segments_disk_writer

type (
	DocumentID = uint64
	Document   struct {
		Term          string     `json:"term"`
		DocId         DocumentID `json:"doc_id"`
		TermFrequency uint32     `json:"term_frequency"`
	}
)
