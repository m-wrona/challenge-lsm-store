package ext

import "segments-disk-writer"

const (
	version uint8 = 1
)

type Segments struct {
	Entries [][]segments_disk_writer.Document `json:"segments"`
}
