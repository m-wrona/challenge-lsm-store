package segments_disk_writer

type (
	Key   []byte
	Value []byte
)

type Segments struct {
	Entries [][]Document `json:"segments"`
}
