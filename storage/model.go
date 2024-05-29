package storage

import (
	"io"
	"segments-disk-writer/ext"
)

// Entry represents single block of data that can be stored.
// TODO clarify how to manage different versions (i.e if new fields should be added or some removed etc.)
// I guess the simplest approach is to make this Entry "append-only" with deprecated fields which
// could be removed after lazy migration (compaction) after some time.
// We could also try EntryV1, EntryV2 approach but I guess it would require some interface which would
// make usage and code maintenance even harder.
type Entry struct {
	Version uint8
	Bytes   []byte
}

func WriteEntry(w io.Writer, e Entry) (int, error) {
	return ext.WriteChunk(w, e.Version, e.Bytes)
}

func ReadEntry(r io.Reader, e *Entry) (int, error) {
	return ext.ReadChunk(r, e.Version, e.Bytes)
}
