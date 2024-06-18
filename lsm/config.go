package lsm

type Config struct {
	MemoryThreshold   int
	Dir               string
	SparseKeyDistance int // TODO pass to SSTable writer (for now hardcoded)
}
