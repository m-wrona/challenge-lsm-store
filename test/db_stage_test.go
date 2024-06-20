package test

import (
	"challenge-lsm-store/ext"
	"challenge-lsm-store/lsm"
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"math"
	"math/rand/v2"
	"os"
	"sync"
	"testing"
	"time"
)

const (
	defaultMemoryThreshold = math.MaxInt64
)

type DBStage struct {
	t      *testing.T
	random *rand.Rand

	storage *lsm.OSStorageProvider
	store   *lsm.Tree
	tempDir string

	segments  *ext.Segments
	documents []ext.Document
}

func NewDBStage(t *testing.T) *DBStage {
	dirPath, err := os.MkdirTemp(t.TempDir(), "*")
	require.NoError(t, err, "create temp dir")

	return &DBStage{
		t:       t,
		random:  rand.New(rand.NewPCG(64, 1024)),
		tempDir: dirPath,
	}
}

func (s *DBStage) TearDown() {
	// note: temp dir for test will be deleted automatically
}

func (s *DBStage) Given() *DBStage {
	return s
}

func (s *DBStage) When() *DBStage {
	return s
}

func (s *DBStage) Then() *DBStage {
	return s
}

func (s *DBStage) And() *DBStage {
	return s
}

func (s *DBStage) TempDir() string {
	return s.tempDir
}

func (s *DBStage) StoreIsUpAndRunning(cfg lsm.Config) *DBStage {
	storage, err := lsm.NewOSStorageProvider(cfg)
	require.Nil(s.t, err, "OS storage provider create error")

	s.storage = storage
	s.store, err = lsm.New(storage, cfg)
	require.Nil(s.t, err, "LSM store create error")

	return s
}

func (s *DBStage) SegmentsAreLoadedFromReader(in io.Reader) *DBStage {
	s.segments = &ext.Segments{}
	err := ext.Read(in, json.Unmarshal, s.segments)
	require.Nil(s.t, err, "read segments error")
	return s
}

func (s *DBStage) SegmentsArePresent() *DBStage {
	require.NotEmpty(s.t, s.segments.Entries, "segments are present")
	return s
}

func (s *DBStage) SegmentsAreLoadedIntoStore() *DBStage {
	// TODO this should be DB level boot operation really so files will be always there and
	// in the background we could start loading some of the data into memory to warm-up the cache...
	var wg sync.WaitGroup
	start := time.Now()
	s.t.Logf("loading segments...")
	for si, segment := range s.segments.Entries {
		wg.Add(1)

		go func() {
			defer wg.Done()

			memoryStorage, err := s.storage.NewMemoryStorage()
			require.Nil(s.t, err, "memory storage error")

			for _, doc := range segment {
				docBytes, err := json.Marshal(doc)
				require.Nil(s.t, err, "marshal document error")
				memoryStorage.Load(doc.Key(), docBytes)
			}

			if si%2 == 0 {
				// to make only part of data available in memory
				s.store.LoadIntoMemory(memoryStorage)
			}
			err = s.store.WriteToFile(memoryStorage)
			require.Nil(s.t, err, "load into store error")

			s.t.Logf("segment loaded: %d (ouf of %d), documents: %d", si+1, len(s.segments.Entries), len(segment))
		}()
	}
	wg.Wait()
	s.t.Logf("segments loaded in %s", time.Since(start))
	return s
}

func (s *DBStage) RandomDocumentsAreChosenFromSegments(nr int) *DBStage {
	s.documents = make([]ext.Document, 0)
	for range nr {
		sIdx := s.random.IntN(len(s.segments.Entries))
		docIdx := s.random.IntN(len(s.segments.Entries[sIdx]))
		s.documents = append(s.documents, s.segments.Entries[sIdx][docIdx])
	}
	return s
}

func (s *DBStage) DocumentsArePresentInStore() *DBStage {
	assert.NotNil(s.t, s.documents, "documents to check are missing")
	for _, doc := range s.documents {
		v, err := s.store.Get(doc.Key())
		assert.Nil(s.t, err, "get document error")
		assert.NotEmptyf(s.t, v, "document not found in store: %s", doc)
	}
	return s
}
