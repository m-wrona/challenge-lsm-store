package test

import (
	"challenge-lsm-store/lsm"
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"sync"
	"testing"
	"time"
)

const (
	// store related settings
	inMemoryThreshold   = 1000000 //bigger threshold to make sure no file dump happens quickly
	fileMemoryThreshold = 1       //min threshold to make sure everything is dumped into a file at once

	// present in lsm package, copy-paste for testing on purpose
	dirWal               = "wal"
	dirTables            = "tables"
	expFilesForEachTable = 3 //files: data, index, sparse index
)

type LSMStage struct {
	t  *testing.T
	wg sync.WaitGroup

	store   *lsm.Tree
	tempDir string

	errPut error
}

func NewLSMStage(t *testing.T) *LSMStage {
	dirPath, err := os.MkdirTemp(t.TempDir(), "*")
	require.NoError(t, err, "create temp dir")

	return &LSMStage{
		t:       t,
		tempDir: dirPath,
	}
}

func (s *LSMStage) TearDown() {
	// note: temp dir for test will be deleted automatically
}

func (s *LSMStage) Given() *LSMStage {
	return s
}

func (s *LSMStage) When() *LSMStage {
	return s
}

func (s *LSMStage) Then() *LSMStage {
	return s
}

func (s *LSMStage) And() *LSMStage {
	return s
}

func (s *LSMStage) TempDir() string {
	return s.tempDir
}

func (s *LSMStage) StoreIsUpAndRunning(cfg lsm.Config) *LSMStage {
	storage, err := lsm.NewOSStorageProvider(cfg)
	require.Nil(s.t, err, "OS storage provider create error")

	s.store, err = lsm.New(storage, cfg)
	require.Nil(s.t, err, "LSM store create error")

	return s
}

func (s *LSMStage) KeyValueIsPut(key, value []byte) *LSMStage {
	s.errPut = s.store.Put(key, value)
	return s
}

func (s *LSMStage) UpsertIsOK() *LSMStage {
	assert.Nilf(s.t, s.errPut, "upsert error")
	return s
}

func (s *LSMStage) KeyIsPresentWithValue(key, expValue []byte) *LSMStage {
	v, err := s.store.Get(key)
	assert.Nil(s.t, err, "get value error")
	if err == nil {
		assert.Equalf(s.t, expValue, v, "unexpected value for key: %s", key)
	}
	return s
}

func (s *LSMStage) KeyValuesHaveBeenPut(v ...pair) *LSMStage {
	for _, kv := range v {
		errPut := s.store.Put(kv.key, kv.value)
		assert.Nilf(s.t, errPut, "put value error - key: %s, value: %s", kv.key, kv.value)
	}
	return s
}

func (s *LSMStage) WALFilesArePresent() *LSMStage {
	walDir := fmt.Sprintf("%s/%s", s.tempDir, dirWal)
	files, err := ListNonEmptyFiles(walDir)
	require.Nil(s.t, err, "WAL read dir error")
	assert.NotEmpty(s.t, files, "no WAL files found in: %s", walDir)
	return s
}

func (s *LSMStage) WALFilesAreNotPresent() *LSMStage {
	walDir := fmt.Sprintf("%s/%s", s.tempDir, dirWal)
	files, err := ListNonEmptyFiles(walDir)
	require.Nil(s.t, err, "WAL read dir error")
	assert.Emptyf(s.t, files, "WAL files found in %s: %+v", walDir, files)
	return s
}

func (s *LSMStage) TableDirectoriesArePresent() *LSMStage {
	tablesDir := fmt.Sprintf("%s/%s", s.tempDir, dirTables)
	files, err := ListNonEmptyFiles(fmt.Sprintf("%s/%s", s.tempDir, dirTables))
	require.Nil(s.t, err, "tables read dir error")
	assert.NotEmpty(s.t, files, "no table directories found in: %s", tablesDir)

	for _, tableDir := range files {
		assert.Truef(s.t, tableDir.IsDir(), "not a directory: %s", tableDir.Name())
		subDirPath := fmt.Sprintf("%s/%s/%s", s.tempDir, dirTables, tableDir.Name())
		tableFiles, err := ListNonEmptyFiles(subDirPath)
		require.Nil(s.t, err, "table dir read error")
		assert.Equalf(s.t, expFilesForEachTable, len(tableFiles), "unexpected files found in table dir %s: %+v",
			subDirPath, tableFiles)
	}

	return s
}

func (s *LSMStage) TableDirectoriesAreNotPresent() *LSMStage {
	tablesDir := fmt.Sprintf("%s/%s", s.tempDir, dirTables)
	files, err := ListNonEmptyFiles(fmt.Sprintf("%s/%s", s.tempDir, dirTables))
	require.Nil(s.t, err, "tables read dir error")
	assert.Emptyf(s.t, files, "table directories found in %s: %+v", tablesDir, files)
	return s
}

func (s *LSMStage) WaitTillNoWALFilesArePresent() *LSMStage {
	// TODO use some nice lib for re-tries here
	for i := 0; i < maxRetries; i++ {
		walDir := fmt.Sprintf("%s/%s", s.tempDir, dirWal)
		files, err := ListNonEmptyFiles(walDir)
		require.Nil(s.t, err, "WAL read dir error")
		if len(files) == 0 {
			break
		}

		time.Sleep(retryDelay)
	}
	return s
}

func (s *LSMStage) spawn(ctx context.Context, routines int, ticker *time.Ticker, fn func()) *LSMStage {
	s.wg.Add(routines)
	for i := 0; i < routines; i++ {
		go func() {
			defer s.wg.Done()

			for {
				select {
				case <-ctx.Done():
					return

				case <-ticker.C:
					fn()
				}
			}

		}()
	}
	return s
}

func (s *LSMStage) ManyClientsDoWithFreq(ctx context.Context, fn func()) *LSMStage {
	ticker := time.NewTicker(longTestTickerFreq)
	s.spawn(ctx, longTestRoutineCount, ticker, fn)
	return s
}

func (s *LSMStage) WaitForClients() *LSMStage {
	s.wg.Wait()
	return s
}
