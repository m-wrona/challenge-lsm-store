package test

import (
	"context"
	"os"
	"testing"
	"time"
)

const (
	// test related settings
	// TODO could go with env vars
	maxRetries = 20
	retryDelay = 50 * time.Millisecond

	longTestsDuration    = 2 * time.Second
	longTestRoutineCount = 20
	longTestTickerFreq   = 10 * time.Millisecond
)

type pair struct {
	key   []byte
	value []byte
}

func LongTestRunOnly(t *testing.T) (context.Context, context.CancelFunc) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	return context.WithTimeout(context.Background(), longTestsDuration)
}

func ListNonEmptyFiles(dir string) ([]os.DirEntry, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	nonEmpty := make([]os.DirEntry, 0, len(files))
	for _, f := range files {
		stat, err := f.Info()
		if err != nil {
			return nil, err
		}
		if stat.Size() > 0 {
			nonEmpty = append(nonEmpty, f)
		}
	}

	return nonEmpty, nil
}
