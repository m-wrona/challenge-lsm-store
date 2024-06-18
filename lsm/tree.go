package lsm

import (
	"challenge-lsm-store/sstable"
	"sync"
)

type storageProvider interface {
	NewMemoryStorage() (*memoryStorage, error)
	NewSSTableWriter() (*sstable.Writer, error)
	FilesStorage() ([]*fileStorage, error)
}

// Tree represents single tree for LSM store. Tree is not thread-safe.
type Tree struct {
	cfg             Config
	storageProvider storageProvider

	flushingMu sync.RWMutex
	flushing   map[*memoryStorage]struct{}

	current *memoryStorage
}

// TODO replace config with options to make default settings possible
func New(storageProvider storageProvider, cfg Config) (*Tree, error) {
	storage, err := storageProvider.NewMemoryStorage()
	if err != nil {
		return nil, err
	}

	return &Tree{
		cfg:             cfg,
		storageProvider: storageProvider,
		current:         storage,
		flushing:        make(map[*memoryStorage]struct{}),
	}, nil
}

func (t *Tree) Put(key []byte, value []byte) error {
	if err := t.current.Put(key, value); err != nil {
		return err
	}

	if t.current.Size() > t.cfg.MemoryThreshold {
		if err := t.newStorage(); err != nil {
			return err
		}
	}

	return nil
}

func (t *Tree) newStorage() error {
	newStorage, err := t.storageProvider.NewMemoryStorage()
	if err != nil {
		return err
	}

	writer, err := t.storageProvider.NewSSTableWriter()
	if err != nil {
		return err
	}

	old := t.current
	t.current = newStorage
	t.flushingMu.Lock()
	t.flushing[old] = struct{}{}
	t.flushingMu.Unlock()

	go func() {
		defer writer.Close() // TODO log error

		if err := old.Write(writer); err != nil {
			// TODO log error
			// TODO should we retry here or just try to move WAL to SSTable by some manual actions using CLI?
		} else {
			t.flushingMu.Lock()
			delete(t.flushing, old)
			t.flushingMu.Unlock()

			_ = old.Clear() // TODO log error
		}
	}()

	return nil
}

func (t *Tree) Get(key []byte) ([]byte, error) {
	// check current memory
	value, found := t.current.Get(key)
	if found {
		return value, nil
	}

	// check memory which is currently being dumped to storage
	t.flushingMu.RLock()
	defer t.flushingMu.RUnlock()
	for f, _ := range t.flushing {
		value, found := f.Get(key)
		if found {
			return value, nil
		}
	}

	return t.findInFiles(key)
}

func (t *Tree) findInFiles(key []byte) ([]byte, error) {
	files, err := t.storageProvider.FilesStorage()
	if err != nil {
		return nil, err
	}

	for _, r := range files {
		value, found, err := r.Find(key)
		if err != nil {
			return nil, err
		}
		if found {
			return value, nil
		}
	}

	return nil, nil
}
