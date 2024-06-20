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

	//TODO move to separate structure to manage it more easily
	flushingMu sync.RWMutex
	flushing   map[*memoryStorage]struct{}

	currentMu sync.RWMutex
	current   *memoryStorage
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
	t.currentMu.Lock()
	defer t.currentMu.Unlock()
	err := t.current.Put(key, value)

	if err != nil {
		return err
	}

	if t.current.Size() > t.cfg.MemoryThreshold {
		newMemoryStorage, err := t.storageProvider.NewMemoryStorage()
		if err != nil {
			return err
		}

		old := t.current
		t.current = newMemoryStorage
		go t.dumpToFile(old)
	}

	return nil
}

// TODO delegate it with flushing map & its mu to separate struct
func (t *Tree) dumpToFile(old *memoryStorage) {
	writer, err := t.storageProvider.NewSSTableWriter()
	if err != nil {
		//TODO log error here
		return
	}

	t.flushingMu.Lock()
	t.flushing[old] = struct{}{}
	t.flushingMu.Unlock()

	defer writer.Close() // TODO log error

	if err := old.Write(writer); err != nil {
		// TODO log error
		// TODO should we retry here or just try to move WAL to SSTable by some manual actions using CLI?
	} else {
		t.flushingMu.Lock()
		delete(t.flushing, old)
		t.flushingMu.Unlock()

		if err := old.Clear(); err != nil {
			// TODO log error
			println(err.Error())
		}
	}
}

func (t *Tree) Get(key []byte) ([]byte, error) {
	t.currentMu.RLock()
	value, found := t.current.Get(key)
	t.currentMu.RUnlock()
	if found {
		return value, nil
	}

	value, ok := t.findInFlushingMemory(key)
	if ok {
		return value, nil
	}

	return t.findInFiles(key)
}

func (t *Tree) findInFlushingMemory(key []byte) ([]byte, bool) {
	t.flushingMu.RLock()
	defer t.flushingMu.RUnlock()
	for f, _ := range t.flushing {
		value, found := f.Get(key)
		if found {
			return value, true
		}
	}

	return nil, false
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
