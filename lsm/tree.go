package lsm

import (
	"challenge-lsm-store/sstable"
	"sync"
)

type storageProvider interface {
	NewMemoryStorage() (*MemoryStorage, error)
	NewSSTableWriter() (*sstable.Writer, error)
	FilesStorage() ([]*fileStorage, error)
}

// Tree represents single tree for LSM store. Tree is not thread-safe.
type Tree struct {
	cfg             Config
	storageProvider storageProvider

	//TODO move to separate structure to manage it more easily
	flushingMu sync.RWMutex
	flushing   map[*MemoryStorage]struct{}

	currentMu sync.RWMutex
	current   *MemoryStorage
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
		flushing:        make(map[*MemoryStorage]struct{}),
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
		go func() {
			// TODO log error here
			_ = t.WriteToFile(old)
			// TODO if we couldn't move data from memory into file I guess we should revert operation

			// TODO it could take a while till old memory storage will appear in flushing area thus consider 2 options:
			// 1) run write to file directly (not as a routine) which may block put for much longer
			// 2) we can add old storage to flushing map here and run write to file anyways
			// 3) write to file can return a channel which will signal when writing started thus when old memory is in flushing area already
		}()
	}

	return nil
}

// LoadIntoMemory loads data from storage into current memory without touching WAL
// TODO this fn is rather for testing purposes now (which is bad) to speed up import
func (t *Tree) LoadIntoMemory(memoryStorage *MemoryStorage) {
	t.currentMu.Lock()
	defer t.currentMu.Unlock()

	for pair := range memoryStorage.memory.GetAll() {
		t.current.Load(pair.GetKey(), pair.GetValue())
	}
}

// WriteToFile moves data from memory into files
// TODO delegate it with flushing map & its mu to separate struct
func (t *Tree) WriteToFile(memoryStorage *MemoryStorage) error {
	writer, err := t.storageProvider.NewSSTableWriter()
	if err != nil {
		return err
	}

	t.flushingMu.Lock()
	t.flushing[memoryStorage] = struct{}{}
	t.flushingMu.Unlock()

	defer writer.Close() // TODO log error

	if err := memoryStorage.Write(writer); err != nil {
		// TODO should we retry here or just try to move WAL to SSTable by some manual actions using CLI?
		return err
	} else {
		t.flushingMu.Lock()
		delete(t.flushing, memoryStorage)
		t.flushingMu.Unlock()

		if err := memoryStorage.Clear(); err != nil {
			return err
		}
	}

	return nil
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
