package lsm

import (
	"challenge-lsm-store/memtable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func Test_LSM_Tree_PutWithoutClearingMemory(t *testing.T) {
	//GIVEN a tree
	storage := &mockStorageProvider{}
	tree, err := New(
		storage,
		Config{
			MemoryThreshold: 1000,
		},
	)
	require.Nil(t, err, "couldn't create a new tree")

	//WHEN key-value is stored
	require.Nil(t, tree.Put([]byte("key1"), []byte("value1")), "put error")

	// THEN key-value is kept in memory
	assert.Equal(t, 1, len(storage.memoryTables), "unexpected memory tables")
	assert.Equal(t, 1, len(storage.walBuffers), "unexpected wal buffers")

	v, err := tree.Get([]byte("key1"))
	assert.Nil(t, err, "get error")
	assert.Equal(t, []byte("value1"), v, "expected value")

	// AND key-value is not stored in tables yet
	assert.Equal(t, 0, len(storage.tableDataWriters), "unexpected file table writers")
}

func Test_LSM_Tree_PutAndDumpMemoryToFile(t *testing.T) {
	//GIVEN a tree
	storage := &mockStorageProvider{}
	tree, err := New(
		storage,
		Config{
			MemoryThreshold: 1,
		},
	)
	require.Nil(t, err, "couldn't create a new tree")

	//WHEN key-value is stored
	require.Nil(t, tree.Put([]byte("key1"), []byte("value1")), "put error")

	<-time.After(50 * time.Millisecond) // TODO replace with re-tries since we can observe when dumping is finished

	// THEN key-value is not kept in memory anymore
	assert.Equal(t, 2, len(storage.memoryTables), "unexpected memory tables")
	assert.Equal(t, 2, len(storage.walBuffers), "unexpected wal buffers")

	v, err := tree.Get([]byte("key1"))
	assert.Nil(t, err, "get error")
	assert.Nil(t, v, "expected no value")

	// AND key-value is dumped to table file
	assert.Equal(t, 1, len(storage.tableDataWriters), "unexpected file table writers")
	assert.NotEqual(t, 0, len(storage.tableDataWriters[0].Bytes()), "file table is empty")
}

func Test_LSM_Tree_GetFromMainMemoryTable(t *testing.T) {
	//GIVEN a tree
	currentTable := memtable.NewMemtable()
	tree := Tree{
		cfg: Config{
			MemoryThreshold: 1000,
		},
		flushing: make(map[*MemoryStorage]struct{}),
		current: &MemoryStorage{
			memory: currentTable,
		},
	}
	//AND value is present in memory
	currentTable.Upsert([]byte("key1"), []byte("value1"))

	//WHEN key-value is get
	v, err := tree.Get([]byte("key1"))

	// THEN key-value is read from memory
	assert.Equal(t, []byte("value1"), v, "expected value")
	assert.Nil(t, err, "get error")
}

func Test_LSM_Tree_GetFromFlushingMemoryTable(t *testing.T) {
	//GIVEN a tree
	tree := Tree{
		cfg: Config{
			MemoryThreshold: 1000,
		},
		flushing: make(map[*MemoryStorage]struct{}),
		current: &MemoryStorage{
			memory: memtable.NewMemtable(),
		},
	}
	//AND value is present in older tables that are being dumped atm.
	flushingTable := memtable.NewMemtable()
	tree.flushing[&MemoryStorage{
		memory: flushingTable,
	}] = struct{}{}
	flushingTable.Upsert([]byte("key1"), []byte("value1"))

	//WHEN key-value is get
	v, err := tree.Get([]byte("key1"))

	// THEN key-value is read from memory
	assert.Equal(t, []byte("value1"), v, "expected value")
	assert.Nil(t, err, "get error")
}

func Test_LSM_Tree_GetFromTableFiles(t *testing.T) {
	//GIVEN a tree
	storage := &mockStorageProvider{}
	tree, err := New(storage, Config{
		MemoryThreshold: 1000,
	})
	require.Nil(t, err, "couldn't create a new tree")
	//AND value is present in table files
	writer, err := storage.NewSSTableWriter()
	require.Nil(t, err, "couldn't create a new table writer")
	require.Nil(t, writer.Write([]byte("key1"), []byte("value1")), "write error")
	storage.MoveSSTablesToFiles()

	//WHEN key-value is get
	v, err := tree.Get([]byte("key1"))

	// THEN key-value is read from memory
	assert.Equal(t, []byte("value1"), v, "expected value")
	assert.Nil(t, err, "get error")
}
