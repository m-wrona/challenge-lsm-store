/*
Basic component/integration tests for LSM store and focusing on its functionality as a whole.
*/
package test

import (
	"challenge-lsm-store/lsm"
	"testing"
)

func Test_LSM_ShouldStoreKeyValue(t *testing.T) {
	stage := NewLSMStage(t)
	defer stage.TearDown()

	stage.Given().
		StoreIsUpAndRunning(lsm.Config{
			MemoryThreshold: inMemoryThreshold,
			Dir:             stage.TempDir(),
		})

	stage.When().
		KeyValueIsPut([]byte("key1"), []byte("value1"))

	stage.Then().
		UpsertIsOK().And().
		KeyIsPresentWithValue([]byte("key1"), []byte("value1"))
}

func Test_LSM_ShouldSupportWriteAndReadOperationsInAnyOrder(t *testing.T) {
	stage := NewLSMStage(t)
	defer stage.TearDown()

	stage.Given().
		StoreIsUpAndRunning(lsm.Config{
			MemoryThreshold: inMemoryThreshold,
			Dir:             stage.TempDir(),
		})

	stage.When().
		KeyValuesHaveBeenPut(
			pair{key: []byte("key1"), value: []byte("value1")},
			pair{key: []byte("key2"), value: []byte("value2")},
			pair{key: []byte("key3"), value: []byte("value3")},
			pair{key: []byte("key10"), value: []byte("value10")}, //out of order, goes after key1
		)

	stage.Then().
		KeyIsPresentWithValue([]byte("key1"), []byte("value1")).And().
		KeyIsPresentWithValue([]byte("key2"), []byte("value2")).And().
		KeyIsPresentWithValue([]byte("key10"), []byte("value10")).And().
		KeyIsPresentWithValue([]byte("key3"), []byte("value3"))
}

func Test_LSM_ShouldStoreKeyValueInMemoryAndWALBeforeFileDump(t *testing.T) {
	stage := NewLSMStage(t)
	defer stage.TearDown()

	stage.Given().
		StoreIsUpAndRunning(lsm.Config{
			MemoryThreshold: inMemoryThreshold,
			Dir:             stage.TempDir(),
		})

	stage.When().
		KeyValueIsPut([]byte("key1"), []byte("value1"))

	stage.Then().
		UpsertIsOK().And().
		KeyIsPresentWithValue([]byte("key1"), []byte("value1")).And().
		WALFilesArePresent().And().
		TableDirectoriesAreNotPresent()
}

func Test_LSM_ShouldDumpKeyValueFromMemoryIntoTableFile(t *testing.T) {
	stage := NewLSMStage(t)
	defer stage.TearDown()

	stage.Given().
		StoreIsUpAndRunning(lsm.Config{
			MemoryThreshold: fileMemoryThreshold,
			Dir:             stage.TempDir(),
		})

	stage.When().
		KeyValueIsPut([]byte("key1"), []byte("value1")).And().
		WaitTillNoWALFilesArePresent() // it should mean that memory has been dumped into file(s)

	stage.Then().
		UpsertIsOK().And().
		KeyIsPresentWithValue([]byte("key1"), []byte("value1")).And().
		WALFilesAreNotPresent().And().
		TableDirectoriesArePresent()
}
