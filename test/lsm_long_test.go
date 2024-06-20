package test

import (
	"challenge-lsm-store/lsm"
	"fmt"
	"testing"
	"time"
)

func Test_LSM_ShouldSupportManyClientsForWrite(t *testing.T) {
	ctx, cancel := LongTestRunOnly(t)
	defer cancel()

	stage := NewLSMStage(t)
	defer stage.TearDown()

	stage.Given().
		StoreIsUpAndRunning(lsm.Config{
			MemoryThreshold: inMemoryThreshold,
			Dir:             stage.TempDir(),
		})

	stage.When().
		ManyClientsDoWithFreq(ctx, func() {
			v := fmt.Sprintf("%d", time.Now().Unix())
			stage.KeyValuesHaveBeenPut(
				pair{key: []byte(v), value: []byte(v)},
			)
		})

	stage.Then().
		WaitForClients()
}

func Test_LSM_ShouldSupportManyClientsForRead(t *testing.T) {
	ctx, cancel := LongTestRunOnly(t)
	defer cancel()

	stage := NewLSMStage(t)
	defer stage.TearDown()

	stage.Given().
		StoreIsUpAndRunning(lsm.Config{
			MemoryThreshold: inMemoryThreshold,
			Dir:             stage.TempDir(),
		}).And().
		KeyValuesHaveBeenPut(
			pair{key: []byte("key1"), value: []byte("value1")},
			pair{key: []byte("key2"), value: []byte("value2")},
			pair{key: []byte("key3"), value: []byte("value3")},
		)

	stage.When().
		ManyClientsDoWithFreq(ctx, func() {
			stage.KeyIsPresentWithValue([]byte("key1"), []byte("value1"))
		})

	stage.Then().
		WaitForClients()
}
