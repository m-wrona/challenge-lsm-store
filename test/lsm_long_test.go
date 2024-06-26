/*
Additional component/integration tests for LSM store which focus on concurrency checks and
some first bottlenecks that can occur.
*/
package test

import (
	"challenge-lsm-store/lsm"
	"fmt"
	"testing"
	"time"
)

func Test_LSM_ShouldSupportManyClientsForMemoryWrite(t *testing.T) {
	LongTestRunOnly(t)
	ctx, cancel := TestContext()
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

func Test_LSM_ShouldSupportManyClientsForFileWrite(t *testing.T) {
	LongTestRunOnly(t)
	ctx, cancel := TestContext()
	defer cancel()

	stage := NewLSMStage(t)
	defer stage.TearDown()

	stage.Given().
		StoreIsUpAndRunning(lsm.Config{
			MemoryThreshold: fileMemoryThreshold,
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

func Test_LSM_ShouldSupportManyClientsForMemoryRead(t *testing.T) {
	LongTestRunOnly(t)
	ctx, cancel := TestContext()
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

func Test_LSM_ShouldSupportManyClientsForFileRead(t *testing.T) {
	LongTestRunOnly(t)
	ctx, cancel := TestContext()
	defer cancel()

	stage := NewLSMStage(t)
	defer stage.TearDown()

	stage.Given().
		StoreIsUpAndRunning(lsm.Config{
			MemoryThreshold: fileMemoryThreshold,
			Dir:             stage.TempDir(),
		}).And().
		KeyValuesHaveBeenPut(
			pair{key: []byte("key1"), value: []byte("value1")},
			pair{key: []byte("key2"), value: []byte("value2")},
			pair{key: []byte("key3"), value: []byte("value3")},
		).And().
		WaitTillNoWALFilesArePresent()

	stage.When().
		ManyClientsDoWithFreq(ctx, func() {
			stage.KeyIsPresentWithValue([]byte("key1"), []byte("value1"))
		})

	stage.Then().
		WaitForClients()
}
