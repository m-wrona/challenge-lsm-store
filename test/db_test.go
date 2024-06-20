/*
*
Test cases from DB perspective, so it should include CLI and statements (DDL & DML)
*/
package test

import (
	"challenge-lsm-store/lsm"
	"errors"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func Test_DB_ShouldLoadSegmentsFileFromDisk(t *testing.T) {
	t.Parallel()
	LongTestRunOnly(t)

	const jsonFilePath = "../segments.json"

	if _, err := os.Stat(jsonFilePath); errors.Is(err, os.ErrNotExist) {
		t.Skipf("skipping benchmark because segments.json does not exist: %s", err)
	}
	jsonFile, err := os.Open(jsonFilePath)
	require.Nilf(t, err, "json file not found: %s", jsonFilePath)

	stage := NewDBStage(t)
	defer stage.TearDown()

	stage.Given().
		StoreIsUpAndRunning(lsm.Config{
			MemoryThreshold: defaultMemoryThreshold,
			Dir:             stage.TempDir(),
		}).And().
		SegmentsAreLoadedFromReader(jsonFile).And().
		SegmentsArePresent().And().
		SegmentsAreLoadedIntoStore()

	t.Run("get random documents from store", func(t *testing.T) {
		stage.When().
			RandomDocumentsAreCheckedFromSegment(10)

		stage.Then().
			DocumentsArePresentInStore()
	})

}
