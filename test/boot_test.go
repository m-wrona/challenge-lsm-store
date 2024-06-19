package test

import (
	"testing"
)

func Test_LSM_Boot_ShouldLoadWalFilesIntoMemory(t *testing.T) {
	// TODO implement this
	t.Skip("check whether existing WAL files are loaded into memory again")

	stage := NewLSMStage(t)
	defer stage.TearDown()
}

func Test_LSM_Boot_ShouldGetKeyValuesFromTableFiles(t *testing.T) {
	// TODO implement this
	t.Skip("check whether data is available using SSTable files once booting is complete")

	stage := NewLSMStage(t)
	defer stage.TearDown()
}
