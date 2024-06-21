package storageio

import (
	"io"
)

type Unmarshal func(data []byte, v any) error

func Read[T any](in io.Reader, unmarshal Unmarshal, out T) error {
	bytes, err := io.ReadAll(in)
	if err != nil {
		return err
	}

	if err := unmarshal(bytes, out); err != nil {
		return err
	}
	return nil
}
