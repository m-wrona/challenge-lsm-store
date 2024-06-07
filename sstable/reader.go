package sstable

import (
	"bytes"
	"fmt"
	"io"
)

type Reader struct {
	dataReader        io.ReadSeekCloser
	indexReader       io.ReadSeekCloser
	sparseIndexReader io.ReadSeekCloser
}

func NewReader(
	dataReader io.ReadSeekCloser,
	indexReader io.ReadSeekCloser,
	sparseIndexReader io.ReadSeekCloser,
) *Reader {
	return &Reader{
		dataReader:        dataReader,
		indexReader:       indexReader,
		sparseIndexReader: sparseIndexReader,
	}
}

func (r *Reader) Find(key []byte) ([]byte, bool, error) {
	from, to, ok, err := r.searchInSparseIndex(key)
	if err != nil {
		return nil, false, fmt.Errorf("sparse index error: %w", err)
	}
	if !ok {
		return nil, false, err
	}

	offset, ok, err := r.searchInIndex(from, to, key)
	if err != nil {
		return nil, false, fmt.Errorf("index error: %w", err)
	}
	if !ok {
		return nil, false, nil
	}

	value, ok, err := r.searchInDataFile(offset, key)
	if err != nil {
		return nil, false, fmt.Errorf("data error: %w", err)
	}

	return value, ok, nil
}

func (r *Reader) searchInDataFile(offset int, searchKey []byte) ([]byte, bool, error) {
	if _, err := r.dataReader.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, false, fmt.Errorf("failed to seek: %w", err)
	}

	for {
		key, value, err := decode(r.dataReader)
		if err != nil && err != io.EOF {
			return nil, false, fmt.Errorf("failed to read: %w", err)
		}
		if err == io.EOF {
			return nil, false, nil
		}

		if bytes.Equal(key, searchKey) {
			return value, true, nil
		}
	}
}

func (r *Reader) searchInIndex(from, to int, searchKey []byte) (int, bool, error) {
	if _, err := r.indexReader.Seek(int64(from), io.SeekStart); err != nil {
		return 0, false, fmt.Errorf("failed to seek: %w", err)
	}

	for {
		key, value, err := decode(r.indexReader)
		if err != nil && err != io.EOF {
			return 0, false, fmt.Errorf("failed to read: %w", err)
		}
		if err == io.EOF {
			return 0, false, nil
		}
		offset := decodeInt(value)

		if bytes.Equal(key, searchKey) {
			return offset, true, nil
		}

		if to > from {
			current, err := r.indexReader.Seek(0, io.SeekCurrent)
			if err != nil {
				return 0, false, fmt.Errorf("failed to seek: %w", err)
			}

			if current > int64(to) {
				return 0, false, nil
			}
		}
	}
}

func (r *Reader) searchInSparseIndex(searchKey []byte) (int, int, bool, error) {
	defer func() {
		_, _ = r.sparseIndexReader.Seek(0, io.SeekStart)
	}()

	from := -1
	for {
		key, value, err := decode(r.sparseIndexReader)
		if err != nil && err != io.EOF {
			return 0, 0, false, err
		}
		if err == io.EOF {
			return from, 0, from != -1, nil
		}

		offset := decodeInt(value)

		cmp := bytes.Compare(key, searchKey)
		if cmp == 0 {
			return offset, offset, true, nil
		} else if cmp < 0 {
			from = offset
		} else if cmp > 0 {
			if from == -1 {
				// if the first key in the sparse index is larger than
				// the search key, it means there is no key
				return 0, 0, false, nil
			} else {
				return from, offset, true, nil
			}
		}
	}
}
