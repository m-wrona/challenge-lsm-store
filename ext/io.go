package ext

import (
	"encoding/binary"
	"io"
)

func write[T any](w io.Writer, v T) error {
	// little endian chosen here since:
	// a) allegedly it's more dominant for ARM, x86, RISC-V
	// b) free var access without address conversion
	// on the other hand network protocols use Big Endian more often.
	// tbh. not sure whether it has bigger meaning nowadays...
	return binary.Write(w, binary.LittleEndian, v)
}

func read[T any](r io.Reader, v T) error {
	return binary.Read(r, binary.LittleEndian, &v)
}
