package fileio

import (
	"bufio"
	"fmt"
	"io"
	"iter"
)

// ReadLines reads lines from r and yields each line converted via the convert function.
// Each line must not exceed maxLineLength bytes; otherwise a panic occurs.
// The []byte passed to convert is reused between iterations — the convert function
// must copy the data if it needs to retain it.
func ReadLines[T any](r io.Reader, maxLineLength int, convert func(line []byte) T) iter.Seq[T] {
	return func(yield func(T) bool) {
		br := bufio.NewReaderSize(r, maxLineLength)
		for {
			b, isPrefix, err := br.ReadLine()
			if err != nil {
				if err == io.EOF {
					break
				}
				panic(fmt.Errorf("error reading line: %w", err))
			}
			if isPrefix {
				panic(fmt.Errorf("line exceeds %d bytes buffer", maxLineLength))
			}
			if len(b) > 0 {
				if !yield(convert(b)) {
					return
				}
			}
		}
	}
}
