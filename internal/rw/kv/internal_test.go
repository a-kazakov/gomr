package kv

// Tests use package-internal access for: loadKey(), readValue(), currentKeyReader/currentKeyVersion fields.

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

// =============================================================================
// Helpers: error-producing io.Writer / io.Reader
// =============================================================================

// failAfterNWriter fails after writing n bytes total.
type failAfterNWriter struct {
	remaining int
	writeErr  error
}

func (f *failAfterNWriter) Write(p []byte) (int, error) {
	if f.remaining <= 0 {
		return 0, f.writeErr
	}
	if len(p) > f.remaining {
		n := f.remaining
		f.remaining = 0
		return n, f.writeErr
	}
	f.remaining -= len(p)
	return len(p), nil
}

// limitedReader returns data from buf, then errors after buf is exhausted.
type limitedErrReader struct {
	buf *bytes.Reader
	err error // error to return after buf is exhausted
}

func (r *limitedErrReader) Read(p []byte) (int, error) {
	n, err := r.buf.Read(p)
	if err == io.EOF {
		return n, r.err
	}
	return n, err
}

// fakeValueReader is a valueReader that returns a non-EOF error.
type fakeValueReader struct {
	callCount int
	err       error
}

func (f *fakeValueReader) readValue(expectedKeyVersion int64, p []byte) (int, error) {
	f.callCount++
	return 0, f.err
}

var errWrite = errors.New("write error")

func TestWriterErrorPaths(t *testing.T) {
	t.Run("error on same key marker", func(t *testing.T) {
		// Write same key twice. The second write will emit uint16(0) as key marker.
		// Make the writer fail at exactly the point where uint16(0) is written.
		//
		// First Write("key1", "v1"):
		//   binary.Write uint16(4) -> 2 bytes
		//   Write key "key1"       -> 4 bytes
		//   binary.Write uint32(2) -> 4 bytes
		//   Write value "v1"       -> 2 bytes
		// Total: 12 bytes
		//
		// Second Write("key1", "v2"):
		//   binary.Write uint16(0) -> 2 bytes  <-- fail here
		w := &failAfterNWriter{remaining: 12, writeErr: errWrite}
		writer := NewWriter(w)
		err := writer.Write([]byte("key1"), []byte("v1"))
		if err != nil {
			t.Fatalf("first Write should succeed, got: %v", err)
		}
		err = writer.Write([]byte("key1"), []byte("v2"))
		if err == nil {
			t.Fatal("expected error on same-key marker write")
		}
	})

	t.Run("error on key length", func(t *testing.T) {
		// Fail on the very first binary.Write (key length for new key).
		w := &failAfterNWriter{remaining: 0, writeErr: errWrite}
		writer := NewWriter(w)
		err := writer.Write([]byte("k"), []byte("v"))
		if err == nil {
			t.Fatal("expected error on key length write")
		}
	})

	t.Run("error on key data", func(t *testing.T) {
		// Allow key length (2 bytes) to succeed, then fail on key data.
		w := &failAfterNWriter{remaining: 2, writeErr: errWrite}
		writer := NewWriter(w)
		err := writer.Write([]byte("k"), []byte("v"))
		if err == nil {
			t.Fatal("expected error on key data write")
		}
	})

	t.Run("error on value length", func(t *testing.T) {
		// Allow key length (2) + key data (3) = 5 bytes, then fail on value length.
		w := &failAfterNWriter{remaining: 5, writeErr: errWrite}
		writer := NewWriter(w)
		err := writer.Write([]byte("abc"), []byte("val"))
		if err == nil {
			t.Fatal("expected error on value length write")
		}
	})

	t.Run("error on value data", func(t *testing.T) {
		// Allow key length (2) + key data (3) + value length (4) = 9 bytes, then fail on value data.
		w := &failAfterNWriter{remaining: 9, writeErr: errWrite}
		writer := NewWriter(w)
		err := writer.Write([]byte("abc"), []byte("val"))
		if err == nil {
			t.Fatal("expected error on value data write")
		}
	})

	t.Run("error on value length for same key", func(t *testing.T) {
		// First write succeeds; second write (same key) writes uint16(0) = 2 bytes,
		// then fails on value length.
		// First write: 2 (keyLen) + 3 (key) + 4 (valLen) + 2 (val) = 11
		// Second write: 2 (zero keyLen) succeeds, then 4 (valLen) fails
		w := &failAfterNWriter{remaining: 13, writeErr: errWrite}
		writer := NewWriter(w)
		err := writer.Write([]byte("abc"), []byte("v1"))
		if err != nil {
			t.Fatalf("first Write should succeed: %v", err)
		}
		err = writer.Write([]byte("abc"), []byte("v2"))
		if err == nil {
			t.Fatal("expected error on value length for same key")
		}
	})

	t.Run("error on value data for same key", func(t *testing.T) {
		// First write: 2 + 3 + 4 + 2 = 11 bytes
		// Second write (same key): 2 (zero keyLen) + 4 (valLen) = 6 bytes succeed, then value data fails
		w := &failAfterNWriter{remaining: 17, writeErr: errWrite}
		writer := NewWriter(w)
		err := writer.Write([]byte("abc"), []byte("v1"))
		if err != nil {
			t.Fatalf("first Write should succeed: %v", err)
		}
		err = writer.Write([]byte("abc"), []byte("v2"))
		if err == nil {
			t.Fatal("expected error on value data for same key")
		}
	})
}

func TestReaderErrorPaths(t *testing.T) {
	t.Run("panic on truncated key data", func(t *testing.T) {
		// Construct a stream: valid key length (say 5), but then truncated data so
		// io.ReadFull fails when reading key bytes.
		var buf bytes.Buffer
		binary.Write(&buf, binary.NativeEndian, uint16(5)) // keyLength = 5
		buf.WriteByte(0x41)                                 // only 1 byte of key data (need 5)
		// The remaining 4 bytes will cause io.ReadFull to return unexpected EOF.

		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expected panic from loadKey on truncated key data")
			}
		}()
		_ = NewReader(&buf) // loadKey is called in constructor
	})

	t.Run("panic on non-eof error reading key length", func(t *testing.T) {
		// Reader that returns a non-EOF error when reading key length.
		errCustom := errors.New("custom read error")
		r := &limitedErrReader{
			buf: bytes.NewReader([]byte{}),
			err: errCustom,
		}

		defer func() {
			rec := recover()
			if rec == nil {
				t.Fatal("expected panic from loadKey on non-EOF error reading key length")
			}
		}()
		_ = NewReader(r)
	})

	t.Run("panic on truncated value length", func(t *testing.T) {
		// Write one valid KV pair, but truncate the stream after the first value
		// so that when readValue tries to read the next value length, it fails.
		//
		// Stream: keyLen=3, key="abc", valLen=2, val="v1", keyLen=0 (same key), [truncated valLen]
		var buf bytes.Buffer
		binary.Write(&buf, binary.NativeEndian, uint16(3))
		buf.WriteString("abc")
		binary.Write(&buf, binary.NativeEndian, uint32(2))
		buf.WriteString("v1")
		binary.Write(&buf, binary.NativeEndian, uint16(0)) // same key continues
		buf.WriteByte(0x00)                                 // only 1 of 4 bytes for value length

		reader := NewReader(&buf)

		// First readValue should succeed
		p := make([]byte, 100)
		kr := reader.GetKeyReader()
		n, err := kr.ReadValue(p)
		if err != nil {
			t.Fatalf("first ReadValue should succeed, got: %v", err)
		}
		if string(p[:n]) != "v1" {
			t.Fatalf("expected 'v1', got %q", string(p[:n]))
		}

		// Second readValue should panic (truncated value length)
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expected panic on truncated value length")
			}
		}()
		kr.ReadValue(p)
	})

	t.Run("panic on truncated value data", func(t *testing.T) {
		// Stream: keyLen=3, key="abc", valLen=5, val=[only 2 bytes]
		var buf bytes.Buffer
		binary.Write(&buf, binary.NativeEndian, uint16(3))
		buf.WriteString("abc")
		binary.Write(&buf, binary.NativeEndian, uint32(5)) // says 5 bytes
		buf.WriteString("ab")                              // only 2 bytes of value

		reader := NewReader(&buf)
		p := make([]byte, 100)
		kr := reader.GetKeyReader()

		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expected panic on truncated value data")
			}
		}()
		kr.ReadValue(p)
	})
}

func TestMultiReaderErrorPaths(t *testing.T) {
	t.Run("panic on non-eof error", func(t *testing.T) {
		// Construct a MultiReader with a currentReader set, then inject a
		// currentKeyReader that returns a non-EOF error.
		errCustom := errors.New("custom value error")

		// Create a valid MultiReader with one reader
		var buf bytes.Buffer
		w := NewWriter(&buf)
		w.Write([]byte("key1"), []byte("v1"))

		mr := NewMultiReader([]io.Reader{bytes.NewReader(buf.Bytes())})

		// Peek to ensure currentReader is loaded
		key := mr.PeekKey()
		if string(key) != "key1" {
			t.Fatalf("expected 'key1', got %q", string(key))
		}

		// Replace currentKeyReader with our fake one
		fakeReader := &fakeValueReader{err: errCustom}
		mr.currentKeyReader = KeyReader{
			keyVersion: mr.currentKeyVersion,
			reader:     fakeReader,
		}

		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expected panic from MultiReader readValue on non-EOF error")
			}
		}()
		p := make([]byte, 100)
		mr.readValue(mr.currentKeyVersion, p)
	})

	t.Run("eof when no current reader", func(t *testing.T) {
		// After exhausting all readers, currentReader is nil.
		// readValue should return EOF even when version matches.
		mr := NewMultiReader([]io.Reader{})
		p := make([]byte, 100)
		_, err := mr.readValue(mr.currentKeyVersion, p)
		if err != io.EOF {
			t.Fatalf("expected io.EOF, got %v", err)
		}
	})
}
