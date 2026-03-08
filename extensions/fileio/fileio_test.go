package fileio

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// LocalBackend
// ---------------------------------------------------------------------------

func TestLocalBackend_Open(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "test.txt")
	if err := os.WriteFile(p, []byte("hello"), 0644); err != nil {
		t.Fatal(err)
	}

	r, err := LocalBackend.Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello" {
		t.Fatalf("got %q, want %q", data, "hello")
	}
}

func TestLocalBackend_Open_FilePrefix(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "test.txt")
	if err := os.WriteFile(p, []byte("world"), 0644); err != nil {
		t.Fatal(err)
	}

	r, err := LocalBackend.Open("file://" + p)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	data, _ := io.ReadAll(r)
	if string(data) != "world" {
		t.Fatalf("got %q, want %q", data, "world")
	}
}

func TestLocalBackend_Create(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "out.txt")

	w, err := LocalBackend.Create(p)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("data")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "data" {
		t.Fatalf("got %q, want %q", got, "data")
	}
}

func TestLocalBackend_Create_FilePrefix(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "out.txt")

	w, err := LocalBackend.Create("file://" + p)
	if err != nil {
		t.Fatal(err)
	}
	w.Write([]byte("pfx"))
	w.Close()

	got, _ := os.ReadFile(p)
	if string(got) != "pfx" {
		t.Fatalf("got %q, want %q", got, "pfx")
	}
}

func TestLocalBackend_Glob(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"a.txt", "b.txt", "c.log"} {
		os.WriteFile(filepath.Join(dir, name), []byte("x"), 0644)
	}
	// Also create a subdirectory to confirm it is excluded.
	os.Mkdir(filepath.Join(dir, "subdir"), 0755)

	matches, err := LocalBackend.Glob(filepath.Join(dir, "*.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 2 {
		t.Fatalf("got %d matches, want 2: %v", len(matches), matches)
	}
}

func TestLocalBackend_Glob_FilePrefix(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "f.dat"), []byte("x"), 0644)

	matches, err := LocalBackend.Glob("file://" + filepath.Join(dir, "*.dat"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 1 {
		t.Fatalf("got %d matches, want 1", len(matches))
	}
}

// ---------------------------------------------------------------------------
// Open / Create round-trip
// ---------------------------------------------------------------------------

func TestOpenCreate_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "rt.txt")

	w, err := Create(p)
	if err != nil {
		t.Fatal(err)
	}
	w.Write([]byte("round-trip"))
	w.Close()

	r, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	data, _ := io.ReadAll(r)
	if string(data) != "round-trip" {
		t.Fatalf("got %q", data)
	}
}

// ---------------------------------------------------------------------------
// Glob
// ---------------------------------------------------------------------------

func TestGlob(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "x.json"), []byte("{}"), 0644)
	os.WriteFile(filepath.Join(dir, "y.json"), []byte("{}"), 0644)
	os.WriteFile(filepath.Join(dir, "z.csv"), []byte(""), 0644)

	files, err := Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 {
		t.Fatalf("got %d files, want 2", len(files))
	}
}

// ---------------------------------------------------------------------------
// ReadLines
// ---------------------------------------------------------------------------

func TestReadLines_Basic(t *testing.T) {
	r := strings.NewReader("line1\nline2\nline3\n")
	var lines []string
	for v := range ReadLines(r, 1024, func(b []byte) string { return string(b) }) {
		lines = append(lines, v)
	}
	if len(lines) != 3 {
		t.Fatalf("got %d lines, want 3: %v", len(lines), lines)
	}
	if lines[0] != "line1" || lines[2] != "line3" {
		t.Fatalf("unexpected lines: %v", lines)
	}
}

func TestReadLines_EmptyFile(t *testing.T) {
	r := strings.NewReader("")
	var count int
	for range ReadLines(r, 1024, func(b []byte) string { return string(b) }) {
		count++
	}
	if count != 0 {
		t.Fatalf("expected 0 lines from empty reader, got %d", count)
	}
}

func TestReadLines_EmptyLines(t *testing.T) {
	// Empty lines (len==0) are skipped by ReadLines.
	r := strings.NewReader("\n\nfoo\n\n")
	var lines []string
	for v := range ReadLines(r, 1024, func(b []byte) string { return string(b) }) {
		lines = append(lines, v)
	}
	if len(lines) != 1 || lines[0] != "foo" {
		t.Fatalf("expected [foo], got %v", lines)
	}
}

func TestReadLines_TypeConversion(t *testing.T) {
	r := strings.NewReader("abc\ndef\n")
	var lengths []int
	for v := range ReadLines(r, 1024, func(b []byte) int { return len(b) }) {
		lengths = append(lengths, v)
	}
	if len(lengths) != 2 || lengths[0] != 3 || lengths[1] != 3 {
		t.Fatalf("unexpected lengths: %v", lengths)
	}
}

func TestReadLines_EarlyBreak(t *testing.T) {
	r := strings.NewReader("a\nb\nc\nd\n")
	var lines []string
	for v := range ReadLines(r, 1024, func(b []byte) string { return string(b) }) {
		lines = append(lines, v)
		if len(lines) == 2 {
			break
		}
	}
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d", len(lines))
	}
}

// ---------------------------------------------------------------------------
// WrapReadCloser / WrapWriteCloser
// ---------------------------------------------------------------------------

func TestWrapReadCloser_CloseOrder(t *testing.T) {
	var order []string
	orig := &trackCloser{name: "orig", order: &order, Reader: strings.NewReader("data")}
	derived := &trackCloser{name: "derived", order: &order, Reader: strings.NewReader("data")}

	wrapped := WrapReadCloser(orig, func(r io.ReadCloser) io.ReadCloser {
		return derived
	})
	if err := wrapped.Close(); err != nil {
		t.Fatal(err)
	}
	if len(order) != 2 || order[0] != "derived" || order[1] != "orig" {
		t.Fatalf("close order = %v, want [derived orig]", order)
	}
}

func TestWrapWriteCloser_CloseOrder(t *testing.T) {
	var order []string
	orig := &trackWriteCloser{name: "orig", order: &order}
	derived := &trackWriteCloser{name: "derived", order: &order}

	wrapped := WrapWriteCloser(orig, func(w io.WriteCloser) io.WriteCloser {
		return derived
	})
	if err := wrapped.Close(); err != nil {
		t.Fatal(err)
	}
	if len(order) != 2 || order[0] != "derived" || order[1] != "orig" {
		t.Fatalf("close order = %v, want [derived orig]", order)
	}
}

// ---------------------------------------------------------------------------
// NewFakeWriteCloser
// ---------------------------------------------------------------------------

func TestNewFakeWriteCloser(t *testing.T) {
	var buf bytes.Buffer
	wc := NewFakeWriteCloser(&buf)
	wc.Write([]byte("test"))
	if err := wc.Close(); err != nil {
		t.Fatal(err)
	}
	if buf.String() != "test" {
		t.Fatalf("got %q", buf.String())
	}
	// Close again should also succeed (no-op).
	if err := wc.Close(); err != nil {
		t.Fatal(err)
	}
}

// ---------------------------------------------------------------------------
// Must* functions
// ---------------------------------------------------------------------------

func TestMustOpen_Success(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "m.txt")
	os.WriteFile(p, []byte("ok"), 0644)

	r := MustOpen(p)
	defer r.Close()
	data, _ := io.ReadAll(r)
	if string(data) != "ok" {
		t.Fatalf("got %q", data)
	}
}

func TestMustOpen_Panic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for nonexistent file")
		}
	}()
	MustOpen("/nonexistent/path/file.txt")
}

func TestMustCreate_Success(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "mc.txt")
	w := MustCreate(p)
	w.Write([]byte("ok"))
	w.Close()
	got, _ := os.ReadFile(p)
	if string(got) != "ok" {
		t.Fatalf("got %q", got)
	}
}

func TestMustCreate_Panic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic")
		}
	}()
	MustCreate("/nonexistent/dir/file.txt")
}

func TestMustGlob_Success(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "a.go"), []byte(""), 0644)
	files := MustGlob(filepath.Join(dir, "*.go"))
	if len(files) != 1 {
		t.Fatalf("got %d files", len(files))
	}
}

func TestMustGlob_Panic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic")
		}
	}()
	MustGlob("[invalid")
}

// ---------------------------------------------------------------------------
// test helpers
// ---------------------------------------------------------------------------

type trackCloser struct {
	io.Reader
	name  string
	order *[]string
}

func (tc *trackCloser) Close() error {
	*tc.order = append(*tc.order, tc.name)
	return nil
}

type trackWriteCloser struct {
	name  string
	order *[]string
}

func (tw *trackWriteCloser) Write(p []byte) (int, error) { return len(p), nil }
func (tw *trackWriteCloser) Close() error {
	*tw.order = append(*tw.order, tw.name)
	return nil
}
