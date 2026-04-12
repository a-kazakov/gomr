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

func TestLocalBackend_Glob_Doublestar(t *testing.T) {
	dir := t.TempDir()
	sub := filepath.Join(dir, "a", "b")
	os.MkdirAll(sub, 0755)
	os.WriteFile(filepath.Join(dir, "top.txt"), []byte("x"), 0644)
	os.WriteFile(filepath.Join(dir, "a", "mid.txt"), []byte("x"), 0644)
	os.WriteFile(filepath.Join(sub, "deep.txt"), []byte("x"), 0644)
	os.WriteFile(filepath.Join(sub, "deep.log"), []byte("x"), 0644)

	matches, err := LocalBackend.Glob(filepath.Join(dir, "**", "*.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 3 {
		t.Fatalf("got %d matches, want 3: %v", len(matches), matches)
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
// BackendRouter
// ---------------------------------------------------------------------------

func TestBackendRouter_LocalPaths(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "test.txt")
	os.WriteFile(p, []byte("hello"), 0644)

	r := NewBackendRouter()

	// Open local path
	rc, err := r.Open(p)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(rc)
	rc.Close()
	if string(data) != "hello" {
		t.Fatalf("got %q, want %q", data, "hello")
	}

	// Create local path
	out := filepath.Join(dir, "out.txt")
	wc, err := r.Create(out)
	if err != nil {
		t.Fatal(err)
	}
	wc.Write([]byte("world"))
	wc.Close()
	got, _ := os.ReadFile(out)
	if string(got) != "world" {
		t.Fatalf("got %q, want %q", got, "world")
	}

	// Glob local path
	matches, err := r.Glob(filepath.Join(dir, "*.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 2 {
		t.Fatalf("got %d matches, want 2", len(matches))
	}
}

func TestBackendRouter_FilePrefixPaths(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "test.txt")
	os.WriteFile(p, []byte("data"), 0644)

	r := NewBackendRouter()

	rc, err := r.Open("file://" + p)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(rc)
	rc.Close()
	if string(data) != "data" {
		t.Fatalf("got %q, want %q", data, "data")
	}
}

func TestBackendRouter_UnregisteredScheme(t *testing.T) {
	r := NewBackendRouter()

	_, err := r.Open("s3://bucket/key.txt")
	if err == nil {
		t.Fatal("expected error for unregistered s3:// scheme in Open")
	}
	if !strings.Contains(err.Error(), `no backend registered for scheme "s3"`) {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = r.Create("s3://bucket/key.txt")
	if err == nil {
		t.Fatal("expected error for unregistered s3:// scheme in Create")
	}

	_, err = r.Glob("s3://bucket/*.txt")
	if err == nil {
		t.Fatal("expected error for unregistered s3:// scheme in Glob")
	}
}

func TestBackendRouter_RegisteredScheme(t *testing.T) {
	r := NewBackendRouter()
	fake := &fakeBackend{openData: "from-fake"}
	r.Register("s3", fake)

	rc, err := r.Open("s3://bucket/key.txt")
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(rc)
	rc.Close()
	if string(data) != "from-fake" {
		t.Fatalf("got %q, want %q", data, "from-fake")
	}

	// Local paths should still go through local backend
	dir := t.TempDir()
	p := filepath.Join(dir, "local.txt")
	os.WriteFile(p, []byte("local"), 0644)
	rc, err = r.Open(p)
	if err != nil {
		t.Fatal(err)
	}
	data, _ = io.ReadAll(rc)
	rc.Close()
	if string(data) != "local" {
		t.Fatalf("got %q, want %q", data, "local")
	}
}

func TestAsBackendRouter_ExistingRouter(t *testing.T) {
	r := NewBackendRouter()
	r.Register("gs", &fakeBackend{})
	r2 := AsBackendRouter(r)
	if r2 != r {
		t.Fatal("AsBackendRouter should return the same router if already a *BackendRouter")
	}
}

func TestAsBackendRouter_WrapPlainBackend(t *testing.T) {
	r := AsBackendRouter(LocalBackend)
	// Should work for local paths
	dir := t.TempDir()
	p := filepath.Join(dir, "test.txt")
	os.WriteFile(p, []byte("wrapped"), 0644)
	rc, err := r.Open(p)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(rc)
	rc.Close()
	if string(data) != "wrapped" {
		t.Fatalf("got %q, want %q", data, "wrapped")
	}
}

// fakeBackend is a test double for scheme-based routing tests.
type fakeBackend struct {
	openData string
}

func (f *fakeBackend) Open(_ string) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(f.openData)), nil
}
func (f *fakeBackend) Create(_ string) (io.WriteCloser, error) {
	return NewFakeWriteCloser(&bytes.Buffer{}), nil
}
func (f *fakeBackend) Glob(_ string) ([]string, error) {
	return nil, nil
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
