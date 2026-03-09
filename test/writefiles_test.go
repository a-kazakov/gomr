package test

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/a-kazakov/gomr"
	"github.com/a-kazakov/gomr/extensions/fileio"
)

type record struct {
	Name  string
	Value int
}

type recordSerializer struct{}

func (s *recordSerializer) MarshalFileName(v *record, dest []byte) int {
	return copy(dest, "output.csv")
}

func (s *recordSerializer) MarshalRecord(v *record, dest []byte) int {
	return copy(dest, fmt.Sprintf("%s,%d\n", v.Name, v.Value))
}

func seedRecords(pipeline gomr.Pipeline, data []record) gomr.Collection[record] {
	return gomr.NewSeedCollection(pipeline,
		func(_ gomr.OperatorContext, emitter gomr.Emitter[record]) {
			for i := range data {
				*emitter.GetEmitPointer() = data[i]
			}
		},
	)
}

func readOutputLines(t *testing.T, dir, filename string) []string {
	t.Helper()
	got, err := os.ReadFile(filepath.Join(dir, filename))
	if err != nil {
		t.Fatalf("failed to read output: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(got)), "\n")
	slices.Sort(lines)
	return lines
}

func TestWriteFiles_Default(t *testing.T) {
	dir := t.TempDir()
	pipeline, scratchDir := newTestPipeline(t)

	gomr.Ignore(fileio.WriteFiles(
		seedRecords(pipeline, []record{{"alice", 10}, {"bob", 20}, {"charlie", 30}}),
		&recordSerializer{}, dir,
		fileio.WithNumShards(1),
	))
	pipeline.WaitForCompletion()

	lines := readOutputLines(t, dir, "output.csv")
	expected := []string{"alice,10", "bob,20", "charlie,30"}
	slices.Sort(expected)

	if !slices.Equal(lines, expected) {
		t.Errorf("default WriteFiles mismatch.\nExpected: %v\nGot:      %v", expected, lines)
	}
	assertNoFiles(t, scratchDir)
}

func TestWriteFiles_WithCustomWriter(t *testing.T) {
	dir := t.TempDir()
	pipeline, scratchDir := newTestPipeline(t)

	// WithCustomWriter takes func(io.Writer) io.Writer — no Close needed.
	gomr.Ignore(fileio.WriteFiles(
		seedRecords(pipeline, []record{{"alice", 10}, {"bob", 20}}),
		&recordSerializer{}, dir,
		fileio.WithCustomWriter(func(w io.Writer) io.Writer {
			return &uppercaseWriter{w: w}
		}),
		fileio.WithNumShards(1),
	))
	pipeline.WaitForCompletion()

	lines := readOutputLines(t, dir, "output.csv")
	expected := []string{"ALICE,10", "BOB,20"}
	slices.Sort(expected)

	if !slices.Equal(lines, expected) {
		t.Errorf("WithCustomWriter mismatch.\nExpected: %v\nGot:      %v", expected, lines)
	}
	assertNoFiles(t, scratchDir)
}

func TestWriteFiles_WithCustomWriteCloser(t *testing.T) {
	dir := t.TempDir()
	pipeline, scratchDir := newTestPipeline(t)

	var closeCalled bool

	// WithCustomWriteCloser takes func(io.Writer) io.WriteCloser — Close is called.
	gomr.Ignore(fileio.WriteFiles(
		seedRecords(pipeline, []record{{"alice", 10}}),
		&recordSerializer{}, dir,
		fileio.WithCustomWriteCloser(func(w io.Writer) io.WriteCloser {
			return &trackingUppercaseWriter{w: w, closed: &closeCalled}
		}),
		fileio.WithNumShards(1),
	))
	pipeline.WaitForCompletion()

	lines := readOutputLines(t, dir, "output.csv")
	if !slices.Equal(lines, []string{"ALICE,10"}) {
		t.Errorf("WithCustomWriteCloser mismatch. Got: %v", lines)
	}
	if !closeCalled {
		t.Error("expected Close to be called on the custom WriteCloser")
	}
	assertNoFiles(t, scratchDir)
}

// uppercaseWriter wraps writes so all bytes are uppercased (no Close).
type uppercaseWriter struct {
	w io.Writer
}

func (u *uppercaseWriter) Write(p []byte) (int, error) {
	return u.w.Write([]byte(strings.ToUpper(string(p))))
}

// trackingUppercaseWriter uppercases and tracks whether Close was called.
type trackingUppercaseWriter struct {
	w      io.Writer
	closed *bool
}

func (u *trackingUppercaseWriter) Write(p []byte) (int, error) {
	return u.w.Write([]byte(strings.ToUpper(string(p))))
}

func (u *trackingUppercaseWriter) Close() error {
	*u.closed = true
	return nil
}
