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

func TestWriteFiles_Default(t *testing.T) {
	dir := t.TempDir()

	pipeline := gomr.NewPipeline()

	data := []record{{"alice", 10}, {"bob", 20}, {"charlie", 30}}
	collection := gomr.NewSeedCollection(pipeline,
		func(_ gomr.OperatorContext, emitter gomr.Emitter[record]) {
			for i := range data {
				*emitter.GetEmitPointer() = data[i]
			}
		},
	)

	// No WithCustomWriter — should write directly.
	fileio.WriteFiles(collection, &recordSerializer{}, dir,
		fileio.WithNumShards(1),
	)

	pipeline.WaitForCompletion()

	got, err := os.ReadFile(filepath.Join(dir, "output.csv"))
	if err != nil {
		t.Fatalf("failed to read output: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(got)), "\n")
	slices.Sort(lines)
	expected := []string{"alice,10", "bob,20", "charlie,30"}
	slices.Sort(expected)

	if !slices.Equal(lines, expected) {
		t.Errorf("default WriteFiles mismatch.\nExpected: %v\nGot:      %v", expected, lines)
	}
}

// uppercaseWriter wraps writes so all bytes are uppercased.
type uppercaseWriter struct {
	w io.Writer
}

func (u *uppercaseWriter) Write(p []byte) (int, error) {
	upper := []byte(strings.ToUpper(string(p)))
	return u.w.Write(upper)
}

func (u *uppercaseWriter) Close() error { return nil }

func TestWriteFiles_WithCustomWriter(t *testing.T) {
	dir := t.TempDir()

	pipeline := gomr.NewPipeline()

	data := []record{{"alice", 10}, {"bob", 20}}
	collection := gomr.NewSeedCollection(pipeline,
		func(_ gomr.OperatorContext, emitter gomr.Emitter[record]) {
			for i := range data {
				*emitter.GetEmitPointer() = data[i]
			}
		},
	)

	// WithCustomWriter uppercases all output.
	fileio.WriteFiles(collection, &recordSerializer{}, dir,
		fileio.WithCustomWriter(func(w io.Writer) io.WriteCloser {
			return &uppercaseWriter{w: w}
		}),
		fileio.WithNumShards(1),
	)

	pipeline.WaitForCompletion()

	got, err := os.ReadFile(filepath.Join(dir, "output.csv"))
	if err != nil {
		t.Fatalf("failed to read output: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(got)), "\n")
	slices.Sort(lines)
	expected := []string{"ALICE,10", "BOB,20"}
	slices.Sort(expected)

	if !slices.Equal(lines, expected) {
		t.Errorf("WithCustomWriter mismatch.\nExpected: %v\nGot:      %v", expected, lines)
	}
}
