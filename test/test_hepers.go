package test

import (
	"cmp"
	"io/fs"
	"iter"
	"path/filepath"
	"slices"
	"testing"

	"github.com/a-kazakov/gomr"
	"github.com/a-kazakov/gomr/parameters"
)

func collectToSliceValue[T any](collection gomr.Collection[T]) gomr.Value[[]T] {
	return gomr.Collect(collection, func(ctx gomr.OperatorContext, receiver gomr.CollectionReceiver[T]) []T {
		result := make([]T, 0)
		for value := range receiver.IterValues() {
			result = append(result, *value)
		}
		return result
	}, func(ctx gomr.OperatorContext, intermediates [][]T) []T {
		result := make([]T, 0)
		for idx := range intermediates {
			result = append(result, intermediates[idx]...)
		}
		return result
	})
}

func verifySliceValue[T cmp.Ordered](t *testing.T, actualValue gomr.Value[[]T], expectedIterator iter.Seq[T]) {
	expected := make([]T, 0)
	for value := range expectedIterator {
		expected = append(expected, value)
	}
	slices.Sort(expected)
	actual := actualValue.Wait()
	slices.Sort(actual)
	if !slices.Equal(actual, expected) {
		t.Errorf("Expected %v, got %v", expected, actual)
	}
}

func createSliceValue[T any](pipeline gomr.Pipeline, value []T) gomr.Value[[]T] {
	collection := gomr.NewSeedCollection(pipeline, func(ctx gomr.OperatorContext, emitter gomr.Emitter[T]) {
		for i := range value {
			*emitter.GetEmitPointer() = value[i]
		}
	})
	return collectToSliceValue(collection)
}

// newTestPipeline creates a pipeline whose scratch space is a test-owned temp
// directory.  The temp directory is cleaned up automatically by t.TempDir()
// even if the test panics.  A t.Cleanup hook verifies that no regular files
// remain in the scratch directory after the test completes.
func newTestPipeline(t *testing.T) gomr.Pipeline {
	t.Helper()
	scratchDir := t.TempDir()
	params := parameters.NewParameters()
	if err := params.LoadFromSource(func(key string) (string, bool) {
		if key == "disk.scratch_paths" {
			return scratchDir, true
		}
		return "", false
	}); err != nil {
		t.Fatalf("failed to configure scratch path: %v", err)
	}
	t.Cleanup(func() {
		assertNoFiles(t, scratchDir)
	})
	return gomr.NewPipelineWithParameters(params)
}

// assertNoFiles walks dir and fails the test if any regular file is found.
// Empty directories are acceptable.
func assertNoFiles(t *testing.T, dir string) {
	t.Helper()
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			t.Errorf("scratch directory not clean: found file %s", path)
		}
		return nil
	})
	if err != nil {
		t.Errorf("failed to walk scratch directory: %v", err)
	}
}

func interleave[T any](seq1, seq2 iter.Seq[T]) iter.Seq[T] {
	return func(yield func(T) bool) {
		next1, stop1 := iter.Pull(seq1)
		defer stop1()
		next2, stop2 := iter.Pull(seq2)
		defer stop2()

		for {
			// Pull from the first iterator
			val1, ok1 := next1()
			if ok1 {
				if !yield(val1) {
					return
				}
			}

			// Pull from the second iterator
			val2, ok2 := next2()
			if ok2 {
				if !yield(val2) {
					return
				}
			}

			// If both are exhausted, we are done
			if !ok1 && !ok2 {
				return
			}
		}
	}
}
