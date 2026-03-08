package metrics

// Tests use package-internal access for: toSnapshot() method on metric types, pushOnce(), internal metric struct fields.

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/a-kazakov/gomr/internal/core"
)

// newTestServerOrSkip creates an httptest.Server, skipping the test if the
// sandbox blocks network binding.
func newTestServerOrSkip(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("cannot listen on loopback (sandbox?): %v", err)
	}
	server := &httptest.Server{
		Listener: ln,
		Config:   &http.Server{Handler: handler},
	}
	server.Start()
	return server
}

// =============================================================================
// PipelineMetrics
// =============================================================================

func TestPipelineMetrics(t *testing.T) {
	t.Run("new", func(t *testing.T) {
		pm := NewPipelineMetrics()
		if pm == nil {
			t.Fatal("NewPipelineMetrics returned nil")
		}
		if len(pm.Operations) != 0 {
			t.Errorf("Operations should be empty, got %d", len(pm.Operations))
		}
		if len(pm.Collections) != 0 {
			t.Errorf("Collections should be empty, got %d", len(pm.Collections))
		}
		if len(pm.Values) != 0 {
			t.Errorf("Values should be empty, got %d", len(pm.Values))
		}
	})

	t.Run("add operation", func(t *testing.T) {
		pm := NewPipelineMetrics()
		op := pm.AddOperation(core.OPERATION_KIND_MAP, "test_op")
		if op == nil {
			t.Fatal("AddOperation returned nil")
		}
		if op.Name != "test_op" {
			t.Errorf("Name = %q, want test_op", op.Name)
		}
		if op.Kind != core.OPERATION_KIND_MAP {
			t.Errorf("Kind = %q, want %q", op.Kind, core.OPERATION_KIND_MAP)
		}
		if op.Id == "" {
			t.Error("Id should not be empty")
		}
		if op.Shuffle != nil {
			t.Error("non-shuffle operation should have nil Shuffle")
		}
		if len(pm.Operations) != 1 {
			t.Errorf("Operations count = %d, want 1", len(pm.Operations))
		}
	})

	t.Run("add operation shuffle", func(t *testing.T) {
		pm := NewPipelineMetrics()
		op := pm.AddOperation(core.OPERATION_KIND_SHUFFLE, "shuffle_op")
		if op.Shuffle == nil {
			t.Error("shuffle operation should have non-nil Shuffle")
		}
	})

	t.Run("add collection", func(t *testing.T) {
		pm := NewPipelineMetrics()
		coll := pm.AddCollection("test_coll", 1024, func() (int, int) { return 5, 100 })
		if coll == nil {
			t.Fatal("AddCollection returned nil")
		}
		if coll.Name != "test_coll" {
			t.Errorf("Name = %q, want test_coll", coll.Name)
		}
		if coll.BatchSize != 1024 {
			t.Errorf("BatchSize = %d, want 1024", coll.BatchSize)
		}
		if len(pm.Collections) != 1 {
			t.Errorf("Collections count = %d, want 1", len(pm.Collections))
		}
	})

	t.Run("add value", func(t *testing.T) {
		pm := NewPipelineMetrics()
		val := pm.AddValue("test_val", func() bool { return true })
		if val == nil {
			t.Fatal("AddValue returned nil")
		}
		if val.Name != "test_val" {
			t.Errorf("Name = %q, want test_val", val.Name)
		}
		if len(pm.Values) != 1 {
			t.Errorf("Values count = %d, want 1", len(pm.Values))
		}
	})

	t.Run("user counters", func(t *testing.T) {
		pm := NewPipelineMetrics()
		op := pm.AddOperation(core.OPERATION_KIND_MAP, "op")
		coll := pm.AddCollection("c", 10, func() (int, int) { return 0, 10 })
		val := pm.AddValue("v", func() bool { return false })

		ic := op.AddInputCollection(coll)
		if ic.Id != coll.Id {
			t.Error("input collection id mismatch")
		}

		oc := op.AddOutputCollection(coll)
		if oc.Id != coll.Id {
			t.Error("output collection id mismatch")
		}

		iv := op.AddInputValue(val)
		if iv.Id != val.Id {
			t.Error("input value id mismatch")
		}

		ov := op.AddOutputValue(val)
		if ov.Id != val.Id {
			t.Error("output value id mismatch")
		}

		if len(op.InputCollections) != 1 || len(op.OutputCollections) != 1 {
			t.Error("collections not added")
		}
		if len(op.InputValues) != 1 || len(op.OutputValues) != 1 {
			t.Error("values not added")
		}
	})
}

// =============================================================================
// OperationMetrics
// =============================================================================

func TestOperationMetrics(t *testing.T) {
	t.Run("set phase", func(t *testing.T) {
		pm := NewPipelineMetrics()
		op := pm.AddOperation(core.OPERATION_KIND_MAP, "op")
		op.SetPhase(core.PHASE_RUNNING)
		if op.CurrentPhase.Load() != core.PHASE_RUNNING {
			t.Errorf("phase = %d, want %d", op.CurrentPhase.Load(), core.PHASE_RUNNING)
		}
		if len(op.Phases) != 1 {
			t.Errorf("phases count = %d, want 1", len(op.Phases))
		}
		// Setting same phase again should not add another entry
		op.SetPhase(core.PHASE_RUNNING)
		if len(op.Phases) != 1 {
			t.Errorf("phases count = %d, want 1 (no duplicate)", len(op.Phases))
		}
	})

	t.Run("try set phase", func(t *testing.T) {
		pm := NewPipelineMetrics()
		op := pm.AddOperation(core.OPERATION_KIND_MAP, "op")
		// Should succeed
		if !op.TrySetPhase(core.PHASE_PENDING, core.PHASE_RUNNING) {
			t.Error("TrySetPhase should succeed")
		}
		if op.CurrentPhase.Load() != core.PHASE_RUNNING {
			t.Errorf("phase = %d, want %d", op.CurrentPhase.Load(), core.PHASE_RUNNING)
		}
		// Should fail (current is RUNNING, not PENDING)
		if op.TrySetPhase(core.PHASE_PENDING, core.PHASE_COMPLETED) {
			t.Error("TrySetPhase should fail for wrong expected phase")
		}
	})

	t.Run("set phase concurrent", func(t *testing.T) {
		pm := NewPipelineMetrics()
		op := pm.AddOperation(core.OPERATION_KIND_MAP, "op")
		done := make(chan struct{})
		var count atomic.Int32
		for i := 0; i < 10; i++ {
			go func(phase int32) {
				op.SetPhase(phase)
				count.Add(1)
				if count.Load() == 10 {
					close(done)
				}
			}(int32(i % 3))
		}
		<-done
	})

	t.Run("input output collections", func(t *testing.T) {
		pm := NewPipelineMetrics()
		op := pm.AddOperation(core.OPERATION_KIND_MAP, "op")
		coll := pm.AddCollection("c", 10, func() (int, int) { return 0, 10 })
		val := pm.AddValue("v", func() bool { return false })

		ic := op.AddInputCollection(coll)
		if ic.Id != coll.Id {
			t.Error("input collection id mismatch")
		}

		oc := op.AddOutputCollection(coll)
		if oc.Id != coll.Id {
			t.Error("output collection id mismatch")
		}

		iv := op.AddInputValue(val)
		if iv.Id != val.Id {
			t.Error("input value id mismatch")
		}

		ov := op.AddOutputValue(val)
		if ov.Id != val.Id {
			t.Error("output value id mismatch")
		}

		if len(op.InputCollections) != 1 || len(op.OutputCollections) != 1 {
			t.Error("collections not added")
		}
		if len(op.InputValues) != 1 || len(op.OutputValues) != 1 {
			t.Error("values not added")
		}
	})

	t.Run("parallelism", func(t *testing.T) {
		pm := NewPipelineMetrics()
		op := pm.AddOperation(core.OPERATION_KIND_MAP, "op")
		op.Parallelism = 4
		snap := op.toSnapshot()
		if snap.Parallelism != 4 {
			t.Errorf("Parallelism = %d, want 4", snap.Parallelism)
		}
	})
}

// =============================================================================
// Snapshots
// =============================================================================

func TestSnapshots(t *testing.T) {
	t.Run("pipeline snapshot", func(t *testing.T) {
		pm := NewPipelineMetrics()
		op := pm.AddOperation(core.OPERATION_KIND_MAP, "map_op")
		coll := pm.AddCollection("coll", 256, func() (int, int) { return 3, 100 })
		val := pm.AddValue("val", func() bool { return true })
		op.AddInputCollection(coll)
		op.AddOutputCollection(coll)
		op.AddInputValue(val)
		op.AddOutputValue(val)
		op.SetPhase(core.PHASE_RUNNING)

		snap := pm.toSnapshot()
		if snap == nil {
			t.Fatal("snapshot is nil")
		}
		if len(snap.Operations) != 1 {
			t.Errorf("operations = %d, want 1", len(snap.Operations))
		}
		if len(snap.Collections) != 1 {
			t.Errorf("collections = %d, want 1", len(snap.Collections))
		}
		if len(snap.Values) != 1 {
			t.Errorf("values = %d, want 1", len(snap.Values))
		}
	})

	t.Run("operation snapshot", func(t *testing.T) {
		pm := NewPipelineMetrics()
		op := pm.AddOperation(core.OPERATION_KIND_MAP, "op")
		op.Parallelism = 4
		op.SetPhase(core.PHASE_RUNNING)

		snap := op.toSnapshot()
		if snap.Name != "op" {
			t.Errorf("Name = %q", snap.Name)
		}
		if snap.Kind != core.OPERATION_KIND_MAP {
			t.Errorf("Kind = %q", snap.Kind)
		}
		if snap.Phase != "running" {
			t.Errorf("Phase = %q, want running", snap.Phase)
		}
		if snap.Parallelism != 4 {
			t.Errorf("Parallelism = %d, want 4", snap.Parallelism)
		}
	})

	t.Run("shuffle snapshot", func(t *testing.T) {
		pm := NewPipelineMetrics()
		op := pm.AddOperation(core.OPERATION_KIND_SHUFFLE, "shuf")
		op.Shuffle.SpillsCount.Store(5)
		op.Shuffle.DiskUsage.Store(1000)
		op.Shuffle.ElementsGathered.Store(50)
		op.Shuffle.GroupsGathered.Store(10)

		snap := op.Shuffle.toSnapshot()
		if snap.SpillsCount != 5 {
			t.Errorf("SpillsCount = %d, want 5", snap.SpillsCount)
		}
		if snap.DiskUsage != 1000 {
			t.Errorf("DiskUsage = %d, want 1000", snap.DiskUsage)
		}
	})

	t.Run("shuffle snapshot nil", func(t *testing.T) {
		var s *ShuffleMetrics
		snap := s.toSnapshot()
		if snap != nil {
			t.Error("nil ShuffleMetrics should produce nil snapshot")
		}
	})

	t.Run("collection snapshot", func(t *testing.T) {
		pm := NewPipelineMetrics()
		coll := pm.AddCollection("c", 512, func() (int, int) { return 7, 50 })
		coll.Completed = true
		snap := coll.toSnapshot()
		if snap.Name != "c" {
			t.Errorf("Name = %q", snap.Name)
		}
		if snap.BatchSize != 512 {
			t.Errorf("BatchSize = %d, want 512", snap.BatchSize)
		}
		if snap.Pressure != 7 {
			t.Errorf("Pressure = %d, want 7", snap.Pressure)
		}
		if snap.Capacity != 50 {
			t.Errorf("Capacity = %d, want 50", snap.Capacity)
		}
		if !snap.Completed {
			t.Error("Completed should be true")
		}
	})

	t.Run("value snapshot", func(t *testing.T) {
		pm := NewPipelineMetrics()
		val := pm.AddValue("v", func() bool { return true })
		snap := val.toSnapshot()
		if snap.Name != "v" {
			t.Errorf("Name = %q", snap.Name)
		}
		if !snap.IsResolved {
			t.Error("IsResolved should be true")
		}
	})

	t.Run("input output snapshots", func(t *testing.T) {
		ic := &InputCollectionMetrics{Id: "c1"}
		ic.ElementsConsumed.Store(100)
		ic.BatchesConsumed.Store(10)
		snap := ic.toSnapshot()
		if snap.ElementsConsumed != 100 || snap.BatchesConsumed != 10 {
			t.Error("input collection snapshot mismatch")
		}

		oc := &OutputCollectionMetrics{Id: "c2"}
		oc.ElementsProduced.Store(200)
		oc.BatchesProduced.Store(20)
		snapOc := oc.toSnapshot()
		if snapOc.ElementsProduced != 200 || snapOc.BatchesProduced != 20 {
			t.Error("output collection snapshot mismatch")
		}

		iv := &InputValueMetrics{Id: "v1", IsConsumed: true}
		snapIv := iv.toSnapshot()
		if !snapIv.IsConsumed || snapIv.Id != "v1" {
			t.Error("input value snapshot mismatch")
		}

		ov := &OutputValueMetrics{Id: "v2", IsProduced: true}
		snapOv := ov.toSnapshot()
		if !snapOv.IsProduced || snapOv.Id != "v2" {
			t.Error("output value snapshot mismatch")
		}
	})

	t.Run("snapshot json", func(t *testing.T) {
		pm := NewPipelineMetrics()
		pm.AddOperation(core.OPERATION_KIND_MAP, "op")
		pm.AddCollection("c", 256, func() (int, int) { return 0, 10 })
		pm.AddValue("v", func() bool { return false })

		snap := pm.toSnapshot()
		data, err := json.Marshal(snap)
		if err != nil {
			t.Fatalf("JSON marshal error: %v", err)
		}
		if len(data) == 0 {
			t.Error("JSON should not be empty")
		}
	})
}

// =============================================================================
// MetricsPusher
// =============================================================================

func TestMetricsPusher(t *testing.T) {
	t.Run("empty url no-ops", func(t *testing.T) {
		pm := NewPipelineMetrics()
		mp := NewMetricsPusher("", time.Second, "job1", pm)
		mp.Start() // should not start goroutine
		mp.Stop()
	})

	t.Run("push once", func(t *testing.T) {
		var received bool
		server := newTestServerOrSkip(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			received = true
			if r.Method != "POST" {
				t.Errorf("method = %s, want POST", r.Method)
			}
			if r.Header.Get("Content-Type") != "application/json" {
				t.Errorf("content-type = %q, want application/json", r.Header.Get("Content-Type"))
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		pm := NewPipelineMetrics()
		pm.AddOperation(core.OPERATION_KIND_MAP, "op")
		mp := NewMetricsPusher(server.URL, time.Second, "testjob", pm)
		mp.pushOnce()
		if !received {
			t.Error("expected HTTP request to be received")
		}
	})

	t.Run("push once url path", func(t *testing.T) {
		var receivedPath string
		server := newTestServerOrSkip(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedPath = r.URL.Path
			w.WriteHeader(http.StatusCreated)
		}))
		defer server.Close()

		pm := NewPipelineMetrics()
		mp := NewMetricsPusher(server.URL, time.Second, "myjob", pm)
		mp.pushOnce()
		if receivedPath != "/sink/myjob" {
			t.Errorf("path = %q, want /sink/myjob", receivedPath)
		}
	})

	t.Run("start and stop", func(t *testing.T) {
		var count atomic.Int32
		server := newTestServerOrSkip(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			count.Add(1)
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		pm := NewPipelineMetrics()
		mp := NewMetricsPusher(server.URL, 50*time.Millisecond, "testjob", pm)
		mp.Start()
		time.Sleep(200 * time.Millisecond)
		mp.Stop()
		if count.Load() < 2 {
			t.Errorf("expected at least 2 pushes, got %d", count.Load())
		}
	})

	t.Run("nil metrics", func(t *testing.T) {
		mp := NewMetricsPusher("http://localhost", time.Second, "job1", nil)
		// pushOnce with nil metrics should not panic
		mp.pushOnce()
	})

	t.Run("negative interval", func(t *testing.T) {
		pm := NewPipelineMetrics()
		mp := NewMetricsPusher("http://localhost", -1, "job1", pm)
		mp.Start() // should not start goroutine due to interval <= 0
		mp.Stop()
	})

	t.Run("non-ok status", func(t *testing.T) {
		server := newTestServerOrSkip(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer server.Close()

		pm := NewPipelineMetrics()
		mp := NewMetricsPusher(server.URL, time.Second, "job1", pm)
		// Should not panic on non-OK status
		mp.pushOnce()
	})

	t.Run("bad url", func(t *testing.T) {
		pm := NewPipelineMetrics()
		mp := NewMetricsPusher("http://127.0.0.1:1", time.Second, "job1", pm)
		// Should not panic on connection error
		mp.pushOnce()
	})

	t.Run("invalid url", func(t *testing.T) {
		pm := NewPipelineMetrics()
		mp := NewMetricsPusher("://bad", time.Second, "job1", pm)
		// Should not panic on bad URL
		mp.pushOnce()
	})
}
