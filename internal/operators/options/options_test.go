package options_test

import (
	"testing"
	"time"

	"github.com/a-kazakov/gomr/internal/core"
	"github.com/a-kazakov/gomr/internal/operators/options"
)

func TestApplyFunctions(t *testing.T) {
	t.Run("empty options return unset fields", func(t *testing.T) {
		seed := options.ApplySeedOptions()
		if seed.OperationName.IsSet() {
			t.Error("seed OperationName should be empty")
		}
		fork := options.ApplyForkOptions()
		if fork.OperationName.IsSet() {
			t.Error("fork OperationName should be empty")
		}
		merge := options.ApplyMergeOptions()
		if merge.OperationName.IsSet() {
			t.Error("merge OperationName should be empty")
		}
		mp := options.ApplyMapOptions()
		if mp.OperationName.IsSet() {
			t.Error("map OperationName should be empty")
		}
		shuf := options.ApplyShuffleOptions()
		if shuf.OperationName.IsSet() {
			t.Error("shuffle OperationName should be empty")
		}
		coll := options.ApplyCollectOptions()
		if coll.OperationName.IsSet() {
			t.Error("collect OperationName should be empty")
		}
		mv := options.ApplyMapValueOptions()
		if mv.OperationName.IsSet() {
			t.Error("mapvalue OperationName should be empty")
		}
		ig := options.ApplyIgnoreOptions()
		if ig.OperationName.IsSet() {
			t.Error("ignore OperationName should be empty")
		}
		tc := options.ApplyToCollectionOptions()
		if tc.OperationName.IsSet() {
			t.Error("tocollection OperationName should be empty")
		}
		sb := options.ApplySpillBufferOptions()
		if sb.OperationName.IsSet() {
			t.Error("spillbuffer OperationName should be empty")
		}
	})

	t.Run("multiple options compose correctly", func(t *testing.T) {
		opts := options.ApplyMapOptions(
			options.WithOperationName("mymap"),
			options.WithParallelism(4),
			options.WithOutBatchSize(128),
			options.WithOutChannelCapacity(16),
			options.WithOutCollectionNames("out1"),
			options.WithUserOperatorContext("ctx"),
		)
		if opts.OperationName.Get() != "mymap" {
			t.Error("OperationName mismatch")
		}
		if opts.Parallelism.Get() != 4 {
			t.Error("Parallelism mismatch")
		}
		if opts.OutBatchSize.Get() != 128 {
			t.Error("OutBatchSize mismatch")
		}
		if opts.OutChannelCapacity.Get() != 16 {
			t.Error("OutChannelCapacity mismatch")
		}
		if opts.OutCollectionNames.Get()[0] != "out1" {
			t.Error("OutCollectionNames mismatch")
		}
		if opts.UserOperatorContext.Get() != "ctx" {
			t.Error("UserOperatorContext mismatch")
		}
	})
}

func TestSharedOptions(t *testing.T) {
	t.Run("operation name propagates to all operator types", func(t *testing.T) {
		name := options.WithOperationName("myop")

		seed := options.ApplySeedOptions(name)
		if seed.OperationName.Get() != "myop" {
			t.Errorf("seed name = %q", seed.OperationName.Get())
		}
		fork := options.ApplyForkOptions(name)
		if fork.OperationName.Get() != "myop" {
			t.Errorf("fork name = %q", fork.OperationName.Get())
		}
		merge := options.ApplyMergeOptions(name)
		if merge.OperationName.Get() != "myop" {
			t.Errorf("merge name = %q", merge.OperationName.Get())
		}
		mp := options.ApplyMapOptions(name)
		if mp.OperationName.Get() != "myop" {
			t.Errorf("map name = %q", mp.OperationName.Get())
		}
		shuf := options.ApplyShuffleOptions(name)
		if shuf.OperationName.Get() != "myop" {
			t.Errorf("shuffle name = %q", shuf.OperationName.Get())
		}
		coll := options.ApplyCollectOptions(name)
		if coll.OperationName.Get() != "myop" {
			t.Errorf("collect name = %q", coll.OperationName.Get())
		}
		mv := options.ApplyMapValueOptions(name)
		if mv.OperationName.Get() != "myop" {
			t.Errorf("mapvalue name = %q", mv.OperationName.Get())
		}
		ig := options.ApplyIgnoreOptions(name)
		if ig.OperationName.Get() != "myop" {
			t.Errorf("ignore name = %q", ig.OperationName.Get())
		}
		tc := options.ApplyToCollectionOptions(name)
		if tc.OperationName.Get() != "myop" {
			t.Errorf("tocollection name = %q", tc.OperationName.Get())
		}
		sb := options.ApplySpillBufferOptions(name)
		if sb.OperationName.Get() != "myop" {
			t.Errorf("spillbuffer name = %q", sb.OperationName.Get())
		}
	})

	t.Run("parallelism applies to map and collect", func(t *testing.T) {
		p := options.WithParallelism(8)

		mp := options.ApplyMapOptions(p)
		if mp.Parallelism.Get() != 8 {
			t.Errorf("map = %d", mp.Parallelism.Get())
		}
		coll := options.ApplyCollectOptions(p)
		if coll.Parallelism.Get() != 8 {
			t.Errorf("collect = %d", coll.Parallelism.Get())
		}
	})

	t.Run("out collection names apply to fork map and shuffle", func(t *testing.T) {
		names := options.WithOutCollectionNames("a", "b")

		fork := options.ApplyForkOptions(names)
		if v := fork.OutCollectionNames.Get(); len(v) != 2 || v[0] != "a" || v[1] != "b" {
			t.Errorf("fork OutCollectionNames = %v", v)
		}
		mp := options.ApplyMapOptions(names)
		if v := mp.OutCollectionNames.Get(); len(v) != 2 {
			t.Errorf("map OutCollectionNames = %v", v)
		}
		shuf := options.ApplyShuffleOptions(names)
		if v := shuf.OutCollectionNames.Get(); len(v) != 2 {
			t.Errorf("shuffle OutCollectionNames = %v", v)
		}
	})

	t.Run("out collection name applies to seed merge tocollection and spillbuffer", func(t *testing.T) {
		name := options.WithOutCollectionName("out")

		seed := options.ApplySeedOptions(name)
		if seed.OutCollectionName.Get() != "out" {
			t.Errorf("seed = %q", seed.OutCollectionName.Get())
		}
		merge := options.ApplyMergeOptions(name)
		if merge.OutCollectionName.Get() != "out" {
			t.Errorf("merge = %q", merge.OutCollectionName.Get())
		}
		tc := options.ApplyToCollectionOptions(name)
		if tc.OutCollectionName.Get() != "out" {
			t.Errorf("tocollection = %q", tc.OutCollectionName.Get())
		}
		sb := options.ApplySpillBufferOptions(name)
		if sb.OutCollectionName.Get() != "out" {
			t.Errorf("spillbuffer = %q", sb.OutCollectionName.Get())
		}
	})

	t.Run("out batch size applies to relevant operators", func(t *testing.T) {
		bs := options.WithOutBatchSize(256)

		seed := options.ApplySeedOptions(bs)
		if seed.OutBatchSize.Get() != 256 {
			t.Errorf("seed = %d", seed.OutBatchSize.Get())
		}
		mp := options.ApplyMapOptions(bs)
		if mp.OutBatchSize.Get() != 256 {
			t.Errorf("map = %d", mp.OutBatchSize.Get())
		}
		shuf := options.ApplyShuffleOptions(bs)
		if shuf.OutBatchSize.Get() != 256 {
			t.Errorf("shuffle = %d", shuf.OutBatchSize.Get())
		}
		tc := options.ApplyToCollectionOptions(bs)
		if tc.OutBatchSize.Get() != 256 {
			t.Errorf("tocollection = %d", tc.OutBatchSize.Get())
		}
		sb := options.ApplySpillBufferOptions(bs)
		if sb.OutBatchSize.Get() != 256 {
			t.Errorf("spillbuffer = %d", sb.OutBatchSize.Get())
		}
	})

	t.Run("out channel capacity applies to all except ignore", func(t *testing.T) {
		cap := options.WithOutChannelCapacity(42)

		seed := options.ApplySeedOptions(cap)
		if seed.OutChannelCapacity.Get() != 42 {
			t.Errorf("seed = %d", seed.OutChannelCapacity.Get())
		}
		fork := options.ApplyForkOptions(cap)
		if fork.OutChannelCapacity.Get() != 42 {
			t.Errorf("fork = %d", fork.OutChannelCapacity.Get())
		}
		merge := options.ApplyMergeOptions(cap)
		if merge.OutChannelCapacity.Get() != 42 {
			t.Errorf("merge = %d", merge.OutChannelCapacity.Get())
		}
		mp := options.ApplyMapOptions(cap)
		if mp.OutChannelCapacity.Get() != 42 {
			t.Errorf("map = %d", mp.OutChannelCapacity.Get())
		}
		shuf := options.ApplyShuffleOptions(cap)
		if shuf.OutChannelCapacity.Get() != 42 {
			t.Errorf("shuffle = %d", shuf.OutChannelCapacity.Get())
		}
		coll := options.ApplyCollectOptions(cap)
		if coll.OutChannelCapacity.Get() != 42 {
			t.Errorf("collect = %d", coll.OutChannelCapacity.Get())
		}
		mv := options.ApplyMapValueOptions(cap)
		if mv.OutChannelCapacity.Get() != 42 {
			t.Errorf("mapvalue = %d", mv.OutChannelCapacity.Get())
		}
		tc := options.ApplyToCollectionOptions(cap)
		if tc.OutChannelCapacity.Get() != 42 {
			t.Errorf("tocollection = %d", tc.OutChannelCapacity.Get())
		}
		sb := options.ApplySpillBufferOptions(cap)
		if sb.OutChannelCapacity.Get() != 42 {
			t.Errorf("spillbuffer = %d", sb.OutChannelCapacity.Get())
		}
	})

	t.Run("user operator context applies to relevant operators", func(t *testing.T) {
		ctx := options.WithUserOperatorContext("my_ctx")

		seed := options.ApplySeedOptions(ctx)
		if seed.UserOperatorContext.Get() != "my_ctx" {
			t.Errorf("seed = %v", seed.UserOperatorContext.Get())
		}
		mp := options.ApplyMapOptions(ctx)
		if mp.UserOperatorContext.Get() != "my_ctx" {
			t.Errorf("map = %v", mp.UserOperatorContext.Get())
		}
		shuf := options.ApplyShuffleOptions(ctx)
		if shuf.UserOperatorContext.Get() != "my_ctx" {
			t.Errorf("shuffle = %v", shuf.UserOperatorContext.Get())
		}
		coll := options.ApplyCollectOptions(ctx)
		if coll.UserOperatorContext.Get() != "my_ctx" {
			t.Errorf("collect = %v", coll.UserOperatorContext.Get())
		}
		mv := options.ApplyMapValueOptions(ctx)
		if mv.UserOperatorContext.Get() != "my_ctx" {
			t.Errorf("mapvalue = %v", mv.UserOperatorContext.Get())
		}
		sb := options.ApplySpillBufferOptions(ctx)
		if sb.UserOperatorContext.Get() != "my_ctx" {
			t.Errorf("spillbuffer = %v", sb.UserOperatorContext.Get())
		}
	})
}

func TestSpecificOptions(t *testing.T) {
	t.Run("shuffle-specific options", func(t *testing.T) {
		opts := options.ApplyShuffleOptions(
			options.WithNumShards(64),
			options.WithScatterParallelism(4),
			options.WithGatherParallelism(2),
			options.WithLocalShuffleBufferSize(1024*1024),
			options.WithLocalShuffleBufferSizeJitter(0.1),
			options.WithShuffleReadBufferSize(8192),
			options.WithShuffleWriteBufferSize(4096),
			options.WithFileMergeThreshold(10),
			options.WithScratchSpacePaths("/tmp/a", "/tmp/b"),
			options.WithTargetWriteLatency(100*time.Millisecond),
		)

		if opts.NumShards.Get() != 64 {
			t.Errorf("NumShards = %d", opts.NumShards.Get())
		}
		if opts.ScatterParallelism.Get() != 4 {
			t.Errorf("ScatterParallelism = %d", opts.ScatterParallelism.Get())
		}
		if opts.GatherParallelism.Get() != 2 {
			t.Errorf("GatherParallelism = %d", opts.GatherParallelism.Get())
		}
		if opts.LocalShuffleBufferSize.Get() != 1024*1024 {
			t.Errorf("LocalShuffleBufferSize = %d", opts.LocalShuffleBufferSize.Get())
		}
		if opts.LocalShuffleBufferSizeJitter.Get() != 0.1 {
			t.Errorf("LocalShuffleBufferSizeJitter = %f", opts.LocalShuffleBufferSizeJitter.Get())
		}
		if opts.ReadBufferSize.Get() != 8192 {
			t.Errorf("ReadBufferSize = %d", opts.ReadBufferSize.Get())
		}
		if opts.WriteBufferSize.Get() != 4096 {
			t.Errorf("WriteBufferSize = %d", opts.WriteBufferSize.Get())
		}
		if opts.FileMergeThreshold.Get() != 10 {
			t.Errorf("FileMergeThreshold = %d", opts.FileMergeThreshold.Get())
		}
		if paths := opts.ScratchSpacePaths.Get(); len(paths) != 2 || paths[0] != "/tmp/a" {
			t.Errorf("ScratchSpacePaths = %v", paths)
		}
		if opts.TargetWriteLatency.Get() != 100*time.Millisecond {
			t.Errorf("TargetWriteLatency = %v", opts.TargetWriteLatency.Get())
		}
	})

	t.Run("spill buffer-specific options", func(t *testing.T) {
		opts := options.ApplySpillBufferOptions(
			options.WithSpillDirectories("/tmp/spill1", "/tmp/spill2"),
			options.WithMaxSpillFileSize(1024*1024*10),
			options.WithSpillWriteBufferSize(4096),
			options.WithSpillReadBufferSize(8192),
			options.WithSpillWriteParallelism(2),
			options.WithSpillReadParallelism(3),
		)

		if dirs := opts.SpillDirectories.Get(); len(dirs) != 2 || dirs[0] != "/tmp/spill1" {
			t.Errorf("SpillDirectories = %v", dirs)
		}
		if opts.MaxSpillFileSize.Get() != 1024*1024*10 {
			t.Errorf("MaxSpillFileSize = %d", opts.MaxSpillFileSize.Get())
		}
		if opts.WriteBufferSize.Get() != 4096 {
			t.Errorf("WriteBufferSize = %d", opts.WriteBufferSize.Get())
		}
		if opts.ReadBufferSize.Get() != 8192 {
			t.Errorf("ReadBufferSize = %d", opts.ReadBufferSize.Get())
		}
		if opts.WriteParallelism.Get() != 2 {
			t.Errorf("WriteParallelism = %d", opts.WriteParallelism.Get())
		}
		if opts.ReadParallelism.Get() != 3 {
			t.Errorf("ReadParallelism = %d", opts.ReadParallelism.Get())
		}
	})

	t.Run("compression algorithm applies to shuffle and spillbuffer", func(t *testing.T) {
		alg := options.WithCompressionAlgorithm(core.CompressionAlgorithmLz4)

		shuf := options.ApplyShuffleOptions(alg)
		if shuf.CompressionAlgorithm.Get() != core.CompressionAlgorithmLz4 {
			t.Errorf("shuffle = %d", shuf.CompressionAlgorithm.Get())
		}
		sb := options.ApplySpillBufferOptions(alg)
		if sb.CompressionAlgorithm.Get() != core.CompressionAlgorithmLz4 {
			t.Errorf("spillbuffer = %d", sb.CompressionAlgorithm.Get())
		}
	})

	t.Run("out value name applies to collect", func(t *testing.T) {
		name := options.WithOutValueName("val")
		coll := options.ApplyCollectOptions(name)
		if coll.OutValueName.Get() != "val" {
			t.Errorf("collect = %q", coll.OutValueName.Get())
		}
	})

	t.Run("out value names applies to mapvalue", func(t *testing.T) {
		names := options.WithOutValueNames("v1", "v2")
		mv := options.ApplyMapValueOptions(names)
		if v := mv.OutValueNames.Get(); len(v) != 2 || v[0] != "v1" {
			t.Errorf("mapvalue = %v", v)
		}
	})
}
