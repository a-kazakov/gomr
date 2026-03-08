package main

import (
	"bytes"
	"fmt"
	"go/format"
	"log"
	"os"
	"strings"
	"text/template"
)

// ShuffleConfig represents a single shuffle variant configuration
type ShuffleConfig struct {
	NumInputs     int
	NumOutputs    int
	WithSideValue bool
}

// Computed properties as methods to keep templates clean
func (c ShuffleConfig) FuncName() string {
	suffix := ""
	if c.WithSideValue {
		suffix = "WithSideValue"
	}
	return fmt.Sprintf("Shuffle%dTo%d%s", c.NumInputs, c.NumOutputs, suffix)
}

func (c ShuffleConfig) ReducerType() string {
	return fmt.Sprintf("Reducer%dTo%d", c.NumInputs, c.NumOutputs)
}

func (c ShuffleConfig) SerializerInterface() string {
	return "ShuffleSerializer"
}

// Helper to generate a range [0, n)
func seq(n int) []int {
	res := make([]int, n)
	for i := range res {
		res[i] = i
	}
	return res
}

// ========== Config Generators ==========

func generateAllVariants(configs []ShuffleConfig) []ShuffleConfig {
	result := make([]ShuffleConfig, 0, len(configs)*2)
	for _, c := range configs {
		result = append(result, ShuffleConfig{c.NumInputs, c.NumOutputs, false})
		result = append(result, ShuffleConfig{c.NumInputs, c.NumOutputs, true})
	}
	return result
}

func generateBaseConfigs() []ShuffleConfig {
	result := make([]ShuffleConfig, 0, 25)
	for numInputs := 1; numInputs <= 5; numInputs++ {
		for numOutputs := 1; numOutputs <= 5; numOutputs++ {
			result = append(result, ShuffleConfig{NumInputs: numInputs, NumOutputs: numOutputs})
		}
	}
	return result
}

func generateSingleInputConfigs() []ShuffleConfig {
	result := make([]ShuffleConfig, 0, 5)
	for numOutputs := 1; numOutputs <= 5; numOutputs++ {
		result = append(result, ShuffleConfig{NumInputs: 1, NumOutputs: numOutputs})
	}
	return result
}

func generateSingleOutputConfigs() []ShuffleConfig {
	result := make([]ShuffleConfig, 0, 5)
	for numInputs := 1; numInputs <= 5; numInputs++ {
		result = append(result, ShuffleConfig{NumInputs: numInputs, NumOutputs: 1})
	}
	return result
}

// ========== Template Helpers ==========

// joinIdx generates comma-separated items with index substitution
// pattern can contain multiple %d which will all be replaced with the same index
func joinIdx(n int, pattern string) string {
	parts := make([]string, n)
	for i := 0; i < n; i++ {
		// Count how many %d in pattern and provide that many arguments
		count := strings.Count(pattern, "%d")
		args := make([]any, count)
		for j := range args {
			args[j] = i
		}
		parts[i] = fmt.Sprintf(pattern, args...)
	}
	return strings.Join(parts, ", ")
}

func buildFuncMap() template.FuncMap {
	return template.FuncMap{
		"seq":   seq,
		"minus": func(a, b int) int { return a - b },
		"bit":   func(i int) int { return 1 << i },
		"neq":   func(a, b int) bool { return a != b },

		// Type parameter helpers
		"inTypes":              func(n int) string { return joinIdx(n, "TIn%d") },
		"outTypes":             func(n int) string { return joinIdx(n, "TOut%d") },
		"inTypesAny":           func(n int) string { return joinIdx(n, "TIn%d any") },
		"outTypesAny":          func(n int) string { return joinIdx(n, "TOut%d any") },
		"serializerStates":     func(n int) string { return joinIdx(n, "TSerializerState%d any") },
		"serializerStateTypes": func(n int) string { return joinIdx(n, "TSerializerState%d") },
		"intermediateTypes":    func(n int) string { return joinIdx(n, "TIntermediate%d") },
		"intermediateTypesAny": func(n int) string { return joinIdx(n, "TIntermediate%d any") },

		// Parameter helpers
		"inCollections":    func(n int) string { return joinIdx(n, "collection%d *pipeline.Collection[TIn%d]") },
		"inCollectionArgs": func(n int) string { return joinIdx(n, "collection%d") },

		// Emitter helpers
		"emitterArgs":   func(n int) string { return joinIdx(n, "emitter%d") },
		"emitterParams": func(n int) string { return joinIdx(n, "emitter%d *primitives.Emitter[TOut%d]") },

		// Return type helpers
		"outCollectionTypes": func(n int) string { return joinIdx(n, "*pipeline.Collection[TOut%d]") },
		"outCollectionArgs":  func(n int) string { return joinIdx(n, "outCollection%d") },

		// Serializer type params for operator calls
		"serializerTypeArgs": func(n int) string { return joinIdx(n, "TSerializer%d") },

		// Receiver helpers
		"receiverArgs": func(n int) string { return joinIdx(n, "&receiver%d") },

		// Metrics helpers
		"outCollectionMetricsArgs": func(n int) string { return joinIdx(n, "outCollection%d.Metrics") },
	}
}

// ========== Templates ==========

const implTemplate = `
package operators

import (
	"bytes"

	"github.com/a-kazakov/gomr/internal/capprobe"
	"github.com/a-kazakov/gomr/internal/core"
	"github.com/a-kazakov/gomr/internal/constants"
	"github.com/a-kazakov/gomr/internal/operators/options"
	"github.com/a-kazakov/gomr/internal/pipeline"
	"github.com/a-kazakov/gomr/internal/primitives"
	"github.com/a-kazakov/gomr/internal/rw/shardedfile"
	"github.com/a-kazakov/gomr/internal/shuf"
	"github.com/a-kazakov/gomr/internal/utils"
	"github.com/a-kazakov/gomr/metrics"
)

{{- range $config := .Configs }}
{{- $ni := $config.NumInputs }}
{{- $no := $config.NumOutputs }}
{{- $sv := $config.WithSideValue }}

func {{ $config.FuncName }}[
	{{- range $i := seq $ni }}
	TSerializer{{$i}} interface {
		*TSerializerState{{$i}}
		core.{{ $config.SerializerInterface }}[TIn{{$i}}, TIntermediate{{$i}}]
	},
	{{- end }}
	TReducer interface {
		*TReducerState
		core.{{ $config.ReducerType }}[{{ intermediateTypes $ni }}, {{ outTypes $no }}]
	},
	{{ serializerStates $ni }},
	TReducerState any,
	{{ inTypesAny $ni }}, {{ intermediateTypesAny $ni }}, {{ outTypesAny $no }}{{ if $sv }}, TSideValue any{{ end }},
](
	{{ inCollections $ni }},
	{{- if $sv }}
	sideValue *pipeline.Value[TSideValue],
	{{- end }}
	opts *options.ShuffleOptions,
) ({{ outCollectionTypes $no }}) {
	p := collection0.Pipeline
	const numInputs = {{ $ni }}
	operationName := utils.OptStrOrDefault(opts.OperationName, "Shuffle")
	{{- range $i := seq $no }}
	outCollection{{$i}}Name := utils.OptGetOrDefault(opts.OutCollectionNames, {{$i}}, "Shuffled value {{$i}}")
	{{- end }}
	numShards, scatterParallelism, gatherParallelism, outBatchSize, localShuffleBufferSize, localShuffleBufferSizeJitter, fileMergeThreshold, readBufferSize, writeBufferSize, targetWriteLatency, scratchSpacePaths := extractShuffleParameters(p, opts)
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	compressionAlgorithm := p.Parameters.Shuffle.DefaultCompressionAlgorithm.Resolve(opts.CompressionAlgorithm)
	goroutineDispatcher := p.GoroutineDispatcher
	{{- range $i := seq $no }}
	outCollection{{$i}}, outChannel{{$i}} := pipeline.NewDerivedCollection[TOut{{$i}}](outCollection{{$i}}Name, outBatchSize, outChannelCapacity, {{ inCollectionArgs $ni }})
	{{- end }}
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_SHUFFLE, operationName)
	{{- range $i := seq $ni }}
	inMetrics{{$i}} := opMetrics.AddInputCollection(collection{{$i}}.Metrics)
	{{- end }}
	{{- if $sv }}
	inValueMetrics := opMetrics.AddInputValue(sideValue.Metrics)
	{{- end }}
	{{- range $i := seq $no }}
	outMetrics{{$i}} := opMetrics.AddOutputCollection(outCollection{{$i}}.Metrics)
	{{- end }}
	opMetrics.Parallelism = scatterParallelism
	shardedFiles := make([]*shardedfile.ShardedFile, scatterParallelism) // placeholder for output
	{{- range $i := seq $ni }}
	inChannel{{$i}} := collection{{$i}}.GetRawChannel()
	{{- end }}
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_SCATTERING) }
	scatterOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{ {{- outCollectionMetricsArgs $no -}} }, nil, scatterParallelism, opts.UserOperatorContext.GetOr(nil))
	gatherOpContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{ {{- outCollectionMetricsArgs $no -}} }, nil, gatherParallelism, opts.UserOperatorContext.GetOr(nil))
	goroutineDispatcher.StartParallelTrackedGoroutines(scatterParallelism, func(goroutineIndex int) {
		{{- if $sv }}
		sideValueResolved := sideValue.Wait()
		if goroutineIndex == 0 {
			inValueMetrics.IsConsumed = true
		}
		{{- end }}
		{{- range $i := seq $ni }}
		var serializerState{{$i}} TSerializerState{{$i}}
		var serializer{{$i}} TSerializer{{$i}} = &serializerState{{$i}}
		serializerSetup{{$i}} := capprobe.Get{{ $config.SerializerInterface }}Setup{{ if $sv }}WithSideValue[TSideValue]{{ end }}(serializer{{$i}})
		if serializerSetup{{$i}} != nil {
			serializerSetup{{$i}}.Setup(scatterOpContexts[goroutineIndex]{{ if $sv }}, sideValueResolved{{ end }})
		}
		{{- end }}
		scatterer := shuf.NewScatterer(
			shuf.ScattererArguments{
				NumLogicalShards:     numShards,
				NumInputs:            numInputs,
				WorkingDirectory:     getShufflePath(scratchSpacePaths, p.GetJobID(), opMetrics.Id, goroutineIndex),
				MaxBufferSize:        localShuffleBufferSize,
				MaxBufferSizeJitter:  localShuffleBufferSizeJitter,
				FileMergeThreshold:   fileMergeThreshold,
				ReadBufferSize:       readBufferSize,
				WriteBufferSize:      writeBufferSize,
				TargetWriteLatency:   targetWriteLatency,
				CompressionAlgorithm: compressionAlgorithm,
				Metrics:              opMetrics.Shuffle,
			},
		)
		{{- range $i := seq $ni }}
		localInChannel{{$i}} := inChannel{{$i}}
		{{- end }}
		channelsOpen := numInputs
		for channelsOpen > 0 {
			select {
			{{- range $i := seq $ni }}
			case inValuesBatch, ok := <-localInChannel{{$i}}:
				if !ok {
					localInChannel{{$i}} = nil
					channelsOpen--
					continue
				}
				firstBatchReceived()
				shuf.AddBatchToScatterer(scatterer, inValuesBatch.Values, serializer{{$i}}, numInputs, {{$i}})
				inMetrics{{$i}}.ElementsConsumed.Add(int64(len(inValuesBatch.Values)))
				inMetrics{{$i}}.BatchesConsumed.Add(1)
				inValuesBatch.Recycle()
			{{- end }}
			}
		}
		opMetrics.TrySetPhase(core.PHASE_SCATTERING, core.PHASE_FLUSHING)
		shardedFiles[goroutineIndex] = scatterer.Finalize()
	}, func() {
		opMetrics.SetPhase(core.PHASE_GATHERING)
		opMetrics.Parallelism = gatherParallelism
		newShardedFilesSize := utils.FilterNilsInPlace(shardedFiles)
		shardedFiles = shardedFiles[:newShardedFilesSize]
		gatherer := shuf.NewGatherer(shardedFiles, opMetrics.Shuffle)
		shardsToProcess := make(chan int32, numShards)
		for shardId := range numShards {
			shardsToProcess <- shardId
		}
		close(shardsToProcess)
		goroutineDispatcher.StartParallelTrackedGoroutines(gatherParallelism, func(goroutineIndex int) {
			{{- if $sv }}
			sideValueResolved := sideValue.Wait()
			{{- end }}
			{{- range $i := seq $ni }}
			var serializerState{{$i}} TSerializerState{{$i}}
			var serializer{{$i}} TSerializer{{$i}} = &serializerState{{$i}}
			serializerSetup{{$i}} := capprobe.Get{{ $config.SerializerInterface }}Setup{{ if $sv }}WithSideValue[TSideValue]{{ end }}(serializer{{$i}})
			if serializerSetup{{$i}} != nil {
				serializerSetup{{$i}}.Setup(gatherOpContexts[goroutineIndex]{{ if $sv }}, sideValueResolved{{ end }})
			}
			bytesBuffer{{$i}} := make([]byte, constants.MAX_SHUFFLE_VALUE_SIZE)
			{{- end }}
			{{- range $i := seq $no }}
			emitter{{$i}} := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel{{$i}}, &outMetrics{{$i}}.ElementsProduced, &outMetrics{{$i}}.BatchesProduced)
			defer emitter{{$i}}.Close()
			{{- end }}
			for logicalShardId := range shardsToProcess {
				var reducerState TReducerState
				var reducer TReducer = &reducerState
				reducerSetup := capprobe.GetReducerSetup{{ if $sv }}WithSideValue[TSideValue]{{ end }}(reducer)
				reducerTeardown := capprobe.GetReducerTo{{$no}}Teardown[{{ outTypes $no }}](reducer)
				if reducerSetup != nil {
					reducerSetup.Setup(gatherOpContexts[goroutineIndex]{{ if $sv }}, sideValueResolved{{ end }})
				}
				{{- range $i := seq $ni }}
				shardReader{{$i}}, shardReaderCloser{{$i}} := gatherer.GetShardReader(numInputs*logicalShardId + {{$i}}, readBufferSize)
				key{{$i}} := shardReader{{$i}}.PeekKey()
				{{- end }}
				for {{ range $i := seq $ni }}{{ if ne $i 0 }} || {{ end }}key{{$i}} != nil{{ end }} {
					var smallestKey []byte
					var includeMask uint64
					{{- range $i := seq $ni }}
					if key{{$i}} != nil {
						if smallestKey == nil {
							smallestKey = key{{$i}}
							includeMask = {{ bit $i }}
						} else {
							cmp := bytes.Compare(key{{$i}}, smallestKey)
							if cmp < 0 {
								smallestKey = key{{$i}}
								includeMask = {{ bit $i }}
							} else if cmp == 0 {
								includeMask |= {{ bit $i }}
							}
						}
					}
					{{- end }}
				{{- range $i := seq $ni }}
				var receiver{{$i}} primitives.IteratorReceiver[TIntermediate{{$i}}]
				if includeMask & {{ bit $i }} != 0 {
					keyReader{{$i}} := shardReader{{$i}}.GetKeyReader()
					receiver{{$i}} = getReceiverFromKeyReader(serializer{{$i}}, &keyReader{{$i}}, smallestKey, bytesBuffer{{$i}}, &opMetrics.Shuffle.ElementsGathered, &opMetrics.Shuffle.GroupsGathered)
				} else {
					receiver{{$i}} = getEmptyReceiver[TIntermediate{{$i}}]()
				}
				{{- end }}
					reducer.Reduce(smallestKey, {{ receiverArgs $ni }}, {{ emitterArgs $no }})
					{{- range $i := seq $ni }}
					receiver{{$i}}.EnsureUsed()
					key{{$i}} = shardReader{{$i}}.PeekKey()
					{{- end }}
				}
				if reducerTeardown != nil {
					reducerTeardown.Teardown({{ emitterArgs $no }})
				}
				{{- range $i := seq $ni }}
				shardReaderCloser{{$i}}.Close()
				{{- end }}
			}
		}, func() {
			for idx := range shardedFiles {
				shardedFiles[idx].Destroy()
			}
			{{- range $i := seq $no }}
			outCollection{{$i}}.Close()
			{{- end }}
			opMetrics.SetPhase(core.PHASE_COMPLETED)
			opMetrics.Parallelism = 0
		})
	})

	return {{ outCollectionArgs $no }}
}
{{ end }}
`

const aliasTemplate = `
package gomr

import (
	"github.com/a-kazakov/gomr/internal/core"
	"github.com/a-kazakov/gomr/internal/operators"
	"github.com/a-kazakov/gomr/internal/operators/options"
	"github.com/a-kazakov/gomr/internal/pipeline"
)

// ========== Shuffle{N}To{M} - full aliases ==========
{{- range $config := .AllConfigs }}
{{- $ni := $config.NumInputs }}
{{- $no := $config.NumOutputs }}
{{- $sv := $config.WithSideValue }}

{{ if not $sv }}
type {{ $config.ReducerType }}[{{ inTypesAny $ni }}, {{ outTypesAny $no }}{{ if $sv }}, TSideValue any{{ end }}] = core.{{ $config.ReducerType }}[{{ inTypes $ni }}, {{ outTypes $no }}{{ if $sv }}, TSideValue{{ end }}]
{{ end }}
func {{ $config.FuncName }}[
	{{- range $i := seq $ni }}
	TSerializer{{$i}} interface {
		*TSerializerState{{$i}}
		core.{{ $config.SerializerInterface }}[TIn{{$i}}, TIntermediate{{$i}}]
	},
	{{- end }}
	TReducer interface {
		*TReducerState
		core.{{ $config.ReducerType }}[{{ intermediateTypes $ni }}, {{ outTypes $no }}]
	},
	{{ serializerStates $ni }},
	TReducerState any,
	{{ inTypesAny $ni }}, {{ intermediateTypesAny $ni }}, {{ outTypesAny $no }}{{ if $sv }}, TSideValue any{{ end }},
](
	{{ inCollections $ni }},
	{{- if $sv }}
	sideValue *pipeline.Value[TSideValue],
	{{- end }}
	opts ...options.ShuffleOption,
) ({{ outCollectionTypes $no }}) {
	return operators.{{ $config.FuncName }}[{{ serializerTypeArgs $ni }}, TReducer, {{ serializerStateTypes $ni }}, TReducerState, {{ inTypes $ni }}, {{ intermediateTypes $ni }}, {{ outTypes $no }}{{ if $sv }}, TSideValue{{ end }}]({{ inCollectionArgs $ni }}, {{ if $sv }}sideValue, {{ end }}options.ApplyShuffleOptions(opts...))
}
{{ end }}

// ========== ShuffleTo{M} -> Shuffle1To{M} (single input, M outputs) ==========
{{- range $config := .SingleInputConfigs }}
{{- $no := $config.NumOutputs }}
{{- $sv := $config.WithSideValue }}

{{ if not $sv }}
type ReducerTo{{$no}}[TIn0 any, {{ outTypesAny $no }}{{ if $sv }}, TSideValue any{{ end }}] = core.Reducer1To{{$no}}[TIn0, {{ outTypes $no }}]
{{ end }}

func ShuffleTo{{$no}}{{ if $sv }}WithSideValue{{ end }}[
	TSerializer0 interface {
		*TSerializerState0
		core.{{ $config.SerializerInterface }}[TIn0, TIntermediate0]
	},
	TReducer interface {
		*TReducerState
		core.Reducer1To{{$no}}[TIntermediate0, {{ outTypes $no }}]
	},
	TSerializerState0 any,
	TReducerState any,
	TIn0 any, TIntermediate0 any, {{ outTypesAny $no }}{{ if $sv }}, TSideValue any{{ end }},
](
	collection0 *pipeline.Collection[TIn0],
	{{- if $sv }}
	sideValue *pipeline.Value[TSideValue],
	{{- end }}
	opts ...options.ShuffleOption,
) ({{ outCollectionTypes $no }}) {
	return operators.Shuffle1To{{$no}}{{ if $sv }}WithSideValue{{ end }}[TSerializer0, TReducer, TSerializerState0, TReducerState, TIn0, TIntermediate0, {{ outTypes $no }}{{ if $sv }}, TSideValue{{ end }}](collection0, {{ if $sv }}sideValue, {{ end }}options.ApplyShuffleOptions(opts...))
}
{{ end }}

// ========== Shuffle{N} -> Shuffle{N}To1 (N inputs, single output) ==========
{{- range $config := .SingleOutputConfigs }}
{{- $ni := $config.NumInputs }}
{{- $sv := $config.WithSideValue }}

{{ if not $sv }}
type Reducer{{$ni}}[{{ inTypesAny $ni }}, TOut0 any{{ if $sv }}, TSideValue any{{ end }}] = core.Reducer{{$ni}}To1[{{ inTypes $ni }}, TOut0{{ if $sv }}, TSideValue{{ end }}]
{{ end }}

func Shuffle{{$ni}}{{ if $sv }}WithSideValue{{ end }}[
	{{- range $i := seq $ni }}
	TSerializer{{$i}} interface {
		*TSerializerState{{$i}}
		core.{{ $config.SerializerInterface }}[TIn{{$i}}, TIntermediate{{$i}}]
	},
	{{- end }}
	TReducer interface {
		*TReducerState
		core.Reducer{{$ni}}To1[{{ intermediateTypes $ni }}, TOut0]
	},
	{{ serializerStates $ni }},
	TReducerState any,
	{{ inTypesAny $ni }}, {{ intermediateTypesAny $ni }}, TOut0 any{{ if $sv }}, TSideValue any{{ end }},
](
	{{ inCollections $ni }},
	{{- if $sv }}
	sideValue *pipeline.Value[TSideValue],
	{{- end }}
	opts ...options.ShuffleOption,
) *pipeline.Collection[TOut0] {
	return operators.Shuffle{{$ni}}To1{{ if $sv }}WithSideValue{{ end }}[{{ serializerTypeArgs $ni }}, TReducer, {{ serializerStateTypes $ni }}, TReducerState, {{ inTypes $ni }}, {{ intermediateTypes $ni }}, TOut0{{ if $sv }}, TSideValue{{ end }}]({{ inCollectionArgs $ni }}, {{ if $sv }}sideValue, {{ end }}options.ApplyShuffleOptions(opts...))
}
{{ end }}
`

const ifaceTemplate = `
package core

import "github.com/a-kazakov/gomr/internal/primitives"


{{- range $config := .Configs }}
{{- $ni := $config.NumInputs }}
{{- $no := $config.NumOutputs }}
{{- $sv := $config.WithSideValue }}
{{ if not $sv }}
type {{ $config.ReducerType }}[{{ inTypesAny $ni }}, {{ outTypesAny $no }}] interface {
	Reduce(key []byte, {{- range $i := seq $ni }}receiver{{$i}} ShuffleReceiver[TIn{{$i}}], {{ end }}{{ emitterParams $no }})
}
{{ end }}
{{ end }}

{{- range $config := .SingleInputConfigs }}
{{- $no := $config.NumOutputs }}
{{- $sv := $config.WithSideValue }}
{{ if not $sv }}
type ReducerTo{{$no}}Teardown[{{ outTypesAny $no }}] interface {
	Teardown({{ emitterParams $no }})
}
{{ end }}
{{ end }}
`

const capprobeTemplate = `
package capprobe

import "github.com/a-kazakov/gomr/internal/core"

{{- range $config := .SingleInputConfigs }}
{{- $no := $config.NumOutputs }}
{{- $sv := $config.WithSideValue }}
{{ if not $sv }}
func GetReducerTo{{$no}}Teardown[{{ outTypesAny $no }}](reducer any) core.ReducerTo{{$no}}Teardown[{{ outTypes $no }}] {
	result, ok := reducer.(core.ReducerTo{{$no}}Teardown[{{ outTypes $no }}])
	if ok {
		return result
	}
	verifyNoTypos[core.ReducerTo{{$no}}Teardown[{{ outTypes $no }}]](reducer)
	return nil
}
{{ end }}
{{ end }}
`

// ========== Template Data ==========

type TemplateData struct {
	Configs             []ShuffleConfig
	AllConfigs          []ShuffleConfig
	SingleInputConfigs  []ShuffleConfig
	SingleOutputConfigs []ShuffleConfig
}

// ========== Build & Main ==========

func buildTemplate(tmplStr string, data TemplateData, outputFile string) {
	tmpl, err := template.New("shuffle").Funcs(buildFuncMap()).Parse(tmplStr)
	if err != nil {
		log.Fatalf("Template parse error: %v", err)
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		log.Fatalf("Template execution error: %v", err)
	}
	formatted, err := format.Source(buf.Bytes())
	if err != nil {
		os.Stdout.Write(buf.Bytes())
		log.Fatalf("Formatting error: %v", err)
	}
	if err := os.WriteFile(outputFile, formatted, 0644); err != nil {
		log.Fatalf("Write file error: %v", err)
	}
}

func main() {
	args := os.Args[1:]
	if len(args) != 1 {
		log.Fatalf("Usage: %s <impl|alias|iface>", os.Args[0])
	}

	baseConfigs := generateBaseConfigs()
	allVariants := generateAllVariants(baseConfigs)

	data := TemplateData{
		Configs:             allVariants,
		AllConfigs:          allVariants,
		SingleInputConfigs:  generateAllVariants(generateSingleInputConfigs()),
		SingleOutputConfigs: generateAllVariants(generateSingleOutputConfigs()),
	}

	switch args[0] {
	case "impl":
		buildTemplate(implTemplate, data, "shuffle_gen.go")
	case "alias":
		buildTemplate(aliasTemplate, data, "operator_shuffle_gen.go")
	case "iface":
		buildTemplate(ifaceTemplate, data, "operator_shuffle_gen.go")
	case "capprobe":
		buildTemplate(capprobeTemplate, data, "shuffle_gen.go")
	default:
		log.Fatalf("Unknown template type: %s", args[0])
	}
}
