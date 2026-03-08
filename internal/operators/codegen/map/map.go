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

// MapConfig represents a single map variant configuration
type MapConfig struct {
	NumOutputs    int
	WithSideValue bool
}

// Computed properties
func (c MapConfig) FuncName() string {
	suffix := ""
	if c.WithSideValue {
		suffix = "WithSideValue"
	}
	return fmt.Sprintf("MapTo%d%s", c.NumOutputs, suffix)
}

func (c MapConfig) MapperType() string {
	suffix := ""
	if c.WithSideValue {
		suffix = "WithSideValue"
	}
	return fmt.Sprintf("MapperTo%d%s", c.NumOutputs, suffix)
}

// ========== Config Generators ==========

func generateBaseConfigs() []MapConfig {
	result := make([]MapConfig, 0, 5)
	for numOutputs := 1; numOutputs <= 5; numOutputs++ {
		result = append(result, MapConfig{NumOutputs: numOutputs})
	}
	return result
}

func generateAllVariants(configs []MapConfig) []MapConfig {
	result := make([]MapConfig, 0, len(configs)*2)
	for _, c := range configs {
		result = append(result, MapConfig{c.NumOutputs, false})
		result = append(result, MapConfig{c.NumOutputs, true})
	}
	return result
}

// ========== Template Helpers ==========

// joinIdx generates comma-separated items with index substitution
func joinIdx(n int, pattern string) string {
	parts := make([]string, n)
	for i := 0; i < n; i++ {
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
		"seq": func(n int) []int {
			res := make([]int, n)
			for i := range res {
				res[i] = i
			}
			return res
		},
		"minus": func(a, b int) int { return a - b },
		"neq":   func(a, b int) bool { return a != b },

		// Type parameter helpers
		"outTypes":    func(n int) string { return joinIdx(n, "TOut%d") },
		"outTypesAny": func(n int) string { return joinIdx(n, "TOut%d any") },

		// Emitter helpers
		"emitterArgs":   func(n int) string { return joinIdx(n, "emitter%d") },
		"emitterParams": func(n int) string { return joinIdx(n, "emitter%d *primitives.Emitter[TOut%d]") },

		// Return type helpers
		"outCollectionTypes":       func(n int) string { return joinIdx(n, "*pipeline.Collection[TOut%d]") },
		"outCollectionArgs":        func(n int) string { return joinIdx(n, "outCollection%d") },
		"outCollectionMetricsArgs": func(n int) string { return joinIdx(n, "outCollection%d.Metrics") },
	}
}

// ========== Templates ==========

const implTemplate = `
package operators

import (
	"github.com/a-kazakov/gomr/internal/core"
	"github.com/a-kazakov/gomr/internal/operators/options"
	"github.com/a-kazakov/gomr/internal/pipeline"
	"github.com/a-kazakov/gomr/internal/primitives"
	"github.com/a-kazakov/gomr/internal/utils"
	"github.com/a-kazakov/gomr/metrics"
)

{{- range $config := .Configs }}
{{- $no := $config.NumOutputs }}
{{- $sv := $config.WithSideValue }}

func {{ $config.FuncName }}[TIn any, {{ outTypesAny $no }}{{ if $sv }}, TSideValue any{{ end }}](
	collection *pipeline.Collection[TIn],
	{{- if $sv }}
	sideValue *pipeline.Value[TSideValue],
	{{- end }}
	mapper core.{{ $config.MapperType }}[TIn, {{ outTypes $no }}{{ if $sv }}, TSideValue{{ end }}],
	opts *options.MapOptions,
) ({{ outCollectionTypes $no }}) {
	p := collection.Pipeline
	operationName := utils.OptStrOrDefault(opts.OperationName, "Map")
	{{- range $i := seq $no }}
	output{{$i}}Name := utils.OptGetOrDefault(opts.OutCollectionNames, {{$i}}, "Mapped value {{$i}}")
	{{- end }}
	params := collection.GetPipeline().Parameters
	outBatchSize := params.Collections.DefaultBatchSize.Resolve(opts.OutBatchSize)
	parallelism := params.Processing.DefaultParallelism.Resolve(opts.Parallelism)
	outChannelCapacity := params.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	{{- range $i := seq $no }}
	outCollection{{$i}}, outChannel{{$i}} := pipeline.NewDerivedCollection[TOut{{$i}}](output{{$i}}Name, outBatchSize, outChannelCapacity, collection)
	{{- end }}
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MAP, operationName)
	inMetrics := opMetrics.AddInputCollection(collection.Metrics)
	{{- if $sv }}
	inValueMetrics := opMetrics.AddInputValue(sideValue.Metrics)
	{{- end }}
	{{- range $i := seq $no }}
	outMetrics{{$i}} := opMetrics.AddOutputCollection(outCollection{{$i}}.Metrics)
	{{- end }}
	receiver := collection.GetReceiver(&inMetrics.ElementsConsumed, &inMetrics.BatchesConsumed)
	receiver.SetFirstBatchHook(func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_RUNNING) })
	{{- if $sv }}
	inValueMetrics.IsConsumed = true
	{{- end }}
	opMetrics.Parallelism = parallelism
	opContexts := p.BuildOperatorContexts(opMetrics, []*metrics.CollectionMetrics{ {{- outCollectionMetricsArgs $no -}} }, nil, parallelism, opts.UserOperatorContext.GetOr(nil))
	p.GoroutineDispatcher.StartParallelTrackedGoroutines(parallelism, func(goroutineIndex int) {
		{{- if $sv }}
		sideValueResolved := sideValue.Wait()
		{{- end }}
		{{- range $i := seq $no }}
		emitter{{$i}} := primitives.NewEmitter(p.Parameters.Processing.DefaultParallelism.Get(), outBatchSize, outChannel{{$i}}, &outMetrics{{$i}}.ElementsProduced, &outMetrics{{$i}}.BatchesProduced)
		defer emitter{{$i}}.Close()
		{{- end }}
		mapper(opContexts[goroutineIndex], receiver, {{ emitterArgs $no }}{{ if $sv }}, sideValueResolved{{ end }})
	}, func() {
		{{- range $i := seq $no }}
		outCollection{{$i}}.Close()
		{{- end }}
		for range receiver.IterBatches() {
			// Ensure the collection is fully consumed
		}
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
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

{{- range $config := .Configs }}
{{- $no := $config.NumOutputs }}
{{- $sv := $config.WithSideValue }}

type {{ $config.MapperType }}[TIn any, {{ outTypesAny $no }}{{ if $sv }}, TSideValue any{{ end }}] = core.{{ $config.MapperType }}[TIn, {{ outTypes $no }}{{ if $sv }}, TSideValue{{ end }}]

func {{ $config.FuncName }}[TIn any, {{ outTypesAny $no }}{{ if $sv }}, TSideValue any{{ end }}](
	collection *pipeline.Collection[TIn],
	{{- if $sv }}
	sideValue *pipeline.Value[TSideValue],
	{{- end }}
	mapper core.{{ $config.MapperType }}[TIn, {{ outTypes $no }}{{ if $sv }}, TSideValue{{ end }}],
	opts ...options.MapOption,
) ({{ outCollectionTypes $no }}) {
	return operators.{{ $config.FuncName }}[TIn, {{ outTypes $no }}{{ if $sv }}, TSideValue{{ end }}](collection, {{ if $sv }}sideValue, {{ end }}mapper, options.ApplyMapOptions(opts...))
}
{{ end }}
`

const ifaceTemplate = `
package core

import "github.com/a-kazakov/gomr/internal/primitives"

{{- range $config := .Configs }}
{{- $no := $config.NumOutputs }}
{{- $sv := $config.WithSideValue }}

type {{ $config.MapperType }}[TIn any, {{ outTypesAny $no }}{{ if $sv }}, TSideValue any{{ end }}] = func(
	ctx *OperatorContext,
	receiver CollectionReceiver[TIn],
	{{ emitterParams $no }},
	{{- if $sv }}
	sideValue TSideValue,
	{{- end }}
)
{{ end }}
`

// ========== Template Data ==========

type TemplateData struct {
	Configs []MapConfig
}

// ========== Build & Main ==========

func buildTemplate(tmplStr string, data TemplateData, outputFile string) {
	tmpl, err := template.New("map").Funcs(buildFuncMap()).Parse(tmplStr)
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

	data := TemplateData{
		Configs: generateAllVariants(generateBaseConfigs()),
	}

	switch args[0] {
	case "impl":
		buildTemplate(implTemplate, data, "map_gen.go")
	case "alias":
		buildTemplate(aliasTemplate, data, "operator_map_gen.go")
	case "iface":
		buildTemplate(ifaceTemplate, data, "operator_map_gen.go")
	default:
		log.Fatalf("Unknown template type: %s", args[0])
	}
}
