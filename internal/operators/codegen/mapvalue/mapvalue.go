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

// MapValueConfig represents a single mapvalue variant configuration
type MapValueConfig struct {
	NumInputs  int
	NumOutputs int
}

// Computed properties
func (c MapValueConfig) FuncName() string {
	return fmt.Sprintf("MapValue%dTo%d", c.NumInputs, c.NumOutputs)
}

// ========== Config Generators ==========

func generateBaseConfigs() []MapValueConfig {
	result := make([]MapValueConfig, 0, 25)
	for numInputs := 1; numInputs <= 5; numInputs++ {
		for numOutputs := 1; numOutputs <= 5; numOutputs++ {
			result = append(result, MapValueConfig{NumInputs: numInputs, NumOutputs: numOutputs})
		}
	}
	return result
}

func generateSingleInputConfigs() []MapValueConfig {
	result := make([]MapValueConfig, 0, 5)
	for numOutputs := 1; numOutputs <= 5; numOutputs++ {
		result = append(result, MapValueConfig{NumInputs: 1, NumOutputs: numOutputs})
	}
	return result
}

func generateSingleOutputConfigs() []MapValueConfig {
	result := make([]MapValueConfig, 0, 5)
	for numInputs := 1; numInputs <= 5; numInputs++ {
		result = append(result, MapValueConfig{NumInputs: numInputs, NumOutputs: 1})
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
		"eq":    func(a, b int) bool { return a == b },

		// Type parameter helpers
		"inTypes":     func(n int) string { return joinIdx(n, "TIn%d") },
		"outTypes":    func(n int) string { return joinIdx(n, "TOut%d") },
		"inTypesAny":  func(n int) string { return joinIdx(n, "TIn%d any") },
		"outTypesAny": func(n int) string { return joinIdx(n, "TOut%d any") },

		// Parameter helpers
		"inputValues":    func(n int) string { return joinIdx(n, "input%d *pipeline.Value[TIn%d]") },
		"inputValueArgs": func(n int) string { return joinIdx(n, "input%d") },
		"inputParams":    func(n int) string { return joinIdx(n, "input%d TIn%d") },
		"inputValueVars": func(n int) string { return joinIdx(n, "inputValue%d") },

		// Output helpers
		"outputParams":      func(n int) string { return joinIdx(n, "output%d TOut%d") },
		"outputValueTypes":  func(n int) string { return joinIdx(n, "*pipeline.Value[TOut%d]") },
		"outputValueArgs":   func(n int) string { return joinIdx(n, "output%d") },
		"outputValueVars":   func(n int) string { return joinIdx(n, "outputValue%d") },
		"outputMetricsArgs": func(n int) string { return joinIdx(n, "output%d.Metrics") },
	}
}

// ========== Templates ==========

const implTemplate = `
package operators

import (
	"github.com/a-kazakov/gomr/internal/core"
	"github.com/a-kazakov/gomr/internal/operators/options"
	"github.com/a-kazakov/gomr/internal/pipeline"
	"github.com/a-kazakov/gomr/internal/utils"
	"github.com/a-kazakov/gomr/metrics"
)

{{- range $config := .AllConfigs }}
{{- $ni := $config.NumInputs }}
{{- $no := $config.NumOutputs }}

func {{ $config.FuncName }}[{{ inTypesAny $ni }}, {{ outTypesAny $no }}](
	{{ inputValues $ni }},
	mapper func(context *core.OperatorContext, {{ inputParams $ni }}) ({{ outputParams $no }}),
	opts *options.MapValueOptions,
) ({{ outputValueTypes $no }}) {
	p := input0.Pipeline
	operationName := utils.OptStrOrDefault(opts.OperationName, "Map value")
	{{- range $i := seq $no }}
	output{{$i}}Name := utils.OptGetOrDefault(opts.OutValueNames, {{$i}}, "Mapped value {{$i}}")
	{{- end }}
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MAP_VALUE, operationName)
	{{- range $i := seq $ni }}
	opMetrics.AddInputValue(input{{$i}}.Metrics)
	{{- end }}
	{{- range $i := seq $no }}
	output{{$i}} := pipeline.NewValue[TOut{{$i}}](p, output{{$i}}Name)
	{{- end }}
	{{- range $i := seq $no }}
	opMetrics.AddOutputValue(output{{$i}}.Metrics)
	{{- end }}
	opMetrics.Parallelism = 1
	opContexts := p.BuildOperatorContexts(opMetrics, nil, []*metrics.ValueMetrics{ {{- outputMetricsArgs $no -}} }, 1, opts.UserOperatorContext.GetOr(nil))
	p.GoroutineDispatcher.StartTrackedGoroutine(func() {
		{{- range $i := seq $ni }}
		inputValue{{$i}} := input{{$i}}.Wait()
		{{- end }}
		opMetrics.SetPhase(core.PHASE_RUNNING)
		{{ outputValueVars $no }} := mapper(opContexts[0], {{ inputValueVars $ni }})
		{{- range $i := seq $no }}
		output{{$i}}.Resolve(outputValue{{$i}})
		{{- end }}
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return {{ outputValueArgs $no }}
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

// ========== MapValue{N}To{M} - full aliases ==========
{{- range $config := .AllConfigs }}
{{- $ni := $config.NumInputs }}
{{- $no := $config.NumOutputs }}

func {{ $config.FuncName }}[{{ inTypesAny $ni }}, {{ outTypesAny $no }}](
	{{ inputValues $ni }},
	mapper func(context *core.OperatorContext, {{ inputParams $ni }}) ({{ outputParams $no }}),
	opts ...options.MapValueOption,
) ({{ outputValueTypes $no }}) {
	return operators.{{ $config.FuncName }}({{ inputValueArgs $ni }}, mapper, options.ApplyMapValueOptions(opts...))
}
{{ end }}

// ========== MapValueTo{M} -> MapValue1To{M} (single input, M outputs) ==========
{{- range $config := .SingleInputConfigs }}
{{- $no := $config.NumOutputs }}
{{- if eq $no 1 }}

func MapValueTo1[TIn0 any, TOut0 any](
	input0 *pipeline.Value[TIn0],
	mapper func(context *core.OperatorContext, input0 TIn0) TOut0,
	opts ...options.MapValueOption,
) *pipeline.Value[TOut0] {
	return operators.MapValue1To1(input0, mapper, options.ApplyMapValueOptions(opts...))
}
{{- else }}

func MapValueTo{{$no}}[TIn0 any, {{ outTypesAny $no }}](
	input0 *pipeline.Value[TIn0],
	mapper func(context *core.OperatorContext, input0 TIn0) ({{ outputParams $no }}),
	opts ...options.MapValueOption,
) ({{ outputValueTypes $no }}) {
	return operators.MapValue1To{{$no}}(input0, mapper, options.ApplyMapValueOptions(opts...))
}
{{- end }}
{{ end }}

// ========== MapValue{N} -> MapValue{N}To1 (N inputs, single output) ==========
{{- range $config := .SingleOutputConfigs }}
{{- $ni := $config.NumInputs }}

func MapValue{{$ni}}[{{ inTypesAny $ni }}, TOut0 any](
	{{ inputValues $ni }},
	mapper func(context *core.OperatorContext, {{ inputParams $ni }}) TOut0,
	opts ...options.MapValueOption,
) *pipeline.Value[TOut0] {
	return operators.MapValue{{$ni}}To1({{ inputValueArgs $ni }}, mapper, options.ApplyMapValueOptions(opts...))
}
{{ end }}
`

// ========== Template Data ==========

type TemplateData struct {
	AllConfigs          []MapValueConfig
	SingleInputConfigs  []MapValueConfig
	SingleOutputConfigs []MapValueConfig
}

// ========== Build & Main ==========

func buildTemplate(tmplStr string, data TemplateData, outputFile string) {
	tmpl, err := template.New("mapvalue").Funcs(buildFuncMap()).Parse(tmplStr)
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
		log.Fatalf("Usage: %s <impl|alias>", os.Args[0])
	}

	data := TemplateData{
		AllConfigs:          generateBaseConfigs(),
		SingleInputConfigs:  generateSingleInputConfigs(),
		SingleOutputConfigs: generateSingleOutputConfigs(),
	}

	switch args[0] {
	case "impl":
		buildTemplate(implTemplate, data, "mapvalue_gen.go")
	case "alias":
		buildTemplate(aliasTemplate, data, "operator_mapvalue_gen.go")
	default:
		log.Fatalf("Unknown template type: %s", args[0])
	}
}
