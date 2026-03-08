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

// MergeConfig represents a single merge variant configuration
type MergeConfig struct {
	NumInputs int
}

// Computed properties
func (c MergeConfig) FuncName() string {
	return fmt.Sprintf("mergeFast%d", c.NumInputs)
}

// ========== Config Generators ==========

func generateConfigs() []MergeConfig {
	result := make([]MergeConfig, 0, 9)
	for numInputs := 2; numInputs <= 10; numInputs++ {
		result = append(result, MergeConfig{NumInputs: numInputs})
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

		// Input helpers
		"inCollections":    func(n int) string { return joinIdx(n, "collection%d *pipeline.Collection[T]") },
		"inCollectionArgs": func(n int) string { return joinIdx(n, "collection%d") },
		"collectionsIdx":   func(n int) string { return joinIdx(n, "collections[%d]") },

		// Channel helpers
		"channelNotNil": func(n int) string {
			parts := make([]string, n)
			for i := 0; i < n; i++ {
				parts[i] = fmt.Sprintf("inChannel%d != nil", i)
			}
			return strings.Join(parts, " || ")
		},
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
)

{{- range $config := .Configs }}
{{- $ni := $config.NumInputs }}

func {{ $config.FuncName }}[T any]({{ inCollections $ni }}, opts *options.MergeOptions) *pipeline.Collection[T] {
	p := collection0.Pipeline
	batchSize := collection0.Metrics.BatchSize
	{{- range $i := seq $ni }}
	{{- if neq $i 0 }}
	if collection{{$i}}.Metrics.BatchSize != batchSize {
		batchSize = -1
	}
	{{- end }}
	{{- end }}
	outCollectionName := utils.OptStrOrDefault(opts.OutCollectionName, "Merged")
	operationName := utils.OptStrOrDefault(opts.OperationName, "Merge")
	outChannelCapacity := p.Parameters.Collections.DefaultCapacity.Resolve(opts.OutChannelCapacity)
	outCollection, outChannel := pipeline.NewDerivedCollection[T](outCollectionName, batchSize, outChannelCapacity, {{ inCollectionArgs $ni }})
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MERGE, operationName)
	{{- range $i := seq $ni }}
	inMetrics{{$i}} := opMetrics.AddInputCollection(collection{{$i}}.Metrics)
	{{- end }}
	outMetrics := opMetrics.AddOutputCollection(outCollection.Metrics)
	coroutineDispatcher := p.GoroutineDispatcher
	{{- range $i := seq $ni }}
	inChannel{{$i}} := collection{{$i}}.GetRawChannel()
	{{- end }}
	firstBatchReceived := func() { opMetrics.TrySetPhase(core.PHASE_PENDING, core.PHASE_RUNNING) }
	coroutineDispatcher.StartTrackedGoroutine(func() {
		opMetrics.Parallelism = 1
		for {{ channelNotNil $ni }} {
			select {
			{{- range $i := seq $ni }}
			case batch, ok := <-inChannel{{$i}}:
				if !ok {
					inChannel{{$i}} = nil
					continue
				}
				firstBatchReceived()
				outChannel <- batch
				batchSize := int64(len(batch.Values))
				inMetrics{{$i}}.ElementsConsumed.Add(batchSize)
				inMetrics{{$i}}.BatchesConsumed.Add(1)
				outMetrics.ElementsProduced.Add(batchSize)
				outMetrics.BatchesProduced.Add(1)
			{{- end }}
			}
		}
		outCollection.Close()
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return outCollection
}
{{ end }}

func Merge[T any](collections []*pipeline.Collection[T], opts *options.MergeOptions) *pipeline.Collection[T] {
	switch len(collections) {
	{{- range $config := .Configs }}
	case {{ $config.NumInputs }}:
		return {{ $config.FuncName }}({{ collectionsIdx $config.NumInputs }}, opts)
	{{- end }}
	default:
		return mergeFanIn(collections, opts)
	}
}
`

// ========== Template Data ==========

type TemplateData struct {
	Configs []MergeConfig
}

// ========== Build & Main ==========

func buildTemplate(tmplStr string, data TemplateData, outputFile string) {
	tmpl, err := template.New("merge").Funcs(buildFuncMap()).Parse(tmplStr)
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
		log.Fatalf("Usage: %s <impl>", os.Args[0])
	}

	data := TemplateData{
		Configs: generateConfigs(),
	}

	switch args[0] {
	case "impl":
		buildTemplate(implTemplate, data, "merge_gen.go")
	default:
		log.Fatalf("Unknown template type: %s", args[0])
	}
}
