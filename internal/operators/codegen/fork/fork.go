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

// ForkConfig represents a single fork variant configuration
type ForkConfig struct {
	NumOutputs int
}

// Computed properties
func (c ForkConfig) FuncName() string {
	return fmt.Sprintf("ForkTo%d", c.NumOutputs)
}

// ========== Config Generators ==========

func generateConfigs() []ForkConfig {
	result := make([]ForkConfig, 0, 4)
	for numOutputs := 2; numOutputs <= 5; numOutputs++ {
		result = append(result, ForkConfig{NumOutputs: numOutputs})
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
		"seq":   func(n int) []int { res := make([]int, n); for i := range res { res[i] = i }; return res },
		"minus": func(a, b int) int { return a - b },
		"neq":   func(a, b int) bool { return a != b },

		// Output helpers
		"outCollectionTypes": func(n int) string { return joinIdx(n, "*pipeline.Collection[T]") },
		"collectionsIdx":     func(n int) string { return joinIdx(n, "collections[%d]") },
	}
}

// ========== Templates ==========

const implTemplate = `
package operators

import (
	"github.com/a-kazakov/gomr/internal/operators/options"
	"github.com/a-kazakov/gomr/internal/pipeline"
)

{{- range $config := .Configs }}
{{- $no := $config.NumOutputs }}

func {{ $config.FuncName }}[T any](collection *pipeline.Collection[T], opts *options.ForkOptions) ({{ outCollectionTypes $no }}) {
	collections := ForkToAny(collection, {{$no}}, opts)
	return {{ collectionsIdx $no }}
}
{{ end }}
`

const aliasTemplate = `
package gomr

import (
	"github.com/a-kazakov/gomr/internal/operators"
	"github.com/a-kazakov/gomr/internal/operators/options"
	"github.com/a-kazakov/gomr/internal/pipeline"
)

{{- range $config := .Configs }}
{{- $no := $config.NumOutputs }}

func {{ $config.FuncName }}[T any](collection *pipeline.Collection[T], opts ...options.ForkOption) ({{ outCollectionTypes $no }}) {
	return operators.{{ $config.FuncName }}(collection, options.ApplyForkOptions(opts...))
}
{{ end }}
`

// ========== Template Data ==========

type TemplateData struct {
	Configs []ForkConfig
}

// ========== Build & Main ==========

func buildTemplate(tmplStr string, data TemplateData, outputFile string) {
	tmpl, err := template.New("fork").Funcs(buildFuncMap()).Parse(tmplStr)
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
		Configs: generateConfigs(),
	}

	switch args[0] {
	case "impl":
		buildTemplate(implTemplate, data, "fork_gen.go")
	case "alias":
		buildTemplate(aliasTemplate, data, "operator_fork_gen.go")
	default:
		log.Fatalf("Unknown template type: %s", args[0])
	}
}
