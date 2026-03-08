package main

import (
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"

	"github.com/a-kazakov/gomr"
	"github.com/a-kazakov/gomr/examples/sample_pipeline"
	trianglecount "github.com/a-kazakov/gomr/examples/triangle_count"
	"github.com/a-kazakov/gomr/parameters"
)

func setupProfiling() {
	go func() {
		slog.Info("Profiling server started on localhost:6060")
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
}

func runSamplePipeline(args []string) {
	fs := flag.NewFlagSet("sample", flag.ExitOnError)
	var count = fs.Int("count", 1000000, "Number of values to generate (default: 1M)")
	params := parameters.NewParameters()
	params.RegisterFlags(fs)
	fs.Parse(args)
	fmt.Printf("Count: %d\n", *count)
	params.LoadFromSource(os.LookupEnv)
	pipeline := gomr.NewPipelineWithParameters(params)
	fmt.Println("Pipeline created with job ID:", pipeline.GetJobID())
	result := sample_pipeline.Build(pipeline, *count)
	pipeline.WaitForCompletion()
	fmt.Println(result.Wait())
}

func runTriangleCountPipeline(args []string) {
	fs := flag.NewFlagSet("triangle-count", flag.ExitOnError)
	var cliqueSize = fs.Int("clique-size", 100, "Number of nodes in the clique (default: 1000)")
	var pairwiseSize = fs.Int("pairwise-size", 100, "Number of nodes in the pairwise (default: 1000)")
	params := parameters.NewParameters()
	params.RegisterFlags(fs)
	fs.Parse(args)
	fmt.Printf("Clique size: %d, Pairwise size: %d\n", *cliqueSize, *pairwiseSize)
	params.LoadFromSource(os.LookupEnv)
	pipeline := gomr.NewPipelineWithParameters(params)
	fmt.Println("Pipeline created with job ID:", pipeline.GetJobID())
	resultValue := trianglecount.Build(pipeline, uint32(*cliqueSize), uint32(*pairwiseSize))
	pipeline.WaitForCompletion()
	result := resultValue.Wait()
	expectedResult := uint64(*cliqueSize) * uint64(*cliqueSize-1) * uint64(*cliqueSize-2) / 6
	if result != expectedResult {
		fmt.Printf("Result %d does not match expected result %d\n", result, expectedResult)
		os.Exit(1)
	}
	fmt.Printf("Result %d matches expected result\n", result)
}

func main() {
	setupProfiling()
	pipelineName := os.Args[1]
	switch pipelineName {
	case "sample":
		runSamplePipeline(os.Args[2:])
	case "triangle-count":
		runTriangleCountPipeline(os.Args[2:])
	default:
		fmt.Printf("Unknown pipeline: %s\n", pipelineName)
		os.Exit(1)
	}
}
