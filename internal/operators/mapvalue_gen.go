package operators

import (
	"github.com/a-kazakov/gomr/internal/core"
	"github.com/a-kazakov/gomr/internal/operators/options"
	"github.com/a-kazakov/gomr/internal/pipeline"
	"github.com/a-kazakov/gomr/internal/utils"
	"github.com/a-kazakov/gomr/metrics"
)

func MapValue1To1[TIn0 any, TOut0 any](
	input0 *pipeline.Value[TIn0],
	mapper func(context *core.OperatorContext, input0 TIn0) (output0 TOut0),
	opts *options.MapValueOptions,
) *pipeline.Value[TOut0] {
	p := input0.Pipeline
	operationName := utils.OptStrOrDefault(opts.OperationName, "Map value")
	output0Name := utils.OptGetOrDefault(opts.OutValueNames, 0, "Mapped value 0")
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MAP_VALUE, operationName)
	opMetrics.AddInputValue(input0.Metrics)
	output0 := pipeline.NewValue[TOut0](p, output0Name)
	opMetrics.AddOutputValue(output0.Metrics)
	opMetrics.Parallelism = 1
	opContexts := p.BuildOperatorContexts(opMetrics, nil, []*metrics.ValueMetrics{output0.Metrics}, 1, opts.UserOperatorContext.GetOr(nil))
	p.GoroutineDispatcher.StartTrackedGoroutine(func() {
		inputValue0 := input0.Wait()
		opMetrics.SetPhase(core.PHASE_RUNNING)
		outputValue0 := mapper(opContexts[0], inputValue0)
		output0.Resolve(outputValue0)
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return output0
}

func MapValue1To2[TIn0 any, TOut0 any, TOut1 any](
	input0 *pipeline.Value[TIn0],
	mapper func(context *core.OperatorContext, input0 TIn0) (output0 TOut0, output1 TOut1),
	opts *options.MapValueOptions,
) (*pipeline.Value[TOut0], *pipeline.Value[TOut1]) {
	p := input0.Pipeline
	operationName := utils.OptStrOrDefault(opts.OperationName, "Map value")
	output0Name := utils.OptGetOrDefault(opts.OutValueNames, 0, "Mapped value 0")
	output1Name := utils.OptGetOrDefault(opts.OutValueNames, 1, "Mapped value 1")
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MAP_VALUE, operationName)
	opMetrics.AddInputValue(input0.Metrics)
	output0 := pipeline.NewValue[TOut0](p, output0Name)
	output1 := pipeline.NewValue[TOut1](p, output1Name)
	opMetrics.AddOutputValue(output0.Metrics)
	opMetrics.AddOutputValue(output1.Metrics)
	opMetrics.Parallelism = 1
	opContexts := p.BuildOperatorContexts(opMetrics, nil, []*metrics.ValueMetrics{output0.Metrics, output1.Metrics}, 1, opts.UserOperatorContext.GetOr(nil))
	p.GoroutineDispatcher.StartTrackedGoroutine(func() {
		inputValue0 := input0.Wait()
		opMetrics.SetPhase(core.PHASE_RUNNING)
		outputValue0, outputValue1 := mapper(opContexts[0], inputValue0)
		output0.Resolve(outputValue0)
		output1.Resolve(outputValue1)
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return output0, output1
}

func MapValue1To3[TIn0 any, TOut0 any, TOut1 any, TOut2 any](
	input0 *pipeline.Value[TIn0],
	mapper func(context *core.OperatorContext, input0 TIn0) (output0 TOut0, output1 TOut1, output2 TOut2),
	opts *options.MapValueOptions,
) (*pipeline.Value[TOut0], *pipeline.Value[TOut1], *pipeline.Value[TOut2]) {
	p := input0.Pipeline
	operationName := utils.OptStrOrDefault(opts.OperationName, "Map value")
	output0Name := utils.OptGetOrDefault(opts.OutValueNames, 0, "Mapped value 0")
	output1Name := utils.OptGetOrDefault(opts.OutValueNames, 1, "Mapped value 1")
	output2Name := utils.OptGetOrDefault(opts.OutValueNames, 2, "Mapped value 2")
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MAP_VALUE, operationName)
	opMetrics.AddInputValue(input0.Metrics)
	output0 := pipeline.NewValue[TOut0](p, output0Name)
	output1 := pipeline.NewValue[TOut1](p, output1Name)
	output2 := pipeline.NewValue[TOut2](p, output2Name)
	opMetrics.AddOutputValue(output0.Metrics)
	opMetrics.AddOutputValue(output1.Metrics)
	opMetrics.AddOutputValue(output2.Metrics)
	opMetrics.Parallelism = 1
	opContexts := p.BuildOperatorContexts(opMetrics, nil, []*metrics.ValueMetrics{output0.Metrics, output1.Metrics, output2.Metrics}, 1, opts.UserOperatorContext.GetOr(nil))
	p.GoroutineDispatcher.StartTrackedGoroutine(func() {
		inputValue0 := input0.Wait()
		opMetrics.SetPhase(core.PHASE_RUNNING)
		outputValue0, outputValue1, outputValue2 := mapper(opContexts[0], inputValue0)
		output0.Resolve(outputValue0)
		output1.Resolve(outputValue1)
		output2.Resolve(outputValue2)
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return output0, output1, output2
}

func MapValue1To4[TIn0 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any](
	input0 *pipeline.Value[TIn0],
	mapper func(context *core.OperatorContext, input0 TIn0) (output0 TOut0, output1 TOut1, output2 TOut2, output3 TOut3),
	opts *options.MapValueOptions,
) (*pipeline.Value[TOut0], *pipeline.Value[TOut1], *pipeline.Value[TOut2], *pipeline.Value[TOut3]) {
	p := input0.Pipeline
	operationName := utils.OptStrOrDefault(opts.OperationName, "Map value")
	output0Name := utils.OptGetOrDefault(opts.OutValueNames, 0, "Mapped value 0")
	output1Name := utils.OptGetOrDefault(opts.OutValueNames, 1, "Mapped value 1")
	output2Name := utils.OptGetOrDefault(opts.OutValueNames, 2, "Mapped value 2")
	output3Name := utils.OptGetOrDefault(opts.OutValueNames, 3, "Mapped value 3")
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MAP_VALUE, operationName)
	opMetrics.AddInputValue(input0.Metrics)
	output0 := pipeline.NewValue[TOut0](p, output0Name)
	output1 := pipeline.NewValue[TOut1](p, output1Name)
	output2 := pipeline.NewValue[TOut2](p, output2Name)
	output3 := pipeline.NewValue[TOut3](p, output3Name)
	opMetrics.AddOutputValue(output0.Metrics)
	opMetrics.AddOutputValue(output1.Metrics)
	opMetrics.AddOutputValue(output2.Metrics)
	opMetrics.AddOutputValue(output3.Metrics)
	opMetrics.Parallelism = 1
	opContexts := p.BuildOperatorContexts(opMetrics, nil, []*metrics.ValueMetrics{output0.Metrics, output1.Metrics, output2.Metrics, output3.Metrics}, 1, opts.UserOperatorContext.GetOr(nil))
	p.GoroutineDispatcher.StartTrackedGoroutine(func() {
		inputValue0 := input0.Wait()
		opMetrics.SetPhase(core.PHASE_RUNNING)
		outputValue0, outputValue1, outputValue2, outputValue3 := mapper(opContexts[0], inputValue0)
		output0.Resolve(outputValue0)
		output1.Resolve(outputValue1)
		output2.Resolve(outputValue2)
		output3.Resolve(outputValue3)
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return output0, output1, output2, output3
}

func MapValue1To5[TIn0 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any](
	input0 *pipeline.Value[TIn0],
	mapper func(context *core.OperatorContext, input0 TIn0) (output0 TOut0, output1 TOut1, output2 TOut2, output3 TOut3, output4 TOut4),
	opts *options.MapValueOptions,
) (*pipeline.Value[TOut0], *pipeline.Value[TOut1], *pipeline.Value[TOut2], *pipeline.Value[TOut3], *pipeline.Value[TOut4]) {
	p := input0.Pipeline
	operationName := utils.OptStrOrDefault(opts.OperationName, "Map value")
	output0Name := utils.OptGetOrDefault(opts.OutValueNames, 0, "Mapped value 0")
	output1Name := utils.OptGetOrDefault(opts.OutValueNames, 1, "Mapped value 1")
	output2Name := utils.OptGetOrDefault(opts.OutValueNames, 2, "Mapped value 2")
	output3Name := utils.OptGetOrDefault(opts.OutValueNames, 3, "Mapped value 3")
	output4Name := utils.OptGetOrDefault(opts.OutValueNames, 4, "Mapped value 4")
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MAP_VALUE, operationName)
	opMetrics.AddInputValue(input0.Metrics)
	output0 := pipeline.NewValue[TOut0](p, output0Name)
	output1 := pipeline.NewValue[TOut1](p, output1Name)
	output2 := pipeline.NewValue[TOut2](p, output2Name)
	output3 := pipeline.NewValue[TOut3](p, output3Name)
	output4 := pipeline.NewValue[TOut4](p, output4Name)
	opMetrics.AddOutputValue(output0.Metrics)
	opMetrics.AddOutputValue(output1.Metrics)
	opMetrics.AddOutputValue(output2.Metrics)
	opMetrics.AddOutputValue(output3.Metrics)
	opMetrics.AddOutputValue(output4.Metrics)
	opMetrics.Parallelism = 1
	opContexts := p.BuildOperatorContexts(opMetrics, nil, []*metrics.ValueMetrics{output0.Metrics, output1.Metrics, output2.Metrics, output3.Metrics, output4.Metrics}, 1, opts.UserOperatorContext.GetOr(nil))
	p.GoroutineDispatcher.StartTrackedGoroutine(func() {
		inputValue0 := input0.Wait()
		opMetrics.SetPhase(core.PHASE_RUNNING)
		outputValue0, outputValue1, outputValue2, outputValue3, outputValue4 := mapper(opContexts[0], inputValue0)
		output0.Resolve(outputValue0)
		output1.Resolve(outputValue1)
		output2.Resolve(outputValue2)
		output3.Resolve(outputValue3)
		output4.Resolve(outputValue4)
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return output0, output1, output2, output3, output4
}

func MapValue2To1[TIn0 any, TIn1 any, TOut0 any](
	input0 *pipeline.Value[TIn0], input1 *pipeline.Value[TIn1],
	mapper func(context *core.OperatorContext, input0 TIn0, input1 TIn1) (output0 TOut0),
	opts *options.MapValueOptions,
) *pipeline.Value[TOut0] {
	p := input0.Pipeline
	operationName := utils.OptStrOrDefault(opts.OperationName, "Map value")
	output0Name := utils.OptGetOrDefault(opts.OutValueNames, 0, "Mapped value 0")
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MAP_VALUE, operationName)
	opMetrics.AddInputValue(input0.Metrics)
	opMetrics.AddInputValue(input1.Metrics)
	output0 := pipeline.NewValue[TOut0](p, output0Name)
	opMetrics.AddOutputValue(output0.Metrics)
	opMetrics.Parallelism = 1
	opContexts := p.BuildOperatorContexts(opMetrics, nil, []*metrics.ValueMetrics{output0.Metrics}, 1, opts.UserOperatorContext.GetOr(nil))
	p.GoroutineDispatcher.StartTrackedGoroutine(func() {
		inputValue0 := input0.Wait()
		inputValue1 := input1.Wait()
		opMetrics.SetPhase(core.PHASE_RUNNING)
		outputValue0 := mapper(opContexts[0], inputValue0, inputValue1)
		output0.Resolve(outputValue0)
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return output0
}

func MapValue2To2[TIn0 any, TIn1 any, TOut0 any, TOut1 any](
	input0 *pipeline.Value[TIn0], input1 *pipeline.Value[TIn1],
	mapper func(context *core.OperatorContext, input0 TIn0, input1 TIn1) (output0 TOut0, output1 TOut1),
	opts *options.MapValueOptions,
) (*pipeline.Value[TOut0], *pipeline.Value[TOut1]) {
	p := input0.Pipeline
	operationName := utils.OptStrOrDefault(opts.OperationName, "Map value")
	output0Name := utils.OptGetOrDefault(opts.OutValueNames, 0, "Mapped value 0")
	output1Name := utils.OptGetOrDefault(opts.OutValueNames, 1, "Mapped value 1")
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MAP_VALUE, operationName)
	opMetrics.AddInputValue(input0.Metrics)
	opMetrics.AddInputValue(input1.Metrics)
	output0 := pipeline.NewValue[TOut0](p, output0Name)
	output1 := pipeline.NewValue[TOut1](p, output1Name)
	opMetrics.AddOutputValue(output0.Metrics)
	opMetrics.AddOutputValue(output1.Metrics)
	opMetrics.Parallelism = 1
	opContexts := p.BuildOperatorContexts(opMetrics, nil, []*metrics.ValueMetrics{output0.Metrics, output1.Metrics}, 1, opts.UserOperatorContext.GetOr(nil))
	p.GoroutineDispatcher.StartTrackedGoroutine(func() {
		inputValue0 := input0.Wait()
		inputValue1 := input1.Wait()
		opMetrics.SetPhase(core.PHASE_RUNNING)
		outputValue0, outputValue1 := mapper(opContexts[0], inputValue0, inputValue1)
		output0.Resolve(outputValue0)
		output1.Resolve(outputValue1)
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return output0, output1
}

func MapValue2To3[TIn0 any, TIn1 any, TOut0 any, TOut1 any, TOut2 any](
	input0 *pipeline.Value[TIn0], input1 *pipeline.Value[TIn1],
	mapper func(context *core.OperatorContext, input0 TIn0, input1 TIn1) (output0 TOut0, output1 TOut1, output2 TOut2),
	opts *options.MapValueOptions,
) (*pipeline.Value[TOut0], *pipeline.Value[TOut1], *pipeline.Value[TOut2]) {
	p := input0.Pipeline
	operationName := utils.OptStrOrDefault(opts.OperationName, "Map value")
	output0Name := utils.OptGetOrDefault(opts.OutValueNames, 0, "Mapped value 0")
	output1Name := utils.OptGetOrDefault(opts.OutValueNames, 1, "Mapped value 1")
	output2Name := utils.OptGetOrDefault(opts.OutValueNames, 2, "Mapped value 2")
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MAP_VALUE, operationName)
	opMetrics.AddInputValue(input0.Metrics)
	opMetrics.AddInputValue(input1.Metrics)
	output0 := pipeline.NewValue[TOut0](p, output0Name)
	output1 := pipeline.NewValue[TOut1](p, output1Name)
	output2 := pipeline.NewValue[TOut2](p, output2Name)
	opMetrics.AddOutputValue(output0.Metrics)
	opMetrics.AddOutputValue(output1.Metrics)
	opMetrics.AddOutputValue(output2.Metrics)
	opMetrics.Parallelism = 1
	opContexts := p.BuildOperatorContexts(opMetrics, nil, []*metrics.ValueMetrics{output0.Metrics, output1.Metrics, output2.Metrics}, 1, opts.UserOperatorContext.GetOr(nil))
	p.GoroutineDispatcher.StartTrackedGoroutine(func() {
		inputValue0 := input0.Wait()
		inputValue1 := input1.Wait()
		opMetrics.SetPhase(core.PHASE_RUNNING)
		outputValue0, outputValue1, outputValue2 := mapper(opContexts[0], inputValue0, inputValue1)
		output0.Resolve(outputValue0)
		output1.Resolve(outputValue1)
		output2.Resolve(outputValue2)
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return output0, output1, output2
}

func MapValue2To4[TIn0 any, TIn1 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any](
	input0 *pipeline.Value[TIn0], input1 *pipeline.Value[TIn1],
	mapper func(context *core.OperatorContext, input0 TIn0, input1 TIn1) (output0 TOut0, output1 TOut1, output2 TOut2, output3 TOut3),
	opts *options.MapValueOptions,
) (*pipeline.Value[TOut0], *pipeline.Value[TOut1], *pipeline.Value[TOut2], *pipeline.Value[TOut3]) {
	p := input0.Pipeline
	operationName := utils.OptStrOrDefault(opts.OperationName, "Map value")
	output0Name := utils.OptGetOrDefault(opts.OutValueNames, 0, "Mapped value 0")
	output1Name := utils.OptGetOrDefault(opts.OutValueNames, 1, "Mapped value 1")
	output2Name := utils.OptGetOrDefault(opts.OutValueNames, 2, "Mapped value 2")
	output3Name := utils.OptGetOrDefault(opts.OutValueNames, 3, "Mapped value 3")
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MAP_VALUE, operationName)
	opMetrics.AddInputValue(input0.Metrics)
	opMetrics.AddInputValue(input1.Metrics)
	output0 := pipeline.NewValue[TOut0](p, output0Name)
	output1 := pipeline.NewValue[TOut1](p, output1Name)
	output2 := pipeline.NewValue[TOut2](p, output2Name)
	output3 := pipeline.NewValue[TOut3](p, output3Name)
	opMetrics.AddOutputValue(output0.Metrics)
	opMetrics.AddOutputValue(output1.Metrics)
	opMetrics.AddOutputValue(output2.Metrics)
	opMetrics.AddOutputValue(output3.Metrics)
	opMetrics.Parallelism = 1
	opContexts := p.BuildOperatorContexts(opMetrics, nil, []*metrics.ValueMetrics{output0.Metrics, output1.Metrics, output2.Metrics, output3.Metrics}, 1, opts.UserOperatorContext.GetOr(nil))
	p.GoroutineDispatcher.StartTrackedGoroutine(func() {
		inputValue0 := input0.Wait()
		inputValue1 := input1.Wait()
		opMetrics.SetPhase(core.PHASE_RUNNING)
		outputValue0, outputValue1, outputValue2, outputValue3 := mapper(opContexts[0], inputValue0, inputValue1)
		output0.Resolve(outputValue0)
		output1.Resolve(outputValue1)
		output2.Resolve(outputValue2)
		output3.Resolve(outputValue3)
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return output0, output1, output2, output3
}

func MapValue2To5[TIn0 any, TIn1 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any](
	input0 *pipeline.Value[TIn0], input1 *pipeline.Value[TIn1],
	mapper func(context *core.OperatorContext, input0 TIn0, input1 TIn1) (output0 TOut0, output1 TOut1, output2 TOut2, output3 TOut3, output4 TOut4),
	opts *options.MapValueOptions,
) (*pipeline.Value[TOut0], *pipeline.Value[TOut1], *pipeline.Value[TOut2], *pipeline.Value[TOut3], *pipeline.Value[TOut4]) {
	p := input0.Pipeline
	operationName := utils.OptStrOrDefault(opts.OperationName, "Map value")
	output0Name := utils.OptGetOrDefault(opts.OutValueNames, 0, "Mapped value 0")
	output1Name := utils.OptGetOrDefault(opts.OutValueNames, 1, "Mapped value 1")
	output2Name := utils.OptGetOrDefault(opts.OutValueNames, 2, "Mapped value 2")
	output3Name := utils.OptGetOrDefault(opts.OutValueNames, 3, "Mapped value 3")
	output4Name := utils.OptGetOrDefault(opts.OutValueNames, 4, "Mapped value 4")
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MAP_VALUE, operationName)
	opMetrics.AddInputValue(input0.Metrics)
	opMetrics.AddInputValue(input1.Metrics)
	output0 := pipeline.NewValue[TOut0](p, output0Name)
	output1 := pipeline.NewValue[TOut1](p, output1Name)
	output2 := pipeline.NewValue[TOut2](p, output2Name)
	output3 := pipeline.NewValue[TOut3](p, output3Name)
	output4 := pipeline.NewValue[TOut4](p, output4Name)
	opMetrics.AddOutputValue(output0.Metrics)
	opMetrics.AddOutputValue(output1.Metrics)
	opMetrics.AddOutputValue(output2.Metrics)
	opMetrics.AddOutputValue(output3.Metrics)
	opMetrics.AddOutputValue(output4.Metrics)
	opMetrics.Parallelism = 1
	opContexts := p.BuildOperatorContexts(opMetrics, nil, []*metrics.ValueMetrics{output0.Metrics, output1.Metrics, output2.Metrics, output3.Metrics, output4.Metrics}, 1, opts.UserOperatorContext.GetOr(nil))
	p.GoroutineDispatcher.StartTrackedGoroutine(func() {
		inputValue0 := input0.Wait()
		inputValue1 := input1.Wait()
		opMetrics.SetPhase(core.PHASE_RUNNING)
		outputValue0, outputValue1, outputValue2, outputValue3, outputValue4 := mapper(opContexts[0], inputValue0, inputValue1)
		output0.Resolve(outputValue0)
		output1.Resolve(outputValue1)
		output2.Resolve(outputValue2)
		output3.Resolve(outputValue3)
		output4.Resolve(outputValue4)
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return output0, output1, output2, output3, output4
}

func MapValue3To1[TIn0 any, TIn1 any, TIn2 any, TOut0 any](
	input0 *pipeline.Value[TIn0], input1 *pipeline.Value[TIn1], input2 *pipeline.Value[TIn2],
	mapper func(context *core.OperatorContext, input0 TIn0, input1 TIn1, input2 TIn2) (output0 TOut0),
	opts *options.MapValueOptions,
) *pipeline.Value[TOut0] {
	p := input0.Pipeline
	operationName := utils.OptStrOrDefault(opts.OperationName, "Map value")
	output0Name := utils.OptGetOrDefault(opts.OutValueNames, 0, "Mapped value 0")
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MAP_VALUE, operationName)
	opMetrics.AddInputValue(input0.Metrics)
	opMetrics.AddInputValue(input1.Metrics)
	opMetrics.AddInputValue(input2.Metrics)
	output0 := pipeline.NewValue[TOut0](p, output0Name)
	opMetrics.AddOutputValue(output0.Metrics)
	opMetrics.Parallelism = 1
	opContexts := p.BuildOperatorContexts(opMetrics, nil, []*metrics.ValueMetrics{output0.Metrics}, 1, opts.UserOperatorContext.GetOr(nil))
	p.GoroutineDispatcher.StartTrackedGoroutine(func() {
		inputValue0 := input0.Wait()
		inputValue1 := input1.Wait()
		inputValue2 := input2.Wait()
		opMetrics.SetPhase(core.PHASE_RUNNING)
		outputValue0 := mapper(opContexts[0], inputValue0, inputValue1, inputValue2)
		output0.Resolve(outputValue0)
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return output0
}

func MapValue3To2[TIn0 any, TIn1 any, TIn2 any, TOut0 any, TOut1 any](
	input0 *pipeline.Value[TIn0], input1 *pipeline.Value[TIn1], input2 *pipeline.Value[TIn2],
	mapper func(context *core.OperatorContext, input0 TIn0, input1 TIn1, input2 TIn2) (output0 TOut0, output1 TOut1),
	opts *options.MapValueOptions,
) (*pipeline.Value[TOut0], *pipeline.Value[TOut1]) {
	p := input0.Pipeline
	operationName := utils.OptStrOrDefault(opts.OperationName, "Map value")
	output0Name := utils.OptGetOrDefault(opts.OutValueNames, 0, "Mapped value 0")
	output1Name := utils.OptGetOrDefault(opts.OutValueNames, 1, "Mapped value 1")
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MAP_VALUE, operationName)
	opMetrics.AddInputValue(input0.Metrics)
	opMetrics.AddInputValue(input1.Metrics)
	opMetrics.AddInputValue(input2.Metrics)
	output0 := pipeline.NewValue[TOut0](p, output0Name)
	output1 := pipeline.NewValue[TOut1](p, output1Name)
	opMetrics.AddOutputValue(output0.Metrics)
	opMetrics.AddOutputValue(output1.Metrics)
	opMetrics.Parallelism = 1
	opContexts := p.BuildOperatorContexts(opMetrics, nil, []*metrics.ValueMetrics{output0.Metrics, output1.Metrics}, 1, opts.UserOperatorContext.GetOr(nil))
	p.GoroutineDispatcher.StartTrackedGoroutine(func() {
		inputValue0 := input0.Wait()
		inputValue1 := input1.Wait()
		inputValue2 := input2.Wait()
		opMetrics.SetPhase(core.PHASE_RUNNING)
		outputValue0, outputValue1 := mapper(opContexts[0], inputValue0, inputValue1, inputValue2)
		output0.Resolve(outputValue0)
		output1.Resolve(outputValue1)
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return output0, output1
}

func MapValue3To3[TIn0 any, TIn1 any, TIn2 any, TOut0 any, TOut1 any, TOut2 any](
	input0 *pipeline.Value[TIn0], input1 *pipeline.Value[TIn1], input2 *pipeline.Value[TIn2],
	mapper func(context *core.OperatorContext, input0 TIn0, input1 TIn1, input2 TIn2) (output0 TOut0, output1 TOut1, output2 TOut2),
	opts *options.MapValueOptions,
) (*pipeline.Value[TOut0], *pipeline.Value[TOut1], *pipeline.Value[TOut2]) {
	p := input0.Pipeline
	operationName := utils.OptStrOrDefault(opts.OperationName, "Map value")
	output0Name := utils.OptGetOrDefault(opts.OutValueNames, 0, "Mapped value 0")
	output1Name := utils.OptGetOrDefault(opts.OutValueNames, 1, "Mapped value 1")
	output2Name := utils.OptGetOrDefault(opts.OutValueNames, 2, "Mapped value 2")
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MAP_VALUE, operationName)
	opMetrics.AddInputValue(input0.Metrics)
	opMetrics.AddInputValue(input1.Metrics)
	opMetrics.AddInputValue(input2.Metrics)
	output0 := pipeline.NewValue[TOut0](p, output0Name)
	output1 := pipeline.NewValue[TOut1](p, output1Name)
	output2 := pipeline.NewValue[TOut2](p, output2Name)
	opMetrics.AddOutputValue(output0.Metrics)
	opMetrics.AddOutputValue(output1.Metrics)
	opMetrics.AddOutputValue(output2.Metrics)
	opMetrics.Parallelism = 1
	opContexts := p.BuildOperatorContexts(opMetrics, nil, []*metrics.ValueMetrics{output0.Metrics, output1.Metrics, output2.Metrics}, 1, opts.UserOperatorContext.GetOr(nil))
	p.GoroutineDispatcher.StartTrackedGoroutine(func() {
		inputValue0 := input0.Wait()
		inputValue1 := input1.Wait()
		inputValue2 := input2.Wait()
		opMetrics.SetPhase(core.PHASE_RUNNING)
		outputValue0, outputValue1, outputValue2 := mapper(opContexts[0], inputValue0, inputValue1, inputValue2)
		output0.Resolve(outputValue0)
		output1.Resolve(outputValue1)
		output2.Resolve(outputValue2)
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return output0, output1, output2
}

func MapValue3To4[TIn0 any, TIn1 any, TIn2 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any](
	input0 *pipeline.Value[TIn0], input1 *pipeline.Value[TIn1], input2 *pipeline.Value[TIn2],
	mapper func(context *core.OperatorContext, input0 TIn0, input1 TIn1, input2 TIn2) (output0 TOut0, output1 TOut1, output2 TOut2, output3 TOut3),
	opts *options.MapValueOptions,
) (*pipeline.Value[TOut0], *pipeline.Value[TOut1], *pipeline.Value[TOut2], *pipeline.Value[TOut3]) {
	p := input0.Pipeline
	operationName := utils.OptStrOrDefault(opts.OperationName, "Map value")
	output0Name := utils.OptGetOrDefault(opts.OutValueNames, 0, "Mapped value 0")
	output1Name := utils.OptGetOrDefault(opts.OutValueNames, 1, "Mapped value 1")
	output2Name := utils.OptGetOrDefault(opts.OutValueNames, 2, "Mapped value 2")
	output3Name := utils.OptGetOrDefault(opts.OutValueNames, 3, "Mapped value 3")
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MAP_VALUE, operationName)
	opMetrics.AddInputValue(input0.Metrics)
	opMetrics.AddInputValue(input1.Metrics)
	opMetrics.AddInputValue(input2.Metrics)
	output0 := pipeline.NewValue[TOut0](p, output0Name)
	output1 := pipeline.NewValue[TOut1](p, output1Name)
	output2 := pipeline.NewValue[TOut2](p, output2Name)
	output3 := pipeline.NewValue[TOut3](p, output3Name)
	opMetrics.AddOutputValue(output0.Metrics)
	opMetrics.AddOutputValue(output1.Metrics)
	opMetrics.AddOutputValue(output2.Metrics)
	opMetrics.AddOutputValue(output3.Metrics)
	opMetrics.Parallelism = 1
	opContexts := p.BuildOperatorContexts(opMetrics, nil, []*metrics.ValueMetrics{output0.Metrics, output1.Metrics, output2.Metrics, output3.Metrics}, 1, opts.UserOperatorContext.GetOr(nil))
	p.GoroutineDispatcher.StartTrackedGoroutine(func() {
		inputValue0 := input0.Wait()
		inputValue1 := input1.Wait()
		inputValue2 := input2.Wait()
		opMetrics.SetPhase(core.PHASE_RUNNING)
		outputValue0, outputValue1, outputValue2, outputValue3 := mapper(opContexts[0], inputValue0, inputValue1, inputValue2)
		output0.Resolve(outputValue0)
		output1.Resolve(outputValue1)
		output2.Resolve(outputValue2)
		output3.Resolve(outputValue3)
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return output0, output1, output2, output3
}

func MapValue3To5[TIn0 any, TIn1 any, TIn2 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any](
	input0 *pipeline.Value[TIn0], input1 *pipeline.Value[TIn1], input2 *pipeline.Value[TIn2],
	mapper func(context *core.OperatorContext, input0 TIn0, input1 TIn1, input2 TIn2) (output0 TOut0, output1 TOut1, output2 TOut2, output3 TOut3, output4 TOut4),
	opts *options.MapValueOptions,
) (*pipeline.Value[TOut0], *pipeline.Value[TOut1], *pipeline.Value[TOut2], *pipeline.Value[TOut3], *pipeline.Value[TOut4]) {
	p := input0.Pipeline
	operationName := utils.OptStrOrDefault(opts.OperationName, "Map value")
	output0Name := utils.OptGetOrDefault(opts.OutValueNames, 0, "Mapped value 0")
	output1Name := utils.OptGetOrDefault(opts.OutValueNames, 1, "Mapped value 1")
	output2Name := utils.OptGetOrDefault(opts.OutValueNames, 2, "Mapped value 2")
	output3Name := utils.OptGetOrDefault(opts.OutValueNames, 3, "Mapped value 3")
	output4Name := utils.OptGetOrDefault(opts.OutValueNames, 4, "Mapped value 4")
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MAP_VALUE, operationName)
	opMetrics.AddInputValue(input0.Metrics)
	opMetrics.AddInputValue(input1.Metrics)
	opMetrics.AddInputValue(input2.Metrics)
	output0 := pipeline.NewValue[TOut0](p, output0Name)
	output1 := pipeline.NewValue[TOut1](p, output1Name)
	output2 := pipeline.NewValue[TOut2](p, output2Name)
	output3 := pipeline.NewValue[TOut3](p, output3Name)
	output4 := pipeline.NewValue[TOut4](p, output4Name)
	opMetrics.AddOutputValue(output0.Metrics)
	opMetrics.AddOutputValue(output1.Metrics)
	opMetrics.AddOutputValue(output2.Metrics)
	opMetrics.AddOutputValue(output3.Metrics)
	opMetrics.AddOutputValue(output4.Metrics)
	opMetrics.Parallelism = 1
	opContexts := p.BuildOperatorContexts(opMetrics, nil, []*metrics.ValueMetrics{output0.Metrics, output1.Metrics, output2.Metrics, output3.Metrics, output4.Metrics}, 1, opts.UserOperatorContext.GetOr(nil))
	p.GoroutineDispatcher.StartTrackedGoroutine(func() {
		inputValue0 := input0.Wait()
		inputValue1 := input1.Wait()
		inputValue2 := input2.Wait()
		opMetrics.SetPhase(core.PHASE_RUNNING)
		outputValue0, outputValue1, outputValue2, outputValue3, outputValue4 := mapper(opContexts[0], inputValue0, inputValue1, inputValue2)
		output0.Resolve(outputValue0)
		output1.Resolve(outputValue1)
		output2.Resolve(outputValue2)
		output3.Resolve(outputValue3)
		output4.Resolve(outputValue4)
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return output0, output1, output2, output3, output4
}

func MapValue4To1[TIn0 any, TIn1 any, TIn2 any, TIn3 any, TOut0 any](
	input0 *pipeline.Value[TIn0], input1 *pipeline.Value[TIn1], input2 *pipeline.Value[TIn2], input3 *pipeline.Value[TIn3],
	mapper func(context *core.OperatorContext, input0 TIn0, input1 TIn1, input2 TIn2, input3 TIn3) (output0 TOut0),
	opts *options.MapValueOptions,
) *pipeline.Value[TOut0] {
	p := input0.Pipeline
	operationName := utils.OptStrOrDefault(opts.OperationName, "Map value")
	output0Name := utils.OptGetOrDefault(opts.OutValueNames, 0, "Mapped value 0")
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MAP_VALUE, operationName)
	opMetrics.AddInputValue(input0.Metrics)
	opMetrics.AddInputValue(input1.Metrics)
	opMetrics.AddInputValue(input2.Metrics)
	opMetrics.AddInputValue(input3.Metrics)
	output0 := pipeline.NewValue[TOut0](p, output0Name)
	opMetrics.AddOutputValue(output0.Metrics)
	opMetrics.Parallelism = 1
	opContexts := p.BuildOperatorContexts(opMetrics, nil, []*metrics.ValueMetrics{output0.Metrics}, 1, opts.UserOperatorContext.GetOr(nil))
	p.GoroutineDispatcher.StartTrackedGoroutine(func() {
		inputValue0 := input0.Wait()
		inputValue1 := input1.Wait()
		inputValue2 := input2.Wait()
		inputValue3 := input3.Wait()
		opMetrics.SetPhase(core.PHASE_RUNNING)
		outputValue0 := mapper(opContexts[0], inputValue0, inputValue1, inputValue2, inputValue3)
		output0.Resolve(outputValue0)
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return output0
}

func MapValue4To2[TIn0 any, TIn1 any, TIn2 any, TIn3 any, TOut0 any, TOut1 any](
	input0 *pipeline.Value[TIn0], input1 *pipeline.Value[TIn1], input2 *pipeline.Value[TIn2], input3 *pipeline.Value[TIn3],
	mapper func(context *core.OperatorContext, input0 TIn0, input1 TIn1, input2 TIn2, input3 TIn3) (output0 TOut0, output1 TOut1),
	opts *options.MapValueOptions,
) (*pipeline.Value[TOut0], *pipeline.Value[TOut1]) {
	p := input0.Pipeline
	operationName := utils.OptStrOrDefault(opts.OperationName, "Map value")
	output0Name := utils.OptGetOrDefault(opts.OutValueNames, 0, "Mapped value 0")
	output1Name := utils.OptGetOrDefault(opts.OutValueNames, 1, "Mapped value 1")
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MAP_VALUE, operationName)
	opMetrics.AddInputValue(input0.Metrics)
	opMetrics.AddInputValue(input1.Metrics)
	opMetrics.AddInputValue(input2.Metrics)
	opMetrics.AddInputValue(input3.Metrics)
	output0 := pipeline.NewValue[TOut0](p, output0Name)
	output1 := pipeline.NewValue[TOut1](p, output1Name)
	opMetrics.AddOutputValue(output0.Metrics)
	opMetrics.AddOutputValue(output1.Metrics)
	opMetrics.Parallelism = 1
	opContexts := p.BuildOperatorContexts(opMetrics, nil, []*metrics.ValueMetrics{output0.Metrics, output1.Metrics}, 1, opts.UserOperatorContext.GetOr(nil))
	p.GoroutineDispatcher.StartTrackedGoroutine(func() {
		inputValue0 := input0.Wait()
		inputValue1 := input1.Wait()
		inputValue2 := input2.Wait()
		inputValue3 := input3.Wait()
		opMetrics.SetPhase(core.PHASE_RUNNING)
		outputValue0, outputValue1 := mapper(opContexts[0], inputValue0, inputValue1, inputValue2, inputValue3)
		output0.Resolve(outputValue0)
		output1.Resolve(outputValue1)
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return output0, output1
}

func MapValue4To3[TIn0 any, TIn1 any, TIn2 any, TIn3 any, TOut0 any, TOut1 any, TOut2 any](
	input0 *pipeline.Value[TIn0], input1 *pipeline.Value[TIn1], input2 *pipeline.Value[TIn2], input3 *pipeline.Value[TIn3],
	mapper func(context *core.OperatorContext, input0 TIn0, input1 TIn1, input2 TIn2, input3 TIn3) (output0 TOut0, output1 TOut1, output2 TOut2),
	opts *options.MapValueOptions,
) (*pipeline.Value[TOut0], *pipeline.Value[TOut1], *pipeline.Value[TOut2]) {
	p := input0.Pipeline
	operationName := utils.OptStrOrDefault(opts.OperationName, "Map value")
	output0Name := utils.OptGetOrDefault(opts.OutValueNames, 0, "Mapped value 0")
	output1Name := utils.OptGetOrDefault(opts.OutValueNames, 1, "Mapped value 1")
	output2Name := utils.OptGetOrDefault(opts.OutValueNames, 2, "Mapped value 2")
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MAP_VALUE, operationName)
	opMetrics.AddInputValue(input0.Metrics)
	opMetrics.AddInputValue(input1.Metrics)
	opMetrics.AddInputValue(input2.Metrics)
	opMetrics.AddInputValue(input3.Metrics)
	output0 := pipeline.NewValue[TOut0](p, output0Name)
	output1 := pipeline.NewValue[TOut1](p, output1Name)
	output2 := pipeline.NewValue[TOut2](p, output2Name)
	opMetrics.AddOutputValue(output0.Metrics)
	opMetrics.AddOutputValue(output1.Metrics)
	opMetrics.AddOutputValue(output2.Metrics)
	opMetrics.Parallelism = 1
	opContexts := p.BuildOperatorContexts(opMetrics, nil, []*metrics.ValueMetrics{output0.Metrics, output1.Metrics, output2.Metrics}, 1, opts.UserOperatorContext.GetOr(nil))
	p.GoroutineDispatcher.StartTrackedGoroutine(func() {
		inputValue0 := input0.Wait()
		inputValue1 := input1.Wait()
		inputValue2 := input2.Wait()
		inputValue3 := input3.Wait()
		opMetrics.SetPhase(core.PHASE_RUNNING)
		outputValue0, outputValue1, outputValue2 := mapper(opContexts[0], inputValue0, inputValue1, inputValue2, inputValue3)
		output0.Resolve(outputValue0)
		output1.Resolve(outputValue1)
		output2.Resolve(outputValue2)
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return output0, output1, output2
}

func MapValue4To4[TIn0 any, TIn1 any, TIn2 any, TIn3 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any](
	input0 *pipeline.Value[TIn0], input1 *pipeline.Value[TIn1], input2 *pipeline.Value[TIn2], input3 *pipeline.Value[TIn3],
	mapper func(context *core.OperatorContext, input0 TIn0, input1 TIn1, input2 TIn2, input3 TIn3) (output0 TOut0, output1 TOut1, output2 TOut2, output3 TOut3),
	opts *options.MapValueOptions,
) (*pipeline.Value[TOut0], *pipeline.Value[TOut1], *pipeline.Value[TOut2], *pipeline.Value[TOut3]) {
	p := input0.Pipeline
	operationName := utils.OptStrOrDefault(opts.OperationName, "Map value")
	output0Name := utils.OptGetOrDefault(opts.OutValueNames, 0, "Mapped value 0")
	output1Name := utils.OptGetOrDefault(opts.OutValueNames, 1, "Mapped value 1")
	output2Name := utils.OptGetOrDefault(opts.OutValueNames, 2, "Mapped value 2")
	output3Name := utils.OptGetOrDefault(opts.OutValueNames, 3, "Mapped value 3")
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MAP_VALUE, operationName)
	opMetrics.AddInputValue(input0.Metrics)
	opMetrics.AddInputValue(input1.Metrics)
	opMetrics.AddInputValue(input2.Metrics)
	opMetrics.AddInputValue(input3.Metrics)
	output0 := pipeline.NewValue[TOut0](p, output0Name)
	output1 := pipeline.NewValue[TOut1](p, output1Name)
	output2 := pipeline.NewValue[TOut2](p, output2Name)
	output3 := pipeline.NewValue[TOut3](p, output3Name)
	opMetrics.AddOutputValue(output0.Metrics)
	opMetrics.AddOutputValue(output1.Metrics)
	opMetrics.AddOutputValue(output2.Metrics)
	opMetrics.AddOutputValue(output3.Metrics)
	opMetrics.Parallelism = 1
	opContexts := p.BuildOperatorContexts(opMetrics, nil, []*metrics.ValueMetrics{output0.Metrics, output1.Metrics, output2.Metrics, output3.Metrics}, 1, opts.UserOperatorContext.GetOr(nil))
	p.GoroutineDispatcher.StartTrackedGoroutine(func() {
		inputValue0 := input0.Wait()
		inputValue1 := input1.Wait()
		inputValue2 := input2.Wait()
		inputValue3 := input3.Wait()
		opMetrics.SetPhase(core.PHASE_RUNNING)
		outputValue0, outputValue1, outputValue2, outputValue3 := mapper(opContexts[0], inputValue0, inputValue1, inputValue2, inputValue3)
		output0.Resolve(outputValue0)
		output1.Resolve(outputValue1)
		output2.Resolve(outputValue2)
		output3.Resolve(outputValue3)
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return output0, output1, output2, output3
}

func MapValue4To5[TIn0 any, TIn1 any, TIn2 any, TIn3 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any](
	input0 *pipeline.Value[TIn0], input1 *pipeline.Value[TIn1], input2 *pipeline.Value[TIn2], input3 *pipeline.Value[TIn3],
	mapper func(context *core.OperatorContext, input0 TIn0, input1 TIn1, input2 TIn2, input3 TIn3) (output0 TOut0, output1 TOut1, output2 TOut2, output3 TOut3, output4 TOut4),
	opts *options.MapValueOptions,
) (*pipeline.Value[TOut0], *pipeline.Value[TOut1], *pipeline.Value[TOut2], *pipeline.Value[TOut3], *pipeline.Value[TOut4]) {
	p := input0.Pipeline
	operationName := utils.OptStrOrDefault(opts.OperationName, "Map value")
	output0Name := utils.OptGetOrDefault(opts.OutValueNames, 0, "Mapped value 0")
	output1Name := utils.OptGetOrDefault(opts.OutValueNames, 1, "Mapped value 1")
	output2Name := utils.OptGetOrDefault(opts.OutValueNames, 2, "Mapped value 2")
	output3Name := utils.OptGetOrDefault(opts.OutValueNames, 3, "Mapped value 3")
	output4Name := utils.OptGetOrDefault(opts.OutValueNames, 4, "Mapped value 4")
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MAP_VALUE, operationName)
	opMetrics.AddInputValue(input0.Metrics)
	opMetrics.AddInputValue(input1.Metrics)
	opMetrics.AddInputValue(input2.Metrics)
	opMetrics.AddInputValue(input3.Metrics)
	output0 := pipeline.NewValue[TOut0](p, output0Name)
	output1 := pipeline.NewValue[TOut1](p, output1Name)
	output2 := pipeline.NewValue[TOut2](p, output2Name)
	output3 := pipeline.NewValue[TOut3](p, output3Name)
	output4 := pipeline.NewValue[TOut4](p, output4Name)
	opMetrics.AddOutputValue(output0.Metrics)
	opMetrics.AddOutputValue(output1.Metrics)
	opMetrics.AddOutputValue(output2.Metrics)
	opMetrics.AddOutputValue(output3.Metrics)
	opMetrics.AddOutputValue(output4.Metrics)
	opMetrics.Parallelism = 1
	opContexts := p.BuildOperatorContexts(opMetrics, nil, []*metrics.ValueMetrics{output0.Metrics, output1.Metrics, output2.Metrics, output3.Metrics, output4.Metrics}, 1, opts.UserOperatorContext.GetOr(nil))
	p.GoroutineDispatcher.StartTrackedGoroutine(func() {
		inputValue0 := input0.Wait()
		inputValue1 := input1.Wait()
		inputValue2 := input2.Wait()
		inputValue3 := input3.Wait()
		opMetrics.SetPhase(core.PHASE_RUNNING)
		outputValue0, outputValue1, outputValue2, outputValue3, outputValue4 := mapper(opContexts[0], inputValue0, inputValue1, inputValue2, inputValue3)
		output0.Resolve(outputValue0)
		output1.Resolve(outputValue1)
		output2.Resolve(outputValue2)
		output3.Resolve(outputValue3)
		output4.Resolve(outputValue4)
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return output0, output1, output2, output3, output4
}

func MapValue5To1[TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIn4 any, TOut0 any](
	input0 *pipeline.Value[TIn0], input1 *pipeline.Value[TIn1], input2 *pipeline.Value[TIn2], input3 *pipeline.Value[TIn3], input4 *pipeline.Value[TIn4],
	mapper func(context *core.OperatorContext, input0 TIn0, input1 TIn1, input2 TIn2, input3 TIn3, input4 TIn4) (output0 TOut0),
	opts *options.MapValueOptions,
) *pipeline.Value[TOut0] {
	p := input0.Pipeline
	operationName := utils.OptStrOrDefault(opts.OperationName, "Map value")
	output0Name := utils.OptGetOrDefault(opts.OutValueNames, 0, "Mapped value 0")
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MAP_VALUE, operationName)
	opMetrics.AddInputValue(input0.Metrics)
	opMetrics.AddInputValue(input1.Metrics)
	opMetrics.AddInputValue(input2.Metrics)
	opMetrics.AddInputValue(input3.Metrics)
	opMetrics.AddInputValue(input4.Metrics)
	output0 := pipeline.NewValue[TOut0](p, output0Name)
	opMetrics.AddOutputValue(output0.Metrics)
	opMetrics.Parallelism = 1
	opContexts := p.BuildOperatorContexts(opMetrics, nil, []*metrics.ValueMetrics{output0.Metrics}, 1, opts.UserOperatorContext.GetOr(nil))
	p.GoroutineDispatcher.StartTrackedGoroutine(func() {
		inputValue0 := input0.Wait()
		inputValue1 := input1.Wait()
		inputValue2 := input2.Wait()
		inputValue3 := input3.Wait()
		inputValue4 := input4.Wait()
		opMetrics.SetPhase(core.PHASE_RUNNING)
		outputValue0 := mapper(opContexts[0], inputValue0, inputValue1, inputValue2, inputValue3, inputValue4)
		output0.Resolve(outputValue0)
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return output0
}

func MapValue5To2[TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIn4 any, TOut0 any, TOut1 any](
	input0 *pipeline.Value[TIn0], input1 *pipeline.Value[TIn1], input2 *pipeline.Value[TIn2], input3 *pipeline.Value[TIn3], input4 *pipeline.Value[TIn4],
	mapper func(context *core.OperatorContext, input0 TIn0, input1 TIn1, input2 TIn2, input3 TIn3, input4 TIn4) (output0 TOut0, output1 TOut1),
	opts *options.MapValueOptions,
) (*pipeline.Value[TOut0], *pipeline.Value[TOut1]) {
	p := input0.Pipeline
	operationName := utils.OptStrOrDefault(opts.OperationName, "Map value")
	output0Name := utils.OptGetOrDefault(opts.OutValueNames, 0, "Mapped value 0")
	output1Name := utils.OptGetOrDefault(opts.OutValueNames, 1, "Mapped value 1")
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MAP_VALUE, operationName)
	opMetrics.AddInputValue(input0.Metrics)
	opMetrics.AddInputValue(input1.Metrics)
	opMetrics.AddInputValue(input2.Metrics)
	opMetrics.AddInputValue(input3.Metrics)
	opMetrics.AddInputValue(input4.Metrics)
	output0 := pipeline.NewValue[TOut0](p, output0Name)
	output1 := pipeline.NewValue[TOut1](p, output1Name)
	opMetrics.AddOutputValue(output0.Metrics)
	opMetrics.AddOutputValue(output1.Metrics)
	opMetrics.Parallelism = 1
	opContexts := p.BuildOperatorContexts(opMetrics, nil, []*metrics.ValueMetrics{output0.Metrics, output1.Metrics}, 1, opts.UserOperatorContext.GetOr(nil))
	p.GoroutineDispatcher.StartTrackedGoroutine(func() {
		inputValue0 := input0.Wait()
		inputValue1 := input1.Wait()
		inputValue2 := input2.Wait()
		inputValue3 := input3.Wait()
		inputValue4 := input4.Wait()
		opMetrics.SetPhase(core.PHASE_RUNNING)
		outputValue0, outputValue1 := mapper(opContexts[0], inputValue0, inputValue1, inputValue2, inputValue3, inputValue4)
		output0.Resolve(outputValue0)
		output1.Resolve(outputValue1)
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return output0, output1
}

func MapValue5To3[TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIn4 any, TOut0 any, TOut1 any, TOut2 any](
	input0 *pipeline.Value[TIn0], input1 *pipeline.Value[TIn1], input2 *pipeline.Value[TIn2], input3 *pipeline.Value[TIn3], input4 *pipeline.Value[TIn4],
	mapper func(context *core.OperatorContext, input0 TIn0, input1 TIn1, input2 TIn2, input3 TIn3, input4 TIn4) (output0 TOut0, output1 TOut1, output2 TOut2),
	opts *options.MapValueOptions,
) (*pipeline.Value[TOut0], *pipeline.Value[TOut1], *pipeline.Value[TOut2]) {
	p := input0.Pipeline
	operationName := utils.OptStrOrDefault(opts.OperationName, "Map value")
	output0Name := utils.OptGetOrDefault(opts.OutValueNames, 0, "Mapped value 0")
	output1Name := utils.OptGetOrDefault(opts.OutValueNames, 1, "Mapped value 1")
	output2Name := utils.OptGetOrDefault(opts.OutValueNames, 2, "Mapped value 2")
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MAP_VALUE, operationName)
	opMetrics.AddInputValue(input0.Metrics)
	opMetrics.AddInputValue(input1.Metrics)
	opMetrics.AddInputValue(input2.Metrics)
	opMetrics.AddInputValue(input3.Metrics)
	opMetrics.AddInputValue(input4.Metrics)
	output0 := pipeline.NewValue[TOut0](p, output0Name)
	output1 := pipeline.NewValue[TOut1](p, output1Name)
	output2 := pipeline.NewValue[TOut2](p, output2Name)
	opMetrics.AddOutputValue(output0.Metrics)
	opMetrics.AddOutputValue(output1.Metrics)
	opMetrics.AddOutputValue(output2.Metrics)
	opMetrics.Parallelism = 1
	opContexts := p.BuildOperatorContexts(opMetrics, nil, []*metrics.ValueMetrics{output0.Metrics, output1.Metrics, output2.Metrics}, 1, opts.UserOperatorContext.GetOr(nil))
	p.GoroutineDispatcher.StartTrackedGoroutine(func() {
		inputValue0 := input0.Wait()
		inputValue1 := input1.Wait()
		inputValue2 := input2.Wait()
		inputValue3 := input3.Wait()
		inputValue4 := input4.Wait()
		opMetrics.SetPhase(core.PHASE_RUNNING)
		outputValue0, outputValue1, outputValue2 := mapper(opContexts[0], inputValue0, inputValue1, inputValue2, inputValue3, inputValue4)
		output0.Resolve(outputValue0)
		output1.Resolve(outputValue1)
		output2.Resolve(outputValue2)
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return output0, output1, output2
}

func MapValue5To4[TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIn4 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any](
	input0 *pipeline.Value[TIn0], input1 *pipeline.Value[TIn1], input2 *pipeline.Value[TIn2], input3 *pipeline.Value[TIn3], input4 *pipeline.Value[TIn4],
	mapper func(context *core.OperatorContext, input0 TIn0, input1 TIn1, input2 TIn2, input3 TIn3, input4 TIn4) (output0 TOut0, output1 TOut1, output2 TOut2, output3 TOut3),
	opts *options.MapValueOptions,
) (*pipeline.Value[TOut0], *pipeline.Value[TOut1], *pipeline.Value[TOut2], *pipeline.Value[TOut3]) {
	p := input0.Pipeline
	operationName := utils.OptStrOrDefault(opts.OperationName, "Map value")
	output0Name := utils.OptGetOrDefault(opts.OutValueNames, 0, "Mapped value 0")
	output1Name := utils.OptGetOrDefault(opts.OutValueNames, 1, "Mapped value 1")
	output2Name := utils.OptGetOrDefault(opts.OutValueNames, 2, "Mapped value 2")
	output3Name := utils.OptGetOrDefault(opts.OutValueNames, 3, "Mapped value 3")
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MAP_VALUE, operationName)
	opMetrics.AddInputValue(input0.Metrics)
	opMetrics.AddInputValue(input1.Metrics)
	opMetrics.AddInputValue(input2.Metrics)
	opMetrics.AddInputValue(input3.Metrics)
	opMetrics.AddInputValue(input4.Metrics)
	output0 := pipeline.NewValue[TOut0](p, output0Name)
	output1 := pipeline.NewValue[TOut1](p, output1Name)
	output2 := pipeline.NewValue[TOut2](p, output2Name)
	output3 := pipeline.NewValue[TOut3](p, output3Name)
	opMetrics.AddOutputValue(output0.Metrics)
	opMetrics.AddOutputValue(output1.Metrics)
	opMetrics.AddOutputValue(output2.Metrics)
	opMetrics.AddOutputValue(output3.Metrics)
	opMetrics.Parallelism = 1
	opContexts := p.BuildOperatorContexts(opMetrics, nil, []*metrics.ValueMetrics{output0.Metrics, output1.Metrics, output2.Metrics, output3.Metrics}, 1, opts.UserOperatorContext.GetOr(nil))
	p.GoroutineDispatcher.StartTrackedGoroutine(func() {
		inputValue0 := input0.Wait()
		inputValue1 := input1.Wait()
		inputValue2 := input2.Wait()
		inputValue3 := input3.Wait()
		inputValue4 := input4.Wait()
		opMetrics.SetPhase(core.PHASE_RUNNING)
		outputValue0, outputValue1, outputValue2, outputValue3 := mapper(opContexts[0], inputValue0, inputValue1, inputValue2, inputValue3, inputValue4)
		output0.Resolve(outputValue0)
		output1.Resolve(outputValue1)
		output2.Resolve(outputValue2)
		output3.Resolve(outputValue3)
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return output0, output1, output2, output3
}

func MapValue5To5[TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIn4 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any](
	input0 *pipeline.Value[TIn0], input1 *pipeline.Value[TIn1], input2 *pipeline.Value[TIn2], input3 *pipeline.Value[TIn3], input4 *pipeline.Value[TIn4],
	mapper func(context *core.OperatorContext, input0 TIn0, input1 TIn1, input2 TIn2, input3 TIn3, input4 TIn4) (output0 TOut0, output1 TOut1, output2 TOut2, output3 TOut3, output4 TOut4),
	opts *options.MapValueOptions,
) (*pipeline.Value[TOut0], *pipeline.Value[TOut1], *pipeline.Value[TOut2], *pipeline.Value[TOut3], *pipeline.Value[TOut4]) {
	p := input0.Pipeline
	operationName := utils.OptStrOrDefault(opts.OperationName, "Map value")
	output0Name := utils.OptGetOrDefault(opts.OutValueNames, 0, "Mapped value 0")
	output1Name := utils.OptGetOrDefault(opts.OutValueNames, 1, "Mapped value 1")
	output2Name := utils.OptGetOrDefault(opts.OutValueNames, 2, "Mapped value 2")
	output3Name := utils.OptGetOrDefault(opts.OutValueNames, 3, "Mapped value 3")
	output4Name := utils.OptGetOrDefault(opts.OutValueNames, 4, "Mapped value 4")
	opMetrics := p.Metrics.AddOperation(core.OPERATION_KIND_MAP_VALUE, operationName)
	opMetrics.AddInputValue(input0.Metrics)
	opMetrics.AddInputValue(input1.Metrics)
	opMetrics.AddInputValue(input2.Metrics)
	opMetrics.AddInputValue(input3.Metrics)
	opMetrics.AddInputValue(input4.Metrics)
	output0 := pipeline.NewValue[TOut0](p, output0Name)
	output1 := pipeline.NewValue[TOut1](p, output1Name)
	output2 := pipeline.NewValue[TOut2](p, output2Name)
	output3 := pipeline.NewValue[TOut3](p, output3Name)
	output4 := pipeline.NewValue[TOut4](p, output4Name)
	opMetrics.AddOutputValue(output0.Metrics)
	opMetrics.AddOutputValue(output1.Metrics)
	opMetrics.AddOutputValue(output2.Metrics)
	opMetrics.AddOutputValue(output3.Metrics)
	opMetrics.AddOutputValue(output4.Metrics)
	opMetrics.Parallelism = 1
	opContexts := p.BuildOperatorContexts(opMetrics, nil, []*metrics.ValueMetrics{output0.Metrics, output1.Metrics, output2.Metrics, output3.Metrics, output4.Metrics}, 1, opts.UserOperatorContext.GetOr(nil))
	p.GoroutineDispatcher.StartTrackedGoroutine(func() {
		inputValue0 := input0.Wait()
		inputValue1 := input1.Wait()
		inputValue2 := input2.Wait()
		inputValue3 := input3.Wait()
		inputValue4 := input4.Wait()
		opMetrics.SetPhase(core.PHASE_RUNNING)
		outputValue0, outputValue1, outputValue2, outputValue3, outputValue4 := mapper(opContexts[0], inputValue0, inputValue1, inputValue2, inputValue3, inputValue4)
		output0.Resolve(outputValue0)
		output1.Resolve(outputValue1)
		output2.Resolve(outputValue2)
		output3.Resolve(outputValue3)
		output4.Resolve(outputValue4)
		opMetrics.SetPhase(core.PHASE_COMPLETED)
		opMetrics.Parallelism = 0
	})
	return output0, output1, output2, output3, output4
}
