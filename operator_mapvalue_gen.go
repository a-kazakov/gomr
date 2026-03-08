package gomr

import (
	"github.com/a-kazakov/gomr/internal/core"
	"github.com/a-kazakov/gomr/internal/operators"
	"github.com/a-kazakov/gomr/internal/operators/options"
	"github.com/a-kazakov/gomr/internal/pipeline"
)

// ========== MapValue{N}To{M} - full aliases ==========

func MapValue1To1[TIn0 any, TOut0 any](
	input0 *pipeline.Value[TIn0],
	mapper func(context *core.OperatorContext, input0 TIn0) (output0 TOut0),
	opts ...options.MapValueOption,
) *pipeline.Value[TOut0] {
	return operators.MapValue1To1(input0, mapper, options.ApplyMapValueOptions(opts...))
}

func MapValue1To2[TIn0 any, TOut0 any, TOut1 any](
	input0 *pipeline.Value[TIn0],
	mapper func(context *core.OperatorContext, input0 TIn0) (output0 TOut0, output1 TOut1),
	opts ...options.MapValueOption,
) (*pipeline.Value[TOut0], *pipeline.Value[TOut1]) {
	return operators.MapValue1To2(input0, mapper, options.ApplyMapValueOptions(opts...))
}

func MapValue1To3[TIn0 any, TOut0 any, TOut1 any, TOut2 any](
	input0 *pipeline.Value[TIn0],
	mapper func(context *core.OperatorContext, input0 TIn0) (output0 TOut0, output1 TOut1, output2 TOut2),
	opts ...options.MapValueOption,
) (*pipeline.Value[TOut0], *pipeline.Value[TOut1], *pipeline.Value[TOut2]) {
	return operators.MapValue1To3(input0, mapper, options.ApplyMapValueOptions(opts...))
}

func MapValue1To4[TIn0 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any](
	input0 *pipeline.Value[TIn0],
	mapper func(context *core.OperatorContext, input0 TIn0) (output0 TOut0, output1 TOut1, output2 TOut2, output3 TOut3),
	opts ...options.MapValueOption,
) (*pipeline.Value[TOut0], *pipeline.Value[TOut1], *pipeline.Value[TOut2], *pipeline.Value[TOut3]) {
	return operators.MapValue1To4(input0, mapper, options.ApplyMapValueOptions(opts...))
}

func MapValue1To5[TIn0 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any](
	input0 *pipeline.Value[TIn0],
	mapper func(context *core.OperatorContext, input0 TIn0) (output0 TOut0, output1 TOut1, output2 TOut2, output3 TOut3, output4 TOut4),
	opts ...options.MapValueOption,
) (*pipeline.Value[TOut0], *pipeline.Value[TOut1], *pipeline.Value[TOut2], *pipeline.Value[TOut3], *pipeline.Value[TOut4]) {
	return operators.MapValue1To5(input0, mapper, options.ApplyMapValueOptions(opts...))
}

func MapValue2To1[TIn0 any, TIn1 any, TOut0 any](
	input0 *pipeline.Value[TIn0], input1 *pipeline.Value[TIn1],
	mapper func(context *core.OperatorContext, input0 TIn0, input1 TIn1) (output0 TOut0),
	opts ...options.MapValueOption,
) *pipeline.Value[TOut0] {
	return operators.MapValue2To1(input0, input1, mapper, options.ApplyMapValueOptions(opts...))
}

func MapValue2To2[TIn0 any, TIn1 any, TOut0 any, TOut1 any](
	input0 *pipeline.Value[TIn0], input1 *pipeline.Value[TIn1],
	mapper func(context *core.OperatorContext, input0 TIn0, input1 TIn1) (output0 TOut0, output1 TOut1),
	opts ...options.MapValueOption,
) (*pipeline.Value[TOut0], *pipeline.Value[TOut1]) {
	return operators.MapValue2To2(input0, input1, mapper, options.ApplyMapValueOptions(opts...))
}

func MapValue2To3[TIn0 any, TIn1 any, TOut0 any, TOut1 any, TOut2 any](
	input0 *pipeline.Value[TIn0], input1 *pipeline.Value[TIn1],
	mapper func(context *core.OperatorContext, input0 TIn0, input1 TIn1) (output0 TOut0, output1 TOut1, output2 TOut2),
	opts ...options.MapValueOption,
) (*pipeline.Value[TOut0], *pipeline.Value[TOut1], *pipeline.Value[TOut2]) {
	return operators.MapValue2To3(input0, input1, mapper, options.ApplyMapValueOptions(opts...))
}

func MapValue2To4[TIn0 any, TIn1 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any](
	input0 *pipeline.Value[TIn0], input1 *pipeline.Value[TIn1],
	mapper func(context *core.OperatorContext, input0 TIn0, input1 TIn1) (output0 TOut0, output1 TOut1, output2 TOut2, output3 TOut3),
	opts ...options.MapValueOption,
) (*pipeline.Value[TOut0], *pipeline.Value[TOut1], *pipeline.Value[TOut2], *pipeline.Value[TOut3]) {
	return operators.MapValue2To4(input0, input1, mapper, options.ApplyMapValueOptions(opts...))
}

func MapValue2To5[TIn0 any, TIn1 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any](
	input0 *pipeline.Value[TIn0], input1 *pipeline.Value[TIn1],
	mapper func(context *core.OperatorContext, input0 TIn0, input1 TIn1) (output0 TOut0, output1 TOut1, output2 TOut2, output3 TOut3, output4 TOut4),
	opts ...options.MapValueOption,
) (*pipeline.Value[TOut0], *pipeline.Value[TOut1], *pipeline.Value[TOut2], *pipeline.Value[TOut3], *pipeline.Value[TOut4]) {
	return operators.MapValue2To5(input0, input1, mapper, options.ApplyMapValueOptions(opts...))
}

func MapValue3To1[TIn0 any, TIn1 any, TIn2 any, TOut0 any](
	input0 *pipeline.Value[TIn0], input1 *pipeline.Value[TIn1], input2 *pipeline.Value[TIn2],
	mapper func(context *core.OperatorContext, input0 TIn0, input1 TIn1, input2 TIn2) (output0 TOut0),
	opts ...options.MapValueOption,
) *pipeline.Value[TOut0] {
	return operators.MapValue3To1(input0, input1, input2, mapper, options.ApplyMapValueOptions(opts...))
}

func MapValue3To2[TIn0 any, TIn1 any, TIn2 any, TOut0 any, TOut1 any](
	input0 *pipeline.Value[TIn0], input1 *pipeline.Value[TIn1], input2 *pipeline.Value[TIn2],
	mapper func(context *core.OperatorContext, input0 TIn0, input1 TIn1, input2 TIn2) (output0 TOut0, output1 TOut1),
	opts ...options.MapValueOption,
) (*pipeline.Value[TOut0], *pipeline.Value[TOut1]) {
	return operators.MapValue3To2(input0, input1, input2, mapper, options.ApplyMapValueOptions(opts...))
}

func MapValue3To3[TIn0 any, TIn1 any, TIn2 any, TOut0 any, TOut1 any, TOut2 any](
	input0 *pipeline.Value[TIn0], input1 *pipeline.Value[TIn1], input2 *pipeline.Value[TIn2],
	mapper func(context *core.OperatorContext, input0 TIn0, input1 TIn1, input2 TIn2) (output0 TOut0, output1 TOut1, output2 TOut2),
	opts ...options.MapValueOption,
) (*pipeline.Value[TOut0], *pipeline.Value[TOut1], *pipeline.Value[TOut2]) {
	return operators.MapValue3To3(input0, input1, input2, mapper, options.ApplyMapValueOptions(opts...))
}

func MapValue3To4[TIn0 any, TIn1 any, TIn2 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any](
	input0 *pipeline.Value[TIn0], input1 *pipeline.Value[TIn1], input2 *pipeline.Value[TIn2],
	mapper func(context *core.OperatorContext, input0 TIn0, input1 TIn1, input2 TIn2) (output0 TOut0, output1 TOut1, output2 TOut2, output3 TOut3),
	opts ...options.MapValueOption,
) (*pipeline.Value[TOut0], *pipeline.Value[TOut1], *pipeline.Value[TOut2], *pipeline.Value[TOut3]) {
	return operators.MapValue3To4(input0, input1, input2, mapper, options.ApplyMapValueOptions(opts...))
}

func MapValue3To5[TIn0 any, TIn1 any, TIn2 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any](
	input0 *pipeline.Value[TIn0], input1 *pipeline.Value[TIn1], input2 *pipeline.Value[TIn2],
	mapper func(context *core.OperatorContext, input0 TIn0, input1 TIn1, input2 TIn2) (output0 TOut0, output1 TOut1, output2 TOut2, output3 TOut3, output4 TOut4),
	opts ...options.MapValueOption,
) (*pipeline.Value[TOut0], *pipeline.Value[TOut1], *pipeline.Value[TOut2], *pipeline.Value[TOut3], *pipeline.Value[TOut4]) {
	return operators.MapValue3To5(input0, input1, input2, mapper, options.ApplyMapValueOptions(opts...))
}

func MapValue4To1[TIn0 any, TIn1 any, TIn2 any, TIn3 any, TOut0 any](
	input0 *pipeline.Value[TIn0], input1 *pipeline.Value[TIn1], input2 *pipeline.Value[TIn2], input3 *pipeline.Value[TIn3],
	mapper func(context *core.OperatorContext, input0 TIn0, input1 TIn1, input2 TIn2, input3 TIn3) (output0 TOut0),
	opts ...options.MapValueOption,
) *pipeline.Value[TOut0] {
	return operators.MapValue4To1(input0, input1, input2, input3, mapper, options.ApplyMapValueOptions(opts...))
}

func MapValue4To2[TIn0 any, TIn1 any, TIn2 any, TIn3 any, TOut0 any, TOut1 any](
	input0 *pipeline.Value[TIn0], input1 *pipeline.Value[TIn1], input2 *pipeline.Value[TIn2], input3 *pipeline.Value[TIn3],
	mapper func(context *core.OperatorContext, input0 TIn0, input1 TIn1, input2 TIn2, input3 TIn3) (output0 TOut0, output1 TOut1),
	opts ...options.MapValueOption,
) (*pipeline.Value[TOut0], *pipeline.Value[TOut1]) {
	return operators.MapValue4To2(input0, input1, input2, input3, mapper, options.ApplyMapValueOptions(opts...))
}

func MapValue4To3[TIn0 any, TIn1 any, TIn2 any, TIn3 any, TOut0 any, TOut1 any, TOut2 any](
	input0 *pipeline.Value[TIn0], input1 *pipeline.Value[TIn1], input2 *pipeline.Value[TIn2], input3 *pipeline.Value[TIn3],
	mapper func(context *core.OperatorContext, input0 TIn0, input1 TIn1, input2 TIn2, input3 TIn3) (output0 TOut0, output1 TOut1, output2 TOut2),
	opts ...options.MapValueOption,
) (*pipeline.Value[TOut0], *pipeline.Value[TOut1], *pipeline.Value[TOut2]) {
	return operators.MapValue4To3(input0, input1, input2, input3, mapper, options.ApplyMapValueOptions(opts...))
}

func MapValue4To4[TIn0 any, TIn1 any, TIn2 any, TIn3 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any](
	input0 *pipeline.Value[TIn0], input1 *pipeline.Value[TIn1], input2 *pipeline.Value[TIn2], input3 *pipeline.Value[TIn3],
	mapper func(context *core.OperatorContext, input0 TIn0, input1 TIn1, input2 TIn2, input3 TIn3) (output0 TOut0, output1 TOut1, output2 TOut2, output3 TOut3),
	opts ...options.MapValueOption,
) (*pipeline.Value[TOut0], *pipeline.Value[TOut1], *pipeline.Value[TOut2], *pipeline.Value[TOut3]) {
	return operators.MapValue4To4(input0, input1, input2, input3, mapper, options.ApplyMapValueOptions(opts...))
}

func MapValue4To5[TIn0 any, TIn1 any, TIn2 any, TIn3 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any](
	input0 *pipeline.Value[TIn0], input1 *pipeline.Value[TIn1], input2 *pipeline.Value[TIn2], input3 *pipeline.Value[TIn3],
	mapper func(context *core.OperatorContext, input0 TIn0, input1 TIn1, input2 TIn2, input3 TIn3) (output0 TOut0, output1 TOut1, output2 TOut2, output3 TOut3, output4 TOut4),
	opts ...options.MapValueOption,
) (*pipeline.Value[TOut0], *pipeline.Value[TOut1], *pipeline.Value[TOut2], *pipeline.Value[TOut3], *pipeline.Value[TOut4]) {
	return operators.MapValue4To5(input0, input1, input2, input3, mapper, options.ApplyMapValueOptions(opts...))
}

func MapValue5To1[TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIn4 any, TOut0 any](
	input0 *pipeline.Value[TIn0], input1 *pipeline.Value[TIn1], input2 *pipeline.Value[TIn2], input3 *pipeline.Value[TIn3], input4 *pipeline.Value[TIn4],
	mapper func(context *core.OperatorContext, input0 TIn0, input1 TIn1, input2 TIn2, input3 TIn3, input4 TIn4) (output0 TOut0),
	opts ...options.MapValueOption,
) *pipeline.Value[TOut0] {
	return operators.MapValue5To1(input0, input1, input2, input3, input4, mapper, options.ApplyMapValueOptions(opts...))
}

func MapValue5To2[TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIn4 any, TOut0 any, TOut1 any](
	input0 *pipeline.Value[TIn0], input1 *pipeline.Value[TIn1], input2 *pipeline.Value[TIn2], input3 *pipeline.Value[TIn3], input4 *pipeline.Value[TIn4],
	mapper func(context *core.OperatorContext, input0 TIn0, input1 TIn1, input2 TIn2, input3 TIn3, input4 TIn4) (output0 TOut0, output1 TOut1),
	opts ...options.MapValueOption,
) (*pipeline.Value[TOut0], *pipeline.Value[TOut1]) {
	return operators.MapValue5To2(input0, input1, input2, input3, input4, mapper, options.ApplyMapValueOptions(opts...))
}

func MapValue5To3[TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIn4 any, TOut0 any, TOut1 any, TOut2 any](
	input0 *pipeline.Value[TIn0], input1 *pipeline.Value[TIn1], input2 *pipeline.Value[TIn2], input3 *pipeline.Value[TIn3], input4 *pipeline.Value[TIn4],
	mapper func(context *core.OperatorContext, input0 TIn0, input1 TIn1, input2 TIn2, input3 TIn3, input4 TIn4) (output0 TOut0, output1 TOut1, output2 TOut2),
	opts ...options.MapValueOption,
) (*pipeline.Value[TOut0], *pipeline.Value[TOut1], *pipeline.Value[TOut2]) {
	return operators.MapValue5To3(input0, input1, input2, input3, input4, mapper, options.ApplyMapValueOptions(opts...))
}

func MapValue5To4[TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIn4 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any](
	input0 *pipeline.Value[TIn0], input1 *pipeline.Value[TIn1], input2 *pipeline.Value[TIn2], input3 *pipeline.Value[TIn3], input4 *pipeline.Value[TIn4],
	mapper func(context *core.OperatorContext, input0 TIn0, input1 TIn1, input2 TIn2, input3 TIn3, input4 TIn4) (output0 TOut0, output1 TOut1, output2 TOut2, output3 TOut3),
	opts ...options.MapValueOption,
) (*pipeline.Value[TOut0], *pipeline.Value[TOut1], *pipeline.Value[TOut2], *pipeline.Value[TOut3]) {
	return operators.MapValue5To4(input0, input1, input2, input3, input4, mapper, options.ApplyMapValueOptions(opts...))
}

func MapValue5To5[TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIn4 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any](
	input0 *pipeline.Value[TIn0], input1 *pipeline.Value[TIn1], input2 *pipeline.Value[TIn2], input3 *pipeline.Value[TIn3], input4 *pipeline.Value[TIn4],
	mapper func(context *core.OperatorContext, input0 TIn0, input1 TIn1, input2 TIn2, input3 TIn3, input4 TIn4) (output0 TOut0, output1 TOut1, output2 TOut2, output3 TOut3, output4 TOut4),
	opts ...options.MapValueOption,
) (*pipeline.Value[TOut0], *pipeline.Value[TOut1], *pipeline.Value[TOut2], *pipeline.Value[TOut3], *pipeline.Value[TOut4]) {
	return operators.MapValue5To5(input0, input1, input2, input3, input4, mapper, options.ApplyMapValueOptions(opts...))
}

// ========== MapValueTo{M} -> MapValue1To{M} (single input, M outputs) ==========

func MapValueTo1[TIn0 any, TOut0 any](
	input0 *pipeline.Value[TIn0],
	mapper func(context *core.OperatorContext, input0 TIn0) TOut0,
	opts ...options.MapValueOption,
) *pipeline.Value[TOut0] {
	return operators.MapValue1To1(input0, mapper, options.ApplyMapValueOptions(opts...))
}

func MapValueTo2[TIn0 any, TOut0 any, TOut1 any](
	input0 *pipeline.Value[TIn0],
	mapper func(context *core.OperatorContext, input0 TIn0) (output0 TOut0, output1 TOut1),
	opts ...options.MapValueOption,
) (*pipeline.Value[TOut0], *pipeline.Value[TOut1]) {
	return operators.MapValue1To2(input0, mapper, options.ApplyMapValueOptions(opts...))
}

func MapValueTo3[TIn0 any, TOut0 any, TOut1 any, TOut2 any](
	input0 *pipeline.Value[TIn0],
	mapper func(context *core.OperatorContext, input0 TIn0) (output0 TOut0, output1 TOut1, output2 TOut2),
	opts ...options.MapValueOption,
) (*pipeline.Value[TOut0], *pipeline.Value[TOut1], *pipeline.Value[TOut2]) {
	return operators.MapValue1To3(input0, mapper, options.ApplyMapValueOptions(opts...))
}

func MapValueTo4[TIn0 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any](
	input0 *pipeline.Value[TIn0],
	mapper func(context *core.OperatorContext, input0 TIn0) (output0 TOut0, output1 TOut1, output2 TOut2, output3 TOut3),
	opts ...options.MapValueOption,
) (*pipeline.Value[TOut0], *pipeline.Value[TOut1], *pipeline.Value[TOut2], *pipeline.Value[TOut3]) {
	return operators.MapValue1To4(input0, mapper, options.ApplyMapValueOptions(opts...))
}

func MapValueTo5[TIn0 any, TOut0 any, TOut1 any, TOut2 any, TOut3 any, TOut4 any](
	input0 *pipeline.Value[TIn0],
	mapper func(context *core.OperatorContext, input0 TIn0) (output0 TOut0, output1 TOut1, output2 TOut2, output3 TOut3, output4 TOut4),
	opts ...options.MapValueOption,
) (*pipeline.Value[TOut0], *pipeline.Value[TOut1], *pipeline.Value[TOut2], *pipeline.Value[TOut3], *pipeline.Value[TOut4]) {
	return operators.MapValue1To5(input0, mapper, options.ApplyMapValueOptions(opts...))
}

// ========== MapValue{N} -> MapValue{N}To1 (N inputs, single output) ==========

func MapValue1[TIn0 any, TOut0 any](
	input0 *pipeline.Value[TIn0],
	mapper func(context *core.OperatorContext, input0 TIn0) TOut0,
	opts ...options.MapValueOption,
) *pipeline.Value[TOut0] {
	return operators.MapValue1To1(input0, mapper, options.ApplyMapValueOptions(opts...))
}

func MapValue2[TIn0 any, TIn1 any, TOut0 any](
	input0 *pipeline.Value[TIn0], input1 *pipeline.Value[TIn1],
	mapper func(context *core.OperatorContext, input0 TIn0, input1 TIn1) TOut0,
	opts ...options.MapValueOption,
) *pipeline.Value[TOut0] {
	return operators.MapValue2To1(input0, input1, mapper, options.ApplyMapValueOptions(opts...))
}

func MapValue3[TIn0 any, TIn1 any, TIn2 any, TOut0 any](
	input0 *pipeline.Value[TIn0], input1 *pipeline.Value[TIn1], input2 *pipeline.Value[TIn2],
	mapper func(context *core.OperatorContext, input0 TIn0, input1 TIn1, input2 TIn2) TOut0,
	opts ...options.MapValueOption,
) *pipeline.Value[TOut0] {
	return operators.MapValue3To1(input0, input1, input2, mapper, options.ApplyMapValueOptions(opts...))
}

func MapValue4[TIn0 any, TIn1 any, TIn2 any, TIn3 any, TOut0 any](
	input0 *pipeline.Value[TIn0], input1 *pipeline.Value[TIn1], input2 *pipeline.Value[TIn2], input3 *pipeline.Value[TIn3],
	mapper func(context *core.OperatorContext, input0 TIn0, input1 TIn1, input2 TIn2, input3 TIn3) TOut0,
	opts ...options.MapValueOption,
) *pipeline.Value[TOut0] {
	return operators.MapValue4To1(input0, input1, input2, input3, mapper, options.ApplyMapValueOptions(opts...))
}

func MapValue5[TIn0 any, TIn1 any, TIn2 any, TIn3 any, TIn4 any, TOut0 any](
	input0 *pipeline.Value[TIn0], input1 *pipeline.Value[TIn1], input2 *pipeline.Value[TIn2], input3 *pipeline.Value[TIn3], input4 *pipeline.Value[TIn4],
	mapper func(context *core.OperatorContext, input0 TIn0, input1 TIn1, input2 TIn2, input3 TIn3, input4 TIn4) TOut0,
	opts ...options.MapValueOption,
) *pipeline.Value[TOut0] {
	return operators.MapValue5To1(input0, input1, input2, input3, input4, mapper, options.ApplyMapValueOptions(opts...))
}
