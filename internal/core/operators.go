package core

// Code generation directives for map and shuffle operator interface variants.
// Run "go generate ./..." from the gomr/ directory after modifying codegen templates.

//go:generate go run ../../internal/operators/codegen/map/map.go iface
//go:generate go run ../../internal/operators/codegen/shuffle/shuffle.go iface
