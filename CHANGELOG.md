# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- Split `extensions/fileio/s3backend` into its own Go module to avoid pulling AWS SDK dependencies for projects that only use local file I/O

## [0.1.0] - 2026-03-08

### Added
- Core MapReduce pipeline framework with Go generics
- Operators: Seed, Map, Fork, Merge, Shuffle, Collect, MapValue, SpillBuffer, ToCollection, Ignore
- Multi-arity operator variants (MapTo2-5, ForkTo2-5, ShuffleNxM, etc.) via code generation
- Disk-spilling shuffle with configurable memory/disk tradeoff
- Real-time metrics collection with HTTP push and React dashboard
- Configurable parameters with flag, environment, and programmatic sources
- Extension: fileio — file I/O with pluggable backends (local, S3), compression (gzip, zstd), TFRecord format
- Extension: marshal — zero-allocation binary serialization for primitive types
- Integration tests for Merge, MapValue, ToCollection, and multi-stage pipelines
- Higher-arity operator smoke tests
- Concurrent access tests for Value and Parameter
- Edge case tests for empty and single-element collections
- Temp file cleanup verification for SpillBuffer
- CI workflow with code coverage reporting
