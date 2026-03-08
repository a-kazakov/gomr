# fileio

Generic file I/O library with pluggable backends and compression. The root package depends only on stdlib; heavy dependencies (AWS SDK, zstd) are opt-in via sub-package imports.

## Install

```
go get github.com/a-kazakov/gomr/extensions/fileio
```

## Usage

### Basic read/write

```go
import "github.com/a-kazakov/gomr/extensions/fileio"

reader, err := fileio.Open("/data/input.txt")
defer reader.Close()

writer, err := fileio.Create("/data/output.txt", fileio.WithBufferSize(1<<20))
defer writer.Close()
```

### With compression

```go
import (
    "github.com/a-kazakov/gomr/extensions/fileio"
    "github.com/a-kazakov/gomr/extensions/fileio/zstd"
)

reader, err := fileio.Open("data.zst", fileio.WithDecompressor(zstd.NewDecompressor()))
writer, err := fileio.Create("out.zst", fileio.WithCompressor(zstd.NewCompressor()))
```

### With S3

```go
import (
    "github.com/a-kazakov/gomr/extensions/fileio"
    "github.com/a-kazakov/gomr/extensions/fileio/s3backend"
)

// Pass an existing *s3.Client
reader, err := fileio.Open("s3://bucket/key.txt", s3backend.WithS3Client(client))

// Or use as a Backend directly
s3 := s3backend.New(client)
reader, err := fileio.Open("s3://bucket/key.txt", fileio.WithBackend(s3))

// nil client = auto-create from AWS environment
reader, err := fileio.Open("s3://bucket/key.txt", s3backend.WithS3Client(nil))
```

### Line reading

```go
reader, _ := fileio.Open("data.txt")
defer reader.Close()

for line := range fileio.ReadLines(reader, 1<<20, func(b []byte) string {
    return string(b)
}) {
    process(line)
}
```

The converter controls memory strategy — copy bytes, use `unsafe.String` for zero-copy, parse into structs, etc.

### Glob

```go
files, err := fileio.Glob("/data/*.txt")
files, err := fileio.Glob("s3://bucket/prefix/*.csv", s3backend.WithS3Client(client))
```

### Custom backend

Implement the `Backend` interface for GCS, Azure, or any other storage:

```go
type Backend interface {
    Open(path string) (io.ReadCloser, error)
    Create(path string) (io.WriteCloser, error)
    Glob(pattern string) ([]string, error)
}

reader, err := fileio.Open("gs://bucket/file.txt", fileio.WithBackend(&myGCSBackend{}))
```

### Custom compression

Implement `Compressor` and/or `Decompressor`:

```go
type Compressor interface {
    WrapWriter(w io.WriteCloser) io.WriteCloser
}

type Decompressor interface {
    WrapReader(r io.ReadCloser) io.ReadCloser
}
```

## gomr pipeline integration

### ReadFiles — read and parse files into a pipeline

```go
import (
    "github.com/a-kazakov/gomr/extensions/fileio"
    "github.com/a-kazakov/gomr/extensions/fileio/zstd"
    "github.com/a-kazakov/gomr/extensions/fileio/s3backend"
)

records := fileio.ReadFiles(p, "s3://bucket/data/*.csv.zst", parseCSV,
    fileio.WithOperationName("Read input"),
    fileio.WithParallelism(8),
    fileio.WithBatchSize(1000),
    fileio.WithDecompressor(zstd.NewDecompressor()),
    s3backend.WithS3Client(client),
)
```

### ListFiles — list matching files for custom per-file processing

```go
files := fileio.ListFiles(p, "/data/*.bin",
    fileio.WithOperationName("Find input files"),
)
// then use gomr.Map(files, ...) with custom per-file logic
```

### WriteFiles — group and write pipeline values to files

```go
import (
    "github.com/a-kazakov/gomr/extensions/fileio"
    "github.com/a-kazakov/gomr/extensions/fileio/tfrecord"
)

outputFiles := fileio.WriteFiles(collection, mySerializer, "/output",
    fileio.WithCustomWriteCloser(func(w io.Writer) io.WriteCloser {
        return fileio.NewFakeWriteCloser(tfrecord.NewWriter(w))
    }),
    fileio.WithOperationName("Write output"),
    fileio.WithNumShards(32),
    fileio.WithGatherParallelism(4),
)
```

## Packages

| Package | Dependencies | Description |
|---------|-------------|-------------|
| `fileio` | stdlib + gomr | Core interfaces, local disk backend, options, line reader, pipeline operations (ReadFiles, ListFiles, WriteFiles) |
| `fileio/gzip` | stdlib | Gzip compressor/decompressor |
| `fileio/zstd` | klauspost/compress | Zstd compressor/decompressor |
| `fileio/s3backend` | AWS SDK v2 | S3 backend implementation |
| `fileio/tfrecord` | stdlib | TFRecord format writer |
