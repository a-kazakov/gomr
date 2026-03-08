package s3backend

import "testing"

func TestParsePath_BucketAndKey(t *testing.T) {
	bucket, key := ParsePath("s3://my-bucket/path/to/file.txt")
	if bucket != "my-bucket" {
		t.Fatalf("bucket = %q, want %q", bucket, "my-bucket")
	}
	if key != "path/to/file.txt" {
		t.Fatalf("key = %q, want %q", key, "path/to/file.txt")
	}
}

func TestParsePath_BucketOnly(t *testing.T) {
	bucket, key := ParsePath("s3://my-bucket")
	if bucket != "my-bucket" {
		t.Fatalf("bucket = %q, want %q", bucket, "my-bucket")
	}
	if key != "" {
		t.Fatalf("key = %q, want empty", key)
	}
}

func TestParsePath_TrailingSlash(t *testing.T) {
	bucket, key := ParsePath("s3://my-bucket/")
	if bucket != "my-bucket" {
		t.Fatalf("bucket = %q, want %q", bucket, "my-bucket")
	}
	if key != "" {
		t.Fatalf("key = %q, want empty", key)
	}
}

func TestParsePath_NestedKey(t *testing.T) {
	bucket, key := ParsePath("s3://data/a/b/c/d.parquet")
	if bucket != "data" {
		t.Fatalf("bucket = %q, want %q", bucket, "data")
	}
	if key != "a/b/c/d.parquet" {
		t.Fatalf("key = %q, want %q", key, "a/b/c/d.parquet")
	}
}

func TestParsePath_NoPrefix(t *testing.T) {
	// ParsePath trims "s3://"; without it the whole string is treated as bucket.
	bucket, key := ParsePath("just-a-bucket")
	if bucket != "just-a-bucket" {
		t.Fatalf("bucket = %q, want %q", bucket, "just-a-bucket")
	}
	if key != "" {
		t.Fatalf("key = %q, want empty", key)
	}
}
