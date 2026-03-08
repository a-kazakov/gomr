package s3backend

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/a-kazakov/gomr/extensions/fileio"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/oklog/ulid/v2"
)

var (
	defaultClient     *s3.Client
	defaultClientOnce sync.Once
)

// DefaultClient returns a lazily-initialized default S3 client created from
// the AWS environment configuration.
func DefaultClient() *s3.Client {
	defaultClientOnce.Do(func() {
		cfg, err := config.LoadDefaultConfig(context.Background())
		if err != nil {
			panic(fmt.Errorf("failed to load AWS config: %w", err))
		}
		defaultClient = s3.NewFromConfig(cfg)
	})
	return defaultClient
}

// ParsePath splits "s3://bucket/key/path" into (bucket, key).
func ParsePath(s3Path string) (bucket, key string) {
	trimmed := strings.TrimPrefix(s3Path, "s3://")
	idx := strings.IndexByte(trimmed, '/')
	if idx < 0 {
		return trimmed, ""
	}
	return trimmed[:idx], trimmed[idx+1:]
}

// New creates an S3 backend using the provided client.
// If client is nil, a default client is lazily created from AWS env config.
func New(client *s3.Client) fileio.Backend {
	return &s3Backend{client: client}
}

type s3Backend struct {
	client *s3.Client
}

func (b *s3Backend) ensureClient() *s3.Client {
	if b.client != nil {
		return b.client
	}
	return DefaultClient()
}

func (b *s3Backend) Open(filePath string) (io.ReadCloser, error) {
	client := b.ensureClient()
	bucket, key := ParsePath(filePath)
	output, err := client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get s3://%s/%s: %w", bucket, key, err)
	}
	return output.Body, nil
}

func (b *s3Backend) Create(filePath string) (io.WriteCloser, error) {
	client := b.ensureClient()
	bucket, key := ParsePath(filePath)
	return newS3Writer(bucket, key, client)
}

func (b *s3Backend) Glob(pattern string) ([]string, error) {
	client := b.ensureClient()
	bucket, keyPattern := ParsePath(pattern)
	prefix := extractGlobPrefix(keyPattern)

	var results []string
	var continuationToken *string
	for {
		output, err := client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to list s3://%s/%s: %w", bucket, prefix, err)
		}
		for _, obj := range output.Contents {
			key := aws.ToString(obj.Key)
			matched, err := filepath.Match(keyPattern, key)
			if err != nil {
				return nil, fmt.Errorf("failed to match pattern %q against key %q: %w", keyPattern, key, err)
			}
			if matched {
				results = append(results, fmt.Sprintf("s3://%s/%s", bucket, key))
			}
		}
		if !aws.ToBool(output.IsTruncated) {
			break
		}
		continuationToken = output.NextContinuationToken
	}
	return results, nil
}

func extractGlobPrefix(pattern string) string {
	firstMeta := len(pattern)
	for i, ch := range pattern {
		if ch == '*' || ch == '?' || ch == '[' {
			firstMeta = i
			break
		}
	}
	lastSlash := strings.LastIndex(pattern[:firstMeta], "/")
	if lastSlash < 0 {
		return ""
	}
	return pattern[:lastSlash+1]
}

type s3Writer struct {
	file   *os.File
	bucket string
	key    string
	client *s3.Client
}

func newS3Writer(bucket, key string, client *s3.Client) (*s3Writer, error) {
	tempDir := os.TempDir()
	tempFilePath := path.Join(tempDir, fmt.Sprintf("tmp-%s.bin", ulid.Make().String()))
	file, err := os.OpenFile(tempFilePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file for S3 upload: %w", err)
	}
	return &s3Writer{
		file:   file,
		bucket: bucket,
		key:    key,
		client: client,
	}, nil
}

func (w *s3Writer) Write(p []byte) (int, error) {
	return w.file.Write(p)
}

func (w *s3Writer) Close() error {
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek temp file: %w", err)
	}
	_, err := w.client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(w.bucket),
		Key:    aws.String(w.key),
		Body:   w.file,
	})
	w.file.Close()
	os.Remove(w.file.Name())
	if err != nil {
		return fmt.Errorf("failed to upload to s3://%s/%s: %w", w.bucket, w.key, err)
	}
	return nil
}
