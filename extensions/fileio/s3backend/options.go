package s3backend

import (
	"github.com/a-kazakov/gomr/extensions/fileio"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type s3ClientOption struct {
	backend fileio.Backend
}

// WithS3Client returns an option that sets the S3 backend.
// This is a shorthand for fileio.WithBackend(s3backend.New(client)).
// Implements fileio.ReadOption, fileio.WriteOption, fileio.GlobOption,
// fileio.ReadFilesOption, fileio.WriteFilesOption.
func WithS3Client(client *s3.Client) s3ClientOption {
	return s3ClientOption{backend: New(client)}
}

func (o s3ClientOption) ApplyReadConfig(c *fileio.ReadConfig)           { c.Backend = o.backend }
func (o s3ClientOption) ApplyWriteConfig(c *fileio.WriteConfig)         { c.Backend = o.backend }
func (o s3ClientOption) ApplyGlobConfig(c *fileio.GlobConfig)           { c.Backend = o.backend }
func (o s3ClientOption) ApplyReadFilesConfig(c *fileio.ReadFilesConfig) { c.Backend = o.backend }
func (o s3ClientOption) ApplyWriteFilesConfig(c *fileio.WriteFilesConfig) {
	c.Backend = o.backend
}
