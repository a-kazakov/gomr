package s3backend

import (
	"github.com/a-kazakov/gomr/extensions/fileio"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type s3ClientOption struct {
	backend fileio.Backend
}

// WithS3Client returns an option that registers the S3 backend for s3:// paths.
// Implements fileio.ReadOption, fileio.WriteOption, fileio.GlobOption,
// fileio.ReadFilesOption, fileio.WriteFilesOption.
func WithS3Client(client *s3.Client) s3ClientOption {
	return s3ClientOption{backend: New(client)}
}

func (o s3ClientOption) register(b fileio.Backend) *fileio.BackendRouter {
	r := fileio.AsBackendRouter(b)
	r.Register("s3", o.backend)
	return r
}

func (o s3ClientOption) ApplyReadConfig(c *fileio.ReadConfig)   { c.Backend = o.register(c.Backend) }
func (o s3ClientOption) ApplyWriteConfig(c *fileio.WriteConfig) { c.Backend = o.register(c.Backend) }
func (o s3ClientOption) ApplyGlobConfig(c *fileio.GlobConfig)   { c.Backend = o.register(c.Backend) }
func (o s3ClientOption) ApplyReadFilesConfig(c *fileio.ReadFilesConfig) {
	c.Backend = o.register(c.Backend)
}
func (o s3ClientOption) ApplyWriteFilesConfig(c *fileio.WriteFilesConfig) {
	c.Backend = o.register(c.Backend)
}
