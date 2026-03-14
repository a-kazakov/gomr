module github.com/a-kazakov/gomr/test

go 1.26

require (
	github.com/a-kazakov/gomr v0.0.0-20260309050634-39726027a8a7
	github.com/a-kazakov/gomr/extensions/fileio v0.0.0-20260309050634-39726027a8a7
	github.com/a-kazakov/gomr/extensions/fileio/s3backend v0.0.0-20260309050634-39726027a8a7
	github.com/aws/aws-sdk-go-v2 v1.41.1
	github.com/aws/aws-sdk-go-v2/credentials v1.19.7
	github.com/aws/aws-sdk-go-v2/service/s3 v1.96.0
	github.com/johannesboyne/gofakes3 v0.0.0-20260208201424-4c385a1f6a73
)

require (
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.4 // indirect
	github.com/aws/aws-sdk-go-v2/config v1.32.7 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.17 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.17 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.17 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.4 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.4.17 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.9.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.17 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.19.17 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.0.5 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.30.9 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.35.13 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.41.6 // indirect
	github.com/aws/smithy-go v1.24.0 // indirect
	github.com/klauspost/compress v1.18.4 // indirect
	github.com/klauspost/cpuid/v2 v2.2.10 // indirect
	github.com/oklog/ulid/v2 v2.1.1 // indirect
	github.com/pierrec/lz4/v4 v4.1.25 // indirect
	github.com/ryszard/goskiplist v0.0.0-20150312221310-2dfbae5fcf46 // indirect
	github.com/zeebo/xxh3 v1.1.0 // indirect
	go.shabbyrobe.org/gocovmerge v0.0.0-20230507111327-fa4f82cfbf4d // indirect
	golang.org/x/sys v0.40.0 // indirect
	golang.org/x/tools v0.8.0 // indirect
)

replace github.com/a-kazakov/gomr => ..

replace github.com/a-kazakov/gomr/extensions/fileio => ../extensions/fileio

replace github.com/a-kazakov/gomr/extensions/fileio/s3backend => ../extensions/fileio/s3backend
