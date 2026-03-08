package test

import (
	"context"
	"fmt"
	"io"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/a-kazakov/gomr"
	"github.com/a-kazakov/gomr/extensions/fileio"
	"github.com/a-kazakov/gomr/extensions/fileio/s3backend"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
)

// tempRecord holds a station temperature converted to degrees Celsius.
type tempRecord struct {
	StationID string
	Date      string
	Element   string
	Celsius   float64
}

// lineSerializer writes all records to a single output file as CSV lines.
type lineSerializer struct{}

func (s *lineSerializer) MarshalFileName(_ *tempRecord, dest []byte) int {
	return copy(dest, "temperatures.csv")
}

func (s *lineSerializer) MarshalRecord(v *tempRecord, dest []byte) int {
	line := fmt.Sprintf("%s,%s,%s,%.1f\n", v.StationID, v.Date, v.Element, v.Celsius)
	return copy(dest, line)
}

// TestS3Integration reads NOAA weather data from a public S3 bucket, maps each
// record (filtering to TMAX/TMIN only, converting tenths-of-degree to Celsius),
// writes the output to a local fake S3 via WriteFiles, and reads it back to
// verify the round-trip — all in a single pipeline.
func TestS3Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping S3 integration test in short mode")
	}

	// --- Fake S3 for writing ---
	faker := gofakes3.New(s3mem.New())
	ts := httptest.NewServer(faker.Server())
	defer ts.Close()

	fakeClient := s3.New(s3.Options{
		BaseEndpoint: aws.String(ts.URL),
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
		Region:       "us-east-1",
		UsePathStyle: true,
	})
	_, err := fakeClient.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String("test-output"),
	})
	if err != nil {
		t.Fatalf("failed to create fake bucket: %v", err)
	}

	// --- Anonymous client for the public NOAA bucket ---
	publicClient := s3.New(s3.Options{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("", "", ""),
	})
	_, err = publicClient.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String("noaa-ghcn-pds"),
		Key:    aws.String("csv/by_year/1788.csv"),
	})
	if err != nil {
		t.Skipf("cannot reach public S3 bucket (no network?): %v", err)
	}

	// --- Single pipeline: ReadFiles → Map → WriteFiles ---
	pipeline := gomr.NewPipeline()

	// Step 1: Read CSV lines from public S3.
	// CSV format: ID,DATE,ELEMENT,DATA_VALUE,M_FLAG,Q_FLAG,S_FLAG,OBS_TIME
	// DATA_VALUE is in tenths of a degree Celsius.
	type rawLine struct {
		fields []string
	}
	lines := fileio.ReadFiles(pipeline, "s3://noaa-ghcn-pds/csv/by_year/1788.csv",
		func(line []byte) rawLine {
			return rawLine{fields: strings.SplitN(string(line), ",", 5)}
		},
		s3backend.WithS3Client(publicClient),
	)

	// Step 2: Map — filter to TMAX/TMIN, convert tenths-of-degree to Celsius.
	temps := gomr.Map(lines, func(
		_ gomr.OperatorContext,
		recv gomr.CollectionReceiver[rawLine],
		emit gomr.Emitter[tempRecord],
	) {
		for r := range recv.IterValues() {
			if len(r.fields) < 4 {
				continue
			}
			elem := r.fields[2]
			if elem != "TMAX" && elem != "TMIN" {
				continue
			}
			tenths, err := strconv.Atoi(r.fields[3])
			if err != nil {
				continue // skip header line
			}
			out := emit.GetEmitPointer()
			out.StationID = r.fields[0]
			out.Date = r.fields[1]
			out.Element = elem
			out.Celsius = float64(tenths) / 10.0
		}
	})

	// Step 3: WriteFiles to the fake S3 bucket.
	gomr.Ignore(fileio.WriteFiles(
		temps,
		&lineSerializer{},
		"s3://test-output",
		s3backend.WithS3Client(fakeClient),
		fileio.WithNumShards(1),
	))

	pipeline.WaitForCompletion()

	// --- Verify: read back from fake S3 ---
	reader, err := fileio.Open("s3://test-output/temperatures.csv",
		s3backend.WithS3Client(fakeClient))
	if err != nil {
		t.Fatalf("failed to read back from fake S3: %v", err)
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("failed to read data: %v", err)
	}

	outputLines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(outputLines) < 100 {
		t.Fatalf("expected at least 100 temperature records, got %d", len(outputLines))
	}
	t.Logf("wrote %d temperature records to fake S3", len(outputLines))

	// Every line should be TMAX or TMIN with a valid Celsius value.
	slices.Sort(outputLines)
	elements := make(map[string]int)
	for _, line := range outputLines {
		parts := strings.SplitN(line, ",", 4)
		if len(parts) != 4 {
			t.Fatalf("malformed output line: %q", line)
		}
		elem := parts[2]
		if elem != "TMAX" && elem != "TMIN" {
			t.Fatalf("unexpected element %q in output (expected only TMAX/TMIN)", elem)
		}
		celsius, err := strconv.ParseFloat(parts[3], 64)
		if err != nil {
			t.Fatalf("invalid Celsius value in line %q: %v", line, err)
		}
		if celsius < -90 || celsius > 60 {
			t.Errorf("suspicious temperature %.1f°C in line %q", celsius, line)
		}
		elements[elem]++
	}

	if elements["TMAX"] == 0 || elements["TMIN"] == 0 {
		t.Errorf("expected both TMAX and TMIN records, got: %v", elements)
	}
	t.Logf("verified output: %d TMAX + %d TMIN records, all valid", elements["TMAX"], elements["TMIN"])
}
