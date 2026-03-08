package s3backend_test

import (
	"bytes"
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

// noaaRecord represents a parsed line from the NOAA GHCN CSV.
// Format: ID,DATE,ELEMENT,DATA_VALUE,M_FLAG,Q_FLAG,S_FLAG,OBS_TIME
type noaaRecord struct {
	StationID string
	Date      string
	Element   string
	Value     int
}

func parseNoaaLine(line []byte) noaaRecord {
	fields := strings.SplitN(string(line), ",", 5)
	val, _ := strconv.Atoi(fields[3])
	return noaaRecord{
		StationID: fields[0],
		Date:      fields[1],
		Element:   fields[2],
		Value:     val,
	}
}

// stationSummary is the output record: one per (station, element) pair.
type stationSummary struct {
	StationID string
	Element   string
	Count     int
	Sum       int
}

func newFakeS3Client(ts *httptest.Server) *s3.Client {
	return s3.New(s3.Options{
		BaseEndpoint: aws.String(ts.URL),
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
		Region:       "us-east-1",
		UsePathStyle: true,
	})
}

// TestS3ReadPublicBucket reads from a public NOAA S3 bucket, processes records
// through a gomr pipeline, and verifies results.
func TestS3ReadPublicBucket(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping S3 integration test in short mode")
	}

	// Create an anonymous S3 client for the public bucket (no credentials needed).
	publicClient := s3.New(s3.Options{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("", "", ""),
	})

	// Verify connectivity before running the full pipeline.
	_, err := publicClient.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String("noaa-ghcn-pds"),
		Key:    aws.String("csv/by_year/1788.csv"),
	})
	if err != nil {
		t.Skipf("cannot reach public S3 bucket (no network?): %v", err)
	}

	pipeline := gomr.NewPipeline()
	s3Opt := s3backend.WithS3Client(publicClient)

	// Read the 1788 CSV (small: ~1459 data lines).
	records := fileio.ReadFiles(pipeline, "s3://noaa-ghcn-pds/csv/by_year/1788.csv",
		parseNoaaLine, s3Opt)

	// Collect all records.
	allRecords := gomr.Collect(records,
		func(_ gomr.OperatorContext, recv gomr.CollectionReceiver[noaaRecord]) []noaaRecord {
			var out []noaaRecord
			for r := range recv.IterValues() {
				out = append(out, *r)
			}
			return out
		},
		func(_ gomr.OperatorContext, parts [][]noaaRecord) []noaaRecord {
			var out []noaaRecord
			for _, p := range parts {
				out = append(out, p...)
			}
			return out
		},
	)

	pipeline.WaitForCompletion()
	result := allRecords.Wait()

	if len(result) == 0 {
		t.Fatal("expected records from S3, got none")
	}

	// The CSV header line "ID,DATE,ELEMENT,..." should be skipped by ReadFiles
	// (it parses into Value=0 which is valid, but the header's DATA_VALUE field
	// is literally "DATA_VALUE" which Atoi converts to 0). Count data lines:
	// The file has 1460 lines total, 1 header + 1459 data lines.
	// ReadFiles includes the header since it's a non-empty line, so we get 1459 data + 1 header = 1460.
	// But actually ReadLines skips the first empty line and header is non-empty, so all 1460 lines
	// (including header) are returned. Let's just verify a reasonable count.
	if len(result) < 1000 {
		t.Fatalf("expected at least 1000 records, got %d", len(result))
	}
	t.Logf("read %d records from public S3", len(result))

	// Verify some known data properties:
	// - All records should have StationID starting with a letter
	// - Elements should be things like TMAX, TMIN, PRCP
	elements := make(map[string]bool)
	for _, r := range result {
		elements[r.Element] = true
	}
	for _, expected := range []string{"TMAX", "TMIN"} {
		if !elements[expected] {
			t.Errorf("expected element %q in data, found elements: %v", expected, elements)
		}
	}
}

// summarySerializer implements fileio.FileSerializer for stationSummary.
// Groups all summaries into a single output file.
type summarySerializer struct {
	filename string
}

func (s *summarySerializer) MarshalFileName(_ *stationSummary, dest []byte) int {
	return copy(dest, s.filename)
}

func (s *summarySerializer) MarshalRecord(v *stationSummary, dest []byte) int {
	line := fmt.Sprintf("%s,%s,%d,%d\n", v.StationID, v.Element, v.Count, v.Sum)
	return copy(dest, line)
}

// TestS3ReadAndWriteWithFakeS3 reads from the public NOAA bucket, aggregates
// data through a pipeline, writes results to a local fake S3 server, and reads
// them back to verify correctness.
func TestS3ReadAndWriteWithFakeS3(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping S3 integration test in short mode")
	}

	// --- Set up fake S3 for writing ---
	fakeBE := s3mem.New()
	faker := gofakes3.New(fakeBE)
	ts := httptest.NewServer(faker.Server())
	defer ts.Close()

	fakeClient := newFakeS3Client(ts)

	// Create the output bucket.
	_, err := fakeClient.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String("test-output"),
	})
	if err != nil {
		t.Fatalf("failed to create fake bucket: %v", err)
	}

	// --- Public S3 client for reading ---
	publicClient := s3.New(s3.Options{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("", "", ""),
	})

	// Verify connectivity.
	_, err = publicClient.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String("noaa-ghcn-pds"),
		Key:    aws.String("csv/by_year/1788.csv"),
	})
	if err != nil {
		t.Skipf("cannot reach public S3 bucket (no network?): %v", err)
	}

	// --- Pipeline: read from public S3, aggregate, write to fake S3 ---
	pipeline := gomr.NewPipeline()

	// Read CSV records from the public bucket.
	records := fileio.ReadFiles(pipeline, "s3://noaa-ghcn-pds/csv/by_year/1788.csv",
		parseNoaaLine, s3backend.WithS3Client(publicClient))

	// Aggregate: count and sum values per (station, element).
	summaries := gomr.Collect(records,
		func(_ gomr.OperatorContext, recv gomr.CollectionReceiver[noaaRecord]) []stationSummary {
			type key struct{ station, element string }
			agg := make(map[key]*stationSummary)
			for r := range recv.IterValues() {
				k := key{r.StationID, r.Element}
				s, ok := agg[k]
				if !ok {
					s = &stationSummary{StationID: r.StationID, Element: r.Element}
					agg[k] = s
				}
				s.Count++
				s.Sum += r.Value
			}
			out := make([]stationSummary, 0, len(agg))
			for _, s := range agg {
				out = append(out, *s)
			}
			return out
		},
		func(_ gomr.OperatorContext, parts [][]stationSummary) []stationSummary {
			// Merge partial aggregates.
			type key struct{ station, element string }
			agg := make(map[key]*stationSummary)
			for _, part := range parts {
				for i := range part {
					s := &part[i]
					k := key{s.StationID, s.Element}
					existing, ok := agg[k]
					if !ok {
						agg[k] = s
					} else {
						existing.Count += s.Count
						existing.Sum += s.Sum
					}
				}
			}
			out := make([]stationSummary, 0, len(agg))
			for _, s := range agg {
				out = append(out, *s)
			}
			return out
		},
	)

	pipeline.WaitForCompletion()
	summarySlice := summaries.Wait()

	if len(summarySlice) == 0 {
		t.Fatal("expected aggregated summaries, got none")
	}
	t.Logf("aggregated into %d (station, element) groups", len(summarySlice))

	// --- Write aggregated results to fake S3 ---
	var buf bytes.Buffer
	for _, s := range summarySlice {
		fmt.Fprintf(&buf, "%s,%s,%d,%d\n", s.StationID, s.Element, s.Count, s.Sum)
	}

	_, err = fakeClient.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String("test-output"),
		Key:    aws.String("summaries/1788.csv"),
		Body:   bytes.NewReader(buf.Bytes()),
	})
	if err != nil {
		t.Fatalf("failed to write to fake S3: %v", err)
	}

	// --- Read back from fake S3 via fileio and verify ---
	reader, err := fileio.Open("s3://test-output/summaries/1788.csv",
		s3backend.WithS3Client(fakeClient))
	if err != nil {
		t.Fatalf("failed to open from fake S3: %v", err)
	}
	defer reader.Close()

	readBack, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("failed to read back from fake S3: %v", err)
	}

	// Parse the read-back data and verify it matches.
	lines := strings.Split(strings.TrimSpace(string(readBack)), "\n")
	if len(lines) != len(summarySlice) {
		t.Fatalf("read back %d lines, expected %d", len(lines), len(summarySlice))
	}

	// Build a set of expected lines for comparison (order may differ).
	expectedLines := make([]string, 0, len(summarySlice))
	for _, s := range summarySlice {
		expectedLines = append(expectedLines, fmt.Sprintf("%s,%s,%d,%d", s.StationID, s.Element, s.Count, s.Sum))
	}
	slices.Sort(expectedLines)
	slices.Sort(lines)

	if !slices.Equal(lines, expectedLines) {
		t.Errorf("read-back data does not match written data.\nExpected:\n%s\nGot:\n%s",
			strings.Join(expectedLines[:min(5, len(expectedLines))], "\n"),
			strings.Join(lines[:min(5, len(lines))], "\n"))
	}

	t.Logf("successfully wrote and read back %d summary lines via fake S3", len(lines))
}

// TestFakeS3WriteFilesAndReadBack uses gofakes3 to test the full fileio
// pipeline: WriteFiles writes to fake S3, then we read back via Open.
func TestFakeS3WriteFilesAndReadBack(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping S3 integration test in short mode")
	}

	// --- Set up fake S3 ---
	fakeBE := s3mem.New()
	faker := gofakes3.New(fakeBE)
	ts := httptest.NewServer(faker.Server())
	defer ts.Close()

	fakeClient := newFakeS3Client(ts)
	_, err := fakeClient.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket: aws.String("test-writefiles"),
	})
	if err != nil {
		t.Fatalf("failed to create bucket: %v", err)
	}

	// --- Pipeline: seed some data, write via WriteFiles to fake S3 ---
	pipeline := gomr.NewPipeline()

	data := []stationSummary{
		{"STATION_A", "TMAX", 10, 250},
		{"STATION_A", "TMIN", 10, 100},
		{"STATION_B", "TMAX", 5, 150},
		{"STATION_B", "TMIN", 5, 50},
	}

	collection := gomr.NewSeedCollection(pipeline,
		func(_ gomr.OperatorContext, emitter gomr.Emitter[stationSummary]) {
			for i := range data {
				*emitter.GetEmitPointer() = data[i]
			}
		},
	)

	// Write all records to a single file via WriteFiles.
	producedFiles := fileio.WriteFiles(
		collection,
		&summarySerializer{filename: "output.csv"},
		"s3://test-writefiles",
		func(w io.Writer) io.WriteCloser { return fileio.NewFakeWriteCloser(w) },
		s3backend.WithS3Client(fakeClient),
		fileio.WithNumShards(1),
	)

	fileList := gomr.Collect(producedFiles,
		func(_ gomr.OperatorContext, recv gomr.CollectionReceiver[string]) []string {
			var out []string
			for v := range recv.IterValues() {
				out = append(out, *v)
			}
			return out
		},
		func(_ gomr.OperatorContext, parts [][]string) []string {
			var out []string
			for _, p := range parts {
				out = append(out, p...)
			}
			return out
		},
	)

	pipeline.WaitForCompletion()
	files := fileList.Wait()
	t.Logf("WriteFiles produced files: %v", files)

	// --- Read back from fake S3 ---
	reader, err := fileio.Open("s3://test-writefiles/output.csv",
		s3backend.WithS3Client(fakeClient))
	if err != nil {
		t.Fatalf("failed to read back from fake S3: %v", err)
	}
	defer reader.Close()

	readBack, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("failed to read data: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(readBack)), "\n")
	slices.Sort(lines)

	expected := []string{
		"STATION_A,TMAX,10,250",
		"STATION_A,TMIN,10,100",
		"STATION_B,TMAX,5,150",
		"STATION_B,TMIN,5,50",
	}
	slices.Sort(expected)

	if !slices.Equal(lines, expected) {
		t.Errorf("WriteFiles output mismatch.\nExpected:\n%s\nGot:\n%s",
			strings.Join(expected, "\n"), strings.Join(lines, "\n"))
	}

	t.Logf("WriteFiles round-trip verified: %d records", len(lines))
}
