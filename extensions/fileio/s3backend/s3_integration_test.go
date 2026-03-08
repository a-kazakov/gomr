package s3backend_test

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

// noaaRecord represents a parsed line from the NOAA GHCN CSV.
// Format: ID,DATE,ELEMENT,DATA_VALUE,M_FLAG,Q_FLAG,S_FLAG,OBS_TIME
type noaaRecord struct {
	StationID string
	Element   string
	Value     int
}

func parseNoaaLine(line []byte) noaaRecord {
	fields := strings.SplitN(string(line), ",", 5)
	val, _ := strconv.Atoi(fields[3])
	return noaaRecord{
		StationID: fields[0],
		Element:   fields[2],
		Value:     val,
	}
}

// stationSummary is the aggregated output: one per (station, element) pair.
type stationSummary struct {
	StationID string
	Element   string
	Count     int
	Sum       int
}

// summarySerializer implements fileio.FileSerializer for stationSummary.
type summarySerializer struct{}

func (s *summarySerializer) MarshalFileName(_ *stationSummary, dest []byte) int {
	return copy(dest, "summary.csv")
}

func (s *summarySerializer) MarshalRecord(v *stationSummary, dest []byte) int {
	line := fmt.Sprintf("%s,%s,%d,%d\n", v.StationID, v.Element, v.Count, v.Sum)
	return copy(dest, line)
}

// TestS3Integration reads NOAA weather data from a public S3 bucket, aggregates
// it through a gomr pipeline, writes the results to a local fake S3 server via
// fileio.WriteFiles, and reads them back to verify the full round-trip.
func TestS3Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping S3 integration test in short mode")
	}

	// --- Set up fake S3 for writing ---
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

	// --- Public S3 client for reading (anonymous) ---
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

	// --- Pipeline: read → aggregate → write ---
	pipeline := gomr.NewPipeline()

	// Read CSV records from the public NOAA bucket (~1459 data lines).
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
	t.Logf("aggregated %d records into %d (station, element) groups", countRecords(summarySlice), len(summarySlice))

	// Verify expected weather elements are present.
	elements := make(map[string]bool)
	for _, s := range summarySlice {
		elements[s.Element] = true
	}
	for _, want := range []string{"TMAX", "TMIN"} {
		if !elements[want] {
			t.Errorf("expected element %q in data, found: %v", want, elements)
		}
	}

	// --- Write aggregated results to fake S3 via a second pipeline ---
	pipeline2 := gomr.NewPipeline()

	summaryData := summarySlice // capture for the seed closure
	collection := gomr.NewSeedCollection(pipeline2,
		func(_ gomr.OperatorContext, emitter gomr.Emitter[stationSummary]) {
			for i := range summaryData {
				*emitter.GetEmitPointer() = summaryData[i]
			}
		},
	)

	fileio.WriteFiles(
		collection,
		&summarySerializer{},
		"s3://test-output",
		func(w io.Writer) io.WriteCloser { return fileio.NewFakeWriteCloser(w) },
		s3backend.WithS3Client(fakeClient),
		fileio.WithNumShards(1),
	)

	pipeline2.WaitForCompletion()

	// --- Read back from fake S3 and verify ---
	reader, err := fileio.Open("s3://test-output/summary.csv",
		s3backend.WithS3Client(fakeClient))
	if err != nil {
		t.Fatalf("failed to open from fake S3: %v", err)
	}
	defer reader.Close()

	readBack, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("failed to read back: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(readBack)), "\n")
	if len(lines) != len(summarySlice) {
		t.Fatalf("read back %d lines, expected %d", len(lines), len(summarySlice))
	}

	expectedLines := make([]string, 0, len(summarySlice))
	for _, s := range summarySlice {
		expectedLines = append(expectedLines, fmt.Sprintf("%s,%s,%d,%d", s.StationID, s.Element, s.Count, s.Sum))
	}
	slices.Sort(expectedLines)
	slices.Sort(lines)

	if !slices.Equal(lines, expectedLines) {
		t.Errorf("round-trip data mismatch.\nExpected (first 5):\n%s\nGot (first 5):\n%s",
			strings.Join(expectedLines[:min(5, len(expectedLines))], "\n"),
			strings.Join(lines[:min(5, len(lines))], "\n"))
	}

	t.Logf("round-trip verified: %d summary lines written to and read back from fake S3", len(lines))
}

func countRecords(summaries []stationSummary) int {
	total := 0
	for _, s := range summaries {
		total += s.Count
	}
	return total
}
