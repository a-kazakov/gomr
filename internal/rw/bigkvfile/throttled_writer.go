package bigkvfile

import (
	"io"
	"time"
)

type ThrottledWriter struct {
	writer        io.Writer
	targetLatency time.Duration
}

func NewThrottledWriter(writer io.Writer, targetLatency time.Duration) *ThrottledWriter {
	return &ThrottledWriter{writer: writer, targetLatency: targetLatency}
}

func (t *ThrottledWriter) Write(p []byte) (n int, err error) {
	start := time.Now()
	n, err = t.writer.Write(p)
	duration := time.Since(start)
	if duration > t.targetLatency { // Back off to avoid overwhelming the disk
		penalty := duration - t.targetLatency
		time.Sleep(min(penalty, 1*time.Second))
	}
	return n, err
}
