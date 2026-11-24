package testsupport

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"testing"
)

// DiscardLogger is for tests that do not read the log.
func DiscardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// LogCapture keeps what a real slog JSON handler writes, so attributes added by With reach the test as they would
// reach the pod log.
type LogCapture struct {
	t      *testing.T
	mu     sync.Mutex
	buffer bytes.Buffer
}

func NewLogCapture(t *testing.T) (*slog.Logger, *LogCapture) {
	capture := &LogCapture{t: t}
	return slog.New(slog.NewJSONHandler(capture, nil)), capture
}

func (c *LogCapture) Write(line []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buffer.Write(line)
}

// All returns every line, decoded. A key written twice on a line reads as its last value: the one the line itself
// set, over the one a With before it set.
func (c *LogCapture) All() []map[string]any {
	c.t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	records := []map[string]any{}
	for _, line := range bytes.Split(c.buffer.Bytes(), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal(line, &record); err != nil {
			c.t.Fatalf("log line is not JSON: %s", line)
		}
		records = append(records, record)
	}
	return records
}

// Records returns every line with the message msg, decoded.
func (c *LogCapture) Records(msg string) []map[string]any {
	c.t.Helper()
	records := []map[string]any{}
	for _, record := range c.All() {
		if record["msg"] == msg {
			records = append(records, record)
		}
	}
	return records
}

// One returns the only line with the message msg; none or several fails the test.
func (c *LogCapture) One(msg string) map[string]any {
	c.t.Helper()
	records := c.Records(msg)
	if len(records) != 1 {
		c.t.Fatalf("want one log line %q, got %d", msg, len(records))
	}
	return records[0]
}

// Objects returns the "object" field of every line with the message msg; a line without one fails the test.
func (c *LogCapture) Objects(msg string) []string {
	c.t.Helper()
	objects := []string{}
	for _, record := range c.Records(msg) {
		object, isText := record["object"].(string)
		if !isText || object == "" {
			c.t.Fatalf("log line %q names no object: %v", msg, record)
		}
		objects = append(objects, object)
	}
	return objects
}
