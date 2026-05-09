// Package bridge provides a Go bridge for parsing P model checker traces and
// replaying them against the Go FSM implementation. This follows the pattern
// established in darepo's p-models/bridge/ package.
package bridge

import (
	"encoding/json"
	"fmt"
	"os"
)

// TraceEntry represents a single entry in a P model checker trace JSON output.
type TraceEntry struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
	Machine string          `json:"machine,omitempty"`
	Event   string          `json:"event,omitempty"`
	State   string          `json:"state,omitempty"`
	Detail  string          `json:"detail,omitempty"`
}

// Trace is a sequence of trace entries from a P model checker run.
type Trace struct {
	Entries []TraceEntry
}

// ParseTraceFile reads a P model checker trace JSON file and returns the
// parsed trace.
func ParseTraceFile(path string) (*Trace, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading trace file: %w", err)
	}

	return ParseTrace(data)
}

// ParseTrace parses raw JSON trace data from the P model checker.
func ParseTrace(data []byte) (*Trace, error) {
	var entries []TraceEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("parsing trace JSON: %w", err)
	}

	return &Trace{Entries: entries}, nil
}

// FilterByType returns only trace entries of the given type.
func (t *Trace) FilterByType(entryType string) []TraceEntry {
	var result []TraceEntry
	for _, e := range t.Entries {
		if e.Type == entryType {
			result = append(result, e)
		}
	}
	return result
}

// FilterByEvent returns only trace entries for the given event name.
func (t *Trace) FilterByEvent(eventName string) []TraceEntry {
	var result []TraceEntry
	for _, e := range t.Entries {
		if e.Event == eventName {
			result = append(result, e)
		}
	}
	return result
}

// ParseEventPayload unmarshals the payload of a trace entry into the given
// type. This is a generic helper following the darepo bridge pattern.
func ParseEventPayload[T any](entry TraceEntry) (T, error) {
	var result T
	if err := json.Unmarshal(entry.Payload, &result); err != nil {
		return result, fmt.Errorf(
			"parsing payload for event %s: %w", entry.Event, err,
		)
	}
	return result, nil
}
