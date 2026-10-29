// Package bridge provides a Go bridge for parsing P model checker traces and
// replaying them against the Go FSM implementation. This follows the pattern
// established in darepo's p-models/bridge/ package.
package bridge

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strconv"
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

var (
	stateChangeLog = regexp.MustCompile(
		`with payload <([0-9]+),([0-9]+),>\.`,
	)
	emptyFilteredBlockLog = regexp.MustCompile(
		`with payload <([0-9]+),\(\),>\.`,
	)
)

// ParseTraceFile reads a P model checker trace JSON file and returns the
// parsed trace.
func ParseTraceFile(path string) (*Trace, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf(
			"reading trace file: %w",
			err,
		)
	}

	return ParseTrace(data)
}

// ParseTrace parses raw JSON trace data from the P model checker.
func ParseTrace(data []byte) (*Trace, error) {
	var entries []TraceEntry
	if err := json.Unmarshal(data, &entries); err == nil {
		return &Trace{Entries: entries}, nil
	}

	// PChecker writes an array of schedules whose records nest event data
	// in details. Its serializer emits empty objects for integer tuple
	// members, so use the corresponding values in the P runtime log.
	var schedules [][]struct {
		Type    string `json:"type"`
		Details struct {
			Log     string          `json:"log"`
			Event   string          `json:"event"`
			Payload json.RawMessage `json:"payload"`
		} `json:"details"`
	}
	if err := json.Unmarshal(data, &schedules); err != nil {
		return nil, fmt.Errorf(
			"parsing trace JSON: %w",
			err,
		)
	}
	if len(schedules) != 1 {
		return nil, fmt.Errorf(
			"expected one P schedule, got %d",
			len(schedules),
		)
	}
	for _, record := range schedules[0] {
		if record.Type != "Announce" {
			continue
		}
		entry := TraceEntry{
			Type:    record.Type,
			Event:   record.Details.Event,
			Payload: record.Details.Payload,
		}
		switch EventType(entry.Event) {
		case EvFSMStateChange:
			matches := stateChangeLog.FindStringSubmatch(
				record.Details.Log,
			)
			if len(matches) != 3 {
				return nil, fmt.Errorf(
					"invalid P state change log: %s",
					record.Details.Log,
				)
			}
			from, err := strconv.Atoi(matches[1])
			if err != nil {
				return nil, fmt.Errorf(
					"invalid P from-state: %w",
					err,
				)
			}
			to, err := strconv.Atoi(matches[2])
			if err != nil {
				return nil, fmt.Errorf(
					"invalid P to-state: %w",
					err,
				)
			}
			entry.Payload, err = json.Marshal(
				StateChangePayload{
					From: FSMState(
						from,
					), To: FSMState(to),
				},
			)
			if err != nil {
				return nil, fmt.Errorf(
					"encoding P state change: %w",
					err,
				)
			}
		case EvFilteredBlockOutbox:
			matches := emptyFilteredBlockLog.FindStringSubmatch(
				record.Details.Log,
			)
			if len(matches) != 2 {
				return nil, fmt.Errorf(
					"unsupported P filtered block log: %s",
					record.Details.Log,
				)
			}
			height, err := strconv.Atoi(matches[1])
			if err != nil {
				return nil, fmt.Errorf(
					"invalid P block height: %w",
					err,
				)
			}
			entry.Payload, err = json.Marshal(
				FilteredBlockPayload{
					Height: height,
				},
			)
			if err != nil {
				return nil, fmt.Errorf(
					"encoding P filtered block: %w",
					err,
				)
			}
		}
		entries = append(entries, entry)
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
			"parsing payload for event %s: %w",
			entry.Event,
			err,
		)
	}
	return result, nil
}
