package tubing_cdc

import (
	"context"
	"fmt"
)

// Row is the structured representation of one database row.
type Row map[string]any

// Event is the in-memory representation passed through a Processor pipeline.
// Insert and snapshot events normally set After, updates set Before and After,
// and deletes set Before.
type Event struct {
	EventID       string
	SchemaVersion string
	Origin        EventOrigin
	Action        string
	Table         TableIdentity
	PrimaryKey    Row
	Position      *BinlogPosition
	Before        Row
	After         Row
	Metadata      map[string]any
}

// TableKey returns the fully-qualified database.table name.
func (e Event) TableKey() string {
	return e.Table.Database + "." + e.Table.Table
}

// Processor transforms zero, one, or many events. Returning no events filters
// the input; returning multiple events implements flat-map behavior.
type Processor func(context.Context, Event) ([]Event, error)

// Pipe composes processors from left to right. A nil processor is ignored.
func Pipe(processors ...Processor) Processor {
	return func(ctx context.Context, event Event) ([]Event, error) {
		events := []Event{event}
		for _, processor := range processors {
			if processor == nil {
				continue
			}
			next := make([]Event, 0, len(events))
			for _, current := range events {
				out, err := processor(ctx, current)
				if err != nil {
					return nil, err
				}
				next = append(next, out...)
			}
			events = next
			if len(events) == 0 {
				break
			}
		}
		return events, nil
	}
}

// Map transforms one event into one event.
func Map(fn func(Event) Event) Processor {
	if fn == nil {
		return nil
	}
	return func(_ context.Context, event Event) ([]Event, error) {
		return []Event{fn(event)}, nil
	}
}

// MapE is the context-aware, error-returning form of Map.
func MapE(fn func(context.Context, Event) (Event, error)) Processor {
	if fn == nil {
		return nil
	}
	return func(ctx context.Context, event Event) ([]Event, error) {
		mapped, err := fn(ctx, event)
		if err != nil {
			return nil, err
		}
		return []Event{mapped}, nil
	}
}

// Filter keeps events for which predicate returns true.
func Filter(predicate func(Event) bool) Processor {
	if predicate == nil {
		return nil
	}
	return func(_ context.Context, event Event) ([]Event, error) {
		if !predicate(event) {
			return nil, nil
		}
		return []Event{event}, nil
	}
}

// FlatMap transforms one event into zero, one, or many events.
func FlatMap(fn func(context.Context, Event) ([]Event, error)) Processor {
	return Processor(fn)
}

// Tap performs an effect without changing the event.
func Tap(fn func(Event) error) Processor {
	if fn == nil {
		return nil
	}
	return func(_ context.Context, event Event) ([]Event, error) {
		if err := fn(event); err != nil {
			return nil, err
		}
		return []Event{event}, nil
	}
}

// ForTable applies processors only to the named fully-qualified table.
func ForTable(tableKey string, processors ...Processor) Processor {
	inner := Pipe(processors...)
	return func(ctx context.Context, event Event) ([]Event, error) {
		if event.TableKey() != tableKey {
			return []Event{event}, nil
		}
		return inner(ctx, event)
	}
}

// ForAction applies processors only to events with the named canal action.
func ForAction(action string, processors ...Processor) Processor {
	inner := Pipe(processors...)
	return func(ctx context.Context, event Event) ([]Event, error) {
		if event.Action != action {
			return []Event{event}, nil
		}
		return inner(ctx, event)
	}
}

// When applies processors only when predicate returns true.
func When(predicate func(Event) bool, processors ...Processor) Processor {
	inner := Pipe(processors...)
	return func(ctx context.Context, event Event) ([]Event, error) {
		if predicate == nil || !predicate(event) {
			return []Event{event}, nil
		}
		return inner(ctx, event)
	}
}

// PipelineErrorHandler decides whether a processor error stops OnRow. Returning
// nil skips the failed input event; returning an error stops processing.
type PipelineErrorHandler func(context.Context, Event, error) error

// StopOnPipelineError is the default strict policy.
func StopOnPipelineError(_ context.Context, _ Event, err error) error {
	return err
}

// SkipPipelineError reports an error and skips the failed input event. Reporter
// may be nil. A reporter failure stops processing.
func SkipPipelineError(reporter func(context.Context, Event, error) error) PipelineErrorHandler {
	return func(ctx context.Context, event Event, err error) error {
		if reporter == nil {
			return nil
		}
		if reportErr := reporter(ctx, event, err); reportErr != nil {
			return fmt.Errorf("pipeline error reporter: %w", reportErr)
		}
		return nil
	}
}
