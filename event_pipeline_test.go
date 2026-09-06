package tubing_cdc

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestPipe_ComposesFilterMapAndFlatMap(t *testing.T) {
	pipeline := Pipe(
		Filter(func(event Event) bool { return event.After["active"] == true }),
		Map(func(event Event) Event {
			event.After["mapped"] = true
			return event
		}),
		FlatMap(func(_ context.Context, event Event) ([]Event, error) {
			copy := event
			copy.Action = "copy"
			return []Event{event, copy}, nil
		}),
	)

	out, err := pipeline(context.Background(), Event{After: Row{"active": true}})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 2 || out[0].After["mapped"] != true || out[1].Action != "copy" {
		t.Fatalf("unexpected output: %#v", out)
	}

	filtered, err := pipeline(context.Background(), Event{After: Row{"active": false}})
	if err != nil {
		t.Fatal(err)
	}
	if len(filtered) != 0 {
		t.Fatalf("expected filtered event, got %#v", filtered)
	}
}

func TestPipelineScopes(t *testing.T) {
	mark := Map(func(event Event) Event {
		event.Metadata = map[string]any{"matched": true}
		return event
	})
	pipeline := Pipe(
		ForTable("db.orders", mark),
		ForAction("insert", Map(func(event Event) Event {
			event.Metadata["insert"] = true
			return event
		})),
	)

	event := Event{Table: TableIdentity{Database: "db", Table: "orders"}, Action: "insert"}
	out, err := pipeline(context.Background(), event)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(out[0].Metadata, map[string]any{"matched": true, "insert": true}) {
		t.Fatalf("metadata: %#v", out[0].Metadata)
	}
}

func TestSkipPipelineError(t *testing.T) {
	want := errors.New("bad row")
	var reported error
	handler := SkipPipelineError(func(_ context.Context, _ Event, err error) error {
		reported = err
		return nil
	})
	if err := handler(context.Background(), Event{}, want); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(reported, want) {
		t.Fatalf("reported %v, want %v", reported, want)
	}
}
