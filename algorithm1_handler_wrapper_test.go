package tubing_cdc

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/schema"
)

func TestWrapHandlerWithAlgorithm1_OnRow_recordsPK(t *testing.T) {
	t.Parallel()
	tbl := &schema.Table{
		Schema: "app",
		Name:   "users",
		Columns: []schema.TableColumn{
			{Name: "id", Type: schema.TYPE_NUMBER},
			{Name: "name", Type: schema.TYPE_STRING},
		},
		PKColumns: []int{0},
	}
	tests := []struct {
		name        string
		action      string
		rows        [][]interface{}
		wantTracked bool
	}{
		{
			name:        "insert",
			action:      canal.InsertAction,
			rows:        [][]interface{}{{int64(5), "x"}},
			wantTracked: true,
		},
		{
			name:        "update_both_pks",
			action:      canal.UpdateAction,
			rows:        [][]interface{}{{int64(1), "a"}, {int64(2), "b"}},
			wantTracked: true,
		},
		{
			name:        "delete",
			action:      canal.DeleteAction,
			rows:        [][]interface{}{{int64(9), "z"}},
			wantTracked: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tr := NewAlgorithm1Tracker()
			if err := tr.BeginCapture("app.users", "L", "H", []string{"id"}); err != nil {
				t.Fatal(err)
			}
			if err := tr.OnWatermark(WatermarkBinlogEvent{NewValue: "L"}); err != nil {
				t.Fatal(err)
			}
			inner := &countingRowHandler{}
			h := wrapHandlerWithAlgorithm1(inner, tr, "app.users")
			err := h.OnRow(&canal.RowsEvent{
				Table:  tbl,
				Action: tt.action,
				Rows:   tt.rows,
			})
			if err != nil {
				t.Fatal(err)
			}
			if inner.n != 1 {
				t.Fatalf("inner OnRow count=%d", inner.n)
			}
			if err := tr.OnWatermark(WatermarkBinlogEvent{NewValue: "H"}); err != nil {
				t.Fatal(err)
			}
			chunk := []map[string]any{{"id": int64(1)}, {"id": int64(2)}, {"id": int64(5)}, {"id": int64(9)}}
			out, err := tr.ReconcileChunkRows(chunk)
			if err != nil {
				t.Fatal(err)
			}
			if !tt.wantTracked {
				return
			}
			// All listed ids should be removed from chunk if they were recorded.
			switch tt.name {
			case "insert":
				if len(out) != 3 {
					t.Fatalf("len=%d want 3 (id 5 dropped)", len(out))
				}
			case "update_both_pks":
				if len(out) != 2 {
					t.Fatalf("len=%d want 2 (ids 1 and 2 dropped)", len(out))
				}
			case "delete":
				if len(out) != 3 {
					t.Fatalf("len=%d want 3 (id 9 dropped)", len(out))
				}
			}
		})
	}
}

func TestWrapHandlerWithAlgorithm1_other_table(t *testing.T) {
	t.Parallel()
	tr := NewAlgorithm1Tracker()
	if err := tr.BeginCapture("app.users", "L", "H", []string{"id"}); err != nil {
		t.Fatal(err)
	}
	if err := tr.OnWatermark(WatermarkBinlogEvent{NewValue: "L"}); err != nil {
		t.Fatal(err)
	}
	inner := &countingRowHandler{}
	h := wrapHandlerWithAlgorithm1(inner, tr, "app.users")
	other := &schema.Table{
		Schema: "app",
		Name:   "other",
		Columns: []schema.TableColumn{
			{Name: "id", Type: schema.TYPE_NUMBER},
		},
		PKColumns: []int{0},
	}
	if err := h.OnRow(&canal.RowsEvent{
		Table:  other,
		Action: canal.InsertAction,
		Rows:   [][]interface{}{{int64(99)}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := tr.OnWatermark(WatermarkBinlogEvent{NewValue: "H"}); err != nil {
		t.Fatal(err)
	}
	out, err := tr.ReconcileChunkRows([]map[string]any{{"id": int64(99)}})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatalf("expected row kept, got %d", len(out))
	}
}

func TestWrapHandlerWithAlgorithm1_nil_tracker(t *testing.T) {
	t.Parallel()
	inner := &countingRowHandler{}
	h := wrapHandlerWithAlgorithm1(inner, nil, "app.users")
	tbl := &schema.Table{Schema: "app", Name: "users", Columns: []schema.TableColumn{{Name: "id"}}}
	if err := h.OnRow(&canal.RowsEvent{Table: tbl, Action: canal.InsertAction, Rows: [][]interface{}{{int64(1)}}}); err != nil {
		t.Fatal(err)
	}
	if inner.n != 1 {
		t.Fatal("inner not called")
	}
}
