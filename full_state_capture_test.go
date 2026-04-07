package tubing_cdc

import (
	"strings"
	"testing"
)

func TestFullStateTableSpec_Validate(t *testing.T) {
	tests := []struct {
		name    string
		spec    FullStateTableSpec
		wantErr bool
		errSub  string
	}{
		{
			name:    "valid",
			spec:    FullStateTableSpec{TableKey: "db.t", PKColumns: []string{"id"}, ChunkSize: 100},
			wantErr: false,
		},
		{
			name:    "empty table",
			spec:    FullStateTableSpec{PKColumns: []string{"id"}, ChunkSize: 1},
			wantErr: true,
			errSub:  "table_key",
		},
		{
			name:    "no pk",
			spec:    FullStateTableSpec{TableKey: "db.t", ChunkSize: 1},
			wantErr: true,
			errSub:  "pk columns",
		},
		{
			name:    "bad chunk size",
			spec:    FullStateTableSpec{TableKey: "db.t", PKColumns: []string{"id"}, ChunkSize: 0},
			wantErr: true,
			errSub:  "chunk_size",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.spec.Validate()
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				if tt.errSub != "" && !strings.Contains(err.Error(), tt.errSub) {
					t.Fatalf("error %q should mention %q", err.Error(), tt.errSub)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPlanFullStateJobs(t *testing.T) {
	cfg := &FullStateCaptureConfig{
		Tables: []FullStateTableSpec{
			{TableKey: "a.one", PKColumns: []string{"id"}, ChunkSize: 10, RunID: "r1"},
			{TableKey: "a.two", PKColumns: []string{"x", "y"}, ChunkSize: 0, RunID: ""},
		},
	}
	tests := []struct {
		name    string
		opts    PlanFullStateJobsOptions
		wantLen int
		wantErr bool
		errSub  string
		kind0   FullStateJobKind
	}{
		{
			name:    "all tables",
			opts:    PlanFullStateJobsOptions{Mode: PlanFullStateAllTables, DefaultChunkSize: 50},
			wantLen: 2,
			kind0:   FullStateJobChunkedTable,
		},
		{
			name:    "one table",
			opts:    PlanFullStateJobsOptions{Mode: PlanFullStateOneTable, TableKey: "a.one"},
			wantLen: 1,
			kind0:   FullStateJobChunkedTable,
		},
		{
			name:    "unknown table",
			opts:    PlanFullStateJobsOptions{Mode: PlanFullStateOneTable, TableKey: "missing.t"},
			wantErr: true,
			errSub:  "unknown table",
		},
		{
			name:    "selected pks",
			opts:    PlanFullStateJobsOptions{Mode: PlanFullStateSelectedPKs, TableKey: "a.one", SelectedPKs: [][]any{{int64(1)}, {int64(2)}}},
			wantLen: 1,
			kind0:   FullStateJobSelectedPKs,
		},
		{
			name:    "selected pks width mismatch",
			opts:    PlanFullStateJobsOptions{Mode: PlanFullStateSelectedPKs, TableKey: "a.one", SelectedPKs: [][]any{{1, 2}}},
			wantErr: true,
			errSub:  "length",
		},
		{
			name:    "nil config",
			opts:    PlanFullStateJobsOptions{Mode: PlanFullStateAllTables},
			wantErr: true,
			errSub:  "config is nil",
		},
		{
			name:    "all tables empty list",
			opts:    PlanFullStateJobsOptions{Mode: PlanFullStateAllTables, DefaultChunkSize: 1},
			wantErr: true,
			errSub:  "no tables",
		},
		{
			name:    "default chunk required",
			opts:    PlanFullStateJobsOptions{Mode: PlanFullStateAllTables, DefaultChunkSize: 0},
			wantErr: true,
			errSub:  "DefaultChunkSize",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var c *FullStateCaptureConfig
			switch tt.name {
			case "nil config":
				c = nil
			case "all tables empty list":
				c = &FullStateCaptureConfig{Tables: nil}
			default:
				c = cfg
			}
			jobs, err := PlanFullStateJobs(c, tt.opts)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				if tt.errSub != "" && !strings.Contains(err.Error(), tt.errSub) {
					t.Fatalf("error %q should mention %q", err.Error(), tt.errSub)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(jobs) != tt.wantLen {
				t.Fatalf("len jobs = %d, want %d", len(jobs), tt.wantLen)
			}
			if len(jobs) > 0 && jobs[0].Kind != tt.kind0 {
				t.Fatalf("job[0].Kind = %v, want %v", jobs[0].Kind, tt.kind0)
			}
		})
	}
}

func TestFullStateJobQueue_FIFO(t *testing.T) {
	q := NewFullStateJobQueue()
	j1 := FullStateJob{Kind: FullStateJobChunkedTable, Spec: FullStateTableSpec{TableKey: "a.b", PKColumns: []string{"id"}, ChunkSize: 1}}
	j2 := FullStateJob{Kind: FullStateJobSelectedPKs, Spec: FullStateTableSpec{TableKey: "c.d", PKColumns: []string{"k"}, ChunkSize: 1}, SelectedPKs: [][]any{{1}}}
	q.Enqueue(j1, j2)
	if q.Len() != 2 {
		t.Fatalf("Len = %d", q.Len())
	}
	got, ok := q.TryDequeue()
	if !ok || got.Spec.TableKey != "a.b" {
		t.Fatalf("first dequeue: %+v ok=%v", got, ok)
	}
	got, ok = q.TryDequeue()
	if !ok || got.Kind != FullStateJobSelectedPKs {
		t.Fatalf("second dequeue: %+v ok=%v", got, ok)
	}
	if q.Len() != 0 {
		t.Fatalf("expected empty, Len=%d", q.Len())
	}
	_, ok = q.TryDequeue()
	if ok {
		t.Fatal("expected empty dequeue")
	}
	q.Clear()
	q.Enqueue(j1)
	if q.Len() != 1 {
		t.Fatalf("after clear+enqueue Len=%d", q.Len())
	}
}
