package tubing_cdc

import (
	"strings"
	"testing"
)

func TestTableIncludeRegex(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{name: "dem_test", input: "cdc_test.dem_test", want: `cdc_test\.dem_test`},
		{name: "simple names", input: "db.t", want: `db\.t`},
		{name: "table name contains dot", input: "a.b.c", want: `a\.b\.c`},
		{name: "no dot", input: "invalid", wantErr: true},
		{name: "empty db", input: ".t", wantErr: true},
		{name: "empty table", input: "db.", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tableIncludeRegex(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestNewTubingCDC_chunkProgressInvalidConfig(t *testing.T) {
	_, err := NewTubingCDC(&Configs{
		Address:                  "127.0.0.1:3306",
		ChunkProgressPersistence: &ChunkProgressPersistence{BadgerDir: ""},
	})
	if err == nil {
		t.Fatal("expected error for empty chunk BadgerDir")
	}
	if !strings.Contains(err.Error(), "BadgerDir") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewTubingCDC_positionPersistenceEmptyBadgerDir(t *testing.T) {
	_, err := NewTubingCDC(&Configs{
		Address:               "127.0.0.1:3306",
		PositionPersistence:   &PositionPersistence{BadgerDir: ""},
	})
	if err == nil {
		t.Fatal("expected error for empty position BadgerDir")
	}
	if !strings.Contains(err.Error(), "BadgerDir") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestTubingCDC_P4EnqueueAndAccessors(t *testing.T) {
	q := NewFullStateJobQueue()
	ctrl := NewChunkProcessingControl()
	cdc := &TubingCDC{fullStateQ: q, chunkControl: ctrl}
	if cdc.FullStateJobQueue() != q {
		t.Fatal("FullStateJobQueue accessor mismatch")
	}
	if cdc.ChunkProcessingControl() != ctrl {
		t.Fatal("ChunkProcessingControl accessor mismatch")
	}
	cfg := &FullStateCaptureConfig{
		Tables: []FullStateTableSpec{{TableKey: "db.t", PKColumns: []string{"id"}, ChunkSize: 5}},
	}
	n, err := cdc.EnqueueFullStateJobs(cfg, PlanFullStateJobsOptions{Mode: PlanFullStateAllTables})
	if err != nil || n != 1 {
		t.Fatalf("EnqueueFullStateJobs: n=%d err=%v", n, err)
	}
	j, ok := q.TryDequeue()
	if !ok || j.Spec.TableKey != "db.t" {
		t.Fatalf("dequeue: %+v ok=%v", j, ok)
	}
}

func TestTubingCDC_EnqueueFullStateJobsNoQueue(t *testing.T) {
	cdc := &TubingCDC{}
	_, err := cdc.EnqueueFullStateJobs(&FullStateCaptureConfig{
		Tables: []FullStateTableSpec{{TableKey: "db.t", PKColumns: []string{"id"}, ChunkSize: 1}},
	}, PlanFullStateJobsOptions{Mode: PlanFullStateAllTables})
	if err == nil {
		t.Fatal("expected error without queue")
	}
	if !strings.Contains(err.Error(), "FullStateJobQueue") {
		t.Fatalf("unexpected: %v", err)
	}
}

func TestTubingCDC_EnqueueFullStateJobs_nilReceiver(t *testing.T) {
	var cdc *TubingCDC
	_, err := cdc.EnqueueFullStateJobs(&FullStateCaptureConfig{
		Tables: []FullStateTableSpec{{TableKey: "db.t", PKColumns: []string{"id"}, ChunkSize: 1}},
	}, PlanFullStateJobsOptions{Mode: PlanFullStateAllTables})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "nil") {
		t.Fatalf("unexpected: %v", err)
	}
}
