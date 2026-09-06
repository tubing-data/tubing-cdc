package tubing_cdc

import (
	"strings"
	"testing"
	"time"
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

func TestFullSyncConfigValidate(t *testing.T) {
	base := &Configs{
		Tables:                   []string{"db.one"},
		Watermark:                &WatermarkTableConfig{TableKey: "db.watermark"},
		ChunkProgressPersistence: &ChunkProgressPersistence{BadgerDir: t.TempDir()},
	}
	valid := &FullSyncConfig{
		Tables:  []FullStateTableSpec{{TableKey: "db.one", PKColumns: []string{"id"}, ChunkSize: 100}},
		RowSink: LoggerRowSink{},
	}
	if err := valid.validate(base); err != nil {
		t.Fatalf("valid config: %v", err)
	}

	tests := []struct {
		name string
		cfg  Configs
		fs   FullSyncConfig
		want string
	}{
		{name: "watermark required", cfg: Configs{ChunkProgressPersistence: base.ChunkProgressPersistence}, fs: *valid, want: "Watermark"},
		{name: "progress required", cfg: Configs{Watermark: base.Watermark}, fs: *valid, want: "ChunkProgressPersistence"},
		{name: "sink required", cfg: *base, fs: FullSyncConfig{Tables: valid.Tables}, want: "RowSink"},
		{name: "tables required", cfg: *base, fs: FullSyncConfig{RowSink: LoggerRowSink{}}, want: "no tables"},
		{name: "duplicate", cfg: *base, fs: FullSyncConfig{RowSink: LoggerRowSink{}, Tables: append(valid.Tables, valid.Tables[0])}, want: "duplicate"},
		{name: "table not replicated", cfg: *base, fs: FullSyncConfig{RowSink: LoggerRowSink{}, Tables: []FullStateTableSpec{{TableKey: "db.two", PKColumns: []string{"id"}, ChunkSize: 1}}}, want: "Configs.Tables"},
		{name: "low level conflict", cfg: Configs{Watermark: base.Watermark, ChunkProgressPersistence: base.ChunkProgressPersistence, Algorithm1: &Algorithm1Config{}}, fs: *valid, want: "cannot be combined"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.fs.validate(&tt.cfg); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("got %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestFullSyncDone(t *testing.T) {
	var nilCDC *TubingCDC
	if nilCDC.FullSyncDone() != nil {
		t.Fatal("nil receiver should have no completion channel")
	}
	done := make(chan error, 1)
	cdc := &TubingCDC{fullSync: &fullSyncRuntime{done: done}}
	if cdc.FullSyncDone() != done {
		t.Fatal("completion channel mismatch")
	}
	select {
	case <-done:
		t.Fatal("completion channel should not be ready")
	case <-time.After(time.Millisecond):
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
		Address:             "127.0.0.1:3306",
		PositionPersistence: &PositionPersistence{BadgerDir: ""},
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
