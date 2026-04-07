package tubing_cdc

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestAlgorithm1ChunkDriverConfig_validate(t *testing.T) {
	wm := &WatermarkTableConfig{TableKey: "wm.db"}
	tests := []struct {
		name    string
		cfg     Algorithm1ChunkDriverConfig
		wantErr bool
		errSub  string
	}{
		{
			name: "ok",
			cfg: Algorithm1ChunkDriverConfig{
				Watermark: wm, Tracker: NewAlgorithm1Tracker(),
				TargetTableKey: "app.data", RowSink: LoggerRowSink{}, JobQueue: NewFullStateJobQueue(),
			},
			wantErr: false,
		},
		{
			name:    "nil watermark",
			cfg:     Algorithm1ChunkDriverConfig{Tracker: NewAlgorithm1Tracker(), TargetTableKey: "a.b", RowSink: LoggerRowSink{}, JobQueue: NewFullStateJobQueue()},
			wantErr: true,
			errSub:  "Watermark",
		},
		{
			name:    "nil tracker",
			cfg:     Algorithm1ChunkDriverConfig{Watermark: wm, TargetTableKey: "a.b", RowSink: LoggerRowSink{}, JobQueue: NewFullStateJobQueue()},
			wantErr: true,
			errSub:  "Tracker",
		},
		{
			name:    "nil sink",
			cfg:     Algorithm1ChunkDriverConfig{Watermark: wm, Tracker: NewAlgorithm1Tracker(), TargetTableKey: "a.b", JobQueue: NewFullStateJobQueue()},
			wantErr: true,
			errSub:  "RowSink",
		},
		{
			name:    "nil queue",
			cfg:     Algorithm1ChunkDriverConfig{Watermark: wm, Tracker: NewAlgorithm1Tracker(), TargetTableKey: "a.b", RowSink: LoggerRowSink{}},
			wantErr: true,
			errSub:  "JobQueue",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.validate()
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

func TestWaitAlgorithm1Phase(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(*Algorithm1Tracker)
		want    Algorithm1Phase
		wantErr bool
		timeout time.Duration
	}{
		{
			name: "timeout awaiting window",
			setup: func(tr *Algorithm1Tracker) {
				_ = tr.BeginCapture("db.t", "low", "high", []string{"id"})
			},
			want:    Algorithm1PhaseWindowOpen,
			wantErr: true,
			timeout: 30 * time.Millisecond,
		},
		{
			name: "opens after notifier",
			setup: func(tr *Algorithm1Tracker) {
				_ = tr.BeginCapture("db.t", "low", "high", []string{"id"})
				go func() {
					time.Sleep(15 * time.Millisecond)
					_ = tr.OnWatermark(WatermarkBinlogEvent{NewValue: "low"})
				}()
			},
			want:    Algorithm1PhaseWindowOpen,
			wantErr: false,
			timeout: time.Second,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := NewAlgorithm1Tracker()
			tt.setup(tr)
			ctx := context.Background()
			err := waitAlgorithm1Phase(ctx, tr, tt.want, tt.timeout)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPkTupleFromRowMap(t *testing.T) {
	tests := []struct {
		name    string
		pk      []string
		row     map[string]any
		want    []any
		wantErr bool
	}{
		{
			name: "ok",
			pk:   []string{"id"},
			row:  map[string]any{"id": int64(1)},
			want: []any{int64(1)},
		},
		{
			name:    "missing",
			pk:      []string{"id"},
			row:     map[string]any{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := pkTupleFromRowMap(tt.pk, tt.row)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("got %v want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("[%d] got %v want %v", i, got[i], tt.want[i])
				}
			}
		})
	}
}

type sliceSink struct {
	payloads [][]byte
}

func (s *sliceSink) Emit(tableKey, action string, payloadJSON []byte) error {
	s.payloads = append(s.payloads, append([]byte(nil), payloadJSON...))
	return nil
}

func TestEmitSnapshotRow(t *testing.T) {
	tests := []struct {
		name        string
		useEnvelope bool
		checkSubstr string
	}{
		{name: "raw json", useEnvelope: false, checkSubstr: `"id":1`},
		{name: "envelope", useEnvelope: true, checkSubstr: `"origin":"snapshot"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var s sliceSink
			err := emitSnapshotRow(&s, "db.t", tt.useEnvelope, map[string]any{"id": float64(1)})
			if err != nil {
				t.Fatal(err)
			}
			if len(s.payloads) != 1 {
				t.Fatalf("payloads len %d", len(s.payloads))
			}
			if !strings.Contains(string(s.payloads[0]), tt.checkSubstr) {
				t.Fatalf("payload %s missing %q", s.payloads[0], tt.checkSubstr)
			}
		})
	}
}

func TestEmitSnapshotRow_envelopePrimaryKeyContract(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		tableKey       string
		row            map[string]any
		wantPKLen      int
		wantDatabase   string
		wantTable      string
		wantOriginSnap bool
	}{
		{
			name:           "nil_schema_snapshot_has_empty_primary_key_object",
			tableKey:       "app.users",
			row:            map[string]any{"id": float64(42), "name": "x"},
			wantPKLen:      0,
			wantDatabase:   "app",
			wantTable:      "users",
			wantOriginSnap: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var s sliceSink
			if err := emitSnapshotRow(&s, tt.tableKey, true, tt.row); err != nil {
				t.Fatal(err)
			}
			if len(s.payloads) != 1 {
				t.Fatalf("got %d payloads", len(s.payloads))
			}
			var env struct {
				Origin     string          `json:"origin"`
				Table      map[string]any  `json:"table"`
				PrimaryKey map[string]any  `json:"primary_key"`
				SchemaVer  string          `json:"schema_version"`
				Action     string          `json:"action"`
				Payload    json.RawMessage `json:"payload"`
			}
			if err := json.Unmarshal(s.payloads[0], &env); err != nil {
				t.Fatal(err)
			}
			if tt.wantOriginSnap && env.Origin != "snapshot" {
				t.Fatalf("origin=%q", env.Origin)
			}
			if env.Table["database"] != tt.wantDatabase || env.Table["table"] != tt.wantTable {
				t.Fatalf("table=%v", env.Table)
			}
			if len(env.PrimaryKey) != tt.wantPKLen {
				t.Fatalf("primary_key len=%d want %d (contract: no schema.Table in driver path)", len(env.PrimaryKey), tt.wantPKLen)
			}
		})
	}
}

func TestAlgorithm1DriverErrorPolicy_documentation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		p        Algorithm1DriverErrorPolicy
		wantZero bool
	}{
		{name: "stop_on_error_default_zero", p: Algorithm1DriverStopOnError, wantZero: true},
		{name: "continue_on_error_nonzero", p: Algorithm1DriverContinueOnError, wantZero: false},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if tt.wantZero && tt.p != 0 {
				t.Fatalf("want zero default, got %d", tt.p)
			}
			if !tt.wantZero && tt.p == 0 {
				t.Fatal("want non-zero policy value")
			}
		})
	}
	if Algorithm1DriverContinueOnError == Algorithm1DriverStopOnError {
		t.Fatal("policies must differ")
	}
}

func TestTubingCDC_StartAlgorithm1ChunkDriver_nilCanal(t *testing.T) {
	wm := &WatermarkTableConfig{TableKey: "w.t"}
	cfg := Algorithm1ChunkDriverConfig{
		Watermark: wm, Tracker: NewAlgorithm1Tracker(),
		TargetTableKey: "a.b", RowSink: LoggerRowSink{}, JobQueue: NewFullStateJobQueue(),
	}
	cdc := &TubingCDC{}
	err := cdc.StartAlgorithm1ChunkDriver(context.Background(), cfg)
	if err == nil || !strings.Contains(err.Error(), "nil") {
		t.Fatalf("got %v", err)
	}
}
