package tubing_cdc

import (
	"errors"
	"strings"
	"testing"
)

func TestAlgorithm1Tracker_BeginCapture(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		target      string
		low, high   string
		pkCols      []string
		wantErr     bool
		errContains string
	}{
		{name: "ok", target: "app.users", low: "a", high: "b", pkCols: []string{"id"}},
		{name: "empty_target", target: "  ", low: "a", high: "b", pkCols: []string{"id"}, wantErr: true},
		{name: "bad_fqn", target: "nodb", low: "a", high: "b", pkCols: []string{"id"}, wantErr: true},
		{name: "same_uuid", target: "app.users", low: "x", high: "x", pkCols: []string{"id"}, wantErr: true},
		{name: "empty_pk_cols", target: "app.users", low: "a", high: "b", pkCols: nil, wantErr: true},
		{name: "blank_pk_col", target: "app.users", low: "a", high: "b", pkCols: []string{" "}, wantErr: true},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			x := NewAlgorithm1Tracker()
			err := x.BeginCapture(tt.target, tt.low, tt.high, tt.pkCols)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Fatalf("err %q should contain %q", err.Error(), tt.errContains)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestAlgorithm1Tracker_cycle_reconcile(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		rows       []map[string]any
		wantIDs    []int64
		recordRows []map[string]any
	}{
		{
			name: "drops_conflicting_pk",
			rows: []map[string]any{
				{"id": int64(1), "name": "a"},
				{"id": int64(2), "name": "b"},
			},
			recordRows: []map[string]any{{"id": int64(2), "name": "bx"}},
			wantIDs:    []int64{1},
		},
		{
			name: "no_conflicts",
			rows: []map[string]any{
				{"id": int64(10)},
			},
			wantIDs: []int64{10},
		},
		{
			name: "composite_pk",
			rows: []map[string]any{
				{"a": int64(1), "b": int64(2)},
				{"a": int64(3), "b": int64(4)},
			},
			recordRows: []map[string]any{{"a": int64(1), "b": int64(2)}},
			wantIDs:    []int64{3}, // second row's first pk component
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tr := NewAlgorithm1Tracker()
			var pkCols []string
			switch tt.name {
			case "composite_pk":
				pkCols = []string{"a", "b"}
			default:
				pkCols = []string{"id"}
			}
			if err := tr.BeginCapture("db.t", "low-u", "high-u", pkCols); err != nil {
				t.Fatal(err)
			}
			if err := tr.OnWatermark(WatermarkBinlogEvent{NewValue: "low-u"}); err != nil {
				t.Fatal(err)
			}
			if tr.Phase() != Algorithm1PhaseWindowOpen {
				t.Fatalf("phase=%v", tr.Phase())
			}
			for _, r := range tt.recordRows {
				tr.RecordTargetRowChange("db.t", r)
			}
			if err := tr.OnWatermark(WatermarkBinlogEvent{NewValue: "high-u"}); err != nil {
				t.Fatal(err)
			}
			if tr.Phase() != Algorithm1PhaseReady {
				t.Fatalf("phase=%v", tr.Phase())
			}
			out, err := tr.ReconcileChunkRows(tt.rows)
			if err != nil {
				t.Fatal(err)
			}
			if len(out) != len(tt.wantIDs) {
				t.Fatalf("len(out)=%d want %d", len(out), len(tt.wantIDs))
			}
			for i, id := range tt.wantIDs {
				var got int64
				if tt.name == "composite_pk" {
					got = out[i]["a"].(int64)
				} else {
					got = out[i]["id"].(int64)
				}
				if got != id {
					t.Fatalf("row %d: got %v want %v", i, got, id)
				}
			}
			if tr.Phase() != Algorithm1PhaseIdle {
				t.Fatalf("after reconcile phase=%v", tr.Phase())
			}
		})
	}
}

func TestAlgorithm1Tracker_reconcile_not_ready(t *testing.T) {
	t.Parallel()
	tr := NewAlgorithm1Tracker()
	_, err := tr.ReconcileChunkRows([]map[string]any{{"id": int64(1)}})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestAlgorithm1Tracker_double_begin(t *testing.T) {
	t.Parallel()
	tr := NewAlgorithm1Tracker()
	if err := tr.BeginCapture("db.t", "l", "h", []string{"id"}); err != nil {
		t.Fatal(err)
	}
	if err := tr.BeginCapture("db.t", "l2", "h2", []string{"id"}); err == nil {
		t.Fatal("expected error")
	}
}

func TestAlgorithm1Tracker_Reset(t *testing.T) {
	t.Parallel()
	tr := NewAlgorithm1Tracker()
	if err := tr.BeginCapture("db.t", "l", "h", []string{"id"}); err != nil {
		t.Fatal(err)
	}
	tr.Reset()
	if tr.Phase() != Algorithm1PhaseIdle {
		t.Fatalf("phase=%v", tr.Phase())
	}
	if err := tr.BeginCapture("db.t", "l2", "h2", []string{"id"}); err != nil {
		t.Fatal(err)
	}
}

func TestChainWatermarkNotifiers(t *testing.T) {
	t.Parallel()
	var a, b int
	chain := ChainWatermarkNotifiers(
		func(WatermarkBinlogEvent) error {
			a++
			return nil
		},
		func(WatermarkBinlogEvent) error {
			b++
			return nil
		},
	)
	if err := chain(WatermarkBinlogEvent{}); err != nil {
		t.Fatal(err)
	}
	if a != 1 || b != 1 {
		t.Fatalf("a=%d b=%d", a, b)
	}
	errChain := ChainWatermarkNotifiers(
		func(WatermarkBinlogEvent) error { return errors.New("stop") },
		func(WatermarkBinlogEvent) error {
			b++
			return nil
		},
	)
	if err := errChain(WatermarkBinlogEvent{}); err == nil {
		t.Fatal("expected error")
	}
}

func TestWatermarkUpdateValueSQL(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		cfg         *WatermarkTableConfig
		newVal      string
		wantErr     bool
		wantSubstrs []string
	}{
		{name: "nil", cfg: nil, wantErr: true},
		{
			name:   "defaults",
			cfg:    &WatermarkTableConfig{TableKey: "ctl._wm"},
			newVal: "uuid-1",
			wantSubstrs: []string{
				"UPDATE `ctl`.`_wm` SET `watermark_value` = ? WHERE `id` = 1",
			},
		},
		{
			name: "custom_cols",
			cfg: &WatermarkTableConfig{
				TableKey:         "a.b",
				ValueColumn:      "v",
				PrimaryKeyColumn: "pk",
			},
			newVal:      "x",
			wantSubstrs: []string{"UPDATE `a`.`b` SET `v` = ? WHERE `pk` = 1"},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			q, args, err := WatermarkUpdateValueSQL(tt.cfg, tt.newVal)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, s := range tt.wantSubstrs {
				if !strings.Contains(q, s) {
					t.Errorf("query %q missing %q", q, s)
				}
			}
			if len(args) != 1 || args[0] != tt.newVal {
				t.Fatalf("args=%v want [%q]", args, tt.newVal)
			}
		})
	}
}

func TestAlgorithm1Config_validate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		cfg     *Algorithm1Config
		wm      *WatermarkTableConfig
		wantErr bool
	}{
		{name: "nil", cfg: nil, wantErr: true},
		{name: "nil_tracker", cfg: &Algorithm1Config{TargetTableKey: "a.b"}, wantErr: true},
		{name: "empty_table", cfg: &Algorithm1Config{Tracker: NewAlgorithm1Tracker()}, wantErr: true},
		{name: "ok", cfg: &Algorithm1Config{Tracker: NewAlgorithm1Tracker(), TargetTableKey: "app.users"}},
		{
			name:    "same_as_watermark",
			cfg:     &Algorithm1Config{Tracker: NewAlgorithm1Tracker(), TargetTableKey: "db.wm"},
			wm:      &WatermarkTableConfig{TableKey: "db.wm"},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.cfg.validate(tt.wm)
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
