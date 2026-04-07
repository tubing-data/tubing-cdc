package tubing_cdc

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
)

// Algorithm1Tracker implements the DBLog paper's watermark-window PK reconciliation for one
// chunk cycle: after a low watermark appears in the binlog, it records primary keys touched on
// the snapshot target table until the high watermark appears; ReconcileChunkRows then drops
// chunk rows whose PK was observed in that window (log wins).
//
// Drive a cycle with BeginCapture, then UPDATE the watermark row to LowUUID (via
// WatermarkUpdateValueSQL + canal.Execute or another client). When the binlog delivers that
// value, the window opens. After reading the PK chunk from the database, UPDATE to HighUUID;
// when HighUUID appears, call ReconcileChunkRows.
//
// Wire OnWatermark into Configs.WatermarkNotifier (e.g. ChainWatermarkNotifiers(tracker.OnWatermark, ...))
// and wrap the event handler with wrapHandlerWithAlgorithm1 so row events update the tracker.
type Algorithm1Tracker struct {
	mu sync.Mutex

	targetTable string
	lowUUID     string
	highUUID    string
	pkColumns   []string

	phase algorithm1Phase
	// pkSeen keys are stable JSON-tuple encodings (see pkTupleKey).
	pkSeen map[string]struct{}
}

type algorithm1Phase int

const (
	algorithm1Idle algorithm1Phase = iota
	algorithm1AwaitLow
	algorithm1WindowOpen
	algorithm1Ready
)

// NewAlgorithm1Tracker returns an idle tracker.
func NewAlgorithm1Tracker() *Algorithm1Tracker {
	return &Algorithm1Tracker{}
}

// BeginCapture arms a new cycle. TargetTableKey must be fully qualified "database.table".
// LowUUID and HighUUID must be distinct non-empty strings (e.g. fresh UUIDs per cycle).
// pkColumnNames lists primary key columns in the same ascending order used for PK-ordered chunking
// (see BuildPKOrderedChunkSelect); encoding for log PKs and chunk rows must match.
func (t *Algorithm1Tracker) BeginCapture(targetTableKey, lowUUID, highUUID string, pkColumnNames []string) error {
	if t == nil {
		return fmt.Errorf("algorithm1: tracker is nil")
	}
	targetTableKey = strings.TrimSpace(targetTableKey)
	lowUUID = strings.TrimSpace(lowUUID)
	highUUID = strings.TrimSpace(highUUID)
	if targetTableKey == "" {
		return fmt.Errorf("algorithm1: target table is empty")
	}
	if _, err := ParseTableIdentity(targetTableKey); err != nil {
		return fmt.Errorf("algorithm1: %w", err)
	}
	if lowUUID == "" || highUUID == "" {
		return fmt.Errorf("algorithm1: low and high UUID must be non-empty")
	}
	if lowUUID == highUUID {
		return fmt.Errorf("algorithm1: low and high UUID must differ")
	}
	if len(pkColumnNames) == 0 {
		return fmt.Errorf("algorithm1: pkColumnNames is empty")
	}
	pkCopy := make([]string, len(pkColumnNames))
	for i, c := range pkColumnNames {
		c = strings.TrimSpace(c)
		if c == "" {
			return fmt.Errorf("algorithm1: empty pk column name")
		}
		pkCopy[i] = c
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.phase != algorithm1Idle {
		return fmt.Errorf("algorithm1: previous cycle not finished (call ReconcileChunkRows or reset)")
	}
	t.targetTable = targetTableKey
	t.lowUUID = lowUUID
	t.highUUID = highUUID
	t.pkColumns = pkCopy
	t.pkSeen = make(map[string]struct{})
	t.phase = algorithm1AwaitLow
	return nil
}

// OnWatermark matches watermark binlog events to the armed low/high UUIDs. Safe to use as
// WatermarkNotifier or inside a chain.
func (t *Algorithm1Tracker) OnWatermark(ev WatermarkBinlogEvent) error {
	if t == nil {
		return nil
	}
	val := strings.TrimSpace(ev.NewValue)
	if val == "" {
		return nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	switch t.phase {
	case algorithm1AwaitLow:
		if val == t.lowUUID {
			t.phase = algorithm1WindowOpen
		}
	case algorithm1WindowOpen:
		if val == t.highUUID {
			t.phase = algorithm1Ready
		}
	case algorithm1Ready, algorithm1Idle:
		// ignore stray watermark events
	}
	return nil
}

// RecordTargetRowChange records one primary key tuple if the window is open and tableKey matches
// the capture target. row must contain JSON keys produced from column names via JSONFieldNameForColumn
// (same shape as rowMapFromCanalRow + DynamicTableEventHandler).
func (t *Algorithm1Tracker) RecordTargetRowChange(tableKey string, row map[string]any) {
	if t == nil || len(row) == 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.phase != algorithm1WindowOpen {
		return
	}
	if strings.TrimSpace(tableKey) != t.targetTable {
		return
	}
	if len(t.pkColumns) == 0 {
		return
	}
	key, err := pkTupleKey(t.pkColumns, row)
	if err != nil {
		return
	}
	t.pkSeen[key] = struct{}{}
}

// Phase returns the current lifecycle phase (for tests and operators).
func (t *Algorithm1Tracker) Phase() Algorithm1Phase {
	if t == nil {
		return Algorithm1PhaseIdle
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return Algorithm1Phase(t.phase)
}

// Algorithm1Phase is the exported view of algorithm1Phase.
type Algorithm1Phase int

const (
	Algorithm1PhaseIdle Algorithm1Phase = iota
	Algorithm1PhaseAwaitLow
	Algorithm1PhaseWindowOpen
	Algorithm1PhaseReady
)

// ReconcileChunkRows returns chunk rows whose PK was not observed in the log between low and high
// watermarks, in input order, then resets the tracker to idle. Row maps must use the same PK JSON
// keys as binlog-derived rows (JSONFieldNameForColumn per BeginCapture pk column list).
func (t *Algorithm1Tracker) ReconcileChunkRows(rows []map[string]any) ([]map[string]any, error) {
	if t == nil {
		return nil, fmt.Errorf("algorithm1: tracker is nil")
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.phase != algorithm1Ready {
		return nil, fmt.Errorf("algorithm1: not ready for reconcile (phase=%v)", Algorithm1Phase(t.phase))
	}
	if len(t.pkColumns) == 0 {
		return nil, fmt.Errorf("algorithm1: pk columns not set")
	}

	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		if row == nil {
			continue
		}
		k, err := pkTupleKey(t.pkColumns, row)
		if err != nil {
			return nil, err
		}
		if _, conflict := t.pkSeen[k]; conflict {
			continue
		}
		out = append(out, row)
	}

	t.phase = algorithm1Idle
	t.targetTable = ""
	t.lowUUID = ""
	t.highUUID = ""
	t.pkColumns = nil
	t.pkSeen = nil
	return out, nil
}

// Reset forcibly returns the tracker to idle (drops partial state).
func (t *Algorithm1Tracker) Reset() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.phase = algorithm1Idle
	t.targetTable = ""
	t.lowUUID = ""
	t.highUUID = ""
	t.pkColumns = nil
	t.pkSeen = nil
}

// ChainWatermarkNotifiers runs notifiers in order; the first error stops the chain.
func ChainWatermarkNotifiers(fns ...WatermarkNotifier) WatermarkNotifier {
	return func(ev WatermarkBinlogEvent) error {
		for _, f := range fns {
			if f == nil {
				continue
			}
			if err := f(ev); err != nil {
				return err
			}
		}
		return nil
	}
}

// WatermarkUpdateValueSQL returns a parameterized UPDATE that sets the watermark value column on
// the singleton row (primary key = 1), matching WatermarkCreateTableSQL layout.
func WatermarkUpdateValueSQL(cfg *WatermarkTableConfig, newValue string) (string, []any, error) {
	if cfg == nil {
		return "", nil, fmt.Errorf("watermark: config is nil")
	}
	if err := cfg.Validate(); err != nil {
		return "", nil, err
	}
	wc := cfg.withDefaults()
	id, err := ParseTableIdentity(strings.TrimSpace(cfg.TableKey))
	if err != nil {
		return "", nil, err
	}
	pk := quoteMySQLIdent(wc.PrimaryKeyColumn)
	valCol := quoteMySQLIdent(wc.ValueColumn)
	db := quoteMySQLIdent(id.Database)
	tbl := quoteMySQLIdent(id.Table)
	q := fmt.Sprintf(
		"UPDATE %s.%s SET %s = ? WHERE %s = 1",
		db, tbl, valCol, pk,
	)
	return q, []any{newValue}, nil
}

func pkTupleKey(pkColumnNames []string, row map[string]any) (string, error) {
	vals := make([]any, 0, len(pkColumnNames))
	for _, col := range pkColumnNames {
		jname := JSONFieldNameForColumn(strings.TrimSpace(col))
		v, ok := row[jname]
		if !ok {
			return "", fmt.Errorf("algorithm1: row missing pk field %q (json key %q)", col, jname)
		}
		vals = append(vals, v)
	}
	b, err := json.Marshal(vals)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
