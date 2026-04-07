package tubing_cdc_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	tubingcdc "tubing-cdc"
)

func mySQLResultToRowMaps(t *testing.T, rr *mysql.Result) []map[string]any {
	t.Helper()
	if rr == nil || rr.Resultset == nil {
		return nil
	}
	defer rr.Close()
	rs := rr.Resultset
	n := rs.RowNumber()
	out := make([]map[string]any, 0, n)
	for i := 0; i < n; i++ {
		m := make(map[string]any, len(rs.Fields))
		for j, f := range rs.Fields {
			v, err := rs.GetValue(i, j)
			if err != nil {
				t.Fatal(err)
			}
			m[tubingcdc.JSONFieldNameForColumn(string(f.Name))] = v
		}
		out = append(out, m)
	}
	return out
}

func waitTrackerPhase(t *testing.T, tr *tubingcdc.Algorithm1Tracker, want tubingcdc.Algorithm1Phase, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-deadline:
			t.Fatalf("timeout waiting phase %v, got %v", want, tr.Phase())
		case <-tick.C:
			if tr.Phase() == want {
				return
			}
		}
	}
}

func setupDBLogSchema(t *testing.T, adminDB *sql.DB) (
	wm *tubingcdc.WatermarkTableConfig,
) {
	t.Helper()
	wm = &tubingcdc.WatermarkTableConfig{TableKey: "cdc_test._dblog_wm"}
	createSQL, insertSQL, err := tubingcdc.WatermarkCreateTableSQL(wm)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := adminDB.Exec(createSQL); err != nil {
		t.Fatalf("create watermark table: %v", err)
	}
	if _, err := adminDB.Exec(insertSQL); err != nil {
		t.Fatalf("seed watermark: %v", err)
	}
	if _, err := adminDB.Exec(`CREATE TABLE IF NOT EXISTS cdc_test.algo_target (
  id BIGINT NOT NULL PRIMARY KEY,
  val VARCHAR(64) NOT NULL
) ENGINE=InnoDB`); err != nil {
		t.Fatalf("create algo_target: %v", err)
	}
	return wm
}

func TestIntegration_watermark_notifier(t *testing.T) {
	tcpAddr, adminDB := mysqlIntegrationTCP(t)
	ctx := context.Background()
	wm := setupDBLogSchema(t, adminDB)

	var mu sync.Mutex
	var seen []string
	notifier := tubingcdc.WatermarkNotifier(func(ev tubingcdc.WatermarkBinlogEvent) error {
		mu.Lock()
		seen = append(seen, ev.NewValue)
		mu.Unlock()
		return nil
	})

	pos := masterPosition(t, adminDB)
	cfg := &tubingcdc.Configs{
		Address:           tcpAddr,
		Username:          "cdc",
		Password:          "cdc_pass",
		Tables:            nil,
		Watermark:         wm,
		WatermarkNotifier: notifier,
	}
	cdc, err := tubingcdc.NewTubingCDC(cfg)
	if err != nil {
		t.Fatalf("NewTubingCDC: %v", err)
	}
	t.Cleanup(func() { cdc.Close() })

	go func() { _ = cdc.RunFrom(pos) }()

	newVal := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	waitCondition(t, 30*time.Second, func() bool {
		_, err := adminDB.ExecContext(ctx,
			`UPDATE cdc_test._dblog_wm SET watermark_value = ? WHERE id = 1`, newVal)
		return err == nil
	})
	// Replication may lag slightly after container start.
	waitCondition(t, 45*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, v := range seen {
			if v == newVal {
				return true
			}
		}
		return false
	})
}

func TestIntegration_algorithm1_manual_cycle(t *testing.T) {
	tests := []struct {
		name      string
		doUpdate  bool
		wantCount int
	}{
		{name: "update_during_window_drops_pk2", doUpdate: true, wantCount: 1},
		{name: "no_update_emits_both", doUpdate: false, wantCount: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tcpAddr, adminDB := mysqlIntegrationTCP(t)
			ctx := context.Background()
			wm := setupDBLogSchema(t, adminDB)

			if _, err := adminDB.Exec(`DELETE FROM cdc_test.algo_target`); err != nil {
				t.Fatal(err)
			}
			if _, err := adminDB.Exec(`INSERT INTO cdc_test.algo_target (id, val) VALUES (1, 'a'), (2, 'b')`); err != nil {
				t.Fatal(err)
			}

			tr := tubingcdc.NewAlgorithm1Tracker()
			cfg := &tubingcdc.Configs{
				Address:           tcpAddr,
				Username:          "cdc",
				Password:          "cdc_pass",
				Tables:            []string{"cdc_test.algo_target"},
				Watermark:         wm,
				WatermarkNotifier: tubingcdc.ChainWatermarkNotifiers(tr.OnWatermark),
				Algorithm1: &tubingcdc.Algorithm1Config{
					Tracker:        tr,
					TargetTableKey: "cdc_test.algo_target",
				},
			}
			cdc, err := tubingcdc.NewTubingCDC(cfg)
			if err != nil {
				t.Fatalf("NewTubingCDC: %v", err)
			}
			t.Cleanup(func() { cdc.Close() })

			pos := masterPosition(t, adminDB)
			go func() { _ = cdc.RunFrom(pos) }()
			time.Sleep(2 * time.Second)

			low := "11111111-1111-1111-1111-111111111111"
			high := "22222222-2222-2222-2222-222222222222"
			if err := tr.BeginCapture("cdc_test.algo_target", low, high, []string{"id"}); err != nil {
				t.Fatal(err)
			}

			qLow, argsLow, err := tubingcdc.WatermarkUpdateValueSQL(wm, low)
			if err != nil {
				t.Fatal(err)
			}
			if rr, err := cdc.Canal().Execute(qLow, argsLow...); err != nil {
				t.Fatalf("watermark low: %v", err)
			} else if rr != nil {
				rr.Close()
			}

			waitTrackerPhase(t, tr, tubingcdc.Algorithm1PhaseWindowOpen, 45*time.Second)

			if tt.doUpdate {
				if _, err := adminDB.ExecContext(ctx, `UPDATE cdc_test.algo_target SET val = 'win' WHERE id = 2`); err != nil {
					t.Fatal(err)
				}
				time.Sleep(800 * time.Millisecond)
			}

			sel, selArgs, err := tubingcdc.BuildPKRowsInSelect("cdc_test", "algo_target", []string{"id"}, [][]any{{int64(1)}, {int64(2)}})
			if err != nil {
				t.Fatal(err)
			}
			selRR, err := cdc.Canal().Execute(sel, selArgs...)
			if err != nil {
				t.Fatalf("select: %v", err)
			}
			rows := mySQLResultToRowMaps(t, selRR)

			qHigh, argsHigh, err := tubingcdc.WatermarkUpdateValueSQL(wm, high)
			if err != nil {
				t.Fatal(err)
			}
			if rr, err := cdc.Canal().Execute(qHigh, argsHigh...); err != nil {
				t.Fatalf("watermark high: %v", err)
			} else if rr != nil {
				rr.Close()
			}

			waitTrackerPhase(t, tr, tubingcdc.Algorithm1PhaseReady, 45*time.Second)

			out, err := tr.ReconcileChunkRows(rows)
			if err != nil {
				t.Fatal(err)
			}
			if len(out) != tt.wantCount {
				t.Fatalf("reconciled rows len=%d want %d", len(out), tt.wantCount)
			}
			ids := make(map[int64]struct{})
			for _, r := range out {
				id, ok := r["id"].(int64)
				if !ok {
					if u, ok := r["id"].(uint64); ok {
						id = int64(u)
					} else {
						t.Fatalf("unexpected id type %T", r["id"])
					}
				}
				ids[id] = struct{}{}
			}
			if _, ok := ids[1]; !ok {
				t.Fatalf("expected id 1 in output, got %v", ids)
			}
			if tt.doUpdate {
				if _, ok := ids[2]; ok {
					t.Fatal("expected id 2 dropped")
				}
			}
		})
	}
}

type countingSnapshotSink struct {
	mu   sync.Mutex
	rows []map[string]any
}

func (s *countingSnapshotSink) Emit(tableKey, action string, payloadJSON []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var m map[string]any
	if err := json.Unmarshal(payloadJSON, &m); err != nil {
		return err
	}
	s.rows = append(s.rows, m)
	return nil
}

func (s *countingSnapshotSink) snapshotIDs() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]int64, 0, len(s.rows))
	for _, m := range s.rows {
		id, ok := asInt64(m["id"])
		if ok {
			out = append(out, id)
		}
	}
	return out
}

func asInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case uint64:
		return int64(x), true
	case float64:
		return int64(x), true
	case json.Number:
		i, err := x.Int64()
		return i, err == nil
	default:
		return 0, false
	}
}

// TestIntegration_chunk_progress_persisted_after_close verifies that after one chunk iteration
// completes (chunk_size=2, two snapshot emits), ChunkProgressStore writes a cursor readable
// before Close and via ReadChunkProgressFromBadger after Close.
func TestIntegration_chunk_progress_persisted_after_close(t *testing.T) {
	tcpAddr, adminDB := mysqlIntegrationTCP(t)
	ctx := context.Background()
	wm := setupDBLogSchema(t, adminDB)

	if _, err := adminDB.Exec(`DELETE FROM cdc_test.algo_target`); err != nil {
		t.Fatal(err)
	}
	for _, id := range []int64{1, 2} {
		if _, err := adminDB.ExecContext(ctx, `INSERT INTO cdc_test.algo_target (id, val) VALUES (?, ?)`, id, fmt.Sprintf("v%d", id)); err != nil {
			t.Fatal(err)
		}
	}

	badgerDir := t.TempDir()
	jobQueue := tubingcdc.NewFullStateJobQueue()
	runID := "integration-restart-1"
	spec := tubingcdc.FullStateTableSpec{
		TableKey:  "cdc_test.algo_target",
		PKColumns: []string{"id"},
		// Table has 2 rows; chunk_size 3 => one chunk, then driver returns (len(raw) < chunkSize) without Delete.
		ChunkSize: 3,
		RunID:     runID,
	}

	buildCfg := func(tr *tubingcdc.Algorithm1Tracker) *tubingcdc.Configs {
		return &tubingcdc.Configs{
			Address:           tcpAddr,
			Username:          "cdc",
			Password:          "cdc_pass",
			Tables:            []string{"cdc_test.algo_target"},
			Watermark:         wm,
			WatermarkNotifier: tubingcdc.ChainWatermarkNotifiers(tr.OnWatermark),
			Algorithm1: &tubingcdc.Algorithm1Config{
				Tracker:        tr,
				TargetTableKey: "cdc_test.algo_target",
			},
			PositionPersistence:      &tubingcdc.PositionPersistence{BadgerDir: badgerDir},
			ChunkProgressPersistence: &tubingcdc.ChunkProgressPersistence{BadgerDir: badgerDir},
			FullStateJobQueue:        jobQueue,
		}
	}

	sink1 := &countingSnapshotSink{}
	tr1 := tubingcdc.NewAlgorithm1Tracker()
	cfg1 := buildCfg(tr1)
	cdc1, err := tubingcdc.NewTubingCDC(cfg1)
	if err != nil {
		t.Fatalf("NewTubingCDC: %v", err)
	}

	pos0 := masterPosition(t, adminDB)
	go func() { _ = cdc1.RunFrom(pos0) }()
	time.Sleep(2 * time.Second)

	drvCfg := tubingcdc.Algorithm1ChunkDriverConfig{
		Watermark:        wm,
		Tracker:          tr1,
		TargetTableKey:   "cdc_test.algo_target",
		RowSink:          sink1,
		JobQueue:         jobQueue,
		ChunkStore:       cdc1.ChunkProgressStore(),
		PhaseWaitTimeout: 45 * time.Second,
		ErrorPolicy:      tubingcdc.Algorithm1DriverStopOnError,
	}
	if err := cdc1.StartAlgorithm1ChunkDriver(ctx, drvCfg); err != nil {
		t.Fatalf("StartAlgorithm1ChunkDriver: %v", err)
	}

	jobQueue.Enqueue(tubingcdc.FullStateJob{Kind: tubingcdc.FullStateJobChunkedTable, Spec: spec})

	waitCondition(t, 90*time.Second, func() bool {
		return len(sink1.snapshotIDs()) >= 2
	})
	// Read cursor before the next chunk iteration mutates it (avoid racing the second watermark cycle).
	rec, err := cdc1.ChunkProgressStore().Get("cdc_test.algo_target", runID)
	if err != nil {
		t.Fatalf("ChunkProgressStore.Get: %v", err)
	}
	cdc1.Close()

	recDisk, err := tubingcdc.ReadChunkProgressFromBadger(badgerDir, "", "cdc_test.algo_target", runID)
	if err != nil {
		t.Fatalf("ReadChunkProgressFromBadger: %v", err)
	}
	if len(recDisk.AfterPK) == 0 {
		t.Fatalf("expected non-empty AfterPK on disk, got %+v", recDisk)
	}
	if len(rec.AfterPK) == 0 {
		t.Fatalf("expected non-empty AfterPK in live store, got %+v", rec)
	}
	id, ok := asInt64(rec.AfterPK[0])
	if !ok {
		t.Fatalf("unexpected AfterPK[0] type %T", rec.AfterPK[0])
	}
	if id != 2 {
		t.Fatalf("AfterPK id=%d want 2 (last row of first chunk)", id)
	}
	id2, ok := asInt64(recDisk.AfterPK[0])
	if !ok {
		t.Fatalf("unexpected disk AfterPK[0] type %T", recDisk.AfterPK[0])
	}
	if id2 != id {
		t.Fatalf("disk vs live AfterPK: %v vs %v", id2, id)
	}
	if len(sink1.snapshotIDs()) != 2 {
		t.Fatalf("first chunk should emit 2 rows, got %v", sink1.snapshotIDs())
	}
}
