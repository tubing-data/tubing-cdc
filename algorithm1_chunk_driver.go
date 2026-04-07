package tubing_cdc

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/google/uuid"
	"github.com/siddontang/go-log/log"
)

// Algorithm1DriverErrorPolicy controls whether the chunk driver keeps dequeuing after a failed job/chunk.
type Algorithm1DriverErrorPolicy int

const (
	// Algorithm1DriverStopOnError stops the driver goroutine and cancels the run context.
	Algorithm1DriverStopOnError Algorithm1DriverErrorPolicy = iota
	// Algorithm1DriverContinueOnError logs, resets the tracker if needed, and proceeds.
	Algorithm1DriverContinueOnError
)

// Algorithm1ChunkDriverConfig wires watermark SQL, tracker, sink, and work queue for the built-in driver.
// Requirements: Watermark and Tracker must match Configs (WatermarkNotifier must invoke Tracker.OnWatermark);
// TargetTableKey must equal Configs.Algorithm1.TargetTableKey and every FullStateJob.Spec.TableKey you enqueue.
type Algorithm1ChunkDriverConfig struct {
	Watermark *WatermarkTableConfig
	Tracker   *Algorithm1Tracker
	// TargetTableKey is the fully qualified table captured by Algorithm1 (same as Algorithm1Config.TargetTableKey).
	TargetTableKey string
	RowSink        RowEventSink
	// UseEnvelope wraps snapshot payloads like DynamicTableEventHandler WithDBLogEnvelope (origin=snapshot).
	UseEnvelope bool
	JobQueue    *FullStateJobQueue
	// ChunkStore is required for FullStateJobChunkedTable jobs; optional for FullStateJobSelectedPKs-only runs.
	ChunkStore *ChunkProgressStore
	// Control is optional; when set, should be the same instance as Configs.ChunkProcessingControl for Pause/Resume.
	Control *ChunkProcessingControl
	// PhaseWaitTimeout bounds how long we poll tracker phase after each watermark Execute. Zero defaults to 60s.
	PhaseWaitTimeout time.Duration
	ErrorPolicy      Algorithm1DriverErrorPolicy
}

func (c Algorithm1ChunkDriverConfig) validate() error {
	if c.Watermark == nil {
		return fmt.Errorf("algorithm1 driver: Watermark is nil")
	}
	if err := c.Watermark.Validate(); err != nil {
		return fmt.Errorf("algorithm1 driver: watermark: %w", err)
	}
	if c.Tracker == nil {
		return fmt.Errorf("algorithm1 driver: Tracker is nil")
	}
	tk := strings.TrimSpace(c.TargetTableKey)
	if tk == "" {
		return fmt.Errorf("algorithm1 driver: TargetTableKey is empty")
	}
	if _, err := ParseTableIdentity(tk); err != nil {
		return fmt.Errorf("algorithm1 driver: %w", err)
	}
	if c.RowSink == nil {
		return fmt.Errorf("algorithm1 driver: RowSink is nil")
	}
	if c.JobQueue == nil {
		return fmt.Errorf("algorithm1 driver: JobQueue is nil")
	}
	return nil
}

func (c Algorithm1ChunkDriverConfig) phaseWait() time.Duration {
	if c.PhaseWaitTimeout <= 0 {
		return 60 * time.Second
	}
	return c.PhaseWaitTimeout
}

type algorithm1SnapshotCycleResult struct {
	Raw        []map[string]any
	Reconciled []map[string]any
}

type algorithm1ChunkDriver struct {
	cfg  Algorithm1ChunkDriverConfig
	cnl  *canal.Canal
	stop context.CancelFunc
}

func newAlgorithm1ChunkDriver(cnl *canal.Canal, cfg Algorithm1ChunkDriverConfig, stop context.CancelFunc) *algorithm1ChunkDriver {
	return &algorithm1ChunkDriver{cnl: cnl, cfg: cfg, stop: stop}
}

func (d *algorithm1ChunkDriver) run(ctx context.Context) {
	target := strings.TrimSpace(d.cfg.TargetTableKey)
	for {
		if d.cfg.Control != nil {
			if err := d.cfg.Control.WaitIfPaused(ctx); err != nil {
				return
			}
		}
		job, ok := d.cfg.JobQueue.TryDequeue()
		if !ok {
			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
				continue
			}
		}
		if strings.TrimSpace(job.Spec.TableKey) != target {
			d.handleErr(fmt.Errorf("algorithm1 driver: job table %q != target %q", job.Spec.TableKey, target))
			continue
		}
		var err error
		switch job.Kind {
		case FullStateJobChunkedTable:
			err = d.runChunkedTable(ctx, job.Spec)
		case FullStateJobSelectedPKs:
			err = d.runSelectedPKs(ctx, job.Spec, job.SelectedPKs)
		default:
			err = fmt.Errorf("algorithm1 driver: unknown job kind %v", job.Kind)
		}
		if err != nil {
			d.handleErr(err)
		}
	}
}

func (d *algorithm1ChunkDriver) handleErr(err error) {
	log.Infof("[algorithm1-driver] %v", err)
	d.cfg.Tracker.Reset()
	if d.cfg.ErrorPolicy == Algorithm1DriverStopOnError && d.stop != nil {
		d.stop()
	}
}

func (d *algorithm1ChunkDriver) runAlgorithm1SnapshotCycle(ctx context.Context, spec FullStateTableSpec, fetchRows func() ([]map[string]any, error)) (algorithm1SnapshotCycleResult, error) {
	var zero algorithm1SnapshotCycleResult
	low := uuid.NewString()
	high := uuid.NewString()
	if err := d.cfg.Tracker.BeginCapture(spec.TableKey, low, high, spec.PKColumns); err != nil {
		return zero, err
	}
	resetOnErr := true
	defer func() {
		if resetOnErr {
			d.cfg.Tracker.Reset()
		}
	}()

	if err := d.writeWatermark(ctx, low); err != nil {
		return zero, err
	}
	if err := waitAlgorithm1Phase(ctx, d.cfg.Tracker, Algorithm1PhaseWindowOpen, d.cfg.phaseWait()); err != nil {
		return zero, err
	}
	rawRows, err := fetchRows()
	if err != nil {
		return zero, err
	}
	if err := d.writeWatermark(ctx, high); err != nil {
		return zero, err
	}
	if err := waitAlgorithm1Phase(ctx, d.cfg.Tracker, Algorithm1PhaseReady, d.cfg.phaseWait()); err != nil {
		return zero, err
	}
	out, err := d.cfg.Tracker.ReconcileChunkRows(rawRows)
	if err != nil {
		return zero, err
	}
	resetOnErr = false
	return algorithm1SnapshotCycleResult{Raw: rawRows, Reconciled: out}, nil
}

func (d *algorithm1ChunkDriver) runChunkedTable(ctx context.Context, spec FullStateTableSpec) error {
	if d.cfg.ChunkStore == nil {
		return fmt.Errorf("algorithm1 driver: ChunkStore is nil (required for chunked table job)")
	}
	if err := spec.Validate(); err != nil {
		return err
	}
	id, err := ParseTableIdentity(strings.TrimSpace(spec.TableKey))
	if err != nil {
		return err
	}
	for {
		if d.cfg.Control != nil {
			if err := d.cfg.Control.WaitIfPaused(ctx); err != nil {
				return err
			}
		}
		rec, err := d.loadChunkCursor(spec)
		if err != nil {
			return err
		}

		cycle, err := d.runAlgorithm1SnapshotCycle(ctx, spec, func() ([]map[string]any, error) {
			q, args, err := BuildPKOrderedChunkSelect(id.Database, id.Table, spec.PKColumns, spec.ChunkSize, rec.AfterPK)
			if err != nil {
				return nil, err
			}
			return d.queryRows(ctx, q, args...)
		})
		if err != nil {
			return err
		}

		for _, row := range cycle.Reconciled {
			if err := emitSnapshotRow(d.cfg.RowSink, spec.TableKey, d.cfg.UseEnvelope, row); err != nil {
				d.cfg.Tracker.Reset()
				return err
			}
		}

		raw := cycle.Raw
		if len(raw) == 0 {
			return d.cfg.ChunkStore.Delete(spec.TableKey, spec.RunID)
		}

		lastPK, err := pkTupleFromRowMap(spec.PKColumns, raw[len(raw)-1])
		if err != nil {
			return err
		}
		next := ChunkProgressRecord{
			TableKey:  spec.TableKey,
			RunID:     spec.RunID,
			ChunkSize: spec.ChunkSize,
			AfterPK:   lastPK,
		}
		if err := d.cfg.ChunkStore.Put(next); err != nil {
			return err
		}

		if len(raw) < spec.ChunkSize {
			return nil
		}
	}
}

func (d *algorithm1ChunkDriver) runSelectedPKs(ctx context.Context, spec FullStateTableSpec, pks [][]any) error {
	if err := spec.Validate(); err != nil {
		return err
	}
	id, err := ParseTableIdentity(strings.TrimSpace(spec.TableKey))
	if err != nil {
		return err
	}
	if err := validatePKRows(spec.PKColumns, pks); err != nil {
		return err
	}

	cycle, err := d.runAlgorithm1SnapshotCycle(ctx, spec, func() ([]map[string]any, error) {
		q, args, err := BuildPKRowsInSelect(id.Database, id.Table, spec.PKColumns, pks)
		if err != nil {
			return nil, err
		}
		return d.queryRows(ctx, q, args...)
	})
	if err != nil {
		return err
	}
	for _, row := range cycle.Reconciled {
		if err := emitSnapshotRow(d.cfg.RowSink, spec.TableKey, d.cfg.UseEnvelope, row); err != nil {
			return err
		}
	}
	return nil
}

func (d *algorithm1ChunkDriver) loadChunkCursor(spec FullStateTableSpec) (ChunkProgressRecord, error) {
	rec, err := d.cfg.ChunkStore.Get(spec.TableKey, spec.RunID)
	if err == nil {
		return rec, nil
	}
	if err != ErrChunkProgressNotFound {
		return ChunkProgressRecord{}, err
	}
	return ChunkProgressRecord{
		TableKey:  spec.TableKey,
		RunID:     spec.RunID,
		ChunkSize: spec.ChunkSize,
		AfterPK:   nil,
	}, nil
}

func (d *algorithm1ChunkDriver) writeWatermark(ctx context.Context, value string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	q, args, err := WatermarkUpdateValueSQL(d.cfg.Watermark, value)
	if err != nil {
		return err
	}
	rr, err := d.cnl.Execute(q, args...)
	if err != nil {
		return fmt.Errorf("algorithm1 driver: watermark execute: %w", err)
	}
	if rr != nil {
		rr.Close()
	}
	return nil
}

func (d *algorithm1ChunkDriver) queryRows(ctx context.Context, q string, args ...any) ([]map[string]any, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	rr, err := d.cnl.Execute(q, args...)
	if err != nil {
		return nil, fmt.Errorf("algorithm1 driver: select: %w", err)
	}
	if rr == nil || rr.Resultset == nil {
		return nil, nil
	}
	defer rr.Close()
	return resultsetToRowMaps(rr.Resultset)
}

func waitAlgorithm1Phase(ctx context.Context, tracker *Algorithm1Tracker, want Algorithm1Phase, timeout time.Duration) error {
	if tracker == nil {
		return fmt.Errorf("algorithm1 driver: nil tracker")
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("algorithm1 driver: timeout waiting for phase %v, current=%v", want, tracker.Phase())
		case <-tick.C:
			if tracker.Phase() == want {
				return nil
			}
			ph := tracker.Phase()
			if want != Algorithm1PhaseIdle && ph == Algorithm1PhaseIdle {
				return fmt.Errorf("algorithm1 driver: tracker became idle while waiting for %v", want)
			}
		}
	}
}

func resultsetToRowMaps(rs *mysql.Resultset) ([]map[string]any, error) {
	if rs == nil {
		return nil, nil
	}
	n := rs.RowNumber()
	out := make([]map[string]any, 0, n)
	for i := 0; i < n; i++ {
		m := make(map[string]any, len(rs.Fields))
		for j, f := range rs.Fields {
			v, err := rs.GetValue(i, j)
			if err != nil {
				return nil, err
			}
			name := string(f.Name)
			m[JSONFieldNameForColumn(name)] = v
		}
		out = append(out, m)
	}
	return out, nil
}

func pkTupleFromRowMap(pkColumns []string, row map[string]any) ([]any, error) {
	if row == nil {
		return nil, fmt.Errorf("algorithm1 driver: nil row for pk tuple")
	}
	out := make([]any, len(pkColumns))
	for i, col := range pkColumns {
		jn := JSONFieldNameForColumn(strings.TrimSpace(col))
		v, ok := row[jn]
		if !ok {
			return nil, fmt.Errorf("algorithm1 driver: row missing pk field %q (json key %q)", col, jn)
		}
		out[i] = v
	}
	return out, nil
}

func emitSnapshotRow(sink RowEventSink, tableKey string, useEnvelope bool, row map[string]any) error {
	if sink == nil || row == nil {
		return nil
	}
	b, err := json.Marshal(row)
	if err != nil {
		return fmt.Errorf("algorithm1 driver: marshal row: %w", err)
	}
	if useEnvelope {
		wrapped, werr := MarshalCDCEventEnvelope(
			DefaultEnvelopeSchemaVersion,
			OriginSnapshot,
			canal.InsertAction,
			tableKey,
			nil,
			row,
			b,
			nil,
			nil,
		)
		if werr != nil {
			return fmt.Errorf("algorithm1 driver: envelope: %w", werr)
		}
		b = wrapped
	}
	return sink.Emit(tableKey, canal.InsertAction, b)
}
