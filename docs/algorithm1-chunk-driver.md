# Algorithm 1 chunk driver (built-in goroutine)

`TubingCDC.StartAlgorithm1ChunkDriver` runs a background loop that:

1. Dequeues `FullStateJob` values from the configured `FullStateJobQueue`.
2. For each job, executes a full **Algorithm 1** window: `Algorithm1Tracker.BeginCapture` → `WatermarkUpdateValueSQL` + `Canal.Execute` (low) → wait until the tracker sees the low value in the binlog → run the chunk `SELECT` (PK-ordered limit or PK `IN` list) → watermark high → wait for ready → `ReconcileChunkRows` → emit surviving rows.

## Wiring checklist

- `Configs.Watermark`, `Configs.WatermarkNotifier` must call `ChainWatermarkNotifiers(tracker.OnWatermark, …)` so the tracker advances when watermark rows appear in replication.
- `Configs.Algorithm1` must use the **same** `Algorithm1Tracker` and `TargetTableKey` as `Algorithm1ChunkDriverConfig`.
- Every enqueued job’s `Spec.TableKey` must equal that `TargetTableKey` (single-table Algorithm 1 scope).
- `Configs.FullStateJobQueue` should be the **same** pointer passed as `Algorithm1ChunkDriverConfig.JobQueue`.
- Chunked table jobs need `ChunkProgressStore` from `TubingCDC.ChunkProgressStore()` (enable `ChunkProgressPersistence`).

## Boundaries and expectations

### Blocking canal / binlog

- Snapshot `SELECT`s and watermark `UPDATE`s use `Canal.Execute`, which takes `connLock` on the canal’s SQL connection.
- **Binlog replication runs on a separate syncer path**; this driver does **not** implement the paper’s “pause log consumption” step. Replication keeps advancing while chunks run.
- For stricter ordering relative to a sink, use an application-level buffer or a single writer goroutine fed by both binlog and snapshot paths.

### Error policy

- `Algorithm1DriverStopOnError` (default): log the error, `Tracker.Reset`, cancel the driver context, and exit the goroutine.
- `Algorithm1DriverContinueOnError`: log, `Tracker.Reset`, then keep dequeuing (mis-sequenced jobs may fail until the queue is healthy).

### Interaction with `RowEventSink`

- Snapshot rows are sent with action `insert` (same string as `canal.InsertAction`) and payload JSON equal to a **single row object** (not update before/after), matching typical `DynamicTableEventHandler` inserts.
- With `UseEnvelope: true`, payloads use `MarshalCDCEventEnvelope` with `origin: snapshot` and `action: insert`. `schema.Table` is not passed, so `primary_key` in the envelope may be empty unless you extend the driver later.

### Timeouts

- `PhaseWaitTimeout` (default 60s) bounds polling for `Algorithm1PhaseWindowOpen` and `Algorithm1PhaseReady` after each watermark `Execute`. Tighten or loosen for your replication lag and load.

### Chunk cursor

- After each non-empty chunk, progress is saved using the **last raw SELECT row’s** PK tuple (including rows dropped by reconciliation), so the next chunk does not skip keys that were only “removed” because they appeared in the log window.

## Lifecycle

- Call `StartAlgorithm1ChunkDriver` **after** `TubingCDC` is constructed and **while** `Run` / `RunFrom` is processing events (otherwise watermark phases never advance).
- `TubingCDC.Close` calls `StopAlgorithm1ChunkDriver` first.
- Starting a driver when one is already running returns an error.
