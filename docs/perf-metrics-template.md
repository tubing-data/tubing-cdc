# Performance run template (copy per iteration)

| Field | Value |
|-------|--------|
| Date | |
| Host / CI runner | |
| Go version | `go version` |
| MySQL version | |
| tubing-cdc commit | |

## Configuration (summarize)

- Tables filtered:
- Chunk size / `PhaseWaitTimeout`:
- `UseEnvelope` (log/snapshot):
- Badger dirs (position / chunk):

## Results

| Metric | Value | Notes |
|--------|--------|--------|
| Binlog apply rate (rows/s) | | From workload generator or `SHOW GLOBAL STATUS` deltas |
| Handler-to-sink latency (p50/p95) | | If instrumented |
| Algorithm 1 cycle time (low → first emit) | | Driver logs or custom span |
| RSS / CPU (avg) | | `ps`, `pprof` |
| Badger directory size | | After run |

## Regression check

```text
benchstat old.txt new.txt
```

Attach stdout from targeted benchmarks, for example:

`BenchmarkMarshalCDCEventEnvelope`, `BenchmarkReconcileChunkRows/*`, `BenchmarkBuildPKOrderedChunkSelect/*`.
