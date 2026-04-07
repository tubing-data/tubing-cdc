# DBLog feature × test coverage matrix

Maps DBLog-related behavior to where it is verified. When in doubt, treat tests and [roadmap.md](roadmap.md) as authoritative over older gap docs.

| Scenario | Unit / table-driven | Integration (Docker) | Notes |
|----------|---------------------|----------------------|--------|
| Watermark DDL / `ParseWatermarkBinlogEvent` | [watermark_test.go](../watermark_test.go) | [dblog_algorithm1_integration_test.go](../dblog_algorithm1_integration_test.go) (`TestIntegration_watermark_notifier`) | |
| `Algorithm1Tracker` phases + `ReconcileChunkRows` | [dblog_algorithm1_test.go](../dblog_algorithm1_test.go) | [dblog_algorithm1_integration_test.go](../dblog_algorithm1_integration_test.go) (`TestIntegration_algorithm1_manual_cycle`) | Driver does **not** pause binlog; see [algorithm1-chunk-driver.md](algorithm1-chunk-driver.md) |
| Chunk SQL + cursor | [chunk_progress_test.go](../chunk_progress_test.go) | — | |
| Chunk progress Badger | [chunk_progress_store_test.go](../chunk_progress_store_test.go) | [dblog_algorithm1_integration_test.go](../dblog_algorithm1_integration_test.go) (`TestIntegration_chunk_progress_persisted_after_close`) | Two-row table, `chunk_size=3` so the driver exits after one chunk without `Delete`; shared Badger with `PositionPersistence` |
| Algorithm1 chunk driver wiring | [algorithm1_chunk_driver_test.go](../algorithm1_chunk_driver_test.go) | — | |
| Event envelope | [event_envelope_test.go](../event_envelope_test.go) | — | Snapshot `primary_key` contract: [algorithm1_chunk_driver_test.go](../algorithm1_chunk_driver_test.go) |
| Multi-MySQL persistence scope | [multi_mysql_cdc_test.go](../multi_mysql_cdc_test.go) | — | |
| Redis leader election | [leader_election_test.go](../leader_election_test.go) (miniredis) | Optional: Redis Testcontainers / Compose (not in default CI) | |
| Basic binlog row delivery | [data_flow_test.go](../data_flow_test.go) | [cdc_dem_test_integration_test.go](../cdc_dem_test_integration_test.go) | |

## CI

- **Default (GitHub Actions):** `go test -v ./...` on `ubuntu-latest` with Docker available so Testcontainers MySQL tests run when the daemon is reachable; tests call `ensureDocker` and skip if Docker is absent.
- **Optional / nightly:** Add a job with Redis service for live lease tests; keep miniredis-based tests as the PR gate.

## Performance artifacts

- Go benchmarks: `go test -run=^$ -bench=. -benchmem ./...` skips tests and runs only benchmarks (see [benchmark_dblog_test.go](../benchmark_dblog_test.go)).
- Optional SQL / metrics templates: [scripts/perf/README.md](../scripts/perf/README.md).

## benchstat regression (local)

```bash
go test -bench='BenchmarkMarshalCDCEventEnvelope|BenchmarkReconcileChunkRows|BenchmarkBuildPKOrderedChunkSelect' -benchmem -count=10 ./... > /tmp/old.txt
# after changes
go test -bench='BenchmarkMarshalCDCEventEnvelope|BenchmarkReconcileChunkRows|BenchmarkBuildPKOrderedChunkSelect' -benchmem -count=10 ./... > /tmp/new.txt
benchstat /tmp/old.txt /tmp/new.txt
```

Install: `go install golang.org/x/perf/cmd/benchstat@latest`.
