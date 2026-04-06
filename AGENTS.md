# Agent guide (tubing-cdc)

Go module **`tubing-cdc`** (import alias often `tubingcdc`). Single package **`tubing_cdc`** at the repository root.

## What this is

MySQL **binlog** CDC built on [go-mysql canal](https://github.com/go-mysql-org/go-mysql): `NewTubingCDC` + `Configs`, optional **Badger/Redis** binlog position persistence, pluggable **`EventHandler`** and **`RowEventSink`**.

Long-term direction: align with the **DBLog** paper (watermarks, chunked PK snapshots, unified envelopes, HA). **Not implemented yet** beyond binlog tail + position store—see [docs/coverage-vs-dblog.md](docs/coverage-vs-dblog.md) and [docs/roadmap.md](docs/roadmap.md).

## Where to edit code

| Area | Files |
|------|--------|
| Public API, canal wiring, `Run` / `RunFrom` | `data_flow.go`, `options.go` |
| Default row logging handler | `cdc_event_handler.go` |
| Dynamic struct + JSON + transforms | `cdc_dynamic_event_handler.go` |
| Sinks (stdout, logger, interface) | `row_event_sink.go` |
| Kafka sink | `kafka_row_sink.go` |
| Elasticsearch sink + composable document id | `elasticsearch_row_sink.go`, `elasticsearch_document_id.go` |
| Badger/Redis position JSON + read helper | `position_store.go` |
| Wrap handler for `OnPosSynced` persistence | `position_handler_wrapper.go` |
| Shared small types | `model.go` |

**Convention:** `Configs.Tables` entries are fully qualified `database.table`.

**Note:** Canal `mysqldump` dump path is intentionally disabled (`Dump.ExecutionPath == ""` in `data_flow.go`).

## Tests

From repository root:

```bash
go test ./...
```

Integration tests (`cdc_dem_test_integration_test.go`, package `tubing_cdc_test`) need Docker available for Testcontainers.

## Docs index

| Doc | Use when |
|-----|----------|
| [docs/context.md](docs/context.md) | Motivation, DBLog goal, current phase |
| [docs/coverage-vs-dblog.md](docs/coverage-vs-dblog.md) | Gap analysis vs paper |
| [docs/architecture.md](docs/architecture.md) | Runtime diagram |
| [docs/usage.md](docs/usage.md) | Basic API |
| [docs/position-persistence.md](docs/position-persistence.md) | Badger + Redis |
| [docs/event-handlers.md](docs/event-handlers.md) | Handlers, sinks, transforms |
| [docs/development.md](docs/development.md) | Compose + tests |
| [docs/roadmap.md](docs/roadmap.md) | P0–P6 phases |
| [docs/references.md](docs/references.md) | Paper citation + PDF |

Start with **this file** + the doc that matches the task (e.g. roadmap for feature planning, event-handlers for sink changes).
