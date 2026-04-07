# DBLog-aligned event envelope (P0)

This document defines the **P0** unified CDC event shape: one JSON object for both binlog-origin rows (today) and future chunked snapshot rows, matching the spirit of DBLog’s single output envelope.

## JSON shape

| Field | Type | Description |
|-------|------|-------------|
| `schema_version` | string | Envelope version, e.g. `tubing-cdc-envelope-v0`. Bump when fields are added or semantics change. |
| `origin` | string | `log` — from MySQL replication; `snapshot` — reserved for chunked PK reads (P2+). |
| `action` | string | Canal row action: `insert`, `update`, `delete`. |
| `table` | object | `database` and `table` (fully qualified identity, same convention as `Configs.Tables`). |
| `primary_key` | object | Map of **JSON column name** → value for the table’s primary key columns (from canal schema). Empty or omitted when there is no PK metadata. |
| `position` | object | Optional MySQL binlog coordinates: `file` (optional) and `pos` (uint32). When only the replication event header is available, `pos` may be set without `file` until the syncer exposes the current log file name. |
| `payload` | JSON | **Legacy row JSON** — the same value the dynamic handler would emit without the envelope: a single row object, or for `update`, `{"before":{...},"after":{...}}`. |

## Compatibility and migration

- **Default behavior is unchanged**: `DynamicTableEventHandler` still emits legacy row-only JSON unless you opt in with `tubingcdc.WithDBLogEnvelope(true)`.
- **`RowEventSink` contract is unchanged**: `Emit(tableKey, action, payloadJSON)` still receives one byte slice per row. With the envelope enabled, `payloadJSON` is the full envelope object; `action` and `tableKey` remain duplicated at the top level for sinks that already use them (e.g. Kafka headers).
- **Downstream consumers** can detect the new shape via `schema_version` or a top-level `origin` field and parse `payload` with existing logic.
- **Elasticsearch / Kafka** sinks treat the message body as opaque JSON unless you add custom routing; for envelope mode, consider parsing `payload` inside your own sink or indexing the full document with a nested `payload` field.

## Go API

- Types and helpers: `event_envelope.go` (`CDCEventEnvelope`, `MarshalCDCEventEnvelope`, `PrimaryKeyFromTableRow`, …).
- Handler option: `WithDBLogEnvelope(true)` on `NewDynamicTableEventHandler`.

## Future work (P2+)

- Populate `origin: "snapshot"` and stable ordering metadata for chunk rows.
- Fill `position.file` from the canal syncer together with `pos` for a complete restart coordinate.
- Optional strict mode or a dedicated `EnvelopeRowSink` if we want to deprecate duplicate `action` in `Emit` for envelope-only pipelines.

See [roadmap.md](roadmap.md) and [coverage-vs-dblog.md](coverage-vs-dblog.md).
