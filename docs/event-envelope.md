# DBLog-aligned event envelope (P0)

This document defines the **P0** unified CDC event shape: one JSON object for both binlog-origin rows (today) and future chunked snapshot rows, matching the spirit of DBLog’s single output envelope.

## JSON shape

| Field | Type | Description |
|-------|------|-------------|
| `event_id` | string | Deterministic SHA-256 identifier derived from source, origin, table, action, binlog position, row ordinal, primary key, and payload. Consumers can use it for replay deduplication. |
| `schema_version` | string | Envelope version, currently `tubing-cdc-envelope-v1`. Version 1 adds stable source/file/row coordinates. |
| `source_id` | string | Stable replication-source identity. `Configs.SourceID` controls it; a single source defaults to its MySQL address and `MultiMySQLCDC` defaults it to the source spec ID. |
| `origin` | string | `log` — from MySQL replication; `snapshot` — reserved for chunked PK reads (P2+). |
| `action` | string | Canal row action: `insert`, `update`, `delete`. |
| `table` | object | `database` and `table` (fully qualified identity, same convention as `Configs.Tables`). |
| `primary_key` | object | Map of **JSON column name** → value for the table’s primary key columns (from canal schema). Empty or omitted when there is no PK metadata. |
| `position` | object | Optional MySQL binlog coordinates: `file` and `pos` (uint32). The dynamic handler follows rotate/position callbacks so log events normally include both. |
| `row_ordinal` | integer | Zero-based logical row number inside a multi-row binlog event. Zero is omitted from JSON; it still participates in event-ID generation. |
| `payload` | JSON | **Legacy row JSON** — the same value the dynamic handler would emit without the envelope: a single row object, or for `update`, `{"before":{...},"after":{...}}`. |

## Compatibility and migration

- **Default behavior is unchanged**: `DynamicTableEventHandler` still emits legacy row-only JSON unless you opt in with `tubingcdc.WithDBLogEnvelope(true)`.
- **`RowEventSink` contract is unchanged**: `Emit(tableKey, action, payloadJSON)` still receives one byte slice per row. With the envelope enabled, `payloadJSON` is the full envelope object; `action` and `tableKey` remain duplicated at the top level for sinks that already use them (e.g. Kafka headers).
- **Downstream consumers** can detect the new shape via `schema_version` or a top-level `origin` field and parse `payload` with existing logic.
- **Kafka** keeps the envelope as the message body. **Elasticsearch** indexes the full envelope by default; its built-in ID extraction and `StoreLatestEntity` mode understand the nested `payload` field.

## Go API

- Types and helpers: `event_envelope.go` (`CDCEventEnvelope`, `MarshalCDCEventEnvelope`, `PrimaryKeyFromTableRow`, …).
- Handler option: `WithDBLogEnvelope(true)` on `NewDynamicTableEventHandler`.

## Remaining limitation

- A custom Elasticsearch `DocumentID` callback receives the original envelope. Use field paths below `payload` or provide an envelope-aware callback.

See [roadmap.md](roadmap.md) and [coverage-vs-dblog.md](coverage-vs-dblog.md).
