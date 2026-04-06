# tubing-cdc

When I joined the second company in my career, I was assigned to build a CDC system. At that time, I realized that when the amount of data in the enterprise was gradually increasing, such a system was needed to handle data synchronization related work, but before that, we needed a lightweight solution. This project aims to design a lightweight, out-of-the-box system to help the booming system quickly establish a data synchronization module.

It wraps [go-mysql canal](https://github.com/go-mysql-org/go-mysql) for MySQL binlog consumption and exposes a small `TubingCDC` API plus pluggable event handlers.

## Usage

Create a client with `Configs`. Table names must be fully qualified as `database.table`; each entry is turned into an include regex for canal.

```go
import tubingcdc "tubing-cdc"

cfg := &tubingcdc.Configs{
    Address:  "127.0.0.1:3306",
    Username: "cdc_user",
    Password: "secret",
    Tables:   []string{"mydb.orders", "mydb.customers"},
}

cdc, err := tubingcdc.NewTubingCDC(cfg)
if err != nil {
    // handle error
}
defer cdc.Close()

// Block and follow the binlog from the current position.
if err := cdc.Run(); err != nil {
    // handle error
}
```

To start from a known binlog position, use `RunFrom(mysql.Position)` instead of `Run()`.

## Event handlers

- **Default (`MyEventHandler`)** — If `Configs.EventHandler` is nil, row events are logged as raw action plus row slices (simple debugging).
- **`DynamicTableEventHandler`** — Call `tubingcdc.NewDynamicTableEventHandler(tables, opts...)` and assign it to `Configs.EventHandler`. For each watched table, the handler:
  - Builds a **runtime struct type** with `reflect.StructOf` from canal’s table schema (fields are `any` so binlog values fit reliably).
  - Logs a **Go struct snippet** once per table (approximate field types for readability).
  - Emits **each row event as JSON** through a pluggable **`RowEventSink`**; `update` events include `before` and `after` objects.
  - Optionally applies **destination field transforms** (`WithRowFieldTransformRules`) so you can derive or reshape columns before JSON reaches the sink.

Pass the same `[]string` you use for `Configs.Tables` so only registered tables are processed, or pass `nil`/empty to allow every table that appears in events.

### Row output sinks (`RowEventSink`)

By default, row JSON goes to **`LoggerRowSink`** (same `[CDC] action table …` lines as before via `go-log`). You can redirect output with **`tubingcdc.WithRowEventSink`**:

| Sink | Role |
|------|------|
| **`LoggerRowSink`** | Default when no option is passed; uses structured log lines. |
| **`StdoutRowSink`** | Writes one line per event to an `io.Writer` (defaults to `os.Stdout`). Optional: set `Writer` to a file or buffer. |
| **`KafkaRowEventSink`** | Publishes each event to Kafka (`segmentio/kafka-go`): message **key** = fully qualified table name, **value** = JSON payload, header **`cdc_action`** = canal action. Call **`Close()`** on shutdown. |

Implement **`RowEventSink`** yourself (`Emit(tableKey, action string, payloadJSON []byte) error`) for other systems (HTTP, Pulsar, etc.).

Default handler (log sink):

```go
tables := []string{"mydb.orders"}
cfg := &tubingcdc.Configs{
    Address:      "127.0.0.1:3306",
    Username:     "cdc_user",
    Password:     "secret",
    Tables:       tables,
    EventHandler: tubingcdc.NewDynamicTableEventHandler(tables),
}
```

Stdout (line-oriented, same prefix as logs):

```go
h := tubingcdc.NewDynamicTableEventHandler(tables, tubingcdc.WithRowEventSink(tubingcdc.StdoutRowSink{}))
// Or: StdoutRowSink{Writer: fileOrPipe}
```

Kafka:

```go
ks, err := tubingcdc.NewKafkaRowEventSink(tubingcdc.KafkaSinkConfig{
    Brokers: []string{"localhost:9092"},
    Topic:   "cdc_rows",
})
if err != nil {
    // handle error
}
defer ks.Close()

h := tubingcdc.NewDynamicTableEventHandler(tables, tubingcdc.WithRowEventSink(ks))
cfg := &tubingcdc.Configs{ /* … */, EventHandler: h }
```

### Destination field transforms (`WithRowFieldTransformRules`)

Before a row is serialized for `RowEventSink`, you can register one or more **`RowFieldTransformRule`** values. Each rule watches a **source column** (the JSON field name is the same as the MySQL column name in the default mapping) and runs a **`Transform` function** that returns a `map[string]any`. That map is **merged into the outgoing row**: new keys are added, and **if a returned key already exists on the row, it is overwritten**. When several rules run, **later rules win** on the same output key.

- **`TableKey`** — If set to a fully qualified name like `mydb.orders`, the rule runs only for that table. If empty, the rule applies to every table the handler processes.
- **`SourceColumn`** — Must match an existing key on the row after the base mapping; if the column is absent, the rule is skipped.
- **`Transform(tableKey, action, value)`** — Receives the current cell value; return any nested structure (maps, slices, etc.) as values in the map.

`update` events apply the same rules independently to **`before`** and **`after`**. If you pass **no** transform rules, the handler keeps the previous behavior and marshals the runtime struct directly (no extra map pass).

Example: add a parsed object from a string column and optionally replace the original field.

```go
h := tubingcdc.NewDynamicTableEventHandler(tables,
    tubingcdc.WithRowEventSink(sink),
    tubingcdc.WithRowFieldTransformRules(
        tubingcdc.RowFieldTransformRule{
            TableKey:     "mydb.orders",
            SourceColumn: "payload_json",
            Transform: func(tableKey, action string, value any) map[string]any {
                // Example: attach structured output; same key overwrites the row.
                return map[string]any{
                    "payload": map[string]any{"raw": value},
                }
            },
        },
    ),
)
```

On DDL, canal calls `OnTableChanged`; the dynamic handler clears its per-table cache so the next rows use an updated layout.

## Local testing

- **Docker Compose** — `docker-compose.yml` starts MySQL (with build context under `docker/mysql/`) on port `3306` for manual CDC experiments.
- **Go tests** — Integration-style tests (e.g. `TestCDC_dem_test_row_events_integration`) use Testcontainers when Docker is available; table-driven unit tests cover config helpers and the dynamic handler.

## Todo

- Expand docker-based workflows so the full CDC path is easy to run end-to-end in CI or locally.
