# tubing-cdc

Lightweight MySQL **binlog** CDC on [go-mysql canal](https://github.com/go-mysql-org/go-mysql): a small `TubingCDC` API, optional binlog **position persistence** (Badger + Redis), and pluggable handlers and sinks. The long-term target is alignment with the **DBLog** paper (watermarks and chunked snapshots); today the implementation is **binlog-only** phase 1.

- **Human narrative + goals:** [docs/context.md](docs/context.md)
- **Paper vs code:** [docs/coverage-vs-dblog.md](docs/coverage-vs-dblog.md)
- **Agent / AI map:** [AGENTS.md](AGENTS.md)
- **Full doc index:** [docs/context.md](docs/context.md#related-docs)

## Quick start

Table names must be fully qualified as `database.table`.

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

if err := cdc.Run(); err != nil {
    // handle error
}
```

Use `RunFrom(mysql.Position)` to resume from a stored position. More detail: [docs/usage.md](docs/usage.md). Persistence: [docs/position-persistence.md](docs/position-persistence.md). Handlers and sinks: [docs/event-handlers.md](docs/event-handlers.md). Runtime diagram: [docs/architecture.md](docs/architecture.md). Tests: [docs/development.md](docs/development.md). Roadmap: [docs/roadmap.md](docs/roadmap.md). Citation: [docs/references.md](docs/references.md).
