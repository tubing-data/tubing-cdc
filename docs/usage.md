# Usage

## Install

From an existing Go module, install a released version:

```bash
go get github.com/tubing-data/tubing-cdc@v0.0.1
```

The package requires Go 1.21 or later. MySQL must have row-based binary logging enabled; Redis,
Kafka, and Elasticsearch are optional and are needed only when their integrations are configured.

Create a client with `Configs`. Table names must be fully qualified as `database.table`; each entry is turned into an include regex for canal.

```go
import tubingcdc "github.com/tubing-data/tubing-cdc"

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

## Next steps

- [position-persistence.md](position-persistence.md) — persist positions across restarts
- [event-handlers.md](event-handlers.md) — dynamic JSON rows, Kafka, transforms
