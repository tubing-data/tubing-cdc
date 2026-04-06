# Usage

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

## Next steps

- [position-persistence.md](position-persistence.md) — persist positions across restarts
- [event-handlers.md](event-handlers.md) — dynamic JSON rows, Kafka, transforms
