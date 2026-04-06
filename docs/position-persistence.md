# Binlog position persistence (Badger + Redis)

To avoid reprocessing events after restarts and to support recovery, you can persist the sync position in two tiers:

1. **Badger (local)** — After each successful canal **`OnPosSynced`** callback (for example when a transaction commits via `XIDEvent`, and likewise for rotate/DDL when canal chooses to save), the latest **`mysql.Position`** plus an optional **GTID** string are written to a local Badger database.
2. **Redis (periodic)** — If `RedisAddr` is set, a background goroutine **SET**s the same JSON snapshot on a configurable interval (**default 5 minutes** when `FlushToRedisInterval` is zero). **`TubingCDC.Close()`** stops that loop, performs a **final Redis write**, then closes Badger and the Redis client—so always call **`Close()`** on shutdown.

Enable this by setting **`Configs.PositionPersistence`** with a non-empty **`BadgerDir`**. The user `EventHandler` is wrapped automatically; your `OnPosSynced` (if overridden) still runs first, and persistence runs only when it returns no error.

| Field | Role |
|--------|------|
| **`BadgerDir`** | Directory for Badger (required to enable). |
| **`BadgerKey`** | Key inside Badger; empty uses a built-in default. |
| **`RedisAddr`** | If empty, only Badger is used (no Redis goroutine). |
| **`RedisPassword`**, **`RedisDB`** | Standard Redis client options. |
| **`RedisKey`** | Redis key for the JSON value; empty uses a built-in default. |
| **`FlushToRedisInterval`** | Interval between Redis writes; **default 5m** if unset or non-positive. |
| **`GTIDFlavor`** | Stored in JSON (e.g. `mysql`, `mariadb`) to help operators parse GTID on recovery. |

Example:

```go
import (
    "time"

    tubingcdc "tubing-cdc"
)

cfg := &tubingcdc.Configs{
    Address:  "127.0.0.1:3306",
    Username: "cdc_user",
    Password: "secret",
    Tables:   []string{"mydb.orders"},
    PositionPersistence: &tubingcdc.PositionPersistence{
        BadgerDir:             "/var/lib/tubing-cdc/positions",
        RedisAddr:             "127.0.0.1:6379",
        RedisKey:              "myapp:cdc:binlog-position",
        FlushToRedisInterval:  5 * time.Minute,
        GTIDFlavor:            "mysql",
    },
}
```

The stored JSON shape is **`tubingcdc.BinlogStateRecord`** (`file`, `pos`, optional `gtid`, `gtid_flavor`). To read the last position from disk without opening the live process (for example to call **`RunFrom`** on startup), use **`tubingcdc.ReadBinlogStateFromBadger(badgerDir, badgerKey)`**; it returns **`mysql.Position`** and a GTID string you can pass to **`mysql.ParseGTIDSet`** together with your chosen flavor.

Implementation: `position_store.go`, `position_handler_wrapper.go`.
