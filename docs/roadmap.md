# Roadmap

Work toward DBLog-style behavior is grouped by dependency. Binlog position storage today is **not** a full replacement for the paper’s chunk-progress and HA state; expect new keys or a dedicated subsystem when P2–P5 land.

| Phase | Focus |
|-------|--------|
| **P0** | **Design** — Define a DBLog-aligned **event envelope** (action type, primary key, LSN or binlog position, origin = `log` \| `snapshot`, table identity, payload). Document compatibility or migration from the current JSON shape and `RowEventSink`. |
| **P1** | **Watermark infrastructure** — Create or configure the watermark table on the source; recognize watermark row updates in the binlog stream (map to canal events). |
| **P2** | **Chunked full state** — Primary-key-ordered chunks with configurable size; **persist chunk progress** (e.g. extend Badger/Redis with a separate namespace from binlog `PositionPersistence`, or another store). |
| **P3** | **Core algorithm** — Implement the paper’s watermark **Algorithm 1**: brief log pause for watermark writes + chunk read; resume; drop chunk PKs that appear between low and high watermark in the log; append surviving chunk rows in order. Align pause/resume with canal’s execution model and any ordered output buffer. |
| **P4** | **Triggers and operations** — APIs or config to start full-state capture for all tables, one table, or selected PKs; pause and resume chunk processing. |
| **P5** | **High availability** — Leader election and standby processes (paper uses ZooKeeper; alternatives such as etcd or Redis are plausible). |
| **P6** | **Multi-database (optional)** — Abstract the replication log protocol for PostgreSQL and others, as in the paper’s scope. |
| **CI and Docker** | Expand docker-based workflows so the full CDC path (and, later, watermark/chunk paths) is easy to run end-to-end in CI or locally; extend integration tests as features ship. |

What exists today vs the paper: [coverage-vs-dblog.md](coverage-vs-dblog.md). Background: [context.md](context.md).
