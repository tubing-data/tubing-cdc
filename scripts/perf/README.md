# Performance harness (optional)

- **SQL template:** [mysql_load_template.sql](mysql_load_template.sql) — creates a small table and a sample multi-row insert; extend with loops or external drivers for steady QPS.
- **Go microbenchmarks:** `go test -bench=. -benchmem ./...` (see [docs/testing-dblog-matrix.md](../../docs/testing-dblog-matrix.md) for `benchstat` usage).
- **End-to-end latency:** add a monotonic `seq` or `ts` column in the workload table; compare wall time when the sink observes each `seq` (outside this repo’s default sinks, add a test hook or log line).

For Docker-based MySQL, reuse the same `docker/mysql/conf.d/binlog.cnf` and grants as integration tests so replication matches CI.
