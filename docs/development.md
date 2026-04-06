# Local development and testing

- **Docker Compose** — `docker-compose.yml` (repository root) starts MySQL (with build context under `docker/mysql/`) on port `3306` for manual CDC experiments.
- **Go tests** — From the repository root, run `go test ./...`. Integration-style tests (e.g. `TestCDC_dem_test_row_events_integration` in `cdc_dem_test_integration_test.go`) use Testcontainers when Docker is available; table-driven unit tests cover config helpers and the dynamic handler.
- **DBLog path (future)** — Once watermarking and chunked snapshots exist, Compose and tests should also cover grants for the watermark table, repeatable chunk boundaries, and ordering checks against the paper’s window semantics. Exact commands will be documented when that code lands.

See [roadmap.md](roadmap.md) for planned features.
