# Local development and testing

- **Docker Compose** — `docker-compose.yml` (repository root) defines:
  - **MySQL** — image built from `docker/mysql/Dockerfile`; port `3306`, database `cdc_test`, root password `root` (for manual CDC experiments).
  - **Elasticsearch** — official `8.12.2` image, single-node, HTTP on port `9200`, security and TLS disabled for local use. Handy when exercising **`ElasticsearchRowEventSink`** or ad-hoc indexing against `http://localhost:9200`.
  - Start everything: `docker compose up -d`. Start only one service: `docker compose up -d mysql` or `docker compose up -d elasticsearch`. Wait until Elasticsearch reports healthy (or `curl -s http://localhost:9200/_cluster/health`) before sending traffic.
- **Go tests** — From the repository root, run `go test ./...`. Integration-style tests (e.g. `TestCDC_dem_test_row_events_integration` in `cdc_dem_test_integration_test.go`) use Testcontainers when Docker is available; table-driven unit tests cover config helpers, the dynamic handler, and the Elasticsearch sink (HTTP flows use `httptest`; a running Compose ES is optional for manual checks).
- **DBLog path (future)** — Once watermarking and chunked snapshots exist, Compose and tests should also cover grants for the watermark table, repeatable chunk boundaries, and ordering checks against the paper’s window semantics. Exact commands will be documented when that code lands.

See [roadmap.md](roadmap.md) for planned features.
