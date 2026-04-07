# Local development and testing

- **Docker Compose** — `docker-compose.yml` (repository root) defines:
  - **MySQL** — image built from `docker/mysql/Dockerfile`; port `3306`, database `cdc_test`, root password `root` (for manual CDC experiments).
  - **Elasticsearch** — official `8.12.2` image, single-node, HTTP on port `9200`, security and TLS disabled for local use. Handy when exercising **`ElasticsearchRowEventSink`** or ad-hoc indexing against `http://localhost:9200`.
  - Start everything: `docker compose up -d`. Start only one service: `docker compose up -d mysql` or `docker compose up -d elasticsearch`. Wait until Elasticsearch reports healthy (or `curl -s http://localhost:9200/_cluster/health`) before sending traffic.
- **Go tests** — From the repository root, run `go test ./...`. Integration tests (`cdc_dem_test_integration_test.go`, `dblog_algorithm1_integration_test.go`, `integration_mysql_test.go`) use Testcontainers when Docker is available; they skip if the Docker daemon is unreachable. Table-driven unit tests cover config helpers, DBLog helpers, the dynamic handler, and the Elasticsearch sink (HTTP flows use `httptest`; a running Compose ES is optional for manual checks).
- **DBLog / correctness matrix** — Feature coverage vs tests is summarized in [testing-dblog-matrix.md](testing-dblog-matrix.md).
- **Performance** — Go benchmarks live in `benchmark_dblog_test.go` (`go test -bench=. -benchmem ./...`). Optional SQL load template and a metrics worksheet: [scripts/perf/README.md](../scripts/perf/README.md), [perf-metrics-template.md](perf-metrics-template.md).

See [roadmap.md](roadmap.md) for phase boundaries and remaining gaps (e.g. PostgreSQL).
