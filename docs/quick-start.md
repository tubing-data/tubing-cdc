# Quick start: MySQL CDC to Elasticsearch

This walkthrough starts a binlog-enabled MySQL, Elasticsearch, and a containerized tubing-cdc example. A sample processing pipeline normalizes `status`, extracts `email_domain`, builds `search_text`, and sends the resulting row to Elasticsearch. The sink uses `StoreLatestEntity`, so each index document represents the latest source row rather than CDC history.

## Requirements

- Docker with Docker Compose v2
- `make` and `curl`
- Ports `3306` and `9200` available locally

## Run the complete demo

From the repository root:

```bash
make demo
```

The command builds the root `Dockerfile`, starts all three services, inserts an order, and queries the `cdc-orders` index. The Elasticsearch hit should contain processed fields like these:

```json
{
  "cdc_table": "cdc_test.orders",
  "cdc_action": "insert",
  "data": {
    "status": "PENDING",
    "email_domain": "example.com",
    "search_text": "Ada Lovelace PENDING"
  }
}
```

## Explore insert, update, and delete

The most visual option is the interactive command:

```bash
make interactive
```

Enter any `INSERT`, `UPDATE`, or `DELETE` statement for `orders` at the `mysql>` prompt. After each statement it displays:

1. The CDC event received from the MySQL binlog, including `before` and `after`
2. The event after the sample processing pipeline
3. The latest entity documents actually stored in the `cdc-orders` Elasticsearch index

For example:

```sql
INSERT INTO orders (customer_name, customer_email, status, amount_cents)
VALUES ('Grace Hopper', 'grace@example.org', 'pending', 2499);

UPDATE orders SET status='paid' WHERE id=1;

DELETE FROM orders WHERE id=1;
```

Type `quit` to leave the interactive loop. The containers remain running so they can be inspected; use `make down` to remove them.

You can also operate each step manually. Start the environment and follow the CDC logs:

Start the environment and follow the CDC logs:

```bash
make quickstart
make logs
```

In another terminal, insert an order and inspect Elasticsearch:

```bash
make seed
make verify
```

Update the newest order. Its Elasticsearch document keeps the MySQL primary key as `_id` and is replaced with only the processed `after` row:

```bash
docker compose exec -T mysql mysql -uroot -proot cdc_test -e \
  "UPDATE orders SET status='paid', amount_cents=1599 ORDER BY id DESC LIMIT 1;"
make verify
```

Delete it. The matching Elasticsearch document is removed:

```bash
docker compose exec -T mysql mysql -uroot -proot cdc_test -e \
  "DELETE FROM orders ORDER BY id DESC LIMIT 1;"
make verify
```

The source-to-index path is:

```text
MySQL orders row -> row binlog -> DynamicTableEventHandler
                  -> Map processing pipeline -> ElasticsearchRowEventSink
                  -> cdc-orders/_doc/{MySQL id}
```

## Useful commands

| Command | Purpose |
| --- | --- |
| `make demo` | Build, start, insert one row, and verify Elasticsearch |
| `make quickstart` | Build and start the full environment |
| `make interactive` | Enter SQL and inspect before/after, processed events, and ES data |
| `make seed` | Insert one source row |
| `make verify` | Query the destination index |
| `make logs` | Follow the CDC application logs |
| `make status` | Show container health and status |
| `make down` | Remove containers, network, and volumes for a clean retry |
| `make test` | Run all Go tests |
| `make check` | Run vet and the repository test suite |

The example application is in `cmd/quickstart/main.go`. Its connection settings can be overridden with `MYSQL_ADDRESS`, `MYSQL_USERNAME`, `MYSQL_PASSWORD`, `ELASTICSEARCH_URL`, and `ELASTICSEARCH_INDEX` when running the binary outside Compose.

If a service does not become healthy, inspect `docker compose ps` and `docker compose logs mysql elasticsearch quickstart-cdc`. Running `make down` resets the initialized database and Elasticsearch data.
