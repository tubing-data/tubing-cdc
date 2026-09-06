.PHONY: quickstart interactive seed verify demo logs status down test check

quickstart:
	docker compose up -d --build

interactive: quickstart
	@bash scripts/quickstart-interactive.sh

seed:
	docker compose exec -T mysql mysql -uroot -proot cdc_test -e "INSERT INTO orders (customer_name, customer_email, status, amount_cents) VALUES ('Ada Lovelace', 'ada@example.com', 'pending', 1299);"

verify:
	curl --fail --silent --show-error 'http://localhost:9200/cdc-orders/_search?pretty'

demo: quickstart
	@echo "Waiting for the CDC process to connect..."
	@sleep 3
	$(MAKE) seed
	@echo "Waiting for the event to reach Elasticsearch..."
	@sleep 2
	$(MAKE) verify

logs:
	docker compose logs -f quickstart-cdc

status:
	docker compose ps

down:
	docker compose down -v --remove-orphans

test:
	go test ./...

check:
	bash scripts/codex/check.sh
