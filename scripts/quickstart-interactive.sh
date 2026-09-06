#!/usr/bin/env bash
set -euo pipefail

compose=(docker compose)

echo
echo "Interactive tubing-cdc demo"
echo "Enter SQL that changes cdc_test.orders. After every statement, this command"
echo "shows the received before/after event, the processed event, and Elasticsearch."
echo
echo "Examples:"
echo "  INSERT INTO orders (customer_name, customer_email, status, amount_cents) VALUES ('Grace Hopper', 'grace@example.org', 'pending', 2499);"
echo "  UPDATE orders SET status='paid' WHERE id=1;"
echo "  DELETE FROM orders WHERE id=1;"
echo
echo "Type quit to exit."

while true; do
  echo
  IFS= read -r -p "mysql> " sql
  case "${sql}" in
    quit|exit|\\q)
      break
      ;;
    "")
      continue
      ;;
  esac

  event_since="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  if ! "${compose[@]}" exec -T mysql mysql -uroot -proot cdc_test -e "${sql}"; then
    echo "SQL failed; fix it and try again." >&2
    continue
  fi

  # CDC and the sink are synchronous, but binlog delivery is asynchronous.
  sleep 1
  echo
  echo "--- CDC pipeline output for this change ---"
  "${compose[@]}" logs --since "${event_since}" --no-log-prefix quickstart-cdc
  echo
  echo "--- Documents currently stored in Elasticsearch ---"
  curl --silent --show-error 'http://localhost:9200/cdc-orders/_search?pretty'
  echo
done
