-- Template: drive binlog volume against a disposable schema (adjust database/table names).
-- Run with: mysql -h HOST -u USER -p < scripts/perf/mysql_load_template.sql
-- Pair with application metrics: rows/sec applied vs handler/sink throughput.

CREATE DATABASE IF NOT EXISTS cdc_perf;
USE cdc_perf;

CREATE TABLE IF NOT EXISTS stress_rows (
  id BIGINT NOT NULL AUTO_INCREMENT,
  payload VARCHAR(512) NOT NULL,
  updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (id)
) ENGINE=InnoDB;

-- Example burst (repeat or wrap in a shell loop for sustained load):
INSERT INTO stress_rows (payload)
SELECT REPEAT('x', 256) FROM (
  SELECT 0 AS n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
  UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9
) a, (
  SELECT 0 AS n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
  UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9
) b;
