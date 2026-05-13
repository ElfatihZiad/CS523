-- Manual Postgres bootstrap (mirror of postgres-init/01-iot-dashboard.sh).
-- Only needed if you already had the hive_db_data volume before adding the
-- init script. Run from the host:
--   docker exec -it hive-metastore-db-2 psql -U hive -d hive_metastore \
--     -c "CREATE DATABASE iot_dashboard;"
--   docker exec -i hive-metastore-db-2 psql -U hive -d iot_dashboard \
--     < spark_app/postgres_ddl.sql

CREATE TABLE IF NOT EXISTS iot_window_stats (
  window_start TIMESTAMP NOT NULL,
  window_end   TIMESTAMP NOT NULL,
  country      TEXT,
  metric       TEXT NOT NULL,
  avg_val      DOUBLE PRECISION,
  min_val      DOUBLE PRECISION,
  max_val      DOUBLE PRECISION,
  sample_count BIGINT
);
CREATE INDEX IF NOT EXISTS idx_window_stats_end    ON iot_window_stats (window_end);
CREATE INDEX IF NOT EXISTS idx_window_stats_metric ON iot_window_stats (metric, country);

CREATE TABLE IF NOT EXISTS iot_anomalies (
  sensor_id   BIGINT,
  event_time  TIMESTAMP NOT NULL,
  country     TEXT,
  sensor_type TEXT,
  metric      TEXT,
  value       DOUBLE PRECISION,
  alert       TEXT
);
CREATE INDEX IF NOT EXISTS idx_anomalies_time ON iot_anomalies (event_time);
