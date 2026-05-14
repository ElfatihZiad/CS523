-- Manual Postgres bootstrap (mirror of postgres-init/01-iot-dashboard.sh).
-- Only needed if you already had the hive_db_data volume before adding the
-- init script. Run from the host:
--   docker exec -it hive-metastore-db-2 psql -U hive -d hive_metastore \
--     -c "CREATE DATABASE iot_dashboard;"
--   docker exec -i hive-metastore-db-2 psql -U hive -d iot_dashboard \
--     < spark_app/postgres_ddl.sql

DROP TABLE IF EXISTS iot_window_stats;
DROP TABLE IF EXISTS iot_anomalies;

CREATE TABLE iot_window_stats (
  window_start TIMESTAMP NOT NULL,
  window_end   TIMESTAMP NOT NULL,
  country      TEXT,
  country_name TEXT,
  region       TEXT,
  metric       TEXT NOT NULL,
  avg_val      DOUBLE PRECISION,
  min_val      DOUBLE PRECISION,
  max_val      DOUBLE PRECISION,
  sample_count BIGINT
);
CREATE INDEX idx_window_stats_end    ON iot_window_stats (window_end);
CREATE INDEX idx_window_stats_metric ON iot_window_stats (metric, country);
CREATE INDEX idx_window_stats_region ON iot_window_stats (region);

CREATE TABLE iot_anomalies (
  sensor_id    BIGINT,
  event_time   TIMESTAMP NOT NULL,
  country      TEXT,
  country_name TEXT,
  region       TEXT,
  sensor_type  TEXT,
  metric       TEXT,
  value        DOUBLE PRECISION,
  alert        TEXT
);
CREATE INDEX idx_anomalies_time   ON iot_anomalies (event_time);
CREATE INDEX idx_anomalies_region ON iot_anomalies (region);
