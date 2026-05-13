#!/usr/bin/env bash
# Runs once on first Postgres boot. Creates the iot_dashboard DB used by
# Grafana + the Spark JDBC sink, alongside the existing hive_metastore DB.
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-'SQL'
    CREATE DATABASE iot_dashboard;
SQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname iot_dashboard <<-'SQL'
    CREATE TABLE IF NOT EXISTS iot_window_stats (
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
    CREATE INDEX IF NOT EXISTS idx_window_stats_end    ON iot_window_stats (window_end);
    CREATE INDEX IF NOT EXISTS idx_window_stats_metric ON iot_window_stats (metric, country);
    CREATE INDEX IF NOT EXISTS idx_window_stats_region ON iot_window_stats (region);

    CREATE TABLE IF NOT EXISTS iot_anomalies (
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
    CREATE INDEX IF NOT EXISTS idx_anomalies_time   ON iot_anomalies (event_time);
    CREATE INDEX IF NOT EXISTS idx_anomalies_region ON iot_anomalies (region);
SQL
