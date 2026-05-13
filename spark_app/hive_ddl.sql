-- Hive DDL for the IoT streaming pipeline.
-- Run once from inside the cs523bdt-lab container:
--   beeline -u jdbc:hive2://localhost:10000 -f /opt/spark_app/hive_ddl.sql

CREATE DATABASE IF NOT EXISTS iot;
USE iot;

-- Drop-and-recreate so schema changes (e.g. country_name / region columns
-- added by the Part 5 static join) are picked up. Data lives in Parquet on
-- HDFS; recreating the table only drops metadata.
DROP TABLE IF EXISTS iot_window_stats;
DROP TABLE IF EXISTS iot_anomalies;

CREATE TABLE iot_window_stats (
  window_start TIMESTAMP,
  window_end   TIMESTAMP,
  country      STRING,
  country_name STRING,
  region       STRING,
  metric       STRING,
  avg_val      DOUBLE,
  min_val      DOUBLE,
  max_val      DOUBLE,
  sample_count BIGINT
)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/iot.db/iot_window_stats';

CREATE TABLE iot_anomalies (
  sensor_id    BIGINT,
  event_time   TIMESTAMP,
  country      STRING,
  country_name STRING,
  region       STRING,
  sensor_type  STRING,
  metric       STRING,
  value        DOUBLE,
  alert        STRING
)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/iot.db/iot_anomalies';
