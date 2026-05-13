# CS523 Big Data Technologies — Final Project

End-to-end real-time IoT air-quality pipeline:
**Apache Kafka → Spark Structured Streaming → HDFS (Parquet) + Hive + Postgres → Grafana.**

Data source: [sensor.community](https://sensor.community/) — a global crowd-sourced
particulate-matter / temperature / humidity sensor network. The producer polls
the public REST API every 5 minutes and pushes records to Kafka; Spark consumes
the stream, enriches it with static country metadata via a Spark SQL broadcast
join, aggregates over 5-minute event-time windows, flags PM2.5 anomalies, and
sinks results to Hive (truth) and Postgres (Grafana cache).

## Project parts (10 + 2 bonus pts)

| Part | Pts | Where it lives |
|---|---|---|
| 1. Real-time ingestion via Kafka | 3 | [producer/job.py](producer/job.py), [docker-compose.yml](docker-compose.yml) |
| 2. Distributed processing via Spark Structured Streaming | 3 | [spark_app/stream_job.py](spark_app/stream_job.py) — `parse_records`, `windowed_aggregates`, `anomaly_stream` |
| 3. Persistent storage in Hive (Parquet on HDFS) | 2 | [spark_app/hive_ddl.sql](spark_app/hive_ddl.sql), `write_window_stats` / `write_anomalies` in `stream_job.py` |
| 4. Live Grafana dashboard | 2 | [grafana/provisioning/](grafana/provisioning/), Postgres mirror in `stream_job.py` |
| 5. **Bonus**: Spark SQL join with static HDFS dataset | +2 | `load_country_lookup` + `enrich_with_country` in `stream_job.py`, [spark_app/data/countries.csv](spark_app/data/countries.csv) |

## Architecture

```
   sensor.community
       REST API
          │
          ▼
   ┌─────────────┐    ┌───────────────────────────────────────────┐
   │  producer   │───▶│ Kafka topic: iot-data                     │
   │ (Python)    │    └───────────────────────┬───────────────────┘
   └─────────────┘                            │
                                              ▼
                          ┌──────────────────────────────────────┐
                          │ Spark Structured Streaming           │
                          │  • parse + explode sensordatavalues  │
                          │  • broadcast-join static countries   │
                          │    (HDFS:/data/countries.csv)        │
                          │  • 5-min tumbling window aggregates  │
                          │  • PM2.5 > 150 anomaly stream        │
                          └──────────────┬───────────────────────┘
                                         │ foreachBatch
                          ┌──────────────┴──────────────┐
                          ▼                             ▼
              ┌───────────────────────┐   ┌───────────────────────┐
              │ HDFS Parquet          │   │ Postgres              │
              │ /user/hive/warehouse/ │   │ iot_dashboard.*       │
              │ iot.db/{table}        │   │ (Grafana cache)       │
              │  ↑ Hive external table│   │                       │
              └───────────────────────┘   └────────────┬──────────┘
                          │                            │
                          ▼                            ▼
                     beeline (JDBC)             Grafana @ :3000
```

## Prerequisites

- Docker Desktop (≥ 6 GB RAM allocated — the lab container is heavy).
- Free host ports: **2181, 3000, 4040, 5432, 8088, 9092, 9870, 10000, 16010**.

## Quick start

```bash
# 1. Boot the entire stack
docker-compose up --build -d

# 2. (First time only on a fresh Postgres volume — auto-bootstraps from
#     postgres-init/. If you already have a hive_db_data volume, run:)
docker exec hive-metastore-db-2 psql -U hive -d hive_metastore \
  -c "CREATE DATABASE iot_dashboard;"
docker exec -i hive-metastore-db-2 psql -U hive -d iot_dashboard \
  < spark_app/postgres_ddl.sql

# 3. Bootstrap HDFS dirs + upload static country lookup
docker exec -it cs523bdt-finalproject bash -c "/opt/spark_app/setup_hdfs.sh"

# 4. Create Hive tables (points at the HDFS paths Spark writes to)
docker exec -it cs523bdt-finalproject bash -c \
  "beeline -u jdbc:hive2://localhost:10000 -f /opt/spark_app/hive_ddl.sql"

# 5. Start the streaming job
docker exec -it cs523bdt-finalproject bash
# inside the container:
/opt/spark_app/submit.sh
```

Open the Grafana dashboard: **http://localhost:3000** → *IoT Air Quality — Live*.
Anonymous viewer is enabled; admin login is `admin` / `admin`.

## Service map

| Service | Container | Port | Purpose |
|---|---|---|---|
| Zookeeper | `zookeeper-server-2` | 2181 | Kafka coordination |
| Kafka broker | `kafka-server-2` | 9092 | Topic `iot-data`, 20 MB max msg |
| Postgres | `hive-metastore-db-2` | 5432 | Hive metastore **+** `iot_dashboard` DB |
| Lab container | `cs523bdt-finalproject` | 4040, 8088, 9870, 10000, 16010 | HDFS / YARN / Hive / HBase / Spark |
| Producer | from `./producer` | — | Polls sensor.community → Kafka |
| Consumer | from `./consumer` | — | Smoke-test consumer (Part 1) |
| Grafana | `iot-grafana` | 3000 | Live dashboard |

UIs: Spark http://localhost:4040 · HDFS http://localhost:9870 · YARN
http://localhost:8088 · HBase http://localhost:16010 · Hive JDBC
`jdbc:hive2://localhost:10000`.

## Verifying each layer

```bash
# Kafka has data
docker-compose logs --tail=20 producer
docker-compose logs --tail=10 consumer

# Spark job is alive
open http://localhost:4040

# Hive table contents
docker exec -it cs523bdt-finalproject bash -c \
  "beeline -u jdbc:hive2://localhost:10000 -e 'SELECT COUNT(*) FROM iot.iot_anomalies;'"

# Underlying HDFS Parquet files
docker exec -it cs523bdt-finalproject bash -c \
  "hdfs dfs -ls /user/hive/warehouse/iot.db/iot_anomalies/"

# Postgres mirror
docker exec -it hive-metastore-db-2 psql -U hive -d iot_dashboard \
  -c "SELECT region, COUNT(*) FROM iot_anomalies GROUP BY region;"
```

## What to expect

- **Anomaly stream** triggers every 15 s. Rows appear in Postgres + HDFS within
  ~1 minute of starting the job, *if* any sensor in the world is currently
  reporting PM2.5 > 150 µg/m³. Lower the threshold in
  [spark_app/stream_job.py](spark_app/stream_job.py) (`anomaly_stream`) for
  guaranteed signal during a demo.
- **Windowed aggregates** run with `outputMode("append")` so a window only
  emits *after* its event-time + 10-min watermark has passed. Expect ~10–15
  minutes before the first window-stats row lands.
- **Grafana refresh** is 30 s. The Region time-series and Country bar chart
  only fill after the first windowed batches arrive.

## File layout

```
.
├── producer/                Kafka producer (Part 1)
├── consumer/                Smoke-test consumer (Part 1)
├── spark_app/               Spark Structured Streaming app
│   ├── stream_job.py        Main pipeline (Parts 2 + 5)
│   ├── submit.sh            spark-submit wrapper
│   ├── setup_hdfs.sh        HDFS bootstrap + static CSV upload
│   ├── hive_ddl.sql         Hive table definitions (Part 3)
│   ├── postgres_ddl.sql     Postgres mirror tables (manual fallback)
│   └── data/countries.csv   Static country metadata (Part 5)
├── postgres-init/           Auto-runs on fresh Postgres volume
├── grafana/provisioning/    Datasource + dashboard JSON (Part 4)
├── docker-compose.yml
└── README.md
```

## Troubleshooting

**`Table not found: iot.iot_anomalies` from `insertInto`**
Spark fell back to its embedded Derby metastore because `hive-site.xml` wasn't
on the classpath. The current code sidesteps the metastore entirely by writing
Parquet straight to the Hive table's `LOCATION`. If you re-introduce
`insertInto`, you'll need to wire Spark to the real metastore.

**`SupportsTriggerAvailableNow` ClassNotFoundException**
Spark / connector version mismatch. The lab image ships Spark 3.1.2; the
Kafka connector default is pinned to match. Override per-run with
`SPARK_KAFKA_PKG=org.apache.spark:spark-sql-kafka-0-10_2.12:<version>
/opt/spark_app/submit.sh`.

**`_pickle.PicklingError: Could not serialize object: IndexError: tuple index out of range`**
Spark 3.1's bundled cloudpickle can't parse Python 3.10+ bytecode. The job
avoids RDD-level ops (`.rdd.isEmpty()` replaced with a SQL-only
`.limit(1).count() == 0`).

**`WARN hdfs.DataStreamer: Caught exception java.lang.InterruptedException`**
Cosmetic — HDFS-4504. Logged when Spark closes an output stream and the async
block-transfer thread is interrupted mid-wait. No data loss.

**Schema mismatch after pulling new code**
Wipe checkpoints + recreate tables + re-upload the static lookup:
```bash
docker exec -it cs523bdt-finalproject bash -c \
  "hdfs dfs -rm -r -skipTrash /user/streaming/checkpoints \
   && hdfs dfs -rm -r -skipTrash /user/hive/warehouse/iot.db \
   && /opt/spark_app/setup_hdfs.sh \
   && beeline -u jdbc:hive2://localhost:10000 -f /opt/spark_app/hive_ddl.sql"
docker exec -i hive-metastore-db-2 psql -U hive -d iot_dashboard \
  < spark_app/postgres_ddl.sql
```

## Stopping / resetting

```bash
docker-compose down            # stop containers, keep volumes
docker-compose down -v         # nuke everything (also re-runs postgres-init on next boot)
```
