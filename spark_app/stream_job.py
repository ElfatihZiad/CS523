"""
Spark Structured Streaming job for the CS523 final project.

Reads the sensor.community IoT feed from Kafka topic `iot-data`, parses each
record, explodes its per-metric `sensordatavalues` array, and produces two
streams:

  1. windowed_stats — 5-minute tumbling-window avg/min/max/count grouped by
     country + metric (e.g. P1, P2, temperature, humidity).
  2. anomalies — individual readings where PM2.5 (value_type = "P2") exceeds
     150 µg/m³ ("very unhealthy" threshold).

Each stream is enriched via a Spark SQL broadcast join with a static country
metadata CSV stored on HDFS (Part 5 bonus), then written twice: as Parquet to
the HDFS path that the Hive table DDL points at (so beeline can SELECT from
`iot.iot_window_stats` / `iot.iot_anomalies` directly), and as JDBC rows to
the Postgres `iot_dashboard` DB consumed by Grafana.

Run the DDL once before starting the job. Submit from inside the
cs523bdt-lab container:

    beeline -u jdbc:hive2://localhost:10000 -n root -f /opt/spark_app/hive_ddl.sql
    /opt/spark_app/submit.sh
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    broadcast,
    col,
    count,
    explode,
    from_json,
    lit,
    max as smax,
    min as smin,
    to_timestamp,
    window,
)
from pyspark.sql.types import (
    ArrayType,
    LongType,
    StringType,
    StructField,
    StructType,
)

KAFKA_BOOTSTRAP = "kafka-server-2:9092"
KAFKA_TOPIC = "iot-data"

HIVE_DB = "iot"
# We write Parquet directly to the Hive table's HDFS LOCATION; Hive picks up
# the files via the external/managed table DDL in hive_ddl.sql. Avoids needing
# Spark to be wired to the Hive metastore.
HIVE_WAREHOUSE = "hdfs:///user/hive/warehouse"
WINDOW_PATH = f"{HIVE_WAREHOUSE}/{HIVE_DB}.db/iot_window_stats"
ANOMALY_PATH = f"{HIVE_WAREHOUSE}/{HIVE_DB}.db/iot_anomalies"

CHECKPOINT_ROOT = "hdfs:///user/streaming/checkpoints"

# Static country lookup uploaded by setup_hdfs.sh — broadcast-joined onto the
# stream so every record carries country_name + region (Part 5 bonus:
# Spark SQL join with static HDFS data).
COUNTRY_LOOKUP_PATH = "hdfs:///data/countries.csv"

JDBC_URL = "jdbc:postgresql://postgres-db:5432/iot_dashboard"
JDBC_PROPS = {
    "user": "hive",
    "password": "hivepassword",
    "driver": "org.postgresql.Driver",
}

RECORD_SCHEMA = StructType([
    StructField("id", LongType()),
    StructField("timestamp", StringType()),
    StructField("location", StructType([
        StructField("country", StringType()),
        StructField("latitude", StringType()),
        StructField("longitude", StringType()),
    ])),
    StructField("sensor", StructType([
        StructField("id", LongType()),
        StructField("sensor_type", StructType([
            StructField("name", StringType()),
        ])),
    ])),
    StructField("sensordatavalues", ArrayType(StructType([
        StructField("value", StringType()),
        StructField("value_type", StringType()),
    ]))),
])


def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("IoTStreamProcessor")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def write_window_stats(batch_df, batch_id):
    # Use a SQL-only emptiness check; .rdd.isEmpty() forces cloudpickle, which
    # is incompatible between Spark 3.1's bundled cloudpickle and Python 3.10+.
    if batch_df.limit(1).count() == 0:
        return
    flat = batch_df.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("country"),
        col("country_name"),
        col("region"),
        col("metric"),
        col("avg_val"),
        col("min_val"),
        col("max_val"),
        col("sample_count"),
    ).persist()
    flat.write.mode("append").parquet(WINDOW_PATH)
    flat.write.mode("append").jdbc(JDBC_URL, "iot_window_stats", properties=JDBC_PROPS)
    flat.unpersist()


def write_anomalies(batch_df, batch_id):
    if batch_df.limit(1).count() == 0:
        return
    flat = batch_df.select(
        col("sensor_id"),
        col("event_time"),
        col("country"),
        col("country_name"),
        col("region"),
        col("sensor_type"),
        col("metric"),
        col("value"),
        col("alert"),
    ).persist()
    flat.write.mode("append").parquet(ANOMALY_PATH)
    flat.write.mode("append").jdbc(JDBC_URL, "iot_anomalies", properties=JDBC_PROPS)
    flat.unpersist()


def kafka_source(spark: SparkSession):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )



