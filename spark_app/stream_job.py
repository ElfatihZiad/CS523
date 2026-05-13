"""
Spark Structured Streaming job for the CS523 final project.

Reads the sensor.community IoT feed from Kafka topic `iot-data`, parses each
record, explodes its per-metric `sensordatavalues` array, and produces two
streams:

  1. windowed_stats — 5-minute tumbling-window avg/min/max/count grouped by
     country + metric (e.g. P1, P2, temperature, humidity). Written to the
     Hive table `iot.iot_window_stats` once each window closes.
  2. anomalies — individual readings where PM2.5 (value_type = "P2") exceeds
     150 µg/m³ ("very unhealthy" threshold). Written to `iot.iot_anomalies`.

Run the DDL in hive_ddl.sql once before starting the job. Submit from inside
the cs523bdt-lab container:

    beeline -u jdbc:hive2://localhost:10000 -f /opt/spark_app/hive_ddl.sql
    /opt/spark_app/submit.sh
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
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
WINDOW_TABLE = f"{HIVE_DB}.iot_window_stats"
ANOMALY_TABLE = f"{HIVE_DB}.iot_anomalies"

CHECKPOINT_ROOT = "hdfs:///user/streaming/checkpoints"

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
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def write_window_stats(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    flat = batch_df.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("country"),
        col("metric"),
        col("avg_val"),
        col("min_val"),
        col("max_val"),
        col("sample_count"),
    ).persist()
    flat.write.mode("append").insertInto(WINDOW_TABLE)
    flat.write.mode("append").jdbc(JDBC_URL, "iot_window_stats", properties=JDBC_PROPS)
    flat.unpersist()


def write_anomalies(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    flat = batch_df.select(
        col("sensor_id"),
        col("event_time"),
        col("country"),
        col("sensor_type"),
        col("metric"),
        col("value"),
        col("alert"),
    ).persist()
    flat.write.mode("append").insertInto(ANOMALY_TABLE)
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


def parse_records(raw_df):
    return (
        raw_df
        .select(from_json(col("value").cast("string"), RECORD_SCHEMA).alias("r"))
        .select("r.*")
        .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("sdv", explode(col("sensordatavalues")))
        .select(
            col("id").alias("sensor_id"),
            col("event_time"),
            col("location.country").alias("country"),
            col("sensor.sensor_type.name").alias("sensor_type"),
            col("sdv.value_type").alias("metric"),
            col("sdv.value").cast("double").alias("value"),
        )
        .filter(col("value").isNotNull())
        .filter(col("event_time").isNotNull())
    )


def windowed_aggregates(parsed_df):
    return (
        parsed_df
        .withWatermark("event_time", "10 minutes")
        .groupBy(
            window(col("event_time"), "5 minutes"),
            col("country"),
            col("metric"),
        )
        .agg(
            avg("value").alias("avg_val"),
            smin("value").alias("min_val"),
            smax("value").alias("max_val"),
            count("value").alias("sample_count"),
        )
    )


def anomaly_stream(parsed_df):
    return (
        parsed_df
        .filter((col("metric") == "P2") & (col("value") > 150))
        .withColumn("alert", lit("HIGH_PM25"))
    )


def main() -> None:
    spark = build_spark()
    parsed = parse_records(kafka_source(spark))

    agg_query = (
        windowed_aggregates(parsed).writeStream
        .outputMode("append")
        .foreachBatch(write_window_stats)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/windowed_stats")
        .queryName("windowed_stats")
        .trigger(processingTime="30 seconds")
        .start()
    )

    anom_query = (
        anomaly_stream(parsed).writeStream
        .outputMode("append")
        .foreachBatch(write_anomalies)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/anomalies")
        .queryName("anomalies")
        .trigger(processingTime="15 seconds")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
