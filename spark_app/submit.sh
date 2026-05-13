#!/usr/bin/env bash
# Submit the IoT streaming job. Run from inside the cs523bdt-lab container:
#   docker exec -it cs523bdt-finalproject bash
#   /opt/spark_app/submit.sh
#
# Override the connector version if the image ships a different Spark build:
#   SPARK_KAFKA_PKG=org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 /opt/spark_app/submit.sh
set -euo pipefail

KAFKA_PKG="${SPARK_KAFKA_PKG:-org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2}"
PG_PKG="${SPARK_POSTGRES_PKG:-org.postgresql:postgresql:42.7.3}"

exec spark-submit \
  --master "local[*]" \
  --packages "$KAFKA_PKG,$PG_PKG" \
  /opt/spark_app/stream_job.py
