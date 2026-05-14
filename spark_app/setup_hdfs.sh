#!/usr/bin/env bash
# One-time HDFS bootstrap. Run inside cs523bdt-finalproject:
#   /opt/spark_app/setup_hdfs.sh
#
# Creates the directories the streaming job expects and uploads the static
# country metadata CSV used by the Spark SQL stream-static join.
set -euo pipefail

hdfs dfs -mkdir -p /data
hdfs dfs -mkdir -p /user/hive/warehouse/iot.db
hdfs dfs -mkdir -p /user/streaming/checkpoints

hdfs dfs -put -f /opt/spark_app/data/countries.csv /data/countries.csv

echo "--- /data ---"
hdfs dfs -ls /data
echo "--- /user/hive/warehouse/iot.db ---"
hdfs dfs -ls /user/hive/warehouse/iot.db || true
