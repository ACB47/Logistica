#!/usr/bin/env bash
set -euo pipefail

echo "== OS =="
uname -a || true

echo
echo "== Java =="
java -version 2>&1 | head -n 2 || true

echo
echo "== Hadoop/HDFS/YARN =="
(hadoop version 2>/dev/null || true) | head -n 2
(hdfs version 2>/dev/null || true) | head -n 2
(yarn version 2>/dev/null || true) | head -n 2

echo
echo "== Spark =="
(spark-submit --version 2>&1 || true) | head -n 15

echo
echo "== Hive/Beeline =="
(hive --version 2>/dev/null || true) | head -n 5
(beeline --version 2>/dev/null || true) | head -n 2

echo
echo "== Kafka CLI =="
command -v kafka-topics.sh || true
command -v kafka-topics || true
(kafka-topics.sh --version 2>/dev/null || true)
(kafka-topics --version 2>/dev/null || true)

echo
echo "== Airflow =="
command -v airflow || true
(airflow version 2>/dev/null || true)

echo
echo "== NiFi =="
ls -ld /home/hadoop/nifi-* 2>/dev/null || true

