#!/usr/bin/env bash
set -euo pipefail

# Parada simplificada de todos los servicios en standalone.

echo "Parando Airflow..."
pkill -f "airflow webserver" || true
pkill -f "airflow scheduler" || true
pkill -f "gunicorn: master" || true

echo "Parando Hive metastore..."
pkill -f "hive --service metastore" || true
pkill -f "HiveMetaStore" || true
pkill -f "org.apache.hadoop.util.RunJar" || true

echo "Parando Kafka..."
pkill -f "kafka.Kafka" || true

echo "Parando YARN/HDFS..."
/home/hadoop/hadoop/sbin/stop-yarn.sh || true
/home/hadoop/hadoop/sbin/stop-dfs.sh || true

echo "Hecho."

