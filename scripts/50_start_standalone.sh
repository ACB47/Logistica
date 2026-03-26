#!/usr/bin/env bash
set -euo pipefail

# Arranque simplificado en standalone (solo master, sin cluster de slaves).
# Levanta: HDFS, YARN (opcional), Hive Metastore, Kafka local y Airflow.

PROJECT_HOME="${PROJECT_HOME:-/home/hadoop/PROYECTOLOGISTICA}"
KAFKA_HOME="${KAFKA_HOME:-/home/hadoop/Descargas/kafka_2.13-4.1.1}"

echo "== 1) HDFS/YARN =="
/home/hadoop/hadoop/sbin/start-dfs.sh
/home/hadoop/hadoop/sbin/start-yarn.sh || true

echo "== 2) Hive Metastore =="
nohup /home/hadoop/hive/bin/hive --service metastore > /tmp/hive_metastore_master.log 2>&1 &

echo "== 3) Kafka local =="
mkdir -p /tmp/kafka_pids
if ! "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server master:9092 --list >/dev/null 2>&1; then
  nohup "$KAFKA_HOME/bin/kafka-server-start.sh" "$KAFKA_HOME/config/kraft/server.properties" > /tmp/kafka_pids/kafka_master.log 2>&1 &
  sleep 6
fi
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server master:9092 --create --if-not-exists --topic datos_crudos --partitions 1 --replication-factor 1 || true
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server master:9092 --create --if-not-exists --topic alertas_globales --partitions 1 --replication-factor 1 || true
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server master:9092 --create --if-not-exists --topic datos_filtrados --partitions 1 --replication-factor 1 || true

echo "== 4) Airflow =="
cd "$PROJECT_HOME"
source .venv-airflow/bin/activate
export AIRFLOW_HOME="$PROJECT_HOME/.airflow"
mkdir -p "$AIRFLOW_HOME"
rm -rf "$AIRFLOW_HOME/dags"
ln -s "$PROJECT_HOME/airflow/dags" "$AIRFLOW_HOME/dags"
airflow db init >/dev/null
nohup airflow webserver --port 8080 > /tmp/airflow_webserver.log 2>&1 &
nohup airflow scheduler > /tmp/airflow_scheduler.log 2>&1 &

echo
echo "Standalone listo."
echo "- Airflow: http://master:8080 (admin/admin)"
echo "- Kafka topics:"
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server master:9092 --list || true
echo "- Spark YARN weather staging: bash scripts/64_run_weather_filtered_staging_yarn.sh"
