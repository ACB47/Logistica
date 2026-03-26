#!/usr/bin/env bash
set -euo pipefail

PROJECT_HOME="${PROJECT_HOME:-/home/hadoop/PROYECTOLOGISTICA}"
BOOTSTRAP="${BOOTSTRAP:-master:9092}"
TOPIC="${TOPIC:-datos_filtrados}"
SPARK_SUBMIT_BIN="${SPARK_SUBMIT_BIN:-spark-submit}"

echo "Ejecutando weather staging en YARN"
echo "PROJECT_HOME=${PROJECT_HOME}"
echo "BOOTSTRAP=${BOOTSTRAP}"
echo "TOPIC=${TOPIC}"

"${SPARK_SUBMIT_BIN}" \
  --master yarn \
  --deploy-mode client \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  "${PROJECT_HOME}/jobs/spark/01_weather_filtered_to_staging.py" \
  --bootstrap "${BOOTSTRAP}" \
  --topic "${TOPIC}"
