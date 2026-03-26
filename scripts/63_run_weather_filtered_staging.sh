#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-full}"
BOOTSTRAP="${BOOTSTRAP:-kafka:9092}"
TOPIC="${TOPIC:-datos_filtrados}"

case "${MODE}" in
  full)
    echo "Preparando servicios necesarios del compose completo..."
    docker-compose up -d kafka namenode datanode spark
    echo "Lanzando spark-submit en el contenedor spark..."
    docker-compose exec -T spark spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /home/jovyan/jobs/spark/01_weather_filtered_to_staging.py \
      --bootstrap "${BOOTSTRAP}" \
      --topic "${TOPIC}"
    ;;
  simple)
    echo "El modo simple no es adecuado para este job porque no levanta HDFS/Hive."
    echo "Usa: bash scripts/63_run_weather_filtered_staging.sh full"
    exit 1
    ;;
  cluster)
    echo "Ejecuta este comando en el nodo con Spark/Hive configurados:"
    echo "spark-submit jobs/spark/01_weather_filtered_to_staging.py --bootstrap ${BOOTSTRAP} --topic ${TOPIC}"
    ;;
  *)
    echo "Modo invalido: ${MODE}"
    echo "Usa: full | simple | cluster"
    exit 1
    ;;
esac
