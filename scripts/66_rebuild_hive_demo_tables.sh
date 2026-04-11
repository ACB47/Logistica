#!/usr/bin/env bash
set -euo pipefail

COMPOSE_CMD=(docker compose)

echo "Reconstruyendo tablas Hive de demo..."

"${COMPOSE_CMD[@]}" exec -T spark spark-submit /home/jovyan/jobs/spark/01_load_master_dimensions.py
"${COMPOSE_CMD[@]}" exec -T spark spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /home/jovyan/jobs/spark/01_weather_filtered_to_staging.py --bootstrap kafka:9092 --topic datos_filtrados_ok
"${COMPOSE_CMD[@]}" exec -T spark spark-submit /home/jovyan/jobs/spark/02_weather_port_enrichment.py
"${COMPOSE_CMD[@]}" exec -T spark spark-submit /home/jovyan/jobs/spark/03_weather_operational_fact.py
"${COMPOSE_CMD[@]}" exec -T spark spark-submit /home/jovyan/jobs/spark/01_raw_to_staging.py
"${COMPOSE_CMD[@]}" exec -T spark spark-submit --packages graphframes:graphframes:0.8.3-spark3.5-s_2.12 /home/jovyan/jobs/spark/02_graph_metrics.py
"${COMPOSE_CMD[@]}" exec -T spark spark-submit /home/jovyan/jobs/spark/03_score_and_alert.py

echo "OK - tablas Hive de demo reconstruidas"
"${COMPOSE_CMD[@]}" exec -T spark spark-sql -e "SHOW TABLES IN logistica"
