#!/usr/bin/env bash
set -euo pipefail

TMP_DIR="/tmp/logistica_vehicle_latest_state"
TMP_TSV="${TMP_DIR}/vehicle_latest_state.tsv"

mkdir -p "${TMP_DIR}"

echo "Creando esquema Cassandra si hace falta..."
docker-compose exec -T cassandra cqlsh -e "CREATE KEYSPACE IF NOT EXISTS logistica WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}; USE logistica; CREATE TABLE IF NOT EXISTS vehicle_latest_state (ship_id text PRIMARY KEY, event_ts timestamp, route_id text, origin_port text, dest_port text, warehouse text, lat double, lon double, speed_kn double, heading double, stock_on_hand int, reorder_point int, updated_at timestamp); TRUNCATE logistica.vehicle_latest_state;"

echo "Extrayendo ultimo estado por vehiculo desde Hive..."
docker-compose exec -T spark spark-submit /home/jovyan/jobs/spark/04_export_latest_vehicle_state.py > "${TMP_TSV}"

echo "Cargando filas en Cassandra..."
while IFS=$'\t' read -r ship_id event_ts route_id origin_port dest_port warehouse lat lon speed_kn heading stock_on_hand reorder_point; do
  [[ -z "${ship_id}" ]] && continue
  docker-compose exec -T cassandra cqlsh -e "INSERT INTO logistica.vehicle_latest_state (ship_id, event_ts, route_id, origin_port, dest_port, warehouse, lat, lon, speed_kn, heading, stock_on_hand, reorder_point, updated_at) VALUES ('${ship_id}', '${event_ts}', '${route_id}', '${origin_port}', '${dest_port}', '${warehouse}', ${lat}, ${lon}, ${speed_kn}, ${heading}, ${stock_on_hand}, ${reorder_point}, toTimestamp(now()));" < /dev/null
done < "${TMP_TSV}"

echo "OK - ultimo estado de vehiculos cargado en Cassandra"
docker-compose exec -T cassandra cqlsh -e "SELECT ship_id, route_id, dest_port, warehouse, stock_on_hand, reorder_point FROM logistica.vehicle_latest_state;"
