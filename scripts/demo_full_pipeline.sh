#!/bin/bash
set -e

echo "=============================================="
echo "  DEMO: Pipeline Completo Logistica KDD"
echo "  Duracion estimada: 8-10 minutos"
echo "=============================================="
echo ""

echo ">>> PARTE 1: Stack Docker (1 min)"
echo "---"
echo "Verificando que el stack esta levantado..."
docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E "kafka|spark|hive|cassandra|airflow|nifi" || true
echo ""

echo ">>> PARTE 2: Productores Kafka (1 min)"
echo "---"
echo "Iniciando productores en background..."
docker run --rm --network logistica_default -d --name demo-producer-ships \
  logistica bash -c "cd /app && python3 ingesta/productores/ships_gps_producer.py --bootstrap kafka:9092 --interval-sec 2"

docker run --rm --network logistica_default -d --name demo-producer-alerts \
  logistica bash -c "cd /app && python3 ingesta/productores/alerts_producer.py --bootstrap kafka:9092 --interval-sec 5"

echo "Productores iniciados. Verificando topics..."
sleep 3
docker exec -it master kafka-topics.sh --bootstrap-server master:9092 --list
echo ""

echo ">>> PARTE 3: Consumidor Raw->HDFS (1 min)"
echo "---"
echo "Iniciando consumidor raw..."
docker run --rm --network logistica_default -d --name demo-consumer-raw \
  logistica bash -c "cd /app && python3 ingesta/consumidores/kafka_to_hdfs_raw.py --bootstrap master:9092 --flush-every-sec 10"

echo "Esperando datos en HDFS..."
sleep 5
docker exec -it namenode hdfs dfs -ls /hadoop/logistica/raw/ships/ | head -10
echo ""

echo ">>> PARTE 4: Spark Jobs - Staging (2 min)"
echo "---"
echo "Ejecutando 01_raw_to_staging.py..."
docker exec -it logistica-spark-1 spark-submit /home/jovyan/jobs/spark/01_raw_to_staging.py

echo "Verificando tablas staging..."
docker exec -it spark /opt/spark/bin/beeline -u "jdbc:hive2://hiveserver:10000" -e "SELECT COUNT(*) as ships_count FROM logistica.stg_ships;"
docker exec -it spark /opt/spark/bin/beeline -u "jdbc:hive2://hiveserver:10000" -e "SELECT COUNT(*) as alerts_count FROM logistica.stg_alerts_clima;"
echo ""

echo ">>> PARTE 5: Spark Jobs - Dimensiones (1 min)"
echo "---"
echo "Ejecutando 01_load_master_dimensions.py..."
docker exec -it logistica-spark-1 spark-submit /home/jovyan/jobs/spark/01_load_master_dimensions.py

echo "Verificando dimensiones..."
docker exec -it spark /opt/spark/bin/beeline -u "jdbc:hive2://hiveserver:10000" -e "SHOW TABLES IN logistica;"
echo ""

echo ">>> PARTE 6: Spark Jobs - Facts y Alertas (2 min)"
echo "---"
echo "Ejecutando enrichment y scoring..."
docker exec -it logistica-spark-1 spark-submit /home/jovyan/jobs/spark/02_weather_port_enrichment.py
docker exec -it logistica-spark-1 spark-submit /home/jovyan/jobs/spark/03_weather_operational_fact.py

echo "Verificando facts..."
docker exec -it spark /opt/spark/bin/beeline -u "jdbc:hive2://hiveserver:10000" -e "SELECT * FROM logistica.fact_weather_operational LIMIT 3;"
echo ""

echo ">>> PARTE 7: Cassandra - Latest State (1 min)"
echo "---"
echo "Ejecutando carga a Cassandra..."
docker exec -it logistica-spark-1 spark-submit /home/jovyan/jobs/spark/04_latest_vehicle_state_cassandra.py

echo "Verificando Cassandra..."
docker exec -it logistica-cassandra-1 cqlsh -e "SELECT * FROM logistica.vehicle_latest_state LIMIT 3;"
echo ""

echo ">>> PARTE 8: Dashboard Streamlit (1 min)"
echo "---"
echo "Dashboard disponible en: http://localhost:8501"
echo "Ejecutando dashboard..."
cd /home/ana/Descargas/PROYECTOLOGISTICA_codigo/Logistica
source .venv-jobs/bin/activate
streamlit run dashboard/app.py --server.port 8501 &
echo ""

echo "=============================================="
echo "  DEMO COMPLETADO"
echo "=============================================="
echo ""
echo "Limpieza de containers de demo..."
docker stop demo-producer-ships demo-producer-alerts demo-consumer-raw 2>/dev/null || true
docker rm demo-producer-ships demo-producer-alerts demo-consumer-raw 2>/dev/null || true

echo "Listo!"
echo ""
echo "Para ver el dashboard: http://localhost:8501"
echo "Para ver Airflow: http://localhost:8080"
echo "Para ver NiFi: http://localhost:9090"
