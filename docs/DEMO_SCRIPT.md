# Demo: Pipeline Logistica KDD (5-10 minutos)

Este guion esta diseñado para la defensa oral del proyecto.

---

## Pre-demo: Verificar stack

```bash
docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E "kafka|spark|hiveserver|cassandra|airflow|nifi|namenode"
```

Todos deben estar "Up". Si no, iniciar:
```bash
./start.sh
```

---

## Minute 1: Ingesta

### Productores Kafka
```bash
# Terminal 1: GPS barcos
python3 ingesta/productores/ships_gps_producer.py --bootstrap master:9092 --topic datos_crudos --ships 5

# Terminal 2: Alertas
python3 ingesta/productores/alerts_producer.py --bootstrap master:9092 --topic alertas_globales
```

### Topics Kafka
```bash
docker exec -it master kafka-topics.sh --bootstrap-server master:9092 --list
```

### Consumidor Raw -> HDFS
```bash
python3 ingesta/consumidores/kafka_to_hdfs_raw.py --bootstrap master:9092
```

Verificar datos en HDFS:
```bash
docker exec -it namenode hdfs dfs -ls /hadoop/logistica/raw/ships/
```

---

## Minute 2-3: Staging

```bash
spark-submit jobs/spark/01_raw_to_staging.py
```

Verificar tablas:
```bash
docker exec -it spark /opt/spark/bin/beeline -u "jdbc:hive2://hiveserver:10000" -e "SELECT COUNT(*) FROM logistica.stg_ships;"
```

---

## Minute 4: Dimensiones

```bash
spark-submit jobs/spark/01_load_master_dimensions.py
```

Verificar dimensiones:
```bash
docker exec -it spark /opt/spark/bin/beeline -u "jdbc:hive2://hiveserver:10000" -e "SHOW TABLES IN logistica;"
```

---

## Minute 5-6: Facts y Analytics

```bash
spark-submit jobs/spark/02_weather_port_enrichment.py
spark-submit jobs/spark/03_weather_operational_fact.py
spark-submit jobs/spark/03_air_recovery_options.py
```

Verificar facts:
```bash
docker exec -it spark /opt/spark/bin/beeline -u "jdbc:hive2://hiveserver:10000" -e "SELECT * FROM logistica.fact_weather_operational LIMIT 3;"
```

---

## Minute 7: Grafos

```bash
spark-submit --packages graphframes:graphframes:0.8.3-spark3.5-s_2.12 jobs/spark/02_graph_metrics.py
```

Verificar nodos criticos:
```bash
docker exec -it spark /opt/spark/bin/beeline -u "jdbc:hive2://hiveserver:10000" -e "SELECT * FROM logistica.graph_critical_nodes;"
```

---

## Minute 8: Cassandra

```bash
spark-submit jobs/spark/04_latest_vehicle_state_cassandra.py
```

Verificar estado ultimo:
```bash
docker exec -it logistica-cassandra-1 cqlsh -e "SELECT * FROM logistica.vehicle_latest_state LIMIT 3;"
```

---

## Minute 9-10: Dashboard

```bash
source .venv-jobs/bin/activate
streamlit run dashboard/app.py --server.port 8501
```

Abrir en navegador: **http://localhost:8501**

Navegar por:
- Pestana "Stock Valladolid" -> tabla con articulos
- Pestana "Control Tower" -> pedidos, Gantt, contingencia

---

## Puntos clave para destacar

1. **End-to-end**: Desde API/Kafka hasta dashboard
2. **Trazabilidad**: Timestamps en cada capa
3. **Multi-tecnologia**: Kafka, Spark, Hive, Cassandra, Airflow
4. **Grafos**: rutas criticas
5. **Dashboard**: Visualizacion operacional

---

## Troubleshooting rapido

Si algo falla:

```bash
# Ver logs de Spark
docker logs logistica-spark-1

# Verificar HDFS
docker exec -it namenode hdfs dfsadmin -report

# Reiniciar servicio
docker restart logistica-spark-1
```