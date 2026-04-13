# Checklist de Entrega Final

**Fecha**: 13/04/2026
**Entrega**: Proyecto Big Data Logística - Big Data Marítima y Cadena de Suministro

---

## 1. Stack Docker

- [x] `./start.sh` ejecuta sin errores
- [x] servicios: Kafka, Spark, HDFS, Hive, Cassandra, Airflow, NiFi, Zeppelin
- [x] puertos expuestos correctamente
- [x] memoria/CPU limitada para entorno local

**Verificación**:
```bash
docker ps --format "table {{.Names}}\t{{.Status}}"
```

---

## 2. Ingesta

- [x] Topic `datos_crudos` creado y funcionando
- [x] Topic `alertas_globales` creado y funcionando
- [x] Productor GPS (`ships_gps_producer.py`)
- [x] Productor alertas (`alerts_producer.py`)
- [x] Consumidor raw→HDFS (`kafka_to_hdfs_raw.py`)
- [x] Trazabilidad en raw (`_ingestion_ts`, `_ingestion_host`)

**Verificación**:
```bash
bash scripts/70_kafka_topic_verification.sh
```

---

## 3. Raw y Staging

- [x] HDFS raw paths: `/hadoop/logistica/raw/{ships,clima,noticias}/`
- [x] Tablas staging: `stg_ships`, `stg_alerts_clima`, `stg_alerts_noticias`
- [x] Spark job: `01_raw_to_staging.py`
- [x] Timestamps normalizados a UTC

**Verificación**:
```bash
docker exec -it namenode hdfs dfs -ls /hadoop/logistica/raw/ships/
```

---

## 4. Dimensiones

- [x] `dim_ports`
- [x] `dim_routes`
- [x] `dim_warehouse`
- [x] `dim_articles_valladolid`
- [x] `fact_customer_orders_douai`
- [x] `fact_article_gantt`

**Verificación**:
```bash
docker exec -it spark /opt/spark/bin/beeline -u "jdbc:hive2://hiveserver:10000" -e "SHOW TABLES IN logistica;"
```

---

## 5. Transformación y Analytics

- [x] Enrichment: `02_weather_port_enrichment.py`
- [x] Facts: `03_weather_operational_fact.py`
- [x] Grafos: `02_graph_metrics.py`
- [x] Scoring: `03_score_and_alert.py`
- [x] Contingencia: `03_air_recovery_options.py`

**Verificación**:
```bash
bash scripts/63_run_weather_filtered_staging.sh full
```

---

## 6. Persistencia

- [x] Hive: todas las tablas curadas
- [x] Cassandra: `vehicle_latest_state`

**Verificación**:
```bash
docker exec -it cassandra cqlsh -e "SELECT * FROM logistica.vehicle_latest_state LIMIT 1;"
```

---

## 7. Orquestación

- [x] Airflow DAG: `logistica_kdd_microbatch`
- [x] Frecuencia: cada 15 minutos
- [x] Reintentos configurados

**Verificación**:
```bash
docker exec -it airflow-webserver airflow dags list
```

---

## 8. Dashboard

- [x] Streamlit app (`dashboard/app.py`)
- [x] Pestañas: Stock Valladolid, Control Tower, Gantt
- [x] Gantt usa `use_container_width=True`

**Verificación**:
```bash
streamlit run dashboard/app.py
# Abrir http://localhost:8501
```

---

## 9. Documentation

- [x] Kafka Topic Contract (`docs/KAFKA_TOPIC_CONTRACT.md`)
- [x] HDFS Routes (`docs/HDFS_ROUTES.md`)
- [x] Dimension Origins (`docs/DIMENSION_ORIGINS.md`)
- [x] Streaming Approach (`docs/STREAMING_APPROACH.md`)
- [x] Demo Script (`docs/DEMO_SCRIPT.md`)
- [x] Memory Structure (`docs/MEMORY_STRUCTURE.md`)
- [x] Architecture Diagram (`docs/ARCHITECTURE.md`)
- [x] Zeppelin Notebooks (`zeppelin/`)

---

## 10. Validación

- [x] Script de validación: `scripts/80_validate_full_pipeline.sh`
- [x] Syntax check Python
- [x] Docker compose config validado

**Verificación**:
```bash
bash scripts/80_validate_full_pipeline.sh
```

---

## Notas para la defensa

1. **Enfoque**: Micro-batch (streaming como evidencia complementaria)
2. **Datos**: Simulados para demo, arquitectura lista para producción
3. **Filosofía**: Trazabilidad completa, debuggable, re-ejecutable
4. **Limitaciones**: Entorno Docker/local, datos simulados
5. **Mejoras**: API real stock, streaming en cluster dedicado

---

## Comandos rápidos para demo

```bash
# Iniciar todo
./start.sh

# Ejecutar pipeline
bash scripts/66_rebuild_hive_demo_tables.sh

# Dashboard
bash scripts/67_run_dashboard.sh

# Validar
bash scripts/80_validate_full_pipeline.sh
```