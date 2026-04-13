# Estructura de Memoria - Proyecto Big Data Logística

Este documento consolida la documentación para la memoria final.

---

## 1. Selección (Selection)

### 1.1 Contexto y Problema
- **Sector**: Cadena de suministro marítima (Shanghai → España → Valladolid)
- **Problema**: Riesgo de stock, tiempos de tránsito variables, alertas climáticas/geopolíticas
- **Objetivo**: Sistema de monitorización y alertas en tiempo casi-real

### 1.2 Fuentes de Datos
- GPS barcos (simulado via `ships_gps_producer.py`)
- Alertas climáticas (Open-Meteo via NiFi)
- Alertas geopolíticas (simulado via `alerts_producer.py`)
- Stock Valladolid (datos maestros en Hive)

**Ver**: `docs/KAFKA_TOPIC_CONTRACT.md`

---

## 2. Preprocesamiento (Preprocessing)

### 2.1 Ingesta
- Kafka topics: `datos_crudos`, `alertas_globales`, `datos_filtrados`
- Productores: `ingesta/productores/`
- Consumidor: `ingesta/consumidores/kafka_to_hdfs_raw.py`

### 2.2 Raw Landing
- HDFS: `/hadoop/logistica/raw/{ships,clima,noticias}/YYYY/MM/DD/HHMM/`
- Trazabilidad: `_ingestion_ts`, `_ingestion_host`, `_ingestion_partition`, `_ingestion_offset`

**Ver**: `docs/HDFS_ROUTES.md`

### 2.3 Staging
- Spark: `jobs/spark/01_raw_to_staging.py`
- Tablas: `stg_ships`, `stg_alerts_clima`, `stg_alerts_noticias`
- Timestamps normalizados a UTC

### 2.4 Dimensiones
- Spark: `jobs/spark/01_load_master_dimensions.py`
- Tablas: `dim_ports`, `dim_routes`, `dim_warehouse`, `dim_articles_valladolid`

**Ver**: `docs/DIMENSION_ORIGINS.md`

---

## 3. Transformación (Transformation)

### 3.1 Enrichment
- Spark: `jobs/spark/02_weather_port_enrichment.py`
- Tabla: `dim_ports_routes_weather`

### 3.2 Grafos
- Spark: `jobs/spark/02_graph_metrics.py` (GraphFrames)
- Métricas: nodos críticos, rutas más cortas, comunidades

### 3.3 Scoring y Alertas
- Spark: `jobs/spark/03_score_and_alert.py`
- Spark: `jobs/spark/03_air_recovery_options.py`
- Tablas: `fact_alerts`, `fact_air_recovery_options`

---

## 4. Evaluación (Evaluation)

### 4.1 Facts Operativos
- Spark: `jobs/spark/03_weather_operational_fact.py`
- Tabla: `fact_weather_operational`

### 4.2 Persistencia
- **Hive**: Tablas curadas (`logistica.fact_*`, `logistica.dim_*`)
- **Cassandra**: Estado último vehículos (`vehicle_latest_state`)

---

## 5. Despliegue (Deployment)

### 5.1 Orquestación
- Airflow DAG: `airflow/dags/logistica_kdd_dag.py`
- Frecuencia: cada 15 minutos (micro-batch)

**Ver**: `docs/STREAMING_APPROACH.md`

### 5.2 Dashboard
- Streamlit: `dashboard/app.py`
- Pestañas: Stock Valladolid, Control Tower, Gantt, Alertas

**Ver**: `docs/DEMO_SCRIPT.md`

---

## 6. Capturas Obligatorias

| Captura | Ubicación en código/dashboard |
|---------|------------------------------|
| Kafka topics | `docker exec master kafka-topics.sh --list` |
| NiFi canvas | http://localhost:9090/nifi |
| Hive tablas | Beeline: `SHOW TABLES IN logistica` |
| Cassandra | `cqlsh -e "SELECT * FROM logistica.vehicle_latest_state"` |
| HDFS raw | `hdfs dfs -ls /hadoop/logistica/raw/` |
| GraphFrames | Resultado de `02_graph_metrics.py` |
| Airflow DAG | http://localhost:8080 |
| Dashboard | http://localhost:8501 |

---

## 7. Justificación Técnica

| Tecnología | Justificación |
|------------|---------------|
| Kafka | Desacoplamiento ingesta/procesamiento, pub/sub |
| HDFS | Persistencia raw escalable, auditoría |
| Spark | Procesamiento distribuido, SQL, ML |
| GraphFrames | Análisis de rutas, criticidad |
| Hive | Consultas SQL históricas, metastore |
| Cassandra | Estado último baja latencia |
| Airflow | Orquestación, reintentos, alertas |
| NiFi | Integración API Open-Meteo |
| Streamlit | Dashboard interactivo rápido |

---

## 8. Ejecución desde Cero

```bash
# 1. Iniciar stack
./start.sh

# 2. Productores
python3 ingesta/productores/ships_gps_producer.py --bootstrap master:9092
python3 ingesta/productores/alerts_producer.py --bootstrap master:9092

# 3. Consumidor
python3 ingesta/consumidores/kafka_to_hdfs_raw.py --bootstrap master:9092

# 4. Spark jobs
spark-submit jobs/spark/01_raw_to_staging.py
spark-submit jobs/spark/01_load_master_dimensions.py
spark-submit jobs/spark/02_weather_port_enrichment.py
spark-submit jobs/spark/03_weather_operational_fact.py
spark-submit jobs/spark/02_graph_metrics.py

# 5. Dashboard
streamlit run dashboard/app.py
```

**Ver**: `docs/DEMO_SCRIPT.md`

---

## 9. Limitaciones y Mejoras Futuras

### Limitaciones
- Entorno Docker/local (no YARN cluster)
- Datos simulados (no API real de stock)
- Micro-batch en lugar de streaming real

### Mejoras
- Integrar API real de stock
- Structured Streaming en cluster dedicado
- MLops: reentrenamiento modelo
- Dashboard en producción con auth
