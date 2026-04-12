# Manual de Desarrollador

Autora del proyecto: **Ana Coloma Bausela**

## 1. Objetivo

Este documento explica la estructura técnica del proyecto, cómo extenderlo y qué piezas son responsables de cada parte del flujo KDD.

## 2. Arquitectura técnica

El proyecto sigue una arquitectura Docker/local con estos componentes principales:

- `NiFi`: ingesta desde Open-Meteo y publicación a Kafka.
- `Kafka`: topics `datos_crudos`, `datos_filtrados`, `datos_filtrados_ok`, `alertas_globales`.
- `HDFS`: raw, staging, curated y master data.
- `Spark`: staging, facts, grafos, contingencia aérea y generación del bundle del dashboard.
- `Hive`: metastore SQL para tablas `stg_*`, `dim_*`, `fact_*`.
- `Cassandra`: última foto operativa de vehículos (`vehicle_latest_state`).
- `Airflow`: orquestación microbatch y DAG mensual.
- `Streamlit`: dashboard de defensa y control tower.

## 3. Rutas importantes del repositorio

- `ingesta/`: productores y consumidores de entrada.
- `jobs/spark/`: jobs Spark ordenados por fase.
- `jobs/cassandra/`: definición del esquema Cassandra.
- `airflow/dags/`: DAGs de orquestación.
- `dashboard/`: aplicación Streamlit.
- `docs/`: documentación general y manuales orientados a entrega.
- `scripts/`: scripts auxiliares de arranque, rebuild y carga.

## 4. Jobs Spark relevantes

- `01_load_master_dimensions.py`: carga dimensiones maestras, artículos de Valladolid, pedidos y Gantt.
- `01_weather_filtered_to_staging.py`: topic Kafka a staging climático.
- `01_raw_to_staging.py`: raw HDFS a staging de barcos y alertas.
- `02_weather_port_enrichment.py`: enriquecimiento clima + puerto + ruta + almacén.
- `02_graph_metrics.py`: hops y criticidad de nodos.
- `03_weather_operational_fact.py`: fact operativa climática.
- `03_air_recovery_options.py`: comparación `barco vs aéreo+camión`.
- `03_score_and_alert.py`: alertas operativas legacy.
- `04_streaming_ml_pipeline.py`: streaming experimental con ventanas de 15 min.
- `99_dashboard_bundle.py`: genera el JSON consumido por el dashboard.

## 5. Reglas importantes de operación

- Los jobs y consultas Hive deben ejecutarse **de forma secuencial**, no en paralelo.
- Si tras un reinicio faltan tablas Hive, ejecutar:

```bash
bash scripts/66_rebuild_hive_demo_tables.sh
```

- Si el dashboard no refleja cambios nuevos, regenerar el bundle:

```bash
docker compose exec -T spark spark-submit /home/jovyan/jobs/spark/99_dashboard_bundle.py
```

## 6. Dashboard

Arranque:

```bash
bash scripts/67_run_dashboard.sh
```

La Control Tower utiliza datos reales o simulados del proyecto para:

- stock Valladolid
- pedidos de Douai y Cleon
- ETA de 10 barcos y su progreso de viaje
- contingencia aérea
- Gantt de cobertura y planificación
- estado de servicios
- mapas marítimos con posiciones GPS ajustadas a corredores sobre agua

Notas de implementación recientes:

- El sidebar Streamlit navega con `st.session_state["vista_actual"]` y botones homogéneos.
- La sección `Arquitectura en vivo` es un panel técnico con control `ON/OFF` por servicio.
- Al arrancar `nifi` desde el dashboard se ejecuta también el bootstrap del flujo Open-Meteo.
- `jobs/spark/99_dashboard_bundle.py` completa la flota hasta 10 barcos para garantizar una demo consistente.

## 7. Cómo extender el proyecto

### Añadir un nuevo origen o destino

Editar:

- `jobs/spark/01_load_master_dimensions.py`

Actualizar:

- `PORTS`
- `ROUTES`
- `AIRPORTS`
- `AIR_RECOVERY_CORRIDORS`

### Añadir nuevos artículos o pedidos

Editar en `01_load_master_dimensions.py`:

- `ARTICLES_VALLADOLID`
- `CUSTOMER_ORDERS_DOUAI`
- `ARTICLE_GANTT`

Después ejecutar:

```bash
docker compose exec -T spark spark-submit /home/jovyan/jobs/spark/01_load_master_dimensions.py
docker compose exec -T spark spark-submit /home/jovyan/jobs/spark/99_dashboard_bundle.py
```

### Añadir nuevas incidencias de barco

Editar:

- `dashboard/app.py`

Bloque:

- `SHIP_CONSTRAINTS`

## 8. Comandos útiles

Levantar stack completo:

```bash
docker compose up -d postgres kafka nifi spark cassandra namenode datanode airflow-webserver
```

Parar stack:

```bash
docker compose down
```

Ver tablas Hive:

```bash
docker compose exec -T spark bash -lc 'spark-sql -S -e "SHOW TABLES IN logistica" 2>/dev/null'
```

Ver estado Cassandra:

```bash
docker compose exec -T cassandra cqlsh -e "SELECT * FROM logistica.vehicle_latest_state;"
```

## 9. Nota final

Este manual está pensado para mantenimiento, ampliación y recuperación del proyecto durante el desarrollo y la entrega.
