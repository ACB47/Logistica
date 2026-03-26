# Documento Word (guion + capturas)

Este fichero es el **esqueleto** del documento Word. La idea es escribir aquí primero (con imágenes/capturas referenciadas) y luego exportar a `.docx`.

---

## 0. Portada

- Título: *Plataforma Big Data para logística marítima y cadena de suministro (Valladolid)*
- Autor / asignatura / fecha
- Entorno: **SER 5 AMD Ryzen 7 5700**, 12 GB RAM, VirtualBox (3 VMs: master + 2 slaves)

---

## 0.1 Estado real de implementación (checkpoint)

- Stack levantado en modo standalone (master):
  - Hadoop/HDFS (+ YARN opcional)
  - Kafka KRaft (nodo local para práctica)
  - Hive Metastore
  - Spark 3.5.x + GraphFrames
  - Airflow 2.10.5
- Jobs KDD implementados:
  - `jobs/spark/01_raw_to_staging.py`
  - `jobs/spark/02_graph_metrics.py`
  - `jobs/spark/03_score_and_alert.py`
- Tablas de salida verificadas:
  - `logistica.stg_ships`, `logistica.stg_alerts_clima`, `logistica.stg_alerts_noticias`
  - `logistica.fact_route_risk`, `logistica.fact_graph_hops`, `logistica.fact_alerts`

---

## 1. Contexto y problema (KDD – Selección)

### 1.1 Sector y caso de uso

- **Cadena de suministro**: importación marítima desde Shanghai a España, con entrega final en Valladolid.
- **Puntos críticos**:
  - stock del almacén (riesgo de ruptura),
  - tiempos de tránsito puerto→Valladolid,
  - tiempos de descarga/gestión/preparación,
  - riesgo externo (geopolítica, clima).

### 1.2 Objetivo del sistema

- Monitorizar barcos por GPS.
- Cruzar esa señal con alertas globales.
- Generar **alertas accionables** (plan de contingencia: vuelo urgente).

### 1.3 Justificación de decisiones (importante para la rúbrica)

- Kafka para desacoplar fuentes y consumidores.
- HDFS para auditoría y escalabilidad (landing raw).
- Spark SQL para limpieza/normalización y joins.
- GraphFrames para modelar rutas y criticidad (grafos).
- Hive como capa SQL histórica/curada (sin Cassandra por restricción).
- Airflow para orquestación y reintentos/alertas.

**Captura obligatoria**: diagrama de arquitectura (lo haremos en la sección 2).

---

## 2. Arquitectura final (KDD – Selección + Preprocesamiento)

### 2.1 Arquitectura (Lambda/Kappa adaptada)

El enunciado pide Lambda/Kappa y streaming; por restricción del proyecto **no usamos Structured Streaming**.

**Solución equivalente**:
- Ingesta en Kafka (topics `datos_crudos`, `alertas_globales`).
- Persistencia **raw** en HDFS (paths del enunciado).
- Procesamiento por **micro‑lotes** desde HDFS con Spark (cada N minutos).
- Publicación de “resultados” (tablas curadas en Hive + alertas finales en Kafka si aplica).

### 2.2 Componentes (modo standalone)

Tabla a completar:
- `master`: NameNode, (ResourceManager opcional), Hive Metastore/HiveServer2 (si aplica), Airflow, Kafka controller/broker (KRaft), NiFi (si aplica)
- `slave01`: no usado en la demo final standalone
- `slave02`: no usado en la demo final standalone

**Capturas**:
- VirtualBox: 3 VMs creadas, RAM/CPU asignada.
- `jps`/servicios Hadoop activos en master y slaves.

---

## 3. Preparación del entorno (VirtualBox + Hadoop)

### 3.1 Recursos y red

- CPU/RAM por VM (justificación: 12 GB total).
- Red: NAT + Host‑Only (o puente) y resolución de nombres (`/etc/hosts`).

**Capturas**:
- Configuración de red por VM.
- `ping` entre `master`, `slave01`, `slave02`.

### 3.2 HDFS + YARN

- Formateo y arranque.
- Validación con `hdfs dfs -ls /`.
- Validación YARN UI.

**Capturas**:
- HDFS UI (NameNode).
- YARN UI (ResourceManager).

---

## 4. Ingesta (KDD – Ingesta y Selección): Kafka KRaft + “landing raw” HDFS

### 4.1 Topics Kafka

- `datos_crudos`: GPS barcos, eventos operativos (puerto, almacén, stock).
- `alertas_globales`: noticias geopolíticas y clima (y/o eventos agregados).

**Capturas**:
- Comandos de creación de topics.
- `kafka-topics --describe`.

### 4.2 Evidencia del flujo NiFi

- Flujo exportado reutilizable:
  - `docs/nifi/OpenMeteo_Kafka_Flow.json`
- Uso en la memoria:
  - referenciar el JSON como artefacto tecnico del canvas real
  - añadir una captura PNG del canvas en ejecucion
  - explicar que el JSON permite reimportar y auditar el flujo

**Capturas**:
- Canvas completo de NiFi.
- Configuracion de `InvokeHTTP`.
- Configuracion de `PublishKafka`.

### 4.2 Formato de eventos (contratos)

Definir JSON mínimo:
- **ShipPositionEvent**: `ship_id`, `ts`, `lat`, `lon`, `speed`, `heading`, `route_id`, `eta_port`
- **AlertEvent**: `source` (clima/noticias), `ts`, `severity`, `region`, `text`, `confidence`
- **StockEvent**: `sku`, `warehouse` (Valladolid), `ts`, `stock_on_hand`, `reorder_point`

**Justificación**: contratos estables para desacoplar.

### 4.3 Landing raw en HDFS

Rutas:
- `/hadoop/logistica/raw/ships`
- `/user/logistica/raw/opensky`
- `/hadoop/logistica/raw/noticias`
- `/hadoop/logistica/raw/clima`

**Capturas**:
- `hdfs dfs -mkdir -p ...`
- `hdfs dfs -ls -R /hadoop/logistica/raw`

> Nota: cómo se hace la copia "raw" depende de la herramienta final (NiFi/consumidores). Se documenta el mecanismo elegido.
> Estado actual: validado con productores Python + consumidor Kafka->HDFS.
> Pendiente de cierre final: flujo equivalente con NiFi + conectores/API.

### 4.4 Ejecución end-to-end (producción + landing raw)

Para tener evidencia en HDFS rápidamente, ejecutamos:
- productor de **GPS barcos** → `datos_crudos`
- productor de **alertas globales** (clima/noticias) → `alertas_globales`
- consumidor que hace **landing raw** JSONL en HDFS

**Paso 1: activar Kafka en `master`**

```bash
cd /home/hadoop/PROYECTOLOGISTICA
source scripts/12_use_existing_kafka.sh
```

**Paso 2: crear entorno Python de ingesta**

```bash
cd /home/hadoop/PROYECTOLOGISTICA
python3 -m venv .venv-ingesta
source .venv-ingesta/bin/activate
pip install -r ingesta/requirements.txt
```

**Paso 3: lanzar productores (2 terminales)**

Terminal A (GPS barcos):
```bash
source .venv-ingesta/bin/activate
python3 ingesta/productores/ships_gps_producer.py --bootstrap master:9092 --topic datos_crudos --ships 10 --interval-sec 0.5
```

Terminal B (alertas clima/noticias):
```bash
source .venv-ingesta/bin/activate
python3 ingesta/productores/alerts_producer.py --bootstrap master:9092 --topic alertas_globales --interval-sec 1.0
```

**Paso 4: lanzar landing raw (3ª terminal)**

```bash
source .venv-ingesta/bin/activate
python3 ingesta/consumidores/kafka_to_hdfs_raw.py --bootstrap master:9092 --group-id logistica-raw-sink \
  --spool-dir /tmp/logistica_spool --flush-every-sec 30
```

**Paso 5: verificar que se genera la evidencia**

```bash
hdfs dfs -ls -R /hadoop/logistica/raw/ships | head
hdfs dfs -ls -R /hadoop/logistica/raw/clima | head
hdfs dfs -ls -R /hadoop/logistica/raw/noticias | head
```

**Capturas recomendadas**:
- 3 terminales mostrando productores + consumidor activos.
- salida de `hdfs dfs -ls -R ...` confirmando ficheros JSONL en `ships/`, `clima/` y `noticias/`.

---

## 5. Preprocesamiento y transformación (KDD – Preprocesamiento/Transformación): Spark SQL + Hive

### 5.1 Limpieza y normalización

- Parsing JSON → columnas tipadas.
- Control de nulos y duplicados.
- Normalización de timestamps/zonas horarias.

**Capturas**:
- Ejecución de un job Spark en YARN.
- Tabla staging en Hive.

**Ejecución del job 01 (ejemplo local/docker)**:

```bash
docker-compose exec -T spark spark-submit /home/jovyan/jobs/spark/01_raw_to_staging.py
```

**Ejecución del job meteorológico validado en Docker**:

```bash
bash scripts/63_run_weather_filtered_staging.sh full
```

Comando directo equivalente:

```bash
docker-compose exec -T spark spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /home/jovyan/jobs/spark/01_weather_filtered_to_staging.py --bootstrap kafka:9092 --topic datos_filtrados
```

**Verificación en HDFS (evidencia)**:

```bash
hdfs dfs -ls -R /hadoop/logistica/staging/stg_ships | head
hdfs dfs -ls -R /hadoop/logistica/staging/stg_alerts_clima | head
hdfs dfs -ls -R /hadoop/logistica/staging/stg_alerts_noticias | head
```

### 5.2 Enriquecimiento con datos maestros (Hive)

Tablas maestras (ejemplos):
- `dim_ports` (Shanghai, Algeciras/Valencia/Barcelona)
- `dim_warehouse` (Valladolid)
- `dim_routes` (rutas marítimas y puerto→Valladolid)
- `dim_skus` (lead time, criticidad)

**Justificación**: joins reproducibles, gobernanza SQL.

**Evidencias sugeridas**:
- `logistica.dim_ports`
- `logistica.dim_routes`
- `logistica.dim_warehouse`
- `logistica.dim_ports_routes_weather`

---

## 6. Análisis de grafos (KDD – Transformación/Minería): GraphFrames

### 6.1 Modelo de grafo

- **Nodos**: puertos, almacén Valladolid, nodos logísticos intermedios (si aplica).
- **Aristas**: rutas con pesos (tiempo estimado, riesgo).

### 6.2 Métricas

- Camino más corto/rápido (peso = tiempo).
- Camino “más seguro” (peso = riesgo compuesto).
- Identificación de nodos críticos (centralidad aprox / comunidades si aplica).

**Capturas**:
- Resultado de rutas recomendadas.
- Tabla Hive `fact_route_risk`.

---

## 7. Minería y acción (KDD – Minería/Interpretación): reglas + scoring de riesgo

Decisión de defensa:
- La entrega se defenderá como `micro-batch documentado`.
- Structured Streaming queda implementado y alineado a ventanas de `15 minutes`, pero se presenta como evidencia técnica complementaria para reducir riesgo operativo en la demo.

### 7.1 Scoring de riesgo (sin streaming estructurado)

Riesgo compuesto ejemplo:
- clima (severidad),
- geopolítica (severidad),
- retraso estimado (vs ETA),
- stock (si bajo reorder point).

Salida:
- **alerta_operativa** con recomendación: “activar vuelo urgente” si \(riesgo\) alto y stock crítico.

**Capturas**:
- Ejemplos de alertas generadas.
- Registro en Hive `fact_alerts`.

---

## 8. Orquestación (KDD – Interpretación/Acción): Airflow

### 8.1 DAG principal

Tareas típicas:
- `landing_to_staging_ships`
- `landing_to_staging_alerts`
- `build_dims`
- `build_graph_metrics`
- `compute_risk_and_alerts`

Requisitos rúbrica:
- reintentos,
- dependencias claras,
- alertas (email o log) en fallo.

**Capturas**:
- Vista del DAG.
- Ejecución correcta (verde) + ejemplo de reintento.

### 8.2 Parámetros reales usados en esta entrega

- `AIRFLOW_HOME`: `/home/hadoop/PROYECTOLOGISTICA/.airflow`
- Usuario UI: `admin` / `admin`
- DAG cargado: `logistica_kdd_microbatch`
- Versión validada en entorno: `apache-airflow==2.10.5`

### 8.3 NiFi (diseño a incluir en la memoria)

Sección a completar con diagrama:
- Fuente HTTP/API -> `InvokeHTTP`
- Parseo/normalización -> `JoltTransformJSON` / `UpdateRecord`
- Enrutado por tipo (`ships`, `clima`, `noticias`) -> `RouteOnAttribute`
- Publicación a Kafka (`PublishKafkaRecord_2_0`) o landing directo a HDFS (`PutHDFS`)
- Trazabilidad/errores -> colas de fallo + reintentos

**Capturas previstas**:
- Canvas de NiFi con processors conectados.
- Config de al menos un `InvokeHTTP` y un `PublishKafkaRecord_2_0`/`PutHDFS`.
- Evidencia de flujo en cola/success.

---

## 9. Evidencias y capturas (checklist)

- HDFS: UIs + `hdfs dfs -ls`.
- Kafka KRaft: topics creados + describe.
- Hive: tablas raw/staging/curated.
- GraphFrames: salida (rutas/riesgo).
- Airflow: DAG + runs.

### 9.1 Checklist exacto de evidencias operativas

**Kafka**

```bash
docker-compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
docker-compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic datos_crudos
docker-compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic datos_filtrados
```

Capturar:
- topics visibles
- detalle de `datos_crudos`
- detalle de `datos_filtrados`

**NiFi**
- captura del canvas completo
- captura de `InvokeHTTP`
- captura de `PublishKafka`
- referencia al export: `docs/nifi/OpenMeteo_Kafka_Flow.json`

**HDFS**

```bash
docker-compose exec -T namenode bash -lc '/opt/hadoop-3.2.1/bin/hdfs dfs -ls -R /hadoop/logistica/raw'
docker-compose exec -T namenode bash -lc '/opt/hadoop-3.2.1/bin/hdfs dfs -ls -R /hadoop/logistica/staging'
docker-compose exec -T namenode bash -lc '/opt/hadoop-3.2.1/bin/hdfs dfs -ls -R /hadoop/logistica/curated'
docker-compose exec -T namenode bash -lc '/opt/hadoop-3.2.1/bin/hdfs dfs -ls -R /hadoop/logistica/master'
```

Capturar al menos:
- `stg_weather_open_meteo`
- `dim_ports_routes_weather`
- `fact_weather_operational`
- `fact_route_risk`
- `fact_graph_centrality`

**Hive / Spark SQL**

```bash
docker-compose exec -T spark spark-sql -e "SHOW TABLES IN logistica"
docker-compose exec -T spark spark-sql -e "SELECT * FROM logistica.stg_weather_open_meteo ORDER BY event_ts DESC LIMIT 5"
docker-compose exec -T spark spark-sql -e "SELECT * FROM logistica.dim_ports_routes_weather ORDER BY event_ts DESC LIMIT 5"
docker-compose exec -T spark spark-sql -e "SELECT * FROM logistica.fact_weather_operational ORDER BY event_ts DESC LIMIT 5"
docker-compose exec -T spark spark-sql -e "SELECT * FROM logistica.fact_alerts ORDER BY severity DESC LIMIT 5"
docker-compose exec -T spark spark-sql -e "SELECT * FROM logistica.fact_graph_centrality ORDER BY degree DESC LIMIT 5"
```

**Cassandra**

```bash
docker-compose exec -T cassandra cqlsh -e "SELECT ship_id, route_id, dest_port, warehouse, stock_on_hand, reorder_point FROM logistica.vehicle_latest_state;"
```

**Spark job clave en Docker**

```bash
bash scripts/63_run_weather_filtered_staging.sh full
```

Capturar:
- terminal del `spark-submit`
- tabla `logistica.stg_weather_open_meteo` despues del job

**Airflow**
- DAG `logistica_kdd_microbatch`
- DAG `logistica_kdd_monthly_retrain`
- vista Graph
- un run exitoso
- si es posible, un reintento

### 9.2 Guion corto de demo final

1. Mostrar NiFi recibiendo API publica y enviando a Kafka.
2. Mostrar Kafka con `datos_crudos` y `datos_filtrados`.
3. Mostrar Hive staging y dimensiones.
4. Mostrar fact table operativa del clima.
5. Mostrar grafo y criticidad de nodos.
6. Mostrar Cassandra con `vehicle_latest_state`.
7. Mostrar Airflow orquestando el flujo.

---

## 10. Conclusiones

- Qué problemas resuelve.
- Limitaciones (no Cassandra, no structured streaming).
- Mejoras futuras (observabilidad, modelos ML más avanzados, etc.).
