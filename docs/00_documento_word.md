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
- Ejecución de un job Spark en Docker.
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

- `AIRFLOW_HOME`: directorio `.airflow` del proyecto o volumen equivalente en Docker
- Usuario UI: `admin` / `admin`
- DAG cargado: `logistica_kdd_microbatch`
- Versión validada en entorno: `apache-airflow==2.10.5`
- Segundo DAG validado: `logistica_kdd_monthly_retrain`

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

### 9.0 Dashboard de exposicion

Se ha anadido un dashboard ligero en `Streamlit` para facilitar la defensa del flujo KDD y reducir la dependencia de cambiar entre multiples interfaces durante la exposicion.

Incluye:
- resumen KDD y estado de cumplimiento de la rubrica
- estado en vivo de servicios Docker con botones `On/Off`
- mapa de barcos con alertas de ruta
- tablas curadas relevantes para la defensa
- diagramas Mermaid: flujo, secuencia, clases y casos de uso

Ejecucion:

```bash
bash scripts/67_run_dashboard.sh
```

- HDFS: UIs + `hdfs dfs -ls`.
- Kafka KRaft: topics creados + describe.
- Hive: tablas raw/staging/curated.
- GraphFrames: salida (rutas/riesgo).
- Airflow: DAG + runs.

### 9.1 Checklist exacto de evidencias operativas

Estado actual de capturas ya realizadas en esta sesion:
- NiFi: canvas + `InvokeHTTP` + `PublishKafka`
- Kafka: consumo correcto de `datos_filtrados`
- Hive: `stg_weather_open_meteo`, `dim_ports_routes_weather`, `fact_weather_operational`
- Hive alertas: `fact_alerts`
- Cassandra: `vehicle_latest_state`
- HDFS curated: listado de `fact_weather_operational`, `fact_route_risk`, `fact_graph_centrality`
- GraphFrames: `fact_graph_centrality`
- Airflow: lista de DAGs + Graph de `logistica_kdd_microbatch` + Graph de `logistica_kdd_monthly_retrain`
- Rebuild tras reinicio validado: `scripts/66_rebuild_hive_demo_tables.sh` restaura dimensiones, staging y facts principales

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

Comprobacion recomendada tras reinicio:

```bash
bash scripts/66_rebuild_hive_demo_tables.sh
docker-compose exec -T spark bash -lc 'spark-sql -S -e "SHOW TABLES IN logistica" 2>/dev/null'
```

**Cassandra**

```bash
docker-compose exec -T cassandra cqlsh -e "SELECT ship_id, route_id, dest_port, warehouse, stock_on_hand, reorder_point FROM logistica.vehicle_latest_state;"
```

Evidencia ya validada en sesion:
- `ship-001 | route-shanghai-algeciras | Algeciras | Valladolid | 11 | 30`
- `ship-002 | route-shanghai-valencia | Valencia | Valladolid | 93 | 30`
- `ship-003 | route-shanghai-barcelona | Barcelona | Valladolid | 95 | 30`

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

### 9.3 Guion formal de defensa

**Introducción**

Este proyecto implementa una plataforma de datos orientada a logística marítima y portuaria. El objetivo principal es integrar datos operativos en tiempo casi real, combinarlos con información histórica y enriquecerlos con análisis de rutas para anticipar riesgos, priorizar acciones y mejorar la toma de decisiones.

La propuesta sigue el ciclo KDD y cubre sus etapas principales: selección, preprocesamiento, transformación, minería e interpretación. La entrega final se defiende sobre una arquitectura Docker/local completamente funcional, con una estrategia de micro-batch documentado para reducir riesgo operativo durante la demo.

**Arquitectura general**

La arquitectura está compuesta por los siguientes bloques:

- `Apache NiFi` para la ingesta desde una API pública real.
- `Apache Kafka` como capa de mensajería, separando `datos_crudos` y `datos_filtrados`.
- `HDFS` como almacenamiento distribuido para persistencia raw, staging y curated.
- `Apache Hive` como capa SQL analítica sobre HDFS.
- `Apache Spark` para limpieza, enriquecimiento, reglas de negocio, facts y grafos.
- `GraphFrames` para el análisis de criticidad y estructura de la red logística.
- `Apache Cassandra` para consultas de baja latencia sobre el último estado de vehículos.
- `Apache Airflow` para la orquestación operativa y el refresco periódico.

**Fase de ingesta y selección**

La fase de ingesta se implementa con NiFi consumiendo una API pública meteorológica. Ese flujo publica dos salidas en Kafka:

- `datos_crudos`, que conserva la respuesta original para trazabilidad.
- `datos_filtrados`, que contiene un JSON simplificado y normalizado para analítica.

Con esto se cubre la separación entre dato bruto y dato preparado, además de dejar evidencia reproducible mediante el export del flujo en `docs/nifi/OpenMeteo_Kafka_Flow.json`.

**Fase de preprocesamiento y transformación**

En Spark se construye una tabla de staging llamada `logistica.stg_weather_open_meteo`, que tipa, valida y normaliza el dato meteorológico recibido desde Kafka. Posteriormente se cargan dimensiones maestras reales en Hive:

- `dim_ports`
- `dim_routes`
- `dim_warehouse`
- `dim_skus`

Estas dimensiones permiten reemplazar referencias embebidas y hacer joins más defendibles desde el punto de vista analítico.

Después se genera `logistica.dim_ports_routes_weather`, que cruza el dato meteorológico con el contexto de puerto, ruta y almacén, calculando:

- nivel de riesgo meteorológico
- estado operativo del puerto
- retraso estimado en horas

**Fase de minería e interpretación**

Sobre la capa enriquecida se construye `logistica.fact_weather_operational`, que representa la tabla operativa final del caso de clima. Esta tabla incluye:

- riesgo operacional
- retraso estimado
- recomendación de acción
- severidad operativa

En paralelo, se mantiene el pipeline legacy basado en datos raw de barcos y alertas, que produce:

- `logistica.fact_route_risk`
- `logistica.fact_alerts`

Esto permite demostrar una narrativa completa de alertas y priorización logística.

**Análisis de grafos**

La red logística se modela con GraphFrames, utilizando puertos y almacenes como nodos y rutas como aristas. Se han implementado dos métricas defendibles:

- distancia o saltos entre nodos, persistida en `logistica.fact_graph_hops`
- criticidad por grado, persistida en `logistica.fact_graph_centrality`

Esto permite identificar nodos clave de la red, como Shanghai, Valladolid o Algeciras, y justificar decisiones operativas asociadas al cuello de botella o a la relevancia estructural del nodo.

**Persistencia multicapa**

La persistencia se divide según el caso de uso:

- `Hive` se utiliza para staging, dimensiones y facts analíticas históricas.
- `Cassandra` se utiliza para consultas rápidas sobre el último estado conocido de cada vehículo.

La tabla `logistica.vehicle_latest_state` en Cassandra permite consultar directamente el último destino, almacén, stock y punto de reorden de cada barco, cubriendo el requisito de baja latencia.

**Orquestación**

La orquestación se resuelve con dos DAGs en Airflow:

- `logistica_kdd_microbatch`, centrado en la ejecución operativa principal.
- `logistica_kdd_monthly_retrain`, centrado en refresco de dimensiones, reconstrucción de grafos y limpieza de temporales.

Además, se ha añadido visibilidad de fallos mediante `email_on_failure`, reforzando la parte operativa de la solución.

**Decisiones de defensa y alcance**

La decisión final para la defensa ha sido utilizar Docker/local como ruta oficial. Aunque existe trabajo de alineación conceptual con despliegues más distribuidos, la demo se apoya en un flujo estable y reproducible en local. Del mismo modo, Structured Streaming queda alineado a ventanas de `15 minutes`, pero la exposición principal se basa en micro-batch documentado para minimizar riesgo durante la presentación.

**Limitaciones actuales**

Las limitaciones principales del proyecto son:

- la defensa no se realiza sobre VMs ni sobre YARN real en producción
- el streaming real no es la ruta principal de demostración
- algunas fuentes de negocio se han simplificado para hacer la demo más controlable
- el bloque ML queda como evidencia técnica complementaria, no como núcleo de la defensa

**Conclusión**

En conjunto, el proyecto ya demuestra una cadena completa y funcional:

NiFi -> Kafka -> HDFS/Hive -> Spark -> GraphFrames -> Cassandra -> Airflow

La solución no solo ingiere y transforma datos, sino que también genera hechos analíticos, recomendaciones operativas, análisis de criticidad de red y consultas de baja latencia. Esto permite defender el proyecto como una plataforma coherente de ingeniería de datos aplicada al dominio logístico.

---

## 10. Conclusiones

- Qué problemas resuelve.
- Limitaciones (streaming real no defendido como ruta principal, Cassandra cargada en micro-batch, no despliegue en cluster/YARN para la entrega final).
- Mejoras futuras (observabilidad, modelos ML más avanzados, etc.).
