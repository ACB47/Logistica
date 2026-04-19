# Documento Word (guion + capturas)

Este documento sirve como base de redacción para la memoria final del proyecto. Su objetivo es consolidar en un único texto la narrativa técnica, las decisiones de arquitectura, las evidencias de ejecución y el guion de defensa antes de su exportación a formato `.docx`.

---

## 0. Portada

- Título: *Plataforma Big Data para logística marítima y cadena de suministro (Valladolid)*
- Autor / asignatura / fecha
- Entorno: **Docker/local sobre Linux**, con `docker compose` para la demo completa y modo `dashboard solo (ultraligero)` para iteración visual sin levantar todo el stack.

---

## 0.1 Estado real de implementación (checkpoint)

- Stack validado en la ruta oficial Docker/local:
  - HDFS (`namenode` + `datanode`)
  - Kafka KRaft
  - NiFi 2.6.0
  - Spark 3.5.x + Hive + GraphFrames
  - Cassandra 5.0
  - Airflow 2.10.5
  - Streamlit para la defensa funcional
- Jobs KDD implementados:
  - `jobs/spark/01_raw_to_staging.py`
  - `jobs/spark/01_load_master_dimensions.py`
  - `jobs/spark/01_weather_filtered_to_staging.py`
  - `jobs/spark/02_weather_port_enrichment.py`
  - `jobs/spark/03_weather_operational_fact.py`
  - `jobs/spark/02_graph_metrics.py`
  - `jobs/spark/03_score_and_alert.py`
  - `jobs/spark/03_air_recovery_options.py`
  - `jobs/spark/99_dashboard_bundle.py`
- Tablas de salida verificadas:
  - `logistica.stg_ships`, `logistica.stg_alerts_clima`, `logistica.stg_alerts_noticias`, `logistica.stg_weather_open_meteo`
  - `logistica.dim_ports`, `logistica.dim_routes`, `logistica.dim_warehouse`, `logistica.dim_skus`
  - `logistica.dim_ports_routes_weather`, `logistica.fact_weather_operational`
  - `logistica.fact_route_risk`, `logistica.fact_graph_hops`, `logistica.fact_graph_centrality`, `logistica.fact_alerts`
  - `logistica.fact_air_recovery_options`, `logistica.dim_articles_valladolid`, `logistica.fact_customer_orders_douai`, `logistica.fact_article_gantt`
  - `logistica.vehicle_latest_state` en Cassandra

Notas operativas ya confirmadas:
- La ruta recomendada para la defensa es Docker/local, no VirtualBox.
- El dashboard puede arrancar sin Docker con `./start.sh` opción `3` o `bash scripts/67_run_dashboard.sh`.
- Si falla una regeneración del bundle, el dashboard reutiliza `jobs/dashboard_bundle_output.last_good.json`.
- Las alertas críticas por correo no bloquean la UI: si SMTP no es accesible, se guarda evidencia demo en `artifacts/email_alerts/`.

Este estado de implementación permite defender el proyecto como una solución funcional de extremo a extremo, con una capa de visualización ejecutiva adaptada a la exposición académica.

---

## 1. Contexto y problema (KDD – Selección)

### 1.1 Sector y caso de uso

El proyecto se sitúa en el contexto de la logística internacional de componentes, con especial foco en la importación marítima desde Asia hacia España y la distribución final al almacén de Valladolid.

Los puntos críticos del caso de uso son los siguientes:
- nivel de stock disponible y riesgo de ruptura en el almacén
- tiempos de tránsito desde el puerto de origen hasta el destino final
- demoras operativas en descarga, gestión portuaria y preparación de expediciones
- impacto de factores externos, especialmente clima adverso y contexto geopolítico

### 1.2 Objetivo del sistema

El objetivo del sistema es monitorizar la situación logística de la cadena de suministro mediante datos de posición, eventos operativos y señales externas de riesgo. A partir de esta información, la plataforma debe identificar incidencias relevantes y traducirlas en alertas accionables, incluyendo la activación de planes de contingencia cuando la continuidad del suministro esté comprometida.

### 1.3 Justificación de decisiones (importante para la rúbrica)

Las principales decisiones tecnológicas se justifican del siguiente modo:
- `Kafka` desacopla las fuentes de datos y los consumidores analíticos.
- `HDFS` proporciona persistencia raw auditable y una base escalable para staging y curated.
- `Spark` resuelve la limpieza, tipado, enriquecimiento y construcción de facts analíticas.
- `Hive` actúa como capa SQL histórica y defendible para consulta de resultados.
- `GraphFrames` permite modelar la red logística como grafo y calcular criticidad estructural.
- `Cassandra` cubre el requisito de consulta rápida sobre el último estado operativo.
- `Airflow` aporta orquestación, dependencias, reintentos y trazabilidad de ejecución.

**Figura recomendada**: diagrama de arquitectura general del proyecto, integrando NiFi, Kafka, HDFS, Spark, Hive, Cassandra, Airflow y Dashboard.

---

## 2. Arquitectura final (KDD – Selección + Preprocesamiento)

### 2.1 Arquitectura (Lambda/Kappa adaptada)

La defensa se apoya en una ruta Docker/local reproducible. La estrategia principal es **micro-batch documentado**, dejando `Structured Streaming` como evidencia técnica complementaria ya alineada a ventanas de `15 minutes`.

**Solución final defendida**:
- Ingesta real con NiFi hacia Kafka (`datos_crudos`, `datos_filtrados`, `alertas_globales`).
- Persistencia **raw/staging/curated** en HDFS y Hive.
- Procesamiento Spark secuencial para evitar bloqueos del metastore Derby.
- Persistencia operativa adicional en Cassandra.
- Exposición ejecutiva y técnica mediante dashboard Streamlit.

### 2.2 Componentes (ruta oficial Docker/local)

La solución final se articula sobre los siguientes componentes:
- `namenode` y `datanode`: persistencia distribuida en HDFS
- `kafka`: mensajería de eventos sobre KRaft
- `nifi`: adquisición, filtrado y publicación inicial de datos
- `spark`: ejecución de jobs batch, enriquecimiento, facts y análisis de grafos
- `cassandra`: consulta de estado actual con baja latencia
- `airflow-webserver`: supervisión y orquestación de workflows
- `dashboard` local en `Streamlit`: visualización ejecutiva y técnica para la defensa

**Capturas**:
- `docker compose ps`
- UI de HDFS, NiFi, Spark y Airflow
- Dashboard Streamlit con datos reales

---

## 3. Preparación del entorno (Docker/local)

### 3.1 Recursos y perfiles de arranque

El proyecto dispone de dos perfiles de ejecución complementarios:

- Ruta completa de demostración, orientada a la validación integral del stack: `docker compose up -d`
- Ruta ligera de trabajo visual, orientada a iterar sobre el dashboard sin penalización de recursos: `./start.sh` y selección de la opción `3`

Cuando se requiere regenerar el dashboard con datos reales, la secuencia validada es la siguiente:
- `bash scripts/66_rebuild_hive_demo_tables.sh`
- `docker compose exec -T spark spark-submit /home/jovyan/jobs/spark/99_dashboard_bundle.py`
- `bash scripts/67_run_dashboard.sh`

**Capturas**:
- Menú de `start.sh`
- `docker compose ps`
- UI HDFS NameNode

### 3.2 Regla crítica de operación

Durante las validaciones se identificó una restricción operativa relevante: los comandos `spark-sql` y `spark-submit` que usan Hive deben ejecutarse **de forma secuencial**, ya que la ejecución paralela puede bloquear el metastore embebido basado en Derby.

Asimismo, si HDFS entra en `safe mode` o la regeneración del bundle falla, el dashboard puede quedarse sin datos. Para mitigar ese riesgo, la aplicación conserva un último bundle válido en `jobs/dashboard_bundle_output.last_good.json` y lo reutiliza automáticamente cuando el nuevo fichero no aporta datos útiles.

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
- En la memoria, este artefacto se utiliza para:
  - referenciar el JSON como evidencia técnica del canvas real
  - acompañarlo de una captura PNG del flujo en ejecución
  - justificar que el export permite reimportar, auditar y reproducir la ingesta

**Capturas**:
- Canvas completo de NiFi.
- Configuración de `InvokeHTTP`.
- Configuración de `PublishKafka`.

### 4.2 Formato de eventos (contratos)

Se propone documentar un contrato JSON mínimo para cada tipo de evento:
- **ShipPositionEvent**: `ship_id`, `ts`, `lat`, `lon`, `speed`, `heading`, `route_id`, `eta_port`
- **AlertEvent**: `source` (clima/noticias), `ts`, `severity`, `region`, `text`, `confidence`
- **StockEvent**: `sku`, `warehouse` (Valladolid), `ts`, `stock_on_hand`, `reorder_point`

**Justificación**: contratos estables y explícitos para desacoplar la ingesta de las fases analíticas posteriores.

### 4.3 Landing raw en HDFS

Rutas:
- `/hadoop/logistica/raw/ships`
- `/user/logistica/raw/opensky`
- `/hadoop/logistica/raw/noticias`
- `/hadoop/logistica/raw/clima`

**Capturas**:
- `hdfs dfs -mkdir -p ...`
- `hdfs dfs -ls -R /hadoop/logistica/raw`

> Nota: el mecanismo concreto de copia raw puede resolverse con NiFi o con consumidores Python, siempre que quede evidenciado el aterrizaje auditable en HDFS.
> En el estado actual del proyecto, este paso ha sido validado con productores Python y un consumidor Kafka->HDFS, manteniendo además la evidencia del flujo NiFi para la memoria.

### 4.4 Ejecución end-to-end (producción + landing raw)

Para tener evidencia en HDFS rápidamente, ejecutamos:
- productor de **GPS barcos** → `datos_crudos`
- productor de **alertas globales** (clima/noticias) → `alertas_globales`
- consumidor que hace **landing raw** JSONL en HDFS

**Paso 1: activar el entorno de mensajería**

```bash
docker compose up -d kafka
```

**Paso 2: crear entorno Python de ingesta**

```bash
cd /home/ana/Descargas/PROYECTOLOGISTICA_codigo/Logistica
python3 -m venv .venv-ingesta
source .venv-ingesta/bin/activate
pip install -r ingesta/requirements.txt
```

**Paso 3: lanzar productores (2 terminales)**

Terminal A (GPS barcos):
```bash
source .venv-ingesta/bin/activate
python3 ingesta/productores/ships_gps_producer.py --bootstrap localhost:9092 --topic datos_crudos --ships 10 --interval-sec 0.5
```

Terminal B (alertas clima/noticias):
```bash
source .venv-ingesta/bin/activate
python3 ingesta/productores/alerts_producer.py --bootstrap localhost:9092 --topic alertas_globales --interval-sec 1.0
```

**Paso 4: lanzar landing raw (3ª terminal)**

```bash
source .venv-ingesta/bin/activate
python3 ingesta/consumidores/kafka_to_hdfs_raw.py --bootstrap localhost:9092 --group-id logistica-raw-sink \
  --spool-dir /tmp/logistica_spool --flush-every-sec 30
```

**Paso 5: verificar que se genera la evidencia**

```bash
hdfs dfs -ls -R /hadoop/logistica/raw/ships | head
hdfs dfs -ls -R /hadoop/logistica/raw/clima | head
hdfs dfs -ls -R /hadoop/logistica/raw/noticias | head
```

**Capturas a incluir**:
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
docker compose exec -T spark spark-submit /home/jovyan/jobs/spark/01_raw_to_staging.py
```

**Ejecución del job meteorológico validado en Docker**:

```bash
bash scripts/63_run_weather_filtered_staging.sh full
```

Comando directo equivalente:

```bash
docker compose exec -T spark spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /home/jovyan/jobs/spark/01_weather_filtered_to_staging.py --bootstrap kafka:9092 --topic datos_filtrados
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

La estrategia de defensa seleccionada para esta entrega es `micro-batch documentado`. Aunque `Structured Streaming` está implementado y ajustado a ventanas de `15 minutes`, su papel en la memoria y en la exposición es complementario, con el fin de minimizar riesgo operativo durante la demostración final.

### 7.1 Scoring de riesgo (sin streaming estructurado)

El riesgo compuesto se calcula a partir de variables como:
- clima (severidad),
- geopolítica (severidad),
- retraso estimado (vs ETA),
- stock (si bajo reorder point).

Como salida, el sistema genera una **alerta operativa** con recomendación de acción. En el caso de mayor severidad, la recomendación puede ser activar una recuperación urgente por vía aérea cuando el riesgo sea alto y el stock resulte crítico.

**Capturas**:
- Ejemplos de alertas generadas.
- Registro en Hive `fact_alerts`.

### 7.2 Caso de uso extendido: barco vs aereo + camion

Para reforzar la fase de interpretación y acción, el proyecto incorpora un caso de uso ampliado centrado en la importación desde Shanghai a puertos españoles con destino final Valladolid.

El sistema compara dos alternativas logísticas:

- continuación marítima del barco hasta el puerto de destino y posterior distribución
- recuperación por `aéreo + camión` hasta Valladolid

Factores de decisión:

- posición GPS actual del barco
- ETA estimada del barco (`Estimated Time Arrival`)
- ETA restante marítima
- retraso meteorológico y operativo
- stock disponible y punto de reorden
- ETA aérea hasta Madrid, Barcelona o Valencia
- tramo terrestre por camión hasta Valladolid
- coste total del corredor alternativo
- horas ganadas frente al barco

Tabla objetivo:

- `logistica.fact_air_recovery_options`

La salida de este caso de uso ofrece una recomendación explicable, `MARITIMO` o `AEREO_CAMION`, junto con una estimación del ahorro de tiempo y del coste total de la contingencia.

Ejemplo ya generado en la tabla:
- `ship-001 | Algeciras | MARITIMO | ROTURA_INMINENTE | 411.5h barco | 25.8h aereo+camion | ahorro 385.7h | 17672.0 EUR`

### 7.3 Stock de Valladolid y clientes Douai / Cleon

El dashboard incorpora un panel específico de stock en el almacén de Valladolid para piezas de componentes de automoción. Cada artículo presenta, como mínimo, la siguiente información:

- referencia numérica de 10 caracteres
- designación del artículo
- piezas por embalaje
- consumo medio diario
- cantidad total de embalajes
- cantidad total de piezas
- stock mínimo de seguridad
- pedidos de los clientes de Douai y Cleon

Adicionalmente, se calcula un semáforo de stock con tres niveles:

- verde: cobertura suficiente
- naranja: cobertura tensionada
- rojo: riesgo de rotura o cobertura crítica

Para planificación visual, también se modela un Gantt por semanas industriales apoyado en:

- `logistica.fact_customer_orders_douai`
- `logistica.fact_article_gantt`

Este planteamiento permite mostrar no solo el estado actual del stock, sino también la secuencia logística prevista para cada referencia.

Además, el panel de flota muestra `ETA` por barco para enlazar visualmente la llegada estimada con el riesgo de ruptura de stock. Esta ETA se representa en formato fecha/hora y se acompaña de la fecha de salida del puerto de origen y de los días de viaje previstos.

La interfaz permite además:

- filtrar por cliente (`Douai` / `Cleon`)
- filtrar por semana industrial
- seleccionar un barco concreto
- activar incidencias simuladas sobre ese barco

Con estos controles, la aplicación recalcula si la llegada prevista sigue `CUBRIENDO` o `NO CUBRIENDO` la necesidad de suministro.

La demo visual incorpora además barcos con salida desde dos puertos asiáticos:

- Shanghai
- Yokohama

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
- Regla operativa: la evidencia visual de Airflow se toma sobre la ruta Docker/local, no sobre VMs.

### 8.3 NiFi en la memoria

La memoria debe describir NiFi como la capa de adquisición y preparación inicial del dato. El flujo validado se estructura del siguiente modo:

- consumo de una API HTTP pública mediante `InvokeHTTP`
- transformación y normalización del payload mediante procesadores de tipo `JoltTransformJSON` o `UpdateRecord`
- enrutado por tipo de evento con `RouteOnAttribute`
- publicación en Kafka mediante `PublishKafkaRecord_2_0` y, cuando procede, aterrizaje complementario en HDFS
- gestión de errores mediante colas de fallo y reintentos

Este diseño permite justificar tanto la trazabilidad de la ingesta como la separación entre dato bruto y dato preparado.

**Capturas asociadas**:
- canvas completo de NiFi con los procesadores conectados
- configuración de `InvokeHTTP`
- configuración de `PublishKafkaRecord_2_0` o del procesador equivalente utilizado
- evidencia visual de flujo procesado correctamente en las colas `success`

---

## 9. Evidencias y capturas

### 9.0 Dashboard de exposición

Se ha incorporado un dashboard ligero en `Streamlit` con el objetivo de facilitar la defensa del flujo KDD y reducir la dependencia de cambiar entre múltiples interfaces durante la exposición.

Incluye:
- resumen KDD y estado de cumplimiento de la rúbrica
- estado en vivo de servicios Docker con botones `On/Off`
- mapa de barcos con alertas de ruta y corredores marítimos reales
- recomendaciones de recuperación `barco vs aereo+camion`
- tablas curadas relevantes para la defensa
- diagramas integrados: flujo, secuencia, clases y casos de uso
- panel de stock de Valladolid y pedidos de Douai/Cleon
- Gantt por semanas industriales
- contenedor técnico superior de ingesta con arquitectura y métricas demo
- panel de alertas críticas con fallback demo si SMTP no está disponible

Nota técnica:
- para evitar bloqueos del metastore Derby, las consultas Hive en `spark-sql` y los `spark-submit` con soporte Hive deben ejecutarse de forma secuencial, no en paralelo.
- en esta red se confirmó salida HTTPS pero no SMTP, por lo que el envío real de correo puede fallar por timeout aunque la aplicación siga operativa gracias al fallback demo.

Ejecución:

```bash
bash scripts/67_run_dashboard.sh
```

Arranque ultraligero alternativo:

```bash
./start.sh
```

Elegir la opción `3) Dashboard solo (ultraligero)`.

- HDFS: UIs + `hdfs dfs -ls`.
- Kafka KRaft: topics creados + describe.
- Hive: tablas raw/staging/curated.
- GraphFrames: salida (rutas/riesgo).
- Airflow: DAG + runs.

### 9.1 Checklist exacto de evidencias operativas

Capturas ya realizadas e incorporables a la memoria:
- NiFi: canvas + `InvokeHTTP` + `PublishKafka`
- Kafka: consumo correcto de `datos_filtrados`
- Hive: `stg_weather_open_meteo`, `dim_ports_routes_weather`, `fact_weather_operational`
- Hive alertas: `fact_alerts`
- Cassandra: `vehicle_latest_state`
- HDFS curated: listado de `fact_weather_operational`, `fact_route_risk`, `fact_graph_centrality`
- GraphFrames: `fact_graph_centrality`
- Airflow: lista de DAGs + Graph de `logistica_kdd_microbatch` + Graph de `logistica_kdd_monthly_retrain`
- Rebuild tras reinicio validado: `scripts/66_rebuild_hive_demo_tables.sh` restaura dimensiones, staging y facts principales
- Dashboard: `Control Tower Valladolid` simplificado, horizonte de 10 semanas industriales completo e ingesta con flota sobre mar
- Dashboard: tabla de ETA con `Nombre de barco` y nombres reales de la flota demo
- Dashboard: recuperación del bundle tras incidencia de HDFS `safe mode`

Evidencias opcionales de refuerzo:
- reintento o reejecución controlada de Airflow, si aporta valor adicional a la defensa

Estado real al cierre de esta revisión:
- La memoria ya cuenta con evidencia de arquitectura, Kafka `datos_crudos` y `datos_filtrados`, HDFS raw y curated, `spark-submit`, `SHOW TABLES IN logistica`, Airflow con run exitoso, capturas del dashboard actual y la captura conjunta de las tres terminales del flujo end-to-end.
- No quedan pendientes reales de capturas obligatorias para la memoria.
- El reintento de Airflow permanece como evidencia opcional de refuerzo.

Tabla de cobertura real de capturas disponibles:

| Evidencia | Estado | Archivo disponible |
|---|---|---|
| NiFi canvas completo | Cubierto | `Capturas de pantalla/04_Nifi_Canvas.png` |
| NiFi en ejecución | Cubierto | `Capturas de pantalla/2Nifi_Vivo.png` |
| Grupo o visión general de proceso NiFi | Cubierto | `Capturas de pantalla/03_NIFI_Grupo Proceso.png` |
| Configuración `InvokeHTTP` | Cubierto | `Capturas de pantalla/05_Nifi_invokehttp.png` |
| Configuración `PublishKafka` | Cubierto | `Capturas de pantalla/06_Nifi_publishKafka.png` |
| Kafka topics | Cubierto | `Capturas de pantalla/07 Kafka_topics.png` |
| Kafka `datos_filtrados` | Cubierto | `Capturas de pantalla/08_kafka_datos_filtrados_ok.png` |
| Hive `stg_weather_open_meteo` | Cubierto | `Capturas de pantalla/09_Hive_stg_weather_open_meteo.png` |
| Hive dimensiones maestras | Cubierto | `Capturas de pantalla/10_Hive_dimensiones_maestras.png` |
| Hive `dim_ports_routes_weather` | Cubierto | `Capturas de pantalla/11_Hive_dim_ports_routes_weather.png` |
| Hive `fact_weather_operational` | Cubierto | `Capturas de pantalla/12__hive_fact_weather_operational.png` |
| Cassandra `vehicle_latest_state` | Cubierto | `Capturas de pantalla/13_cassandra_vehicle_lastest_state .png` |
| Airflow lista de DAGs | Cubierto | `Capturas de pantalla/14_airflow_lista_dags.png` |
| Airflow graph microbatch | Cubierto | `Capturas de pantalla/15_Airflow_microbatch_graph.png` |
| Airflow graph monthly retrain | Cubierto | `Capturas de pantalla/16_Airflow_monthly_graph.png` |
| HDFS curated | Cubierto | `Capturas de pantalla/17__hdfs_curated.png` |
| Hive `fact_graph_centrality` | Cubierto | `Capturas de pantalla/18_hive_fact_graph_centrality.png` |
| Hive `fact_alerts` | Cubierto | `Capturas de pantalla/19_Hive_fact_alerts.png` |
| Hive `fact_air_recovery_options` | Cubierto | `Capturas de pantalla/20_evidencia de fact_air_recovery_option.png` |
| Evidencia de envío mail | Cubierto | `Capturas de pantalla/21_prueba envio mail.png` |
| Arranque de servicios base | Cubierto | `Capturas de pantalla/1levanta_kafka_airflow_nifi_postgres .png` |
| Diagrama general de arquitectura final | Cubierto | `Capturas de pantalla/Diagrama General de Arquitectura.png` |
| Kafka `datos_crudos` con evidencia visual | Cubierto | `Capturas de pantalla/23_KafkaDatosCrudos.png` o `Capturas de pantalla/23B_KafkaDatosCrudos.png` |
| HDFS raw (`ships`, `clima`, `noticias`) | Cubierto | `Capturas de pantalla/24_hdfs_raw.png` |
| Tres terminales del flujo end-to-end | Cubierto | `Capturas de pantalla/29_tres_terminales_end_to_end.png` |
| `spark-submit` visible en ejecución | Cubierto | `Capturas de pantalla/26_spark_submit_staging.png.png` |
| `SHOW TABLES IN logistica` | Cubierto | `Capturas de pantalla/27_show_tables_logistica.png.png` |
| Airflow run exitoso | Cubierto | `Capturas de pantalla/28_airflow_run_exitoso.png` |
| Airflow reintento o reejecución controlada | Opcional | No obligatorio para la entrega |
| Dashboard actual `Control Tower Valladolid` | Cubierto | `Capturas de pantalla/30_dashboard_control_tower.png` |
| Dashboard actual tabla ETA con nombres reales | Cubierto | `Capturas de pantalla/31_dashboard_eta_barcos.png.png` |
| Dashboard actual vista de ingesta con barcos sobre mar | Cubierto | `Capturas de pantalla/31_dashboard_eta_barcos.png.png` |

**Kafka**

```bash
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic datos_crudos
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic datos_filtrados
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
docker compose exec -T namenode bash -lc '/opt/hadoop-3.2.1/bin/hdfs dfs -ls -R /hadoop/logistica/raw'
docker compose exec -T namenode bash -lc '/opt/hadoop-3.2.1/bin/hdfs dfs -ls -R /hadoop/logistica/staging'
docker compose exec -T namenode bash -lc '/opt/hadoop-3.2.1/bin/hdfs dfs -ls -R /hadoop/logistica/curated'
docker compose exec -T namenode bash -lc '/opt/hadoop-3.2.1/bin/hdfs dfs -ls -R /hadoop/logistica/master'
```

Capturar al menos:
- `stg_weather_open_meteo`
- `dim_ports_routes_weather`
- `fact_weather_operational`
- `fact_route_risk`
- `fact_graph_centrality`

**Hive / Spark SQL**

```bash
docker compose exec -T spark spark-sql -e "SHOW TABLES IN logistica"
docker compose exec -T spark spark-sql -e "SELECT * FROM logistica.stg_weather_open_meteo ORDER BY event_ts DESC LIMIT 5"
docker compose exec -T spark spark-sql -e "SELECT * FROM logistica.dim_ports_routes_weather ORDER BY event_ts DESC LIMIT 5"
docker compose exec -T spark spark-sql -e "SELECT * FROM logistica.fact_weather_operational ORDER BY event_ts DESC LIMIT 5"
docker compose exec -T spark spark-sql -e "SELECT * FROM logistica.fact_alerts ORDER BY severity DESC LIMIT 5"
docker compose exec -T spark spark-sql -e "SELECT * FROM logistica.fact_graph_centrality ORDER BY degree DESC LIMIT 5"
```

Comprobación tras reinicio del entorno:

```bash
bash scripts/66_rebuild_hive_demo_tables.sh
docker compose exec -T spark bash -lc 'spark-sql -S -e "SHOW TABLES IN logistica" 2>/dev/null'
```

**Cassandra**

```bash
docker compose exec -T cassandra cqlsh -e "SELECT ship_id, route_id, dest_port, warehouse, stock_on_hand, reorder_point FROM logistica.vehicle_latest_state;"
```

Evidencia ya validada en sesión:
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

1. Mostrar NiFi recibiendo API pública y enviando a Kafka.
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

La solución no solo ingiere y transforma datos, sino que también genera hechos analíticos, recomendaciones operativas, análisis de criticidad de red, consultas de baja latencia y una capa final de visualización ejecutiva en Streamlit. Esto permite defender el proyecto como una plataforma coherente de ingeniería de datos aplicada al dominio logístico.

---

## 10. Conclusiones

El proyecto resuelve un problema real de visibilidad logística sobre la cadena marítima y terrestre, integrando datos operativos, riesgos externos y estado de stock en una plataforma unificada orientada a la toma de decisiones.

Entre sus principales aportaciones destacan la trazabilidad del dato desde la ingesta hasta la visualización, la generación de facts analíticas defendibles, el uso de grafos para identificar nodos críticos y la incorporación de una capa operativa de contingencia capaz de comparar alternativas marítimas y aéreas.

Las limitaciones actuales son conocidas y asumidas en la memoria: el streaming real no constituye la ruta principal de defensa, Cassandra se alimenta en la práctica mediante micro-batch, la entrega final no se apoya en un despliegue cluster/YARN y el envío SMTP real depende de una red con salida saliente habilitada.

Como líneas de mejora futura, el proyecto puede ampliarse con mayor observabilidad, modelos predictivos más avanzados, automatización adicional en Airflow, integración con APIs de correo sobre HTTPS y una validación distribuida más completa sobre infraestructura multi-nodo.
