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
- `Spark` resuelve la limpieza, tipado, enriquecimiento y construcción de facts analíticas. Para maximizar el rendimiento, se ha implementado una estrategia de `Broadcast Hash Joins`. Esta técnica resulta adecuada para esta arquitectura porque tablas de dimensiones como `dim_ports` y `dim_warehouse` son de tamaño reducido. Al marcarlas para broadcast, Spark las distribuye en memoria a cada ejecutor antes de iniciar la transformación, permitiendo que el join con el flujo masivo de datos de barcos se resuelva localmente en cada nodo y eliminando el `shuffle`. El resultado es una reducción clara de latencia y una ejecución más estable en el entorno basado en contenedores.
- `Hive` actúa como capa SQL histórica y defendible para consulta de resultados.
- `GraphFrames` permite modelar la red logística como grafo y calcular criticidad estructural.
- `Cassandra` actúa como la capa de servicio de baja latencia para el dashboard, cubriendo el requisito de consulta rápida sobre el último estado operativo de la flota en contraposición a las consultas analíticas pesadas de Hive.
- `Airflow` aporta orquestación, dependencias, reintentos y trazabilidad de ejecución.

**Figura recomendada**: diagrama de arquitectura general del proyecto, integrando NiFi, Kafka, HDFS, Spark, Hive, Cassandra, Airflow y Dashboard.

---

## 2. Arquitectura final (KDD – Selección + Preprocesamiento)

### 2.1 Arquitectura (Lambda/Kappa adaptada)

La defensa se apoya en una ruta Docker/local reproducible. La estrategia principal es **micro-batch documentado**. Como evidencia técnica complementaria, se ha implementado el job `04_streaming_ml_pipeline.py` utilizando `Structured Streaming`.

Este componente permite procesar eventos mediante ventanas temporales de `15 minutes`, calculando medias móviles de retrasos y estados operativos. Aunque la defensa se centra en el modelo micro-batch para asegurar estabilidad durante la demo, esta implementación demuestra que el sistema puede operar también bajo una arquitectura Kappa más pura.

**Solución final defendida**:
- Ingesta real con NiFi hacia Kafka (`datos_crudos`, `datos_filtrados`, `alertas_globales`).
- Persistencia **raw/staging/curated** en HDFS y Hive.
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

Figura asociada en el documento final:
- `Figura 2. Estado de los servicios principales de la plataforma en el entorno Docker/local.`

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

Figura asociada en el documento final:
- `Figura 3. Servicios principales levantados en la ruta Docker/local para la validación integral del entorno.`

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

El grafo logístico actual se construye sobre **6 nodos** y **9 aristas**. Los nodos representan los puertos de origen, los puertos de entrada en España y el almacén final de Valladolid, mientras que las aristas modelan los corredores marítimos principales y el tramo final de distribución terrestre.

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

### 7.4 Modelos de IA complementarios

Como extensión de la fase de minería del proceso KDD, el proyecto incorpora una capa complementaria de analítica predictiva basada en `Spark MLlib`. Esta parte no constituye el eje principal de la defensa funcional, pero sí refuerza la capacidad del sistema para evolucionar desde una plataforma de integración y analítica operativa hacia un entorno con soporte a la predicción y a la segmentación inteligente.

En concreto, se han trabajado tres modelos. El primero es `K-Means`, utilizado para agrupar puertos según patrones de congestión y riesgo climático, facilitando una segmentación operativa de nodos logísticos. El segundo es `Random Forest Classifier`, empleado para clasificar escenarios de riesgo logístico y anticipar situaciones de criticidad, especialmente aquellas relacionadas con posibles roturas de stock o necesidad de contingencia. El tercero es `Linear Regression`, orientado a estimar o reajustar el `ETA` de llegada de los barcos a partir de variables temporales, meteorológicas y operativas.

Esta capa de modelos se presenta en la memoria como una ampliación analítica del bloque de minería, alineada con el uso de `Structured Streaming` y con la capacidad del proyecto para incorporar lógica predictiva sobre los datos ya integrados en la plataforma.

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

Como evidencia operativa adicional, se realizó una **reejecución controlada** de una tarea desde la vista `Graph` de Airflow. En lugar de provocar un fallo artificial, se utilizó la funcionalidad `Clear` sobre una tarea previamente completada, forzando su nueva planificación y ejecución. Esta operación demuestra que el pipeline admite relanzamientos parciales de forma segura, trazable y sin necesidad de modificar la definición del DAG.

Esta evidencia complementa la ejecución exitosa del DAG y refuerza la capacidad operativa del sistema para repetir fases concretas del flujo cuando resulta necesario refrescar datos o rehacer una etapa intermedia.

### 8.3 NiFi

NiFi actúa como la capa de adquisición y preparación inicial del dato. El flujo validado se estructura del siguiente modo:

- consumo de una API HTTP pública mediante `InvokeHTTP`
- transformación y normalización del payload mediante procesadores de tipo `JoltTransformJSON` o `UpdateRecord`
- enrutado por tipo de evento con `RouteOnAttribute`
- publicación en Kafka mediante `PublishKafkaRecord_2_0` y, cuando procede, aterrizaje complementario en HDFS
- gestión de errores mediante colas de fallo y reintentos

Para garantizar robustez ante picos de tráfico (`data spikes`), se ha configurado el mecanismo de `Back-Pressure` en las conexiones entre procesadores de NiFi. En concreto, se definió un `Back Pressure Object Threshold` de `10.000` objetos, de forma que el flujo se detenga automáticamente aguas arriba si Kafka o HDFS presentan latencia. Con ello se evita el desbordamiento de memoria y se protege la integridad de los eventos.

Este diseño permite justificar tanto la trazabilidad de la ingesta como la separación entre dato bruto y dato preparado.

Las Figuras 4 a 10 documentan el canvas de NiFi, la configuración de `InvokeHTTP` y `PublishKafka`, y la ejecución correcta del flujo.

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

## 10. Conclusiones

El proyecto resuelve un problema real de visibilidad logística sobre la cadena marítima y terrestre, integrando datos operativos, riesgos externos y estado de stock en una plataforma unificada orientada a la toma de decisiones.

Entre sus principales aportaciones destacan la trazabilidad del dato desde la ingesta hasta la visualización, la generación de facts analíticas defendibles, el uso de grafos para identificar nodos críticos y la incorporación de una capa operativa de contingencia capaz de comparar alternativas marítimas y aéreas.

Las limitaciones actuales son conocidas y asumidas en la memoria: el streaming real no constituye la ruta principal de defensa, Cassandra se alimenta en la práctica mediante micro-batch, la entrega final no se apoya en un despliegue cluster/YARN y el envío SMTP real depende de una red con salida saliente habilitada.

La arquitectura ha sido diseñada bajo principios de escalabilidad horizontal. Aunque la demostración actual se despliega sobre contenedores Docker, los artefactos de Spark y la configuración de HDFS son compatibles con su ejecución sobre un gestor de recursos `YARN` en modo clúster. Esto garantiza que la plataforma pueda escalar para procesar volúmenes masivos de datos logísticos sin requerir cambios en el código analítico.

Como líneas de mejora futura, el proyecto puede ampliarse con mayor observabilidad, modelos predictivos más avanzados, automatización adicional en Airflow, integración con APIs de correo sobre HTTPS y una validación distribuida más completa sobre infraestructura multi-nodo.
