# SESSION_STATUS.md

Estado rapido del proyecto para poder retomar la sesion sin reanalizar todo el repo.

## Ultima actualizacion
- Fecha de referencia: 2026-03-25
- Contexto: se reviso el enunciado de `Proyecto Big Data.pdf`, se creo `TODO.md`, se ajusto `AGENTS.md`, se empezo la alineacion del stack con la rubrica y se anadio un script para arrancar/parar Docker por perfiles.

## Resumen ejecutivo
- El proyecto ya tiene una base funcional de demo: productores Kafka, landing raw en HDFS, jobs Spark batch, tablas Hive, soporte Cassandra, notebooks Zeppelin y un DAG de Airflow.
- El proyecto aun no esta cerrado respecto al enunciado final 2025-2026.
- Ya se inicio la alineacion del stack Docker: Kafka paso a KRaft en Compose, NiFi subio a `2.6.0`, Cassandra a `5.0`, y se documento que la ruta oficial de entrega es la de cluster/VM; Docker queda como demo local.
- NiFi si esta presente en Docker y ahora hay un script de gestion por perfiles para evitar levantar todo el stack cuando no hace falta: `scripts/60_manage_docker_stack.sh`.
- Se probo `simple core`, pero el arranque quedo bloqueado por falta de espacio en disco en la maquina host.
- Se libero espacio en disco eliminando cache Docker, cache de `pip` y backends pesados de LM Studio.
- Se reprobo el arranque `simple core` y esta operativo: Kafka KRaft, Postgres, Airflow y NiFi levantan correctamente.
- La inicializacion de Kafka ya crea los tres topics base del proyecto: `datos_crudos`, `alertas_globales` y `datos_filtrados`.
- Se dejo preparada la base del primer flujo real de NiFi con Open-Meteo, credenciales fijas locales y script de healthcheck autenticado.
- El flujo real de NiFi ya quedo validado extremo a extremo con mensajes visibles en Kafka tanto en `datos_crudos` como en `datos_filtrados`.
- Ya existe un job Spark dedicado para mover `datos_filtrados` a Hive staging: `jobs/spark/01_weather_filtered_to_staging.py`.
- El flujo de NiFi ya esta exportado para reutilizarlo y documentarlo en `docs/nifi/OpenMeteo_Kafka_Flow.json`.
- Queda preparado un wrapper para lanzar el job de weather staging: `scripts/63_run_weather_filtered_staging.sh`.
- Ya se probaron varias veces los pasos reales de Spark/Hive; el bloqueo actual ya no es Kafka ni permisos base, sino HDFS sin DataNode utilizable para escritura.
- El despliegue HDFS Docker ya fue corregido: NameNode y DataNode registran correctamente con `fs.defaultFS=hdfs://namenode:8020` y volumenes separados.
- El flujo completo `datos_filtrados -> Spark -> Hive/HDFS` ya esta validado con 3 filas reales cargadas en `logistica.stg_weather_open_meteo`.
- Ya existe una segunda capa KDD sobre el clima: `logistica.dim_ports_routes_weather`, que enriquece `stg_weather_open_meteo` con contexto de ruta, almacen y estado operativo.
- Lo mas importante pendiente ahora es NiFi real con API publica, YARN, topic filtrado, streaming/ventanas de 15 minutos, caso real de Cassandra y documentacion final.

## Estado por fases KDD

### Fase 0 - Base del repositorio
- Estado: avanzada.
- Hecho:
  - `AGENTS.md` creado y actualizado.
  - `TODO.md` creado con checklist completo.
  - Estructura principal del repo identificada.
  - `SESSION_STATUS.md` ya sirve como punto de reanudacion.
- Pendiente:
  - Mantener `SESSION_STATUS.md` al final de cada bloque de trabajo relevante.

### Fase I - Ingesta y seleccion
- Estado: parcial pero funcional.
- Hecho:
  - Productor GPS: `ingesta/productores/ships_gps_producer.py`
  - Productor alertas: `ingesta/productores/alerts_producer.py`
  - Landing raw Kafka -> HDFS: `ingesta/consumidores/kafka_to_hdfs_raw.py`
  - Topics actuales: `datos_crudos`, `alertas_globales`
  - Kafka Docker alineado a KRaft en `docker-compose.yml` y `docker-compose.simple.yml`
  - NiFi Docker alineado a `2.6.0`
  - Script de gestion Docker por perfiles: `scripts/60_manage_docker_stack.sh`
  - Comprobado que NiFi esta declarado en Docker, pero no arranca mientras no se libere espacio en disco
  - Verificado acceso HTTPs a NiFi con respuesta `302` en `https://localhost:8443`
  - Kafka crea correctamente `datos_crudos`, `alertas_globales` y `datos_filtrados` en el arranque ligero
  - NiFi ya usa credenciales locales definidas por entorno y healthcheck funcional
  - Documentado primer flujo real en `docs/01_nifi_open_meteo_flow.md`
  - Script de verificacion NiFi: `scripts/61_nifi_healthcheck.sh`
  - Flujo Open-Meteo validado con consumo real en Kafka:
    - `datos_crudos` contiene el JSON original de la API
    - `datos_filtrados` contiene el JSON transformado para analitica
  - Export del canvas NiFi disponible en `docs/nifi/OpenMeteo_Kafka_Flow.json`
  - Stack `simple core` reanudado con Kafka, NiFi y Postgres activos; Airflow vuelve a levantar tras recrear el contenedor
- Falta para cerrar:
  - flujo real con NiFi y API publica
  - topic de `datos_filtrados`
  - back-pressure y ruta de errores defendibles
  - evidencias finales de auditoria raw

### Fase II - Preprocesamiento y transformacion
- Estado: bastante avanzada.
- Hecho:
  - staging en Hive/HDFS con `jobs/spark/01_raw_to_staging.py`
  - nuevo staging desde Kafka `datos_filtrados` a Hive con `jobs/spark/01_weather_filtered_to_staging.py`
  - limpieza, `dropna`, `dropDuplicates`, schemas explicitos
  - analitica de grafos con `jobs/spark/02_graph_metrics.py`
  - `01_weather_filtered_to_staging.py` ya incorpora `fs.defaultFS` hacia `hdfs://namenode:8020`
  - se creo `/user/hive/warehouse` y `/hadoop/logistica/staging` con permisos de escritura en HDFS
  - `spark-submit` del job ya funciona con el paquete Kafka correcto y crea `logistica.stg_weather_open_meteo`
  - tabla validada con datos reales:
    - `2026-03-26T19:00:00 | 15.1 | 75 | 38.7 | severity 3`
    - `2026-03-26T18:55:00 | 14.5 | 78 | 22.1 | severity 2`
    - `2026-03-26T18:50:00 | 14.2 | 80 | 12.4 | severity 1`
  - enriquecimiento validado en `logistica.dim_ports_routes_weather`:
    - `Algeciras | Shanghai | route-shanghai-algeciras | Valladolid | severity 3 | MEDIO | METEO_VIGILANCIA | 6.0h`
    - `Algeciras | Shanghai | route-shanghai-algeciras | Valladolid | severity 2 | MEDIO | OPERATIVO | 3.0h`
    - `Algeciras | Shanghai | route-shanghai-algeciras | Valladolid | severity 1 | BAJO | OPERATIVO | 1.0h`
- Falta para cerrar:
  - sustituir la dimension embebida de puertos/rutas por dimensiones maestras reales en Hive
  - mejor cierre del caso de grafos para la defensa
  - validacion en YARN del flujo principal
  - terminar de preparar o reutilizar `.venv-jobs` para ejecucion local con PySpark si se quiere correr fuera del cluster

### Fase III - Mineria y accion
- Estado: parcial.
- Hecho:
  - scoring y alertas batch: `jobs/spark/03_score_and_alert.py`
  - pipeline streaming/ML experimental: `jobs/spark/04_streaming_ml_pipeline.py`
  - modulo email: `jobs/spark/05_email_alerts.py`
  - Cassandra Docker subida a `5.0` en Compose
- Falta para cerrar:
  - ajustar ventanas a 15 minutos
  - decidir estrategia final: streaming real o micro-batch defendido
  - caso Cassandra de ultimo estado por vehiculo
  - integracion completa de alertas y pruebas de extremo a extremo

### Fase IV - Orquestacion
- Estado: parcial y utilizable.
- Hecho:
  - DAG existente: `airflow/dags/logistica_kdd_dag.py`
  - dependencias y reintentos basicos
- Falta para cerrar:
  - DAG orientado al requisito de reentrenamiento mensual
  - limpieza de temporales en HDFS
  - alertas de fallo mejor definidas
  - validacion en la version final de Airflow de la entrega

### Fase V - Documentacion y defensa
- Estado: empezada pero no cerrada.
- Hecho:
  - guia base: `docs/00_documento_word.md`
  - notebooks Zeppelin existentes en `zeppelin/`
- Falta para cerrar:
  - memoria final completa
  - capturas obligatorias
  - narrativa clara de arquitectura final y limitaciones
  - guion de demo final

## Desajustes importantes detectados contra el enunciado
- Kafka Docker ya esta pasado a KRaft, pero falta validarlo en una ejecucion real del stack.
- NiFi en Docker ya esta alineado a la version objetivo, pero sigue faltando un flujo real configurado.
- Airflow en Docker principal no esta alineado claramente con 2.10.x.
- Cassandra Docker ya esta en 5.0; falta validar compatibilidad en una ejecucion real.
- Falta evidenciar YARN en la ruta principal de ejecucion.
- Falta separar `datos_crudos` y `datos_filtrados`.
- El job de streaming usa ventanas de 5 minutos, no 15.
- En Docker full, HDFS ya escribe correctamente; ahora el punto a vigilar es no perder mensajes de Kafka al recrear el broker durante pruebas.
- El bloqueo por espacio en disco ya no aplica: `/` volvio a tener margen libre suficiente tras la limpieza.

## Decision de arquitectura actual
- Ruta oficial de entrega: cluster/VM con HDFS + YARN + Kafka KRaft + Spark + Hive + Cassandra + Airflow + NiFi.
- Ruta Docker: demo local y validacion rapida del stack, no sustituto completo de la defensa de la rubrica.
- Esta decision ya esta reflejada tambien en `README.md`.

## Archivos clave para retomar
- `TODO.md`
- `AGENTS.md`
- `SESSION_STATUS.md`
- `scripts/60_manage_docker_stack.sh`
- `scripts/61_nifi_healthcheck.sh`
- `scripts/63_run_weather_filtered_staging.sh`
- `docs/00_documento_word.md`
- `docs/01_nifi_open_meteo_flow.md`
- `airflow/dags/logistica_kdd_dag.py`
- `jobs/spark/01_raw_to_staging.py`
- `jobs/spark/01_weather_filtered_to_staging.py`
- `jobs/spark/02_weather_port_enrichment.py`
- `jobs/spark/02_graph_metrics.py`
- `jobs/spark/03_score_and_alert.py`
- `jobs/spark/04_streaming_ml_pipeline.py`
- `ingesta/consumidores/kafka_to_hdfs_raw.py`
- `docker-compose.yml`
- `docker-compose.simple.yml`

## Siguiente bloque recomendado
1. Capturar este resultado para la memoria: canvas NiFi, topic `datos_filtrados`, tabla `logistica.stg_weather_open_meteo`, tabla `logistica.dim_ports_routes_weather` y sus parquet en HDFS.
2. Conectar `dim_ports_routes_weather` con `fact_route_risk` o `fact_alerts` para construir una fact table operativa final.
3. Ejecutar y documentar al menos un job Spark en YARN.
4. Cerrar el caso Cassandra de baja latencia y ajustar streaming a ventanas de 15 minutos.
5. Afinar Airflow para orquestar este flujo y sus dependencias.

## Regla de mantenimiento
- Cada vez que se cierre un bloque de trabajo relevante, actualizar este archivo con:
  - que se hizo
  - que queda pendiente
  - en que fase estamos
  - siguiente paso recomendado
