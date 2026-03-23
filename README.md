# Proyecto Logística Marítima (Big Data)

Este repositorio contiene la **guía paso a paso** y los **artefactos** para el proyecto integral de Ingeniería de Datos (ciclo KDD) usando stack Apache.

## Modo de ejecución elegido (simplificado)

- Ejecución en **standalone** sobre `master` para reducir complejidad operativa.
- Se mantiene el stack lógico del proyecto (Kafka + HDFS + Spark + Hive + Airflow), pero sin depender de ejecución distribuida en slaves.
- Los scripts de referencia para este modo son:
  - `scripts/50_start_standalone.sh`
  - `scripts/51_stop_standalone.sh`

## Restricciones del proyecto (según enunciado + criterio)

- **Sí usamos**: HDFS, Kafka en modo **KRaft**, Spark (SQL + GraphFrames), Hive, Airflow, NiFi (si aplica).
- **NO usamos**: Apache Cassandra, **NO** usamos Spark **Structured Streaming**.

> Nota: el PDF original menciona Cassandra y Structured Streaming como parte de la rúbrica. En esta entrega lo sustituimos por:
> - Persistencia **solo SQL en Hive** (tablas raw/staging/curated).
> - “Near‑real‑time” mediante **micro‑lotes**: ingestamos a HDFS y procesamos en intervalos cortos con Spark (batch) orquestado por Airflow.

## Caso de uso (resumen)

- **Origen**: puerto Shanghai (China).
- **Destino**: Algeciras / Valencia / Barcelona.
- **Empresa**: almacén y clientes en Valladolid. Aduanas internas.
- **Objetivo**: monitorizar barcos (GPS) y, si hay alertas (geopolítica o clima adverso), **avisar** para activar un plan de contingencia: vuelo urgente para evitar ruptura de stock.

## Kafka topics

- `datos_crudos`
- `alertas_globales`

## HDFS (raw landing)

- Barcos: `/hadoop/logistica/raw/ships`
- Aviones (OpenSky): `/user/logistica/raw/opensky`
- Noticias (geopolítica): `/hadoop/logistica/raw/noticias`
- Clima: `/hadoop/logistica/raw/clima`

## Documentación

- Guion principal: `docs/00_documento_word.md`

## Estado actual del proyecto (checkpoint)

- Servicios base levantados: HDFS/YARN, Hive Metastore, Kafka KRaft (3 nodos), Airflow.
- Pipeline KDD implementado (batch/micro-lote):
  - `jobs/spark/01_raw_to_staging.py`
  - `jobs/spark/02_graph_metrics.py`
  - `jobs/spark/03_score_and_alert.py`
- Tablas Hive verificadas:
  - staging: `logistica.stg_ships`, `logistica.stg_alerts_clima`, `logistica.stg_alerts_noticias`
  - curadas: `logistica.fact_route_risk`, `logistica.fact_graph_hops`, `logistica.fact_alerts`
- Airflow operativo con DAG: `logistica_kdd_microbatch`.

## Pendiente para cierre completo de entrega

- Inyección con fuentes tipo API (o su emulación controlada) documentada de forma explícita.
- Diseño/diagrama del flujo NiFi para ingestión y enrutado de eventos.
- Completar documento Word con capturas finales (Airflow run, tablas Hive, flujo de ingesta y NiFi).

