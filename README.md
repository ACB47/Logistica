# Proyecto Logística Marítima (Big Data)

Este repositorio contiene la **guía paso a paso** y los **artefactos** para el proyecto integral de Ingeniería de Datos (ciclo KDD) usando stack Apache.

## Restricciones del proyecto (según enunciado + tu criterio)

- **Sí usamos**: VirtualBox (3 VMs: `master`, `slave01`, `slave02`), HDFS, YARN, Kafka en modo **KRaft**, Spark (SQL + GraphFrames), Hive, Airflow, NiFi (si aplica).
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

