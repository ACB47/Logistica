# Fase KDD I — Ingesta y Selección (Kafka -> HDFS raw)

Objetivo: generar eventos del caso de uso, publicarlos en Kafka y dejar copia raw en HDFS.

Nota importante:
- En este repositorio la "API" se emula con productores Python (`ships_gps_producer.py`, `alerts_producer.py`).
- En la fase final se podrá sustituir por APIs reales o por NiFi invocando HTTP(s) de fuentes externas.

## 1) Preparación en standalone (solo master)

En `master`:

```bash
cd /home/hadoop/PROYECTOLOGISTICA
source scripts/12_use_existing_kafka.sh
```

## 2) Kafka local

Arranca Kafka en `master` y crea topics:

```bash
export KAFKA_HOME=/home/hadoop/Descargas/kafka_2.13-4.1.1
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server master:9092 --create --if-not-exists --topic datos_crudos --partitions 1 --replication-factor 1
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server master:9092 --create --if-not-exists --topic alertas_globales --partitions 1 --replication-factor 1
$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server master:9092 --list
```

## 3) Crear rutas HDFS raw (master)

```bash
hdfs dfs -mkdir -p /hadoop/logistica/raw/ships
hdfs dfs -mkdir -p /user/logistica/raw/opensky
hdfs dfs -mkdir -p /hadoop/logistica/raw/noticias
hdfs dfs -mkdir -p /hadoop/logistica/raw/clima
```

## 4) Python venv para productores/consumidores (master)

```bash
cd /home/hadoop/PROYECTOLOGISTICA
python3 -m venv .venv-ingesta
source .venv-ingesta/bin/activate
pip install --upgrade pip
pip install -r ingesta/requirements.txt
```

## 5) Lanzar productores (master)

Terminal A (barcos → `datos_crudos`):

```bash
source .venv-ingesta/bin/activate
python3 ingesta/productores/ships_gps_producer.py --bootstrap master:9092 --topic datos_crudos
```

Terminal B (alertas clima/noticias → `alertas_globales`):

```bash
source .venv-ingesta/bin/activate
python3 ingesta/productores/alerts_producer.py --bootstrap master:9092 --topic alertas_globales
```

## 6) Landing raw en HDFS (master)

Terminal C (consume ambos topics y escribe ficheros JSONL rotados a HDFS):

```bash
source .venv-ingesta/bin/activate
python3 ingesta/consumidores/kafka_to_hdfs_raw.py --bootstrap master:9092 --group-id logistica-raw-sink --spool-dir /tmp/logistica_spool --flush-every-sec 30
```

Verificación:

```bash
hdfs dfs -ls -R /hadoop/logistica/raw
hdfs dfs -ls -R /user/logistica/raw/opensky
```

## 7) Arranque mínimo recomendado para demo

Para una demo rápida y evidencia en capturas:
1. Arranca productores (Terminal A y B) durante 2-3 minutos.
2. Mantén el consumidor (Terminal C) activo hasta ver ficheros `.jsonl` en `/hadoop/logistica/raw/...`.
3. Lanza `spark-submit jobs/spark/01_raw_to_staging.py` para refrescar staging.

