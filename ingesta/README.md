# Fase KDD I — Ingesta y Selección (Kafka → HDFS raw)

Objetivo: generar eventos del caso de uso, publicarlos en Kafka y dejar **copia raw** en HDFS.

## 1) Preparación en cada VM (master/slaves)

En cada VM:

```bash
cd /home/hadoop/PROYECTOLOGISTICA
source scripts/12_use_existing_kafka.sh
```

## 2) Kafka KRaft (3 nodos)

Sigue `kafka/kraft/README.md` y arranca Kafka en `master`, `slave01`, `slave02`.

Cuando esté arriba, desde `master`:

```bash
bash scripts/20_create_kafka_topics.sh master:9092
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

Terminal C (consume ambos topics y escribe ficheros JSONL “rotados” a HDFS):

```bash
source .venv-ingesta/bin/activate
python3 ingesta/consumidores/kafka_to_hdfs_raw.py --bootstrap master:9092
```

Verificación:

```bash
hdfs dfs -ls -R /hadoop/logistica/raw
hdfs dfs -ls -R /user/logistica/raw/opensky
```

