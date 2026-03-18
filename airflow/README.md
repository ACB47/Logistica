# Airflow (orquestación KDD)

Como no usamos Structured Streaming, orquestamos el “casi tiempo real” con **micro‑lotes**:

- cada N minutos: leer raw de HDFS → staging/curated (Hive) → métricas grafo → scoring → alertas.

## Instalación recomendada (venv)

```bash
cd /home/hadoop/PROYECTOLOGISTICA
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r airflow/requirements.txt
```

## Inicialización local

```bash
export AIRFLOW_HOME=$PWD/.airflow
airflow db init
airflow users create \
  --username admin --password admin \
  --firstname Admin --lastname Admin \
  --role Admin --email admin@example.com
```

## Ejecutar

Terminal 1:

```bash
export AIRFLOW_HOME=$PWD/.airflow
airflow webserver --port 8080
```

Terminal 2:

```bash
export AIRFLOW_HOME=$PWD/.airflow
airflow scheduler
```

## DAGs

- Copia/activa los DAGs desde `airflow/dags/` (Airflow los detecta si `AIRFLOW_HOME/dags` apunta ahí o si configuras `dags_folder`).

