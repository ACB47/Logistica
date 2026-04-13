from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


WEBHDFS_BASE = "http://namenode:9870/webhdfs/v1"
SPARK_CONTAINER = "logistica-spark-1"
CASSANDRA_CONTAINER = "logistica-cassandra-1"


DEFAULT_ARGS = {
    "owner": "logistica",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": True,
    "email": [os.getenv("AIRFLOW_ALERT_EMAIL", "logistica-alerts@outlook.com")],
}


with DAG(
    dag_id="logistica_kdd_microbatch",
    description="KDD logistica: micro-batch documentado desde landing hasta facts y limpieza HDFS",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 3, 18),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["kdd", "logistica", "microbatch"],
) as dag:
    pyspark_python = "/home/hadoop/PROYECTOLOGISTICA/.venv-ingesta/bin/python3"

    ensure_hdfs_paths = BashOperator(
        task_id="ensure_hdfs_paths",
        bash_command=(
            "python3 - <<'PY'\n"
            "from urllib.request import urlopen, Request\n"
            f"base = '{WEBHDFS_BASE}'\n"
            "paths = [\n"
            "    '/hadoop/logistica/raw/ships',\n"
            "    '/user/logistica/raw/opensky',\n"
            "    '/hadoop/logistica/raw/noticias',\n"
            "    '/hadoop/logistica/raw/clima',\n"
            "    '/hadoop/logistica/staging',\n"
            "    '/hadoop/logistica/curated',\n"
            "]\n"
            "for path in paths:\n"
            "    req = Request(f\"{base}{path}?op=MKDIRS&user.name=root\", method='PUT')\n"
            "    with urlopen(req) as resp:\n"
            "        print(path, resp.status)\n"
            "PY"
        ),
    )

    spark_raw_to_staging = BashOperator(
        task_id="spark_raw_to_staging",
        bash_command=(
            f"docker exec {SPARK_CONTAINER} spark-submit /home/jovyan/jobs/spark/01_raw_to_staging.py"
        ),
    )

    spark_build_dimensions = BashOperator(
        task_id="spark_build_dimensions",
        bash_command=(
            f"docker exec {SPARK_CONTAINER} spark-submit /home/jovyan/jobs/spark/01_load_master_dimensions.py"
        ),
    )

    spark_weather_filtered_to_staging = BashOperator(
        task_id="spark_weather_filtered_to_staging",
        bash_command=(
            f"docker exec {SPARK_CONTAINER} spark-submit "
            "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 "
            "/home/jovyan/jobs/spark/01_weather_filtered_to_staging.py --bootstrap kafka:9092 --topic datos_filtrados"
        ),
    )

    spark_weather_port_enrichment = BashOperator(
        task_id="spark_weather_port_enrichment",
        bash_command=(
            f"docker exec {SPARK_CONTAINER} spark-submit /home/jovyan/jobs/spark/02_weather_port_enrichment.py"
        ),
    )

    spark_build_graph_metrics = BashOperator(
        task_id="spark_build_graph_metrics",
        bash_command=(
            f"docker exec {SPARK_CONTAINER} spark-submit "
            "--packages graphframes:graphframes:0.8.3-spark3.5-s_2.12 "
            "/home/jovyan/jobs/spark/02_graph_metrics.py"
        ),
    )

    spark_score_and_alert = BashOperator(
        task_id="spark_score_and_alert",
        bash_command=(
            f"docker exec {SPARK_CONTAINER} spark-submit /home/jovyan/jobs/spark/03_score_and_alert.py"
        ),
    )

    spark_weather_operational_fact = BashOperator(
        task_id="spark_weather_operational_fact",
        bash_command=(
            f"docker exec {SPARK_CONTAINER} spark-submit /home/jovyan/jobs/spark/03_weather_operational_fact.py"
        ),
    )

    cassandra_latest_vehicle_state = BashOperator(
        task_id="cassandra_latest_vehicle_state",
        bash_command=(
            f"TMP_TSV=/tmp/vehicle_latest_state.tsv && "
            f"docker exec {CASSANDRA_CONTAINER} cqlsh -e \"CREATE KEYSPACE IF NOT EXISTS logistica WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}; USE logistica; CREATE TABLE IF NOT EXISTS vehicle_latest_state (ship_id text PRIMARY KEY, event_ts timestamp, route_id text, origin_port text, dest_port text, warehouse text, lat double, lon double, speed_kn double, heading double, stock_on_hand int, reorder_point int, updated_at timestamp); TRUNCATE logistica.vehicle_latest_state;\" && "
            f"docker exec {SPARK_CONTAINER} spark-submit /home/jovyan/jobs/spark/04_export_latest_vehicle_state.py > $TMP_TSV && "
            f"while IFS=$'\\t' read -r ship_id event_ts route_id origin_port dest_port warehouse lat lon speed_kn heading stock_on_hand reorder_point; do "
            "[[ -z \"$ship_id\" ]] && continue; "
            f"docker exec {CASSANDRA_CONTAINER} cqlsh -e \"INSERT INTO logistica.vehicle_latest_state (ship_id, event_ts, route_id, origin_port, dest_port, warehouse, lat, lon, speed_kn, heading, stock_on_hand, reorder_point, updated_at) VALUES ('$ship_id', '$event_ts', '$route_id', '$origin_port', '$dest_port', '$warehouse', $lat, $lon, $speed_kn, $heading, $stock_on_hand, $reorder_point, toTimestamp(now()));\" < /dev/null; "
            "done < $TMP_TSV"
        ),
    )

    cleanup_hdfs_tmp = BashOperator(
        task_id="cleanup_hdfs_tmp",
        bash_command=(
            "python3 - <<'PY'\n"
            "from urllib.request import urlopen, Request\n"
            f"base = '{WEBHDFS_BASE}'\n"
            "mkdir_paths = ['/hadoop/logistica/checkpoint', '/hadoop/logistica/tmp']\n"
            "for path in mkdir_paths:\n"
            "    req = Request(f\"{base}{path}?op=MKDIRS&user.name=root\", method='PUT')\n"
            "    with urlopen(req) as resp:\n"
            "        print(path, resp.status)\n"
            "delete_req = Request(f\"{base}/hadoop/logistica/tmp?op=DELETE&recursive=true&user.name=root\", method='DELETE')\n"
            "with urlopen(delete_req) as resp:\n"
            "    print('/hadoop/logistica/tmp', resp.status)\n"
            "req = Request(f\"{base}/hadoop/logistica/tmp?op=MKDIRS&user.name=root\", method='PUT')\n"
            "with urlopen(req) as resp:\n"
            "    print('/hadoop/logistica/tmp', resp.status)\n"
            "PY"
        ),
    )

    ensure_hdfs_paths >> spark_raw_to_staging >> spark_build_dimensions
    spark_build_dimensions >> spark_weather_filtered_to_staging >> spark_weather_port_enrichment >> spark_weather_operational_fact
    spark_build_dimensions >> spark_build_graph_metrics >> spark_score_and_alert
    [spark_weather_operational_fact, spark_score_and_alert] >> cassandra_latest_vehicle_state >> cleanup_hdfs_tmp


with DAG(
    dag_id="logistica_kdd_monthly_retrain",
    description="KDD logistica: reentrenamiento mensual de grafo y limpieza de temporales HDFS",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 3, 18),
    schedule="0 3 1 * *",
    catchup=False,
    max_active_runs=1,
    tags=["kdd", "logistica", "monthly"],
) as monthly_dag:
    pyspark_python = "/home/hadoop/PROYECTOLOGISTICA/.venv-ingesta/bin/python3"

    monthly_build_dimensions = BashOperator(
        task_id="monthly_build_dimensions",
        bash_command=(
            f"docker exec {SPARK_CONTAINER} spark-submit /home/jovyan/jobs/spark/01_load_master_dimensions.py"
        ),
    )

    monthly_retrain_graph = BashOperator(
        task_id="monthly_retrain_graph",
        bash_command=(
            f"docker exec {SPARK_CONTAINER} spark-submit "
            "--packages graphframes:graphframes:0.8.3-spark3.5-s_2.12 /home/jovyan/jobs/spark/02_graph_metrics.py"
        ),
    )

    monthly_refresh_weather_fact = BashOperator(
        task_id="monthly_refresh_weather_fact",
        bash_command=(
            f"docker exec {SPARK_CONTAINER} spark-submit /home/jovyan/jobs/spark/03_weather_operational_fact.py"
        ),
    )

    monthly_cleanup_hdfs_tmp = BashOperator(
        task_id="monthly_cleanup_hdfs_tmp",
        bash_command=(
            "python3 - <<'PY'\n"
            "from urllib.request import urlopen, Request\n"
            f"base = '{WEBHDFS_BASE}'\n"
            "mkdir_paths = ['/hadoop/logistica/checkpoint', '/hadoop/logistica/tmp']\n"
            "for path in mkdir_paths:\n"
            "    req = Request(f\"{base}{path}?op=MKDIRS&user.name=root\", method='PUT')\n"
            "    with urlopen(req) as resp:\n"
            "        print(path, resp.status)\n"
            "for path in ['/hadoop/logistica/tmp', '/hadoop/logistica/checkpoint/ships', '/hadoop/logistica/checkpoint/alerts']:\n"
            "    req = Request(f\"{base}{path}?op=DELETE&recursive=true&user.name=root\", method='DELETE')\n"
            "    with urlopen(req) as resp:\n"
            "        print(path, resp.status)\n"
            "PY"
        ),
    )

    monthly_build_dimensions >> monthly_retrain_graph >> monthly_refresh_weather_fact >> monthly_cleanup_hdfs_tmp
