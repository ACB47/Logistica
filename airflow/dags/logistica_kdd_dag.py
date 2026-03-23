from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


DEFAULT_ARGS = {
    "owner": "logistica",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="logistica_kdd_microbatch",
    description="KDD logística: micro-lotes HDFS→Spark→Hive→alertas",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 3, 18),
    schedule="*/10 * * * *",  # cada 10 minutos (ajustable)
    catchup=False,
    max_active_runs=1,
    tags=["kdd", "logistica", "microbatch"],
) as dag:
    pyspark_python = "/home/hadoop/PROYECTOLOGISTICA/.venv-ingesta/bin/python3"

    ensure_hdfs_paths = BashOperator(
        task_id="ensure_hdfs_paths",
        bash_command=(
            "hdfs dfs -mkdir -p "
            "/hadoop/logistica/raw/ships "
            "/user/logistica/raw/opensky "
            "/hadoop/logistica/raw/noticias "
            "/hadoop/logistica/raw/clima "
            "&& hdfs dfs -mkdir -p /hadoop/logistica/staging /hadoop/logistica/curated"
        ),
    )

    spark_raw_to_staging = BashOperator(
        task_id="spark_raw_to_staging",
        bash_command=(
            f"export PYSPARK_PYTHON={pyspark_python} && "
            "spark-submit "
            "--master local[*] "
            "--deploy-mode client "
            "jobs/spark/01_raw_to_staging.py"
        ),
    )

    spark_build_graph_metrics = BashOperator(
        task_id="spark_build_graph_metrics",
        bash_command=(
            f"export PYSPARK_PYTHON={pyspark_python} && "
            "spark-submit "
            "--master local[*] "
            "--deploy-mode client "
            "--packages graphframes:graphframes:0.8.3-spark3.5-s_2.12 "
            "jobs/spark/02_graph_metrics.py"
        ),
    )

    spark_score_and_alert = BashOperator(
        task_id="spark_score_and_alert",
        bash_command=(
            f"export PYSPARK_PYTHON={pyspark_python} && "
            "spark-submit "
            "--master local[*] "
            "--deploy-mode client "
            "jobs/spark/03_score_and_alert.py"
        ),
    )

    ensure_hdfs_paths >> spark_raw_to_staging >> spark_build_graph_metrics >> spark_score_and_alert

