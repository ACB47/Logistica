from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


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

    spark_build_dimensions = BashOperator(
        task_id="spark_build_dimensions",
        bash_command=(
            f"export PYSPARK_PYTHON={pyspark_python} && "
            "spark-submit "
            "--master local[*] "
            "--deploy-mode client "
            "jobs/spark/01_load_master_dimensions.py"
        ),
    )

    spark_weather_filtered_to_staging = BashOperator(
        task_id="spark_weather_filtered_to_staging",
        bash_command=(
            f"export PYSPARK_PYTHON={pyspark_python} && "
            "spark-submit "
            "--master local[*] "
            "--deploy-mode client "
            "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 "
            "jobs/spark/01_weather_filtered_to_staging.py --bootstrap kafka:9092 --topic datos_filtrados"
        ),
    )

    spark_weather_port_enrichment = BashOperator(
        task_id="spark_weather_port_enrichment",
        bash_command=(
            f"export PYSPARK_PYTHON={pyspark_python} && "
            "spark-submit "
            "--master local[*] "
            "--deploy-mode client "
            "jobs/spark/02_weather_port_enrichment.py"
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

    spark_weather_operational_fact = BashOperator(
        task_id="spark_weather_operational_fact",
        bash_command=(
            f"export PYSPARK_PYTHON={pyspark_python} && "
            "spark-submit "
            "--master local[*] "
            "--deploy-mode client "
            "jobs/spark/03_weather_operational_fact.py"
        ),
    )

    cassandra_latest_vehicle_state = BashOperator(
        task_id="cassandra_latest_vehicle_state",
        bash_command="bash scripts/65_load_vehicle_latest_state_cassandra.sh",
    )

    cleanup_hdfs_tmp = BashOperator(
        task_id="cleanup_hdfs_tmp",
        bash_command=(
            "hdfs dfs -mkdir -p /hadoop/logistica/checkpoint /hadoop/logistica/tmp "
            "&& hdfs dfs -rm -r -f /hadoop/logistica/tmp/* || true"
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
            f"export PYSPARK_PYTHON={pyspark_python} && "
            "spark-submit --master local[*] --deploy-mode client jobs/spark/01_load_master_dimensions.py"
        ),
    )

    monthly_retrain_graph = BashOperator(
        task_id="monthly_retrain_graph",
        bash_command=(
            f"export PYSPARK_PYTHON={pyspark_python} && "
            "spark-submit --master local[*] --deploy-mode client "
            "--packages graphframes:graphframes:0.8.3-spark3.5-s_2.12 jobs/spark/02_graph_metrics.py"
        ),
    )

    monthly_refresh_weather_fact = BashOperator(
        task_id="monthly_refresh_weather_fact",
        bash_command=(
            f"export PYSPARK_PYTHON={pyspark_python} && "
            "spark-submit --master local[*] --deploy-mode client jobs/spark/03_weather_operational_fact.py"
        ),
    )

    monthly_cleanup_hdfs_tmp = BashOperator(
        task_id="monthly_cleanup_hdfs_tmp",
        bash_command=(
            "hdfs dfs -mkdir -p /hadoop/logistica/checkpoint /hadoop/logistica/tmp "
            "&& hdfs dfs -rm -r -f /hadoop/logistica/tmp/* "
            "&& hdfs dfs -rm -r -f /hadoop/logistica/checkpoint/ships/* || true "
            "&& hdfs dfs -rm -r -f /hadoop/logistica/checkpoint/alerts/* || true"
        ),
    )

    monthly_build_dimensions >> monthly_retrain_graph >> monthly_refresh_weather_fact >> monthly_cleanup_hdfs_tmp
