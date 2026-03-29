from __future__ import annotations

import json
from datetime import datetime

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number


def rows_to_dicts(dataframe, limit: int | None = None) -> list[dict]:
    if limit is not None:
        dataframe = dataframe.limit(limit)

    rows = []
    for row in dataframe.collect():
        record = {}
        for key, value in row.asDict().items():
            if isinstance(value, datetime):
                record[key] = value.isoformat()
            else:
                record[key] = value
        rows.append(record)
    return rows


def main() -> None:
    spark = (
        SparkSession.builder.appName("logistica-dashboard-bundle")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
        .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse")
        .enableHiveSupport()
        .getOrCreate()
    )

    errors: list[str] = []
    payload: dict[str, object] = {
        "errors": errors,
        "ships_latest": [],
        "fact_alerts": [],
        "fact_weather_operational": [],
        "graph_centrality": [],
        "dim_ports": [],
        "dim_routes": [],
    }

    try:
        dim_ports = spark.table("logistica.dim_ports")
        payload["dim_ports"] = rows_to_dicts(dim_ports.orderBy("port_name"))
    except Exception as exc:  # pragma: no cover - runtime defensive branch
        errors.append(f"dim_ports: {exc}")

    try:
        dim_routes = spark.table("logistica.dim_routes")
        payload["dim_routes"] = rows_to_dicts(dim_routes.orderBy("route_id"))
    except Exception as exc:  # pragma: no cover
        errors.append(f"dim_routes: {exc}")

    try:
        ships = spark.table("logistica.stg_ships")
        latest_window = Window.partitionBy("ship_id").orderBy(col("ts").desc())
        ships_latest = (
            ships.withColumn("row_num", row_number().over(latest_window))
            .filter(col("row_num") == 1)
            .drop("row_num")
            .orderBy("ship_id")
        )
        payload["ships_latest"] = rows_to_dicts(ships_latest)
    except Exception as exc:  # pragma: no cover
        errors.append(f"ships_latest: {exc}")

    try:
        fact_alerts = spark.table("logistica.fact_alerts").orderBy(col("severity").desc(), col("via_port"))
        payload["fact_alerts"] = rows_to_dicts(fact_alerts, limit=20)
    except Exception as exc:  # pragma: no cover
        errors.append(f"fact_alerts: {exc}")

    try:
        weather = spark.table("logistica.fact_weather_operational").orderBy(col("event_ts").desc())
        payload["fact_weather_operational"] = rows_to_dicts(weather, limit=20)
    except Exception as exc:  # pragma: no cover
        errors.append(f"fact_weather_operational: {exc}")

    try:
        centrality = spark.table("logistica.fact_graph_centrality").orderBy(col("degree").desc(), col("node_id"))
        payload["graph_centrality"] = rows_to_dicts(centrality, limit=20)
    except Exception as exc:  # pragma: no cover
        errors.append(f"fact_graph_centrality: {exc}")

    print(json.dumps(payload, ensure_ascii=True))
    spark.stop()


if __name__ == "__main__":
    main()
