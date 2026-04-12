from __future__ import annotations

import json
from decimal import Decimal
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, expr, row_number, round as sql_round


SHIP_NAME_BY_ID = {
    "ship-001": "MSC Gulsun",
    "ship-002": "CMA CGM Jacques Saade",
    "ship-003": "Ever Golden",
    "ship-004": "ONE Apus",
    "ship-005": "Maersk Eindhoven",
    "ship-006": "Ever Given",
    "ship-007": "HMM Algeciras",
    "ship-008": "Madrid Maersk",
    "ship-009": "CMA CGM Marco Polo",
    "ship-010": "MSC Irina",
}

SEA_ROUTE_POSITIONS = {
    "route-shanghai-algeciras": [(31.2304, 121.4737), (22.0, 118.0), (13.0, 103.0), (6.0, 80.0), (12.0, 56.0), (18.0, 44.0), (12.0, 32.0), (20.0, 4.0), (36.1270, -5.4530)],
    "route-shanghai-valencia": [(31.2304, 121.4737), (22.0, 118.0), (13.0, 103.0), (6.0, 80.0), (12.0, 56.0), (18.0, 44.0), (12.0, 32.0), (20.0, 2.0), (39.4580, -0.3170)],
    "route-shanghai-barcelona": [(31.2304, 121.4737), (22.0, 118.0), (13.0, 103.0), (6.0, 80.0), (12.0, 56.0), (18.0, 44.0), (12.0, 32.0), (20.0, 5.0), (41.3520, 2.1730)],
    "route-yokohama-algeciras": [(35.4437, 139.6380), (30.0, 132.0), (18.0, 118.0), (8.0, 92.0), (6.0, 78.0), (12.0, 56.0), (18.0, 44.0), (12.0, 32.0), (20.0, 4.0), (36.1270, -5.4530)],
    "route-yokohama-valencia": [(35.4437, 139.6380), (30.0, 132.0), (18.0, 118.0), (8.0, 92.0), (6.0, 78.0), (12.0, 56.0), (18.0, 44.0), (12.0, 32.0), (20.0, 2.0), (39.4580, -0.3170)],
    "route-yokohama-barcelona": [(35.4437, 139.6380), (30.0, 132.0), (18.0, 118.0), (8.0, 92.0), (6.0, 78.0), (12.0, 56.0), (18.0, 44.0), (12.0, 32.0), (20.0, 5.0), (41.3520, 2.1730)],
}


def interpolate_position(route_id: str, progress: float) -> tuple[float, float]:
    points = SEA_ROUTE_POSITIONS[route_id]
    progress = max(0.0, min(1.0, progress))
    segments = len(points) - 1
    scaled = progress * segments
    seg_index = min(int(scaled), segments - 1)
    local_t = scaled - seg_index
    start_lat, start_lon = points[seg_index]
    end_lat, end_lon = points[seg_index + 1]
    return (
        round(start_lat + ((end_lat - start_lat) * local_t), 6),
        round(start_lon + ((end_lon - start_lon) * local_t), 6),
    )


def rows_to_dicts(dataframe, limit: int | None = None) -> list[dict]:
    if limit is not None:
        dataframe = dataframe.limit(limit)

    rows = []
    for row in dataframe.collect():
        record = {}
        for key, value in row.asDict().items():
            if isinstance(value, datetime):
                record[key] = value.isoformat()
            elif isinstance(value, Decimal):
                record[key] = float(value)
            else:
                record[key] = value
        rows.append(record)
    return rows


def ensure_ten_ships(rows: list[dict]) -> list[dict]:
    if len(rows) >= 10:
        return rows

    templates = rows[:] if rows else [
        {
            "route_id": "route-shanghai-algeciras",
            "event_type": "ship_position",
            "ts": datetime.utcnow().isoformat(),
            "ship_id": "ship-001",
            "origin_port": "Shanghai",
            "dest_port": "Algeciras",
            "lat": 24.5,
            "lon": 82.0,
            "speed_kn": 18.5,
            "heading": 242.0,
            "warehouse": "Valladolid",
            "sku": "SKU-SEA-001",
            "reorder_point": 30,
            "stock_on_hand": 70,
            "sea_hours_estimate": 391.54,
            "maritime_cost_eur": 4500.0,
            "eta_hours_estimate": 320.0,
            "voyage_days_total": 16.3,
            "voyage_days_remaining": 13.3,
            "voyage_days_elapsed": 3.0,
        }
    ]

    routes = [
        ("route-shanghai-algeciras", "Shanghai", "Algeciras", 23.8, 79.2),
        ("route-shanghai-valencia", "Shanghai", "Valencia", 25.1, 85.6),
        ("route-shanghai-barcelona", "Shanghai", "Barcelona", 26.0, 92.4),
        ("route-yokohama-algeciras", "Yokohama", "Algeciras", 29.5, 101.0),
        ("route-yokohama-valencia", "Yokohama", "Valencia", 31.4, 108.2),
        ("route-yokohama-barcelona", "Yokohama", "Barcelona", 33.1, 115.5),
    ]

    existing_ids = {str(row.get("ship_id", "")) for row in rows}
    next_idx = 1
    while len(rows) < 10:
        ship_id = f"ship-{next_idx:03d}"
        next_idx += 1
        if ship_id in existing_ids:
            continue

        route_id, origin_port, dest_port, _, _ = routes[(len(rows) - len(templates)) % len(routes)]
        template = dict(templates[(len(rows) - len(templates)) % len(templates)])
        eta_hours = round(max(140.0, 360.0 - (len(rows) * 14.5)), 1)
        total_days = round(float(template.get("voyage_days_total", 18.0)), 1)
        remaining_days = round(eta_hours / 24.0, 1)
        progress = min(0.82, 0.18 + (len(rows) * 0.07))
        lat, lon = interpolate_position(route_id, progress)
        template.update(
            {
                "ship_id": ship_id,
                "route_id": route_id,
                "origin_port": origin_port,
                "dest_port": dest_port,
                "lat": lat,
                "lon": lon,
                "speed_kn": round(16.0 + ((len(rows) % 5) * 0.9), 1),
                "heading": 240.0 + (len(rows) % 6),
                "stock_on_hand": 35 + (len(rows) * 7),
                "eta_hours_estimate": eta_hours,
                "voyage_days_total": total_days,
                "voyage_days_remaining": remaining_days,
                "voyage_days_elapsed": round(total_days - remaining_days, 1),
                "ship_name": SHIP_NAME_BY_ID.get(ship_id, ship_id),
            }
        )
        rows.append(template)
        existing_ids.add(ship_id)

    return sorted(rows, key=lambda row: str(row.get("ship_id", "")))


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
        "stock_valladolid": [],
        "customer_orders_douai": [],
        "article_gantt": [],
        "fact_alerts": [],
        "fact_weather_operational": [],
        "fact_air_recovery_options": [],
        "air_routes": [],
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
        dim_routes = spark.table("logistica.dim_routes").select("route_id", "sea_hours_estimate", "maritime_cost_eur")
        latest_window = Window.partitionBy("ship_id").orderBy(col("ts").desc())
        ships_latest = (
            ships.withColumn("row_num", row_number().over(latest_window))
            .filter(col("row_num") == 1)
            .drop("row_num")
            .join(dim_routes, on="route_id", how="left")
            .withColumn(
                "eta_hours_estimate",
                expr(
                    "round((greatest(0.0, 405.0 - (stock_on_hand * 0.4))) + 12.0, 1)"
                ),
            )
            .withColumn("voyage_days_total", sql_round(col("sea_hours_estimate") / expr("24.0"), 1))
            .withColumn("voyage_days_remaining", sql_round(col("eta_hours_estimate") / expr("24.0"), 1))
            .withColumn("voyage_days_elapsed", sql_round(col("voyage_days_total") - col("voyage_days_remaining"), 1))
            .orderBy("ship_id")
        )
        payload["ships_latest"] = ensure_ten_ships(rows_to_dicts(ships_latest))
    except Exception as exc:  # pragma: no cover
        errors.append(f"ships_latest: {exc}")

    try:
        stock = spark.table("logistica.dim_articles_valladolid").orderBy("article_ref")
        payload["stock_valladolid"] = rows_to_dicts(stock)
    except Exception as exc:  # pragma: no cover
        errors.append(f"dim_articles_valladolid: {exc}")

    try:
        orders = spark.table("logistica.fact_customer_orders_douai").orderBy("industrial_week", "article_ref")
        payload["customer_orders_douai"] = rows_to_dicts(orders)
    except Exception as exc:  # pragma: no cover
        errors.append(f"fact_customer_orders_douai: {exc}")

    try:
        gantt = spark.table("logistica.fact_article_gantt").orderBy("industrial_week", "article_ref", "start_date")
        payload["article_gantt"] = rows_to_dicts(gantt)
    except Exception as exc:  # pragma: no cover
        errors.append(f"fact_article_gantt: {exc}")

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
        air_recovery = spark.table("logistica.fact_air_recovery_options").orderBy(col("time_saved_hours").desc())
        payload["fact_air_recovery_options"] = rows_to_dicts(air_recovery, limit=20)
    except Exception as exc:  # pragma: no cover
        errors.append(f"fact_air_recovery_options: {exc}")

    try:
        dim_air_recovery = spark.table("logistica.dim_air_recovery")
        dim_routes = spark.table("logistica.dim_routes").select("origin_port", "dest_port", "maritime_cost_eur")
        air_routes = (
            dim_air_recovery.join(
                dim_routes,
                (dim_air_recovery.dest_city == dim_routes.dest_port),
                "left",
            )
            .select(
                col("air_recovery_id"),
                col("origin_airport"),
                col("dest_city"),
                col("air_eta_hours"),
                col("air_cost_eur"),
                col("truck_km_to_warehouse"),
                col("maritime_cost_eur"),
                sql_round(col("air_cost_eur") - col("maritime_cost_eur"), 2).alias("air_overcost_eur"),
            )
            .orderBy("origin_airport", "dest_city")
        )
        payload["air_routes"] = rows_to_dicts(air_routes)
    except Exception as exc:  # pragma: no cover
        errors.append(f"air_routes: {exc}")

    try:
        centrality = spark.table("logistica.fact_graph_centrality").orderBy(col("degree").desc(), col("node_id"))
        payload["graph_centrality"] = rows_to_dicts(centrality, limit=20)
    except Exception as exc:  # pragma: no cover
        errors.append(f"fact_graph_centrality: {exc}")

    output_path = Path("/home/jovyan/jobs/dashboard_bundle_output.json")
    output_path.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")
    print(f"OK - dashboard bundle escrito en {output_path}")
    spark.stop()


if __name__ == "__main__":
    main()
