from __future__ import annotations

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    coalesce,
    current_timestamp,
    lit,
    round as sql_round,
    row_number,
    sqrt,
    when,
)


EARTH_RADIUS_KM = 6371.0
TRUCK_SPEED_KMH = 68.0
TRUCK_COST_PER_KM_EUR = 1.45
PORT_HANDLING_HOURS = 6.0
AIR_HANDLING_HOURS = 4.0
HOURLY_DEMAND_UNITS = 1.6


def main() -> None:
    spark = (
        SparkSession.builder.appName("logistica-03-air-recovery-options")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
        .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse")
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sql("CREATE DATABASE IF NOT EXISTS logistica")

    ships = spark.table("logistica.stg_ships")
    weather_operational = spark.table("logistica.fact_weather_operational")
    fact_alerts = spark.table("logistica.fact_alerts")
    dim_routes = spark.table("logistica.dim_routes")
    dim_ports = spark.table("logistica.dim_ports")
    dim_air_recovery = spark.table("logistica.dim_air_recovery")
    dim_warehouse = spark.table("logistica.dim_warehouse")

    latest_window = Window.partitionBy("ship_id").orderBy(col("ts").desc())
    latest_ships = (
        ships.withColumn("row_num", row_number().over(latest_window))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    origin_ports = dim_ports.select(
        col("port_name").alias("origin_port"),
        col("lat").alias("origin_lat"),
        col("lon").alias("origin_lon"),
    )
    dest_ports = dim_ports.select(
        col("port_name").alias("dest_port"),
        col("lat").alias("dest_lat"),
        col("lon").alias("dest_lon"),
    )

    ship_context = (
        latest_ships.alias("s")
        .join(dim_routes.alias("r"), on="route_id", how="left")
        .join(origin_ports.alias("o"), on="origin_port", how="left")
        .join(dest_ports.alias("d"), on="dest_port", how="left")
        .join(dim_warehouse.alias("w"), col("s.warehouse") == col("w.warehouse_name"), "left")
        .join(
            weather_operational.alias("wo"),
            (col("s.route_id") == col("wo.route_id")) & (col("s.dest_port") == col("wo.port_ref")),
            "left",
        )
        .join(
            fact_alerts.select(
                col("via_port"),
                col("risk_level").alias("graph_risk_level"),
                col("severity").alias("alert_severity"),
                col("recommendation").alias("graph_recommendation"),
            ).alias("fa"),
            col("s.dest_port") == col("fa.via_port"),
            "left",
        )
        .withColumn(
            "total_route_distance_km",
            sql_round(
                lit(EARTH_RADIUS_KM)
                * sqrt(
                    ((col("dest_lat") - col("origin_lat")) * (col("dest_lat") - col("origin_lat")))
                    + ((col("dest_lon") - col("origin_lon")) * (col("dest_lon") - col("origin_lon")))
                ),
                2,
            ),
        )
        .withColumn(
            "remaining_route_distance_km",
            sql_round(
                lit(EARTH_RADIUS_KM)
                * sqrt(
                    ((col("dest_lat") - col("s.lat")) * (col("dest_lat") - col("s.lat")))
                    + ((col("dest_lon") - col("s.lon")) * (col("dest_lon") - col("s.lon")))
                ),
                2,
            ),
        )
        .withColumn(
            "remaining_ratio",
            when(col("total_route_distance_km") <= lit(0.0), lit(1.0)).otherwise(
                col("remaining_route_distance_km") / col("total_route_distance_km")
            ),
        )
        .withColumn(
            "ship_remaining_hours",
            sql_round(
                (col("sea_hours_estimate") * col("remaining_ratio"))
                + col("inland_hours_estimate")
                + col("weather_delay_hours_estimate")
                + lit(PORT_HANDLING_HOURS),
                1,
            ),
        )
        .withColumn(
            "hours_until_stock_break",
            sql_round((col("stock_on_hand") - col("reorder_point")) / lit(HOURLY_DEMAND_UNITS), 1),
        )
    )

    air_options = (
        ship_context.crossJoin(dim_air_recovery)
        .withColumn(
            "truck_eta_hours",
            sql_round(col("truck_km_to_warehouse") / lit(TRUCK_SPEED_KMH), 1),
        )
        .withColumn(
            "truck_cost_eur",
            sql_round(col("truck_km_to_warehouse") * lit(TRUCK_COST_PER_KM_EUR), 2),
        )
        .withColumn(
            "air_total_eta_hours",
            sql_round(col("air_eta_hours") + col("truck_eta_hours") + lit(AIR_HANDLING_HOURS), 1),
        )
        .withColumn(
            "air_total_cost_eur",
            sql_round(col("air_cost_eur") + col("truck_cost_eur"), 2),
        )
        .withColumn(
            "time_saved_hours",
            sql_round(col("ship_remaining_hours") - col("air_total_eta_hours"), 1),
        )
        .withColumn(
            "stock_break_risk",
            when(col("hours_until_stock_break") <= lit(0), lit("ROTURA_INMINENTE"))
            .when(col("ship_remaining_hours") > col("hours_until_stock_break"), lit("ROTURA_PROBABLE"))
            .otherwise(lit("COBERTURA_OK")),
        )
        .withColumn(
            "ia_recommendation",
            when(
                (coalesce(col("severity"), col("alert_severity")) >= lit(4))
                | (col("weather_risk_level") == lit("ALTO"))
                | (col("stock_break_risk") != lit("COBERTURA_OK")),
                lit("evaluar_aereo_urgente"),
            ).otherwise(lit("mantener_maritimo")),
        )
        .withColumn(
            "recommended_mode",
            when(
                (col("ia_recommendation") == lit("evaluar_aereo_urgente"))
                & (col("time_saved_hours") > lit(0.0))
                & (col("air_total_eta_hours") <= col("hours_until_stock_break")),
                lit("AEREO_CAMION"),
            ).otherwise(lit("MARITIMO")),
        )
        .withColumn(
            "reason_summary",
            when(
                col("recommended_mode") == lit("AEREO_CAMION"),
                lit("El barco llega tarde para cubrir stock y la opcion aerea evita rotura con mejor ETA"),
            ).otherwise(lit("La ruta maritima sigue siendo viable con menor coste total")),
        )
    )

    best_window = Window.partitionBy("ship_id").orderBy(
        col("recommended_mode").desc(),
        col("air_total_cost_eur").asc(),
        col("air_total_eta_hours").asc(),
    )

    best_options = (
        air_options.withColumn("row_num", row_number().over(best_window))
        .filter(col("row_num") == 1)
        .drop("row_num")
        .select(
            current_timestamp().alias("decision_ts"),
            col("s.ship_id").alias("ship_id"),
            col("s.route_id").alias("route_id"),
            col("s.origin_port").alias("origin_port"),
            col("s.dest_port").alias("dest_port"),
            col("warehouse_name").alias("warehouse"),
            col("air_recovery_id").alias("air_option_id"),
            col("dest_city").alias("air_dest_city"),
            "weather_risk_level",
            "port_operational_status",
            coalesce(col("severity"), col("alert_severity")).alias("weather_severity"),
            "graph_risk_level",
            "graph_recommendation",
            col("s.stock_on_hand").alias("stock_on_hand"),
            col("s.reorder_point").alias("reorder_point"),
            "hours_until_stock_break",
            "ship_remaining_hours",
            "air_total_eta_hours",
            "time_saved_hours",
            "air_total_cost_eur",
            "truck_cost_eur",
            "stock_break_risk",
            "recommended_mode",
            "ia_recommendation",
            "reason_summary",
        )
    )

    spark.sql("DROP TABLE IF EXISTS logistica.fact_air_recovery_options")
    (
        best_options.write.mode("overwrite")
        .format("parquet")
        .option("path", "hdfs://namenode:8020/hadoop/logistica/curated/fact_air_recovery_options")
        .saveAsTable("logistica.fact_air_recovery_options")
    )

    print("OK - opciones aereo+camion creadas: logistica.fact_air_recovery_options")
    spark.sql(
        "SELECT ship_id, dest_port, recommended_mode, stock_break_risk, ship_remaining_hours, air_total_eta_hours, time_saved_hours, air_total_cost_eur "
        "FROM logistica.fact_air_recovery_options ORDER BY time_saved_hours DESC LIMIT 10"
    ).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
