from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, round as sql_round, when


PORT_ROUTE_REFERENCE = [
    ("Algeciras", "Shanghai", "route-shanghai-algeciras", "Valladolid", 120, 80),
    ("Valencia", "Shanghai", "route-shanghai-valencia", "Valladolid", 140, 90),
    ("Barcelona", "Shanghai", "route-shanghai-barcelona", "Valladolid", 135, 85),
]


def main() -> None:
    spark = (
        SparkSession.builder.appName("logistica-02-weather-port-enrichment")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
        .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse")
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sql("CREATE DATABASE IF NOT EXISTS logistica")

    weather = spark.table("logistica.stg_weather_open_meteo")
    route_context = spark.createDataFrame(
        PORT_ROUTE_REFERENCE,
        ["port_ref", "origin_port", "route_id", "warehouse", "stock_on_hand", "reorder_point"],
    )

    enriched = (
        weather.alias("w")
        .join(route_context.alias("r"), on="port_ref", how="left")
        .withColumn(
            "weather_risk_level",
            when(col("severity") >= lit(4), lit("ALTO"))
            .when(col("severity") >= lit(2), lit("MEDIO"))
            .otherwise(lit("BAJO")),
        )
        .withColumn(
            "port_operational_status",
            when(col("stock_on_hand").isNull(), lit("SIN_CONTEXTO"))
            .when(col("stock_on_hand") <= col("reorder_point"), lit("STOCK_CRITICO"))
            .when(col("severity") >= lit(3), lit("METEO_VIGILANCIA"))
            .otherwise(lit("OPERATIVO")),
        )
        .withColumn(
            "weather_delay_hours_estimate",
            when(col("severity") >= lit(4), lit(10.0))
            .when(col("severity") >= lit(3), lit(6.0))
            .when(col("severity") >= lit(2), lit(3.0))
            .otherwise(lit(1.0)),
        )
        .withColumn("weather_delay_hours_estimate", sql_round(col("weather_delay_hours_estimate"), 1))
        .withColumn("enriched_at", current_timestamp())
    )

    spark.sql("DROP TABLE IF EXISTS logistica.dim_ports_routes_weather")
    (
        enriched.write.mode("overwrite")
        .format("parquet")
        .option("path", "hdfs://namenode:8020/hadoop/logistica/curated/dim_ports_routes_weather")
        .saveAsTable("logistica.dim_ports_routes_weather")
    )

    print("OK - tabla enriquecida creada: logistica.dim_ports_routes_weather")
    spark.sql(
        "SELECT port_ref, origin_port, route_id, severity, weather_risk_level, port_operational_status, weather_delay_hours_estimate "
        "FROM logistica.dim_ports_routes_weather ORDER BY event_ts DESC LIMIT 10"
    ).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
