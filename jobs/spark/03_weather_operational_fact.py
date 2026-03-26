from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, when


def main() -> None:
    spark = (
        SparkSession.builder.appName("logistica-03-weather-operational-fact")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
        .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse")
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sql("CREATE DATABASE IF NOT EXISTS logistica")

    weather_routes = spark.table("logistica.dim_ports_routes_weather")

    operational = (
        weather_routes.withColumn(
            "recommended_action",
            when(
                (col("weather_risk_level") == lit("MEDIO"))
                & (col("port_operational_status") == lit("METEO_VIGILANCIA")),
                lit("reprogramar ventana portuaria y avisar a operacion terrestre"),
            )
            .when(
                col("port_operational_status") == lit("STOCK_CRITICO"),
                lit("priorizar stock alternativo y escalado logistico"),
            )
            .otherwise(lit("seguimiento normal de la ruta")),
        )
        .withColumn(
            "operational_severity",
            when(col("weather_risk_level") == lit("ALTO"), lit(5))
            .when(col("port_operational_status") == lit("METEO_VIGILANCIA"), lit(4))
            .when(col("weather_risk_level") == lit("MEDIO"), lit(3))
            .otherwise(lit(2)),
        )
        .withColumn(
            "kdd_phase",
            lit("interpretacion_y_accion"),
        )
        .withColumn("operational_ts", current_timestamp())
        .select(
            "operational_ts",
            "kdd_phase",
            "port_id",
            "port_ref",
            "origin_port",
            "route_id",
            "warehouse_id",
            "warehouse",
            "event_ts",
            "temperature_c",
            "humidity_pct",
            "wind_speed_kmh",
            "severity",
            "weather_risk_level",
            "port_operational_status",
            "weather_delay_hours_estimate",
            "recommended_action",
            "operational_severity",
        )
    )

    spark.sql("DROP TABLE IF EXISTS logistica.fact_weather_operational")
    (
        operational.write.mode("overwrite")
        .format("parquet")
        .option("path", "hdfs://namenode:8020/hadoop/logistica/curated/fact_weather_operational")
        .saveAsTable("logistica.fact_weather_operational")
    )

    print("OK - tabla operativa creada: logistica.fact_weather_operational")
    spark.sql(
        "SELECT port_ref, route_id, weather_risk_level, port_operational_status, weather_delay_hours_estimate, recommended_action "
        "FROM logistica.fact_weather_operational ORDER BY event_ts DESC LIMIT 10"
    ).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
