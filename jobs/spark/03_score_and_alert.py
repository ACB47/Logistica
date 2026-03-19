from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, min as sql_min, when
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


def main() -> None:
    spark = (
        SparkSession.builder.appName("logistica-03-score-and-alert")
        .config("spark.sql.warehouse.dir", "hdfs:///hadoop/logistica/warehouse")
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sql("CREATE DATABASE IF NOT EXISTS logistica")

    # Inputs
    fact_route_risk = spark.table("logistica.fact_route_risk")
    ships = spark.table("logistica.stg_ships")

    # Stock snapshot por puerto destino (aquí lo tratamos como "stock del almacén asociado
    # al flujo de ese puerto", para poder cruzarlo con `via_port`).
    stock_by_dest = (
        ships.groupBy(col("dest_port").alias("via_port"))
        .agg(
            sql_min(col("stock_on_hand")).alias("stock_on_hand_min"),
            lit(1).cast(IntegerType()).alias("sample_weight"),
            # reorder_point es constante en el staging, pero lo agregamos por consistencia.
            sql_min(col("reorder_point")).alias("reorder_point"),
        )
    )

    # Cruzamos riesgo (de la analítica de grafo) con stock crítico.
    enriched = (
        fact_route_risk.join(stock_by_dest, on="via_port", how="left")
        .withColumn(
            "stock_status",
            when(
                col("stock_on_hand_min").isNull() | (col("reorder_point").isNull()),
                lit("SIN_DATOS"),
            )
            .when(col("stock_on_hand_min") <= col("reorder_point"), lit("CRITICO"))
            .otherwise(lit("OK")),
        )
    )

    # Reglas de decisión (documentables en el informe):
    # - "vuelo urgente" si el grafo estima riesgo ALTO y el stock está CRITICO.
    # - Si hay stock CRITICO pero riesgo MEDIO/BAJO -> menor severidad (seguimiento y plan B).
    operational = (
        enriched.withColumn(
            "recommendation",
            when((col("risk_level") == lit("ALTO")) & (col("stock_status") == lit("CRITICO")),
                 lit("activar vuelo urgente: priorizar redistribucion/transferencia")),
        )
        .withColumn(
            "recommendation",
            when(
                col("recommendation").isNotNull(),
                col("recommendation"),
            ).when(
                col("stock_status") == lit("CRITICO"),
                lit("seguir plan de contingencia: replanificar rutas y priorizar stock alternativo"),
            ).otherwise(lit("seguimiento normal: sin accion urgente")),
        )
        .withColumn(
            "severity",
            when(
                (col("risk_level") == lit("ALTO")) & (col("stock_status") == lit("CRITICO")),
                lit(5).cast(IntegerType()),
            )
            .when((col("stock_status") == lit("CRITICO")) & (col("risk_level") == lit("MEDIO")), lit(4))
            .when((col("stock_status") == lit("CRITICO")) & (col("risk_level") == lit("BAJO")), lit(3))
            .when(col("risk_level") == lit("ALTO"), lit(3))
            .when(col("risk_level") == lit("MEDIO"), lit(2))
            .otherwise(lit(1))
            .cast(IntegerType()),
        )
        .withColumn("alert_ts", current_timestamp())
        .withColumn("source", lit("operativa_graph_stock").cast(StringType()))
    )

    # Normalizamos selección final para una tabla "de hechos" de alertas.
    alerts_final = operational.select(
        "alert_ts",
        "source",
        "via_port",
        "risk_score",
        "risk_level",
        "estimated_total_hours",
        "stock_on_hand_min",
        "reorder_point",
        "stock_status",
        "recommendation",
        "severity",
    )

    # Persistencia Hive + evidencia en HDFS.
    spark.sql("DROP TABLE IF EXISTS logistica.fact_alerts")
    (
        alerts_final.write.mode("overwrite")
        .format("parquet")
        .option("path", "hdfs:///hadoop/logistica/curated/fact_alerts")
        .saveAsTable("logistica.fact_alerts")
    )

    print("OK - Alertas operativas creadas: logistica.fact_alerts")
    spark.sql("SHOW TABLES IN logistica").show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()

