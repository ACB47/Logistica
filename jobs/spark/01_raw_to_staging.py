from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import abs as sql_abs
from pyspark.sql.functions import col, from_json, hash as spark_hash, lit, pmod
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


SHIP_SCHEMA = StructType(
    [
        StructField("event_type", StringType(), True),
        StructField("ts", StringType(), True),
        StructField("ship_id", StringType(), True),
        StructField("route_id", StringType(), True),
        StructField("origin_port", StringType(), True),
        StructField("dest_port", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("speed_kn", DoubleType(), True),
        StructField("heading", DoubleType(), True),
    ]
)

ALERT_SCHEMA = StructType(
    [
        StructField("event_type", StringType(), True),
        StructField("ts", StringType(), True),
        StructField("source", StringType(), True),
        StructField("severity", IntegerType(), True),
        StructField("region", StringType(), True),
        StructField("text", StringType(), True),
        StructField("confidence", DoubleType(), True),
    ]
)


def read_jsonl_text(spark: SparkSession, path_glob: str, schema: StructType):
    """
    Lee JSONL como texto (una línea = un JSON) y aplica un esquema explícito.
    """
    text_df = spark.read.text(path_glob)
    parsed = text_df.select(from_json(col("value"), schema).alias("data")).select("data.*")
    return parsed


def main() -> None:
    spark = (
        SparkSession.builder.appName("logistica-01-raw-to-staging")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
        .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse")
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sql("CREATE DATABASE IF NOT EXISTS logistica")

    # RAW paths (particionados por fecha/hora)
    # Estructura real generada por el sink:
    #   /ships/YYYY/MM/DD/HHMM/*.jsonl
    ships_glob = "hdfs://namenode:8020/hadoop/logistica/raw/ships/*/*/*/*/*.jsonl"
    clima_glob = "hdfs://namenode:8020/hadoop/logistica/raw/clima/*/*/*/*/*.jsonl"
    noticias_glob = "hdfs://namenode:8020/hadoop/logistica/raw/noticias/*/*/*/*/*.jsonl"

    staging_base = "hdfs://namenode:8020/hadoop/logistica/staging"
    ships_path = f"{staging_base}/stg_ships"
    clima_path = f"{staging_base}/stg_alerts_clima"
    noticias_path = f"{staging_base}/stg_alerts_noticias"

    ships = read_jsonl_text(spark, ships_glob, SHIP_SCHEMA)
    clima = read_jsonl_text(spark, clima_glob, ALERT_SCHEMA)
    noticias = read_jsonl_text(spark, noticias_glob, ALERT_SCHEMA)

    # Limpieza mínima
    ships_clean = (
        ships.dropna(subset=["ship_id", "ts", "lat", "lon"])
        .dropDuplicates(["ship_id", "ts"])
    )
    clima_clean = (
        clima.dropna(subset=["ts", "region", "text"])
        .dropDuplicates(["ts", "region", "text"])
    )
    noticias_clean = (
        noticias.dropna(subset=["ts", "region", "text"])
        .dropDuplicates(["ts", "region", "text"])
    )

    # ---------------------------------------------------------
    # Stock del almacén (simulación reproducible para proyecto)
    # ---------------------------------------------------------
    # En ausencia de una API real de stock, generamos un snapshot
    # determinista por `ship_id` para demostrar reglas de decisión.
    ships_clean = (
        ships_clean.withColumn("warehouse", lit("Valladolid"))
        .withColumn("sku", lit("SKU-SEA-001"))
        .withColumn("reorder_point", lit(30))
        .withColumn(
            "stock_on_hand",
            pmod(sql_abs(spark_hash(col("ship_id"))), lit(100)).cast(IntegerType()),
        )
    )

    # Recrear tablas staging por ser un ejemplo reproducible
    spark.sql("DROP TABLE IF EXISTS logistica.stg_ships")
    spark.sql("DROP TABLE IF EXISTS logistica.stg_alerts_clima")
    spark.sql("DROP TABLE IF EXISTS logistica.stg_alerts_noticias")

    (
        ships_clean.write.mode("overwrite")
        .format("parquet")
        .option("path", ships_path)
        .saveAsTable("logistica.stg_ships")
    )
    (
        clima_clean.write.mode("overwrite")
        .format("parquet")
        .option("path", clima_path)
        .saveAsTable("logistica.stg_alerts_clima")
    )
    (
        noticias_clean.write.mode("overwrite")
        .format("parquet")
        .option("path", noticias_path)
        .saveAsTable("logistica.stg_alerts_noticias")
    )

    print("OK - tablas staging creadas:")
    spark.sql("SHOW TABLES IN logistica").show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
