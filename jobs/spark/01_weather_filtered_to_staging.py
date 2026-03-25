from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, lit, to_timestamp, when
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType


WEATHER_SCHEMA = StructType(
    [
        StructField("ts", StringType(), True),
        StructField("temperature_c", DoubleType(), True),
        StructField("humidity_pct", IntegerType(), True),
        StructField("wind_speed_kmh", DoubleType(), True),
        StructField("wind_direction_deg", IntegerType(), True),
        StructField("weather_code", IntegerType(), True),
        StructField("port_ref", StringType(), True),
        StructField("source", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("region", StringType(), True),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Carga datos_filtrados de Kafka a Hive")
    parser.add_argument("--bootstrap", default="kafka:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="datos_filtrados", help="Topic Kafka a leer")
    parser.add_argument("--starting-offsets", default="earliest", help="Offsets iniciales Kafka")
    parser.add_argument("--ending-offsets", default="latest", help="Offsets finales Kafka")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    spark = (
        SparkSession.builder.appName("logistica-01-weather-filtered-to-staging")
        .config("spark.sql.warehouse.dir", "hdfs:///user/hive/warehouse")
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sql("CREATE DATABASE IF NOT EXISTS logistica")

    weather_kafka = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("endingOffsets", args.ending_offsets)
        .load()
    )

    weather = (
        weather_kafka.select(from_json(col("value").cast("string"), WEATHER_SCHEMA).alias("data"))
        .select("data.*")
        .dropna(subset=["ts", "source", "port_ref", "event_type"])
        .dropDuplicates(["ts", "source", "port_ref", "event_type"])
        .withColumn("event_ts", to_timestamp(col("ts")))
        .withColumn(
            "severity",
            when(col("wind_speed_kmh") >= lit(50.0), lit(4))
            .when(col("wind_speed_kmh") >= lit(35.0), lit(3))
            .when(col("wind_speed_kmh") >= lit(20.0), lit(2))
            .otherwise(lit(1)),
        )
        .withColumn("ingested_at", current_timestamp())
    )

    spark.sql("DROP TABLE IF EXISTS logistica.stg_weather_open_meteo")
    (
        weather.write.mode("overwrite")
        .format("parquet")
        .option("path", "hdfs:///hadoop/logistica/staging/stg_weather_open_meteo")
        .saveAsTable("logistica.stg_weather_open_meteo")
    )

    print("OK - tabla staging creada: logistica.stg_weather_open_meteo")
    spark.sql("SELECT * FROM logistica.stg_weather_open_meteo ORDER BY event_ts DESC LIMIT 10").show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
