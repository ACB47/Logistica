from __future__ import annotations

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number


def main() -> None:
    spark = (
        SparkSession.builder.appName("logistica-04-export-latest-vehicle-state")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
        .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse")
        .enableHiveSupport()
        .getOrCreate()
    )

    window = Window.partitionBy("ship_id").orderBy(col("ts").desc())
    rows = (
        spark.table("logistica.stg_ships")
        .withColumn("row_num", row_number().over(window))
        .filter(col("row_num") == 1)
        .select(
            "ship_id",
            col("ts").cast("string").alias("event_ts"),
            "route_id",
            "origin_port",
            "dest_port",
            "warehouse",
            col("lat").cast("string").alias("lat"),
            col("lon").cast("string").alias("lon"),
            col("speed_kn").cast("string").alias("speed_kn"),
            col("heading").cast("string").alias("heading"),
            col("stock_on_hand").cast("string").alias("stock_on_hand"),
            col("reorder_point").cast("string").alias("reorder_point"),
        )
        .orderBy("ship_id")
        .collect()
    )

    for row in rows:
        print("\t".join("" if value is None else str(value) for value in row))

    spark.stop()


if __name__ == "__main__":
    main()
