from __future__ import annotations

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, current_timestamp, row_number


def main() -> None:
    spark = (
        SparkSession.builder.appName("logistica-04-latest-vehicle-state-cassandra")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
        .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse")
        .config("spark.cassandra.connection.host", "cassandra")
        .enableHiveSupport()
        .getOrCreate()
    )

    ships = spark.table("logistica.stg_ships")

    latest_window = Window.partitionBy("ship_id").orderBy(col("ts").desc())
    latest = (
        ships.withColumn("row_num", row_number().over(latest_window))
        .filter(col("row_num") == 1)
        .drop("row_num")
        .withColumnRenamed("ts", "event_ts")
        .withColumn("updated_at", current_timestamp())
        .select(
            "ship_id",
            "event_ts",
            "route_id",
            "origin_port",
            "dest_port",
            "warehouse",
            "lat",
            "lon",
            "speed_kn",
            "heading",
            "stock_on_hand",
            "reorder_point",
            "updated_at",
        )
    )

    (
        latest.write.format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(table="vehicle_latest_state", keyspace="logistica")
        .save()
    )

    print("OK - estado actual de vehiculos cargado en Cassandra: logistica.vehicle_latest_state")
    latest.orderBy(col("event_ts").desc()).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
