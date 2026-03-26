from __future__ import annotations

from pyspark.sql import SparkSession


PORTS = [
    ("PORT-ALG", "Algeciras", "ES", "Estrecho de Gibraltar", 36.1270, -5.4530, "port"),
    ("PORT-VLC", "Valencia", "ES", "Costa Mediterranea", 39.4580, -0.3170, "port"),
    ("PORT-BCN", "Barcelona", "ES", "Costa Mediterranea", 41.3520, 2.1730, "port"),
    ("PORT-SHA", "Shanghai", "CN", "Asia Pacifico", 31.2304, 121.4737, "origin_hub"),
]

ROUTES = [
    ("route-shanghai-algeciras", "Shanghai", "Algeciras", "maritime", 391.54, 8.0),
    ("route-shanghai-valencia", "Shanghai", "Valencia", "maritime", 403.15, 8.0),
    ("route-shanghai-barcelona", "Shanghai", "Barcelona", "maritime", 413.60, 8.0),
]

WAREHOUSES = [
    ("WH-VLL", "Valladolid", "ES", "Castilla y Leon", "cross_dock", 41.6520, -4.7240),
]

SKUS = [
    ("SKU-SEA-001", "Carga maritima general", "standard", "contenedor 40ft", "active"),
]


def main() -> None:
    spark = (
        SparkSession.builder.appName("logistica-01-load-master-dimensions")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
        .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse")
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sql("CREATE DATABASE IF NOT EXISTS logistica")

    dim_ports = spark.createDataFrame(
        PORTS,
        ["port_id", "port_name", "country_code", "macro_region", "lat", "lon", "port_type"],
    )
    dim_routes = spark.createDataFrame(
        ROUTES,
        ["route_id", "origin_port", "dest_port", "route_mode", "sea_hours_estimate", "inland_hours_estimate"],
    )
    dim_warehouse = spark.createDataFrame(
        WAREHOUSES,
        ["warehouse_id", "warehouse_name", "country_code", "region_name", "warehouse_type", "lat", "lon"],
    )
    dim_skus = spark.createDataFrame(
        SKUS,
        ["sku_id", "sku_name", "sku_family", "packaging", "status"],
    )

    targets = [
        (dim_ports, "logistica.dim_ports", "hdfs://namenode:8020/hadoop/logistica/master/dim_ports"),
        (dim_routes, "logistica.dim_routes", "hdfs://namenode:8020/hadoop/logistica/master/dim_routes"),
        (dim_warehouse, "logistica.dim_warehouse", "hdfs://namenode:8020/hadoop/logistica/master/dim_warehouse"),
        (dim_skus, "logistica.dim_skus", "hdfs://namenode:8020/hadoop/logistica/master/dim_skus"),
    ]

    for _, table_name, _ in targets:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    for dataframe, table_name, path in targets:
        dataframe.write.mode("overwrite").format("parquet").option("path", path).saveAsTable(table_name)

    print("OK - dimensiones maestras creadas")
    spark.sql("SHOW TABLES IN logistica").show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
