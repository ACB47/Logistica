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

ARTICLES_VALLADOLID = [
    ("1000001023", "Modulo ABS delantero", "componentes coche", 18, 42, 756, 420, 2, "Valladolid", "Algeciras"),
    ("1000002087", "Centralita motor ECU", "electronica coche", 6, 11, 66, 90, 4, "Valladolid", "Valencia"),
    ("1000003154", "Juego de sensores radar", "asistencia conduccion", 10, 16, 160, 120, 2, "Valladolid", "Barcelona"),
    ("1000004278", "Bomba de combustible", "powertrain", 12, 35, 480, 240, 4, "Valladolid", "Algeciras"),
    ("1000005311", "Modulo airbag lateral", "seguridad pasiva", 16, 18, 288, 130, 3, "Valladolid", "Valencia"),
    ("1000006422", "Cableado de puerta", "electrico interior", 20, 25, 500, 180, 4, "Valladolid", "Barcelona"),
    ("1000007533", "Sensor de angulo volante", "direccion", 14, 9, 150, 80, 3, "Valladolid", "Algeciras"),
    ("1000008644", "Modulo de climatizacion HVAC", "confort", 8, 7, 90, 50, 3, "Valladolid", "Valencia"),
    ("1000009755", "Motor limpia parabrisas", "electromecanico", 10, 22, 220, 110, 3, "Valladolid", "Barcelona"),
    ("1000010866", "Camara ADAS frontal", "vision inteligente", 6, 6, 60, 32, 3, "Valladolid", "Algeciras"),
]

CUSTOMER_ORDERS_DOUAI = [
    ("ORD-DOU-001", "1000001023", "Douai", "FR", "IW14", "2026-04-01", "2026-04-08", 9, 378, "sea_committed"),
    ("ORD-DOU-002", "1000002087", "Douai", "FR", "IW14", "2026-04-02", "2026-04-05", 8, 48, "risk_stock"),
    ("ORD-DOU-003", "1000003154", "Douai", "FR", "IW15", "2026-04-06", "2026-04-11", 7, 112, "sea_committed"),
    ("ORD-DOU-004", "1000004278", "Douai", "FR", "IW15", "2026-04-07", "2026-04-12", 12, 144, "sea_committed"),
    ("ORD-DOU-005", "1000005311", "Douai", "FR", "IW15", "2026-04-07", "2026-04-10", 4, 64, "sea_committed"),
    ("ORD-DOU-006", "1000006422", "Douai", "FR", "IW15", "2026-04-08", "2026-04-11", 6, 120, "sea_committed"),
    ("ORD-DOU-007", "1000007533", "Douai", "FR", "IW16", "2026-04-11", "2026-04-13", 2, 28, "sea_committed"),
    ("ORD-DOU-008", "1000008644", "Douai", "FR", "IW16", "2026-04-12", "2026-04-14", 2, 16, "sea_committed"),
    ("ORD-DOU-009", "1000009755", "Douai", "FR", "IW16", "2026-04-13", "2026-04-15", 5, 50, "sea_committed"),
    ("ORD-DOU-010", "1000010866", "Douai", "FR", "IW16", "2026-04-13", "2026-04-15", 1, 6, "risk_stock"),
]

ARTICLE_GANTT = [
    ("1000001023", "Recepcion maritima", "2026-03-31", "2026-04-07", "IW14", "sea"),
    ("1000001023", "Preparacion almacen", "2026-04-07", "2026-04-08", "IW14", "warehouse"),
    ("1000001023", "Envio a cliente Douai", "2026-04-08", "2026-04-10", "IW14", "truck"),
    ("1000002087", "Alternativa aerea Shanghai-Madrid", "2026-04-01", "2026-04-02", "IW14", "air"),
    ("1000002087", "Camion Madrid-Valladolid", "2026-04-02", "2026-04-03", "IW14", "truck"),
    ("1000002087", "Expedicion Valladolid-Douai", "2026-04-03", "2026-04-05", "IW14", "truck"),
    ("1000003154", "Recepcion maritima", "2026-04-05", "2026-04-10", "IW15", "sea"),
    ("1000003154", "Entrega cliente Douai", "2026-04-10", "2026-04-11", "IW15", "truck"),
    ("1000004278", "Recepcion maritima", "2026-04-06", "2026-04-11", "IW15", "sea"),
    ("1000004278", "Entrega cliente Douai", "2026-04-11", "2026-04-12", "IW15", "truck"),
    ("1000005311", "Recepcion maritima", "2026-04-07", "2026-04-10", "IW15", "sea"),
    ("1000005311", "Entrega cliente Douai", "2026-04-10", "2026-04-11", "IW15", "truck"),
    ("1000006422", "Recepcion maritima", "2026-04-08", "2026-04-10", "IW15", "sea"),
    ("1000006422", "Entrega cliente Douai", "2026-04-10", "2026-04-11", "IW15", "truck"),
    ("1000007533", "Recepcion maritima", "2026-04-10", "2026-04-12", "IW16", "sea"),
    ("1000007533", "Entrega cliente Douai", "2026-04-12", "2026-04-13", "IW16", "truck"),
    ("1000008644", "Recepcion maritima", "2026-04-11", "2026-04-13", "IW16", "sea"),
    ("1000008644", "Entrega cliente Douai", "2026-04-13", "2026-04-14", "IW16", "truck"),
    ("1000009755", "Recepcion maritima", "2026-04-11", "2026-04-14", "IW16", "sea"),
    ("1000009755", "Entrega cliente Douai", "2026-04-14", "2026-04-15", "IW16", "truck"),
    ("1000010866", "Alternativa aerea Shanghai-Madrid", "2026-04-12", "2026-04-13", "IW16", "air"),
    ("1000010866", "Camion Madrid-Valladolid", "2026-04-13", "2026-04-13", "IW16", "truck"),
    ("1000010866", "Expedicion Valladolid-Douai", "2026-04-14", "2026-04-15", "IW16", "truck"),
]

AIRPORTS = [
    ("AIR-PVG", "Shanghai Pudong", "Shanghai", "CN", 31.1443, 121.8083, "origin_air_hub"),
    ("AIR-MAD", "Adolfo Suarez Madrid-Barajas", "Madrid", "ES", 40.4893, -3.5676, "destination_airport"),
    ("AIR-BCN", "Barcelona-El Prat", "Barcelona", "ES", 41.2974, 2.0833, "destination_airport"),
    ("AIR-VLC", "Valencia Airport", "Valencia", "ES", 39.4893, -0.4816, "destination_airport"),
]

AIR_RECOVERY_CORRIDORS = [
    ("AIRREC-PVG-MAD", "Shanghai Pudong", "Madrid", 17.0, 18400.0, 1.00, 225.0),
    ("AIRREC-PVG-BCN", "Shanghai Pudong", "Barcelona", 16.0, 17600.0, 0.95, 690.0),
    ("AIRREC-PVG-VLC", "Shanghai Pudong", "Valencia", 16.5, 17150.0, 0.93, 360.0),
]


def main() -> None:
    spark = (
        SparkSession.builder.appName("logistica-01-load-master-dimensions")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
        .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse")
        .config("spark.sql.shuffle.partitions", "4")
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
    dim_airports = spark.createDataFrame(
        AIRPORTS,
        ["airport_id", "airport_name", "city_name", "country_code", "lat", "lon", "airport_type"],
    )
    dim_air_recovery = spark.createDataFrame(
        AIR_RECOVERY_CORRIDORS,
        [
            "air_recovery_id",
            "origin_airport",
            "dest_city",
            "air_eta_hours",
            "air_cost_eur",
            "air_co2_factor",
            "truck_km_to_warehouse",
        ],
    )
    dim_articles_valladolid = spark.createDataFrame(
        ARTICLES_VALLADOLID,
        [
            "article_ref",
            "article_name",
            "article_family",
            "pieces_per_pack",
            "daily_consumption_avg",
            "total_stock_pieces",
            "safety_stock_min",
            "total_stock_packs",
            "warehouse_name",
            "inbound_port",
        ],
    )
    fact_customer_orders_douai = spark.createDataFrame(
        CUSTOMER_ORDERS_DOUAI,
        [
            "order_id",
            "article_ref",
            "customer_city",
            "customer_country",
            "industrial_week",
            "planned_dispatch_date",
            "planned_delivery_date",
            "ordered_packs",
            "ordered_pieces",
            "order_status",
        ],
    )
    fact_article_gantt = spark.createDataFrame(
        ARTICLE_GANTT,
        ["article_ref", "task_name", "start_date", "end_date", "industrial_week", "transport_mode"],
    )

    targets = [
        (dim_ports, "logistica.dim_ports", "hdfs://namenode:8020/hadoop/logistica/master/dim_ports"),
        (dim_routes, "logistica.dim_routes", "hdfs://namenode:8020/hadoop/logistica/master/dim_routes"),
        (dim_warehouse, "logistica.dim_warehouse", "hdfs://namenode:8020/hadoop/logistica/master/dim_warehouse"),
        (dim_skus, "logistica.dim_skus", "hdfs://namenode:8020/hadoop/logistica/master/dim_skus"),
        (dim_airports, "logistica.dim_airports", "hdfs://namenode:8020/hadoop/logistica/master/dim_airports"),
        (dim_air_recovery, "logistica.dim_air_recovery", "hdfs://namenode:8020/hadoop/logistica/master/dim_air_recovery"),
        (dim_articles_valladolid, "logistica.dim_articles_valladolid", "hdfs://namenode:8020/hadoop/logistica/master/dim_articles_valladolid"),
        (fact_customer_orders_douai, "logistica.fact_customer_orders_douai", "hdfs://namenode:8020/hadoop/logistica/curated/fact_customer_orders_douai"),
        (fact_article_gantt, "logistica.fact_article_gantt", "hdfs://namenode:8020/hadoop/logistica/curated/fact_article_gantt"),
    ]

    for _, table_name, _ in targets:
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    for dataframe, table_name, path in targets:
        (
            dataframe.coalesce(1)
            .write.mode("overwrite")
            .format("parquet")
            .option("path", path)
            .saveAsTable(table_name)
        )

    print("OK - dimensiones maestras creadas")
    spark.sql("SHOW TABLES IN logistica").show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
