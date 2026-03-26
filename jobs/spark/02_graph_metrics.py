from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, lit, when, sqrt
from pyspark.sql.types import DoubleType

from graphframes import GraphFrame


def main() -> None:
    spark = (
        SparkSession.builder.appName("logistica-02-graph-metrics")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
        .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse")
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sql("CREATE DATABASE IF NOT EXISTS logistica")

    ships = spark.table("logistica.stg_ships")
    clima = spark.table("logistica.stg_alerts_clima")
    noticias = spark.table("logistica.stg_alerts_noticias")

    # ---------------------------------
    # 1) Riesgo global (a falta de ML)
    # ---------------------------------
    alerts_all = clima.select("severity").unionByName(noticias.select("severity"))
    risk_score = alerts_all.agg(avg(col("severity")).alias("risk")).collect()[0]["risk"]
    risk_score = float(risk_score) if risk_score is not None else 0.0

    # ---------------------------------
    # 2) Modelo de grafo (puertos -> Valladolid)
    # ---------------------------------
    # Coordenadas (aprox) para estimar el tiempo por distancia
    coords = [
        ("Shanghai", 31.2304, 121.4737),
        ("Algeciras", 36.127, -5.453),
        ("Valencia", 39.458, -0.317),
        ("Barcelona", 41.352, 2.173),
        ("Valladolid", 41.652, -4.724),  # referencia interna
    ]
    # Construcción de tabla de coordenadas (más robusto)
    ports_coord = spark.createDataFrame(coords, ["port_name", "lat", "lon"])

    # Puertos vistos en datos
    ports_from_data = ships.select(col("origin_port").alias("port_name")).unionByName(
        ships.select(col("dest_port").alias("port_name"))
    ).distinct()

    ports = ports_from_data.unionByName(spark.createDataFrame([("Valladolid",)], ["port_name"])).distinct()
    vertices = ports.select(col("port_name").alias("id")).withColumn(
        "type", when(col("id") == "Valladolid", lit("warehouse")).otherwise(lit("port"))
    )

    # Aristas Shanghai -> puerto de destino
    ship_routes = ships.select("origin_port", "dest_port", "speed_kn").groupBy(
        "origin_port", "dest_port"
    ).agg(avg("speed_kn").alias("avg_speed_kn"))

    src_dst = ship_routes.alias("r").join(
        ports_coord.alias("pc_src"),
        col("r.origin_port") == col("pc_src.port_name"),
        "inner",
    ).join(
        ports_coord.alias("pc_dst"),
        col("r.dest_port") == col("pc_dst.port_name"),
        "inner",
    )

    # Distancia aproximada usando una fórmula lineal por simplicidad (dataset pequeño).
    # Para proyecto documental se explica como aproximación.
    distance_km = (
        (col("pc_dst.lat") - col("pc_src.lat")) * (111.32)
    ) ** 2 + (
        (col("pc_dst.lon") - col("pc_src.lon")) * (111.32)
    ) ** 2
    distance_km = sqrt(distance_km).cast(DoubleType())

    edges_1 = (
        src_dst.select(
            col("r.origin_port").alias("src"),
            col("r.dest_port").alias("dst"),
            distance_km.alias("distance_km"),
            col("r.avg_speed_kn").alias("avg_speed_kn"),
        )
        .withColumn("weight_hours", col("distance_km") / (col("avg_speed_kn") * lit(1.852)))
        .select("src", "dst", col("weight_hours").alias("weight"))
    )

    # Aristas puerto de destino -> Valladolid (tramo interno aproximado)
    # Usamos coste fijo (ej. 8h) para el tramo terrestre/gestión interna.
    port_to_valladolid = edges_1.select(col("dst").alias("src")).distinct().withColumn(
        "dst", lit("Valladolid")
    )
    edges_2 = port_to_valladolid.withColumn("weight", lit(8.0)).select("src", "dst", "weight")

    edges = edges_1.unionByName(edges_2)

    g = GraphFrame(vertices, edges)

    # ---------------------------------
    # 3) Métricas GraphFrames (evidencia)
    # ---------------------------------
    shortest = g.shortestPaths(landmarks=["Shanghai"])
    # distancias: Map<landmark, hops>
    shortest_hops = shortest.select(
        col("id").alias("node_id"),
        col("type").alias("node_type"),
        col("distances").getItem("Shanghai").alias("hops_from_shanghai"),
    )

    centrality = (
        g.degrees.join(vertices, g.degrees.id == vertices.id, "left")
        .select(
            g.degrees.id.alias("node_id"),
            col("type").alias("node_type"),
            col("degree"),
            when(col("degree") >= lit(2), lit("NODO_CRITICO")).otherwise(lit("NODO_NORMAL")).alias(
                "criticality_level"
            ),
        )
    )

    # ---------------------------------
    # 4) Risk table por ruta (puerto intermedio)
    # ---------------------------------
    # Para cada puerto intermedio: hop=1 y total ~ peso(shanghai->puerto) + 8h
    # (Es una aproximación para la documentación.)
    risk_by_route = (
        edges_1.filter(col("dst") != "Valladolid")
        .select(col("dst").alias("via_port"), col("weight").alias("time_shanghai_to_via_hours"))
        .withColumn("time_via_to_valladolid_hours", lit(8.0))
        .withColumn("estimated_total_hours", col("time_shanghai_to_via_hours") + col("time_via_to_valladolid_hours"))
        .withColumn("risk_score", lit(risk_score))
        .withColumn(
            "risk_level",
            when(col("risk_score") >= lit(4.0), lit("ALTO"))
            .when(col("risk_score") >= lit(2.5), lit("MEDIO"))
            .otherwise(lit("BAJO")),
        )
    )

    spark.sql("DROP TABLE IF EXISTS logistica.fact_route_risk")
    (
        risk_by_route.write.mode("overwrite")
        .format("parquet")
        .option("path", "hdfs://namenode:8020/hadoop/logistica/curated/fact_route_risk")
        .saveAsTable("logistica.fact_route_risk")
    )

    # También persistimos la tabla de hops para evidencia
    spark.sql("DROP TABLE IF EXISTS logistica.fact_graph_hops")
    shortest_hops.write.mode("overwrite").format("parquet").option(
        "path", "hdfs://namenode:8020/hadoop/logistica/curated/fact_graph_hops"
    ).saveAsTable("logistica.fact_graph_hops")

    spark.sql("DROP TABLE IF EXISTS logistica.fact_graph_centrality")
    centrality.write.mode("overwrite").format("parquet").option(
        "path", "hdfs://namenode:8020/hadoop/logistica/curated/fact_graph_centrality"
    ).saveAsTable("logistica.fact_graph_centrality")

    print("OK - GraphFrames metrics creadas:")
    spark.sql("SHOW TABLES IN logistica").show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
