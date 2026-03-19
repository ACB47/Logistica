from pyspark.sql import SparkSession


def main() -> None:
    spark = SparkSession.builder.appName("check-hive-tables").enableHiveSupport().getOrCreate()
    spark.sql("SHOW DATABASES").show(truncate=False)
    spark.sql("SHOW TABLES IN logistica").show(truncate=False)

    for tbl in ["stg_ships", "stg_alerts_clima", "stg_alerts_noticias", "fact_route_risk", "fact_graph_hops"]:
        try:
            cnt = spark.table(f"logistica.{tbl}").count()
            print(f"logistica.{tbl} rows={cnt}")
        except Exception as e:
            print(f"WARNING: no existe logistica.{tbl} ({e})")

    spark.stop()


if __name__ == "__main__":
    main()

