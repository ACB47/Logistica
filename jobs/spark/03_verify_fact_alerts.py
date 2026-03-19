from pyspark.sql import SparkSession


def main() -> None:
    spark = SparkSession.builder.appName("verify-fact-alerts").enableHiveSupport().getOrCreate()
    try:
        df = spark.table("logistica.fact_alerts")
        print("logistica.fact_alerts rows=", df.count())
        df.orderBy("alert_ts", ascending=False).show(20, truncate=False)
    except Exception as e:
        print("WARNING: no existe logistica.fact_alerts:", e)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

