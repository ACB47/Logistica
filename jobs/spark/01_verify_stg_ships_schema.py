from pyspark.sql import SparkSession


def main() -> None:
    spark = (
        SparkSession.builder.appName("verify-stg-ships-schema")
        .enableHiveSupport()
        .getOrCreate()
    )

    df = spark.table("logistica.stg_ships")
    print("stg_ships columns:")
    print(df.columns)

    # Muestra valores para asegurar que existe la lógica.
    cols_to_show = [c for c in ["ship_id", "ts", "stock_on_hand", "reorder_point"] if c in df.columns]
    print("Sample rows (selected cols):", cols_to_show)
    df.select(*cols_to_show).orderBy("ts").show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()

