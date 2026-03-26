from __future__ import annotations

import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, col, count, from_json, lit, stddev, to_timestamp, when, window, udf
)
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType
)
from pyspark.ml.classification import RandomForestClassificationModel, RandomForestClassifier
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import LinearRegressionModel, LinearRegression
from pyspark.ml.clustering import KMeansModel, KMeans


SHIP_SCHEMA = StructType([
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
])

ALERT_SCHEMA = StructType([
    StructField("event_type", StringType(), True),
    StructField("ts", StringType(), True),
    StructField("source", StringType(), True),
    StructField("severity", IntegerType(), True),
    StructField("region", StringType(), True),
    StructField("text", StringType(), True),
    StructField("confidence", DoubleType(), True),
])


@udf
def classify_risk(severity_avg: float, speed_std: float) -> str:
    score = severity_avg * 0.7 + (speed_std / 10) * 0.3
    if score >= 3.5:
        return "CRITICO"
    elif score >= 2.0:
        return "ALTO"
    elif score >= 1.0:
        return "MEDIO"
    return "BAJO"


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("logistica-streaming-ml")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
        .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse")
        .config("spark.sql.streaming.checkpointLocation", "hdfs://namenode:8020/hadoop/logistica/checkpoint")
        .config("spark.cassandra.connection.host", "cassandra")
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "datos_crudos")
        .option("startingOffsets", "latest")
        .load()
    )

    ships_df = (
        kafka_df.select(from_json(col("value").cast("string"), SHIP_SCHEMA).alias("data"))
        .select("data.*")
        .filter(col("event_type") == "ship_position")
        .withColumn("event_ts", to_timestamp(col("ts")))
        .dropna(subset=["event_ts", "ship_id", "dest_port", "route_id"])
    )

    alerts_kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "alertas_globales")
        .option("startingOffsets", "latest")
        .load()
    )

    alerts_df = (
        alerts_kafka_df.select(from_json(col("value").cast("string"), ALERT_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_ts", to_timestamp(col("ts")))
        .dropna(subset=["event_ts", "region", "source"])
    )

    windowed_ships = (
        ships_df
        .withWatermark("event_ts", "15 minutes")
        .groupBy(
            window(col("event_ts"), "15 minutes"),
            col("dest_port"),
            col("route_id")
        )
        .agg(
            avg("speed_kn").alias("avg_speed"),
            stddev("speed_kn").alias("speed_std"),
            count("*").alias("ship_count"),
            avg("lat").alias("avg_lat"),
            avg("lon").alias("avg_lon")
        )
    )

    windowed_alerts = (
        alerts_df
        .withWatermark("event_ts", "15 minutes")
        .groupBy(
            window(col("event_ts"), "15 minutes"),
            col("region"),
            col("source")
        )
        .agg(
            avg("severity").alias("avg_severity"),
            avg("confidence").alias("avg_confidence"),
            count("*").alias("alert_count")
        )
    )

    def write_to_cassandra_ml(df, epoch_id):
        if df.count() > 0:
            df.write \
                .format("org.apache.spark.sql.cassandra") \
                .mode("append") \
                .options(table="ship_metrics", keyspace="logistica") \
                .save()

    query_ships = (
        windowed_ships
        .writeStream
        .foreachBatch(write_to_cassandra_ml)
        .outputMode("append")
        .option("checkpointLocation", "hdfs://namenode:8020/hadoop/logistica/checkpoint/ships")
        .start()
    )

    def write_alerts_to_cassandra(df, epoch_id):
        if df.count() > 0:
            df.write \
                .format("org.apache.spark.sql.cassandra") \
                .mode("append") \
                .options(table="alert_metrics", keyspace="logistica") \
                .save()

    query_alerts = (
        windowed_alerts
        .writeStream
        .foreachBatch(write_alerts_to_cassandra)
        .outputMode("append")
        .option("checkpointLocation", "hdfs://namenode:8020/hadoop/logistica/checkpoint/alerts")
        .start()
    )

    training_data = spark.table("logistica.fact_route_risk")
    if training_data.count() > 0:
        risk_features = training_data.select(
            col("risk_score").alias("label"),
            col("time_shanghai_to_via_hours").alias("feature1"),
            col("time_via_to_valladolid_hours").alias("feature2"),
            col("estimated_total_hours").alias("feature3")
        ).na.drop()

        feature_cols = ["feature1", "feature2", "feature3"]
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        assembled = assembler.transform(risk_features).select("features", "label")

        train_data, test_data = assembled.randomSplit([0.8, 0.2], seed=42)

        lr = LinearRegression(featuresCol="features", labelCol="label", maxIter=10)
        lr_model = lr.fit(train_data)
        lr_predictions = lr_model.evaluate(test_data)

        risk_features_indexed = risk_features.withColumn(
            "risk_label",
            when(col("label") >= 3.5, lit(3.0))
            .when(col("label") >= 2.0, lit(2.0))
            .when(col("label") >= 1.0, lit(1.0))
            .otherwise(lit(0.0))
        )
        assembled_rf = assembler.transform(risk_features_indexed).select("features", "risk_label")

        rf = RandomForestClassifier(featuresCol="features", labelCol="risk_label", numTrees=5)
        rf_model = rf.fit(assembled_rf)
        rf_predictions = rf_model.transform(assembled_rf)

        rf_accuracy = rf_predictions.filter(
            col("prediction") == col("risk_label")
        ).count() / float(rf_predictions.count())

        kmeans_data = assembled.select("features")
        kmeans = KMeans(k=3, seed=42)
        kmeans_model = kmeans.fit(kmeans_data)

        print(f"=== ML Metrics ===")
        print(f"Linear Regression RMSE: {lr_predictions.rootMeanSquaredError:.4f}")
        print(f"Linear Regression R2: {lr_predictions.r2:.4f}")
        print(f"RandomForest Accuracy: {rf_accuracy:.4f}")
        print(f"KMeans Clusters: {kmeans_model.clusterCenters()}")

    query_ships.awaitTermination()
    query_alerts.awaitTermination()


if __name__ == "__main__":
    main()
