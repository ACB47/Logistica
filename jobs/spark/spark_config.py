from pyspark.sql import SparkSession


def get_optimized_spark(app_name: str = "logistica-spark-job") -> SparkSession:
    """
    Crea una SparkSession optimizada para entorno local con recursos limitados.
    
    Configuraciones optimizadas:
    - Memoria reducida: driver 1g, executor 1g
    - Particiones reducidas: 10 (vs 200 por defecto)
    - Modo local eficiente: local[*]
    - Serialización optimizada
    - Compression activa
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.sql.shuffle.partitions", "10")
        .config("spark.default.parallelism", "10")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryoserializer.buffer.max", "256m")
        .config("spark.rdd.compress", "true")
        .config("spark.io.compression.codec", "snappy")
        .config("spark.sql.autoBroadcastJoinThreshold", "10m")
        .config("spark.sql.files.maxPartitionBytes", "128m")
        .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse")
        .enableHiveSupport()
        .getOrCreate()
    )


def get_dashboard_spark(app_name: str = "logistica-dashboard") -> SparkSession:
    """
    SparkSession optimizada para el dashboard con caché y recursos受限.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "512m")
        .config("spark.sql.shuffle.partitions", "5")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse")
        .config("spark.sql.crossJoin.enabled", "true")
        .enableHiveSupport()
        .getOrCreate()
    )


def get_streaming_spark(app_name: str = "logistica-streaming") -> SparkSession:
    """
    SparkSession optimizada para streaming con micro-batches.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.sql.shuffle.partitions", "10")
        .config("spark.sql.streaming.checkpointLocation", "hdfs://namenode:8020/hadoop/logistica/checkpoint")
        .config("spark.sql.streaming.maxOffsetsPerTrigger", "1000")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.warehouse.dir", "hdfs://namenode:8020/user/hive/warehouse")
        .enableHiveSupport()
        .getOrCreate()
    )