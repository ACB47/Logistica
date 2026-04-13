"""
Módulo de caché y conexiones optimizadas para el dashboard Streamlit.

Proporciona:
- @st.cache_resource: para conexiones a bases de datos (Spark, Cassandra)
- @st.cache_data: para consultas pesadas que no cambian frecuentemente
"""

import streamlit as st
from typing import Optional
import logging

logger = logging.getLogger(__name__)


@st.cache_resource(show_spinner=False)
def get_spark_session() -> "SparkSession":
    """
    Crea y cachea una SparkSession optimizada para el dashboard.
    
    Returns:
        SparkSession configurada con Hive support y recursos optimizados.
    """
    from pyspark.sql import SparkSession
    
    logger.info("Creando SparkSession optimizada para dashboard...")
    
    spark = (
        SparkSession.builder
        .appName("logistica-dashboard")
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
        .config("spark.ui.enabled", "false")
        .enableHiveSupport()
        .getOrCreate()
    )
    
    logger.info("SparkSession creada exitosamente")
    return spark


@st.cache_resource(show_spinner=False)
def get_cassandra_session():
    """
    Crea y cachea una conexión a Cassandra.
    
    Returns:
        Cluster de Cassandra conectado.
    """
    from cassandra.cluster import Cluster
    
    logger.info("Conectando a Cassandra...")
    
    try:
        cluster = Cluster(["cassandra"])
        session = cluster.connect("logistica")
        logger.info("Conexión a Cassandra establecida")
        return cluster
    except Exception as e:
        logger.error(f"Error conectando a Cassandra: {e}")
        return None


def close_cassandra():
    """Cierra la conexión a Cassandra (llamar al desmontar)."""
    cluster = get_cassandra_session._cache.get("value")
    if cluster:
        cluster.shutdown()
        logger.info("Conexión a Cassandra cerrada")


@st.cache_data(ttl=300, show_spinner="Cargando datos de Hive...")
def query_hive_table(table_name: str, columns: Optional[list] = None, limit: int = 1000) -> "pd.DataFrame":
    """
    Consulta una tabla de Hive con caché.
    
    Args:
        table_name: Nombre de la tabla (incluir esquema: logistica.tabla)
        columns: Lista de columnas a seleccionar (None = todas)
        limit: Límite de filas
    
    Returns:
        DataFrame de pandas con los resultados.
    """
    spark = get_spark_session()
    
    cols_str = ", ".join(columns) if columns else "*"
    query = f"SELECT {cols_str} FROM {table_name} LIMIT {limit}"
    
    logger.info(f"Ejecutando consulta: {query}")
    
    df = spark.sql(query).toPandas()
    
    logger.info(f"Consulta completada: {len(df)} filas recuperadas")
    return df


@st.cache_data(ttl=600, show_spinner="Cargando datos de Cassandra...")
def query_cassandra_table(cql_query: str) -> list:
    """
    Ejecuta una consulta CQL en Cassandra con caché.
    
    Args:
        cql_query: Consulta CQL a ejecutar.
    
    Returns:
        Lista de resultados.
    """
    cluster = get_cassandra_session()
    
    if cluster is None:
        logger.warning("Cassandra no disponible")
        return []
    
    session = cluster.connect("logistica")
    
    logger.info(f"Ejecutando CQL: {cql_query[:100]}...")
    
    result = session.execute(cql_query)
    rows = list(result)
    
    logger.info(f"Consulta completada: {len(rows)} filas")
    return rows


@st.cache_data(ttl=300)
def get_cassandra_vehicle_state() -> list:
    """Obtiene el estado actual de vehículos desde Cassandra."""
    cql = "SELECT * FROM vehicle_latest_state"
    return query_cassandra_table(cql)


@st.cache_data(ttl=600)
def get_dim_ports() -> "pd.DataFrame":
    """Obtiene dimensiones de puertos desde Hive."""
    return query_hive_table("logistica.dim_ports")


@st.cache_data(ttl=600)
def get_dim_routes() -> "pd.DataFrame":
    """Obtiene dimensiones de rutas desde Hive."""
    return query_hive_table("logistica.dim_routes")


@st.cache_data(ttl=300)
def get_stock_valladolid() -> "pd.DataFrame":
    """Obtiene stock de Valladolid desde Hive."""
    return query_hive_table("logistica.dim_articles_valladolid")


@st.cache_data(ttl=300)
def get_fact_alerts() -> "pd.DataFrame":
    """Obtiene alertas operativas desde Hive."""
    return query_hive_table("logistica.fact_alerts")


@st.cache_data(ttl=600)
def get_fact_weather_operational() -> "pd.DataFrame":
    """Obtiene datos operativos del clima desde Hive."""
    return query_hive_table("logistica.fact_weather_operational")


@st.cache_data(ttl=600)
def get_fact_graph_centrality() -> "pd.DataFrame":
    """Obtiene centrality de grafos desde Hive."""
    return query_hive_table("logistica.fact_graph_centrality")


@st.cache_data(ttl=600)
def get_ships_latest() -> "pd.DataFrame":
    """Obtiene últimas posiciones de barcos desde Hive."""
    return query_hive_table("logistica.stg_ships")


@st.cache_data(ttl=600)
def get_article_gantt() -> "pd.DataFrame":
    """Obtiene datos de Gantt de artículos desde Hive."""
    return query_hive_table("logistica.fact_article_gantt")


@st.cache_data(ttl=600)
def get_customer_orders_douai() -> "pd.DataFrame":
    """Obtiene pedidos de clientes desde Hive."""
    return query_hive_table("logistica.fact_customer_orders_douai")


@st.cache_data(ttl=600)
def get_air_recovery_options() -> "pd.DataFrame":
    """Obtiene opciones de recuperación aérea desde Hive."""
    return query_hive_table("logistica.fact_air_recovery_options")


def clear_all_caches():
    """Limpia todos los cachés de Streamlit."""
    st.cache_data.clear()
    st.cache_resource.clear()
    logger.info("Todos los cachés han sido limpiados")


def get_memory_usage() -> dict:
    """Retorna información de uso de memoria del entorno Spark."""
    try:
        spark = get_spark_session()
        conf = spark.sparkContext.getConf()
        return {
            "driver_memory": conf.get("spark.driver.memory", "N/A"),
            "executor_memory": conf.get("spark.executor.memory", "N/A"),
            "shuffle_partitions": conf.get("spark.sql.shuffle.partitions", "N/A"),
        }
    except Exception as e:
        logger.warning(f"No se pudo obtener información de memoria: {e}")
        return {}