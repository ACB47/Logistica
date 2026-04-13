# Diagrama de Arquitectura

```mermaid
flowchart TB
    subgraph Fuentes
        GPS[Productor GPS<br/>ships_gps_producer.py]
        ALERT[Productor Alertas<br/>alerts_producer.py]
        NIFI[NiFi<br/>Open-Meteo API]
    end

    subgraph Ingesta
        KAFKA[Kafka Cluster<br/>datos_crudos<br/>alertas_globales<br/>datos_filtrados]
    end

    subgraph Raw
        HDFS_RAW[HDFS Raw<br/>/hadoop/logistica/raw/]
    end

    subgraph Staging
        SPARK_STG[Spark Raw→Staging<br/>01_raw_to_staging.py]
        HIVE_STG[(Hive Staging<br/>stg_ships<br/>stg_alerts_*)]
    end

    subgraph Dimensiones
        SPARK_DIM[Spark Dimensiones<br/>01_load_master_dimensions.py]
        HIVE_DIM[(Hive Dimensiones<br/>dim_ports<br/>dim_routes<br/>dim_warehouse<br/>dim_articles_*)]
    end

    subgraph Transformacion
        SPARK_ENR[Spark Enrichment<br/>02_weather_port_enrichment.py]
        SPARK_GRAPH[Spark Grafos<br/>02_graph_metrics.py]
        SPARK_SCORING[Spark Scoring<br/>03_score_and_alert.py]
    end

    subgraph Curated
        HIVE_CUR[(Hive Curated<br/>fact_weather_*<br/>fact_alerts<br/>fact_air_recovery<br/>graph_critical_nodes)]
        CASSandra[(Cassandra<br/>vehicle_latest_state)]
    end

    subgraph Orquestacion
        AIRFLOW[Airflow DAG<br/>logistica_kdd_microbatch]
    end

    subgraph Visualizacion
        DASH[Streamlit Dashboard<br/>Control Tower<br/>Stock Valladolid<br/>Gantt]
    end

    GPS --> KAFKA
    ALERT --> KAFKA
    NIFI --> KAFKA

    KAFKA --> HDFS_RAW

    HDFS_RAW --> SPARK_STG
    SPARK_STG --> HIVE_STG

    HIVE_STG --> SPARK_DIM
    SPARK_DIM --> HIVE_DIM

    HIVE_DIM --> SPARK_ENR
    HIVE_DIM --> SPARK_GRAPH
    HIVE_DIM --> SPARK_SCORING

    SPARK_ENR --> HIVE_CUR
    SPARK_GRAPH --> HIVE_CUR
    SPARK_SCORING --> HIVE_CUR
    SPARK_SCORING --> CASSandra

    AIRFLOW -->|orquesta| SPARK_STG
    AIRFLOW -->|orquesta| SPARK_DIM
    AIRFLOW -->|orquesta| SPARK_ENR
    AIRFLOW -->|orquesta| SPARK_GRAPH
    AIRFLOW -->|orquesta| SPARK_SCORING

    HIVE_CUR --> DASH
    CASSandra --> DASH
```

---

## Flujo de Datos

```
1. Productores → Kafka (datos_crudos, alertas_globales)
2. Consumidor → HDFS Raw (/raw/ships, /raw/clima, /raw/noticias)
3. Spark Raw→Staging → Hive Staging (stg_*)
4. Spark Dimensiones → Hive Dimensiones (dim_*)
5. Spark Enrichment → Hive Curated (fact_*)
6. Spark Grafos → Hive Curated (graph_*)
7. Spark Scoring → Hive Curated + Cassandra
8. Dashboard → Lee Hive + Cassandra
```

---

## Componentes por Capa

| Capa | Componente | Función |
|------|-----------|---------|
| Ingesta | Kafka | Message broker, pub/sub |
| Ingesta | NiFi | API integration (Open-Meteo) |
| Raw | HDFS | Persistencia auditoría |
| Staging | Spark + Hive | Limpieza, tipado, deduplicación |
| Dimensiones | Spark + Hive | Catálogos maestros |
| Transformación | Spark | Enrichment, grafos, ML |
| Curated | Hive + Cassandra | Facts analíticos, estado último |
| Orquestación | Airflow | DAG, reintentos, alertas |
| Visualización | Streamlit | Dashboard operacional |
