# Enfoque de Streaming vs Micro-batch

Este documento clarifica la decisión técnica tomada para la defensa del proyecto.

---

## Opción elegida: Micro-batch documentado

Para la defensa del proyecto se utiliza **micro-batch** como enfoque principal por las siguientes razones:

1. **Fiabilidad**: El entorno Docker/local tiene recursos limitados
2. **Trazabilidad**: Cada ejecución es un job discrete con logs claros
3. **Re-ejecutable**: Fácil de re-ejecutar en caso de fallo
4. **Documentado**: El DAG de Airflow muestra cada paso

---

## Detalle del Micro-batch (cada 15 minutos)

```
DAG: logistica_kdd_microbatch
Schedule: */15 * * * * (cada 15 minutos)

Tareas:
1. ensure_hdfs_paths     -> Crea directorios HDFS si no existen
2. spark_raw_to_staging     -> Limpia raw -> staging
3. spark_build_dimensions   -> Carga dimensiones Hive
4. spark_weather_filtered_to_staging -> Datos filtrados
5. spark_weather_port_enrichment -> Enrich con puertos
6. spark_build_graph_metrics  -> Métricas de grafo
7. spark_score_and_alert    -> Scoring y alertas
8. spark_air_recovery    -> Opciones contingencia
9. spark_load_cassandra   -> Latest state Cassandra
10. cleanup_old_partitions -> Limpieza HDFS
```

---

## Streaming como evidencia técnica complementaria

El código de streaming existe en `jobs/spark/04_streaming_ml_pipeline.py` como **evidencia técnica** de que el equipo domina Structured Streaming, pero no se ejecuta en producción por las razones anteriores.

### Para ejecutar streaming manualmente (solo en entorno con recursos):

```bash
# En el contenedor Spark
docker exec -it logistica-spark-1 spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /home/jovyan/jobs/spark/04_streaming_ml_pipeline.py
```

### Características del streaming:

- Consume de `datos_crudos` y `alertas_globales`
- Ventana de 15 minutos (como exige el enunciado)
- UDF para clasificación de riesgo
- ML integrado: RandomForest, LinearRegression, KMeans
- Checkpoint en HDFS

---

## Comparación

| Aspecto | Micro-batch (defensa) | Streaming (experimental) |
|---------|---------------------|-------------------------|
| Fiabilidad | Alta | Media |
| Debug | Fácil | Complejo |
| Recursos | Bajo | Alto |
| Latencia | ~15 min | ~segundos |
| Trazabilidad | Excelente | Buena |

---

## Recomendación para la defensa

**Presentar micro-batch como enfoque principal** con:
1. DAG de Airflow visible cada 15 minutos
2. Logs de ejecución exitosos
3. Streaming como evidencia de conocimiento técnico avanzado

Esto demuestra:
- Dominio de KDD completo (ingesta → procesamiento → analytics)
- Capacidad de resolver problemas reales (micro-batch > streaming en recursos limitados)
- Conocimiento de streaming (código existe pero no se usa por decisión técnica)