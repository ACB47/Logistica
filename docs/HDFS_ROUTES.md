# Rutas HDFS Finales

Documento que especifica las rutas HDFS confirmadas para la entrega final.

---

## Raw Layer (`/hadoop/logistica/raw/`)

| Subcarpeta | Descripción | Partición |
|-----------|--------------|-----------|
| `ships/YYYY/MM/DD/HHMM/*.jsonl` | Posiciones GPS de barcos | Minute |
| `clima/YYYY/MM/DD/HHMM/*.jsonl` | Alertas climáticas | Minute |
| `noticias/YYYY/MM/DD/HHMM/*.jsonl` | Alertas geopolíticas | Minute |

Ejemplo:
```
/hadoop/logistica/raw/ships/2026/04/13/1030/data.jsonl
/hadoop/logistica/raw/clima/2026/04/13/1030/data.jsonl
/hadoop/logistica/raw/noticias/2026/04/13/1030/data.jsonl
```

---

## Staging Layer (`/hadoop/logistica/staging/`)

| Tabla | Descripción |
|-------|-------------|
| `stg_ships` | Posiciones GPS limpiadas y deduplicadas |
| `stg_alerts_clima` | Alertas climáticas procesadas |
| `stg_alerts_noticias` | Alertas geopolíticas procesadas |

Rutas:
```
hdfs://namenode:8020/hadoop/logistica/staging/stg_ships
hdfs://namenode:8020/hadoop/logistica/staging/stg_alerts_clima
hdfs://namenode:8020/hadoop/logistica/staging/stg_alerts_noticias
```

---

## Curated Layer (`/hadoop/logistica/curated/`)

| Tabla | Descripción |
|-------|-------------|
| `fact_weather_operational` | Facts operativos de clima |
| `fact_alerts` | Alertas consolidadas |
| `fact_air_recovery_options` | Opciones de contingencia aérea |
| `dim_ports_routes_weather` | Dimensión puertos y rutas con clima |

Rutas:
```
hdfs://namenode:8020/hadoop/logistica/curated/fact_weather_operational
hdfs://namenode:8020/hadoop/logistica/curated/fact_alerts
hdfs://namenode:8020/hadoop/logistica/curated/fact_air_recovery_options
hdfs://namenode:8020/hadoop/logistica/curated/dim_ports_routes_weather
```

---

## Hive Tables

| Database | Tabla | Descripción |
|----------|------|-------------|
| `logistica` | `dim_ports` | Maestro de puertos |
| `logistica` | `dim_routes` | Maestro de rutas |
| `logistica` | `dim_warehouse` | Maestro de almacenes |
| `logistica` | `dim_articles_valladolid` | Artículos Valladolid |
| `logistica` | `fact_customer_orders_douai` | Pedidos cliente Douai |
| `logistica` | `fact_article_gantt` | Gantt de planificación |

---

## Verificación de Rutas

```bash
# Verificar estructura raw
hdfs dfs -ls /hadoop/logistica/raw/
hdfs dfs -ls /hadoop/logistica/raw/ships/
hdfs dfs -ls /hadoop/logistica/raw/ships/2026/

# Verificar staging
hdfs dfs -ls /hadoop/logistica/staging/

# Verificar curated
hdfs dfs -ls /hadoop/logistica/curated/

# Listar tablas Hive
docker exec -it spark /opt/spark/bin/beeline -u "jdbc:hive2://hiveserver:10000" -e "SHOW TABLES IN logistica;"
```