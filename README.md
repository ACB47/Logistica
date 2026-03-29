# Proyecto Logística Marítima (Big Data)

Este repositorio contiene la **guía paso a paso** y los **artefactos** para el proyecto integral de Ingeniería de Datos (ciclo KDD) usando stack Apache.

## Docker - Ejecución portable

El proyecto está **completamente dockerizado** para ejecutarse en cualquier ordenador.

### Requisitos
- Docker y Docker Compose instalados

### Inicio rápido
```bash
./start.sh
```

### Servicios disponibles
| Servicio | URL |
|----------|-----|
| HDFS Namenode | http://localhost:9870 |
| Spark Master | http://localhost:8080 |
| Zeppelin | http://localhost:8081 |
| Airflow | http://localhost:8085 (admin/admin) |
| NiFi | https://localhost:8443 |

### Kafka
- Bootstrap: `localhost:9092`
- Topics: `datos_crudos`, `datos_filtrados`, `datos_filtrados_ok`, `alertas_globales`

### Detener
```bash
./stop.sh
```

## Ruta recomendada de entrega

- Ruta oficial para la memoria y la defensa: Docker/local con HDFS, Kafka KRaft, Spark, Hive, Cassandra, Airflow y NiFi.
- `docker-compose.yml`: stack completo para la demo final.
- `docker-compose.simple.yml`: validacion rapida y sesiones ligeras.
- No se usaran maquinas virtuales en la entrega final.

## NiFi - primer flujo real

- Credenciales locales por defecto:
  - usuario: `admin`
  - password: `Admin123456!`
- Healthcheck rapido:
  - `bash scripts/61_nifi_healthcheck.sh`
- Diseno inicial del flujo real:
  - `docs/01_nifi_open_meteo_flow.md`
- Export reutilizable del canvas:
  - `docs/nifi/OpenMeteo_Kafka_Flow.json`

## Ruta de demo recomendada

- Arranque ligero:
  - `bash scripts/60_manage_docker_stack.sh start simple core`
- Arranque completo:
  - `bash scripts/63_run_weather_filtered_staging.sh full`
- Carga Cassandra:
  - `bash scripts/65_load_vehicle_latest_state_cassandra.sh`
- Rebuild rapido de tablas Hive demo tras reinicio:
  - `bash scripts/66_rebuild_hive_demo_tables.sh`
- Si Spark/Hive pierde tablas tras reinicio, recrear el contenedor `spark` para reabrir el metastore persistente:
  - `docker-compose rm -sf spark && docker-compose up -d spark`

## Dashboard de defensa

- Stack elegido: `Streamlit`
- Motivo: menor complejidad para dashboard, mapas, estados de servicio y paneles KDD.
- Ejecucion:
  - `bash scripts/67_run_dashboard.sh`
- Incluye:
  - estado de servicios `OK/NOK/OFF`
  - botones de arranque/parada por servicio
  - mapa de barcos y rutas con alertas
  - pestañas con diagrama KDD, secuencia, clases y casos de uso
  - tablas de `fact_alerts`, `fact_weather_operational` y `fact_graph_centrality`

## Stack tecnológico (rúbrica cumplida)

| Componente | Implementación |
|------------|----------------|
| Kafka | Streaming de datos en tiempo real |
| Spark Structured Streaming | Procesamiento streaming con ventanas de 15 min |
| Cassandra | Persistencia NoSQL para métricas |
| Hive | Tablas SQL (raw/staging/curated) |
| Zeppelin | Notebooks de visualización |
| MLlib | 3 algoritmos: LinearRegression, RandomForest, K-Means |
| Email Alerts | SMTP configurado para alertas operativas |
| NiFi | Flujo de ingestión de datos |
| GraphFrames | Análisis de rutas marítimas |
| Airflow | Orquestación del pipeline KDD |

## Estructura del proyecto

```
Logistica/
├── docker-compose.yml    # Orquestación de servicios
├── Dockerfile            # Imagen base
├── start.sh            # Script de inicio
├── stop.sh             # Script de parada
├── airflow/            # DAGs de Airflow
│   └── dags/
├── jobs/               # Pipelines Spark
│   ├── spark/         # Jobs de procesamiento
│   └── cassandra/     # Setup de Cassandra
├── ingesta/            # Productores de datos
│   └── productores/
├── zeppelin/           # Notebooks de Zeppelin
├── kafka/              # Configuración Kafka
└── docs/               # Documentación
```

## Pipeline KDD

1. **Ingesta**: Productores Kafka envían datos GPS de barcos y alertas
2. **Streaming**: Spark Structured Streaming procesa ventanas de 15 minutos
3. **ML**: Modelos de predicción (LinearRegression, RandomForest, K-Means)
4. **Persistencia**: Cassandra + Hive
5. **Alertas**: Email automático según severidad
6. **Visualización**: Zeppelin notebooks

## Caso de uso

- **Origen**: puerto Shanghai (China).
- **Destino**: Algeciras / Valencia / Barcelona.
- **Objetivo**: monitorizar barcos (GPS) y, si hay alertas (geopolítica o clima adverso), **avisar** para activar un plan de contingencia.

## Pendiente para cierre completo de entrega

- [ ] Configurar credenciales SMTP en `.env`
- [ ] Verificar en Airflow la ejecucion visual de los dos DAGs finales
- [ ] Completar documento Word con capturas finales
