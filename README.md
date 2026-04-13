# Proyecto Logística Marítima (Big Data)

Autora del proyecto: **Ana Coloma Bausela**

Este repositorio contiene la **guía paso a paso** y los **artefactos** para el proyecto integral de Ingeniería de Datos (ciclo KDD) usando stack Apache.

## Docker - Ejecución portable

El proyecto está **completamente dockerizado** para ejecutarse en cualquier ordenador.

### Requisitos
- Docker con `docker compose` disponible

### Inicio rápido
```bash
./start.sh
```

### Dashboard con datos reales
- Ruta validada para la demo completa:
  - `docker compose up -d postgres kafka nifi spark cassandra namenode datanode airflow-webserver`
  - `bash scripts/66_rebuild_hive_demo_tables.sh`
  - `docker compose exec -T spark spark-submit /home/jovyan/jobs/spark/99_dashboard_bundle.py`
  - `bash scripts/67_run_dashboard.sh`

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
  - password: `CHANGE_ME_NIFI_PASSWORD` o valor definido en `.env`
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
  - `docker compose rm -sf spark && docker compose up -d spark`

## Optimización local de recursos

- `docker-compose.yml` ya limita memoria y CPU en los servicios más pesados para evitar saturar el host:
  - `spark`: `2g`, `1.5` CPU
  - `kafka`, `namenode`, `datanode`, `cassandra`, `nifi`, `airflow-webserver`: perfiles reducidos para demo local
- El arranque ahora usa `healthcheck` y `depends_on.condition` en Kafka, HDFS y Cassandra para evitar bucles de arranque.
- Los jobs Spark comparten configuración optimizada en `jobs/spark/spark_config.py`.
- El dashboard usa `docker compose` v2 y detecta correctamente los nombres de contenedor actuales (`logistica-spark-1`, `logistica-kafka-1`, etc.).

## DASHBOARD

Código fuente Python del dashboard:

- `dashboard/app.py`: aplicación principal Streamlit, navegación, vistas y lógica visual.
- `dashboard/cache_utils.py`: utilidades de caché y conexiones para consultas a Hive/Cassandra.
- `jobs/spark/99_dashboard_bundle.py`: generación del bundle JSON consumido por el dashboard.
- `scripts/67_run_dashboard.sh`: arranque local del dashboard Streamlit.

Diseño y experiencia visual del dashboard:

- Estilo visual corporativo con `Streamlit` y `Plotly`, orientado a defensa ejecutiva y auditoría técnica.
- Menú lateral jerárquico con navegación por botones, identidad de marca y accesos técnicos del stack.
- Portada `Resumen Ejecutivo` con KPIs, donuts operativos, proyección visual y regla económica explícita de contingencia aérea.
- Vista `Control Tower Valladolid` con enfoque operacional: stock, cobertura, Gantt industrial, flujo de contingencia multimodal y envío SMTP de alertas críticas.
- Vista `Arquitectura en vivo` con enfoque técnico minimalista: estado del pipeline, controles del stack y lista viva de servicios KDD.
- Pestañas de auditoría KDD, persistencia, orquestación y evidencias diseñadas para mostrar trazabilidad técnica durante la defensa.

Operación del dashboard:

- Stack elegido: `Streamlit`
- Ejecución:

```bash
bash scripts/67_run_dashboard.sh
```

- Si el dashboard aparece vacío, levantar primero el stack HDFS y regenerar el bundle.
- Si el panel de servicios no refleja cambios recientes, refrescar la página o esperar el TTL de caché de 15 segundos.
- Incluye:
  - estado de servicios `OK/NOK/OFF`
  - botones de arranque/parada por servicio
  - menú lateral corporativo con navegación por botones
  - visor integrado de documentación Markdown
  - mapa de barcos y rutas con alertas
  - pestañas con diagramas KDD, secuencia, clases y casos de uso
  - tablas de `fact_alerts`, `fact_weather_operational` y `fact_graph_centrality`
  - portada ejecutiva con regla explícita de contingencia aérea (`>= 24h` ahorro y `<= 18.000 EUR` inversión media)
  - envío de alertas por correo desde `Control Tower` con destinatario editable en la UI

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

## Funcionalidades del sistema

| Módulo | Descripción |
|---|---|
| Ingesta (KDD) | NiFi consume Open-Meteo, publica en Kafka `datos_crudos` y `datos_filtrados`, y deja evidencia exportable del flujo. |
| Procesamiento Spark | Spark genera staging en Hive, dimensiones maestras, enriquecimiento climático, facts operativas, alertas, análisis de grafos y comparación barco vs aéreo+camión. |
| Persistencia | Hive/HDFS para histórico, staging y curated. Cassandra para `vehicle_latest_state` de baja latencia. |
| Dashboard | Dashboard Streamlit con estado de servicios, mapa, alertas, tablas clave y diagramas KDD/UML. |
| Orquestación | Airflow con DAG principal `logistica_kdd_microbatch` y DAG mensual `logistica_kdd_monthly_retrain`. |
| Evidencias | Capturas, consultas, export NiFi, parquet en HDFS y tablas Hive/Cassandra para la defensa. |

## Responsabilidades de cada componente

| Componente | Responsabilidad única | Entrada | Salida |
|---|---|---|---|
| NiFi | Consumir API pública y publicar raw/filtered | Open-Meteo | Kafka `datos_crudos`, `datos_filtrados` |
| Kafka | Cola de mensajes | NiFi | Lectura por Spark |
| HDFS | Persistencia raw/staging/curated | NiFi / Spark | JSONL y parquet |
| Spark | Limpiar, enriquecer, calcular facts y grafos | Kafka + HDFS + Hive | Hive + HDFS + Cassandra |
| Hive | Capa SQL analítica | Spark | Tablas `stg_*`, `dim_*`, `fact_*` |
| Cassandra | Estado actual de baja latencia | Spark/loader | `vehicle_latest_state` |
| Airflow | Orquestación | Scripts y jobs | Ejecución programada |
| Dashboard | Exposición y consulta visual | Hive + Cassandra + estado Docker | UI web |

## Flujo de datos

```text
Open-Meteo -> NiFi -> Kafka (datos_crudos / datos_filtrados)
                         |
                         v
                      Spark
                         |
        -----------------------------------------
        |                    |                  |
        v                    v                  v
     Hive/HDFS         GraphFrames         Cassandra
(staging/dim/fact)   (risk/centrality)  (vehicle_latest_state)
                         |
                         v
                      Airflow
                         |
                         v
                     Dashboard
```

## Persistencia y tablas principales

| Tabla | Tipo | Uso |
|---|---|---|
| `logistica.stg_weather_open_meteo` | Hive staging | clima filtrado desde Kafka |
| `logistica.stg_ships` | Hive staging | posiciones de barcos y stock simulado |
| `logistica.dim_ports` | Hive dimensión | puertos |
| `logistica.dim_routes` | Hive dimensión | rutas |
| `logistica.dim_warehouse` | Hive dimensión | almacenes |
| `logistica.dim_ports_routes_weather` | Hive enriquecida | clima + ruta + puerto |
| `logistica.fact_weather_operational` | Hive fact | acción operativa final |
| `logistica.fact_air_recovery_options` | Hive fact | comparación barco vs aéreo+camión, ETA y coste total |
| `logistica.fact_route_risk` | Hive fact | riesgo por ruta |
| `logistica.fact_graph_centrality` | Hive fact | criticidad de nodos |
| `logistica.fact_alerts` | Hive fact | alertas operativas |
| `logistica.vehicle_latest_state` | Cassandra | último estado por vehículo |
| `logistica.dim_articles_valladolid` | Hive dimensión | stock detallado por artículo en Valladolid |
| `logistica.fact_customer_orders_douai` | Hive fact | pedidos del cliente de Douai por semana industrial |
| `logistica.fact_article_gantt` | Hive fact | planificación por artículo y modo logístico |

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

- **Origen principal**: puerto Shanghai (China).
- **Origen alternativo**: puerto Yokohama (Japón).
- **Destino**: Algeciras / Valencia / Barcelona.
- **Cliente final**: Valladolid -> clientes en Douai y Cleon.
- **Objetivo**: monitorizar barcos (GPS) y, si hay alertas (geopolítica o clima adverso), **avisar** para activar un plan de contingencia.

## Caso de uso extendido: recuperación aérea hasta Valladolid

El caso de uso principal se amplía con una capa de decisión logística orientada a contingencias. Cuando la ruta marítima entre Shanghai y los puertos españoles presenta riesgo operativo o amenaza el stock, el sistema compara:

- la llegada restante del barco según su posición GPS actual
- frente a una alternativa `aéreo + camión` hasta Valladolid

La comparación contempla:

- ETA estimada del barco (Estimated Time Arrival)
- tiempo restante del barco hasta puerto y almacén
- retraso meteorológico y portuario
- tiempo de vuelo hasta aeropuertos españoles
- tramo terrestre por camión hasta Valladolid
- coste total del envío aéreo + camión
- horas ganadas frente a la llegada marítima
- riesgo de ruptura de stock

Resultado persistido:

- `logistica.fact_air_recovery_options`

Recomendación final esperada:

- `MARITIMO` si la ruta sigue siendo viable y más barata
- `AEREO_CAMION` si la alternativa aérea evita rotura de stock y mejora el ETA con un coste justificable

El panel de barcos del dashboard también incorpora `ETA` por barco para visualizar de forma directa si la llegada prevista compromete el stock. Esa ETA se muestra en formato fecha/hora estimada de llegada, junto con la fecha de salida del puerto de origen y los días de viaje.

Además, la demo permite:

- seleccionar un barco concreto
- activar incidencias simuladas (`Huelga`, `Tormenta`, `Piratas`, `Problemas geopolíticos`, `Problemas técnicos`, `Huelga portuaria`)
- recalcular ETA y cobertura de stock
- lanzar una alerta si el suministro queda en `NO CUBRE`

## Interactividad del dashboard

El dashboard permite navegación y simulaciones en tiempo de demo:

- navegación por vistas desde el sidebar
- visor integrado de documentación técnica
- selección de barco concreto en las vistas operativas
- incidencias activables por barco
- visualización GPS de 10 barcos desde `Shanghai` y `Yokohama`

Con estos controles se recalculan o actualizan:

- ETA visible
- estado de cobertura
- horizonte de 10 semanas
- Gantt de cobertura
- propuesta de contingencia aérea

Ejemplo ya generado en tabla:

- `ship-001 | Algeciras | MARITIMO | ROTURA_INMINENTE | 411.5h barco | 25.8h aereo+camion | ahorro 385.7h | 17672.0 EUR`

## Nota operativa sobre Hive/Spark

El metastore Hive embebido del contenedor `spark` funciona correctamente, pero las ejecuciones `spark-submit` y las consultas `spark-sql` que usen Hive deben lanzarse **de forma secuencial**, nunca en paralelo. Si se solapan, Derby puede bloquear el metastore temporalmente.

## Pendiente para cierre completo de entrega

- [ ] Configurar credenciales SMTP en `.env`
- [ ] Verificar en Airflow la ejecucion visual de los dos DAGs finales
- [ ] Completar documento Word con capturas finales
