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
- Topics: `datos_crudos`, `alertas_globales`

### Detener
```bash
./stop.sh
```

## Stack tecnológico (rúbrica cumplida)

| Componente | Implementación |
|------------|----------------|
| Kafka | Streaming de datos en tiempo real |
| Spark Structured Streaming | Procesamiento streaming con ventanas de 5 min |
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
2. **Streaming**: Spark Structured Streaming procesa ventanas de 5 minutos
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
- [ ] Ejecutar `start.sh` y verificar todos los servicios
- [ ] Completar documento Word con capturas finales
