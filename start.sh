#!/bin/bash
set -e

COMPOSE_CMD=(docker compose)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "=========================================="
echo "PROYECTO LOGISTICA - INICIO"
echo "=========================================="

if [ -f .env ]; then
    echo "Cargando configuracion de .env..."
    source .env
fi

echo ""
echo "Elige el modo de ejecucion:"
echo ""
echo "  1) Completo (recomendado para evaluacion)"
echo "     - HDFS + Hive"
echo "     - Kafka (KRaft)"
echo "     - Spark Master + Worker"
echo "     - Cassandra"
echo "     - Airflow"
echo "     - NiFi"
echo ""
echo "  2) Simple (menos recursos)"
echo "     - MinIO (取代 HDFS)"
echo "     - Kafka (KRaft)"
echo "     - Spark"
echo "     - Cassandra"
echo "     - Airflow"
echo "     - NiFi"
echo ""
echo "  3) Dashboard solo (ultraligero)"
echo "     - Sin Docker"
echo "     - Streamlit local usando bundle existente"
echo "     - Recomendado para rediseño UI sin bloquear el equipo"
echo ""
read -p "Opcion [1/2/3]: " opcion

case $opcion in
    3)
        echo ""
        echo "Iniciando modo DASHBOARD SOLO..."
        echo "No se levantan contenedores Docker."
        echo "Se usará el bundle local del dashboard si ya existe."
        echo ""
        bash scripts/67_run_dashboard.sh
        exit 0
        ;;
    2)
        echo ""
        echo "Iniciando modo SIMPLE..."
        "${COMPOSE_CMD[@]}" -f docker-compose.simple.yml up -d kafka kafka-init postgres minio spark cassandra nifi airflow
        echo ""
        echo "=========================================="
        echo "SERVICIOS LEVANTADOS (SIMPLE)"
        echo "=========================================="
        echo ""
        echo "Accesos web:"
        echo "  - Spark Master:  http://localhost:8080"
        echo "  - Airflow:       http://localhost:8085 (admin/admin)"
        echo "  - NiFi:          https://localhost:8443"
        echo "  - MinIO Console: http://localhost:9001 (minioadmin/minioadmin)"
        echo ""
        echo "Puertos:"
        echo "  - Kafka:     localhost:9092"
        echo "  - Cassandra:  localhost:9042"
        echo ""
        COMPOSE_FILE="docker-compose.simple.yml"
        ;;
    *)
        echo ""
        echo "Iniciando modo COMPLETO..."
        "${COMPOSE_CMD[@]}" up -d kafka kafka-init namenode datanode cassandra cassandra-init spark nifi postgres airflow-webserver
        echo ""
        echo "=========================================="
        echo "SERVICIOS LEVANTADOS (COMPLETO)"
        echo "=========================================="
        echo ""
        echo "Accesos web:"
        echo "  - HDFS Namenode: http://localhost:9870"
        echo "  - Spark Master:  http://localhost:8080"
        echo "  - Airflow:       http://localhost:8085 (admin/admin)"
        echo "  - NiFi:          https://localhost:8443"
        echo ""
        echo "Puertos:"
        echo "  - Kafka:       localhost:9092"
        echo "  - Cassandra:   localhost:9042"
        echo "  - Hive Server: localhost:10000"
        echo "  - Hive Meta:   localhost:9083"
        echo ""
        COMPOSE_FILE="docker-compose.yml"
        ;;
esac

echo "=========================================="
echo "Comandos utiles:"
echo "  docker compose -f ${COMPOSE_FILE} logs -f [servicio]"
echo "  docker compose -f ${COMPOSE_FILE} down"
echo "=========================================="
