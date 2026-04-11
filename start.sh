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
echo "     - Zeppelin"
echo "     - Airflow"
echo "     - NiFi"
echo ""
echo "  2) Simple (menos recursos)"
echo "     - MinIO (取代 HDFS)"
echo "     - Kafka (KRaft)"
echo "     - Spark"
echo "     - Cassandra"
echo "     - Zeppelin"
echo "     - Airflow"
echo "     - NiFi"
echo ""
read -p "Opcion [1/2]: " opcion

case $opcion in
    2)
        echo ""
        echo "Iniciando modo SIMPLE..."
        "${COMPOSE_CMD[@]}" -f docker-compose.simple.yml up -d
        echo ""
        echo "=========================================="
        echo "SERVICIOS LEVANTADOS (SIMPLE)"
        echo "=========================================="
        echo ""
        echo "Accesos web:"
        echo "  - Spark Master:  http://localhost:8080"
        echo "  - Zeppelin:       http://localhost:8081"
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
        "${COMPOSE_CMD[@]}" up -d
        echo ""
        echo "=========================================="
        echo "SERVICIOS LEVANTADOS (COMPLETO)"
        echo "=========================================="
        echo ""
        echo "Accesos web:"
        echo "  - HDFS Namenode: http://localhost:9870"
        echo "  - Spark Master:  http://localhost:8080"
        echo "  - Zeppelin:       http://localhost:8081"
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
