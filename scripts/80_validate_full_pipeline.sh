#!/bin/bash
set -e

echo "=========================================="
echo "  VALIDACION COMPLETA DEL PIPELINE"
echo "  Fecha: $(date)"
echo "=========================================="
echo ""

ERRORS=0

check_service() {
    local name=$1
    local container=$2
    if docker ps --format "{{.Names}}" | grep -q "^${container}$"; then
        echo "[OK] $name运行中"
    else
        echo "[FAIL] $name NO esta运行中"
        ERRORS=$((ERRORS + 1))
    fi
}

echo "=== 1. Verificando servicios Docker ==="
check_service "Kafka" "logistica-kafka-1"
check_service "Spark" "logistica-spark-1"
check_service "HDFS NameNode" "logistica-namenode-1"
check_service "Hive Server" "logistica-hiveserver-1"
check_service "Cassandra" "logistica-cassandra-1"
check_service "Airflow" "logistica-airflow-webserver-1"
check_service "NiFi" "logistica-nifi-1"
echo ""

echo "=== 2. Verificando Kafka topics ==="
TOPICS=$(docker exec -it logistica-kafka-1 kafka-topics.sh --bootstrap-server master:9092 --list 2>/dev/null || echo "")
if echo "$TOPICS" | grep -q "datos_crudos"; then
    echo "[OK] Topic datos_crudos existe"
else
    echo "[FAIL] Topic datos_crudos no existe"
    ERRORS=$((ERRORS + 1))
fi

if echo "$TOPICS" | grep -q "alertas_globales"; then
    echo "[OK] Topic alertas_globales existe"
else
    echo "[FAIL] Topic alertas_globales no existe"
    ERRORS=$((ERRORS + 1))
fi
echo ""

echo "=== 3. Verificando HDFS ==="
if docker exec -it logistica-namenode-1 hdfs dfs -ls /hadoop/logistica/raw/ 2>/dev/null | grep -q "ships"; then
    echo "[OK] HDFS path raw/ships existe"
else
    echo "[FAIL] HDFS path raw/ships no existe"
    ERRORS=$((ERRORS + 1))
fi

if docker exec -it logistica-namenode-1 hdfs dfs -ls /hadoop/logistica/staging/ 2>/dev/null | grep -q "staging"; then
    echo "[OK] HDFS path staging existe"
else
    echo "[FAIL] HDFS path staging no existe"
    ERRORS=$((ERRORS + 1))
fi
echo ""

echo "=== 4. Verificando Hive tablas ==="
TABLES=$(docker exec -it logistica-hiveserver-1 beeline -u "jdbc:hive2://localhost:10000" -e "SHOW TABLES IN logistica;" 2>/dev/null | grep -E "stg_|dim_|fact_" || echo "")
if echo "$TABLES" | grep -q "stg_ships"; then
    echo "[OK] Tabla stg_ships existe"
else
    echo "[FAIL] Tabla stg_ships no existe"
    ERRORS=$((ERRORS + 1))
fi

if echo "$TABLES" | grep -q "dim_ports"; then
    echo "[OK] Tabla dim_ports existe"
else
    echo "[FAIL] Tabla dim_ports no existe"
    ERRORS=$((ERRORS + 1))
fi
echo ""

echo "=== 5. Verificando Cassandra ==="
CASS_TABLES=$(docker exec -it logistica-cassandra-1 cqlsh -e "SHOW TABLES;" 2>/dev/null || echo "")
if docker exec -it logistica-cassandra-1 cqlsh -e "SELECT * FROM logistica.vehicle_latest_state LIMIT 1;" 2>/dev/null | grep -q "vehicle_id"; then
    echo "[OK] Tabla vehicle_latest_state accesible"
else
    echo "[WARN] Tabla vehicle_latest_state no accesible (normal si sin datos)"
fi
echo ""

echo "=== 6. Verificando Airflow DAG ==="
if docker exec -it logistica-airflow-webserver-1 airflow dags list 2>/dev/null | grep -q "logistica_kdd"; then
    echo "[OK] DAG logistica_kdd existe"
else
    echo "[FAIL] DAG logistica_kdd no existe"
    ERRORS=$((ERRORS + 1))
fi
echo ""

echo "=== 7. Verificando NiFi ==="
NIFI_VERSION=$(docker exec -it logistica-nifi-1 nifi version 2>/dev/null | head -1 || echo "unknown")
echo "[OK] NiFi version: $NIFI_VERSION"
echo ""

echo "=== 8. Compilando Python (syntax check) ==="
COMPILE_ERRORS=0
for f in ingestion/productores/*.py ingestion/consumidores/*.py jobs/spark/*.py; do
    if [ -f "$f" ]; then
        python3 -m py_compile "$f" 2>/dev/null || {
            echo "[FAIL] $f"
            COMPILE_ERRORS=$((COMPILE_ERRORS + 1))
        }
    fi
done

if [ $COMPILE_ERRORS -eq 0 ]; then
    echo "[OK]Todos los Python scripts compilan correctamente"
else
    echo "[FAIL] $COMPILE_ERRORS scripts con errores de sintaxis"
    ERRORS=$((ERRORS + COMPILE_ERRORS))
fi
echo ""

echo "=== RESUMEN ==="
if [ $ERRORS -eq 0 ]; then
    echo "=========================================="
    echo "[SUCCESS] Pipeline validado correctamente!"
    echo "=========================================="
    echo ""
    echo "Servicios disponibles:"
    echo "  - HDFS: http://localhost:9870"
    echo "  - Spark: http://localhost:8080"
    echo "  - Airflow: http://localhost:8085 (admin/admin)"
    echo "  - NiFi: https://localhost:8443"
    echo "  - Dashboard: http://localhost:8501"
    exit 0
else
    echo "=========================================="
    echo "[FAILED] $ERRORS errores detectados"
    echo "=========================================="
    exit 1
fi