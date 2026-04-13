#!/bin/bash
set -e

BOOTSTRAP="${1:-master:9092}"

echo "=== Verificacion de Topicos Kafka ==="
echo "Bootstrap: $BOOTSTRAP"
echo ""

echo "[1] Listando topicos..."
docker exec -it master kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list

echo ""
echo "[2] Detalle de topic 'datos_crudos'..."
docker exec -it master kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --topic datos_crudos --describe

echo ""
echo "[3] Detalle de topic 'alertas_globales'..."
docker exec -it master kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --topic alertas_globales --describe

echo ""
echo "[4] Detalle de topic 'datos_filtrados'..."
docker exec -it master kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --topic datos_filtrados --describe

echo ""
echo "[5] Consumer groups activos..."
docker exec -it master kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP" --list

echo ""
echo "[6] Ultimos mensajes de datos_crudos (5)..."
docker exec -it master kafka-console-consumer.sh --bootstrap-server "$BOOTSTRAP" --topic datos_crudos --from-beginning --max-messages 5 --timeout-ms 5000 | head -30 || true

echo ""
echo "=== Verificacion completada ==="