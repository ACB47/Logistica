#!/bin/bash
set -e

COMPOSE_CMD=(docker compose)

echo "=========================================="
echo "PROYECTO LOGISTICA - PARADA"
echo "=========================================="
echo ""
echo "Que modo quieres detener?"
echo "  1) Completo (docker-compose.yml)"
echo "  2) Simple (docker-compose.simple.yml)"
read -p "Opcion [1/2]: " opcion

case $opcion in
    2)
        COMPOSE_FILE="docker-compose.simple.yml"
        ;;
    *)
        COMPOSE_FILE="docker-compose.yml"
        ;;
esac

echo ""
echo "Deteniendo servicios..."
"${COMPOSE_CMD[@]}" -f "$COMPOSE_FILE" down

echo ""
read -p "Desea eliminar los volumenes de datos? (s/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Ss]$ ]]; then
    "${COMPOSE_CMD[@]}" -f "$COMPOSE_FILE" down -v
    echo "Volumenes eliminados."
fi

echo ""
echo "Servicios detenidos."
