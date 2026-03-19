#!/usr/bin/env bash
set -euo pipefail

# Despliega Kafka KRaft (3 nodos) en master + slave01 + slave02.
#
# Uso (en master):
#   bash scripts/40_setup_kraft_cluster_3vms.sh
#
# Requiere:
# - que la carpeta Kafka exista en /home/hadoop/Descargas/kafka_2.13-4.1.1 en cada VM
# - que puedas SSH sin prompts (o con prompts te pedirá contraseña)
#
# Este script:
# - genera cluster.id en master
# - genera server.properties en cada VM (con node.id y advertised.listeners)
# - hace kafka-storage format en cada VM
# - arranca kafka-server-start en background en cada VM

KAFKA_HOME="${KAFKA_HOME:-/home/hadoop/Descargas/kafka_2.13-4.1.1}"
USER_NAME="${USER_NAME:-hadoop}"
HOSTS=("master:1" "slave01:2" "slave02:3")
PROJECT_HOME="${PROJECT_HOME:-/home/hadoop/PROYECTOLOGISTICA}"

CONFIG_DIR="${KAFKA_HOME}/config/kraft"
CONFIG_FILE_NAME="server.properties"
PID_DIR="/tmp/kafka_pids"
LOCAL_TMP_DIR="/tmp/kafka_kraft_configs_$$"

mkdir -p "${PID_DIR}"
mkdir -p "${LOCAL_TMP_DIR}"

for h in "${HOSTS[@]}"; do
  host="${h%%:*}"
  echo "Comprobando ${host}..."
  ssh "${USER_NAME}@${host}" "test -x \"${KAFKA_HOME}/bin/kafka-server-start.sh\" && echo OK" || {
    echo "ERROR: falta ${KAFKA_HOME} en ${host}"
    exit 1
  }
done

echo "Generando cluster.id en master..."
CLUSTER_ID="$(ssh "${USER_NAME}@master" "export KAFKA_HOME='${KAFKA_HOME}' && ${KAFKA_HOME}/bin/kafka-storage.sh random-uuid" | tr -d '\r\n')"
echo "CLUSTER_ID=${CLUSTER_ID}"

for item in "${HOSTS[@]}"; do
  host="${item%%:*}"
  node_id="${item##*:}"
  advertised_host="${host}"

  echo "Generando config local para ${host} (node.id=${node_id})..."
  local_config="${LOCAL_TMP_DIR}/server_${host}.properties"

  sed \
    -e "s/^node.id=.*/node.id=${node_id}/" \
    -e "s/<HOSTNAME>/${advertised_host}/g" \
    "${PROJECT_HOME}/kafka/kraft/server.properties.template" > "${local_config}"

  echo "Copiando config a ${host}:${CONFIG_DIR}/${CONFIG_FILE_NAME}"
  ssh "${USER_NAME}@${host}" "mkdir -p \"${CONFIG_DIR}\""
  scp "${local_config}" "${USER_NAME}@${host}:${CONFIG_DIR}/${CONFIG_FILE_NAME}"

  echo "Formateando KRaft storage en ${host}..."
  ssh "${USER_NAME}@${host}" "export KAFKA_HOME='${KAFKA_HOME}'; mkdir -p /home/hadoop/kafka_logs; rm -rf /home/hadoop/kafka_logs/*; '${KAFKA_HOME}/bin/kafka-storage.sh' format -t '${CLUSTER_ID}' -c '${CONFIG_DIR}/${CONFIG_FILE_NAME}'"
done

echo "Arrancando Kafka en background..."
for item in "${HOSTS[@]}"; do
  host="${item%%:*}"
  echo "Arrancando broker+controller en ${host}..."
  ssh "${USER_NAME}@${host}" "export KAFKA_HOME='${KAFKA_HOME}'; mkdir -p '${PID_DIR}'; nohup '${KAFKA_HOME}/bin/kafka-server-start.sh' '${CONFIG_DIR}/${CONFIG_FILE_NAME}' > '${PID_DIR}/kafka_${host}.log' 2>&1 &"
done

echo "Esperando a que Kafka responda..."
for i in {1..30}; do
  if ssh "${USER_NAME}@master" "${KAFKA_HOME}/bin/kafka-topics.sh --bootstrap-server master:9092 --list" >/dev/null 2>&1; then
    echo "Kafka OK"
    ssh "${USER_NAME}@master" "${KAFKA_HOME}/bin/kafka-topics.sh --bootstrap-server master:9092 --list"
    exit 0
  fi
  echo " - intento ${i}/30 (aún no)"
  sleep 2
done

echo "ERROR: Kafka no respondió en el tiempo esperado."
echo "Revisa logs: /tmp/kafka_pids/kafka_<host>.log"
exit 1

