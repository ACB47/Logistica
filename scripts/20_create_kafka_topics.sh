#!/usr/bin/env bash
set -euo pipefail

# Crea los topics del proyecto.
#
# Uso:
#   bash scripts/20_create_kafka_topics.sh master:9092

BOOTSTRAP="${1:-master:9092}"
PARTITIONS="${PARTITIONS:-1}"
REPLICATION_FACTOR="${REPLICATION_FACTOR:-1}"
KAFKA_TOPICS="${KAFKA_HOME:-}/bin/kafka-topics.sh"
if [[ -z "${KAFKA_HOME:-}" ]]; then
  # fallback a PATH
  KAFKA_TOPICS="/opt/kafka/bin/kafka-topics.sh"
fi

TOPICS=(
  datos_crudos
  alertas_globales
  datos_filtrados
  datos_filtrados_ok
)

for topic in "${TOPICS[@]}"; do
  "${KAFKA_TOPICS}" --bootstrap-server "${BOOTSTRAP}" --create --if-not-exists \
    --topic "${topic}" \
    --partitions "${PARTITIONS}" \
    --replication-factor "${REPLICATION_FACTOR}"
done

echo
echo "Topics creados:"
"${KAFKA_TOPICS}" --bootstrap-server "${BOOTSTRAP}" --list
