#!/usr/bin/env bash
set -euo pipefail

# Crea los topics del proyecto.
#
# Uso:
#   bash scripts/20_create_kafka_topics.sh master:9092

BOOTSTRAP="${1:-master:9092}"
KAFKA_TOPICS="${KAFKA_HOME:-}/bin/kafka-topics.sh"
if [[ -z "${KAFKA_HOME:-}" ]]; then
  # fallback a PATH
  KAFKA_TOPICS="kafka-topics.sh"
fi

"${KAFKA_TOPICS}" --bootstrap-server "${BOOTSTRAP}" --create \
  --topic datos_crudos \
  --partitions 3 \
  --replication-factor 2

"${KAFKA_TOPICS}" --bootstrap-server "${BOOTSTRAP}" --create \
  --topic alertas_globales \
  --partitions 3 \
  --replication-factor 2

echo
echo "Topics creados:"
"${KAFKA_TOPICS}" --bootstrap-server "${BOOTSTRAP}" --list

