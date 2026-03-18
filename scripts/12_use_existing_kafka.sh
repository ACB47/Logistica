#!/usr/bin/env bash
set -euo pipefail

# Activa Kafka ya descargado (sin instalar en /opt).
#
# Uso:
#   source scripts/12_use_existing_kafka.sh
#
# Nota: usa "source" para que exporte variables en tu shell actual.

export KAFKA_HOME="/home/hadoop/Descargas/kafka_2.13-4.1.1"
export PATH="$KAFKA_HOME/bin:$PATH"

echo "KAFKA_HOME=$KAFKA_HOME"
echo "kafka-topics.sh version:"
kafka-topics.sh --version

