#!/usr/bin/env bash
set -euo pipefail

# Instala Kafka 3.9.1 en modo KRaft (sin ZooKeeper) en /opt/kafka_3.9.1
# Requiere permisos de sudo para escribir en /opt.
#
# Uso:
#   bash scripts/10_install_kafka_kraft.sh

KAFKA_VERSION="3.9.1"
SCALA_VERSION="2.13"
TGZ="kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
URL_PRIMARY="https://dlcdn.apache.org/kafka/${KAFKA_VERSION}/${TGZ}"
URL_FALLBACK="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${TGZ}"

echo "Descargando ${TGZ}"
cd /tmp
if [[ ! -f "${TGZ}" ]]; then
  echo "- Intento 1: ${URL_PRIMARY}"
  if ! curl -fL -o "${TGZ}" "${URL_PRIMARY}"; then
    echo "- Intento 2 (fallback): ${URL_FALLBACK}"
    curl -fL -o "${TGZ}" "${URL_FALLBACK}"
  fi
fi

echo "Instalando en /opt/kafka_${KAFKA_VERSION}"
sudo mkdir -p "/opt/kafka_${KAFKA_VERSION}"
sudo tar -xzf "${TGZ}" -C /opt
sudo rm -rf "/opt/kafka_${KAFKA_VERSION}"
sudo mv "/opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION}" "/opt/kafka_${KAFKA_VERSION}"
sudo chown -R "$(id -u):$(id -g)" "/opt/kafka_${KAFKA_VERSION}"

echo
echo "Añade Kafka a PATH (por ejemplo en ~/.bashrc):"
echo "  export KAFKA_HOME=/opt/kafka_${KAFKA_VERSION}"
echo "  export PATH=\\$KAFKA_HOME/bin:\\$PATH"

echo
echo "Verificación:"
echo "  /opt/kafka_${KAFKA_VERSION}/bin/kafka-topics.sh --version"

