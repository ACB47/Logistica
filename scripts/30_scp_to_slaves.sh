#!/usr/bin/env bash
set -euo pipefail

# Copia desde master a slave01 y slave02.
#
# Uso (en master):
#   bash scripts/30_scp_to_slaves.sh
#
# Requisitos:
# - Resolver nombres: master, slave01, slave02 (DNS o /etc/hosts)
# - SSH funcionando a los slaves como el mismo usuario (por defecto: hadoop)
# - En los slaves existir /home/hadoop y permisos de escritura

USER_NAME="${USER_NAME:-hadoop}"
SLAVES=("slave01" "slave02")

# Qué copiar
SRC_PROJECT="/home/${USER_NAME}/PROYECTOLOGISTICA"
SRC_KAFKA="/home/${USER_NAME}/Descargas/kafka_2.13-4.1.1"

# Dónde copiar en los slaves
DEST_HOME="/home/${USER_NAME}"

echo "Verificando orígenes locales..."
for p in "${SRC_PROJECT}" "${SRC_KAFKA}"; do
  if [[ ! -e "$p" ]]; then
    echo "ERROR: no existe $p"
    exit 1
  fi
done

for host in "${SLAVES[@]}"; do
  echo
  echo "== Copiando a ${USER_NAME}@${host} =="

  echo "- Preparando carpetas destino (limpieza previa para evitar permisos antiguos)..."
  ssh "${USER_NAME}@${host}" "rm -rf \"${DEST_HOME}/PROYECTOLOGISTICA\" && mkdir -p \"${DEST_HOME}/Descargas\""

  echo "- Copiando proyecto (sin .git) vía tar+ssh (más robusto)..."
  tar --exclude=".git" -czf - -C "${DEST_HOME}" "PROYECTOLOGISTICA" | ssh "${USER_NAME}@${host}" "tar -xzf - -C \"${DEST_HOME}\""

  echo "- Copiando Kafka vía tar+ssh..."
  ssh "${USER_NAME}@${host}" "rm -rf \"${DEST_HOME}/Descargas/kafka_2.13-4.1.1\""
  tar -czf - -C "/home/${USER_NAME}/Descargas" "kafka_2.13-4.1.1" | ssh "${USER_NAME}@${host}" "tar -xzf - -C \"${DEST_HOME}/Descargas\""

  echo "- OK en ${host}"
done

echo
echo "Comprobación rápida (en cada slave):"
echo "  ls -la /home/${USER_NAME}/PROYECTOLOGISTICA"
echo "  /home/${USER_NAME}/Descargas/kafka_2.13-4.1.1/bin/kafka-topics.sh --version"

