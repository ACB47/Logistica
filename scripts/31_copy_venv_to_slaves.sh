#!/usr/bin/env bash
set -euo pipefail

# Copia únicamente el venv de ingesta al mismo path en las slaves.
#
# Uso (en master):
#   bash scripts/31_copy_venv_to_slaves.sh

USER_NAME="${USER_NAME:-hadoop}"
SLAVES=("slave01" "slave02")
SRC_VENV="/home/${USER_NAME}/PROYECTOLOGISTICA/.venv-ingesta"
DEST_VENV_DIR="/home/${USER_NAME}/PROYECTOLOGISTICA"

if [[ ! -d "${SRC_VENV}" ]]; then
  echo "ERROR: no existe ${SRC_VENV} en master"
  exit 1
fi

for host in "${SLAVES[@]}"; do
  echo
  echo "== Copiando venv a ${host} =="
  ssh "${USER_NAME}@${host}" "mkdir -p \"${DEST_VENV_DIR}\""
  tar -czf - -C "/home/${USER_NAME}/PROYECTOLOGISTICA" ".venv-ingesta" | ssh "${USER_NAME}@${host}" "tar -xzf - -C \"${DEST_VENV_DIR}\""
done

echo
echo "Verificación rápida:"
for host in "${SLAVES[@]}"; do
  ssh "${USER_NAME}@${host}" "test -d \"${DEST_VENV_DIR}/.venv-ingesta\" && echo OK_${host} || echo MISSING_${host}"
done

