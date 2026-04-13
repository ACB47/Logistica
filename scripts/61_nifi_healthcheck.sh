#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [[ -f "${REPO_ROOT}/.env" ]]; then
  set -a
  source "${REPO_ROOT}/.env"
  set +a
fi

NIFI_BASE_URL="${NIFI_BASE_URL:-https://localhost:8443}"
NIFI_USERNAME="${NIFI_USERNAME:-admin}"
NIFI_PASSWORD="${NIFI_PASSWORD:-CHANGE_ME_NIFI_PASSWORD}"

echo "Comprobando NiFi en ${NIFI_BASE_URL}"

for _ in {1..30}; do
  if curl -kfsS -o /tmp/nifi_headers.$$ -D /tmp/nifi_headers.$$ "${NIFI_BASE_URL}" >/dev/null 2>&1; then
    sed -n '1,10p' /tmp/nifi_headers.$$
    break
  fi
  sleep 2
done

if [[ ! -f /tmp/nifi_headers.$$ ]]; then
  echo "NiFi no responde todavia en ${NIFI_BASE_URL}"
  exit 1
fi

TOKEN=""
for _ in {1..30}; do
  TOKEN="$(curl -kfsS -X POST "${NIFI_BASE_URL}/nifi-api/access/token" \
    -H 'Content-Type: application/x-www-form-urlencoded; charset=UTF-8' \
    --data-urlencode "username=${NIFI_USERNAME}" \
    --data-urlencode "password=${NIFI_PASSWORD}" || true)"
  [[ -n "${TOKEN}" ]] && break
  sleep 2
done

if [[ -z "${TOKEN}" ]]; then
  echo "No se pudo obtener token de acceso de NiFi"
  exit 1
fi

echo "Token recibido correctamente"

ABOUT_JSON="$(curl -kfsS "${NIFI_BASE_URL}/nifi-api/flow/about" \
  -H "Authorization: Bearer ${TOKEN}")"

python3 -c 'import json,sys; data=json.loads(sys.argv[1]); about=data.get("about", {}); print("NiFi name:", about.get("title", "desconocido")); print("NiFi version:", about.get("version", "desconocida"))' "${ABOUT_JSON}"

rm -f /tmp/nifi_headers.$$
