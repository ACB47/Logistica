#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_CMD=(docker compose)

ACTION="${1:-status}"
STACK_MODE="${2:-simple}"
PROFILE="${3:-core}"
REMOVE_VOLUMES="${REMOVE_VOLUMES:-0}"

if [[ ! -d "${REPO_ROOT}/scripts" ]]; then
  echo "No se pudo localizar la raiz del repositorio."
  exit 1
fi

case "${STACK_MODE}" in
  simple)
    COMPOSE_FILE="docker-compose.simple.yml"
    AIRFLOW_SERVICE="airflow"
    STORAGE_SERVICES=(minio)
    ;;
  full)
    COMPOSE_FILE="docker-compose.yml"
    AIRFLOW_SERVICE="airflow-webserver"
    STORAGE_SERVICES=(namenode datanode)
    ;;
  *)
    echo "Modo invalido: ${STACK_MODE}"
    echo "Usa: simple | full"
    exit 1
    ;;
esac

CORE_SERVICES=(kafka postgres "${AIRFLOW_SERVICE}" nifi)
PROCESSING_SERVICES=(spark cassandra)
INIT_SERVICES=(kafka-init)

join_services() {
  local IFS=' '
  printf '%s' "$*"
}

profile_services() {
  case "${PROFILE}" in
    core)
      printf '%s\n' "${CORE_SERVICES[@]}"
      ;;
    data)
      printf '%s\n' "${CORE_SERVICES[@]}" "${STORAGE_SERVICES[@]}"
      ;;
    processing)
      printf '%s\n' "${CORE_SERVICES[@]}" "${PROCESSING_SERVICES[@]}"
      ;;
    demo)
      printf '%s\n' "${CORE_SERVICES[@]}" "${PROCESSING_SERVICES[@]}" zeppelin "${STORAGE_SERVICES[@]}"
      ;;
    full)
      printf '%s\n' "${CORE_SERVICES[@]}" "${PROCESSING_SERVICES[@]}" zeppelin "${STORAGE_SERVICES[@]}" "${INIT_SERVICES[@]}"
      ;;
    *)
      echo "Perfil invalido: ${PROFILE}" >&2
      echo "Usa: core | data | processing | demo | full" >&2
      exit 1
      ;;
  esac
}

SERVICES=()
while IFS= read -r service; do
  [[ -n "${service}" ]] && SERVICES+=("${service}")
done < <(profile_services)

run_compose() {
  "${COMPOSE_CMD[@]}" -f "${COMPOSE_FILE}" "$@"
}

start_stack() {
  echo "Repositorio: ${REPO_ROOT}"
  echo "Compose: ${COMPOSE_FILE}"
  echo "Perfil: ${PROFILE}"

  if [[ "${PROFILE}" == "full" ]]; then
    echo "Levantando stack completo..."
    run_compose up -d "${SERVICES[@]}"
    return
  fi

  echo "Fase 1/3: servicios base"
  run_compose up -d "${CORE_SERVICES[@]}"

  if [[ "${PROFILE}" == "data" || "${PROFILE}" == "demo" || "${PROFILE}" == "full" ]]; then
    echo "Fase 2/3: almacenamiento"
    run_compose up -d "${STORAGE_SERVICES[@]}"
  fi

  if [[ "${PROFILE}" == "processing" || "${PROFILE}" == "demo" || "${PROFILE}" == "full" ]]; then
    echo "Fase 3/3: procesamiento"
    run_compose up -d "${PROCESSING_SERVICES[@]}"
  fi

  if [[ "${PROFILE}" == "demo" ]]; then
    echo "Fase extra: visualizacion"
    run_compose up -d zeppelin
  fi

  echo "Inicializando topics Kafka"
  run_compose up -d "${INIT_SERVICES[@]}"
}

stop_stack() {
  echo "Repositorio: ${REPO_ROOT}"
  echo "Compose: ${COMPOSE_FILE}"
  echo "Perfil: ${PROFILE}"

  if [[ "${PROFILE}" == "full" ]]; then
    echo "Parando stack completo..."
    if [[ "${REMOVE_VOLUMES}" == "1" ]]; then
      run_compose down -v
    else
      run_compose down
    fi
    return
  fi

  echo "Parando servicios: $(join_services "${SERVICES[@]}")"
  run_compose stop "${SERVICES[@]}"

  if [[ "${REMOVE_VOLUMES}" == "1" ]]; then
    echo "REMOVE_VOLUMES=1 ignorado en parada parcial; usa perfil full si quieres down -v."
  fi
}

show_status() {
  echo "Repositorio: ${REPO_ROOT}"
  echo "Compose: ${COMPOSE_FILE}"
  run_compose ps
}

show_usage() {
  cat <<'EOF'
Uso:
  bash scripts/60_manage_docker_stack.sh <accion> [simple|full] [core|data|processing|demo|full]

Acciones:
  start    Arranca el perfil indicado por fases.
  stop     Para el perfil indicado sin borrar datos.
  status   Muestra estado de servicios del compose.

Perfiles:
  core        Kafka + Postgres + Airflow + NiFi
  data        core + almacenamiento (MinIO o HDFS)
  processing  core + Spark + Cassandra
  demo        data + processing + Zeppelin
  full        todos los servicios del compose

Ejemplos:
  bash scripts/60_manage_docker_stack.sh start simple core
  bash scripts/60_manage_docker_stack.sh start simple demo
  bash scripts/60_manage_docker_stack.sh start full data
  bash scripts/60_manage_docker_stack.sh stop simple demo
  REMOVE_VOLUMES=1 bash scripts/60_manage_docker_stack.sh stop full full
EOF
}

cd "${REPO_ROOT}"

case "${ACTION}" in
  start)
    start_stack
    ;;
  stop)
    stop_stack
    ;;
  status)
    show_status
    ;;
  help|-h|--help)
    show_usage
    ;;
  *)
    echo "Accion invalida: ${ACTION}"
    show_usage
    exit 1
    ;;
esac
