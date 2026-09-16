#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: $0 [--site SITE] [--prefix PREFIX] [--restart-runtime]"
  echo "Copies isolated modules to every Frappe runtime and runs idempotent setup."
}

site="frontend"
prefix="frappe_docker"
restart_runtime=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --site)
      site="${2:?--site requires a value}"
      shift 2
      ;;
    --prefix)
      prefix="${2:?--prefix requires a value}"
      shift 2
      ;;
    --restart-runtime)
      restart_runtime=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      usage >&2
      exit 2
      ;;
  esac
done

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
api_source="${repo_dir}/server/api_agent_sync.py"
setup_source="${repo_dir}/server/agent_sync_setup.py"
app_target="/home/frappe/frappe-bench/apps/db_connector/db_connector"
containers=(
  "${prefix}-backend-1"
  "${prefix}-queue-long-1"
  "${prefix}-queue-short-1"
  "${prefix}-scheduler-1"
  "${prefix}-frontend-1"
)

python3 -m py_compile "$api_source" "$setup_source"

for container in "${containers[@]}"; do
  if ! docker inspect "$container" >/dev/null 2>&1; then
    echo "Required container not found: $container" >&2
    exit 1
  fi
  docker cp "$api_source" "${container}:${app_target}/api_agent_sync.py"
  docker cp "$setup_source" "${container}:${app_target}/agent_sync_setup.py"
  docker exec "$container" python -m py_compile \
    "${app_target}/api_agent_sync.py" \
    "${app_target}/agent_sync_setup.py"
done

backend="${prefix}-backend-1"
docker exec "$backend" bash -lc \
  "cd /home/frappe/frappe-bench && bench --site '$site' execute db_connector.agent_sync_setup.install"

if [[ "$restart_runtime" -eq 1 ]]; then
  docker restart "${containers[@]}"
fi

echo "Installed with the global switch unchanged. Configure CCD Agent Sync Settings in Desk."
