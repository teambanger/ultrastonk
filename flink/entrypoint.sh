#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == "standalone-job" && -n "${FLINK_RESTORE_PATH:-}" ]]; then
  shift
  exec /docker-entrypoint.sh standalone-job --fromSavepoint "$FLINK_RESTORE_PATH" "$@"
fi
exec /docker-entrypoint.sh "$@"
