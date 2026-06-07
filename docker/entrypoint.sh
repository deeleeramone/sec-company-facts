#!/usr/bin/env bash
set -eu

DATA_DIR="${SEC_DATA_DIR:-/data}"
DOLT_SQL_HOST="${DOLT_SQL_HOST:-127.0.0.1}"
DOLT_SQL_PORT="${DOLT_SQL_PORT:-3306}"
DOLT_SQL_DB="${DOLT_SQL_DB:-sec_company_facts}"
DOLT_REMOTE="${DOLT_REMOTE:-deeleeramone/sec-company-facts}"
REPO_DIR="${DATA_DIR}/${DOLT_SQL_DB}"

# Self-contained data bootstrap. The image may already carry a baked clone, a
# named volume may persist a prior clone, or this may be a cold start with
# nothing present — handle all three. The public DoltHub repo clones anonymously.
if [ ! -d "${REPO_DIR}/.dolt" ]; then
    echo "[entrypoint] no Dolt repository at ${REPO_DIR}; cloning ${DOLT_REMOTE} ..."
    mkdir -p "${DATA_DIR}"
    if ! dolt clone "${DOLT_REMOTE}" "${REPO_DIR}"; then
        echo "[entrypoint] FATAL: dolt clone of ${DOLT_REMOTE} failed" >&2
        exit 1
    fi
else
    # Catch up to the remote before serving (server isn't running yet, so a CLI
    # pull is safe). Tolerate offline starts — serve the existing data if pull fails.
    echo "[entrypoint] updating ${REPO_DIR} from ${DOLT_REMOTE} ..."
    ( cd "${REPO_DIR}" && dolt pull origin main ) \
        || echo "[entrypoint] WARN: startup pull failed; serving existing data" >&2
fi

echo "[entrypoint] serving ${REPO_DIR} as database '${DOLT_SQL_DB}' on ${DOLT_SQL_HOST}:${DOLT_SQL_PORT} (read-only)"

# data_dir defaults to the working dir; config supplies host/port/read_only.
cd "${DATA_DIR}"
dolt sql-server --config /app/dolt-sql-server.yaml &
DOLT_PID=$!

trap 'kill -TERM "${DOLT_PID}" 2>/dev/null || true' TERM INT

echo "[entrypoint] waiting for dolt sql-server ..."
i=0
until python -c "import socket,sys; s=socket.socket(); s.settimeout(1); sys.exit(0 if s.connect_ex(('${DOLT_SQL_HOST}', ${DOLT_SQL_PORT}))==0 else 1)"; do
    i=$((i + 1))
    if [ "$i" -ge 120 ]; then
        echo "[entrypoint] FATAL: dolt sql-server did not open ${DOLT_SQL_HOST}:${DOLT_SQL_PORT}" >&2
        kill -TERM "${DOLT_PID}" 2>/dev/null || true
        exit 1
    fi
    if ! kill -0 "${DOLT_PID}" 2>/dev/null; then
        echo "[entrypoint] FATAL: dolt sql-server exited during startup" >&2
        exit 1
    fi
    sleep 1
done
echo "[entrypoint] dolt sql-server ready"

uvicorn sec_app.server:app \
    --host "${WIDGETS_HOST:-0.0.0.0}" \
    --port "${WIDGETS_PORT:-8000}" \
    --workers "${WIDGETS_WORKERS:-1}" &
APP_PID=$!

wait -n "${DOLT_PID}" "${APP_PID}"
EXIT=$?
echo "[entrypoint] a managed process exited (rc=${EXIT}); shutting down"
kill -TERM "${DOLT_PID}" "${APP_PID}" 2>/dev/null || true
exit "${EXIT}"
