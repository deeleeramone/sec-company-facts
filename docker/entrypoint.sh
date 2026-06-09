#!/usr/bin/env bash
set -eu

DATA_DIR="${SEC_DATA_DIR:-/data}"
DOLT_SQL_HOST="${DOLT_SQL_HOST:-127.0.0.1}"
DOLT_SQL_PORT="${DOLT_SQL_PORT:-3306}"
DOLT_SQL_DB="${DOLT_SQL_DB:-sec_company_facts}"
DOLT_REMOTE="${DOLT_REMOTE:-deeleeramone/sec-company-facts}"
# Shallow clone depth. A serving replica only needs current state, so depth 1
# downloads just the latest commit's data — far less to stream/buffer/index,
# which keeps peak RAM (and time) low on small cloud hosts. Set to empty for a
# full-history clone. GOMEMLIMIT/GOGC (set in the image) additionally cap the Go
# heap during the clone.
DOLT_CLONE_DEPTH="${DOLT_CLONE_DEPTH:-1}"
REPO_DIR="${DATA_DIR}/${DOLT_SQL_DB}"

# Ensure Dolt has a commit identity for the merge commit a `dolt pull` creates.
# Baked into the image's global config too; this re-asserts it at runtime so the
# pull works even when the container runs as a different user/home. Generic and
# anonymous by design — never a real user's identity.
if ! dolt config --global --get user.name >/dev/null 2>&1; then
    dolt config --global --add user.name  "${DOLT_USER_NAME:-sec-app container}" >/dev/null 2>&1 || true
fi
if ! dolt config --global --get user.email >/dev/null 2>&1; then
    dolt config --global --add user.email "${DOLT_USER_EMAIL:-sec-app@localhost}" >/dev/null 2>&1 || true
fi

# Self-contained data bootstrap. The image may already carry a baked clone, a
# named volume may persist a prior clone, or this may be a cold start with
# nothing present — handle all three. The public DoltHub repo clones anonymously.
if [ ! -d "${REPO_DIR}/.dolt" ]; then
    DEPTH_ARGS=""
    if [ -n "${DOLT_CLONE_DEPTH}" ]; then
        DEPTH_ARGS="--depth ${DOLT_CLONE_DEPTH}"
        echo "[entrypoint] cloning ${DOLT_REMOTE} (shallow, depth=${DOLT_CLONE_DEPTH}) ..."
    else
        echo "[entrypoint] cloning ${DOLT_REMOTE} (full history) ..."
    fi
    mkdir -p "${DATA_DIR}"
    # shellcheck disable=SC2086 -- DEPTH_ARGS is intentionally word-split (flag + value or empty)
    if ! dolt clone ${DEPTH_ARGS} "${DOLT_REMOTE}" "${REPO_DIR}"; then
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
