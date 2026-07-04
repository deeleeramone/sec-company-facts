#!/usr/bin/env bash
set -eu

DATA_DIR="${SEC_DATA_DIR:-/data}"
PARQUET_DIR="${SEC_PARQUET_DIR:-${DATA_DIR}/parquet}"
MANIFEST_URL="${PARQUET_MANIFEST_URL:-}"

manifest_value() {
    python -c "import json,sys;print(json.load(open(sys.argv[1])).get('data_version',''))" "$1" 2>/dev/null || echo ""
}

fetch_bundle() {
    local base tmp_manifest remote_ver local_ver staging
    base="${PARQUET_BASE_URL:-${MANIFEST_URL%/*}}"
    tmp_manifest="$(mktemp)"
    echo "[entrypoint] fetching manifest ${MANIFEST_URL}"
    if ! curl -fsSL --retry 5 --retry-delay 3 -o "${tmp_manifest}" "${MANIFEST_URL}"; then
        echo "[entrypoint] WARN: could not fetch manifest" >&2
        return 1
    fi
    remote_ver="$(manifest_value "${tmp_manifest}")"
    local_ver="$(manifest_value "${PARQUET_DIR}/manifest.json")"
    if [ -n "${local_ver}" ] && [ "${remote_ver}" = "${local_ver}" ]; then
        echo "[entrypoint] parquet already current (data_version=${local_ver}); skipping download"
        rm -f "${tmp_manifest}"
        return 0
    fi
    echo "[entrypoint] downloading parquet bundle (remote=${remote_ver:-?} local=${local_ver:-none})"
    staging="${DATA_DIR}/parquet.new"
    rm -rf "${staging}"
    mkdir -p "${staging}"
    python -c "import json,sys;m=json.load(open(sys.argv[1]));print('\n'.join(f for t in m['tables'].values() for f in t['files']))" "${tmp_manifest}" \
        | while IFS= read -r f; do
            [ -n "${f}" ] || continue
            echo "[entrypoint]   ${f}"
            curl -fsSL --retry 5 --retry-delay 3 -o "${staging}/${f}" "${base}/${f}"
        done
    cp "${tmp_manifest}" "${staging}/manifest.json"
    rm -f "${tmp_manifest}"
    rm -rf "${DATA_DIR}/parquet.old"
    if [ -d "${PARQUET_DIR}" ]; then
        mv "${PARQUET_DIR}" "${DATA_DIR}/parquet.old"
    fi
    mv "${staging}" "${PARQUET_DIR}"
    rm -rf "${DATA_DIR}/parquet.old"
    echo "[entrypoint] parquet bundle ready (data_version=${remote_ver})"
}

if [ -n "${MANIFEST_URL}" ]; then
    if ! fetch_bundle; then
        if [ ! -f "${PARQUET_DIR}/manifest.json" ]; then
            echo "[entrypoint] FATAL: no manifest and download failed" >&2
            exit 1
        fi
        echo "[entrypoint] WARN: download failed; serving existing parquet" >&2
    fi
fi

if [ ! -f "${PARQUET_DIR}/manifest.json" ]; then
    echo "[entrypoint] FATAL: ${PARQUET_DIR}/manifest.json not found." >&2
    echo "[entrypoint] Set PARQUET_MANIFEST_URL to download a bundle, or mount a parquet dir at ${PARQUET_DIR}." >&2
    exit 1
fi

export SEC_BACKEND=duckdb
export SEC_PARQUET_DIR="${PARQUET_DIR}"
echo "[entrypoint] serving with duckdb over ${PARQUET_DIR}"

uvicorn sec_app.server:app \
    --host "${WIDGETS_HOST:-0.0.0.0}" \
    --port "${WIDGETS_PORT:-8000}" \
    --workers "${WIDGETS_WORKERS:-1}"
