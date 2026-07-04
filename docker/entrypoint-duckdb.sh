#!/usr/bin/env bash
set -euo pipefail

DATA_DIR="${SEC_DATA_DIR:-/data}"
PARQUET_DIR="${SEC_PARQUET_DIR:-${DATA_DIR}/parquet}"
MANIFEST_URL="${PARQUET_MANIFEST_URL:-}"

manifest_value() {
    python -c "import json,sys;print(json.load(open(sys.argv[1])).get('data_version',''))" "$1" 2>/dev/null || echo ""
}

# Downloads the bundle to a staging dir and atomically swaps it in ONLY if every
# manifest-listed file materialized. Returns non-zero (leaving the existing volume
# untouched) on any failure, so the caller can fall back to serving existing data
# and a later restart retries — never a half-swapped, version-committed bundle.
fetch_bundle() {
    local base tmp_manifest files_list remote_ver local_ver staging f
    base="${PARQUET_BASE_URL:-${MANIFEST_URL%/*}}"
    tmp_manifest="$(mktemp)"
    files_list="$(mktemp)"
    echo "[entrypoint] fetching manifest ${MANIFEST_URL}"
    if ! curl -fsSL --retry 5 --retry-delay 3 -o "${tmp_manifest}" "${MANIFEST_URL}"; then
        echo "[entrypoint] WARN: could not fetch manifest" >&2
        rm -f "${tmp_manifest}" "${files_list}"; return 1
    fi
    remote_ver="$(manifest_value "${tmp_manifest}")"
    local_ver="$(manifest_value "${PARQUET_DIR}/manifest.json")"
    if [ -n "${local_ver}" ] && [ "${remote_ver}" = "${local_ver}" ]; then
        echo "[entrypoint] parquet already current (data_version=${local_ver}); skipping download"
        rm -f "${tmp_manifest}" "${files_list}"; return 0
    fi
    echo "[entrypoint] downloading parquet bundle (remote=${remote_ver:-?} local=${local_ver:-none})"
    if ! python -c "import json,sys;m=json.load(open(sys.argv[1]));print('\n'.join(f for t in m['tables'].values() for f in t['files']))" "${tmp_manifest}" > "${files_list}"; then
        echo "[entrypoint] WARN: could not parse manifest file list" >&2
        rm -f "${tmp_manifest}" "${files_list}"; return 1
    fi
    if [ ! -s "${files_list}" ]; then
        echo "[entrypoint] WARN: manifest lists no files" >&2
        rm -f "${tmp_manifest}" "${files_list}"; return 1
    fi
    staging="${DATA_DIR}/parquet.new"
    rm -rf "${staging}"
    mkdir -p "${staging}"
    while IFS= read -r f; do
        [ -n "${f}" ] || continue
        echo "[entrypoint]   ${f}"
        if ! curl -fsSL --retry 5 --retry-delay 3 -o "${staging}/${f}" "${base}/${f}"; then
            echo "[entrypoint] WARN: download failed for ${f}" >&2
            rm -rf "${staging}"; rm -f "${tmp_manifest}" "${files_list}"; return 1
        fi
    done < "${files_list}"
    while IFS= read -r f; do
        [ -n "${f}" ] || continue
        if [ ! -s "${staging}/${f}" ]; then
            echo "[entrypoint] WARN: ${f} missing/empty after download" >&2
            rm -rf "${staging}"; rm -f "${tmp_manifest}" "${files_list}"; return 1
        fi
    done < "${files_list}"
    cp "${tmp_manifest}" "${staging}/manifest.json"
    rm -f "${tmp_manifest}" "${files_list}"
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
