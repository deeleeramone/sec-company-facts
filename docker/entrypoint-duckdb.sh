#!/usr/bin/env bash
set -euo pipefail

DATA_DIR="${SEC_DATA_DIR:-/data}"
PARQUET_DIR="${SEC_PARQUET_DIR:-${DATA_DIR}/parquet}"
MANIFEST_URL="${PARQUET_MANIFEST_URL:-}"

manifest_value() {
    python -c "import json,sys;print(json.load(open(sys.argv[1])).get('data_version',''))" "$1" 2>/dev/null || echo ""
}

fetch_bundle() {
    local base tmp_manifest plan remote_ver local_ver staging act name reused=0 fetched=0
    base="${PARQUET_BASE_URL:-${MANIFEST_URL%/*}}"
    tmp_manifest="$(mktemp)"
    plan="$(mktemp)"
    echo "[entrypoint] fetching manifest ${MANIFEST_URL}"
    if ! curl -fsSL --retry 5 --retry-delay 3 -o "${tmp_manifest}" "${MANIFEST_URL}"; then
        echo "[entrypoint] WARN: could not fetch manifest" >&2
        rm -f "${tmp_manifest}" "${plan}"; return 1
    fi
    remote_ver="$(manifest_value "${tmp_manifest}")"
    local_ver="$(manifest_value "${PARQUET_DIR}/manifest.json")"
    if [ -n "${local_ver}" ] && [ "${remote_ver}" = "${local_ver}" ]; then
        echo "[entrypoint] parquet already current (data_version=${local_ver}); skipping"
        rm -f "${tmp_manifest}" "${plan}"; return 0
    fi
    echo "[entrypoint] syncing parquet bundle (remote=${remote_ver:-?} local=${local_ver:-none})"
    if ! python - "${tmp_manifest}" "${PARQUET_DIR}/manifest.json" > "${plan}" <<'PYEOF'
import json, sys
rem = json.load(open(sys.argv[1]))
try:
    loc = json.load(open(sys.argv[2]))
except Exception:
    loc = {}
have = {}
for t in (loc.get("tables") or {}).values():
    for f in t.get("files", []):
        have[f["name"]] = f.get("sha256")
for t in rem.get("tables", {}).values():
    for f in t.get("files", []):
        act = "REUSE" if have.get(f["name"]) == f["sha256"] else "FETCH"
        print(act + "\t" + f["name"])
PYEOF
    then
        echo "[entrypoint] WARN: could not parse manifest" >&2
        rm -f "${tmp_manifest}" "${plan}"; return 1
    fi
    if [ ! -s "${plan}" ]; then
        echo "[entrypoint] WARN: manifest lists no files" >&2
        rm -f "${tmp_manifest}" "${plan}"; return 1
    fi
    staging="${DATA_DIR}/parquet.new"
    rm -rf "${staging}"
    mkdir -p "${staging}"
    while IFS=$'\t' read -r act name; do
        [ -n "${name}" ] || continue
        if [ "${act}" = REUSE ] && [ -f "${PARQUET_DIR}/${name}" ]; then
            ln "${PARQUET_DIR}/${name}" "${staging}/${name}" 2>/dev/null \
                || cp "${PARQUET_DIR}/${name}" "${staging}/${name}"
            reused=$((reused + 1))
        elif ! curl -fsSL --retry 5 --retry-delay 3 -o "${staging}/${name}" "${base}/${name}"; then
            echo "[entrypoint] WARN: download failed for ${name}" >&2
            rm -rf "${staging}"; rm -f "${tmp_manifest}" "${plan}"; return 1
        else
            fetched=$((fetched + 1))
        fi
    done < "${plan}"
    while IFS=$'\t' read -r act name; do
        [ -n "${name}" ] || continue
        if [ ! -s "${staging}/${name}" ]; then
            echo "[entrypoint] WARN: ${name} missing/empty after sync" >&2
            rm -rf "${staging}"; rm -f "${tmp_manifest}" "${plan}"; return 1
        fi
    done < "${plan}"
    cp "${tmp_manifest}" "${staging}/manifest.json"
    rm -f "${tmp_manifest}" "${plan}"
    rm -rf "${DATA_DIR}/parquet.old"
    if [ -d "${PARQUET_DIR}" ]; then
        mv "${PARQUET_DIR}" "${DATA_DIR}/parquet.old"
    fi
    mv "${staging}" "${PARQUET_DIR}"
    rm -rf "${DATA_DIR}/parquet.old"
    echo "[entrypoint] parquet ready (data_version=${remote_ver}; reused=${reused} fetched=${fetched})"
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
