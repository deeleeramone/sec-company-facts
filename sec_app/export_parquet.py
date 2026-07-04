from __future__ import annotations

import argparse
import binascii
import json
import os
import sys
import urllib.parse
import urllib.request
from datetime import date, datetime, timezone
from pathlib import Path

DEFAULT_OWNER = "deeleeramone"
DEFAULT_REPO = "sec-company-facts"
DEFAULT_BRANCH = "main"

CSV_BASE = "https://www.dolthub.com/csv"
API_BASE = "https://www.dolthub.com/api/v1alpha1"

BIG_TABLES = frozenset({"facts_enc", "standardized_statements_enc"})
SORT_KEY = "cik"
BLOB_PAGE = 250

TABLE_TYPES: dict[str, dict[str, str]] = {
    "cik_canonical": {"cik": "VARCHAR", "primary_cik": "VARCHAR"},
    "companies": {
        "cik": "VARCHAR",
        "entity_name": "VARCHAR",
        "source_mtime": "TIMESTAMP",
        "source_content_hash": "VARCHAR",
    },
    "entities": {"id": "BIGINT", "cik": "VARCHAR", "entity_name": "VARCHAR"},
    "exchange_rates": {
        "rate_date": "DATE",
        "from_currency": "VARCHAR",
        "to_currency": "VARCHAR",
        "rate": "DOUBLE",
    },
    "xbrl_tags": {
        "tag_id": "INTEGER",
        "namespace": "VARCHAR",
        "tag": "VARCHAR",
        "label": "VARCHAR",
        "description": "VARCHAR",
    },
    "accessions": {"accn_id": "INTEGER", "accn": "VARCHAR"},
    "sources": {"source_id": "INTEGER", "source": "VARCHAR"},
    "std_presentation": {
        "company_type": "VARCHAR",
        "statement": "VARCHAR",
        "tag": "VARCHAR",
        "label": "VARCHAR",
        "parent": "VARCHAR",
        "sequence": "INTEGER",
        "factor": "VARCHAR",
        "balance": "VARCHAR",
        "unit": "VARCHAR",
    },
    "cik_tags": {"cik": "VARCHAR", "tag_id": "INTEGER"},
    "facts_enc": {
        "id": "BIGINT",
        "cik": "VARCHAR",
        "tag_id": "INTEGER",
        "unit": "VARCHAR",
        "start": "DATE",
        "end": "DATE",
        "val": "DOUBLE",
        "val_text": "VARCHAR",
        "accn_id": "INTEGER",
        "fy": "INTEGER",
        "fp": "VARCHAR",
        "form": "VARCHAR",
        "filed": "DATE",
        "frame": "VARCHAR",
    },
    "funds": {"cik": "VARCHAR", "series_id": "VARCHAR", "class_id": "VARCHAR", "symbol": "VARCHAR"},
    "multi_cik_tickers": {"ticker": "VARCHAR", "cik": "VARCHAR", "priority": "INTEGER"},
    "primary_tickers": {"cik": "VARCHAR", "ticker": "VARCHAR", "name": "VARCHAR", "rank": "INTEGER"},
    "processed_ciks": {
        "cik": "VARCHAR",
        "has_balance": "BOOLEAN",
        "has_income": "BOOLEAN",
        "has_cash_flow": "BOOLEAN",
        "computed_at": "TIMESTAMP",
    },
    "standardized_statements_enc": {
        "cik": "VARCHAR",
        "statement": "VARCHAR",
        "period_ending": "DATE",
        "fiscal_year": "INTEGER",
        "fiscal_period": "VARCHAR",
        "calendar_year": "INTEGER",
        "calendar_period": "VARCHAR",
        "frequency": "VARCHAR",
        "tag": "VARCHAR",
        "val": "DOUBLE",
        "currency": "VARCHAR",
        "company_type": "VARCHAR",
        "source_id": "INTEGER",
    },
    "tickers": {
        "cik": "VARCHAR",
        "ticker": "VARCHAR",
        "name": "VARCHAR",
        "is_primary": "BOOLEAN",
        "rank": "INTEGER",
    },
    "cik_gics": {
        "cik": "VARCHAR",
        "sic4": "INTEGER",
        "sector": "VARCHAR",
        "industry": "VARCHAR",
        "sub_industry": "VARCHAR",
    },
}

SUBMISSIONS_TYPES = {"cik": "VARCHAR", "payload": "BLOB", "source_mtime": "TIMESTAMP"}

ALL_TABLES = list(TABLE_TYPES.keys()) + ["submissions"]


def _sql_quote(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _json_query(owner: str, repo: str, branch: str, sql: str) -> list[dict]:
    q = urllib.parse.urlencode({"q": sql})
    url = f"{API_BASE}/{owner}/{repo}/{branch}?{q}"
    req = urllib.request.Request(url, headers={"User-Agent": "sec-app-export"})
    with urllib.request.urlopen(req, timeout=300) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    status = payload.get("query_execution_status")
    if status not in ("Success", "RowLimit"):
        raise RuntimeError(f"DoltHub query failed: {payload.get('query_execution_message')} :: {sql[:200]}")
    return payload.get("rows", []) or []


def _data_version(owner: str, repo: str, branch: str) -> str:
    rows = _json_query(owner, repo, branch, "SELECT dolt_hashof_db() AS v")
    return str(rows[0]["v"]) if rows else "unknown"


def _columns_struct(types: dict[str, str]) -> str:
    return "{" + ", ".join(f"'{c}': '{t}'" for c, t in types.items()) + "}"


def _read_csv_expr(source: str, types: dict[str, str]) -> str:
    s = source.replace("'", "''")
    return (
        f"read_csv('{s}', header=true, columns={_columns_struct(types)}, "
        "nullstr='', quote='\"', escape='\"')"
    )


def _shard_index(path: Path) -> int:
    stem = path.stem.split("_")[-1]
    return int(stem) if stem.isdigit() else 0


def _copy_parquet(con, select_sql: str, target: Path, big: bool, shard_bytes: str) -> list[str]:
    if not big:
        con.execute(
            f"COPY ({select_sql}) TO '{str(target).replace(chr(39), chr(39) * 2)}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        return [target.name]

    shard_dir = target.parent / f"_{target.name}"
    con.execute("SET threads = 1")
    try:
        con.execute(
            f"COPY ({select_sql}) TO '{str(shard_dir).replace(chr(39), chr(39) * 2)}' "
            f"(FORMAT PARQUET, COMPRESSION ZSTD, FILE_SIZE_BYTES '{shard_bytes}', OVERWRITE_OR_IGNORE true)"
        )
    finally:
        con.execute("RESET threads")

    files: list[str] = []
    for i, shard in enumerate(sorted(shard_dir.glob("*.parquet"), key=_shard_index)):
        name = f"{target.name}-{i}.parquet"
        shard.rename(target.parent / name)
        files.append(name)
    shard_dir.rmdir()
    return files


def _parquet_count(con, out_dir: Path, files: list[str]) -> int:
    paths = ", ".join("'" + str(out_dir / f).replace("'", "''") + "'" for f in files)
    return int(con.execute(f"SELECT COUNT(*) FROM read_parquet([{paths}])").fetchone()[0])


def _bytes_of(out_dir: Path, files: list[str]) -> int:
    return sum((out_dir / f).stat().st_size for f in files)


def _export_csv_table(con, owner, repo, branch, table, out_dir, sort, shard_bytes):
    types = TABLE_TYPES[table]
    url = f"{CSV_BASE}/{owner}/{repo}/{branch}/{table}"
    big = table in BIG_TABLES
    order = f" ORDER BY {SORT_KEY}" if (big and sort) else ""
    select_sql = f"SELECT * FROM {_read_csv_expr(url, types)}{order}"
    target = (out_dir / table) if big else (out_dir / f"{table}.parquet")
    print(f"[export] streaming {table} csv -> parquet (big={big}, sort={sort and big}) ...", flush=True)
    return _copy_parquet(con, select_sql, target, big, shard_bytes)


def _coerce(value, duck_type: str):
    if value is None or value == "":
        return None
    t = duck_type.upper()
    if t in ("VARCHAR",):
        return str(value)
    if t in ("INTEGER", "BIGINT"):
        return int(float(value))
    if t == "DOUBLE":
        return float(value)
    if t == "BOOLEAN":
        return str(value) not in ("0", "false", "False", "")
    if t == "DATE":
        return date.fromisoformat(str(value)[:10])
    if t == "TIMESTAMP":
        return datetime.fromisoformat(str(value).replace("T", " ")[:19])
    return value


def _arrow_type(duck_type: str):
    import pyarrow as pa

    return {
        "VARCHAR": pa.string(),
        "INTEGER": pa.int32(),
        "BIGINT": pa.int64(),
        "DOUBLE": pa.float64(),
        "BOOLEAN": pa.bool_(),
        "DATE": pa.date32(),
        "TIMESTAMP": pa.timestamp("us"),
        "BLOB": pa.binary(),
    }[duck_type.upper()]


def _write_arrow(out_path: Path, columns: dict[str, str], data: dict[str, list]) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = pa.schema([(name, _arrow_type(t)) for name, t in columns.items()])
    table = pa.table({name: pa.array(data[name], type=_arrow_type(t)) for name, t in columns.items()}, schema=schema)
    pq.write_table(table, out_path, compression="zstd")


def _export_submissions(owner, repo, branch, out_dir, ciks) -> list[str]:
    print("[export] fetching submissions via JSON HEX (keyset) ...", flush=True)
    data: dict[str, list] = {"cik": [], "payload": [], "source_mtime": []}
    cik_filter = f"AND cik IN ({', '.join(_sql_quote(c) for c in ciks)}) " if ciks else ""
    last = ""
    total = 0
    while True:
        sql = (
            "SELECT cik, HEX(payload) AS payload_hex, source_mtime FROM submissions "
            f"WHERE cik > {_sql_quote(last)} {cik_filter}ORDER BY cik LIMIT {BLOB_PAGE}"
        )
        rows = _json_query(owner, repo, branch, sql)
        if not rows:
            break
        for r in rows:
            data["cik"].append(str(r["cik"]))
            h = r.get("payload_hex")
            data["payload"].append(binascii.unhexlify(h) if h else None)
            data["source_mtime"].append(_coerce(r.get("source_mtime"), "TIMESTAMP"))
        last = str(rows[-1]["cik"])
        total += len(rows)
        print(f"[export] submissions {total} rows ...", flush=True)
        if len(rows) < BLOB_PAGE:
            break
    _write_arrow(out_dir / "submissions.parquet", SUBMISSIONS_TYPES, data)
    return ["submissions.parquet"]


def _export_json_slice(owner, repo, branch, table, ciks, out_dir) -> list[str]:
    types = TABLE_TYPES[table]
    cols = ", ".join(types.keys())
    has_id = "id" in types
    data: dict[str, list] = {c: [] for c in types}
    for cik in ciks:
        q = _sql_quote(cik)
        last = 0
        offset = 0
        while True:
            if has_id:
                sql = f"SELECT {cols} FROM {table} WHERE cik = {q} AND id > {last} ORDER BY id LIMIT 1000"
            else:
                sql = f"SELECT {cols} FROM {table} WHERE cik = {q} LIMIT 1000 OFFSET {offset}"
            rows = _json_query(owner, repo, branch, sql)
            if not rows:
                break
            for r in rows:
                for c, t in types.items():
                    data[c].append(_coerce(r.get(c), t))
            if has_id:
                last = rows[-1]["id"]
            else:
                offset += 1000
            if len(rows) < 1000:
                break
    _write_arrow(out_dir / f"{table}.parquet", types, data)
    return [f"{table}.parquet"]


def run_export(owner, repo, branch, out, tables, sort, shard_bytes, ciks):
    import duckdb

    out_dir = Path(out)
    out_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute("PRAGMA enable_progress_bar=false")
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    tmp = os.environ.get("DUCKDB_TEMP_DIR")
    if tmp:
        con.execute(f"SET temp_directory = '{tmp}'")

    version = _data_version(owner, repo, branch)
    manifest_tables: dict[str, dict] = {}

    for table in tables:
        if table == "submissions":
            files = _export_submissions(owner, repo, branch, out_dir, ciks)
        elif ciks and table in BIG_TABLES:
            print(f"[export] slice {table} for {len(ciks)} ciks via JSON ...", flush=True)
            files = _export_json_slice(owner, repo, branch, table, ciks, out_dir)
        else:
            files = _export_csv_table(con, owner, repo, branch, table, out_dir, sort, shard_bytes)
        rows = _parquet_count(con, out_dir, files)
        manifest_tables[table] = {"files": files, "rows": rows, "bytes": _bytes_of(out_dir, files)}
        print(f"[export] {table}: {rows} rows, {len(files)} file(s)", flush=True)

    manifest = {
        "format_version": 1,
        "data_version": version,
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "source": {"owner": owner, "repo": repo, "branch": branch},
        "sliced_ciks": sorted(ciks) if ciks else None,
        "tables": manifest_tables,
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
    print(f"[export] wrote manifest.json (data_version={version})", flush=True)
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m sec_app.export_parquet",
        description="Export the DoltHub SEC repo to parquet via the REST API (no clone).",
    )
    parser.add_argument("--owner", default=DEFAULT_OWNER)
    parser.add_argument("--repo", default=DEFAULT_REPO)
    parser.add_argument("--branch", default=DEFAULT_BRANCH)
    parser.add_argument("--out", required=True, help="Output directory for parquet + manifest.json")
    parser.add_argument("--tables", default="", help="Comma-separated subset (default: all)")
    parser.add_argument("--no-sort", action="store_true", help="Skip cik sort of big tables (lighter export)")
    parser.add_argument("--shard-bytes", default="1500MB", help="Target parquet shard size for big tables")
    parser.add_argument("--ciks", default="", help="Comma-separated CIK slice for big tables (test exports)")
    args = parser.parse_args(argv)

    tables = [t.strip() for t in args.tables.split(",") if t.strip()] if args.tables else list(ALL_TABLES)
    unknown = [t for t in tables if t not in ALL_TABLES]
    if unknown:
        parser.error(f"unknown tables: {unknown}")
    ciks = [c.strip().zfill(10) for c in args.ciks.split(",") if c.strip()]

    run_export(
        owner=args.owner,
        repo=args.repo,
        branch=args.branch,
        out=args.out,
        tables=tables,
        sort=not args.no_sort,
        shard_bytes=args.shard_bytes,
        ciks=ciks,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
