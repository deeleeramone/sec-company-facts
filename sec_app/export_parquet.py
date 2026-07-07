from __future__ import annotations

import argparse
import binascii
import hashlib
import json
import os
import shutil
import sys
import threading
import time
import urllib.error
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
BLOB_PAGE = 20
NUM_BUCKETS = 64

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
    "submissions": {"cik": "VARCHAR", "payload": "BLOB", "source_mtime": "TIMESTAMP"},
}

ALL_TABLES = list(TABLE_TYPES.keys())


def _duck_columns(table: str) -> dict[str, str]:
    return TABLE_TYPES[table]


def _sql_quote(value) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _order_clause(table: str, sort: bool, types: dict[str, str]) -> str:
    if table in BIG_TABLES and sort and SORT_KEY in types:
        return f' ORDER BY "{SORT_KEY}"'
    return ""



def _copy_parquet(con, select_sql: str, target: Path, big: bool, num_buckets: str) -> list[str]:
    if not big:
        con.execute(
            f"COPY ({select_sql}) TO '{str(target).replace(chr(39), chr(39) * 2)}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        return [target.name]

    part_dir = target.parent / f"_{target.name}_parts"
    if part_dir.exists():
        shutil.rmtree(part_dir)
    for stale in target.parent.glob(f"{target.name}-b*.parquet"):
        stale.unlink()
    q = str(part_dir).replace(chr(39), chr(39) * 2)
    con.execute(f"SET partitioned_write_max_open_files = {int(num_buckets)}")
    con.execute("SET threads = 1")
    try:
        con.execute(
            f"COPY (SELECT *, (CAST(cik AS BIGINT) % {int(num_buckets)}) AS _bkt FROM ({select_sql})) "
            f"TO '{q}' (FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY (_bkt), OVERWRITE_OR_IGNORE true)"
        )
    finally:
        con.execute("RESET threads")
        con.execute("RESET partitioned_write_max_open_files")
    files: list[str] = []
    for sub in part_dir.glob("_bkt=*"):
        b = int(sub.name.split("=")[1])
        for j, part in enumerate(sorted(sub.glob("*.parquet"))):
            name = f"{target.name}-b{b:05d}" + (f"-{j}" if j else "") + ".parquet"
            part.rename(target.parent / name)
            files.append(name)
    shutil.rmtree(part_dir)
    return sorted(files)


def _write_table(
    con, table: str, relation: str, out_dir: Path, sort: bool, num_buckets: str, scratch_dir: Path
) -> list[str]:
    types = _duck_columns(table)
    cols = ", ".join(f'CAST("{c}" AS {t}) AS "{c}"' for c, t in types.items())
    big = table in BIG_TABLES

    if not (big and sort):
        target = (out_dir / table) if big else (out_dir / f"{table}.parquet")
        return _copy_parquet(con, f"SELECT {cols} FROM {relation}", target, big, num_buckets)

    raw = scratch_dir / f"_{table}_raw.parquet"
    raw_q = str(raw).replace("'", "''")
    con.execute(f"COPY (SELECT {cols} FROM {relation}) TO '{raw_q}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    try:
        sorted_sql = f"SELECT * FROM read_parquet('{raw_q}') ORDER BY \"{SORT_KEY}\""
        return _copy_parquet(con, sorted_sql, out_dir / table, True, num_buckets)
    finally:
        raw.unlink(missing_ok=True)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _parquet_count(con, out_dir: Path, files: list[str]) -> int:
    paths = ", ".join("'" + str(out_dir / f).replace("'", "''") + "'" for f in files)
    return int(con.execute(f"SELECT COUNT(*) FROM read_parquet([{paths}])").fetchone()[0])


def _bytes_of(out_dir: Path, files: list[str]) -> int:
    return sum((out_dir / f).stat().st_size for f in files)


def _pymysql_connect(server: str, user: str, password: str):
    import pymysql  # pylint: disable=import-outside-toplevel

    host, _, rest = server.partition(":")
    port, _, db = rest.partition("/")
    if not host or not port or not db:
        raise SystemExit(f"--server must be host:port/database (got {server!r})")
    return pymysql.connect(
        host=host,
        port=int(port),
        user=user,
        password=password or "",
        database=db,
        charset="utf8mb4",
        connect_timeout=60,
        read_timeout=3600,
    )


def _server_data_version(conn) -> str:
    try:
        cur = conn.cursor()
        cur.execute("SELECT dolt_hashof_db()")
        row = cur.fetchone()
        cur.close()
        return str(row[0]) if row and row[0] is not None else "unknown"
    except Exception:
        return "unknown"


STREAM_BATCH = 50000


def _stream_to_parquet(conn, table: str, types: dict[str, str], where: str, dest: str) -> int:
    import pymysql.cursors  # pylint: disable=import-outside-toplevel
    import pyarrow as pa
    import pyarrow.parquet as pq

    names = list(types.keys())
    tps = list(types.values())
    schema = pa.schema([(n, _arrow_type(t)) for n, t in types.items()])
    cols_sql = ", ".join(f"`{c}`" for c in names)
    cur = conn.cursor(pymysql.cursors.SSCursor)
    cur.execute(f"SELECT {cols_sql} FROM `{table}`{where}")
    writer = pq.ParquetWriter(dest, schema, compression="zstd")
    total = 0
    try:
        while True:
            rows = cur.fetchmany(STREAM_BATCH)
            if not rows:
                break
            arrays = []
            for k, t in enumerate(tps):
                vals = [r[k] for r in rows]
                if t == "BOOLEAN":
                    vals = [None if v is None else bool(v) for v in vals]
                arrays.append(pa.array(vals, type=_arrow_type(t)))
            writer.write_table(pa.Table.from_arrays(arrays, names=names))
            total += len(rows)
    finally:
        writer.close()
        cur.close()
    return total


def _export_server_table(conn, con, table, out_dir, sort, num_buckets, ciks, scratch_dir) -> list[str]:
    types = _duck_columns(table)
    where = ""
    if ciks and SORT_KEY in types:
        where = " WHERE cik IN (" + ", ".join(_sql_quote(c) for c in ciks) + ")"
    big = table in BIG_TABLES
    print(f"[export] {table} <- server (stream){' + sort' if big and sort else ''} ...", flush=True)
    if not big:
        dest = out_dir / f"{table}.parquet"
        _stream_to_parquet(conn, table, types, where, str(dest))
        return [f"{table}.parquet"]
    raw = scratch_dir / f"_{table}_raw.parquet"
    _stream_to_parquet(conn, table, types, where, str(raw))
    raw_q = str(raw).replace("'", "''")
    order = f' ORDER BY "{SORT_KEY}"' if sort else ""
    try:
        return _copy_parquet(con, f"SELECT * FROM read_parquet('{raw_q}'){order}", out_dir / table, True, num_buckets)
    finally:
        raw.unlink(missing_ok=True)


def _json_query(owner: str, repo: str, branch: str, sql: str, attempts: int = 4) -> list[dict]:
    q = urllib.parse.urlencode({"q": sql})
    url = f"{API_BASE}/{owner}/{repo}/{branch}?{q}"
    last_err: Exception | None = None
    for i in range(attempts):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "sec-app-export"})
            with urllib.request.urlopen(req, timeout=300) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
            status = payload.get("query_execution_status")
            if status in ("Success", "RowLimit"):
                return payload.get("rows", []) or []
            msg = str(payload.get("query_execution_message") or "")
            last_err = RuntimeError(f"DoltHub query failed: {msg} :: {sql[:200]}")
            if "deadline" not in msg and "timeout" not in msg.lower():
                raise last_err
        except (urllib.error.URLError, TimeoutError, ConnectionError, json.JSONDecodeError) as err:
            last_err = err
        print(f"[export] json query attempt {i + 1}/{attempts} failed; retrying", flush=True)
    raise last_err if last_err else RuntimeError("json query failed")


def _rest_data_version(owner: str, repo: str, branch: str) -> str:
    rows = _json_query(owner, repo, branch, "SELECT dolt_hashof_db() AS v")
    return str(rows[0]["v"]) if rows else "unknown"


def _rest_count(owner: str, repo: str, branch: str, table: str) -> int:
    rows = _json_query(owner, repo, branch, f"SELECT COUNT(*) AS n FROM {table}")
    return int(rows[0]["n"])


def _stream_csv_to_parquet(con, url, types, target: Path, big: bool, num_buckets: str, expected_rows: int) -> list[str]:
    attempts = 6
    last_err: Exception | None = None
    for attempt in range(attempts):
        r, w = os.pipe()
        err: dict = {}

        def _pump(w=w, err=err):
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "sec-app-export"})
                with urllib.request.urlopen(req, timeout=3600) as resp:
                    while True:
                        chunk = resp.read(1 << 20)
                        if not chunk:
                            break
                        os.write(w, chunk)
            except Exception as exc:  # noqa: BLE001
                err["e"] = exc
            finally:
                os.close(w)

        thread = threading.Thread(target=_pump)
        thread.start()
        src = _read_csv_expr(f"/proc/self/fd/{r}", types)
        try:
            files = _copy_parquet(con, f"SELECT * FROM {src}", target, big, num_buckets)
            thread.join()
            os.close(r)
            if err:
                raise err["e"]
            got = _parquet_count(con, target.parent, files)
            if got != expected_rows:
                raise RuntimeError(f"row count mismatch: got {got}, expected {expected_rows}")
            return files
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            thread.join()
            try:
                os.close(r)
            except OSError:
                pass
            print(f"[export] stream attempt {attempt + 1}/{attempts} failed for {url}: {exc}", flush=True)
            if attempt + 1 < attempts:
                delay = min(300, 15 * 2 ** attempt)
                print(f"[export] backing off {delay}s (DoltHub likely rate-limited) before retry", flush=True)
                time.sleep(delay)
    raise RuntimeError(f"failed to stream {url} after {attempts} attempts: {last_err}")


def _read_csv_expr(csv_path: str, types: dict[str, str]) -> str:
    cols = "{" + ", ".join(f"'{c}': '{t}'" for c, t in types.items()) + "}"
    p = csv_path.replace("'", "''")
    return f"read_csv('{p}', header=true, columns={cols}, nullstr='', quote='\"', escape='\"')"


def _export_csv_table(con, owner, repo, branch, table, out_dir, sort, num_buckets) -> list[str]:
    url = f"{CSV_BASE}/{owner}/{repo}/{branch}/{table}"
    big = table in BIG_TABLES
    target = (out_dir / table) if big else (out_dir / f"{table}.parquet")
    expected = _rest_count(owner, repo, branch, table)
    print(f"[export] {table} <- REST stream ({expected} rows) ...", flush=True)
    return _stream_csv_to_parquet(con, url, TABLE_TYPES[table], target, big, num_buckets, expected)


def _coerce(value, duck_type: str):
    if value is None or value == "":
        return None
    t = duck_type.upper()
    if t == "VARCHAR":
        return str(value)
    if t in ("INTEGER", "BIGINT"):
        return int(float(value))
    if t == "DOUBLE":
        return float(value)
    if t == "BOOLEAN":
        return str(value) not in ("0", "false", "False")
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


def _write_arrow_bucketed(out_dir: Path, table: str, columns: dict[str, str], data: dict[str, list], num_buckets: int) -> list[str]:
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = pa.schema([(n, _arrow_type(t)) for n, t in columns.items()])
    by_bucket: dict[int, list[int]] = {}
    for i, cik in enumerate(data["cik"]):
        by_bucket.setdefault(int(cik) % num_buckets, []).append(i)
    files: list[str] = []
    for b in sorted(by_bucket):
        idx = by_bucket[b]
        arrays = {n: pa.array([data[n][i] for i in idx], type=_arrow_type(t)) for n, t in columns.items()}
        name = f"{table}-b{b:05d}.parquet"
        pq.write_table(pa.table(arrays, schema=schema), out_dir / name, compression="zstd")
        files.append(name)
    return files


def _export_submissions_rest(owner, repo, branch, out_dir, ciks, num_buckets) -> list[str]:
    print("[export] submissions <- JSON HEX (keyset) ...", flush=True)
    types = TABLE_TYPES["submissions"]
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
        if len(rows) < BLOB_PAGE:
            break
    print(f"[export] submissions: {total} rows", flush=True)
    return _write_arrow_bucketed(out_dir, "submissions", types, data, num_buckets)


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


def run_export(
    *,
    source,
    owner,
    repo,
    branch,
    server,
    server_user,
    server_password,
    out,
    tables,
    sort,
    num_buckets,
    ciks,
    tmp_dir,
):
    import duckdb

    out_dir = Path(out)
    out_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("PRAGMA enable_progress_bar=false")
    spill = os.environ.get("DUCKDB_TEMP_DIR") or tmp_dir
    if spill:
        Path(spill).mkdir(parents=True, exist_ok=True)
        con.execute(f"SET temp_directory = '{str(spill).replace(chr(39), chr(39) * 2)}'")
    mem = os.environ.get("DUCKDB_MEMORY_LIMIT")
    if mem:
        con.execute(f"SET memory_limit = '{mem}'")

    conn = None
    scratch = None
    if source == "server":
        scratch = Path(spill) if spill else (out_dir / "_scratch")
        scratch.mkdir(parents=True, exist_ok=True)
        conn = _pymysql_connect(server, server_user, server_password)
        version = _server_data_version(conn)
    else:
        version = _rest_data_version(owner, repo, branch)

    manifest_tables: dict[str, dict] = {}
    for table in tables:
        if source == "server":
            files = _export_server_table(conn, con, table, out_dir, sort, num_buckets, ciks, scratch)
        elif table == "submissions":
            files = _export_submissions_rest(owner, repo, branch, out_dir, ciks, num_buckets)
        elif ciks and table in BIG_TABLES:
            print(f"[export] slice {table} for {len(ciks)} ciks via JSON ...", flush=True)
            files = _export_json_slice(owner, repo, branch, table, ciks, out_dir)
        else:
            files = _export_csv_table(con, owner, repo, branch, table, out_dir, sort, num_buckets)
        rows = _parquet_count(con, out_dir, files)
        entries = [{"name": f, "sha256": _sha256(out_dir / f), "bytes": (out_dir / f).stat().st_size} for f in files]
        manifest_tables[table] = {"files": entries, "rows": rows}
        print(f"[export] {table}: {rows} rows, {len(files)} file(s)", flush=True)

    if conn is not None:
        conn.close()

    manifest = {
        "data_version": version,
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "source": {"mode": source, "owner": owner, "repo": repo, "branch": branch, "server": server},
        "sliced_ciks": sorted(ciks) if ciks else None,
        "tables": manifest_tables,
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
    print(f"[export] wrote manifest.json (source={source} data_version={version})", flush=True)
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m sec_app.export_parquet",
        description="Export the SEC dataset to parquet for the DuckDB serving image.",
    )
    parser.add_argument(
        "--source",
        choices=["rest", "server"],
        default="rest",
        help="rest: DoltHub REST API (no clone/server). server: a running Dolt/MySQL sql-server.",
    )
    parser.add_argument("--owner", default=DEFAULT_OWNER, help="[rest] DoltHub owner")
    parser.add_argument("--repo", default=DEFAULT_REPO, help="[rest] DoltHub repo")
    parser.add_argument("--branch", default=DEFAULT_BRANCH, help="[rest] branch")
    parser.add_argument("--server", default="", help="[server] host:port/database")
    parser.add_argument("--server-user", default="root")
    parser.add_argument("--server-password", default="")
    parser.add_argument("--out", required=True, help="Output directory for parquet + manifest.json")
    parser.add_argument("--tables", default="", help="Comma-separated subset (default: all)")
    parser.add_argument("--no-sort", action="store_true", help="Skip cik sort of big tables (lighter export)")
    parser.add_argument("--buckets", type=int, default=NUM_BUCKETS, help="Number of cik buckets for big tables")
    parser.add_argument("--ciks", default="", help="Comma-separated CIK slice (test exports)")
    parser.add_argument("--tmp-dir", default="", help="[rest] scratch dir for downloaded CSVs (default: <out>/_tmp)")
    args = parser.parse_args(argv)

    if args.source == "server" and not args.server:
        parser.error("--source server requires --server host:port/database")

    tables = [t.strip() for t in args.tables.split(",") if t.strip()] if args.tables else list(ALL_TABLES)
    unknown = [t for t in tables if t not in ALL_TABLES]
    if unknown:
        parser.error(f"unknown tables: {unknown}")
    ciks = [c.strip().zfill(10) for c in args.ciks.split(",") if c.strip()]

    run_export(
        source=args.source,
        owner=args.owner,
        repo=args.repo,
        branch=args.branch,
        server=args.server,
        server_user=args.server_user,
        server_password=args.server_password,
        out=args.out,
        tables=tables,
        sort=not args.no_sort,
        num_buckets=args.buckets,
        ciks=ciks,
        tmp_dir=args.tmp_dir or None,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
