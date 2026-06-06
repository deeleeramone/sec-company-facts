"""Dolt ingest for the SEC store."""

from __future__ import annotations

import atexit
import gzip
import hashlib
import json
import multiprocessing
import os
import fcntl
import time
import urllib.request
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator, cast

import pyarrow as pa

from sec_app.db.backend import connect_dolt, table_name as _backend_table
from openbb_sec.utils.company_facts import MULTI_CIK_TICKERS, resolve_company_facts
from openbb_sec.utils.definitions import HEADERS
from openbb_sec.utils.statement_schema._detection import get_filing_dates

COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
COMPANY_TICKERS_MF_URL = "https://www.sec.gov/files/company_tickers_mf.json"
CIK_LOOKUP_URL = "https://www.sec.gov/Archives/edgar/cik-lookup-data.txt"
COMPANYFACTS_ZIP_URL = "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip"
SUBMISSIONS_ZIP_URL = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"


def _table(name: str) -> str:
    return _backend_table(name)


def _connect(db_path: str | None = None):
    """Return a Dolt connection.

    Connection details come from the ``DOLT_SQL_*`` environment variables and the
    schema is assumed already present in the repo. ``db_path`` is ignored.
    """
    del db_path
    conn = connect_dolt()
    _OPEN_CONNECTIONS.add(conn)
    global _SCHEMA_ENSURED
    if not _SCHEMA_ENSURED:
        _ensure_standardized_source_column(conn)
        _SCHEMA_ENSURED = True
    return conn


# Open connections not yet finalized; closed at each write entry point and as a
# process-exit safety net.
_OPEN_CONNECTIONS: set[Any] = set()

# Set once per process after standardized_statements.source is verified/added.
_SCHEMA_ENSURED = False


def _ensure_standardized_source_column(conn) -> None:
    """Idempotently ensure standardized_statements has the ``source`` column.

    The provenance column was added after the table was first published, so a
    clone of the existing DoltHub repo may predate it. Add it if missing before
    any materialization writes ``source`` — otherwise the INSERT fails against
    the older schema.
    """
    table = _table("standardized_statements")
    try:
        row = conn.execute(
            "SELECT COUNT(*) FROM information_schema.columns "
            "WHERE table_schema = DATABASE() AND table_name = 'standardized_statements' "
            "AND column_name = 'source'"
        ).fetchone()
    except Exception as exc:  # pylint: disable=broad-except
        print(f"[schema] could not verify standardized_statements.source: {exc!r}", flush=True)
        return
    if row and int(row[0]) > 0:
        return
    print("[schema] adding missing standardized_statements.source column", flush=True)
    conn.execute(f"ALTER TABLE {table} ADD COLUMN source VARCHAR(256)")


def _finalize(conn) -> None:
    """Close the connection.

    Safe to call more than once and on an already-closed connection — all
    failures are swallowed so finalize never masks the real error.
    """
    _OPEN_CONNECTIONS.discard(conn)
    try:
        conn.finalize()
        print("[db] connection finalized", flush=True)
    except Exception as err:
        print(f"[db] finalize skipped error={err!r}", flush=True)


@atexit.register
def _finalize_open_connections() -> None:
    """Last-resort flush so an unhandled exception can't strand the WAL."""
    for conn in list(_OPEN_CONNECTIONS):
        _finalize(conn)


def _zpad_cik(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    digits = text.lstrip("0") or "0"
    if not digits.isdigit():
        if text.isdigit():
            digits = text
        else:
            return None
    return digits.zfill(10)


def _parse_date(value: Any):
    if not value:
        return None
    if isinstance(value, str):
        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError:
            return None
    return None


def _cik_from_filename(name: str) -> str | None:
    base = os.path.basename(name)
    if not base.startswith("CIK") or not base.endswith(".json"):
        return None
    stem = base[3:-5]
    if "-" in stem or not stem.isdigit():
        return None
    return stem.zfill(10)


def _zip_entry_mtime(info: zipfile.ZipInfo) -> datetime:
    return datetime(*info.date_time)


def _http_get(url: str) -> bytes:
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=120) as resp:
        body = resp.read()
        encoding = resp.headers.get("Content-Encoding", "").lower()
    if encoding == "gzip":
        return gzip.decompress(body)
    if encoding == "deflate":
        import zlib

        return zlib.decompress(body)
    return body


def _default_download_cache() -> Path:
    return Path(os.environ.get("SEC_DOWNLOAD_CACHE") or "/var/cache/openbb_sec")


def _stream_download(url: str, dest: Path, chunk_size: int = 1 << 20) -> Path:
    import time as _t

    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + f".{os.getpid()}.part")
    req = urllib.request.Request(url, headers=HEADERS)
    print(f"[download] GET {url} -> {dest}", flush=True)
    t0 = _t.time()
    with urllib.request.urlopen(req, timeout=600) as resp:
        total = int(resp.headers.get("Content-Length") or 0)
        with open(tmp, "wb") as out:
            downloaded = 0
            last_log = t0
            while True:
                chunk = resp.read(chunk_size)
                if not chunk:
                    break
                out.write(chunk)
                downloaded += len(chunk)
                now = _t.time()
                if now - last_log >= 5:
                    pct = (downloaded / total * 100) if total else 0
                    rate = downloaded / (now - t0) / (1 << 20)
                    print(
                        f"[download] {dest.name}: {downloaded / 1e9:.2f}/{total / 1e9:.2f} GB ({pct:.1f}%)  {rate:.1f} MiB/s",
                        flush=True,
                    )
                    last_log = now
    tmp.replace(dest)
    elapsed = _t.time() - t0
    print(f"[download] {dest.name}: done in {elapsed:.1f}s ({dest.stat().st_size / 1e9:.2f} GB)", flush=True)
    return dest


def _with_update_lock(func):
    def wrapper(*args, **kwargs):
        lock_path = Path("/tmp/openbb_sec_update.lock")
        with open(lock_path, "w", encoding="utf-8") as lock_file:
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                print("[update] another update is already running; skipping this invocation", flush=True)
                return {"changed_facts": 0, "changed_subs": 0, "standardized_rows": 0}
            return func(*args, **kwargs)

    return wrapper


def download_companyfacts_zip(dest: str | Path | None = None) -> Path:
    target = Path(dest) if dest else _default_download_cache() / "companyfacts.zip"
    return _stream_download(COMPANYFACTS_ZIP_URL, target)


def download_submissions_zip(dest: str | Path | None = None) -> Path:
    target = Path(dest) if dest else _default_download_cache() / "submissions.zip"
    return _stream_download(SUBMISSIONS_ZIP_URL, target)


def _insert_arrow(conn, table: str, rows: list[dict[str, Any]], schema: pa.Schema) -> None:
    if not rows:
        print(f"[db] insert skip table={table} rows=0", flush=True)
        return
    print(f"[db] insert begin table={table} rows={len(rows):,}", flush=True)
    conn.bulk_insert(table, rows, schema)
    print(f"[db] insert done table={table} rows={len(rows):,}", flush=True)


def _replace_rows_for_ciks(conn, table: str, rows: list[dict[str, Any]], schema: pa.Schema) -> None:
    if not rows:
        print(f"[db] replace skip table={table} rows=0", flush=True)
        return
    ciks = sorted({row.get("cik") for row in rows if isinstance(row, dict) and row.get("cik")})
    print(f"[db] replace begin table={table} rows={len(rows):,} ciks={len(ciks):,}", flush=True)
    # Delete the touched CIKs in chunks — a single IN(...) list with 600k+
    # entries blows past MySQL's max_allowed_packet and is slow on DuckDB too.
    for i in range(0, len(ciks), 500):
        chunk = ciks[i : i + 500]
        placeholders = ",".join(["?"] * len(chunk))
        conn.execute(f"DELETE FROM {table} WHERE cik IN ({placeholders})", chunk)
    conn.bulk_insert(table, rows, schema)
    print(f"[db] replace done table={table} rows={len(rows):,} ciks={len(ciks):,}", flush=True)


def _delete_and_insert_rows(conn, table: str, cik: str, rows: list[dict[str, Any]], schema: pa.Schema) -> None:
    print(f"[db] replace-cik begin table={table} cik={cik} rows={len(rows):,}", flush=True)
    conn.execute(f"DELETE FROM {table} WHERE cik = ?", [cik])
    if rows:
        _insert_arrow(conn, table, rows, schema)
    else:
        print(f"[db] replace-cik no-insert table={table} cik={cik}", flush=True)
    print(f"[db] replace-cik done table={table} cik={cik} rows={len(rows):,}", flush=True)


def _delete_all(conn, *tables: str) -> None:
    for table in tables:
        conn.execute(f"DELETE FROM {table}")


def _delete_ciks(conn, table: str, ciks: set[str]) -> None:
    for cik in ciks:
        conn.execute(f"DELETE FROM {table} WHERE cik = ?", [cik])


def _existing_hashes(conn, table: str) -> dict[str, str]:
    rows = conn.execute(f"SELECT cik, source_content_hash FROM {table}").fetchall()
    return {cik: content_hash for cik, content_hash in rows if content_hash}


def _existing_mtimes(conn, table: str) -> dict[str, datetime]:
    rows = conn.execute(f"SELECT cik, source_mtime FROM {table}").fetchall()
    return {cik: source_mtime for cik, source_mtime in rows if source_mtime is not None}


def _existing_payload_hashes(conn, table: str) -> dict[str, str]:
    hashes: dict[str, str] = {}
    cur = conn.execute(f"SELECT cik, payload FROM {table}")
    while True:
        rows = cur.fetchmany(1000)
        if not rows:
            break
        for cik, payload in rows:
            if payload is None:
                continue
            if isinstance(payload, memoryview):
                payload = payload.tobytes()
            elif isinstance(payload, str):
                payload = payload.encode("utf-8")
            try:
                raw = gzip.decompress(payload)
                obj = json.loads(raw)
                raw = json.dumps(obj, separators=(",", ":"), sort_keys=True).encode("utf-8")
            except Exception:
                raw = payload
            hashes[cik] = hashlib.sha256(raw).hexdigest()
    return hashes


def _iter_companyfacts_zip(
    zip_path: str | Path, only_ciks: set[str] | None = None
) -> Iterator[tuple[str, bytes, datetime]]:
    with zipfile.ZipFile(zip_path) as zf:
        for info in zf.infolist():
            cik = _cik_from_filename(info.filename)
            if cik is None:
                continue
            if only_ciks is not None and cik not in only_ciks:
                continue
            with zf.open(info) as fh:
                payload = fh.read()
            yield cik, payload, _zip_entry_mtime(info)


def _statement_ciks(conn) -> set[str]:
    rows = conn.execute(
        f"SELECT cik FROM {_table('processed_ciks')} WHERE has_balance OR has_income OR has_cash_flow"
    ).fetchall()
    return {cik for (cik,) in rows}


def _merge_overflow_filings(main: dict[str, Any], overflow: dict[str, Any]) -> None:
    filings = main.get("filings")
    if not isinstance(filings, dict):
        return
    recent = filings.get("recent")
    if not isinstance(recent, dict):
        recent = {}
        filings["recent"] = recent
    for key, values in overflow.items():
        if not isinstance(values, list):
            continue
        current = recent.get(key)
        if isinstance(current, list):
            current.extend(values)
        else:
            recent[key] = list(values)


def _iter_submissions_zip(
    zip_path: str | Path, cik_filter: set[str] | None = None
) -> Iterator[tuple[str, dict[str, Any], datetime]]:
    with zipfile.ZipFile(zip_path) as zf:
        for info in zf.infolist():
            cik = _cik_from_filename(info.filename)
            if cik is None:
                continue
            if cik_filter is not None and cik not in cik_filter:
                continue
            with zf.open(info) as fh:
                main_bytes = fh.read()
            try:
                data = json.loads(main_bytes)
            except json.JSONDecodeError:
                continue
            if not isinstance(data, dict):
                continue
            mtime = _zip_entry_mtime(info)
            files_list = (data.get("filings") or {}).get("files") or []
            if isinstance(files_list, list):
                for entry in files_list:
                    name = entry.get("name") if isinstance(entry, dict) else entry
                    if not isinstance(name, str):
                        continue
                    try:
                        sub_info = zf.getinfo(name)
                    except KeyError:
                        continue
                    with zf.open(sub_info) as sfh:
                        overflow_bytes = sfh.read()
                    try:
                        overflow = json.loads(overflow_bytes)
                    except json.JSONDecodeError:
                        continue
                    if not isinstance(overflow, dict):
                        continue
                    _merge_overflow_filings(data, overflow)
                    sub_mtime = _zip_entry_mtime(sub_info)
                    if sub_mtime > mtime:
                        mtime = sub_mtime
            yield cik, data, mtime


_COMPANIES_SCHEMA = pa.schema(
    [
        ("cik", pa.string()),
        ("entity_name", pa.string()),
        ("source_mtime", pa.timestamp("s")),
        ("source_content_hash", pa.string()),
    ]
)

_TAG_META_SCHEMA = pa.schema(
    [
        ("cik", pa.string()),
        ("namespace", pa.string()),
        ("tag", pa.string()),
        ("label", pa.string()),
        ("description", pa.string()),
    ]
)

_FACTS_SCHEMA = pa.schema(
    [
        ("cik", pa.string()),
        ("namespace", pa.string()),
        ("tag", pa.string()),
        ("unit", pa.string()),
        ("start", pa.date32()),
        ("end", pa.date32()),
        ("val", pa.float64()),
        ("val_text", pa.string()),
        ("accn", pa.string()),
        ("fy", pa.int32()),
        ("fp", pa.string()),
        ("form", pa.string()),
        ("filed", pa.date32()),
        ("frame", pa.string()),
    ]
)

_SUBMISSIONS_SCHEMA = pa.schema(
    [
        ("cik", pa.string()),
        ("payload", pa.binary()),
        ("source_mtime", pa.timestamp("s")),
    ]
)

_PROCESSED_SCHEMA = pa.schema(
    [
        ("cik", pa.string()),
        ("has_balance", pa.bool_()),
        ("has_income", pa.bool_()),
        ("has_cash_flow", pa.bool_()),
        ("computed_at", pa.timestamp("s")),
    ]
)

_STANDARDIZED_SCHEMA = pa.schema(
    [
        ("cik", pa.string()),
        ("statement", pa.string()),
        ("period_ending", pa.date32()),
        ("fiscal_year", pa.int32()),
        ("fiscal_period", pa.string()),
        ("calendar_year", pa.int32()),
        ("calendar_period", pa.string()),
        ("frequency", pa.string()),
        ("tag", pa.string()),
        ("label", pa.string()),
        ("parent", pa.string()),
        ("sequence", pa.int32()),
        ("factor", pa.string()),
        ("balance", pa.string()),
        ("unit", pa.string()),
        ("val", pa.float64()),
        ("currency", pa.string()),
        ("company_type", pa.string()),
        ("source", pa.string()),
    ]
)

_TICKERS_SCHEMA = pa.schema(
    [
        ("cik", pa.string()),
        ("ticker", pa.string()),
        ("name", pa.string()),
        ("is_primary", pa.bool_()),
        ("rank", pa.int32()),
    ]
)

_FUNDS_SCHEMA = pa.schema(
    [
        ("cik", pa.string()),
        ("series_id", pa.string()),
        ("class_id", pa.string()),
        ("symbol", pa.string()),
    ]
)

_ENTITIES_SCHEMA = pa.schema(
    [
        ("cik", pa.string()),
        ("entity_name", pa.string()),
    ]
)

_MULTI_CIK_SCHEMA = pa.schema(
    [
        ("ticker", pa.string()),
        ("cik", pa.string()),
        ("priority", pa.int32()),
    ]
)


def ingest_companyfacts_zip(
    zip_path: str | Path,
    db_path: str | None = None,
    limit: int | None = None,
    **kwargs: Any,
) -> dict[str, int]:
    del kwargs
    print(f"[facts] ingest start zip={zip_path} limit={limit}", flush=True)

    inline_on = os.environ.get("SEC_INLINE_STANDARDIZED", "1") == "1"
    # The standardized resolve (~0.5s/large company) is the bottleneck; run it in a
    # process pool across cores. Create the pool BEFORE opening the DuckDB
    # connection so the forked workers don't inherit a DB handle. Workers only
    # resolve in-memory payloads — they never touch the database.
    n_workers = max(1, min(int(os.environ.get("SEC_INGEST_WORKERS", "0")) or (os.cpu_count() or 2), os.cpu_count() or 2))
    pool = multiprocessing.Pool(n_workers) if (inline_on and n_workers > 1) else None
    batch_size = int(os.environ.get("SEC_INGEST_BATCH", "256"))
    if pool is not None:
        print(f"[facts] standardized resolve pool workers={n_workers} batch={batch_size}", flush=True)

    conn = _connect(db_path)
    existing_hashes = _existing_hashes(conn, _table("companies"))

    # Tags that define "has a statement" — only CIKs whose facts intersect these
    # get standardized statements materialized (same membership as processed_ciks).
    _stmt_tag_sets = _load_statement_tag_sets()
    _all_stmt_tags = _stmt_tag_sets["balance_sheet"] | _stmt_tag_sets["income_statement"] | _stmt_tag_sets["cash_flow"]

    stats = {"files": 0, "changed": 0, "companies": 0, "tag_meta": 0, "facts": 0, "standardized": 0}
    resolve_batch: list[tuple[str, dict[str, Any]]] = []

    def _flush_resolve_batch() -> None:
        if not resolve_batch:
            return
        n = len(resolve_batch)
        print(f"[standardized] resolving batch of {n} companies across {n_workers} workers", flush=True)
        if pool is not None:
            results = pool.map(_resolve_std_worker, resolve_batch)
        else:
            results = [_resolve_std_worker(b) for b in resolve_batch]
        rows_total = 0
        for cik_done, rows, err in results:
            if err:
                print(f"[facts] cik={cik_done} standardized resolve FAILED: {err}", flush=True)
            if rows:
                _insert_arrow(conn, _table("standardized_statements"), rows, _STANDARDIZED_SCHEMA)
            rows_total += len(rows)
            stats["standardized"] += len(rows)
        print(f"[standardized] batch done companies={n} rows={rows_total:,}", flush=True)
        resolve_batch.clear()

    for cik, payload, mtime in _iter_companyfacts_zip(zip_path):
        if limit is not None and stats["files"] >= limit:
            print(f"[facts] limit reached files={stats['files']:,} limit={limit}", flush=True)
            break
        stats["files"] += 1
        print(f"[facts] file begin idx={stats['files']:,} cik={cik} mtime={mtime.isoformat()}", flush=True)
        try:
            data = json.loads(payload)
            if not isinstance(data, dict):
                data = {}
        except json.JSONDecodeError:
            data = {}
            print(f"[facts] cik={cik} payload json decode failed; using empty object", flush=True)

        cik_norm = _zpad_cik(data.get("cik")) or cik
        content_hash = hashlib.sha256(payload).hexdigest()
        prev_hash = existing_hashes.get(cik_norm)
        if prev_hash == content_hash:
            continue

        stats["changed"] += 1
        existing_hashes[cik_norm] = content_hash
        # Only delete prior rows when this CIK actually existed before (an update).
        # On a fresh ingest (prev_hash is None) the tables hold nothing for it, so
        # the DELETEs would be pure full-table scans over the large tables — skip.
        if prev_hash is not None:
            conn.execute(f"DELETE FROM {_table('companies')} WHERE cik = ?", [cik_norm])
            conn.execute(f"DELETE FROM {_table('tag_meta')} WHERE cik = ?", [cik_norm])
            conn.execute(f"DELETE FROM {_table('facts')} WHERE cik = ?", [cik_norm])
            conn.execute(f"DELETE FROM {_table('standardized_statements')} WHERE cik = ?", [cik_norm])

        companies: list[dict[str, Any]] = [
            {
                "cik": cik_norm,
                "entity_name": data.get("entityName") or "",
                "source_mtime": mtime,
                "source_content_hash": content_hash,
            }
        ]
        tag_meta: list[dict[str, Any]] = []
        facts: list[dict[str, Any]] = []

        for namespace, tag_dict in (data.get("facts") or {}).items():
            if not isinstance(tag_dict, dict):
                continue
            for tag, payload_obj in tag_dict.items():
                if not isinstance(payload_obj, dict):
                    continue
                tag_meta.append(
                    {
                        "cik": cik_norm,
                        "namespace": namespace,
                        "tag": tag,
                        "label": payload_obj.get("label") or "",
                        "description": payload_obj.get("description") or "",
                    }
                )
                for unit, periods in (payload_obj.get("units") or {}).items():
                    if not isinstance(periods, list):
                        continue
                    for period in periods:
                        if not isinstance(period, dict):
                            continue
                        val = period.get("val")
                        try:
                            val_f = float(val) if val is not None else None
                        except (TypeError, ValueError):
                            val_f = None
                        fy = period.get("fy")
                        try:
                            fy_i = int(fy) if fy is not None else None
                        except (TypeError, ValueError):
                            fy_i = None
                        facts.append(
                            {
                                "cik": cik_norm,
                                "namespace": namespace,
                                "tag": tag,
                                "unit": unit,
                                "start": _parse_date(period.get("start")),
                                "end": _parse_date(period.get("end")),
                                "val": val_f,
                                "val_text": None,
                                "accn": period.get("accn"),
                                "fy": fy_i,
                                "fp": period.get("fp"),
                                "form": period.get("form"),
                                "filed": _parse_date(period.get("filed")),
                                "frame": period.get("frame"),
                            }
                        )

        # A company with no fact values carries no financial data — do not insert
        # an empty companies row for it. (Existing rows for this CIK were already
        # deleted above, so this also removes a CIK that has lost all its facts.)
        if not facts:
            print(f"[facts] cik={cik_norm} no fact values — skipping (company not inserted)", flush=True)
            continue

        print(
            f"[facts] cik={cik_norm} insert start companies={len(companies):,} tag_meta={len(tag_meta):,} facts={len(facts):,}",
            flush=True,
        )
        _insert_arrow(conn, _table("companies"), companies, _COMPANIES_SCHEMA)
        _insert_arrow(conn, _table("tag_meta"), tag_meta, _TAG_META_SCHEMA)
        _insert_arrow(conn, _table("facts"), facts, _FACTS_SCHEMA)

        # Materialize standardized statements in the SAME pass (single-pass design),
        # resolving from the in-memory payload — no second read of the DB. ONLY for
        # CIKs that actually have a statement (their tags intersect the statement
        # tag sets); others are skipped entirely — never blindly resolved/inserted.
        # Set SEC_INLINE_STANDARDIZED=0 to disable and use the standalone step.
        has_statements = any(
            tm["namespace"] in ("us-gaap", "ifrs-full") and tm["tag"] in _all_stmt_tags for tm in tag_meta
        )
        if has_statements and inline_on:
            # Queue the (CPU-heavy) resolve for the worker pool; the standardized
            # rows are inserted when the batch flushes.
            resolve_batch.append((cik_norm, data))
            if len(resolve_batch) >= batch_size:
                _flush_resolve_batch()

        print(
            f"[facts] cik={cik_norm} insert done companies={len(companies):,} tag_meta={len(tag_meta):,} facts={len(facts):,} standardized_queued={has_statements and inline_on}",
            flush=True,
        )
        stats["companies"] += len(companies)
        stats["tag_meta"] += len(tag_meta)
        stats["facts"] += len(facts)
        if stats["files"] % 100 == 0:
            print(
                f"[facts] files={stats['files']:,} changed={stats['changed']:,} companies={stats['companies']:,} tag_meta={stats['tag_meta']:,} facts={stats['facts']:,} standardized={stats['standardized']:,}",
                flush=True,
            )

    _flush_resolve_batch()  # resolve + insert any remaining queued companies
    if pool is not None:
        pool.close()
        pool.join()

    print("[facts] recomputing processed_ciks start", flush=True)
    compute_processed_ciks(db_path=db_path)
    print("[facts] recomputing processed_ciks done", flush=True)
    print(
        f"[facts] ingest done files={stats['files']:,} changed={stats['changed']:,} companies={stats['companies']:,} tag_meta={stats['tag_meta']:,} facts={stats['facts']:,} standardized={stats['standardized']:,}",
        flush=True,
    )
    _finalize(conn)
    return stats


def ingest_directory(
    src_dir: str | Path,
    db_path: str | None = None,
    limit: int | None = None,
    **kwargs: Any,
) -> dict[str, int]:
    del src_dir, db_path, limit, kwargs
    raise ValueError("Local directory ingest is not supported")


def _load_statement_tag_sets() -> dict[str, set[str]]:
    """us-gaap/ifrs-full XBRL tags per statement, read from the schema JSONs.

    This is the single source of truth for which CIKs "have" a statement: the
    exact membership used both to populate processed_ciks and to decide which
    CIKs get standardized statements materialized. A CIK whose tags intersect
    none of these sets is never resolved or inserted.
    """
    # The statement-schema JSONs ship inside the openbb-sec provider (PyPI), not
    # in this serving package.
    import openbb_sec.utils.statement_schema as _statement_schema_pkg  # pylint: disable=import-outside-toplevel

    schema_dir = Path(_statement_schema_pkg.__file__).resolve().parent / "schemas"
    tag_sets: dict[str, set[str]] = {}
    for stmt in ("balance_sheet", "income_statement", "cash_flow"):
        with open(schema_dir / f"{stmt}.json") as fh:
            data = json.load(fh)
        tags: set[str] = set()

        def walk(obj: Any) -> None:
            if isinstance(obj, dict):
                xbrl = obj.get("xbrl_tags")
                if isinstance(xbrl, list):
                    for t in xbrl:
                        if (
                            isinstance(t, dict)
                            and t.get("namespace") in {"us-gaap", "ifrs-full"}
                            and isinstance(t.get("tag"), str)
                        ):
                            tags.add(t["tag"])
                for v in obj.values():
                    walk(v)
            elif isinstance(obj, list):
                for v in obj:
                    walk(v)

        walk(data)
        tag_sets[stmt] = tags
    return tag_sets


def compute_processed_ciks(
    db_path: str | None = None,
    only_ciks: set[str] | None = None,
) -> dict[str, int]:
    target_label = "ALL" if only_ciks is None else f"{len(only_ciks):,}"
    print(
        f"[processed_ciks] start only_ciks={target_label}",
        flush=True,
    )
    conn = _connect(db_path)

    if only_ciks is not None and not only_ciks:
        print("[processed_ciks] no target ciks; nothing to do", flush=True)
        _finalize(conn)
        return {"total": 0, "with_balance": 0, "with_income": 0, "with_cash_flow": 0}

    tag_sets = _load_statement_tag_sets()

    tag_rows: list[dict[str, str]] = []
    for stmt, tags in (
        ("balance_sheet", tag_sets["balance_sheet"]),
        ("income_statement", tag_sets["income_statement"]),
        ("cash_flow", tag_sets["cash_flow"]),
    ):
        for tag in tags:
            tag_rows.append({"tag": tag, "stmt": stmt})

    try:
        print("[processed_ciks] tx begin", flush=True)
        conn.begin()
        conn.create_temp_table("_stmt_tag_map", "tag VARCHAR(512), stmt VARCHAR(64)")
        _insert_arrow(
            conn,
            "_stmt_tag_map",
            tag_rows,
            pa.schema([("tag", pa.string()), ("stmt", pa.string())]),
        )

        if only_ciks is None:
            conn.execute(f"DELETE FROM {_table('processed_ciks')}")
            source_sql = f"""
                SELECT
                    tm.cik,
                    MAX(CASE WHEN m.stmt = 'balance_sheet' THEN 1 ELSE 0 END) AS has_balance,
                    MAX(CASE WHEN m.stmt = 'income_statement' THEN 1 ELSE 0 END) AS has_income,
                    MAX(CASE WHEN m.stmt = 'cash_flow' THEN 1 ELSE 0 END) AS has_cash_flow
                FROM {_table("tag_meta")} tm
                JOIN _stmt_tag_map m ON m.tag = tm.tag
                WHERE tm.namespace IN ('us-gaap', 'ifrs-full')
                GROUP BY tm.cik
            """
        else:
            target_rows = [{"cik": cik} for cik in sorted(only_ciks)]
            conn.create_temp_table("_target_ciks", "cik VARCHAR(10)")
            _insert_arrow(conn, "_target_ciks", target_rows, pa.schema([("cik", pa.string())]))
            conn.execute(f"DELETE FROM {_table('processed_ciks')} WHERE cik IN (SELECT cik FROM _target_ciks)")
            source_sql = f"""
                SELECT
                    tm.cik,
                    MAX(CASE WHEN m.stmt = 'balance_sheet' THEN 1 ELSE 0 END) AS has_balance,
                    MAX(CASE WHEN m.stmt = 'income_statement' THEN 1 ELSE 0 END) AS has_income,
                    MAX(CASE WHEN m.stmt = 'cash_flow' THEN 1 ELSE 0 END) AS has_cash_flow
                FROM {_table("tag_meta")} tm
                JOIN _target_ciks t ON t.cik = tm.cik
                JOIN _stmt_tag_map m ON m.tag = tm.tag
                WHERE tm.namespace IN ('us-gaap', 'ifrs-full')
                GROUP BY tm.cik
            """

        now = datetime.now().replace(microsecond=0)
        conn.execute(
            f"""
            INSERT INTO {_table("processed_ciks")} (cik, has_balance, has_income, has_cash_flow, computed_at)
            SELECT cik, has_balance = 1, has_income = 1, has_cash_flow = 1, ?
            FROM ({source_sql}) q
            WHERE has_balance = 1 OR has_income = 1 OR has_cash_flow = 1
            """,
            [now],
        )

        total, with_balance, with_income, with_cash_flow = conn.execute(
            f"""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN has_balance THEN 1 ELSE 0 END) AS with_balance,
                SUM(CASE WHEN has_income THEN 1 ELSE 0 END) AS with_income,
                SUM(CASE WHEN has_cash_flow THEN 1 ELSE 0 END) AS with_cash_flow
            FROM {_table("processed_ciks")}
            WHERE computed_at = ?
            """,
            [now],
        ).fetchone()
        conn.commit()
        print("[processed_ciks] tx commit", flush=True)
    except Exception as e:
        conn.rollback()
        print(f"[processed_ciks] tx rollback error={e!r}", flush=True)
        raise

    result = {
        "total": int(total or 0),
        "with_balance": int(with_balance or 0),
        "with_income": int(with_income or 0),
        "with_cash_flow": int(with_cash_flow or 0),
    }
    print(
        f"[processed_ciks] done total={result['total']:,} with_balance={result['with_balance']:,} with_income={result['with_income']:,} with_cash_flow={result['with_cash_flow']:,}",
        flush=True,
    )
    _finalize(conn)
    return result


def ingest_submissions_zip(
    zip_path: str | Path,
    db_path: str | None = None,
    limit: int | None = None,
    flush_every: int = 100,
    progress_every: int = 1000,
    cik_filter: set[str] | None = None,
) -> dict[str, int]:
    del flush_every, progress_every
    print(f"[submissions] ingest start zip={zip_path} limit={limit}", flush=True)
    conn = _connect(db_path)
    existing_hashes = _existing_payload_hashes(conn, _table("submissions"))

    if cik_filter is None:
        rows = conn.execute(
            f"SELECT cik FROM {_table('processed_ciks')} WHERE has_balance OR has_income OR has_cash_flow"
        ).fetchall()
        cik_filter = {row[0] for row in rows}

    if not cik_filter:
        print("[submissions] cik_filter empty; nothing to ingest", flush=True)
        _finalize(conn)
        return {"files_processed": 0, "submissions": 0, "dei_facts": 0}

    stats = {"files_processed": 0, "changed": 0, "submissions": 0, "dei_facts": 0}
    print(f"[submissions] scan start zip={zip_path}", flush=True)

    dei_numeric = (("EntitySicCode", "sic", "Standard Industrial Classification (SIC) Code"),)
    dei_text = (
        ("EntityType", "entityType", "Entity Type"),
        ("EntityFilerCategory", "category", "Entity Filer Category"),
        ("EntitySicDescription", "sicDescription", "SIC Industry Description"),
        ("EntityFiscalYearEnd", "fiscalYearEnd", "Fiscal Year End (MMDD)"),
        ("EntityStateOfIncorporation", "stateOfIncorporation", "State of Incorporation"),
        ("EntityName", "name", "Registrant Name"),
        ("EntityEin", "ein", "Employer Identification Number"),
        ("EntityLei", "lei", "Legal Entity Identifier"),
    )

    for cik, data, mtime in _iter_submissions_zip(zip_path, cik_filter=cik_filter):
        if limit is not None and stats["files_processed"] >= limit:
            print(f"[submissions] limit reached files={stats['files_processed']:,} limit={limit}", flush=True)
            break
        stats["files_processed"] += 1
        print(f"[submissions] cik={cik} begin idx={stats['files_processed']:,} mtime={mtime.isoformat()}", flush=True)
        payload_json = json.dumps(data, separators=(",", ":"), sort_keys=True).encode("utf-8")
        payload_bytes = gzip.compress(payload_json, compresslevel=6, mtime=0)
        payload_hash = hashlib.sha256(payload_json).hexdigest()
        if existing_hashes.get(cik) == payload_hash:
            print(f"[submissions] cik={cik} unchanged", flush=True)
            continue

        stats["changed"] += 1
        existing_hashes[cik] = payload_hash
        print(f"[submissions] cik={cik} changed hash={payload_hash[:12]} mtime={mtime.isoformat()}", flush=True)
        print(f"[submissions] cik={cik} deleting existing dei/submission rows", flush=True)
        conn.execute(f"DELETE FROM {_table('submissions')} WHERE cik = ?", [cik])
        conn.execute(f"DELETE FROM {_table('facts')} WHERE cik = ? AND namespace = 'dei'", [cik])
        conn.execute(f"DELETE FROM {_table('tag_meta')} WHERE cik = ? AND namespace = 'dei'", [cik])

        submissions: list[dict[str, Any]] = [{"cik": cik, "payload": payload_bytes, "source_mtime": mtime}]
        tag_meta: list[dict[str, Any]] = []
        facts: list[dict[str, Any]] = []

        end_date = mtime.date()
        for tag, source_key, description in dei_numeric:
            raw = data.get(source_key)
            if raw in (None, ""):
                continue
            try:
                val_f = float(str(raw))
            except (TypeError, ValueError):
                continue
            tag_meta.append({"cik": cik, "namespace": "dei", "tag": tag, "label": tag, "description": description})
            facts.append(
                {
                    "cik": cik,
                    "namespace": "dei",
                    "tag": tag,
                    "unit": "pure",
                    "start": None,
                    "end": end_date,
                    "val": val_f,
                    "val_text": None,
                    "accn": None,
                    "fy": None,
                    "fp": None,
                    "form": "submissions",
                    "filed": end_date,
                    "frame": None,
                }
            )

        for tag, source_key, description in dei_text:
            raw = data.get(source_key)
            if raw in (None, ""):
                continue
            tag_meta.append({"cik": cik, "namespace": "dei", "tag": tag, "label": tag, "description": description})
            facts.append(
                {
                    "cik": cik,
                    "namespace": "dei",
                    "tag": tag,
                    "unit": "string",
                    "start": None,
                    "end": end_date,
                    "val": None,
                    "val_text": str(raw),
                    "accn": None,
                    "fy": None,
                    "fp": None,
                    "form": "submissions",
                    "filed": end_date,
                    "frame": None,
                }
            )

        _insert_arrow(conn, _table("submissions"), submissions, _SUBMISSIONS_SCHEMA)
        _insert_arrow(conn, _table("tag_meta"), tag_meta, _TAG_META_SCHEMA)
        _insert_arrow(conn, _table("facts"), facts, _FACTS_SCHEMA)
        print(
            f"[submissions] cik={cik} inserted submissions={len(submissions)} tag_meta={len(tag_meta)} dei_facts={len(facts)}",
            flush=True,
        )
        stats["submissions"] += len(submissions)
        stats["dei_facts"] += len(facts)

    print(
        f"[submissions] scan done files={stats['files_processed']:,} changed={stats['changed']:,} submissions={stats['submissions']:,} dei_facts={stats['dei_facts']:,}",
        flush=True,
    )

    _finalize(conn)
    return stats


def _iso(v):
    if v is None:
        return None
    if isinstance(v, str):
        return v
    iso = getattr(v, "isoformat", None)
    return iso() if callable(iso) else str(v)


def _load_company_facts_from_conn(conn, cik: str) -> dict[str, Any]:
    """Load company facts using an already-open connection to avoid second-connection conflicts."""
    verbose = os.environ.get("SEC_LOAD_VERBOSE", "1") == "1"
    cik_padded = cik.zfill(10)
    if verbose:
        print(f"[standardized] cik={cik_padded} load facts begin", flush=True)
    company = conn.execute(
        f"SELECT cik, entity_name FROM {_table('companies')} WHERE cik = ? LIMIT 1", [cik_padded]
    ).fetchone()
    if not company:
        if verbose:
            print(f"[standardized] cik={cik_padded} load facts missing company row", flush=True)
        raise ValueError(f"CIK {cik_padded} not found")
    meta_rows = conn.execute(
        f"SELECT namespace, tag, label, description FROM {_table('tag_meta')} WHERE cik = ?", [cik_padded]
    ).fetchall()
    fact_rows = conn.execute(
        f'SELECT namespace, tag, unit, start, "end", val, val_text, accn, fy, fp, form, filed, frame '
        f'FROM {_table("facts")} WHERE cik = ? ORDER BY namespace, tag, unit, "end", filed',
        [cik_padded],
    ).fetchall()
    if verbose:
        print(
            f"[standardized] cik={cik_padded} load facts rows meta={len(meta_rows):,} facts={len(fact_rows):,}",
            flush=True,
        )
    facts: dict[str, Any] = {}
    for namespace, tag, label, description in meta_rows:
        facts.setdefault(namespace, {})[tag] = {"label": label, "description": description, "units": {}}
    for row in fact_rows:
        namespace, tag, unit, start, end, val, val_text, accn, fy, fp, form, filed, frame = row
        ns = facts.setdefault(namespace, {})
        tag_entry = ns.setdefault(tag, {"label": None, "description": None, "units": {}})
        unit_list = tag_entry["units"].setdefault(unit, [])
        record: dict[str, Any] = {
            "end": _iso(end),
            "val": val_text if val_text else val,
            "accn": accn,
            "fy": fy,
            "fp": fp,
            "form": form,
            "filed": _iso(filed),
        }
        if start is not None:
            record["start"] = _iso(start)
        if frame:
            record["frame"] = frame
        unit_list.append(record)
    if verbose:
        print(f"[standardized] cik={cik_padded} load facts done", flush=True)
    return {"cik": cik_padded, "entityName": company[1] or "", "facts": facts}


def _resolve_standardized_with_fallback(cik: str, facts_json: dict[str, Any]):
    """Resolve standardized statements, picking the period up front.

    Many filers (foreign 20-F, small/annual-only) report no quarterly periods.
    Calling period="both" for them does the full annual extraction, raises on the
    missing quarterly, and then the annual retry redoes it — doubling the work for
    a large fraction of companies. Instead, check cheaply whether quarterly
    periods exist and resolve once with the right period.
    """
    facts = facts_json.get("facts", facts_json)
    period = "both" if get_filing_dates(facts, "quarterly") else "annual"
    try:
        return resolve_company_facts(facts_json, period=period)
    except Exception:
        if period == "both":
            return resolve_company_facts(facts_json, period="annual")
        raise


def _resolve_std_worker(args: tuple) -> tuple:
    """Pool worker: resolve standardized rows for one company. CPU-only, no DB.

    Returns (cik, rows, error_repr_or_None). The resolve step (~0.5s for a large
    filer) is the build's bottleneck; running it across cores is the whole point.
    """
    cik_norm, data = args
    try:
        result = _resolve_standardized_with_fallback(cik_norm, data)
        return cik_norm, _standardized_rows_for_result(cik_norm, result), None
    except Exception as e:  # noqa: BLE001
        return cik_norm, [], repr(e)


def _standardized_rows_for_result(cik: str, result) -> list[dict[str, Any]]:
    """Flatten a resolved StandardizedStatements result into DB rows."""
    rows: list[dict[str, Any]] = []
    for stmt_name in ("balance_sheet", "income_statement", "cash_flow"):
        for rec in getattr(result, stmt_name, None) or []:
            if not isinstance(rec, dict):
                if hasattr(rec, "model_dump"):
                    rec = rec.model_dump()
                elif hasattr(rec, "dict"):
                    rec = rec.dict()
            if not isinstance(rec, dict):
                continue
            pe = rec.get("period_ending")
            if isinstance(pe, str):
                try:
                    pe = datetime.strptime(pe, "%Y-%m-%d").date()
                except ValueError:
                    pe = None
            val = rec.get("value", rec.get("val"))
            try:
                val_f = float(val) if val is not None else None
            except (TypeError, ValueError):
                val_f = None
            rows.append(
                {
                    "cik": cik,
                    "statement": stmt_name,
                    "period_ending": pe,
                    "fiscal_year": rec.get("fiscal_year"),
                    "fiscal_period": rec.get("fiscal_period"),
                    "calendar_year": rec.get("calendar_year"),
                    "calendar_period": rec.get("calendar_period"),
                    "frequency": rec.get("frequency"),
                    "tag": rec.get("tag") or "",
                    "label": rec.get("label"),
                    "parent": rec.get("parent"),
                    "sequence": rec.get("sequence"),
                    "factor": rec.get("factor"),
                    "balance": rec.get("balance"),
                    "unit": rec.get("unit"),
                    "val": val_f,
                    "currency": result.currency,
                    "company_type": result.company_type,
                    "source": (rec.get("source") or "")[:256],
                }
            )
    return rows


def materialize_standardized_statements(
    db_path: str | None = None,
    workers: int = 4,
    progress_every: int = 25,
    insert_batch_rows: int = 10_000,
    only_ciks: set[str] | None = None,
) -> dict[str, int]:
    # Single-threaded by design: the production container is memory-limited
    # (2 GB), where spawning resolver processes would OOM. Per-CIK fact lookups
    # are fast because of the cik index declared in the schema. One connection is
    # used for both reads and the per-CIK write transaction.
    del workers, progress_every, insert_batch_rows
    os.environ.setdefault("SEC_LOAD_VERBOSE", "0")  # keep per-CIK load logs quiet at scale

    conn = _connect(db_path)
    stats = {"ciks_processed": 0, "ciks_empty": 0, "rows_inserted": 0}
    try:
        if only_ciks is None:
            cik_list = [
                c
                for (c,) in conn.execute(
                    f"SELECT cik FROM {_table('processed_ciks')} "
                    "WHERE has_balance OR has_income OR has_cash_flow ORDER BY cik"
                ).fetchall()
            ]
        elif not only_ciks:
            return stats
        else:
            cik_list = sorted(only_ciks)

        total = len(cik_list)
        print(f"[standardized] start cik_count={total:,}", flush=True)
        t0 = time.time()

        for cik in cik_list:
            try:
                facts_json = _load_company_facts_from_conn(conn, cik)
                result = _resolve_standardized_with_fallback(cik, facts_json)
                rows = _standardized_rows_for_result(cik, result)
            except Exception as e_resolve:
                print(f"[standardized] cik={cik} resolve FAILED: {e_resolve!r} — skipping", flush=True)
                stats["ciks_processed"] += 1
                stats["ciks_empty"] += 1
                continue

            try:
                conn.begin()
                _delete_and_insert_rows(conn, _table("standardized_statements"), cik, rows, _STANDARDIZED_SCHEMA)
                conn.commit()
            except Exception as e_insert:
                conn.rollback()
                print(f"[standardized] cik={cik} INSERT FAILED: {e_insert!r}", flush=True)
                raise

            stats["ciks_processed"] += 1
            stats["rows_inserted"] += len(rows)
            if not rows:
                stats["ciks_empty"] += 1

            if stats["ciks_processed"] % 200 == 0 or stats["ciks_processed"] == total:
                el = max(time.time() - t0, 1e-6)
                rate = stats["ciks_processed"] / el
                eta = (total - stats["ciks_processed"]) / rate if rate > 0 else 0.0
                print(
                    f"[standardized] progress {stats['ciks_processed']:,}/{total:,} "
                    f"({100 * stats['ciks_processed'] / max(total, 1):.0f}%)  rows={stats['rows_inserted']:,}  "
                    f"{rate:.0f} co/s  elapsed={el:.0f}s  ETA~{eta:.0f}s",
                    flush=True,
                )
    finally:
        _finalize(conn)

    print(
        f"[standardized] done ciks_processed={stats['ciks_processed']:,}"
        f"  rows_inserted={stats['rows_inserted']:,}  ciks_empty={stats['ciks_empty']:,}",
        flush=True,
    )
    return stats


def _fetch_company_tickers() -> list[dict[str, Any]]:
    raw = json.loads(_http_get(COMPANY_TICKERS_URL))
    if isinstance(raw, dict):
        entries = [v for _, v in sorted(raw.items(), key=lambda kv: int(kv[0]) if kv[0].isdigit() else 0)]
    else:
        entries = list(raw)

    out: list[dict[str, Any]] = []
    for rank, entry in enumerate(entries):
        if not isinstance(entry, dict):
            continue
        entry = cast(dict[str, Any], entry)
        cik = _zpad_cik(entry.get("cik_str") or entry.get("cik"))
        ticker = entry.get("ticker")
        if cik is None or not ticker:
            continue
        out.append(
            {
                "cik": cik,
                "ticker": str(ticker).upper().replace(".", "-"),
                "name": entry.get("title") or entry.get("name") or "",
                "is_primary": True,
                "rank": rank,
            }
        )
    return out


def _fetch_company_tickers_mf() -> list[dict[str, Any]]:
    raw = json.loads(_http_get(COMPANY_TICKERS_MF_URL))
    fields = raw.get("fields") or []
    data = raw.get("data") or []
    if not fields or not data:
        return []
    index = {name: i for i, name in enumerate(fields)}

    def _get(row: list[Any], name: str):
        i = index.get(name)
        return row[i] if i is not None and i < len(row) else None

    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in data:
        if not isinstance(row, list):
            continue
        cik = _zpad_cik(_get(row, "cik"))
        class_id = _get(row, "classId") or _get(row, "class_id")
        if cik is None or not class_id or class_id in seen:
            continue
        seen.add(class_id)
        out.append(
            {
                "cik": cik,
                "series_id": _get(row, "seriesId") or _get(row, "series_id") or "",
                "class_id": class_id,
                "symbol": (_get(row, "symbol") or "").upper().replace(".", "-"),
            }
        )
    return out


def _fetch_entities() -> list[dict[str, Any]]:
    raw = _http_get(CIK_LOOKUP_URL).decode("latin-1")
    out: list[dict[str, Any]] = []
    for line in raw.splitlines():
        if ":" not in line:
            continue
        head, sep, tail = line.rstrip(":").rpartition(":")
        if not sep:
            continue
        cik = _zpad_cik(tail.strip())
        if cik is None:
            continue
        name = head.strip()
        if not name:
            continue
        out.append({"cik": cik, "entity_name": name})
    return out


def ingest_cross_reference(db_path: str | None = None) -> dict[str, int]:
    print("[xref] ingest start", flush=True)
    conn = _connect(db_path)
    tickers = _fetch_company_tickers()
    funds = _fetch_company_tickers_mf()
    entities = _fetch_entities()
    print(f"[xref] fetched tickers={len(tickers):,} funds={len(funds):,} entities={len(entities):,}", flush=True)

    if tickers:
        print(f"[xref] replacing ticker rows for {len({row['cik'] for row in tickers}):,} ciks", flush=True)
    _replace_rows_for_ciks(conn, _table("tickers"), tickers, _TICKERS_SCHEMA)
    if funds:
        print(f"[xref] replacing fund rows for {len({row['cik'] for row in funds}):,} ciks", flush=True)
    _replace_rows_for_ciks(conn, _table("funds"), funds, _FUNDS_SCHEMA)
    if entities:
        print(f"[xref] replacing entity rows for {len({row['cik'] for row in entities}):,} ciks", flush=True)
    _replace_rows_for_ciks(conn, _table("entities"), entities, _ENTITIES_SCHEMA)
    print("[xref] ingest done", flush=True)
    _finalize(conn)
    return {"tickers": len(tickers), "funds": len(funds), "entities": len(entities)}


def ingest_multi_cik_overrides(db_path: str | None = None) -> int:
    print("[overrides] ingest start", flush=True)
    conn = _connect(db_path)
    rows: list[dict[str, Any]] = []
    for ticker, ciks in MULTI_CIK_TICKERS.items():
        for priority, cik in enumerate(ciks):
            rows.append({"ticker": ticker, "cik": cik, "priority": priority})

    print(f"[overrides] prepared rows={len(rows):,}", flush=True)

    if rows:
        print(f"[overrides] replacing ciks={len({row['cik'] for row in rows}):,}", flush=True)
    _replace_rows_for_ciks(conn, _table("multi_cik_tickers"), rows, _MULTI_CIK_SCHEMA)
    print(f"[overrides] ingest done rows={len(rows):,}", flush=True)
    _finalize(conn)
    return len(rows)


def ingest_exchange_rates(db_path: str | None = None, lookback_days: int = 3650) -> dict[str, int]:
    """Refresh the exchange_rates table from ECB reference rates.

    Idempotent: ``load_exchange_rates`` replaces the table atomically (it deletes
    and reloads in one transaction only after the ECB fetch succeeds), so a failed
    refresh leaves the existing rates intact instead of emptying the table.
    ECB publishes ~30 major currencies; statements in any currency ECB does not
    cover stay unconverted and are excluded from USD aggregations by the read-side
    queries.
    """
    from sec_app.db.exchange_rates import load_exchange_rates  # pylint: disable=import-outside-toplevel

    print("[rates] ingest start", flush=True)
    conn = _connect(db_path)
    try:
        load_exchange_rates(conn, lookback_days=lookback_days)
        count = conn.execute(f"SELECT COUNT(*) FROM {_table('exchange_rates')}").fetchone()
    finally:
        _finalize(conn)
    rows = int(count[0]) if count else 0
    print(f"[rates] ingest done rows={rows:,}", flush=True)
    return {"rows": rows}


@_with_update_lock
def run_update(
    companyfacts_zip: str | Path | None = None,
    companyfacts_dir: str | Path | None = None,
    submissions_zip: str | Path | None = None,
    db_path: str | None = None,
    workers: int = 4,
    download_from_sec: bool = False,
    download_dest: str | Path | None = None,
    skip_submissions: bool = False,
) -> dict[str, int]:
    del companyfacts_dir, workers

    if companyfacts_zip is not None or submissions_zip is not None:
        raise ValueError("Local input paths are not supported")

    if not download_from_sec:
        raise ValueError("Set download_from_sec=True")

    max_changed_ciks = int(os.environ.get("SEC_MAX_CHANGED_CIKS", "2000"))
    allow_large_update = os.environ.get("SEC_ALLOW_LARGE_UPDATE", "0") == "1"

    print("[update] start", flush=True)
    print("[update] fetching ticker/fund/entity lists", flush=True)
    ingest_cross_reference(db_path=db_path)
    ingest_multi_cik_overrides(db_path=db_path)

    conn = _connect(db_path)

    print("[update] downloading companyfacts", flush=True)
    facts_zip = download_companyfacts_zip(Path(download_dest) / "companyfacts.zip" if download_dest else None)
    valid_fact_ciks = _statement_ciks(conn)
    print(f"[update] statement CIK universe size={len(valid_fact_ciks):,}", flush=True)

    changed_sub_ciks: set[str] = set()
    sub_stats = {"files_processed": 0, "refreshed": 0, "submissions": 0, "dei_facts": 0}
    if not skip_submissions:
        print("[update] downloading submissions", flush=True)
        subs_zip = download_submissions_zip(Path(download_dest) / "submissions.zip" if download_dest else None)
        print("[update] scanning submissions for diffs (statement CIKs only)", flush=True)
        existing_sub_hashes = _existing_payload_hashes(conn, _table("submissions"))

        for cik, data, mtime in _iter_submissions_zip(subs_zip, cik_filter=valid_fact_ciks):
            sub_stats["files_processed"] += 1
            payload_json = json.dumps(data, separators=(",", ":"), sort_keys=True).encode("utf-8")
            payload_bytes = gzip.compress(payload_json, compresslevel=6, mtime=0)
            payload_hash = hashlib.sha256(payload_json).hexdigest()
            if existing_sub_hashes.get(cik) == payload_hash:
                print(f"[update:subs] cik={cik} unchanged", flush=True)
                continue

            changed_sub_ciks.add(cik)
            existing_sub_hashes[cik] = payload_hash
            print(f"[update:subs] cik={cik} changed hash={payload_hash[:12]} mtime={mtime.isoformat()}", flush=True)

        print(
            f"[update:subs] scan done checked={sub_stats['files_processed']:,} changed={len(changed_sub_ciks):,}",
            flush=True,
        )

        if not allow_large_update and len(changed_sub_ciks) > max_changed_ciks:
            raise RuntimeError(
                f"Safety stop: changed_sub_ciks={len(changed_sub_ciks):,} exceeds SEC_MAX_CHANGED_CIKS={max_changed_ciks:,}. "
                "Set SEC_ALLOW_LARGE_UPDATE=1 to override."
            )

        if changed_sub_ciks:
            print(f"[update:subs] applying changes for {len(changed_sub_ciks):,} ciks", flush=True)
            for cik, data, mtime in _iter_submissions_zip(subs_zip, cik_filter=changed_sub_ciks):
                print(f"[update:subs] cik={cik} apply begin", flush=True)
                payload_json = json.dumps(data, separators=(",", ":"), sort_keys=True).encode("utf-8")
                payload_bytes = gzip.compress(payload_json, compresslevel=6, mtime=0)
                submissions = [{"cik": cik, "payload": payload_bytes, "source_mtime": mtime}]
                tag_meta: list[dict[str, Any]] = []
                facts: list[dict[str, Any]] = []

                end_date = mtime.date()
                for tag, source_key, description in (
                    ("EntitySicCode", "sic", "Standard Industrial Classification (SIC) Code"),
                ):
                    raw = data.get(source_key)
                    if raw in (None, ""):
                        continue
                    try:
                        val_f = float(str(raw))
                    except (TypeError, ValueError):
                        continue
                    tag_meta.append(
                        {"cik": cik, "namespace": "dei", "tag": tag, "label": tag, "description": description}
                    )
                    facts.append(
                        {
                            "cik": cik,
                            "namespace": "dei",
                            "tag": tag,
                            "unit": "pure",
                            "start": None,
                            "end": end_date,
                            "val": val_f,
                            "val_text": None,
                            "accn": None,
                            "fy": None,
                            "fp": None,
                            "form": "submissions",
                            "filed": end_date,
                            "frame": None,
                        }
                    )
                for tag, source_key, description in (
                    ("EntityType", "entityType", "Entity Type"),
                    ("EntityFilerCategory", "category", "Entity Filer Category"),
                    ("EntitySicDescription", "sicDescription", "SIC Industry Description"),
                    ("EntityFiscalYearEnd", "fiscalYearEnd", "Fiscal Year End (MMDD)"),
                    ("EntityStateOfIncorporation", "stateOfIncorporation", "State of Incorporation"),
                    ("EntityName", "name", "Registrant Name"),
                    ("EntityEin", "ein", "Employer Identification Number"),
                    ("EntityLei", "lei", "Legal Entity Identifier"),
                ):
                    raw = data.get(source_key)
                    if raw in (None, ""):
                        continue
                    tag_meta.append(
                        {"cik": cik, "namespace": "dei", "tag": tag, "label": tag, "description": description}
                    )
                    facts.append(
                        {
                            "cik": cik,
                            "namespace": "dei",
                            "tag": tag,
                            "unit": "string",
                            "start": None,
                            "end": end_date,
                            "val": None,
                            "val_text": str(raw),
                            "accn": None,
                            "fy": None,
                            "fp": None,
                            "form": "submissions",
                            "filed": end_date,
                            "frame": None,
                        }
                    )

                try:
                    print(f"[update:subs] cik={cik} tx begin", flush=True)
                    conn.begin()
                    conn.execute(f"DELETE FROM {_table('submissions')} WHERE cik = ?", [cik])
                    conn.execute(f"DELETE FROM {_table('facts')} WHERE cik = ? AND namespace = 'dei'", [cik])
                    conn.execute(f"DELETE FROM {_table('tag_meta')} WHERE cik = ? AND namespace = 'dei'", [cik])
                    _insert_arrow(conn, _table("submissions"), submissions, _SUBMISSIONS_SCHEMA)
                    _insert_arrow(conn, _table("tag_meta"), tag_meta, _TAG_META_SCHEMA)
                    _insert_arrow(conn, _table("facts"), facts, _FACTS_SCHEMA)
                    conn.commit()
                    print(
                        f"[update:subs] cik={cik} tx commit submissions={len(submissions):,} tag_meta={len(tag_meta):,} dei_facts={len(facts):,}",
                        flush=True,
                    )
                except Exception as e_sub_apply:
                    conn.rollback()
                    print(f"[update:subs] cik={cik} tx rollback error={e_sub_apply!r}", flush=True)
                    raise

                sub_stats["refreshed"] += 1
                sub_stats["submissions"] += len(submissions)
                sub_stats["dei_facts"] += len(facts)

            print(
                f"[update:subs] applied refreshed={sub_stats['refreshed']:,} submissions={sub_stats['submissions']:,} dei_facts={sub_stats['dei_facts']:,}",
                flush=True,
            )

    print("[update] scanning companyfacts for diffs", flush=True)
    existing_facts = _existing_hashes(conn, _table("companies"))
    changed_facts: set[str] = set()
    fact_stats = {"files_checked": 0, "changed": 0, "companies": 0, "tag_meta": 0, "facts": 0}

    if skip_submissions:
        print("[update:facts] scanning all companyfacts for diffs", flush=True)
        facts_iter = _iter_companyfacts_zip(facts_zip)
    elif changed_sub_ciks:
        print(f"[update:facts] limiting companyfacts scan to {len(changed_sub_ciks):,} changed CIKs", flush=True)
        facts_iter = _iter_companyfacts_zip(facts_zip, only_ciks=changed_sub_ciks)
    else:
        print("[update:facts] no changed CIKs from submissions; skipping companyfacts refresh", flush=True)
        facts_iter = iter(())

    for cik, payload, mtime in facts_iter:
        fact_stats["files_checked"] += 1
        print(f"[update:facts] cik={cik} begin idx={fact_stats['files_checked']:,} mtime={mtime.isoformat()}", flush=True)
        content_hash = hashlib.sha256(payload).hexdigest()
        if existing_facts.get(cik) == content_hash:
            print(f"[update:facts] cik={cik} unchanged", flush=True)
            continue

        changed_facts.add(cik)
        fact_stats["changed"] += 1
        print(f"[update:facts] cik={cik} changed hash={content_hash[:12]} mtime={mtime.isoformat()}", flush=True)

        try:
            data = json.loads(payload)
            if not isinstance(data, dict):
                data = {}
        except json.JSONDecodeError:
            data = {}

        cik_norm = _zpad_cik(data.get("cik")) or cik
        companies = [
            {
                "cik": cik_norm,
                "entity_name": data.get("entityName") or "",
                "source_mtime": mtime,
                "source_content_hash": content_hash,
            }
        ]
        tag_meta: list[dict[str, Any]] = []
        facts: list[dict[str, Any]] = []

        for namespace, tag_dict in (data.get("facts") or {}).items():
            if not isinstance(tag_dict, dict):
                continue
            for tag, payload_obj in tag_dict.items():
                if not isinstance(payload_obj, dict):
                    continue
                tag_meta.append(
                    {
                        "cik": cik_norm,
                        "namespace": namespace,
                        "tag": tag,
                        "label": payload_obj.get("label") or "",
                        "description": payload_obj.get("description") or "",
                    }
                )
                for unit, periods in (payload_obj.get("units") or {}).items():
                    if not isinstance(periods, list):
                        continue
                    for period in periods:
                        if not isinstance(period, dict):
                            continue
                        val = period.get("val")
                        try:
                            val_f = float(val) if val is not None else None
                        except (TypeError, ValueError):
                            val_f = None
                        fy = period.get("fy")
                        try:
                            fy_i = int(fy) if fy is not None else None
                        except (TypeError, ValueError):
                            fy_i = None
                        facts.append(
                            {
                                "cik": cik_norm,
                                "namespace": namespace,
                                "tag": tag,
                                "unit": unit,
                                "start": _parse_date(period.get("start")),
                                "end": _parse_date(period.get("end")),
                                "val": val_f,
                                "val_text": None,
                                "accn": period.get("accn"),
                                "fy": fy_i,
                                "fp": period.get("fp"),
                                "form": period.get("form"),
                                "filed": _parse_date(period.get("filed")),
                                "frame": period.get("frame"),
                            }
                        )

        try:
            conn.begin()
            conn.execute(f"DELETE FROM {_table('companies')} WHERE cik = ?", [cik_norm])
            conn.execute(f"DELETE FROM {_table('tag_meta')} WHERE cik = ?", [cik_norm])
            conn.execute(f"DELETE FROM {_table('facts')} WHERE cik = ?", [cik_norm])
            _insert_arrow(conn, _table("companies"), companies, _COMPANIES_SCHEMA)
            _insert_arrow(conn, _table("tag_meta"), tag_meta, _TAG_META_SCHEMA)
            _insert_arrow(conn, _table("facts"), facts, _FACTS_SCHEMA)
            conn.commit()
        except Exception as e_fact_apply:
            conn.rollback()
            print(f"[update:facts] cik={cik_norm} tx rollback error={e_fact_apply!r}", flush=True)
            raise
        print(
            f"[update:facts] cik={cik} inserted companies={len(companies)} tag_meta={len(tag_meta)} facts={len(facts)}",
            flush=True,
        )
        fact_stats["companies"] += len(companies)
        fact_stats["tag_meta"] += len(tag_meta)
        fact_stats["facts"] += len(facts)

    print(
        f"[update:facts] scan done checked={fact_stats['files_checked']:,} changed={fact_stats['changed']:,}",
        flush=True,
    )

    if changed_facts:
        print(f"[update] recomputing processed_ciks for {len(changed_facts):,} ciks", flush=True)
        compute_processed_ciks(db_path=db_path, only_ciks=changed_facts)

    # standardized_statements is derived purely from companyfacts: financial
    # line items, their reporting currency, and company_type (which detect_type
    # classifies from financial-tag presence only). It does NOT depend on
    # submissions/dei metadata (SIC, entityType, fiscalYearEnd, ...). So only
    # CIKs whose facts actually changed need rematerializing — re-running it for
    # every CIK whose submissions index changed (a new 8-K/Form 4/13F, etc.)
    # just rewrites byte-identical rows.
    ciks_to_materialize = changed_facts
    if ciks_to_materialize:
        print(f"[update] materializing standardized_statements for {len(ciks_to_materialize):,} changed ciks", flush=True)
        std_stats = materialize_standardized_statements(db_path=db_path, only_ciks=ciks_to_materialize)
    else:
        std_stats = {"rows_inserted": 0}

    _finalize(conn)

    # Refresh FX rates every update — they change daily and feed the USD
    # normalization in the ranking queries.
    try:
        ingest_exchange_rates(db_path=db_path)
    except Exception as e_rates:  # pylint: disable=broad-except
        print(f"[update] exchange-rate refresh failed (non-fatal) error={e_rates!r}", flush=True)

    print("[update] done", flush=True)

    return {
        "changed_facts": fact_stats.get("changed", 0),
        "changed_subs": sub_stats.get("refreshed", 0),
        "standardized_rows": std_stats.get("rows_inserted", 0),
    }
