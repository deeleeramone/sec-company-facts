"""Dolt storage backend for the SEC store.

Both reads and writes go over the MySQL protocol to a ``dolt sql-server``.
Connection details come from ``DOLT_SQL_HOST``, ``DOLT_SQL_PORT``,
``DOLT_SQL_USER``, ``DOLT_SQL_PASSWORD`` and ``DOLT_SQL_DB``.
"""

from __future__ import annotations

import os
import time
from datetime import date, datetime
from typing import Any, Sequence

import pyarrow as pa


# Query logging; disabled with SEC_LOG_SQL=0.
def _sql_logging_enabled() -> bool:
    return os.environ.get("SEC_LOG_SQL", "1") != "0"


def _log_sql(one_line_sql: str, params: Sequence[Any] | None) -> None:
    if not _sql_logging_enabled():
        return
    if len(one_line_sql) > 2000:
        one_line_sql = one_line_sql[:2000] + " …[truncated]"
    suffix = f" -- params={list(params)}" if params else ""
    print(f"[sql] > {one_line_sql}{suffix}", flush=True)


def _log_sql_done(rowcount: int, elapsed_s: float) -> None:
    if not _sql_logging_enabled():
        return
    rows = rowcount if rowcount is not None and rowcount >= 0 else "?"
    print(f"[sql] < {rows} rows in {elapsed_s * 1000:.1f} ms", flush=True)


# Tables whose Dolt schema carries a surrogate ``id BIGINT`` PK. ``DoltConn``
# allocates ids above the current high-water mark so unchanged rows keep their
# ids (and never produce a spurious diff) across runs.
DOLT_ID_TABLES = frozenset({"facts", "entities"})


# --------------------------------------------------------------------------- #
# Dolt backend (MySQL protocol)
# --------------------------------------------------------------------------- #

# Maximum number of rows per multi-row INSERT batch against the Dolt sql-server.
# Each row in ``facts`` is ~16 small scalar columns; 1000 rows / batch keeps the
# packet well under MySQL's default max_allowed_packet (4MB) while amortising
# round-trip cost. Tuned down for ``submissions`` because each row carries a
# gzip BLOB.
_DEFAULT_BATCH = 1000
_BLOB_BATCH = 25


class _DoltResult:
    """Result wrapper over a PyMySQL cursor."""

    __slots__ = ("_cur",)

    def __init__(self, cur):
        self._cur = cur

    def fetchone(self):
        return self._cur.fetchone()

    def fetchall(self):
        return self._cur.fetchall()

    def fetchmany(self, n):
        return self._cur.fetchmany(n)

    def __iter__(self):
        return iter(self._cur)


def _translate_placeholders(sql: str) -> str:
    """Convert ``?`` placeholders to MySQL ``%s``.

    PyMySQL treats ``%`` as a format char when params are supplied, so any
    literal ``%`` already in the SQL is doubled defensively.
    """
    if "%" in sql:
        sql = sql.replace("%", "%%")
    return sql.replace("?", "%s")


def _pyval(v):
    """Coerce values to types PyMySQL/Dolt accept directly."""
    if v is None:
        return None
    if isinstance(v, (bytes, bytearray, memoryview)):
        return bytes(v) if not isinstance(v, bytes) else v
    if isinstance(v, bool):
        # MySQL stores BOOLEAN as TINYINT; pass an int explicitly.
        return 1 if v else 0
    if isinstance(v, datetime):
        # PyMySQL formats datetime/date natively.
        return v
    if isinstance(v, date):
        return v
    return v


class DoltConn:
    """Write wrapper over a PyMySQL connection to ``dolt sql-server``.

    All writes flow through this single connection so the long-running sql-server
    process remains the authoritative writer for the Dolt repository.
    """

    dialect = "dolt"

    def __init__(self, conn):
        self._conn = conn
        # Cache of registered "view" name -> in-memory rows, picked up by a
        # follow-up bulk_insert. Callers needing a JOIN materialise into a TEMP
        # table via create_temp_table + bulk_insert instead.
        self._registered: dict[str, list[dict[str, Any]]] = {}
        self._id_high_water: dict[str, int] = {}

    def raw(self):
        return self._conn

    # ---- core SQL ----------------------------------------------------- #

    def execute(self, sql: str, params: Sequence[Any] | None = None):
        cur = self._conn.cursor()
        sql_my = _translate_placeholders(sql)
        if params is None:
            cur.execute(sql_my)
        else:
            cur.execute(sql_my, tuple(_pyval(p) for p in params))
        return _DoltResult(cur)

    def register(self, view: str, arrow_table: pa.Table) -> None:
        # Not supported as a real SQL view against Dolt; we cache the rows so a
        # follow-up bulk_insert can pick them up. Callers that need to JOIN
        # against the data must instead materialise into a TEMP table.
        self._registered[view] = arrow_table.to_pylist()

    def unregister(self, view: str) -> None:
        self._registered.pop(view, None)

    # ---- bulk insert -------------------------------------------------- #

    def _next_id_block(self, table_unqualified: str, count: int) -> int:
        """Allocate ``count`` ids starting at MAX(id)+1; returns the first id.

        We cache the high-water mark per table so a long-running ingest does not
        re-query for every batch — subsequent calls just bump the cache.
        """
        hw = self._id_high_water.get(table_unqualified)
        if hw is None:
            cur = self._conn.cursor()
            cur.execute(f"SELECT COALESCE(MAX(id),0) FROM {table_unqualified}")
            row = cur.fetchone()
            hw = int(row[0] if row else 0)
        start = hw + 1
        self._id_high_water[table_unqualified] = hw + count
        return start

    def bulk_insert(self, table: str, rows: list[dict[str, Any]], schema: pa.Schema) -> None:
        if not rows:
            return
        # Table arrives qualified or unqualified; for Dolt we want unqualified.
        unqualified = table.split(".")[-1]
        names = [f.name for f in schema]

        is_blob_table = any(isinstance(rows[0].get(n), (bytes, bytearray, memoryview)) for n in names)
        batch_size = _BLOB_BATCH if is_blob_table else _DEFAULT_BATCH

        needs_id = unqualified in DOLT_ID_TABLES
        col_list = (["id"] + names) if needs_id else list(names)
        col_sql = ", ".join(f"`{c}`" if c in {"end", "start", "rank"} else c for c in col_list)
        placeholders = "(" + ",".join(["%s"] * len(col_list)) + ")"

        cur = self._conn.cursor()
        n = len(rows)
        for i in range(0, n, batch_size):
            chunk = rows[i : i + batch_size]
            if needs_id:
                base = self._next_id_block(unqualified, len(chunk))
                values: list[tuple] = []
                for j, row in enumerate(chunk):
                    values.append((base + j, *[_pyval(row.get(c)) for c in names]))
            else:
                values = [tuple(_pyval(row.get(c)) for c in names) for row in chunk]
            sql = f"INSERT INTO {unqualified} ({col_sql}) VALUES " + ",".join([placeholders] * len(values))
            cur.execute(sql, [v for tup in values for v in tup])

    def create_temp_table(self, name: str, columns_sql: str) -> None:
        cur = self._conn.cursor()
        cur.execute(f"DROP TEMPORARY TABLE IF EXISTS {name}")
        cur.execute(f"CREATE TEMPORARY TABLE {name} ({columns_sql})")

    def executemany(self, sql: str, seq_of_params) -> None:
        seq = list(seq_of_params)
        if not seq:
            return
        cur = self._conn.cursor()
        sql_my = _translate_placeholders(sql)
        cur.executemany(sql_my, [tuple(_pyval(p) for p in params) for params in seq])

    # ---- txn / lifecycle --------------------------------------------- #

    def begin(self) -> None:
        self._conn.cursor().execute("START TRANSACTION")

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def finalize(self) -> None:
        # Nothing to checkpoint on Dolt; commit/push happens externally via
        # ``CALL DOLT_ADD/COMMIT/PUSH`` from the workflow.
        try:
            self._conn.commit()
        except Exception:
            pass
        try:
            self._conn.close()
        except Exception:
            pass

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:
            pass


# --------------------------------------------------------------------------- #
# Connection factories
# --------------------------------------------------------------------------- #


def connect_dolt() -> DoltConn:
    try:
        import pymysql  # type: ignore
    except ImportError as err:  # pragma: no cover - import guard
        raise RuntimeError("Writes require pymysql. Install with: pip install pymysql") from err

    host = os.environ.get("DOLT_SQL_HOST", "127.0.0.1")
    port = int(os.environ.get("DOLT_SQL_PORT", "3306"))
    user = os.environ.get("DOLT_SQL_USER", "root")
    password = os.environ.get("DOLT_SQL_PASSWORD", "")
    database = os.environ.get("DOLT_SQL_DB", "sec_company_facts")

    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        # Per-CIK atomicity comes from explicit START TRANSACTION blocks in the
        # ingest code, so unwrapped statements can autocommit.
        autocommit=True,
        charset="utf8mb4",
        local_infile=False,
        connect_timeout=60,
        read_timeout=600,
        write_timeout=600,
        # ANSI_QUOTES so "end" / "start" / "rank" are read as identifiers.
        init_command="SET SESSION sql_mode='ANSI_QUOTES'",
    )
    print(f"[db] dolt connected host={host} port={port} db={database}", flush=True)
    return DoltConn(conn)


def table_name(unqualified: str) -> str:
    """Return the bare table name (Dolt uses no schema prefix)."""
    return unqualified.split(".")[-1]


def connect_read(db_path: str | None = None):
    """Read-only connection to the dolt sql-server. ``db_path`` is ignored."""
    try:
        import pymysql  # type: ignore  # pylint: disable=import-outside-toplevel
    except ImportError as err:  # pragma: no cover - import guard
        raise RuntimeError("Reads require pymysql. Install with: pip install pymysql") from err

    host = os.environ.get("DOLT_SQL_HOST", "127.0.0.1")
    port = int(os.environ.get("DOLT_SQL_PORT", "3306"))
    user = os.environ.get("DOLT_SQL_USER", "root")
    password = os.environ.get("DOLT_SQL_PASSWORD", "")
    database = os.environ.get("DOLT_SQL_DB", "sec_company_facts")

    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        charset="utf8mb4",
        autocommit=True,
        connect_timeout=60,
        read_timeout=600,
        write_timeout=600,
        init_command="SET SESSION sql_mode='ANSI_QUOTES'",
    )
    return _DoltReadConn(conn)


class _DoltReadConn:
    """Read-only wrapper over a PyMySQL connection to the dolt sql-server."""

    __slots__ = ("_conn",)

    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql: str, params: Sequence[Any] | None = None):
        cur = self._conn.cursor()
        one_line = " ".join(sql.split())
        _log_sql(one_line, params)
        t0 = time.perf_counter()
        if params is None:
            cur.execute(sql)
        else:
            cur.execute(_translate_placeholders(sql), tuple(_pyval(p) for p in params))
        _log_sql_done(cur.rowcount, time.perf_counter() - t0)
        return _DoltResult(cur)

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:
            pass
