"""Pluggable storage backend for the SEC ingest pipeline.

Two backends are supported:

* ``duckdb`` (default, local dev) — writes to a single DuckDB file at
  ``SEC_DB_PATH``. Bulk inserts go through the native PyArrow integration.
* ``dolt`` (CI / nightly DoltHub refresh) — connects via MySQL protocol to a
  ``dolt sql-server`` running on top of a cloned Dolt repository. All writes go
  through this connection so the running server stays authoritative; commits and
  pushes are issued via ``CALL DOLT_*`` from the workflow itself.

Select the backend with ``SEC_DB_BACKEND=duckdb|dolt``. The Dolt backend reads
``DOLT_SQL_HOST``, ``DOLT_SQL_PORT``, ``DOLT_SQL_USER``, ``DOLT_SQL_PASSWORD``
and ``DOLT_SQL_DB`` (defaults: 127.0.0.1 / 3306 / root / empty / sec).

The wrapper exposes a small DuckDB-compatible surface (``execute`` with ``?``
parameters, ``register``/``unregister`` for ad-hoc Arrow views, ``bulk_insert``
for typed row batches) so the ingest code in :mod:`openbb_sec.db.ingest` can
stay backend-agnostic.
"""

from __future__ import annotations

import os
from datetime import date, datetime
from typing import Any, Sequence

import pyarrow as pa


# Tables whose Dolt schema carries a surrogate ``id BIGINT`` PK that is NOT
# present in the DuckDB schema. ``DoltConn.bulk_insert`` allocates ids above the
# current high-water mark so unchanged rows keep their ids (and never produce a
# spurious diff) across runs.
DOLT_ID_TABLES = frozenset({"facts", "entities"})


def _backend_name() -> str:
    return (os.environ.get("SEC_DB_BACKEND") or "duckdb").strip().lower()


def using_dolt() -> bool:
    return _backend_name() == "dolt"


# --------------------------------------------------------------------------- #
# DuckDB backend
# --------------------------------------------------------------------------- #


class DuckDBConn:
    """Thin wrapper that exposes a stable surface over a DuckDB connection.

    The native DuckDB connection is also returned by ``raw()`` for the few
    places that need direct access (e.g. PyArrow registration).
    """

    dialect = "duckdb"

    def __init__(self, conn):
        self._conn = conn

    def raw(self):
        return self._conn

    def execute(self, sql: str, params: Sequence[Any] | None = None):
        if params is None:
            return self._conn.execute(sql)
        return self._conn.execute(sql, list(params))

    def register(self, view: str, arrow_table: pa.Table) -> None:
        self._conn.register(view, arrow_table)

    def unregister(self, view: str) -> None:
        self._conn.unregister(view)

    def bulk_insert(self, table: str, rows: list[dict[str, Any]], schema: pa.Schema) -> None:
        if not rows:
            return
        import uuid as _uuid

        view = f"_batch_{_uuid.uuid4().hex}"
        arr = pa.Table.from_pylist(rows, schema=schema)
        self._conn.register(view, arr)
        try:
            self._conn.execute(f"INSERT INTO {table} SELECT * FROM {view}")
        finally:
            self._conn.unregister(view)

    def executemany(self, sql: str, seq_of_params) -> None:
        for params in seq_of_params:
            self._conn.execute(sql, list(params))

    def create_temp_table(self, name: str, columns_sql: str) -> None:
        self._conn.execute(f"DROP TABLE IF EXISTS {name}")
        self._conn.execute(f"CREATE TEMP TABLE {name} ({columns_sql})")

    def begin(self) -> None:
        self._conn.execute("BEGIN")

    def commit(self) -> None:
        self._conn.execute("COMMIT")

    def rollback(self) -> None:
        self._conn.execute("ROLLBACK")

    def finalize(self) -> None:
        try:
            self._conn.execute("CHECKPOINT")
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
    """DuckDB-shaped result wrapping a PyMySQL cursor."""

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
    """Convert DuckDB ``?`` placeholders to MySQL ``%s``.

    PyMySQL also treats ``%`` as a format char in non-parameterised queries, so
    any literal ``%`` already in the SQL must be doubled when params are
    supplied. The ingest code does not use ``%`` literals in query text, so a
    naive replacement is safe; we still escape defensively.
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
    """DuckDB-shaped wrapper over a PyMySQL connection to ``dolt sql-server``.

    All writes flow through this single connection so the long-running sql-server
    process remains the authoritative writer for the cloned Dolt repository.
    """

    dialect = "dolt"

    def __init__(self, conn):
        self._conn = conn
        # Map of registered "view" name -> in-memory rows. DuckDB allows joining
        # against a PyArrow table by name; Dolt has no such facility, so the
        # ingest code that registers a view either (a) follows up with a SELECT
        # against it for which there is no Dolt equivalent (and must be
        # rewritten), or (b) is replaced with bulk_insert. We keep this dict for
        # the rare case where the same row set is needed in two SQL stmts —
        # those callers materialise into a TEMP table via ``create_temp_table``
        # + ``bulk_insert`` instead.
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
        raise RuntimeError("SEC_DB_BACKEND=dolt requires pymysql. Install with: pip install '.[dolt]'") from err

    host = os.environ.get("DOLT_SQL_HOST", "127.0.0.1")
    port = int(os.environ.get("DOLT_SQL_PORT", "3306"))
    user = os.environ.get("DOLT_SQL_USER", "root")
    password = os.environ.get("DOLT_SQL_PASSWORD", "")
    database = os.environ.get("DOLT_SQL_DB", "sec")

    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        # autocommit=True matches DuckDB's default behaviour (each unwrapped
        # statement commits). The ingest code wraps multi-statement updates in
        # explicit ``begin()``/``commit()``/``rollback()`` blocks via the
        # ``DoltConn`` wrapper, which issues ``START TRANSACTION`` and so still
        # gives us atomic per-CIK writes even with autocommit on.
        autocommit=True,
        charset="utf8mb4",
        local_infile=False,
        connect_timeout=60,
        read_timeout=600,
        write_timeout=600,
    )
    print(f"[db] dolt connected host={host} port={port} db={database}", flush=True)
    return DoltConn(conn)


def table_name(unqualified: str) -> str:
    """Return the fully qualified table name appropriate for the active backend.

    DuckDB uses the ``sec.<name>`` schema; Dolt uses bare table names per
    ``dolt/schema.sql``. ``unqualified`` may already be qualified (it gets
    passed through unchanged in that case).
    """
    if "." in unqualified:
        unqualified = unqualified.split(".")[-1]
    if using_dolt():
        return unqualified
    from openbb_sec.db.schema import DATABASE_NAME

    return f"{DATABASE_NAME}.{unqualified}"
