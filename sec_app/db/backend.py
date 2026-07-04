from __future__ import annotations

import os
import time
from datetime import date, datetime
from typing import Any, Sequence

import pyarrow as pa


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


DOLT_ID_TABLES = frozenset({"facts", "facts_enc", "entities"})


_DEFAULT_BATCH = 1000
_BLOB_BATCH = 25


class _DoltResult:

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
    if "%" in sql:
        sql = sql.replace("%", "%%")
    return sql.replace("?", "%s")


def _pyval(v):
    if v is None:
        return None
    if isinstance(v, (bytes, bytearray, memoryview)):
        return bytes(v) if not isinstance(v, bytes) else v
    if isinstance(v, bool):
        return 1 if v else 0
    if isinstance(v, datetime):
        return v
    if isinstance(v, date):
        return v
    return v


class DoltConn:

    dialect = "dolt"

    def __init__(self, conn):
        self._conn = conn
        self._registered: dict[str, list[dict[str, Any]]] = {}
        self._id_high_water: dict[str, int] = {}

    def raw(self):
        return self._conn


    def execute(self, sql: str, params: Sequence[Any] | None = None):
        cur = self._conn.cursor()
        sql_my = _translate_placeholders(sql)
        if params is None:
            cur.execute(sql_my)
        else:
            cur.execute(sql_my, tuple(_pyval(p) for p in params))
        return _DoltResult(cur)

    def register(self, view: str, arrow_table: pa.Table) -> None:
        self._registered[view] = arrow_table.to_pylist()

    def unregister(self, view: str) -> None:
        self._registered.pop(view, None)


    def _next_id_block(self, table_unqualified: str, count: int) -> int:
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


    def begin(self) -> None:
        self._conn.cursor().execute("START TRANSACTION")

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def finalize(self) -> None:
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
        autocommit=True,
        charset="utf8mb4",
        local_infile=False,
        connect_timeout=60,
        read_timeout=600,
        write_timeout=600,
        init_command="SET SESSION sql_mode='ANSI_QUOTES'",
    )
    print(f"[db] dolt connected host={host} port={port} db={database}", flush=True)
    return DoltConn(conn)


def table_name(unqualified: str) -> str:
    return unqualified.split(".")[-1]


def connect_read(db_path: str | None = None):
    from sec_app.db.dialect import is_duckdb  # pylint: disable=import-outside-toplevel

    if is_duckdb():
        from sec_app.db.duckdb_backend import connect_read as _duck_connect_read  # pylint: disable=import-outside-toplevel

        return _duck_connect_read()

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


def data_version() -> str:
    from sec_app.db.dialect import is_duckdb  # pylint: disable=import-outside-toplevel

    if is_duckdb():
        from sec_app.db.duckdb_backend import data_version as _duck_data_version  # pylint: disable=import-outside-toplevel

        return _duck_data_version()

    sess = connect_read()
    try:
        row = sess.execute("SELECT dolt_hashof_db()").fetchone()
        return str(row[0]) if row and row[0] is not None else "unknown"
    finally:
        sess.close()


class _DoltReadConn:

    __slots__ = ("_conn",)

    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql: str, params: Sequence[Any] | None = None, *, stream: bool = False):
        if stream:
            import pymysql.cursors  # pylint: disable=import-outside-toplevel

            cur = self._conn.cursor(pymysql.cursors.SSCursor)
        else:
            cur = self._conn.cursor()
        one_line = " ".join(sql.split())
        _log_sql(one_line, params)
        t0 = time.perf_counter()
        if params is None:
            cur.execute(sql)
        else:
            cur.execute(_translate_placeholders(sql), tuple(_pyval(p) for p in params))
        _log_sql_done(-1 if stream else cur.rowcount, time.perf_counter() - t0)
        return _DoltResult(cur)

    def close(self) -> None:
        try:
            self._conn.close()
        except Exception:
            pass
