from __future__ import annotations

import json
import os
import re
import threading
import time
from pathlib import Path
from typing import Any, Sequence

from sec_app.db.backend import _log_sql, _log_sql_done, _pyval

_BACKTICK_IDENT = re.compile(r"`(\w+)`")

_lock = threading.Lock()
_conn = None
_data_version: str | None = None


def _parquet_dir() -> Path:
    return Path(os.environ.get("SEC_PARQUET_DIR", "/data/parquet"))


def _load_manifest(pdir: Path) -> dict:
    path = pdir / "manifest.json"
    if not path.exists():
        raise RuntimeError(f"parquet manifest not found at {path}")
    return json.loads(path.read_text())


def _quote_path(p: str) -> str:
    return "'" + p.replace("'", "''") + "'"


def _init():
    global _conn, _data_version
    if _conn is not None:
        return
    with _lock:
        if _conn is not None:
            return
        import duckdb  # pylint: disable=import-outside-toplevel

        pdir = _parquet_dir()
        manifest = _load_manifest(pdir)
        _data_version = str(manifest.get("data_version", "unknown"))

        conn = duckdb.connect(":memory:")
        threads = os.environ.get("DUCKDB_THREADS")
        if threads:
            conn.execute(f"SET threads = {int(threads)}")
        mem = os.environ.get("DUCKDB_MEMORY_LIMIT")
        if mem:
            conn.execute(f"SET memory_limit = '{mem}'")
        tmp = os.environ.get("DUCKDB_TEMP_DIR")
        if tmp:
            conn.execute(f"SET temp_directory = {_quote_path(tmp)}")

        conn.execute("CREATE OR REPLACE MACRO date_format(d, fmt) AS strftime(d, fmt)")

        for table, info in manifest.get("tables", {}).items():
            files = [str(pdir / f["name"]) for f in info.get("files", [])]
            if not files:
                continue
            file_list = ", ".join(_quote_path(f) for f in files)
            conn.execute(
                f'CREATE VIEW "{table}" AS SELECT * FROM read_parquet([{file_list}])'
            )
        _conn = conn
        print(f"[db] duckdb ready parquet_dir={pdir} version={_data_version}", flush=True)


def data_version() -> str:
    _init()
    return _data_version or "unknown"


class _DuckResult:

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
        while True:
            rows = self._cur.fetchmany(1000)
            if not rows:
                break
            for row in rows:
                yield row


class _DuckReadConn:

    __slots__ = ("_cur",)

    def __init__(self, cur):
        self._cur = cur

    def execute(self, sql: str, params: Sequence[Any] | None = None, *, stream: bool = False):
        sql_duck = _BACKTICK_IDENT.sub(r'"\1"', sql)
        one_line = " ".join(sql_duck.split())
        _log_sql(one_line, params)
        t0 = time.perf_counter()
        if params is None:
            self._cur.execute(sql_duck)
        else:
            self._cur.execute(sql_duck, tuple(_pyval(p) for p in params))
        _log_sql_done(-1, time.perf_counter() - t0)
        return _DuckResult(self._cur)

    def close(self) -> None:
        try:
            self._cur.close()
        except Exception:
            pass


def connect_read():
    _init()
    return _DuckReadConn(_conn.cursor())
