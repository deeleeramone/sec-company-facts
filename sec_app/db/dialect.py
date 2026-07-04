from __future__ import annotations

import os


def backend() -> str:
    return os.environ.get("SEC_BACKEND", "dolt").strip().lower()


def is_duckdb() -> bool:
    return backend() == "duckdb"


def quote_literal(value) -> str:
    text = str(value)
    if is_duckdb():
        return "'" + text.replace("'", "''") + "'"
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def cast_uint(expr: str) -> str:
    return f"CAST({expr} AS UBIGINT)" if is_duckdb() else f"CAST({expr} AS UNSIGNED)"


def like_escape_suffix() -> str:
    return " ESCAPE '\\'" if is_duckdb() else ""


def table_exists(sess, name: str) -> bool:
    if is_duckdb():
        row = sess.execute(
            f"SELECT 1 FROM information_schema.tables WHERE table_name = {quote_literal(name)} LIMIT 1"
        ).fetchone()
    else:
        row = sess.execute(f"SHOW TABLES LIKE {quote_literal(name)}").fetchone()
    return row is not None
