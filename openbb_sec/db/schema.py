"""DuckDB schema for the SEC store."""

from __future__ import annotations

DATABASE_NAME = "sec"

DDL: tuple[str, ...] = (
    f"CREATE SCHEMA IF NOT EXISTS {DATABASE_NAME}",
    # Small reference tables get a cik index, created with the (empty) table and
    # maintained as rows insert. The LARGE tables (facts ~100M+, tag_meta,
    # standardized_statements) intentionally have NO secondary index: DuckDB keeps
    # ART indexes resident in memory, which would not fit the container. Those
    # tables are written clustered by cik (one company at a time), so DuckDB's
    # min/max zonemaps already prune `WHERE cik = ?` scans without an index.
    f"""
    CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.companies (
        cik VARCHAR,
        entity_name VARCHAR,
        source_mtime TIMESTAMP,
        source_content_hash VARCHAR
    )
    """,
    f"CREATE INDEX IF NOT EXISTS idx_companies_cik ON {DATABASE_NAME}.companies (cik)",
    f"""
    CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.tag_meta (
        cik VARCHAR,
        namespace VARCHAR,
        tag VARCHAR,
        label VARCHAR,
        description VARCHAR
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.facts (
        cik VARCHAR,
        namespace VARCHAR,
        tag VARCHAR,
        unit VARCHAR,
        start DATE,
        "end" DATE,
        val DOUBLE,
        val_text VARCHAR,
        accn VARCHAR,
        fy INTEGER,
        fp VARCHAR,
        form VARCHAR,
        filed DATE,
        frame VARCHAR
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.tickers (
        cik VARCHAR,
        ticker VARCHAR,
        name VARCHAR,
        is_primary BOOLEAN,
        rank INTEGER
    )
    """,
    f"CREATE INDEX IF NOT EXISTS idx_tickers_cik ON {DATABASE_NAME}.tickers (cik)",
    f"""
    CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.funds (
        cik VARCHAR,
        series_id VARCHAR,
        class_id VARCHAR,
        symbol VARCHAR
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.entities (
        cik VARCHAR,
        entity_name VARCHAR
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.multi_cik_tickers (
        ticker VARCHAR,
        cik VARCHAR,
        priority INTEGER
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.submissions (
        cik VARCHAR,
        payload BLOB,
        source_mtime TIMESTAMP
    )
    """,
    f"CREATE INDEX IF NOT EXISTS idx_submissions_cik ON {DATABASE_NAME}.submissions (cik)",
    f"""
    CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.processed_ciks (
        cik VARCHAR,
        has_balance BOOLEAN,
        has_income BOOLEAN,
        has_cash_flow BOOLEAN,
        computed_at TIMESTAMP
    )
    """,
    f"CREATE INDEX IF NOT EXISTS idx_processed_ciks_cik ON {DATABASE_NAME}.processed_ciks (cik)",
    f"""
    CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.standardized_statements (
        cik VARCHAR,
        statement VARCHAR,
        period_ending DATE,
        fiscal_year INTEGER,
        fiscal_period VARCHAR,
        calendar_year INTEGER,
        calendar_period VARCHAR,
        frequency VARCHAR,
        tag VARCHAR,
        label VARCHAR,
        parent VARCHAR,
        sequence INTEGER,
        factor VARCHAR,
        balance VARCHAR,
        unit VARCHAR,
        val DOUBLE,
        currency VARCHAR,
        company_type VARCHAR
    )
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.exchange_rates (
        rate_date DATE,
        from_currency VARCHAR,
        to_currency VARCHAR,
        rate DOUBLE
    )
    """,
    f"""
    CREATE OR REPLACE VIEW {DATABASE_NAME}.primary_tickers AS
    SELECT cik, ticker, name, rank
    FROM {DATABASE_NAME}.tickers
    WHERE coalesce(is_primary, false)
    """,
    f"""
    CREATE OR REPLACE VIEW {DATABASE_NAME}.cik_canonical AS
    WITH per_ticker_primary AS (
        SELECT ticker, cik AS primary_cik
        FROM (
            SELECT ticker, cik, priority,
                   row_number() OVER (PARTITION BY ticker ORDER BY priority, cik) AS rn
            FROM {DATABASE_NAME}.multi_cik_tickers
        )
        WHERE rn = 1
    ),
    cik_to_primary AS (
        SELECT m.cik AS cik, p.primary_cik AS primary_cik
        FROM {DATABASE_NAME}.multi_cik_tickers m
        JOIN per_ticker_primary p ON p.ticker = m.ticker
    )
    SELECT c.cik AS cik, coalesce(p.primary_cik, c.cik) AS primary_cik
    FROM {DATABASE_NAME}.companies c
    LEFT JOIN cik_to_primary p ON p.cik = c.cik
    """,
)


def init_db(conn) -> None:
    # Normalize legacy objects that may exist as either table or view.
    for obj in ("primary_tickers", "cik_canonical"):
        for drop_stmt in (
            f"DROP VIEW IF EXISTS {DATABASE_NAME}.{obj}",
            f"DROP TABLE IF EXISTS {DATABASE_NAME}.{obj}",
        ):
            try:
                conn.execute(drop_stmt)
            except Exception:
                pass
    # Tables AND their cik indexes are created here on (initially empty) tables,
    # so on a fresh build the indexes exist before any rows are inserted and are
    # maintained as data loads — never built after the fact over a full table.
    for stmt in DDL:
        conn.execute(stmt)
