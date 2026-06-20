from __future__ import annotations

from sec_app.db.backend import connect_dolt
from sec_app.db.sectors import _classify_compute_cte

_SCHEMA = (
    "CREATE TABLE cik_gics ("
    " cik VARCHAR(10) NOT NULL PRIMARY KEY,"
    " sic4 INT,"
    " sector VARCHAR(64),"
    " industry VARCHAR(160),"
    " sub_industry VARCHAR(512),"
    " INDEX idx_cik_gics_sector (sector),"
    " INDEX idx_cik_gics_industry (industry)"
    ")"
)


def rebuild() -> int:
    conn = connect_dolt()
    try:
        rows = conn.execute(
            _classify_compute_cte()
            + " SELECT cik, sic4, sector, industry, sub_industry FROM classified"
        ).fetchall()
        conn.execute("DROP TABLE IF EXISTS cik_gics")
        conn.execute(_SCHEMA)
        batch = 500
        for i in range(0, len(rows), batch):
            chunk = rows[i : i + batch]
            values = ", ".join(["(?, ?, ?, ?, ?)"] * len(chunk))
            params = [v for r in chunk for v in (r[0], r[1], r[2], r[3], r[4])]
            conn.execute(
                "INSERT INTO cik_gics (cik, sic4, sector, industry, sub_industry) VALUES " + values,
                params,
            )
        return len(rows)
    finally:
        conn.close()


if __name__ == "__main__":
    n = rebuild()
    print(f"cik_gics rebuilt: {n} rows", flush=True)
