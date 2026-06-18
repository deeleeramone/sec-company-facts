"""Sector / industry taxonomy and aggregate statistics over SEC SIC codes.

A GICS-like reclassification layered on raw SIC codes:

* **Health Care** is carved out across SIC divisions (drugs, biotech, devices,
  life-sciences tools, providers, managed care, distributors) because SIC
  scatters those across Manufacturing, Services, Insurance and Wholesale.
* **Cannabis** is treated as an agricultural product -> the Agriculture sector
  (it otherwise hides in "Medicinal Chemicals" 2833 and Ag-Crops 0100, and
  branded MSOs carry no SIC/keyword signal, so a curated ticker list is used).
* Everything else rolls up by SIC division -> 2-digit major group.

Granularity is driven by the ``industry`` argument:

* no ``industry``     -> **sector level** (all sectors, or one row if ``sector`` given)
* ``industry`` given  -> that single **industry** (``sector`` narrows valid choices)

All aggregation is at the CIK grain. ``% With Revenue`` / ``% Profitable`` /
``Median Net Margin`` / ``Median ROA`` are currency-invariant (ratios within a
company's own reporting currency), so no FX conversion is required.
"""

from __future__ import annotations

from statistics import median
from typing import Any

from sec_app.db.query import _q, _rows, _session

# Resolve the dei:EntitySicCode tag to its id as a scalar so facts_enc uses the
# (tag_id, ...) covering index instead of full-scanning 40M rows.
_SIC_CODE_SUBQ = "(SELECT tag_id FROM xbrl_tags WHERE namespace='dei' AND tag='EntitySicCode')"

# Cannabis = agricultural product. Detected by cannabis-specific name terms plus
# a curated list of branded MSO/LP primary tickers (names like Verano / Curaleaf
# / Cresco carry no SIC or keyword signal). Supplements that also sit in SIC 2833
# (USANA, Mannatech, ChromaDex...) deliberately do NOT match.
_CANNABIS_NAME_TERMS = ["cannabis", "marijuana", "marihuana", "cannabin", "hemp"]
_CANNABIS_TICKERS = [
    "CURLF", "TCNNF", "CRLBF", "VRNO", "GTBIF", "TSNDF", "AAWH", "AYRWF", "JUSHF",
    "MRMD", "CXXIF", "GLASF", "CGC", "TLRY", "ACB", "CRON", "SNDL", "OGI", "VFF",
    "CWBHF", "PLNH", "GRUSF", "VREOF", "ITHUF", "ACRHF", "LOWLF", "MMNFF",
]

# Health Care carve-out: 4-digit SIC -> GICS-style sub-industry. These override
# the major-group default (e.g. 2834 would otherwise be Manufacturing/Chemicals).
# 8000-8099 (all health services) is handled as a range below.
_HEALTHCARE_4D: dict[int, str] = {
    2834: "Pharmaceuticals",
    2836: "Biotechnology",
    2835: "Life Sciences Tools & Diagnostics",
    3826: "Life Sciences Tools & Diagnostics",
    8731: "Life Sciences Tools & Diagnostics",
    8734: "Life Sciences Tools & Diagnostics",
    3841: "Medical Devices & Equipment",
    3842: "Medical Devices & Equipment",
    3843: "Medical Devices & Equipment",
    3844: "Medical Devices & Equipment",
    3845: "Medical Devices & Equipment",
    3851: "Medical Devices & Equipment",
    6324: "Managed Care",
    5122: "Health Care Distributors",
    5047: "Health Care Distributors",
}

# 2-digit SIC major group -> (sector, industry) default. Health Care drug/device
# codes and cannabis are pulled out ahead of these by the rule ordering below.
_MAJOR_GROUP: dict[int, tuple[str, str]] = {
    # Agriculture, Forestry & Fishing
    1: ("Agriculture", "Crops"),
    2: ("Agriculture", "Livestock"),
    7: ("Agriculture", "Agricultural Services"),
    8: ("Agriculture", "Forestry"),
    9: ("Agriculture", "Fishing, Hunting & Trapping"),
    # Mining & Extraction
    10: ("Mining & Extraction", "Metal Mining"),
    12: ("Mining & Extraction", "Coal Mining"),
    13: ("Mining & Extraction", "Oil & Gas Extraction"),
    14: ("Mining & Extraction", "Nonmetallic Minerals Mining"),
    # Construction
    15: ("Construction", "Building Construction"),
    16: ("Construction", "Heavy Construction"),
    17: ("Construction", "Special Trade Contractors"),
    # Manufacturing
    20: ("Manufacturing", "Food & Beverage"),
    21: ("Manufacturing", "Tobacco"),
    22: ("Manufacturing", "Textiles"),
    23: ("Manufacturing", "Apparel"),
    24: ("Manufacturing", "Lumber & Wood"),
    25: ("Manufacturing", "Furniture & Fixtures"),
    26: ("Manufacturing", "Paper"),
    27: ("Manufacturing", "Printing & Publishing"),
    28: ("Manufacturing", "Chemicals"),
    29: ("Manufacturing", "Petroleum Refining"),
    30: ("Manufacturing", "Rubber & Plastics"),
    31: ("Manufacturing", "Leather & Footwear"),
    32: ("Manufacturing", "Stone, Clay & Glass"),
    33: ("Manufacturing", "Primary Metals"),
    34: ("Manufacturing", "Fabricated Metal"),
    35: ("Manufacturing", "Machinery & Computer Equipment"),
    36: ("Manufacturing", "Electronics & Electrical Equipment"),
    37: ("Manufacturing", "Transportation Equipment"),
    38: ("Manufacturing", "Instruments"),
    39: ("Manufacturing", "Miscellaneous Manufacturing"),
    # Transportation & Utilities
    40: ("Transportation & Utilities", "Railroads"),
    41: ("Transportation & Utilities", "Transit & Ground Passenger"),
    42: ("Transportation & Utilities", "Trucking & Warehousing"),
    44: ("Transportation & Utilities", "Water Transportation"),
    45: ("Transportation & Utilities", "Air Transportation"),
    46: ("Transportation & Utilities", "Pipelines"),
    47: ("Transportation & Utilities", "Transportation Services"),
    48: ("Transportation & Utilities", "Communications"),
    49: ("Transportation & Utilities", "Utilities"),
    # Wholesale Trade
    50: ("Wholesale Trade", "Durable Goods"),
    51: ("Wholesale Trade", "Nondurable Goods"),
    # Retail Trade
    52: ("Retail Trade", "Building Materials & Garden"),
    53: ("Retail Trade", "General Merchandise"),
    54: ("Retail Trade", "Food Stores"),
    55: ("Retail Trade", "Automotive Dealers"),
    56: ("Retail Trade", "Apparel & Accessory Stores"),
    57: ("Retail Trade", "Furniture & Home Furnishings"),
    58: ("Retail Trade", "Eating & Drinking Places"),
    59: ("Retail Trade", "Miscellaneous Retail"),
    # Finance, Insurance & Real Estate
    60: ("Finance, Insurance & Real Estate", "Depository Institutions"),
    61: ("Finance, Insurance & Real Estate", "Nondepository Credit"),
    62: ("Finance, Insurance & Real Estate", "Securities & Brokers"),
    63: ("Finance, Insurance & Real Estate", "Insurance Carriers"),
    64: ("Finance, Insurance & Real Estate", "Insurance Agents & Brokers"),
    65: ("Finance, Insurance & Real Estate", "Real Estate"),
    67: ("Finance, Insurance & Real Estate", "Holding & Investment Offices"),
    # Services
    70: ("Services", "Hotels & Lodging"),
    72: ("Services", "Personal Services"),
    73: ("Services", "Business Services"),
    75: ("Services", "Auto Repair & Services"),
    76: ("Services", "Miscellaneous Repair"),
    78: ("Services", "Motion Pictures"),
    79: ("Services", "Amusement & Recreation"),
    80: ("Services", "Health Services"),
    81: ("Services", "Legal Services"),
    82: ("Services", "Educational Services"),
    83: ("Services", "Social Services"),
    87: ("Services", "Engineering & Management Services"),
    89: ("Services", "Services, NEC"),
    99: ("Other / Nonclassifiable", "Nonclassifiable"),
}


def _rules() -> list[tuple[str, str, str]]:
    """Ordered (condition_sql, sector, industry). First match wins, so cannabis
    and the Health Care 4-digit overrides come before the major-group defaults."""
    rules: list[tuple[str, str, str]] = [("can.cik IS NOT NULL", "Agriculture", "Cannabis")]

    by_industry: dict[str, list[int]] = {}
    for sic, industry in _HEALTHCARE_4D.items():
        by_industry.setdefault(industry, []).append(sic)
    for industry, sics in by_industry.items():
        cond = "c.sic4 IN (%s)" % ",".join(str(s) for s in sorted(sics))
        rules.append((cond, "Health Care", industry))
    rules.append(("c.sic4 BETWEEN 8000 AND 8099", "Health Care", "Health Care Providers & Services"))

    for mg, (sector, industry) in sorted(_MAJOR_GROUP.items()):
        rules.append(("c.sic4 BETWEEN %d AND %d" % (mg * 100, mg * 100 + 99), sector, industry))
    return rules


def _case(which: str) -> str:
    idx = 1 if which == "sector" else 2
    fallback = "Other / Nonclassifiable" if which == "sector" else "Nonclassifiable"
    lines = ["CASE"]
    for rule in _rules():
        lines.append("WHEN %s THEN %s" % (rule[0], _q(rule[idx])))
    lines.append("ELSE %s END" % _q(fallback))
    return "\n".join(lines)


def _cannabis_subquery() -> str:
    """SELECT of cannabis CIKs (name terms UNION curated MSO/LP tickers)."""
    name_likes = " OR ".join(
        "LOWER(co.entity_name) LIKE %s" % _q("%" + t + "%") for t in _CANNABIS_NAME_TERMS
    )
    tickers_in = ",".join(_q(t) for t in _CANNABIS_TICKERS)
    return (
        f"SELECT cik FROM companies co WHERE {name_likes} "
        f"UNION SELECT cik FROM tickers WHERE ticker IN ({tickers_in})"
    )


def _classify_cte() -> str:
    """WITH ... clause exposing `classified(cik, sector, industry)`."""
    return f"""WITH code AS (
  SELECT cik, CAST(val AS UNSIGNED) AS sic4
  FROM facts_enc
  WHERE tag_id = {_SIC_CODE_SUBQ} AND val IS NOT NULL
),
cannabis AS (
  {_cannabis_subquery()}
),
classified AS (
  SELECT c.cik,
         {_case('sector')} AS sector,
         {_case('industry')} AS industry
  FROM code c
  LEFT JOIN cannabis can ON can.cik = c.cik
)"""


def cik_in_clause(
    sector: str | None = None,
    industry: str | None = None,
    db_path: str | None = None,
) -> str | None:
    """Resolve a sector/industry to a literal CIK IN-list body, e.g. ``'001','002'``.

    Returns ``None`` when no filter is requested, or ``''`` when a filter is
    requested but matches no company. Resolving to a literal list up front (one
    ~classification query) keeps the downstream ranking query's planner from
    re-evaluating the whole classification per row, which it does — disastrously —
    when the classification is embedded as an ``IN (subquery)``."""
    if not sector and not industry:
        return None
    where: list[str] = []
    if sector:
        where.append("sector = %s" % _q(sector))
    if industry:
        where.append("industry = %s" % _q(industry))
    sql = f"{_classify_cte()} SELECT cik FROM classified WHERE " + " AND ".join(where)
    sess = _session(db_path)
    try:
        rows = _rows(sess, sql)
    finally:
        sess.close()
    return ",".join(_q(r[0]) for r in rows)


def _latest_metric_cte(name: str, tag: str, alias: str, frequency: str = "annual") -> str:
    return f"""{name} AS (
  SELECT cik, val AS {alias} FROM (
    SELECT cik, val,
           row_number() OVER (PARTITION BY cik ORDER BY fiscal_year DESC, period_ending DESC) AS rn
    FROM standardized_statements_enc
    WHERE tag = {_q(tag)} AND frequency = {_q(frequency)} AND val IS NOT NULL
  ) z WHERE rn = 1
)"""


def _options(rows: list[tuple]) -> list[dict[str, Any]]:
    return rows


def list_sectors(db_path: str | None = None) -> list[dict[str, Any]]:
    """Dropdown options: every sector with its company count (largest first)."""
    sql = f"{_classify_cte()} SELECT sector, COUNT(*) AS n FROM classified GROUP BY sector ORDER BY n DESC"
    sess = _session(db_path)
    try:
        rows = _rows(sess, sql)
    finally:
        sess.close()
    return [{"label": f"{sector} ({n})", "value": sector} for sector, n in rows]


def list_industries(sector: str | None = None, db_path: str | None = None) -> list[dict[str, Any]]:
    """Dropdown options for industries, narrowed to ``sector`` when provided."""
    where = "WHERE sector = %s" % _q(sector) if sector else ""
    sql = (
        f"{_classify_cte()} SELECT sector, industry, COUNT(*) AS n "
        f"FROM classified {where} GROUP BY sector, industry ORDER BY n DESC"
    )
    sess = _session(db_path)
    try:
        rows = _rows(sess, sql)
    finally:
        sess.close()
    return [
        {"label": f"{industry} ({n})", "value": industry, "extraInfo": {"sector": sector_}}
        for sector_, industry, n in rows
    ]


def sector_industry_aggregates(
    sector: str | None = None,
    industry: str | None = None,
    db_path: str | None = None,
) -> list[dict[str, Any]]:
    """Aggregate operating statistics by sector or industry.

    * ``industry`` given -> one row for that industry (``sector`` optional, scopes it).
    * else               -> one row per sector (filtered to ``sector`` if given).
    """
    group_field = "industry" if industry else "sector"

    where: list[str] = []
    if industry:
        where.append("cl.industry = %s" % _q(industry))
    if sector:
        where.append("cl.sector = %s" % _q(sector))
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    sql = f"""{_classify_cte()},
{_latest_metric_cte('rev', 'total_revenue', 'revenue')},
{_latest_metric_cte('ni', 'net_income', 'net_income')},
{_latest_metric_cte('ta', 'total_assets', 'assets')}
SELECT cl.sector, cl.industry, r.revenue, n.net_income, a.assets
FROM classified cl
LEFT JOIN rev r ON r.cik = cl.cik
LEFT JOIN ni n ON n.cik = cl.cik
LEFT JOIN ta a ON a.cik = cl.cik
{where_sql}"""

    sess = _session(db_path)
    try:
        rows = _rows(sess, sql)
    finally:
        sess.close()

    groups: dict[str, dict[str, Any]] = {}
    for sec, ind, revenue, net_income, assets in rows:
        key = ind if group_field == "industry" else sec
        bucket = groups.setdefault(key, {"sector": sec, "industry": ind, "recs": []})
        bucket["recs"].append((revenue, net_income, assets))

    out: list[dict[str, Any]] = []
    for key, bucket in groups.items():
        recs = bucket["recs"]
        n = len(recs)
        n_rev = sum(1 for rv, ni, ta in recs if rv and rv > 0)
        n_prof = sum(1 for rv, ni, ta in recs if ni and ni > 0)
        margins = [ni / rv * 100 for rv, ni, ta in recs if rv and rv > 0 and ni is not None]
        roas = [ni / ta * 100 for rv, ni, ta in recs if ta and ta > 0 and ni is not None]

        row: dict[str, Any] = {}
        if group_field == "industry":
            row["Sector"] = bucket["sector"]
            row["Industry"] = key
        else:
            row["Sector"] = key
        row["Companies"] = n
        row["% With Revenue"] = round(100 * n_rev / n) if n else None
        row["% Profitable"] = round(100 * n_prof / n) if n else None
        row["Median Net Margin %"] = round(median(margins), 1) if margins else None
        row["Median ROA %"] = round(median(roas), 1) if roas else None
        out.append(row)

    out.sort(key=lambda r: r["Companies"], reverse=True)
    return out
