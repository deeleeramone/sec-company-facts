from __future__ import annotations

from statistics import median
from typing import Any

from sec_app.db.query import _q, _rows, _session

_SIC_CODE_SUBQ = "(SELECT tag_id FROM xbrl_tags WHERE namespace='dei' AND tag='EntitySicCode')"

_CANNABIS_NAME_TERMS = ["cannabis", "marijuana", "marihuana", "cannabin", "hemp"]
_CANNABIS_TICKERS = [
    "CURLF", "TCNNF", "CRLBF", "VRNO", "GTBIF", "TSNDF", "AAWH", "AYRWF", "JUSHF",
    "MRMD", "CXXIF", "GLASF", "CGC", "TLRY", "ACB", "CRON", "SNDL", "OGI", "VFF",
    "CWBHF", "PLNH", "GRUSF", "VREOF", "ITHUF", "ACRHF", "LOWLF", "MMNFF",
]

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

_MAJOR_GROUP: dict[int, tuple[str, str]] = {
    1: ("Agriculture", "Crops"),
    2: ("Agriculture", "Livestock"),
    7: ("Agriculture", "Agricultural Services"),
    8: ("Agriculture", "Forestry"),
    9: ("Agriculture", "Fishing, Hunting & Trapping"),
    10: ("Mining & Extraction", "Metal Mining"),
    12: ("Mining & Extraction", "Coal Mining"),
    13: ("Mining & Extraction", "Oil & Gas Extraction"),
    14: ("Mining & Extraction", "Nonmetallic Minerals Mining"),
    15: ("Construction", "Building Construction"),
    16: ("Construction", "Heavy Construction"),
    17: ("Construction", "Special Trade Contractors"),
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
    40: ("Transportation & Utilities", "Railroads"),
    41: ("Transportation & Utilities", "Transit & Ground Passenger"),
    42: ("Transportation & Utilities", "Trucking & Warehousing"),
    44: ("Transportation & Utilities", "Water Transportation"),
    45: ("Transportation & Utilities", "Air Transportation"),
    46: ("Transportation & Utilities", "Pipelines"),
    47: ("Transportation & Utilities", "Transportation Services"),
    48: ("Transportation & Utilities", "Communications"),
    49: ("Transportation & Utilities", "Utilities"),
    50: ("Wholesale Trade", "Durable Goods"),
    51: ("Wholesale Trade", "Nondurable Goods"),
    52: ("Retail Trade", "Building Materials & Garden"),
    53: ("Retail Trade", "General Merchandise"),
    54: ("Retail Trade", "Food Stores"),
    55: ("Retail Trade", "Automotive Dealers"),
    56: ("Retail Trade", "Apparel & Accessory Stores"),
    57: ("Retail Trade", "Furniture & Home Furnishings"),
    58: ("Retail Trade", "Eating & Drinking Places"),
    59: ("Retail Trade", "Miscellaneous Retail"),
    60: ("Finance, Insurance & Real Estate", "Depository Institutions"),
    61: ("Finance, Insurance & Real Estate", "Nondepository Credit"),
    62: ("Finance, Insurance & Real Estate", "Securities & Brokers"),
    63: ("Finance, Insurance & Real Estate", "Insurance Carriers"),
    64: ("Finance, Insurance & Real Estate", "Insurance Agents & Brokers"),
    65: ("Finance, Insurance & Real Estate", "Real Estate"),
    67: ("Finance, Insurance & Real Estate", "Holding & Investment Offices"),
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
    name_likes = " OR ".join(
        "LOWER(co.entity_name) LIKE %s" % _q("%" + t + "%") for t in _CANNABIS_NAME_TERMS
    )
    tickers_in = ",".join(_q(t) for t in _CANNABIS_TICKERS)
    return (
        f"SELECT cik FROM companies co WHERE {name_likes} "
        f"UNION SELECT cik FROM tickers WHERE ticker IN ({tickers_in})"
    )


def _classify_cte() -> str:
    descr_subq = "(SELECT tag_id FROM xbrl_tags WHERE namespace='dei' AND tag='EntitySicDescription')"
    return f"""WITH code AS (
  SELECT cik, CAST(val AS UNSIGNED) AS sic4
  FROM facts_enc
  WHERE tag_id = {_SIC_CODE_SUBQ} AND val IS NOT NULL
),
sic_descr AS (
  SELECT cik, val_text AS descr FROM facts_enc
  WHERE tag_id = {descr_subq} AND val_text IS NOT NULL
),
cannabis AS (
  {_cannabis_subquery()}
),
classified AS (
  SELECT c.cik, c.sic4,
         {_case('sector')} AS sector,
         {_case('industry')} AS industry,
         COALESCE(d.descr, CONCAT('SIC ', CAST(c.sic4 AS CHAR))) AS sub_industry
  FROM code c
  LEFT JOIN cannabis can ON can.cik = c.cik
  LEFT JOIN sic_descr d ON d.cik = c.cik
)"""


def cik_in_clause(
    sector: str | None = None,
    industry: str | None = None,
    db_path: str | None = None,
) -> str | None:
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


def _revenue_cte() -> str:
    return """rev AS (
  SELECT cik, val AS revenue FROM (
    SELECT cik, val,
           row_number() OVER (
               PARTITION BY cik
               ORDER BY fiscal_year DESC, period_ending DESC,
                        CASE WHEN tag = 'total_revenue' THEN 0 ELSE 1 END
           ) AS rn
    FROM standardized_statements_enc
    WHERE tag IN ('total_revenue', 'operating_revenue')
      AND frequency = 'annual' AND val IS NOT NULL AND val <> 0
  ) z WHERE rn = 1
)"""


def _has_revenue_cte() -> str:
    tags = (
        "'total_revenue', 'operating_revenue', 'total_interest_income', "
        "'premiums_earned', 'revenues_excl_interest_dividends', 'total_noninterest_income'"
    )
    return f"""has_rev AS (
  SELECT DISTINCT cik FROM standardized_statements_enc
  WHERE frequency = 'annual' AND val IS NOT NULL AND val <> 0
    AND period_ending >= CURRENT_DATE - INTERVAL 2 YEAR AND tag IN ({tags})
)"""


def _productive_cte() -> str:
    tags = "'net_ppe', 'net_inventory', 'goodwill', 'intangible_assets'"
    return f"""productive AS (
  SELECT DISTINCT cik FROM standardized_statements_enc
  WHERE frequency = 'annual' AND val IS NOT NULL AND val > 0
    AND period_ending >= CURRENT_DATE - INTERVAL 2 YEAR AND tag IN ({tags})
)"""


def list_sectors(db_path: str | None = None) -> list[dict[str, Any]]:
    sql = f"{_classify_cte()} SELECT sector, COUNT(*) AS n FROM classified GROUP BY sector ORDER BY n DESC"
    sess = _session(db_path)
    try:
        rows = _rows(sess, sql)
    finally:
        sess.close()
    return [{"label": f"{sector} ({n})", "value": sector} for sector, n in rows]


def list_industries(sector: str | None = None, db_path: str | None = None) -> list[dict[str, Any]]:
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
    if industry:
        group_field = "sub_industry"
        where_sql = "WHERE cl.industry = %s" % _q(industry)
    elif sector:
        group_field = "industry"
        where_sql = "WHERE cl.sector = %s" % _q(sector)
    else:
        group_field = "sector"
        where_sql = ""

    sql = f"""{_classify_cte()},
{_revenue_cte()},
{_has_revenue_cte()},
{_productive_cte()},
realbiz AS (
  SELECT cik FROM has_rev
  UNION
  SELECT cik FROM productive
),
{_latest_metric_cte('ni', 'net_income', 'net_income')},
{_latest_metric_cte('ta', 'total_assets', 'assets')}
SELECT cl.sector, cl.industry, cl.sub_industry, r.revenue, n.net_income, a.assets,
       CASE WHEN hr.cik IS NOT NULL THEN 1 ELSE 0 END AS has_revenue
FROM classified cl
JOIN realbiz rb ON rb.cik = cl.cik
LEFT JOIN rev r ON r.cik = cl.cik
LEFT JOIN has_rev hr ON hr.cik = cl.cik
LEFT JOIN ni n ON n.cik = cl.cik
LEFT JOIN ta a ON a.cik = cl.cik
{where_sql}"""

    sess = _session(db_path)
    try:
        rows = _rows(sess, sql)
    finally:
        sess.close()

    groups: dict[str, dict[str, Any]] = {}
    for sec, ind, sub, revenue, net_income, assets, has_revenue in rows:
        key = {"sector": sec, "industry": ind, "sub_industry": sub}[group_field]
        bucket = groups.setdefault(key, {"sector": sec, "industry": ind, "sub": sub, "recs": []})
        bucket["recs"].append((revenue, net_income, assets, has_revenue))

    out: list[dict[str, Any]] = []
    for key, bucket in groups.items():
        recs = bucket["recs"]
        n = len(recs)
        n_rev = sum(1 for rv, ni, ta, hr in recs if hr)
        n_with_ni = sum(1 for rv, ni, ta, hr in recs if ni is not None)
        n_prof = sum(1 for rv, ni, ta, hr in recs if ni and ni > 0)
        margins = [ni / rv * 100 for rv, ni, ta, hr in recs if rv and rv > 0 and ni is not None]
        roas = [ni / ta * 100 for rv, ni, ta, hr in recs if ta and ta > 0 and ni is not None]

        row: dict[str, Any] = {"Sector": bucket["sector"]}
        if group_field in ("industry", "sub_industry"):
            row["Industry"] = bucket["industry"]
        if group_field == "sub_industry":
            row["Sub-Industry"] = bucket["sub"]
        row["Companies"] = n
        row["With Revenue"] = round(100 * n_rev / n, 1) if n else None
        row["Profitable"] = round(100 * n_prof / n_with_ni, 1) if n_with_ni else None
        row["Median Net Margin"] = round(median(margins), 2) if margins else None
        row["Median ROA"] = round(median(roas), 2) if roas else None
        out.append(row)

    out.sort(key=lambda r: r["Companies"], reverse=True)
    return out
