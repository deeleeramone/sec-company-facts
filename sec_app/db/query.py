from __future__ import annotations

import gzip
import json as _json
from typing import Any

from openbb_core.app.model.abstract.error import OpenBBError

from sec_app.db.cache import dolt_cached
from sec_app.db.dialect import like_escape_suffix, quote_literal
from sec_app.db.schema import DATABASE_NAME


def _session(db_path: str | None = None):
    from sec_app.db.backend import connect_read  # pylint: disable=import-outside-toplevel

    return connect_read(db_path)


def _translate_sql(sql: str) -> str:
    translated = sql.replace(f"{DATABASE_NAME}.", "")
    return translated


def _rows(sess, sql: str) -> list[tuple]:
    sql = _translate_sql(sql)
    return [tuple(r) for r in sess.execute(sql).fetchall()]


def _row(sess, sql: str):
    rows = _rows(sess, sql)
    return rows[0] if rows else None


def _primary_ticker_by_cik() -> str:
    return f"""(
        SELECT cik, ticker FROM (
            SELECT cik, ticker,
                row_number() OVER (
                    PARTITION BY cik ORDER BY coalesce(`rank`, 2147483647) ASC, ticker ASC
                ) AS tk_rn
            FROM {DATABASE_NAME}.primary_tickers
        ) AS pt_ranked WHERE tk_rn = 1
    )"""


def _zpad_cik(cik: Any) -> str:
    if cik is None:
        raise OpenBBError("CIK is required")
    text = str(cik).strip()
    if not text:
        raise OpenBBError("CIK is required")
    digits = text.lstrip("0") or "0"
    if not digits.isdigit():
        raise OpenBBError(f"Invalid CIK: {cik!r}")
    return digits.zfill(10)


def _normalise_ticker(ticker: str) -> str:
    return ticker.upper().replace(".", "-")


def _q(value: str) -> str:
    return quote_literal(value)


def load_company_facts(
    cik: str,
    db_path: str | None = None,
) -> dict[str, Any]:
    cik_padded = _zpad_cik(cik)
    sess = _session(db_path)
    try:
        company = _row(
            sess,
            f"SELECT cik, entity_name FROM {DATABASE_NAME}.companies WHERE cik = {_q(cik_padded)} LIMIT 1",
        )
        if not company:
            raise OpenBBError(f"CIK {cik_padded} not found in the SEC database.")
        meta_rows = _rows(
            sess,
            f"SELECT x.namespace, x.tag, x.label, x.description "
            f"FROM {DATABASE_NAME}.cik_tags ct "
            f"JOIN {DATABASE_NAME}.xbrl_tags x ON x.tag_id = ct.tag_id "
            f"WHERE ct.cik = {_q(cik_padded)}",
        )
        fact_rows = _rows(
            sess,
            f"SELECT x.namespace, x.tag, f.unit, f.`start`, f.`end`, f.val, f.val_text, "
            f"a.accn, f.fy, f.fp, f.form, f.filed, f.frame "
            f"FROM {DATABASE_NAME}.facts_enc f "
            f"JOIN {DATABASE_NAME}.xbrl_tags x ON x.tag_id = f.tag_id "
            f"LEFT JOIN {DATABASE_NAME}.accessions a ON a.accn_id = f.accn_id "
            f"WHERE f.cik = {_q(cik_padded)} "
            f"ORDER BY x.namespace, x.tag, f.unit, f.`end`, f.filed",
        )
    finally:
        sess.close()

    facts: dict[str, dict[str, dict[str, Any]]] = {}
    for namespace, tag, label, description in meta_rows:
        facts.setdefault(namespace, {})[tag] = {
            "label": label,
            "description": description,
            "units": {},
        }
    for row in fact_rows:
        (
            namespace,
            tag,
            unit,
            start,
            end,
            val,
            val_text,
            accn,
            fy,
            fp,
            form,
            filed,
            frame,
        ) = row
        ns = facts.setdefault(namespace, {})
        tag_entry = ns.setdefault(tag, {"label": None, "description": None, "units": {}})
        unit_list = tag_entry["units"].setdefault(unit, [])
        record: dict[str, Any] = {
            "end": end.isoformat() if end is not None else None,
            "val": val_text if val_text else val,
            "accn": accn,
            "fy": fy,
            "fp": fp,
            "form": form,
            "filed": filed.isoformat() if filed is not None else None,
        }
        if start is not None:
            record["start"] = start.isoformat()
        if frame:
            record["frame"] = frame
        unit_list.append(record)

    return {"cik": cik_padded, "entityName": company[1] or "", "facts": facts}


def load_standardized_statements(
    cik_list: list[str],
    period: str,
    db_path: str | None = None,
):
    if period not in ("annual", "quarterly") or not cik_list:
        return None
    sess = _session(db_path)
    base_cols = "s.statement, s.period_ending, s.fiscal_year, s.fiscal_period, s.currency, s.tag, s.val, s.company_type"
    try:
        ciks_in = "(" + ",".join(_q(c) for c in cik_list) + ")"
        where = f"WHERE s.cik IN {ciks_in} AND s.frequency = {_q(period)}"
        rows = _rows(
            sess,
            f"SELECT {base_cols}, src.source "
            f"FROM {DATABASE_NAME}.standardized_statements_enc s "
            f"JOIN {DATABASE_NAME}.sources src ON src.source_id = s.source_id {where}",
        )
        name_row = _row(
            sess,
            f"SELECT entity_name FROM {DATABASE_NAME}.companies WHERE cik = {_q(cik_list[0])} LIMIT 1",
        )
    finally:
        sess.close()
    if not rows:
        return None

    from openbb_sec.utils.company_facts import StandardizedStatements, _schema

    entity_name = name_row[0] if name_row and name_row[0] else ""
    company_type = next((r[7] for r in rows if r[7]), "industrial")

    rowdefs = {
        stmt: {rd.tag: rd for rd in _schema.get_rows(stmt, company_type)}
        for stmt in ("income_statement", "balance_sheet", "cash_flow")
    }
    buckets: dict[str, list[dict[str, Any]]] = {
        "income_statement": [],
        "balance_sheet": [],
        "cash_flow": [],
    }
    for row in rows:
        statement, period_ending, fy, fp, currency, tag, val, _ctype = row[:8]
        bucket = buckets.get(statement)
        if bucket is None:
            continue
        rd = rowdefs.get(statement, {}).get(tag)
        bucket.append(
            {
                "period_ending": period_ending.isoformat() if hasattr(period_ending, "isoformat") else str(period_ending),
                "fiscal_period": fp,
                "fiscal_year": fy,
                "currency": currency,
                "tag": tag,
                "value": val,
                "source": (row[8] if len(row) > 8 else "") or "",
                "label": rd.label if rd else tag,
                "description": rd.description if rd else "",
                "parent": rd.parent if rd else None,
                "sequence": rd.sequence if rd else None,
                "factor": rd.factor if rd else "+",
                "balance": rd.balance if rd else "",
                "unit": rd.unit if rd else "monetary",
            }
        )

    return StandardizedStatements(
        entity_name=entity_name,
        cik=cik_list[0],
        company_type=company_type,
        income_statement=buckets["income_statement"],
        balance_sheet=buckets["balance_sheet"],
        cash_flow=buckets["cash_flow"],
    )


@dolt_cached
def list_companies_with_financials(
    db_path: str | None = None,
) -> list[dict[str, str]]:
    sess = _session(db_path)
    try:
        rows = _rows(
            sess,
            f"""
                        SELECT min(t.ticker) AS ticker, min(c.entity_name) AS name
              FROM {DATABASE_NAME}.companies c
              JOIN {DATABASE_NAME}.tickers t ON t.cik = c.cik
              JOIN {DATABASE_NAME}.processed_ciks p ON p.cik = c.cik
             WHERE p.has_balance AND p.has_income AND p.has_cash_flow
             GROUP BY c.cik
             ORDER BY ticker
            """,
        )
    finally:
        sess.close()
    return [{"label": name, "value": ticker} for ticker, name in rows]


@dolt_cached
def list_company_choices(
    db_path: str | None = None,
) -> list[dict]:
    sess = _session(db_path)
    try:
        rows = _rows(
            sess,
            f"""
            SELECT pt.ticker AS ticker,
                   c.entity_name AS name,
                   pt.cik AS cik,
                   pt.`rank` AS `rank`
              FROM {DATABASE_NAME}.primary_tickers pt
              JOIN {DATABASE_NAME}.companies c ON c.cik = pt.cik
              JOIN {DATABASE_NAME}.processed_ciks p ON p.cik = pt.cik
             WHERE p.has_balance AND p.has_income AND p.has_cash_flow
             ORDER BY `rank` ASC, ticker ASC
            """,
        )
    finally:
        sess.close()
    return [
        {
            "label": name,
            "value": ticker,
            "extraInfo": {
                "description": f"{ticker} | {cik}",
                "rightOfDescription": "",
            },
        }
        for ticker, name, cik, _rank in rows
    ]


def resolve_symbol(ticker: str, db_path: str | None = None) -> list[str]:
    sym = _normalise_ticker(ticker)
    sess = _session(db_path)
    try:
        multi = _rows(
            sess,
            f"SELECT cik FROM {DATABASE_NAME}.multi_cik_tickers WHERE ticker = {_q(sym)} ORDER BY priority",
        )
        if multi:
            return [row[0] for row in multi]
        common = _row(
            sess,
            f"SELECT cik FROM {DATABASE_NAME}.tickers WHERE ticker = {_q(sym)} LIMIT 1",
        )
        if common:
            return [common[0]]
        fund = _row(
            sess,
            f"SELECT cik FROM {DATABASE_NAME}.funds WHERE symbol = {_q(sym)} LIMIT 1",
        )
        if fund:
            return [fund[0]]
    finally:
        sess.close()
    return []


def resolve_cik(cik: str, db_path: str | None = None) -> str:
    cik_padded = _zpad_cik(cik)
    sess = _session(db_path)
    try:
        row = _row(
            sess,
            f"SELECT ticker FROM {DATABASE_NAME}.tickers WHERE cik = {_q(cik_padded)} ORDER BY ticker LIMIT 1",
        )
    finally:
        sess.close()
    return row[0] if row else ""


def search_entities(
    keyword: str,
    db_path: str | None = None,
    limit: int = 1000,
) -> list[tuple[str, str]]:
    pat = "%" + keyword.replace("%", "\\%") + "%"
    sess = _session(db_path)
    try:
        rows = _rows(
            sess,
            f"SELECT cik, entity_name FROM {DATABASE_NAME}.entities "
            f"WHERE entity_name LIKE {_q(pat)}{like_escape_suffix()} "
            f"ORDER BY entity_name LIMIT {int(limit)}",
        )
    finally:
        sess.close()
    return [(row[0], row[1]) for row in rows]


SEC_LOCATION_CODES: dict[str, str] = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "DC": "District of Columbia",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
    "GU": "Guam",
    "PR": "Puerto Rico",
    "VI": "Virgin Islands, U.S.",
    "X1": "United States",
    "A0": "Alberta, Canada",
    "A1": "British Columbia, Canada",
    "A2": "Manitoba, Canada",
    "A3": "New Brunswick, Canada",
    "A4": "Newfoundland, Canada",
    "A5": "Nova Scotia, Canada",
    "A6": "Ontario, Canada",
    "A7": "Prince Edward Island, Canada",
    "A8": "Quebec, Canada",
    "A9": "Saskatchewan, Canada",
    "B0": "Yukon, Canada",
    "Z4": "Canada (Federal Level)",
    "B2": "Afghanistan",
    "Y6": "Aland Islands",
    "B3": "Albania",
    "B4": "Algeria",
    "B5": "American Samoa",
    "B6": "Andorra",
    "B7": "Angola",
    "1A": "Anguilla",
    "B8": "Antarctica",
    "B9": "Antigua and Barbuda",
    "C1": "Argentina",
    "1B": "Armenia",
    "1C": "Aruba",
    "C3": "Australia",
    "C4": "Austria",
    "1D": "Azerbaijan",
    "C5": "Bahamas",
    "C6": "Bahrain",
    "C7": "Bangladesh",
    "C8": "Barbados",
    "1F": "Belarus",
    "C9": "Belgium",
    "D1": "Belize",
    "G6": "Benin",
    "D0": "Bermuda",
    "D2": "Bhutan",
    "D3": "Bolivia",
    "1E": "Bosnia and Herzegovina",
    "B1": "Botswana",
    "D4": "Bouvet Island",
    "D5": "Brazil",
    "D6": "British Indian Ocean Territory",
    "D9": "Brunei Darussalam",
    "E0": "Bulgaria",
    "X2": "Burkina Faso",
    "E2": "Burundi",
    "E3": "Cambodia",
    "E4": "Cameroon",
    "E8": "Cape Verde",
    "E9": "Cayman Islands",
    "F0": "Central African Republic",
    "F2": "Chad",
    "F3": "Chile",
    "F4": "China",
    "F6": "Christmas Island",
    "F7": "Cocos (Keeling) Islands",
    "F8": "Colombia",
    "F9": "Comoros",
    "G0": "Congo",
    "Y3": "Congo, the Democratic Republic of the",
    "G1": "Cook Islands",
    "G2": "Costa Rica",
    "L7": "Cote d'Ivoire",
    "1M": "Croatia",
    "G3": "Cuba",
    "G4": "Cyprus",
    "2N": "Czech Republic",
    "G7": "Denmark",
    "1G": "Djibouti",
    "G9": "Dominica",
    "G8": "Dominican Republic",
    "H1": "Ecuador",
    "H2": "Egypt",
    "H3": "El Salvador",
    "H4": "Equatorial Guinea",
    "1J": "Eritrea",
    "1H": "Estonia",
    "H5": "Ethiopia",
    "H7": "Falkland Islands (Malvinas)",
    "H6": "Faroe Islands",
    "H8": "Fiji",
    "H9": "Finland",
    "I0": "France",
    "I3": "French Guiana",
    "I4": "French Polynesia",
    "2C": "French Southern Territories",
    "I5": "Gabon",
    "I6": "Gambia",
    "2Q": "Georgia",
    "2M": "Germany",
    "J0": "Ghana",
    "J1": "Gibraltar",
    "J3": "Greece",
    "J4": "Greenland",
    "J5": "Grenada",
    "J6": "Guadeloupe",
    "J8": "Guatemala",
    "Y7": "Guernsey",
    "J9": "Guinea",
    "S0": "Guinea-Bissau",
    "K0": "Guyana",
    "K1": "Haiti",
    "K4": "Heard Island and McDonald Islands",
    "X4": "Holy See (Vatican City State)",
    "K2": "Honduras",
    "K3": "Hong Kong",
    "K5": "Hungary",
    "K6": "Iceland",
    "K7": "India",
    "K8": "Indonesia",
    "K9": "Iran, Islamic Republic of",
    "L0": "Iraq",
    "L2": "Ireland",
    "Y8": "Isle of Man",
    "L3": "Israel",
    "L6": "Italy",
    "L8": "Jamaica",
    "M0": "Japan",
    "Y9": "Jersey",
    "M2": "Jordan",
    "1P": "Kazakstan",
    "M3": "Kenya",
    "J2": "Kiribati",
    "M4": "Korea, Democratic People's Republic of",
    "M5": "Korea, Republic of",
    "M6": "Kuwait",
    "1N": "Kyrgyzstan",
    "M7": "Lao People's Democratic Republic",
    "1R": "Latvia",
    "M8": "Lebanon",
    "M9": "Lesotho",
    "N0": "Liberia",
    "N1": "Libyan Arab Jamahiriya",
    "N2": "Liechtenstein",
    "1Q": "Lithuania",
    "N4": "Luxembourg",
    "N5": "Macau",
    "1U": "Macedonia, the Former Yugoslav Republic of",
    "N6": "Madagascar",
    "N7": "Malawi",
    "N8": "Malaysia",
    "N9": "Maldives",
    "O0": "Mali",
    "O1": "Malta",
    "1T": "Marshall Islands",
    "O2": "Martinique",
    "O3": "Mauritania",
    "O4": "Mauritius",
    "2P": "Mayotte",
    "O5": "Mexico",
    "1K": "Micronesia, Federated States of",
    "1S": "Moldova, Republic of",
    "O9": "Monaco",
    "P0": "Mongolia",
    "Z5": "Montenegro",
    "P1": "Montserrat",
    "P2": "Morocco",
    "P3": "Mozambique",
    "E1": "Myanmar",
    "T6": "Namibia",
    "P5": "Nauru",
    "P6": "Nepal",
    "P7": "Netherlands",
    "P8": "Netherlands Antilles",
    "1W": "New Caledonia",
    "Q2": "New Zealand",
    "Q3": "Nicaragua",
    "Q4": "Niger",
    "Q5": "Nigeria",
    "Q6": "Niue",
    "Q7": "Norfolk Island",
    "1V": "Northern Mariana Islands",
    "Q8": "Norway",
    "P4": "Oman",
    "R0": "Pakistan",
    "1Y": "Palau",
    "1X": "Palestinian Territory, Occupied",
    "R1": "Panama",
    "R2": "Papua New Guinea",
    "R4": "Paraguay",
    "R5": "Peru",
    "R6": "Philippines",
    "R8": "Pitcairn",
    "R9": "Poland",
    "S1": "Portugal",
    "S3": "Qatar",
    "S4": "Reunion",
    "S5": "Romania",
    "1Z": "Russian Federation",
    "S6": "Rwanda",
    "Z0": "Saint Barthelemy",
    "U8": "Saint Helena",
    "U7": "Saint Kitts and Nevis",
    "U9": "Saint Lucia",
    "Z1": "Saint Martin",
    "V0": "Saint Pierre and Miquelon",
    "V1": "Saint Vincent and the Grenadines",
    "Y0": "Samoa",
    "S8": "San Marino",
    "S9": "Sao Tome and Principe",
    "T0": "Saudi Arabia",
    "T1": "Senegal",
    "Z2": "Serbia",
    "T2": "Seychelles",
    "T8": "Sierra Leone",
    "U0": "Singapore",
    "2B": "Slovakia",
    "2A": "Slovenia",
    "D7": "Solomon Islands",
    "U1": "Somalia",
    "T3": "South Africa",
    "1L": "South Georgia and the South Sandwich Islands",
    "U3": "Spain",
    "F1": "Sri Lanka",
    "V2": "Sudan",
    "V3": "Suriname",
    "L9": "Svalbard and Jan Mayen",
    "V6": "Swaziland",
    "V7": "Sweden",
    "V8": "Switzerland",
    "V9": "Syrian Arab Republic",
    "F5": "Taiwan",
    "2D": "Tajikistan",
    "W0": "Tanzania, United Republic of",
    "W1": "Thailand",
    "Z3": "Timor-Leste",
    "W2": "Togo",
    "W3": "Tokelau",
    "W4": "Tonga",
    "W5": "Trinidad and Tobago",
    "W6": "Tunisia",
    "W8": "Turkey",
    "2E": "Turkmenistan",
    "W7": "Turks and Caicos Islands",
    "2G": "Tuvalu",
    "W9": "Uganda",
    "2H": "Ukraine",
    "C0": "United Arab Emirates",
    "X0": "United Kingdom",
    "2J": "United States Minor Outlying Islands",
    "X3": "Uruguay",
    "2K": "Uzbekistan",
    "2L": "Vanuatu",
    "X5": "Venezuela",
    "Q1": "Viet Nam",
    "D8": "Virgin Islands, British",
    "X8": "Wallis and Futuna",
    "U5": "Western Sahara",
    "T7": "Yemen",
    "Y4": "Zambia",
    "Y5": "Zimbabwe",
    "XX": "Unknown",
}

_US_STATE_CODES = frozenset(
    {
        "AL",
        "AK",
        "AZ",
        "AR",
        "CA",
        "CO",
        "CT",
        "DE",
        "DC",
        "FL",
        "GA",
        "HI",
        "ID",
        "IL",
        "IN",
        "IA",
        "KS",
        "KY",
        "LA",
        "ME",
        "MD",
        "MA",
        "MI",
        "MN",
        "MS",
        "MO",
        "MT",
        "NE",
        "NV",
        "NH",
        "NJ",
        "NM",
        "NY",
        "NC",
        "ND",
        "OH",
        "OK",
        "OR",
        "PA",
        "RI",
        "SC",
        "SD",
        "TN",
        "TX",
        "UT",
        "VT",
        "VA",
        "WA",
        "WV",
        "WI",
        "WY",
        "X1",
        "GU",
        "PR",
        "VI",
    }
)


def _decode_location(code: str | None) -> str | None:
    if not code:
        return None
    return SEC_LOCATION_CODES.get(code)


def _decode_state(code: str | None) -> str | None:
    if not code or code not in _US_STATE_CODES:
        return None
    return SEC_LOCATION_CODES.get(code)


def _decode_country(code: str | None) -> str | None:
    if not code or code in _US_STATE_CODES:
        return None
    return SEC_LOCATION_CODES.get(code)


def company_profile(
    cik_or_ticker: str,
    db_path: str | None = None,
) -> dict[str, Any] | None:
    sess = _session(db_path)
    try:
        token = str(cik_or_ticker).strip()
        ciks: list[str] = []
        if token.upper() != token.lower() or not token.isdigit():
            ciks = resolve_symbol(token, db_path)
        if not ciks:
            try:
                ciks = [_zpad_cik(token)]
            except OpenBBError:
                return None

        primary_cik_row = _row(
            sess,
            f"SELECT primary_cik FROM {DATABASE_NAME}.cik_canonical WHERE cik = {_q(ciks[0])} LIMIT 1",
        )
        if not primary_cik_row:
            return None
        primary_cik = primary_cik_row[0]
        related_rows = _rows(
            sess,
            f"SELECT cik FROM {DATABASE_NAME}.cik_canonical WHERE primary_cik = {_q(primary_cik)}",
        )
        all_ciks = sorted({r[0] for r in related_rows} | {primary_cik})
        related_ciks = [c for c in all_ciks if c != primary_cik]
        ciks_in = "(" + ",".join(_q(c) for c in all_ciks) + ")"

        name_row = _row(
            sess,
            f"SELECT entity_name FROM {DATABASE_NAME}.companies WHERE cik = {_q(primary_cik)} LIMIT 1",
        )
        entity_name = name_row[0] if name_row else None

        ticker_rows = _rows(
            sess,
            f"SELECT ticker, is_primary, cik FROM {DATABASE_NAME}.tickers "
            f"WHERE cik IN {ciks_in} ORDER BY is_primary DESC, length(ticker), ticker",
        )
        primary_ticker = next((t for t, p, _ in ticker_rows if p == 1), None)
        if primary_ticker is None and ticker_rows:
            primary_ticker = ticker_rows[0][0]
        secondary_tickers = [t for t, p, _ in ticker_rows if t != primary_ticker]

        DEI_TAGS = (
            "EntitySicCode",
            "EntitySicDescription",
            "EntityFiscalYearEnd",
            "EntityStateOfIncorporation",
        )
        tag_in = "(" + ",".join(_q(t) for t in DEI_TAGS) + ")"
        dei_rows = _rows(
            sess,
            f"""
            SELECT tag, val, val_text, `end`
              FROM (
                SELECT x.tag, f.val, f.val_text, f.`end`,
                       row_number() OVER (
                           PARTITION BY x.tag ORDER BY f.`end` DESC, f.filed DESC
                       ) AS rn
                  FROM {DATABASE_NAME}.facts_enc f
                  JOIN {DATABASE_NAME}.xbrl_tags x ON x.tag_id = f.tag_id
                 WHERE f.cik IN {ciks_in}
                   AND x.namespace='dei' AND x.tag IN {tag_in}
              ) AS r
             WHERE rn = 1
            """,
        )
        dei = {tag: {"val": val, "val_text": val_text, "end": end} for tag, val, val_text, end in dei_rows}

        period_row = _row(
            sess,
            f"""
            SELECT min(period_ending), max(period_ending)
              FROM {DATABASE_NAME}.standardized_statements_enc
             WHERE cik IN {ciks_in}
            """,
        )
        first_period, latest_period = (None, None)
        if period_row and period_row[0] is not None:
            first_period, latest_period = period_row

        sub = load_submissions(primary_cik, db_path)
        exchanges_raw = (sub.get("exchanges") if sub else None) or []
        seen_ex: set = set()
        exchanges = [e for e in exchanges_raw if not (e in seen_ex or seen_ex.add(e))] or None
    finally:
        sess.close()

    sic_code = None
    sic_name = None
    if "EntitySicCode" in dei:
        v = dei["EntitySicCode"]["val"]
        sic_code = str(int(v)).zfill(4) if v is not None else None
    if "EntitySicDescription" in dei:
        sic_name = dei["EntitySicDescription"]["val_text"]

    state_of_inc = None
    if "EntityStateOfIncorporation" in dei:
        state_of_inc = dei["EntityStateOfIncorporation"]["val_text"]

    fy_end = None
    if "EntityFiscalYearEnd" in dei:
        fy_end = dei["EntityFiscalYearEnd"]["val_text"]

    return {
        "primary_ticker": primary_ticker,
        "secondary_tickers": secondary_tickers,
        "name": entity_name,
        "cik": primary_cik,
        "related_ciks": related_ciks,
        "exchanges": exchanges,
        "state_of_incorporation": state_of_inc,
        "state_of_incorporation_name": _decode_location(state_of_inc),
        "sic": sic_code,
        "sic_name": sic_name,
        "fiscal_year_end": fy_end,
        "first_period_captured": first_period,
        "latest_period_captured": latest_period,
    }


def load_submissions(
    cik: str,
    db_path: str | None = None,
) -> dict[str, Any] | None:
    cik_padded = _zpad_cik(cik)
    sess = _session(db_path)
    try:
        row = _row(
            sess,
            f"SELECT payload FROM {DATABASE_NAME}.submissions WHERE cik = {_q(cik_padded)} LIMIT 1",
        )
    finally:
        sess.close()
    if not row or row[0] is None:
        return None
    payload = row[0]
    if isinstance(payload, memoryview):
        payload = payload.tobytes()
    if isinstance(payload, str):
        payload = payload.encode("utf-8")
    raw = payload
    return _json.loads(gzip.decompress(raw))


def _standardized_where_clause(
    statement: str,
    tag: str,
    frequency: str | None = None,
    period_ending: str | None = None,
    company_type: str | None = None,
    currency: str | None = None,
) -> str:
    filters = [
        f"s.statement = {_q(statement)}",
        f"s.tag = {_q(tag)}",
        "s.val IS NOT NULL",
    ]
    if frequency:
        filters.append(f"s.frequency = {_q(frequency)}")
    if period_ending:
        filters.append(f"s.period_ending = CAST({_q(period_ending)} AS DATE)")
    if company_type:
        filters.append(f"s.company_type = {_q(company_type)}")
    if currency:
        filters.append(f"s.currency = {_q(currency)}")
    return " AND ".join(filters)


def _latest_standardized_period(
    sess,
    statement: str,
    tag: str,
    frequency: str | None = None,
    company_type: str | None = None,
    currency: str | None = None,
) -> str | None:
    where_sql = _standardized_where_clause(
        statement=statement,
        tag=tag,
        frequency=frequency,
        company_type=company_type,
        currency=currency,
    )
    row = _row(
        sess,
        f"SELECT max(s.period_ending) FROM {DATABASE_NAME}.standardized_statements_enc s WHERE {where_sql}",
    )
    if not row or row[0] is None:
        return None
    return row[0].isoformat()


@dolt_cached
def top_companies_by_metric(
    statement: str,
    tag: str,
    limit: int = 25,
    exclude_financial_template: bool = True,
    frequency: str | None = "annual",
    negate: bool = False,
    cik_filter: str | None = None,
    db_path: str | None = None,
) -> list[dict[str, Any]]:
    sess = _session(db_path)
    try:
        where_sql = _standardized_where_clause(
            statement=statement,
            tag=tag,
            frequency=frequency,
        )
        where_sql += " AND s.period_ending >= CURRENT_DATE - INTERVAL 2 YEAR"
        if cik_filter:
            where_sql += f" AND s.cik IN ({cik_filter})"
        sign = "-1.0" if negate else "1.0"
        negate_filter = " AND l.val < 0" if negate else ""
        financial_filter = " AND l.company_type NOT IN ('financial', 'insurance')" if exclude_financial_template else ""
        pass_through_revenue_cte = ""
        pass_through_revenue_join = ""
        pass_through_revenue_filter = ""
        if statement == "income_statement" and tag in {"total_revenue", "operating_revenue"}:
            pass_through_revenue_cte = f"""
            , revenue_cost AS (
                SELECT
                    s.cik,
                    s.period_ending,
                    s.fiscal_year,
                    s.fiscal_period,
                    MAX(s.val) AS cost_val
                FROM {DATABASE_NAME}.standardized_statements_enc s
                WHERE s.statement = 'income_statement'
                  AND s.frequency = {_q(frequency)}
                  AND s.tag IN ('total_cost_of_revenue', 'operating_cost_of_revenue')
                  AND s.val IS NOT NULL
                  AND s.period_ending >= CURRENT_DATE - INTERVAL 2 YEAR
                GROUP BY s.cik, s.period_ending, s.fiscal_year, s.fiscal_period
            )
            """
            pass_through_revenue_join = """
                LEFT JOIN revenue_cost rc
                    ON rc.cik = l.canonical_cik
                   AND rc.period_ending = l.period_ending
                   AND rc.fiscal_year = l.fiscal_year
                   AND rc.fiscal_period = l.fiscal_period
            """
            pass_through_revenue_filter = (
                " AND (l.val IS NULL OR l.val <= 0 OR rc.cost_val IS NULL OR rc.cost_val <= 0"
                " OR rc.cost_val / NULLIF(l.val, 0) < 0.99)"
            )
        rows = _rows(
            sess,
            f"""
            WITH latest AS (
                SELECT
                    s.cik AS canonical_cik,
                    s.period_ending, s.fiscal_year, s.fiscal_period, s.currency, s.val, s.company_type,
                    row_number() OVER (
                        PARTITION BY s.cik ORDER BY s.period_ending DESC
                    ) AS rn
                FROM {DATABASE_NAME}.standardized_statements_enc s
                WHERE {where_sql}
            )
            {pass_through_revenue_cte}
            , pairs AS (
                SELECT DISTINCT currency, period_ending
                FROM latest WHERE rn = 1 AND currency != 'USD'
            )
            , nearest_rates AS (
                SELECT currency, period_ending, rate FROM (
                    SELECT p.currency, p.period_ending, er.rate,
                        row_number() OVER (
                            PARTITION BY p.currency, p.period_ending ORDER BY er.rate_date DESC
                        ) AS nr_rn
                    FROM pairs p
                    JOIN {DATABASE_NAME}.exchange_rates er
                        ON er.from_currency = p.currency
                        AND er.to_currency = 'USD'
                        AND er.rate_date <= p.period_ending
                ) AS er_ranked
                WHERE nr_rn = 1
            )
            , scored AS (
                SELECT
                    l.canonical_cik, l.period_ending, l.fiscal_year, l.fiscal_period, l.currency,
                    l.val * IF(l.currency = 'USD', 1.0, nr.rate) * {sign} AS value_usd
                FROM latest l
                                {pass_through_revenue_join}
                LEFT JOIN nearest_rates nr
                    ON nr.currency = l.currency AND nr.period_ending = l.period_ending
                WHERE l.rn = 1
                                    AND (l.currency = 'USD' OR nr.rate IS NOT NULL){negate_filter}{financial_filter}{pass_through_revenue_filter}
                ORDER BY value_usd DESC
                LIMIT {int(limit) * 4}
            )
            SELECT
                pt.ticker AS ticker,
                COALESCE(c.entity_name, '') AS name,
                DATE_FORMAT(t.period_ending, '%Y-%m-%d') AS period_ending,
                t.fiscal_year, t.fiscal_period, t.currency, t.value_usd
            FROM scored t
            JOIN {DATABASE_NAME}.companies c ON c.cik = t.canonical_cik
            JOIN {_primary_ticker_by_cik()} pt ON pt.cik = t.canonical_cik
            ORDER BY t.value_usd DESC
            LIMIT {int(limit)}
            """,
        )
    finally:
        sess.close()
    return [
        {
            "ticker": ticker,
            "name": name,
            "period_ending": period_ending,
            "fiscal_year": fiscal_year,
            "fiscal_period": fiscal_period,
            "currency": currency,
            "value": value_usd,
        }
        for (ticker, name, period_ending, fiscal_year, fiscal_period, currency, value_usd) in rows
    ]


@dolt_cached
def top_companies_by_sum(
    statement: str,
    tag_a: str,
    tag_b: str,
    limit: int = 25,
    exclude_financial_template: bool = True,
    cik_filter: str | None = None,
    db_path: str | None = None,
) -> list[dict[str, Any]]:
    sess = _session(db_path)
    where_a = _standardized_where_clause(statement=statement, tag=tag_a, frequency="annual")
    where_b = _standardized_where_clause(statement=statement, tag=tag_b, frequency="annual")
    _recent = " AND s.period_ending >= CURRENT_DATE - INTERVAL 2 YEAR"
    where_a += _recent
    where_b += _recent
    if cik_filter:
        _cf = f" AND s.cik IN ({cik_filter})"
        where_a += _cf
        where_b += _cf
    try:
        rows = _rows(
            sess,
            f"""
            WITH a_ranked AS (
                SELECT
                    s.cik AS canonical_cik,
                    s.cik AS source_cik,
                    s.period_ending, s.fiscal_year, s.fiscal_period, s.currency, s.val,
                    s.company_type,
                    row_number() OVER (
                        PARTITION BY s.cik, s.period_ending
                        ORDER BY s.cik DESC
                    ) AS rn
                FROM {DATABASE_NAME}.standardized_statements_enc s                WHERE {where_a}
            )
            , b_ranked AS (
                SELECT
                    s.cik AS canonical_cik,
                    s.period_ending, s.val,
                    row_number() OVER (
                        PARTITION BY s.cik, s.period_ending
                        ORDER BY s.cik DESC
                    ) AS rn
                FROM {DATABASE_NAME}.standardized_statements_enc s                WHERE {where_b}
            )
            , summed AS (
                SELECT
                    a.canonical_cik, a.source_cik,
                    a.period_ending, a.fiscal_year, a.fiscal_period, a.currency,
                    a.company_type,
                    a.val + b.val AS val,
                    row_number() OVER (
                        PARTITION BY a.canonical_cik ORDER BY a.period_ending DESC
                    ) AS company_rn
                FROM a_ranked a
                JOIN b_ranked b
                    ON b.canonical_cik = a.canonical_cik
                    AND b.period_ending = a.period_ending
                    AND b.rn = 1
                WHERE a.rn = 1
            )
            , pairs AS (
                SELECT DISTINCT currency, period_ending
                FROM summed
                WHERE company_rn = 1 AND currency != 'USD'
            )
            , nearest_rates AS (
                SELECT currency, period_ending, rate FROM (
                    SELECT p.currency, p.period_ending, er.rate,
                        row_number() OVER (
                            PARTITION BY p.currency, p.period_ending
                            ORDER BY er.rate_date DESC
                        ) AS nr_rn
                    FROM pairs p
                    LEFT JOIN {DATABASE_NAME}.exchange_rates er
                        ON er.from_currency = p.currency
                        AND er.to_currency = 'USD'
                        AND er.rate_date <= p.period_ending
                ) AS er_ranked
                WHERE nr_rn = 1
            )
            , scored AS (
                SELECT
                    s.canonical_cik, s.period_ending, s.fiscal_year, s.fiscal_period, s.currency,
                    s.val * IF(s.currency = 'USD', 1.0, nr.rate) AS value_usd
                FROM summed s
                LEFT JOIN nearest_rates nr ON nr.currency = s.currency AND nr.period_ending = s.period_ending
                WHERE s.company_rn = 1
                  AND (s.currency = 'USD' OR nr.rate IS NOT NULL)
                  {"AND s.company_type NOT IN ('financial', 'insurance')" if exclude_financial_template else ""}
                ORDER BY value_usd DESC
                LIMIT {int(limit) * 4}
            )
            SELECT
                pt.ticker AS ticker,
                COALESCE(c.entity_name, '') AS name,
                DATE_FORMAT(t.period_ending, '%Y-%m-%d') AS period_ending,
                t.fiscal_year, t.fiscal_period, t.currency, t.value_usd
            FROM scored t
            JOIN {DATABASE_NAME}.companies c ON c.cik = t.canonical_cik
            JOIN {_primary_ticker_by_cik()} pt ON pt.cik = t.canonical_cik
            ORDER BY t.value_usd DESC
            LIMIT {int(limit)}
            """,
        )
    finally:
        sess.close()
    return [
        {
            "ticker": ticker,
            "name": name,
            "period_ending": period_ending,
            "fiscal_year": fiscal_year,
            "fiscal_period": fiscal_period,
            "currency": currency,
            "value": value_usd,
        }
        for (ticker, name, period_ending, fiscal_year, fiscal_period, currency, value_usd) in rows
    ]


@dolt_cached
def top_companies_by_ratio(
    statement: str,
    numerator_tag: str,
    denominator_tag: str,
    limit: int = 25,
    min_value: float | None = None,
    max_value: float | None = None,
    min_denominator: float | None = None,
    denominator_statement: str | None = None,
    exclude_financial_template: bool = True,
    cik_filter: str | None = None,
    db_path: str | None = None,
) -> list[dict[str, Any]]:
    sess = _session(db_path)
    denom_stmt = denominator_statement if denominator_statement is not None else statement
    num_where = _standardized_where_clause(statement=statement, tag=numerator_tag, frequency="annual")
    denom_where = _standardized_where_clause(statement=denom_stmt, tag=denominator_tag, frequency="annual")
    _recent = " AND s.period_ending >= CURRENT_DATE - INTERVAL 2 YEAR"
    num_where += _recent
    denom_where += _recent
    if cik_filter:
        num_where += f" AND s.cik IN ({cik_filter})"
    bounds: list[str] = []
    if min_value is not None:
        bounds.append(f"value >= {float(min_value)}")
    if max_value is not None:
        bounds.append(f"value <= {float(max_value)}")
    bounds_sql = (" AND " + " AND ".join(bounds)) if bounds else ""
    min_denom_sql = f" AND d.val >= {float(min_denominator)}" if min_denominator is not None else ""
    financial_filter_sql = "  AND s.company_type NOT IN ('financial', 'insurance')" if exclude_financial_template else ""
    try:
        rows = _rows(
            sess,
            f"""
            WITH num_ranked AS (
                SELECT
                    s.cik AS canonical_cik,
                    s.cik AS source_cik,
                    s.company_type,
                    s.period_ending, s.fiscal_year, s.fiscal_period, s.val,
                    row_number() OVER (
                        PARTITION BY s.cik, s.period_ending
                        ORDER BY s.cik DESC
                    ) AS rn
                FROM {DATABASE_NAME}.standardized_statements_enc s                WHERE {num_where}{financial_filter_sql}
            )
            , denom_ranked AS (
                SELECT
                    s.cik AS canonical_cik,
                    s.period_ending, s.val,
                    row_number() OVER (
                        PARTITION BY s.cik, s.period_ending
                        ORDER BY s.cik DESC
                    ) AS rn
                FROM {DATABASE_NAME}.standardized_statements_enc s                WHERE {denom_where}
            )
            , paired AS (
                SELECT
                    n.canonical_cik, n.period_ending, n.fiscal_year, n.fiscal_period,
                    n.val AS num_val, d.val AS denom_val,
                    row_number() OVER (
                        PARTITION BY n.canonical_cik ORDER BY n.period_ending DESC
                    ) AS company_rn
                FROM num_ranked n
                JOIN denom_ranked d
                    ON d.canonical_cik = n.canonical_cik
                    AND d.period_ending = n.period_ending
                    AND d.rn = 1
                WHERE n.rn = 1 AND d.val > 0{min_denom_sql}
            )
            , computed AS (
                SELECT
                    p.canonical_cik, p.period_ending, p.fiscal_year, p.fiscal_period,
                    round(p.num_val / p.denom_val * 100, 2) AS value
                FROM paired p
                WHERE p.company_rn = 1
            )
            , scored AS (
                SELECT * FROM computed
                WHERE 1 = 1{bounds_sql}
                ORDER BY value DESC
                LIMIT {int(limit) * 4}
            )
            SELECT
                pt.ticker AS ticker,
                COALESCE(c.entity_name, '') AS name,
                DATE_FORMAT(t.period_ending, '%Y-%m-%d') AS period_ending,
                t.fiscal_year, t.fiscal_period, t.value
            FROM scored t
            JOIN {DATABASE_NAME}.companies c ON c.cik = t.canonical_cik
            JOIN {_primary_ticker_by_cik()} pt ON pt.cik = t.canonical_cik
            ORDER BY t.value DESC
            LIMIT {int(limit)}
            """,
        )
    finally:
        sess.close()
    return [
        {
            "ticker": ticker,
            "name": name,
            "period_ending": period_ending,
            "fiscal_year": fiscal_year,
            "fiscal_period": fiscal_period,
            "value": value,
        }
        for (ticker, name, period_ending, fiscal_year, fiscal_period, value) in rows
    ]
