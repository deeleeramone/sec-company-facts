"""Read-side helpers for the SEC DuckDB store.

CIK values are always 10-digit zero-padded strings.
"""

from __future__ import annotations

import gzip
import json as _json
from typing import Any

from openbb_core.app.model.abstract.error import OpenBBError

from openbb_sec.db.schema import DATABASE_NAME
from openbb_sec.utils.definitions import (
    SEC_DB_PATH,
)


def _session(db_path: str | None = None):
    import duckdb  # pylint: disable=import-outside-toplevel

    return duckdb.connect(database=db_path or SEC_DB_PATH, read_only=True)


def _translate_sql(sql: str) -> str:
    translated = sql
    translated = translated.replace("ifNull(", "coalesce(")
    translated = translated.replace("lagInFrame(", "lag(")
    translated = translated.replace("argMax(", "arg_max(")
    translated = translated.replace("addYears(today(), -1)", "(CURRENT_DATE - INTERVAL '1 year')")
    translated = translated.replace("addYears(today(), -2)", "(CURRENT_DATE - INTERVAL '2 years')")
    translated = translated.replace(
        "if(s.cik = coalesce(cc.primary_cik, s.cik), 1, 0)",
        "CASE WHEN s.cik = coalesce(cc.primary_cik, s.cik) THEN 1 ELSE 0 END",
    )
    translated = translated.replace(
        "if(l.currency = 'USD', 1.0, nr.rate)",
        "CASE WHEN l.currency = 'USD' THEN 1.0 ELSE nr.rate END",
    )
    translated = translated.replace(
        "if(s.currency = 'USD', 1.0, nr.rate)",
        "CASE WHEN s.currency = 'USD' THEN 1.0 ELSE nr.rate END",
    )
    translated = translated.replace('toString(l.period_ending)', 'CAST(l.period_ending AS VARCHAR)')
    translated = translated.replace('toString(s.period_ending)', 'CAST(s.period_ending AS VARCHAR)')
    translated = translated.replace('toString(p.period_ending)', 'CAST(p.period_ending AS VARCHAR)')
    return translated


def _rows(sess, sql: str) -> list[tuple]:
    sql = _translate_sql(sql)
    return [tuple(r) for r in sess.execute(sql).fetchall()]


def _row(sess, sql: str):
    rows = _rows(sess, sql)
    return rows[0] if rows else None


def _primary_ticker_by_cik() -> str:
    """One ticker per CIK for cik->ticker lookups in aggregate rankings.

    The primary_tickers view keeps every is_primary ticker (share classes, ADRs,
    preferreds, structured products all carry is_primary=true), so joining it
    directly fans a single company into several ranking rows. Pick the lowest
    SEC popularity rank (common stock — e.g. JPM rank 12 over the JPM-P*/VYLD/AMJB
    preferreds at 7000+) so each company appears once."""
    return f"""(
        SELECT cik, ticker FROM (
            SELECT cik, ticker,
                row_number() OVER (
                    PARTITION BY cik ORDER BY coalesce(rank, 2147483647) ASC, ticker ASC
                ) AS tk_rn
            FROM {DATABASE_NAME}.primary_tickers
        ) WHERE tk_rn = 1
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
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def load_company_facts(
    cik: str,
    db_path: str | None = None,
) -> dict[str, Any]:
    """Reconstruct the SEC company-facts JSON shape for a single CIK."""
    cik_padded = _zpad_cik(cik)
    sess = _session(db_path)
    try:
        company = _row(
            sess,
            f"SELECT cik, entity_name FROM {DATABASE_NAME}.companies WHERE cik = {_q(cik_padded)} LIMIT 1",
        )
        if not company:
            raise OpenBBError(f"CIK {cik_padded} not found in local SEC database ({SEC_DB_PATH}).")
        meta_rows = _rows(
            sess,
            f"SELECT namespace, tag, label, description FROM {DATABASE_NAME}.tag_meta WHERE cik = {_q(cik_padded)}",
        )
        fact_rows = _rows(
            sess,
            f'SELECT namespace, tag, unit, start, "end", val, val_text, '
            f"accn, fy, fp, form, filed, frame "
            f"FROM {DATABASE_NAME}.facts WHERE cik = {_q(cik_padded)} "
            f'ORDER BY namespace, tag, unit, "end", filed',
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
             WHERE p.has_balance OR p.has_income OR p.has_cash_flow
             GROUP BY c.cik
             ORDER BY ticker
            """,
        )
    finally:
        sess.close()
    return [{"label": name, "value": ticker} for ticker, name in rows]


def list_company_choices(
    db_path: str | None = None,
) -> list[dict]:
    """Return one entry per company that has standardized financial data,
    formatted for autocomplete/selection UIs:

        {
          "label": "<company name>",
          "value": "<primary ticker>",
          "extraInfo": {
            "description": "<ticker> | <cik>",
            "rightOfDescription": "<sic name>"
          }
        }

    Ordered by SEC's source rank (largest by market cap first).
    """
    sess = _session(db_path)
    try:
        rows = _rows(
            sess,
            f"""
            WITH sic AS (
                SELECT cik, val_text AS sic_name
                  FROM (
                    SELECT cik, val_text,
                           row_number() OVER (PARTITION BY cik ORDER BY "end" DESC) AS rn
                      FROM {DATABASE_NAME}.facts
                     WHERE namespace='dei' AND tag='EntitySicDescription'
                  )
                 WHERE rn = 1
            )
            SELECT pt.ticker        AS ticker,
                   c.entity_name    AS name,
                   pt.cik           AS cik,
                    min(t.rank)      AS rank,
                   sic.sic_name     AS sic_name
              FROM {DATABASE_NAME}.primary_tickers pt
              JOIN {DATABASE_NAME}.companies        c  ON c.cik  = pt.cik
                JOIN {DATABASE_NAME}.tickers          t  ON t.cik  = pt.cik AND t.ticker = pt.ticker
              JOIN {DATABASE_NAME}.processed_ciks   p  ON p.cik  = pt.cik
              LEFT JOIN sic                            ON sic.cik = pt.cik
             WHERE p.has_balance OR p.has_income OR p.has_cash_flow
               GROUP BY pt.ticker, c.entity_name, pt.cik, sic.sic_name
               ORDER BY rank ASC, ticker ASC
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
                "rightOfDescription": sic_name or "",
            },
        }
        for ticker, name, cik, _rank, sic_name in rows
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
            f"WHERE entity_name ILIKE {_q(pat)} "
            f"ORDER BY entity_name LIMIT {int(limit)}",
        )
    finally:
        sess.close()
    return [(row[0], row[1]) for row in rows]


# SEC EDGAR's "stateOrCountry" code list (used in submissions JSON and in
# dei.EntityStateOfIncorporation).  Combines US states, Canadian provinces,
# and countries.  Authoritative source is SEC's EDGAR filer manual; the
# XBRL stpr / country taxonomies only cover a subset (US states only / ISO
# 3166-1 only) so we ship the SEC list directly.
SEC_LOCATION_CODES: dict[str, str] = {
    # US states + DC
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
    # US territories
    "GU": "Guam",
    "PR": "Puerto Rico",
    "VI": "Virgin Islands, U.S.",
    "X1": "United States",
    # Canadian provinces (SEC custom codes, not ISO)
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
    # Other countries (SEC custom codes)
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

# Sets for quick US-vs-foreign classification.
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
    """Decode a SEC EDGAR stateOrCountry code to its human name."""
    if not code:
        return None
    return SEC_LOCATION_CODES.get(code)


def _decode_state(code: str | None) -> str | None:
    """Only return a name if the code is a US state/territory."""
    if not code or code not in _US_STATE_CODES:
        return None
    return SEC_LOCATION_CODES.get(code)


def _decode_country(code: str | None) -> str | None:
    """Only return a name if the code is a foreign country (non-US-state)."""
    if not code or code in _US_STATE_CODES:
        return None
    return SEC_LOCATION_CODES.get(code)


def _decode_sic(code: str | None) -> str | None:
    """SIC descriptions live in the dei facts already (EntitySicDescription).
    Provided here as a stub so callers have one decode entry-point per code
    type; falls through to None if not pre-supplied."""
    return None  # company_profile already pulls EntitySicDescription


def company_profile(
    cik_or_ticker: str,
    db_path: str | None = None,
) -> dict[str, Any] | None:
    """Comprehensive company profile in one round-trip.

    Resolves a CIK or ticker, follows the multi-CIK ``cik_canonical``
    rollup if applicable, then assembles:

    * primary ticker / secondary tickers / name
    * canonical CIK + all related CIKs
    * exchanges (from submissions blob)
    * stateOfIncorporation + country code
    * SIC code + SIC description
    * latest shares outstanding + EntityPublicFloat (closest proxy for
      market cap — full market cap needs price data, not in this DB)
    * fiscal year end (MMDD)
    * earliest + latest period captured (from standardized_statements)

    Returns None if the input doesn't resolve.
    """
    # Resolve input → set of CIKs (handles ticker, single CIK, multi-CIK).
    sess = _session(db_path)
    try:
        token = str(cik_or_ticker).strip()
        ciks: list[str] = []
        if token.upper() != token.lower() or not token.isdigit():
            # Likely a ticker — try resolution.
            ciks = resolve_symbol(token, db_path)
        if not ciks:
            try:
                ciks = [_zpad_cik(token)]
            except OpenBBError:
                return None

        # Canonical CIK (newest) + all related CIKs that roll up to it.
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
        # All CIKs that roll up to the primary, including the primary itself
        # (used for the IN clause).  ``related_ciks`` returned to the caller
        # excludes the primary so it doesn't double-list it.
        all_ciks = sorted({r[0] for r in related_rows} | {primary_cik})
        related_ciks = [c for c in all_ciks if c != primary_cik]
        ciks_in = "(" + ",".join(_q(c) for c in all_ciks) + ")"

        # Entity name.
        name_row = _row(
            sess,
            f"SELECT entity_name FROM {DATABASE_NAME}.companies WHERE cik = {_q(primary_cik)} LIMIT 1",
        )
        entity_name = name_row[0] if name_row else None

        # Tickers — primary + secondaries (across the related CIKs).
        ticker_rows = _rows(
            sess,
            f"SELECT ticker, is_primary, cik FROM {DATABASE_NAME}.tickers "
            f"WHERE cik IN {ciks_in} ORDER BY is_primary DESC, length(ticker), ticker",
        )
        primary_ticker = next((t for t, p, _ in ticker_rows if p == 1), None)
        # Fallback: when none flagged primary (xref pre-dates is_primary), pick
        # shortest+alpha-first.
        if primary_ticker is None and ticker_rows:
            primary_ticker = ticker_rows[0][0]
        secondary_tickers = [t for t, p, _ in ticker_rows if t != primary_ticker]

        # DEI scalars (latest per tag across related CIKs).  Only tags
        # with high coverage (>=90% of submissions) included.
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
            SELECT tag, val, val_text, "end"
              FROM (
                SELECT tag, val, val_text, "end",
                       row_number() OVER (
                           PARTITION BY tag ORDER BY "end" DESC, filed DESC
                       ) AS rn
                  FROM {DATABASE_NAME}.facts
                 WHERE cik IN {ciks_in}
                   AND namespace='dei' AND tag IN {tag_in}
              )
             WHERE rn = 1
            """,
        )
        dei = {tag: {"val": val, "val_text": val_text, "end": end} for tag, val, val_text, end in dei_rows}

        # Period range from standardized_statements (latest = "as-of").
        period_row = _row(
            sess,
            f"""
            SELECT min(period_ending), max(period_ending)
              FROM {DATABASE_NAME}.standardized_statements
             WHERE cik IN {ciks_in}
            """,
        )
        first_period, latest_period = (None, None)
        if period_row and period_row[0] is not None:
            first_period, latest_period = period_row

        # Submissions: exchanges only (address fields dropped — too patchy
        # across the universe; major listings like Shell have null
        # addresses.business.stateOrCountry).
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
    """Return the full submissions JSON payload for a CIK (gzip-decompressed).

    The payload is stored as gzip-compressed JSON in a ``String`` column.
    The stored payload is base64-decoded and gzip-decompressed before JSON
    parsing.
    """
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
        f"SELECT max(s.period_ending) FROM {DATABASE_NAME}.standardized_statements s WHERE {where_sql}",
    )
    if not row or row[0] is None:
        return None
    return row[0].isoformat()


def _canonical_standardized_cte(where_sql: str) -> str:
    return f"""
    WITH ranked AS (
        SELECT
            coalesce(cc.primary_cik, s.cik) AS canonical_cik,
            s.cik AS source_cik,
            s.statement,
            s.tag,
            s.period_ending,
            s.fiscal_year,
            s.fiscal_period,
            s.frequency,
            s.company_type,
            s.currency,
            s.val,
            row_number() OVER (
                PARTITION BY coalesce(cc.primary_cik, s.cik), s.statement, s.tag, s.period_ending
                ORDER BY if(s.cik = coalesce(cc.primary_cik, s.cik), 1, 0) DESC, s.cik DESC
            ) AS rn
        FROM {DATABASE_NAME}.standardized_statements s
        LEFT JOIN {DATABASE_NAME}.cik_canonical cc ON cc.cik = s.cik
        WHERE {where_sql}
    )
    """


def top_companies_by_metric(
    statement: str,
    tag: str,
    limit: int = 25,
    exclude_financial_template: bool = True,
    frequency: str | None = "annual",
    negate: bool = False,
    db_path: str | None = None,
) -> list[dict[str, Any]]:
    """Top N companies by a standardized line item.

    frequency='annual' uses the most recent full-year period (default).
    frequency=None uses the most recent period regardless of quarterly/annual.
    All values converted to USD via nearest prior ECB exchange rate.
    """
    sess = _session(db_path)
    try:
        where_sql = _standardized_where_clause(
            statement=statement,
            tag=tag,
            frequency=frequency,
        )
        oi_eq_rev_cte = (
            f"""
            , operating_income_equals_revenue AS (
                SELECT DISTINCT coalesce(cc.primary_cik, oi.cik) AS canonical_cik
                FROM {DATABASE_NAME}.standardized_statements oi
                LEFT JOIN {DATABASE_NAME}.cik_canonical cc ON cc.cik = oi.cik
                JOIN {DATABASE_NAME}.standardized_statements rev
                    ON rev.cik = oi.cik
                    AND rev.period_ending = oi.period_ending
                    AND rev.frequency = 'annual'
                    AND rev.tag = 'total_revenue'
                WHERE oi.tag = 'total_operating_income'
                  AND oi.frequency = 'annual'
                  AND oi.period_ending >= addYears(today(), -2)
                  AND rev.val > 1e9
                  AND oi.val >= rev.val * 0.99
            )"""
            if tag == "total_operating_income"
            else ""
        )
        rows = _rows(
            sess,
            _canonical_standardized_cte(where_sql)
            + f"""
            , latest AS (
                SELECT *,
                    row_number() OVER (
                        PARTITION BY canonical_cik ORDER BY period_ending DESC
                    ) AS company_rn
                FROM ranked
                WHERE rn = 1
            )
            , gross_presenters AS (
                SELECT DISTINCT rev.cik, rev.period_ending
                FROM {DATABASE_NAME}.standardized_statements rev
                JOIN {DATABASE_NAME}.standardized_statements gp
                    ON gp.cik = rev.cik
                    AND gp.period_ending = rev.period_ending
                    AND gp.frequency = 'annual'
                    AND gp.tag = 'total_gross_profit'
                WHERE rev.tag = 'total_revenue'
                  AND rev.frequency = 'annual'
                  AND rev.period_ending >= addYears(today(), -1)
                  AND gp.val IS NOT NULL
                  AND rev.val > 0
                  AND gp.val / rev.val < 0.01
            )
            , zero_cogs_filers AS (
                SELECT DISTINCT coalesce(cc.primary_cik, cogs.cik) AS canonical_cik
                FROM {DATABASE_NAME}.standardized_statements cogs
                LEFT JOIN {DATABASE_NAME}.cik_canonical cc ON cc.cik = cogs.cik
                JOIN {DATABASE_NAME}.standardized_statements rev
                    ON rev.cik = cogs.cik
                    AND rev.period_ending = cogs.period_ending
                    AND rev.frequency = 'annual'
                    AND rev.tag = 'total_revenue'
                WHERE cogs.tag = 'total_cost_of_revenue'
                  AND cogs.frequency = 'annual'
                  AND cogs.period_ending >= addYears(today(), -2)
                  AND cogs.val = 0
                  AND rev.val > 1e9
            )
            , implausible_vals AS (
                SELECT DISTINCT a.canonical_cik
                FROM (
                    SELECT
                        coalesce(cc.primary_cik, s.cik) AS canonical_cik,
                        s.val,
                        lagInFrame(s.val) OVER (
                            PARTITION BY coalesce(cc.primary_cik, s.cik)
                            ORDER BY s.period_ending
                            ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
                        ) AS prev_val
                    FROM {DATABASE_NAME}.standardized_statements s
                    LEFT JOIN {DATABASE_NAME}.cik_canonical cc ON cc.cik = s.cik
                    WHERE s.statement = {_q(statement)}
                      AND s.tag = {_q(tag)}
                      AND s.frequency = 'annual'
                      AND s.val > 0
                ) a
                WHERE a.prev_val > 0 AND a.val / a.prev_val > 500
            )
            {oi_eq_rev_cte}
            , pairs AS (
                SELECT DISTINCT currency, period_ending
                FROM latest
                WHERE company_rn = 1 AND currency != 'USD'
            )
            , nearest_rates AS (
                SELECT p.currency, p.period_ending,
                    argMax(er.rate, er.rate_date) AS rate
                FROM pairs p
                LEFT JOIN {DATABASE_NAME}.exchange_rates er
                    ON er.from_currency = p.currency
                    AND er.to_currency = 'USD'
                    AND er.rate_date <= p.period_ending
                GROUP BY p.currency, p.period_ending
            )
            SELECT
                ifNull(pt.ticker, '') AS ticker,
                ifNull(c.entity_name, '') AS name,
                toString(l.period_ending) AS period_ending,
                l.fiscal_year,
                l.fiscal_period,
                l.currency,
                l.val * if(l.currency = 'USD', 1.0, nr.rate) * {"-1.0" if negate else "1.0"} AS value_usd
            FROM latest l
            LEFT JOIN nearest_rates nr ON nr.currency = l.currency AND nr.period_ending = l.period_ending
            LEFT JOIN {DATABASE_NAME}.companies c ON c.cik = l.canonical_cik
            LEFT JOIN {_primary_ticker_by_cik()} pt ON pt.cik = l.canonical_cik
            WHERE l.company_rn = 1
              AND l.period_ending >= addYears(today(), -1)
              AND (l.currency = 'USD' OR nr.rate IS NOT NULL)
              AND (l.source_cik, l.period_ending) NOT IN (SELECT cik, period_ending FROM gross_presenters)
              AND l.canonical_cik NOT IN (SELECT canonical_cik FROM implausible_vals)
              AND pt.ticker != ''
              {"AND l.val < 0" if negate else ""}
              {"AND l.canonical_cik NOT IN (SELECT canonical_cik FROM operating_income_equals_revenue)" if tag == "total_operating_income" else ""}
              {"AND l.company_type NOT IN ('financial', 'insurance')" if exclude_financial_template else ""}
              {"AND l.canonical_cik NOT IN (SELECT canonical_cik FROM zero_cogs_filers)" if exclude_financial_template else ""}
            ORDER BY value_usd DESC
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


def top_companies_by_sum(
    statement: str,
    tag_a: str,
    tag_b: str,
    limit: int = 25,
    exclude_financial_template: bool = True,
    db_path: str | None = None,
) -> list[dict[str, Any]]:
    """Rank companies by the sum of two tags (e.g. FCF = OCF + capex where capex is negative)."""
    sess = _session(db_path)
    where_a = _standardized_where_clause(statement=statement, tag=tag_a, frequency="annual")
    where_b = _standardized_where_clause(statement=statement, tag=tag_b, frequency="annual")
    try:
        rows = _rows(
            sess,
            f"""
            WITH a_ranked AS (
                SELECT
                    coalesce(cc.primary_cik, s.cik) AS canonical_cik,
                    s.cik AS source_cik,
                    s.period_ending, s.fiscal_year, s.fiscal_period, s.currency, s.val,
                    s.company_type,
                    row_number() OVER (
                        PARTITION BY coalesce(cc.primary_cik, s.cik), s.period_ending
                        ORDER BY if(s.cik = coalesce(cc.primary_cik, s.cik), 1, 0) DESC, s.cik DESC
                    ) AS rn
                FROM {DATABASE_NAME}.standardized_statements s
                LEFT JOIN {DATABASE_NAME}.cik_canonical cc ON cc.cik = s.cik
                WHERE {where_a}
            )
            , b_ranked AS (
                SELECT
                    coalesce(cc.primary_cik, s.cik) AS canonical_cik,
                    s.period_ending, s.val,
                    row_number() OVER (
                        PARTITION BY coalesce(cc.primary_cik, s.cik), s.period_ending
                        ORDER BY if(s.cik = coalesce(cc.primary_cik, s.cik), 1, 0) DESC, s.cik DESC
                    ) AS rn
                FROM {DATABASE_NAME}.standardized_statements s
                LEFT JOIN {DATABASE_NAME}.cik_canonical cc ON cc.cik = s.cik
                WHERE {where_b}
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
                SELECT p.currency, p.period_ending,
                    argMax(er.rate, er.rate_date) AS rate
                FROM pairs p
                LEFT JOIN {DATABASE_NAME}.exchange_rates er
                    ON er.from_currency = p.currency
                    AND er.to_currency = 'USD'
                    AND er.rate_date <= p.period_ending
                GROUP BY p.currency, p.period_ending
            )
            SELECT
                ifNull(pt.ticker, '') AS ticker,
                ifNull(c.entity_name, '') AS name,
                toString(s.period_ending) AS period_ending,
                s.fiscal_year,
                s.fiscal_period,
                s.currency,
                s.val * if(s.currency = 'USD', 1.0, nr.rate) AS value_usd
            FROM summed s
            LEFT JOIN nearest_rates nr ON nr.currency = s.currency AND nr.period_ending = s.period_ending
            LEFT JOIN {DATABASE_NAME}.companies c ON c.cik = s.canonical_cik
            LEFT JOIN {_primary_ticker_by_cik()} pt ON pt.cik = s.canonical_cik
            WHERE s.company_rn = 1
              AND s.period_ending >= addYears(today(), -1)
              AND (s.currency = 'USD' OR nr.rate IS NOT NULL)
              AND pt.ticker != ''
              {"AND s.company_type NOT IN ('financial', 'insurance')" if exclude_financial_template else ""}
            ORDER BY value_usd DESC
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
    db_path: str | None = None,
) -> list[dict[str, Any]]:
    sess = _session(db_path)
    denom_stmt = denominator_statement if denominator_statement is not None else statement
    num_where = _standardized_where_clause(statement=statement, tag=numerator_tag, frequency="annual")
    denom_where = _standardized_where_clause(statement=denom_stmt, tag=denominator_tag, frequency="annual")
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
                    coalesce(cc.primary_cik, s.cik) AS canonical_cik,
                    s.cik AS source_cik,
                    s.company_type,
                    s.period_ending, s.fiscal_year, s.fiscal_period, s.val,
                    row_number() OVER (
                        PARTITION BY coalesce(cc.primary_cik, s.cik), s.period_ending
                        ORDER BY if(s.cik = coalesce(cc.primary_cik, s.cik), 1, 0) DESC, s.cik DESC
                    ) AS rn
                FROM {DATABASE_NAME}.standardized_statements s
                LEFT JOIN {DATABASE_NAME}.cik_canonical cc ON cc.cik = s.cik
                WHERE {num_where}{financial_filter_sql}
            )
            , denom_ranked AS (
                SELECT
                    coalesce(cc.primary_cik, s.cik) AS canonical_cik,
                    s.period_ending, s.val,
                    row_number() OVER (
                        PARTITION BY coalesce(cc.primary_cik, s.cik), s.period_ending
                        ORDER BY if(s.cik = coalesce(cc.primary_cik, s.cik), 1, 0) DESC, s.cik DESC
                    ) AS rn
                FROM {DATABASE_NAME}.standardized_statements s
                LEFT JOIN {DATABASE_NAME}.cik_canonical cc ON cc.cik = s.cik
                WHERE {denom_where}
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
            SELECT
                ifNull(pt.ticker, '') AS ticker,
                ifNull(c.entity_name, '') AS name,
                toString(p.period_ending) AS period_ending,
                p.fiscal_year,
                p.fiscal_period,
                round(p.num_val / p.denom_val * 100, 2) AS value
            FROM paired p
            LEFT JOIN {DATABASE_NAME}.companies c ON c.cik = p.canonical_cik
            LEFT JOIN {_primary_ticker_by_cik()} pt ON pt.cik = p.canonical_cik
            WHERE p.company_rn = 1
              AND p.period_ending >= addYears(today(), -1)
              AND pt.ticker != ''{bounds_sql}
            ORDER BY value DESC
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
