"""FastAPI backend for the OpenBB Workspace SEC Financial Statements app.

Exposes:
* GET /balance_sheet?symbol=AAPL&period=FY&transform=None&limit=10
* GET /income_statement?symbol=AAPL&period=FY&transform=None&limit=10
* GET /cash_flow?symbol=AAPL&period=FY&transform=None&limit=10
* GET /widgets.json
* GET /apps.json

``period`` selects the underlying data frequency and ``transform`` selects
the optional growth calculation applied on top — the two are independent
because YoY / PoP can be applied to TTM data as well as to raw FY / Q.

Period mapping for raw values (transform=None):
  FY  -> period='annual'
  Q   -> period='quarterly'
  TTM -> period='ttm'

Growth mapping (transform != None) routes through the
``Sec*GrowthFetcher`` family — they accept a wider period vocabulary that
embeds the YoY / PoP semantics:
  (FY,  % YoY) -> growth period='annual'         (annual YoY %)
  (Q,   % YoY) -> growth period='quarterly_yoy'  (same Q vs prior year %)
  (Q,   % PoP) -> growth period='quarterly'      (sequential quarter %)
  (TTM, % PoP) -> growth period='ttm'            (TTM PoP %)
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Literal

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from openbb_sec.models.balance_sheet import (
    SecBalanceSheetFetcher,
)
from openbb_sec.models.balance_sheet_growth import (
    SecBalanceSheetGrowthData,
    SecBalanceSheetGrowthFetcher,
)
from openbb_sec.models.cash_flow import (
    SecCashFlowStatementFetcher,
)
from openbb_sec.models.cash_flow_growth import (
    SecCashFlowStatementGrowthData,
    SecCashFlowStatementGrowthFetcher,
)
from openbb_sec.models.income_statement import (
    SecIncomeStatementFetcher,
)
from openbb_sec.models.income_statement_growth import (
    SecIncomeStatementGrowthData,
    SecIncomeStatementGrowthFetcher,
)

# Serve standardized financials from the local Dolt store. The openbb-sec
# fetchers import ``get_standardized_financials`` from the provider at call time,
# so rebinding it here points them at the DB-backed implementation (precomputed
# table + provider standardization fallback) instead of re-fetching per request.
import openbb_sec.utils.company_facts as _provider_company_facts
from sec_app.standardize import get_standardized_financials as _db_get_standardized_financials

_provider_company_facts.get_standardized_financials = _db_get_standardized_financials

PeriodChoice = Literal["FY", "Q", "TTM"]
TransformChoice = Literal["None", "% YoY", "% PoP"]

_PERIOD_MAP: dict[str, str] = {
    "FY": "annual",
    "Q": "quarterly",
    "TTM": "ttm",
}

# (period, transform) -> period kwarg for the *Growth fetcher.
# TTM intentionally absent: the upstream growth fetcher's "ttm" mapping
# resolves to PeriodType "pop" which runs on quarterly (not TTM) records.
# (TTM, %YoY) and (TTM, %PoP) are computed locally from raw TTM rows.
# Annual PoP === Annual YoY (the prior period IS the prior year), so both
# (FY, %YoY) and (FY, %PoP) route to growth period="annual".
_GROWTH_MAP: dict[tuple[str, str], str] = {
    ("FY", "% YoY"): "annual",
    ("FY", "% PoP"): "annual",
    ("Q", "% YoY"): "quarterly_yoy",
    ("Q", "% PoP"): "quarterly",
}

# Raw fetcher -> growth Data class for the TTM-local growth path.  Keyed by
# raw fetcher so the right Pydantic schema is used to wrap the computed rows.
_GROWTH_DATA_FOR = {
    SecBalanceSheetFetcher: SecBalanceSheetGrowthData,
    SecIncomeStatementFetcher: SecIncomeStatementGrowthData,
    SecCashFlowStatementFetcher: SecCashFlowStatementGrowthData,
}

_TTM_GROWTH_ID_FIELDS = (
    "period_ending",
    "fiscal_period",
    "fiscal_year",
    "reported_currency",
)

_NON_MONETARY_FIELDS = frozenset({"period_ending", "fiscal_year", "fiscal_period", "reported_currency"})


def _convert_rows_to_usd(rows):
    """Return a new list of model instances with monetary fields converted to USD.

    Uses the nearest available ECB rate on or before each period_ending date
    (within a 14-day lookback window) to handle weekends and holidays.
    Rows already in USD, or with no reported_currency, are returned unchanged.
    If a rate cannot be found the row is returned unconverted (best-effort).
    """
    import bisect
    from collections import defaultdict

    if not rows:
        return rows

    pairs: set[tuple] = set()
    for row in rows:
        currency = getattr(row, "reported_currency", None)
        period = getattr(row, "period_ending", None)
        if currency and currency != "USD" and period is not None:
            pairs.add((str(period), currency))

    if not pairs:
        return rows

    try:
        from sec_app.db.backend import connect_read

        client = connect_read()
        all_dates = sorted({p[0] for p in pairs})
        currencies = list({p[1] for p in pairs})
        min_date = all_dates[0]
        max_date = all_dates[-1]
        sql = (
            "SELECT CAST(rate_date AS CHAR), from_currency, rate "
            "FROM exchange_rates "
            "WHERE from_currency IN ({currencies}) "
            "AND to_currency = 'USD' "
            "AND rate_date BETWEEN CAST('{min_date}' AS DATE) - INTERVAL 14 DAY "
            "AND CAST('{max_date}' AS DATE) "
            "ORDER BY from_currency, rate_date"
        ).format(
            currencies=", ".join(f"'{c}'" for c in currencies),
            min_date=min_date,
            max_date=max_date,
        )
        result = client.execute(sql)
        rates_by_currency: dict[str, list] = defaultdict(list)
        for r in result.fetchall():
            rates_by_currency[r[1]].append((r[0], r[2]))
        rate_map: dict[tuple, float] = {}
        for date_str, currency in pairs:
            rate_list = rates_by_currency.get(currency, [])
            if not rate_list:
                continue
            idx = bisect.bisect_right(rate_list, (date_str, float("inf"))) - 1
            if idx >= 0:
                rate_map[(date_str, currency)] = rate_list[idx][1]
        client.close()
    except Exception:
        return rows

    converted = []
    model = type(rows[0])
    monetary_fields = [
        f
        for f, info in model.model_fields.items()
        if f not in _NON_MONETARY_FIELDS
        and ("float" in str(info.annotation).lower() or "int" in str(info.annotation).lower())
    ]
    for row in rows:
        currency = getattr(row, "reported_currency", None)
        period = getattr(row, "period_ending", None)
        if not currency or currency == "USD" or period is None:
            converted.append(row)
            continue
        rate = rate_map.get((str(period), currency))
        if rate is None:
            converted.append(row)
            continue
        updates = {f: getattr(row, f) * rate for f in monetary_fields if getattr(row, f, None) is not None}
        updates["reported_currency"] = "USD"
        converted.append(row.model_copy(update=updates))
    return converted


async def _fetch_ttm_growth(raw_fetcher, symbol: str, mode: str):
    """Compute TTM YoY or TTM PoP locally from raw TTM rows.

    The upstream Sec*GrowthFetcher's ``period="ttm"`` actually runs PoP on
    quarterly data (not on TTM aggregates), so neither YoY nor PoP on TTM
    is correctly served by the existing mapping.  We pull raw TTM via the
    raw fetcher (which produces one row per quarter end with 4-quarter
    rolling sums for duration items and averages for instant items), sort
    ascending, and walk index-offset pairs:

      * mode="yoy" -> offset 4 (same calendar quarter, prior year)
      * mode="pop" -> offset 1 (immediately preceding TTM)

    The resulting rows are validated as the corresponding *GrowthData
    Pydantic class so downstream display logic treats them identically to
    fetcher-produced growth rows.
    """
    growth_cls = _GROWTH_DATA_FOR[raw_fetcher]
    result = await raw_fetcher.fetch_data({"symbol": symbol, "period": "ttm", "limit": None}, {})
    rows, metadata = _extract_rows_and_metadata(result)
    if not rows:
        return [], _ttm_growth_metadata(metadata, mode)

    serialized = [r.model_dump(mode="json") for r in rows]
    serialized.sort(key=lambda r: r.get("period_ending") or "")

    offset = 4 if mode == "yoy" else 1
    growth_field_names = set(growth_cls.model_fields.keys())
    id_fields = set(_TTM_GROWTH_ID_FIELDS)

    output_dicts: list[dict] = []
    for i in range(offset, len(serialized)):
        cur = serialized[i]
        prior = serialized[i - offset]
        out: dict = {f: cur.get(f) for f in _TTM_GROWTH_ID_FIELDS}
        for k, v in cur.items():
            if k in id_fields or v is None or not isinstance(v, (int, float)):
                continue
            pv = prior.get(k)
            if pv is None or pv == 0 or not isinstance(pv, (int, float)):
                continue
            growth_key = f"growth_{k}"
            if growth_key not in growth_field_names:
                continue
            out[growth_key] = round((v - pv) / abs(pv), 4)
        output_dicts.append(out)

    output_dicts.sort(key=lambda r: r.get("period_ending") or "", reverse=True)
    return [growth_cls.model_validate(d) for d in output_dicts], _ttm_growth_metadata(metadata, mode)


_HERE = Path(__file__).resolve().parent


def _extract_rows_and_metadata(fetch_result):
    if hasattr(fetch_result, "result"):
        return fetch_result.result, getattr(fetch_result, "metadata", {}) or {}
    return fetch_result, {}


def _humanize_source_label(source: str) -> str:
    label = " ".join(source.split())

    if label.lower().startswith("derived:"):
        derived_body = label.split(":", 1)[1].strip()
        derived_field = derived_body.split("(", 1)[0].strip()
        if derived_field:
            return f"derived from prior period {_humanize(derived_field)}"
        return "derived from prior period"

    if "(fallback)" in label:
        base = label.replace("(fallback)", "").strip()
        return f"{base} (tag sourced from filing vintage other than original)"

    return label


def _source_group_key(source: str) -> str:
    normalized = " ".join(source.split())
    if normalized.lower().startswith("derived:"):
        derived_body = normalized.split(":", 1)[1].strip()
        derived_field = derived_body.split("(", 1)[0].strip()
        if derived_field:
            return f"derived:{derived_field.lower()}"
        return "derived"
    return normalized


def _format_provenance_from_sources(sources) -> str:
    if not isinstance(sources, dict) or not sources:
        return "No source provenance returned by fetcher."

    period_entries: list[tuple[str, str, str]] = []
    for date_key in sorted(sources, reverse=True):
        src = sources.get(date_key)
        if isinstance(src, str) and src:
            normalized = _source_group_key(src)
            period_entries.append((date_key, _humanize_source_label(src), normalized))

    if not period_entries:
        return "No source provenance returned by fetcher."

    unique_sources = {normalized for _, _, normalized in period_entries}
    if len(unique_sources) == 1:
        return f"Source: {period_entries[0][1]}"

    runs: list[dict[str, str | int]] = []
    run_start, run_src, run_norm = period_entries[0]
    run_end = run_start
    run_count = 1
    for date_key, src, normalized in period_entries[1:]:
        if normalized == run_norm:
            run_end = date_key
            run_count += 1
            continue

        runs.append(
            {
                "start": run_start,
                "end": run_end,
                "src": run_src,
                "count": run_count,
            }
        )
        run_start, run_end, run_src, run_norm = date_key, date_key, src, normalized
        run_count = 1

    runs.append(
        {
            "start": run_start,
            "end": run_end,
            "src": run_src,
            "count": run_count,
        }
    )

    lines: list[str] = []
    for run in runs[:8]:
        start = run["start"]
        end = run["end"]
        src = run["src"]
        if start == end:
            lines.append(f"- {start}: {src}")
        else:
            lines.append(f"- {start} to {end}: {src}")

    if len(runs) > 8:
        lines.append(f"- +{len(runs) - 8} more source runs")

    return "Sources by period:\n" + "\n".join(lines)


def _line_item_metadata(field: str, field_metadata: dict[str, dict]) -> dict[str, str]:
    unknown = {
        "description": "No metadata available for this standardized line item.",
        "provenance": "No source provenance returned by fetcher.",
    }

    meta = field_metadata.get(field)
    if not isinstance(meta, dict):
        return unknown

    description = meta.get("description")
    if not isinstance(description, str) or not description.strip():
        description = unknown["description"]

    return {
        "description": description,
        "provenance": _format_provenance_from_sources(meta.get("sources")),
    }


def _ttm_growth_metadata(raw_metadata: dict, mode: str) -> dict:
    fields = raw_metadata.get("fields") if isinstance(raw_metadata, dict) else {}
    if not isinstance(fields, dict):
        fields = {}

    growth_fields: dict[str, dict] = {}
    for tag, info in fields.items():
        if not isinstance(tag, str) or not isinstance(info, dict):
            continue
        base_desc = info.get("description")
        if isinstance(base_desc, str) and base_desc.strip():
            desc = (
                f"Year-over-year growth for {base_desc}"
                if mode == "yoy"
                else f"Period-over-period growth for {base_desc}"
            )
        else:
            desc = (
                f"Year-over-year growth for {_humanize(tag)}"
                if mode == "yoy"
                else f"Period-over-period growth for {_humanize(tag)}"
            )

        growth_info = dict(info)
        growth_info["description"] = desc
        growth_fields[f"growth_{tag}"] = growth_info

    out = dict(raw_metadata) if isinstance(raw_metadata, dict) else {}
    out["fields"] = growth_fields
    out["derived"] = "local_ttm_growth"
    return out


app = FastAPI(
    title="SEC Financial Statements Widgets",
    version="0.1.0",
    description="OpenBB Workspace widget backend for the SEC standardized "
    "balance sheet, income statement, and cash flow statement.",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=False,
)


def _ci_lookup(value: str, choices) -> str | None:
    """Return the canonical choice matching ``value`` case-insensitively."""
    return next((c for c in choices if c.lower() == value.lower()), None)


async def _fetch_statement(
    raw_fetcher,
    growth_fetcher,
    symbol: str,
    period: str,
    transform: str,
    limit: int,
):
    """Run the appropriate fetcher (raw or growth) and return Data instances.

    Returning Pydantic model instances directly lets FastAPI's serializer
    invoke ``@model_serializer`` (NaN -> None, declared field order
    preserved).  ``limit=0`` (or negative) returns every period.
    """
    period_key = _ci_lookup(period, _PERIOD_MAP)
    if period_key is None:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid period {period!r}; expected one of " + ", ".join(_PERIOD_MAP),
        )

    transform_key = _ci_lookup(transform, ("None", "% YoY", "% PoP"))
    if transform_key is None:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid transform {transform!r}; expected None, % YoY, or % PoP",
        )

    try:
        if transform_key == "None":
            # Force ``limit=None`` so the fetcher returns every available
            # period.  The cash-flow standard model defaults limit to 5,
            # which would silently truncate before our widget-level slice.
            result = await raw_fetcher.fetch_data(
                {"symbol": symbol, "period": _PERIOD_MAP[period_key], "limit": None},
                {},
            )
            rows, metadata = _extract_rows_and_metadata(result)
        elif period_key == "TTM":
            # Local YoY/PoP over raw TTM rows — see _fetch_ttm_growth.
            mode = "yoy" if transform_key == "% YoY" else "pop"
            rows, metadata = await _fetch_ttm_growth(raw_fetcher, symbol, mode)
        else:
            combo = (period_key, transform_key)
            if combo not in _GROWTH_MAP:
                raise HTTPException(
                    status_code=400,
                    detail=f"Transform {transform_key!r} is not available for period {period_key!r}",
                )
            result = await growth_fetcher.fetch_data(
                {"symbol": symbol, "period": _GROWTH_MAP[combo], "limit": None},
                {},
            )
            rows, metadata = _extract_rows_and_metadata(result)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    if limit and limit > 0:
        rows = rows[:limit]
    return rows, metadata


_LABEL_KEY = "Line Item"
_SKIP_FIELDS = frozenset({"period_ending"})  # already used as the column key

# Per-field value formatters — applied during transpose so the workspace's
# numeric formatter doesn't render integers (like fiscal_year) with thousands
# separators ("FY 2,024").
_VALUE_FORMATTERS = {
    "fiscal_year": lambda v: None if v is None else f"FY{int(v)}",
}
_ACRONYMS = {
    "ppe": "PPE",
    "nci": "NCI",
    "aoci": "AOCI",
    "ttm": "TTM",
    "yoy": "YoY",
    "pop": "PoP",
    "usd": "USD",
    "us": "US",
    "ifrs": "IFRS",
    "gaap": "GAAP",
    "eps": "EPS",
    "ebit": "EBIT",
    "ebitda": "EBITDA",
    "fy": "FY",
    "ar": "AR",
    "ap": "AP",
    "sga": "SG&A",
    "rd": "R&D",
    "os": "OS",
}


def _humanize(field: str) -> str:
    """snake_case → Title Case, with common financial acronyms uppercased."""
    return " ".join(_ACRONYMS.get(p, p.capitalize()) for p in field.split("_"))


def _is_imputed(values) -> bool:
    """A field carries no real data when every period is None or 0.

    SEC standardization fills missing tags with 0 for additivity (so a
    bank's blank-on-AAPL ``loans_and_leases`` doesn't break a sum); those
    imputed zeros are noise in the display.
    """
    return all(v is None or v == 0 for v in values)


def _untransposed(rows):
    """Return wide period-major rows (one dict per period).

    Drops columns that are None across every period and applies the same
    per-field formatters as ``_transpose_for_widget``.  Field declaration
    order from the Pydantic model is preserved on every emitted dict.
    """
    if not rows:
        return []

    model = type(rows[0])
    serialized = [r.model_dump(mode="json") for r in rows]
    # Charts plot rows left-to-right; sort oldest-first so the time axis
    # reads chronologically.  (Transposed mode keeps newest-first columns.)
    serialized.sort(key=lambda r: r.get("period_ending") or "")

    keep_fields: list[str] = []
    for field in model.model_fields:
        values = [r.get(field) for r in serialized]
        if _is_imputed(values):
            continue
        keep_fields.append(field)

    output: list[dict] = []
    for r in serialized:
        out: dict = {}
        for field in keep_fields:
            v = r.get(field)
            formatter = _VALUE_FORMATTERS.get(field)
            if formatter is not None:
                v = formatter(v)
            out[field] = v
        output.append(out)
    return output


def _transpose_for_widget(rows, field_metadata: dict[str, dict]):
    """Pivot wide period-major rows into long field-major rows.

    Input  : list of Pydantic Data instances, one per period (most-recent
             first), each carrying every field declared on the model.
    Output : list of dicts, one per *field*, with one column per period.
             - field declaration order preserved
             - fields whose value is None / unset for every period are dropped
             - period_ending is used as the column key, so it is not also
               emitted as its own row

    Example output row:
      {"Line Item": "Total Assets",
       "2025-09-27": 359241000000.0,
       "2024-09-28": 364980000000.0}
    """
    if not rows:
        return []

    model = type(rows[0])
    serialized = [r.model_dump(mode="json") for r in rows]
    # Force newest-first columns regardless of fetcher order, then namespace
    # the column key with fiscal_period so switching FY ↔ Q ↔ TTM cannot
    # collide on a shared anchor date (e.g. AAPL's Sept-27 quarter end is
    # both an FY end and a TTM anchor).  Without the prefix the workspace
    # carries over the prior column slot and the new period's data lands in
    # the wrong order.
    serialized.sort(key=lambda r: r.get("period_ending") or "", reverse=True)
    columns = [f"{r.get('fiscal_period') or ''} {r.get('period_ending') or ''}".strip() for r in serialized]

    output: list[dict] = []
    for field in model.model_fields:
        if field in _SKIP_FIELDS:
            continue
        values = [r.get(field) for r in serialized]
        if _is_imputed(values):
            continue
        formatter = _VALUE_FORMATTERS.get(field)
        if formatter is not None:
            values = [formatter(v) for v in values]
        line_item_meta = _line_item_metadata(field, field_metadata)
        out_row: dict = {
            _LABEL_KEY: _humanize(field),
            "description": line_item_meta["description"],
            "provenance": line_item_meta["provenance"],
        }
        for col, val in zip(columns, values):
            out_row[col] = val
        output.append(out_row)

    return output


def _shape(rows, transpose: bool, metadata: dict):
    field_metadata = metadata.get("fields") if isinstance(metadata, dict) else {}
    if not isinstance(field_metadata, dict):
        field_metadata = {}
    return _transpose_for_widget(rows, field_metadata) if transpose else _untransposed(rows)


@app.get("/balance_sheet")
async def balance_sheet(
    symbol: str = Query(..., description="Ticker symbol (e.g. AAPL)"),
    period: PeriodChoice = Query("FY", description="FY/Q/TTM"),
    transform: TransformChoice = Query("None", description="None / % YoY / % PoP"),
    transpose: bool = Query(True, description="Pivot fields to rows, periods to columns"),
    limit: int = Query(10, ge=0, description="Max rows; 0 = all"),
    convert_to_usd: bool = Query(True, description="Convert reported values to USD using historical ECB exchange rates."),
):
    rows, metadata = await _fetch_statement(
        SecBalanceSheetFetcher,
        SecBalanceSheetGrowthFetcher,
        symbol,
        period,
        transform,
        limit,
    )
    if convert_to_usd:
        rows = _convert_rows_to_usd(rows)
    return _shape(rows, transpose, metadata)


@app.get("/income_statement")
async def income_statement(
    symbol: str = Query(...),
    period: PeriodChoice = Query("FY"),
    transform: TransformChoice = Query("None"),
    transpose: bool = Query(True),
    limit: int = Query(10, ge=0),
    convert_to_usd: bool = Query(True, description="Convert reported values to USD using historical ECB exchange rates."),
):
    rows, metadata = await _fetch_statement(
        SecIncomeStatementFetcher,
        SecIncomeStatementGrowthFetcher,
        symbol,
        period,
        transform,
        limit,
    )
    if convert_to_usd:
        rows = _convert_rows_to_usd(rows)
    return _shape(rows, transpose, metadata)


@app.get("/cash_flow")
async def cash_flow(
    symbol: str = Query(...),
    period: PeriodChoice = Query("FY"),
    transform: TransformChoice = Query("None"),
    transpose: bool = Query(True),
    limit: int = Query(10, ge=0),
    convert_to_usd: bool = Query(True, description="Convert reported values to USD using historical ECB exchange rates."),
):
    rows, metadata = await _fetch_statement(
        SecCashFlowStatementFetcher,
        SecCashFlowStatementGrowthFetcher,
        symbol,
        period,
        transform,
        limit,
    )
    if convert_to_usd:
        rows = _convert_rows_to_usd(rows)
    return _shape(rows, transpose, metadata)


@app.get("/companies")
def companies():
    """Dropdown options for the symbol parameter — every company that has
    standardized statements, ordered by SEC source rank (largest first).
    Each entry is the {label, value, extraInfo: {...}} shape the workspace
    expects from an ``optionsEndpoint``."""
    from sec_app.db.query import list_company_choices

    return JSONResponse(list_company_choices())


_CURATED_METRICS = [
    {"label": "Revenue", "value": "income_statement|total_revenue"},
    {"label": "Net Income", "value": "income_statement|net_income"},
    {"label": "Operating Income", "value": "income_statement|total_operating_income"},
    {"label": "Gross Profit", "value": "income_statement|total_gross_profit"},
    {"label": "R&D Expense", "value": "income_statement|rd_expense"},
    {"label": "Total Assets", "value": "balance_sheet|total_assets"},
    {"label": "Return on Equity %", "value": "cross|return_on_equity"},
    {"label": "Long-Term Debt", "value": "balance_sheet|long_term_debt"},
    {"label": "Cash & Equivalents", "value": "balance_sheet|cash_and_equivalents"},
    {"label": "Total Debt", "value": "balance_sheet|long_term_debt+current_portion_of_long_term_debt"},
    {"label": "Operating Cash Flow", "value": "cash_flow|net_cash_from_operating_activities"},
    {
        "label": "Free Cash Flow",
        "value": "cash_flow|net_cash_from_operating_activities+purchase_of_plant_property_and_equipment",
    },
    {"label": "Capital Expenditures", "value": "cash_flow|purchase_of_plant_property_and_equipment"},
    {"label": "Gross Margin %", "value": "income_statement|total_gross_profit/total_revenue"},
    {"label": "Operating Margin %", "value": "income_statement|total_operating_income/total_revenue"},
    {"label": "Net Profit Margin %", "value": "income_statement|net_income/total_revenue"},
    {"label": "Net Interest Income", "value": "income_statement|net_interest_income"},
    {"label": "Net Interest Margin", "value": "bank|net_interest_margin"},
]


_BANK_RATIOS = {
    "bank|net_interest_margin": {
        "statement": "income_statement",
        "numerator_tag": "net_interest_income",
        "denominator_tag": "total_assets",
        "denominator_statement": "balance_sheet",
        "exclude_financial_template": False,
        "min_denominator": 1e9,
        "min_value": 0.0,
        "max_value": 20.0,
    },
    "cross|return_on_equity": {
        "statement": "income_statement",
        "numerator_tag": "net_income",
        "denominator_tag": "total_equity",
        "denominator_statement": "balance_sheet",
        "exclude_financial_template": True,
        "min_denominator": 1e8,
        "min_value": 0.0,
        "max_value": 500.0,
    },
}


@app.get("/metrics")
def metrics():
    return JSONResponse(_CURATED_METRICS)


@app.get("/top_companies")
def top_companies(
    metric: str = Query("income_statement|total_revenue"),
    limit: int = Query(25, ge=1, le=100),
    sector: str | None = Query(None),
    industry: str | None = Query(None),
):
    from sec_app.db.query import top_companies_by_ratio, top_companies_by_metric, top_companies_by_sum
    from sec_app.db.sectors import cik_in_clause

    # Optional sector/industry scope: resolve to a literal CIK IN-list once.
    cik_filter = cik_in_clause(sector or None, industry or None)
    if cik_filter == "":  # filter requested, but no company matched
        return JSONResponse([])

    if metric in _BANK_RATIOS:
        return JSONResponse(top_companies_by_ratio(**_BANK_RATIOS[metric], limit=limit, cik_filter=cik_filter))
    parts = metric.split("|", 1)
    if len(parts) != 2:
        from fastapi import HTTPException

        raise HTTPException(status_code=400, detail="metric must be 'statement|tag'")
    statement, tag = parts
    # A literal '+' in a query string decodes to a space; tags never contain
    # spaces, so restore it so the sum metric (e.g. FCF) routes correctly.
    tag = tag.replace(" ", "+")
    _FINANCIAL_TEMPLATE_TAGS = {"net_interest_income", "net_income"}
    if "+" in tag:
        tag_a, tag_b = tag.split("+", 1)
        return JSONResponse(
            top_companies_by_sum(
                statement=statement, tag_a=tag_a, tag_b=tag_b, limit=limit, cik_filter=cik_filter
            )
        )
    if "/" in tag:
        num_tag, denom_tag = tag.split("/", 1)
        _MARGIN_BOUNDS = {"total_revenue": (0.0, 99.99)}
        min_v, max_v = _MARGIN_BOUNDS.get(denom_tag, (None, None))
        min_denom = 1e9 if denom_tag == "total_revenue" else None
        return JSONResponse(
            top_companies_by_ratio(
                statement=statement,
                numerator_tag=num_tag,
                denominator_tag=denom_tag,
                limit=limit,
                min_value=min_v,
                max_value=max_v,
                min_denominator=min_denom,
                cik_filter=cik_filter,
            )
        )
    exclude_fin = tag not in _FINANCIAL_TEMPLATE_TAGS
    freq = None if statement == "balance_sheet" else "annual"
    negate = tag in {"purchase_of_plant_property_and_equipment"}
    return JSONResponse(
        top_companies_by_metric(
            statement=statement,
            tag=tag,
            limit=limit,
            exclude_financial_template=exclude_fin,
            frequency=freq,
            negate=negate,
            cik_filter=cik_filter,
        )
    )


@app.get("/sectors")
def sectors():
    """Dropdown options for the sector parameter (each sector + company count)."""
    from sec_app.db.sectors import list_sectors

    return JSONResponse(list_sectors())


@app.get("/industries")
def industries(sector: str | None = Query(None)):
    """Dropdown options for the industry parameter, narrowed by the chosen sector."""
    from sec_app.db.sectors import list_industries

    return JSONResponse(list_industries(sector or None))


@app.get("/sector_aggregates")
def sector_aggregates(
    sector: str | None = Query(None),
    industry: str | None = Query(None),
):
    """Operating aggregates by sector, or by industry within a sector.

    Nothing selected -> sector-level overview. ``sector`` -> its industry breakdown.
    ``industry`` -> that single industry.
    """
    from sec_app.db.sectors import sector_industry_aggregates

    return JSONResponse(sector_industry_aggregates(sector=sector or None, industry=industry or None))


@app.get("/widgets.json")
def widgets_json():
    with open(_HERE / "widgets.json") as f:
        return JSONResponse(json.load(f))


@app.get("/apps.json")
def apps_json():
    with open(_HERE / "apps.json") as f:
        return JSONResponse(json.load(f))


@app.get("/health")
def health():
    return {"status": "ok"}
