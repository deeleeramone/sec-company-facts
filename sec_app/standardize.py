"""DB-backed standardized financials for the serving layer.

Mirrors the provider's ``get_standardized_financials`` signature but sources data
from the local Dolt store: the precomputed ``standardized_statements`` table when
available (fast, no re-standardization), with a fallback to resolving raw company
facts through the provider's pure standardization engine.

``sec_app.server`` rebinds the provider symbol to this implementation at startup,
so the openbb-sec fetchers (which import ``get_standardized_financials`` at call
time) serve from the DB instead of re-fetching/standardizing per request.
"""

from __future__ import annotations

from openbb_core.app.model.abstract.error import OpenBBError
from openbb_sec.utils.company_facts import (  # provider (PyPI): pure standardization
    MULTI_CIK_TICKERS,
    StandardizedStatements,
    _schema,
    resolve_company_facts,
)

from sec_app.db.query import (  # serving (local): Dolt reads
    load_company_facts,
    load_standardized_statements,
    resolve_symbol,
)


async def get_standardized_financials(
    symbol: str | None = None,
    cik: str | int | None = None,
    fiscal_years: list[int] | None = None,
    period: str = "both",
    use_cache: bool = True,
    pit_mode: bool = False,
    include_preliminary: bool = False,
) -> StandardizedStatements:
    """Standardized statements served from the local Dolt store."""
    if symbol and not cik:
        resolved = resolve_symbol(symbol)
        cik = resolved[0] if resolved else None
        if not cik:
            raise OpenBBError(f"Could not find CIK for symbol: {symbol}")

    symbol_upper = symbol.upper() if symbol else ""
    if symbol_upper in MULTI_CIK_TICKERS:
        cik_list = MULTI_CIK_TICKERS[symbol_upper]
    elif isinstance(cik, int):
        cik_list = [str(cik).zfill(10)]
    elif isinstance(cik, str):
        cik_list = [cik.lstrip("0").zfill(10)] if cik else []
    else:
        raise OpenBBError("Either symbol or cik must be provided.")

    if not cik_list:
        raise OpenBBError("Either symbol or cik must be provided.")

    # Fast path: precomputed annual/quarterly table (no re-standardization).
    if not pit_mode and not include_preliminary and not fiscal_years:
        prebuilt = load_standardized_statements(cik_list, period)
        if prebuilt is not None:
            return prebuilt

    # Fallback: resolve from raw facts via the provider's pure engine.
    responses = [load_company_facts(c) for c in cik_list]
    if len(responses) == 1:
        facts_json = responses[0]
    else:
        primary = responses[0]
        merged_facts = _schema.merge_facts(*(r for r in responses))
        facts_json = {
            "entityName": primary.get("entityName", ""),
            "cik": primary.get("cik", ""),
            "facts": merged_facts,
        }

    return resolve_company_facts(
        facts_json,
        fiscal_years=fiscal_years,
        period=period,
        pit_mode=pit_mode,
        include_preliminary=include_preliminary,
    )
