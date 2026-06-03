"""Shared types, constants, and dataclasses for statement_schema."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

# 10-KT / 20-FT / 40-FT are transition-period ANNUAL reports (filed when a
# company changes its fiscal-year end); without them, companies that filed only
# transition reports (e.g. SPIRE ALABAMA, all 10-KT/A) yield zero filing dates
# and therefore zero standardized statements.
ANNUAL_FORMS = frozenset(
    {
        "10-K", "10-K/A", "10-KT", "10-KT/A",
        "20-F", "20-F/A", "20-FT", "20-FT/A",
        "40-F", "40-F/A", "40-FT", "40-FT/A",
    }
)
QUARTERLY_FORMS = frozenset({"10-Q", "10-Q/A", "10-QT", "10-QT/A"})
SEMI_ANNUAL_FORMS = frozenset({"6-K", "6-K/A"})
PRELIMINARY_FORMS = frozenset({"8-K", "8-K/A"})
ALL_FORMS = ANNUAL_FORMS | QUARTERLY_FORMS | SEMI_ANNUAL_FORMS
Frequency = Literal["annual", "quarterly"]
StatementName = Literal["income_statement", "balance_sheet", "cash_flow"]
CompanyType = Literal["industrial", "financial", "diversified", "insurance"]
_TOLERANCE_FLOOR = 100_000
_TOLERANCE_CAP = 1_000_000

_SCHEMAS_DIR = Path(__file__).resolve().parent / "schemas"


def _tolerance(*values: float | None) -> float:
    """Scale-adaptive tolerance: 0.1% of max magnitude, floored at 100k, capped at 1M."""
    scale = max((abs(v) for v in values if v is not None), default=0)
    return max(_TOLERANCE_FLOOR, min(_TOLERANCE_CAP, scale * 0.001))


@dataclass(frozen=True)
class RowDef:
    """A single row definition from the schema."""

    tag: str
    label: str
    description: str
    parent: str | None
    sequence: int
    factor: str
    balance: str
    unit: str  # "monetary", "per_share", or "shares"
    period_type: str  # "duration" or "instant"
    xbrl_tags: tuple[dict[str, str], ...]  # ({"tag": ..., "namespace": ...}, ...)


@dataclass
class RowResult:
    """Extracted values for one standardized row."""

    tag: str
    label: str
    description: str
    parent: str | None
    sequence: int | float
    factor: str
    balance: str
    unit: str
    period_type: str
    values: dict[str, float]  # {period_end_date: value}
    sources: dict[str, str] = field(
        default_factory=dict
    )  # {date: "ns:Tag" or "imputed: ..."}


@dataclass
class ValidationWarning:
    """A discrepancy detected during post-extraction validation."""

    date: str
    tag: str
    expected: float
    actual: float
    formula: str
    identity: str


@dataclass
class StatementResult:
    """A fully extracted statement."""

    statement: str
    company_type: str
    frequency: str
    currency: str
    dates: list[str]
    rows: list[RowResult]
    fiscal_data: dict[str, dict[str, Any]] = field(default_factory=dict)
    diagnostics: list[ValidationWarning] = field(default_factory=list)
    preliminary_dates: set[str] = field(default_factory=set)
