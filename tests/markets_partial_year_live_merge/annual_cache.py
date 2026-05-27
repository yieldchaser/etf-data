"""Adapter on top of `port.py` adding render-style derivation + months_expected.

`port.py::build_annual_cache_fixed` is the Python mirror of the post-Wave-1
hardened `docs/markets.html::_buildAnnualCache`. This module wraps it so the
bug-condition tests can ask for ``months_present``, ``months_expected``, and
the renderer's ``render_style`` directly.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, Optional, Tuple

from .known_real_events import is_known_real_event
from .port import adjusted_from_monthly, build_annual_cache_fixed


MonthlyPoint = Tuple[str, Optional[float]]  # ("YYYY-MM", close)


@dataclass(frozen=True)
class AnnualCell:
    year: int
    ret: float
    partial: bool      # the FIXED partial flag (calendar-based)
    count: int         # months that contributed to the compound
    months_present: int        # months with data points in `year`
    months_expected: int       # 12 for past, asof_month for asof year
    asset_id: str
    asset_class: str
    boundary_anomaly: bool

    @property
    def is_partial_strict(self) -> bool:
        """True iff ``months_present < months_expected`` (the design's
        canonical predicate for `isPartial`)."""
        return self.months_present < self.months_expected


def build_annual_cache(
    monthly: Iterable[MonthlyPoint],
    *,
    asof_year: Optional[int] = None,
    asof_month: Optional[int] = None,
    asset_id: str = "",
    asset_class: str = "",
) -> Dict[int, AnnualCell]:
    """Replay the FIXED `_buildAnnualCache` and decorate each cell with
    months_present / months_expected / boundary_anomaly. Mirrors the
    post-Wave-1 JS exactly so the bug-condition tests pass when the fix is
    applied.
    """
    rows = sorted(
        ((ym, close) for ym, close in monthly if close is not None),
        key=lambda r: r[0],
    )
    if not rows:
        return {}

    adjusted = adjusted_from_monthly(rows)
    raw = build_annual_cache_fixed(
        adjusted,
        asof_year=asof_year,
        asof_month=asof_month,
        asset_class=asset_class,
    )

    out: Dict[int, AnnualCell] = {}
    for yr, cell in raw.items():
        months_expected = cell.get("monthsExpected", 12)
        months_present = cell.get("monthsPresent", cell["count"])
        out[yr] = AnnualCell(
            year=yr,
            ret=cell["ret"],
            partial=cell["partial"],
            count=cell["count"],
            months_present=months_present,
            months_expected=months_expected,
            asset_id=asset_id,
            asset_class=asset_class,
            boundary_anomaly=bool(cell.get("boundaryAnomaly", False)),
        )
    return out


def derive_render_style(cell: AnnualCell) -> str:
    """Mirror of the FIXED `docs/markets.html` render policy.

    The fixed renderer (matrixCellClass + .matrix-cell-partial +
    .matrix-cell-unvalidated) treats two cases as "dimmed":
      * partial cells (uniform across asset classes — was the inconsistency
        before; the fix decouples cell-level dimming from the diverging
        palette via `filter: saturate(0.3)`)
      * unvalidated extreme cells (|ret|>0.70 AND not in KNOWN_REAL_EVENTS)
    Otherwise the cell renders at full saturation. The fixed code never
    emits `suppressed` — extremes are always rendered (non-destructive,
    user constraint) with the dim+⚠+tooltip treatment.
    """
    if cell.is_partial_strict:
        return "dimmed"
    if cell.boundary_anomaly:
        return "dimmed"
    return "saturated"
