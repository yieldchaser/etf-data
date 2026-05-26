"""Bug-condition predicates from the design document.

These encode `isBugCondition1` and `isBugCondition2` from
`.kiro/specs/markets-partial-year-and-live-merge-fix/design.md` so the test
modules can call them directly.

`KNOWN_REAL_EVENTS` lives in `known_real_events.py` (single source of truth,
trivially extensible). `is_known_real_event` is re-exported here for
convenience.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Set

from .known_real_events import KNOWN_REAL_EVENTS, is_known_real_event  # re-export


__all__ = [
    "AssetSnapshot",
    "CellSnapshot",
    "DUAL_REGISTRY_EQUITY_HOLDOUTS",
    "KNOWN_REAL_EVENTS",
    "is_bug_condition_1",
    "is_bug_condition_2",
    "is_known_real_event",
    "is_live_source",
    "is_saturated_artifact",
]


# ---------------------------------------------------------------------------
# Bug Condition C1 — Partial-year matrix cells.
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class CellSnapshot:
    """The minimal cell shape the bug-condition predicates need."""
    asset_id: str
    asset_class: str
    year: int
    months_present: int
    months_expected: int
    rendered_value: float
    render_style: str  # one of {saturated, dimmed, suppressed}
    partial_flag: bool


def is_bug_condition_1(cell: CellSnapshot) -> bool:
    """C1 from design.md — true when the cell exhibits the partial-year defect."""
    is_partial = cell.months_present < cell.months_expected
    inconsistent = is_partial and cell.render_style == "saturated"
    broken = (
        is_partial
        and abs(cell.rendered_value) > 0.90
        and cell.asset_class in {"agriculture", "equity"}
    )
    extreme = (
        (not is_partial)
        and abs(cell.rendered_value) > 0.70
        and not is_known_real_event(cell.asset_id, cell.year)
    )
    return inconsistent or broken or extreme


# ---------------------------------------------------------------------------
# Bug Condition C2 — Dual-registry equity live-merge holdouts.
# ---------------------------------------------------------------------------
DUAL_REGISTRY_EQUITY_HOLDOUTS: Set[str] = {"sp500", "nasdaq", "nasdaq100"}


@dataclass(frozen=True)
class AssetSnapshot:
    asset_id: str
    name: str
    source: str
    live_fetchable: bool


def is_bug_condition_2(asset: AssetSnapshot) -> bool:
    """C2 from design.md — true when the asset is a dual-registry equity that
    retained its Excel source label even though a live fetch is feasible."""
    is_dual = asset.asset_id in DUAL_REGISTRY_EQUITY_HOLDOUTS
    excel_labelled = asset.source.startswith("Mega_Markets_Historical")
    return is_dual and excel_labelled and asset.live_fetchable


def is_live_source(source: str) -> bool:
    """A `meta.source` is "live" when it starts with `yfinance:` or `fred:`."""
    return source.startswith("yfinance:") or source.startswith("fred:")


def is_saturated_artifact(rendered_value: Optional[float], partial: bool) -> bool:
    """Heuristic from the design: a partial cell whose rendered value is so
    large it would saturate the diverging colour palette is an artifact —
    the partial flag should suppress it."""
    if rendered_value is None:
        return False
    return partial and abs(rendered_value) > 0.50
