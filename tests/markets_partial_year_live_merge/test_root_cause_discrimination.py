"""Root-cause discrimination harness for the bugfix.

This module is intentionally non-asserting where the live JSON cannot
discriminate root cause on its own — instead it WRITES a structured
``root_cause_evidence.json`` next to the test file so the bugfix author can
read which candidate the data supports. The harness asserts that the
evidence file was written.

Bug 1 candidates (design Hypothesized Root Cause §"Bug 1"):
  H1  unit-mismatch at Excel↔FRED boundary — month-over-month price ratio
      outside [0.5, 2.0] in an ag asset.
  H2  near-zero / unit-misaligned base divisor.
  H3  asof-year monthsExpected mismatch (magnitude-driven color overpowers
      40 % opacity).
  H4  activeCellColor zscore-vs-return path inconsistency.
  H5  firstYear/lastYear shift after merge.

Bug 2 candidates:
  L1  live fetch returned 0 rows for ^GSPC / ^NDX / NASDAQCOM.
  L2  cache freshness drift (cache present but stale).
  L3  yfinance MultiIndex shape change.
  L4  FRED rate limit on NASDAQCOM.
  L5  verdict aggregation never iterated per-asset.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

import pytest

from .annual_cache import build_annual_cache
from .bug_conditions import DUAL_REGISTRY_EQUITY_HOLDOUTS


EVIDENCE_PATH = Path(__file__).parent / "root_cause_evidence.json"
AG_ASSETS = ("wheat", "corn", "sugar", "soybeans", "cotton", "coffee")


def _scan_boundary_jumps(monthly):
    """Report month-over-month price ratios outside [0.5, 2.0] (H1 evidence)."""
    rows = sorted(
        ((ym, p) for ym, p in monthly if p is not None),
        key=lambda r: r[0],
    )
    out: List[dict] = []
    for i in range(1, len(rows)):
        prev_ym, prev = rows[i - 1]
        curr_ym, curr = rows[i]
        if prev is None or curr is None or prev <= 0:
            continue
        ratio = curr / prev
        if ratio < 0.5 or ratio > 2.0:
            out.append({
                "from_ym": prev_ym, "from_close": prev,
                "to_ym": curr_ym, "to_close": curr,
                "ratio": round(ratio, 4),
            })
    return out


def _scan_zero_or_nan_base(monthly):
    """Find any month whose ``prev`` is zero / NaN / negative (H2 evidence)."""
    rows = sorted(monthly, key=lambda r: r[0])
    out: List[dict] = []
    for i in range(1, len(rows)):
        prev = rows[i - 1][1]
        if prev is None or (isinstance(prev, float) and prev != prev):  # NaN
            out.append({"prev_ym": rows[i - 1][0], "prev": "NaN_or_None"})
        elif prev <= 0:
            out.append({"prev_ym": rows[i - 1][0], "prev": prev})
    return out


def test_record_root_cause_evidence(market_returns, metadata, asof_year_month):
    """Write `root_cause_evidence.json` summarising which root-cause candidate
    the live data supports. This test ALWAYS records evidence; failures are
    triggered only when the file cannot be written (defence in depth)."""
    asof_year, asof_month = asof_year_month
    evidence: Dict[str, object] = {
        "asof_year": asof_year,
        "asof_month": asof_month,
        "bug_1": {},
        "bug_2": {},
    }

    # ─── Bug 1 — agriculture diagnosis ───────────────────────────────
    bug1: Dict[str, dict] = {}
    for aid in AG_ASSETS:
        entry = market_returns["assets"].get(aid)
        if entry is None:
            bug1[aid] = {"missing": True}
            continue
        cache = build_annual_cache(
            entry["monthly"],
            asof_year=asof_year, asof_month=asof_month,
            asset_id=aid, asset_class=entry["meta"].get("category", ""),
        )
        cell_2026 = cache.get(2026)
        boundary_jumps = _scan_boundary_jumps(entry["monthly"])
        zero_or_nan = _scan_zero_or_nan_base(entry["monthly"])
        bug1[aid] = {
            "source": entry["meta"].get("source"),
            "cell_2026": (
                {"ret": round(cell_2026.ret, 4),
                 "partial": cell_2026.partial,
                 "count": cell_2026.count,
                 "months_present": cell_2026.months_present,
                 "months_expected": cell_2026.months_expected}
                if cell_2026 is not None else None
            ),
            "boundary_jumps": boundary_jumps,
            "zero_or_nan_base_months": zero_or_nan,
        }
    evidence["bug_1"] = {
        "ag_asset_diagnosis": bug1,
        "h1_unit_mismatch_evidence": [
            f"{aid}: {len(diag.get('boundary_jumps') or [])} boundary jump(s) outside [0.5, 2.0]"
            for aid, diag in bug1.items()
        ],
        "h2_zero_or_nan_base_evidence": [
            f"{aid}: {len(diag.get('zero_or_nan_base_months') or [])} zero/NaN base month(s)"
            for aid, diag in bug1.items()
        ],
        "h3_magnitude_driven_color_evidence": (
            "applies whenever a partial cell's |ret| >= 0.50; see test 1b "
            "saturation outcomes"
        ),
        "verdict_for_bug_1": (
            "If h1 boundary jumps are present in any ag asset → unit-mismatch "
            "is supported. If h1 is empty AND h2 is empty AND ag 2026 |ret| is "
            "moderate → root cause is h3 magnitude-driven-color overpowering "
            "the .matrix-cell-partial 40% opacity."
        ),
    }

    # ─── Bug 2 — dual-registry equity diagnosis ─────────────────────
    bug2: Dict[str, dict] = {}
    for aid in sorted(DUAL_REGISTRY_EQUITY_HOLDOUTS):
        entry = market_returns["assets"].get(aid)
        if entry is None:
            bug2[aid] = {"missing": True}
            continue
        bug2[aid] = {
            "source": entry["meta"].get("source"),
            "starts_with_excel": entry["meta"].get("source", "").startswith(
                "Mega_Markets_Historical"
            ),
            "last": entry["meta"].get("last"),
        }
    mdf = metadata.get("markets_data_freshness", {})
    slc = mdf.get("self_living_check") if mdf else None
    evidence["bug_2"] = {
        "dual_registry_diagnosis": bug2,
        "metadata_has_markets_data_freshness": metadata.get("markets_data_freshness") is not None,
        "metadata_self_living_check": slc,
        "metadata_has_holdouts_field": bool(slc) and "holdouts" in (slc or {}),
        "verdict_for_bug_2": (
            "If meta.markets_data_freshness is missing entirely (no slc block) "
            "→ root cause supports L5 (verdict aggregation never built); the "
            "live-fetch path can only be discriminated between L1 (empty fetch) "
            "and L2 (cache drift) by capturing stdout from "
            "`python -m conviction.markets_history --full-refresh "
            "--assets sp500,nasdaq,nasdaq100,djia` — see "
            "test_live_fetch_logging_record below."
        ),
    }

    # ─── Finding summary ──────────────────────────────────────────────
    h1_jumps_total = sum(
        len(d.get("boundary_jumps") or []) for d in bug1.values()
    )
    h2_zero_total = sum(
        len(d.get("zero_or_nan_base_months") or []) for d in bug1.values()
    )
    # A unit-mismatch boundary jump signature would land near the Excel↔FRED
    # join — i.e. a recent year (>= 2010). Old-history jumps (1986 cotton
    # crash, etc.) are real price moves, not unit collisions.
    recent_jumps = [
        (aid, j)
        for aid, d in bug1.items()
        for j in (d.get("boundary_jumps") or [])
        if str(j.get("to_ym", ""))[:4] >= "2010"
    ]
    has_h1 = len(recent_jumps) > 0
    has_h2 = h2_zero_total > 0
    excel_holdouts = [aid for aid, d in bug2.items() if d.get("starts_with_excel")]
    metadata_missing_freshness = (
        metadata.get("markets_data_freshness") is None
    )
    bug1_finding = (
        "h1_unit_mismatch" if has_h1 and not has_h2
        else "h2_zero_or_nan_base" if has_h2 and not has_h1
        else "h1_and_h2_combined" if (has_h1 and has_h2)
        else "h3_magnitude_driven_color_OR_h5_firstYear_shift"
    )
    if has_h1:
        bug1_finding += (
            " — at least one ag asset has a recent (>=2010) month-over-month "
            "price ratio outside [0.5, 2.0] near the Excel↔FRED boundary "
            "(the unit/scale-mismatch signature)"
        )
    bug2_finding = []
    if metadata_missing_freshness:
        bug2_finding.append("L5_verdict_aggregation_never_built")
    if excel_holdouts:
        bug2_finding.append(
            "live-fetch path either L1 (empty) or L2 (cache drift); "
            "discriminate by capturing stdout from `python -m conviction.markets_history "
            "--full-refresh --assets sp500,nasdaq,nasdaq100,djia` and reading "
            "the `EMPTY: ...` log lines added by design Fix Implementation #6"
        )
    evidence["summary"] = {
        "bug_1_finding": bug1_finding,
        "bug_1_h1_jumps_total": h1_jumps_total,
        "bug_1_h1_recent_boundary_jumps": [
            {"asset": aid, **j} for aid, j in recent_jumps
        ],
        "bug_1_h2_zero_or_nan_total": h2_zero_total,
        "bug_2_finding": bug2_finding,
        "bug_2_excel_holdouts": excel_holdouts,
        "metadata_missing_self_living_check": metadata_missing_freshness,
        "user_constraints_acknowledged": [
            "(1) KNOWN_REAL_EVENTS is a single-source-of-truth dict in "
            "tests/markets_partial_year_live_merge/known_real_events.py; "
            "extending the whitelist is one new line",
            "(2) unvalidated extreme treatment is non-destructive (dim + ⚠ + "
            "tooltip), never drop the cell — encoded in test 1c by allowing "
            "either suppression OR validation, and in design Fix Implementation #4",
            "(3) S&P/NASDAQ holdouts: if live fetch genuinely fails, keep the "
            "honest Excel label and name them in `holdouts` — encoded in test 1f",
            "(4) discriminator reports h1 vs h2 vs h3 for ag bug; L1 vs L2 vs "
            "L5 for live-fetch bug",
        ],
    }

    EVIDENCE_PATH.write_text(
        json.dumps(evidence, indent=2, default=str), encoding="utf-8"
    )
    assert EVIDENCE_PATH.exists()
    # Also log a one-liner so a `pytest -v` operator sees the verdict.
    print(f"\nROOT CAUSE EVIDENCE written to {EVIDENCE_PATH}")
    print(json.dumps({
        "bug_1_h1_jumps_per_asset": {
            aid: len(d.get("boundary_jumps") or []) for aid, d in bug1.items()
        },
        "bug_2_excel_holdouts": [
            aid for aid, d in bug2.items() if d.get("starts_with_excel")
        ],
        "metadata_has_self_living_check": (
            metadata.get("markets_data_freshness") is not None
            and metadata["markets_data_freshness"].get("self_living_check") is not None
        ),
    }, indent=2))
