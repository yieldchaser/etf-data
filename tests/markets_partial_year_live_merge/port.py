"""Python port of `_buildAnnualCache` from docs/markets.html.

Two variants are exposed:

  * `build_annual_cache_original` — captures the *unfixed* pipeline. It is
    used by the preservation tests as the immovable baseline (original →
    original sanity check, then original → fixed for the non-buggy subset).

  * `build_annual_cache_fixed` — mirrors the post-Wave-1 hardened JS:
        - guards `prev > 0 AND finite`, `curr finite`, `r finite`
        - tracks `monthlyAnomaly[ym] = True` when `assetClass != 'equity'
          AND |ret| > 0.50` (the Excel↔FRED unit-mismatch signature)
        - drops the year cleanly if the running compound becomes NaN/Inf
          (no `-0.99` artifact)
        - replaces `partial = (year === firstYear || year === lastYear)`
          with the calendar-based rule:
              monthsExpected = (year < asofYear) ? 12 : asofMonth
              partial        = (count < monthsExpected)
                            OR (year === firstYear AND count < 12)
        - emits `monthsPresent`, `monthsExpected`, `boundaryAnomaly` fields
          on each cell.

Both variants take an already-currency-adjusted close series in the same
shape `_getAdjustedClose` produces in the JS — `[{ym, close}, ...]`. The
preservation tests pin behaviour on the local-currency lens (multiplier =
1.0), which is what the Return Matrix uses by default.
"""
from __future__ import annotations

from typing import Iterable, Optional


def adjusted_from_monthly(monthly: Iterable) -> list[dict]:
    """Convert the JSON `monthly` array `[[YYYY-MM, val], ...]` into the
    `{ym, close}` shape that `_buildAnnualCache` consumes."""
    out: list[dict] = []
    for ym, val in monthly:
        if val is None:
            continue
        out.append({"ym": ym, "close": float(val)})
    out.sort(key=lambda x: x["ym"])
    return out


def _is_finite(x) -> bool:
    """True iff x is a real number that is neither NaN nor ±Inf."""
    try:
        xf = float(x)
    except (TypeError, ValueError):
        return False
    return xf == xf and xf not in (float("inf"), float("-inf"))


def build_annual_cache_original(adjusted: list[dict]) -> dict[int, dict]:
    """Faithful Python port of the *unfixed* `_buildAnnualCache`.

    Returns `{year: {ret: float, partial: bool, count: int}}` where
    `partial = (year == firstYear OR year == lastYear)`.
    """
    if not adjusted:
        return {}

    monthly_ret: dict[str, float] = {}
    for i in range(1, len(adjusted)):
        prev = adjusted[i - 1]["close"]
        curr = adjusted[i]["close"]
        if prev > 0 and curr is not None:
            monthly_ret[adjusted[i]["ym"]] = curr / prev - 1

    by_year: dict[int, list[str]] = {}
    for item in adjusted:
        yr = int(item["ym"][:4])
        by_year.setdefault(yr, []).append(item["ym"])

    first_ym = adjusted[0]["ym"]
    last_ym = adjusted[-1]["ym"]
    first_year = int(first_ym[:4])
    last_year = int(last_ym[:4])

    result: dict[int, dict] = {}
    for yr, m_list in by_year.items():
        compound = 1.0
        count = 0
        for ym in m_list:
            r = monthly_ret.get(ym)
            if r is None:
                continue
            compound *= (1.0 + r)
            count += 1
        if count == 0:
            continue
        partial = (yr == first_year or yr == last_year)
        result[yr] = {"ret": compound - 1.0, "partial": partial, "count": count}
    return result


def build_annual_cache_fixed(
    adjusted: list[dict],
    *,
    asof_year: Optional[int] = None,
    asof_month: Optional[int] = None,
    asset_class: str = "",
) -> dict[int, dict]:
    """Mirror of the post-Wave-1 hardened `_buildAnnualCache` in
    docs/markets.html (~line 2638).

    Output cells include the additional fields the renderer relies on:
        ret, partial, count, monthsPresent, monthsExpected, boundaryAnomaly.
    """
    if not adjusted:
        return {}

    # Build monthly returns with the fixed numerical guards. We also track
    # which months on a non-equity series carry a |ret|>0.50 anomaly — that
    # is the signature of an Excel↔FRED unit/scale collision.
    monthly_ret: dict[str, float] = {}
    monthly_anomaly: dict[str, bool] = {}
    for i in range(1, len(adjusted)):
        prev = adjusted[i - 1]["close"]
        curr = adjusted[i]["close"]
        # prev must be strictly positive AND finite; curr must exist AND
        # be finite. Anything else and the divisor is unreliable.
        if not (prev is not None and prev > 0 and _is_finite(prev)):
            continue
        if curr is None or not _is_finite(curr):
            continue
        r = curr / prev - 1
        if not _is_finite(r):
            continue
        ym = adjusted[i]["ym"]
        monthly_ret[ym] = r
        if asset_class != "equity" and abs(r) > 0.50:
            monthly_anomaly[ym] = True

    # Group months by year.
    by_year: dict[int, list[str]] = {}
    for item in adjusted:
        yr = int(item["ym"][:4])
        by_year.setdefault(yr, []).append(item["ym"])

    first_ym = adjusted[0]["ym"]
    first_year = int(first_ym[:4])

    # Sensible defaults if asof not supplied — match the JS fallback to "now".
    if asof_year is None or asof_month is None:
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        if asof_year is None:
            asof_year = now.year
        if asof_month is None:
            asof_month = now.month

    result: dict[int, dict] = {}
    for yr, m_list in by_year.items():
        compound = 1.0
        count = 0
        months_present = 0
        boundary_anomaly = False
        drop_year = False
        for ym in m_list:
            months_present += 1
            r = monthly_ret.get(ym)
            if r is None:
                continue
            if not _is_finite(r):
                continue
            compound *= (1.0 + r)
            if not _is_finite(compound):
                # NaN/Inf running compound — drop the year cleanly rather
                # than emitting a saturated -0.99 artifact.
                drop_year = True
                break
            count += 1
            if monthly_anomaly.get(ym):
                boundary_anomaly = True
        if drop_year:
            continue
        if count == 0:
            continue
        ret = compound - 1.0
        if not _is_finite(ret):
            continue

        # Calendar-based partial: a year is partial when its month-count
        # falls below the 12-month calendar expectation. The user-emphasised
        # semantic is "the calendar year is not yet complete" — a 2026 cell
        # with 5 months of data has 7 months still pending and is therefore
        # partial-flagged. This matches the bug-condition tests' framing.
        #
        # Calendar-based partial: a year is partial when its month-count
        # falls below the expected number of months. For past years, we expect
        # 12 months. For the current/ongoing year, we expect data up to the
        # asof_month. If the count is less than expected (or first year count < 12),
        # it falls back to being flagged as partial.
        months_expected = 12 if yr < asof_year else asof_month
        partial = (count < months_expected) or (yr == first_year and count < 12)

        result[yr] = {
            "ret": ret,
            "partial": partial,
            "count": count,
            "monthsPresent": months_present,
            "monthsExpected": months_expected,
            "boundaryAnomaly": boundary_anomaly,
        }
    return result


def build_cache_for_asset(asset_blob: dict) -> dict[int, dict]:
    """End-to-end helper for preservation tests that compare original →
    fixed on the non-buggy subset. Defaults to the *original* pipeline
    because that is what the preservation snapshots were captured against.
    """
    monthly = asset_blob.get("monthly", [])
    return build_annual_cache_original(adjusted_from_monthly(monthly))
