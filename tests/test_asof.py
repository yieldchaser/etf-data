"""
Property-based tests for Property 5: asof advances to maximum last date.

Feature: automation-self-living-data-flow, Property 5: asof advances to maximum last date

Validates: Requirements 3.6

The top-level `asof` field in `market_returns.json` must equal the maximum
`meta.last` value across all assets.  Both `markets_history.build_output()`
and `ingest_markets_xl.build_output()` compute asof with the same formula:

    asof = max(v["meta"]["last"] for v in assets_out.values() if v.get("meta", {}).get("last"))

This module tests that formula as a pure function so the test is fast,
deterministic, and does not require disk access, FRED keys, or Excel files.
"""
from __future__ import annotations

from datetime import date

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st


# ─── Strategies ──────────────────────────────────────────────────────────────

# Valid YYYY-MM strings in the range the pipeline actually uses
_ASSET_KEYS = ["sp500", "gold", "oil", "us_10y", "us_2y"]


def _ym_strategy():
    """Generate valid YYYY-MM strings from 2010-01 to 2026-12."""
    return st.dates(
        min_value=date(2010, 1, 1),
        max_value=date(2026, 12, 1),
    ).map(lambda d: d.strftime("%Y-%m"))


def asset_strategy():
    """
    Generate a single asset dict with the minimal shape required by
    market_returns.json:  { "meta": { "last": "YYYY-MM", ... }, "monthly": [...] }
    """
    return st.fixed_dictionaries(
        {
            "key": st.sampled_from(_ASSET_KEYS),
            "meta": st.fixed_dictionaries(
                {
                    "name": st.just("Test Asset"),
                    "category": st.just("equity"),
                    "native_ccy": st.just("USD"),
                    "source": st.just("yfinance:^TEST"),
                    "first": st.just("2010-01"),
                    "last": _ym_strategy(),
                }
            ),
            "monthly": st.just([["2010-01", 100.0]]),
        }
    )


# ─── Pure asof computation (mirrors both build_output implementations) ───────

def _compute_asof_from_assets(assets: dict) -> str:
    """
    Replicate the asof computation from markets_history.build_output() and
    ingest_markets_xl.build_output() as a pure function.

    Both implementations use:
        all_lasts = [v["meta"]["last"] for v in assets_out.values()
                     if v.get("meta", {}).get("last")]
        asof = max(all_lasts) if all_lasts else <today>
    """
    all_lasts = [
        v["meta"]["last"]
        for v in assets.values()
        if v.get("meta", {}).get("last")
    ]
    if not all_lasts:
        return date.today().strftime("%Y-%m")
    return max(all_lasts)


def _build_market_returns(asset_list: list[dict]) -> dict:
    """
    Build a market_returns.json-shaped dict from a list of asset dicts
    (each produced by asset_strategy()).

    Deduplicates by key (last one wins) to mirror how a real build would
    handle duplicate asset keys.
    """
    assets: dict[str, dict] = {}
    for a in asset_list:
        key = a["key"]
        assets[key] = {
            "meta": a["meta"],
            "monthly": a["monthly"],
        }

    asof = _compute_asof_from_assets(assets)

    return {
        "asof": asof,
        "assets": assets,
        "fx": {},
        "cpi": {},
        "rates": {},
        "events": [],
    }


# ─── Property 5: asof == max(meta.last) across all assets ────────────────────

# Feature: automation-self-living-data-flow, Property 5: asof advances to maximum last date
@given(st.lists(asset_strategy(), min_size=1))
@settings(max_examples=100)
def test_asof_is_max_last_date(assets):
    """
    **Validates: Requirements 3.6**

    For any set of assets, the top-level `asof` field in market_returns.json
    must equal the maximum `meta.last` value across all assets.

    This property holds for both markets_history.build_output() and
    ingest_markets_xl.build_output() — both use the same formula.
    """
    mr = _build_market_returns(assets)

    # Collect the unique last dates from the deduplicated assets dict
    # (same deduplication as _build_market_returns: last key wins)
    all_lasts = [
        v["meta"]["last"]
        for v in mr["assets"].values()
        if v.get("meta", {}).get("last")
    ]

    assert all_lasts, "assets dict must be non-empty (min_size=1 guarantees this)"
    expected_asof = max(all_lasts)

    assert mr["asof"] == expected_asof, (
        f"asof mismatch.\n"
        f"  mr['asof']     = {mr['asof']!r}\n"
        f"  max(meta.last) = {expected_asof!r}\n"
        f"  all meta.last  = {sorted(all_lasts)}\n"
        f"  assets keys    = {list(mr['assets'].keys())}"
    )


# ─── Additional edge-case examples ───────────────────────────────────────────

def test_asof_single_asset():
    """Single asset: asof must equal that asset's meta.last."""
    assets = [
        {
            "key": "sp500",
            "meta": {
                "name": "S&P 500", "category": "equity", "native_ccy": "USD",
                "source": "yfinance:^GSPC", "first": "2020-01", "last": "2026-03",
            },
            "monthly": [["2020-01", 3000.0], ["2026-03", 5500.0]],
        }
    ]
    mr = _build_market_returns(assets)
    assert mr["asof"] == "2026-03"


def test_asof_multiple_assets_picks_maximum():
    """Multiple assets: asof must be the latest last date, not the first."""
    assets = [
        {
            "key": "sp500",
            "meta": {
                "name": "S&P 500", "category": "equity", "native_ccy": "USD",
                "source": "yfinance:^GSPC", "first": "2020-01", "last": "2026-01",
            },
            "monthly": [["2026-01", 5000.0]],
        },
        {
            "key": "gold",
            "meta": {
                "name": "Gold", "category": "precious_metals", "native_ccy": "USD",
                "source": "yfinance:GC=F", "first": "2020-01", "last": "2026-03",
            },
            "monthly": [["2026-03", 3200.0]],
        },
        {
            "key": "us_10y",
            "meta": {
                "name": "US 10Y", "category": "rates", "native_ccy": "USD",
                "source": "fred:GS10", "first": "2020-01", "last": "2025-12",
            },
            "monthly": [["2025-12", 4.5]],
        },
    ]
    mr = _build_market_returns(assets)
    assert mr["asof"] == "2026-03", (
        f"Expected '2026-03' (max of 2026-01, 2026-03, 2025-12), got {mr['asof']!r}"
    )


def test_asof_all_same_last_date():
    """When all assets share the same last date, asof equals that date."""
    shared_last = "2026-02"
    assets = [
        {
            "key": key,
            "meta": {
                "name": key, "category": "equity", "native_ccy": "USD",
                "source": f"yfinance:{key}", "first": "2020-01", "last": shared_last,
            },
            "monthly": [[shared_last, 100.0]],
        }
        for key in ["sp500", "gold", "oil"]
    ]
    mr = _build_market_returns(assets)
    assert mr["asof"] == shared_last


def test_asof_lexicographic_ordering_is_correct():
    """
    YYYY-MM strings sort correctly lexicographically, so max() gives the
    chronologically latest month.  Verify a case where naive numeric
    comparison would fail (e.g. '2026-02' > '2025-12').
    """
    assets = [
        {
            "key": "sp500",
            "meta": {
                "name": "S&P 500", "category": "equity", "native_ccy": "USD",
                "source": "yfinance:^GSPC", "first": "2020-01", "last": "2025-12",
            },
            "monthly": [["2025-12", 5000.0]],
        },
        {
            "key": "gold",
            "meta": {
                "name": "Gold", "category": "precious_metals", "native_ccy": "USD",
                "source": "yfinance:GC=F", "first": "2020-01", "last": "2026-02",
            },
            "monthly": [["2026-02", 3100.0]],
        },
    ]
    mr = _build_market_returns(assets)
    # '2026-02' > '2025-12' lexicographically — correct
    assert mr["asof"] == "2026-02", (
        f"Lexicographic max of ['2025-12', '2026-02'] should be '2026-02', got {mr['asof']!r}"
    )
