"""
tests/test_cache_fallback.py

Feature: automation-self-living-data-flow, Property 7: Cache fallback data appears in output

Validates: Requirements 6.4

When a live fetch fails but a parquet cache exists, markets_history.build_output()
must include that asset's cached monthly data in the output assets dict so the
next build has a valid baseline to merge onto.
"""
from __future__ import annotations

import pandas as pd
import pytest
from hypothesis import given, settings, HealthCheck
from hypothesis import strategies as st
from pathlib import Path
from unittest.mock import patch


# ─── Strategies ──────────────────────────────────────────────────────────────

def asset_key_strategy():
    """
    Sample from non-FX asset keys that exist in ASSET_REGISTRY.
    'oil' is not a registry key; the actual keys are wti_crude / brent_crude.
    us_10y / us_2y live in RATES_REGISTRY (not ASSET_REGISTRY) so they are
    handled separately in the rates section of build_output — we test the
    assets section here.
    """
    return st.sampled_from(["sp500", "gold", "silver", "nasdaq", "wti_crude"])


def monthly_data_strategy():
    """
    Generate lists of [YYYY-MM, float] pairs (1–12 months).
    Months are drawn from a fixed set of valid YYYY-MM strings so the
    parquet round-trip is deterministic.
    """
    valid_months = [
        "2020-01", "2020-02", "2020-03", "2020-04", "2020-05", "2020-06",
        "2020-07", "2020-08", "2020-09", "2020-10", "2020-11", "2020-12",
    ]
    month_strategy = st.sampled_from(valid_months)
    value_strategy = st.floats(min_value=1.0, max_value=10000.0, allow_nan=False, allow_infinity=False)
    pair_strategy = st.tuples(month_strategy, value_strategy).map(list)
    return (
        st.lists(pair_strategy, min_size=1, max_size=12)
        .map(lambda pairs: sorted(
            {p[0]: p for p in pairs}.values(),  # deduplicate by month
            key=lambda p: p[0],
        ))
        .filter(lambda pairs: len(pairs) >= 1)
    )


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _build_series_from_monthly(monthly: list) -> pd.Series:
    """
    Convert [[YYYY-MM, val], ...] pairs to a pandas Series with a
    DatetimeIndex (end-of-month timestamps) — matching the format that
    _write_cache / _read_cache expect.
    """
    index = pd.to_datetime([f"{m[0]}-28" for m in monthly])
    values = [m[1] for m in monthly]
    return pd.Series(values, index=index, dtype=float)


def _monthly_keys(monthly: list) -> set:
    """Return the set of YYYY-MM strings from a monthly list."""
    return {m[0] for m in monthly}


# ─── Property 7: Cache fallback data appears in output ───────────────────────

@given(asset_key_strategy(), monthly_data_strategy())
@settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow], deadline=None)
def test_cache_fallback_in_output(asset_key, cached_monthly):
    """
    Feature: automation-self-living-data-flow, Property 7: Cache fallback data appears in output

    Validates: Requirements 6.4

    When live fetch returns empty but a parquet cache exists for an asset,
    build_output({}) must include that asset in output["assets"] with the
    cached monthly data.
    """
    import tempfile
    import predator.markets_history as mh

    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)

        # Build a pandas Series from the monthly pairs and write it to tmp_path
        # using the same parquet schema that _write_cache uses.
        series = _build_series_from_monthly(cached_monthly)
        cache_file = tmp_path / f"{asset_key}.parquet"
        df = pd.DataFrame({"value": series})
        df.to_parquet(cache_file)

        # Patch CACHE_DIR so _read_cache reads from tmp_path, not the real cache.
        # Also patch load_existing to return a minimal empty structure so
        # build_output doesn't try to read the real market_returns.json from disk.
        empty_existing = {
            "asof": "",
            "assets": {},
            "fx": {},
            "cpi": {},
            "rates": {},
            "events": [],
        }

        with (
            patch.object(mh, "CACHE_DIR", tmp_path),
            patch("predator.ingest_markets_xl.load_existing", return_value=empty_existing),
        ):
            # Pass empty results — simulates all live fetches failing
            output = mh.build_output({})

    # The asset must appear in the output
    assert asset_key in output["assets"], (
        f"Asset '{asset_key}' not found in output['assets'] after cache fallback.\n"
        f"Cached months: {[m[0] for m in cached_monthly]}\n"
        f"Output asset keys: {sorted(output['assets'].keys())}"
    )

    # The cached months must all be present in the output monthly list
    out_asset = output["assets"][asset_key]
    assert "monthly" in out_asset, (
        f"Asset '{asset_key}' in output has no 'monthly' key. Keys: {list(out_asset.keys())}"
    )
    out_months = _monthly_keys(out_asset["monthly"])
    expected_months = _monthly_keys(cached_monthly)
    assert expected_months.issubset(out_months), (
        f"Not all cached months appear in output for '{asset_key}'.\n"
        f"Expected months: {sorted(expected_months)}\n"
        f"Output months:   {sorted(out_months)}"
    )
