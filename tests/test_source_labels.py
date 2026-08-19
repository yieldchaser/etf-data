"""
tests/test_source_labels.py
===========================
Property-based tests for source-label honesty in the Excel ingest pipeline.

# Feature: automation-self-living-data-flow, Property 4: Excel ingest preserves live source labels
"""
from __future__ import annotations

from datetime import date

import pandas as pd
import pytest
from hypothesis import given, settings, HealthCheck
from hypothesis import strategies as st

from conviction.ingest_markets_xl import build_output, ASSET_REGISTRY

# ─── Asset IDs that exist in the Excel ASSET_REGISTRY ────────────────────────
# These are the only keys build_output() will process (it skips unknown IDs).
_REGISTRY_ASSET_IDS = list({aid for (_, _), (aid, _, _, _) in ASSET_REGISTRY.items()})

# ─── Strategies ──────────────────────────────────────────────────────────────

def asset_with_live_source_strategy():
    """
    Generate an asset dict whose meta.source starts with 'yfinance:' or 'fred:'.
    The asset key is drawn from the real ASSET_REGISTRY so build_output() will
    recognise it and process it.
    """
    live_sources = st.sampled_from([
        "yfinance:^GSPC",
        "fred:SP500",
        "yfinance:GC=F",
        "fred:DCOILWTICO",
    ])
    asset_keys = st.sampled_from(_REGISTRY_ASSET_IDS)

    @st.composite
    def _strategy(draw):
        key = draw(asset_keys)
        source = draw(live_sources)
        return {
            "key": key,
            "meta": {
                "name": key,
                "category": "equity",
                "native_ccy": "USD",
                "source": source,
                "first": "2020-01",
                "last": "2024-12",
            },
            "monthly": [["2020-01", 3000.0], ["2024-12", 4800.0]],
        }

    return _strategy()


def monthly_data_strategy():
    """
    Generate a list of ["YYYY-MM", float] pairs (1–24 months).
    Months are drawn from 2020-01 to 2026-04.
    """
    month_strategy = st.dates(
        min_value=date(2020, 1, 1),
        max_value=date(2026, 4, 1),
    ).map(lambda d: d.strftime("%Y-%m"))

    return st.lists(
        st.tuples(month_strategy, st.floats(min_value=1.0, max_value=100_000.0, allow_nan=False, allow_infinity=False)),
        min_size=1,
        max_size=24,
    ).map(lambda pairs: [[ym, val] for ym, val in pairs])


def _monthly_to_dataframe(monthly: list[list]) -> pd.DataFrame:
    """Convert a list of [YYYY-MM, float] pairs to a Date/Close DataFrame."""
    rows = []
    for ym, val in monthly:
        # Use end-of-month date so _to_monthly_eop resampling keeps the row
        try:
            ts = pd.Timestamp(ym + "-01") + pd.offsets.MonthEnd(0)
            rows.append({"Date": ts, "Close": float(val)})
        except Exception:
            pass
    if not rows:
        return pd.DataFrame(columns=["Date", "Close"])
    return pd.DataFrame(rows)


# ─── Property 4: Excel ingest preserves live source labels ───────────────────

# Feature: automation-self-living-data-flow, Property 4: Excel ingest preserves live source labels
@given(asset_with_live_source_strategy(), monthly_data_strategy())
@settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
def test_excel_ingest_preserves_live_source_label(existing_asset, new_monthly):
    """
    **Validates: Requirements 3.5**

    For any existing asset with a live source label (yfinance: or fred:),
    running the Excel ingest merge (build_output with merge=True) MUST NOT
    overwrite that source label with the Excel filename.

    The output asset's meta.source must:
    1. Equal the original live source label (unchanged).
    2. NOT start with 'Mega_Markets_Historical'.
    """
    asset_key = existing_asset["key"]
    original_source = existing_asset["meta"]["source"]

    # Build the existing market_returns.json-shaped dict
    existing = {
        "asof": existing_asset["meta"]["last"],
        "generated_utc": "",
        "assets": {
            asset_key: {
                "meta": existing_asset["meta"],
                "monthly": existing_asset["monthly"],
            }
        },
        "fx": {},
        "cpi": {},
        "rates": {},
        "events": [],
    }

    # Convert the generated monthly data to a DataFrame (what build_output expects)
    raw_df = _monthly_to_dataframe(new_monthly)

    # build_output skips assets with no monthly data after resampling,
    # so we need at least one valid row. If the DataFrame is empty, skip.
    if raw_df.empty or len(raw_df) == 0:
        return

    raw_series = {asset_key: raw_df}

    # Run the Excel ingest merge
    result = build_output(raw_series, existing, merge=True)

    # The asset must still be present in the output
    assert asset_key in result["assets"], (
        f"Asset '{asset_key}' disappeared from output after merge. "
        f"raw_df had {len(raw_df)} rows."
    )

    out_source = result["assets"][asset_key]["meta"]["source"]

    # Property 4a: source label must be unchanged
    assert out_source == original_source, (
        f"Excel ingest overwrote live source label for '{asset_key}'.\n"
        f"  original: {original_source!r}\n"
        f"  got:      {out_source!r}\n"
        f"  This violates Requirement 3.5: Excel_Backfill_Module SHALL preserve "
        f"source labels starting with 'yfinance:' or 'fred:'."
    )

    # Property 4b: source must not start with the Excel filename
    assert not out_source.startswith("Mega_Markets_Historical"), (
        f"Excel ingest set source to Excel filename for '{asset_key}' "
        f"which already had a live source label.\n"
        f"  original: {original_source!r}\n"
        f"  got:      {out_source!r}"
    )
