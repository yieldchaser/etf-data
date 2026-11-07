"""
Tests for the DYNF Factor Rotation Engine and iShares Factor Proxy Decomposition.

Verifies:
1. Parquet factor proxy archives (MTUM, QUAL, VLUE, USMV, SIZE, IWB) exist and are populated.
2. docs/data/dynf_factor_engine.json conforms to schema and holds continuous multi-factor history.
3. Active rebalancing flow and market price drift decomposition mathematical consistency.
4. Kinematic velocity & acceleration classification logic.
5. Strict zero-emoji compliance.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
PROXIES_DIR = REPO_ROOT / "data" / "factor_proxies"
ENGINE_JSON = REPO_ROOT / "docs" / "data" / "dynf_factor_engine.json"
BUILD_SCRIPT = REPO_ROOT / "scripts" / "build_dynf_factor_engine.py"
BACKFILL_SCRIPT = REPO_ROOT / "scripts" / "backfill_factor_proxy_etfs.py"

CANONICAL_FACTORS = ["momentum", "quality", "value", "min_vol", "size"]
PROXY_TICKERS = ["MTUM", "QUAL", "VLUE", "USMV", "SIZE", "IWB"]


def test_factor_proxy_archives_exist_and_populated():
    """Verify all 6 factor proxy Parquet archives exist with multi-year depth."""
    assert PROXIES_DIR.exists(), f"Missing factor proxy directory: {PROXIES_DIR}"

    for ticker in PROXY_TICKERS:
        p_path = PROXIES_DIR / f"{ticker}.parquet"
        assert p_path.exists(), f"Missing proxy archive: {p_path}"

        df = pd.read_parquet(p_path)
        assert len(df) > 5000, f"Expected >5000 rows for {ticker}, got {len(df)}"

        required_cols = {"ETF_Ticker", "ticker", "name", "weight", "Holdings_As_Of"}
        missing = required_cols - set(df.columns)
        assert not missing, f"Missing columns in {ticker}: {missing}"

        # Verify historical depth covers 2019 to present
        df["Holdings_As_Of"] = pd.to_datetime(df["Holdings_As_Of"])
        min_date = df["Holdings_As_Of"].min()
        max_date = df["Holdings_As_Of"].max()
        assert min_date <= pd.Timestamp("2019-06-01"), f"{ticker} start date too late: {min_date}"
        assert max_date >= pd.Timestamp("2026-10-01"), f"{ticker} end date too early: {max_date}"
        assert df["Holdings_As_Of"].nunique() >= 400, f"Expected >=400 dates for {ticker}, got {df['Holdings_As_Of'].nunique()}"


def test_dynf_factor_engine_json_schema():
    """Verify generated JSON file conforms to frontend contract and holds valid data."""
    assert ENGINE_JSON.exists(), f"Missing {ENGINE_JSON}"

    data = json.loads(ENGINE_JSON.read_text(encoding="utf-8"))
    assert data.get("fund") == "DYNF"
    assert data.get("ticker") == "DYNF"
    assert "macro_regime" in data and len(data["macro_regime"]) > 3
    assert "as_of" in data and len(data["as_of"]) == 10

    # Validate 5 canonical style factors
    current = data.get("current_factors", {})
    assert set(current.keys()) == set(CANONICAL_FACTORS), f"Unexpected factor keys: {set(current.keys())}"

    for f_key in CANONICAL_FACTORS:
        item = current[f_key]
        assert "label" in item and item["label"]
        assert "proxy_ticker" in item and item["proxy_ticker"] in PROXY_TICKERS
        assert "color" in item and item["color"].startswith("#")
        assert "current_weight" in item and 0.0 <= item["current_weight"] <= 100.0
        assert "delta_30d" in item
        assert "flow_active_30d" in item
        assert "drift_30d" in item
        assert item["status"] in {
            "Accelerating Inflow",
            "Expanding",
            "Accelerating Distribution",
            "Decelerating Distribution",
            "Neutral",
        }
        assert item["badge"] in {"up", "down", "neutral"}

        # Validate flow + drift sum approximately to delta_30d
        d30 = item["delta_30d"]
        split_sum = round(item["flow_active_30d"] + item["drift_30d"], 2)
        assert abs(d30 - split_sum) <= 0.05, f"Flow ({item['flow_active_30d']}) + Drift ({item['drift_30d']}) != delta_30d ({d30})"

        # Top drivers
        assert "top_drivers" in item and len(item["top_drivers"]) > 0
        for driver in item["top_drivers"]:
            assert "ticker" in driver and driver["ticker"]
            assert "weight" in driver and driver["weight"] >= 0.0

    # Validate timeline
    timeline = data.get("timeline", [])
    assert len(timeline) >= 300, f"Expected >=300 timeline points, got {len(timeline)}"
    for pt in timeline:
        assert "date" in pt and len(pt["date"]) == 10
        assert "regime" in pt and pt["regime"]
        for f_key in CANONICAL_FACTORS:
            assert f_key in pt and 0.0 <= pt[f_key] <= 100.0

    # Validate annual snapshots
    milestones = data.get("annual_snapshots", [])
    assert len(milestones) >= 7, f"Expected >=7 annual snapshots, got {len(milestones)}"
    years = [m["year"] for m in milestones]
    assert 2019 in years and 2026 in years


def test_kinematic_classification_logic():
    """Verify kinematics velocity and acceleration formulas."""
    from scripts.build_dynf_factor_engine import classify_kinematics

    # Inflows
    assert classify_kinematics(0.05, 0.02) == ("Accelerating Inflow", "up")
    assert classify_kinematics(0.05, 0.08) == ("Expanding", "neutral")

    # Distributions / Outflows
    assert classify_kinematics(-0.05, -0.02) == ("Accelerating Distribution", "down")
    assert classify_kinematics(-0.02, -0.05) == ("Decelerating Distribution", "neutral")

    # Neutral
    assert classify_kinematics(0.005, 0.005) == ("Neutral", "neutral")
    assert classify_kinematics(-0.005, -0.005) == ("Neutral", "neutral")


def test_factor_proxy_lookup_and_binary_search():
    """Verify FactorProxyLookup indexing and chronological binary search."""
    from scripts.build_dynf_factor_engine import FactorProxyLookup

    dates = [pd.Timestamp("2024-01-05"), pd.Timestamp("2024-01-12"), pd.Timestamp("2024-01-19")]
    df_iwb = pd.DataFrame([
        {"ticker": "AAPL", "weight": 0.07, "Holdings_As_Of": dates[0]},
        {"ticker": "MSFT", "weight": 0.06, "Holdings_As_Of": dates[0]},
        {"ticker": "AAPL", "weight": 0.075, "Holdings_As_Of": dates[1]},
        {"ticker": "MSFT", "weight": 0.065, "Holdings_As_Of": dates[1]},
        {"ticker": "AAPL", "weight": 0.08, "Holdings_As_Of": dates[2]},
        {"ticker": "MSFT", "weight": 0.07, "Holdings_As_Of": dates[2]},
    ])

    dummy_proxies = {t: df_iwb.copy() for t in PROXY_TICKERS}
    lookup = FactorProxyLookup(dummy_proxies)

    # Date between dates[0] and dates[1] should return dates[0] holdings
    mid_date = pd.Timestamp("2024-01-08")
    h = lookup.get_latest_holdings_before("IWB", mid_date)
    assert h["AAPL"] == 0.07

    # Exact date should return that date's holdings
    h_exact = lookup.get_latest_holdings_before("IWB", dates[1])
    assert h_exact["AAPL"] == 0.075

    # Date after all dates should return latest
    h_future = lookup.get_latest_holdings_before("IWB", pd.Timestamp("2025-01-01"))
    assert h_future["AAPL"] == 0.08


def test_lttb_multivariate_downsample_logic():
    """Verify LTTB preserves endpoints, bucket counts, and extrema."""
    import numpy as np
    from scripts.build_dynf_factor_engine import lttb_multivariate_downsample

    x = np.arange(100, dtype=float)
    # 5 synthetic factor channels with a sharp peak at index 50
    Y = np.zeros((100, 5), dtype=float)
    Y[50, 0] = 50.0  # Momentum peak

    indices = lttb_multivariate_downsample(x, Y, target_points=25)
    assert 0 in indices, "First point must be included"
    assert 99 in indices, "Last point must be included"
    assert 50 in indices, "Extreme peak at index 50 must be preserved by LTTB"
    assert len(indices) <= 25


def test_macro_regime_state_machine_hysteresis():
    """Verify 5-day hysteresis filter eliminates transient regime chatter."""
    from scripts.build_dynf_factor_engine import determine_macro_regime_state_machine

    # Construct series with 10 days of Expansion, 2 days of noise, and continuing Expansion
    rows = []
    for i in range(20):
        if i in (10, 11):  # 2 days of transient noise (would trigger Recovery without filter)
            rows.append({
                "exp_momentum": -2.0, "exp_quality": -1.0,
                "exp_value": 3.0, "exp_min_vol": 0.0, "exp_size": 2.0
            })
        else:  # Steady Expansion
            rows.append({
                "exp_momentum": 3.0, "exp_quality": 1.5,
                "exp_value": -1.0, "exp_min_vol": -1.0, "exp_size": 0.5
            })

    df = pd.DataFrame(rows)
    regimes = determine_macro_regime_state_machine(df, hysteresis_days=5)

    # Transient 2-day noise at indices 10 and 11 should be filtered out
    assert regimes[10] == "Expansion (Momentum Leadership)"
    assert regimes[11] == "Expansion (Momentum Leadership)"


def test_zero_emojis_in_factor_engine_artifacts():
    """Strict gate: verify no emoji characters exist in code or JSON output."""
    # Pattern matching Unicode emojis (Surrogate pairs, Pictographs, Emoticons, Symbols)
    emoji_pattern = re.compile(
        r"[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF"
        r"\U0001F700-\U0001F77F\U0001F780-\U0001F7FF\U0001F800-\U0001F8FF"
        r"\U0001F900-\U0001F9FF\U0001FA00-\U0001FA6F\U0001FA70-\U0001FAFF"
        r"\U00002702-\U000027B0\U000024C2-\U0001F251]"
    )

    targets = [BUILD_SCRIPT, BACKFILL_SCRIPT, ENGINE_JSON]
    for target in targets:
        if target.exists():
            text = target.read_text(encoding="utf-8")
            matches = emoji_pattern.findall(text)
            assert not matches, f"Found emojis {matches} in {target}"
