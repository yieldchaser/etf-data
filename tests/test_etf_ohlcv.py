"""Contract and integrity tests for ETF daily OHLCV dataset."""

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = ROOT / "docs" / "data" / "flows" / "catalog.json"
HISTORY_PATH = ROOT / "docs" / "data" / "flows" / "etf_history.json"


def test_etf_history_exists_and_matches_catalog_cardinality():
    assert HISTORY_PATH.is_file(), f"etf_history.json must exist at {HISTORY_PATH}"
    assert CATALOG_PATH.is_file(), f"catalog.json must exist at {CATALOG_PATH}"

    catalog = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))
    history_data = json.loads(HISTORY_PATH.read_text(encoding="utf-8"))

    assert history_data.get("schema_version") == 1
    assert "updated_utc" in history_data
    assert history_data.get("instrument_count") == 160

    catalog_tickers = {item["ticker"] for item in catalog.get("instruments", [])}
    assert len(catalog_tickers) == 160, "Catalog must have exactly 160 tickers"

    history_tickers = set(history_data.get("history", {}).keys())
    assert history_tickers == catalog_tickers, "etf_history.json tickers must exactly match catalog.json"


def test_ohlcv_bars_schema_and_geometric_integrity():
    history_data = json.loads(HISTORY_PATH.read_text(encoding="utf-8"))
    history = history_data.get("history", {})

    for ticker, bars in history.items():
        assert len(bars) >= 30, f"{ticker} has only {len(bars)} bars, expected >= 30"

        prev_date = ""
        for i, b in enumerate(bars):
            assert "d" in b and "o" in b and "h" in b and "l" in b and "c" in b and "v" in b, f"Missing key in {ticker} bar {i}: {b}"

            d = b["d"]
            o = b["o"]
            h = b["h"]
            l = b["l"]
            c = b["c"]
            v = b["v"]

            # Strict chronological order and no duplicates
            assert d > prev_date, f"{ticker} bars must be strictly monotonic in date: {d} <= {prev_date}"
            prev_date = d

            # Numerical validity
            assert isinstance(o, (int, float)) and o > 0, f"Invalid open in {ticker}: {o}"
            assert isinstance(h, (int, float)) and h > 0, f"Invalid high in {ticker}: {h}"
            assert isinstance(l, (int, float)) and l > 0, f"Invalid low in {ticker}: {l}"
            assert isinstance(c, (int, float)) and c > 0, f"Invalid close in {ticker}: {c}"
            assert isinstance(v, (int, float)) and v >= 0, f"Invalid volume in {ticker}: {v}"

            # Geometric integrity
            assert h >= l, f"High < Low in {ticker} on {d}: H={h}, L={l}"
            assert h >= min(o, c), f"High < min(Open, Close) in {ticker} on {d}: H={h}, O={o}, C={c}"
            assert l <= max(o, c), f"Low > max(Open, Close) in {ticker} on {d}: L={l}, O={o}, C={c}"


def test_volume_studies_node_math_contracts():
    import shutil
    import subprocess
    import pytest

    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js not available in current environment")

    test_script = ROOT / "tests" / "test_volume_studies.mjs"
    assert test_script.is_file(), f"Mathematical test script must exist at {test_script}"

    res = subprocess.run([node, str(test_script)], capture_output=True, text=True)
    assert res.returncode == 0, f"Node volume studies test failed:\nstdout: {res.stdout}\nstderr: {res.stderr}"
    assert ("All 14 mathematical contract tests PASSED" in res.stdout or
            "All 13 mathematical contract tests PASSED" in res.stdout or
            "mathematical contract tests PASSED successfully" in res.stdout)


def test_volume_heatmap_node_math_contracts():
    import shutil
    import subprocess
    import pytest

    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js not available in current environment")

    test_script = ROOT / "tests" / "test_volume_heatmap.mjs"
    assert test_script.is_file(), f"VolumeHeatmap test script must exist at {test_script}"

    res = subprocess.run([node, str(test_script)], capture_output=True, text=True)
    assert res.returncode == 0, f"Node VolumeHeatmap test failed:\nstdout: {res.stdout}\nstderr: {res.stderr}"
    assert "All VolumeHeatmap tests PASSED successfully." in res.stdout


def test_trough_peak_node_math_contracts():
    import shutil
    import subprocess
    import pytest

    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js not available in current environment")

    test_script = ROOT / "tests" / "test_trough_peak.mjs"
    assert test_script.is_file(), f"TroughPeak test script must exist at {test_script}"

    res = subprocess.run([node, str(test_script)], capture_output=True, text=True)
    assert res.returncode == 0, f"Node TroughPeak test failed:\nstdout: {res.stdout}\nstderr: {res.stderr}"
    assert "All TroughPeak tests PASSED successfully." in res.stdout


def test_split_adjustment_is_log_space_idempotent():
    from scripts.build_etf_ohlcv import apply_split_adjustments_to_bars

    # Case A: Unadjusted 1:5 reverse split (factor=0.2 -> price jumps 5x if unadjusted)
    raw_bars = [
        {"d": "2024-01-02", "o": 10.0, "h": 11.0, "l": 9.5, "c": 10.0, "v": 5000},
        {"d": "2024-01-03", "o": 10.2, "h": 10.8, "l": 9.8, "c": 10.5, "v": 6000},
        {"d": "2024-01-04", "o": 52.0, "h": 54.0, "l": 50.0, "c": 53.0, "v": 1200},
        {"d": "2024-01-05", "o": 53.0, "h": 55.0, "l": 51.0, "c": 54.0, "v": 1300},
    ]
    splits = {"SYN": [{"split_date": "2024-01-04", "factor": 0.2}]}

    once, applied1 = apply_split_adjustments_to_bars(raw_bars, "SYN", splits)
    assert len(applied1) == 1
    assert once[0]["c"] == 50.0
    assert once[1]["c"] == 52.5
    assert once[0]["v"] == 1000
    assert once[2]["c"] == 53.0

    # Applying a second time must be a strict no-op (idempotent)
    twice, applied2 = apply_split_adjustments_to_bars(once, "SYN", splits)
    assert applied2 == []
    assert twice == once

    # Case B: Over-adjusted reverse split artifact (e.g., 5x too high pre-split -> obs ~ 0.2)
    over_bars = [
        {"d": "2023-08-08", "o": 2928.75, "h": 2988.75, "l": 2898.75, "c": 2943.75, "v": 11299},
        {"d": "2023-08-09", "o": 595.5, "h": 630.0, "l": 594.75, "c": 623.25, "v": 48069},
    ]
    over_splits = {"NV_SYN": [{"split_date": "2023-08-09", "factor": 0.2}]}
    healed1, app_over1 = apply_split_adjustments_to_bars(over_bars, "NV_SYN", over_splits)
    assert len(app_over1) == 1
    assert healed1[0]["c"] == 588.75
    assert healed1[0]["v"] == 56495
    healed2, app_over2 = apply_split_adjustments_to_bars(healed1, "NV_SYN", over_splits)
    assert app_over2 == []
    assert healed2 == healed1


def test_bad_tick_sanitization_vs_real_split_and_crash_discrimination():
    from scripts.build_etf_ohlcv import (
        detect_and_apply_unknown_splits,
        has_overlapping_scale_mismatch,
        sanitize_bad_ticks,
        sanitize_ticker_bars,
    )

    # 1. Single-day V-shaped bad tick is healed from NAV (or midpoint) without changing neighbors
    v_bars = [
        {"d": "2024-05-31", "o": 27.2, "h": 28.56, "l": 27.19, "c": 28.56, "v": 160500},
        {"d": "2024-06-03", "o": 28.75, "h": 29.28, "l": 0.27, "c": 10.17, "v": 90600},
        {"d": "2024-06-04", "o": 28.76, "h": 29.01, "l": 28.19, "c": 28.93, "v": 148000},
    ]
    nav_map = {
        "2024-05-31": {"nav": 28.51, "perf_pct": 0.0501},
        "2024-06-03": {"nav": 28.77, "perf_pct": 0.0091},
        "2024-06-04": {"nav": 28.90, "perf_pct": 0.0045},
    }
    clean_v, healed = sanitize_bad_ticks(v_bars, "BAD", nav_map=nav_map)
    assert len(healed) == 1
    assert clean_v[1]["c"] == 28.77
    assert clean_v[1]["l"] == 28.75
    assert clean_v[1]["h"] >= clean_v[1]["c"] >= clean_v[1]["l"]

    # Midpoint fallback when NAV is missing
    clean_mid, healed_mid = sanitize_bad_ticks(v_bars, "BAD", nav_map=None)
    assert len(healed_mid) == 1
    assert clean_mid[1]["c"] == 28.74  # Python round(28.745, 2) == 28.74

    # 2. Permanent unrecorded split (>= 3.5x without revert and without NAV agreement) IS adjusted
    perm_bars = [
        {"d": "2024-02-01", "o": 100.0, "h": 102.0, "l": 98.0, "c": 100.0, "v": 1000},
        {"d": "2024-02-02", "o": 24.5, "h": 25.5, "l": 24.0, "c": 25.0, "v": 4000},
        {"d": "2024-02-05", "o": 25.0, "h": 26.0, "l": 24.8, "c": 25.5, "v": 4100},
    ]
    adj_perm, det_perm = detect_and_apply_unknown_splits(perm_bars, "UNREG", nav_map=None)
    assert len(det_perm) == 1
    assert adj_perm[0]["c"] == 25.0
    assert adj_perm[0]["v"] == 4000

    # 3. Genuine extreme market move confirmed on both sides by NAV is NOT misclassified as split
    crash_bars = [
        {"d": "2024-11-15", "o": 44.0, "h": 47.6, "l": 40.3, "c": 42.72, "v": 233080},
        {"d": "2024-11-18", "o": 207.8, "h": 213.6, "l": 155.4, "c": 158.82, "v": 111450},
    ]
    crash_nav = {
        "2024-11-15": {"nav": 42.72, "perf_pct": -0.0872},
        "2024-11-18": {"nav": 158.82, "perf_pct": 2.7177},
    }
    no_split, det_crash = sanitize_ticker_bars(crash_bars, "VOL_REAL", known_splits={}, nav_map=crash_nav)
    assert det_crash["unknown_splits"] == []
    assert no_split[0]["c"] == 42.72 and no_split[1]["c"] == 158.82

    # 4. Overlapping incremental scale mismatch detection triggers full refetch
    assert has_overlapping_scale_mismatch(
        [{"d": "2026-10-01", "c": 100.0}],
        [{"d": "2026-10-01", "c": 20.0}],
    ) is True
    assert has_overlapping_scale_mismatch(
        [{"d": "2026-10-01", "c": 100.0}],
        [{"d": "2026-10-01", "c": 100.0}],
    ) is False


def test_checked_in_etf_history_has_clean_dfen_and_nvds_series():
    history = json.loads(HISTORY_PATH.read_text(encoding="utf-8")).get("history", {})
    dfen_map = {b["d"]: b for b in history["DFEN"]}
    assert dfen_map["2024-06-03"]["c"] > 25.0
    assert dfen_map["2024-06-03"]["l"] > 25.0

    nvds_map = {b["d"]: b for b in history["NVDS"]}
    ratio = nvds_map["2023-08-09"]["c"] / nvds_map["2023-08-08"]["c"]
    assert 0.85 <= ratio <= 1.25, f"Unexpected NVDS 2023-08-09 ratio: {ratio}"


def test_three_to_two_forward_split_adjustment():
    from scripts.build_etf_ohlcv import apply_split_adjustments_to_bars

    # 3:2 forward split: factor = 1.5, pre-split price multiplier R_s = 1 / 1.5 = 0.666667
    bars = [
        {"d": "2024-03-01", "o": 150.0, "h": 155.0, "l": 148.0, "c": 150.0, "v": 20000},
        {"d": "2024-03-04", "o": 100.0, "h": 104.0, "l": 98.0, "c": 100.0, "v": 30000},
    ]
    splits = {"SPLIT_3_2": [{"split_date": "2024-03-04", "factor": 1.5}]}
    adjusted, applied = apply_split_adjustments_to_bars(bars, "SPLIT_3_2", splits)
    assert len(applied) == 1
    assert round(adjusted[0]["c"], 2) == 100.0
    assert adjusted[0]["v"] == 30000
    assert adjusted[1]["c"] == 100.0

    # Second pass must be a strict no-op
    twice, applied2 = apply_split_adjustments_to_bars(adjusted, "SPLIT_3_2", splits)
    assert applied2 == []
    assert twice == adjusted


def test_total_return_close_reconstruction_and_no_phantom_drop():
    history = json.loads(HISTORY_PATH.read_text(encoding="utf-8")).get("history", {})

    # Check that tc is universally present and positive across all instruments
    for ticker in ("TLT", "TMF", "COWZ", "TQQQ", "SPXL"):
        bars = history[ticker]
        assert all("tc" in b and isinstance(b["tc"], (int, float)) and b["tc"] > 0 for b in bars)
        # Terminal bar tc must match c
        assert bars[-1]["tc"] == bars[-1]["c"]
        # TLT pays regular monthly dividends; historical tc should be lower than c going back in time
        assert bars[0]["tc"] < bars[0]["c"]


def test_terminal_bar_bad_tick_sanitization():
    from scripts.build_etf_ohlcv import sanitize_bad_ticks

    # Terminal bar spike with NAV reference
    bars = [
        {"d": "2024-11-05", "o": 50.0, "h": 52.0, "l": 49.0, "c": 50.0, "v": 10000},
        {"d": "2024-11-06", "o": 120.0, "h": 125.0, "l": 118.0, "c": 122.0, "v": 12000},
    ]
    nav_map = {
        "2024-11-05": {"nav": 50.0, "perf_pct": 0.01},
        "2024-11-06": {"nav": 51.0, "perf_pct": 0.02},
    }
    clean_bars, healed = sanitize_bad_ticks(bars, "TERM_SPIKE", nav_map=nav_map)
    assert len(healed) == 1
    assert clean_bars[1]["c"] == 51.0



