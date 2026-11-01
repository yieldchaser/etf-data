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
    assert "All 13 mathematical contract tests PASSED" in res.stdout

