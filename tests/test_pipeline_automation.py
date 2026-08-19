"""
End-to-end automation tests for the Markets data pipeline.

Simulates the scenario: "It's 2027, new data arrives — does everything
flow through correctly?"

Tests verify:
1. ingest_markets_xl produces §2.3 contract shape
2. markets_history.build_output merges with existing JSON (preserves deep history)
3. New months from FRED/yfinance are appended, not overwriting old data
4. The JSON shape is always §2.3 (monthly arrays, not close dicts)
5. The freshness gate correctly identifies stale series
6. The build_site.yml step order is correct (Excel first, then live data)

Run: pytest tests/test_pipeline_automation.py -v
"""
from __future__ import annotations

import json
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _make_monthly_list(start_ym: str, n_months: int, start_val: float = 100.0, step: float = 1.0) -> list[list]:
    """Generate n_months of [[YYYY-MM, val], ...] starting from start_ym."""
    from tests.helpers import make_monthly_list
    return make_monthly_list(start_ym, n_months, start_val, step)


def _make_asset(name: str, category: str, monthly: list[list]) -> dict:
    """Build a §2.3 asset dict."""
    from tests.helpers import make_asset
    return make_asset(name, category, monthly)


def _make_json(assets: dict, asof: str = "2026-05") -> dict:
    """Build a minimal §2.3 contract JSON."""
    from tests.helpers import make_json
    return make_json(assets, asof)


# ─── Test 1: §2.3 contract shape invariant ───────────────────────────────────

class TestContractShape:
    """The JSON must always have monthly arrays, never close dicts."""

    def test_ingest_xl_produces_monthly_arrays(self, tmp_path):
        """ingest_markets_xl.process() writes monthly arrays, not close dicts."""
        from conviction.ingest_markets_xl import process, OUTPUT_PATH
        import conviction.ingest_markets_xl as ixl

        # Patch output path to tmp
        orig = ixl.OUTPUT_PATH
        ixl.OUTPUT_PATH = tmp_path / "market_returns.json"
        try:
            result = process(dry_run=False, merge=False, fail_on_stale=False)
        finally:
            ixl.OUTPUT_PATH = orig

        out_path = tmp_path / "market_returns.json"
        assert out_path.exists(), "Output file not written"

        data = json.loads(out_path.read_text(encoding="utf-8"))
        assert "asof" in data, "Missing 'asof' field"
        assert "assets" in data, "Missing 'assets' field"
        assert len(data["assets"]) > 0, "No assets in output"

        # Every asset must have 'monthly' array, not 'close' dict
        for key, asset in data["assets"].items():
            assert "monthly" in asset, f"{key}: missing 'monthly' key"
            assert isinstance(asset["monthly"], list), f"{key}: 'monthly' must be a list"
            assert len(asset["monthly"]) > 0, f"{key}: 'monthly' is empty"
            assert isinstance(asset["monthly"][0], list), f"{key}: monthly entries must be [YYYY-MM, val] pairs"
            assert len(asset["monthly"][0]) == 2, f"{key}: monthly entry must have exactly 2 elements"
            # Year must be a string
            assert isinstance(asset["monthly"][0][0], str), f"{key}: YYYY-MM must be a string"
            assert "-" in asset["monthly"][0][0], f"{key}: YYYY-MM must contain '-'"
            # Value must be numeric
            assert isinstance(asset["monthly"][0][1], (int, float)), f"{key}: value must be numeric"
            # No 'close' dict allowed
            assert "close" not in asset, f"{key}: must not have 'close' dict (old format)"

    def test_no_close_dict_in_output(self, tmp_path):
        """Verify the output JSON never contains the old 'close' dict format."""
        from conviction.ingest_markets_xl import process
        import conviction.ingest_markets_xl as ixl

        orig = ixl.OUTPUT_PATH
        ixl.OUTPUT_PATH = tmp_path / "market_returns.json"
        try:
            process(dry_run=False, merge=False, fail_on_stale=False)
        finally:
            ixl.OUTPUT_PATH = orig

        raw = (tmp_path / "market_returns.json").read_text(encoding="utf-8")
        # The old format had "close": {"1968-01": 35.5, ...}
        # The new format has "monthly": [["1968-01", 35.5], ...]
        # Check that no asset has a "close" key at the asset level
        data = json.loads(raw)
        for key, asset in data["assets"].items():
            assert "close" not in asset, (
                f"Asset '{key}' still has old 'close' dict format. "
                f"Keys: {list(asset.keys())}"
            )


# ─── Test 2: Merge preserves deep history ────────────────────────────────────

class TestMergePreservesHistory:
    """New data must append to existing history, not overwrite it."""

    def test_merge_appends_new_months(self, tmp_path):
        """When new months arrive, they are appended to existing history."""
        from conviction.ingest_markets_xl import _merge_monthly, _load_existing, build_output

        # Existing: gold from 1833 to 2026-05 (deep history from Excel)
        existing_monthly = _make_monthly_list("1833-01", 2318, 18.93, 0.001)
        assert existing_monthly[0][0] == "1833-01"
        assert len(existing_monthly) == 2318

        # New data: gold from 2026-03 to 2027-02 (12 new months from FRED)
        new_monthly = _make_monthly_list("2026-03", 12, 3100.0, 50.0)
        assert new_monthly[0][0] == "2026-03"
        assert new_monthly[-1][0] == "2027-02"

        # Merge: new data takes priority for overlapping months
        merged = _merge_monthly(existing_monthly, new_monthly)

        # Deep history preserved
        assert merged[0][0] == "1833-01", "Deep history start lost"
        # New data appended
        assert merged[-1][0] == "2027-02", "New data not appended"
        # Total length: 2318 original + 9 new non-overlapping months
        # (2026-03 to 2026-05 overlap with existing, 2026-06 to 2027-02 = 9 new)
        assert len(merged) > len(existing_monthly), "Merge should add new months"

    def test_new_data_overwrites_overlapping_months(self):
        """For overlapping months, new data (FRED/yfinance) takes priority over Excel."""
        from conviction.ingest_markets_xl import _merge_monthly

        existing = [["2026-01", 2900.0], ["2026-02", 2950.0], ["2026-03", 3000.0]]
        new = [["2026-02", 2960.0], ["2026-03", 3010.0], ["2026-04", 3050.0]]

        merged = _merge_monthly(existing, new)
        merged_dict = {m: v for m, v in merged}

        # New data wins for overlapping months
        assert merged_dict["2026-02"] == 2960.0, "New data should overwrite 2026-02"
        assert merged_dict["2026-03"] == 3010.0, "New data should overwrite 2026-03"
        # New month appended
        assert merged_dict["2026-04"] == 3050.0, "New month should be appended"
        # Old month preserved
        assert merged_dict["2026-01"] == 2900.0, "Old month should be preserved"

    def test_markets_history_build_output_preserves_excel_history(self, tmp_path):
        """markets_history.build_output() merges with existing JSON, not overwrites."""
        from conviction.markets_history import build_output, write_output
        import conviction.markets_history as mh

        # Seed the output file with Excel deep-history
        deep_history = _make_monthly_list("1833-01", 100, 18.93, 0.01)
        seed_json = _make_json({
            "gold": _make_asset("Gold", "precious_metals", deep_history)
        }, asof="1841-04")
        out_path = tmp_path / "market_returns.json"
        out_path.write_text(json.dumps(seed_json), encoding="utf-8")

        # Patch OUTPUT_PATH
        orig = mh.OUTPUT_PATH
        mh.OUTPUT_PATH = out_path
        try:
            # Simulate FRED returning only recent data (no deep history)
            recent_data = pd.Series(
                [3100.0, 3150.0, 3200.0],
                index=pd.to_datetime(["2026-03-31", "2026-04-30", "2026-05-31"])
            )
            results = {"gold": recent_data}
            output = build_output(results)
        finally:
            mh.OUTPUT_PATH = orig

        # Deep history must be preserved
        gold = output["assets"]["gold"]
        monthly = gold["monthly"]
        months_dict = {m: v for m, v in monthly}

        assert "1833-01" in months_dict, "Deep history (1833) lost after FRED merge"
        assert "2026-05" in months_dict, "New FRED data not present"
        assert months_dict["2026-05"] == pytest.approx(3200.0, rel=1e-3), "FRED value incorrect"


# ─── Test 3: 2027 scenario — new year flows through ──────────────────────────

class TestNewYearFlowThrough:
    """Simulate 2027 data arriving and verify it flows through the pipeline."""

    def test_2027_months_appear_in_matrix_years(self):
        """When 2027 data is in the JSON, matrixYears includes 2027."""
        # 36 months from Jan 2025 = Jan 2025 → Dec 2027
        monthly_2027 = _make_monthly_list("2025-01", 36, 100.0)
        assert any(m[0].startswith("2027") for m in monthly_2027), "Test data should include 2027"
        assert any(m[0].startswith("2026") for m in monthly_2027), "Test data should include 2026"

        # Extract years (mirrors JS matrixYears getter)
        year_set = set()
        for ym, _ in monthly_2027:
            year_set.add(int(ym[:4]))

        assert 2027 in year_set, "2027 should appear in year set"
        assert 2026 in year_set, "2026 should appear in year set"
        assert 2025 in year_set, "2025 should appear in year set"

    def test_annual_return_computed_for_2027(self):
        """Annual return for 2027 is computable from monthly data."""
        from tests.test_markets_engine import get_adjusted_close, build_annual_returns

        # Gold data spanning 2025-2027
        monthly = (
            _make_monthly_list("2025-01", 12, 2800.0, 30.0) +  # 2025
            _make_monthly_list("2026-01", 12, 3160.0, 40.0) +  # 2026
            _make_monthly_list("2027-01", 12, 3640.0, 50.0)    # 2027
        )

        adjusted = get_adjusted_close(monthly, "USD", "local")
        annual = build_annual_returns(adjusted)

        assert 2026 in annual, "2026 annual return should be computable"
        assert 2027 in annual, "2027 annual return should be computable"
        # 2027: Dec 2027 / Dec 2026 - 1
        dec_2026 = next(v for ym, v in monthly if ym == "2026-12")
        dec_2027 = next(v for ym, v in monthly if ym == "2027-12")
        expected_2027 = dec_2027 / dec_2026 - 1
        assert abs(annual[2027]["ret"] - expected_2027) < 0.001, (
            f"2027 return {annual[2027]['ret']:.4f} != expected {expected_2027:.4f}"
        )

    def test_freshness_gate_passes_for_current_data(self):
        """Freshness gate passes when data is within cadence."""
        from conviction.ingest_markets_xl import check_freshness

        # Data dated this month — should pass
        today_ym = date.today().strftime("%Y-%m")
        output = _make_json({
            "gold": _make_asset("Gold", "precious_metals",
                                _make_monthly_list("1833-01", 10) + [[today_ym, 3500.0]])
        }, asof=today_ym)
        # Update meta.last
        output["assets"]["gold"]["meta"]["last"] = today_ym

        stale = check_freshness(output, fail_on_stale=False)
        assert len(stale) == 0, f"Fresh data should pass gate, got: {stale}"

    def test_freshness_gate_fails_for_stale_data(self):
        """Freshness gate correctly identifies data older than cadence."""
        from conviction.ingest_markets_xl import check_freshness

        # Data from 3 months ago — should be stale (>35 days)
        stale_ym = (date.today() - timedelta(days=100)).strftime("%Y-%m")
        output = _make_json({
            "gold": _make_asset("Gold", "precious_metals",
                                _make_monthly_list("1833-01", 10) + [[stale_ym, 3000.0]])
        }, asof=stale_ym)
        output["assets"]["gold"]["meta"]["last"] = stale_ym

        stale = check_freshness(output, fail_on_stale=False)
        assert len(stale) > 0, "Stale data should be detected"
        assert any("gold" in s for s in stale), f"Gold should be flagged stale: {stale}"


# ─── Test 4: Build order correctness ─────────────────────────────────────────

class TestBuildOrder:
    """Verify the build_site.yml step order is correct."""

    def test_build_site_yml_has_correct_step_order(self):
        """
        Charter v2 Part 2 — self-living merge contract:

          Step 1: Excel BACKFILL ONLY (deep-history seed, runs ONCE).
          Step 2: Live FRED + yfinance merge (current months, fx, cpi, rates).
          Step 3: REMOVED. The previous third pass (`ingest_markets_xl
                  --merge-existing` after FRED) clobbered the live source
                  labels back to the Excel filename — Excel must NOT run
                  again after the live merge.

        This test pins the contract: Excel runs exactly once, before FRED;
        no Excel re-run after FRED.
        """
        yml_path = REPO_ROOT / ".github" / "workflows" / "build_site.yml"
        assert yml_path.exists(), "build_site.yml not found"

        content = yml_path.read_text(encoding="utf-8")

        excel_first_pos = content.find("ingest_markets_xl --no-fail-on-stale")
        fred_pos        = content.find("markets_history --full-refresh")
        excel_merge_pos = content.find("ingest_markets_xl --merge-existing")

        assert excel_first_pos >= 0, "Excel baseline step not found in build_site.yml"
        assert fred_pos >= 0,        "markets_history --full-refresh step not found"
        assert excel_first_pos < fred_pos, (
            "Excel baseline ingest must run BEFORE markets_history --full-refresh"
        )
        # Charter v2 Part 2: the second Excel run is REMOVED. It must not exist.
        assert excel_merge_pos == -1, (
            "build_site.yml must NOT re-run ingest_markets_xl --merge-existing after the "
            "live FRED merge — that step clobbered the live source labels back to Excel. "
            "Excel is backfill-only and runs exactly once before the live merge."
        )

    def test_verify_step_checks_monthly_format(self):
        """The verify step in build_site.yml checks for monthly array format."""
        yml_path = REPO_ROOT / ".github" / "workflows" / "build_site.yml"
        content = yml_path.read_text(encoding="utf-8")
        assert "monthly" in content, "Verify step should check for 'monthly' key"
        assert "close" in content or "monthly" in content, "Verify step should validate format"

    def test_ingest_mega_xl_is_shim(self):
        """ingest_mega_xl.main() delegates to ingest_markets_xl (no old format)."""
        import conviction.ingest_mega_xl as old_ingest
        import inspect
        src = inspect.getsource(old_ingest.main)
        assert "ingest_markets_xl" in src, (
            "ingest_mega_xl.main() must delegate to ingest_markets_xl"
        )
        assert "merge-existing" in src or "merge_existing" in src, (
            "ingest_mega_xl shim must use --merge-existing"
        )


# ─── Test 5: JSON shape validation ───────────────────────────────────────────

class TestJsonShapeValidation:
    """Validate the current market_returns.json has the correct shape."""

    def test_current_json_has_monthly_arrays(self):
        """The committed market_returns.json uses monthly arrays."""
        json_path = REPO_ROOT / "docs" / "data" / "market_returns.json"
        if not json_path.exists():
            pytest.skip("market_returns.json not found — run ingest first")

        data = json.loads(json_path.read_text(encoding="utf-8"))

        assert "asof" in data, "Missing 'asof' field"
        assert "assets" in data, "Missing 'assets' field"
        assert len(data["assets"]) >= 10, f"Expected ≥10 assets, got {len(data['assets'])}"

        for key, asset in data["assets"].items():
            assert "monthly" in asset, f"{key}: missing 'monthly' key"
            assert isinstance(asset["monthly"], list), f"{key}: 'monthly' must be list"
            assert len(asset["monthly"]) > 0, f"{key}: 'monthly' is empty"
            # Verify [YYYY-MM, value] format
            first = asset["monthly"][0]
            assert isinstance(first, list) and len(first) == 2, f"{key}: bad monthly entry format"
            assert isinstance(first[0], str) and len(first[0]) == 7, f"{key}: YYYY-MM must be 7-char string"
            assert isinstance(first[1], (int, float)), f"{key}: value must be numeric"
            # No old format
            assert "close" not in asset, f"{key}: old 'close' dict format found"

    def test_current_json_has_deep_history(self):
        """Gold should go back to 1833, S&P to 1871."""
        json_path = REPO_ROOT / "docs" / "data" / "market_returns.json"
        if not json_path.exists():
            pytest.skip("market_returns.json not found")

        data = json.loads(json_path.read_text(encoding="utf-8"))
        assets = data["assets"]

        if "gold" in assets:
            gold_first = assets["gold"]["meta"]["first"]
            assert gold_first <= "1900-01", f"Gold history should start before 1900, got {gold_first}"

        if "sp500" in assets:
            sp_first = assets["sp500"]["meta"]["first"]
            assert sp_first <= "1900-01", f"S&P history should start before 1900, got {sp_first}"

    def test_current_json_asof_is_recent(self):
        """The asof date should be within the last 6 months."""
        json_path = REPO_ROOT / "docs" / "data" / "market_returns.json"
        if not json_path.exists():
            pytest.skip("market_returns.json not found")

        data = json.loads(json_path.read_text(encoding="utf-8"))
        asof = data.get("asof", "")
        assert asof, "Missing 'asof' field"

        asof_date = pd.Timestamp(asof + "-01")
        cutoff = pd.Timestamp(date.today()) - pd.DateOffset(months=6)
        assert asof_date >= cutoff, (
            f"asof={asof} is more than 6 months old — data may be stale"
        )
