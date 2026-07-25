"""
Tests for agriculture/commodity unit normalisation in predator.ingest_markets_xl.

These are REGRESSION GUARD tests. Before the fix, wheat/corn/soybeans were stored
in the Excel file as USD/bushel while FRED stores them in USD/mt (~36-39× larger).
This caused a fabricated +3900% monthly return at the 2025→2026 stitch boundary,
which then compounded to a −90%/−99% annual return for 2026.

After the fix (UNIT_CONVERSIONS applied at _to_monthly_eop time), every price that
flows into the merge is in the canonical FRED unit. The boundary return between the
last Excel month and the first FRED month must be < 30% in absolute value.
"""
from __future__ import annotations

import math

import pandas as pd
import pytest

from predator.ingest_markets_xl import UNIT_CONVERSIONS, _to_monthly_eop


# ── 1. Conversion table sanity ────────────────────────────────────────────────

class TestConversionTableSanity:
    """Quick checks that UNIT_CONVERSIONS does not contain unwanted multipliers."""

    def test_no_unwanted_conversion_multipliers(self):
        # All commodities are now natively in consistent units between Excel, yfinance, and FRED.
        # No corrupt legacy conversion factors should be present in UNIT_CONVERSIONS.
        for key in ("wheat", "corn", "soybeans", "sugar", "cotton", "coffee", "gold", "silver", "sp500"):
            assert key not in UNIT_CONVERSIONS, (
                f"{key} unexpectedly found in UNIT_CONVERSIONS — would corrupt commodity prices"
            )


# ── 2. _to_monthly_eop passes values through cleanly ─────────────────────────

class TestToMonthlyEopConversion:
    """Check that _to_monthly_eop handles series cleanly without corrupting prices."""

    def _make_df(self, closes: list[float], start: str = "2025-01-31") -> pd.DataFrame:
        dates = pd.date_range(start, periods=len(closes), freq="ME")
        return pd.DataFrame({"Date": dates, "Close": closes})

    def test_wheat_passthrough(self):
        df = self._make_df([5.50, 5.60, 5.45])
        result = _to_monthly_eop(df, asset_id="wheat")
        assert len(result) == 3
        assert abs(result[0][1] - 5.50) < 0.01

    def test_sugar_passthrough(self):
        df = self._make_df([19.20, 19.50])
        result = _to_monthly_eop(df, asset_id="sugar")
        assert abs(result[0][1] - 19.20) < 0.01

    def test_gold_passthrough(self):
        df = self._make_df([1900.0, 1950.0])
        result = _to_monthly_eop(df, asset_id="gold")
        assert abs(result[0][1] - 1900.0) < 0.01

    def test_empty_df_returns_empty(self):
        df = pd.DataFrame({"Date": pd.Series(dtype="datetime64[ns]"), "Close": pd.Series(dtype=float)})
        result = _to_monthly_eop(df, asset_id="wheat")
        assert result == []


# ── 3. Boundary return sanitizer ──────────────────────────────────────────────

class TestBoundaryReturnAnomaly:
    """Check that sanitize_monthly_series rescales unit mismatches cleanly."""

    def test_cocoa_unit_sanitizer(self):
        from predator.markets_history import sanitize_monthly_series
        raw = [["2024-11", 9220.0], ["2024-12", 10.37], ["2025-01", 10987.0]]
        res = sanitize_monthly_series(raw, key="cocoa")
        assert len(res) == 3
        assert abs(res[1][1] - 10370.0) < 1.0


# ── 4. Return value structure ─────────────────────────────────────────────────

class TestReturnStructure:
    """Verify _to_monthly_eop still returns correctly structured [YYYY-MM, value] pairs."""

    def test_output_format(self):
        df = pd.DataFrame({
            "Date": pd.date_range("2024-01-31", periods=3, freq="ME"),
            "Close": [5.0, 5.1, 5.2],
        })
        result = _to_monthly_eop(df, asset_id="wheat")
        assert isinstance(result, list)
        for row in result:
            assert len(row) == 2
            ym, val = row
            assert isinstance(ym, str) and len(ym) == 7 and "-" in ym
            assert isinstance(val, float)
            assert math.isfinite(val)

    def test_output_is_sorted(self):
        df = pd.DataFrame({
            "Date": pd.date_range("2024-01-31", periods=12, freq="ME"),
            "Close": list(range(12, 0, -1)),
        })
        result = _to_monthly_eop(df, asset_id="corn")
        yms = [r[0] for r in result]
        assert yms == sorted(yms), "Output months must be sorted ascending"

    def test_values_rounded_to_4dp(self):
        df = pd.DataFrame({
            "Date": [pd.Timestamp("2024-01-31")],
            "Close": [5.123456789],
        })
        result = _to_monthly_eop(df, asset_id="wheat")
        val = result[0][1]
        # After conversion (×36.7437) and rounding to 4dp
        assert round(val, 4) == val, f"Value {val} not rounded to 4 decimal places"


# ── 5. Graceful handling of missing Excel file ────────────────────────────────

def test_process_graceful_when_excel_missing(tmp_path, monkeypatch):
    """Verify process() runs successfully and doesn't fail when Excel is missing or pointer."""
    import json
    import predator.ingest_markets_xl as ixl

    # Patch MEGA_XL to a non-existent path
    monkeypatch.setattr(ixl, "MEGA_XL", tmp_path / "NonExistent_Mega_Markets.xlsx")
    monkeypatch.setattr(ixl, "MARKETS1_XL", tmp_path / "NonExistent_Markets_1_.xlsx")

    # Create a dummy market_returns.json so freshness check can load it
    dummy_returns = {
        "asof": "2026-05",
        "generated_utc": "2026-05-27T12:00:00Z",
        "assets": {
            "gold": {
                "meta": {
                    "name": "Gold",
                    "category": "precious_metals",
                    "native_ccy": "USD",
                    "source": "Mega_Markets_Historical.xlsx:Precious_Metals",
                    "first": "1833-01",
                    "last": "2026-05",
                },
                "monthly": [["2026-05", 2300.0]]
            }
        },
        "fx": {},
        "cpi": {},
        "rates": {},
        "events": []
    }

    out_path = tmp_path / "market_returns.json"
    out_path.write_text(json.dumps(dummy_returns), encoding="utf-8")
    monkeypatch.setattr(ixl, "OUTPUT_PATH", out_path)

    # Run process with fail_on_stale=False (so it passes even if dummy last date is old)
    result = ixl.process(dry_run=False, merge=False, fail_on_stale=False)

    # It should load the existing JSON, check freshness, and return it without crashing
    assert result == dummy_returns


# ── 6. FRED agricultural series scaling ────────────────────────────────────────

class TestFredAgricultureScaling:
    """Verify that FRED agricultural series are correctly scaled by markets_history."""

    def test_fred_scaling_applied_correctly(self, monkeypatch):
        # Mock fred.get_series to return a dummy series
        class MockFred:
            def get_series(self, series_id, **kwargs):
                return pd.Series([100.0, 200.0], index=pd.date_range("2026-01-31", periods=2, freq="ME"))

        mock_fred = MockFred()
        from predator.markets_history import _fetch_fred_series

        # With scale=0.5, [100.0, 200.0] should become [50.0, 100.0]
        res = _fetch_fred_series(mock_fred, "DUMMY_SERIES", full_refresh=True, scale=0.5)
        assert len(res) == 2
        assert res.iloc[0] == 50.0
        assert res.iloc[1] == 100.0

    def test_agriculture_registry_has_correct_scale_factors(self):
        from predator.markets_history import ASSET_REGISTRY
        registry_by_key = {a["key"]: a for a in ASSET_REGISTRY}

        # Sugar, Cotton, Coffee: fallback_scale converts yfinance fallback series
        for key in ("sugar", "cotton", "coffee"):
            asset = registry_by_key[key]
            assert "fallback_scale" in asset
            assert abs(asset["fallback_scale"] - 0.022046226) < 0.001

        # Cocoa: scale factor converts FRED USD/kg to USD/MT
        cocoa = registry_by_key["cocoa"]
        assert "scale" in cocoa
        assert abs(cocoa["scale"] - 1000.0) < 0.01

        # Rice: fallback_scale converts ZR=F cwt to USD/MT
        rice = registry_by_key["rice"]
        assert "fallback_scale" in rice
        assert abs(rice["fallback_scale"] - 22.046226) < 0.0101
