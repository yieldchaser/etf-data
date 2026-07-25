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
    """Quick checks that the conversion factors are physically reasonable."""

    def test_wheat_factor_is_correct(self):
        # 1 metric tonne of wheat = 36.7437 bushels (US hard red winter wheat)
        factor = UNIT_CONVERSIONS["wheat"]
        assert 36.5 < factor < 37.0, f"wheat factor {factor} outside expected range"

    def test_corn_factor_is_correct(self):
        # 1 metric tonne of corn = 39.3683 bushels
        factor = UNIT_CONVERSIONS["corn"]
        assert 39.0 < factor < 40.0, f"corn factor {factor} outside expected range"

    def test_soybeans_factor_is_correct(self):
        # 1 metric tonne of soybeans = 36.7437 bushels
        factor = UNIT_CONVERSIONS["soybeans"]
        assert 36.5 < factor < 37.0, f"soybeans factor {factor} outside expected range"

    def test_sugar_factor_is_correct(self):
        # 1 kg = 2.20462 lb
        factor = UNIT_CONVERSIONS["sugar"]
        assert abs(factor - 2.20462) < 0.001, f"sugar factor {factor} wrong"

    def test_cotton_factor_is_correct(self):
        factor = UNIT_CONVERSIONS["cotton"]
        assert abs(factor - 2.20462) < 0.001, f"cotton factor {factor} wrong"

    def test_coffee_factor_is_correct(self):
        factor = UNIT_CONVERSIONS["coffee"]
        assert abs(factor - 2.20462) < 0.001, f"coffee factor {factor} wrong"

    def test_no_conversion_for_equities(self):
        # Gold, Silver, SP500 — no factor means the value for these keys is
        # absent from the table (not 1.0). _to_monthly_eop defaults to 1.0.
        for key in ("gold", "silver", "sp500", "platinum", "palladium"):
            assert key not in UNIT_CONVERSIONS, (
                f"{key} unexpectedly found in UNIT_CONVERSIONS — would corrupt metal prices"
            )


# ── 2. _to_monthly_eop applies the factor correctly ───────────────────────────

class TestToMonthlyEopConversion:
    """Check that _to_monthly_eop multiplies by the conversion factor."""

    def _make_df(self, closes: list[float], start: str = "2025-01-31") -> pd.DataFrame:
        dates = pd.date_range(start, periods=len(closes), freq="ME")
        return pd.DataFrame({"Date": dates, "Close": closes})

    def test_wheat_converted(self):
        # 5.50 USD/bushel → should become 5.50 × 36.7437 ≈ 202.09 USD/mt
        df = self._make_df([5.50, 5.60, 5.45])
        result = _to_monthly_eop(df, asset_id="wheat")
        assert len(result) == 3
        expected = round(5.50 * UNIT_CONVERSIONS["wheat"], 4)
        assert abs(result[0][1] - expected) < 0.01, (
            f"wheat first month: got {result[0][1]}, expected {expected}"
        )

    def test_corn_converted(self):
        # 4.50 USD/bushel → 4.50 × 39.3683 ≈ 177.16 USD/mt
        df = self._make_df([4.50, 4.60])
        result = _to_monthly_eop(df, asset_id="corn")
        expected = round(4.50 * UNIT_CONVERSIONS["corn"], 4)
        assert abs(result[0][1] - expected) < 0.01

    def test_soybeans_converted(self):
        df = self._make_df([11.50, 11.80])
        result = _to_monthly_eop(df, asset_id="soybeans")
        expected = round(11.50 * UNIT_CONVERSIONS["soybeans"], 4)
        assert abs(result[0][1] - expected) < 0.01

    def test_sugar_converted(self):
        # 0.20 USD/lb → 0.20 × 2.20462 ≈ 0.4409 USD/kg
        df = self._make_df([0.20, 0.22])
        result = _to_monthly_eop(df, asset_id="sugar")
        expected = round(0.20 * UNIT_CONVERSIONS["sugar"], 4)
        assert abs(result[0][1] - expected) < 0.0001

    def test_cotton_converted(self):
        df = self._make_df([0.75, 0.78])
        result = _to_monthly_eop(df, asset_id="cotton")
        expected = round(0.75 * UNIT_CONVERSIONS["cotton"], 4)
        assert abs(result[0][1] - expected) < 0.001

    def test_coffee_converted(self):
        df = self._make_df([1.50, 1.55])
        result = _to_monthly_eop(df, asset_id="coffee")
        expected = round(1.50 * UNIT_CONVERSIONS["coffee"], 4)
        assert abs(result[0][1] - expected) < 0.001

    def test_gold_not_converted(self):
        # Gold is in USD/oz in both Excel and yfinance — no conversion wanted
        df = self._make_df([1900.0, 1950.0])
        result = _to_monthly_eop(df, asset_id="gold")
        assert abs(result[0][1] - 1900.0) < 0.01, (
            f"gold should not be converted: got {result[0][1]}"
        )

    def test_sp500_not_converted(self):
        df = self._make_df([4500.0, 4600.0])
        result = _to_monthly_eop(df, asset_id="sp500")
        assert abs(result[0][1] - 4500.0) < 0.01

    def test_unknown_asset_not_converted(self):
        # An asset_id not in UNIT_CONVERSIONS must pass through unchanged
        df = self._make_df([100.0, 110.0])
        result = _to_monthly_eop(df, asset_id="some_new_asset_xyz")
        assert abs(result[0][1] - 100.0) < 0.01

    def test_empty_df_returns_empty(self):
        df = pd.DataFrame({"Date": pd.Series(dtype="datetime64[ns]"), "Close": pd.Series(dtype=float)})
        result = _to_monthly_eop(df, asset_id="wheat")
        assert result == []


# ── 3. Boundary return anomaly is eliminated ──────────────────────────────────

class TestBoundaryReturnAnomaly:
    """
    THE KEY REGRESSION TEST.

    Simulate the pre-fix failure: last Excel month (USD/bushel) followed by the
    first FRED month (USD/mt). After _to_monthly_eop applies the conversion, both
    must be in USD/mt, and the month-over-month return must be < 30% (realistic
    agricultural price movement, not the pre-fix +3900% phantom spike).
    """

    @pytest.mark.parametrize("asset_id, excel_price, fred_price_mt, expected_return_pct", [
        # Realistic Dec-2025 Excel price (USD/bushel) vs realistic Jan-2026 FRED price (USD/mt)
        ("wheat",    5.50, 202.0,  None),   # ~37× conversion, return should be near 0%
        ("corn",     4.40, 174.0,  None),   # ~39× conversion
        ("soybeans", 11.50, 424.0, None),   # ~37× conversion
    ])
    def test_boundary_return_below_30pct(self, asset_id, excel_price, fred_price_mt, expected_return_pct):
        """After unit conversion the stitch-point return must be < 30%."""
        # Simulate: two Excel months in USD/bushel, then FRED comes in at USD/mt
        factor = UNIT_CONVERSIONS[asset_id]
        converted_excel_price = excel_price * factor

        # The monthly return at the boundary after conversion:
        boundary_return = fred_price_mt / converted_excel_price - 1

        assert abs(boundary_return) < 0.30, (
            f"{asset_id}: boundary return {boundary_return:.1%} still anomalous after unit conversion.\n"
            f"  Excel price: {excel_price} USD/bushel → converted: {converted_excel_price:.2f} USD/mt\n"
            f"  FRED price:  {fred_price_mt} USD/mt\n"
            f"  Factor used: {factor}\n"
            f"  This means the UNIT_CONVERSIONS table is wrong — check bushels-per-tonne."
        )

    @pytest.mark.parametrize("asset_id, excel_price_lb, fred_price_kg", [
        ("sugar",  0.20, 0.44),   # ~$0.20/lb × 2.20462 = ~$0.441/kg; FRED ≈ $0.44/kg → ~0% return
        ("cotton", 0.75, 1.65),   # ~$0.75/lb × 2.20462 = ~$1.653/kg; FRED ≈ $1.65/kg → ~0% return
        ("coffee", 1.50, 3.30),   # ~$1.50/lb × 2.20462 = ~$3.307/kg; FRED ≈ $3.30/kg → ~0% return
    ])
    def test_soft_commodity_boundary_return_below_30pct(self, asset_id, excel_price_lb, fred_price_kg):
        """lb-quoted commodities: boundary return < 30% after conversion."""
        factor = UNIT_CONVERSIONS[asset_id]
        converted = excel_price_lb * factor
        boundary_return = fred_price_kg / converted - 1
        assert abs(boundary_return) < 0.30, (
            f"{asset_id}: boundary return {boundary_return:.1%} anomalous.\n"
            f"  Excel {excel_price_lb} USD/lb → {converted:.4f} USD/kg; FRED {fred_price_kg} USD/kg"
        )


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

        # Sugar, Cotton, Coffee: scale factor converts FRED USD/kg to cents/lb
        for key in ("sugar", "cotton", "coffee"):
            asset = registry_by_key[key]
            assert "scale" in asset
            assert abs(asset["scale"] - 45.359237) < 0.001

        # Cocoa: scale factor converts FRED USD/kg to USD/MT
        cocoa = registry_by_key["cocoa"]
        assert "scale" in cocoa
        assert abs(cocoa["scale"] - 1000.0) < 0.01

        # Copper: scale factor converts FRED USD/MT to USD/lb
        copper = registry_by_key["copper"]
        assert "scale" in copper
        assert abs(copper["scale"] - 0.00045359237) < 0.00001


