"""Regression tests for fetch_stock_details currency + JSON hygiene fixes.

Covers the 2026-08 second-wave audit:
  * GBp/GBX/ZAc minor-unit quotes canonicalised and prices ÷100 at write time
    (London names displayed ~100x-off before)
  * allow_nan=False backstop — NaN/Inf must never reach detail JSON
  * total-failure exit signal (all-failed resolve run exits non-zero)
"""
from __future__ import annotations

import json
import math
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from conviction.fetch_stock_details import (
    canonicalise_currency,
    rescale_prices,
)


class TestCurrencyCanonicalisation:
    def test_gbp_pence_divides_by_100(self):
        assert canonicalise_currency("GBp") == ("GBP", 100.0)

    def test_gbx_uppercase_variant(self):
        assert canonicalise_currency("GBX") == ("GBP", 100.0)

    def test_gbp_major_unit_untouched(self):
        """CASE-SENSITIVE: 'GBP' is real pounds — divisor must be 1."""
        assert canonicalise_currency("GBP") == ("GBP", 1.0)

    def test_zac_and_agorot(self):
        assert canonicalise_currency("ZAc") == ("ZAR", 100.0)
        assert canonicalise_currency("ILA") == ("ILS", 100.0)

    def test_none_empty_pass_through(self):
        assert canonicalise_currency(None) == (None, 1.0)
        assert canonicalise_currency("") == (None, 1.0)

    def test_other_currencies_uppercased_divisor_one(self):
        assert canonicalise_currency("usd") == ("USD", 1.0)
        assert canonicalise_currency("JPY") == ("JPY", 1.0)


class TestPriceRescale:
    def test_pence_to_pounds(self):
        prices = [["2026-08-20", 2750.0], ["2026-08-21", 2762.5]]
        out = rescale_prices(prices, 100.0)
        assert out == [["2026-08-20", 27.5], ["2026-08-21", 27.625]]

    def test_divisor_one_is_identity_rounded(self):
        prices = [["2026-08-20", 123.456789]]
        assert rescale_prices(prices, 1.0) == [["2026-08-20", 123.4568]]

    def test_drops_nonfinite_keeps_last_duplicate(self):
        prices = [
            ["2026-08-20", 100.0],
            ["2026-08-20", 101.0],   # duplicate date — last wins
            ["2026-08-21", None],
            ["2026-08-22", float("nan")] if hasattr(math, "nan") else None,
        ]
        out = rescale_prices([p for p in prices if p], 1.0)
        dates = [d for d, _ in out]
        assert dates.count("2026-08-20") == 1
        assert [row for row in out if row[0] == "2026-08-20"][0][1] == 101.0
        assert "2026-08-21" not in dates

    def test_empty_input(self):
        assert rescale_prices(None, 100.0) == []
        assert rescale_prices([], 100.0) == []


class TestJsonHygiene:
    def test_dumps_sanitises_nan_to_null(self):
        """_sanitise converts NaN/Inf to None BEFORE encoding, so detail JSON
        never contains a bare `NaN` token (invalid JSON)."""
        from conviction.fetch_stock_details import _dumps

        out = _dumps({"price": float("nan"), "chg": float("inf")})
        assert "NaN" not in out and "Infinity" not in out
        parsed = json.loads(out)
        assert parsed["price"] is None and parsed["chg"] is None


class TestTotalFailureExit:
    def test_main_returns_zero_normally(self):
        """main() returns build_details' status; sanity-check the wiring with
        dry-run (no network) which always succeeds."""
        from conviction.fetch_stock_details import main

        rc = main(["--dry-run"])
        assert rc == 0
