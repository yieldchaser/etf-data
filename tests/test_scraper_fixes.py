"""Regression tests for the 2026-08 scraper-fix wave.

Covers:
  * clean_dataframe weight_unit declaration (percent / fraction / legacy
    heuristic) — kills the one-truncated-page 100x failure mode
  * Invesco date-field preference (effectiveDate canonical) and the
    coverage tripwire on truncated responses
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

import scraper as scr  # noqa: E402


def _raw_table(max_weight_pct: float) -> pd.DataFrame:
    """Issuer-style HTML table with weights expressed in PERCENT."""
    return pd.DataFrame({
        "Ticker": ["AAA", "BBB", "CCC"],
        "Security Name": ["Alpha Corp", "Beta Inc", "Gamma Ltd"],
        "Weighting": [f"{max_weight_pct}", "0.5", "0.3"],
    })


class TestWeightUnitDeclaration:
    def test_legacy_heuristic_divides_when_max_gt_1(self):
        out = scr.clean_dataframe(_raw_table(9.8), "FPX", "2026-08-20")
        assert out["weight"].iloc[0] == pytest.approx(0.098)

    def test_percent_unit_always_divides_even_below_one(self):
        """THE regression case: a diversified fund with max weight ≤ 1.0%
        previously stored percent values as fractions (0.71 read as 71%)."""
        out = scr.clean_dataframe(_raw_table(0.71), "QMOM", "2026-08-20",
                                  weight_unit="percent")
        assert out["weight"].iloc[0] == pytest.approx(0.0071)

    def test_fraction_unit_never_divides(self):
        out = scr.clean_dataframe(_raw_table(0.71), "X", "2026-08-20",
                                  weight_unit="fraction")
        assert out["weight"].iloc[0] == pytest.approx(0.71)


class TestIngressPlaceholderFilter:
    def _mixed_table(self):
        return pd.DataFrame({
            "Ticker": ["AAA", "$KRW", "Cash&Other", "BBB", "CCC"],
            "Security Name": ["Alpha", "Korean Won hedge", "Cash aggregate",
                              "Beta Inc", "Gamma Ltd"],
            "Weighting": ["1.2", "0.4", "0.9", "0.8", "0.3"],
        })

    def test_drops_placeholders_keeps_equities(self):
        out = scr.clean_dataframe(self._mixed_table(), "QMOM", "2026-08-22",
                                  weight_unit="percent")
        assert sorted(out["ticker"]) == ["AAA", "BBB", "CCC"]
        assert (out["weight"] > 0).all()

    def test_all_placeholder_table_returns_none(self):
        """A table of only placeholders must return None (matching the
        Invesco path's '0 equity rows' semantics), never a valid-looking
        empty frame that gets recorded as an as-of day."""
        tbl = pd.DataFrame({
            "Ticker": ["$JPY", "Cash&Other"],
            "Security Name": ["Yen", "Cash"],
            "Weighting": ["0.1", "0.2"],
        })
        out = scr.clean_dataframe(tbl, "IMOM", "2026-08-22", weight_unit="percent")
        assert out is None


class TestInvescoDateAndCoverage:
    def _api_payload(self, eff_date, biz_date, reported, n_holdings=5):
        return {
            "effectiveDate": eff_date,
            "effectiveBusinessDate": biz_date,
            "totalNumberOfHoldings": reported,
            "holdings": [
                {"ticker": f"T{i}", "issuerName": f"Co {i}",
                 "percentageOfTotalNetAssets": 1.0,
                 "securityTypeName": "Common Stock"}
                for i in range(n_holdings)
            ],
        }

    class _FakeResp:
        def __init__(self, payload):
            self._payload = payload
        def raise_for_status(self): pass
        def json(self): return self._payload

    def test_effective_date_is_canonical(self, monkeypatch):
        """fetch_invesco_holdings.py treats effectiveDate as canonical;
        scraper.py now agrees when both fields are present."""
        payload = self._api_payload("2026-08-19", "2026-08-18", 5)

        class FakeCffi:
            @staticmethod
            def get(url, **kw):
                return TestInvescoDateAndCoverage._FakeResp(payload)

        monkeypatch.setattr(scr, "cffi_requests", FakeCffi)
        df, as_of = scr.fetch_invesco_api("SPMO", cusip := "46138E339")
        assert as_of == "2026-08-19"
        assert len(df) == 5

    def test_truncated_response_warns_but_returns(self, monkeypatch, capsys):
        """5 equity rows vs 200 reported must trigger the coverage warning
        while still returning usable data (warn-only by design)."""
        payload = self._api_payload("2026-08-20", "2026-08-20", 200)

        class FakeCffi:
            @staticmethod
            def get(url, **kw):
                return TestInvescoDateAndCoverage._FakeResp(payload)

        monkeypatch.setattr(scr, "cffi_requests", FakeCffi)
        df, as_of = scr.fetch_invesco_api("SPMO", "46138E339")
        out = capsys.readouterr().out
        assert "COVERAGE" in out and "truncated" in out
        assert len(df) == 5 and as_of == "2026-08-20"

    def test_complete_response_no_warning(self, monkeypatch, capsys):
        payload = self._api_payload("2026-08-20", "2026-08-20", 103, n_holdings=100)

        class FakeCffi:
            @staticmethod
            def get(url, **kw):
                return TestInvescoDateAndCoverage._FakeResp(payload)

        monkeypatch.setattr(scr, "cffi_requests", FakeCffi)
        scr.fetch_invesco_api("SPMO", "46138E339")
        assert "COVERAGE" not in capsys.readouterr().out
