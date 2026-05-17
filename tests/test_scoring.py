"""Tests for the scoring engine. Run: pytest tests/ -v"""
from __future__ import annotations
import pandas as pd
import pytest
from pathlib import Path

from predator.scoring import Config, Sanitizer, compute_leaderboard, rank_multiplier
from predator import history as hist


CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"


@pytest.fixture
def cfg() -> Config:
    return Config.from_yaml(CONFIG_PATH)


def _h(rows: list[tuple]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=["ETF_Ticker", "ticker", "name", "weight", "Holdings_As_Of", "Date_Scraped"])


# ─── Sanitizer ────────────────────────────────────────────────────────────────
class TestSanitizer:
    def test_blocks_usd_variants(self, cfg):
        """Power Query: Ticker <> 'USD' and Ticker <> '$USD'."""
        df = _h([
            ("FPX", "USD",  "Dollar",      0.01, "2026-05-01", "2026-05-01"),
            ("FPX", "$USD", "Dollar",      0.01, "2026-05-01", "2026-05-01"),
            ("FPX", "AAPL", "Apple Inc",   0.05, "2026-05-01", "2026-05-01"),
        ])
        result = cfg.sanitizer.apply(df)
        assert set(result["ticker"]) == {"AAPL"}

    def test_blocks_invesco_money_market_by_exact_company_name(self, cfg):
        """Power Query: [Company] <> 'Short-Term Investment Trust - Invesco Government & Agency Portfolio'"""
        df = _h([
            ("FPX", "AGPXX", "Short-Term Investment Trust - Invesco Government & Agency Portfolio", 0.05, "2026-05-01", "2026-05-01"),
            ("FPX", "AAPL",  "Apple Inc",                                                            0.05, "2026-05-01", "2026-05-01"),
        ])
        assert set(cfg.sanitizer.apply(df)["ticker"]) == {"AAPL"}

    def test_partial_company_name_match_does_not_block(self, cfg):
        """Power Query uses exact (case-insensitive) company match, not substring.
        A holding NAMED 'Cash & Equivalents' is NOT blocked by the default config."""
        df = _h([
            ("FPX", "X1", "Cash & Equivalents",  0.01, "2026-05-01", "2026-05-01"),
            ("FPX", "X2", "U.S. Treasury Bill",  0.01, "2026-05-01", "2026-05-01"),
            ("FPX", "X4", "Microsoft Corp",      0.05, "2026-05-01", "2026-05-01"),
        ])
        result = cfg.sanitizer.apply(df)
        assert set(result["ticker"]) == {"X1", "X2", "X4"}

    def test_ticker_standardization(self, cfg):
        """BRK-B → BRK.B, BF/B → BF.B"""
        df = _h([
            ("FPX", "BRK-B", "Berkshire B",  0.05, "2026-05-01", "2026-05-01"),
            ("FPX", "BF/B",  "Brown-Forman", 0.04, "2026-05-01", "2026-05-01"),
        ])
        result = cfg.sanitizer.apply(df)
        assert set(result["ticker"]) == {"BRK.B", "BF.B"}

    def test_goog_to_googl_normalization(self, cfg):
        """Power Query FixGoogle step: GOOG renamed to GOOGL."""
        df = _h([
            ("SPMO", "GOOG",  "Alphabet Inc", 0.04, "2026-05-01", "2026-05-01"),
            ("SPMO", "GOOGL", "Alphabet Inc", 0.05, "2026-05-01", "2026-05-01"),
        ])
        result = cfg.sanitizer.apply(df)
        # Both rows now have ticker GOOGL
        assert (result["ticker"] == "GOOGL").all()
        assert len(result) == 2

    def test_case_insensitive_ticker_block(self, cfg):
        df = _h([
            ("FPX", "usd", "lowercase variant", 0.01, "2026-05-01", "2026-05-01"),
            ("FPX", "AAPL", "Apple",            0.05, "2026-05-01", "2026-05-01"),
        ])
        assert set(cfg.sanitizer.apply(df)["ticker"]) == {"AAPL"}


# ─── Scoring ──────────────────────────────────────────────────────────────────
class TestScoring:
    def test_rank_multiplier(self, cfg):
        bps = cfg.rank_breakpoints
        assert rank_multiplier(1, bps) == 1.5
        assert rank_multiplier(10, bps) == 1.5
        assert rank_multiplier(11, bps) == 1.2
        assert rank_multiplier(30, bps) == 1.2
        assert rank_multiplier(31, bps) == 1.0
        assert rank_multiplier(500, bps) == 1.0

    def test_all_16_etfs_in_config(self, cfg):
        expected = {"CSD", "FPX", "FPXI", "QMOM", "IMOM", "XMMO", "XSMO", "PIE",
                    "COWZ", "CALF", "SPHQ", "SPMO", "SPHB", "RPG", "QQQM", "XLG"}
        assert set(cfg.etf_tier_map().keys()) == expected

    def test_blob_top_rank(self, cfg):
        """Top-1 in QQQM (Blob, 2 pts) with 9% weight.
        New formula: 9 × 2 × 1.5 = 27 (weight_pct × points × rank_mult)"""
        df = _h([
            ("QQQM", "NVDA", "NVIDIA", 0.10, "2026-01-01", "2026-01-01"),
            ("QQQM", "NVDA", "NVIDIA", 0.09, "2026-05-01", "2026-05-01"),
        ])
        lb, _ = compute_leaderboard(df, cfg)
        # Single Score = 0.09 × 100 × 2 × 1.5 = 27 → int64 = 27
        assert lb.iloc[0]["final_score"] == 27

    def test_high_conviction_flag(self, cfg):
        df = _h([
            ("FPX",  "GEV", "GE Vernova", 0.12, "2026-05-01", "2026-05-01"),
            ("QMOM", "GEV", "GE Vernova", 0.04, "2026-05-01", "2026-05-01"),
            ("SPMO", "GEV", "GE Vernova", 0.03, "2026-05-01", "2026-05-01"),
            ("SPHQ", "GEV", "GE Vernova", 0.04, "2026-05-01", "2026-05-01"),
            ("FPX",  "GEV", "GE Vernova", 0.10, "2026-01-01", "2026-01-01"),
            ("QMOM", "GEV", "GE Vernova", 0.03, "2026-01-01", "2026-01-01"),
            ("SPMO", "GEV", "GE Vernova", 0.02, "2026-01-01", "2026-01-01"),
            ("SPHQ", "GEV", "GE Vernova", 0.03, "2026-01-01", "2026-01-01"),
        ])
        lb, _ = compute_leaderboard(df, cfg)
        row = lb[lb["ticker"] == "GEV"].iloc[0]
        assert row["flag"] == "HIGH_CONVICTION"

    def test_speculative_beta_overridden_by_quality(self, cfg):
        df = _h([
            ("SPHB", "X1", "X1", 0.05, "2026-05-01", "2026-05-01"),
            ("COWZ", "X1", "X1", 0.04, "2026-05-01", "2026-05-01"),
            ("SPHB", "X1", "X1", 0.04, "2026-01-01", "2026-01-01"),
            ("COWZ", "X1", "X1", 0.03, "2026-01-01", "2026-01-01"),
        ])
        lb, _ = compute_leaderboard(df, cfg)
        assert lb.iloc[0]["flag"] == ""

    def test_new_bonus_for_scout(self, cfg):
        df = _h([
            ("QQQM", "X3", "X3", 0.01, "2026-01-01", "2026-01-01"),
            ("FPX",  "X3", "X3", 0.04, "2026-05-01", "2026-05-01"),
            ("QQQM", "X3", "X3", 0.01, "2026-05-01", "2026-05-01"),
        ])
        _, latest = compute_leaderboard(df, cfg)
        fpx_row = latest[(latest["ETF_Ticker"] == "FPX") & (latest["ticker"] == "X3")].iloc[0]
        assert fpx_row["is_new"]
        assert fpx_row["new_bonus"] == 200.0  # 40 × 5

    def test_no_new_bonus_for_quality(self, cfg):
        df = _h([
            ("QQQM", "X2", "X2", 0.05, "2026-01-01", "2026-01-01"),
            ("COWZ", "X2", "X2", 0.05, "2026-05-01", "2026-05-01"),
            ("QQQM", "X2", "X2", 0.05, "2026-05-01", "2026-05-01"),
        ])
        _, latest = compute_leaderboard(df, cfg)
        cowz_row = latest[(latest["ETF_Ticker"] == "COWZ") & (latest["ticker"] == "X2")].iloc[0]
        assert cowz_row["is_new"] and cowz_row["new_bonus"] == 0.0

    def test_invalid_input(self, cfg):
        with pytest.raises(ValueError, match="missing required columns"):
            compute_leaderboard(pd.DataFrame([{"ETF_Ticker": "FPX"}]), cfg)

    def test_unknown_etf_dropped(self, cfg):
        df = _h([
            ("UNKNOWN", "X", "X", 0.10, "2026-05-01", "2026-05-01"),
            ("FPX",     "Y", "Y", 0.10, "2026-05-01", "2026-05-01"),
            ("FPX",     "Y", "Y", 0.08, "2026-01-01", "2026-01-01"),
        ])
        lb, _ = compute_leaderboard(df, cfg)
        assert set(lb["ticker"]) == {"Y"}

    def test_as_of_time_travel(self, cfg):
        """Asking for leaderboard at a past date should use only data ≤ that date."""
        df = _h([
            ("FPX", "A", "A", 0.10, "2026-01-01", "2026-01-01"),  # only A early
            ("FPX", "A", "A", 0.10, "2026-05-01", "2026-05-01"),
            ("FPX", "B", "B", 0.08, "2026-05-01", "2026-05-01"),  # B appears later
        ])
        lb_past, _ = compute_leaderboard(df, cfg, as_of=pd.Timestamp("2026-02-01"))
        lb_today, _ = compute_leaderboard(df, cfg)
        assert set(lb_past["ticker"]) == {"A"}
        assert set(lb_today["ticker"]) == {"A", "B"}

    def test_score_formula_matches_power_query(self, cfg):
        """End-to-end check of the calibrated scoring formula.

        Setup: GEV-like name in Scout + Quality + Quant + Trend at varying weights/ranks.
        Compute expected score from the Power Query formula by hand.
        Verify ours matches to the integer (matches Int64.Type cast)."""
        df = _h([
            # Latest snapshot — varying weights, all top-10 → rank_mult=1.5
            ("FPX",  "GEV", "GE Vernova", 0.12, "2026-05-01", "2026-05-01"),  # Scout, 40
            ("QMOM", "GEV", "GE Vernova", 0.04, "2026-05-01", "2026-05-01"),  # Quant, 40
            ("SPHQ", "GEV", "GE Vernova", 0.05, "2026-05-01", "2026-05-01"),  # Quality, 30
            ("SPMO", "GEV", "GE Vernova", 0.03, "2026-05-01", "2026-05-01"),  # Trend, 10
            # Seed history so nothing flags as NEW
            ("FPX",  "GEV", "GE Vernova", 0.10, "2026-01-01", "2026-01-01"),
            ("QMOM", "GEV", "GE Vernova", 0.03, "2026-01-01", "2026-01-01"),
            ("SPHQ", "GEV", "GE Vernova", 0.04, "2026-01-01", "2026-01-01"),
            ("SPMO", "GEV", "GE Vernova", 0.02, "2026-01-01", "2026-01-01"),
        ])
        lb, _ = compute_leaderboard(df, cfg)
        row = lb[lb["ticker"] == "GEV"].iloc[0]
        # Expected per Power Query: Weight × Points × Rank_Mult × 100
        expected = (
            0.12 * 40 * 1.5 * 100 +   # FPX Scout    = 720
            0.04 * 40 * 1.5 * 100 +   # QMOM Quant   = 240
            0.05 * 30 * 1.5 * 100 +   # SPHQ Quality = 225
            0.03 * 10 * 1.5 * 100     # SPMO Trend   = 45
        )  # = 1230
        assert int(expected) == 1230
        assert row["final_score"] == 1230
        assert row["flag"] == "HIGH_CONVICTION"


# ─── History / temporal ───────────────────────────────────────────────────────
class TestHistory:
    def _build(self, days: int = 30) -> pd.DataFrame:
        # GEV in 4 ETFs every day for `days` days → consistent HC
        rows = []
        for i in range(days):
            d = (pd.Timestamp("2026-05-15") - pd.Timedelta(days=i)).strftime("%Y-%m-%d")
            rows += [
                ("FPX",  "GEV", "GE Vernova", 0.12 + i*0.001, d, d),
                ("QMOM", "GEV", "GE Vernova", 0.04,           d, d),
                ("SPMO", "GEV", "GE Vernova", 0.03,           d, d),
                ("SPHQ", "GEV", "GE Vernova", 0.04,           d, d),
                ("QQQM", "AAPL", "Apple",     0.07,           d, d),
            ]
        return _h(rows)

    def test_hc_streak(self, cfg):
        df = self._build(days=30)
        historical = hist.historical_leaderboards(df, cfg)
        assert len(historical) >= 14
        score_p = hist.score_panel(historical)
        flag_p = hist.flag_panel(historical)
        streaks = hist.streaks_and_deltas(score_p, flag_p)
        gev_row = streaks[streaks["ticker"] == "GEV"].iloc[0]
        # GEV was in HC every snapshot in the window
        assert gev_row["hc_streak"] >= 10

    def test_changelog_shape(self, cfg):
        df = self._build(days=30)
        historical = hist.historical_leaderboards(df, cfg)
        score_p = hist.score_panel(historical)
        flag_p = hist.flag_panel(historical)
        streaks = hist.streaks_and_deltas(score_p, flag_p)
        # Today's leaderboard for the enrichment lookup
        lb_today, _ = compute_leaderboard(df, cfg)
        chg = hist.changelog(historical, lb_today, streaks, top_n=5)
        assert "entered_hc" in chg
        assert "exited_hc" in chg
        assert "biggest_gainers" in chg
        assert chg["today"] is not None
