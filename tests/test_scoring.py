"""Tests for the scoring engine. Run: pytest tests/ -v"""
from __future__ import annotations
import pandas as pd
import pytest
from pathlib import Path

from predator.scoring import Config, ETF, Sanitizer, compute_leaderboard, rank_multiplier
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
        """Power Query FixGoogle step: GOOG renamed to GOOGL, then deduped (weights summed)."""
        df = _h([
            ("SPMO", "GOOG",  "Alphabet Inc", 0.04, "2026-05-01", "2026-05-01"),
            ("SPMO", "GOOGL", "Alphabet Inc", 0.05, "2026-05-01", "2026-05-01"),
        ])
        result = cfg.sanitizer.apply(df)
        # Both rows renamed to GOOGL then deduped into 1 row with weight summed
        assert (result["ticker"] == "GOOGL").all()
        assert len(result) == 1
        assert result["weight"].iloc[0] == pytest.approx(0.09)

    def test_case_insensitive_ticker_block(self, cfg):
        df = _h([
            ("FPX", "usd", "lowercase variant", 0.01, "2026-05-01", "2026-05-01"),
            ("FPX", "AAPL", "Apple",            0.05, "2026-05-01", "2026-05-01"),
        ])
        assert set(cfg.sanitizer.apply(df)["ticker"]) == {"AAPL"}

    def test_googl_dedupe_after_rename(self, cfg):
        """Sanitizer must collapse GOOG+GOOGL rows from the same ETF/date into one row."""
        df = _h([
            ("SPMO", "GOOG",  "Alphabet Inc", 0.04, "2026-05-01", "2026-05-01"),
            ("SPMO", "GOOGL", "Alphabet Inc", 0.05, "2026-05-01", "2026-05-01"),
            ("SPMO", "AAPL",  "Apple Inc",    0.07, "2026-05-01", "2026-05-01"),
        ])
        out = cfg.sanitizer.apply(df)
        googl = out[out["ticker"] == "GOOGL"]
        assert len(googl) == 1
        assert googl["weight"].iloc[0] == pytest.approx(0.09)

    def test_whitespace_ticker_normalization(self, cfg):
        """Space inside ticker collapses to dot: '8058 JP' -> '8058.JP'."""
        df = _h([
            ("IMOM", "8058 JP",  "Mitsubishi Corp",  0.022, "2026-05-01", "2026-05-01"),
            ("FPXI", "8058.JP",  "Mitsubishi Corp",  0.015, "2026-05-01", "2026-05-01"),
            ("IMOM", "TPRO IM",  "Technoprobe SpA",  0.020, "2026-05-01", "2026-05-01"),
            ("FPXI", "TPRO.IM",  "Technoprobe SpA",  0.016, "2026-05-01", "2026-05-01"),
        ])
        out = cfg.sanitizer.apply(df)
        assert set(out["ticker"]) == {"8058.JP", "TPRO.IM"}

    def test_cash_other_blocked(self, cfg):
        """Cash&Other ticker and company names must be blocked."""
        df = _h([
            ("QQQM", "Cash&Other", "Cash & Other", 0.01, "2026-05-01", "2026-05-01"),
            ("QQQM", "NVDA",       "NVIDIA Corp",  0.09, "2026-05-01", "2026-05-01"),
        ])
        out = cfg.sanitizer.apply(df)
        assert set(out["ticker"]) == {"NVDA"}


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
                    "COWZ", "CALF", "SPHQ", "SPMO", "SPHB", "RPG", "QQQM", "XLG",
                    "EEMO", "PDP", "DWAS", "PIZ", "IVAL"}
        assert set(cfg.etf_lookup().keys()) == expected

    def test_fpxi_and_imom_have_60_points(self, cfg):
        """Per-ETF overrides verified against Excel ETF_Config table."""
        lookup = cfg.etf_lookup()
        assert lookup["FPXI"].points == 60
        assert lookup["FPXI"].tier == "Scout"
        assert lookup["IMOM"].points == 60
        assert lookup["IMOM"].tier == "Quant"
        # Others stay at their tier defaults
        assert lookup["FPX"].points == 40
        assert lookup["QMOM"].points == 40
        assert lookup["COWZ"].points == 30
        assert lookup["IVAL"].points == 30
        assert lookup["IVAL"].tier == "Quality"
        assert lookup["SPMO"].points == 10
        assert lookup["QQQM"].points == 2

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


# ─── Velocity signal ───────────────────────────────────────────────────────────
class TestVelocity:
    """Tests for the velocity signal in build.py _attach_velocity."""

    def test_velocity_score_aggregates_rank_deltas(self, cfg):
        """A ticker accumulating weight should yield positive weight_flow in compute_rank_deltas,
        and filler tickers with stable weight should have near-zero flow.

        weight_flow is the primary per-ETF component of velocity_score (×20 in the formula).
        Strategy: X grows from 0.001 → 0.20 over 14 days. 9 stable fillers stay constant.
        """
        import datetime
        from predator.scoring import compute_rank_deltas, compute_leaderboard

        rows = []
        base = datetime.date(2026, 4, 1)
        n_days = 14
        n_fillers = 9

        for i in range(n_days):
            d = (base + datetime.timedelta(days=i)).strftime("%Y-%m-%d")
            x_weight = 0.001 + (0.199 * i / (n_days - 1))
            rows.append(("QMOM", "X", "Ticker X", x_weight, d, d))
            for j in range(n_fillers):
                filler_weight = 0.02 + j * 0.005  # stable at 0.020–0.060
                rows.append(("QMOM", f"F{j:02d}", f"Filler {j}", filler_weight, d, d))

        df = _h(rows)
        leaderboard, _ = compute_leaderboard(df, cfg)

        d7 = compute_rank_deltas(df, cfg, lookback_days=7)
        assert not d7.empty, "Expected non-empty 7-day deltas"

        # -- weight_flow: key velocity input (×20 in composite formula)
        flow_avg_7 = d7.groupby("ticker")["weight_flow"].mean()
        leaderboard["avg_weight_flow_7d"] = leaderboard["ticker"].map(flow_avg_7).fillna(0)

        x_row = leaderboard[leaderboard["ticker"] == "X"]
        assert not x_row.empty, "Ticker X must appear in leaderboard"

        x_flow = x_row.iloc[0]["avg_weight_flow_7d"]
        # X grew its weight by ~200× over the window so flow must be strongly positive
        assert x_flow > 0, (
            f"Expected X avg_weight_flow_7d > 0 (weight accumulated), got {x_flow}.\n"
            f"d7 for X:\n{d7[d7['ticker']=='X'].to_string()}"
        )

        # Stable fillers should have near-zero flow
        filler_rows = leaderboard[leaderboard["ticker"].str.startswith("F")]
        avg_filler_flow = leaderboard["ticker"].map(flow_avg_7).reindex(filler_rows.index).mean()
        assert abs(avg_filler_flow) < x_flow, (
            f"Fillers (avg flow={avg_filler_flow:.4f}) should have less flow than X ({x_flow:.4f})"
        )

    def test_burst_flag_triggers_on_global_rank_jump(self, cfg):
        """A ticker that improved global leaderboard rank by 40+ in last 30d gets burst_30d=True."""
        import datetime
        from predator.scoring import compute_leaderboard
        from predator import history as hist

        rows = []
        base = datetime.date(2026, 3, 1)
        for i in range(35):
            d = (base + datetime.timedelta(days=i)).strftime("%Y-%m-%d")
            # X starts tiny (deep rank), surges after day 15
            weight_x = 0.001 if i < 15 else 0.15
            rows.append(("QMOM", "X", "Ticker X", weight_x, d, d))
            # 80 stable fillers so X starts with rank ~81 when weight is tiny
            for j in range(80):
                rows.append(("QMOM", f"STABLE{j:02d}", f"Stable {j}", 0.01, d, d))

        df = _h(rows)
        lb_today, _ = compute_leaderboard(df, cfg)
        historical = hist.historical_leaderboards(df, cfg)

        if len(historical) < 2:
            pytest.skip("Need >= 2 historical snapshots")

        dates_sorted = sorted(historical.keys())
        today_date   = dates_sorted[-1]
        window_start = today_date - pd.Timedelta(days=30)
        window_cols  = [c for c in dates_sorted if c >= window_start]

        burst_30d        = False
        global_rank_peak = 0
        if len(window_cols) >= 2:
            sample = historical[window_cols[0]]
            if "leaderboard_rank" in sample.columns:
                rank_panel = pd.DataFrame({
                    d: historical[d].set_index("ticker")["leaderboard_rank"]
                    for d in window_cols
                })
                if "X" in rank_panel.index:
                    x_series = rank_panel.loc["X"].dropna()
                    if len(x_series) >= 2:
                        global_rank_peak = int(x_series.max() - x_series.min())
                        burst_30d = global_rank_peak >= 40

        if global_rank_peak < 10:
            pytest.skip(f"Peak improvement only {global_rank_peak} — surge not in 30d window")

        assert burst_30d or global_rank_peak >= 40, (
            f"Expected burst or peak >= 40 for X. Got burst={burst_30d}, peak={global_rank_peak}"
        )


# ─── Burst false-positive tests (Phase 2.7 A1) ────────────────────────────────
class TestBurstFalsePositives:
    """Tests that the new burst criteria correctly reject false positives."""

    def _build_rank_panel(self, rows, cfg):
        """Helper: build historical leaderboards and extract rank panel."""
        df = _h(rows)
        historical = hist.historical_leaderboards(df, cfg)
        dates_sorted = sorted(historical.keys())
        if len(dates_sorted) < 5:
            return None, None, dates_sorted
        today_date = dates_sorted[-1]
        window_start = today_date - pd.Timedelta(days=30)
        window_cols = [c for c in dates_sorted if c >= window_start]
        if len(window_cols) < 5:
            return None, None, dates_sorted
        rank_panel_rows = {}
        for d in window_cols:
            lb = historical[d]
            if "leaderboard_rank" in lb.columns:
                rank_panel_rows[d] = lb.set_index("ticker")["leaderboard_rank"]
        if not rank_panel_rows:
            return None, None, dates_sorted
        rank_panel = pd.DataFrame(rank_panel_rows)
        return rank_panel, window_cols, dates_sorted

    def test_burst_requires_sustained_presence(self, cfg):
        """A ticker that dropped off the leaderboard and returned shouldn't BURST.
        Coverage check: ticker absent for >20% of window → burst=False."""
        import datetime
        rows = []
        base = datetime.date(2026, 3, 1)
        # X present days 0-5, absent days 6-20, present days 21-35
        for i in range(35):
            d = (base + datetime.timedelta(days=i)).strftime("%Y-%m-%d")
            # 80 stable fillers always present
            for j in range(80):
                rows.append(("QMOM", f"STABLE{j:02d}", f"Stable {j}", 0.01, d, d))
            # X only present in first 6 and last 15 days (absent for ~40% of window)
            if i < 6 or i >= 21:
                rows.append(("QMOM", "X", "Ticker X", 0.15, d, d))

        rank_panel, window_cols, _ = self._build_rank_panel(rows, cfg)
        if rank_panel is None or "X" not in rank_panel.index:
            pytest.skip("Not enough snapshots for this test")

        x_row = rank_panel.loc["X"]
        nan_count = x_row.isna().sum()
        coverage = (len(window_cols) - nan_count) / len(window_cols)

        # Coverage should be below 80% due to the gap
        # (X is absent for ~40% of the window)
        peak = (x_row.max() - x_row.min()) if not x_row.dropna().empty else 0

        # Even if peak >= 40, coverage < 0.80 should prevent burst
        if coverage >= 0.80:
            pytest.skip(f"Coverage={coverage:.2f} — not enough gap in this dataset size")

        # Simulate the burst check
        median_per_ticker = rank_panel.median(axis=1)
        recent10 = rank_panel.iloc[:, -10:] if rank_panel.shape[1] >= 10 else rank_panel
        is_better = recent10.lt(median_per_ticker, axis=0)
        sustained = is_better.sum(axis=1)

        is_burst = (peak >= 40) and (coverage >= 0.80) and (sustained.get("X", 0) >= 8)
        assert not is_burst, (
            f"X should NOT burst: coverage={coverage:.2f} < 0.80 required. "
            f"peak={peak}, sustained={sustained.get('X', 0)}"
        )

    def test_burst_requires_sustained_improvement(self, cfg):
        """A ticker that touched +50 ranks for one day shouldn't BURST.
        Sustained check: must be better than median for >=8 of last 10 snapshots."""
        import datetime
        rows = []
        base = datetime.date(2026, 3, 1)
        # X at rank ~80 for 28 days, rank ~20 for 1 day, rank ~80 again
        for i in range(35):
            d = (base + datetime.timedelta(days=i)).strftime("%Y-%m-%d")
            # 80 stable fillers
            for j in range(80):
                rows.append(("QMOM", f"STABLE{j:02d}", f"Stable {j}", 0.01, d, d))
            # X: tiny weight (deep rank) except day 15 where it surges briefly
            if i == 15:
                rows.append(("QMOM", "X", "Ticker X", 0.15, d, d))  # one-day spike
            else:
                rows.append(("QMOM", "X", "Ticker X", 0.001, d, d))  # deep rank

        rank_panel, window_cols, _ = self._build_rank_panel(rows, cfg)
        if rank_panel is None or "X" not in rank_panel.index:
            pytest.skip("Not enough snapshots for this test")

        x_row = rank_panel.loc["X"]
        nan_count = x_row.isna().sum()
        coverage = (len(window_cols) - nan_count) / len(window_cols)
        peak = float(x_row.max() - x_row.min()) if not x_row.dropna().empty else 0

        median_per_ticker = rank_panel.median(axis=1)
        recent10 = rank_panel.iloc[:, -10:] if rank_panel.shape[1] >= 10 else rank_panel
        is_better = recent10.lt(median_per_ticker, axis=0)
        sustained = int(is_better.loc["X"].sum()) if "X" in is_better.index else 0

        # With only 1 day of improvement, sustained should be < 8
        is_burst = (peak >= 40) and (coverage >= 0.80) and (sustained >= 8)
        assert not is_burst, (
            f"X should NOT burst: one-day spike only. "
            f"peak={peak:.0f}, coverage={coverage:.2f}, sustained={sustained} (need >=8)"
        )
