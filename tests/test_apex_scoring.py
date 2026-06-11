"""Tests for §30 apex-mode scoring (Predator v3) and the §31 velocity overlay.

Run: pytest tests/test_apex_scoring.py -v
"""
from __future__ import annotations
import dataclasses
import math

import pandas as pd
import pytest
import yaml
from pathlib import Path

from predator.scoring import (
    ApexConfig,
    Config,
    compute_leaderboard,
    conviction_multiplier,
)
from predator.history import accumulation_velocity, predictive_overlay


CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"


@pytest.fixture
def apex_cfg() -> Config:
    """Apex scoring mode — §30 formula."""
    c = Config.from_yaml(CONFIG_PATH)
    return dataclasses.replace(c, scoring_mode="apex")


@pytest.fixture
def conviction_cfg() -> Config:
    """Conviction scoring mode — §24 formula (regression guard baseline)."""
    c = Config.from_yaml(CONFIG_PATH)
    return dataclasses.replace(c, scoring_mode="conviction")


def _h(rows: list[tuple]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=["ETF_Ticker", "ticker", "name", "weight", "Holdings_As_Of", "Date_Scraped"])


# ─── §30 Config parsing ───────────────────────────────────────────────────────
class TestApexConfig:
    def test_apex_config_parses_from_yaml(self):
        """config.yaml carries scoring_mode: apex and the full apex block."""
        cfg = Config.from_yaml(CONFIG_PATH)
        assert cfg.scoring_mode == "apex"
        assert cfg.apex is not None
        assert cfg.apex.conc_floor == 0.60
        assert cfg.apex.conc_target_neff == 3.0
        assert cfg.apex.breadth_alpha == 0.35
        assert cfg.apex.universe_eta == 0.50
        assert cfg.apex.universe_lambda == 40.0
        assert cfg.apex.universe_clip_lo == 0.80
        assert cfg.apex.universe_clip_hi == 1.25
        assert cfg.apex.conc_flag_share == 0.85
        # Apex mode REQUIRES conviction params (shared per-holding path)
        assert cfg.conviction is not None

    def test_apex_defaults_when_block_absent(self, tmp_path):
        """scoring_mode: apex without an apex: block → all-default ApexConfig."""
        raw = yaml.safe_load(CONFIG_PATH.read_text())
        raw.pop("apex", None)
        raw["scoring_mode"] = "apex"
        p = tmp_path / "cfg.yaml"
        p.write_text(yaml.safe_dump(raw))
        cfg = Config.from_yaml(p)
        assert cfg.apex == ApexConfig()
        assert cfg.conviction is not None

    def test_apex_config_validation(self):
        with pytest.raises(ValueError, match="conc_target_neff"):
            ApexConfig(conc_target_neff=1.0)
        with pytest.raises(ValueError, match="conc_floor"):
            ApexConfig(conc_floor=1.5)
        with pytest.raises(ValueError, match="universe_clip_lo"):
            ApexConfig(universe_clip_lo=1.3, universe_clip_hi=1.1)


# ─── Conviction-mode regression guard ─────────────────────────────────────────
class TestConvictionRegression:
    def test_conviction_mode_unchanged_by_apex_code(self, conviction_cfg):
        """With apex code present, conviction mode must still produce the exact
        §24 score: Final = Σ points_i × C_i, and all apex multipliers == 1.0."""
        df = _h([
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
        lb, _ = compute_leaderboard(df, conviction_cfg)
        row = lb[lb["ticker"] == "GEV"].iloc[0]

        # Hand-computed §24 score: each ETF holds only GEV → n=1, rank=1,
        # grank = 1.0, conviction = ln(1+w)/ln(1+ow_star).
        cv = conviction_cfg.conviction
        expected = sum(
            pts * conviction_multiplier(w, 1, 1, cv)
            for pts, w in [(40, 0.12), (40, 0.04), (30, 0.05), (10, 0.03)]
        )
        assert row["final_score"] == int(expected)

        # Apex multipliers must be neutral 1.0 in conviction mode
        assert (lb["m_conc"] == 1.0).all()
        assert (lb["m_breadth"] == 1.0).all()
        assert (lb["m_universe"] == 1.0).all()
        # Descriptive diagnostics still present (stable JSON schema)
        for col in ("hhi", "n_eff", "tier_entropy"):
            assert col in lb.columns

    def test_legacy_mode_unchanged_by_apex_code(self):
        """Legacy formula still yields the exact Power Query score with
        neutral apex multipliers."""
        cfg = dataclasses.replace(
            Config.from_yaml(CONFIG_PATH), scoring_mode="legacy", etf_maturity_days=0)
        df = _h([
            ("QQQM", "NVDA", "NVIDIA", 0.10, "2026-01-01", "2026-01-01"),
            ("QQQM", "NVDA", "NVIDIA", 0.09, "2026-05-01", "2026-05-01"),
        ])
        lb, _ = compute_leaderboard(df, cfg)
        # Single Score = 0.09 × 100 × 2 × 1.5 = 27
        assert lb.iloc[0]["final_score"] == 27
        assert lb.iloc[0]["m_conc"] == 1.0
        assert lb.iloc[0]["m_breadth"] == 1.0
        assert lb.iloc[0]["m_universe"] == 1.0


# ─── §30(a) Concentration multiplier ──────────────────────────────────────────
class TestConcentration:
    def test_m_conc_floor_for_single_etf(self, apex_cfg):
        """All score in 1 ETF → HHI = 1, N_eff = 1 → M_conc == conc_floor."""
        df = _h([
            ("QMOM", "SOLO", "Solo Corp", 0.10, "2026-05-01", "2026-05-01"),
            ("QMOM", "SOLO", "Solo Corp", 0.09, "2026-01-01", "2026-01-01"),
        ])
        lb, _ = compute_leaderboard(df, apex_cfg)
        row = lb[lb["ticker"] == "SOLO"].iloc[0]
        assert row["hhi"] == pytest.approx(1.0)
        assert row["n_eff"] == pytest.approx(1.0)
        assert row["m_conc"] == pytest.approx(apex_cfg.apex.conc_floor)

    def test_m_conc_releases_at_target_neff(self, apex_cfg):
        """Equal thirds across 3 ETFs → N_eff = 3 = conc_target_neff → M_conc == 1.0."""
        rows = []
        # Same weight, same points (Quant 40), single holding each → equal scores
        for etf in ("QMOM", "XMMO", "XSMO"):
            rows.append((etf, "TRI", "Triple Corp", 0.05, "2026-05-01", "2026-05-01"))
            rows.append((etf, "TRI", "Triple Corp", 0.05, "2026-01-01", "2026-01-01"))
        lb, _ = compute_leaderboard(_h(rows), apex_cfg)
        row = lb[lb["ticker"] == "TRI"].iloc[0]
        assert row["hhi"] == pytest.approx(1.0 / 3.0, abs=1e-4)
        assert row["n_eff"] == pytest.approx(3.0, abs=0.01)
        assert row["m_conc"] == pytest.approx(1.0)


# ─── §30(b) Breadth multiplier ────────────────────────────────────────────────
class TestBreadth:
    def test_mono_tier_m_breadth_is_one(self, apex_cfg):
        """Single strategy tier → entropy 0 → M_breadth == 1.0."""
        df = _h([
            ("QMOM", "MONO", "Mono Corp", 0.05, "2026-05-01", "2026-05-01"),
            ("QMOM", "MONO", "Mono Corp", 0.05, "2026-01-01", "2026-01-01"),
        ])
        lb, _ = compute_leaderboard(df, apex_cfg)
        row = lb[lb["ticker"] == "MONO"].iloc[0]
        assert row["tier_entropy"] == 0.0
        assert row["m_breadth"] == pytest.approx(1.0)

    def test_two_tier_equal_split_beats_mono_tier(self, apex_cfg):
        """Equal split across 2 tiers → M_breadth = 1 + α·ln(2)/ln(N_tiers) > 1."""
        rows = []
        # FPX (Scout, 40 pts) + QMOM (Quant, 40 pts), same weight, single
        # holding each → identical per-holding scores → exact 50/50 tier split.
        for etf in ("FPX", "QMOM"):
            rows.append((etf, "DUO", "Duo Corp", 0.05, "2026-05-01", "2026-05-01"))
            rows.append((etf, "DUO", "Duo Corp", 0.05, "2026-01-01", "2026-01-01"))
        lb, _ = compute_leaderboard(_h(rows), apex_cfg)
        row = lb[lb["ticker"] == "DUO"].iloc[0]

        n_tiers = len(apex_cfg.all_tier_names())
        expected = 1.0 + apex_cfg.apex.breadth_alpha * math.log(2) / math.log(n_tiers)
        assert row["m_breadth"] == pytest.approx(expected, abs=1e-3)
        assert row["m_breadth"] > 1.0  # strictly above mono-tier's 1.0


# ─── §30(c) Universe multiplier ───────────────────────────────────────────────
class TestUniverse:
    def test_m_universe_clipped_to_bounds(self, apex_cfg):
        """M_universe must stay within [universe_clip_lo, universe_clip_hi]
        even for extreme penetration ratios."""
        rows = []
        # Dominant whale + tiny fillers → penetration spread far beyond the clip range
        rows.append(("QMOM", "BIG", "Big Corp", 0.50, "2026-05-01", "2026-05-01"))
        rows.append(("QMOM", "BIG", "Big Corp", 0.50, "2026-01-01", "2026-01-01"))
        for j in range(10):
            rows.append(("QMOM", f"F{j:02d}", f"Filler {j}", 0.01, "2026-05-01", "2026-05-01"))
            rows.append(("QMOM", f"F{j:02d}", f"Filler {j}", 0.01, "2026-01-01", "2026-01-01"))
        lb, _ = compute_leaderboard(_h(rows), apex_cfg)

        ax = apex_cfg.apex
        assert (lb["m_universe"] >= ax.universe_clip_lo).all()
        assert (lb["m_universe"] <= ax.universe_clip_hi).all()
        # The whale's penetration ratio is far above median → ceiling-clipped
        big = lb[lb["ticker"] == "BIG"].iloc[0]
        assert big["m_universe"] == pytest.approx(ax.universe_clip_hi)


# ─── §30 CONCENTRATED flag ────────────────────────────────────────────────────
class TestConcentratedFlag:
    def test_equal_two_etf_split_not_flagged(self, apex_cfg):
        """Equal 2-ETF split (s_max = 0.5 < conc_flag_share) must NOT flag —
        a 50/50 split is the normal shape of any 2-book name, not an anomaly."""
        rows = []
        for etf in ("QMOM", "XMMO"):
            rows.append((etf, "CC", "Concentrated Corp", 0.05, "2026-05-01", "2026-05-01"))
            rows.append((etf, "CC", "Concentrated Corp", 0.05, "2026-01-01", "2026-01-01"))
        lb, _ = compute_leaderboard(_h(rows), apex_cfg)
        row = lb[lb["ticker"] == "CC"].iloc[0]
        assert row["etf_count"] == 2
        assert row["max_etf_share"] == pytest.approx(0.5, abs=1e-3)
        assert row["max_etf_share"] < apex_cfg.apex.conc_flag_share
        assert row["flag"] != "CONCENTRATED"

    def test_concentrated_fires_for_dominated_two_etf(self, apex_cfg):
        """2-ETF ticker where one fund supplies ≥ 85% of score mass and it is
        not HC → CONCENTRATED."""
        rows = [
            # QMOM (Quant 40) w=0.20 dominates SPMO (Trend 10) w=0.05 →
            # score shares ≈ 0.94 / 0.06 → s_max well above 0.85.
            ("QMOM", "DOM", "Dominated Corp", 0.20, "2026-05-01", "2026-05-01"),
            ("QMOM", "DOM", "Dominated Corp", 0.20, "2026-01-01", "2026-01-01"),
            ("SPMO", "DOM", "Dominated Corp", 0.05, "2026-05-01", "2026-05-01"),
            ("SPMO", "DOM", "Dominated Corp", 0.05, "2026-01-01", "2026-01-01"),
        ]
        lb, _ = compute_leaderboard(_h(rows), apex_cfg)
        row = lb[lb["ticker"] == "DOM"].iloc[0]
        assert row["etf_count"] == 2
        assert row["max_etf_share"] >= apex_cfg.apex.conc_flag_share
        assert row["flag"] == "CONCENTRATED"

    def test_high_conviction_takes_priority_over_concentrated(self, apex_cfg):
        """A ticker that satisfies BOTH HC and CONCENTRATED conditions must be
        flagged HIGH_CONVICTION (priority: HC > CONCENTRATED > SPEC_BETA)."""
        rows = []
        # HCT top-of-book overweight in 4 ETFs (conviction ≥ hc_theta in each):
        #   IMOM (60 pts): w=0.95, OW≈10.45 → wovr clamps at 1.5 → score 90
        #   SPMO (10 pts): w=0.25, OW=2.75 → wovr≈0.82 → score ≈8.2
        #   XLG/QQQM (Core 2 pts): w=0.25, OW=2.75 → wovr≈0.82 → score ≈1.6
        # Mass share of IMOM ≈ 90/101.5 ≈ 0.89 ≥ conc_flag_share 0.85
        # (CONCENTRATED condition also true), but conviction_etf_count = 4 ≥ 4
        # → HC wins.
        for etf, w in [("IMOM", 0.95), ("SPMO", 0.25), ("XLG", 0.25), ("QQQM", 0.25)]:
            rows.append((etf, "HCT", "HC Ticker", w, "2026-05-01", "2026-05-01"))
            rows.append((etf, "HCT", "HC Ticker", w, "2026-01-01", "2026-01-01"))
            for j in range(10):
                rows.append((etf, f"FILL_{etf}_{j}", f"Filler {j}", 0.005, "2026-05-01", "2026-05-01"))
                rows.append((etf, f"FILL_{etf}_{j}", f"Filler {j}", 0.005, "2026-01-01", "2026-01-01"))
        lb, latest = compute_leaderboard(_h(rows), apex_cfg)
        row = lb[lb["ticker"] == "HCT"].iloc[0]

        # Sanity: setup actually exercises both conditions
        hct = latest[latest["ticker"] == "HCT"]
        assert (hct["conviction"] >= apex_cfg.conviction.hc_theta).all(), (
            f"Setup invalid: convictions {hct['conviction'].tolist()} must all be >= hc_theta"
        )
        assert row["max_etf_share"] >= apex_cfg.apex.conc_flag_share, (
            f"Setup invalid: max_etf_share={row['max_etf_share']} must be >= conc_flag_share"
        )
        assert row["flag"] == "HIGH_CONVICTION"

    def test_concentrated_fires_for_single_etf_outsized_bet(self, apex_cfg):
        """Single-ETF holding with OW = weight × n_holdings ≥ ow_star → CONCENTRATED;
        a single-ETF filler position stays unflagged."""
        rows = []
        # BIGBET: w=0.5 in an 11-name book → OW = 5.5 ≥ ow_star (4.0)
        rows.append(("QMOM", "BIGBET", "Big Bet Corp", 0.50, "2026-05-01", "2026-05-01"))
        rows.append(("QMOM", "BIGBET", "Big Bet Corp", 0.50, "2026-01-01", "2026-01-01"))
        for j in range(10):
            rows.append(("QMOM", f"F{j:02d}", f"Filler {j}", 0.05, "2026-05-01", "2026-05-01"))
            rows.append(("QMOM", f"F{j:02d}", f"Filler {j}", 0.05, "2026-01-01", "2026-01-01"))
        lb, _ = compute_leaderboard(_h(rows), apex_cfg)

        big = lb[lb["ticker"] == "BIGBET"].iloc[0]
        assert big["etf_count"] == 1
        assert big["flag"] == "CONCENTRATED"
        # Filler: OW = 0.05 × 11 = 0.55 < 4.0 → no flag
        filler = lb[lb["ticker"] == "F00"].iloc[0]
        assert filler["flag"] == ""

    def test_concentrated_never_emitted_in_conviction_mode(self, conviction_cfg):
        """Legacy/conviction flag logic is byte-identical to before apex."""
        rows = []
        for etf in ("QMOM", "XMMO"):
            rows.append((etf, "CC", "Concentrated Corp", 0.05, "2026-05-01", "2026-05-01"))
            rows.append((etf, "CC", "Concentrated Corp", 0.05, "2026-01-01", "2026-01-01"))
        lb, _ = compute_leaderboard(_h(rows), conviction_cfg)
        assert "CONCENTRATED" not in set(lb["flag"])


# ─── Owner directive: single-fund monsters stay visible ──────────────────────
class TestSingleFundVisibility:
    def test_single_fund_massive_bet_stays_visible(self, apex_cfg, conviction_cfg):
        """A really large single-ETF bet must remain on the leaderboard with
        at least conc_floor × raw conviction mass (owner directive)."""
        rows = []
        rows.append(("QMOM", "WHALE", "Whale Corp", 0.50, "2026-05-01", "2026-05-01"))
        rows.append(("QMOM", "WHALE", "Whale Corp", 0.50, "2026-01-01", "2026-01-01"))
        for j in range(10):
            rows.append(("QMOM", f"F{j:02d}", f"Filler {j}", 0.01, "2026-05-01", "2026-05-01"))
            rows.append(("QMOM", f"F{j:02d}", f"Filler {j}", 0.01, "2026-01-01", "2026-01-01"))
        df = _h(rows)

        lb_apex, _ = compute_leaderboard(df, apex_cfg)
        lb_conv, _ = compute_leaderboard(df, conviction_cfg)

        whale_apex = lb_apex[lb_apex["ticker"] == "WHALE"]
        assert not whale_apex.empty, "WHALE vanished from the apex leaderboard"

        raw_mass = lb_conv[lb_conv["ticker"] == "WHALE"].iloc[0]["final_score"]
        apex_score = whale_apex.iloc[0]["final_score"]
        # int truncation can cost at most 1 point on each side
        assert apex_score >= apex_cfg.apex.conc_floor * raw_mass - 1, (
            f"Apex score {apex_score} fell below conc_floor × raw mass "
            f"({apex_cfg.apex.conc_floor} × {raw_mass})"
        )
        # The whale still dwarfs the fillers — top of the board
        assert whale_apex.iloc[0]["leaderboard_rank"] == 1


# ─── §31 accumulation_velocity ────────────────────────────────────────────────
class TestAccumulationVelocity:
    def _panel(self, extra: dict | None = None) -> pd.DataFrame:
        dates = pd.date_range("2026-05-01", periods=10, freq="D")
        data = {
            "UP":    [100.0 + 10 * i for i in range(10)],
            "FLAT1": [100.0] * 10,
            "FLAT2": [200.0] * 10,
            "FLAT3": [150.0] * 10,
        }
        if extra:
            data.update(extra)
        return pd.DataFrame(data, index=dates).T

    def test_rising_ticker_gets_positive_velocity_z(self):
        vel = accumulation_velocity(self._panel())
        up = vel[vel["ticker"] == "UP"].iloc[0]
        flat = vel[vel["ticker"] == "FLAT1"].iloc[0]
        assert up["slope"] == pytest.approx(10.0)
        assert up["velocity_z"] > 0
        assert flat["slope"] == pytest.approx(0.0)
        assert up["velocity_z"] > flat["velocity_z"]

    def test_fewer_than_four_points_is_nan_safe(self):
        nan = float("nan")
        panel = self._panel(extra={"SPARSE": [nan] * 7 + [100.0, 110.0, 120.0]})
        vel = accumulation_velocity(panel)  # must not raise
        sparse = vel[vel["ticker"] == "SPARSE"].iloc[0]
        assert pd.isna(sparse["slope"])
        assert pd.isna(sparse["slope_pct"])
        # Other tickers unaffected
        assert vel[vel["ticker"] == "UP"].iloc[0]["velocity_z"] > 0

    def test_empty_panel_returns_empty_frame(self):
        vel = accumulation_velocity(pd.DataFrame())
        assert vel.empty
        assert list(vel.columns) == ["ticker", "slope", "slope_pct", "velocity_z", "ignition_z"]


# ─── §31 predictive_overlay ───────────────────────────────────────────────────
class TestPredictiveOverlay:
    def _fixture(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        dates = pd.date_range("2026-05-01", periods=10, freq="D")
        # UP and DOWN are exact mirrors so the cross-sectional mean slope_pct
        # is 0 — FLAT sits exactly at the mean → velocity_z = 0.
        panel = pd.DataFrame({
            "UP":   [100.0 + 10 * i for i in range(10)],
            "DOWN": [190.0 - 10 * i for i in range(10)],
            "FLAT": [145.0] * 10,
        }, index=dates).T
        lb = pd.DataFrame({
            "ticker": ["UP", "DOWN", "FLAT"],
            "final_score": pd.array([1000, 1000, 1000], dtype="int64"),
            "etf_count": [2, 2, 2],
            "total_weight": [0.1, 0.1, 0.1],
            "leaderboard_rank": [1, 2, 3],
        })
        return lb, panel

    def test_overlay_directions_and_ceiling(self):
        lb, panel = self._fixture()
        out = predictive_overlay(lb, panel)

        up = out[out["ticker"] == "UP"].iloc[0]
        down = out[out["ticker"] == "DOWN"].iloc[0]
        flat = out[out["ticker"] == "FLAT"].iloc[0]

        assert up["apex_score"] > up["final_score"]
        assert flat["apex_score"] == flat["final_score"]
        assert down["apex_score"] < down["final_score"]
        # tanh-bounded kicker: apex_score can never exceed final_score × 1.35
        assert (out["apex_score"] <= out["final_score"] * 1.35).all()
        # Re-ranked by apex_score
        assert up["apex_rank"] == 1
        assert flat["apex_rank"] == 2
        assert down["apex_rank"] == 3

    def test_overlay_returns_copy_with_new_columns(self):
        lb, panel = self._fixture()
        out = predictive_overlay(lb, panel)
        assert out is not lb
        assert "apex_score" not in lb.columns  # original untouched
        for col in ("velocity_z", "ignition_z", "apex_score", "apex_rank"):
            assert col in out.columns

    def test_empty_panel_neutral_overlay(self):
        lb, _ = self._fixture()
        out = predictive_overlay(lb, pd.DataFrame())
        assert (out["apex_score"] == out["final_score"]).all()
        assert (out["apex_rank"] == out["leaderboard_rank"]).all()
        assert (out["velocity_z"] == 0.0).all()
        assert (out["ignition_z"] == 0.0).all()

    # ── §31.2 conviction-quality multiplier (m_conv) ──────────────────────────

    def test_low_conviction_lowers_apex_below_no_factor(self):
        """avg_conviction < 1 must produce apex_score strictly below the value
        that would result with no m_conv factor (i.e., m_conv == 1.0)."""
        lb, panel = self._fixture()
        # Inject avg_conviction < 1 for all tickers
        lb["avg_conviction"] = 0.60

        out_with   = predictive_overlay(lb, panel, conv_floor=0.50, conv_cap=1.25, conv_gamma=1.0)
        out_neutral = predictive_overlay(lb.assign(avg_conviction=1.0), panel,
                                         conv_floor=0.50, conv_cap=1.25, conv_gamma=1.0)

        # Every apex_score with low conviction must be ≤ the neutral baseline
        for ticker in ["UP", "DOWN", "FLAT"]:
            gated = out_with.loc[out_with["ticker"] == ticker, "apex_score"].iloc[0]
            base  = out_neutral.loc[out_neutral["ticker"] == ticker, "apex_score"].iloc[0]
            assert gated <= base, (
                f"{ticker}: low-conviction apex_score {gated} should be ≤ neutral {base}"
            )
        # At least one must be strictly lower (not all tied at 0)
        assert any(
            out_with.loc[out_with["ticker"] == t, "apex_score"].iloc[0]
            < out_neutral.loc[out_neutral["ticker"] == t, "apex_score"].iloc[0]
            for t in ["UP", "FLAT"]
        )

    def test_high_conviction_raises_apex_capped_at_conv_cap_gamma(self):
        """avg_conviction > 1 raises apex_score, but is capped at conv_cap ** conv_gamma."""
        lb, panel = self._fixture()
        # Very high avg_conviction — should be clipped to conv_cap
        conv_cap = 1.25
        conv_gamma = 1.0
        lb["avg_conviction"] = 2.00  # above cap

        out = predictive_overlay(lb, panel, conv_floor=0.50, conv_cap=conv_cap, conv_gamma=conv_gamma)
        # Effective multiplier is conv_cap ** conv_gamma = 1.25
        out_at_cap = predictive_overlay(lb.assign(avg_conviction=conv_cap), panel,
                                        conv_floor=0.50, conv_cap=conv_cap, conv_gamma=conv_gamma)

        for ticker in ["UP", "DOWN", "FLAT"]:
            score_high = out.loc[out["ticker"] == ticker, "apex_score"].iloc[0]
            score_cap  = out_at_cap.loc[out_at_cap["ticker"] == ticker, "apex_score"].iloc[0]
            # Both should be identical (avg_conviction=2.0 clips to conv_cap=1.25)
            assert score_high == score_cap, (
                f"{ticker}: apex_score with avg_conviction=2.0 ({score_high}) "
                f"should equal cap value ({score_cap})"
            )

        # Capped scores must be higher than neutral (avg_conviction=1.0)
        out_neutral = predictive_overlay(lb.assign(avg_conviction=1.0), panel,
                                         conv_floor=0.50, conv_cap=conv_cap, conv_gamma=conv_gamma)
        flat_capped  = out.loc[out["ticker"] == "FLAT", "apex_score"].iloc[0]
        flat_neutral = out_neutral.loc[out_neutral["ticker"] == "FLAT", "apex_score"].iloc[0]
        assert flat_capped >= flat_neutral

    def test_missing_conviction_is_neutral(self):
        """Missing (None/NaN) avg_conviction must give m_conv = 1.0 (neutral)."""
        lb, panel = self._fixture()
        # No avg_conviction column at all
        lb_no_conv = lb.drop(columns=["avg_conviction"], errors="ignore")
        # avg_conviction = 1.0 explicitly
        lb_one = lb.assign(avg_conviction=1.0)

        out_missing = predictive_overlay(lb_no_conv, panel, conv_floor=0.50, conv_cap=1.25, conv_gamma=1.0)
        out_one     = predictive_overlay(lb_one, panel, conv_floor=0.50, conv_cap=1.25, conv_gamma=1.0)

        for ticker in ["UP", "DOWN", "FLAT"]:
            score_missing = out_missing.loc[out_missing["ticker"] == ticker, "apex_score"].iloc[0]
            score_one     = out_one.loc[out_one["ticker"] == ticker, "apex_score"].iloc[0]
            assert score_missing == score_one, (
                f"{ticker}: missing conviction score {score_missing} != "
                f"explicit 1.0 score {score_one}"
            )
