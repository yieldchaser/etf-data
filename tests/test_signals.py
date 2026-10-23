"""BURST / CRATER (2026-10 redesign) and the partial-snapshot guard.

See docs/SIGNALS_STUDY.md. Run: pytest tests/test_signals.py -v
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from conviction import history as hist
from conviction.scoring import Config, MotionConfig, Sanitizer, compute_leaderboard, clear_sanitizer_cache

CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"
MOTION = MotionConfig()


def _historical(series: dict[str, list[tuple[int, int]]], start="2026-06-01") -> dict:
    """series: ticker -> [(rank, etf_count), ...] per snapshot (same length)."""
    n = len(next(iter(series.values())))
    dates = pd.date_range(start, periods=n, freq="D")
    out = {}
    for i, d in enumerate(dates):
        rows = [{"ticker": t, "leaderboard_rank": v[i][0], "etf_count": v[i][1]}
                for t, v in series.items() if v[i] is not None]
        out[d] = pd.DataFrame(rows)
    return out


def _flags(series, kind):
    bc = hist.burst_crater_panels(_historical(series), MOTION)
    last = bc[kind].columns[-1]
    return set(bc[kind].index[bc[kind][last].to_numpy(dtype=bool)])


class TestBurstCrater:
    def test_one_day_glitch_fires_neither(self):
        """#100, one-day spike to #140, back to ~#100: the old range rule (≥ 40)
        fired BURST or CRATER on this; smoothing + direction must not."""
        ranks = [100] * 8 + [140] + [100, 105, 102, 100, 101, 100]
        s = {"GLITCH": [(r, 4) for r in ranks]}
        assert _flags(s, "burst") == set()
        assert _flags(s, "crater") == set()

    def test_genuine_fund_confirmed_surge_is_burst(self):
        ranks = [260, 255, 250, 240, 225, 210, 195, 180, 170, 160, 150, 145, 140, 140]
        funds = [3] * 6 + [4] * 8
        assert _flags({"UP": list(zip(ranks, funds))}, "burst") == {"UP"}

    def test_surge_without_fund_add_is_not_burst(self):
        ranks = [260, 255, 250, 240, 225, 210, 195, 180, 170, 160, 150, 145, 140, 140]
        assert _flags({"UP": [(r, 3) for r in ranks]}, "burst") == set()

    def test_surge_without_fund_add_is_a_surge(self):
        """The rank move alone is tagged SURGE — momentum, shown but not a signal."""
        ranks = [260, 255, 250, 240, 225, 210, 195, 180, 170, 160, 150, 145, 140, 140]
        assert _flags({"UP": [(r, 3) for r in ranks]}, "surge") == {"UP"}

    def test_x1_5_move_with_fund_add_is_price_grade_not_burst(self):
        """×1.5 clears the price-driven bar (×1.4) but not BURST's ×1.65, even
        with a fund joining: it shows as the dim (SURGE) grade."""
        ranks = [225, 225, 225, 215, 205, 195, 185, 175, 165, 158, 152, 150, 150, 150]
        funds = [3] * 6 + [4] * 8
        s = {"UP": list(zip(ranks, funds))}
        assert _flags(s, "burst") == set() and _flags(s, "surge") == {"UP"}

    def test_x1_3_move_fires_neither_grade(self):
        ranks = [195, 195, 195, 190, 185, 180, 175, 170, 165, 160, 155, 150, 150, 150]
        s = {"UP": [(r, 3) for r in ranks]}
        assert _flags(s, "burst") == set() and _flags(s, "surge") == set()
        down = {"DN": [(r, 3) for r in reversed(ranks)]}
        assert _flags(down, "slide") == set()

    def test_burst_is_not_also_a_surge(self):
        ranks = [260, 255, 250, 240, 225, 210, 195, 180, 170, 160, 150, 145, 140, 140]
        funds = [3] * 6 + [4] * 8
        s = {"UP": list(zip(ranks, funds))}
        assert _flags(s, "burst") == {"UP"} and _flags(s, "surge") == set()

    def test_fall_without_fund_exit_is_a_slide_not_a_crater(self):
        ranks = [100] * 10 + [100, 100, 105, 110, 120, 135, 150, 165, 180, 195, 210, 220, 230, 235]
        s = {"DOWN": [(r, 5) for r in ranks]}
        assert _flags(s, "slide") == {"DOWN"}
        assert _flags(s, "crater") == set()

    def test_crater_needs_only_x135_over_its_window(self):
        """#100 → #140 over ~4 weeks with a fund leaving is a CRATER (×1.4)."""
        ranks = [100] * 12 + [100, 102, 105, 108, 112, 116, 120, 124, 128, 132, 136, 138, 140, 140]
        funds = [5] * 16 + [4] * 10
        assert _flags({"DOWN": list(zip(ranks, funds))}, "crater") == {"DOWN"}

    def test_collapse_with_fund_exit_is_crater(self):
        ranks = [100] * 10 + [100, 100, 105, 110, 120, 135, 150, 165, 180, 195, 210, 220, 230, 235]
        funds = [5] * 16 + [4] * 8          # CRATER measures over 20 snapshots
        s = {"DOWN": list(zip(ranks, funds))}
        assert _flags(s, "crater") == {"DOWN"}
        assert _flags(s, "burst") == set()

    def test_crater_when_the_fund_exit_precedes_the_slide(self):
        """Yageo (2327), 2026-07: PIE sold at #6 and the rank slid to #15+
        about 13 snapshots later — outside a 10-snapshot window, inside
        CRATER's 20-snapshot one."""
        ranks = [6] * 12 + [7, 8, 8, 8, 8, 8, 10, 10, 12, 15, 14, 13, 15, 15, 15]
        funds = [5] * 10 + [4] * 17          # exit 16 snapshots before the last
        s = {"YAGEO": list(zip(ranks, funds))}
        assert _flags(s, "crater") == {"YAGEO"}

    def test_crater_needs_an_exit_somewhere_in_its_window(self):
        ranks = [6] * 12 + [7, 8, 8, 8, 8, 8, 10, 10, 12, 15, 14, 13, 15, 15, 15]
        assert _flags({"DRIFT": [(r, 5) for r in ranks]}, "crater") == set()

    def test_burst_still_needs_the_add_inside_its_move_window(self):
        """Widening BURST's window hurt it in the study, so an add 13+
        snapshots before the surge does not confirm a BURST."""
        ranks = [260] * 12 + [255, 250, 240, 225, 210, 195, 180, 170, 160, 150, 145, 140, 140, 140, 140]
        funds = [3] * 10 + [4] * 17
        assert _flags({"UP": list(zip(ranks, funds))}, "burst") == set()

    def test_fall_is_never_a_burst(self):
        """The old 'peak improvement' was worst − best, so a pure fall counted."""
        ranks = [50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 170, 180]
        funds = [5] * 7 + [6] * 7
        assert _flags({"FALL": list(zip(ranks, funds))}, "burst") == set()

    def test_deep_board_churn_is_irrelevant(self):
        """#2,000 → #1,200 is a big move in positions but outside the top 300."""
        ranks = list(np.linspace(2000, 1200, 14).astype(int))
        funds = [1] * 6 + [2] * 8
        assert _flags({"DEEP": list(zip(ranks, funds))}, "burst") == set()

    def test_spent_move_is_not_burst(self):
        """Surged then gave most of it back — no longer in force."""
        ranks = [260, 240, 200, 160, 120, 100, 100, 120, 150, 180, 200, 210, 215, 215]
        funds = [3] * 6 + [4] * 8
        assert _flags({"SPENT": list(zip(ranks, funds))}, "burst") == set()

    def test_backtest_and_signal_history_share_the_implementation(self):
        """signal_history must emit the same BURST flags as the shared panels."""
        cfg = Config.from_yaml(CONFIG_PATH)
        ranks = [260, 255, 250, 240, 225, 210, 195, 180, 170, 160, 150, 145, 140, 140]
        funds = [3] * 6 + [4] * 8
        historical = _historical({"UP": list(zip(ranks, funds))})
        for d, lb in historical.items():
            lb["flag"] = ""; lb["held_by"] = "FPX"; lb["any_new"] = False
        raw = pd.DataFrame(columns=["ETF_Ticker", "ticker", "name", "weight", "Holdings_As_Of", "Date_Scraped"])
        records = hist.signal_history(raw, historical, cfg)
        assert any(r.get("burst") for r in records.get("UP", []))


class TestPartialSnapshotGuard:
    @staticmethod
    def _book(etf, date, names, total=1.0):
        w = total / len(names)
        return [(etf, t, f"{t} Corp", w, date, date) for t in names]

    @pytest.fixture(autouse=True)
    def _fresh(self):
        clear_sanitizer_cache(); yield; clear_sanitizer_cache()

    def _san(self):
        return Sanitizer(blocked_tickers=(), blocked_name_patterns=(), blocked_name_exact=(),
                         ticker_replacements=(), ticker_renames={}, partial_snapshot_min_ratio=0.9)

    def _df(self, rows):
        return pd.DataFrame(rows, columns=["ETF_Ticker", "ticker", "name", "weight", "Holdings_As_Of", "Date_Scraped"])

    def test_incomplete_scrape_is_dropped(self):
        names = [f"N{i:02d}" for i in range(20)]
        rows = []
        for d in ["2026-08-24", "2026-08-25", "2026-08-26", "2026-08-27"]:
            rows += self._book("EEMO", d, names)
        rows += self._book("EEMO", "2026-08-29", names[1:15], total=0.72)   # partial: #1 missing
        out = self._san().apply(self._df(rows))
        assert "2026-08-29" not in set(out["Holdings_As_Of"])
        assert len(set(out["Holdings_As_Of"])) == 4

    def test_genuine_reconstitution_is_kept(self):
        names = [f"N{i:02d}" for i in range(20)]
        rows = []
        for d in ["2026-09-17", "2026-09-18", "2026-09-19", "2026-09-20"]:
            rows += self._book("EEMO", d, names)
        rows += self._book("EEMO", "2026-09-21", names[:15], total=1.0)      # fewer names, full weight
        out = self._san().apply(self._df(rows))
        assert "2026-09-21" in set(out["Holdings_As_Of"])

    def test_engine_falls_back_to_last_complete_book(self):
        """With the guard on, a name missing only from the partial scrape stays held."""
        cfg = Config.from_yaml(CONFIG_PATH)
        assert cfg.sanitizer.partial_snapshot_min_ratio == 0.9
        names = ["HYNIX"] + [f"N{i:02d}" for i in range(19)]
        rows = []
        for d in ["2026-08-24", "2026-08-25", "2026-08-26", "2026-08-27"]:
            rows += [("EEMO", "HYNIX", "SK hynix Inc", 0.13, d, d)]
            rows += [("EEMO", t, f"{t} Corp", 0.87 / 19, d, d) for t in names[1:]]
        rows += [("EEMO", t, f"{t} Corp", 0.72 / 19, "2026-08-29", "2026-08-29") for t in names[1:]]
        lb, _ = compute_leaderboard(self._df(rows), cfg)
        assert "HYNIX" in set(lb["ticker"])


class TestScreeningSignals:
    @staticmethod
    def _lb(**over):
        base = dict(ticker="X", etf_count=2, etf_count_delta_30d=0, leaderboard_rank=150, flag="",
                    conviction_etf_count=0, strategy_tier_count=1, avg_rank_delta_7d=0.0, score_streak=0)
        base.update(over)
        return pd.DataFrame([base])

    def _run(self, lb, ago):
        return hist.screening_signals(lb, pd.Series([ago], index=lb.index), Config.from_yaml(CONFIG_PATH)).iloc[0]

    def test_funds_down_needs_a_recent_loss_inside_the_relevant_board(self):
        assert self._run(self._lb(etf_count=3, leaderboard_rank=150), ago=4)["funds_down"]
        assert not self._run(self._lb(etf_count=3, leaderboard_rank=900), ago=4)["funds_down"]
        assert not self._run(self._lb(etf_count=3, leaderboard_rank=150), ago=3)["funds_down"]

    def test_funds_up_uses_the_30_day_change(self):
        assert self._run(self._lb(etf_count_delta_30d=1), ago=2)["funds_up"]
        assert not self._run(self._lb(etf_count_delta_30d=0), ago=1)["funds_up"]

    def test_stealth_is_a_second_fund_joining(self):
        assert self._run(self._lb(etf_count=2, leaderboard_rank=400), ago=1)["stealth_buy"]
        assert not self._run(self._lb(etf_count=2, leaderboard_rank=400), ago=2)["stealth_buy"]   # no join
        assert not self._run(self._lb(etf_count=3, leaderboard_rank=400), ago=2)["stealth_buy"]   # not the 2nd
        assert not self._run(self._lb(etf_count=2, leaderboard_rank=900), ago=1)["stealth_buy"]   # irrelevant

    def test_new_name_counts_its_funds_as_additions(self):
        """A name absent from the board 14 days ago had 0 holders, not 'no change'."""
        row = self._run(self._lb(etf_count=2, leaderboard_rank=300), ago=0)
        assert row["etf_count_delta_14d"] == 2

    def test_pre_hc_is_one_step_from_hc_and_improving(self):
        good = dict(etf_count=3, conviction_etf_count=1, strategy_tier_count=2, avg_rank_delta_7d=3.0, score_streak=2)
        assert self._run(self._lb(**good), ago=3)["pre_hc"]
        assert not self._run(self._lb(**{**good, "flag": "HIGH_CONVICTION"}), ago=3)["pre_hc"]
        assert not self._run(self._lb(**{**good, "conviction_etf_count": 0}), ago=3)["pre_hc"]
        assert not self._run(self._lb(**{**good, "avg_rank_delta_7d": -1.0}), ago=3)["pre_hc"]


def test_speculative_beta_is_retired():
    """2026-10 audit: no edge in either regime (+0.4% / −0.3%) — the flag is gone."""
    cfg = Config.from_yaml(CONFIG_PATH)
    rows = [("SPMO", "TR", "Trend Only", 0.5, "2026-05-01", "2026-05-01"),
            ("SPMO", "Z", "Zed", 0.5, "2026-05-01", "2026-05-01")]
    df = pd.DataFrame(rows, columns=["ETF_Ticker", "ticker", "name", "weight", "Holdings_As_Of", "Date_Scraped"])
    lb, _ = compute_leaderboard(df, cfg)
    assert "SPECULATIVE_BETA" not in set(lb["flag"])
