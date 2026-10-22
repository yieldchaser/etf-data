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

    def test_collapse_with_fund_exit_is_crater(self):
        ranks = [100, 100, 105, 110, 120, 135, 150, 165, 180, 195, 210, 220, 230, 235]
        funds = [5] * 6 + [4] * 8
        s = {"DOWN": list(zip(ranks, funds))}
        assert _flags(s, "crater") == {"DOWN"}
        assert _flags(s, "burst") == set()

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
