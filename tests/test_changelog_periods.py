"""Tests for history.changelog() — the per-period Changes tab payload.

Every table is computed in the backend from the full daily leaderboards. The
previous frontend derived new entrants and exits from score_history.json,
which is capped at 600 tickers, so every name outside the cap looked "new".

Run: pytest tests/test_changelog_periods.py -v
"""
from __future__ import annotations

import dataclasses
from pathlib import Path

import pandas as pd
import pytest

from conviction.scoring import Config, compute_leaderboard
from conviction import history as hist

CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"


@pytest.fixture
def cfg() -> Config:
    c = Config.from_yaml(CONFIG_PATH)
    # Widen the lookback so historical_leaderboards spans the full fixture, and
    # lower the maturity gate so the short synthetic fixture still scores > 0
    # (the default 30-day maturity would zero out tickers in a 2-snapshot fixture).
    return dataclasses.replace(
        c,
        etf_maturity_days=1,
        **{"history": dataclasses.replace(c.history, leaderboard_lookback_days=120)},
    )


def _h(rows: list[tuple]) -> pd.DataFrame:
    return pd.DataFrame(
        rows,
        columns=["ETF_Ticker", "ticker", "name", "weight", "Holdings_As_Of", "Date_Scraped"],
    )


def _make_raw() -> pd.DataFrame:
    """
    Two snapshot dates (within the 120d lookback), three tickers across two real
    ETFs (FPX Scout, QMOM Quant).

    DROPPED: held by FPX + QMOM on day 0, then by NEITHER on day 1 (drops out
             of the universe entirely) — a clean universe exit.
    STAYS:   held by FPX on both days — must never appear as an exit.
    NEW:     absent on day 0, held by QMOM on day 1 — a new entrant, not an exit.

    Dates are ~60 days apart so multi-day lookbacks (30/60/90) all resolve the
    day-0 snapshot as the start, exercising the multi-period path.
    """
    # Each fund's book sums to 100% — under the v4 κ = 2 convexity a toy book
    # of two 5% positions (10% invested) scores ≈ 0, which would make DROPPED
    # look unheld at the start date.
    rows = [
        # Day 0 (2026-04-01)
        ("FPX",  "DROPPED", "Dropped Co", 0.45, "2026-04-01", "2026-04-01"),
        ("QMOM", "DROPPED", "Dropped Co", 1.00, "2026-04-01", "2026-04-01"),
        ("FPX",  "STAYS",   "Stays Co",   0.55, "2026-04-01", "2026-04-01"),
        # Day 1 (2026-06-01)
        ("FPX",  "STAYS",   "Stays Co",   1.00, "2026-06-01", "2026-06-01"),
        ("QMOM", "NEW",     "New Co",     1.00, "2026-06-01", "2026-06-01"),
    ]
    return _h(rows)


@pytest.fixture
def raw_fixture() -> pd.DataFrame:
    return _make_raw()


@pytest.fixture
def historical_fixture(raw_fixture, cfg) -> dict:
    return hist.historical_leaderboards(raw_fixture, cfg)


@pytest.fixture
def latest_leaderboard(raw_fixture, cfg) -> pd.DataFrame:
    lb, _ = compute_leaderboard(raw_fixture, cfg)
    return lb


# ── Tests ──────────────────────────────────────────────────────────────────────

PERIODS = [1, 7, 14, 30, 60, 90, "YTD"]


def _tickers(out, period, key):
    return {r["ticker"] for r in out["periods"][str(period)][key]}


class TestChangelogPeriods:
    def test_dropped_ticker_is_an_exit_and_lost_its_funds(self, historical_fixture):
        out = hist.changelog(historical_fixture, [90, 180])
        for p in (90, 180):
            assert "DROPPED" in _tickers(out, p, "exits")
            assert "DROPPED" in _tickers(out, p, "funds_lost")

    def test_staying_ticker_is_neither_new_nor_exit(self, historical_fixture):
        out = hist.changelog(historical_fixture, [90])
        assert "STAYS" not in _tickers(out, 90, "exits")
        assert "STAYS" not in _tickers(out, 90, "new")

    def test_new_ticker_is_new_not_exit(self, historical_fixture):
        out = hist.changelog(historical_fixture, [90])
        assert "NEW" in _tickers(out, 90, "new")
        assert "NEW" not in _tickers(out, 90, "exits")

    def test_record_shape(self, historical_fixture):
        out = hist.changelog(historical_fixture, [90])
        rec = next(r for r in out["periods"]["90"]["exits"] if r["ticker"] == "DROPPED")
        assert rec["company"] == "Dropped Co"
        assert rec["rank_now"] is None and rec["score_now"] is None
        assert rec["funds_then"] == 2 and rec["funds_now"] == 0
        assert rec["score_then"] is not None

    def test_returns_every_requested_period_key(self, historical_fixture):
        out = hist.changelog(historical_fixture, PERIODS)
        for p in PERIODS:
            blk = out["periods"][str(p)]
            assert blk["since"] == "2026-04-01"
            assert set(blk["counts"]) == {"funds_added", "funds_lost", "hc_entered", "hc_exited",
                                          "climbers", "fallers", "new", "exits"}

    def test_handles_empty_historical(self):
        out = hist.changelog({}, [1, 7, "YTD"])
        for p in ("1", "7", "YTD"):
            assert out["periods"][p]["exits"] == []

    def test_top_n_caps_list_length_but_not_counts(self, historical_fixture):
        out = hist.changelog(historical_fixture, [90], top_n=0)
        assert out["periods"]["90"]["exits"] == []
        assert out["periods"]["90"]["counts"]["exits"] >= 1


def _boards(rows_then, rows_now):
    """rows: ticker -> (rank, funds, score, flag)."""
    def lb(rows):
        return pd.DataFrame([{"ticker": t, "leaderboard_rank": r, "etf_count": e, "final_score": s,
                              "flag": f, "company": t + " Inc", "tiers": "Scout + Quant"}
                             for t, (r, e, s, f) in rows.items()])
    return {pd.Timestamp("2026-09-01"): lb(rows_then), pd.Timestamp("2026-09-08"): lb(rows_now)}


class TestChangesSelection:
    def test_deep_board_noise_is_not_a_climber(self):
        """#3,000 → #1,000 is 2,000 positions but irrelevant; #300 → #120 is the move."""
        h = _boards({"DEEP": (3000, 1, 0.1, ""), "REAL": (300, 3, 20, "")},
                    {"DEEP": (1000, 1, 0.3, ""), "REAL": (120, 4, 40, "")})
        out = hist.changelog(h, [7])
        assert _tickers(out, 7, "climbers") == {"REAL"}

    def test_top_of_board_shuffle_needs_a_real_move(self):
        """#2 → #1 is a ×2 rank ratio but one position — not a climber."""
        h = _boards({"A": (2, 9, 900, ""), "B": (1, 9, 950, "")},
                    {"A": (1, 9, 960, ""), "B": (2, 9, 940, "")})
        out = hist.changelog(h, [7])
        assert _tickers(out, 7, "climbers") == set()
        assert _tickers(out, 7, "fallers") == set()

    def test_faller_must_have_been_relevant(self):
        h = _boards({"WAS": (50, 6, 300, ""), "NEVER": (900, 2, 5, "")},
                    {"WAS": (200, 5, 90, ""), "NEVER": (2500, 2, 1, "")})
        out = hist.changelog(h, [7])
        assert _tickers(out, 7, "fallers") == {"WAS"}

    def test_funds_tables_order_by_size_then_rank(self):
        h = _boards({"A": (100, 3, 50, ""), "B": (40, 3, 80, ""), "C": (60, 3, 60, "")},
                    {"A": (90, 5, 60, ""), "B": (35, 4, 85, ""), "C": (55, 4, 66, "")})
        out = hist.changelog(h, [7])
        assert [r["ticker"] for r in out["periods"]["7"]["funds_added"]] == ["A", "B", "C"]

    def test_hc_transitions(self):
        h = _boards({"IN": (30, 4, 100, ""), "OUT": (10, 5, 200, "HIGH_CONVICTION")},
                    {"IN": (12, 5, 190, "HIGH_CONVICTION"), "OUT": (25, 3, 120, "")})
        out = hist.changelog(h, [7])
        assert _tickers(out, 7, "hc_entered") == {"IN"}
        assert _tickers(out, 7, "hc_exited") == {"OUT"}
