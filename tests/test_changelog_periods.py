"""Tests for compute_universe_exits_by_period() in predator/history.py.

This function powers the multi-day "Dropped from Universe" table in the
Changes tab. The previous frontend path scanned score_history.json, which is
capped at 600 tickers (all current names) — so exited names were truncated
and never rendered for 7d/30d/90d/YTD. These tests lock in the uncapped
backend computation.

Run: pytest tests/test_changelog_periods.py -v
"""
from __future__ import annotations

import dataclasses
from pathlib import Path

import pandas as pd
import pytest

from predator.scoring import Config, compute_leaderboard
from predator import history as hist

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
    rows = [
        # Day 0 (2026-04-01)
        ("FPX",  "DROPPED", "Dropped Co", 0.05, "2026-04-01", "2026-04-01"),
        ("QMOM", "DROPPED", "Dropped Co", 0.04, "2026-04-01", "2026-04-01"),
        ("FPX",  "STAYS",   "Stays Co",   0.06, "2026-04-01", "2026-04-01"),
        # Day 1 (2026-06-01)
        ("FPX",  "STAYS",   "Stays Co",   0.06, "2026-06-01", "2026-06-01"),
        ("QMOM", "NEW",     "New Co",     0.04, "2026-06-01", "2026-06-01"),
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


class TestUniverseExitsByPeriod:
    def test_dropped_ticker_shows_for_long_lookback(self, historical_fixture, latest_leaderboard):
        """A name that fully exits the universe must appear as an exit for any
        period whose start snapshot precedes the exit."""
        result = hist.compute_universe_exits_by_period(
            historical_fixture, latest_leaderboard, periods=[90, 180], top_n=50,
        )
        # Both periods resolve start = day 0 (the only prior snapshot), so
        # DROPPED must appear in both.
        for p in ("90", "180"):
            tickers = {r["ticker"] for r in result[p]}
            assert "DROPPED" in tickers, f"DROPPED missing from {p}d exits: {tickers}"

    def test_staying_ticker_never_an_exit(self, historical_fixture, latest_leaderboard):
        """A name still held today must never be reported as a universe exit."""
        result = hist.compute_universe_exits_by_period(
            historical_fixture, latest_leaderboard, periods=[90], top_n=50,
        )
        tickers = {r["ticker"] for r in result["90"]}
        assert "STAYS" not in tickers, f"STAYS should not be an exit: {tickers}"

    def test_new_ticker_never_an_exit(self, historical_fixture, latest_leaderboard):
        """A name that wasn't held at the start date but is held now is a new
        entrant, NOT an exit — must be absent."""
        result = hist.compute_universe_exits_by_period(
            historical_fixture, latest_leaderboard, periods=[90], top_n=50,
        )
        tickers = {r["ticker"] for r in result["90"]}
        assert "NEW" not in tickers, f"NEW should not be an exit: {tickers}"

    def test_exit_record_shape(self, historical_fixture, latest_leaderboard):
        """Each exit record carries the prior score so the UI 'Prev. Score'
        column reflects what the ticker had before dropping out."""
        result = hist.compute_universe_exits_by_period(
            historical_fixture, latest_leaderboard, periods=[90], top_n=50,
        )
        rec = next(r for r in result["90"] if r["ticker"] == "DROPPED")
        assert rec["company"] == "Dropped Co"
        assert rec["final_score"] == 0.0          # gone today
        assert rec["score_delta"] is not None and rec["score_delta"] < 0
        assert rec["score_delta_pct"] == -1.0     # convention: full exit = -100%

    def test_returns_every_requested_period_key(self, historical_fixture, latest_leaderboard):
        """Output dict must contain a key for every requested period, even if
        empty — the frontend reads by String(changesPeriod) and would crash
        on an undefined access otherwise."""
        periods = [1, 7, 14, 30, 60, 90, "YTD"]
        result = hist.compute_universe_exits_by_period(
            historical_fixture, latest_leaderboard, periods=periods, top_n=50,
        )
        for p in periods:
            assert str(p) in result, f"missing period key {p!r}"

    def test_ytd_uses_january_first_as_start(self, historical_fixture, latest_leaderboard):
        """YTD start = Jan 1 of today's year. With day-0 = 2026-01-01, the
        YTD start snapshot is day 0, so DROPPED must appear."""
        result = hist.compute_universe_exits_by_period(
            historical_fixture, latest_leaderboard, periods=["YTD"], top_n=50,
        )
        tickers = {r["ticker"] for r in result["YTD"]}
        assert "DROPPED" in tickers, f"YTD should include DROPPED: {tickers}"

    def test_short_lookback_excludes_early_exit(self, historical_fixture, latest_leaderboard):
        """A 1-day lookback's start snapshot is the immediately-prior date (day 0),
        but on day 0 DROPPED was still held — so for the 1-day window the *start*
        snapshot still includes DROPPED and it counts as an exit. This is correct
        behaviour; the test pins it so a regression is caught. (The 1-day case is
        also produced by changelog()'s universe_exits; both must agree.)"""
        result = hist.compute_universe_exits_by_period(
            historical_fixture, latest_leaderboard, periods=[1], top_n=50,
        )
        tickers = {r["ticker"] for r in result["1"]}
        assert "DROPPED" in tickers, f"1d exit should include DROPPED: {tickers}"

    def test_handles_empty_historical(self, latest_leaderboard):
        """No snapshots → return empty lists for every period, never raise."""
        result = hist.compute_universe_exits_by_period(
            {}, latest_leaderboard, periods=[1, 7, "YTD"], top_n=50,
        )
        for p in ("1", "7", "YTD"):
            assert result[p] == []

    def test_top_n_caps_list_length(self, historical_fixture, latest_leaderboard):
        """top_n bounds the returned list length."""
        result = hist.compute_universe_exits_by_period(
            historical_fixture, latest_leaderboard, periods=[90], top_n=1,
        )
        assert len(result["90"]) <= 1
