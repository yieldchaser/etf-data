"""Regression tests for markets/stats.py math.

Pins two real defects found in the audit:
  1. series_stats week/month windows: a calendar window holding only NaN
     values raised IndexError, which markets/build.py's per-series catch-all
     then converted into silently dropping the whole series from markets.json.
  2. ±inf leaking through pct_change() when a prior close is 0, poisoning
     streak / last-20 counters in both series_stats and log_rows.

Also pins the core change/percentile conventions so future edits can't shift
them unnoticed (weekly = 7 calendar days back; monthly = 30; YTD baseline =
first close of current year; percentile = fraction of history <= close).
"""
from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

from markets.stats import log_rows, series_stats


def _daily_df(closes: list[float], end: str | None = None) -> pd.DataFrame:
    n = len(closes)
    end_ts = pd.Timestamp(end) if end else pd.Timestamp.today().normalize()
    dates = pd.date_range(end=end_ts, periods=n, freq="D")
    return pd.DataFrame({"Date": dates, "Close": closes})


class TestWindowGuards:
    def test_all_nan_older_window_returns_none_not_crash(self):
        """Rows exist before today-7d but every Close there is NaN — used to
        raise IndexError and (via build.py's try/except) drop the series."""
        closes = [float("nan")] * 20 + [100.0, 101.0, 102.0]
        df = _daily_df(closes)
        st = series_stats(df)
        assert st["week_pct"] is None
        assert st["month_pct"] is None
        assert st["close_today"] == 102.0

    def test_short_series_returns_empty_dict(self):
        assert series_stats(pd.DataFrame({"Date": [], "Close": []})) == {}
        assert series_stats(None) == {}

    def test_week_and_month_windows_use_calendar_days(self):
        # flat history, one step up on the last day
        closes = [100.0] * 40 + [110.0]
        df = _daily_df(closes)
        st = series_stats(df)
        assert st["week_pct"] == pytest.approx(0.10)
        # 41 rows: the day 30 calendar-days back exists and holds 100
        assert st["month_pct"] == pytest.approx(0.10)


class TestReturnSanitisation:
    def test_zero_prior_close_does_not_poison_counters(self):
        """closes [100, 0, 110]: the legit -100% (100→0) survives, but the
        ±inf from 0→110 is neutralised instead of poisoning the counters."""
        df = _daily_df([100.0, 0.0, 110.0])
        st = series_stats(df)
        assert st["day_pct"] is None  # base==0 guard
        assert st["last_20_up"] == 0
        assert st["last_20_down"] == 1
        assert st["current_streak"] == -1

    def test_streak_counts_consecutive_direction(self):
        closes = [100, 101, 102, 101, 100.5, 101.5]
        st = series_stats(_daily_df(closes))
        # last move up after two down-moves → streak restarts at +1
        assert st["current_streak"] == 1

    def test_percentile_bounds(self):
        st = series_stats(_daily_df([10.0, 20.0, 30.0]))
        assert 0 < st["percentile_all_time"] <= 1.0
        assert st["percentile_all_time"] == pytest.approx(1.0)  # close_today is max


class TestLogRows:
    def test_basic_shape_and_finite_outputs(self):
        closes = [100.0 * (1 + 0.01 * i) for i in range(25)]
        rows = log_rows(_daily_df(closes), n=200)
        assert len(rows) == 25
        last = rows[-1]
        assert set(last) == {"d", "close", "pct", "streak", "level"}
        assert all(math.isfinite(r["pct"]) for r in rows[1:])
        assert all(0 < r["level"] <= 1 for r in rows)

    def test_zero_close_inf_neutralised(self):
        rows = log_rows(_daily_df([100.0, 0.0, 110.0]))
        pcts = [r["pct"] for r in rows]
        assert all(p is None or math.isfinite(p) for p in pcts)

    def test_n_cap_returns_most_recent_rows(self):
        closes = [100.0 + i for i in range(50)]
        rows = log_rows(_daily_df(closes), n=5)
        assert len(rows) == 5
        assert rows[-1]["close"] == 149.0
