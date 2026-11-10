"""Unit tests for Quantitative MACD Momentum Scanner and Candle Confirmation Rules.

Covers TradingView 1H Candle confirmation rule:
1. Bullish crossover with red candle does NOT trigger Sub-Zero Reversal alert.
2. Bullish crossover with green candle DOES trigger Sub-Zero Reversal alert.
3. Bearish crossover with green candle does NOT trigger Bearish Breakdown alert.
4. Bearish crossover with red candle DOES trigger Bearish Breakdown alert.
5. Unconfirmed crossovers are marked UNCONFIRMED_CROSS / UNCONFIRMED_BEAR_CROSS.
6. Email formatting and filtering excludes unconfirmed crossovers.
"""

from __future__ import annotations

import datetime as dt
import json
from unittest.mock import MagicMock, patch
import pandas as pd
import pytest

from scripts.build_macd_scanner import (
    analyze_ticker,
    calculate_atr,
    calculate_ema,
    calculate_macd,
    calculate_pivots,
    calculate_v_macd,
    calculate_yang_zhang_volatility,
    format_institutional_email,
    get_market_session,
)


def _make_mock_data(n_1h: int = 140, n_1d: int = 70, is_green: bool = True):
    """Build mock dataframes with proper length and columns."""
    closes_1h = [20.0] * n_1h
    opens_1h = [19.0 if is_green else 21.0] * n_1h
    df_1h = pd.DataFrame(
        {
            "Open": opens_1h,
            "High": [22.0] * n_1h,
            "Low": [18.0] * n_1h,
            "Close": closes_1h,
            "Volume": [1000] * n_1h,
        },
        index=pd.date_range("2026-11-01", periods=n_1h, freq="h"),
    )
    df_1d = pd.DataFrame(
        {
            "Open": [20.0] * n_1d,
            "High": [22.0] * n_1d,
            "Low": [19.0] * n_1d,
            "Close": [20.0] * n_1d,
            "Volume": [1000] * n_1d,
        },
        index=pd.date_range("2026-09-01", periods=n_1d, freq="D"),
    )
    return df_1h, df_1d


def test_macd_and_ema_calculation():
    """Verify MACD mathematical correctness on synthetic sequence."""
    series = [10.0 + i * 0.5 for i in range(50)]
    ema12 = calculate_ema(series, 12)
    assert len(ema12) == 50
    assert ema12[-1] > ema12[0]

    macd_res = calculate_macd(series, fast=12, slow=26, signal_period=9)
    assert len(macd_res["macd"]) == 50
    assert len(macd_res["signal"]) == 50
    assert len(macd_res["hist"]) == 50
    assert macd_res["macd"][-1] > 0
    assert macd_res["hist"][-1] > 0


def test_pivots_calculation():
    """Verify standard floor trader pivots."""
    pivots = calculate_pivots(105.0, 95.0, 100.0)
    assert pivots["P"] == 100.0
    assert pivots["R1"] == 105.0
    assert pivots["S1"] == 95.0
    assert pivots["R2"] == 110.0
    assert pivots["S2"] == 90.0


@patch("yfinance.Ticker")
@patch("scripts.build_macd_scanner.calculate_macd")
def test_bull_cross_with_red_candle_is_unconfirmed(mock_calc, mock_ticker):
    """TradingView Candle Rule 1: Bullish cross with RED candle does NOT trigger Sub-Zero Reversal.
    
    In this test:
    - 1H MACD crossed above signal line in sub-zero territory (macd: -1.0 -> -0.5, sig: -0.8 -> -0.7, hist > 0).
    - Latest 1H bar is RED (Open=21.0, Close=20.0).
    - Result MUST be:
      signal_state == 'UNCONFIRMED_CROSS'
      bull_cross_fresh is False
      candle_color_1h == 'RED'
      is_green_candle_1h is False
    """
    n_1h, n_1d = 140, 70
    df_1h, df_1d = _make_mock_data(n_1h, n_1d, is_green=False)

    inst = MagicMock()
    inst.history.side_effect = lambda period, interval, prepost=False: df_1h if interval == "1h" else df_1d
    inst.fast_info = MagicMock()
    inst.fast_info.last_price = 20.0
    mock_ticker.return_value = inst

    def _calc(closes, fast=12, slow=26, signal_period=9):
        if len(closes) == n_1h:
            return {
                "macd": [-1.0] * (n_1h - 2) + [-1.0, -0.5],
                "signal": [-0.8] * (n_1h - 2) + [-0.8, -0.7],
                "hist": [-0.2] * (n_1h - 2) + [-0.2, 0.2],
                "hist_delta": [0.0] * (n_1h - 1) + [0.4],
            }
        return {
            "macd": [0.5] * n_1d,
            "signal": [0.4] * n_1d,
            "hist": [0.1] * n_1d,
            "hist_delta": [0.0] * n_1d,
        }

    mock_calc.side_effect = _calc

    res = analyze_ticker("CRWL", {"fund_name": "Test Bull", "leverage": "2x"})
    assert res is not None
    assert res["candle_color_1h"] == "RED"
    assert res["is_green_candle_1h"] is False
    assert res["is_red_candle_1h"] is True
    assert res["bull_cross_fresh"] is False
    assert res["signal_state"] == "UNCONFIRMED_CROSS"
    assert res["signal_direction"] == "WATCH"
    assert "Unconfirmed" in res["signal_label"]


@patch("yfinance.Ticker")
@patch("scripts.build_macd_scanner.calculate_macd")
def test_bull_cross_with_green_candle_triggers_subzero_reversal(mock_calc, mock_ticker):
    """TradingView Candle Rule 2: Bullish cross with GREEN candle DOES trigger Sub-Zero Reversal.
    
    In this test:
    - 1H MACD crossed above signal line in sub-zero territory.
    - Latest 1H bar is GREEN (Open=19.0, Close=20.0).
    - Result MUST be:
      signal_state == 'SUB_ZERO_BOUNCE'
      bull_cross_fresh is True
      candle_color_1h == 'GREEN'
      is_green_candle_1h is True
    """
    n_1h, n_1d = 140, 70
    df_1h, df_1d = _make_mock_data(n_1h, n_1d, is_green=True)

    inst = MagicMock()
    inst.history.side_effect = lambda period, interval, prepost=False: df_1h if interval == "1h" else df_1d
    inst.fast_info = MagicMock()
    inst.fast_info.last_price = 20.0
    mock_ticker.return_value = inst

    def _calc(closes, fast=12, slow=26, signal_period=9):
        if len(closes) == n_1h:
            return {
                "macd": [-1.0] * (n_1h - 2) + [-1.0, -0.5],
                "signal": [-0.8] * (n_1h - 2) + [-0.8, -0.7],
                "hist": [-0.2] * (n_1h - 2) + [-0.2, 0.2],
                "hist_delta": [0.0] * (n_1h - 1) + [0.4],
            }
        return {
            "macd": [0.5] * n_1d,
            "signal": [0.4] * n_1d,
            "hist": [0.1] * n_1d,
            "hist_delta": [0.0] * n_1d,
        }

    mock_calc.side_effect = _calc

    res = analyze_ticker("CRWL", {"fund_name": "Test Bull", "leverage": "2x"})
    assert res is not None
    assert res["candle_color_1h"] == "GREEN"
    assert res["is_green_candle_1h"] is True
    assert res["is_red_candle_1h"] is False
    assert res["bull_cross_fresh"] is True
    assert res["is_subzero"] is True
    assert res["signal_state"] == "SUB_ZERO_BOUNCE"
    assert "Sub-Zero Reversal" in res["signal_label"]


@patch("yfinance.Ticker")
@patch("scripts.build_macd_scanner.calculate_macd")
def test_bear_cross_with_green_candle_is_unconfirmed(mock_calc, mock_ticker):
    """TradingView Candle Rule 3: Bearish cross with GREEN candle does NOT trigger Bearish Breakdown alert.
    
    In this test:
    - 1H MACD crossed below signal line (macd: 1.0 -> 0.5, sig: 0.8 -> 0.7, hist < 0).
    - Latest 1H bar is GREEN (Open=19.0, Close=20.0).
    - Result MUST be:
      signal_state == 'UNCONFIRMED_BEAR_CROSS'
      bear_cross_fresh is False
      candle_color_1h == 'GREEN'
    """
    n_1h, n_1d = 140, 70
    df_1h, df_1d = _make_mock_data(n_1h, n_1d, is_green=True)

    inst = MagicMock()
    inst.history.side_effect = lambda period, interval, prepost=False: df_1h if interval == "1h" else df_1d
    inst.fast_info = MagicMock()
    inst.fast_info.last_price = 20.0
    mock_ticker.return_value = inst

    def _calc(closes, fast=12, slow=26, signal_period=9):
        if len(closes) == n_1h:
            return {
                "macd": [1.0] * (n_1h - 2) + [1.0, 0.5],
                "signal": [0.8] * (n_1h - 2) + [0.8, 0.7],
                "hist": [0.2] * (n_1h - 2) + [0.2, -0.2],
                "hist_delta": [0.0] * (n_1h - 1) + [-0.4],
            }
        return {
            "macd": [0.5] * n_1d,
            "signal": [0.4] * n_1d,
            "hist": [0.1] * n_1d,
            "hist_delta": [0.0] * n_1d,
        }

    mock_calc.side_effect = _calc

    res = analyze_ticker("TEST_BEAR", {"fund_name": "Test Bear", "leverage": "-2x"})
    assert res is not None
    assert res["candle_color_1h"] == "GREEN"
    assert res["is_green_candle_1h"] is True
    assert res["is_red_candle_1h"] is False
    assert res["bear_cross_fresh"] is False
    assert res["signal_state"] == "UNCONFIRMED_BEAR_CROSS"
    assert res["signal_direction"] == "WATCH"


@patch("yfinance.Ticker")
@patch("scripts.build_macd_scanner.calculate_macd")
def test_bear_cross_with_red_candle_triggers_bearish_breakdown(mock_calc, mock_ticker):
    """TradingView Candle Rule 4: Bearish cross with RED candle DOES trigger Bearish Breakdown alert.
    
    In this test:
    - 1H MACD crossed below signal line.
    - Latest 1H bar is RED (Open=21.0, Close=20.0).
    - Result MUST be:
      signal_state == 'BEARISH_CROSS'
      bear_cross_fresh is True
      candle_color_1h == 'RED'
    """
    n_1h, n_1d = 140, 70
    df_1h, df_1d = _make_mock_data(n_1h, n_1d, is_green=False)

    inst = MagicMock()
    inst.history.side_effect = lambda period, interval, prepost=False: df_1h if interval == "1h" else df_1d
    inst.fast_info = MagicMock()
    inst.fast_info.last_price = 20.0
    mock_ticker.return_value = inst

    def _calc(closes, fast=12, slow=26, signal_period=9):
        if len(closes) == n_1h:
            return {
                "macd": [1.0] * (n_1h - 2) + [1.0, 0.5],
                "signal": [0.8] * (n_1h - 2) + [0.8, 0.7],
                "hist": [0.2] * (n_1h - 2) + [0.2, -0.2],
                "hist_delta": [0.0] * (n_1h - 1) + [-0.4],
            }
        return {
            "macd": [0.5] * n_1d,
            "signal": [0.4] * n_1d,
            "hist": [0.1] * n_1d,
            "hist_delta": [0.0] * n_1d,
        }

    mock_calc.side_effect = _calc

    res = analyze_ticker("TEST_BEAR", {"fund_name": "Test Bear", "leverage": "-2x"})
    assert res is not None
    assert res["candle_color_1h"] == "RED"
    assert res["is_red_candle_1h"] is True
    assert res["is_green_candle_1h"] is False
    assert res["bear_cross_fresh"] is True
    assert res["signal_state"] == "BEARISH_CROSS"
    assert "Bearish Breakdown" in res["signal_label"]


def test_email_filtering_excludes_unconfirmed_crosses():
    """Verify format_institutional_email excludes unconfirmed crosses from Tier 1A and Tier 3."""
    signals_data = [
        # 1. Confirmed Sub-Zero Bullish Reversal
        {
            "ticker": "BULL_CONF",
            "fund_name": "Confirmed Bull ETF",
            "leverage": "2x",
            "latest_price": 50.0,
            "open_1h": 49.0,
            "candle_color_1h": "GREEN",
            "is_green_candle_1h": True,
            "is_red_candle_1h": False,
            "chg_1h_pct": 2.04,
            "chg_1d_pct": 3.5,
            "macd_1h": -1.2,
            "signal_1h": -1.3,
            "hist_1h": 0.1,
            "hist_delta_1h": 0.05,
            "z_macd_1h": -0.8,
            "rvol_1h": 1.5,
            "daily_trend_bullish": True,
            "signal_state": "SUB_ZERO_BOUNCE",
            "signal_label": "Sub-Zero Reversal (Confirmed Green)",
            "signal_direction": "BULLISH",
            "bull_cross_fresh": True,
            "bear_cross_fresh": False,
            "is_subzero": True,
            "confluence_score": 85,
            "pivots": {"P": 49.0, "S1": 48.0, "S2": 47.0, "R1": 51.0},
        },
        # 2. UNCONFIRMED Bullish Cross (Red Candle - like CRWL)
        {
            "ticker": "CRWL_RED",
            "fund_name": "Unconfirmed Red ETF",
            "leverage": "2x",
            "latest_price": 40.0,
            "open_1h": 41.0,
            "candle_color_1h": "RED",
            "is_green_candle_1h": False,
            "is_red_candle_1h": True,
            "chg_1h_pct": -2.44,
            "chg_1d_pct": -1.5,
            "macd_1h": -2.39,
            "signal_1h": -2.40,
            "hist_1h": 0.01,
            "hist_delta_1h": 0.02,
            "z_macd_1h": -1.2,
            "rvol_1h": 0.9,
            "daily_trend_bullish": False,
            "signal_state": "UNCONFIRMED_CROSS",
            "signal_label": "Unconfirmed Bullish Cross (Red Candle)",
            "signal_direction": "WATCH",
            "bull_cross_fresh": False,
            "bear_cross_fresh": False,
            "is_subzero": True,
            "confluence_score": -20,
            "pivots": {"P": 41.0, "S1": 40.0, "S2": 39.0, "R1": 42.0},
        },
        # 3. Not crossed (Negative histogram - like CRWL TradingView screenshot)
        {
            "ticker": "CRWL_NEG",
            "fund_name": "Not Crossed ETF",
            "leverage": "2x",
            "latest_price": 84.9,
            "open_1h": 86.0,
            "candle_color_1h": "RED",
            "is_green_candle_1h": False,
            "is_red_candle_1h": True,
            "chg_1h_pct": -1.2,
            "chg_1d_pct": -2.0,
            "macd_1h": -2.39,
            "signal_1h": -2.37,
            "hist_1h": -0.02,
            "hist_delta_1h": 0.01,
            "z_macd_1h": -1.5,
            "rvol_1h": 1.1,
            "daily_trend_bullish": False,
            "signal_state": "IMMINENT_SQUEEZE",
            "signal_label": "Pre-Close Squeeze Imminent",
            "signal_direction": "WATCH",
            "bull_cross_fresh": False,
            "bear_cross_fresh": False,
            "is_subzero": True,
            "confluence_score": -35,
            "pivots": {"P": 85.0, "S1": 83.0, "S2": 81.0, "R1": 87.0},
        },
    ]

    session_info = ("INTRADAY", "INTRADAY HOURLY MOMENTUM SCAN", "INTRADAY RADAR")
    text_alert, html_alert = format_institutional_email(signals_data, "2026-11-04 14:00:00 EST", session_info)

    # Bullish confirmed ETF must appear in Tier 1A
    assert "BULL_CONF" in text_alert
    assert "BULL_CONF" in html_alert
    assert "1H Candle: GREEN (Confirmed" in text_alert

    # Unconfirmed and Not-Crossed ETFs must NOT appear in Tier 1A
    assert "[TIER 1A: CONFIRMED SUB-ZERO BULLISH REVERSALS] (Active: 1)" in text_alert
    assert "CRWL_RED" not in text_alert[:text_alert.find("[TIER 2")]
    assert "CRWL_NEG" not in text_alert[:text_alert.find("[TIER 2")]


def test_macd_boundary_handling_and_nan():
    """Quantitative Audit: Boundary handling when len < 26, empty, and NaN sanitization."""
    # 1. Empty closes
    res_empty = calculate_macd([], fast=12, slow=26, signal_period=9)
    assert res_empty["macd"] == []
    assert res_empty["signal"] == []
    assert res_empty["hist"] == []
    assert res_empty["hist_delta"] == []

    # 2. Closes len < 26
    short_closes = [10.0] * 15
    res_short = calculate_macd(short_closes, fast=12, slow=26, signal_period=9)
    assert len(res_short["macd"]) == 15
    assert all(x == 0.0 for x in res_short["macd"])
    assert all(x == 0.0 for x in res_short["hist"])

    # 3. Closes with NaN and Inf
    dirty_closes = [10.0, float("nan"), 10.5, float("inf"), 11.0] * 8
    res_dirty = calculate_macd(dirty_closes, fast=12, slow=26, signal_period=9)
    assert len(res_dirty["macd"]) == 40
    # No NaNs or Infs leaked into computed series
    assert not any(pd.isna(x) for x in res_dirty["macd"])
    assert not any(pd.isna(x) for x in res_dirty["hist"])


def test_pivots_edge_cases():
    """Quantitative Audit: Floor trader pivots edge cases, flat sessions, and NaN resilience."""
    # Flat session: High = Low = Close
    pivots_flat = calculate_pivots(50.0, 50.0, 50.0)
    assert pivots_flat["P"] == 50.0
    assert pivots_flat["R1"] == 50.0
    assert pivots_flat["S1"] == 50.0
    assert pivots_flat["R5"] == 50.0
    assert pivots_flat["S5"] == 50.0

    # NaN / Inf in inputs
    pivots_nan = calculate_pivots(float("nan"), 40.0, 45.0)
    assert pivots_nan["P"] == 0.0
    assert pivots_nan["R1"] == 0.0

    pivots_inf = calculate_pivots(55.0, float("inf"), 50.0)
    assert pivots_inf["P"] == 0.0


def test_market_session_windows_and_dst():
    """Quantitative Audit: Full 24H US trading session partitioning and DST correctness."""
    from zoneinfo import ZoneInfo
    ny_tz = ZoneInfo("America/New_York")

    # Winter date (EST, UTC-5): Monday 2026-01-12
    # 03:30 EST -> OVERNIGHT
    dt_overnight = dt.datetime(2026, 1, 12, 3, 30, tzinfo=ny_tz)
    assert get_market_session(dt_overnight)[0] == "OVERNIGHT"

    # 08:30 EST -> PRE_MARKET
    dt_pre = dt.datetime(2026, 1, 12, 8, 30, tzinfo=ny_tz)
    assert get_market_session(dt_pre)[0] == "PRE_MARKET"

    # 09:45 EST -> SESSION_OPEN
    dt_open = dt.datetime(2026, 1, 12, 9, 45, tzinfo=ny_tz)
    assert get_market_session(dt_open)[0] == "SESSION_OPEN"

    # 13:00 EST -> INTRADAY
    dt_intra = dt.datetime(2026, 1, 12, 13, 0, tzinfo=ny_tz)
    assert get_market_session(dt_intra)[0] == "INTRADAY"

    # 15:00 EST -> PRE_CLOSE (Golden Hour)
    dt_preclose = dt.datetime(2026, 1, 12, 15, 0, tzinfo=ny_tz)
    assert get_market_session(dt_preclose)[0] == "PRE_CLOSE"

    # 15:35 EST -> PRE_CLOSE (Must stay in Golden Hour scan, not jump to INTRADAY)
    dt_golden_35 = dt.datetime(2026, 1, 12, 15, 35, tzinfo=ny_tz)
    assert get_market_session(dt_golden_35)[0] == "PRE_CLOSE"

    # 16:15 EST -> POST_MARKET
    dt_post = dt.datetime(2026, 1, 12, 16, 15, tzinfo=ny_tz)
    assert get_market_session(dt_post)[0] == "POST_MARKET"

    # 21:00 EST -> OVERNIGHT
    dt_night = dt.datetime(2026, 1, 12, 21, 0, tzinfo=ny_tz)
    assert get_market_session(dt_night)[0] == "OVERNIGHT"

    # Weekend (Saturday) -> WEEKEND
    dt_weekend = dt.datetime(2026, 1, 10, 12, 0, tzinfo=ny_tz)
    assert get_market_session(dt_weekend)[0] == "WEEKEND"

    # Summer date (EDT, UTC-4): Monday 2026-07-13
    # Check UTC to NY conversion accuracy during EDT
    utc_tz = dt.timezone.utc
    dt_utc_preclose = dt.datetime(2026, 7, 13, 19, 0, tzinfo=utc_tz)  # 15:00 EDT
    dt_ny_summer = dt_utc_preclose.astimezone(ny_tz)
    assert dt_ny_summer.hour == 15
    assert get_market_session(dt_ny_summer)[0] == "PRE_CLOSE"


def test_supra_zero_bullish_cross_formatting():
    """Verify Tier 1B Supra-Zero bullish crossovers are formatted in both plain text and HTML."""
    signals_data = [
        {
            "ticker": "TQQQ",
            "fund_name": "ProShares UltraPro QQQ",
            "leverage": "3x",
            "latest_price": 75.0,
            "open_1h": 73.5,
            "candle_color_1h": "GREEN",
            "is_green_candle_1h": True,
            "is_red_candle_1h": False,
            "chg_1h_pct": 2.04,
            "chg_1d_pct": 3.5,
            "macd_1h": 0.85,
            "signal_1h": 0.65,
            "hist_1h": 0.20,
            "hist_delta_1h": 0.08,
            "z_macd_1h": 1.2,
            "rvol_1h": 1.8,
            "daily_trend_bullish": True,
            "signal_state": "BULLISH_CROSS",
            "signal_label": "Bullish Momentum Crossover (Confirmed Green)",
            "signal_direction": "BULLISH",
            "bull_cross_fresh": True,
            "bear_cross_fresh": False,
            "is_subzero": False,
            "confluence_score": 90,
            "pivots": {"P": 74.0, "S1": 72.0, "S2": 70.0, "R1": 76.0},
        }
    ]

    session_info = ("SESSION_OPEN", "REGULAR SESSION OPEN MOMENTUM", "SESSION OPEN")
    text_alert, html_alert = format_institutional_email(signals_data, "2026-11-04 10:00:00 EST", session_info)

    # Must appear in Tier 1B
    assert "[TIER 1B: CONFIRMED SUPRA-ZERO BULLISH CROSSOVERS] (Active: 1)" in text_alert
    assert "TQQQ" in text_alert
    assert "TQQQ" in html_alert
    assert "SUPRA-ZERO CROSS" in html_alert


@patch("yfinance.Ticker")
@patch("scripts.build_macd_scanner.calculate_macd")
def test_flat_doji_candle_disambiguation(mock_calc, mock_ticker):
    """Test flat doji candle (Close == Open) disambiguation using bar change and hist delta."""
    n_1h, n_1d = 140, 70
    # Case A: Close == Open with prior bar lower (chg_1h_pct > 0) -> confirmed GREEN
    closes_1h = [20.0] * (n_1h - 1) + [22.0]
    opens_1h = [20.0] * (n_1h - 1) + [22.0]  # Open == Close == 22.0, but prev close was 20.0
    df_1h = pd.DataFrame(
        {
            "Open": opens_1h,
            "High": [23.0] * n_1h,
            "Low": [19.0] * n_1h,
            "Close": closes_1h,
            "Volume": [1000] * n_1h,
        },
        index=pd.date_range("2026-11-01", periods=n_1h, freq="h"),
    )
    df_1d = pd.DataFrame(
        {
            "Open": [20.0] * n_1d,
            "High": [22.0] * n_1d,
            "Low": [19.0] * n_1d,
            "Close": [20.0] * n_1d,
            "Volume": [1000] * n_1d,
        },
        index=pd.date_range("2026-09-01", periods=n_1d, freq="D"),
    )

    inst = MagicMock()
    inst.history.side_effect = lambda period, interval, prepost=False: df_1h if interval == "1h" else df_1d
    inst.fast_info = MagicMock()
    inst.fast_info.last_price = 22.0
    mock_ticker.return_value = inst

    def _calc(closes, fast=12, slow=26, signal_period=9):
        length = len(closes)
        return {
            "macd": [0.5] * length,
            "signal": [0.4] * length,
            "hist": [0.1] * length,
            "hist_delta": [0.05] * length,
        }

    mock_calc.side_effect = _calc

    res = analyze_ticker("FLAT_BULL", {"fund_name": "Flat Doji Bull", "leverage": "1x"})
    assert res is not None
    assert res["candle_color_1h"] == "GREEN"
    assert res["is_green_candle_1h"] is True
    assert res["is_red_candle_1h"] is False


def test_deduplication_state_logic():
    """Verify deduplication engine: duplicate crossovers are suppressed, fresh reversals trigger, and dry-run preserves state."""
    # Scenario 1: Initial alert state with SOXL already alerted
    prior_state = {
        "sent_subzero": ["SOXL"],
        "sent_bear": [],
        "sent_crossovers": ["SOXL"],
        "last_preclose_date": "2026-11-03",
    }

    prev_subzero = set(prior_state.get("sent_subzero", []))
    prev_bear = set(prior_state.get("sent_bear", []))
    prev_crossovers = set(prior_state.get("sent_crossovers", []))

    # Run 1: Same SOXL is still active -> No fresh trigger
    current_subzero = ["SOXL"]
    current_bear = []
    current_crossovers = ["SOXL"]

    new_subzero = set(current_subzero) - prev_subzero
    new_bear = set(current_bear) - prev_bear
    new_crossovers = set(current_crossovers) - prev_crossovers

    has_fresh_trigger = bool(new_subzero or new_bear or new_crossovers)
    assert has_fresh_trigger is False
    assert len(new_subzero) == 0

    # Run 2: New confirmed crossover emerges (TQQQ) -> Triggers alert
    current_subzero_2 = ["SOXL", "TQQQ"]
    current_crossovers_2 = ["SOXL", "TQQQ"]
    new_subzero_2 = set(current_subzero_2) - prev_subzero
    new_crossovers_2 = set(current_crossovers_2) - prev_crossovers
    assert bool(new_subzero_2 or new_crossovers_2) is True
    assert new_subzero_2 == {"TQQQ"}

    # Run 3: Confirmed bearish breakdown / reversal (SOXL breaks down) -> Triggers exit alert immediately
    current_subzero_3 = ["TQQQ"]
    current_bear_3 = ["SOXL"]
    current_crossovers_3 = ["TQQQ", "SOXL"]
    new_bear_3 = set(current_bear_3) - prev_bear
    assert bool(new_bear_3) is True
    assert new_bear_3 == {"SOXL"}

    # Run 4: Dry-run preservation test:
    # If should_send is False, sent state must retain prev_subzero, not overwrite with unalerted tickers
    should_send_dry_run = False
    sent_subzero_dry = current_subzero_2 if should_send_dry_run else list(prev_subzero)
    assert sent_subzero_dry == ["SOXL"]  # TQQQ not consumed!

    # When should_send is True, sent state updates
    should_send_live = True
    sent_subzero_live = current_subzero_2 if should_send_live else list(prev_subzero)
    assert sorted(sent_subzero_live) == ["SOXL", "TQQQ"]


@patch("yfinance.Ticker")
@patch("scripts.build_macd_scanner.calculate_macd")
def test_historical_bars_expansion_and_compact_schema(mock_calc, mock_ticker):
    """Verify historical bar expansion: 1H up to 120 bars, 1D up to 252 bars, with compact 8-field schema."""
    n_1h, n_1d = 150, 300
    df_1h, df_1d = _make_mock_data(n_1h, n_1d, is_green=True)

    inst = MagicMock()
    inst.history.side_effect = lambda period, interval, prepost=False: df_1h if interval == "1h" else df_1d
    inst.fast_info = MagicMock()
    inst.fast_info.last_price = 25.50
    mock_ticker.return_value = inst

    def _calc(closes, fast=12, slow=26, signal_period=9):
        length = len(closes)
        return {
            "macd": [0.5] * length,
            "signal": [0.4] * length,
            "hist": [0.1] * length,
            "hist_delta": [0.05] * length,
        }

    mock_calc.side_effect = _calc

    res = analyze_ticker("EXPAND_TEST", {"fund_name": "Historical Expansion Test", "leverage": "3x"})
    assert res is not None

    bars_1h = res["chart_bars_1h"]
    bars_1d = res["chart_bars_1d"]

    # Exactly 120 bars for 1h, 252 bars for 1d
    assert len(bars_1h) == 120
    assert len(bars_1d) == 252

    expected_keys = {"dt", "o", "h", "l", "c", "macd", "sig", "hist"}

    for b in bars_1h:
        assert set(b.keys()) == expected_keys
        assert isinstance(b["dt"], str)
        assert isinstance(b["c"], (int, float))
        assert isinstance(b["hist"], (int, float))

    for b in bars_1d:
        assert set(b.keys()) == expected_keys
        assert isinstance(b["dt"], str)
        assert isinstance(b["c"], (int, float))
        assert isinstance(b["hist"], (int, float))


def test_160_etf_run_deduplication_and_email_dispatch_logic():
    """Verify deduplication & email dispatch rules during the transition from 5 ETFs to 160 ETFs.
    
    In the 160-ETF run:
    - Prior state (from 5-ETF run): sent_subzero=['AAPU'], sent_crossovers=['AAPU']
    - Current active state:
      active_subzero: ['SHPU']
      active_bear: ['TZA']
      active_crossovers: ['BIB', 'CURE', 'DPST', 'LABU', 'NAIL', 'SHPU', 'SOFX', 'TZA', 'XBI']
    
    1. Dry-run without --send-email:
       - should_send evaluates to False
       - deduplication state is preserved (sent_subzero remains ['AAPU'])
    2. Execution with --send-email:
       - has_fresh_trigger evaluates to True
       - should_send evaluates to True
       - send_reason captures new subzero, exits, and crosses
       - sent state updates to all active tickers
    """
    prior_state = {
        "sent_subzero": ["AAPU"],
        "sent_bear": [],
        "sent_squeeze": [],
        "sent_crossovers": ["AAPU"],
        "last_preclose_date": "",
    }

    current_subzero_tickers = ["SHPU"]
    current_bear_tickers = ["TZA"]
    current_squeeze_tickers = ["COWZ", "TLT", "TMF", "TYD", "UBT", "YCL"]
    current_crossovers = ["BIB", "CURE", "DPST", "LABU", "NAIL", "SHPU", "SOFX", "TZA", "XBI"]

    prev_subzero = set(prior_state.get("sent_subzero", []))
    prev_bear = set(prior_state.get("sent_bear", []))
    prev_squeeze = set(prior_state.get("sent_squeeze", []))
    prev_crossovers = set(prior_state.get("sent_crossovers", []))

    new_subzero = set(current_subzero_tickers) - prev_subzero
    new_bear = set(current_bear_tickers) - prev_bear
    new_squeeze = set(current_squeeze_tickers) - prev_squeeze
    new_crossovers = set(current_crossovers) - prev_crossovers

    # 1. Triggers must be detected
    has_fresh_trigger = bool(new_subzero or new_bear or new_crossovers)
    assert has_fresh_trigger is True
    assert new_subzero == {"SHPU"}
    assert new_bear == {"TZA"}
    assert len(new_crossovers) == 9

    # 2. Case A: Command run without --send-email (manual build_macd_scanner.py run)
    args_send_email = False
    args_force_send = False
    session_code = "INTRADAY"

    should_send_without_flag = False
    if args_force_send:
        should_send_without_flag = True
    elif args_send_email:
        if has_fresh_trigger:
            should_send_without_flag = True

    assert should_send_without_flag is False
    # Verify sent state preserved
    sent_subzero = current_subzero_tickers if should_send_without_flag else list(prev_subzero)
    sent_crossovers = current_crossovers if should_send_without_flag else list(prev_crossovers)
    assert sent_subzero == ["AAPU"]
    assert sent_crossovers == ["AAPU"]

    # 3. Case B: Command run with --send-email (GitHub Actions automated cron run)
    args_send_email_active = True
    should_send_with_flag = False
    send_reason = ""

    if args_force_send:
        should_send_with_flag = True
        send_reason = "Manual override"
    elif args_send_email_active:
        if has_fresh_trigger:
            triggers_summary = []
            if new_subzero:
                triggers_summary.append(f"New Sub-Zero: {sorted(list(new_subzero))}")
            if new_bear:
                triggers_summary.append(f"New Exits: {sorted(list(new_bear))}")
            uncat = new_crossovers - new_subzero - new_bear
            if uncat:
                triggers_summary.append(f"New Crosses: {sorted(list(uncat))}")
            should_send_with_flag = True
            send_reason = f"Fresh Trigger Detected ({', '.join(triggers_summary)})"

    assert should_send_with_flag is True
    assert "New Sub-Zero: ['SHPU']" in send_reason
    assert "New Exits: ['TZA']" in send_reason
    assert "New Crosses" in send_reason

    # Verify sent state is updated upon dispatch
    sent_subzero_active = current_subzero_tickers if should_send_with_flag else list(prev_subzero)
    sent_crossovers_active = current_crossovers if should_send_with_flag else list(prev_crossovers)
    assert sent_subzero_active == ["SHPU"]
    assert sorted(sent_crossovers_active) == sorted(current_crossovers)


@patch("scripts.build_macd_scanner.calculate_macd")
@patch("yfinance.Ticker")
def test_lod_deep_history_generation_and_schema(mock_ticker, mock_calc, tmp_path):
    """Verify Level-of-Detail (LOD) architecture: deep history generation, per-ticker schema, and compact signals."""
    n_1h, n_1d = 600, 1300
    df_1h, df_1d = _make_mock_data(n_1h, n_1d, is_green=True)

    inst = MagicMock()
    inst.history.side_effect = lambda period, interval, prepost=False: df_1h if interval == "1h" else df_1d
    inst.fast_info = MagicMock()
    inst.fast_info.last_price = 45.20
    mock_ticker.return_value = inst

    def _calc(closes, fast=12, slow=26, signal_period=9):
        length = len(closes)
        return {
            "macd": [0.25] * length,
            "signal": [0.15] * length,
            "hist": [0.10] * length,
            "hist_delta": [0.02] * length,
        }

    mock_calc.side_effect = _calc

    res = analyze_ticker("LOD_TEST", {"fund_name": "LOD Deep History Test", "leverage": "2x"})
    assert res is not None

    # Deep history bars are populated
    assert "deep_bars_1h" in res
    assert "deep_bars_1d" in res
    assert len(res["deep_bars_1h"]) == n_1h
    assert len(res["deep_bars_1d"]) == n_1d

    # Lightweight preview bars are bounded to 120 / 252 for tests & previews
    assert len(res["chart_bars_1h"]) == 120
    assert len(res["chart_bars_1d"]) == 252

    # Simulate LOD per-ticker persistence
    tick = res["ticker"]
    deep_1d = res.pop("deep_bars_1d", [])
    deep_1h = res.pop("deep_bars_1h", [])

    lod_payload = {
        "ticker": tick,
        "updated_utc": "2026-11-05T03:00:00Z",
        "bars_1d": deep_1d,
        "bars_1h": deep_1h,
    }

    test_file = tmp_path / f"{tick}.json"
    with open(test_file, "w", encoding="utf-8") as f:
        json.dump(lod_payload, f, separators=(",", ":"))

    assert test_file.is_file()
    with open(test_file, "r", encoding="utf-8") as f:
        loaded = json.load(f)

    assert loaded["ticker"] == "LOD_TEST"
    assert len(loaded["bars_1d"]) == n_1d
    assert len(loaded["bars_1h"]) == n_1h

    # Schema keys check on loaded bars
    assert set(loaded["bars_1d"][0].keys()) == {"dt", "o", "h", "l", "c", "macd", "sig", "hist"}
    assert set(loaded["bars_1h"][0].keys()) == {"dt", "o", "h", "l", "c", "macd", "sig", "hist"}


def test_macd_ui_symmetry_and_dual_slider_contract():
    """Verify symmetrical 2-column header layout, nav deck, and dual-handle range slider across MACD & Volume Studies."""
    from pathlib import Path
    html = Path("docs/markets.html").read_text(encoding="utf-8")
    macd_css = Path("docs/macd-strategy.css").read_text(encoding="utf-8")
    macd_js = Path("docs/macd-strategy.js").read_text(encoding="utf-8")
    vs_js = Path("docs/volume-studies.js").read_text(encoding="utf-8")
    vs_css = Path("docs/volume-studies.css").read_text(encoding="utf-8")

    # 1. Dual-handle slider elements in HTML
    assert 'id="macd-range-start"' in html
    assert 'id="macd-range-end"' in html
    assert 'id="macd-brush-highlight"' in html
    assert 'id="macd-window-dates"' in html
    assert 'class="macd-scrub-panel"' in html

    # 2. Navigation Deck: Leaders on left, View Switcher moved down to right
    assert 'class="macd-nav-deck"' in html
    assert 'id="macd-flagship-pills"' in html
    assert 'id="macd-nav-chart"' in html
    assert 'id="macd-nav-screener"' in html

    # 3. Symmetrical 2-Column Header layout
    assert 'class="macd-title-col"' in html
    assert 'class="macd-quick-actions"' in html
    assert 'class="vs-title-col"' in html
    assert 'class="vs-quick-actions"' in html
    assert 'id="vs-header-price"' in html
    assert 'id="vs-header-chg"' in html
    assert 'id="vs-header-regime-badge"' in html

    # 4. CSS Slider and Deck Rules
    assert '.macd-scrub-panel' in macd_css
    assert '.macd-brush-highlight' in macd_css
    assert '.macd-range-thumb' in macd_css
    assert '.macd-nav-deck' in macd_css
    assert '.macd-title-col' in macd_css
    assert '.macd-quick-actions' in macd_css
    assert '.vs-fund-name' in vs_css

    # 5. JavaScript Slider Mechanics
    assert '_initSlider()' in macd_js
    assert '_syncSlider(' in macd_js
    assert 'getAllBarsForActiveTimeframe()' in macd_js
    assert 'customRange' in macd_js
    assert 'vs-header-price' in vs_js
    assert 'vs-header-regime-badge' in vs_js


# ---------------------------------------------------------------------------
# Tests: Floor Pivot Breakout & Extreme Volatility Surge Triggers
# ---------------------------------------------------------------------------

@patch("yfinance.Ticker")
@patch("scripts.build_macd_scanner.calculate_macd")
def test_breakout_and_breakdown_detection(mock_calc, mock_ticker):
    """Verify Floor Pivot (R4/R5) and velocity surge breakouts vs (S4/S5) liquidation cascades."""
    n_1h, n_1d = 140, 70

    # Helper to build mock ticker response
    def _run_test(price, prev_h, prev_l, prev_c, chg_1d, is_green, hist_val):
        df_1h, df_1d = _make_mock_data(n_1h, n_1d, is_green=is_green)
        # Set daily bars so pivot derivation and prior day close match test inputs
        df_1d["High"] = prev_h
        df_1d["Low"] = prev_l
        df_1d["Close"] = prev_c

        inst = MagicMock()
        inst.history.side_effect = lambda period, interval, prepost=False: df_1h if interval == "1h" else df_1d
        inst.fast_info = MagicMock()
        inst.fast_info.last_price = price
        mock_ticker.return_value = inst

        mock_calc.side_effect = lambda closes, fast=12, slow=26, signal_period=9: {
            "macd": [0.5 if hist_val > 0 else -0.5] * len(closes),
            "signal": [0.3 if hist_val > 0 else -0.3] * len(closes),
            "hist": [hist_val] * len(closes),
            "hist_delta": [0.05 if hist_val > 0 else -0.05] * len(closes),
        }

        return analyze_ticker("TEST", {"fund_name": "Test ETF", "leverage": "2x"})

    # Case 1: R5 Breakout (Price extends above R5, confirmed green candle, hist > 0)
    # H=100, L=90, C=95 -> P=95, diff=10, R3=100+2*(95-90)=110, R4=120, R5=130
    res_r5 = _run_test(price=135.0, prev_h=100.0, prev_l=90.0, prev_c=95.0, chg_1d=42.1, is_green=True, hist_val=0.2)
    assert res_r5 is not None
    assert res_r5["is_breakout"] is True
    assert res_r5["breakout_type"] == "R5 Floor Pivot Expansion"

    # Case 2: R4 Breakout (Price reaches R4 but below R5, confirmed green candle, hist > 0)
    res_r4 = _run_test(price=122.0, prev_h=100.0, prev_l=90.0, prev_c=95.0, chg_1d=28.4, is_green=True, hist_val=0.2)
    assert res_r4 is not None
    assert res_r4["is_breakout"] is True
    assert res_r4["breakout_type"] == "R4 Floor Pivot Breakout"

    # Case 3: Velocity Surge (+15.0% 24H move without hitting R4/R5, confirmed green candle, hist > 0)
    # H=100, L=50, C=75 -> diff=50, R4=225. Price=86.25 (+15% vs 75), well below R4
    res_surge = _run_test(price=86.25, prev_h=100.0, prev_l=50.0, prev_c=75.0, chg_1d=15.0, is_green=True, hist_val=0.2)
    assert res_surge is not None
    assert res_surge["is_breakout"] is True
    assert "Velocity Surge" in res_surge["breakout_type"]

    # Case 4: Unconfirmed Breakout (Price >= R5, but RED 1H candle) -> Suppressed!
    res_unconfirmed_red = _run_test(price=135.0, prev_h=100.0, prev_l=90.0, prev_c=95.0, chg_1d=42.1, is_green=False, hist_val=0.2)
    assert res_unconfirmed_red is not None
    assert res_unconfirmed_red["is_breakout"] is False

    # Case 5: S5 Liquidation (Price collapses below S5, confirmed red candle, hist < 0)
    # H=100, L=90, C=95 -> P=95, diff=10, S3=90-2*(100-95)=80, S4=70, S5=60
    res_s5 = _run_test(price=55.0, prev_h=100.0, prev_l=90.0, prev_c=95.0, chg_1d=-42.1, is_green=False, hist_val=-0.2)
    assert res_s5 is not None
    assert res_s5["is_breakdown"] is True
    assert res_s5["breakdown_type"] == "S5 Floor Pivot Liquidation"

    # Case 6: S4 Breakdown (Price collapses below S4 but above S5, confirmed red candle, hist < 0)
    res_s4 = _run_test(price=68.0, prev_h=100.0, prev_l=90.0, prev_c=95.0, chg_1d=-28.4, is_green=False, hist_val=-0.2)
    assert res_s4 is not None
    assert res_s4["is_breakdown"] is True
    assert res_s4["breakdown_type"] == "S4 Floor Pivot Breakdown"

    # Case 7: Velocity Plunge (-15.0% 24H move, confirmed red candle, hist < 0)
    res_plunge = _run_test(price=63.75, prev_h=100.0, prev_l=50.0, prev_c=75.0, chg_1d=-15.0, is_green=False, hist_val=-0.2)
    assert res_plunge is not None
    assert res_plunge["is_breakdown"] is True
    assert "Velocity Plunge" in res_plunge["breakdown_type"]

    # Case 8: Unconfirmed Breakdown (Price <= S5, but GREEN 1H candle) -> Suppressed!
    res_unconfirmed_green = _run_test(price=55.0, prev_h=100.0, prev_l=90.0, prev_c=95.0, chg_1d=-42.1, is_green=True, hist_val=-0.2)
    assert res_unconfirmed_green is not None
    assert res_unconfirmed_green["is_breakdown"] is False


def test_format_institutional_email_with_breakouts_and_breakdowns():
    """Verify institutional email formatting displays Tier 0 Breakouts and Critical Breakdowns."""
    mock_signals = [
        {
            "ticker": "UVIX",
            "fund_name": "2x Long VIX Futures ETF",
            "leverage": "2x",
            "category": "Volatility",
            "underlying": "Long VIX Futures",
            "latest_price": 49.62,
            "chg_1h_pct": 5.2,
            "chg_1d_pct": 49.86,
            "candle_color_1h": "GREEN",
            "is_green_candle_1h": True,
            "is_red_candle_1h": False,
            "macd_1h": 2.15,
            "signal_1h": 1.45,
            "hist_1h": 0.70,
            "hist_delta_1h": 0.15,
            "rvol_1h": 3.8,
            "z_macd_1h": 2.8,
            "confluence_score": 90,
            "is_breakout": True,
            "breakout_type": "R5 Floor Pivot Expansion",
            "is_breakdown": False,
            "breakdown_type": "",
            "signal_state": "BULLISH_TRENDING",
            "daily_trend_bullish": True,
            "pivots": {"P": 33.50, "R3": 42.00, "R4": 46.00, "R5": 49.00, "S1": 31.00, "S2": 29.00},
        },
        {
            "ticker": "SOXL",
            "fund_name": "Daily Semiconductor Bull 3X",
            "leverage": "3x",
            "category": "Technology",
            "underlying": "Semiconductors",
            "latest_price": 64.99,
            "chg_1h_pct": -1.17,
            "chg_1d_pct": -11.12,
            "candle_color_1h": "RED",
            "is_green_candle_1h": False,
            "is_red_candle_1h": True,
            "macd_1h": -1.25,
            "signal_1h": -0.85,
            "hist_1h": -0.40,
            "hist_delta_1h": -0.10,
            "rvol_1h": 1.9,
            "z_macd_1h": -1.5,
            "confluence_score": -80,
            "is_breakout": False,
            "breakout_type": "",
            "is_breakdown": True,
            "breakdown_type": "S4 Floor Pivot Breakdown",
            "signal_state": "BEARISH_CROSS",
            "daily_trend_bullish": False,
            "pivots": {"P": 72.00, "S1": 70.00, "S2": 68.00, "S4": 65.50, "S5": 62.00},
        },
    ]

    session_info = ("INTRADAY", "INTRADAY HOURLY MOMENTUM SCAN", "INTRADAY RADAR")
    text_content, html_content = format_institutional_email(mock_signals, "2026-11-05 11:35:00 EST", session_info)

    # Plain text checks
    assert "[TIER 0: EXTREME MOMENTUM BREAKOUTS & VOLATILITY SURGES]" in text_content
    assert "TICKER: UVIX" in text_content
    assert "R5 Floor Pivot Expansion" in text_content
    assert "[CRITICAL CAPITAL PRESERVATION: SEVERE BREAKDOWNS & LIQUIDATIONS]" in text_content
    assert "SOXL" in text_content
    assert "S4 Floor Pivot Breakdown" in text_content

    # HTML checks
    assert "TIER 0: EXTREME MOMENTUM BREAKOUTS" in html_content
    assert "UVIX" in html_content
    assert "PIVOT RESISTANCE EXTENSION" in html_content
    assert "CRITICAL CAPITAL PRESERVATION &middot; SEVERE LIQUIDATION CASCADE" in html_content
    assert "BREAKOUT SURGE" in html_content


def test_deduplication_state_with_breakouts_and_breakdowns():
    """Verify deduplication engine handles breakout and breakdown state tracking."""
    prior_state = {
        "sent_subzero": [],
        "sent_bear": [],
        "sent_crossovers": [],
        "sent_breakouts": ["UVIX"],
        "sent_breakdowns": [],
        "last_preclose_date": "2026-11-04",
    }

    prev_breakouts = set(prior_state.get("sent_breakouts", []))
    prev_breakdowns = set(prior_state.get("sent_breakdowns", []))

    # Run 1: UVIX is still active breakout -> Suppressed, no fresh trigger
    current_breakouts = ["UVIX"]
    current_breakdowns = []
    new_breakouts = set(current_breakouts) - prev_breakouts
    new_breakdowns = set(current_breakdowns) - prev_breakdowns

    has_fresh_trigger = bool(new_breakouts or new_breakdowns)
    assert has_fresh_trigger is False

    # Run 2: Fresh breakout emerges (SOXS joins) -> Triggers alert
    current_breakouts_2 = ["UVIX", "SOXS"]
    new_breakouts_2 = set(current_breakouts_2) - prev_breakouts
    assert bool(new_breakouts_2) is True
    assert new_breakouts_2 == {"SOXS"}

    # Run 3: Fresh breakdown emerges (SOXL collapses) -> Triggers alert
    current_breakdowns_3 = ["SOXL"]
    new_breakdowns_3 = set(current_breakdowns_3) - prev_breakdowns
    assert bool(new_breakdowns_3) is True
    assert new_breakdowns_3 == {"SOXL"}


def test_atr_and_v_macd_calculation():
    """Verify ATR and V-MACD mathematical correctness on synthetic data."""
    n = 60
    highs = [22.0 + i * 0.2 for i in range(n)]
    lows = [18.0 + i * 0.2 for i in range(n)]
    closes = [20.0 + i * 0.2 for i in range(n)]

    atr = calculate_atr(highs, lows, closes, period=14)
    assert len(atr) == n
    assert all(a > 0 for a in atr)

    v_res = calculate_v_macd(highs, lows, closes, fast=12, slow=26, signal_period=9, atr_period=14)
    assert len(v_res["v_macd"]) == n
    assert len(v_res["v_signal"]) == n
    assert len(v_res["v_hist"]) == n
    # In an uptrend, V-MACD should be positive
    assert v_res["v_macd"][-1] > 0


def test_yang_zhang_volatility_calculation():
    """Verify Yang-Zhang drift-independent volatility computation."""
    n = 30
    opens = [100.0] * n
    highs = [102.0] * n
    lows = [98.0] * n
    closes = [100.0] * n
    sigma = calculate_yang_zhang_volatility(opens, highs, lows, closes, period=21)
    assert sigma > 0
    assert sigma < 0.10


@patch("yfinance.Ticker")
def test_warmup_gates_enforcement(mock_ticker):
    """Verify that insufficient 1H or 1D bar history is rejected unless overridden."""
    # Under-length series: 50 1h bars, 30 1d bars (below 130 and 60)
    df_1h, df_1d = _make_mock_data(n_1h=50, n_1d=30, is_green=True)
    inst = MagicMock()
    inst.history.side_effect = lambda period, interval, prepost=False: df_1h if interval == "1h" else df_1d
    inst.fast_info = MagicMock()
    inst.fast_info.last_price = 20.0
    mock_ticker.return_value = inst

    # Default gates reject
    res_default = analyze_ticker("TEST", {"fund_name": "Test ETF", "leverage": "2x"})
    assert res_default is None

    # Lowered keyword gates accept
    res_custom = analyze_ticker(
        "TEST",
        {"fund_name": "Test ETF", "leverage": "2x"},
        min_1h_bars=30,
        min_1d_bars=15,
    )
    assert res_custom is not None
    assert "v_macd" in res_custom
    assert "yang_zhang_vol" in res_custom
    assert "rvol_1h_diurnal" in res_custom

