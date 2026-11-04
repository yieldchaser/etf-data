"""Unit tests for Quantitative MACD Momentum Scanner and Candle Confirmation Rules.

Covers Raghav Sir's TradingView confirmation rule:
1. Bullish crossover with red candle does NOT trigger Sub-Zero Reversal alert.
2. Bullish crossover with green candle DOES trigger Sub-Zero Reversal alert.
3. Bearish crossover with green candle does NOT trigger Bearish Breakdown alert.
4. Bearish crossover with red candle DOES trigger Bearish Breakdown alert.
5. Unconfirmed crossovers are marked UNCONFIRMED_CROSS / UNCONFIRMED_BEAR_CROSS.
6. Email formatting and filtering excludes unconfirmed crossovers.
"""

from __future__ import annotations

import datetime as dt
from unittest.mock import MagicMock, patch
import pandas as pd
import pytest

from scripts.build_macd_scanner import (
    analyze_ticker,
    calculate_ema,
    calculate_macd,
    calculate_pivots,
    format_institutional_email,
    get_market_session,
)


def _make_mock_data(n_1h: int = 35, n_1d: int = 40, is_green: bool = True):
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
    """Raghav Sir Rule 1: Bullish cross with RED candle does NOT trigger Sub-Zero Reversal.
    
    In this test:
    - 1H MACD crossed above signal line in sub-zero territory (macd: -1.0 -> -0.5, sig: -0.8 -> -0.7, hist > 0).
    - Latest 1H bar is RED (Open=21.0, Close=20.0).
    - Result MUST be:
      signal_state == 'UNCONFIRMED_CROSS'
      bull_cross_fresh is False
      candle_color_1h == 'RED'
      is_green_candle_1h is False
    """
    n_1h, n_1d = 35, 40
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
    """Raghav Sir Rule 2: Bullish cross with GREEN candle DOES trigger Sub-Zero Reversal.
    
    In this test:
    - 1H MACD crossed above signal line in sub-zero territory.
    - Latest 1H bar is GREEN (Open=19.0, Close=20.0).
    - Result MUST be:
      signal_state == 'SUB_ZERO_BOUNCE'
      bull_cross_fresh is True
      candle_color_1h == 'GREEN'
      is_green_candle_1h is True
    """
    n_1h, n_1d = 35, 40
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
    """Raghav Sir Rule 3: Bearish cross with GREEN candle does NOT trigger Bearish Breakdown alert.
    
    In this test:
    - 1H MACD crossed below signal line (macd: 1.0 -> 0.5, sig: 0.8 -> 0.7, hist < 0).
    - Latest 1H bar is GREEN (Open=19.0, Close=20.0).
    - Result MUST be:
      signal_state == 'UNCONFIRMED_BEAR_CROSS'
      bear_cross_fresh is False
      candle_color_1h == 'GREEN'
    """
    n_1h, n_1d = 35, 40
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
    """Raghav Sir Rule 4: Bearish cross with RED candle DOES trigger Bearish Breakdown alert.
    
    In this test:
    - 1H MACD crossed below signal line.
    - Latest 1H bar is RED (Open=21.0, Close=20.0).
    - Result MUST be:
      signal_state == 'BEARISH_CROSS'
      bear_cross_fresh is True
      candle_color_1h == 'RED'
    """
    n_1h, n_1d = 35, 40
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
