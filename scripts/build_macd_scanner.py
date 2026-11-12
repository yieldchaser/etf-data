#!/usr/bin/env python3
"""Quantitative MACD Momentum Scanner and Email Alert Engine for Leveraged ETFs.

This script:
1. Ingests the curated instruments from `docs/data/flows/catalog.json`.
2. Fetches intraday 1-hour OHLCV and daily OHLCV bars via Yahoo Finance Chart API:
   - Uses Regular Trading Hours (RTH, prepost=False) for canonical 1H MACD (12, 26, 9 EMA).
   - In pre-market (04:00 - 09:30 ET), captures live pre-market price to compute indicated gap.
   - Computes traditional Auto Floor Trader Pivots (P, S1-S5, R1-R5) from the last COMPLETED
     regular trading session (strictly session date < today's date during market/premarket).
3. Detects signals across all market sessions:
   - Tier 1A: Confirmed Sub-Zero Bullish Reversals (high convexity value turnarounds).
   - Tier 1B: Confirmed Supra-Zero Bullish Crossovers (momentum continuation).
   - Tier 2: Pre-Close Squeeze Watch (histogram compression >= 35% heading into 15:00-16:00 ET).
   - Tier 3: Bearish Breakdowns / Mandatory Cash Parking (long de-grossing exits).
4. Dispatches tactical alerts to configured recipients:
   - "Whenever or as soon as data triggers": event-driven dispatch upon fresh crossovers.
   - Scheduled Pre-Close Golden Hour briefing at 15:00 ET.
   - Deduplication engine tracks state in `latest_macd_alert.json` to prevent inbox spam.
   - Responsive High-DPI formatting: optimized for Phone + Laptop, zero emojis.
5. Saves real-time artifacts:
   - `docs/data/flows/macd_signals.json` (consumed by frontend Canvas 2D engine).
   - `docs/data/flows/latest_macd_alert.json` (audit trail and deduplication state).
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import email.mime.multipart
import email.mime.text
import email.utils
import html as html_lib
import json
import math
import os
import re
import smtplib
import ssl
import sys
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

REPO_ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = REPO_ROOT / "docs" / "data" / "flows" / "catalog.json"
SIGNALS_OUT_MACD = REPO_ROOT / "docs" / "data" / "macd" / "signals.json"
ALERT_OUT_MACD = REPO_ROOT / "docs" / "data" / "macd" / "latest_alert.json"
SIGNALS_OUT_FLOWS = REPO_ROOT / "docs" / "data" / "flows" / "macd_signals.json"
ALERT_OUT_FLOWS = REPO_ROOT / "docs" / "data" / "flows" / "latest_macd_alert.json"
HISTORY_DIR_MACD = REPO_ROOT / "docs" / "data" / "macd" / "history"
HISTORY_DIR_FLOWS = REPO_ROOT / "docs" / "data" / "flows" / "history"
SIGNALS_OUT = SIGNALS_OUT_MACD
ALERT_OUT = ALERT_OUT_MACD

NY_TZ = ZoneInfo("America/New_York")


def _atomic_write_json(path: Path, payload: Any, **dump_kwargs: Any) -> None:
    """Write JSON atomically via sibling temp file + os.replace."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, **dump_kwargs)
    os.replace(tmp_path, path)


def get_market_session(now_ny: dt.datetime) -> tuple[str, str, str]:
    """Detect current US market session: returns (session_code, session_name, badge_text)."""
    weekday = now_ny.weekday()  # 0=Mon, 4=Fri, 5=Sat, 6=Sun
    if weekday >= 5:
        return "WEEKEND", "WEEKEND QUANTITATIVE AUDIT", "WEEKEND SCAN"

    time_float = now_ny.hour + now_ny.minute / 60.0
    tz_abbr = now_ny.tzname() or "ET"

    if time_float < 4.0:
        return "OVERNIGHT", "OVERNIGHT OFF-HOURS RADAR", "OVERNIGHT SCAN"
    elif 4.0 <= time_float < 9.5:
        return "PRE_MARKET", "PRE-MARKET INDICATIVE RADAR", "PRE-MARKET SCAN"
    elif 9.5 <= time_float < 10.5:
        return "SESSION_OPEN", "REGULAR SESSION OPEN MOMENTUM", "SESSION OPEN"
    elif 10.5 <= time_float < 14.75:
        return "INTRADAY", "INTRADAY HOURLY MOMENTUM SCAN", "INTRADAY RADAR"
    elif 14.75 <= time_float < 16.0:
        return "PRE_CLOSE", "PRE-MARKET-CLOSE GOLDEN HOUR SCAN", f"15:00 {tz_abbr} PRE-CLOSE"
    elif 16.0 <= time_float < 20.0:
        return "POST_MARKET", "POST-MARKET CLOSING AUDIT", "POST-MARKET SCAN"
    else:
        return "OVERNIGHT", "OVERNIGHT OFF-HOURS RADAR", "OFF-HOURS SCAN"


def calculate_ema(series: list[float], period: int) -> list[float]:
    """Calculate strictly causal Exponential Moving Average with smoothing alpha = 2 / (period + 1)."""
    n = len(series)
    out = [0.0] * n
    if n == 0 or period <= 0:
        return out

    # Locate first finite value to avoid zero-seed bias on leading NaNs
    first_valid: float | None = None
    for val in series:
        if val is not None and not math.isnan(val) and not math.isinf(val):
            first_valid = float(val)
            break
    if first_valid is None:
        return out

    # Sanitize inputs (forward-fill non-finite values without mutating past observations)
    clean: list[float] = []
    last_valid = first_valid
    for val in series:
        if val is not None and not math.isnan(val) and not math.isinf(val):
            last_valid = float(val)
        clean.append(last_valid)

    alpha = 2.0 / (period + 1)
    seed_len = min(period, n)
    running_sum = 0.0
    for i in range(seed_len):
        running_sum += clean[i]
        out[i] = running_sum / (i + 1)

    for i in range(seed_len, n):
        out[i] = clean[i] * alpha + out[i - 1] * (1.0 - alpha)

    return out


def calculate_macd(
    closes: list[float], fast: int = 12, slow: int = 26, signal_period: int = 9
) -> dict[str, list[float]]:
    """Compute standard MACD Line (fast - slow), Signal Line, and Histogram."""
    n = len(closes)
    if n < slow or slow <= 0 or fast <= 0 or signal_period <= 0:
        return {
            "macd": [0.0] * n,
            "signal": [0.0] * n,
            "hist": [0.0] * n,
            "hist_delta": [0.0] * n,
        }

    ema_fast = calculate_ema(closes, fast)
    ema_slow = calculate_ema(closes, slow)
    macd_line = [f - s for f, s in zip(ema_fast, ema_slow)]
    signal_line = calculate_ema(macd_line, signal_period)
    hist = [m - s for m, s in zip(macd_line, signal_line)]

    hist_delta = [0.0] * n
    for i in range(1, n):
        hist_delta[i] = hist[i] - hist[i - 1]

    return {
        "macd": macd_line,
        "signal": signal_line,
        "hist": hist,
        "hist_delta": hist_delta,
    }


def calculate_pivots(high: float, low: float, close: float) -> dict[str, float]:
    """Calculate traditional Floor Trader Pivots (P, S1-S5, R1-R5)."""
    if any(math.isnan(v) or math.isinf(v) for v in (high, low, close)):
        return {k: 0.0 for k in ["P", "R1", "R2", "R3", "R4", "R5", "S1", "S2", "S3", "S4", "S5"]}

    p = (high + low + close) / 3.0
    diff = high - low

    r1 = 2.0 * p - low
    s1 = 2.0 * p - high

    r2 = p + diff
    s2 = p - diff

    r3 = high + 2.0 * (p - low)
    s3 = low - 2.0 * (high - p)

    r4 = r3 + diff
    s4 = s3 - diff

    r5 = r4 + diff
    s5 = s4 - diff

    return {
        "P": round(p, 4),
        "R1": round(r1, 4),
        "R2": round(r2, 4),
        "R3": round(r3, 4),
        "R4": round(r4, 4),
        "R5": round(r5, 4),
        "S1": round(s1, 4),
        "S2": round(s2, 4),
        "S3": round(s3, 4),
        "S4": round(s4, 4),
        "S5": round(s5, 4),
    }


def clean_category_name(raw_cat: str) -> str:
    """Strip leading numbers and maintenance notes from category string."""
    if not raw_cat:
        return "Unclassified"
    cleaned = re.sub(r"^\d+\.\s*", "", str(raw_cat))
    cleaned = re.sub(r"\s*\(Pruned to Key Anchors Only\)", "", cleaned, flags=re.IGNORECASE)
    return cleaned.strip()


def calculate_atr(
    highs: list[float], lows: list[float], closes: list[float], period: int = 14
) -> list[float]:
    """Calculate Average True Range (ATR) with exponential smoothing."""
    n = len(closes)
    if n == 0 or period <= 0:
        return [0.0] * n
    tr = [0.0] * n
    tr[0] = max(0.0, float(highs[0]) - float(lows[0]))
    for i in range(1, n):
        h = float(highs[i])
        l = float(lows[i])
        cp = float(closes[i - 1])
        tr[i] = max(h - l, abs(h - cp), abs(l - cp))
    return calculate_ema(tr, period)


def calculate_yang_zhang_volatility(
    opens: list[float], highs: list[float], lows: list[float], closes: list[float], period: int = 21
) -> float:
    """Compute Yang-Zhang (2000) drift-independent & overnight jump-aware daily volatility."""
    n = len(closes)
    if n < 5 or period < 2:
        return 0.015
    k_bars = min(period, n - 1)
    sub_o = opens[-k_bars:]
    sub_h = highs[-k_bars:]
    sub_l = lows[-k_bars:]
    sub_c = closes[-k_bars:]
    sub_prev_c = closes[-k_bars - 1 : -1]

    o_rets = []
    c_rets = []
    rs_vals = []
    for i in range(k_bars):
        c_prev = float(sub_prev_c[i])
        o_val = float(sub_o[i])
        h_val = float(sub_h[i])
        l_val = float(sub_l[i])
        c_val = float(sub_c[i])
        if c_prev <= 0 or o_val <= 0 or h_val <= 0 or l_val <= 0 or c_val <= 0:
            continue
        o_i = math.log(o_val / c_prev)
        c_i = math.log(c_val / o_val)
        u_i = math.log(h_val / o_val)
        d_i = math.log(l_val / o_val)
        rs_i = u_i * (u_i - c_i) + d_i * (d_i - c_i)
        o_rets.append(o_i)
        c_rets.append(c_i)
        rs_vals.append(rs_i)

    m = len(o_rets)
    if m < 2:
        return 0.015
    mean_o = sum(o_rets) / m
    s_o2 = sum((x - mean_o) ** 2 for x in o_rets) / (m - 1)
    mean_c = sum(c_rets) / m
    s_c2 = sum((x - mean_c) ** 2 for x in c_rets) / (m - 1)
    mean_rs = sum(rs_vals) / m
    k = 0.34 / (1.34 + (m + 1.0) / (m - 1.0))
    var_yz = max(1e-8, s_o2 + k * s_c2 + (1.0 - k) * mean_rs)
    return math.sqrt(var_yz)


def calculate_v_macd(
    highs: list[float],
    lows: list[float],
    closes: list[float],
    fast: int = 12,
    slow: int = 26,
    signal_period: int = 9,
    atr_period: int = 14,
) -> dict[str, list[float]]:
    """Compute ATR-normalized V-MACD Line, Signal Line, and Histogram."""
    n = len(closes)
    if n < slow or slow <= 0 or fast <= 0 or signal_period <= 0:
        return {
            "v_macd": [0.0] * n,
            "v_signal": [0.0] * n,
            "v_hist": [0.0] * n,
            "v_hist_delta": [0.0] * n,
        }
    ema_fast = calculate_ema(closes, fast)
    ema_slow = calculate_ema(closes, slow)
    atr = calculate_atr(highs, lows, closes, atr_period)
    v_macd_line = [(f - s) / (a + 1e-9) if a > 1e-9 else 0.0 for f, s, a in zip(ema_fast, ema_slow, atr)]
    v_signal_line = calculate_ema(v_macd_line, signal_period)
    v_hist = [m - s for m, s in zip(v_macd_line, v_signal_line)]
    v_hist_delta = [0.0] * n
    for i in range(1, n):
        v_hist_delta[i] = v_hist[i] - v_hist[i - 1]
    return {
        "v_macd": v_macd_line,
        "v_signal": v_signal_line,
        "v_hist": v_hist,
        "v_hist_delta": v_hist_delta,
    }


def analyze_ticker(
    ticker: str,
    catalog_meta: dict[str, Any],
    min_1h_bars: int = 130,
    min_1d_bars: int = 60,
    is_premarket: bool | None = None,
) -> dict[str, Any] | None:
    """Fetch 1h (RTH) and 1d bars, compute technical metrics, and classify signals."""
    import yfinance as yf

    try:
        t = yf.Ticker(ticker)
        # Fetch up to 730d of 1h RTH bars (max Yahoo intraday depth) and up to 5y of 1d bars
        df_1h = t.history(period="730d", interval="1h", prepost=False)
        if df_1h is None or getattr(df_1h, "empty", True):
            # Yahoo Finance errors on period="730d" when ETF inception is ~2.0-2.2 years ago
            # because it resolves period relative to firstTradeDate. Anchor explicitly at now-715d.
            start_715d = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=715)).strftime("%Y-%m-%d")
            try:
                df_1h = t.history(start=start_715d, interval="1h", prepost=False)
            except TypeError:
                pass
        df_1d = t.history(period="5y", interval="1d")

        if df_1h is None or df_1d is None or len(df_1h) < min_1h_bars or len(df_1d) < min_1d_bars:
            return None

        # Clean 1H series (sanitize missing/NaN values across all OHLCV columns)
        df_1h = df_1h.dropna(subset=["Open", "High", "Low", "Close", "Volume"])

        # RTH session calendar filter (America/New_York 09:30 <= t < 16:00 ET, DST safe)
        now_ny = dt.datetime.now(dt.timezone.utc).astimezone(NY_TZ)
        now_time_float = now_ny.hour + now_ny.minute / 60.0
        if is_premarket is None:
            is_premarket = now_ny.weekday() < 5 and 4.0 <= now_time_float < 9.5
        ts_1h_seq = df_1h.index
        try:
            idx_tz = getattr(df_1h.index, "tz", None)
            if idx_tz is None:
                df_1h_ny_idx = df_1h.index.tz_localize(NY_TZ)
            else:
                df_1h_ny_idx = df_1h.index.tz_convert(NY_TZ)
            rth_mask = [
                bool(9.5 <= (ts.hour + ts.minute / 60.0) < 16.0)
                for ts in df_1h_ny_idx
            ]
            if sum(rth_mask) >= min_1h_bars:
                df_1h = df_1h.iloc[rth_mask]
                df_1h_ny_idx = df_1h_ny_idx[rth_mask]
            ts_1h_seq = df_1h_ny_idx
        except Exception:
            ts_1h_seq = df_1h.index

        # Drop still-forming live 1H candle during today's RTH session so intra-bar
        # noise never triggers premature crossover signals before the hour settles.
        if (
            now_ny.weekday() < 5
            and 9.5 <= now_time_float < 16.0
            and len(df_1h) > min_1h_bars
            and len(ts_1h_seq) == len(df_1h)
            and hasattr(ts_1h_seq[-1], "hour")
        ):
            last_ts_ny = ts_1h_seq[-1]
            if hasattr(last_ts_ny, "date") and last_ts_ny.date() == now_ny.date():
                if last_ts_ny.hour == 15 and last_ts_ny.minute >= 30:
                    bar_end_ny = last_ts_ny.replace(hour=16, minute=0, second=0, microsecond=0)
                else:
                    bar_end_ny = last_ts_ny + dt.timedelta(hours=1)
                if bar_end_ny > now_ny:
                    df_1h = df_1h.iloc[:-1]
                    ts_1h_seq = ts_1h_seq[:-1]

        if len(df_1h) < min_1h_bars:
            return None
        closes_1h = [float(x) for x in df_1h["Close"]]
        volumes_1h = [float(x) for x in df_1h["Volume"]]
        highs_1h = [float(x) for x in df_1h["High"]]
        lows_1h = [float(x) for x in df_1h["Low"]]
        opens_1h = [float(x) for x in df_1h["Open"]]
        timestamps_1h = [
            ts.strftime("%Y-%m-%d %H:%M") if hasattr(ts, "strftime") else str(ts)[:16]
            for ts in ts_1h_seq
        ]

        # Clean 1D series (raw full history for pivots/prior close lookup, settled 1D for indicators)
        df_1d = df_1d.dropna(subset=["Open", "High", "Low", "Close", "Volume"])
        if len(df_1d) < min_1d_bars:
            return None

        def _to_ny_date(ts: Any) -> dt.date:
            if hasattr(ts, "tz") and ts.tz is not None:
                return ts.tz_convert(NY_TZ).date()
            elif hasattr(ts, "date"):
                return ts.date()
            return dt.date.fromisoformat(str(ts)[:10])

        today_date = now_ny.date()
        is_post_close = (now_ny.weekday() >= 5) or (now_time_float >= 16.0)
        df_1d_settled = df_1d
        if not is_post_close and len(df_1d) > min_1d_bars and _to_ny_date(df_1d.index[-1]) == today_date:
            df_1d_settled = df_1d.iloc[:-1]

        closes_1d = [float(x) for x in df_1d_settled["Close"]]
        opens_1d = [float(x) for x in df_1d_settled["Open"]]
        highs_1d = [float(x) for x in df_1d_settled["High"]]
        lows_1d = [float(x) for x in df_1d_settled["Low"]]
        volumes_1d = [float(x) for x in df_1d_settled["Volume"]]
        timestamps_1d = [
            ts.strftime("%Y-%m-%d") if hasattr(ts, "strftime") else str(ts)[:10]
            for ts in df_1d_settled.index
        ]

        # Compute 1H MACD
        m_1h = calculate_macd(closes_1h, fast=12, slow=26, signal_period=9)
        macd_1h = m_1h["macd"]
        sig_1h = m_1h["signal"]
        hist_1h = m_1h["hist"]
        hist_delta_1h = m_1h["hist_delta"]

        # Compute 1H ATR and V-MACD
        atr_1h = calculate_atr(highs_1h, lows_1h, closes_1h, period=14)
        v_1h = calculate_v_macd(highs_1h, lows_1h, closes_1h, fast=12, slow=26, signal_period=9, atr_period=14)
        v_macd_1h = v_1h["v_macd"]
        v_sig_1h = v_1h["v_signal"]
        v_hist_1h = v_1h["v_hist"]
        v_hist_delta_1h = v_1h["v_hist_delta"]

        # Compute 1D MACD and Yang-Zhang Daily Volatility
        m_1d = calculate_macd(closes_1d, fast=12, slow=26, signal_period=9)
        hist_1d = m_1d["hist"]
        sigma_yz = calculate_yang_zhang_volatility(opens_1d, highs_1d, lows_1d, closes_1d, period=21)

        # Reconcile latest price against official exchange last_price if available
        official_last = getattr(t.fast_info, "last_price", None)
        if official_last and float(official_last) > 0 and not math.isnan(float(official_last)):
            latest_price = round(float(official_last), 2)
        else:
            latest_price = round(float(closes_1h[-1]), 2)

        # Check for Live Pre-Market Tick (1m resolution for real-time accuracy)
        live_premarket_price: float | None = None
        live_premarket_chg_pct: float | None = None
        if is_premarket:
            try:
                df_ext = t.history(period="1d", interval="1m", prepost=True)
                if not df_ext.empty:
                    last_ext_px = float(df_ext["Close"].iloc[-1])
                    if last_ext_px > 0 and latest_price > 0 and not math.isnan(last_ext_px):
                        live_premarket_price = round(last_ext_px, 2)
                        live_premarket_chg_pct = round(((last_ext_px - latest_price) / latest_price) * 100.0, 2)
            except Exception:
                pass
        # Compute 1H candle metrics strictly from canonical settled 1H bar closes
        latest_open_1h = float(opens_1h[-1])
        latest_close_1h = float(closes_1h[-1])
        latest_high_1h = float(highs_1h[-1])
        latest_low_1h = float(lows_1h[-1])
        prev_close_1h = float(closes_1h[-2]) if len(closes_1h) >= 2 else latest_close_1h
        chg_1h_pct = (
            round(((latest_close_1h - prev_close_1h) / prev_close_1h) * 100.0, 2)
            if prev_close_1h > 0
            else 0.0
        )

        # 1H Candle Confirmation (TradingView Rule)
        if latest_close_1h > latest_open_1h:
            is_green_candle_1h = True
            is_red_candle_1h = False
        elif latest_close_1h < latest_open_1h:
            is_green_candle_1h = False
            is_red_candle_1h = True
        else:
            # Flat doji candle (Close == Open): disambiguate using bar change vs prev close
            if chg_1h_pct > 0:
                is_green_candle_1h = True
                is_red_candle_1h = False
            elif chg_1h_pct < 0:
                is_green_candle_1h = False
                is_red_candle_1h = True
            else:
                # Zero change: disambiguate using histogram delta direction
                is_green_candle_1h = bool(hist_delta_1h[-1] >= 0)
                is_red_candle_1h = bool(hist_delta_1h[-1] < 0)

        candle_color_1h = "GREEN" if is_green_candle_1h else "RED"

        # 24H Price change vs prior completed day close (strictly session before today)
        prior_session_days = df_1d[df_1d.index.map(lambda x: _to_ny_date(x) < today_date)]
        if not prior_session_days.empty:
            prior_day_bar = prior_session_days.iloc[-1]
            prior_day_close = float(prior_day_bar["Close"])
        else:
            prior_day_bar = df_1d.iloc[-1]
            prior_day_close = float(prior_day_bar["Close"])

        chg_1d_pct = (
            round(((latest_price - prior_day_close) / prior_day_close) * 100.0, 2)
            if prior_day_close > 0
            else 0.0
        )

        # Floor Pivots
        pivot_reference_days = (
            df_1d[df_1d.index.map(lambda x: _to_ny_date(x) <= today_date)]
            if is_post_close
            else prior_session_days
        )
        if not pivot_reference_days.empty:
            pivot_bar = pivot_reference_days.iloc[-1]
        else:
            pivot_bar = df_1d.iloc[-1]

        prev_day_h = float(pivot_bar["High"])
        prev_day_l = float(pivot_bar["Low"])
        prev_day_c = float(pivot_bar["Close"])
        pivots = calculate_pivots(prev_day_h, prev_day_l, prev_day_c)

        # Volume Metrics: Trailing 20-bar RVOL & Diurnally Deseasonalized Same-Hour RVOL
        recent_vol_20 = volumes_1h[-21:-1]
        vol_sma20 = sum(recent_vol_20) / len(recent_vol_20) if recent_vol_20 else 1.0
        current_vol = float(volumes_1h[-1])
        rvol_1h = round(current_vol / vol_sma20, 2) if vol_sma20 > 0 else 1.0
        if math.isnan(rvol_1h) or math.isinf(rvol_1h):
            rvol_1h = 1.0

        rvol_1h_diurnal = rvol_1h
        try:
            latest_ts_str = timestamps_1h[-1]
            target_hr = int(latest_ts_str[11:13]) if len(latest_ts_str) >= 13 else -1
            if target_hr >= 0:
                same_hr_vols = [
                    float(volumes_1h[j])
                    for j in range(max(0, len(volumes_1h) - 140), len(volumes_1h) - 1)
                    if len(timestamps_1h[j]) >= 13 and timestamps_1h[j][11:13].isdigit() and int(timestamps_1h[j][11:13]) == target_hr
                ][-20:]
                if len(same_hr_vols) >= 5:
                    med_hr = sorted(same_hr_vols)[len(same_hr_vols) // 2]
                    if med_hr > 0:
                        rvol_1h_diurnal = round(current_vol / med_hr, 2)
        except Exception:
            rvol_1h_diurnal = rvol_1h
        if math.isnan(rvol_1h_diurnal) or math.isinf(rvol_1h_diurnal):
            rvol_1h_diurnal = rvol_1h

        # MACD Z-score over strictly prior 60 bars ([-61:-1], excluding current bar to prevent lookahead)
        window_macd = macd_1h[-61:-1] if len(macd_1h) >= 61 else macd_1h[:-1]
        if window_macd:
            mean_macd = sum(window_macd) / len(window_macd)
            var_macd = sum((x - mean_macd) ** 2 for x in window_macd) / len(window_macd)
            std_macd = math.sqrt(var_macd) if var_macd > 0 else 1e-4
            z_macd = round((macd_1h[-1] - mean_macd) / std_macd, 2)
        else:
            z_macd = 0.0
        if math.isnan(z_macd) or math.isinf(z_macd):
            z_macd = 0.0

        # Crossover Detection
        curr_m = macd_1h[-1]
        curr_s = sig_1h[-1]
        prev_m = macd_1h[-2] if len(macd_1h) >= 2 else curr_m
        prev_s = sig_1h[-2] if len(sig_1h) >= 2 else curr_s

        is_bull_cross = (curr_m > curr_s) and (prev_m <= prev_s)
        is_bear_cross = (curr_m < curr_s) and (prev_m >= prev_s)

        # Check freshness of crossover (0 = triggered on latest closed bar, 1 = 1 bar ago)
        bull_cross_bars_ago = -1
        bear_cross_bars_ago = -1
        for lookback in range(1, min(6, len(macd_1h))):
            if (macd_1h[-lookback] > sig_1h[-lookback]) and (
                macd_1h[-lookback - 1] <= sig_1h[-lookback - 1]
            ):
                bull_cross_bars_ago = lookback - 1
                break
            if (macd_1h[-lookback] < sig_1h[-lookback]) and (
                macd_1h[-lookback - 1] >= sig_1h[-lookback - 1]
            ):
                bear_cross_bars_ago = lookback - 1
                break

        # Dimensionless ATR Squeeze & Compression Check (purely scale-invariant)
        curr_atr = atr_1h[-1] if atr_1h and atr_1h[-1] > 0 else 1.0
        spread = abs(curr_m - curr_s)
        prev_spread = abs(prev_m - prev_s)
        compression_rate = (
            (prev_spread - spread) / prev_spread if prev_spread > 0 else 0.0
        )
        is_imminent = bool(
            (curr_m < curr_s)
            and (compression_rate >= 0.35)
            and (spread / curr_atr <= max(0.08 * abs(curr_s) / curr_atr, 0.05))
        )

        # Quantitative Candle Confirmation Enforcement:
        # Bullish Crossover requires: MACD > Signal (hist > 0) AND confirmed Green Candle (Close >= Open).
        # Bearish Crossover requires: MACD < Signal (hist < 0) AND confirmed Red Candle (Close <= Open).
        has_recent_bull_cross = (
            (is_bull_cross or bull_cross_bars_ago in (0, 1))
            and (curr_m > curr_s)
            and (hist_1h[-1] > 0)
        )
        has_recent_bear_cross = (
            (is_bear_cross or bear_cross_bars_ago in (0, 1))
            and (curr_m < curr_s)
            and (hist_1h[-1] < 0)
        )

        bull_cross_fresh = bool(has_recent_bull_cross and is_green_candle_1h)
        bear_cross_fresh = bool(has_recent_bear_cross and is_red_candle_1h)

        # Signal Classification
        if has_recent_bull_cross:
            if is_green_candle_1h:
                if curr_m < 0:
                    signal_state = "SUB_ZERO_BOUNCE"
                    signal_label = "Sub-Zero Reversal (Confirmed Green)"
                    signal_direction = "BULLISH"
                else:
                    signal_state = "BULLISH_CROSS"
                    signal_label = "Bullish Momentum Crossover (Confirmed Green)"
                    signal_direction = "BULLISH"
            else:
                signal_state = "UNCONFIRMED_CROSS"
                signal_label = "Unconfirmed Bullish Cross (Red Candle)"
                signal_direction = "WATCH"
        elif has_recent_bear_cross:
            if is_red_candle_1h:
                signal_state = "BEARISH_CROSS"
                signal_label = "Bearish Breakdown Crossover (Confirmed Red)"
                signal_direction = "BEARISH"
            else:
                signal_state = "UNCONFIRMED_BEAR_CROSS"
                signal_label = "Unconfirmed Bearish Cross (Green Candle)"
                signal_direction = "WATCH"
        elif is_imminent:
            signal_state = "IMMINENT_SQUEEZE"
            signal_label = "Pre-Close Squeeze Imminent"
            signal_direction = "WATCH"
        elif curr_m > curr_s and hist_1h[-1] > 0:
            signal_state = "BULLISH_TRENDING"
            signal_label = "Bullish Momentum Active"
            signal_direction = "BULLISH"
        else:
            signal_state = "BEARISH_TRENDING"
            signal_label = "Bearish Momentum Active"
            signal_direction = "BEARISH"

        # Floor Pivot Extension & Volatility-Scaled Surge/Plunge Triggers:
        r5 = pivots.get("R5", 0.0)
        r4 = pivots.get("R4", 0.0)
        r3 = pivots.get("R3", 0.0)
        r2 = pivots.get("R2", 0.0)
        s5 = pivots.get("S5", 0.0)
        s4 = pivots.get("S4", 0.0)
        s3 = pivots.get("S3", 0.0)
        s2 = pivots.get("S2", 0.0)

        tau_surge = max(3.5, min(30.0, 2.25 * sigma_yz * 100.0))
        tau_extreme = max(10.0, min(40.0, 3.0 * sigma_yz * 100.0))

        has_pm_price = bool(is_premarket and live_premarket_price is not None and live_premarket_price > 0)
        has_pm_surge = bool(is_premarket and live_premarket_chg_pct is not None and live_premarket_chg_pct >= 10.0)
        has_pm_plunge = bool(is_premarket and live_premarket_chg_pct is not None and live_premarket_chg_pct <= -10.0)

        is_breakout_r5 = bool(r5 > 0 and (latest_price >= r5 or (has_pm_price and live_premarket_price >= r5)))
        is_breakout_r4 = bool(r4 > 0 and (latest_price >= r4 or (has_pm_price and live_premarket_price >= r4)))
        is_breakout_r3 = bool(has_pm_price and r3 > 0 and live_premarket_price >= r3 and (live_premarket_chg_pct or 0) >= 8.0)
        is_velocity_surge = bool(
            (chg_1d_pct >= tau_surge and rvol_1h_diurnal >= 1.25)
            or (chg_1d_pct >= tau_extreme)
            or has_pm_surge
        )
        is_green = bool(is_green_candle_1h or (has_pm_price and (live_premarket_chg_pct or 0) >= 5.0))
        is_breakout = bool(
            (is_breakout_r5 or is_breakout_r4 or is_breakout_r3 or is_velocity_surge)
            and is_green
            and (hist_1h[-1] > 0 or has_pm_surge)
        )

        breakout_type = ""
        if is_breakout:
            if has_pm_surge:
                pm_chg = live_premarket_chg_pct if live_premarket_chg_pct is not None else chg_1d_pct
                if is_breakout_r5:
                    breakout_type = f"Pre-Market Velocity Surge ({pm_chg:+.1f}%) · R5 Pivot Extension"
                elif is_breakout_r4:
                    breakout_type = f"Pre-Market Velocity Surge ({pm_chg:+.1f}%) · R4 Pivot Extension"
                else:
                    breakout_type = f"Pre-Market Velocity Surge ({pm_chg:+.1f}%)"
            elif is_breakout_r5:
                breakout_type = "R5 Floor Pivot Expansion"
            elif is_breakout_r4:
                breakout_type = "R4 Floor Pivot Breakout"
            elif is_breakout_r3:
                breakout_type = "R3 Pre-Market Extension"
            elif is_velocity_surge:
                breakout_type = f"Velocity Surge ({chg_1d_pct:+.1f}% vs {tau_surge:.1f}% YZ)"

        is_breakdown_s5 = bool(s5 > 0 and (latest_price <= s5 or (has_pm_price and live_premarket_price <= s5)))
        is_breakdown_s4 = bool(s4 > 0 and (latest_price <= s4 or (has_pm_price and live_premarket_price <= s4)))
        is_breakdown_s3 = bool(has_pm_price and s3 > 0 and live_premarket_price <= s3 and (live_premarket_chg_pct or 0) <= -8.0)
        is_velocity_plunge = bool(
            (chg_1d_pct <= -tau_surge and rvol_1h_diurnal >= 1.25)
            or (chg_1d_pct <= -tau_extreme)
            or has_pm_plunge
        )
        is_red = bool(is_red_candle_1h or (has_pm_price and (live_premarket_chg_pct or 0) <= -5.0))
        is_breakdown = bool(
            (is_breakdown_s5 or is_breakdown_s4 or is_breakdown_s3 or is_velocity_plunge)
            and is_red
            and (hist_1h[-1] < 0 or has_pm_plunge)
        )

        breakdown_type = ""
        if is_breakdown:
            if is_breakdown_s5:
                breakdown_type = "S5 Floor Pivot Liquidation"
            elif is_breakdown_s4:
                breakdown_type = "S4 Floor Pivot Breakdown"
            elif is_breakdown_s3:
                breakdown_type = "S3 Pre-Market Breakdown"
            elif is_velocity_plunge:
                pm_chg = live_premarket_chg_pct if (is_premarket and live_premarket_chg_pct is not None) else chg_1d_pct
                breakdown_type = (
                    f"Pre-Market Velocity Plunge ({pm_chg:+.1f}%)"
                    if has_pm_plunge
                    else f"Velocity Plunge ({chg_1d_pct:+.1f}% vs -{tau_surge:.1f}% YZ)"
                )

        # Continuous Confluence Score (-100 to +100) via tanh(w^T z)
        z1 = 1.0 if (curr_m > curr_s and hist_1h[-1] > 0) else -1.0
        z2 = 1.0 if hist_1d[-1] > 0 else -1.0
        z3 = 1.0 if rvol_1h_diurnal >= 1.25 and (curr_m > curr_s and is_green_candle_1h) else (-1.0 if rvol_1h_diurnal >= 1.25 and is_red_candle_1h else 0.0)
        z4 = 1.0 if (curr_m < 0 and curr_m > curr_s and is_green_candle_1h) else 0.0
        z5 = 1.0 if is_breakout else (-1.0 if is_breakdown else 0.0)

        u = 0.45 * z1 + 0.45 * z2 + 0.25 * z3 + 0.20 * z4 + 0.25 * z5
        score = int(round(100.0 * math.tanh(u)))
        score = max(-100, min(100, score))

        # Full deep 1H bars for LOD on-demand artifact (up to 730d / ~5,000 bars)
        deep_bars_1h = []
        for i in range(len(closes_1h)):
            deep_bars_1h.append(
                {
                    "dt": timestamps_1h[i],
                    "o": round(float(opens_1h[i]), 2 if opens_1h[i] >= 10 else 3),
                    "h": round(float(highs_1h[i]), 2 if highs_1h[i] >= 10 else 3),
                    "l": round(float(lows_1h[i]), 2 if lows_1h[i] >= 10 else 3),
                    "c": round(float(closes_1h[i]), 2 if closes_1h[i] >= 10 else 3),
                    "macd": round(float(macd_1h[i]), 3),
                    "sig": round(float(sig_1h[i]), 3),
                    "hist": round(float(hist_1h[i]), 3),
                }
            )

        # Retain trailing 120 bars for preview/tests
        chart_bars_1h = deep_bars_1h[-120:] if len(deep_bars_1h) > 120 else deep_bars_1h

        # Full deep 1D bars for LOD on-demand artifact (up to 5y / ~1,260 bars)
        deep_bars_1d = []
        macd_1d = m_1d["macd"]
        sig_1d = m_1d["signal"]
        hist_1d_series = m_1d["hist"]
        for i in range(len(closes_1d)):
            deep_bars_1d.append(
                {
                    "dt": timestamps_1d[i],
                    "o": round(float(opens_1d[i]), 2 if opens_1d[i] >= 10 else 3),
                    "h": round(float(highs_1d[i]), 2 if highs_1d[i] >= 10 else 3),
                    "l": round(float(lows_1d[i]), 2 if lows_1d[i] >= 10 else 3),
                    "c": round(float(closes_1d[i]), 2 if closes_1d[i] >= 10 else 3),
                    "macd": round(float(macd_1d[i]), 3),
                    "sig": round(float(sig_1d[i]), 3),
                    "hist": round(float(hist_1d_series[i]), 3),
                }
            )

        # Retain trailing 252 bars for preview/tests
        chart_bars_1d = deep_bars_1d[-252:] if len(deep_bars_1d) > 252 else deep_bars_1d

        # Parallel volume arrays (kept separate so 8-key OHLC+MACD bar schema remains invariant)
        deep_v_1h = [
            int(round(max(0.0, float(v)))) if (v is not None and math.isfinite(float(v))) else 0
            for v in volumes_1h
        ]
        deep_v_1d = [
            int(round(max(0.0, float(v)))) if (v is not None and math.isfinite(float(v))) else 0
            for v in volumes_1d
        ]
        chart_v_1h = deep_v_1h[-120:] if len(deep_v_1h) > 120 else deep_v_1h
        chart_v_1d = deep_v_1d[-252:] if len(deep_v_1d) > 252 else deep_v_1d

        last_ts = ts_1h_seq[-1]
        as_of_et = timestamps_1h[-1]
        if hasattr(last_ts, "tzinfo") and last_ts.tzinfo is not None:
            as_of_utc = last_ts.astimezone(dt.timezone.utc).strftime("%Y-%m-%d %H:%M")
        else:
            as_of_utc = as_of_et

        return {
            "ticker": ticker,
            "fund_name": catalog_meta.get("fund_name", ticker),
            "leverage": catalog_meta.get("leverage", "1x"),
            "category": clean_category_name(catalog_meta.get("category", "Unclassified")),
            "underlying": catalog_meta.get("underlying", "—"),
            "latest_price": latest_price,
            "open_1h": round(latest_open_1h, 4),
            "high_1h": round(latest_high_1h, 4),
            "low_1h": round(latest_low_1h, 4),
            "close_1h": round(latest_close_1h, 4),
            "candle_color_1h": candle_color_1h,
            "is_green_candle_1h": is_green_candle_1h,
            "is_red_candle_1h": is_red_candle_1h,
            "candle_confirmed": bool(bull_cross_fresh or bear_cross_fresh),
            "live_premarket_price": live_premarket_price,
            "live_premarket_chg_pct": live_premarket_chg_pct,
            "chg_1h_pct": chg_1h_pct,
            "chg_1d_pct": chg_1d_pct,
            "macd_1h": round(curr_m, 4),
            "signal_1h": round(curr_s, 4),
            "hist_1h": round(hist_1h[-1], 4),
            "hist_delta_1h": round(hist_delta_1h[-1], 4),
            "v_macd": round(float(v_macd_1h[-1]), 4) if v_macd_1h else round(curr_m, 4),
            "v_signal": round(float(v_sig_1h[-1]), 4) if v_sig_1h else round(curr_s, 4),
            "v_hist": round(float(v_hist_1h[-1]), 4) if v_hist_1h else round(hist_1h[-1], 4),
            "v_hist_delta": round(float(v_hist_delta_1h[-1]), 4) if v_hist_delta_1h else round(hist_delta_1h[-1], 4),
            "yang_zhang_vol": round(sigma_yz, 4),
            "rvol_1h": rvol_1h,
            "rvol_1h_diurnal": rvol_1h_diurnal,
            "atr_1h": round(float(atr_1h[-1]), 4) if atr_1h else 0.0,
            "z_macd_1h": z_macd,
            "hist_1d": round(hist_1d[-1], 4),
            "daily_trend_bullish": bool(hist_1d[-1] > 0),
            "signal_state": signal_state,
            "signal_label": signal_label,
            "signal_direction": signal_direction,
            "bull_cross_fresh": bull_cross_fresh,
            "bear_cross_fresh": bear_cross_fresh,
            "is_subzero": bool(curr_m < 0),
            "is_breakout": is_breakout,
            "breakout_type": breakout_type,
            "is_breakdown": is_breakdown,
            "breakdown_type": breakdown_type,
            "confluence_score": score,
            "pivots": pivots,
            "as_of_utc": as_of_utc,
            "as_of_et": as_of_et,
            "chart_bars_1h": chart_bars_1h,
            "chart_bars_1d": chart_bars_1d,
            "chart_v_1h": chart_v_1h,
            "chart_v_1d": chart_v_1d,
            "deep_bars_1h": deep_bars_1h,
            "deep_bars_1d": deep_bars_1d,
            "deep_v_1h": deep_v_1h,
            "deep_v_1d": deep_v_1d,
        }
    except Exception:
        return None


def format_institutional_email(
    signals_data: list[dict[str, Any]],
    scan_time_str: str,
    session_info: tuple[str, str, str],
    universe_count: int | None = None,
) -> tuple[str, str]:
    """Build high-DPI responsive HTML & Plain Text alert without emojis or fluff."""
    session_code, session_name, session_badge = session_info
    resolved_universe_count = universe_count if universe_count and universe_count > 0 else len(signals_data)

    tier0_breakouts = [
        s
        for s in signals_data
        if s.get("is_breakout")
    ]
    tier_breakdowns = [
        s
        for s in signals_data
        if s.get("is_breakdown")
    ]
    tier1_subzero = [
        s
        for s in signals_data
        if (s["signal_state"] == "SUB_ZERO_BOUNCE" or (s.get("bull_cross_fresh") and s.get("is_subzero")))
        and s.get("candle_color_1h") == "GREEN"
        and s.get("hist_1h", 0) > 0
    ]
    tier1_bull = [
        s
        for s in signals_data
        if (s["signal_state"] == "BULLISH_CROSS" or (s.get("bull_cross_fresh") and not s.get("is_subzero")))
        and s.get("candle_color_1h") == "GREEN"
        and s.get("hist_1h", 0) > 0
    ]
    tier2_imminent = [s for s in signals_data if s["signal_state"] == "IMMINENT_SQUEEZE"]
    tier_bear = [
        s
        for s in signals_data
        if (s["signal_state"] == "BEARISH_CROSS" or s.get("bear_cross_fresh"))
        and s.get("candle_color_1h") == "RED"
        and s.get("hist_1h", 0) < 0
    ]

    # Sort tiers
    tier0_breakouts.sort(key=lambda x: x.get("chg_1d_pct", 0), reverse=True)
    tier_breakdowns.sort(key=lambda x: x.get("chg_1d_pct", 0))
    tier1_subzero.sort(key=lambda x: x.get("confluence_score", 0), reverse=True)
    tier1_bull.sort(key=lambda x: x.get("confluence_score", 0), reverse=True)
    tier2_imminent.sort(key=lambda x: x.get("confluence_score", 0), reverse=True)

    # 1. Plain Text Format
    pt_lines = [
        "=" * 80,
        f"CONVICTION LABS · QUANTITATIVE MACD RADAR ({session_name})",
        f"SCAN WINDOW: {scan_time_str}",
        "=" * 80,
        "",
        f"[TIER 0: EXTREME MOMENTUM BREAKOUTS & VOLATILITY SURGES] (Active: {len(tier0_breakouts)})",
        "Trigger: Price >= R4/R5 Floor Pivot Extension or YZ Vol-Scaled Surge (>= 2.25 sigma_YZ & Diurnal RVOL >= 1.25x) with Confirmed 1H Green Candle",
        "-" * 80,
    ]

    if tier0_breakouts:
        for it in tier0_breakouts:
            piv = it.get("pivots", {})
            px_display = f"${it['latest_price']:.2f}"
            if it.get("live_premarket_price"):
                pm_chg = it.get("live_premarket_chg_pct")
                chg_str = f" | {pm_chg:+.2f}%" if pm_chg is not None else ""
                px_display += f" (Pre-Market: ${it['live_premarket_price']:.2f}{chg_str})"
            diurnal_rvol = it.get("rvol_1h_diurnal", it["rvol_1h"])
            yz_pct = it.get("yang_zhang_vol", 0.015) * 100.0
            pt_lines.extend(
                [
                    f"TICKER: {it['ticker']:<6} | {it['fund_name']} ({it['leverage']})",
                    f"  Price: {px_display} (1H: {it['chg_1h_pct']:+.2f}% | 24H: {it['chg_1d_pct']:+.2f}%)",
                    f"  Trigger: {it.get('breakout_type', 'Breakout')} | Score: {it.get('confluence_score', 0)}/100",
                    f"  1H Candle: {it.get('candle_color_1h', 'GREEN')} (Confirmed Green: O=${it.get('open_1h', 0):.2f} -> C=${it.get('close_1h', it.get('latest_price', 0)):.2f})",
                    f"  1H MACD: {it['macd_1h']:+.4f} (V-MACD: {it.get('v_macd', 0.0):+.3f}) | Signal: {it['signal_1h']:+.4f} | Hist: {it['hist_1h']:+.4f} (GREEN)",
                    f"  Diurnal RVOL: {diurnal_rvol:.2f}x (20B: {it['rvol_1h']:.2f}x) | YZ Vol: {yz_pct:.2f}% | Z-Score: {it['z_macd_1h']:.2f} sigma",
                    f"  Floor Pivots: P=${piv.get('P', 0):.2f} R3=${piv.get('R3', 0):.2f} R4=${piv.get('R4', 0):.2f} R5=${piv.get('R5', 0):.2f}",
                    "",
                ]
            )
    else:
        pt_lines.extend(["None active in this scan window.", ""])

    pt_lines.extend(
        [
            "",
            f"[TIER 1A: CONFIRMED SUB-ZERO BULLISH REVERSALS] (Active: {len(tier1_subzero)})",
            "Trigger: 1-Hour MACD crossed above Signal Line while MACD < 0 (High Convexity Turnaround)",
            "-" * 80,
        ]
    )

    for it in tier1_subzero:
        piv = it.get("pivots", {})
        px_display = f"${it['latest_price']:.2f}"
        if it.get("live_premarket_price"):
            pm_chg = it.get('live_premarket_chg_pct')
            chg_str = f" | {pm_chg:+.2f}%" if pm_chg is not None else ""
            px_display += f" (Pre-Market: ${it['live_premarket_price']:.2f}{chg_str})"
        diurnal_rvol = it.get("rvol_1h_diurnal", it["rvol_1h"])
        yz_pct = it.get("yang_zhang_vol", 0.015) * 100.0
        pt_lines.extend(
            [
                f"TICKER: {it['ticker']:<6} | {it['fund_name']} ({it['leverage']})",
                f"  Price: {px_display} (1H: {it['chg_1h_pct']:+.2f}% | 24H: {it['chg_1d_pct']:+.2f}%)",
                f"  1H Candle: {it.get('candle_color_1h', 'GREEN')} (Confirmed Green: O=${it.get('open_1h', 0):.2f} -> C=${it.get('close_1h', it.get('latest_price', 0)):.2f})",
                f"  1H MACD: {it['macd_1h']:+.4f} (V-MACD: {it.get('v_macd', 0.0):+.3f}) | Signal: {it['signal_1h']:+.4f} | Hist: {it['hist_1h']:+.4f} (GREEN)",
                f"  Diurnal RVOL: {diurnal_rvol:.2f}x | YZ Vol: {yz_pct:.2f}% | Z-Score: {it['z_macd_1h']:.2f} sigma | Score: {it['confluence_score']}/100",
                f"  Floor Pivots: S2=${piv.get('S2', 0):.2f} S1=${piv.get('S1', 0):.2f} P=${piv.get('P', 0):.2f} R1=${piv.get('R1', 0):.2f}",
                f"  1D Trend: {'BULLISH' if it['daily_trend_bullish'] else 'BEARISH'}",
                "",
            ]
        )

    pt_lines.extend(
        [
            "",
            f"[TIER 1B: CONFIRMED SUPRA-ZERO BULLISH CROSSOVERS] (Active: {len(tier1_bull)})",
            "Trigger: 1-Hour MACD crossed above Signal Line while MACD >= 0 (Momentum Continuation)",
            "-" * 80,
        ]
    )

    for it in tier1_bull:
        piv = it.get("pivots", {})
        px_display = f"${it['latest_price']:.2f}"
        if it.get("live_premarket_price"):
            pm_chg = it.get('live_premarket_chg_pct')
            chg_str = f" | {pm_chg:+.2f}%" if pm_chg is not None else ""
            px_display += f" (Pre-Market: ${it['live_premarket_price']:.2f}{chg_str})"
        diurnal_rvol = it.get("rvol_1h_diurnal", it["rvol_1h"])
        yz_pct = it.get("yang_zhang_vol", 0.015) * 100.0
        pt_lines.extend(
            [
                f"TICKER: {it['ticker']:<6} | {it['fund_name']} ({it['leverage']})",
                f"  Price: {px_display} (1H: {it['chg_1h_pct']:+.2f}% | 24H: {it['chg_1d_pct']:+.2f}%)",
                f"  1H Candle: {it.get('candle_color_1h', 'GREEN')} (Confirmed Green: O=${it.get('open_1h', 0):.2f} -> C=${it.get('close_1h', it.get('latest_price', 0)):.2f})",
                f"  1H MACD: {it['macd_1h']:+.4f} (V-MACD: {it.get('v_macd', 0.0):+.3f}) | Signal: {it['signal_1h']:+.4f} | Hist: {it['hist_1h']:+.4f} (GREEN)",
                f"  Diurnal RVOL: {diurnal_rvol:.2f}x | YZ Vol: {yz_pct:.2f}% | Z-Score: {it['z_macd_1h']:.2f} sigma | Score: {it['confluence_score']}/100",
                f"  Floor Pivots: S2=${piv.get('S2', 0):.2f} S1=${piv.get('S1', 0):.2f} P=${piv.get('P', 0):.2f} R1=${piv.get('R1', 0):.2f}",
                f"  1D Trend: {'BULLISH' if it['daily_trend_bullish'] else 'BEARISH'}",
                "",
            ]
        )

    pt_lines.extend(
        [
            "",
            f"[TIER 2: PRE-CLOSE SQUEEZE RADAR] (Impending: {len(tier2_imminent)})",
            "Trigger: Histogram spread compression >= 35% within ATR-scaled convergence tolerance",
            "-" * 80,
        ]
    )
    for it in tier2_imminent[:8]:
        spread = abs(it["macd_1h"] - it["signal_1h"])
        atr_val = it.get("atr_1h", 0.0)
        atr_str = f" ({spread / atr_val:.3f}x ATR)" if atr_val > 0 else ""
        diurnal_rvol = it.get("rvol_1h_diurnal", it["rvol_1h"])
        pt_lines.append(
            f"{it['ticker']:<6} | Px: ${it['latest_price']:<7.2f} | Spread: {spread:.4f}{atr_str} | Diurnal RVOL: {diurnal_rvol:3.1f}x | {it['fund_name']}"
        )

    if tier_breakdowns:
        pt_lines.extend(
            [
                "",
                f"[CRITICAL CAPITAL PRESERVATION: SEVERE BREAKDOWNS & LIQUIDATIONS] (Active: {len(tier_breakdowns)})",
                "Trigger: Price <= S4/S5 Floor Pivot Support or YZ Vol-Scaled Plunge (<= -2.25 sigma_YZ & Diurnal RVOL >= 1.25x) with Confirmed 1H Red Candle",
                "-" * 80,
            ]
        )
        for it in tier_breakdowns[:12]:
            piv = it.get("pivots", {})
            pt_lines.append(
                f"{it['ticker']:<6} | Px: ${it['latest_price']:<7.2f} | 24H: {it['chg_1d_pct']:+.2f}% | Trigger: {it.get('breakdown_type', 'Breakdown')} | S4=${piv.get('S4', 0):.2f} S5=${piv.get('S5', 0):.2f} | ACTION: MANDATORY EXIT / CASH PRESERVATION"
            )

    pt_lines.extend(
        [
            "",
            f"[TIER 3: BEARISH MACD CROSSOVER / CASH PARKING] (Active Exits: {len(tier_bear)})",
            "Trigger: 1-Hour MACD crossed below Signal Line. Longs liquidate to cash immediately.",
            "-" * 80,
        ]
    )
    for it in tier_bear[:8]:
        pt_lines.append(
            f"{it['ticker']:<6} | Px: ${it['latest_price']:<7.2f} | 1H Candle: {it.get('candle_color_1h', 'RED')} | 1H MACD: {it['macd_1h']:+6.3f} | Hist: {it['hist_1h']:+6.3f} (RED) | ACTION: EXIT TO CASH"
        )

    pt_lines.extend(
        [
            "",
            "=" * 80,
            "RISK & EXECUTION PROTOCOL:",
            "Leveraged ETFs are subject to constant daily leverage resets and volatility drag.",
            "Historical quantitative simulations prove cash parking beats continuous holding.",
            "Interactive dashboard: https://yieldchaser.github.io/etf-data/markets.html?tab=flows&view=macd_strategy",
            "=" * 80,
        ]
    )
    text_content = "\n".join(pt_lines)

    # 2-A0. HTML Card Generation for Tier 0 Breakouts
    cards_breakout_html = []
    for it in tier0_breakouts:
        piv = it.get("pivots", {})
        p = piv.get("P", 0.0)
        r3 = piv.get("R3", 0.0)
        r4 = piv.get("R4", 0.0)
        r5 = piv.get("R5", 0.0)
        chg_1h = it.get("chg_1h_pct", 0.0)
        chg_1h_color = "#34d399" if chg_1h >= 0 else "#fb7185"
        chg_24h = it.get("chg_1d_pct", 0.0)
        chg_24h_color = "#34d399" if chg_24h >= 0 else "#fb7185"
        score = it.get("confluence_score", 0)
        score_color = "#34d399" if score >= 40 else ("#38bdf8" if score >= 0 else "#facc15")
        px_display = f"${it['latest_price']:.2f}"
        pm_display_html = ""
        if it.get("live_premarket_price"):
            pm_chg = it.get("live_premarket_chg_pct")
            pm_color = "#34d399" if (pm_chg or 0) >= 0 else "#fb7185"
            chg_badge = f" ({pm_chg:+.2f}%)" if pm_chg is not None else ""
            pm_display_html = f'<div style="font-size:10px; color:{pm_color}; font-weight:700; margin-top:2px;">Pre-Mkt: ${it["live_premarket_price"]:.2f}{chg_badge}</div>'

        safe_ticker = html_lib.escape(str(it["ticker"]), quote=True)
        safe_lev = html_lib.escape(str(it.get("leverage", "1x")), quote=True)
        safe_breakout = html_lib.escape(str(it.get("breakout_type", "VOLATILITY BREAKOUT")), quote=True)
        safe_name = html_lib.escape(str(it.get("fund_name", it["ticker"])), quote=True)
        safe_und = html_lib.escape(str(it.get("underlying", "—")), quote=True)

        card = f"""
        <!-- CARD: {safe_ticker} BREAKOUT -->
        <table role="presentation" width="100%" border="0" cellspacing="0" cellpadding="0" style="background-color:#140e1f; border:1px solid #3b2054; border-left:4px solid #c084fc; border-radius:6px; margin-bottom:14px; overflow:hidden;">
          <tr>
            <td style="padding:14px 16px;">
              <table role="presentation" width="100%" border="0" cellspacing="0" cellpadding="0">
                <tr>
                  <td style="vertical-align:middle;">
                    <span style="font-family:ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size:19px; font-weight:800; color:#c084fc; letter-spacing:0.03em;">{safe_ticker}</span>
                    <span style="display:inline-block; margin:2px 2px 2px 6px; background-color:#2e1065; color:#e9d5ff; padding:2px 7px; border-radius:3px; font-size:10.5px; font-weight:700; font-family:ui-monospace, monospace;">{safe_lev}</span>
                    <span style="display:inline-block; margin:2px 2px 2px 4px; background-color:rgba(192,132,252,0.2); color:#c084fc; border:1px solid rgba(192,132,252,0.45); padding:2px 7px; border-radius:3px; font-size:9.5px; font-weight:800; text-transform:uppercase; letter-spacing:0.04em;">{safe_breakout}</span>
                    <span style="display:inline-block; margin:2px 2px 2px 4px; background-color:rgba(16,185,129,0.2); color:#34d399; border:1px solid rgba(52,211,153,0.4); padding:2px 7px; border-radius:3px; font-size:9.5px; font-weight:700; text-transform:uppercase; letter-spacing:0.04em;">1H CANDLE: GREEN</span>
                  </td>
                  <td style="text-align:right; vertical-align:middle; white-space:nowrap; padding-left:12px;">
                    <div style="font-family:ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size:18px; font-weight:800; color:#ffffff;">{px_display}</div>
                    {pm_display_html}
                  </td>
                </tr>
                <tr>
                  <td style="padding-top:4px; font-size:11.5px; color:#cbd5e1;">
                    {safe_name} &middot; <span style="color:#94a3b8;">Underlying: {safe_und}</span>
                  </td>
                  <td style="padding-top:4px; text-align:right; font-family:ui-monospace, monospace; font-size:11.5px;">
                    <span style="color:{chg_1h_color}; font-weight:700;">1H: {chg_1h:+.2f}%</span>
                    <span style="color:#64748b; margin:0 4px;">|</span>
                    <span style="color:{chg_24h_color}; font-weight:800;">24H: {chg_24h:+.2f}%</span>
                  </td>
                </tr>
              </table>

              <table role="presentation" width="100%" border="0" cellspacing="0" cellpadding="0" style="margin-top:12px; background-color:#0b0814; border:1px solid #3b2054; border-radius:4px; table-layout:fixed;">
                <tr>
                  <td style="padding:8px 8px; border-right:1px solid #3b2054; width:25%;">
                    <div style="font-size:9px; color:#c084fc; text-transform:uppercase; letter-spacing:0.05em; font-weight:700;">1H MACD</div>
                    <div style="font-family:ui-monospace, monospace; font-size:13px; font-weight:700; color:#c084fc; margin-top:2px;">
                      {it.get('macd_1h', 0):+.3f}
                    </div>
                    <div style="font-family:ui-monospace, monospace; font-size:9.5px; color:#94a3b8; margin-top:1px;">
                      Sig: {it.get('signal_1h', 0):+.3f} &middot; V: {it.get('v_macd', 0.0):+.2f}
                    </div>
                  </td>
                  <td style="padding:8px 8px; border-right:1px solid #3b2054; width:25%;">
                    <div style="font-size:9px; color:#94a3b8; text-transform:uppercase; letter-spacing:0.05em; font-weight:700;">HISTOGRAM</div>
                    <div style="font-family:ui-monospace, monospace; font-size:13px; font-weight:700; color:#34d399; margin-top:2px;">
                      {it.get('hist_1h', 0):+.4f}
                    </div>
                    <div style="font-family:ui-monospace, monospace; font-size:9.5px; color:#94a3b8; margin-top:1px;">
                      Delta: {it.get('hist_delta_1h', 0):+.4f}
                    </div>
                  </td>
                  <td style="padding:8px 8px; border-right:1px solid #3b2054; width:25%;">
                    <div style="font-size:9px; color:#94a3b8; text-transform:uppercase; letter-spacing:0.05em; font-weight:700;">DIURNAL RVOL</div>
                    <div style="font-family:ui-monospace, monospace; font-size:13px; font-weight:700; color:#e2e8f0; margin-top:2px;">
                      {it.get('rvol_1h_diurnal', it.get('rvol_1h', 1.0)):.2f}x
                    </div>
                    <div style="font-family:ui-monospace, monospace; font-size:9.5px; color:#94a3b8; margin-top:1px;">
                      20B: {it.get('rvol_1h', 1.0):.2f}x &middot; YZ {it.get('yang_zhang_vol', 0.015) * 100:.1f}%
                    </div>
                  </td>
                  <td style="padding:8px 8px; width:25%;">
                    <div style="font-size:9px; color:#94a3b8; text-transform:uppercase; letter-spacing:0.05em; font-weight:700;">SCORE</div>
                    <div style="font-family:ui-monospace, monospace; font-size:13px; font-weight:800; color:{score_color}; margin-top:2px;">
                      {score:+d}/100
                    </div>
                    <div style="font-family:ui-monospace, monospace; font-size:9.5px; color:#94a3b8; margin-top:1px;">
                      Confluence
                    </div>
                  </td>
                </tr>
              </table>

              <table role="presentation" width="100%" border="0" cellspacing="0" cellpadding="0" style="margin-top:10px; background-color:#0b0814; border:1px solid #3b2054; border-radius:4px; font-size:11px;">
                <tr>
                  <td style="padding:7px 10px;">
                    <table role="presentation" width="100%" border="0" cellspacing="0" cellpadding="0">
                      <tr>
                        <td style="font-size:9.5px; font-weight:700; color:#c084fc; text-transform:uppercase; letter-spacing:0.04em;">
                          PIVOT RESISTANCE EXTENSION
                        </td>
                        <td style="text-align:right; font-family:ui-monospace, monospace; font-size:11px;">
                          <span style="color:#64748b;">P:</span> <span style="color:#94a3b8;">${p:.2f}</span> &nbsp;
                          <span style="color:#64748b;">R3:</span> <span style="color:#cbd5e1;">${r3:.2f}</span> &nbsp;
                          <span style="color:#c084fc;">R4:</span> <strong style="color:#c084fc;">${r4:.2f}</strong> &nbsp;
                          <span style="color:#e9d5ff;">R5:</span> <strong style="color:#e9d5ff;">${r5:.2f}</strong>
                        </td>
                      </tr>
                    </table>
                  </td>
                </tr>
              </table>

            </td>
          </tr>
        </table>
        """
        cards_breakout_html.append(card)

    cards_breakout_block = (
        "\n".join(cards_breakout_html)
        if cards_breakout_html
        else """
    <div style="padding:16px; background-color:#111622; border:1px solid #1e2638; border-radius:6px; color:#94a3b8; text-align:center; font-size:12px;">
      No active Floor Pivot (R4/R5) or extreme velocity surge breakouts in this scan window.
    </div>
    """
    )

    # 2. HTML Card Generation for Tier 1A Sub-Zero Reversals
    cards_html = []
    for it in tier1_subzero:
        piv = it.get("pivots", {})
        p = piv.get("P", 0.0)
        s1 = piv.get("S1", 0.0)
        s2 = piv.get("S2", 0.0)
        r1 = piv.get("R1", 0.0)
        z = it.get("z_macd_1h", 0.0)
        regime_desc = "Sub-Zero Exhaustion" if z <= -0.5 else "Sub-Zero Mean Reversion"
        is_bull_trend = it.get("daily_trend_bullish", False)
        trend_text = "BULLISH (DUAL TIMEFRAME)" if is_bull_trend else "BEARISH (COUNTER-TREND ALPHA)"
        trend_color = "#34d399" if is_bull_trend else "#fb7185"
        chg_1h = it.get("chg_1h_pct", 0.0)
        chg_1h_color = "#34d399" if chg_1h >= 0 else "#fb7185"
        chg_24h = it.get("chg_1d_pct", 0.0)
        chg_24h_color = "#34d399" if chg_24h >= 0 else "#fb7185"
        score = it.get("confluence_score", 0)
        score_color = "#34d399" if score >= 40 else ("#38bdf8" if score >= 0 else "#facc15")

        px_display = f"${it['latest_price']:.2f}"
        pm_display_html = ""
        if it.get("live_premarket_price"):
            pm_chg = it.get('live_premarket_chg_pct')
            pm_color = '#34d399' if (pm_chg or 0) >= 0 else '#fb7185'
            chg_badge = f" ({pm_chg:+.2f}%)" if pm_chg is not None else ""
            pm_display_html = f'<div style="font-size:10px; color:{pm_color}; font-weight:700; margin-top:2px;">Pre-Mkt: ${it["live_premarket_price"]:.2f}{chg_badge}</div>'

        safe_ticker = html_lib.escape(str(it["ticker"]), quote=True)
        safe_lev = html_lib.escape(str(it.get("leverage", "1x")), quote=True)
        safe_name = html_lib.escape(str(it.get("fund_name", it["ticker"])), quote=True)
        safe_und = html_lib.escape(str(it.get("underlying", "—")), quote=True)

        card = f"""
        <!-- CARD: {safe_ticker} -->
        <table role="presentation" width="100%" border="0" cellspacing="0" cellpadding="0" style="background-color:#111622; border:1px solid #1e2638; border-left:4px solid #38bdf8; border-radius:6px; margin-bottom:14px; overflow:hidden;">
          <tr>
            <td style="padding:14px 16px;">
              <!-- Header Row: Ticker, Badges, Price -->
              <table role="presentation" width="100%" border="0" cellspacing="0" cellpadding="0">
                <tr>
                  <td style="vertical-align:middle;">
                    <span style="font-family:ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size:19px; font-weight:800; color:#38bdf8; letter-spacing:0.03em;">{safe_ticker}</span>
                    <span style="display:inline-block; margin:2px 2px 2px 6px; background-color:#1e2638; color:#cbd5e1; padding:2px 7px; border-radius:3px; font-size:10.5px; font-weight:700; font-family:ui-monospace, monospace;">{safe_lev}</span>
                    <span style="display:inline-block; margin:2px 2px 2px 4px; background-color:rgba(16,185,129,0.15); color:#34d399; border:1px solid rgba(52,211,153,0.35); padding:2px 7px; border-radius:3px; font-size:9.5px; font-weight:700; text-transform:uppercase; letter-spacing:0.04em;">SUB-ZERO REVERSAL</span>
                    <span style="display:inline-block; margin:2px 2px 2px 4px; background-color:rgba(16,185,129,0.2); color:#34d399; border:1px solid rgba(52,211,153,0.4); padding:2px 7px; border-radius:3px; font-size:9.5px; font-weight:700; text-transform:uppercase; letter-spacing:0.04em;">1H CANDLE: GREEN (CONFIRMED)</span>
                  </td>
                  <td style="text-align:right; vertical-align:middle; white-space:nowrap; padding-left:12px;">
                    <div style="font-family:ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size:18px; font-weight:800; color:#ffffff;">{px_display}</div>
                    {pm_display_html}
                  </td>
                </tr>
                <tr>
                  <td style="padding-top:4px; font-size:11.5px; color:#cbd5e1;">
                    {safe_name} &middot; <span style="color:#94a3b8;">Underlying: {safe_und}</span>
                  </td>
                  <td style="padding-top:4px; text-align:right; font-family:ui-monospace, monospace; font-size:11.5px;">
                    <span style="color:{chg_1h_color}; font-weight:700;">1H: {chg_1h:+.2f}%</span>
                    <span style="color:#64748b; margin:0 4px;">|</span>
                    <span style="color:{chg_24h_color};">24H: {chg_24h:+.2f}%</span>
                  </td>
                </tr>
              </table>

              <!-- 4-Box Metric Strip (Optimized for Mobile + Laptop) -->
              <table role="presentation" width="100%" border="0" cellspacing="0" cellpadding="0" style="margin-top:12px; background-color:#0b0f17; border:1px solid #1e2638; border-radius:4px; table-layout:fixed;">
                <tr>
                  <td style="padding:8px 8px; border-right:1px solid #1e2638; width:25%;">
                    <div style="font-size:9px; color:#94a3b8; text-transform:uppercase; letter-spacing:0.05em; font-weight:700;">1H MACD</div>
                    <div style="font-family:ui-monospace, monospace; font-size:13px; font-weight:700; color:#38bdf8; margin-top:2px;">
                      {it.get('macd_1h', 0):+.3f}
                    </div>
                    <div style="font-family:ui-monospace, monospace; font-size:9.5px; color:#94a3b8; margin-top:1px;">
                      Sig: {it.get('signal_1h', 0):+.3f} &middot; V: {it.get('v_macd', 0.0):+.2f}
                    </div>
                  </td>
                  <td style="padding:8px 8px; border-right:1px solid #1e2638; width:25%;">
                    <div style="font-size:9px; color:#94a3b8; text-transform:uppercase; letter-spacing:0.05em; font-weight:700;">HISTOGRAM</div>
                    <div style="font-family:ui-monospace, monospace; font-size:13px; font-weight:700; color:#34d399; margin-top:2px;">
                      {it.get('hist_1h', 0):+.4f}
                    </div>
                    <div style="font-family:ui-monospace, monospace; font-size:9.5px; color:#94a3b8; margin-top:1px;">
                      Delta: {it.get('hist_delta_1h', 0):+.4f}
                    </div>
                  </td>
                  <td style="padding:8px 8px; border-right:1px solid #1e2638; width:25%;">
                    <div style="font-size:9px; color:#94a3b8; text-transform:uppercase; letter-spacing:0.05em; font-weight:700;">DIURNAL RVOL</div>
                    <div style="font-family:ui-monospace, monospace; font-size:13px; font-weight:700; color:#e2e8f0; margin-top:2px;">
                      {it.get('rvol_1h_diurnal', it.get('rvol_1h', 1.0)):.2f}x
                    </div>
                    <div style="font-family:ui-monospace, monospace; font-size:9.5px; color:#94a3b8; margin-top:1px;">
                      20B: {it.get('rvol_1h', 1.0):.2f}x &middot; YZ {it.get('yang_zhang_vol', 0.015) * 100:.1f}%
                    </div>
                  </td>
                  <td style="padding:8px 8px; width:25%;">
                    <div style="font-size:9px; color:#94a3b8; text-transform:uppercase; letter-spacing:0.05em; font-weight:700;">SCORE</div>
                    <div style="font-family:ui-monospace, monospace; font-size:13px; font-weight:800; color:{score_color}; margin-top:2px;">
                      {score:+d}/100
                    </div>
                    <div style="font-family:ui-monospace, monospace; font-size:9.5px; color:#94a3b8; margin-top:1px;">
                      Confluence
                    </div>
                  </td>
                </tr>
              </table>

              <!-- Structured Floor Pivots & Macro Alignment Box -->
              <table role="presentation" width="100%" border="0" cellspacing="0" cellpadding="0" style="margin-top:10px; background-color:#0b0f17; border:1px solid #1e2638; border-radius:4px; font-size:11px;">
                <tr>
                  <td style="padding:7px 10px; border-bottom:1px solid #1e2638;">
                    <table role="presentation" width="100%" border="0" cellspacing="0" cellpadding="0">
                      <tr>
                        <td style="font-size:9.5px; font-weight:700; color:#94a3b8; text-transform:uppercase; letter-spacing:0.04em;">
                          FLOOR PIVOTS (AUTO)
                        </td>
                        <td style="text-align:right; font-family:ui-monospace, monospace; font-size:11px;">
                          <span style="color:#64748b;">S2:</span> <span style="color:#38bdf8; font-weight:700;">${s2:.2f}</span> &nbsp;
                          <span style="color:#64748b;">S1:</span> <span style="color:#38bdf8; font-weight:700;">${s1:.2f}</span> &nbsp;
                          <span style="color:#94a3b8;">P:</span> <strong style="color:#ffffff;">${p:.2f}</strong> &nbsp;
                          <span style="color:#64748b;">R1:</span> <span style="color:#fb7185; font-weight:700;">${r1:.2f}</span>
                        </td>
                      </tr>
                    </table>
                  </td>
                </tr>
                <tr>
                  <td style="padding:7px 10px;">
                    <table role="presentation" width="100%" border="0" cellspacing="0" cellpadding="0">
                      <tr>
                        <td style="font-size:10.5px; color:#cbd5e1;">
                          Regime: <strong style="color:#ffffff;">{z:.2f}&sigma;</strong> ({regime_desc})
                        </td>
                        <td style="text-align:right; font-size:10.5px;">
                          1D Macro: <strong style="color:{trend_color};">{trend_text}</strong>
                        </td>
                      </tr>
                    </table>
                  </td>
                </tr>
              </table>

            </td>
          </tr>
        </table>
        """
        cards_html.append(card)

    cards_block = (
        "\n".join(cards_html)
        if cards_html
        else """
    <div style="padding:16px; background-color:#111622; border:1px solid #1e2638; border-radius:6px; color:#94a3b8; text-align:center; font-size:12px;">
      No active sub-zero crossovers triggered during this diagnostic window.
    </div>
    """
    )

    # 2B. HTML Card Generation for Tier 1B Supra-Zero Crossovers
    cards_bull_html = []
    for it in tier1_bull:
        piv = it.get("pivots", {})
        p = piv.get("P", 0.0)
        s1 = piv.get("S1", 0.0)
        s2 = piv.get("S2", 0.0)
        r1 = piv.get("R1", 0.0)
        z = it.get("z_macd_1h", 0.0)
        regime_desc = "Supra-Zero Momentum Expansion"
        is_bull_trend = it.get("daily_trend_bullish", False)
        trend_text = "BULLISH (DUAL TIMEFRAME)" if is_bull_trend else "BEARISH (PULLBACK RISK)"
        trend_color = "#34d399" if is_bull_trend else "#fb7185"
        chg_1h = it.get("chg_1h_pct", 0.0)
        chg_1h_color = "#34d399" if chg_1h >= 0 else "#fb7185"
        chg_24h = it.get("chg_1d_pct", 0.0)
        chg_24h_color = "#34d399" if chg_24h >= 0 else "#fb7185"
        score = it.get("confluence_score", 0)
        score_color = "#34d399" if score >= 40 else ("#38bdf8" if score >= 0 else "#facc15")

        px_display = f"${it['latest_price']:.2f}"
        pm_display_html = ""
        if it.get("live_premarket_price"):
            pm_chg = it.get('live_premarket_chg_pct')
            pm_color = '#34d399' if (pm_chg or 0) >= 0 else '#fb7185'
            chg_badge = f" ({pm_chg:+.2f}%)" if pm_chg is not None else ""
            pm_display_html = f'<div style="font-size:10px; color:{pm_color}; font-weight:700; margin-top:2px;">Pre-Mkt: ${it["live_premarket_price"]:.2f}{chg_badge}</div>'

        safe_ticker = html_lib.escape(str(it["ticker"]), quote=True)
        safe_lev = html_lib.escape(str(it.get("leverage", "1x")), quote=True)
        safe_name = html_lib.escape(str(it.get("fund_name", it["ticker"])), quote=True)
        safe_und = html_lib.escape(str(it.get("underlying", "—")), quote=True)

        card = f"""
        <!-- CARD: {safe_ticker} -->
        <table role="presentation" width="100%" border="0" cellspacing="0" cellpadding="0" style="background-color:#111622; border:1px solid #1e2638; border-left:4px solid #10b981; border-radius:6px; margin-bottom:14px; overflow:hidden;">
          <tr>
            <td style="padding:14px 16px;">
              <!-- Header Row: Ticker, Badges, Price -->
              <table role="presentation" width="100%" border="0" cellspacing="0" cellpadding="0">
                <tr>
                  <td style="vertical-align:middle;">
                    <span style="font-family:ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size:19px; font-weight:800; color:#34d399; letter-spacing:0.03em;">{safe_ticker}</span>
                    <span style="display:inline-block; margin:2px 2px 2px 6px; background-color:#1e2638; color:#cbd5e1; padding:2px 7px; border-radius:3px; font-size:10.5px; font-weight:700; font-family:ui-monospace, monospace;">{safe_lev}</span>
                    <span style="display:inline-block; margin:2px 2px 2px 4px; background-color:rgba(16,185,129,0.15); color:#34d399; border:1px solid rgba(52,211,153,0.35); padding:2px 7px; border-radius:3px; font-size:9.5px; font-weight:700; text-transform:uppercase; letter-spacing:0.04em;">SUPRA-ZERO CROSS</span>
                    <span style="display:inline-block; margin:2px 2px 2px 4px; background-color:rgba(16,185,129,0.2); color:#34d399; border:1px solid rgba(52,211,153,0.4); padding:2px 7px; border-radius:3px; font-size:9.5px; font-weight:700; text-transform:uppercase; letter-spacing:0.04em;">1H CANDLE: GREEN (CONFIRMED)</span>
                  </td>
                  <td style="text-align:right; vertical-align:middle; white-space:nowrap; padding-left:12px;">
                    <div style="font-family:ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size:18px; font-weight:800; color:#ffffff;">{px_display}</div>
                    {pm_display_html}
                  </td>
                </tr>
                <tr>
                  <td style="padding-top:4px; font-size:11.5px; color:#cbd5e1;">
                    {safe_name} &middot; <span style="color:#94a3b8;">Underlying: {safe_und}</span>
                  </td>
                  <td style="padding-top:4px; text-align:right; font-family:ui-monospace, monospace; font-size:11.5px;">
                    <span style="color:{chg_1h_color}; font-weight:700;">1H: {chg_1h:+.2f}%</span>
                    <span style="color:#64748b; margin:0 4px;">|</span>
                    <span style="color:{chg_24h_color};">24H: {chg_24h:+.2f}%</span>
                  </td>
                </tr>
              </table>

              <!-- 4-Box Metric Strip -->
              <table role="presentation" width="100%" border="0" cellspacing="0" cellpadding="0" style="margin-top:12px; background-color:#0b0f17; border:1px solid #1e2638; border-radius:4px; table-layout:fixed;">
                <tr>
                  <td style="padding:8px 8px; border-right:1px solid #1e2638; width:25%;">
                    <div style="font-size:9px; color:#94a3b8; text-transform:uppercase; letter-spacing:0.05em; font-weight:700;">1H MACD</div>
                    <div style="font-family:ui-monospace, monospace; font-size:13px; font-weight:700; color:#38bdf8; margin-top:2px;">
                      {it.get('macd_1h', 0):+.3f}
                    </div>
                    <div style="font-family:ui-monospace, monospace; font-size:9.5px; color:#94a3b8; margin-top:1px;">
                      Sig: {it.get('signal_1h', 0):+.3f} &middot; V: {it.get('v_macd', 0.0):+.2f}
                    </div>
                  </td>
                  <td style="padding:8px 8px; border-right:1px solid #1e2638; width:25%;">
                    <div style="font-size:9px; color:#94a3b8; text-transform:uppercase; letter-spacing:0.05em; font-weight:700;">HISTOGRAM</div>
                    <div style="font-family:ui-monospace, monospace; font-size:13px; font-weight:700; color:#34d399; margin-top:2px;">
                      {it.get('hist_1h', 0):+.4f}
                    </div>
                    <div style="font-family:ui-monospace, monospace; font-size:9.5px; color:#94a3b8; margin-top:1px;">
                      Delta: {it.get('hist_delta_1h', 0):+.4f}
                    </div>
                  </td>
                  <td style="padding:8px 8px; border-right:1px solid #1e2638; width:25%;">
                    <div style="font-size:9px; color:#94a3b8; text-transform:uppercase; letter-spacing:0.05em; font-weight:700;">DIURNAL RVOL</div>
                    <div style="font-family:ui-monospace, monospace; font-size:13px; font-weight:700; color:#e2e8f0; margin-top:2px;">
                      {it.get('rvol_1h_diurnal', it.get('rvol_1h', 1.0)):.2f}x
                    </div>
                    <div style="font-family:ui-monospace, monospace; font-size:9.5px; color:#94a3b8; margin-top:1px;">
                      20B: {it.get('rvol_1h', 1.0):.2f}x &middot; YZ {it.get('yang_zhang_vol', 0.015) * 100:.1f}%
                    </div>
                  </td>
                  <td style="padding:8px 8px; width:25%;">
                    <div style="font-size:9px; color:#94a3b8; text-transform:uppercase; letter-spacing:0.05em; font-weight:700;">SCORE</div>
                    <div style="font-family:ui-monospace, monospace; font-size:13px; font-weight:800; color:{score_color}; margin-top:2px;">
                      {score:+d}/100
                    </div>
                    <div style="font-family:ui-monospace, monospace; font-size:9.5px; color:#94a3b8; margin-top:1px;">
                      Confluence
                    </div>
                  </td>
                </tr>
              </table>

              <!-- Structured Floor Pivots & Macro Alignment Box -->
              <table role="presentation" width="100%" border="0" cellspacing="0" cellpadding="0" style="margin-top:10px; background-color:#0b0f17; border:1px solid #1e2638; border-radius:4px; font-size:11px;">
                <tr>
                  <td style="padding:7px 10px; border-bottom:1px solid #1e2638;">
                    <table role="presentation" width="100%" border="0" cellspacing="0" cellpadding="0">
                      <tr>
                        <td style="font-size:9.5px; font-weight:700; color:#94a3b8; text-transform:uppercase; letter-spacing:0.04em;">
                          FLOOR PIVOTS (AUTO)
                        </td>
                        <td style="text-align:right; font-family:ui-monospace, monospace; font-size:11px;">
                          <span style="color:#64748b;">S2:</span> <span style="color:#38bdf8; font-weight:700;">${s2:.2f}</span> &nbsp;
                          <span style="color:#64748b;">S1:</span> <span style="color:#38bdf8; font-weight:700;">${s1:.2f}</span> &nbsp;
                          <span style="color:#94a3b8;">P:</span> <strong style="color:#ffffff;">${p:.2f}</strong> &nbsp;
                          <span style="color:#64748b;">R1:</span> <span style="color:#fb7185; font-weight:700;">${r1:.2f}</span>
                        </td>
                      </tr>
                    </table>
                  </td>
                </tr>
                <tr>
                  <td style="padding:7px 10px;">
                    <table role="presentation" width="100%" border="0" cellspacing="0" cellpadding="0">
                      <tr>
                        <td style="font-size:10.5px; color:#cbd5e1;">
                          Regime: <strong style="color:#ffffff;">{z:.2f}&sigma;</strong> ({regime_desc})
                        </td>
                        <td style="text-align:right; font-size:10.5px;">
                          1D Macro: <strong style="color:{trend_color};">{trend_text}</strong>
                        </td>
                      </tr>
                    </table>
                  </td>
                </tr>
              </table>

            </td>
          </tr>
        </table>
        """
        cards_bull_html.append(card)

    cards_bull_block = (
        "\n".join(cards_bull_html)
        if cards_bull_html
        else """
    <div style="padding:16px; background-color:#111622; border:1px solid #1e2638; border-radius:6px; color:#94a3b8; text-align:center; font-size:12px;">
      No active supra-zero crossovers triggered during this diagnostic window.
    </div>
    """
    )

    # 3. Tier 2: Pre-Close Squeeze Table Rows
    squeeze_rows = []
    for it in tier2_imminent[:8]:
        spread = abs(it["macd_1h"] - it["signal_1h"])
        atr_val = it.get("atr_1h", 0.0)
        spread_disp = f"{spread:.4f} ({spread / atr_val:.2f} ATR)" if atr_val > 0 else f"{spread:.4f}"
        diurnal_rvol = it.get("rvol_1h_diurnal", it.get("rvol_1h", 1.0))
        safe_ticker = html_lib.escape(str(it["ticker"]), quote=True)
        safe_name = html_lib.escape(str(it.get("fund_name", it["ticker"])), quote=True)
        safe_lev = html_lib.escape(str(it.get("leverage", "1x")), quote=True)
        row = f"""
        <tr style="border-bottom:1px solid #1e2638;">
          <td style="padding:9px 10px; font-family:ui-monospace, monospace; font-weight:700; color:#38bdf8; font-size:12px;">{safe_ticker}</td>
          <td style="padding:9px 10px; color:#e2e8f0; font-size:11.5px;">
            {safe_name} <span style="font-size:10px; color:#94a3b8;">({safe_lev})</span>
          </td>
          <td style="padding:9px 10px; font-family:ui-monospace, monospace; text-align:right; color:#ffffff; font-weight:700; font-size:12px;">${it['latest_price']:.2f}</td>
          <td style="padding:9px 10px; font-family:ui-monospace, monospace; text-align:right; color:#facc15; font-weight:700; font-size:12px;">{spread_disp}</td>
          <td style="padding:9px 10px; font-family:ui-monospace, monospace; text-align:right; color:#cbd5e1; font-size:11.5px;">{diurnal_rvol:.2f}x</td>
          <td style="padding:9px 10px; text-align:center;">
            <span style="background-color:rgba(250,204,21,0.15); color:#facc15; border:1px solid rgba(250,204,21,0.35); padding:2px 7px; border-radius:3px; font-size:9.5px; font-weight:700; letter-spacing:0.03em;">PRE-CLOSE SQUEEZE</span>
          </td>
        </tr>
        """
        squeeze_rows.append(row)

    squeeze_table_body = (
        "\n".join(squeeze_rows)
        if squeeze_rows
        else """
    <tr><td colspan="6" style="padding:14px; text-align:center; color:#94a3b8;">No pending crossovers within ATR-scaled spread compression tolerance.</td></tr>
    """
    )

    # 4-A. Critical Capital Preservation: Severe Breakdown Table Rows
    breakdown_rows = []
    for it in tier_breakdowns[:10]:
        piv = it.get("pivots", {})
        s4 = piv.get("S4", 0.0)
        s5 = piv.get("S5", 0.0)
        safe_ticker = html_lib.escape(str(it["ticker"]), quote=True)
        safe_name = html_lib.escape(str(it.get("fund_name", it["ticker"])), quote=True)
        safe_lev = html_lib.escape(str(it.get("leverage", "1x")), quote=True)
        row = f"""
        <tr style="border-bottom:1px solid #2d151d;">
          <td style="padding:9px 10px; font-family:ui-monospace, monospace; font-weight:800; color:#fda4af; font-size:12px;">{safe_ticker}</td>
          <td style="padding:9px 10px; color:#cbd5e1; font-size:11.5px;">
            {safe_name} <span style="font-size:10px; color:#94a3b8;">({safe_lev})</span>
          </td>
          <td style="padding:9px 10px; font-family:ui-monospace, monospace; text-align:right; color:#ffffff; font-weight:700; font-size:12px;">${it['latest_price']:.2f}</td>
          <td style="padding:9px 10px; font-family:ui-monospace, monospace; text-align:right; color:#fb7185; font-weight:800; font-size:12px;">{it['chg_1d_pct']:+.2f}%</td>
          <td style="padding:9px 10px; font-family:ui-monospace, monospace; text-align:right; color:#fda4af; font-size:11px;">S4: ${s4:.2f} / S5: ${s5:.2f}</td>
          <td style="padding:9px 10px; text-align:center;">
            <span style="background-color:rgba(225,29,72,0.25); color:#fda4af; border:1px solid rgba(244,63,94,0.5); padding:3px 8px; border-radius:3px; font-size:9.5px; font-weight:800; letter-spacing:0.04em;">MANDATORY EXIT</span>
          </td>
        </tr>
        """
        breakdown_rows.append(row)

    breakdown_table_body = (
        "\n".join(breakdown_rows)
        if breakdown_rows
        else """
    <tr><td colspan="6" style="padding:14px; text-align:center; color:#94a3b8;">No severe liquidation breakdowns (S4/S5 or &le; -2.25&sigma; YZ) detected.</td></tr>
    """
    )

    # 4. Tier 3: Bearish Breakdown Table Rows (High Contrast!)
    bear_rows = []
    for it in tier_bear[:8]:
        safe_ticker = html_lib.escape(str(it["ticker"]), quote=True)
        safe_name = html_lib.escape(str(it.get("fund_name", it["ticker"])), quote=True)
        safe_lev = html_lib.escape(str(it.get("leverage", "1x")), quote=True)
        row = f"""
        <tr style="border-bottom:1px solid #1e2638;">
          <td style="padding:9px 10px; font-family:ui-monospace, monospace; font-weight:800; color:#fda4af; font-size:12px;">{safe_ticker}</td>
          <td style="padding:9px 10px; color:#cbd5e1; font-size:11.5px;">
            {safe_name} <span style="font-size:10px; color:#94a3b8;">({safe_lev})</span>
          </td>
          <td style="padding:9px 10px; font-family:ui-monospace, monospace; text-align:right; color:#ffffff; font-weight:700; font-size:12px;">${it['latest_price']:.2f}</td>
          <td style="padding:9px 10px; font-family:ui-monospace, monospace; text-align:center; color:#fb7185; font-weight:700; font-size:11px;">RED</td>
          <td style="padding:9px 10px; font-family:ui-monospace, monospace; text-align:right; color:#fb7185; font-size:11.5px;">{it['macd_1h']:+.3f}</td>
          <td style="padding:9px 10px; font-family:ui-monospace, monospace; text-align:right; color:#fb7185; font-weight:700; font-size:11.5px;">{it['hist_1h']:+.4f}</td>
          <td style="padding:9px 10px; text-align:center;">
            <span style="background-color:rgba(244,63,94,0.2); color:#fda4af; border:1px solid rgba(251,113,133,0.45); padding:3px 8px; border-radius:3px; font-size:9.5px; font-weight:800; letter-spacing:0.04em;">EXIT TO CASH</span>
          </td>
        </tr>
        """
        bear_rows.append(row)

    bear_table_body = (
        "\n".join(bear_rows)
        if bear_rows
        else """
    <tr><td colspan="7" style="padding:14px; text-align:center; color:#94a3b8;">No fresh bearish breakdowns detected in this scan.</td></tr>
    """
    )

    safe_session_badge = html_lib.escape(str(session_badge), quote=True)
    safe_scan_time_str = html_lib.escape(str(scan_time_str), quote=True)

    # 5. Master HTML Template
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta name="color-scheme" content="dark">
  <meta name="supported-color-schemes" content="dark">
  <title>Conviction Labs Quantitative Momentum Radar</title>
  <style>
    body {{
      margin: 0;
      padding: 0;
      background-color: #080c14;
      color: #e2e8f0;
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
      -webkit-font-smoothing: antialiased;
    }}
    table {{ border-collapse: collapse; }}
    @media only screen and (max-width: 640px) {{
      .wrap-td {{ padding: 6px 4px !important; }}
      .box-td {{ padding: 12px 10px !important; }}
      .header-td {{ padding: 14px 12px 10px 12px !important; }}
      .kpi-text {{ font-size: 13px !important; }}
      .kpi-sub {{ font-size: 8.5px !important; }}
      .table-scroll {{ width: 100% !important; overflow-x: auto !important; -webkit-overflow-scrolling: touch !important; display: block !important; }}
    }}
  </style>
</head>
<body style="margin:0; padding:0; background-color:#080c14; color:#e2e8f0;">
  <table role="presentation" width="100%" border="0" cellspacing="0" cellpadding="0" style="background-color:#080c14; width:100%;">
    <tr>
      <td class="wrap-td" align="center" style="padding:24px 12px;">
        
        <!-- Main Email Container Box (680px for desktop elegance, fluid on phone) -->
        <table role="presentation" width="100%" border="0" cellspacing="0" cellpadding="0" style="max-width:680px; background-color:#0b0f17; border:1px solid #1e2638; border-radius:8px; overflow:hidden;">
          
          <!-- Top Cyan Accent Bar -->
          <tr>
            <td style="height:4px; background:linear-gradient(90deg, #38bdf8, #22d3ee); line-height:4px; font-size:4px;">&nbsp;</td>
          </tr>

          <!-- Header -->
          <tr>
            <td class="header-td" style="padding:20px 22px 16px 22px; border-bottom:1px solid #1e2638; background-color:#0f172a;">
              <table role="presentation" width="100%" border="0" cellspacing="0" cellpadding="0">
                <tr>
                  <td>
                    <div style="font-size:16px; font-weight:800; color:#ffffff; letter-spacing:0.04em; text-transform:uppercase;">
                      CONVICTION LABS &middot; QUANTITATIVE RADAR
                    </div>
                    <div style="font-size:11px; font-weight:700; color:#38bdf8; letter-spacing:0.06em; text-transform:uppercase; margin-top:3px;">
                      1-HOUR MACD MOMENTUM &amp; TRADITIONAL FLOOR PIVOTS
                    </div>
                  </td>
                  <td style="text-align:right; vertical-align:middle;">
                    <span style="display:inline-block; background-color:rgba(56,189,248,0.15); color:#38bdf8; border:1px solid rgba(56,189,248,0.35); padding:3px 9px; border-radius:4px; font-size:10px; font-weight:800; letter-spacing:0.04em; text-transform:uppercase;">
                      {safe_session_badge}
                    </span>
                  </td>
                </tr>
              </table>
              <div style="font-size:11px; color:#cbd5e1; margin-top:8px;">
                Scan Window: <strong style="color:#ffffff;">{safe_scan_time_str}</strong>
              </div>
            </td>
          </tr>

          <!-- Executive KPI Snapshot Strip -->
          <tr>
            <td style="padding:12px 16px; background-color:#090d15; border-bottom:1px solid #1e2638;">
              <table role="presentation" width="100%" border="0" cellspacing="0" cellpadding="0" style="table-layout:fixed;">
                <tr>
                  <td style="padding:6px 4px; border-right:1px solid #1e2638; text-align:center;">
                    <div class="kpi-sub" style="font-size:9px; color:#c084fc; font-weight:700; text-transform:uppercase; letter-spacing:0.04em;">BREAKOUT SURGE</div>
                    <div class="kpi-text" style="font-family:ui-monospace, monospace; font-size:15px; font-weight:800; color:#c084fc; margin-top:2px;">{len(tier0_breakouts)} ACTIVE</div>
                  </td>
                  <td style="padding:6px 4px; border-right:1px solid #1e2638; text-align:center;">
                    <div class="kpi-sub" style="font-size:9px; color:#94a3b8; font-weight:700; text-transform:uppercase; letter-spacing:0.04em;">SUB-ZERO REV</div>
                    <div class="kpi-text" style="font-family:ui-monospace, monospace; font-size:15px; font-weight:800; color:#38bdf8; margin-top:2px;">{len(tier1_subzero)} ACTIVE</div>
                  </td>
                  <td style="padding:6px 4px; border-right:1px solid #1e2638; text-align:center;">
                    <div class="kpi-sub" style="font-size:9px; color:#94a3b8; font-weight:700; text-transform:uppercase; letter-spacing:0.04em;">SUPRA-ZERO CROSS</div>
                    <div class="kpi-text" style="font-family:ui-monospace, monospace; font-size:15px; font-weight:800; color:#34d399; margin-top:2px;">{len(tier1_bull)} ACTIVE</div>
                  </td>
                  <td style="padding:6px 4px; text-align:center;">
                    <div class="kpi-sub" style="font-size:9px; color:#94a3b8; font-weight:700; text-transform:uppercase; letter-spacing:0.04em;">MANDATORY EXITS</div>
                    <div class="kpi-text" style="font-family:ui-monospace, monospace; font-size:15px; font-weight:800; color:#fb7185; margin-top:2px;">{len(tier_bear) + len(tier_breakdowns)} CASH</div>
                  </td>
                </tr>
              </table>
            </td>
          </tr>

          <!-- Body Content Area -->
          <tr>
            <td class="box-td" style="padding:18px 22px;">

              <!-- SECTION 0: TIER 0 EXTREME MOMENTUM BREAKOUTS -->
              <div style="margin-bottom:24px;">
                <div style="font-size:12px; font-weight:800; color:#c084fc; text-transform:uppercase; letter-spacing:0.05em; margin-bottom:4px;">
                  TIER 0: EXTREME MOMENTUM BREAKOUTS &amp; VOLATILITY SURGES
                </div>
                <div style="font-size:11px; color:#cbd5e1; margin-bottom:14px; line-height:1.5;">
                  Price breached traditional Floor Pivot resistance (R4/R5 extension) or registered Yang-Zhang volatility-scaled surge (&ge; 2.25&sigma;<sub>YZ</sub> with Diurnal RVOL &ge; 1.25x, or &ge; 3&sigma;<sub>YZ</sub> shock) with confirmed 1-Hour Green Candle. Priority quantitative momentum breakouts.
                </div>
                
                {cards_breakout_block}

              </div>

              <!-- SECTION 1A: TIER 1A SUB-ZERO REVERSALS -->
              <div style="margin-bottom:20px;">
                <div style="font-size:12px; font-weight:800; color:#38bdf8; text-transform:uppercase; letter-spacing:0.05em; margin-bottom:4px;">
                  TIER 1A: CONFIRMED SUB-ZERO BULLISH REVERSALS
                </div>
                <div style="font-size:11px; color:#cbd5e1; margin-bottom:14px; line-height:1.5;">
                  Confirmed 1-Hour bar close where MACD Line crossed above Signal Line while MACD &lt; 0 with confirmed Green Candle. High-convexity turnaround points at structural floor pivot levels.
                </div>
                
                {cards_block}

              </div>

              <!-- SECTION 1B: TIER 1B SUPRA-ZERO BULLISH CROSSOVERS -->
              <div style="margin-top:24px; margin-bottom:20px;">
                <div style="font-size:12px; font-weight:800; color:#34d399; text-transform:uppercase; letter-spacing:0.05em; margin-bottom:4px;">
                  TIER 1B: CONFIRMED SUPRA-ZERO BULLISH CROSSOVERS
                </div>
                <div style="font-size:11px; color:#cbd5e1; margin-bottom:14px; line-height:1.5;">
                  Confirmed 1-Hour bar close where MACD Line crossed above Signal Line while MACD &ge; 0 with confirmed Green Candle. Upward momentum continuation in established trends.
                </div>
                
                {cards_bull_block}

              </div>

              <!-- SECTION 2: TIER 2 PRE-CLOSE SQUEEZE RADAR -->
              <div style="margin-top:24px; margin-bottom:20px;">
                <div style="font-size:12px; font-weight:800; color:#facc15; text-transform:uppercase; letter-spacing:0.05em; margin-bottom:4px;">
                  TIER 2: PRE-CLOSE SQUEEZE RADAR (15:00 ET SCAN)
                </div>
                <div style="font-size:11px; color:#cbd5e1; margin-bottom:12px; line-height:1.5;">
                  Histogram spread compression &ge; 35% within ATR-scaled convergence tolerance heading into the final market hour (15:00&ndash;16:00 ET). Pre-positioning watch ahead of benchmark rebalancing flow auctions.
                </div>
                
                <div class="table-scroll" style="width:100%; overflow-x:auto;">
                  <table role="presentation" width="100%" border="0" cellspacing="0" cellpadding="0" style="font-size:11px; background-color:#111622; border:1px solid #1e2638; border-radius:6px; min-width:520px; overflow:hidden;">
                    <thead>
                      <tr style="background-color:#161f30; color:#cbd5e1; font-size:10px; text-transform:uppercase; letter-spacing:0.05em;">
                        <th style="padding:8px 10px; text-align:left;">Ticker</th>
                        <th style="padding:8px 10px; text-align:left;">Instrument</th>
                        <th style="padding:8px 10px; text-align:right;">Price</th>
                        <th style="padding:8px 10px; text-align:right;">Spread</th>
                        <th style="padding:8px 10px; text-align:right;">Diurnal RVOL</th>
                        <th style="padding:8px 10px; text-align:center;">Status</th>
                      </tr>
                    </thead>
                    <tbody>
                      {squeeze_table_body}
                    </tbody>
                  </table>
                </div>
              </div>

              <!-- SECTION 3: TIER 3 BEARISH BREAKDOWN & CRITICAL CAPITAL PRESERVATION -->
              <div style="margin-top:24px; margin-bottom:20px;">
                <div style="font-size:12px; font-weight:800; color:#fb7185; text-transform:uppercase; letter-spacing:0.05em; margin-bottom:4px;">
                  CRITICAL CAPITAL PRESERVATION &middot; SEVERE LIQUIDATION CASCADE
                </div>
                <div style="font-size:11px; color:#cbd5e1; margin-bottom:12px; line-height:1.5;">
                  Price breached Floor Pivot support (S4/S5) or suffered Yang-Zhang volatility-scaled plunge (&le; -2.25&sigma;<sub>YZ</sub> with Diurnal RVOL &ge; 1.25x, or &le; -3&sigma;<sub>YZ</sub> shock) on confirmed Red Candle. Severe structural liquidation cascade requiring immediate exit to cash.
                </div>
                
                <div class="table-scroll" style="width:100%; overflow-x:auto; margin-bottom:18px;">
                  <table role="presentation" width="100%" border="0" cellspacing="0" cellpadding="0" style="font-size:11px; background-color:#160b11; border:1px solid #3d1520; border-radius:6px; min-width:520px; overflow:hidden;">
                    <thead>
                      <tr style="background-color:#230e16; color:#fda4af; font-size:10px; text-transform:uppercase; letter-spacing:0.05em;">
                        <th style="padding:8px 10px; text-align:left;">Ticker</th>
                        <th style="padding:8px 10px; text-align:left;">Instrument</th>
                        <th style="padding:8px 10px; text-align:right;">Price</th>
                        <th style="padding:8px 10px; text-align:right;">24H Change</th>
                        <th style="padding:8px 10px; text-align:right;">Pivot Breach</th>
                        <th style="padding:8px 10px; text-align:center;">Action</th>
                      </tr>
                    </thead>
                    <tbody>
                      {breakdown_table_body}
                    </tbody>
                  </table>
                </div>

                <div style="font-size:12px; font-weight:800; color:#fb7185; text-transform:uppercase; letter-spacing:0.05em; margin-bottom:4px;">
                  TIER 3: BEARISH MACD CROSSOVER &middot; SYSTEMATIC EXITS
                </div>
                <div style="font-size:11px; color:#cbd5e1; margin-bottom:12px; line-height:1.5;">
                  1-Hour MACD crossed below Signal Line on confirmed Red Candle. Exit longs immediately to cash.
                </div>
                
                <div class="table-scroll" style="width:100%; overflow-x:auto;">
                  <table role="presentation" width="100%" border="0" cellspacing="0" cellpadding="0" style="font-size:11px; background-color:#111622; border:1px solid #1e2638; border-radius:6px; min-width:520px; overflow:hidden;">
                    <thead>
                      <tr style="background-color:#161f30; color:#cbd5e1; font-size:10px; text-transform:uppercase; letter-spacing:0.05em;">
                        <th style="padding:8px 10px; text-align:left;">Ticker</th>
                        <th style="padding:8px 10px; text-align:left;">Instrument</th>
                        <th style="padding:8px 10px; text-align:right;">Price</th>
                        <th style="padding:8px 10px; text-align:center;">Candle</th>
                        <th style="padding:8px 10px; text-align:right;">1H MACD</th>
                        <th style="padding:8px 10px; text-align:right;">Hist</th>
                        <th style="padding:8px 10px; text-align:center;">Action</th>
                      </tr>
                    </thead>
                    <tbody>
                      {bear_table_body}
                    </tbody>
                  </table>
                </div>
              </div>

              <!-- RISK & EXECUTION PROTOCOL -->
              <div style="margin-top:24px; padding:13px 15px; background-color:#0b0f17; border:1px solid #1e2638; border-radius:6px; font-size:11px; color:#cbd5e1; line-height:1.6;">
                <strong style="color:#ffffff;">Execution &amp; Risk Protocol:</strong> Leveraged and inverse ETFs are subject to constant daily leverage resets and non-linear path decay (&Delta;V &approx; -&frac12;(k&sup2;-k)&sigma;&sup2;&Delta;t). Empirical multi-year backtests demonstrate that cash parking during bearish regimes outperforms continuous long/short flipping by over 300%. Maximum recommended holding duration is 30 market hours.
              </div>

              <!-- DASHBOARD CTA BUTTON -->
              <div style="margin-top:20px; text-align:center;">
                <a href="https://yieldchaser.github.io/etf-data/markets.html?tab=flows&amp;view=macd_strategy" target="_blank" style="display:inline-block; background-color:#38bdf8; color:#0b0f17; font-weight:800; font-size:12px; text-transform:uppercase; letter-spacing:0.05em; padding:11px 24px; border-radius:4px; text-decoration:none;">
                  OPEN LIVE INTERACTIVE MACD DASHBOARD
                </a>
              </div>

            </td>
          </tr>

          <!-- Footer -->
          <tr>
            <td style="padding:14px 20px; background-color:#090d15; border-top:1px solid #1e2638; font-size:10.5px; color:#94a3b8; text-align:center;">
              Conviction Labs &middot; {resolved_universe_count} Curated Leveraged &amp; Inverse ETF Universe &middot; Quantitative Research Engine
            </td>
          </tr>

        </table>

      </td>
    </tr>
  </table>
</body>
</html>"""

    return text_content, html_content


# Canonical alias for systematic alert formatting
format_alert_email = format_institutional_email


def send_email_alert(
    subject: str, text_body: str, html_body: str, recipient: str = ""
) -> bool:
    """Send email via SMTP SSL (465) or STARTTLS (587) with multi-recipient support."""
    sender = (os.environ.get("ALERT_EMAIL_SENDER") or "").strip()
    password = (os.environ.get("ALERT_EMAIL_PASSWORD") or "").strip().replace(" ", "")
    smtp_host = (os.environ.get("ALERT_SMTP_HOST") or "smtp.gmail.com").strip()
    smtp_port_raw = (os.environ.get("ALERT_SMTP_PORT") or "465").strip()
    try:
        smtp_port = int(smtp_port_raw)
    except ValueError:
        smtp_port = 465

    if not sender or not password:
        print("[SMTP Engine] ALERT_EMAIL_SENDER or ALERT_EMAIL_PASSWORD not configured.")
        print("[SMTP Engine] Executed deterministic dry run. Artifacts written to disk.")
        return False

    # Parse potential comma/semicolon-separated recipients
    if isinstance(recipient, str):
        recipients_list = [r.strip() for r in re.split(r"[,;]", recipient) if r.strip()]
    else:
        recipients_list = [str(r).strip() for r in recipient if str(r).strip()]

    if not recipients_list:
        recipients_list = [sender]

    try:
        ssl_ctx = ssl.create_default_context()
        all_sent = True
        # Send individual message per recipient to ensure maximum deliverability (1-on-1 inbox classification)
        for target_email in recipients_list:
            msg = email.mime.multipart.MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = sender
            msg["To"] = target_email
            msg["Date"] = email.utils.formatdate(localtime=True)
            # Let SMTP server generate authentic DKIM-aligned Message-ID automatically

            msg.attach(email.mime.text.MIMEText(text_body, "plain", "utf-8"))
            msg.attach(email.mime.text.MIMEText(html_body, "html", "utf-8"))

            if smtp_port == 465:
                with smtplib.SMTP_SSL(smtp_host, smtp_port, context=ssl_ctx, timeout=20) as server:
                    server.login(sender, password)
                    refused = server.sendmail(sender, [target_email], msg.as_string())
                    if refused:
                        print(f"[SMTP Engine] Refused by server for {target_email}: {refused}")
                        all_sent = False
            else:
                with smtplib.SMTP(smtp_host, smtp_port, timeout=20) as server:
                    server.ehlo()
                    server.starttls(context=ssl_ctx)
                    server.ehlo()
                    server.login(sender, password)
                    refused = server.sendmail(sender, [target_email], msg.as_string())
                    if refused:
                        print(f"[SMTP Engine] Refused by server for {target_email}: {refused}")
                        all_sent = False

        if all_sent:
            print(f"[SMTP Engine] Successfully dispatched individual email alerts to {recipients_list} via {smtp_host}:{smtp_port}!")
        else:
            print(f"[SMTP Engine] One or more recipients were refused during dispatch via {smtp_host}:{smtp_port}.")
        return all_sent
    except Exception as ex:
        print(f"[SMTP Engine] Error dispatching email: {ex}")
        return False


def format_alert_subject_lead_tickers(
    tickers_set: set[str],
    sig_map: dict[str, Any],
    is_premarket: bool = False,
    max_count: int = 4,
) -> str:
    """Format top leader tickers for email alert subject lines, sorted by absolute return magnitude."""
    def _sort_key(tk: str) -> float:
        rec = sig_map.get(tk, {})
        if is_premarket and rec.get("live_premarket_chg_pct") is not None:
            return abs(float(rec["live_premarket_chg_pct"]))
        c1d = rec.get("chg_1d_pct")
        val = c1d if c1d is not None else (rec.get("chg_1h_pct") or 0.0)
        return abs(float(val))

    sorted_t = sorted(list(tickers_set), key=_sort_key, reverse=True)
    parts = []
    for tk in sorted_t[:max_count]:
        rec = sig_map.get(tk, {})
        chg = rec.get("live_premarket_chg_pct") if (is_premarket and rec.get("live_premarket_chg_pct") is not None) else rec.get("chg_1d_pct")
        if chg is not None:
            parts.append(f"{tk} ({chg:+.1f}%)")
        else:
            parts.append(tk)
    out = ", ".join(parts)
    if len(sorted_t) > max_count:
        out += f" (+{len(sorted_t) - max_count} more)"
    return out


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run 1H/1D MACD Momentum Scanner across ETF Universe."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limit number of tickers scanned (0 = full catalog)",
    )
    parser.add_argument(
        "--send-email",
        action="store_true",
        help="Enable email alerting engine if triggers detected",
    )
    parser.add_argument(
        "--force-send",
        "--force-email",
        dest="force_send",
        action="store_true",
        help="Force dispatch email regardless of deduplication state",
    )
    parser.add_argument(
        "--recipient",
        type=str,
        default="",
        help="Custom recipient email (defaults to ALERT_EMAIL_RECIPIENT env var)",
    )
    args = parser.parse_args()

    recipient = (args.recipient or os.environ.get("ALERT_EMAIL_RECIPIENT") or "").strip()

    if not CATALOG_PATH.is_file():
        print(f"Catalog file not found at {CATALOG_PATH}")
        sys.exit(1)

    with open(CATALOG_PATH, "r", encoding="utf-8") as f:
        catalog = json.load(f)

    # Full curated universe loaded dynamically from catalog.json
    all_instruments = catalog.get("instruments", [])
    total_catalog_count = len(all_instruments)
    instruments = all_instruments[: args.limit] if args.limit > 0 else all_instruments
    ticker_aum: dict[str, float] = {}
    for inst in all_instruments:
        tk = str(inst.get("ticker", "")).strip()
        try:
            aum_v = float(inst.get("aum_m") or 0.0)
        except (TypeError, ValueError):
            aum_v = 0.0
        ticker_aum[tk] = aum_v if math.isfinite(aum_v) else 0.0

    now_utc = dt.datetime.now(dt.timezone.utc)
    now_ny = now_utc.astimezone(NY_TZ)
    session_info = get_market_session(now_ny)
    session_code, session_name, session_badge = session_info

    print(
        f"Scanning MACD momentum across {len(instruments)} instruments in session [{session_name}] at {now_ny.strftime('%Y-%m-%d %H:%M:%S %Z')}..."
    )
    results: list[dict[str, Any]] = []
    failed_tickers: list[str] = []

    def _worker(inst: dict[str, Any]) -> dict[str, Any] | None:
        ticker = inst["ticker"]
        meta = {
            "fund_name": inst.get("fund_name", ticker),
            "leverage": inst.get("leverage", "1x"),
            "category": inst.get("category", "Unclassified"),
            "underlying": inst.get("underlying", "—"),
        }
        return analyze_ticker(ticker, meta)

    # Process tickers with bounded concurrency
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        future_map = {executor.submit(_worker, inst): inst["ticker"] for inst in instruments}
        for future in concurrent.futures.as_completed(future_map):
            ticker = future_map[future]
            try:
                res = future.result()
                if res:
                    results.append(res)
                else:
                    failed_tickers.append(ticker)
            except Exception as e:
                print(f"Error scanning {ticker}: {e}")
                failed_tickers.append(ticker)

    results.sort(key=lambda x: (-x.get("confluence_score", 0), x.get("ticker", "")))
    if failed_tickers:
        sample_failed = ", ".join(sorted(failed_tickers)[:10])
        suffix = f" (+{len(failed_tickers) - 10} more)" if len(failed_tickers) > 10 else ""
        print(
            f"Successfully processed {len(results)}/{len(instruments)} ETF instruments "
            f"(skipped/failed: {len(failed_tickers)}: {sample_failed}{suffix})."
        )
    else:
        print(f"Successfully processed {len(results)} active ETF instruments.")

    scan_time_str = now_ny.strftime("%Y-%m-%d %H:%M:%S %Z")

    # Ensure Tier 1 LOD history directories exist
    HISTORY_DIR_MACD.mkdir(parents=True, exist_ok=True)
    HISTORY_DIR_FLOWS.mkdir(parents=True, exist_ok=True)

    # Derive flagship preview set deterministically by descending AUM & top confluence
    featured_ordered = [
        str(tk).strip()
        for tk in (catalog.get("featured_tickers", []) if isinstance(catalog, dict) else [])
        if str(tk).strip()
    ]
    if not featured_ordered:
        featured_ordered = [
            str(inst.get("ticker", "")).strip()
            for inst in all_instruments
            if inst.get("featured")
        ]
    featured_by_aum = sorted(featured_ordered, key=lambda tk: (-ticker_aum.get(tk, 0.0), tk))
    top_confluence = sorted(results, key=lambda x: abs(x.get("confluence_score", 0)), reverse=True)
    flagship_preview_set = {it["ticker"] for it in top_confluence[:8]} | set(featured_by_aum[:8])

    # Write per-ticker deep LOD history files atomically (5Y Daily + 2Y 1H bars)
    for item in results:
        tick = item["ticker"]
        deep_1d = item.pop("deep_bars_1d", [])
        deep_1h = item.pop("deep_bars_1h", [])
        deep_v_1d = item.pop("deep_v_1d", [])
        deep_v_1h = item.pop("deep_v_1h", [])

        hist_payload = {
            "ticker": tick,
            "updated_utc": now_utc.isoformat(),
            "bars_1d": deep_1d,
            "bars_1h": deep_1h,
            "v_1d": deep_v_1d,
            "v_1h": deep_v_1h,
        }
        for hdir in (HISTORY_DIR_MACD, HISTORY_DIR_FLOWS):
            _atomic_write_json(hdir / f"{tick}.json", hist_payload, separators=(",", ":"))

        # Compact signals.json: preserve 60 preview bars for top flagship tickers,
        # and clear for others so signals.json stays lightweight
        if tick not in flagship_preview_set:
            item["chart_bars_1h"] = []
            item["chart_bars_1d"] = []
            item["chart_v_1h"] = []
            item["chart_v_1d"] = []
        else:
            item["chart_bars_1h"] = item.get("chart_bars_1h", [])[-60:]
            item["chart_bars_1d"] = item.get("chart_bars_1d", [])[-60:]
            item["chart_v_1h"] = item.get("chart_v_1h", [])[-60:]
            item["chart_v_1d"] = item.get("chart_v_1d", [])[-60:]

    # Structure payload for Web Dashboard (Tier 0 Screener Radar)
    payload = {
        "updated_utc": now_utc.isoformat(),
        "scan_time_str": scan_time_str,
        "session": session_code,
        "session_name": session_name,
        "universe_count": total_catalog_count,
        "total_scanned": len(results),
        "failed_count": len(failed_tickers),
        "signals": results,
    }

    is_full_universe = (
        total_catalog_count > 0
        and args.limit == 0
        and len(results) >= int(total_catalog_count * 0.5)
    )

    # Save to JSON atomically (both dedicated macd/ directory and flows/ fallback)
    # INVARIANT: Only overwrite production signal artifacts when scanning full universe.
    if not is_full_universe:
        print(
            f"[Scanner Safety] Limited test scan active ({len(results)} of {total_catalog_count} instruments). "
            f"Preserving full production signals artifacts at {SIGNALS_OUT_MACD}."
        )
    else:
        for p in (SIGNALS_OUT_MACD, SIGNALS_OUT_FLOWS):
            _atomic_write_json(p, payload, separators=(",", ":"))
        print(f"Saved signals artifact to {SIGNALS_OUT_MACD} ({SIGNALS_OUT_MACD.stat().st_size:,} bytes)")

    # Identify active signal sets for this run (Confirmed Candle Rules)
    current_breakouts = sorted(
        [s["ticker"] for s in results if s.get("is_breakout")]
    )
    current_breakdowns = sorted(
        [s["ticker"] for s in results if s.get("is_breakdown")]
    )
    current_subzero_tickers = sorted(
        [
            s["ticker"]
            for s in results
            if (s["signal_state"] == "SUB_ZERO_BOUNCE" or (s.get("bull_cross_fresh") and s.get("is_subzero")))
            and s.get("candle_color_1h") == "GREEN"
            and s.get("hist_1h", 0) > 0
        ]
    )
    current_bear_tickers = sorted(
        [
            s["ticker"]
            for s in results
            if (s["signal_state"] == "BEARISH_CROSS" or s.get("bear_cross_fresh"))
            and s.get("candle_color_1h") == "RED"
            and s.get("hist_1h", 0) < 0
        ]
    )
    current_squeeze_tickers = sorted(
        [s["ticker"] for s in results if s["signal_state"] == "IMMINENT_SQUEEZE"]
    )

    # Check for Active Crossovers on recent bar (Must be confirmed by candle color)
    current_crossovers = sorted(
        [
            s["ticker"]
            for s in results
            if (
                (s.get("bull_cross_fresh") and s.get("candle_color_1h") == "GREEN" and s.get("hist_1h", 0) > 0)
                or (s.get("bear_cross_fresh") and s.get("candle_color_1h") == "RED" and s.get("hist_1h", 0) < 0)
            )
        ]
    )

    # Audit unconfirmed crossovers (e.g. bullish crossover with red candle)
    unconfirmed_subzero = sorted(
        [s["ticker"] for s in results if s.get("signal_state") == "UNCONFIRMED_CROSS" and s.get("is_subzero")]
    )
    print(
        f"[Trigger Evaluation] Unconfirmed Bullish Crosses (Awaiting Green Candle): {len(unconfirmed_subzero)}: {unconfirmed_subzero}"
    )

    # Load prior alert state for deduplication
    prior_state: dict[str, Any] = {}
    alert_read_path = ALERT_OUT_MACD if ALERT_OUT_MACD.is_file() else ALERT_OUT_FLOWS
    if alert_read_path.is_file():
        try:
            with open(alert_read_path, "r", encoding="utf-8") as f:
                prior_state = json.load(f)
        except Exception:
            prior_state = {}

    prev_subzero = set(prior_state.get("sent_subzero", prior_state.get("active_subzero", [])))
    prev_bear = set(prior_state.get("sent_bear", prior_state.get("active_bear", [])))
    prev_squeeze = set(prior_state.get("sent_squeeze", prior_state.get("active_squeeze", [])))
    prev_crossovers = set(prior_state.get("sent_crossovers", prior_state.get("active_crossovers", [])))
    prev_breakouts = set(prior_state.get("sent_breakouts", prior_state.get("active_breakouts", [])))
    prev_breakdowns = set(prior_state.get("sent_breakdowns", prior_state.get("active_breakdowns", [])))

    new_subzero = set(current_subzero_tickers) - prev_subzero
    new_bear = set(current_bear_tickers) - prev_bear
    new_squeeze = set(current_squeeze_tickers) - prev_squeeze
    new_crossovers = set(current_crossovers) - prev_crossovers
    new_breakouts = set(current_breakouts) - prev_breakouts
    new_breakdowns = set(current_breakdowns) - prev_breakdowns

    has_fresh_trigger = bool(new_breakouts or new_breakdowns or new_subzero or new_bear or new_crossovers)
    is_pre_close_time = session_code == "PRE_CLOSE"

    print(
        f"[Trigger Evaluation] Active Breakouts (R4/R5/Velocity): {len(current_breakouts)} (New: {len(new_breakouts)}: {sorted(list(new_breakouts))})"
    )
    print(
        f"[Trigger Evaluation] Active Breakdowns (S4/S5/Liquidation): {len(current_breakdowns)} (New: {len(new_breakdowns)}: {sorted(list(new_breakdowns))})"
    )
    print(
        f"[Trigger Evaluation] Active Sub-Zero: {len(current_subzero_tickers)} (New: {len(new_subzero)}: {sorted(list(new_subzero))})"
    )
    print(
        f"[Trigger Evaluation] Active Bearish Exits: {len(current_bear_tickers)} (New: {len(new_bear)}: {sorted(list(new_bear))})"
    )
    print(
        f"[Trigger Evaluation] Pre-Close Squeeze: {len(current_squeeze_tickers)} (New: {len(new_squeeze)})"
    )
    print(
        f"[Trigger Evaluation] Active Crossovers: {len(current_crossovers)} (New: {len(new_crossovers)}: {sorted(list(new_crossovers))})"
    )

    # Build Email Alert Text and HTML
    text_alert, html_alert = format_institutional_email(
        results, scan_time_str, session_info, universe_count=total_catalog_count
    )

    should_send = False
    send_reason = ""
    today_ny = now_ny.strftime("%Y-%m-%d")
    last_preclose = prior_state.get("last_preclose_date", "")

    if args.force_send:
        should_send = True
        send_reason = "Manual override (--force-send enabled)"
    elif args.send_email:
        if has_fresh_trigger:
            triggers_summary = []
            if new_breakouts:
                triggers_summary.append(f"New Breakouts: {sorted(list(new_breakouts))}")
            if new_breakdowns:
                triggers_summary.append(f"New Breakdowns: {sorted(list(new_breakdowns))}")
            if new_subzero:
                triggers_summary.append(f"New Sub-Zero: {sorted(list(new_subzero))}")
            if new_bear:
                triggers_summary.append(f"New Exits: {sorted(list(new_bear))}")
            uncat = new_crossovers - new_subzero - new_bear
            if uncat:
                triggers_summary.append(f"New Crosses: {sorted(list(uncat))}")
            should_send = True
            send_reason = f"Fresh Trigger Detected ({', '.join(triggers_summary)})"
        elif is_pre_close_time:
            if last_preclose != today_ny:
                should_send = True
                send_reason = f"Pre-Close Golden Hour Scheduled Scan (15:00 {now_ny.tzname() or 'ET'})"
            else:
                print("[Alert Engine] Pre-Close Golden Hour diagnostic already dispatched today.")
        else:
            print("[Alert Engine] No fresh trigger changes detected. Email suppressed to preserve recipient inbox.")

    # Dispatch email BEFORE persisting sent_* state so transient SMTP failures never suppress alerts
    dispatched = False
    if should_send:
        # Dynamic alert subject with ticker highlights prioritized by absolute % return magnitude
        sig_map = {s.get("ticker"): s for s in results if isinstance(s, dict)}
        is_premarket = (session_code == "PRE_MARKET")
        format_lead_tickers = lambda s: format_alert_subject_lead_tickers(s, sig_map, is_premarket=is_premarket)

        uncat_crossovers = new_crossovers - new_bear
        try:
            if new_breakouts:
                lead_tickers = format_lead_tickers(new_breakouts)
                subject = f"[Conviction Labs] VOLATILITY BREAKOUT: {lead_tickers} · {session_badge} · {scan_time_str}"
            elif new_breakdowns and not new_breakouts and not new_subzero:
                lead_tickers = format_lead_tickers(new_breakdowns)
                subject = f"[Conviction Labs] CRITICAL BREAKDOWN: {lead_tickers} · {session_badge} · {scan_time_str}"
            elif new_subzero:
                lead_tickers = format_lead_tickers(new_subzero)
                subject = f"[Conviction Labs] SUB-ZERO REVERSAL: {lead_tickers} · {session_badge} · {scan_time_str}"
            elif new_bear and not new_subzero and not uncat_crossovers:
                lead_tickers = format_lead_tickers(new_bear)
                subject = f"[Conviction Labs] BEARISH EXIT: {lead_tickers} · {session_badge} · {scan_time_str}"
            elif new_crossovers:
                lead_tickers = format_lead_tickers(new_crossovers)
                subject = f"[Conviction Labs] 1H MOMENTUM CROSS: {lead_tickers} · {session_badge} · {scan_time_str}"
            elif is_pre_close_time:
                subject = f"[Conviction Labs] PRE-CLOSE SQUEEZE RADAR · {session_badge} · {scan_time_str}"
            else:
                subject = f"[Conviction Labs] 1H MACD Momentum Radar · {session_badge} · {scan_time_str}"
        except Exception as ex_subj:
            print(f"[Alert Engine Warning] Subject formatting fallback triggered: {ex_subj}")
            subject = f"[Conviction Labs] 1H MACD Momentum Radar · {session_badge} · {scan_time_str}"

        print(f"[Alert Engine] Dispatching email ({send_reason})...")
        print(f"[Alert Engine] Subject: {subject}")
        dispatched = send_email_alert(subject, text_alert, html_alert, recipient)

    def _next_sent_state(current_list: list[str], prev_set: set[str]) -> list[str]:
        if dispatched:
            return current_list
        if args.send_email or args.force_send:
            # Prune tickers that exited active state so subsequent fresh re-entries alert properly
            return sorted(prev_set & set(current_list))
        return sorted(prev_set)

    # Save Alert payload for inspection and deduplication tracking atomically
    alert_payload = {
        "timestamp_utc": now_utc.isoformat(),
        "scan_time_str": scan_time_str,
        "session": session_code,
        "dispatched": dispatched,
        "active_breakouts": current_breakouts,
        "active_breakdowns": current_breakdowns,
        "active_subzero": current_subzero_tickers,
        "active_bear": current_bear_tickers,
        "active_squeeze": current_squeeze_tickers,
        "active_crossovers": current_crossovers,
        "sent_breakouts": _next_sent_state(current_breakouts, prev_breakouts),
        "sent_breakdowns": _next_sent_state(current_breakdowns, prev_breakdowns),
        "sent_subzero": _next_sent_state(current_subzero_tickers, prev_subzero),
        "sent_bear": _next_sent_state(current_bear_tickers, prev_bear),
        "sent_squeeze": _next_sent_state(current_squeeze_tickers, prev_squeeze),
        "sent_crossovers": _next_sent_state(current_crossovers, prev_crossovers),
        "new_breakouts": sorted(list(new_breakouts)),
        "new_breakdowns": sorted(list(new_breakdowns)),
        "new_subzero": sorted(list(new_subzero)),
        "new_bear": sorted(list(new_bear)),
        "new_squeeze": sorted(list(new_squeeze)),
        "new_crossovers": sorted(list(new_crossovers)),
        "last_preclose_date": today_ny if (dispatched and is_pre_close_time) else last_preclose,
        "text_content": text_alert,
        "html_content": html_alert,
    }

    if is_full_universe:
        for p in (ALERT_OUT_MACD, ALERT_OUT_FLOWS):
            _atomic_write_json(p, alert_payload, indent=2)
        print(f"Saved latest alert artifact to {ALERT_OUT_MACD}")
    else:
        print(f"[Scanner Safety] Limited test scan: skipped overwriting {ALERT_OUT_MACD}.")


if __name__ == "__main__":
    main()
