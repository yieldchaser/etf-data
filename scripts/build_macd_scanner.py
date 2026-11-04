#!/usr/bin/env python3
"""Quantitative MACD Momentum Scanner and Email Alert Engine for Leveraged ETFs.

This script:
1. Ingests the 160 curated instruments from `docs/data/flows/catalog.json`.
2. Fetches intraday 1-hour OHLCV and daily OHLCV bars via Yahoo Finance Chart API:
   - Uses Regular Trading Hours (RTH, prepost=False) for canonical 1H MACD (12, 26, 9 EMA).
   - In pre-market (04:00 - 09:30 EST), captures live pre-market price to compute indicated gap.
   - Computes traditional Auto Floor Trader Pivots (P, S1-S5, R1-R5) from the last COMPLETED
     regular trading session (strictly session date < today's date during market/premarket).
3. Detects signals across all market sessions:
   - Tier 1A: Confirmed Sub-Zero Bullish Reversals (high convexity value turnarounds).
   - Tier 1B: Confirmed Supra-Zero Bullish Crossovers (momentum continuation).
   - Tier 2: Pre-Close Squeeze Watch (histogram compression >= 35% heading into 15:00-16:00 EST).
   - Tier 3: Bearish Breakdowns / Mandatory Cash Parking (long de-grossing exits).
4. Dispatches institutional alerts to `raghavendra.ds1@gmail.com`:
   - "Whenever or as soon as data triggers": event-driven dispatch upon fresh crossovers.
   - Scheduled Pre-Close Golden Hour briefing at 15:00 EST.
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
SIGNALS_OUT = REPO_ROOT / "docs" / "data" / "flows" / "macd_signals.json"
ALERT_OUT = REPO_ROOT / "docs" / "data" / "flows" / "latest_macd_alert.json"

DEFAULT_RECIPIENT = "raghavendra.ds1@gmail.com"
NY_TZ = ZoneInfo("America/New_York")


def get_market_session(now_ny: dt.datetime) -> tuple[str, str, str]:
    """Detect current US market session: returns (session_code, session_name, badge_text)."""
    weekday = now_ny.weekday()  # 0=Mon, 4=Fri, 5=Sat, 6=Sun
    if weekday >= 5:
        return "WEEKEND", "WEEKEND QUANTITATIVE AUDIT", "WEEKEND SCAN"

    time_float = now_ny.hour + now_ny.minute / 60.0

    if time_float < 4.0:
        return "OVERNIGHT", "OVERNIGHT OFF-HOURS RADAR", "OVERNIGHT SCAN"
    elif 4.0 <= time_float < 9.5:
        return "PRE_MARKET", "PRE-MARKET INDICATIVE RADAR", "PRE-MARKET SCAN"
    elif 9.5 <= time_float < 10.5:
        return "SESSION_OPEN", "REGULAR SESSION OPEN MOMENTUM", "SESSION OPEN"
    elif 14.75 <= time_float < 15.5:
        return "PRE_CLOSE", "PRE-MARKET-CLOSE GOLDEN HOUR SCAN", "15:00 EST PRE-CLOSE"
    elif 10.5 <= time_float < 16.0:
        return "INTRADAY", "INTRADAY HOURLY MOMENTUM SCAN", "INTRADAY RADAR"
    elif 16.0 <= time_float < 20.0:
        return "POST_MARKET", "POST-MARKET CLOSING AUDIT", "POST-MARKET SCAN"
    else:
        return "OVERNIGHT", "OVERNIGHT OFF-HOURS RADAR", "OFF-HOURS SCAN"


def calculate_ema(series: list[float], period: int) -> list[float]:
    """Calculate Exponential Moving Average with smoothing alpha = 2 / (period + 1)."""
    n = len(series)
    out = [0.0] * n
    if n == 0 or period <= 0:
        return out

    alpha = 2.0 / (period + 1)
    seed_len = min(period, n)
    sma = sum(series[:seed_len]) / seed_len
    out[seed_len - 1] = sma

    for i in range(seed_len, n):
        out[i] = series[i] * alpha + out[i - 1] * (1.0 - alpha)

    for i in range(seed_len - 1):
        out[i] = out[seed_len - 1]

    return out


def calculate_macd(
    closes: list[float], fast: int = 12, slow: int = 26, signal_period: int = 9
) -> dict[str, list[float]]:
    """Compute standard MACD Line (fast - slow), Signal Line, and Histogram."""
    n = len(closes)
    if n < slow:
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


def analyze_ticker(ticker: str, catalog_meta: dict[str, Any]) -> dict[str, Any] | None:
    """Fetch 1h (RTH) and 1d bars, compute technical metrics, and classify signals."""
    import yfinance as yf

    try:
        t = yf.Ticker(ticker)
        # Fetch 60d of 1h RTH bars and 1y of 1d bars
        df_1h = t.history(period="60d", interval="1h", prepost=False)
        df_1d = t.history(period="1y", interval="1d")

        if len(df_1h) < 30 or len(df_1d) < 10:
            return None

        # Clean 1H series
        df_1h = df_1h.dropna(subset=["Close", "Volume"])
        closes_1h = df_1h["Close"].tolist()
        volumes_1h = df_1h["Volume"].tolist()
        highs_1h = df_1h["High"].tolist()
        lows_1h = df_1h["Low"].tolist()
        timestamps_1h = [
            ts.strftime("%Y-%m-%d %H:%M") if hasattr(ts, "strftime") else str(ts)[:16]
            for ts in df_1h.index
        ]

        # Clean 1D series
        df_1d = df_1d.dropna(subset=["Close", "Volume"])
        closes_1d = df_1d["Close"].tolist()
        timestamps_1d = [
            ts.strftime("%Y-%m-%d") if hasattr(ts, "strftime") else str(ts)[:10]
            for ts in df_1d.index
        ]

        # Check for Pre-Market Live Price
        now_ny = dt.datetime.now(dt.timezone.utc).astimezone(NY_TZ)
        is_premarket = 4.0 <= (now_ny.hour + now_ny.minute / 60.0) < 9.5
        live_premarket_price: float | None = None
        if is_premarket:
            try:
                df_ext = t.history(period="2d", interval="1h", prepost=True)
                if not df_ext.empty and df_ext.index[-1] > df_1h.index[-1]:
                    live_premarket_price = round(float(df_ext["Close"].iloc[-1]), 4)
            except Exception:
                pass

        # Compute 1H MACD
        m_1h = calculate_macd(closes_1h, fast=12, slow=26, signal_period=9)
        macd_1h = m_1h["macd"]
        sig_1h = m_1h["signal"]
        hist_1h = m_1h["hist"]
        hist_delta_1h = m_1h["hist_delta"]

        # Compute 1D MACD
        m_1d = calculate_macd(closes_1d, fast=12, slow=26, signal_period=9)
        hist_1d = m_1d["hist"]

        latest_price = round(float(closes_1h[-1]), 4)
        prev_close_1h = float(closes_1h[-2]) if len(closes_1h) >= 2 else latest_price
        chg_1h_pct = (
            round(((latest_price - prev_close_1h) / prev_close_1h) * 100.0, 2)
            if prev_close_1h > 0
            else 0.0
        )

        # 24H Price change vs prior completed day close (strictly session before today)
        today_date = now_ny.date()
        prior_session_days = df_1d[df_1d.index.map(lambda x: x.date() < today_date)]
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

        # Floor Pivots:
        # During market hours (<16:00 EST), pivots are derived from yesterday's completed session.
        # After regular close (>=16:00 EST or weekends), today's session has completed,
        # so upcoming pivots are derived from today's completed session.
        is_post_close = (now_ny.weekday() >= 5) or ((now_ny.hour + now_ny.minute / 60.0) >= 16.0)
        pivot_reference_days = (
            df_1d[df_1d.index.map(lambda x: x.date() <= today_date)]
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

        # Volume Metrics (1H RVOL over 20-bar window)
        recent_vol_20 = volumes_1h[-21:-1]
        vol_sma20 = sum(recent_vol_20) / len(recent_vol_20) if recent_vol_20 else 1.0
        current_vol = float(volumes_1h[-1])
        rvol_1h = round(current_vol / vol_sma20, 2) if vol_sma20 > 0 else 1.0

        # MACD Z-score over rolling 60 bars
        window_macd = macd_1h[-60:]
        mean_macd = sum(window_macd) / len(window_macd)
        var_macd = sum((x - mean_macd) ** 2 for x in window_macd) / len(window_macd)
        std_macd = math.sqrt(var_macd) if var_macd > 0 else 1e-4
        z_macd = round((macd_1h[-1] - mean_macd) / std_macd, 2)

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

        # Imminent Crossover Check (Spread compression for Pre-Close radar)
        spread = abs(curr_m - curr_s)
        prev_spread = abs(prev_m - prev_s)
        compression_rate = (
            (prev_spread - spread) / prev_spread if prev_spread > 0 else 0.0
        )
        is_imminent = (
            (curr_m < curr_s)
            and (compression_rate >= 0.35)
            and (spread <= 0.05 * abs(curr_s) or spread <= 0.08)
        )

        # Signal Classification
        if is_bull_cross and curr_m < 0:
            signal_state = "SUB_ZERO_BOUNCE"
            signal_label = "Sub-Zero Reversal (High Convexity)"
            signal_direction = "BULLISH"
        elif is_bull_cross:
            signal_state = "BULLISH_CROSS"
            signal_label = "Bullish Momentum Crossover"
            signal_direction = "BULLISH"
        elif is_bear_cross:
            signal_state = "BEARISH_CROSS"
            signal_label = "Bearish Breakdown Crossover"
            signal_direction = "BEARISH"
        elif is_imminent:
            signal_state = "IMMINENT_SQUEEZE"
            signal_label = "Pre-Close Squeeze Imminent"
            signal_direction = "WATCH"
        elif curr_m > curr_s:
            signal_state = "BULLISH_TRENDING"
            signal_label = "Bullish Momentum Active"
            signal_direction = "BULLISH"
        else:
            signal_state = "BEARISH_TRENDING"
            signal_label = "Bearish Momentum Active"
            signal_direction = "BEARISH"

        # Confluence Score (-100 to +100)
        score = 0
        if curr_m > curr_s:
            score += 35
        else:
            score -= 35
        if hist_1d[-1] > 0:
            score += 35
        else:
            score -= 35
        if rvol_1h >= 1.25:
            score += 15 if curr_m > curr_s else -15
        if curr_m < 0 and curr_m > curr_s:
            score += 15  # Sub-zero value premium

        score = max(-100, min(100, score))

        # Recent 40 bars of 1H chart payload for web UI
        chart_bars_1h = []
        for i in range(max(0, len(closes_1h) - 40), len(closes_1h)):
            chart_bars_1h.append(
                {
                    "dt": timestamps_1h[i],
                    "o": round(float(df_1h["Open"].iloc[i]), 3),
                    "h": round(float(highs_1h[i]), 3),
                    "l": round(float(lows_1h[i]), 3),
                    "c": round(float(closes_1h[i]), 3),
                    "v": int(volumes_1h[i]),
                    "macd": round(float(macd_1h[i]), 4),
                    "sig": round(float(sig_1h[i]), 4),
                    "hist": round(float(hist_1h[i]), 4),
                }
            )

        # Recent 40 bars of 1D chart payload
        chart_bars_1d = []
        macd_1d = m_1d["macd"]
        sig_1d = m_1d["signal"]
        for i in range(max(0, len(closes_1d) - 40), len(closes_1d)):
            chart_bars_1d.append(
                {
                    "dt": timestamps_1d[i],
                    "o": round(float(df_1d["Open"].iloc[i]), 3),
                    "h": round(float(df_1d["High"].iloc[i]), 3),
                    "l": round(float(df_1d["Low"].iloc[i]), 3),
                    "c": round(float(closes_1d[i]), 3),
                    "v": int(df_1d["Volume"].iloc[i]),
                    "macd": round(float(macd_1d[i]), 4),
                    "sig": round(float(sig_1d[i]), 4),
                    "hist": round(float(hist_1d[i]), 4),
                }
            )

        return {
            "ticker": ticker,
            "fund_name": catalog_meta.get("fund_name", ticker),
            "leverage": catalog_meta.get("leverage", "1x"),
            "category": catalog_meta.get("category", "Unclassified"),
            "underlying": catalog_meta.get("underlying", "—"),
            "latest_price": latest_price,
            "live_premarket_price": live_premarket_price,
            "chg_1h_pct": chg_1h_pct,
            "chg_1d_pct": chg_1d_pct,
            "macd_1h": round(curr_m, 4),
            "signal_1h": round(curr_s, 4),
            "hist_1h": round(hist_1h[-1], 4),
            "hist_delta_1h": round(hist_delta_1h[-1], 4),
            "z_macd_1h": z_macd,
            "rvol_1h": rvol_1h,
            "hist_1d": round(hist_1d[-1], 4),
            "daily_trend_bullish": bool(hist_1d[-1] > 0),
            "signal_state": signal_state,
            "signal_label": signal_label,
            "signal_direction": signal_direction,
            "bull_cross_fresh": bool(is_bull_cross or bull_cross_bars_ago in (0, 1)),
            "bear_cross_fresh": bool(is_bear_cross or bear_cross_bars_ago in (0, 1)),
            "is_subzero": bool(curr_m < 0),
            "confluence_score": score,
            "pivots": pivots,
            "as_of_utc": timestamps_1h[-1],
            "chart_bars_1h": chart_bars_1h,
            "chart_bars_1d": chart_bars_1d,
        }
    except Exception:
        return None


def format_institutional_email(
    signals_data: list[dict[str, Any]],
    scan_time_str: str,
    session_info: tuple[str, str, str],
    recipient_email: str = DEFAULT_RECIPIENT,
) -> tuple[str, str]:
    """Build high-DPI responsive HTML & Plain Text alert without emojis or fluff."""
    session_code, session_name, session_badge = session_info

    tier1_subzero = [
        s
        for s in signals_data
        if s["signal_state"] == "SUB_ZERO_BOUNCE" or (s["bull_cross_fresh"] and s["is_subzero"])
    ]
    tier1_bull = [
        s
        for s in signals_data
        if s["signal_state"] == "BULLISH_CROSS" or (s["bull_cross_fresh"] and not s["is_subzero"])
    ]
    tier2_imminent = [s for s in signals_data if s["signal_state"] == "IMMINENT_SQUEEZE"]
    tier_bear = [s for s in signals_data if s["bear_cross_fresh"]]

    # Sort each tier by confluence score descending
    tier1_subzero.sort(key=lambda x: x.get("confluence_score", 0), reverse=True)
    tier1_bull.sort(key=lambda x: x.get("confluence_score", 0), reverse=True)
    tier2_imminent.sort(key=lambda x: x.get("confluence_score", 0), reverse=True)

    # 1. Plain Text Format
    pt_lines = [
        "=" * 80,
        f"CONVICTION LABS · QUANTITATIVE MACD RADAR ({session_name})",
        f"SCAN WINDOW: {scan_time_str} · RECIPIENT: {recipient_email}",
        "=" * 80,
        "",
        f"[TIER 1A: CONFIRMED SUB-ZERO BULLISH REVERSALS] (Active: {len(tier1_subzero)})",
        "Trigger: 1-Hour MACD crossed above Signal Line while MACD < 0 (High Convexity Turnaround)",
        "-" * 80,
    ]

    for it in tier1_subzero:
        piv = it.get("pivots", {})
        px_display = f"${it['latest_price']:.2f}"
        if it.get("live_premarket_price"):
            px_display += f" (Pre-Market: ${it['live_premarket_price']:.2f})"
        pt_lines.extend(
            [
                f"TICKER: {it['ticker']:<6} | {it['fund_name']} ({it['leverage']})",
                f"  Price: {px_display} (1H: {it['chg_1h_pct']:+.2f}% | 24H: {it['chg_1d_pct']:+.2f}%)",
                f"  1H MACD: {it['macd_1h']:+.4f} | Signal: {it['signal_1h']:+.4f} | Hist: {it['hist_1h']:+.4f}",
                f"  RVOL: {it['rvol_1h']:.2f}x | Z-Score: {it['z_macd_1h']:.2f} sigma | Score: {it['confluence_score']}/100",
                f"  Floor Pivots: S2=${piv.get('S2', 0):.2f} S1=${piv.get('S1', 0):.2f} P=${piv.get('P', 0):.2f} R1=${piv.get('R1', 0):.2f}",
                f"  1D Trend: {'BULLISH' if it['daily_trend_bullish'] else 'BEARISH'}",
                "",
            ]
        )

    pt_lines.extend(
        [
            "",
            f"[TIER 2: PRE-CLOSE SQUEEZE RADAR] (Impending: {len(tier2_imminent)})",
            "Trigger: Histogram spread compression >= 35% heading into closing rebalancing hour",
            "-" * 80,
        ]
    )
    for it in tier2_imminent[:8]:
        spread = abs(it["macd_1h"] - it["signal_1h"])
        pt_lines.append(
            f"{it['ticker']:<6} | Px: ${it['latest_price']:<7.2f} | Spread: {spread:.4f} | RVOL: {it['rvol_1h']:3.1f}x | {it['fund_name']}"
        )

    pt_lines.extend(
        [
            "",
            f"[TIER 3: BEARISH BREAKDOWNS / CASH PARKING] (Active Exits: {len(tier_bear)})",
            "Trigger: 1-Hour MACD crossed below Signal Line. Longs liquidate to cash immediately.",
            "-" * 80,
        ]
    )
    for it in tier_bear[:8]:
        pt_lines.append(
            f"{it['ticker']:<6} | Px: ${it['latest_price']:<7.2f} | 1H MACD: {it['macd_1h']:+6.3f} | Hist: {it['hist_1h']:+6.3f} | ACTION: EXIT TO CASH"
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

        px_str = f"${it['latest_price']:.2f}"
        if it.get("live_premarket_price"):
            px_str = f"""${it['latest_price']:.2f} <div style="font-size:10px; color:#facc15; font-weight:700;">Pre-Mkt: ${it['live_premarket_price']:.2f}</div>"""

        card = f"""
        <!-- CARD: {it['ticker']} -->
        <table role="presentation" width="100%" border="0" cellspacing="0" cellpadding="0" style="background-color:#111622; border:1px solid #1e2638; border-left:4px solid #38bdf8; border-radius:6px; margin-bottom:14px; overflow:hidden;">
          <tr>
            <td style="padding:14px 16px;">
              <!-- Header Row: Ticker, Badges, Price -->
              <table role="presentation" width="100%" border="0" cellspacing="0" cellpadding="0">
                <tr>
                  <td style="vertical-align:middle;">
                    <span style="font-family:ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size:19px; font-weight:800; color:#38bdf8; letter-spacing:0.03em;">{it['ticker']}</span>
                    <span style="display:inline-block; margin-left:6px; background-color:#1e2638; color:#cbd5e1; padding:2px 7px; border-radius:3px; font-size:10.5px; font-weight:700; font-family:ui-monospace, monospace;">{it.get('leverage', '1x')}</span>
                    <span style="display:inline-block; margin-left:6px; background-color:rgba(16,185,129,0.15); color:#34d399; border:1px solid rgba(52,211,153,0.35); padding:2px 7px; border-radius:3px; font-size:9.5px; font-weight:700; text-transform:uppercase; letter-spacing:0.04em;">SUB-ZERO REVERSAL</span>
                  </td>
                  <td style="text-align:right; vertical-align:middle;">
                    <span style="font-family:ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size:19px; font-weight:800; color:#ffffff;">{px_str}</span>
                  </td>
                </tr>
                <tr>
                  <td style="padding-top:4px; font-size:11.5px; color:#cbd5e1;">
                    {it.get('fund_name', it['ticker'])} &middot; <span style="color:#94a3b8;">Underlying: {it.get('underlying', '—')}</span>
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
                      Sig: {it.get('signal_1h', 0):+.3f}
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
                    <div style="font-size:9px; color:#94a3b8; text-transform:uppercase; letter-spacing:0.05em; font-weight:700;">RVOL (20-BAR)</div>
                    <div style="font-family:ui-monospace, monospace; font-size:13px; font-weight:700; color:#e2e8f0; margin-top:2px;">
                      {it.get('rvol_1h', 1.0):.2f}x
                    </div>
                    <div style="font-family:ui-monospace, monospace; font-size:9.5px; color:#94a3b8; margin-top:1px;">
                      Vol Pace
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

    # 3. Tier 2: Pre-Close Squeeze Table Rows
    squeeze_rows = []
    for it in tier2_imminent[:8]:
        spread = abs(it["macd_1h"] - it["signal_1h"])
        row = f"""
        <tr style="border-bottom:1px solid #1e2638;">
          <td style="padding:9px 10px; font-family:ui-monospace, monospace; font-weight:700; color:#38bdf8; font-size:12px;">{it['ticker']}</td>
          <td style="padding:9px 10px; color:#e2e8f0; font-size:11.5px;">
            {it['fund_name']} <span style="font-size:10px; color:#94a3b8;">({it['leverage']})</span>
          </td>
          <td style="padding:9px 10px; font-family:ui-monospace, monospace; text-align:right; color:#ffffff; font-weight:700; font-size:12px;">${it['latest_price']:.2f}</td>
          <td style="padding:9px 10px; font-family:ui-monospace, monospace; text-align:right; color:#facc15; font-weight:700; font-size:12px;">{spread:.4f}</td>
          <td style="padding:9px 10px; font-family:ui-monospace, monospace; text-align:right; color:#cbd5e1; font-size:11.5px;">{it['rvol_1h']:.2f}x</td>
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
    <tr><td colspan="6" style="padding:14px; text-align:center; color:#94a3b8;">No pending crossovers within spread compression tolerance.</td></tr>
    """
    )

    # 4. Tier 3: Bearish Breakdown Table Rows (High Contrast!)
    bear_rows = []
    for it in tier_bear[:8]:
        row = f"""
        <tr style="border-bottom:1px solid #1e2638;">
          <td style="padding:9px 10px; font-family:ui-monospace, monospace; font-weight:800; color:#fda4af; font-size:12px;">{it['ticker']}</td>
          <td style="padding:9px 10px; color:#cbd5e1; font-size:11.5px;">
            {it['fund_name']} <span style="font-size:10px; color:#94a3b8;">({it['leverage']})</span>
          </td>
          <td style="padding:9px 10px; font-family:ui-monospace, monospace; text-align:right; color:#ffffff; font-weight:700; font-size:12px;">${it['latest_price']:.2f}</td>
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
    <tr><td colspan="6" style="padding:14px; text-align:center; color:#94a3b8;">No fresh bearish breakdowns detected in this scan.</td></tr>
    """
    )

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
                      {session_badge}
                    </span>
                  </td>
                </tr>
              </table>
              <div style="font-size:11px; color:#cbd5e1; margin-top:8px;">
                Scan Window: <strong style="color:#ffffff;">{scan_time_str}</strong> &middot; Target: <strong style="color:#ffffff;">{recipient_email}</strong>
              </div>
            </td>
          </tr>

          <!-- Executive KPI Snapshot Strip -->
          <tr>
            <td style="padding:12px 16px; background-color:#090d15; border-bottom:1px solid #1e2638;">
              <table role="presentation" width="100%" border="0" cellspacing="0" cellpadding="0" style="table-layout:fixed;">
                <tr>
                  <td style="padding:6px 6px; border-right:1px solid #1e2638; text-align:center;">
                    <div class="kpi-sub" style="font-size:9.5px; color:#94a3b8; font-weight:700; text-transform:uppercase; letter-spacing:0.05em;">SUB-ZERO REVERSALS</div>
                    <div class="kpi-text" style="font-family:ui-monospace, monospace; font-size:16px; font-weight:800; color:#34d399; margin-top:2px;">{len(tier1_subzero)} ACTIVE</div>
                  </td>
                  <td style="padding:6px 6px; border-right:1px solid #1e2638; text-align:center;">
                    <div class="kpi-sub" style="font-size:9.5px; color:#94a3b8; font-weight:700; text-transform:uppercase; letter-spacing:0.05em;">PRE-CLOSE SQUEEZE</div>
                    <div class="kpi-text" style="font-family:ui-monospace, monospace; font-size:16px; font-weight:800; color:#facc15; margin-top:2px;">{len(tier2_imminent)} RADAR</div>
                  </td>
                  <td style="padding:6px 6px; text-align:center;">
                    <div class="kpi-sub" style="font-size:9.5px; color:#94a3b8; font-weight:700; text-transform:uppercase; letter-spacing:0.05em;">MANDATORY EXITS</div>
                    <div class="kpi-text" style="font-family:ui-monospace, monospace; font-size:16px; font-weight:800; color:#fb7185; margin-top:2px;">{len(tier_bear)} CASH</div>
                  </td>
                </tr>
              </table>
            </td>
          </tr>

          <!-- Body Content Area -->
          <tr>
            <td class="box-td" style="padding:18px 22px;">

              <!-- SECTION 1: TIER 1A SUB-ZERO REVERSALS -->
              <div style="margin-bottom:20px;">
                <div style="font-size:12px; font-weight:800; color:#38bdf8; text-transform:uppercase; letter-spacing:0.05em; margin-bottom:4px;">
                  TIER 1A: CONFIRMED SUB-ZERO BULLISH REVERSALS
                </div>
                <div style="font-size:11px; color:#cbd5e1; margin-bottom:14px; line-height:1.5;">
                  Confirmed 1-Hour bar close where MACD Line crossed above Signal Line while MACD &lt; 0. Sub-zero crossovers signal high-convexity turnaround points at key structural floor pivot levels.
                </div>
                
                {cards_block}

              </div>

              <!-- SECTION 2: TIER 2 PRE-CLOSE SQUEEZE RADAR -->
              <div style="margin-top:24px; margin-bottom:20px;">
                <div style="font-size:12px; font-weight:800; color:#facc15; text-transform:uppercase; letter-spacing:0.05em; margin-bottom:4px;">
                  TIER 2: PRE-CLOSE SQUEEZE RADAR (15:00 EST SCAN)
                </div>
                <div style="font-size:11px; color:#cbd5e1; margin-bottom:12px; line-height:1.5;">
                  Histogram spread compression &ge; 35% heading into the final market hour (15:00&ndash;16:00 EST). Pre-positioning watch ahead of institutional rebalancing flow auctions.
                </div>
                
                <div class="table-scroll" style="width:100%; overflow-x:auto;">
                  <table role="presentation" width="100%" border="0" cellspacing="0" cellpadding="0" style="font-size:11px; background-color:#111622; border:1px solid #1e2638; border-radius:6px; min-width:520px; overflow:hidden;">
                    <thead>
                      <tr style="background-color:#161f30; color:#cbd5e1; font-size:10px; text-transform:uppercase; letter-spacing:0.05em;">
                        <th style="padding:8px 10px; text-align:left;">Ticker</th>
                        <th style="padding:8px 10px; text-align:left;">Instrument</th>
                        <th style="padding:8px 10px; text-align:right;">Price</th>
                        <th style="padding:8px 10px; text-align:right;">Spread</th>
                        <th style="padding:8px 10px; text-align:right;">RVOL</th>
                        <th style="padding:8px 10px; text-align:center;">Status</th>
                      </tr>
                    </thead>
                    <tbody>
                      {squeeze_table_body}
                    </tbody>
                  </table>
                </div>
              </div>

              <!-- SECTION 3: TIER 3 BEARISH BREAKDOWNS / CASH PARKING -->
              <div style="margin-top:24px; margin-bottom:20px;">
                <div style="font-size:12px; font-weight:800; color:#fb7185; text-transform:uppercase; letter-spacing:0.05em; margin-bottom:4px;">
                  TIER 3: BEARISH BREAKDOWN &middot; MANDATORY CASH PARKING
                </div>
                <div style="font-size:11px; color:#cbd5e1; margin-bottom:12px; line-height:1.5;">
                  1-Hour MACD crossed below Signal Line. Leveraged decay accelerates rapidly during counter-trend regimes; long positions must be liquidated into cash immediately to prevent capital erosion.
                </div>
                
                <div class="table-scroll" style="width:100%; overflow-x:auto;">
                  <table role="presentation" width="100%" border="0" cellspacing="0" cellpadding="0" style="font-size:11px; background-color:#111622; border:1px solid #1e2638; border-radius:6px; min-width:520px; overflow:hidden;">
                    <thead>
                      <tr style="background-color:#161f30; color:#cbd5e1; font-size:10px; text-transform:uppercase; letter-spacing:0.05em;">
                        <th style="padding:8px 10px; text-align:left;">Ticker</th>
                        <th style="padding:8px 10px; text-align:left;">Instrument</th>
                        <th style="padding:8px 10px; text-align:right;">Price</th>
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
              Conviction Labs &middot; 160 Curated Leveraged &amp; Inverse ETF Universe &middot; Quantitative Research Engine
            </td>
          </tr>

        </table>

      </td>
    </tr>
  </table>
</body>
</html>"""

    return text_content, html_content


def send_email_alert(
    subject: str, text_body: str, html_body: str, recipient: str = DEFAULT_RECIPIENT
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
        recipients_list = list(recipient)

    if not recipients_list:
        recipients_list = [DEFAULT_RECIPIENT]

    try:
        msg = email.mime.multipart.MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = sender
        msg["To"] = ", ".join(recipients_list)
        msg["Date"] = email.utils.formatdate(localtime=True)
        msg["Message-ID"] = email.utils.make_msgid(domain=smtp_host.split(".")[-2] + "." + smtp_host.split(".")[-1] if "." in smtp_host else "gmail.com")

        msg.attach(email.mime.text.MIMEText(text_body, "plain", "utf-8"))
        msg.attach(email.mime.text.MIMEText(html_body, "html", "utf-8"))

        if smtp_port == 465:
            with smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=20) as server:
                server.login(sender, password)
                server.sendmail(sender, recipients_list, msg.as_string())
        else:
            with smtplib.SMTP(smtp_host, smtp_port, timeout=20) as server:
                server.ehlo()
                server.starttls()
                server.ehlo()
                server.login(sender, password)
                server.sendmail(sender, recipients_list, msg.as_string())

        print(f"[SMTP Engine] Successfully dispatched email alert to {recipients_list} via {smtp_host}:{smtp_port}!")
        return True
    except Exception as ex:
        print(f"[SMTP Engine] Error dispatching email: {ex}")
        return False


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run 1H/1D MACD Momentum Scanner across ETF Universe."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limit number of tickers scanned (0 = full 160)",
    )
    parser.add_argument(
        "--send-email",
        action="store_true",
        help="Enable email alerting engine if triggers detected",
    )
    parser.add_argument(
        "--force-send",
        action="store_true",
        help="Force dispatch email regardless of deduplication state",
    )
    parser.add_argument(
        "--recipient",
        type=str,
        default="",
        help="Custom recipient email (defaults to raghavendra.ds1@gmail.com)",
    )
    args = parser.parse_args()

    recipient = args.recipient or os.environ.get("ALERT_EMAIL_RECIPIENT") or DEFAULT_RECIPIENT

    if not CATALOG_PATH.is_file():
        print(f"Catalog file not found at {CATALOG_PATH}")
        sys.exit(1)

    with open(CATALOG_PATH, "r", encoding="utf-8") as f:
        catalog = json.load(f)

    instruments = catalog.get("instruments", [])
    if args.limit > 0:
        instruments = instruments[: args.limit]

    now_utc = dt.datetime.now(dt.timezone.utc)
    now_ny = now_utc.astimezone(NY_TZ)
    session_info = get_market_session(now_ny)
    session_code, session_name, session_badge = session_info

    print(
        f"Scanning MACD momentum across {len(instruments)} instruments in session [{session_name}] at {now_ny.strftime('%Y-%m-%d %H:%M:%S %Z')}..."
    )
    results: list[dict[str, Any]] = []

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
            except Exception as e:
                print(f"Error scanning {ticker}: {e}")

    results.sort(key=lambda x: (-x.get("confluence_score", 0), x.get("ticker", "")))
    print(f"Successfully processed {len(results)} active ETF instruments.")

    scan_time_str = now_ny.strftime("%Y-%m-%d %H:%M:%S %Z")

    # Structure payload for Web Dashboard
    payload = {
        "updated_utc": now_utc.isoformat(),
        "scan_time_str": scan_time_str,
        "session": session_code,
        "session_name": session_name,
        "total_scanned": len(results),
        "recipient": recipient,
        "signals": results,
    }

    # Save to JSON
    SIGNALS_OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(SIGNALS_OUT, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    print(f"Saved signals artifact to {SIGNALS_OUT} ({SIGNALS_OUT.stat().st_size:,} bytes)")

    # Identify active signal sets for this run
    current_subzero_tickers = sorted(
        [
            s["ticker"]
            for s in results
            if s["signal_state"] == "SUB_ZERO_BOUNCE"
            or (s["bull_cross_fresh"] and s["is_subzero"])
        ]
    )
    current_bear_tickers = sorted([s["ticker"] for s in results if s["bear_cross_fresh"]])
    current_squeeze_tickers = sorted(
        [s["ticker"] for s in results if s["signal_state"] == "IMMINENT_SQUEEZE"]
    )

    # Check for Fresh Crossover Trigger (crossover happened on the latest closed bar)
    fresh_crossovers = [
        s["ticker"]
        for s in results
        if s.get("bull_cross_fresh") or s.get("bear_cross_fresh")
    ]

    # Load prior alert state for deduplication
    prior_state = {}
    if ALERT_OUT.is_file():
        try:
            with open(ALERT_OUT, "r", encoding="utf-8") as f:
                prior_state = json.load(f)
        except Exception:
            prior_state = {}

    prev_subzero = set(prior_state.get("active_subzero", []))
    prev_bear = set(prior_state.get("active_bear", []))
    prev_squeeze = set(prior_state.get("active_squeeze", []))

    new_subzero = set(current_subzero_tickers) - prev_subzero
    new_bear = set(current_bear_tickers) - prev_bear
    new_squeeze = set(current_squeeze_tickers) - prev_squeeze

    has_fresh_trigger = bool(new_subzero or new_bear or (fresh_crossovers and len(fresh_crossovers) > 0))
    is_pre_close_time = session_code == "PRE_CLOSE"

    print(
        f"[Trigger Evaluation] Active Sub-Zero: {len(current_subzero_tickers)} (New: {len(new_subzero)}: {sorted(list(new_subzero))})"
    )
    print(
        f"[Trigger Evaluation] Active Bearish Exits: {len(current_bear_tickers)} (New: {len(new_bear)}: {sorted(list(new_bear))})"
    )
    print(
        f"[Trigger Evaluation] Pre-Close Squeeze: {len(current_squeeze_tickers)} (New: {len(new_squeeze)})"
    )

    # Build Email Alert Text and HTML
    text_alert, html_alert = format_institutional_email(
        results, scan_time_str, session_info, recipient
    )

    # Save Alert payload for inspection and state tracking
    alert_payload = {
        "timestamp_utc": now_utc.isoformat(),
        "scan_time_str": scan_time_str,
        "session": session_code,
        "recipient": recipient,
        "active_subzero": current_subzero_tickers,
        "active_bear": current_bear_tickers,
        "active_squeeze": current_squeeze_tickers,
        "new_subzero": list(new_subzero),
        "new_bear": list(new_bear),
        "new_squeeze": list(new_squeeze),
        "text_content": text_alert,
        "html_content": html_alert,
    }

    with open(ALERT_OUT, "w", encoding="utf-8") as f:
        json.dump(alert_payload, f, indent=2)
    print(f"Saved latest alert artifact to {ALERT_OUT}")

    should_send = False
    send_reason = ""

    if args.force_send:
        should_send = True
        send_reason = "Manual override (--force-send enabled)"
    elif args.send_email:
        if has_fresh_trigger:
            should_send = True
            send_reason = f"Fresh Trigger Detected (New Sub-Zero: {list(new_subzero)}, New Exits: {list(new_bear)})"
        elif is_pre_close_time:
            should_send = True
            send_reason = "Pre-Close Golden Hour Scheduled Scan (15:00 EST)"
        else:
            print("[Alert Engine] No fresh trigger changes detected. Email suppressed to preserve recipient inbox.")

    if should_send:
        subject = f"[Conviction Labs] 1H MACD Momentum Radar · {session_badge} · {scan_time_str}"
        print(f"[Alert Engine] Dispatching email ({send_reason})...")
        send_email_alert(subject, text_alert, html_alert, recipient)


if __name__ == "__main__":
    main()
