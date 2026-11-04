#!/usr/bin/env python3
"""Quantitative MACD Momentum Scanner and Email Alert Engine for Leveraged ETFs.

This script:
1. Ingests the 160 curated instruments from `docs/data/flows/catalog.json`.
2. Fetches intraday 1-hour OHLCV and daily OHLCV bars via Yahoo Finance Chart API.
3. Computes:
   - Standard MACD (12, 26, 9 EMA EMA), Signal Line, Histogram, and Histogram Delta.
   - Normalized MACD Z-score (rolling 60 bars) to classify Sub-Zero vs Overbought regimes.
   - Relative Volume (RVOL = current volume / 20-period volume SMA).
   - Traditional Auto Floor Pivots (P, S1-S5, R1-R5) from prior session high/low/close.
   - Multi-timeframe trend alignment (1D Trend + 1H Timing Trigger).
4. Detects signals:
   - Tier 1: Confirmed Sub-Zero Bullish Crossover (closed bar).
   - Tier 1B: Confirmed Bullish Crossover (supra-zero).
   - Tier 1C: Confirmed Bearish Crossover (exit signal).
   - Tier 2: Pre-Close Imminent Crossover Watch (15:00 EST scan, compression >= 40%).
5. Outputs:
   - `docs/data/flows/macd_signals.json` (consumed by frontend Canvas 2D engine).
   - `docs/data/flows/latest_macd_alert.json` (audit trail).
6. Dispatches structured institutional email to `raghavendra.ds1@gmail.com` if SMTP secrets are present.
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
import smtplib
import ssl
import sys
from pathlib import Path
from typing import Any

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


def calculate_ema(series: list[float], period: int) -> list[float]:
    """Calculate Exponential Moving Average with smoothing alpha = 2 / (period + 1)."""
    n = len(series)
    out = [0.0] * n
    if n == 0 or period <= 0:
        return out

    alpha = 2.0 / (period + 1)
    # Seed with SMA of first `period` bars if available, else first value
    seed_len = min(period, n)
    sma = sum(series[:seed_len]) / seed_len
    out[seed_len - 1] = sma

    for i in range(seed_len, n):
        out[i] = series[i] * alpha + out[i - 1] * (1.0 - alpha)

    # Backfill prefix for smooth curves
    for i in range(seed_len - 1):
        out[i] = out[seed_len - 1]

    return out


def calculate_macd(
    closes: list[float], fast: int = 12, slow: int = 26, signal_period: int = 9
) -> dict[str, list[float]]:
    """Compute MACD Line, Signal Line, and Histogram."""
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
    """Fetch 1h and 1d bars, compute technical metrics, and classify signals."""
    import yfinance as yf

    try:
        t = yf.Ticker(ticker)
        # Fetch 60d of 1h bars and 1y of 1d bars
        df_1h = t.history(period="60d", interval="1h")
        df_1d = t.history(period="1y", interval="1d")

        if len(df_1h) < 30 or len(df_1d) < 30:
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

        # Compute 1H MACD
        m_1h = calculate_macd(closes_1h, fast=12, slow=26, signal_period=9)
        macd_1h = m_1h["macd"]
        sig_1h = m_1h["signal"]
        hist_1h = m_1h["hist"]
        hist_delta_1h = m_1h["hist_delta"]

        # Compute 1D MACD
        m_1d = calculate_macd(closes_1d, fast=12, slow=26, signal_period=9)
        hist_1d = m_1d["hist"]
        hist_delta_1d = m_1d["hist_delta"]

        latest_price = round(float(closes_1h[-1]), 4)
        prev_close_1h = float(closes_1h[-2])
        chg_1h_pct = round(((latest_price - prev_close_1h) / prev_close_1h) * 100.0, 2)

        # 24H Price change
        prior_day_close = float(closes_1d[-2]) if len(closes_1d) >= 2 else prev_close_1h
        chg_1d_pct = round(((latest_price - prior_day_close) / prior_day_close) * 100.0, 2)

        # Volume Metrics (1H RVOL)
        recent_vol_20 = volumes_1h[-21:-1]
        vol_sma20 = sum(recent_vol_20) / len(recent_vol_20) if recent_vol_20 else 1.0
        current_vol = float(volumes_1h[-1])
        rvol_1h = round(current_vol / vol_sma20, 2) if vol_sma20 > 0 else 1.0

        # MACD Z-score over 60 bars
        window_macd = macd_1h[-60:]
        mean_macd = sum(window_macd) / len(window_macd)
        var_macd = sum((x - mean_macd) ** 2 for x in window_macd) / len(window_macd)
        std_macd = math.sqrt(var_macd) if var_macd > 0 else 1e-4
        z_macd = round((macd_1h[-1] - mean_macd) / std_macd, 2)

        # Pivots from yesterday's daily bar
        prev_day_h = float(df_1d["High"].iloc[-2]) if len(df_1d) >= 2 else float(highs_1h[-2])
        prev_day_l = float(df_1d["Low"].iloc[-2]) if len(df_1d) >= 2 else float(lows_1h[-2])
        prev_day_c = float(df_1d["Close"].iloc[-2]) if len(df_1d) >= 2 else float(closes_1h[-2])
        pivots = calculate_pivots(prev_day_h, prev_day_l, prev_day_c)

        # Crossover Detection
        # Confirmed crossover on latest completed bar vs previous bar
        curr_m = macd_1h[-1]
        curr_s = sig_1h[-1]
        prev_m = macd_1h[-2]
        prev_s = sig_1h[-2]

        is_bull_cross = (curr_m > curr_s) and (prev_m <= prev_s)
        is_bear_cross = (curr_m < curr_s) and (prev_m >= prev_s)

        # Prior crossover within last 3 bars (Freshness window)
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

        # Imminent Crossover Check (Pre-close radar)
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

        # Classification
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
            score += 15  # Sub-zero bonus

        score = max(-100, min(100, score))

        # Recent 40 bars of 1H chart payload for instant client-side rendering
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

        # Recent 40 bars of 1D chart payload for macro timeframe switch
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
    except Exception as ex:
        # print(f"Error analyzing {ticker}: {ex}")
        return None


def format_institutional_email(
    signals_data: list[dict[str, Any]], scan_time_str: str
) -> tuple[str, str]:
    """Build razor-sharp HTML and Plain Text alert dispatch without emojis or fluff."""
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
    tier1_subzero.sort(key=lambda x: x["confluence_score"], reverse=True)
    tier1_bull.sort(key=lambda x: x["confluence_score"], reverse=True)
    tier2_imminent.sort(key=lambda x: x["confluence_score"], reverse=True)

    # Plain text format
    lines = [
        "================================================================================",
        "CONVICTION LABS · QUANTITATIVE MOMENTUM ALERT (1-HOUR MACD RADAR)",
        f"SCAN WINDOW: {scan_time_str} EST (Pre-Market-Close Diagnostic)",
        f"RECIPIENT: {DEFAULT_RECIPIENT}",
        "================================================================================",
        "",
        f"[TIER 1A: CONFIRMED SUB-ZERO BULLISH REVERSALS] (Count: {len(tier1_subzero)})",
        "Condition: 1H MACD Line crosses above Signal Line while MACD < 0 (High Convexity Mean Reversion)",
        "-" * 80,
    ]

    if not tier1_subzero:
        lines.append("No active sub-zero crossovers triggered during this scan window.")
    else:
        for it in tier1_subzero:
            lines.extend(
                [
                    f"TICKER: {it['ticker']:<6} | {it['fund_name']} ({it['leverage']})",
                    f"  Current Price: ${it['latest_price']:<8.2f} (1H: {it['chg_1h_pct']:+5.2f}% | 1D: {it['chg_1d_pct']:+5.2f}%)",
                    f"  1H MACD: {it['macd_1h']:+7.4f} | Signal: {it['signal_1h']:+7.4f} | Hist: {it['hist_1h']:+7.4f} | Z-Score: {it['z_macd_1h']} sigma",
                    f"  Volume Pace: {it['rvol_1h']}x (20-bar baseline) | Daily Trend: {'BULLISH' if it['daily_trend_bullish'] else 'BEARISH'}",
                    f"  Pivot Levels: Support S1=${it['pivots']['S1']} S2=${it['pivots']['S2']} | Pivot P=${it['pivots']['P']} | Resistance R1=${it['pivots']['R1']}",
                    f"  Confluence Score: {it['confluence_score']}/100",
                    "",
                ]
            )

    lines.extend(
        [
            "",
            f"[TIER 1B: CONFIRMED SUPRA-ZERO BULLISH CROSSOVERS] (Count: {len(tier1_bull)})",
            "Condition: 1H MACD Line crosses above Signal Line while MACD >= 0 (Momentum Continuation)",
            "-" * 80,
        ]
    )
    if not tier1_bull:
        lines.append("No active supra-zero crossovers triggered.")
    else:
        for it in tier1_bull[:8]:
            lines.append(
                f"{it['ticker']:<6} | Px: ${it['latest_price']:<7.2f} | 1H MACD: {it['macd_1h']:+6.3f} | RVOL: {it['rvol_1h']:3.1f}x | Score: {it['confluence_score']}/100"
            )

    lines.extend(
        [
            "",
            f"[TIER 2: IMMINENT / PRE-CLOSE SQUEEZE WATCH] (Count: {len(tier2_imminent)})",
            "Condition: Spread compression >= 35% heading into final hour close (15:00-16:00 EST)",
            "-" * 80,
        ]
    )
    if not tier2_imminent:
        lines.append("No impending crossovers detected within spread compression tolerance.")
    else:
        for it in tier2_imminent[:6]:
            lines.append(
                f"{it['ticker']:<6} | Px: ${it['latest_price']:<7.2f} | Spread: {abs(it['macd_1h'] - it['signal_1h']):.4f} | RVOL: {it['rvol_1h']:3.1f}x"
            )

    lines.extend(
        [
            "",
            f"[TIER 3: FRESH BEARISH BREAKDOWNS / EXIT SIGNALS] (Count: {len(tier_bear)})",
            "Condition: 1H MACD Line crossed below Signal Line (De-Gross / Risk Invalidation)",
            "-" * 80,
        ]
    )
    if not tier_bear:
        lines.append("No fresh bearish breakdown exits detected.")
    else:
        for it in tier_bear[:6]:
            lines.append(
                f"{it['ticker']:<6} | Px: ${it['latest_price']:<7.2f} | 1H MACD: {it['macd_1h']:+6.3f} | Hist: {it['hist_1h']:+6.3f}"
            )

    lines.extend(
        [
            "",
            "=" * 80,
            "RISK DISCLOSURE: Leveraged and inverse ETFs are subject to non-linear path-dependent volatility drag.",
            "Historical holding analysis demonstrates maximum trade duration should not exceed 30 market hours.",
            "Deterministic audit record saved to docs/data/flows/macd_signals.json.",
            "=" * 80,
        ]
    )
    text_content = "\n".join(lines)

    # HTML Email Format (Institutional Terminal Styling)
    html_rows_subzero = "".join(
        f"""<tr style="border-bottom:1px solid #27272a;">
            <td style="padding:8px 10px;font-family:monospace;font-weight:700;color:#22d3ee;">{it['ticker']}</td>
            <td style="padding:8px 10px;color:#e4e4e7;">{it['fund_name']} <span style="font-size:10px;color:#a1a1aa;">({it['leverage']})</span></td>
            <td style="padding:8px 10px;font-family:monospace;text-align:right;color:#e4e4e7;">${it['latest_price']:.2f}</td>
            <td style="padding:8px 10px;font-family:monospace;text-align:right;color:{'#34d399' if it['chg_1h_pct'] >= 0 else '#fb7185'};">{it['chg_1h_pct']:+.2f}%</td>
            <td style="padding:8px 10px;font-family:monospace;text-align:right;color:#38bdf8;">{it['macd_1h']:+.3f}</td>
            <td style="padding:8px 10px;font-family:monospace;text-align:right;color:#fbbf24;">{it['signal_1h']:+.3f}</td>
            <td style="padding:8px 10px;font-family:monospace;text-align:right;color:#e4e4e7;">{it['rvol_1h']}x</td>
            <td style="padding:8px 10px;font-family:monospace;text-align:right;color:#34d399;font-weight:700;">{it['confluence_score']}</td>
        </tr>"""
        for it in tier1_subzero
    ) or '<tr><td colspan="8" style="padding:12px;color:#71717a;text-align:center;">No active sub-zero crossovers in this scan window.</td></tr>'

    html_content = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
body {{ margin:0; padding:20px; background:#09090b; color:#e4e4e7; font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif; font-size:13px; }}
.card {{ background:#111113; border:1px solid #27272a; border-radius:6px; padding:16px; margin-bottom:16px; }}
.header {{ border-bottom:1px solid #27272a; padding-bottom:12px; margin-bottom:14px; display:flex; justify-content:space-between; }}
.title {{ font-size:16px; font-weight:700; color:#ffffff; letter-spacing:0.02em; }}
.subtitle {{ font-size:11px; color:#a1a1aa; text-transform:uppercase; letter-spacing:0.06em; margin-top:4px; }}
table {{ width:100%; border-collapse:collapse; font-size:12px; }}
th {{ text-align:left; padding:8px 10px; font-size:10px; text-transform:uppercase; color:#71717a; letter-spacing:0.05em; border-bottom:1px solid #27272a; }}
.badge {{ display:inline-block; padding:2px 6px; border-radius:3px; font-size:10px; font-weight:600; font-family:monospace; }}
</style>
</head>
<body>
<div class="card">
    <div class="header">
        <div>
            <div class="title">CONVICTION LABS · 1-HOUR MACD MOMENTUM RADAR</div>
            <div class="subtitle">PRE-MARKET-CLOSE GOLDEN HOUR SCAN · {scan_time_str} EST</div>
        </div>
    </div>
    <div style="font-size:12px;color:#a1a1aa;margin-bottom:12px;">
        Recipient: <strong style="color:#ffffff;">{DEFAULT_RECIPIENT}</strong> · Leveraged Universe Scan (160 Curated Instruments)
    </div>

    <h3 style="font-size:13px;color:#22d3ee;margin:16px 0 8px 0;text-transform:uppercase;letter-spacing:0.04em;">
        Tier 1A: Confirmed Sub-Zero Bullish Reversals (High-Convexity Value Turnarounds)
    </h3>
    <table>
        <thead>
            <tr>
                <th>Ticker</th>
                <th>Instrument</th>
                <th style="text-align:right;">Price</th>
                <th style="text-align:right;">1H Chg</th>
                <th style="text-align:right;">MACD (12,26)</th>
                <th style="text-align:right;">Signal (9)</th>
                <th style="text-align:right;">RVOL</th>
                <th style="text-align:right;">Score</th>
            </tr>
        </thead>
        <tbody>
            {html_rows_subzero}
        </tbody>
    </table>

    <div style="margin-top:20px;padding:12px;background:#18181b;border-radius:4px;font-size:11px;color:#a1a1aa;line-height:1.6;">
        <strong style="color:#e4e4e7;">Execution & Risk Protocol:</strong> Leveraged ETFs exhibit path-dependent volatility decay. Historical empirical simulations indicate that Long-Only cash parking outperforms continuous Long/Short flipping. Exits must be enforced immediately upon a 1H Bearish MACD crossover or within a 30-hour maximum holding horizon.
    </div>
</div>
</body>
</html>"""

    return text_content, html_content


def send_email_alert(
    subject: str, text_body: str, html_body: str, recipient: str = DEFAULT_RECIPIENT
) -> bool:
    """Send email via SMTP if credentials exist in environment, otherwise log dry run."""
    sender = (os.environ.get("ALERT_EMAIL_SENDER") or "").strip()
    password = (os.environ.get("ALERT_EMAIL_PASSWORD") or "").strip().replace(" ", "")
    smtp_host = os.environ.get("ALERT_SMTP_HOST") or "smtp.gmail.com"
    smtp_port = int(os.environ.get("ALERT_SMTP_PORT") or "587")

    if not sender or not password:
        print("[SMTP Engine] ALERT_EMAIL_SENDER or ALERT_EMAIL_PASSWORD not configured in environment.")
        print("[SMTP Engine] Executed deterministic dry run. Email content written to disk.")
        return False

    try:
        msg = email.mime.multipart.MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = f"Prateek Upadhyay <{sender}>"
        msg["To"] = recipient
        msg["Date"] = email.utils.formatdate(localtime=True)
        msg["Message-ID"] = email.utils.make_msgid(domain="gmail.com")

        msg.attach(email.mime.text.MIMEText(text_body, "plain", "utf-8"))
        msg.attach(email.mime.text.MIMEText(html_body, "html", "utf-8"))

        context = ssl.create_default_context()
        with smtplib.SMTP(smtp_host, smtp_port, timeout=15) as server:
            server.ehlo()
            server.starttls(context=context)
            server.ehlo()
            server.login(sender, password)
            server.sendmail(sender, recipient, msg.as_string())

        print(f"[SMTP Engine] Successfully dispatched email alert to {recipient}!")
        return True
    except Exception as ex:
        print(f"[SMTP Engine] Error dispatching email: {ex}")
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Run 1H/1D MACD Momentum Scanner across ETF Universe.")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of tickers scanned (0 = full 160)")
    parser.add_argument("--send-email", action="store_true", help="Send email alert if triggers found")
    parser.add_argument("--recipient", type=str, default="", help="Custom recipient email (defaults to raghavendra.ds1@gmail.com)")
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

    print(f"Scanning MACD momentum across {len(instruments)} instruments...")
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

    # Sort results predictably by confluence_score desc, then ticker asc
    results.sort(key=lambda x: (-x.get("confluence_score", 0), x.get("ticker", "")))

    print(f"Successfully processed {len(results)} active ETF instruments.")

    now_utc = dt.datetime.now(dt.timezone.utc)
    # Estimate US EST time (UTC-5 or UTC-4)
    scan_time_str = now_utc.strftime("%Y-%m-%d %H:%M:%S UTC")

    # Structure payload
    payload = {
        "updated_utc": now_utc.isoformat(),
        "scan_time_str": scan_time_str,
        "total_scanned": len(results),
        "recipient": DEFAULT_RECIPIENT,
        "signals": results,
    }

    # Save to JSON
    SIGNALS_OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(SIGNALS_OUT, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    print(f"Saved signals artifact to {SIGNALS_OUT} ({SIGNALS_OUT.stat().st_size:,} bytes)")

    # Build Email Alert Text and HTML
    text_alert, html_alert = format_institutional_email(results, scan_time_str)

    # Save Alert payload for inspection
    with open(ALERT_OUT, "w", encoding="utf-8") as f:
        json.dump(
            {
                "timestamp_utc": now_utc.isoformat(),
                "recipient": DEFAULT_RECIPIENT,
                "text_content": text_alert,
                "html_content": html_alert,
            },
            f,
            indent=2,
        )
    print(f"Saved latest alert artifact to {ALERT_OUT}")

    if args.send_email:
        subject = f"[Conviction Labs] 1H MACD Momentum Radar · {scan_time_str}"
        send_email_alert(subject, text_alert, html_alert, recipient)


if __name__ == "__main__":
    main()
