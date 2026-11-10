#!/usr/bin/env python3
"""
Natural Gas ETF Data Pipeline
==============================
Fetches historical price/volume data for 6 Natural Gas ETFs via the Yahoo
Finance v8 chart API (no external dependencies beyond stdlib + numpy/pandas).
Computes a full suite of volume and price metrics and writes structured JSON
for the dashboard.

Designed to be run by GitHub Actions on a schedule, or locally.
"""

import json
import logging
import math
import ssl
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("data_pipeline")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DOCS_DATA_DIR = PROJECT_ROOT / "docs" / "data"
DASHBOARD_JSON = DATA_DIR / "dashboard_data.json"
SIGNALS_JSON = DATA_DIR / "latest_signals.json"
SPLIT_WARNINGS_JSON = DATA_DIR / "split_warnings.json"
KNOWN_SPLITS_JSON   = DATA_DIR / "known_splits.json"

# Any single-day price ratio >= this value is impossible for a 3x leveraged gas
# ETF to produce organically (would require NG=F to move ~100% in one day) and
# is therefore treated as an unregistered split/consolidation event.
SPLIT_ANOMALY_THRESHOLD = 4.0

YAHOO_BASE_URL = "https://query1.finance.yahoo.com/v8/finance/chart/"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
MAX_RETRIES = 3
RETRY_DELAY_SECS = 2

ETF_CONFIG: Dict[str, Dict[str, str]] = {
    "BOIL": {
        "side": "long",
        "pair": "KOLD",
        "name": "ProShares Ultra Bloomberg Natural Gas",
    },
    "HNU.TO": {
        "side": "long",
        "pair": "HND.TO",
        "name": "Betapro Natural Gas 2x Bull",
    },
    "3NGL.L": {
        "side": "long",
        "pair": "3NGS.L",
        "name": "WisdomTree Natural Gas 3x Daily Long",
    },
    "KOLD": {
        "side": "short",
        "pair": "BOIL",
        "name": "ProShares UltraShort Bloomberg Natural Gas",
    },
    "HND.TO": {
        "side": "short",
        "pair": "HNU.TO",
        "name": "Betapro Natural Gas Inverse 2x",
    },
    "3NGS.L": {
        "side": "short",
        "pair": "3NGL.L",
        "name": "WisdomTree Natural Gas 3x Daily Short",
    },
}

PAIRS = [
    ("BOIL", "KOLD"),
    ("HNU.TO", "HND.TO"),
    ("3NGL.L", "3NGS.L"),
]

RVOL_WINDOWS = [5, 10, 21, 63, 126, 252]   # +5d for fast-window layer
ZSCORE_WINDOWS = RVOL_WINDOWS
VOL_PCT_WINDOWS = RVOL_WINDOWS
PRICE_PCT_WINDOWS = RVOL_WINDOWS
VROC_WINDOWS = [5, 10, 21]
MA_WINDOWS = [10, 21, 50, 200]
ROLLING_CORR_WINDOW = 30

# Volatility modelling windows
HV_WINDOWS = [10, 21, 63, 252]  # Realized historical volatility windows
ATR_WINDOW = 14                  # ATR lookback
VOV_WINDOW = 21                  # Vol-of-vol: std of 10d HV over this many periods
VOL_REGIME_WINDOW = 252          # Window for vol regime percentile ranking

# VPS weights — now 5-component; vol regime inverse adds context-sensitivity
VPS_W_RVOL = 0.25       # was 0.30
VPS_W_ZSCORE = 0.20     # was 0.25
VPS_W_PCT = 0.25        # was 0.30
VPS_W_VROC = 0.10       # was 0.15
VPS_W_VOLREGIME = 0.20  # NEW: inverted vol regime (high when vol is quiet → signals stronger)

# CVI / VCVI alert thresholds
CVI_WARNING = 60
CVI_CRITICAL = 80
VCVI_WARNING = 55   # VCVI thresholds slightly lower since it's already vol-adjusted
VCVI_CRITICAL = 72
VPS_ELEVATED = 60
VPS_CRITICAL = 80

# Volatility alert thresholds
VOV_WARNING = 60          # Vol-of-vol: regime becoming unstable
VOV_CRITICAL = 90         # Vol-of-vol: extreme regime instability
ATR_BREAKOUT_MULT = 2.0   # Alert when daily move > N × ATR-14
ATR_BREAKOUT_RVOL = 1.5   # ...and RVOL exceeds this
VOL_REGIME_HIGH = 80      # Alert when vol regime is in top 20th pct of own history

# Conviction Event thresholds — strict multi-gate filter for true anomalies (~1-2/yr)
CONVICTION_VCVI_MIN = 72        # Gate 1: VCVI-21 must reach "critical" level
CONVICTION_BREADTH_MIN = 3      # Gate 2: min N of 5 vol pct windows ≥ 85th pct
CONVICTION_BREADTH_PCT = 85     # Gate 2: percentile threshold per window
CONVICTION_ATR_MULT = 1.5       # Gate 3: |daily move| > N × ATR-14
CONVICTION_VOL_REGIME_MAX = 70  # Gate 4: vol regime must be ≤ this (non-turbulent)
CONVICTION_MIN_GAP_DAYS = 15    # Dedup: min calendar days between distinct events

# Elevated Watch thresholds — 3-gate softer filter (~4-8 events/ETF/year)
WATCH_VCVI_MIN = 60             # Gate 1: VCVI-21 (lower bar than conviction)
WATCH_BREADTH_MIN = 2           # Gate 2: only 2 of N windows required
WATCH_BREADTH_PCT = 75          # Gate 2: 75th pct (vs 85th for conviction)
WATCH_ATR_MULT = 1.2            # Gate 3: 1.2× ATR (vs 1.5× for conviction)
WATCH_MIN_GAP_DAYS = 7          # Dedup interval (tighter than conviction)

# Extreme override — fires even if VCVI < CONVICTION_VCVI_MIN (bypasses Gate 1 only)
EXTREME_OVERRIDE_VCVI_MIN = 90  # Must be >= 90 (exceptional capitulation)
EXTREME_OVERRIDE_ATR_MULT = 2.0 # AND move > 2× ATR-14 (severe price dislocation)

# NG=F seasonal z-score thresholds for Gate 5 directional confirmation
CONVICTION_NG_Z_LONG  = -0.5   # Long-side fires only when gas z-score ≤ -0.5 (seasonally low)
CONVICTION_NG_Z_SHORT =  0.2   # Short-side fires only when gas z-score ≥ +0.2 (early move sufficient)

# Momentum guard — raises VCVI bar for short-side when NG=F is in uptrend
MOMENTUM_GUARD_VCVI_BOOST = 13  # Add to CONVICTION_VCVI_MIN when gas seasonal_z > 0

# NG=F Volatility Regime Detection — 3-tier classification
# Natural gas can enter "ultra-volatile" outlier regimes where typical signal
# patterns may not hold (e.g., 2022 bull run at ~$9/MMBtu, Jan 2026 >$7/MMBtu).
# Each historical signal is tagged with its ambient regime for stratified analysis.
NG_REGIME_EXTREME_PRICE   = 7.0   # > $7/MMBtu → extreme outlier (Jan 2026 analog)
NG_REGIME_HIGH_PRICE      = 4.5   # > $4.5/MMBtu → elevated
NG_REGIME_EXTREME_Z       = 2.5   # |seasonal z| ≥ 2.5σ → extreme (2022 analog)
NG_REGIME_ELEVATED_Z      = 1.5   # |seasonal z| ≥ 1.5σ → elevated
NG_REGIME_EXTREME_HV_PCT  = 90    # NG=F 21d HV at 90th pct of its own 2yr history → extreme
NG_REGIME_ELEVATED_HV_PCT = 70    # NG=F 21d HV at 70th pct → elevated

# Fast-window spike detection (Feature 1)
FAST_VCVI_THRESHOLD = 45        # 5d VCVI threshold for weather-spike flag
SHARP_SPIKE_ATR_MULT = 2.0      # |daily move| > N × ATR to qualify as sharp spike

# Gas price level gate — NG=F Henry Hub futures (Feature 2)
NG_TICKER = "NG=F"
NG_PRICE_WINDOW_DAYS = 504      # ~2 trading years (252 × 2)
NG_HIGH_QUARTILE = 75.0         # Short-side signals credible when gas in top 25%
NG_LOW_QUARTILE = 25.0          # Long-side signals credible when gas in bottom 25%

# Leveraged ETF annual decay rates (Feature 6)
ETF_ANNUAL_DECAY = {
    "BOIL":   0.35,   # 2x long ~35%/yr from daily rebalancing
    "KOLD":   0.35,   # 2x short ~35%/yr
    "HNU.TO": 0.40,   # 2x Canadian — slightly higher drag
    "HND.TO": 0.40,
    "3NGL.L": 0.55,   # 3x long — ~55%/yr
    "3NGS.L": 0.55,   # 3x short — ~55%/yr
}
DECAY_CORRECTION_WINDOW = 252   # Rolling window (1yr) for decay-adjusted percentile

# Hardcoded seed — used ONLY to bootstrap data/known_splits.json on first run.
# After that, known_splits.json is the sole source of truth and is auto-updated
# when new splits are detected from price data.
_SPLITS_SEED: Dict[str, List[Tuple[str, float]]] = {
    "3NGL.L": [
        ("2016-03-18", 10.0),
        ("2019-02-25", 10.0),
        ("2020-04-17", 10.0),
        ("2023-03-27", 10.0),
        ("2024-01-12", 10.0),
        ("2024-07-22", 420.0),
        ("2024-09-09", 10.0),
        ("2026-03-03", 10.0),
    ],
    "3NGS.L": [
        ("2019-06-04", 10.0),
        ("2021-09-15", 10.0),
        ("2022-05-30", 10.0),
        ("2022-09-12", 10.0),
        ("2022-12-19", 17000.0),
        ("2024-07-22", 1.0 / 17),
    ],
}


def _load_known_splits() -> Dict[str, List[Tuple[str, float]]]:
    """Load split history from data/known_splits.json.

    Falls back to the hardcoded _SPLITS_SEED if the file is absent or corrupt
    (e.g. first run in a fresh clone).
    """
    if KNOWN_SPLITS_JSON.exists():
        try:
            with open(KNOWN_SPLITS_JSON) as f:
                raw = json.load(f)
            loaded: Dict[str, List[Tuple[str, float]]] = {}
            for ticker, entries in raw.get("splits", {}).items():
                loaded[ticker] = [(e["date"], float(e["ratio"])) for e in entries]
            logger.info(
                "Loaded known splits for %d ticker(s) from %s",
                len(loaded), KNOWN_SPLITS_JSON.name,
            )
            return loaded
        except Exception as exc:
            logger.warning(
                "Could not load %s (%s) — falling back to hardcoded seed",
                KNOWN_SPLITS_JSON, exc,
            )
    return {k: list(v) for k, v in _SPLITS_SEED.items()}


def _save_known_splits(splits: Dict[str, List[Tuple[str, float]]]) -> None:
    """Persist the splits dict to data/known_splits.json.

    The file is committed automatically by GitHub Actions alongside dashboard
    data, so new auto-detected splits are permanent after the next push.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    payload: dict = {
        "note": (
            "Auto-managed by data_pipeline.py. New corporate actions are appended "
            "automatically when detected from price data. "
            "'source': 'manual' = verified from WisdomTree/LSE filings; "
            "'auto_detected' = inferred from price discontinuity."
        ),
        "splits": {
            ticker: [{"date": d, "ratio": r} for d, r in sorted(entries)]
            for ticker, entries in sorted(splits.items())
        },
    }
    with open(KNOWN_SPLITS_JSON, "w") as f:
        json.dump(payload, f, indent=2)
    logger.info("Saved known splits → %s", KNOWN_SPLITS_JSON.name)

# Side-Wide Volume Convergence (SWVC) — rolling tri-ETF same-side detection
SWVC_RVOL_THRESHOLD = 2.0   # Min RVOL-21d to qualify as a "spike" for one ETF
SWVC_LOOKBACK_DAYS  = 15    # How many trading days back to search for spikes
SWVC_WINDOW_DAYS    = 10    # All 3 spikes must fall within this window to "converge"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _apply_split_adjustments(
    df: pd.DataFrame,
    ticker: str,
    known_splits: Dict[str, List[Tuple[str, float]]],
) -> pd.DataFrame:
    """Apply known split/consolidation adjustments to an OHLCV DataFrame.

    Uses the dynamically loaded known_splits dict (data/known_splits.json) so
    new auto-detected splits are handled without any code change.

    For each event we check whether Yahoo Finance has already reflected it via
    log-space distance at the split boundary. Only missing adjustments are
    applied, preventing double-counting.

    For a reverse split (N:1, ratio=N): pre-split prices ×N, volume ÷N.
    For a forward split (1:M, ratio=1/M): pre-split prices ×(1/M), volume ÷(1/M).
    """
    splits = known_splits.get(ticker)
    if not splits:
        return df

    df = df.copy()
    price_cols = [c for c in ("open", "high", "low", "close") if c in df.columns]

    for date_str, ratio in splits:
        split_date = pd.Timestamp(date_str)
        pre_mask = df.index < split_date
        if not pre_mask.any():
            continue  # No data before this split

        pre_close  = df.loc[pre_mask, "close"].dropna()
        post_close = df.loc[df.index >= split_date, "close"].dropna()
        if pre_close.empty or post_close.empty:
            continue

        price_before = pre_close.iloc[-1]
        price_after  = post_close.iloc[0]
        if price_before <= 0 or price_after <= 0:
            continue

        observed_ratio = price_after / price_before

        # In log-space: is the observed jump closer to 1.0 (already adjusted)
        # or to the expected ratio (unadjusted)?
        try:
            log_observed = abs(math.log(observed_ratio))
        except ValueError:
            continue
        log_expected = abs(math.log(ratio))
        already_applied = abs(log_observed) < abs(log_observed - log_expected)

        if not already_applied:
            logger.info(
                "Applying split adjustment for %s: ×%.4g on %s "
                "(observed jump ×%.4g vs expected ×%.4g)",
                ticker, ratio, date_str, observed_ratio, ratio,
            )
            for col in price_cols:
                df.loc[pre_mask, col] = (df.loc[pre_mask, col] * ratio)
            if "volume" in df.columns:
                df.loc[pre_mask, "volume"] = (df.loc[pre_mask, "volume"] / ratio)
        else:
            logger.debug(
                "Split %s ×%.4g on %s already reflected in Yahoo data — skipping",
                ticker, ratio, date_str,
            )

    return df


def _detect_and_apply_unknown_splits(
    df: pd.DataFrame, ticker: str
) -> Tuple[pd.DataFrame, List[dict]]:
    """Scan for large price discontinuities not covered by MANUAL_SPLITS and auto-apply them.

    Any single-day price ratio >= SPLIT_ANOMALY_THRESHOLD (4×) or
    <= 1/SPLIT_ANOMALY_THRESHOLD (0.25×) is treated as an unregistered split.

    Why 4× is safe: a 4× organic daily move for a 3x leveraged gas ETF would
    require NG=F to move ~100% in a single trading session — essentially impossible.

    Auto-applied corrections keep the data clean immediately.  Each event is
    returned in the detections list so callers can persist it to split_warnings.json,
    creating a visible record that prompts adding the event to MANUAL_SPLITS.
    """
    df = df.copy()
    price_cols = [c for c in ("open", "high", "low", "close") if c in df.columns]
    detections: List[dict] = []

    close = df["close"]
    daily_ratio = (close / close.shift(1)).dropna()

    lo = 1.0 / SPLIT_ANOMALY_THRESHOLD
    hi = SPLIT_ANOMALY_THRESHOLD
    anomalies = daily_ratio[(daily_ratio >= hi) | (daily_ratio <= lo)].sort_index()

    for date, ratio in anomalies.items():
        pre_mask = df.index < date
        if not pre_mask.any():
            continue

        logger.warning(
            "AUTO-DETECTED unregistered split for %s on %s: observed ×%.4g — "
            "auto-applying correction. Add to MANUAL_SPLITS to silence this warning.",
            ticker, date.strftime("%Y-%m-%d"), ratio,
        )
        for col in price_cols:
            df.loc[pre_mask, col] = df.loc[pre_mask, col] * ratio
        if "volume" in df.columns:
            df.loc[pre_mask, "volume"] = df.loc[pre_mask, "volume"] / ratio

        detections.append({
            "ticker": ticker,
            "date": date.strftime("%Y-%m-%d"),
            "observed_ratio": round(float(ratio), 6),
            "direction": "reverse_split" if ratio > 1 else "forward_split",
            "auto_applied": True,
            "action_required": (
                f"Add (\"{date.strftime('%Y-%m-%d')}\", {round(float(ratio), 4)}) "
                f"to MANUAL_SPLITS[\"{ticker}\"] in data_pipeline.py and trough_peak_data.py"
            ),
        })

    return df, detections


def _safe_float(val: Any) -> Optional[float]:
    """Convert a value to float, returning None for NaN / Inf / non-numeric."""
    if val is None:
        return None
    try:
        f = float(val)
        if math.isnan(f) or math.isinf(f):
            return None
        return round(f, 6)
    except (TypeError, ValueError):
        return None


def _safe_dict(d: dict) -> dict:
    """Recursively sanitise a dict so it is JSON-serialisable."""
    out = {}
    for k, v in d.items():
        if isinstance(v, dict):
            out[k] = _safe_dict(v)
        elif isinstance(v, list):
            out[k] = [_safe_dict(i) if isinstance(i, dict) else _safe_float(i) if isinstance(i, (float, np.floating)) else i for i in v]
        elif isinstance(v, (float, np.floating)):
            out[k] = _safe_float(v)
        elif isinstance(v, (np.integer,)):
            out[k] = int(v)
        elif isinstance(v, (np.bool_,)):
            out[k] = bool(v)
        elif isinstance(v, pd.Timestamp):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out


# ---------------------------------------------------------------------------
# Seasonality helpers (Feature 3)
# ---------------------------------------------------------------------------
def _seasonal_weight(month: int) -> float:
    """Winter premium (+30%) for Nov-Feb; summer discount (-15%) for Jun-Aug."""
    if month in (11, 12, 1, 2):
        return 1.30
    if month in (6, 7, 8):
        return 0.85
    return 1.00


def _season_label(month: int) -> str:
    if month in (11, 12, 1, 2):
        return "winter"
    if month in (3, 4, 5):
        return "spring"
    if month in (6, 7, 8):
        return "summer"
    return "fall"


# ---------------------------------------------------------------------------
# Decay-adjusted price percentile (Feature 6)
# ---------------------------------------------------------------------------
def _decay_adjusted_price_percentile(
    close: "pd.Series", annual_decay: float, window: int = 21
) -> "pd.Series":
    """
    For each date, project historical prices forward to today's equivalent value
    using the leveraged-ETF decay model, then rank today's price against
    that adjusted distribution (0-100).

    adj_price[i] = price[i] × exp(-(annual_decay / 252) × age_days)

    This removes the structural downward drift so that 'low price' actually
    signals that the underlying gas price moved against the ETF, not just that time has passed.
    """
    daily_decay = annual_decay / 252.0
    result = pd.Series(np.nan, index=close.index, dtype=float)

    close_arr = close.values
    n = len(close_arr)

    for i in range(window, n):
        window_slice = close_arr[i - window: i + 1]  # length = window + 1
        ages = np.arange(len(window_slice) - 1, -1, -1, dtype=float)  # 0=today
        adj_prices = window_slice * np.exp(-daily_decay * ages)
        current = adj_prices[-1]
        pct = float((adj_prices[:-1] < current).sum() / len(adj_prices[:-1]) * 100)
        result.iloc[i] = pct

    return result.bfill().fillna(50.0)


# ---------------------------------------------------------------------------
# NG=F price context (Feature 2) — seasonally-adjusted z-score
# ---------------------------------------------------------------------------
# Why seasonal z-score instead of raw 2-year percentile?
# Natural gas is one of the most volatile commodities on earth — it regularly
# traverses its entire 2-year price range in a single winter or summer cycle.
# A raw percentile rank just tells you "cheap vs recent history" which is almost
# always 'mid' and thus meaningless. The correct question is:
#   "Is gas unusually HIGH or LOW for THIS time of year?"
# Seasonal z-score answers that by comparing today's price to the distribution
# of NG=F closes for the SAME CALENDAR MONTH across all available history.
# Threshold: |z| > 1.5σ activates the gate (gas is anomalously far from norm).
# ---------------------------------------------------------------------------
NG_SEASONAL_Z_GATE = 1.5      # σ threshold — gates fire above/below this

def _compute_ng_seasonal_z_series(ng_close: pd.Series) -> pd.Series:
    """
    Compute per-date seasonal z-score for NG=F in log-space using a rolling
    lookback of the prior same-month observations (no lookahead bias).
    Returns a Series aligned to ng_close.index.
    """
    result = pd.Series(np.nan, index=ng_close.index, dtype=float)
    for month in range(1, 13):
        mask = ng_close.index.month == month
        idx = ng_close.index[mask]
        prices = ng_close.loc[idx]
        
        # Log prices to normalize right-skewness
        log_prices = np.log(prices)
        
        # 10 years lookback corresponds to ~210 same-month trading day observations
        means = log_prices.rolling(window=210, min_periods=12).mean().shift(1)
        stds  = log_prices.rolling(window=210, min_periods=12).std().shift(1)
        
        z = ((log_prices - means) / stds).where(stds > 0)
        result.loc[idx] = z.values
    return result


def _compute_ng_regime_series(ng_close: pd.Series, ng_z_series: pd.Series) -> pd.Series:
    """
    Classify each NG=F date into a volatility regime (no lookahead bias).

      extreme : price > $7  OR  |z| >= 2.5σ  OR  21d HV at 90th pct of own 2yr rolling history
      elevated: price > $4.5 OR |z| >= 1.5σ  OR  21d HV at 70th pct
      normal  : everything else

    Anchored to known outlier regimes:
      2022 ultra-bull run (gas ~$9, z ~+3 to +4)  → extreme
      Jan 2026 cold snap (gas >$7, z elevated)     → extreme
    """
    # NG=F realized vol (21d, annualized %)
    log_ret = np.log(ng_close / ng_close.shift(1))
    hv21 = log_ret.rolling(21, min_periods=11).std() * np.sqrt(252) * 100
    # Rolling 2yr HV percentile (prior 504 bars only — no lookahead)
    hv_pct = _percentile_rank(hv21, 504)

    z_abs = ng_z_series.reindex(ng_close.index).abs()

    result = pd.Series('normal', index=ng_close.index, dtype=object)

    # Elevated (set first, then extreme overwrites)
    elevated_mask = (
        (ng_close > NG_REGIME_HIGH_PRICE) |
        (z_abs >= NG_REGIME_ELEVATED_Z) |
        (hv_pct >= NG_REGIME_ELEVATED_HV_PCT)
    ).fillna(False)
    result[elevated_mask] = 'elevated'

    # Extreme (overrides elevated)
    extreme_mask = (
        (ng_close > NG_REGIME_EXTREME_PRICE) |
        (z_abs >= NG_REGIME_EXTREME_Z) |
        (hv_pct >= NG_REGIME_EXTREME_HV_PCT)
    ).fillna(False)
    result[extreme_mask] = 'extreme'

    return result


def _fetch_ng_price_context() -> dict:
    """
    Fetch NG=F (NYMEX natural gas futures) and compute a seasonally-adjusted
    z-score: how many σ above/below the typical price for this calendar month.

    Also computes raw 2yr percentile for reference display, but the GATE logic
    uses seasonal_zscore exclusively (more meaningful for a volatile seasonal commodity).

    Returns dict with price, seasonal_zscore, tier, and gate flags for both sides.
    """
    logger.info("Fetching NG=F for gas price level gate …")
    df, _ = _yahoo_fetch_one(NG_TICKER, {})

    if df is None or df.empty or len(df) < 60:
        logger.warning("Could not fetch NG=F — gas price gate unavailable")
        return {
            "price":           None,
            "seasonal_zscore": None,
            "percentile_2yr":  None,
            "tier":            "unknown",
            "gate_short":      None,
            "gate_long":       None,
            "history_days":    0,
            "seasonal_note":   "insufficient data",
        }

    close = df["close"].dropna()
    current = float(close.iloc[-1])
    current_month = close.index[-1].month

    # Raw 2yr percentile (for display only — shown in the bar)
    window_close = close.iloc[-NG_PRICE_WINDOW_DAYS:]
    pct_2yr = float((window_close < current).sum() / len(window_close) * 100)

    # Seasonal z-score: compare current price in log-space to prior 10 years of same-month prices
    cutoff_year = close.index[-1].year - 10
    same_month_prices = close[(close.index.month == current_month) & (close.index.year >= cutoff_year)].iloc[:-1]
    if len(same_month_prices) < 12:
        # Not enough monthly data — fall back to 2yr percentile gate
        seasonal_z = None
        seasonal_note = f"only {len(same_month_prices)} {_season_label(current_month)}-month observations; using 2yr pct fallback"
        gate_short = pct_2yr >= NG_HIGH_QUARTILE
        gate_long  = pct_2yr <= NG_LOW_QUARTILE
    else:
        log_same_month = np.log(same_month_prices)
        m_mean_log = float(log_same_month.mean())
        m_std_log  = float(log_same_month.std())
        current_log = math.log(current)
        seasonal_z = round((current_log - m_mean_log) / m_std_log, 2) if m_std_log > 0 else 0.0
        g_mean = math.exp(m_mean_log)
        seasonal_note = (
            f"vs {len(same_month_prices)} {_season_label(current_month)}-month obs "
            f"(gMean=${g_mean:.2f}, std_log={m_std_log:.3f})"
        )
        # Gate: fires when gas is anomalously high (short) or low (long) for this month
        gate_short = seasonal_z >= NG_SEASONAL_Z_GATE
        gate_long  = seasonal_z <= -NG_SEASONAL_Z_GATE

    # Tier label driven by seasonal z-score (or 2yr pct fallback)
    z_ref = seasonal_z if seasonal_z is not None else (pct_2yr / 50 - 1) * 3
    if z_ref is not None and z_ref >= 2.5:
        tier = "extreme_high"
    elif z_ref is not None and z_ref >= NG_SEASONAL_Z_GATE:
        tier = "seasonal_high"
    elif z_ref is not None and z_ref <= -2.5:
        tier = "extreme_low"
    elif z_ref is not None and z_ref <= -NG_SEASONAL_Z_GATE:
        tier = "seasonal_low"
    else:
        tier = "seasonal_mid"

    # ---- NG=F own realized volatility (21d annualized) and HV percentile ----
    log_ret_ng = np.log(close / close.shift(1))
    ng_hv21_series = log_ret_ng.rolling(21, min_periods=11).std() * np.sqrt(252) * 100
    ng_hv21_current = _safe_float(ng_hv21_series.iloc[-1])
    recent_hvs = ng_hv21_series.dropna().iloc[-NG_PRICE_WINDOW_DAYS:]
    ng_hv_pct = (
        round(float((recent_hvs < ng_hv21_current).sum() / len(recent_hvs) * 100), 1)
        if ng_hv21_current is not None and len(recent_hvs) > 10
        else None
    )

    # ---- Current volatility regime classification ----
    z_abs_current = abs(seasonal_z) if seasonal_z is not None else 0.0
    if (current > NG_REGIME_EXTREME_PRICE
            or z_abs_current >= NG_REGIME_EXTREME_Z
            or (ng_hv_pct is not None and ng_hv_pct >= NG_REGIME_EXTREME_HV_PCT)):
        regime = "extreme"
    elif (current > NG_REGIME_HIGH_PRICE
            or z_abs_current >= NG_REGIME_ELEVATED_Z
            or (ng_hv_pct is not None and ng_hv_pct >= NG_REGIME_ELEVATED_HV_PCT)):
        regime = "elevated"
    else:
        regime = "normal"

    # ---- Precompute full series for historical event tagging (internal only) ----
    ng_z_full_series = _compute_ng_seasonal_z_series(close)
    ng_regime_full_series = _compute_ng_regime_series(close, ng_z_full_series)

    # Save clean NG=F history for tail risk & flow models
    try:
        ng_records = []
        for dt_idx, r in df.iterrows():
            ng_records.append({
                "date": dt_idx.strftime("%Y-%m-%d"),
                "open": round(float(r["open"]), 4) if "open" in r and not pd.isna(r["open"]) else None,
                "high": round(float(r["high"]), 4) if "high" in r and not pd.isna(r["high"]) else None,
                "low": round(float(r["low"]), 4) if "low" in r and not pd.isna(r["low"]) else None,
                "close": round(float(r["close"]), 4) if "close" in r and not pd.isna(r["close"]) else None,
                "volume": float(r["volume"]) if "volume" in r and not pd.isna(r["volume"]) else 0.0,
            })
        if ng_records:
            DOCS_DATA_DIR.mkdir(parents=True, exist_ok=True)
            with open(DATA_DIR / "ng_history.json", "w", encoding="utf-8") as f:
                json.dump(ng_records, f)
            with open(DOCS_DATA_DIR / "ng_history.json", "w", encoding="utf-8") as f:
                json.dump(ng_records, f)
            logger.info("Exported %d NG=F bars to ng_history.json", len(ng_records))
    except Exception as ex:
        logger.warning("Could not export ng_history.json: %s", ex)

    return {
        "price":           round(current, 3),
        "seasonal_zscore": seasonal_z,
        "percentile_2yr":  round(pct_2yr, 1),
        "tier":            tier,
        "gate_short":      gate_short,   # True = gas anomalously HIGH for season → short credible
        "gate_long":       gate_long,    # True = gas anomalously LOW for season → long credible
        "history_days":    len(window_close),
        "seasonal_note":   seasonal_note,
        "season":          _season_label(current_month),
        "month":           current_month,
        "ng_hv_21d":       ng_hv21_current,
        "ng_hv_pct":       ng_hv_pct,
        "regime":          regime,       # 'normal' | 'elevated' | 'extreme'
        "_close_series":   close,
        "_z_series":       ng_z_full_series,
        "_regime_series":  ng_regime_full_series,
    }


def _market_status() -> str:
    """Return a simple NYSE market-status string based on current ET time."""
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        from backports.zoneinfo import ZoneInfo  # type: ignore[no-redef]

    now_et = datetime.now(ZoneInfo("America/New_York"))
    weekday = now_et.weekday()  # 0=Mon .. 6=Sun
    if weekday >= 5:
        return "closed"
    hour, minute = now_et.hour, now_et.minute
    t = hour * 60 + minute
    if t < 4 * 60:
        return "closed"
    if t < 9 * 60 + 30:
        return "pre_market"
    if t < 16 * 60:
        return "open"
    if t < 20 * 60:
        return "after_hours"
    return "closed"


# ---------------------------------------------------------------------------
# Data fetching – Yahoo Finance v8 chart API (no yfinance dependency)
# ---------------------------------------------------------------------------
def _yahoo_fetch_one(
    ticker: str,
    known_splits: Dict[str, List[Tuple[str, float]]],
) -> Tuple[Optional[pd.DataFrame], Optional[dict]]:
    """Fetch full daily OHLCV history for a single ticker via Yahoo Finance v8 chart API.

    Uses period1/period2 parameters to request the complete daily history
    (period1 set to 2007-01-01 to capture all available data for every ETF).

    Returns (df, live_snapshot) where live_snapshot uses meta.regularMarketPrice
    which is always the current real-time price, independent of the daily bar cadence.
    """
    period1 = int(datetime(2007, 1, 1).timestamp())
    # Add buffer of 1 day to period2 so Yahoo doesn't omit today's active bar due to timezone skew
    period2 = int(datetime.now(timezone.utc).timestamp()) + 86400
    url = (
        f"{YAHOO_BASE_URL}{urllib.request.quote(ticker)}"
        f"?period1={period1}&period2={period2}&interval=1d&includePrePost=false"
    )

    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            resp = urllib.request.urlopen(req, context=ctx, timeout=30)
            raw = json.loads(resp.read())

            result = raw["chart"]["result"][0]
            meta = result.get("meta", {})
            timestamps = result["timestamp"]
            quote = result["indicators"]["quote"][0]

            dates = [datetime.fromtimestamp(ts, timezone.utc).replace(tzinfo=None) for ts in timestamps]
            df = pd.DataFrame({
                "open": quote.get("open"),
                "high": quote.get("high"),
                "low": quote.get("low"),
                "close": quote.get("close"),
                "volume": quote.get("volume"),
            }, index=pd.DatetimeIndex(dates))

            # Drop rows with missing close or volume
            df = df.dropna(subset=["close", "volume"])
            df["volume"] = df["volume"].astype(float)
            df.sort_index(inplace=True)

            # Yahoo sometimes returns two rows for the same calendar date (e.g. the
            # official session bar + an updated intraday snapshot with revised volume).
            # Both have the same date string and close, causing hist[-1] == hist[-2]
            # which makes processPrecomputed() compute 0% daily change.
            # Keep only the last (most up-to-date) row per calendar date.
            df = df.groupby(df.index.date).last()
            df.index = pd.DatetimeIndex(df.index)

            # Apply known split adjustments (from data/known_splits.json)
            df = _apply_split_adjustments(df, ticker, known_splits)

            # Extract real-time snapshot from meta — regularMarketPrice updates
            # continuously and is independent of the daily bar update cadence.
            # Some TSX tickers (HNU.TO / HND.TO) don't return regularMarketPrice or
            # regularMarketVolume in Yahoo's meta; fall back to the last bar in df.
            live_price = meta.get("regularMarketPrice")
            live_vol   = meta.get("regularMarketVolume")
            if live_price is None and not df.empty:
                live_price = float(df["close"].iloc[-1])
            if live_vol is None and not df.empty:
                live_vol = int(df["volume"].iloc[-1])
            # In Yahoo Finance v8 chart API:
            # - regularMarketChangePercent is the clean live % change from the official quote
            # - previousClose in meta is often None
            # - chartPreviousClose is the price prior to period1 (2007), which is pre-reverse-split
            #   (e.g. $7,708,000 for BOIL) and MUST NEVER be used as the session previous close!
            reg_chg_pct = meta.get("regularMarketChangePercent")

            prev_close = meta.get("previousClose")
            if (prev_close is None or prev_close <= 0 or
                (live_price is not None and (prev_close > live_price * 5 or prev_close < live_price * 0.2))):
                if not df.empty:
                    today_utc = datetime.now(timezone.utc).date()
                    if df.index[-1].date() == today_utc and len(df) >= 2:
                        prev_close = float(df["close"].iloc[-2])
                    else:
                        prev_close = float(df["close"].iloc[-1])
                else:
                    prev_close = None

            live_snapshot: Optional[dict] = None
            if live_price is not None:
                change_pct = None
                if reg_chg_pct is not None and not np.isnan(reg_chg_pct):
                    change_pct = round(float(reg_chg_pct), 4)
                elif prev_close and prev_close > 0:
                    change_pct = round((live_price - prev_close) / prev_close * 100, 4)
                live_snapshot = {
                    "price":      round(float(live_price), 4),
                    "volume":     int(live_vol) if live_vol is not None else None,
                    "prev_close": round(float(prev_close), 4) if prev_close else None,
                    "change_pct": change_pct,
                }
                logger.info("  → Live snapshot %s: price=%.4f chg=%.2f%%",
                            ticker, live_price, change_pct or 0.0)

            return df, live_snapshot

        except Exception as e:
            logger.warning("Attempt %d/%d for %s failed: %s", attempt, MAX_RETRIES, ticker, e)
            if attempt < MAX_RETRIES:
                import time
                time.sleep(RETRY_DELAY_SECS * attempt)

    return None, None


def _fetch_all() -> Tuple[Dict[str, pd.DataFrame], Dict[str, dict], List[dict]]:
    """Download full daily OHLCV history for all 6 ETFs via Yahoo Finance v8 API.

    Two-pass split handling:
      Pass 1 (_apply_split_adjustments): applies all entries from
              data/known_splits.json with smart already-applied detection.
      Pass 2 (_detect_and_apply_unknown_splits): scans for any remaining
              large jumps not yet in the file, applies them, and records
              the events so run_pipeline() can persist them back to
              known_splits.json and commit them alongside the data.

    Returns (frames, live_snapshots, newly_detected_splits).
    live_snapshots maps ticker → {price, volume, prev_close, change_pct} from
    meta.regularMarketPrice, which is real-time and always current.
    """
    known_splits = _load_known_splits()
    frames: Dict[str, pd.DataFrame] = {}
    live_snapshots: Dict[str, dict] = {}
    all_detected: List[dict] = []

    for ticker in ETF_CONFIG:
        logger.info("Fetching %s from Yahoo Finance …", ticker)
        df, snapshot = _yahoo_fetch_one(ticker, known_splits)

        if df is not None and not df.empty:
            # Pass 2: auto-detect splits not yet in known_splits.json
            df, detected = _detect_and_apply_unknown_splits(df, ticker)
            if detected:
                # Merge into the in-memory dict so it's available for saving
                for event in detected:
                    bucket = known_splits.setdefault(event["ticker"], [])
                    existing_dates = {d for d, _ in bucket}
                    if event["date"] not in existing_dates:
                        bucket.append((event["date"], event["observed_ratio"]))
                        bucket.sort()
            all_detected.extend(detected)

            frames[ticker] = df
            first = df.index[0].strftime("%Y-%m-%d")
            last  = df.index[-1].strftime("%Y-%m-%d")
            logger.info("  → %d daily rows for %s (%s to %s)", len(df), ticker, first, last)
        else:
            logger.error("No data for %s after %d retries", ticker, MAX_RETRIES)

        if snapshot:
            live_snapshots[ticker] = snapshot

    # Persist updated splits dict so the next run treats new events as "known"
    if all_detected:
        _save_known_splits(known_splits)

    return frames, live_snapshots, all_detected


# ---------------------------------------------------------------------------
# Metric computation
# ---------------------------------------------------------------------------
def _percentile_rank(series: pd.Series, window: int) -> pd.Series:
    """Rolling percentile rank of the last value within the window (0-100)."""
    def _rank_last(arr):
        if len(arr) < 2:
            return np.nan
        val = arr[-1]
        return (np.sum(arr < val) / (len(arr) - 1)) * 100.0

    return series.rolling(window, min_periods=max(2, window // 2)).apply(_rank_last, raw=True)



def _sigmoid_scalar(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-max(-30.0, min(30.0, x))))


def compute_upgraded_vdds_series(df: pd.DataFrame, ewma_span: int = 21, atr_span: int = 14) -> pd.Series:
    """
    Split-invariant, volume-preserving VDDS* kernel matching docs/js/metrics.js.
    VDDS* in [0.50, 1.50]x, centered at 1.00x Balanced.
    Eliminates algebraic volume cancellation fallacy (VDDS = C_t / VWAP_21).
    """
    n = len(df)
    if n == 0:
        return pd.Series([], dtype=float)

    alpha21 = 2.0 / (ewma_span + 1.0)
    alpha_atr = 2.0 / (atr_span + 1.0)
    kappa_d = 0.25
    gamma_d = 0.65
    kappa_e = 0.35
    beta_e = 0.35
    lambda_v = 0.90
    price_eps = 1e-4
    eps = 1e-9

    canonical_splits = [4, 5, 8, 10, 12, 15, 20, 25, 0.5, 0.25, 0.2, 0.1]

    ewma_dollar_vol = 0.0
    ewma_share_vol = 0.0
    ewma_pct_atr = 0.03
    ewma_variance = 0.0009
    ewma_efficiency = 0.0
    synthetic_price_idx = 100.0
    dwma_synthetic_price_idx = 100.0

    vdds_out = np.zeros(n, dtype=float)

    closes = df["close"].values
    highs = df["high"].values if "high" in df.columns else closes
    lows = df["low"].values if "low" in df.columns else closes
    opens = df["open"].values if "open" in df.columns else closes
    vols = df["volume"].values

    for i in range(n):
        close = max(float(closes[i]), price_eps)
        high = max(float(highs[i]), close)
        low = min(max(float(lows[i]), price_eps), close)
        vol = max(float(vols[i]), 0.0)

        typ_price = (high + low + 2.0 * close) * 0.25
        dollar_turnover = typ_price * vol

        prev_raw_close = max(float(closes[i - 1]), price_eps) if i > 0 else close
        eff_prev_close = prev_raw_close
        is_split = False
        split_factor = 1.0

        if i > 0:
            raw_ratio = close / prev_raw_close
            if raw_ratio >= 2.8 or raw_ratio <= 1.0 / 2.8:
                open_px = max(float(opens[i]), price_eps)
                open_ratio = open_px / prev_raw_close
                best_factor = open_ratio
                min_err = 999.0
                for cand in canonical_splits:
                    err = abs(open_ratio - cand) / cand
                    if err < min_err and err < 0.25:
                        min_err = err
                        best_factor = cand
                eff_prev_close = prev_raw_close * best_factor
                is_split = True
                split_factor = best_factor

        eff_prev_close = max(eff_prev_close, price_eps)
        log_ret = math.log(max(0.05, min(20.0, close / eff_prev_close))) if i > 0 else 0.0
        intrabar_range = max(0.0, high - low)
        true_range = max(intrabar_range, abs(high - eff_prev_close), abs(low - eff_prev_close), price_eps * close) if i > 0 else max(intrabar_range, 0.015 * close, price_eps * close)
        pct_true_range = max(price_eps, min(2.0, true_range / eff_prev_close))

        if i == 0:
            ewma_dollar_vol = max(dollar_turnover, eps)
            ewma_share_vol = max(vol, eps)
            ewma_pct_atr = max(pct_true_range, 0.005)
            ewma_variance = ewma_pct_atr * ewma_pct_atr
            ewma_efficiency = dollar_turnover / ewma_pct_atr
            synthetic_price_idx = 100.0
            dwma_synthetic_price_idx = 100.0
        else:
            synthetic_price_idx *= math.exp(log_ret)

        baseline_dollar_vol = max(ewma_dollar_vol, eps)
        baseline_share_vol = max(ewma_share_vol / split_factor, eps) if (is_split and split_factor > 0) else max(ewma_share_vol, eps)
        causal_pct_atr = max(ewma_pct_atr, price_eps)
        causal_daily_sigma = max(math.sqrt(ewma_variance), price_eps)
        baseline_efficiency = max(ewma_efficiency, eps)

        drvol = dollar_turnover / baseline_dollar_vol if dollar_turnover > 0 else 0.0
        reg_drvol = (dollar_turnover + kappa_d * baseline_dollar_vol) / ((1.0 + kappa_d) * baseline_dollar_vol)
        participation_gate = math.pow(max(reg_drvol, 0.0), gamma_d) * _sigmoid_scalar((drvol - 0.75) / 0.25)

        clv_denom = max(intrabar_range, 0.25 * causal_pct_atr * close, price_eps * close)
        clv = max(-1.0, min(1.0, (2.0 * close - high - low) / clv_denom))
        ret_impulse = math.tanh(log_ret / causal_pct_atr)
        order_flow_phi = math.tanh(0.55 * clv + 0.75 * ret_impulse)

        eff_vol_floor = max(pct_true_range, 0.20 * causal_daily_sigma, price_eps)
        curr_eff = dollar_turnover / eff_vol_floor
        reg_eff = (curr_eff + kappa_e * baseline_efficiency) / ((1.0 + kappa_e) * baseline_efficiency)
        vol_eff = max(0.50, min(2.00, math.pow(max(reg_eff, 0.0), beta_e)))

        anchor_displacement = math.tanh(
            math.log(max(synthetic_price_idx, eps) / max(dwma_synthetic_price_idx, eps)) /
            max(2.5 * causal_daily_sigma, price_eps)
        )

        dir_composite = 0.70 * order_flow_phi + 0.30 * anchor_displacement
        flow_kernel = participation_gate * vol_eff * dir_composite
        vdds_star = max(0.50, min(1.50, 1.0 + 0.50 * math.tanh(lambda_v * flow_kernel)))
        vdds_out[i] = vdds_star

        # Causal EWMA updates
        capped_dt = min(dollar_turnover, 5.0 * baseline_dollar_vol)
        ewma_dollar_vol = alpha21 * capped_dt + (1.0 - alpha21) * baseline_dollar_vol
        capped_sv = min(vol, 5.0 * baseline_share_vol)
        ewma_share_vol = alpha21 * capped_sv + (1.0 - alpha21) * baseline_share_vol
        ewma_pct_atr = alpha_atr * pct_true_range + (1.0 - alpha_atr) * causal_pct_atr
        ewma_variance = 0.05 * (log_ret * log_ret) + 0.95 * ewma_variance
        ewma_efficiency = alpha21 * min(curr_eff, 5.0 * baseline_efficiency) + (1.0 - alpha21) * baseline_efficiency

        dwma_weight = min(drvol, 4.0)
        dwma_alpha = alpha21 * dwma_weight / (1.0 + alpha21 * dwma_weight)
        dwma_synthetic_price_idx = dwma_alpha * synthetic_price_idx + (1.0 - dwma_alpha) * dwma_synthetic_price_idx

    return pd.Series(vdds_out, index=df.index)


def compute_etf_metrics(df: pd.DataFrame, side: str = "long", ticker: str = "",
                        ng_close: "Optional[pd.Series]" = None,
                        ng_seasonal_z_series: "Optional[pd.Series]" = None,
                        ng_regime_series: "Optional[pd.Series]" = None) -> dict:
    """
    Given a DataFrame with columns [open, high, low, close, volume],
    compute the full metric suite and return a dict.
    """
    if df.empty:
        return {}

    close = df["close"]
    volume = df["volume"].astype(float)
    dollar_volume = close * volume
    pct_change = close.pct_change() * 100

    # --- RVOL ---
    rvol = {}
    for w in RVOL_WINDOWS:
        avg = volume.rolling(w, min_periods=max(1, w // 2)).mean()
        rvol[f"{w}d"] = _safe_float((volume.iloc[-1] / avg.iloc[-1]) if avg.iloc[-1] != 0 else np.nan)

    # --- Volume Z-Score ---
    z_score = {}
    for w in ZSCORE_WINDOWS:
        mean_ = volume.rolling(w, min_periods=max(1, w // 2)).mean()
        std_ = volume.rolling(w, min_periods=max(1, w // 2)).std()
        val = (volume.iloc[-1] - mean_.iloc[-1]) / std_.iloc[-1] if std_.iloc[-1] != 0 else np.nan
        z_score[f"{w}d"] = _safe_float(val)

    # --- VROC ---
    vroc = {}
    for w in VROC_WINDOWS:
        if len(volume) > w:
            old = volume.iloc[-w - 1]
            cur = volume.iloc[-1]
            vroc[f"{w}d"] = _safe_float(((cur - old) / old) * 100 if old != 0 else np.nan)
        else:
            vroc[f"{w}d"] = None

    # --- Volume Percentile ---
    vol_pct_series = {}
    vol_pct = {}
    for w in VOL_PCT_WINDOWS:
        s = _percentile_rank(volume, w)
        vol_pct_series[w] = s
        vol_pct[f"{w}d"] = _safe_float(s.iloc[-1])

    # --- Price Percentile ---
    price_pct_series = {}
    price_pct = {}
    for w in PRICE_PCT_WINDOWS:
        s = _percentile_rank(close, w)
        price_pct_series[w] = s
        price_pct[f"{w}d"] = _safe_float(s.iloc[-1])

    # --- CVI (Capitulation Volume Index) ---
    # CVI = vol_percentile * (1 - price_percentile / 100), scaled 0-100
    # Computed for ALL ETFs (long AND short)
    cvi = {}
    for w in VOL_PCT_WINDOWS:
        vp = vol_pct.get(f"{w}d")
        pp = price_pct.get(f"{w}d")
        if vp is not None and pp is not None:
            cvi[f"{w}d"] = _safe_float(vp * (1.0 - pp / 100.0))
        else:
            cvi[f"{w}d"] = None

    # --- Dollar Volume Metrics (DV) ---
    # All metrics parallel their share-volume counterparts — same windows, same formulas.
    # dollar_volume = close × volume (split-agnostic; no adjustments needed — already applied).
    dv_rvol: Dict[str, Optional[float]] = {}
    dv_zscore: Dict[str, Optional[float]] = {}
    dv_percentile: Dict[str, Optional[float]] = {}
    dv_vroc: Dict[str, Optional[float]] = {}
    dv_pct_series: Dict[int, pd.Series] = {}

    for w in RVOL_WINDOWS:
        avg_dv = dollar_volume.rolling(w, min_periods=max(1, w // 2)).mean()
        dv_rvol[f"{w}d"] = _safe_float(
            (dollar_volume.iloc[-1] / avg_dv.iloc[-1]) if avg_dv.iloc[-1] != 0 else np.nan
        )
        std_dv = dollar_volume.rolling(w, min_periods=max(1, w // 2)).std()
        dz = (
            (dollar_volume.iloc[-1] - avg_dv.iloc[-1]) / std_dv.iloc[-1]
            if std_dv.iloc[-1] != 0 else np.nan
        )
        dv_zscore[f"{w}d"] = _safe_float(dz)
        s_dv = _percentile_rank(dollar_volume, w)
        dv_pct_series[w] = s_dv
        dv_percentile[f"{w}d"] = _safe_float(s_dv.iloc[-1])

    for w in VROC_WINDOWS:
        if len(dollar_volume) > w:
            old_dv = dollar_volume.iloc[-w - 1]
            cur_dv = dollar_volume.iloc[-1]
            dv_vroc[f"{w}d"] = _safe_float(
                ((cur_dv - old_dv) / old_dv) * 100 if old_dv != 0 else np.nan
            )
        else:
            dv_vroc[f"{w}d"] = None

    # DVCVI = dv_percentile × (1 − price_percentile / 100)
    # Spikes when dollar volume is high AND price is low — sharper capitulation signal
    # because dollar volume is naturally suppressed when price is low (double confirmation).
    dvcvi: Dict[str, Optional[float]] = {}
    for w in VOL_PCT_WINDOWS:
        dp = dv_percentile.get(f"{w}d")
        pp = price_pct.get(f"{w}d")
        if dp is not None and pp is not None:
            dvcvi[f"{w}d"] = _safe_float(dp * (1.0 - pp / 100.0))
        else:
            dvcvi[f"{w}d"] = None

    # Upgraded split-invariant, volume-preserving VDDS* series (matches JS docs/js/metrics.js)
    # Eliminates algebraic volume cancellation fallacy (VDDS = C_t / VWAP_21).
    vdds_21_series = compute_upgraded_vdds_series(df)

    # =========================================================================
    # VOLATILITY MODELLING BLOCK
    # =========================================================================

    # --- Realized Historical Volatility (annualized %) ---
    # HV = rolling std of log returns × √252 × 100
    log_ret = np.log(close / close.shift(1))
    hv_series: Dict[int, pd.Series] = {}
    hv: Dict[str, Optional[float]] = {}
    for w in HV_WINDOWS:
        s = log_ret.rolling(w, min_periods=max(2, w // 2)).std() * np.sqrt(252) * 100
        hv_series[w] = s
        hv[f"{w}d"] = _safe_float(s.iloc[-1])

    # --- ATR-14 (Average True Range as % of current price) ---
    high_s = df["high"]
    low_s = df["low"]
    true_range = pd.concat([
        high_s - low_s,
        (high_s - close.shift(1)).abs(),
        (low_s - close.shift(1)).abs(),
    ], axis=1).max(axis=1)
    atr14_series = true_range.rolling(ATR_WINDOW, min_periods=max(2, ATR_WINDOW // 2)).mean()
    atr14_raw = atr14_series.iloc[-1]
    atr14_pct = _safe_float(
        (atr14_raw / close.iloc[-1] * 100) if (close.iloc[-1] > 0 and not np.isnan(atr14_raw)) else np.nan
    )

    # --- Vol Regime Percentile ---
    # Where does the current 21d HV sit relative to its own 252-day history?
    # 0th = historically quiet, 100th = historically extreme
    # Keep full series for historical echoes computation.
    vol_regime_full_series = _percentile_rank(hv_series[21], VOL_REGIME_WINDOW)
    vol_regime_pct = _safe_float(vol_regime_full_series.iloc[-1])

    # --- HV Term Structure (HV10 / HV63) ---
    # < 0.7 = calming / storm passed   |   > 1.3 = accelerating / building storm
    hv10_val = hv.get("10d")
    hv63_val = hv.get("63d")
    if hv10_val is not None and hv63_val is not None and hv63_val > 0:
        hv_term_structure = _safe_float(hv10_val / hv63_val)
    else:
        hv_term_structure = None

    # --- Vol-of-Vol (VoV-21) ---
    # 21-period std of the 10d HV series (already in % annualized).
    # Units: percentage points — how much the annualized short-term vol swings day-to-day.
    # High VoV means the volatility itself is rapidly changing — unstable regime.
    vov21 = _safe_float(
        hv_series[10].rolling(VOV_WINDOW, min_periods=max(2, VOV_WINDOW // 2)).std().iloc[-1]
    )

    # --- VCVI (Vol-Adjusted Capitulation Volume Index) ---
    # VCVI = CVI × (1.5 − vol_regime_pct / 100)
    #   vol_regime=0th  → ×1.5  (quiet vol env → volume spike is MORE remarkable)
    #   vol_regime=50th → ×1.0  (neutral)
    #   vol_regime=100th → ×0.5 (turbulent env → volume spikes expected, discount signal)
    vcvi: Dict[str, Optional[float]] = {}
    for w in VOL_PCT_WINDOWS:
        cvi_val = cvi.get(f"{w}d")
        if cvi_val is not None and vol_regime_pct is not None:
            multiplier = 1.5 - vol_regime_pct / 100.0
            vcvi[f"{w}d"] = _safe_float(max(0.0, cvi_val * multiplier))
        else:
            vcvi[f"{w}d"] = cvi_val  # Fallback to raw CVI

    # Apply vol-regime multiplier to DVCVI so it matches VCVI scale
    if vol_regime_pct is not None:
        dv_mult = 1.5 - vol_regime_pct / 100.0
        for w in VOL_PCT_WINDOWS:
            dvcvi_val = dvcvi.get(f"{w}d")
            if dvcvi_val is not None:
                dvcvi[f"{w}d"] = _safe_float(max(0.0, dvcvi_val * dv_mult))

    # --- Historical Echoes ---
    # Reconstruct rolling VCVI-21 series from already-computed intermediates
    # cvi_21_rolling = vol_pct_21 * (1 - price_pct_21 / 100) vectorised over full history
    cvi_21_rolling = vol_pct_series[21] * (1.0 - price_pct_series[21] / 100.0)
    vcvi_21_rolling = (cvi_21_rolling * (1.5 - vol_regime_full_series / 100.0)).clip(lower=0)

    historical_echoes = _compute_historical_echoes(
        close=close,
        vcvi_21_series=vcvi_21_rolling,
        vol_regime_series=vol_regime_full_series,
        vcvi_threshold=VCVI_WARNING,     # 55 — same as alert threshold
        vol_regime_max=60.0,             # only count signals in non-turbulent regimes
        fwd_windows=[5, 10, 21, 42, 63, 126, 252],
        ng_regime_series=ng_regime_series,
    )

    # --- Conviction Events (strict multi-gate anomaly filter) ---
    conviction_events = _detect_conviction_events(
        close=close,
        pct_change=pct_change,
        vcvi_21_series=vcvi_21_rolling,
        vol_regime_series=vol_regime_full_series,
        vol_pct_series_dict=vol_pct_series,
        atr14_series=atr14_series,
        windows=VOL_PCT_WINDOWS,
        ng_close=ng_close,
        etf_side=side,
        ng_seasonal_z_series=ng_seasonal_z_series,
        ng_regime_series=ng_regime_series,
        vdds_series=vdds_21_series,  # Gate 6: VDDS advisory annotation
    )

    # --- Elevated Watch Events (3-gate softer filter, Feature 5) ---
    conviction_dates_list = [
        pd.Timestamp(e["date"]) for e in (conviction_events.get("events") or [])
    ]
    elevated_watch = _detect_elevated_watch_events(
        close=close,
        pct_change=pct_change,
        vcvi_21_series=vcvi_21_rolling,
        vol_pct_series_dict=vol_pct_series,
        atr14_series=atr14_series,
        windows=VOL_PCT_WINDOWS,
        conviction_dates=conviction_dates_list,
    )

    # --- Decay-Adjusted Price Percentile (Feature 6) ---
    annual_decay = ETF_ANNUAL_DECAY.get(ticker, 0.0)
    decay_adj_pct_current = None
    decay_adj_vcvi_21 = None
    if annual_decay > 0 and len(close) >= DECAY_CORRECTION_WINDOW:
        try:
            decay_adj_pct_series = _decay_adjusted_price_percentile(
                close, annual_decay, 21
            )
            decay_adj_pct_current = _safe_float(decay_adj_pct_series.iloc[-1])
            # Recompute CVI + VCVI using decay-adjusted price percentile
            vol_pct_21_val = vol_pct.get("21d")
            if vol_pct_21_val is not None and decay_adj_pct_current is not None and vol_regime_pct is not None:
                decay_adj_cvi = vol_pct_21_val * (1.0 - decay_adj_pct_current / 100.0)
                multiplier = 1.5 - vol_regime_pct / 100.0
                decay_adj_vcvi_21 = _safe_float(max(0.0, decay_adj_cvi * multiplier))
        except Exception as e:
            logger.warning("Decay correction failed for %s: %s", ticker, e)

    decay_block = {
        "annual_rate":       annual_decay,
        "adj_price_pct":     decay_adj_pct_current,
        "adj_vcvi_21d":      decay_adj_vcvi_21,
        "correction_active": annual_decay > 0,
    }

    # --- Seasonality context (Feature 3) ---
    current_month = close.index[-1].month
    vcvi_21_current = vcvi.get("21d")
    seasonality_block = {
        "month":              current_month,
        "season":             _season_label(current_month),
        "weight":             _seasonal_weight(current_month),
        "adj_vcvi_21d":       _safe_float(
            vcvi_21_current * _seasonal_weight(current_month)
        ) if vcvi_21_current is not None else None,
    }

    # --- Sharp Spike / Fast Signal detection (Feature 1) ---
    vcvi_5d = vcvi.get("5d")
    today_move_pct_signed = _safe_float(pct_change.iloc[-1]) or 0.0
    sharp_spike = bool(
        vcvi_5d is not None
        and atr14_pct is not None
        and atr14_pct > 0
        and abs(today_move_pct_signed) > SHARP_SPIKE_ATR_MULT * atr14_pct
        and vcvi_5d > FAST_VCVI_THRESHOLD
    )
    if sharp_spike:
        fast_signal = (
            "weather_top_candidate" if side == "short" else "weather_bottom_candidate"
        )
    else:
        fast_signal = None

    # =========================================================================
    # END VOLATILITY BLOCK
    # =========================================================================

    # --- VPS (Volume Pressure Score) — now 5-component with vol regime ---
    # Normalise components to 0-100 then weighted sum
    def _norm_0_100(val, lo, hi):
        if val is None or lo is None or hi is None or hi == lo:
            return 50.0
        return max(0, min(100, (val - lo) / (hi - lo) * 100))

    # Use 21-day window as representative
    rvol_val = rvol.get("21d")
    zscore_val = z_score.get("21d")
    pct_val = vol_pct.get("21d")
    vroc_val = vroc.get("10d")

    rvol_norm = _norm_0_100(rvol_val, 0.3, 3.0)
    zscore_norm = _norm_0_100(zscore_val, -2.0, 4.0)
    pct_norm = pct_val if pct_val is not None else 50.0
    vroc_norm = _norm_0_100(vroc_val, -80, 200)
    # InvVolRegime: 100 when vol is quiet → signals more significant; 0 when turbulent
    inv_vol_regime_norm = (100.0 - vol_regime_pct) if vol_regime_pct is not None else 50.0

    vps = _safe_float(
        VPS_W_RVOL * rvol_norm
        + VPS_W_ZSCORE * zscore_norm
        + VPS_W_PCT * pct_norm
        + VPS_W_VROC * vroc_norm
        + VPS_W_VOLREGIME * inv_vol_regime_norm
    )

    # --- DV-VPS (Dollar Volume Pressure Score) — parallel to VPS, uses DV metrics ---
    dv_rvol_21_val = dv_rvol.get("21d")
    dv_zscore_21_val = dv_zscore.get("21d")
    dv_pct_21_val = dv_percentile.get("21d")
    dv_vroc_10_val = dv_vroc.get("10d")
    dv_rvol_norm  = _norm_0_100(dv_rvol_21_val, 0.3, 3.0)
    dv_zscore_norm = _norm_0_100(dv_zscore_21_val, -2.0, 4.0)
    dv_pct_norm   = dv_pct_21_val if dv_pct_21_val is not None else 50.0
    dv_vroc_norm  = _norm_0_100(dv_vroc_10_val, -80, 200)
    dv_vps = _safe_float(
        VPS_W_RVOL * dv_rvol_norm
        + VPS_W_ZSCORE * dv_zscore_norm
        + VPS_W_PCT * dv_pct_norm
        + VPS_W_VROC * dv_vroc_norm
        + VPS_W_VOLREGIME * inv_vol_regime_norm
    )

    # VDDS scalar (current reading of the 21d VDDS series)
    vdds = _safe_float(vdds_21_series.iloc[-1])

    # --- MWCA (Multi-Window Convergence Alarm) ---
    mwca = all(
        vol_pct.get(f"{w}d") is not None and vol_pct[f"{w}d"] >= 90
        for w in VOL_PCT_WINDOWS
    )

    # --- Rolling Spearman Correlation (price-volume, 30d) ---
    rolling_corr = None
    if len(df) >= ROLLING_CORR_WINDOW:
        try:
            corr_series = close.rolling(ROLLING_CORR_WINDOW).corr(volume)
            rolling_corr = _safe_float(corr_series.iloc[-1])
        except Exception:
            pass

    # --- Moving Averages ---
    ma_price = {}
    ma_volume = {}
    for w in MA_WINDOWS:
        if len(close) >= w:
            ma_price[f"{w}d"] = _safe_float(close.rolling(w).mean().iloc[-1])
            ma_volume[f"{w}d"] = _safe_float(volume.rolling(w).mean().iloc[-1])
        else:
            ma_price[f"{w}d"] = None
            ma_volume[f"{w}d"] = None

    # --- Alerts ---
    alerts: List[dict] = []
    now_iso = datetime.now(timezone.utc).isoformat()

    # VCVI alerts — message is directional based on which side fired
    # Short-side VCVI spike = ETF price low while volume surges = gas is at a peak
    # Long-side VCVI spike  = ETF price low while volume surges = gas is at a bottom
    _vcvi_setup = "potential GAS TOP — short/inverse setup" if side == "short" \
                  else "potential GAS BOTTOM — long/leveraged setup"
    for w_key, vcvi_val in vcvi.items():
        if vcvi_val is not None and vcvi_val >= VCVI_CRITICAL:
            alerts.append({
                "type": "vcvi_critical",
                "side": side,
                "message": f"VCVI ({w_key}) = {vcvi_val:.1f} — {_vcvi_setup}",
                "timestamp": now_iso,
            })
        elif vcvi_val is not None and vcvi_val >= VCVI_WARNING:
            alerts.append({
                "type": "vcvi_warning",
                "side": side,
                "message": f"VCVI ({w_key}) = {vcvi_val:.1f} — {_vcvi_setup}",
                "timestamp": now_iso,
            })

    # Legacy CVI alerts (retained for compatibility)
    for w_key, cvi_val in cvi.items():
        if cvi_val is not None and cvi_val >= CVI_CRITICAL:
            alerts.append({
                "type": "cvi_critical",
                "side": side,
                "message": f"CVI ({w_key}) = {cvi_val:.1f} — {_vcvi_setup}",
                "timestamp": now_iso,
            })
        elif cvi_val is not None and cvi_val >= CVI_WARNING:
            alerts.append({
                "type": "cvi_warning",
                "side": side,
                "message": f"CVI ({w_key}) = {cvi_val:.1f} — {_vcvi_setup}",
                "timestamp": now_iso,
            })

    if vps is not None and vps >= VPS_CRITICAL:
        alerts.append({
            "type": "vps_critical",
            "message": f"VPS = {vps:.1f} — extreme volume pressure",
            "timestamp": now_iso,
        })
    elif vps is not None and vps >= VPS_ELEVATED:
        alerts.append({
            "type": "vps_elevated",
            "message": f"VPS = {vps:.1f} — elevated volume pressure",
            "timestamp": now_iso,
        })

    if mwca:
        alerts.append({
            "type": "mwca_alarm",
            "message": "Volume percentile > 90th across ALL windows simultaneously",
            "timestamp": now_iso,
        })

    # Fast spike alert (Feature 1) — fires before standard ATR breakout check
    if sharp_spike and vcvi_5d is not None:
        _spike_setup = "weather GAS TOP candidate (short/inverse)" if side == "short" \
                       else "weather GAS BOTTOM candidate (long/leveraged)"
        alerts.append({
            "type": "fast_spike_critical",
            "side": side,
            "message": (
                f"SHARP SPIKE — 5d VCVI={vcvi_5d:.0f}, move {today_move_pct_signed:+.1f}% "
                f"({abs(today_move_pct_signed)/atr14_pct:.1f}×ATR) → {_spike_setup}"
            ),
            "timestamp": now_iso,
        })

    # ATR breakout alert: today's move > N × ATR-14 AND elevated RVOL
    today_move_pct = abs(float(pct_change.iloc[-1])) if not np.isnan(pct_change.iloc[-1]) else 0.0
    if (atr14_pct is not None and atr14_pct > 0
            and today_move_pct > ATR_BREAKOUT_MULT * atr14_pct
            and rvol_val is not None and rvol_val > ATR_BREAKOUT_RVOL):
        ratio = today_move_pct / atr14_pct
        alerts.append({
            "type": "atr_breakout",
            "message": (
                f"ATR breakout: {today_move_pct:.1f}% move = {ratio:.1f}× ATR-14 "
                f"({atr14_pct:.1f}%), RVOL {rvol_val:.1f}x"
            ),
            "timestamp": now_iso,
        })

    # VoV spike: regime instability warning
    if vov21 is not None and vov21 >= VOV_CRITICAL:
        alerts.append({
            "type": "vov_extreme",
            "message": f"VoV-21 = {vov21:.0f}% — extreme volatility regime instability",
            "timestamp": now_iso,
        })
    elif vov21 is not None and vov21 >= VOV_WARNING:
        alerts.append({
            "type": "vov_elevated",
            "message": f"VoV-21 = {vov21:.0f}% — volatility regime becoming unstable",
            "timestamp": now_iso,
        })

    # High vol regime warning: volume signals should be discounted
    if vol_regime_pct is not None and vol_regime_pct >= VOL_REGIME_HIGH:
        alerts.append({
            "type": "high_vol_regime",
            "message": (
                f"Vol regime {vol_regime_pct:.0f}th pct — high-volatility environment; "
                "volume spikes are less anomalous"
            ),
            "timestamp": now_iso,
        })

    # --- History (full lifetime OHLCV for HV sparkline scrolling) ---
    hist_df = df.copy()
    history: List[dict] = []
    for dt, row in hist_df.iterrows():
        history.append({
            "date": dt.strftime("%Y-%m-%d"),
            "open": _safe_float(row["open"]),
            "high": _safe_float(row["high"]),
            "low": _safe_float(row["low"]),
            "close": _safe_float(row["close"]),
            "volume": int(row["volume"]) if not np.isnan(row["volume"]) else 0,
        })

    # --- Current snapshot ---
    current = {
        "price": _safe_float(close.iloc[-1]),
        "volume": int(volume.iloc[-1]) if not np.isnan(volume.iloc[-1]) else 0,
        "change_pct": _safe_float(pct_change.iloc[-1]),
        "dollar_volume": _safe_float(dollar_volume.iloc[-1]),
    }

    # --- Full RVOL series (needed for IPSI computation later) ---
    # We store the 21d RVOL series tail so the caller can use it
    rvol_21_series = volume / volume.rolling(21, min_periods=1).mean()

    return {
        "current": current,
        "rvol": rvol,
        "z_score": z_score,
        "vroc": vroc,
        "vol_percentile": vol_pct,
        "price_percentile": price_pct,
        "cvi": cvi,
        "vcvi": vcvi,
        "volatility": {
            "hv": hv,
            "vol_regime_pct": vol_regime_pct,
            "hv_term_structure": hv_term_structure,
            "atr14_pct": atr14_pct,
            "vov21": vov21,
        },
        "historical_echoes": historical_echoes,
        "conviction_events": conviction_events,
        "elevated_watch": elevated_watch,       # Feature 5
        "decay": decay_block,                   # Feature 6
        "seasonality": seasonality_block,       # Feature 3
        "sharp_spike": sharp_spike,             # Feature 1
        "fast_signal": fast_signal,             # Feature 1
        "vps": vps,
        "mwca": mwca,
        "rolling_correlation": rolling_corr,
        "moving_averages": {"price": ma_price, "volume": ma_volume},
        "history": history,
        "alerts": alerts,
        "_rvol_21_last": _safe_float(rvol_21_series.iloc[-1]),
        # --- Dollar Volume Metrics ---
        "dv_rvol": dv_rvol,
        "dv_zscore": dv_zscore,
        "dv_percentile": dv_percentile,
        "dv_vroc": dv_vroc,
        "dvcvi": dvcvi,
        "dv_vps": dv_vps,
        "vdds": vdds,
    }


# ---------------------------------------------------------------------------
# Historical Echoes — pattern matching + forward-return study
# ---------------------------------------------------------------------------
def _compute_historical_echoes(
    close: pd.Series,
    vcvi_21_series: pd.Series,
    vol_regime_series: pd.Series,
    vcvi_threshold: float = 55.0,
    vol_regime_max: float = 60.0,
    fwd_windows: list = None,
    min_gap_days: int = 10,
    max_display: int = 8,
    ng_regime_series: "Optional[pd.Series]" = None,
) -> dict:
    """
    Scan the full history for dates where:
      - VCVI-21d >= vcvi_threshold  (capitulation signal active)
      - vol_regime_pct <= vol_regime_max  (not in extreme-turbulence regime)

    For each qualifying date, compute forward returns at each window.
    Return aggregated stats + the most recent individual instances.

    min_gap_days ensures we count distinct events, not consecutive days
    of the same signal episode.
    """
    if fwd_windows is None:
        fwd_windows = [5, 10, 21, 42, 63, 126, 252]

    # Align series to common index
    df_work = pd.DataFrame({
        "close": close,
        "vcvi_21": vcvi_21_series,
        "vol_regime": vol_regime_series,
    }).dropna(subset=["close", "vcvi_21", "vol_regime"])

    # Attach NG regime (forward-fill to align NG=F dates → ETF dates)
    if ng_regime_series is not None and not ng_regime_series.empty:
        df_work["ng_regime"] = ng_regime_series.reindex(
            df_work.index.union(ng_regime_series.index)
        ).ffill().reindex(df_work.index)
    else:
        df_work["ng_regime"] = "unknown"

    if df_work.empty:
        return {"count": 0, "threshold_vcvi": vcvi_threshold,
                "threshold_vol_regime": vol_regime_max,
                "forward_returns": {}, "occurrences": []}

    # Exclude only the last 5 trading days — allow partial forward returns (null for incomplete windows)
    cutoff = df_work.index[-1] - pd.Timedelta(days=5)
    df_hist = df_work[df_work.index <= cutoff]

    if df_hist.empty:
        return {"count": 0, "threshold_vcvi": vcvi_threshold,
                "threshold_vol_regime": vol_regime_max,
                "forward_returns": {}, "occurrences": []}

    signal_mask = (df_hist["vcvi_21"] >= vcvi_threshold) & \
                  (df_hist["vol_regime"] <= vol_regime_max)

    signal_idx = df_hist.index[signal_mask].tolist()

    # Deduplicate: enforce min_gap_days between recorded instances
    deduplicated: list = []
    last_date = None
    for dt in signal_idx:
        if last_date is None or (dt - last_date).days >= min_gap_days:
            deduplicated.append(dt)
            last_date = dt

    if not deduplicated:
        return {"count": 0, "threshold_vcvi": vcvi_threshold,
                "threshold_vol_regime": vol_regime_max,
                "forward_returns": {}, "occurrences": []}

    # Compute forward returns for each qualifying date
    close_arr = df_work["close"]
    occurrences = []
    for dt in deduplicated:
        entry_pos = df_work.index.get_loc(dt)
        entry_price = close_arr.iloc[entry_pos]
        if entry_price <= 0:
            continue

        fwd_rets: Dict[str, Optional[float]] = {}
        for w in fwd_windows:
            exit_pos = entry_pos + w
            if exit_pos < len(close_arr):
                exit_price = close_arr.iloc[exit_pos]
                fwd_rets[f"{w}d"] = _safe_float((exit_price / entry_price - 1) * 100)
            else:
                fwd_rets[f"{w}d"] = None

        # Lead-time: days until best forward return within the first 63d (Feature 4)
        peak_window = [w for w in fwd_windows if w <= 63]
        days_to_peak = None
        if peak_window and entry_pos + max(peak_window) < len(close_arr):
            close_arr.iloc[entry_pos: entry_pos + max(peak_window) + 1]
            best_ret = None
            best_day = None
            for w in peak_window:
                if entry_pos + w < len(close_arr):
                    ret = fwd_rets.get(f"{w}d")
                    if ret is not None and (best_ret is None or abs(ret) > abs(best_ret)):
                        best_ret = ret
                        best_day = w
            days_to_peak = best_day

        occ_month = dt.month
        ng_reg = df_hist.loc[dt, "ng_regime"] if "ng_regime" in df_hist.columns else "unknown"
        occurrences.append({
            "date": dt.strftime("%Y-%m-%d"),
            "vcvi": _safe_float(df_hist.loc[dt, "vcvi_21"]),
            "vol_regime_pct": _safe_float(df_hist.loc[dt, "vol_regime"]),
            "price": _safe_float(entry_price),
            "fwd": fwd_rets,
            "days_to_peak": days_to_peak,
            "season": _season_label(occ_month),
            "seasonality_weight": _seasonal_weight(occ_month),
            "ng_regime": ng_reg,
        })

    if not occurrences:
        return {"count": 0, "threshold_vcvi": vcvi_threshold,
                "threshold_vol_regime": vol_regime_max,
                "forward_returns": {}, "occurrences": []}

    # Aggregate forward return statistics
    # Clip at ±200% before computing mean/best/worst — anything beyond is a data artifact
    # (reverse splits etc.) that would distort statistics. Median is unaffected by clipping.
    CLIP_PCT = 200.0

    def _agg_fwd_stats(occ_list, fwd_windows):
        stats = {}
        for w in fwd_windows:
            key = f"{w}d"
            rets = [o["fwd"][key] for o in occ_list if o["fwd"].get(key) is not None]
            if rets:
                rets_arr = np.array(rets)
                rets_clipped = np.clip(rets_arr, -CLIP_PCT, CLIP_PCT)
                stats[key] = {
                    "median":   _safe_float(float(np.median(rets_arr))),
                    "mean":     _safe_float(float(np.mean(rets_clipped))),
                    "win_rate": _safe_float(float(np.mean(rets_arr > 0) * 100)),
                    "best":     _safe_float(float(np.max(rets_clipped))),
                    "worst":    _safe_float(float(np.min(rets_clipped))),
                    "count":    len(rets),
                }
        return stats

    forward_stats = _agg_fwd_stats(occurrences, fwd_windows)

    # Regime-stratified forward return stats
    regime_fwd_stats: Dict[str, dict] = {}
    for regime_name in ("normal", "elevated", "extreme"):
        regime_occs = [o for o in occurrences if o.get("ng_regime") == regime_name]
        if regime_occs:
            r_stats = _agg_fwd_stats(regime_occs, fwd_windows)
            if r_stats:
                regime_fwd_stats[regime_name] = {"count": len(regime_occs), "forward_returns": r_stats}

    # Return most recent instances for display (most recent first)
    recent = list(reversed(occurrences))[:max_display]

    # Signal edge: find the most ACTIONABLE window (capped at 63d).
    # Beyond 63d, leveraged ETF decay dominates the signal — not a tradeable edge.
    # Score = |median| × |win_rate − 50|² (squaring win_rate deviation rewards consistency).
    best_edge_window = None
    best_edge_score = 0.0
    for key, stats in forward_stats.items():
        w_days = int(key.rstrip("d"))
        if w_days > 63 or stats["count"] < 10:
            continue
        wr_dev = abs((stats["win_rate"] or 50) - 50)
        score = abs(stats["median"] or 0) * (wr_dev ** 2)
        if score > best_edge_score:
            best_edge_score = score
            best_edge_window = key

    # Lead-time aggregate statistics (Feature 4)
    lead_times = [o["days_to_peak"] for o in occurrences if o.get("days_to_peak") is not None]
    if lead_times:
        lead_time_stats = {
            "median_days": int(np.median(lead_times)),
            "p25_days":    int(np.percentile(lead_times, 25)),
            "p75_days":    int(np.percentile(lead_times, 75)),
            "count":       len(lead_times),
        }
    else:
        lead_time_stats = None

    return {
        "count": len(occurrences),
        "threshold_vcvi": vcvi_threshold,
        "threshold_vol_regime": vol_regime_max,
        "forward_returns": forward_stats,
        "forward_returns_by_regime": regime_fwd_stats,   # regime-stratified stats
        "signal_edge_window": best_edge_window,
        "lead_time": lead_time_stats,
        "occurrences": recent,
    }


# ---------------------------------------------------------------------------
# Conviction Events — strict multi-gate anomaly filter
# ---------------------------------------------------------------------------
def _detect_conviction_events(
    close: pd.Series,
    pct_change: pd.Series,
    vcvi_21_series: pd.Series,
    vol_regime_series: pd.Series,
    vol_pct_series_dict: Dict[int, pd.Series],
    atr14_series: pd.Series,
    windows: list,
    min_gap_days: int = CONVICTION_MIN_GAP_DAYS,
    max_display: int = 25,
    ng_close: "Optional[pd.Series]" = None,
    etf_side: str = "long",
    ng_seasonal_z_series: "Optional[pd.Series]" = None,
    ng_regime_series: "Optional[pd.Series]" = None,
    vdds_series: "Optional[pd.Series]" = None,  # Gate 6: VDDS advisory annotation
) -> dict:
    """
    Scan full history for Conviction Events — dates where ALL gates fire:
      Gate 1: VCVI-21 >= 72  (critical vol-adjusted capitulation)
              OR Extreme Override: VCVI >= 90 AND |move| > 2× ATR (bypasses Gate 1 min)
      Gate 2: >= 3 of 5 vol-pct windows >= 85th percentile  (broad breadth)
      Gate 3: |daily move| > 1.5 × ATR-14  (price dislocation)
      Gate 4: vol regime <= 70th percentile  (non-turbulent context)
      Gate 5: NG=F seasonal z-score directional confirmation (when available)
              Long-side: z <= -0.5 (gas anomalously low for season)
              Short-side: z >= +0.2 (gas anomalously high for season)
              Momentum guard: short-side raises VCVI bar by 13 when gas seasonal_z > 0

    Designed to flag only ~1-2 genuine anomalies per ETF per year.
    """
    # Build aligned dataframe
    df_work = pd.DataFrame({
        "close": close,
        "pct_change": pct_change,
        "vcvi_21": vcvi_21_series,
        "vol_regime": vol_regime_series,
        "atr14": atr14_series,
    })

    # Add vol percentile windows
    for w in windows:
        col = f"vol_pct_{w}d"
        if w in vol_pct_series_dict:
            df_work[col] = vol_pct_series_dict[w]

    # Add NG=F seasonal z-score series (aligned to ETF dates via forward-fill)
    if ng_seasonal_z_series is not None and not ng_seasonal_z_series.empty:
        df_work["ng_seasonal_z"] = ng_seasonal_z_series.reindex(
            df_work.index.union(ng_seasonal_z_series.index)
        ).ffill().reindex(df_work.index)
    else:
        df_work["ng_seasonal_z"] = np.nan

    # Add NG=F regime series (forward-fill to align NG=F dates → ETF dates)
    if ng_regime_series is not None and not ng_regime_series.empty:
        df_work["ng_regime"] = ng_regime_series.reindex(
            df_work.index.union(ng_regime_series.index)
        ).ffill().reindex(df_work.index).fillna("unknown")
    else:
        df_work["ng_regime"] = "unknown"

    # Add VDDS series (Gate 6 annotation — advisory only, not a blocking filter)
    if vdds_series is not None and not vdds_series.empty:
        df_work["vdds"] = vdds_series.reindex(df_work.index)
    else:
        df_work["vdds"] = np.nan

    df_work = df_work.dropna(subset=["close", "vcvi_21", "vol_regime", "atr14", "pct_change"])
    if df_work.empty:
        return {"count": 0, "events": [], "annual_rate": None, "gates": _conviction_gate_spec()}

    atr_pct = df_work["atr14"] / df_work["close"] * 100
    daily_move_pct = df_work["pct_change"].abs()

    # Momentum guard (Change D): for short-side, raise VCVI bar when gas is in seasonal uptrend
    if etf_side == "short":
        ng_z_col = df_work["ng_seasonal_z"].fillna(0.0)
        vcvi_min_effective = np.where(
            ng_z_col > 0,
            CONVICTION_VCVI_MIN + MOMENTUM_GUARD_VCVI_BOOST,
            CONVICTION_VCVI_MIN,
        )
        momentum_guard_active = pd.Series(ng_z_col > 0, index=df_work.index)
    else:
        vcvi_min_effective = CONVICTION_VCVI_MIN
        momentum_guard_active = pd.Series(False, index=df_work.index)

    # Gate 1: VCVI-21 >= effective minimum (may be boosted by momentum guard)
    g1 = df_work["vcvi_21"] >= vcvi_min_effective

    # Extreme Override (Change A): bypass Gate 1 minimum if VCVI >= 90 AND move > 2× ATR
    extreme_override = (
        (df_work["vcvi_21"] >= EXTREME_OVERRIDE_VCVI_MIN) &
        (daily_move_pct > EXTREME_OVERRIDE_ATR_MULT * atr_pct)
    )
    g1_or_override = g1 | extreme_override

    # Gate 2: breadth — count windows above threshold
    breadth_cols = [f"vol_pct_{w}d" for w in windows if f"vol_pct_{w}d" in df_work.columns]
    if breadth_cols:
        breadth_count = (df_work[breadth_cols] >= CONVICTION_BREADTH_PCT).sum(axis=1)
        g2 = breadth_count >= CONVICTION_BREADTH_MIN
    else:
        g2 = pd.Series(False, index=df_work.index)

    # Gate 3: price dislocation — |daily move %| > N × ATR-14 as % of price
    g3 = daily_move_pct > (CONVICTION_ATR_MULT * atr_pct)

    # Gate 4: vol regime context — must be non-turbulent
    g4 = df_work["vol_regime"] <= CONVICTION_VOL_REGIME_MAX

    # Gate 5 (Change C): NG=F seasonal z-score directional confirmation (when available)
    ng_z = df_work["ng_seasonal_z"]
    ng_z_available = ng_z.notna()
    if etf_side == "long":
        # Long-side: gas should be seasonally low (z <= -1.0) or data unavailable
        g5 = (~ng_z_available) | (ng_z <= CONVICTION_NG_Z_LONG)
    else:
        # Short-side: gas should be seasonally high (z >= +1.0) or data unavailable
        g5 = (~ng_z_available) | (ng_z >= CONVICTION_NG_Z_SHORT)

    # ALL gates must fire (Gate 1 allows extreme override)
    all_pass = g1_or_override & g2 & g3 & g4 & g5
    signal_dates = df_work.index[all_pass].tolist()

    # Deduplicate with min gap
    deduplicated: list = []
    last_date = None
    for dt in signal_dates:
        if last_date is None or (dt - last_date).days >= min_gap_days:
            deduplicated.append(dt)
            last_date = dt

    if not deduplicated:
        return {"count": 0, "events": [], "annual_rate": None, "gates": _conviction_gate_spec()}

    # Forward return windows
    fwd_windows = [5, 10, 21, 42, 63]
    close_arr = df_work["close"]

    # Build event records with forward returns
    events = []
    for dt in deduplicated:
        row = df_work.loc[dt]
        bc = int((pd.Series({c: row[c] for c in breadth_cols}) >= CONVICTION_BREADTH_PCT).sum()) if breadth_cols else 0
        entry_pos = df_work.index.get_loc(dt)
        entry_price = close_arr.iloc[entry_pos]

        fwd_rets: Dict[str, Optional[float]] = {}
        for w in fwd_windows:
            exit_pos = entry_pos + w
            if exit_pos < len(close_arr) and entry_price > 0:
                fwd_rets[f"{w}d"] = _safe_float((close_arr.iloc[exit_pos] / entry_price - 1) * 100)
            else:
                fwd_rets[f"{w}d"] = None

        occ_month = dt.month
        is_extreme_override = bool(extreme_override.loc[dt]) if dt in extreme_override.index else False
        is_momentum_guard = bool(momentum_guard_active.loc[dt]) if dt in momentum_guard_active.index else False
        # Use direct index access (not .get()) — pandas Series.get() default handling
        # differs from dict.get() across versions; the 'in row.index' guard is sufficient.
        ng_z_val = _safe_float(row["ng_seasonal_z"]) if "ng_seasonal_z" in row.index else None
        ng_reg = str(row["ng_regime"]) if "ng_regime" in row.index else "unknown"
        vdds_val = _safe_float(row["vdds"]) if "vdds" in row.index else None
        events.append({
            "date": dt.strftime("%Y-%m-%d"),
            "vcvi": _safe_float(row["vcvi_21"]),
            "vol_regime_pct": _safe_float(row["vol_regime"]),
            "daily_move_pct": _safe_float(row["pct_change"]),
            "atr_ratio": _safe_float(abs(row["pct_change"]) / (row["atr14"] / row["close"] * 100)) if row["atr14"] > 0 else None,
            "breadth_count": bc,
            "price": _safe_float(row["close"]),
            "season": _season_label(occ_month),
            "seasonality_weight": _seasonal_weight(occ_month),
            "fwd": fwd_rets,
            "extreme_override":      is_extreme_override,
            "momentum_guard_active": is_momentum_guard,
            "ng_seasonal_z":         ng_z_val,
            "ng_regime":             ng_reg,
            "vdds":                  vdds_val,   # Gate 6 annotation
        })

    # Aggregate forward return statistics
    CLIP_PCT = 200.0
    forward_stats: Dict[str, dict] = {}
    for w in fwd_windows:
        key = f"{w}d"
        rets = [e["fwd"][key] for e in events if e["fwd"].get(key) is not None]
        if rets:
            rets_arr = np.array(rets)
            rets_clipped = np.clip(rets_arr, -CLIP_PCT, CLIP_PCT)
            forward_stats[key] = {
                "median":   _safe_float(float(np.median(rets_arr))),
                "mean":     _safe_float(float(np.mean(rets_clipped))),
                "win_rate": _safe_float(float(np.mean(rets_arr > 0) * 100)),
                "best":     _safe_float(float(np.max(rets_clipped))),
                "worst":    _safe_float(float(np.min(rets_clipped))),
                "count":    len(rets),
            }

    # Annual rate
    total_days = (df_work.index[-1] - df_work.index[0]).days
    annual_rate = _safe_float(len(events) / (total_days / 365.25)) if total_days > 90 else None

    # Return most recent first
    events_display = list(reversed(events))[:max_display]

    return {
        "count": len(events),
        "events": events_display,
        "annual_rate": annual_rate,
        "gates": _conviction_gate_spec(),
        "forward_returns": forward_stats,
    }


def _conviction_gate_spec() -> dict:
    """Return the gate thresholds used, for frontend display."""
    return {
        "vcvi_min":              CONVICTION_VCVI_MIN,
        "breadth_min":           CONVICTION_BREADTH_MIN,
        "breadth_pct":           CONVICTION_BREADTH_PCT,
        "atr_mult":              CONVICTION_ATR_MULT,
        "vol_regime_max":        CONVICTION_VOL_REGIME_MAX,
        "extreme_override_vcvi": EXTREME_OVERRIDE_VCVI_MIN,
        "extreme_override_atr":  EXTREME_OVERRIDE_ATR_MULT,
        "ng_z_long":             CONVICTION_NG_Z_LONG,
        "ng_z_short":            CONVICTION_NG_Z_SHORT,
        "momentum_guard_boost":  MOMENTUM_GUARD_VCVI_BOOST,
    }


# ---------------------------------------------------------------------------
# Elevated Watch — 3-gate softer filter (Feature 5)
# ---------------------------------------------------------------------------
def _detect_elevated_watch_events(
    close: "pd.Series",
    pct_change: "pd.Series",
    vcvi_21_series: "pd.Series",
    vol_pct_series_dict: Dict[int, "pd.Series"],
    atr14_series: "pd.Series",
    windows: list,
    conviction_dates: list = None,
    min_gap_days: int = WATCH_MIN_GAP_DAYS,
    max_display: int = 15,
) -> dict:
    """
    3-Gate Elevated Watch — softer than Conviction Events, no vol-regime gate.
      Gate 1: VCVI-21 >= 60   (warning level vs 72 for conviction)
      Gate 2: >= 2 of N vol-pct windows >= 75th pct   (breadth vs 3/85)
      Gate 3: |daily move| > 1.2 × ATR-14   (movement vs 1.5× for conviction)

    Conviction dates are excluded to avoid double-counting.
    Designed for ~4-8 events per ETF per year.
    """
    conviction_dates = conviction_dates or []

    df_work = pd.DataFrame({
        "close": close,
        "pct_change": pct_change,
        "vcvi_21": vcvi_21_series,
        "atr14": atr14_series,
    })
    for w in windows:
        col = f"vol_pct_{w}d"
        if w in vol_pct_series_dict:
            df_work[col] = vol_pct_series_dict[w]

    df_work = df_work.dropna(subset=["close", "vcvi_21", "atr14", "pct_change"])
    if df_work.empty:
        return {"count": 0, "events": [], "annual_rate": None, "gates": _watch_gate_spec()}

    # Gate 1
    g1 = df_work["vcvi_21"] >= WATCH_VCVI_MIN

    # Gate 2: breadth
    breadth_cols = [f"vol_pct_{w}d" for w in windows if f"vol_pct_{w}d" in df_work.columns]
    if breadth_cols:
        breadth_count = (df_work[breadth_cols] >= WATCH_BREADTH_PCT).sum(axis=1)
        g2 = breadth_count >= WATCH_BREADTH_MIN
    else:
        g2 = pd.Series(False, index=df_work.index)

    # Gate 3: price dislocation
    atr_pct = df_work["atr14"] / df_work["close"] * 100
    daily_move_pct = df_work["pct_change"].abs()
    g3 = daily_move_pct > (WATCH_ATR_MULT * atr_pct)

    all_pass = g1 & g2 & g3
    signal_dates = df_work.index[all_pass].tolist()

    # Build set of conviction date windows to exclude (±3 days)
    conviction_set = set()
    for cd in conviction_dates:
        for delta in range(-3, 4):
            conviction_set.add(cd + pd.Timedelta(days=delta))

    # Deduplicate
    deduplicated: list = []
    last_date = None
    for dt in signal_dates:
        if dt in conviction_set:
            continue
        if last_date is None or (dt - last_date).days >= min_gap_days:
            deduplicated.append(dt)
            last_date = dt

    if not deduplicated:
        return {"count": 0, "events": [], "annual_rate": None, "gates": _watch_gate_spec()}

    fwd_windows = [5, 10, 21, 42, 63]
    close_arr = df_work["close"]

    events = []
    for dt in deduplicated:
        row = df_work.loc[dt]
        bc = int((pd.Series({c: row[c] for c in breadth_cols}) >= WATCH_BREADTH_PCT).sum()) if breadth_cols else 0
        entry_pos = df_work.index.get_loc(dt)
        entry_price = close_arr.iloc[entry_pos]
        occ_month = dt.month

        fwd_rets: Dict[str, Optional[float]] = {}
        for w in fwd_windows:
            exit_pos = entry_pos + w
            if exit_pos < len(close_arr) and entry_price > 0:
                fwd_rets[f"{w}d"] = _safe_float((close_arr.iloc[exit_pos] / entry_price - 1) * 100)
            else:
                fwd_rets[f"{w}d"] = None

        events.append({
            "date": dt.strftime("%Y-%m-%d"),
            "vcvi": _safe_float(row["vcvi_21"]),
            "daily_move_pct": _safe_float(row["pct_change"]),
            "atr_ratio": _safe_float(abs(row["pct_change"]) / (row["atr14"] / row["close"] * 100)) if row["atr14"] > 0 else None,
            "breadth_count": bc,
            "price": _safe_float(row["close"]),
            "season": _season_label(occ_month),
            "seasonality_weight": _seasonal_weight(occ_month),
            "fwd": fwd_rets,
        })

    CLIP_PCT = 200.0
    forward_stats: Dict[str, dict] = {}
    for w in fwd_windows:
        key = f"{w}d"
        rets = [e["fwd"][key] for e in events if e["fwd"].get(key) is not None]
        if rets:
            rets_arr = np.array(rets)
            rets_clipped = np.clip(rets_arr, -CLIP_PCT, CLIP_PCT)
            forward_stats[key] = {
                "median":   _safe_float(float(np.median(rets_arr))),
                "mean":     _safe_float(float(np.mean(rets_clipped))),
                "win_rate": _safe_float(float(np.mean(rets_arr > 0) * 100)),
                "best":     _safe_float(float(np.max(rets_clipped))),
                "worst":    _safe_float(float(np.min(rets_clipped))),
                "count":    len(rets),
            }

    total_days = (df_work.index[-1] - df_work.index[0]).days
    annual_rate = _safe_float(len(events) / (total_days / 365.25)) if total_days > 90 else None

    events_display = list(reversed(events))[:max_display]

    return {
        "count": len(events),
        "events": events_display,
        "annual_rate": annual_rate,
        "gates": _watch_gate_spec(),
        "forward_returns": forward_stats,
    }


def _watch_gate_spec() -> dict:
    return {
        "vcvi_min": WATCH_VCVI_MIN,
        "breadth_min": WATCH_BREADTH_MIN,
        "breadth_pct": WATCH_BREADTH_PCT,
        "atr_mult": WATCH_ATR_MULT,
    }


# ---------------------------------------------------------------------------
# Cross-instrument metrics
# ---------------------------------------------------------------------------
def compute_pairs(
    metrics: Dict[str, dict],
) -> Dict[str, dict]:
    """Compute IPSI and pair status for each long/short pair."""
    pairs_out: Dict[str, dict] = {}
    for long_t, short_t in PAIRS:
        key = f"{long_t}_{short_t}"
        long_rvol = metrics.get(long_t, {}).get("_rvol_21_last")
        short_rvol = metrics.get(short_t, {}).get("_rvol_21_last")

        if long_rvol is not None and short_rvol is not None:
            # Regularized split- & float-invariant IPSI with Tikhonov prior
            ipsi = _safe_float(min(5.0, max(0.2, (short_rvol + 0.5) / (long_rvol + 0.5))))
        else:
            ipsi = None

        # Status buckets with regularized threshold and dual confirmation
        if ipsi is None:
            status = "quiet"
        elif ipsi > 2.2 and short_rvol is not None and short_rvol >= 1.5:
            status = "stress"
        elif ipsi > 1.4:
            status = "elevated"
        else:
            status = "quiet"

        pairs_out[key] = {
            "ipsi": ipsi,
            "long_rvol": long_rvol,
            "short_rvol": short_rvol,
            "status": status,
        }

    return pairs_out


# ---------------------------------------------------------------------------
# Side-Wide Volume Convergence (SWVC)
# ---------------------------------------------------------------------------
def _compute_side_convergence(
    frames: Dict[str, pd.DataFrame],
    lookback_days: int = SWVC_LOOKBACK_DAYS,
    window_days: int = SWVC_WINDOW_DAYS,
    rvol_threshold: float = SWVC_RVOL_THRESHOLD,
    rvol_period: int = 21,
) -> dict:
    """
    For each side (long / short), scan the last `lookback_days` trading days
    of each of the 3 ETFs' RVOL-21d series and find the most recent session
    where RVOL >= rvol_threshold.

    If all 3 ETFs on a side had a qualifying spike within `window_days` of each
    other, the side is considered "converged" — a rare, cross-market signal
    indicating that independent investor bases in the US, Canada and UK are all
    acting simultaneously.

    Returns a dict with 'long' and 'short' keys.
    """
    sides: Dict[str, list] = {"long": [], "short": []}
    for ticker, cfg in ETF_CONFIG.items():
        sides[cfg["side"]].append(ticker)

    out: Dict[str, dict] = {}

    for side_name, tickers in sides.items():
        etf_results: Dict[str, dict] = {}

        for ticker in tickers:
            df = frames.get(ticker)
            if df is None or len(df) < rvol_period + 1:
                etf_results[ticker] = {
                    "spiked": False, "date": None,
                    "days_ago": None, "peak_rvol": None,
                }
                continue

            vol = df["volume"].astype(float)
            # Compute rolling 21d RVOL for the tail (lookback + period for warm-up)
            tail_len = lookback_days + rvol_period + 5
            vol_tail = vol.iloc[-tail_len:]
            avg21 = vol_tail.rolling(rvol_period, min_periods=max(2, rvol_period // 2)).mean()
            rvol_tail = (vol_tail / avg21).iloc[-lookback_days:]

            # Find days that exceeded threshold
            spike_mask = rvol_tail >= rvol_threshold
            if not spike_mask.any():
                etf_results[ticker] = {
                    "spiked": False, "date": None,
                    "days_ago": None, "peak_rvol": None,
                }
                continue

            # Most recent spike date + its RVOL value
            last_spike_dt = rvol_tail[spike_mask].index[-1]
            last_spike_rvol = _safe_float(rvol_tail[spike_mask].iloc[-1])
            trading_days_ago = int(spike_mask[::-1].values.argmax())  # reverse argmax gives offset from end

            etf_results[ticker] = {
                "spiked": True,
                "date": last_spike_dt.strftime("%Y-%m-%d"),
                "days_ago": trading_days_ago,
                "peak_rvol": last_spike_rvol,
            }

        # Determine convergence across this side's 3 ETFs
        spiked = {t: r for t, r in etf_results.items() if r["spiked"]}
        score = len(spiked)

        # Calculate calendar-day spread between the earliest and latest spike
        window_spread = None
        all_3_within_window = False
        if score >= 2:
            # Map ticker -> spike date
            spike_dates_map = {}
            for t, r in spiked.items():
                if r["date"]:
                    spike_dates_map[t] = pd.Timestamp(r["date"])
            if len(spike_dates_map) >= 2:
                dates_list = sorted(spike_dates_map.values())
                span = (dates_list[-1] - dates_list[0]).days
                window_spread = span
                if score == 3 and span <= window_days:
                    all_3_within_window = True

        if all_3_within_window:
            status = "converged"
        elif score == 3:
            status = "partial"   # all 3 spiked but outside the window
        elif score == 2:
            status = "partial"
        elif score == 1:
            status = "single"
        else:
            status = "quiet"

        out[side_name] = {
            "status": status,
            "score": score,
            "total": len(tickers),
            "window_days": window_days,
            "window_spread_days": window_spread,
            "threshold_rvol": rvol_threshold,
            "lookback_days": lookback_days,
            "etfs": etf_results,
        }

    return out


# ---------------------------------------------------------------------------
# Pipeline orchestration
# ---------------------------------------------------------------------------
def run_pipeline() -> None:
    """Execute the full data pipeline."""
    logger.info("=" * 60)
    logger.info("Natural Gas ETF Data Pipeline – starting")
    logger.info("=" * 60)

    # Ensure output directory exists
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # ---- 1. Fetch live data from Yahoo Finance ----
    frames, live_snapshots, auto_detected_splits = _fetch_all()

    # ---- 1b. Fetch NG=F gas price context (Feature 2) ----
    ng_price_context = _fetch_ng_price_context()
    # Extract internal series (not JSON-serialised) — precomputed in _fetch_ng_price_context
    ng_close_series:  "Optional[pd.Series]" = ng_price_context.pop("_close_series",  None)
    ng_z_series:      "Optional[pd.Series]" = ng_price_context.pop("_z_series",      None)
    ng_regime_series: "Optional[pd.Series]" = ng_price_context.pop("_regime_series", None)

    if not frames:
        logger.error("No data frames available – aborting")
        sys.exit(1)

    # ---- 2. Compute per-ETF metrics ----
    all_metrics: Dict[str, dict] = {}
    for ticker, df in frames.items():
        logger.info("Computing metrics for %s …", ticker)
        try:
            m = compute_etf_metrics(
                df,
                side=ETF_CONFIG[ticker]["side"],
                ticker=ticker,
                ng_close=ng_close_series,
                ng_seasonal_z_series=ng_z_series,
                ng_regime_series=ng_regime_series,
            )
            all_metrics[ticker] = m
        except Exception:
            logger.exception("Metric computation failed for %s", ticker)

    # ---- 2b. Overlay real-time prices from meta.regularMarketPrice ----
    # meta.regularMarketPrice updates continuously; the daily bar updates ~hourly.
    # This ensures current.price always reflects the live market price.
    for ticker, snap in live_snapshots.items():
        if ticker not in all_metrics:
            continue
        cur = all_metrics[ticker]["current"]
        if snap.get("price") is not None:
            cur["price"] = snap["price"]
        if snap.get("volume") is not None:
            cur["volume"] = snap["volume"]
        if snap.get("change_pct") is not None:
            cur["change_pct"] = snap["change_pct"]
        if snap.get("price") is not None and snap.get("volume") is not None:
            cur["dollar_volume"] = round(snap["price"] * snap["volume"], 4)

    # ---- 3. Cross-instrument metrics ----
    pairs_data = compute_pairs(all_metrics)

    # ---- 3b. Side-Wide Volume Convergence ----
    side_convergence = _compute_side_convergence(frames)

    # ---- 4. Aggregate signals ----
    all_signals: List[dict] = []
    for ticker, m in all_metrics.items():
        for alert in m.get("alerts", []):
            all_signals.append({**alert, "ticker": ticker})
        # Add ng_price gate annotation to fast spike alerts if applicable
        if m.get("sharp_spike") and ng_price_context.get("price") is not None:
            etf_side = ETF_CONFIG[ticker]["side"]
            gate_active = (
                ng_price_context.get("gate_short") if etf_side == "short"
                else ng_price_context.get("gate_long")
            )
            if not gate_active:
                # Re-label existing fast_spike as unconfirmed
                for sig in all_signals:
                    if sig.get("ticker") == ticker and sig.get("type") == "fast_spike_critical":
                        sig["type"] = "fast_spike_unconfirmed"
                        sig["message"] += f" [NG=F at {ng_price_context['percentile_2yr']:.0f}th pct — gas price gate not active]"

    # Add pair-level alerts
    for pair_key, pair_info in pairs_data.items():
        if pair_info["status"] == "stress":
            all_signals.append({
                "type": "ipsi_stress",
                "ticker": pair_key,
                "message": f"IPSI stress for {pair_key} — ratio {pair_info['ipsi']:.2f}" if pair_info["ipsi"] else f"IPSI stress for {pair_key}",
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })

    # ---- 5. Build output ----
    etfs_out: Dict[str, dict] = {}
    for ticker, m in all_metrics.items():
        cfg = ETF_CONFIG[ticker]
        entry = {
            "side": cfg["side"],
            "pair": cfg["pair"],
            "name": cfg["name"],
            "current": m["current"],
            "rvol": m["rvol"],
            "z_score": m["z_score"],
            "vroc": m["vroc"],
            "vol_percentile": m["vol_percentile"],
            "price_percentile": m["price_percentile"],
            "cvi": m["cvi"],
            "vcvi": m["vcvi"],
            "volatility": m["volatility"],
            "historical_echoes": m["historical_echoes"],
            "conviction_events": m["conviction_events"],
            "elevated_watch": m.get("elevated_watch"),          # Feature 5
            "decay": m.get("decay"),                            # Feature 6
            "seasonality": m.get("seasonality"),                # Feature 3
            "sharp_spike": m.get("sharp_spike", False),         # Feature 1
            "fast_signal": m.get("fast_signal"),                # Feature 1
            "vps": m["vps"],
            "mwca": m["mwca"],
            "rolling_correlation": m["rolling_correlation"],
            "moving_averages": m["moving_averages"],
            "history": m["history"],
            "alerts": m["alerts"],
            # Dollar Volume Metrics
            "dv_rvol": m.get("dv_rvol", {}),
            "dv_zscore": m.get("dv_zscore", {}),
            "dv_percentile": m.get("dv_percentile", {}),
            "dv_vroc": m.get("dv_vroc", {}),
            "dvcvi": m.get("dvcvi", {}),
            "dv_vps": m.get("dv_vps"),
            "vdds": m.get("vdds"),
        }
        etfs_out[ticker] = _safe_dict(entry)

    # Add SWVC-level alerts to signals
    for side_name, sc in side_convergence.items():
        if sc.get("status") == "converged":
            tickers_str = ", ".join(sc.get("etfs", {}).keys())
            span = sc.get("window_spread_days")
            all_signals.append({
                "type": "swvc_converged",
                "ticker": f"{side_name}_side",
                "message": (
                    f"All 3 {side_name} ETFs ({tickers_str}) spiked within "
                    f"{span} calendar days — cross-market side-wide convergence"
                ),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })

    dashboard = {
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "market_status": _market_status(),
        "ng_price_context": _safe_dict(ng_price_context),   # Feature 2
        "etfs": etfs_out,
        "pairs": _safe_dict(pairs_data),
        "side_convergence": _safe_dict(side_convergence),
        "signals": all_signals,
    }

    # ---- 6. Write dashboard JSON ----
    with open(DASHBOARD_JSON, "w") as f:
        json.dump(dashboard, f, indent=2, default=str)
    logger.info("Wrote %s (%.1f KB)", DASHBOARD_JSON, DASHBOARD_JSON.stat().st_size / 1024)

    # ---- 7. Write lightweight signals JSON ----
    latest_signals = {
        "last_updated": dashboard["last_updated"],
        "market_status": dashboard["market_status"],
        "signals": all_signals,
        "pair_status": {k: v["status"] for k, v in pairs_data.items()},
        "etf_summary": {
            ticker: {
                "price": etfs_out[ticker]["current"]["price"],
                "change_pct": etfs_out[ticker]["current"]["change_pct"],
                "vps": etfs_out[ticker]["vps"],
                "mwca": etfs_out[ticker]["mwca"],
                "vol_regime_pct": (etfs_out[ticker].get("volatility") or {}).get("vol_regime_pct"),
                "hv_term_structure": (etfs_out[ticker].get("volatility") or {}).get("hv_term_structure"),
            }
            for ticker in etfs_out
        },
    }
    with open(SIGNALS_JSON, "w") as f:
        json.dump(latest_signals, f, indent=2, default=str)
    logger.info("Wrote %s (%.1f KB)", SIGNALS_JSON, SIGNALS_JSON.stat().st_size / 1024)

    # ---- 8. Write split warnings JSON (auto-detected unregistered splits) ----
    split_warnings = {
        "last_checked": datetime.now(timezone.utc).isoformat(),
        "detected_count": len(auto_detected_splits),
        "detected_splits": auto_detected_splits,
        "note": (
            "Empty detected_splits = all known splits are registered in MANUAL_SPLITS. "
            "Non-empty = new consolidation/split found; add entries to MANUAL_SPLITS "
            "in data_pipeline.py and trough_peak_data.py to silence future warnings."
        ),
    }
    with open(SPLIT_WARNINGS_JSON, "w") as f:
        json.dump(split_warnings, f, indent=2, default=str)
    if auto_detected_splits:
        logger.warning(
            "SPLIT WARNINGS: %d unregistered split(s) auto-applied — "
            "review %s and update MANUAL_SPLITS",
            len(auto_detected_splits), SPLIT_WARNINGS_JSON,
        )
    else:
        logger.info("Split check clean — no unregistered splits detected")

    logger.info("Pipeline complete - %d ETFs, %d signals", len(etfs_out), len(all_signals))

    # 9. Generate Historical Return Distributions and Tail Risk Engine
    try:
        from scripts.generate_return_distributions import generate_distributions
        logger.info("Generating historical return distributions and tail risk metrics...")
        generate_distributions()
        logger.info("Historical return distributions generation complete")
    except Exception as exc:
        logger.error("Failed to generate return distributions: %s", exc)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    run_pipeline()