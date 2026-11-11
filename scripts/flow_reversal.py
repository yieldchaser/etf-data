#!/usr/bin/env python3
"""Shared source reversal-pair netting and derived flow metrics helpers.

Canonicalizes:
- ``net_reversal_pairs``: Detects consecutive-day Trackinsight shares-outstanding
  reporting errors (opposite-signed spikes >= 5x prior 60-day median |flow|
  that cancel within 20%) and nets the pair onto the second day.
- ``apply_derived_metrics``: Computes cumulative_flow, daily_inflow, daily_outflow,
  flow_zscore, flow_5d, flow_20d, regime, and pressure over netted flows while
  preserving raw ``usd_flow`` verbatim.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


def regime_label(z_score: Any) -> str:
    """Classify a flow z-score into regime labels without NaN coercion leaks."""
    if pd.isna(z_score):
        return "UNAVAILABLE"
    z_val = float(z_score)
    if z_val > 1.5:
        return "ACCUMULATION"
    if z_val < -1.5:
        return "DISTRIBUTION"
    return "BALANCED"


def net_reversal_pairs(flow: Any) -> pd.Series:
    """Return flow series with consecutive-day source reversal pairs netted onto day T.

    Preserves missing (NaN) entries at their original positions.
    """
    if isinstance(flow, pd.Series):
        series = flow
    elif isinstance(flow, (np.ndarray, list, tuple)):
        series = pd.Series(flow)
    else:
        series = pd.Series(list(flow))
    numeric = pd.to_numeric(series, errors="coerce").astype(float)
    arr = numeric.to_numpy(dtype=float).copy()
    valid_abs = numeric.abs()
    typical = valid_abs.rolling(60, min_periods=20).median().shift(2).to_numpy()

    for i in range(1, len(arr)):
        a = arr[i - 1]
        b = arr[i]
        t = typical[i]
        if not (np.isfinite(a) and np.isfinite(b) and np.isfinite(t)):
            continue
        if a * b >= 0:
            continue
        if min(abs(a), abs(b)) >= 5.0 * max(t, 1.0) and abs(a + b) <= 0.2 * max(abs(a), abs(b)):
            arr[i] = a + b
            arr[i - 1] = 0.0

    return pd.Series(arr, index=series.index, dtype=float)


def apply_derived_metrics(
    df: pd.DataFrame,
    *,
    preserve_na: bool = False,
    net_reversals: bool = True,
    round_decimals: int | None = 4,
) -> pd.DataFrame:
    """Compute derived flow columns while keeping raw ``usd_flow`` unchanged.

    Args:
        df: Input DataFrame containing at least ``usd_flow``.
        preserve_na: If True (used by schema validators), leave NaN flow rows as
            NaN in derived fields and mark ``regime`` as ``"UNAVAILABLE"``.
            If False (used by authoritative CSV writers), fill derived scalars
            with 0.0 and classify 0 as ``"BALANCED"``.
        net_reversals: Whether to apply ``net_reversal_pairs`` to derived metrics.
        round_decimals: Decimal rounding for numeric columns (None disables).
    """
    if df.empty:
        return df.copy()

    result = df.copy()
    flow = pd.to_numeric(result["usd_flow"], errors="coerce").astype(float)
    filled_raw = flow.fillna(0.0)
    result["cumulative_flow"] = filled_raw.cumsum()
    result["daily_inflow"] = flow.clip(lower=0)
    result["daily_outflow"] = flow.clip(upper=0)

    metric_flow = net_reversal_pairs(flow) if net_reversals else flow.copy()
    mean_30d = metric_flow.rolling(30, min_periods=5).mean()
    std_30d = metric_flow.rolling(30, min_periods=5).std()

    if preserve_na:
        z_score = ((metric_flow - mean_30d) / std_30d).where(std_30d > 0)
        flow_5d = metric_flow.rolling(5, min_periods=1).sum()
        flow_20d = metric_flow.rolling(20, min_periods=1).sum()
        result["flow_zscore"] = z_score
        result["flow_5d"] = flow_5d
        result["flow_20d"] = flow_20d
        result["regime"] = z_score.map(regime_label)

        filled_metric = metric_flow.fillna(0.0)
        signs = np.sign(filled_metric)
        streaks = signs.groupby((signs != signs.shift()).cumsum()).cumsum()
        momentum_bonus = np.where(flow_5d > 0, 10.0, np.where(flow_5d < 0, -10.0, 0.0))
        streak_bonus = np.minimum(streaks.abs() * 2, 20) * np.sign(streaks)
        result["pressure"] = (z_score * 25.0 + momentum_bonus + streak_bonus).clip(-100.0, 100.0)
    else:
        raw_z = np.where(std_30d > 0, (metric_flow - mean_30d) / std_30d, 0.0)
        z_series = pd.Series(raw_z, index=result.index, dtype=float).fillna(0.0)
        result["flow_zscore"] = z_series
        result["flow_5d"] = metric_flow.rolling(5, min_periods=1).sum().fillna(0.0)
        result["flow_20d"] = metric_flow.rolling(20, min_periods=1).sum().fillna(0.0)
        result["regime"] = np.where(
            z_series > 1.5,
            "ACCUMULATION",
            np.where(z_series < -1.5, "DISTRIBUTION", "BALANCED"),
        )

        filled_metric = metric_flow.fillna(0.0)
        signs = np.sign(filled_metric)
        streaks = signs.groupby((signs != signs.shift()).cumsum()).cumsum()
        momentum_bonus = np.where(
            result["flow_5d"] > 0, 10.0, np.where(result["flow_5d"] < 0, -10.0, 0.0)
        )
        streak_bonus = np.minimum(streaks.abs() * 2, 20) * np.sign(streaks)
        raw_p = (z_series * 25.0) + momentum_bonus + streak_bonus
        result["pressure"] = pd.Series(np.clip(raw_p, -100.0, 100.0), index=result.index).fillna(0.0)

    if round_decimals is not None:
        for col in (
            "usd_flow",
            "daily_inflow",
            "daily_outflow",
            "cumulative_flow",
            "nav",
            "perf_pct",
            "flow_zscore",
            "flow_5d",
            "flow_20d",
            "pressure",
        ):
            if col in result.columns:
                result[col] = pd.to_numeric(result[col], errors="coerce").round(round_decimals)

    return result
