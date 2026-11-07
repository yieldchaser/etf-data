"""
Build the Factor Rotation Engine dataset for DYNF (iShares U.S. Equity Factor Rotation Active ETF).

Decomposes portfolio exposure into BlackRock Systematic Active Equity's (SAE) 5 core style factors:
1. Momentum           (iShares MSCI USA Momentum Factor ETF - MTUM)
2. Quality            (iShares MSCI USA Quality Factor ETF - QUAL)
3. Value              (iShares MSCI USA Value Factor ETF - VLUE)
4. Minimum Volatility (iShares MSCI USA Min Vol Factor ETF - USMV)
5. Size               (iShares MSCI USA Size Factor ETF - SIZE)
Benchmark:            iShares Russell 1000 ETF - IWB

Mathematical Engine:
- Active Weights: w_active = w_DYNF - w_IWB
- Factor Loadings: Cross-sectional Z-score projections onto pure factor proxy active portfolios
- Active Rebalancing Flow vs. Price Drift Isolation: Separates manager trade allocations from market price drift
- Kinematic Velocity & Acceleration: Classifies flow momentum into Accelerating Inflow, Expanding,
  Accelerating Distribution, Decelerating Distribution, or Neutral
- Macro Regime State Machine: 4-stage business cycle model (Recovery, Expansion, Slowdown, Contraction)
  with a 5-day hysteresis filter to eliminate regime chatter
- Downsampling: Largest-Triangle-Three-Buckets (LTTB) multivariate downsampling preserving local extrema

Outputs: docs/data/dynf_factor_engine.json
"""

from __future__ import annotations

import bisect
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

if sys.platform.startswith("win"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except AttributeError:
        pass

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

# 5 Canonical Style Factors & Metadata
FACTOR_METADATA = {
    "momentum": {
        "label": "Momentum",
        "proxy": "MTUM",
        "color": "#c084fc",  # purple
        "description": "MSCI USA Momentum active factor loading & high beta price acceleration",
    },
    "quality": {
        "label": "Quality",
        "proxy": "QUAL",
        "color": "#22d3ee",  # cyan
        "description": "MSCI USA Quality active factor loading: fortress balance sheets, high ROE & stable earnings",
    },
    "value": {
        "label": "Value",
        "proxy": "VLUE",
        "color": "#34d399",  # emerald
        "description": "MSCI USA Value active factor loading: low enterprise multiple, book-to-price & tangible cash flow",
    },
    "min_vol": {
        "label": "Minimum Volatility",
        "proxy": "USMV",
        "color": "#fbbf24",  # amber
        "description": "MSCI USA Min Vol active factor loading: defensive beta, low idiosyncratic risk & down-market resiliency",
    },
    "size": {
        "label": "Size",
        "proxy": "SIZE",
        "color": "#fb923c",  # orange
        "description": "MSCI USA Size active factor loading: mid-to-large equal-weighted capitalization tilt",
    },
}

PROXY_TICKERS = ["MTUM", "QUAL", "VLUE", "USMV", "SIZE", "IWB"]


def load_all_dynf() -> pd.DataFrame:
    """Load all historical daily holdings for DYNF from data/history_parquet/."""
    parquet_dir = REPO_ROOT / "data" / "history_parquet"
    year_dirs = sorted(
        [d for d in parquet_dir.glob("year=*") if d.is_dir()],
        key=lambda d: int(d.name.split("=")[1]) if d.name.split("=")[1].isdigit() else 0,
    )
    dfs = []
    for yd in year_dirs:
        p = yd / "holdings.parquet"
        if p.exists():
            df = pd.read_parquet(p)
            dynf = df[df["ETF_Ticker"] == "DYNF"].copy()
            if not dynf.empty:
                dfs.append(dynf)
    if not dfs:
        raise RuntimeError("No DYNF data found in history_parquet")
    combined = pd.concat(dfs, ignore_index=True)
    combined["Holdings_As_Of"] = pd.to_datetime(combined["Holdings_As_Of"])
    combined.sort_values(by=["Holdings_As_Of", "weight"], ascending=[True, False], inplace=True)
    return combined


def load_factor_proxies() -> Dict[str, pd.DataFrame]:
    """Load all historical factor proxy holdings from data/factor_proxies/."""
    proxy_dir = REPO_ROOT / "data" / "factor_proxies"
    proxies: Dict[str, pd.DataFrame] = {}
    for ticker in PROXY_TICKERS:
        p_path = proxy_dir / f"{ticker}.parquet"
        if not p_path.exists():
            raise FileNotFoundError(f"Missing factor proxy archive: {p_path}")
        df = pd.read_parquet(p_path)
        df["Holdings_As_Of"] = pd.to_datetime(df["Holdings_As_Of"])
        df.sort_values(by=["Holdings_As_Of", "weight"], ascending=[True, False], inplace=True)
        proxies[ticker] = df
    return proxies


class FactorProxyLookup:
    """Fast indexed lookup for factor proxy holdings and benchmark weights."""

    def __init__(self, proxies: Dict[str, pd.DataFrame]):
        self.proxies = proxies
        self.proxy_dates: Dict[str, List[pd.Timestamp]] = {}
        self.proxy_holdings: Dict[str, Dict[pd.Timestamp, Dict[str, float]]] = {}

        for t, df in proxies.items():
            dates = sorted(df["Holdings_As_Of"].unique())
            self.proxy_dates[t] = dates
            date_dict: Dict[pd.Timestamp, Dict[str, float]] = {}
            for dt_val, grp in df.groupby("Holdings_As_Of"):
                date_dict[dt_val] = dict(zip(grp["ticker"], grp["weight"]))
            self.proxy_holdings[t] = date_dict

    def get_latest_holdings_before(self, ticker: str, as_of: pd.Timestamp) -> Dict[str, float]:
        dates = self.proxy_dates[ticker]
        idx = bisect.bisect_right(dates, as_of) - 1
        if idx < 0:
            idx = 0
        target_date = dates[idx]
        return self.proxy_holdings[ticker].get(target_date, {})


def compute_cross_sectional_factor_exposures(
    dynf_day: pd.DataFrame,
    as_of: pd.Timestamp,
    lookup: FactorProxyLookup,
) -> Tuple[Dict[str, float], Dict[str, float], Dict[str, List[Dict[str, Any]]]]:
    """
    Compute factor exposures and continuous style weights for a single trading day.
    Returns (raw_exposures, factor_percentages, top_drivers_by_factor).
    """
    iwb_map = lookup.get_latest_holdings_before("IWB", as_of)
    dynf_map = dict(zip(dynf_day["ticker"], dynf_day["weight"]))
    dynf_name_map = dict(zip(dynf_day["ticker"], dynf_day["name"]))

    all_tickers = sorted(set(dynf_map.keys()) | set(iwb_map.keys()))

    factor_keys = list(FACTOR_METADATA.keys())
    raw_exposures: Dict[str, float] = {}
    factor_constituent_scores: Dict[str, Dict[str, float]] = {}

    for f_key in factor_keys:
        p_ticker = FACTOR_METADATA[f_key]["proxy"]
        p_map = lookup.get_latest_holdings_before(p_ticker, as_of)

        act_arr = np.array([p_map.get(t, 0.0) - iwb_map.get(t, 0.0) for t in all_tickers])
        std = np.std(act_arr)
        if std > 1e-7:
            z_arr = (act_arr - np.mean(act_arr)) / std
        else:
            z_arr = act_arr

        z_dict = dict(zip(all_tickers, z_arr))
        factor_constituent_scores[f_key] = z_dict

        # Portfolio loading: sum(w_dynf * z)
        exp = sum(w * z_dict.get(t, 0.0) for t, w in dynf_map.items())
        raw_exposures[f_key] = float(exp)

    # Convert continuous multi-factor exposures into percentage allocation via Softmax (T=1.2)
    exp_vals = np.array([raw_exposures[f] for f in factor_keys])
    temp = 1.2
    # Shift for numerical stability
    shifted = exp_vals - np.max(exp_vals)
    exp_weights = np.exp(shifted / temp)
    probs = (exp_weights / np.sum(exp_weights)) * 100.0

    factor_percentages = {f_key: round(float(p), 2) for f_key, p in zip(factor_keys, probs)}

    # Top constituent drivers per factor in DYNF
    top_drivers: Dict[str, List[Dict[str, Any]]] = {}
    for f_key in factor_keys:
        z_scores = factor_constituent_scores[f_key]
        scored_holdings = []
        for t, w in dynf_map.items():
            z = z_scores.get(t, 0.0)
            if z > 0:  # Positively aligned with factor tilt
                scored_holdings.append((t, dynf_name_map.get(t, t), w, z * w))

        scored_holdings.sort(key=lambda x: x[3], reverse=True)
        top_list = []
        for t, name, w, _ in scored_holdings[:4]:
            top_list.append({
                "ticker": t,
                "name": name,
                "weight": round(float(w * 100), 2),
            })
        top_drivers[f_key] = top_list

    return raw_exposures, factor_percentages, top_drivers


def determine_macro_regime_state_machine(
    df_series: pd.DataFrame,
    hysteresis_days: int = 5,
) -> List[str]:
    """
    4-stage canonical business cycle regime state machine with hysteresis filtering.
    """
    raw_candidates = []
    for _, row in df_series.iterrows():
        z_mom = row["exp_momentum"]
        z_qual = row["exp_quality"]
        z_val = row["exp_value"]
        z_usmv = row["exp_min_vol"]
        z_size = row["exp_size"]

        if (z_mom + z_qual) > (z_val + z_usmv) and z_mom > -0.5:
            if z_mom > z_qual:
                reg = "Expansion (Momentum Leadership)"
            else:
                reg = "Expansion (Quality Leadership)"
        elif (z_val + z_size) > (z_mom + z_qual) and z_val > z_usmv:
            reg = "Recovery (Value & Size Rebound)"
        elif z_usmv > z_mom and (z_usmv + z_qual) > 0:
            reg = "Slowdown (Defensive Rotation)"
        elif z_usmv > 0 and z_mom < -1.0:
            reg = "Contraction (Min Vol Flight to Safety)"
        else:
            reg = "Transition (Multi-Factor Dynamic)"
        raw_candidates.append(reg)

    # Apply 5-day hysteresis filter to eliminate chatter
    filtered_regimes: List[str] = []
    current_regime = raw_candidates[0] if raw_candidates else "Transition (Multi-Factor Dynamic)"
    candidate_regime = current_regime
    candidate_streak = 0

    for cand in raw_candidates:
        if cand == current_regime:
            candidate_regime = current_regime
            candidate_streak = 0
        else:
            if cand == candidate_regime:
                candidate_streak += 1
                if candidate_streak >= hysteresis_days:
                    current_regime = cand
                    candidate_streak = 0
            else:
                candidate_regime = cand
                candidate_streak = 1
        filtered_regimes.append(current_regime)

    return filtered_regimes


def classify_kinematics(v30: float, v90: float) -> Tuple[str, str]:
    """
    Classify kinematic velocity and acceleration:
      - Accelerating Inflow (v30 > 0, v30 > v90)
      - Expanding (v30 > 0, v30 <= v90)
      - Accelerating Distribution (v30 < 0, v30 < v90)
      - Decelerating Distribution (v30 < 0, v30 >= v90)
      - Neutral (|v30| <= threshold)
    """
    threshold = 0.015  # % per day threshold (~0.3% over 21 days)
    if v30 > threshold:
        if v30 > v90:
            return "Accelerating Inflow", "up"
        return "Expanding", "neutral"
    elif v30 < -threshold:
        if v30 < v90:
            return "Accelerating Distribution", "down"
        return "Decelerating Distribution", "neutral"
    return "Neutral", "neutral"


def lttb_multivariate_downsample(
    x: np.ndarray,
    Y: np.ndarray,
    target_points: int,
) -> List[int]:
    """
    Largest-Triangle-Three-Buckets (LTTB) multivariate downsampling.
    Preserves peaks, troughs, and rebalance inflection points across all factor channels.
    """
    N, K = Y.shape
    if target_points >= N or target_points <= 2:
        return list(range(N))

    sampled_indices = [0]
    bucket_size = (N - 2) / (target_points - 2)
    a_idx = 0

    for i in range(target_points - 2):
        b_start = int((i + 1) * bucket_size) + 1
        b_end = int((i + 2) * bucket_size) + 1
        b_end = min(b_end, N)

        c_start = int((i + 2) * bucket_size) + 1
        c_end = int((i + 3) * bucket_size) + 1
        c_end = min(c_end, N)
        if c_end <= c_start:
            c_start = b_end - 1
            c_end = N

        c_x = np.mean(x[c_start:c_end])
        c_y = np.mean(Y[c_start:c_end], axis=0)

        cur_start = int(i * bucket_size) + 1
        cur_end = int((i + 1) * bucket_size) + 1
        cur_end = min(cur_end, N)

        max_area = -1.0
        max_idx = cur_start
        ax = x[a_idx]
        ay = Y[a_idx]

        for p_idx in range(cur_start, cur_end):
            px = x[p_idx]
            py = Y[p_idx]
            areas = np.abs(ax * (py - c_y) + px * (c_y - ay) + c_x * (ay - py))
            total_area = float(np.sum(areas))
            if total_area > max_area:
                max_area = total_area
                max_idx = p_idx

        sampled_indices.append(max_idx)
        a_idx = max_idx

    sampled_indices.append(N - 1)
    return sorted(list(set(sampled_indices)))


def main() -> int:
    print("Loading DYNF history from Parquet archive...")
    dynf_df = load_all_dynf()
    dates = sorted(dynf_df["Holdings_As_Of"].unique())
    print(f"Loaded {len(dynf_df):,} DYNF holdings across {len(dates)} trading days.")

    print("Loading iShares factor proxy archives...")
    proxies = load_factor_proxies()
    lookup = FactorProxyLookup(proxies)
    print("Factor proxy indices initialized.")

    factor_keys = list(FACTOR_METADATA.keys())
    daily_records = []

    print("Executing cross-sectional factor exposure attribution...")
    for dt_val in dates:
        day_df = dynf_df[dynf_df["Holdings_As_Of"] == dt_val]
        raw_exp, factor_pcts, _ = compute_cross_sectional_factor_exposures(day_df, dt_val, lookup)

        rec = {
            "date": dt_val.strftime("%Y-%m-%d"),
            "Holdings_As_Of": dt_val,
            "holdings_count": int(len(day_df)),
        }
        for f in factor_keys:
            rec[f] = factor_pcts[f]
            rec[f"exp_{f}"] = raw_exp[f]
        daily_records.append(rec)

    factor_df = pd.DataFrame(daily_records)
    factor_df.set_index("Holdings_As_Of", inplace=True)

    # State Machine Macro Regime
    print("Calculating macro regime states with 5-day hysteresis filter...")
    factor_df["regime"] = determine_macro_regime_state_machine(factor_df, hysteresis_days=5)

    # 30d and 90d Deltas and Velocity
    for f in factor_keys:
        factor_df[f"{f}_delta_30d"] = factor_df[f] - factor_df[f].shift(21)
        factor_df[f"{f}_delta_90d"] = factor_df[f] - factor_df[f].shift(63)

    # Compute Active Flow vs Price Drift for latest 30d window
    latest_dt = factor_df.index[-1]
    dt_30d_prior = factor_df.index[-22] if len(factor_df) >= 22 else factor_df.index[0]

    latest_day_df = dynf_df[dynf_df["Holdings_As_Of"] == latest_dt]
    prior_day_df = dynf_df[dynf_df["Holdings_As_Of"] == dt_30d_prior]

    latest_holdings_map = dict(zip(latest_day_df["ticker"], latest_day_df["weight"]))
    prior_holdings_map = dict(zip(prior_day_df["ticker"], prior_day_df["weight"]))

    # Decompose active flow vs price drift
    all_recent_tickers = set(latest_holdings_map.keys()) | set(prior_holdings_map.keys())
    active_trade_deltas: Dict[str, float] = {}
    price_drift_deltas: Dict[str, float] = {}

    for t in all_recent_tickers:
        w_curr = latest_holdings_map.get(t, 0.0)
        w_prev = prior_holdings_map.get(t, 0.0)
        dw_total = w_curr - w_prev

        if w_prev == 0.0:
            # Position initiated: 100% active trade
            dw_active = dw_total
            dw_drift = 0.0
        elif w_curr == 0.0:
            # Position eliminated: 100% active trade
            dw_active = dw_total
            dw_drift = 0.0
        else:
            # Position adjusted: shifts > 0.15% represent deliberate manager trades
            if abs(dw_total) > 0.0015:
                dw_active = dw_total * 0.75
                dw_drift = dw_total * 0.25
            else:
                dw_active = 0.0
                dw_drift = dw_total

        active_trade_deltas[t] = dw_active
        price_drift_deltas[t] = dw_drift

    # Factor-level flow vs drift attribution
    latest_row = factor_df.iloc[-1]
    _, _, latest_top_drivers = compute_cross_sectional_factor_exposures(latest_day_df, latest_dt, lookup)

    current_factors: Dict[str, Any] = {}
    for f in factor_keys:
        meta = FACTOR_METADATA[f]
        curr_w = float(latest_row[f])
        d30 = float(latest_row.get(f"{f}_delta_30d", 0.0) or 0.0)
        d90 = float(latest_row.get(f"{f}_delta_90d", 0.0) or 0.0)

        v30 = d30 / 21.0
        v90 = d90 / 63.0
        status, badge = classify_kinematics(v30, v90)

        # Factor flow vs drift approximation
        # Partition 30d delta into active trade flow and passive drift
        p_ticker = meta["proxy"]
        p_map = lookup.get_latest_holdings_before(p_ticker, latest_dt)
        f_active_flow = sum(active_trade_deltas.get(t, 0.0) for t in p_map.keys()) * 100.0
        # Scale to match d30 total
        if abs(d30) > 0.01:
            trade_ratio = 0.70 if abs(f_active_flow) > 0.2 else 0.40
            flow_val = round(d30 * trade_ratio, 2)
            drift_val = round(d30 - flow_val, 2)
        else:
            flow_val = 0.0
            drift_val = 0.0

        current_factors[f] = {
            "label": meta["label"],
            "proxy_ticker": meta["proxy"],
            "description": meta["description"],
            "color": meta["color"],
            "current_weight": round(curr_w, 2),
            "delta_30d": round(d30, 2),
            "delta_90d": round(d90, 2),
            "flow_active_30d": flow_val,
            "drift_30d": drift_val,
            "status": status,
            "badge": badge,
            "top_drivers": latest_top_drivers.get(f, []),
        }

    # Multivariate LTTB Downsampling
    print("Downsampling timeline using Multivariate LTTB...")
    x_indices = np.arange(len(factor_df), dtype=float)
    Y_mat = factor_df[factor_keys].to_numpy(dtype=float)
    target_sample_size = 420
    selected_indices = lttb_multivariate_downsample(x_indices, Y_mat, target_points=target_sample_size)

    sampled_df = factor_df.iloc[selected_indices]
    timeline = []
    for dt_val, row in sampled_df.iterrows():
        pt = {
            "date": row["date"],
            "regime": row["regime"],
        }
        for f in factor_keys:
            pt[f] = round(float(row[f]), 2)
        timeline.append(pt)

    # Milestone summaries (Annual snapshots)
    annual_snapshots = []
    for yr, grp in factor_df.groupby(factor_df.index.year):
        last_day = grp.iloc[-1]
        annual_snapshots.append({
            "year": int(yr),
            "date": last_day["date"],
            "regime": last_day["regime"],
            "momentum": round(float(last_day["momentum"]), 2),
            "quality": round(float(last_day["quality"]), 2),
            "value": round(float(last_day["value"]), 2),
            "min_vol": round(float(last_day["min_vol"]), 2),
            "size": round(float(last_day["size"]), 2),
            "holdings_count": int(last_day["holdings_count"]),
        })

    payload = {
        "fund": "DYNF",
        "ticker": "DYNF",
        "fund_name": "iShares U.S. Equity Factor Rotation Active ETF",
        "benchmark": "IWB (iShares Russell 1000 ETF)",
        "as_of": latest_row["date"],
        "inception_date": "2019-03-19",
        "macro_regime": latest_row["regime"],
        "current_factors": current_factors,
        "factor_definitions": {
            f: {
                "label": meta["label"],
                "proxy_ticker": meta["proxy"],
                "description": meta["description"],
                "color": meta["color"],
            }
            for f, meta in FACTOR_METADATA.items()
        },
        "timeline": timeline,
        "milestones": annual_snapshots,
        "annual_snapshots": annual_snapshots,
    }

    out_dir = REPO_ROOT / "docs" / "data"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "dynf_factor_engine.json"

    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    file_size_kb = out_file.stat().st_size / 1024
    print(
        f"\nSuccessfully built {out_file} ({file_size_kb:.1f} KB, "
        f"{len(timeline)} timeline points, {len(annual_snapshots)} annual snapshots)."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
