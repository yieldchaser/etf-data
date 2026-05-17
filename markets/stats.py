"""
markets/stats.py — Pure function library for time-series statistics.

No I/O. All functions operate on DataFrames with at least a value column.
"""
from __future__ import annotations
import pandas as pd


def series_stats(df: pd.DataFrame, value_col: str = "Close") -> dict:
    """
    Compute summary statistics for a price / rate time series.

    Parameters
    ----------
    df : DataFrame with at least a 'Date' column and the value_col.
         Rows should be sorted ascending by Date (or will be sorted internally).
    value_col : column containing the price / rate values.

    Returns
    -------
    dict with keys:
        close_today, close_yesterday, day_pct,
        week_pct, month_pct, ytd_pct,
        all_time_low, all_time_high, all_time_low_date, all_time_high_date,
        sessions_observed,
        current_streak  (signed int: positive = up days, negative = down days),
        last_20_up, last_20_down,
        percentile_all_time  (0–1, 1 = highest ever)
    """
    if df is None or df.empty:
        return {}

    df = df.copy()
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.sort_values("Date").reset_index(drop=True)
    vals = df[value_col].dropna()
    if vals.empty:
        return {}

    close_today = float(vals.iloc[-1])
    close_yesterday = float(vals.iloc[-2]) if len(vals) >= 2 else None

    def _pct(current, base):
        if base is None or base == 0:
            return None
        return (current - base) / abs(base)

    # Day, week, month, YTD pct changes
    day_pct = _pct(close_today, close_yesterday) if close_yesterday is not None else None

    today_dt = df["Date"].iloc[-1]

    def _closest_before_days(n_days: int):
        cutoff = today_dt - pd.Timedelta(days=n_days)
        cand = df[df["Date"] <= cutoff]
        if cand.empty:
            return None
        return float(cand[value_col].dropna().iloc[-1])

    week_pct  = _pct(close_today, _closest_before_days(7))
    month_pct = _pct(close_today, _closest_before_days(30))
    ytd_start = df[df["Date"].dt.year == today_dt.year]
    ytd_pct   = _pct(close_today, float(ytd_start[value_col].dropna().iloc[0])) if not ytd_start.empty else None

    # All-time range
    all_time_low  = float(vals.min())
    all_time_high = float(vals.max())
    all_time_low_date  = df.loc[df[value_col] == vals.min(), "Date"].iloc[-1].strftime("%Y-%m-%d")
    all_time_high_date = df.loc[df[value_col] == vals.max(), "Date"].iloc[-1].strftime("%Y-%m-%d")

    sessions_observed = len(vals)

    # Daily returns
    rets = vals.pct_change().dropna()

    # Current streak: walk back from today counting consecutive same-direction days
    def _streak(returns: pd.Series) -> int:
        if returns.empty:
            return 0
        sign = 1 if returns.iloc[-1] > 0 else -1
        count = 0
        for r in reversed(returns.tolist()):
            if (r > 0 and sign == 1) or (r < 0 and sign == -1) or r == 0:
                if r != 0:
                    count += 1
                else:
                    break
            else:
                break
        return count * sign

    current_streak = _streak(rets)

    # Last 20 sessions up/down
    last20 = rets.iloc[-20:] if len(rets) >= 20 else rets
    last_20_up   = int((last20 > 0).sum())
    last_20_down = int((last20 < 0).sum())

    # Percentile of today's close in the all-time distribution
    percentile_all_time = float((vals <= close_today).mean())  # fraction of history below or equal

    return {
        "close_today":          close_today,
        "close_yesterday":      close_yesterday,
        "day_pct":              day_pct,
        "week_pct":             week_pct,
        "month_pct":            month_pct,
        "ytd_pct":              ytd_pct,
        "all_time_low":         all_time_low,
        "all_time_high":        all_time_high,
        "all_time_low_date":    all_time_low_date,
        "all_time_high_date":   all_time_high_date,
        "sessions_observed":    sessions_observed,
        "current_streak":       current_streak,
        "last_20_up":           last_20_up,
        "last_20_down":         last_20_down,
        "percentile_all_time":  percentile_all_time,
    }


def log_rows(df: pd.DataFrame, value_col: str = "Close", n: int = 200) -> list[dict]:
    """
    Build a row list for the PRICE LOG widget (last n sessions).

    Each row: {d, close, pct, streak, level}
      - d      : date string YYYY-MM-DD
      - close  : close price (float, rounded to 4 dp)
      - pct    : day-over-day change % (float, rounded to 4 dp)
      - streak : running streak at that date (signed int)
      - level  : percentile of close in the all-time distribution (0–1)
    """
    if df is None or df.empty:
        return []

    df = df.copy()
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.sort_values("Date").reset_index(drop=True)
    df["_ret"] = df[value_col].pct_change()

    all_vals = df[value_col].dropna().tolist()
    n_total = len(all_vals)

    rows = []
    streak = 0
    for i, row in df.iterrows():
        close_val = row[value_col]
        if pd.isna(close_val):
            continue
        ret = row["_ret"]
        if pd.isna(ret):
            day_pct = None
            streak = 0
        else:
            day_pct = round(float(ret), 6)
            if ret > 0:
                streak = streak + 1 if streak >= 0 else 1
            elif ret < 0:
                streak = streak - 1 if streak <= 0 else -1
            # else 0 return: streak resets to 0 (flat day)
            else:
                streak = 0

        # Percentile: fraction of all-time values <= today's close
        below = sum(1 for v in all_vals if v <= close_val)
        level = round(below / n_total, 4) if n_total > 0 else None

        rows.append({
            "d":      row["Date"].strftime("%Y-%m-%d"),
            "close":  round(float(close_val), 4),
            "pct":    day_pct,
            "streak": int(streak),
            "level":  level,
        })

    # Return last n rows (most recent)
    return rows[-n:]
