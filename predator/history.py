"""
Temporal analytics — daily leaderboards, score deltas, streaks, changelog.

A "snapshot date" is any Holdings_As_Of value present in the source data. For
each snapshot date D, we compute the leaderboard using all data with
Holdings_As_Of <= D (so each leaderboard uses each ETF's latest available
data as of D). The series of daily leaderboards drives every temporal feature.
"""
from __future__ import annotations
import pandas as pd
from typing import Iterable

from .scoring import Config, compute_leaderboard


def snapshot_dates(history: pd.DataFrame, lookback_days: int) -> list[pd.Timestamp]:
    """Distinct Holdings_As_Of values in the last `lookback_days` days, ascending."""
    s = pd.to_datetime(history["Holdings_As_Of"], errors="coerce").dropna()
    if s.empty:
        return []
    latest = s.max()
    cutoff = latest - pd.Timedelta(days=lookback_days)
    return sorted(s[s >= cutoff].unique().tolist())


def historical_leaderboards(history: pd.DataFrame, cfg: Config) -> dict[pd.Timestamp, pd.DataFrame]:
    """Compute leaderboard at each snapshot date in the lookback window."""
    dates = snapshot_dates(history, cfg.history.leaderboard_lookback_days)
    out: dict[pd.Timestamp, pd.DataFrame] = {}
    for d in dates:
        lb, _ = compute_leaderboard(history, cfg, as_of=d)
        out[d] = lb
    return out


def score_panel(historical: dict[pd.Timestamp, pd.DataFrame]) -> pd.DataFrame:
    """
    Wide-form panel: index = ticker, columns = snapshot date, values = final_score.
    Missing cells are NaN (ticker not on that day's leaderboard).
    """
    frames = []
    for d, lb in historical.items():
        s = lb.set_index("ticker")["final_score"].rename(d)
        frames.append(s)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, axis=1).sort_index(axis=1)


def flag_panel(historical: dict[pd.Timestamp, pd.DataFrame]) -> pd.DataFrame:
    """Wide-form panel of daily flags per ticker."""
    frames = []
    for d, lb in historical.items():
        s = lb.set_index("ticker")["flag"].rename(d)
        frames.append(s)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, axis=1).sort_index(axis=1).fillna("")


def streaks_and_deltas(
    score_pnl: pd.DataFrame,
    flag_pnl: pd.DataFrame,
) -> pd.DataFrame:
    """
    Per-ticker temporal features computed from the panel:
        score_today, score_yesterday, score_delta, score_delta_pct
        score_streak  — consecutive days of same-direction score change (+ up, − down)
        hc_streak     — consecutive trailing days flagged HIGH_CONVICTION
        days_observed — count of days the ticker appears in the panel
        score_percentile — percentile of score_today vs ticker's own history
    """
    if score_pnl.empty:
        return pd.DataFrame()

    score_pnl = score_pnl.sort_index(axis=1)
    flag_pnl = flag_pnl.reindex_like(score_pnl).fillna("")

    today = score_pnl.columns[-1]
    yday = score_pnl.columns[-2] if score_pnl.shape[1] >= 2 else None

    rows = []
    for ticker, scores in score_pnl.iterrows():
        s = scores.dropna()
        if s.empty:
            continue
        score_today = s.iloc[-1] if s.index[-1] == today else float("nan")
        score_yday = s.iloc[-2] if len(s) >= 2 else float("nan")

        # Score streak — walk back from latest, count consecutive same-direction diffs
        score_streak = 0
        if len(s) >= 2:
            diffs = s.diff().dropna().tolist()
            if diffs:
                sign = 1 if diffs[-1] > 0 else (-1 if diffs[-1] < 0 else 0)
                if sign != 0:
                    for d in reversed(diffs):
                        if (d > 0 and sign > 0) or (d < 0 and sign < 0):
                            score_streak += sign
                        else:
                            break

        # HC streak — consecutive trailing days flagged HIGH_CONVICTION
        flags = flag_pnl.loc[ticker].tolist() if ticker in flag_pnl.index else []
        hc_streak = 0
        for f in reversed(flags):
            if f == "HIGH_CONVICTION":
                hc_streak += 1
            else:
                break

        # Score percentile vs ticker's own history (excludes today's value to avoid trivial 100th)
        prior = s.iloc[:-1] if s.index[-1] == today else s
        if len(prior) >= 3 and pd.notna(score_today):
            score_percentile = (prior < score_today).mean()
        else:
            score_percentile = float("nan")

        delta = (score_today - score_yday) if pd.notna(score_today) and pd.notna(score_yday) else float("nan")
        delta_pct = (delta / score_yday) if pd.notna(delta) and pd.notna(score_yday) and score_yday != 0 else float("nan")

        rows.append({
            "ticker": ticker,
            "score_today": round(score_today, 2) if pd.notna(score_today) else None,
            "score_yesterday": round(score_yday, 2) if pd.notna(score_yday) else None,
            "score_delta": round(delta, 2) if pd.notna(delta) else None,
            "score_delta_pct": round(delta_pct, 4) if pd.notna(delta_pct) else None,
            "score_streak": int(score_streak),
            "hc_streak": int(hc_streak),
            "days_observed": int(len(s)),
            "score_percentile": round(score_percentile, 3) if pd.notna(score_percentile) else None,
        })
    return pd.DataFrame(rows)


def changelog(
    historical: dict[pd.Timestamp, pd.DataFrame],
    latest: pd.DataFrame,
    streaks: pd.DataFrame,
    top_n: int = 15,
) -> dict:
    """
    Day-over-day movers.

    Categories:
        entered_hc       — newly flagged HIGH_CONVICTION today vs yesterday
        exited_hc        — flagged HIGH_CONVICTION yesterday, no longer today
        biggest_gainers  — top score_delta_pct
        biggest_losers   — bottom score_delta_pct
        new_entrants     — ticker present today but not in any leaderboard 7d ago
    """
    dates = sorted(historical.keys())
    if len(dates) < 2:
        return {"today": None, "yesterday": None, "entered_hc": [], "exited_hc": [],
                "biggest_gainers": [], "biggest_losers": [], "new_entrants": []}

    today, yday = dates[-1], dates[-2]
    today_lb = historical[today]
    yday_lb = historical[yday]

    today_hc = set(today_lb.loc[today_lb["flag"] == "HIGH_CONVICTION", "ticker"])
    yday_hc = set(yday_lb.loc[yday_lb["flag"] == "HIGH_CONVICTION", "ticker"])
    entered = today_hc - yday_hc
    exited = yday_hc - today_hc

    by_ticker = today_lb.set_index("ticker")
    by_ticker_yday = yday_lb.set_index("ticker")
    streaks_by_ticker = streaks.set_index("ticker") if not streaks.empty else pd.DataFrame()

    def _enrich(ticker: str, prefer_yday: bool = False) -> dict:
        src = by_ticker_yday if prefer_yday and ticker in by_ticker_yday.index else by_ticker
        if ticker not in src.index:
            return {"ticker": ticker, "company": "", "final_score": None, "etf_count": None,
                    "tiers": "", "score_delta": None}
        row = src.loc[ticker]
        # If duplicate ticker in source, .loc returns a DataFrame — take first row
        if isinstance(row, pd.DataFrame):
            row = row.iloc[0]
        delta = None
        if not streaks_by_ticker.empty and ticker in streaks_by_ticker.index:
            d = streaks_by_ticker.loc[ticker]
            if isinstance(d, pd.DataFrame):
                d = d.iloc[0]
            delta = d.get("score_delta")
        return {
            "ticker": ticker,
            "company": row.get("company", ""),
            "final_score": float(row.get("final_score")) if pd.notna(row.get("final_score")) else None,
            "etf_count": int(row.get("etf_count")) if pd.notna(row.get("etf_count")) else None,
            "tiers": row.get("tiers", ""),
            "score_delta": float(delta) if delta is not None and pd.notna(delta) else None,
        }

    entered_records = sorted([_enrich(t) for t in entered],
                              key=lambda r: -(r["final_score"] or 0))[:top_n]
    exited_records = sorted([_enrich(t, prefer_yday=True) for t in exited],
                             key=lambda r: -(r["final_score"] or 0))[:top_n]

    # Gainers / losers by score_delta_pct (filter to non-tiny scores to avoid noise)
    if not streaks.empty:
        movers = streaks.dropna(subset=["score_delta_pct"]).copy()
        movers = movers[movers["score_today"].fillna(0) >= 20]
        movers["enriched"] = movers["ticker"].apply(lambda t: _enrich(t))
        gainers = movers.sort_values("score_delta_pct", ascending=False).head(top_n)
        losers = movers.sort_values("score_delta_pct", ascending=True).head(top_n)

        def _movers_records(df):
            recs = []
            for _, r in df.iterrows():
                rec = dict(r["enriched"])
                rec["score_delta_pct"] = float(r["score_delta_pct"])
                rec["score_streak"] = int(r["score_streak"])
                recs.append(rec)
            return recs

        gainers_recs = _movers_records(gainers)
        losers_recs = _movers_records(losers)
    else:
        gainers_recs, losers_recs = [], []

    # New entrants — appear today, not in any leaderboard 7+ days ago
    week_ago_dates = [d for d in dates if d <= today - pd.Timedelta(days=7)]
    if week_ago_dates:
        old_tickers = set()
        for d in week_ago_dates:
            old_tickers.update(historical[d]["ticker"].tolist())
        new = sorted(set(today_lb["ticker"]) - old_tickers)
        new_records = sorted([_enrich(t) for t in new],
                              key=lambda r: -(r["final_score"] or 0))[:top_n]
    else:
        new_records = []

    return {
        "today": today.strftime("%Y-%m-%d"),
        "yesterday": yday.strftime("%Y-%m-%d"),
        "entered_hc": entered_records,
        "exited_hc": exited_records,
        "biggest_gainers": gainers_recs,
        "biggest_losers": losers_recs,
        "new_entrants": new_records,
    }
