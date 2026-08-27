"""
Temporal analytics — daily leaderboards, score deltas, streaks, changelog.

A "snapshot date" is any Holdings_As_Of value present in the source data. For
each snapshot date D, we compute the leaderboard using all data with
Holdings_As_Of <= D (so each leaderboard uses each ETF's latest available
data as of D). The series of daily leaderboards drives every temporal feature.
"""
from __future__ import annotations
import numpy as np
import pandas as pd
from typing import Iterable

from .scoring import Config, compute_leaderboard


def snapshot_dates(history: pd.DataFrame, lookback_days: int) -> list[pd.Timestamp]:
    """Distinct Holdings_As_Of values in the last `lookback_days` days, ascending."""
    s = pd.to_datetime(history["Holdings_As_Of"], errors="coerce").dropna()
    if s.empty:
        return []
    # Normalize to tz-naive so sorted() and comparisons never raise TypeError when
    # the source data contains ISO-8601 strings with timezone offsets.
    if getattr(s.dt, "tz", None) is not None:
        s = s.dt.tz_convert(None)
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
        score_yday = (s.loc[yday] if yday in s.index else float("nan")) if yday is not None else float("nan")

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
            score_percentile = float(((prior < score_today).sum() + 0.5 * (prior == score_today).sum()) / len(prior))
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


def accumulation_velocity(score_pnl: pd.DataFrame, window: int = 10) -> pd.DataFrame:
    """
    §31 OLS accumulation velocity over the trailing `window` snapshots.

    For each ticker with ≥ 4 non-NaN points in the window:
        x_i       = (date_i − first window date).days   (actual date spacing)
        slope     = cov(x, y) / var(x)                  (OLS on final_score)
        slope_pct = slope / max(|mean(y)|, 1)           (scale-free)

    Ignition (acceleration): split the ticker's non-NaN window points into a
    first half and second half (each needs ≥ 3 points, else NaN);
        ignition_raw = slope(second half) − slope(first half),
    normalized by max(|mean(y)|, 1).

    Both metrics are z-scored cross-sectionally over tickers with valid
    values (std == 0 or < 2 valid → z = 0; invalid tickers stay NaN).

    Returns columns: ticker, slope, slope_pct, velocity_z, ignition_z.
    """
    cols_out = ["ticker", "slope", "slope_pct", "velocity_z", "ignition_z"]
    if score_pnl is None or score_pnl.empty:
        return pd.DataFrame(columns=cols_out)

    pnl = score_pnl.sort_index(axis=1).iloc[:, -window:]
    first_col = pd.Timestamp(pnl.columns[0])

    def _ols_slope(x: np.ndarray, y: np.ndarray) -> float:
        """OLS slope = cov(x, y) / var(x); NaN when var(x) is 0."""
        vx = x - x.mean()
        denom = (vx ** 2).sum()
        if denom <= 0:
            return float("nan")
        return float((vx * (y - y.mean())).sum() / denom)

    rows = []
    for ticker, series in pnl.iterrows():
        s = series.dropna()
        if len(s) < 4:
            rows.append({"ticker": ticker, "slope": float("nan"),
                         "slope_pct": float("nan"), "ignition_raw": float("nan")})
            continue
        x = np.array([(pd.Timestamp(d) - first_col).days for d in s.index], dtype=float)
        y = s.to_numpy(dtype=float)
        slope = _ols_slope(x, y)
        denom_y = max(abs(float(y.mean())), 1.0)
        slope_pct = slope / denom_y if pd.notna(slope) else float("nan")

        # Ignition — acceleration between window halves (each needs ≥ 3 points)
        half = len(s) // 2
        if half >= 3 and (len(s) - half) >= 3:
            s1 = _ols_slope(x[:half], y[:half])
            s2 = _ols_slope(x[half:], y[half:])
            ignition_raw = ((s2 - s1) / denom_y
                            if pd.notna(s1) and pd.notna(s2) else float("nan"))
        else:
            ignition_raw = float("nan")
        rows.append({"ticker": ticker, "slope": slope, "slope_pct": slope_pct,
                     "ignition_raw": ignition_raw})

    out = pd.DataFrame(rows)

    def _zscore(col: pd.Series) -> pd.Series:
        """Cross-sectional z-score; std == 0 or < 2 valid → 0 (NaN stays NaN)."""
        valid = col.dropna()
        if len(valid) < 2:
            return pd.Series(0.0, index=col.index).where(col.notna())
        std = valid.std()
        if pd.isna(std) or std == 0:
            return pd.Series(0.0, index=col.index).where(col.notna())
        return (col - valid.mean()) / std

    out["velocity_z"] = _zscore(out["slope_pct"])
    out["ignition_z"] = _zscore(out["ignition_raw"])
    return out[cols_out]


def predictive_overlay(leaderboard: pd.DataFrame, score_pnl: pd.DataFrame,
                       *, v_alpha: float = 0.25, i_alpha: float = 0.10,
                       window: int = 10,
                       conv_floor: float = 0.50, conv_cap: float = 1.25,
                       conv_gamma: float = 1.0) -> pd.DataFrame:
    """
    §31 bounded temporal kicker on top of the leaderboard.

        apex_score = final_score × (1 + v_alpha·tanh(velocity_z / 2)
                                      + i_alpha·tanh(ignition_z / 2))
                                 × m_conv

    where m_conv = clip(avg_conviction, conv_floor, conv_cap) ** conv_gamma.

    Breadth-summed score lets multi-fund filler outrank single-fund conviction;
    m_conv discounts underweight breadth (avg_conviction < 1) and rewards
    overweight conviction (avg_conviction > 1), capped at conv_cap ** conv_gamma.
    Missing/None avg_conviction → m_conv = 1.0 (neutral).

    tanh bounds the kicker to ±(v_alpha + i_alpha), so momentum can re-order
    neighbours but never fabricate conviction — risk-averse by construction.
    apex_score is floored at 0 and cast int64; apex_rank is 1..n ordered by
    (apex_score desc, etf_count desc, total_weight desc).

    Empty/insufficient panel → apex_score = final_score,
    apex_rank = leaderboard_rank (neutral columns, stable schema).
    """
    out = leaderboard.copy()
    if out.empty or score_pnl is None or score_pnl.empty:
        out["velocity_z"] = 0.0
        out["ignition_z"] = 0.0
        out["apex_score"] = out.get("final_score", pd.Series(dtype="int64"))
        out["apex_rank"] = out.get("leaderboard_rank", pd.Series(dtype="int64"))
        return out

    vel = accumulation_velocity(score_pnl, window=window).set_index("ticker")
    out["velocity_z"] = out["ticker"].map(vel["velocity_z"]).fillna(0.0).round(3) if not vel.empty else 0.0
    out["ignition_z"] = out["ticker"].map(vel["ignition_z"]).fillna(0.0).round(3) if not vel.empty else 0.0

    kicker = (1.0
              + v_alpha * np.tanh(out["velocity_z"] / 2.0)
              + i_alpha * np.tanh(out["ignition_z"] / 2.0))

    # §31.2 conviction-quality multiplier — breadth-summed score lets multi-fund
    # filler outrank single-fund conviction; m_conv discounts underweight breadth.
    _conv = out["avg_conviction"].fillna(1.0) if "avg_conviction" in out.columns else pd.Series(1.0, index=out.index)
    m_conv = _conv.clip(lower=conv_floor, upper=conv_cap).pow(conv_gamma)

    out["apex_score"] = (out["final_score"] * kicker * m_conv).clip(lower=0.0).astype("int64")

    order = out.sort_values(
        ["apex_score", "etf_count", "total_weight", "ticker"],
        ascending=[False, False, False, True],
    ).index
    out["apex_rank"] = pd.Series(range(1, len(out) + 1), index=order).astype("int64")
    return out


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

    # Universe exits — present yesterday but not today
    exited_universe = set(yday_lb["ticker"]) - set(today_lb["ticker"])

    def _enrich_exit(ticker: str) -> dict:
        if ticker not in by_ticker_yday.index:
            return {"ticker": ticker, "company": "", "final_score": 0.0, "etf_count": 0,
                    "tiers": "", "score_delta": None, "score_delta_pct": -1.0}
        row = by_ticker_yday.loc[ticker]
        if isinstance(row, pd.DataFrame):
            row = row.iloc[0]
        yday_score = float(row.get("final_score")) if pd.notna(row.get("final_score")) else 0.0
        return {
            "ticker": ticker,
            "company": row.get("company", ""),
            "final_score": 0.0,
            "etf_count": 0,
            "tiers": row.get("tiers", ""),
            "score_delta": -yday_score,
            "score_delta_pct": -1.0,
        }

    exited_universe_records = sorted(
        [_enrich_exit(t) for t in exited_universe],
        key=lambda r: -(abs(r["score_delta"]) if r["score_delta"] is not None else 0)
    )[:top_n]

    return {
        "today": today.strftime("%Y-%m-%d"),
        "yesterday": yday.strftime("%Y-%m-%d"),
        "entered_hc": entered_records,
        "exited_hc": exited_records,
        "biggest_gainers": gainers_recs,
        "biggest_losers": losers_recs,
        "new_entrants": new_records,
        "universe_exits": exited_universe_records,
    }


def compute_universe_exits_by_period(
    historical: dict[pd.Timestamp, pd.DataFrame],
    latest: pd.DataFrame,
    periods: list[int | str],
    top_n: int = 50,
) -> dict[str, list[dict]]:
    """
    Universe exits for every lookback period, computed from the FULL uncapped
    daily leaderboards (not the 600-ticker-capped score_history.json the
    frontend previously scanned).

    A ticker is an "exit" for period N if it had final_score > 0 in the
    snapshot closest to (today − N days) but is absent from today's
    leaderboard (i.e. held by zero ETFs now). "YTD" uses Jan-1 of the
    current year as the start.

    Mirrors the 1-day logic in changelog()'s universe_exits path, but
    generalised across periods so the frontend "Dropped from Universe"
    table is accurate for 7d/14d/30d/60d/90d/YTD — not just 1d.

    Returns {str(period): [{ticker, company, final_score, etf_count, tiers,
                            score_delta, score_delta_pct}, ...]} keyed by the
    stringified period (matching the frontend's String(changesPeriod) reads).
    """
    dates = sorted(historical.keys())
    if len(dates) < 2:
        return {str(p): [] for p in periods}

    today = dates[-1]
    today_lb = historical[today]
    today_tickers: set[str] = set(today_lb["ticker"].tolist())

    # Company / tier lookup from today's leaderboard (enrichment source for the
    # exit record; falls back to "" if the ticker is already gone).
    by_ticker_today = today_lb.set_index("ticker") if "ticker" in today_lb.columns else pd.DataFrame()

    def _enrich_exit(ticker: str, start_lb: pd.DataFrame, start_idx) -> dict:
        # Pull the prior score/company from the START snapshot (the day the
        # ticker was last seen) so the "Prev. Score" column reflects what it
        # was before dropping out. NB: `in` on a DataFrame checks COLUMNS, so
        # test membership against the index explicitly.
        if "ticker" not in start_lb.columns or ticker not in start_idx.index:
            return {"ticker": ticker, "company": "", "final_score": 0.0,
                    "etf_count": 0, "tiers": "", "score_delta": None,
                    "score_delta_pct": -1.0}
        row = start_idx.loc[ticker]
        if isinstance(row, pd.DataFrame):
            row = row.iloc[0]
        prior_score = float(row.get("final_score")) if pd.notna(row.get("final_score")) else 0.0
        return {
            "ticker": ticker,
            "company": row.get("company", ""),
            "final_score": 0.0,
            "etf_count": int(row.get("etf_count")) if pd.notna(row.get("etf_count")) else 0,
            "tiers": row.get("tiers", ""),
            "score_delta": -prior_score,
            "score_delta_pct": -1.0,
        }

    out: dict[str, list[dict]] = {}
    for p in periods:
        # Resolve the start snapshot for this period.
        if p == "YTD":
            start_target = pd.Timestamp(year=today.year, month=1, day=1)
        else:
            try:
                n_days = int(p)
            except (ValueError, TypeError):
                out[str(p)] = []
                continue
            start_target = today - pd.Timedelta(days=n_days)

        # Snapshot closest to the target date (must be strictly before today so
        # 1-day doesn't collapse to today itself).
        candidates = [d for d in dates if d < today]
        if not candidates:
            out[str(p)] = []
            continue
        start_d = min(candidates, key=lambda d: abs((d - start_target).total_seconds()))

        start_lb = historical[start_d]
        if "ticker" not in start_lb.columns or "final_score" not in start_lb.columns:
            out[str(p)] = []
            continue

        # Held-and-scored at the start date, gone from today's universe.
        scored_at_start = start_lb.loc[start_lb["final_score"].fillna(0) > 0, "ticker"]
        start_tickers: set[str] = set(scored_at_start.tolist())
        exited = start_tickers - today_tickers

        start_idx = start_lb.set_index("ticker")
        records = sorted(
            (_enrich_exit(t, start_lb, start_idx) for t in exited),
            # Biggest absolute score drop first (most material exits on top)
            key=lambda r: -(abs(r["score_delta"]) if r["score_delta"] is not None else 0),
        )[:top_n]
        out[str(p)] = records

    return out


def _quality_etfs_from_config(cfg: Config) -> frozenset[str]:
    """Quality-tier ETF tickers straight from config.yaml (single source of
    truth). The old hardcoded {COWZ, CALF, SPHQ} silently ignored five
    Quality-tier funds the config added later."""
    return frozenset(
        t for t, e_info in cfg.etf_lookup().items() if getattr(e_info, "tier", None) == "Quality"
    )


def signal_history(
    raw: pd.DataFrame,
    historical: dict[pd.Timestamp, pd.DataFrame],
    cfg: Config,
) -> dict[str, list[dict]]:
    """
    Per-ticker, per-date signal records.  Called from build.py to populate the
    full flag_history payload.

    For each snapshot date d (sorted ascending), computes:
        n      any_new (omit when False)
        qa     quality_adopted (omit when False)
        burst  burst_30d (omit when False)
        crater crater_30d (omit when False)

    Retired signals (2026-08 signal-evaluation study — no forward predictive
    lift / unstable across walk-forward grids): the velocity composite (vs),
    stealth_accumulation (st), conviction_divergence (dv), quality_defected
    (qd). Their historical entries simply stop appearing; old entries in
    existing payloads remain readable.

    Returns dict[ticker -> list[{d, flag, rank, n?, qa?, burst?, crater?}]].

    Keys kept short (1-2 chars) to minimise JSON payload.
    Performance: all cross-date panel operations are vectorized (numpy/pandas).
    Only the outer loop (~99 dates) is Python-level.
    """
    if not historical:
        return {}

    import numpy as np

    quality_etfs = _quality_etfs_from_config(cfg)

    # ── 1. Build wide panels (ticker × date) ──────────────────────────────────
    dates_sorted: list[pd.Timestamp] = sorted(historical.keys())

    def _build_panel(col: str) -> pd.DataFrame:
        frames = []
        for d in dates_sorted:
            snap = historical[d]
            if col in snap.columns:
                frames.append(snap.set_index("ticker")[col].rename(d))
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, axis=1).sort_index(axis=1)

    rank_panel = _build_panel("leaderboard_rank")   # ticker × date, float (NaN for missing)
    hb_panel   = _build_panel("held_by")            # ticker × date, str

    rp_cols = list(rank_panel.columns) if not rank_panel.empty else []

    # ── 2. Pre-compute burst/crater panels (ticker × date_idx → bool) ─────────
    # Do this once for all date indices to avoid repeating the window operation.
    burst_sets: list[set] = []  # burst_sets[di] = set of burst tickers at date di
    crater_sets: list[set] = []  # crater_sets[di] = set of crater tickers at date di
    if not rank_panel.empty and len(rp_cols) >= 5:
        rp_arr = rank_panel.values.astype(float)   # shape (n_tickers_rp, n_dates)
        rp_tickers = list(rank_panel.index)
        for di in range(len(rp_cols)):
            if di < 5:
                burst_sets.append(set())
                crater_sets.append(set())
                continue
            win_start = max(0, di - 30)
            win = rp_arr[:, win_start: di + 1]  # shape (n_tickers_rp, win_len)
            n_win = win.shape[1]
            valid_mask = np.isfinite(win)
            coverage = valid_mask.sum(axis=1) / n_win
            # worst/best ignoring NaN
            worst = np.where(valid_mask, win, -np.inf).max(axis=1)
            best  = np.where(valid_mask, win,  np.inf).min(axis=1)
            peak  = np.where(np.isfinite(worst) & np.isfinite(best), worst - best, 0.0)
            # Sustained: better than window median for ≥8 of last 10 valid snapshots
            recent10 = win[:, -10:] if n_win >= 10 else win
            with_nan = np.where(valid_mask[:, -10:] if n_win >= 10 else valid_mask, win[:, -min(10, n_win):], np.nan)
            med = np.nanmedian(rp_arr[:, win_start: di + 1], axis=1)
            better = (with_nan < med[:, None])
            better_count = np.nansum(better, axis=1)
            # Coverage gate mirrors build.py's _attach_velocity: newer names
            # (actual_obs < 15) get a pass since they can't reach 80% of a
            # 30-day window; sustained_count ≥ 8 still guards against noise.
            actual_obs = valid_mask.sum(axis=1)
            coverage_ok = (coverage >= 0.80) | (actual_obs < 15)
            is_burst_arr = (peak >= 40) & coverage_ok & (better_count >= 8)
            burst_sets.append(set(t for t, b in zip(rp_tickers, is_burst_arr) if b))

            # Crater: exact polar opposite — sustained WORSE than median
            worse = (with_nan > med[:, None])
            worse_count = np.nansum(worse, axis=1)
            is_crater_arr = (peak >= 40) & coverage_ok & (worse_count >= 8)
            crater_sets.append(set(t for t, b in zip(rp_tickers, is_crater_arr) if b))
    else:
        burst_sets = [set()] * len(dates_sorted)
        crater_sets = [set()] * len(dates_sorted)

    # ── 5. Pre-compute per-date quality sets from hb_panel ────────────────────
    # For each (ticker, date_idx): now_q and then_q sets
    # Precompute as frozensets: hb_q_panel[ticker][di] = frozenset of quality ETFs held
    hb_q: dict[str, list[frozenset]] = {}  # ticker -> list[frozenset] len=n_dates
    if not hb_panel.empty:
        for tkr in hb_panel.index:
            row_hb = hb_panel.loc[tkr]
            q_list: list[frozenset] = []
            for val in row_hb:
                if isinstance(val, str) and val:
                    q_set = frozenset(t.strip() for t in val.split(",") if t.strip()) & quality_etfs
                else:
                    q_set = frozenset()
                q_list.append(q_set)
            hb_q[tkr] = q_list

    # ── 6. Per-date signal computation — outer loop only ─────────────────────
    result: dict[str, list[dict]] = {}

    for di, d in enumerate(dates_sorted):
        snap = historical[d]
        d_str = d.strftime("%Y-%m-%d")

        # Quality ~30d ago index
        q_past_di: int | None = None
        if di > 0:
            target_past = d - pd.Timedelta(days=30)
            q_past_di = min(range(di), key=lambda k: abs((dates_sorted[k] - target_past).total_seconds()))

        # Burst set for this date
        b_set = burst_sets[di] if di < len(burst_sets) else set()
        c_set = crater_sets[di] if di < len(crater_sets) else set()

        # Iterate tickers in this snapshot
        snap_tickers   = snap["ticker"].tolist()
        snap_flags     = snap["flag"].tolist() if "flag" in snap.columns else [""] * len(snap_tickers)
        snap_ranks     = snap["leaderboard_rank"].tolist() if "leaderboard_rank" in snap.columns else [0] * len(snap_tickers)
        snap_any_new   = snap["any_new"].tolist() if "any_new" in snap.columns else [False] * len(snap_tickers)
        snap_etf_count = snap["etf_count"].tolist() if "etf_count" in snap.columns else [0] * len(snap_tickers)

        for j, tkr in enumerate(snap_tickers):
            flag      = str(snap_flags[j]) if pd.notna(snap_flags[j]) and snap_flags[j] else ""
            rank_val  = int(snap_ranks[j]) if snap_ranks[j] == snap_ranks[j] else 0
            any_new   = bool(snap_any_new[j])
            etf_count = int(snap_etf_count[j]) if snap_etf_count[j] == snap_etf_count[j] else 0

            entry: dict = {"d": d_str, "flag": flag, "rank": rank_val}

            if any_new:
                entry["n"] = 1

            # qa — quality adoption (Quality ETF cosign vs ~30d ago)
            if q_past_di is not None and tkr in hb_q:
                q_list = hb_q[tkr]
                now_q  = q_list[di]         if di < len(q_list)         else frozenset()
                then_q = q_list[q_past_di]  if q_past_di < len(q_list)  else frozenset()
                if now_q - then_q:
                    entry["qa"] = 1

            # burst
            if tkr in b_set:
                entry["burst"] = 1

            # crater
            if tkr in c_set:
                entry["crater"] = 1

            result.setdefault(tkr, []).append(entry)

    return result


def compute_exits(
    history: pd.DataFrame,
    cfg: Config,
    lookback_days: int,
) -> pd.DataFrame:
    """
    For each (ETF, ticker), find holdings that were present ~lookback_days ago but are NOT present now.

    Gracefully handles ETFs with history shorter than lookback_days by omitting them.
    Returns a DataFrame with columns: ETF_Ticker, ticker, name, rank_then, weight_then.
    """
    etf_lookup = cfg.etf_lookup()
    df = history.copy()
    df["Holdings_As_Of"] = pd.to_datetime(df["Holdings_As_Of"], errors="coerce")
    df = df.dropna(subset=["Holdings_As_Of", "ticker", "weight"])
    df = df[df["weight"] > 0]
    df = df[df["ETF_Ticker"].isin(etf_lookup)]
    df = cfg.sanitizer.apply(df)
    if df.empty:
        return pd.DataFrame(columns=["ETF_Ticker", "ticker", "name", "rank_then", "weight_then"])

    latest_date_per_etf = df.groupby("ETF_Ticker")["Holdings_As_Of"].max()
    latest_date_per_etf.index.name = "ETF_Ticker"

    def _ranked_at(date_per_etf: pd.Series) -> pd.DataFrame:
        d = df.merge(date_per_etf.rename("target_date").reset_index(), on="ETF_Ticker")
        d = d[d["Holdings_As_Of"] == d["target_date"]].copy()
        d = d.sort_values(["ETF_Ticker", "weight", "ticker"], ascending=[True, False, True])
        d["rank"] = d.groupby("ETF_Ticker").cumcount() + 1
        return d[["ETF_Ticker", "ticker", "name", "rank", "weight"]]

    now_df = _ranked_at(latest_date_per_etf)

    # Past: closest snapshot <= (latest - lookback) per ETF
    target_dates = latest_date_per_etf - pd.Timedelta(days=lookback_days)
    snapshot_dates = df.groupby("ETF_Ticker")["Holdings_As_Of"].unique()
    chosen = {}
    for etf, dates in snapshot_dates.items():
        target = target_dates[etf]
        dates_le = [d for d in dates if d <= target]
        if dates_le:
            chosen[etf] = max(dates_le)

    if not chosen:
        return pd.DataFrame(columns=["ETF_Ticker", "ticker", "name", "rank_then", "weight_then"])

    chosen_series = pd.Series(chosen, name="target_date")
    chosen_series.index.name = "ETF_Ticker"
    then_df = _ranked_at(chosen_series)

    # Find exits: in then_df but NOT in now_df
    merged = then_df.merge(now_df[["ETF_Ticker", "ticker"]], on=["ETF_Ticker", "ticker"], how="left", indicator=True)
    exits = merged[merged["_merge"] == "left_only"].copy()

    exits = exits.rename(columns={"rank": "rank_then", "weight": "weight_then"})
    return exits[["ETF_Ticker", "ticker", "name", "rank_then", "weight_then"]]
