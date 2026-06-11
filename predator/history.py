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
        ["apex_score", "etf_count", "total_weight"],
        ascending=[False, False, False],
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

    return {
        "today": today.strftime("%Y-%m-%d"),
        "yesterday": yday.strftime("%Y-%m-%d"),
        "entered_hc": entered_records,
        "exited_hc": exited_records,
        "biggest_gainers": gainers_recs,
        "biggest_losers": losers_recs,
        "new_entrants": new_records,
    }


QUALITY_ETFS: frozenset[str] = frozenset({"COWZ", "CALF", "SPHQ"})


def signal_history(
    raw: pd.DataFrame,
    historical: dict[pd.Timestamp, pd.DataFrame],
    cfg: Config,
) -> dict[str, list[dict]]:
    """
    Per-ticker, per-date signal records.  Called from build.py to populate the
    full flag_history payload.

    For each snapshot date d (sorted ascending), computes:
        n    any_new (omit when False)
        st   stealth_accumulation (omit when False)
        dv   conviction_divergence +1/-1 (omit when 0)
        qa   quality_adopted (omit when False)
        qd   quality_defected (omit when False)
        vs   velocity_score rounded 1dp (omit when 0)
        burst burst_30d (omit when False)

    Returns dict[ticker -> list[{d, flag, rank, n?, st?, dv?, qa?, qd?, vs?, burst?}]].

    Keys kept short (1-2 chars) to minimise JSON payload.
    Performance: all cross-date panel operations are vectorized (numpy/pandas).
    Only the outer loop (~99 dates) is Python-level.
    """
    if not historical:
        return {}

    import bisect
    import numpy as np

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
    sc_pnl     = _build_panel("final_score")        # ticker × date
    ec_panel   = _build_panel("etf_count")          # ticker × date
    hb_panel   = _build_panel("held_by")            # ticker × date, str

    rp_cols = list(rank_panel.columns) if not rank_panel.empty else []
    sc_cols = list(sc_pnl.columns)     if not sc_pnl.empty     else []
    ec_cols = list(ec_panel.columns)   if not ec_panel.empty   else []

    # ── 2. Build per-(ETF,ticker) rank/weight panels from raw ────────────────
    # Vectorized: sort once, assign rank via cumcount, pivot to wide panels.
    raw2 = raw.copy()
    raw2["Holdings_As_Of"] = pd.to_datetime(raw2["Holdings_As_Of"], errors="coerce")
    etf_lookup = cfg.etf_lookup()
    raw2 = raw2.dropna(subset=["Holdings_As_Of", "ticker", "weight"])
    raw2 = raw2[raw2["weight"] > 0]
    raw2 = raw2[raw2["ETF_Ticker"].isin(etf_lookup)]
    raw2 = cfg.sanitizer.apply(raw2)

    raw_dates_sorted: list[pd.Timestamp] = sorted(raw2["Holdings_As_Of"].unique().tolist())

    # Rank within each (ETF, date) group once — vectorized
    raw2 = raw2.sort_values(
        ["Holdings_As_Of", "ETF_Ticker", "weight", "ticker"],
        ascending=[True, True, False, True]
    )
    raw2["_rank"] = raw2.groupby(["Holdings_As_Of", "ETF_Ticker"]).cumcount() + 1

    # Build ticker-level panels: avg_rank and avg_weight per (ticker, raw_date)
    # avg across ETFs that hold the ticker on each date
    # Shape: ticker × raw_date (float, NaN when ticker not held)
    grp = raw2.groupby(["ticker", "Holdings_As_Of"])
    avg_rank_raw = grp["_rank"].mean().unstack("Holdings_As_Of")   # ticker × raw_date
    avg_wt_raw   = grp["weight"].mean().unstack("Holdings_As_Of")  # ticker × raw_date
    avg_rank_raw = avg_rank_raw.reindex(columns=sorted(avg_rank_raw.columns))
    avg_wt_raw   = avg_wt_raw.reindex(columns=sorted(avg_wt_raw.columns))

    raw_date_list: list[pd.Timestamp] = list(avg_rank_raw.columns)  # sorted timestamps
    rk_arr = avg_rank_raw.values    # shape (n_tickers_raw, n_raw_dates)
    wt_arr = avg_wt_raw.values
    tkr_idx_raw = {t: i for i, t in enumerate(avg_rank_raw.index)}

    # ── 3. Pre-compute score_streak panel (ticker × date_idx → int) ──────────
    # For each ticker and each column index, count consecutive same-direction moves.
    # Vectorized across tickers using numpy.
    streak_arr: np.ndarray | None = None
    streak_tickers: list = []
    if not sc_pnl.empty:
        sc_vals = sc_pnl.values.astype(float)   # shape (n_tickers, n_dates)
        n_tickers_sc, n_sc_dates = sc_vals.shape
        streak_arr = np.zeros((n_tickers_sc, n_sc_dates), dtype=np.int32)
        streak_tickers = list(sc_pnl.index)
        # diffs: shape (n_tickers, n_dates-1); NaN where either end is NaN
        diffs = np.diff(sc_vals, axis=1)
        signs = np.sign(diffs)  # +1, 0, -1; NaN stays NaN
        for i in range(1, n_sc_dates):
            col_diff = diffs[:, i - 1] if i <= diffs.shape[1] else np.zeros(n_tickers_sc)
            col_sign = signs[:, i - 1] if i <= signs.shape[1] else np.zeros(n_tickers_sc)
            # For tickers with a valid same-direction step, streak continues
            # Reset per ticker: streak[t, i] = streak[t, i-1] + sign if sign != 0 and same direction
            prev = streak_arr[:, i - 1]
            same_dir = (col_sign != 0) & (np.sign(prev) == col_sign)
            new_streak = np.where(same_dir, prev + col_sign.astype(np.int32), col_sign.astype(np.int32))
            # Where diff is NaN (either end is NaN), streak = 0
            nan_mask = ~np.isfinite(col_diff)
            streak_arr[:, i] = np.where(nan_mask, 0, new_streak).astype(np.int32)
        streak_ticker_map = {t: i for i, t in enumerate(streak_tickers)}
    else:
        streak_ticker_map = {}

    # ── 4. Pre-compute burst panel (ticker × date_idx → bool) ────────────────
    # Do this once for all date indices to avoid repeating the window operation.
    burst_sets: list[set] = []  # burst_sets[di] = set of burst tickers at date di
    if not rank_panel.empty and len(rp_cols) >= 5:
        rp_arr = rank_panel.values.astype(float)   # shape (n_tickers_rp, n_dates)
        rp_tickers = list(rank_panel.index)
        for di in range(len(rp_cols)):
            if di < 5:
                burst_sets.append(set())
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
            is_burst_arr = (peak >= 40) & (coverage >= 0.80) & (better_count >= 8)
            burst_sets.append(set(t for t, b in zip(rp_tickers, is_burst_arr) if b))
    else:
        burst_sets = [set()] * len(dates_sorted)

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
                    q_set = frozenset(t.strip() for t in val.split(",") if t.strip()) & QUALITY_ETFS
                else:
                    q_set = frozenset()
                q_list.append(q_set)
            hb_q[tkr] = q_list

    # ── 6. Per-date signal computation — outer loop only ─────────────────────
    result: dict[str, list[dict]] = {}

    for di, d in enumerate(dates_sorted):
        snap = historical[d]
        d_str = d.strftime("%Y-%m-%d")

        # Find nearest raw date ≤ d for avg_rank/avg_wt "now"
        # raw_date_list is sorted; use bisect to find the insertion point
        raw_now_idx: int | None = None
        raw_prev7_idx: int | None = None
        if raw_date_list:
            pos = bisect.bisect_right(raw_date_list, d) - 1
            if pos >= 0:
                raw_now_idx = pos
                prev7_i = raw_now_idx - 7
                if prev7_i >= 0:
                    raw_prev7_idx = prev7_i

        # Map leaderboard date index di to score_panel column index (they align since
        # sc_pnl is built from the same historical dict, same dates).
        # sc_cols is sorted and sc_pnl.columns = sorted historical keys that have final_score.
        # Use bisect to find d in sc_cols.
        sc_di = bisect.bisect_left(sc_cols, d) if sc_cols else -1
        sc_di = sc_di if (sc_di < len(sc_cols) and sc_cols[sc_di] == d) else -1

        # Score delta pct (7 leaderboard snapshots back) — vectorized
        sdp_series: pd.Series | None = None
        sc_7_idx_abs = sc_di - 7
        if sc_di >= 0 and sc_7_idx_abs >= 0:
            cur_col  = sc_pnl[sc_cols[sc_di]]
            prev_col = sc_pnl[sc_cols[sc_7_idx_abs]]
            valid = cur_col.notna() & prev_col.notna() & (prev_col != 0)
            delta = ((cur_col - prev_col) / prev_col.abs()).clip(-10, 10)
            sdp_series = delta.where(valid)

        # Global rank delta ~30 snapshots back — vectorized
        grd30_series: pd.Series | None = None
        rank_30d_idx = di - 30
        if rp_cols and di < len(rp_cols) and rank_30d_idx >= 0 and rank_30d_idx < len(rp_cols):
            cur_r  = rank_panel[rp_cols[di]]
            prev_r = rank_panel[rp_cols[rank_30d_idx]]
            valid_r = cur_r.notna() & prev_r.notna()
            grd30_series = (prev_r - cur_r).where(valid_r)

        # Peak rank improvement in 30-snapshot window (for vs) — vectorized
        peak30_series: pd.Series | None = None
        if rp_cols and di >= 5:
            win_start = max(0, di - 30)
            win_cols_v = rp_cols[win_start: di + 1]
            if len(win_cols_v) >= 2:
                win_v = rank_panel[win_cols_v]
                peak30_series = (win_v.max(axis=1) - win_v.min(axis=1)).fillna(0.0)

        # etf_count delta ~30 snapshots back — vectorized
        ec_delta_series: pd.Series | None = None
        if ec_cols and di < len(ec_cols) and rank_30d_idx >= 0 and rank_30d_idx < len(ec_cols):
            cur_ec  = ec_panel[ec_cols[di]]
            prev_ec = ec_panel[ec_cols[rank_30d_idx]]
            ec_delta_series = (cur_ec - prev_ec).fillna(0.0)

        # Quality ~30d ago index
        q_past_di: int | None = None
        if di > 0:
            target_past = d - pd.Timedelta(days=30)
            q_past_di = min(range(di), key=lambda k: abs((dates_sorted[k] - target_past).total_seconds()))

        # Burst set for this date
        b_set = burst_sets[di] if di < len(burst_sets) else set()

        # Iterate tickers in this snapshot
        snap_tickers   = snap["ticker"].tolist()
        snap_flags     = snap["flag"].tolist() if "flag" in snap.columns else [""] * len(snap_tickers)
        snap_ranks     = snap["leaderboard_rank"].tolist() if "leaderboard_rank" in snap.columns else [0] * len(snap_tickers)
        snap_any_new   = snap["any_new"].tolist() if "any_new" in snap.columns else [False] * len(snap_tickers)
        snap_etf_count = snap["etf_count"].tolist() if "etf_count" in snap.columns else [0] * len(snap_tickers)

        for j, tkr in enumerate(snap_tickers):
            flag      = snap_flags[j] if snap_flags[j] else ""
            rank_val  = int(snap_ranks[j]) if snap_ranks[j] == snap_ranks[j] else 0
            any_new   = bool(snap_any_new[j])
            etf_count = int(snap_etf_count[j]) if snap_etf_count[j] == snap_etf_count[j] else 0

            entry: dict = {"d": d_str, "flag": flag, "rank": rank_val}

            if any_new:
                entry["n"] = 1

            # avg rank delta and weight flow over 7 raw snapshots
            rd7_tkr = 0.0
            wf7_tkr = 0.0
            if raw_now_idx is not None and raw_prev7_idx is not None and tkr in tkr_idx_raw:
                ti = tkr_idx_raw[tkr]
                r_now  = rk_arr[ti, raw_now_idx]
                r_then = rk_arr[ti, raw_prev7_idx]
                w_now  = wt_arr[ti, raw_now_idx]
                w_then = wt_arr[ti, raw_prev7_idx]
                if np.isfinite(r_now) and np.isfinite(r_then):
                    rd7_tkr = float(r_then - r_now)   # positive = improved
                if np.isfinite(w_now) and np.isfinite(w_then):
                    wf7_tkr = float(w_now - w_then)
                elif np.isfinite(w_now):
                    wf7_tkr = float(w_now)

            # st — stealth accumulation
            if wf7_tkr > 0.03 and rd7_tkr < 1.0 and etf_count >= 3:
                entry["st"] = 1

            # dv — conviction divergence
            sdp = float(sdp_series.get(tkr, 0.0) or 0.0) if sdp_series is not None else 0.0
            grd = float(grd30_series.get(tkr, 0.0) or 0.0) if grd30_series is not None else 0.0
            dv = 0
            if sdp > 0 and grd < 0:
                dv = -1
            elif sdp < 0 and grd > 0:
                dv = 1
            if dv != 0:
                entry["dv"] = dv

            # qa / qd — quality adoption / defection
            if q_past_di is not None and tkr in hb_q:
                q_list = hb_q[tkr]
                now_q  = q_list[di]         if di < len(q_list)         else frozenset()
                then_q = q_list[q_past_di]  if q_past_di < len(q_list)  else frozenset()
                if now_q - then_q:
                    entry["qa"] = 1
                if then_q - now_q:
                    entry["qd"] = 1

            # vs — velocity_score
            grd30  = grd
            peak30 = float(peak30_series.get(tkr, 0.0) or 0.0) if peak30_series is not None else 0.0
            ec_d30 = float(ec_delta_series.get(tkr, 0.0) or 0.0) if ec_delta_series is not None else 0.0
            streak_val = 0
            if tkr in streak_ticker_map and streak_arr is not None and sc_di >= 0:
                ti_sc = streak_ticker_map[tkr]
                if sc_di < streak_arr.shape[1]:
                    streak_val = int(streak_arr[ti_sc, sc_di])

            vs = (
                min(max(grd30,   -200), 200) * 0.5  +
                min(max(peak30,     0), 200) * 0.25 +
                rd7_tkr                      * 1.0  +
                wf7_tkr                      * 20.0 +
                ec_d30                       * 5.0  +
                min(max(streak_val, -10), 10) * 1.0
            )
            vs_r = round(vs, 1)
            if vs_r != 0:
                entry["vs"] = vs_r

            # burst
            if tkr in b_set:
                entry["burst"] = 1

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
