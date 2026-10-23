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

from .scoring import Config, MotionConfig, compute_leaderboard


def snapshot_dates(history: pd.DataFrame, lookback_days: int,
                   start: pd.Timestamp | None = None) -> list[pd.Timestamp]:
    """Distinct Holdings_As_Of values in the last `lookback_days` days, ascending,
    and not before `start` (the current fund universe's first snapshot)."""
    s = pd.to_datetime(history["Holdings_As_Of"], errors="coerce").dropna()
    if s.empty:
        return []
    # Normalize to tz-naive so sorted() and comparisons never raise TypeError when
    # the source data contains ISO-8601 strings with timezone offsets.
    if getattr(s.dt, "tz", None) is not None:
        s = s.dt.tz_convert(None)
    latest = s.max()
    cutoff = latest - pd.Timedelta(days=lookback_days)
    if start is not None:
        cutoff = max(cutoff, pd.Timestamp(start))
    return sorted(s[s >= cutoff].unique().tolist())


def historical_leaderboards(history: pd.DataFrame, cfg: Config) -> dict[pd.Timestamp, pd.DataFrame]:
    """Compute leaderboard at each snapshot date in the lookback window."""
    dates = snapshot_dates(history, cfg.history.leaderboard_lookback_days, cfg.history.universe_start)
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

    Research only: the APEX kicker built on it was removed from the engine in
    2026-10; scripts/scoring_study.py uses it to reproduce that test (V7).

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


def burst_crater_panels(historical: dict[pd.Timestamp, pd.DataFrame], motion) -> dict[str, pd.DataFrame]:
    """BURST / CRATER for every snapshot date (2026-10 redesign).

    The single implementation behind build.py (today's flags), signal_history
    (per-ticker flag history) and backtest.py (trigger strategies). See
    scoring.MotionConfig for the rule. The pre-2026-10 rule measured the
    RANGE (worst − best) of the raw rank over 30 days, so a one-day spike from
    a partial ETF scrape (#100 → #140 → #100) passed the ≥ 40-rank test for
    BURST and CRATER alike; it flagged ~850 names a day (median rank ~1,700)
    and its CRATER names went on to OUTPERFORM. Smoothing, a direction, a
    rank-relative size, a relevance floor and fund confirmation fix each of
    those (docs/SIGNALS_STUDY.md).

    Returns ticker × date frames: burst, crater, surge, slide (bool; SURGE /
    SLIDE exclude names already BURST / CRATER), rank_now (smoothed),
    rank_then / funds_delta (BURST's window: smoothed rank and holder change
    over lookback_snapshots) and crater_rank_then / crater_funds_delta (the
    same over CRATER's longer crater_lookback_snapshots).
    """
    dates = sorted(historical.keys())
    empty = pd.DataFrame()
    if len(dates) < 2:
        return {"burst": empty, "crater": empty, "surge": empty, "slide": empty,
                "rank_then": empty, "rank_now": empty, "funds_delta": empty,
                "crater_rank_then": empty, "crater_funds_delta": empty}

    def _panel(col: str) -> pd.DataFrame:
        cols = {d: historical[d].set_index("ticker")[col] for d in dates if col in historical[d].columns}
        return pd.DataFrame(cols).sort_index(axis=1)

    k = motion.lookback_snapshots
    kc = getattr(motion, "crater_lookback_snapshots", k)
    ranks = _panel("leaderboard_rank").astype(float)
    funds = _panel("etf_count").reindex(index=ranks.index, columns=ranks.columns).fillna(0)
    smooth = ranks.T.rolling(motion.smooth_snapshots, min_periods=motion.smooth_snapshots).median().T

    # Rank momentum over k snapshots. SURGE / SLIDE (the price-driven grade
    # of BURST / CRATER) use the lower momentum_min_log_rank_move bar.
    then = smooth.shift(k, axis=1)
    win_best = smooth.T.rolling(k + 1, min_periods=k + 1).min().T
    win_worst_k = smooth.T.rolling(k + 1, min_periods=k + 1).max().T
    up_move, down_move = np.log(then / smooth), np.log(smooth / then)
    up_held = (smooth <= motion.relevance_rank) & (np.log(smooth / win_best) <= motion.persist_log_tol)
    down_held = (then <= motion.relevance_rank) & (np.log(win_worst_k / smooth) <= motion.persist_log_tol)
    m_bar = getattr(motion, "momentum_min_log_rank_move", motion.min_log_rank_move)
    surge = (up_move >= m_bar) & up_held
    slide = (down_move >= m_bar) & down_held

    # BURST — the full ×1.65 move, with a fund joining over the same k snapshots
    funds_delta = funds - funds.shift(k, axis=1)
    burst = (up_move >= motion.min_log_rank_move) & up_held

    # CRATER — over kc snapshots: a fund's exit often comes before the slide
    crater_then = smooth.shift(kc, axis=1)
    crater_funds_delta = funds - funds.shift(kc, axis=1)
    win_worst = smooth.T.rolling(kc + 1, min_periods=kc + 1).max().T
    crater = ((np.log(smooth / crater_then) >= getattr(motion, "crater_min_log_rank_move", motion.min_log_rank_move))
              & (crater_then <= motion.relevance_rank)
              & (np.log(win_worst / smooth) <= motion.persist_log_tol))

    if motion.require_fund_confirmation:
        burst &= funds_delta >= 1
        crater &= crater_funds_delta <= -1
    burst, crater = burst.fillna(False), crater.fillna(False)
    return {"burst": burst, "crater": crater,
            # a name already flagged BURST / CRATER is not also tagged SURGE / SLIDE
            "surge": surge.fillna(False) & ~burst, "slide": slide.fillna(False) & ~crater,
            "rank_then": then, "rank_now": smooth, "funds_delta": funds_delta,
            "crater_rank_then": crater_then, "crater_funds_delta": crater_funds_delta}


def screening_signals(leaderboard: pd.DataFrame, etf_count_14d_ago: pd.Series, cfg: Config) -> pd.DataFrame:
    """Screening chips (2026-10 audit, docs/SIGNALS_STUDY.md §4), computed in
    the build — not the browser — so each has one tested definition.

    Needs leaderboard columns: etf_count, etf_count_delta_30d,
    leaderboard_rank, flag, conviction_etf_count, strategy_tier_count,
    avg_rank_delta_7d, score_streak. `etf_count_14d_ago` is aligned to the
    leaderboard rows (0 for names that were not on the board).

    Each signal was kept only because its 20-day return vs currency peers had
    the same sign in the Feb–May (momentum-friendly) and May–Oct (unwind)
    regimes:
        funds_down   rank ≤ funds_rank_max, lost a holder within 14d   −2.7% / −7.0%
        funds_up     rank ≤ funds_rank_max, gained a holder within 30d  +6.2% / +1.5%
        stealth_buy  a 2nd fund joined within 14d, rank ≤ stealth_rank_max  +4.9% / +1.7%
        pre_hc       one step from HIGH_CONVICTION and improving       +7.4% / +0.1%
    The pre-audit Stealth rule (≤ 2 funds, +10% weight in 7d) mostly caught
    price drift in small positions (~250 names a day, no edge).
    """
    sg = cfg.signals
    out = leaderboard.copy()
    rank = out["leaderboard_rank"]
    d14 = (out["etf_count"] - pd.Series(etf_count_14d_ago, index=out.index).fillna(0)).astype(int)
    out["etf_count_delta_14d"] = d14
    out["funds_down"] = (d14 < 0) & (rank <= sg.funds_rank_max)
    out["funds_up"] = (out["etf_count_delta_30d"] > 0) & (rank <= sg.funds_rank_max)
    out["stealth_buy"] = (out["etf_count"] == 2) & (d14 >= 1) & (rank <= sg.stealth_rank_max)
    out["pre_hc"] = ((out["flag"] != "HIGH_CONVICTION")
                     & (out["etf_count"] >= sg.pre_hc_min_etfs)
                     & (out["conviction_etf_count"] >= 1)
                     & (out["strategy_tier_count"] >= cfg.high_conviction_min_tiers)
                     & (out["avg_rank_delta_7d"] > 0)
                     & (out["score_streak"].fillna(0) >= 2))
    return out


def changelog(
    historical: dict[pd.Timestamp, pd.DataFrame],
    periods: Iterable[int | str],
    *,
    funds_rank_max: int = 500,
    movers_rank_max: int = 300,
    min_rank_move: int = 10,
    top_n: int = 15,
) -> dict:
    """
    The Changes tab: what moved in the conviction picture between the
    snapshot closest to (today − N days) and today, for every period N.
    Built from the full daily leaderboards (every held name), so no table
    depends on the 600-ticker score_history.json.

    Per period, each list holds {ticker, company, tier, rank_then, rank_now,
    funds_then, funds_now, score_then, score_now} (None where the name was
    not on the board), ordered most material first:

        funds_added  gained ≥ 1 holding fund, now inside funds_rank_max
        funds_lost   lost ≥ 1 holding fund, was inside funds_rank_max
                     (includes names that left the board entirely)
        hc_entered   HIGH_CONVICTION now, not then
        hc_exited    HIGH_CONVICTION then, not now
        climbers     rank improved by the largest factor, now inside
                     movers_rank_max (a #300→#150 move counts as much as
                     #20→#10; at least min_rank_move positions so #2→#1
                     is not a "×2 move")
        fallers      mirror: rank worsened most, was inside movers_rank_max
        new          on the board now, not held then
        exits        held then, on no fund's book now

    `counts` gives each list's full size before the top_n cut. Fund-count
    change is the most robust signal in the 2026-10 audit
    (docs/SIGNALS_STUDY.md §4), which is why it leads the tab.
    """
    periods = list(periods)
    dates = sorted(historical.keys())
    keys = ("funds_added", "funds_lost", "hc_entered", "hc_exited",
            "climbers", "fallers", "new", "exits")
    empty = {"since": None, "counts": {k: 0 for k in keys}, **{k: [] for k in keys}}
    if len(dates) < 2:
        return {"today": None, "periods": {str(p): dict(empty) for p in periods}}

    def _frame(d):
        lb = historical[d]
        lb = lb[lb["etf_count"].fillna(0) > 0] if "etf_count" in lb.columns else lb
        lb = lb.drop_duplicates("ticker").set_index("ticker")
        tiers = lb["tiers"].fillna("") if "tiers" in lb.columns else pd.Series("", index=lb.index)
        return pd.DataFrame({
            "company": lb["company"].fillna("") if "company" in lb.columns else "",
            "tier": tiers.str.split(" + ", regex=False).str[0],
            "rank": lb["leaderboard_rank"],
            "funds": lb["etf_count"],
            "score": lb["final_score"],
            "hc": lb["flag"].eq("HIGH_CONVICTION") if "flag" in lb.columns else False,
        }, index=lb.index)

    today = dates[-1]
    now = _frame(today)
    out = {"today": today.strftime("%Y-%m-%d"), "periods": {}}
    for p in periods:
        if p == "YTD":
            target = pd.Timestamp(year=today.year, month=1, day=1)
        else:
            target = today - pd.Timedelta(days=int(p))
        start = min(dates[:-1], key=lambda d: abs((d - target).total_seconds()))
        then = _frame(start)
        m = now.join(then, how="outer", lsuffix="_now", rsuffix="_then")
        m["funds_now"] = m["funds_now"].fillna(0)
        m["funds_then"] = m["funds_then"].fillna(0)
        m["hc_now"] = m["hc_now"].eq(True)
        m["hc_then"] = m["hc_then"].eq(True)
        m["company"] = m["company_now"].where(m["company_now"].notna(), m["company_then"])
        m["tier"] = m["tier_now"].where(m["tier_now"].notna(), m["tier_then"])
        dfunds = m["funds_now"] - m["funds_then"]
        on_now, on_then = m["rank_now"].notna(), m["rank_then"].notna()
        both = on_now & on_then
        moved = (m["rank_then"] - m["rank_now"]).abs() >= min_rank_move
        lr = np.log(m["rank_then"] / m["rank_now"])

        # Sort keys live on m itself: .assign(Series) on an EMPTY filtered
        # frame would adopt the Series' index and resurrect every row.
        m["_dfunds"], m["_lr"] = dfunds, lr
        sel = {
            "funds_added": m[(dfunds >= 1) & (m["rank_now"] <= funds_rank_max)]
                .sort_values(["_dfunds", "rank_now"], ascending=[False, True]),
            "funds_lost": m[(dfunds <= -1) & (m["rank_then"] <= funds_rank_max)]
                .sort_values(["_dfunds", "rank_then"], ascending=[True, True]),
            "hc_entered": m[m["hc_now"] & ~m["hc_then"]].sort_values("rank_now"),
            "hc_exited": m[m["hc_then"] & ~m["hc_now"]].sort_values("rank_then"),
            "climbers": m[both & moved & (lr > 0) & (m["rank_now"] <= movers_rank_max)]
                .sort_values("_lr", ascending=False),
            "fallers": m[both & moved & (lr < 0) & (m["rank_then"] <= movers_rank_max)]
                .sort_values("_lr"),
            "new": m[on_now & ~on_then].sort_values("rank_now"),
            "exits": m[on_then & ~on_now].sort_values("rank_then"),
        }

        def _num(v, nd=None):
            if v is None or pd.isna(v):
                return None
            return round(float(v), nd) if nd is not None else int(v)

        def _records(df):
            return [{
                "ticker": str(t),
                "company": "" if pd.isna(r["company"]) else str(r["company"]),
                "tier": "" if pd.isna(r["tier"]) else str(r["tier"]),
                "rank_then": _num(r["rank_then"]), "rank_now": _num(r["rank_now"]),
                "funds_then": _num(r["funds_then"]), "funds_now": _num(r["funds_now"]),
                "score_then": _num(r["score_then"], 1), "score_now": _num(r["score_now"], 1),
            } for t, r in df.head(top_n).iterrows()]

        out["periods"][str(p)] = {
            "since": start.strftime("%Y-%m-%d"),
            "counts": {k: int(len(v)) for k, v in sel.items()},
            **{k: _records(v) for k, v in sel.items()},
        }
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
        sg, sl SURGE / SLIDE momentum tags (omit when False)

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


    # ── 2. BURST / CRATER per date — the shared implementation ───────────────
    motion = cfg.motion if cfg.motion is not None else MotionConfig()
    bc = burst_crater_panels(historical, motion)
    kinds = ("burst", "crater", "surge", "slide")
    flag_sets: dict[str, list[set]] = {k: [] for k in kinds}
    for d in dates_sorted:
        for k in kinds:
            panel = bc[k]
            flag_sets[k].append(set(panel.index[panel[d].to_numpy(dtype=bool)])
                                if not panel.empty and d in panel.columns else set())

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

        b_set, c_set, sg_set, sl_set = (flag_sets[k][di] for k in kinds)

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

            # sg / sl — SURGE / SLIDE momentum tags
            if tkr in sg_set:
                entry["sg"] = 1
            if tkr in sl_set:
                entry["sl"] = 1

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
