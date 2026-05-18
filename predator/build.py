"""
Build script — runs in CI after every data push.

Loads holdings from this repo's data/all_history.csv (the file the scraper
writes), runs sanitizer + scoring + temporal analytics, and writes the
outputs the dashboard consumes.

Outputs (in docs/data/):
    leaderboard.json         — today's leaderboard, enriched with temporal stats
    holdings_latest.json     — per-(ETF, ticker) detail with rank/weight deltas
    changelog.json           — entries/exits/movers vs yesterday
    score_history.parquet    — wide-form score panel for sparklines
    metadata.json            — build info, source row count, flag tallies, config snapshot
    leaderboard.parquet      — columnar dump for DuckDB-WASM time travel

Usage:
    python -m predator.build
    python -m predator.build --source path/to/all_history.csv
    python -m predator.build --output docs/data --config config.yaml
"""
from __future__ import annotations
import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd


class _SafeEncoder(json.JSONEncoder):
    """Encode NaN / ±Inf floats as JSON null instead of invalid bare NaN.

    Python's built-in json module emits NaN as the bare token ``NaN`` which is
    not valid JSON (ECMA-404 / RFC 8259).  Pandas DataFrames merged with a
    left-join produce float NaN for missing rows even when the source Python
    values were None, because Pandas upcasts nullable object columns to
    float64.  This encoder intercepts those values at serialisation time and
    replaces them with JSON ``null``.
    """
    def iterencode(self, o, _one_shot=False):
        # Walk the object tree once and sanitise in-place before encoding.
        return super().iterencode(self._sanitise(o), _one_shot)

    def _sanitise(self, obj):
        if isinstance(obj, float):
            return None if (math.isnan(obj) or math.isinf(obj)) else obj
        if isinstance(obj, dict):
            return {k: self._sanitise(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [self._sanitise(v) for v in obj]
        return obj


def _dumps(obj, **kwargs) -> str:
    """json.dumps using _SafeEncoder (NaN → null)."""
    kwargs.setdefault("cls", _SafeEncoder)
    return json.dumps(obj, **kwargs)

if sys.platform.startswith("win"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except AttributeError:
        pass


from .scoring import Config, compute_leaderboard, compute_rank_deltas
from . import history as hist


# Default: scraper writes here, build reads from here. Same repo.
DEFAULT_SOURCE = "data/all_history.csv"
# Fallback: pull live from GitHub if local file is missing (for first-time / local dev runs).
FALLBACK_SOURCE = "https://raw.githubusercontent.com/yieldchaser/etf-data/main/data/all_history.csv"


def fetch_history(source: str) -> pd.DataFrame:
    p = Path(source)
    if not p.exists() and not source.startswith("http"):
        print(f"  {source} not found locally — falling back to {FALLBACK_SOURCE}")
        source = FALLBACK_SOURCE
    print(f"Loading: {source}")
    df = pd.read_csv(source)
    print(f"  {len(df):,} rows · {df['ETF_Ticker'].nunique()} ETFs · "
          f"{df['Holdings_As_Of'].min()} → {df['Holdings_As_Of'].max()}")
    return df


def build(source: str, output_dir: Path, config_path: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    cfg = Config.from_yaml(config_path)
    raw = fetch_history(source)
    raw_rows = len(raw)

    print("\nRunning sanitizer + scoring…")
    leaderboard, latest = compute_leaderboard(raw, cfg)

    sanitized_rows = len(latest) if not latest.empty else 0
    print(f"  leaderboard: {len(leaderboard)} unique tickers · "
          f"{(leaderboard['flag']=='HIGH_CONVICTION').sum()} HC · "
          f"{(leaderboard['flag']=='SPECULATIVE_BETA').sum()} SPEC β")

    # Multi-period rank deltas (Phase 2) — compute once per period
    print("\nComputing rank deltas for all periods…")
    deltas_by_period: dict[int, pd.DataFrame] = {}
    for n_days in cfg.history.delta_periods_days:
        deltas_by_period[n_days] = compute_rank_deltas(raw, cfg, lookback_days=n_days)
        print(f"  {n_days}d delta: {len(deltas_by_period[n_days])} rows")
    # Primary (7d) for backward-compat columns
    primary_period = cfg.history.rank_delta_lookback_days
    _prim = deltas_by_period.get(primary_period)
    deltas = _prim if _prim is not None else next(iter(deltas_by_period.values()))

    print("\nComputing historical leaderboards…")
    historical = hist.historical_leaderboards(raw, cfg)
    print(f"  {len(historical)} daily leaderboards (lookback {cfg.history.leaderboard_lookback_days}d)")

    score_pnl = hist.score_panel(historical)
    flag_pnl = hist.flag_panel(historical)
    streaks = hist.streaks_and_deltas(score_pnl, flag_pnl)
    chg = hist.changelog(historical, leaderboard, streaks, top_n=cfg.history.changelog_top_n)

    # Enrich leaderboard with temporal fields
    if not streaks.empty:
        leaderboard = leaderboard.merge(
            streaks[["ticker", "score_delta", "score_delta_pct", "score_streak",
                     "hc_streak", "score_percentile", "days_observed"]],
            on="ticker", how="left"
        )
    else:
        for col in ("score_delta", "score_delta_pct", "score_streak", "hc_streak",
                    "score_percentile", "days_observed"):
            leaderboard[col] = None

    # ── Concentration risk score ──────────────────────────────────────────
    def _compute_concentration(latest_df: pd.DataFrame) -> pd.DataFrame:
        """Per ticker: what fraction of the score comes from its single top ETF?
        100 = entirely one ETF; 25 = perfectly diversified across 4 ETFs."""
        grouped = latest_df.groupby("ticker")["score"]
        totals = grouped.sum()
        maxes  = grouped.max()
        # Guard against division by zero (tickers with 0 total score)
        top_share = (maxes / totals.replace(0, float('nan'))).fillna(1.0).clip(0, 1.0)
        return pd.DataFrame({
            "ticker": totals.index,
            "top_etf_share": top_share.values.round(3),
            "concentration_score": (top_share * 100).round(0).astype(int).values,
        })

    conc = _compute_concentration(latest)
    leaderboard = leaderboard.merge(conc, on="ticker", how="left")

    # ── Multi-period SCORE deltas (attach as score_deltas_by_period dict) ────
    print("\nComputing per-period score deltas for leaderboard…")
    raw_dt = raw.copy()
    raw_dt["Holdings_As_Of"] = pd.to_datetime(raw_dt["Holdings_As_Of"], errors="coerce")
    latest_date = raw_dt["Holdings_As_Of"].max()
    ytd_start = pd.Timestamp(year=latest_date.year, month=1, day=1)

    all_periods: list[int | str] = list(cfg.history.delta_periods_days) + ["YTD"]
    score_deltas_by_period: dict[int | str, dict] = {}

    for n in cfg.history.delta_periods_days:
        col = f"score_delta_pct_{n}d" if n != cfg.history.rank_delta_lookback_days else None
        if col and col in leaderboard.columns:
            # Already computed in the period-loop above — extract to dict (preserve NaN → null)
            score_deltas_by_period[n] = leaderboard.set_index("ticker")[col].to_dict()
        elif n == cfg.history.rank_delta_lookback_days and "score_delta_pct" in leaderboard.columns:
            score_deltas_by_period[n] = leaderboard.set_index("ticker")["score_delta_pct"].to_dict()
        else:
            # Re-compute from historical snapshot if column not present
            cutoff = latest_date - pd.Timedelta(days=n)
            raw_past = raw_dt[raw_dt["Holdings_As_Of"] <= cutoff]
            if not raw_past.empty:
                try:
                    lb_past, _ = compute_leaderboard(raw_past, cfg)
                    ps = lb_past.set_index("ticker")["final_score"].to_dict()
                    today_s = leaderboard.set_index("ticker")["final_score"]
                    delta = {}
                    for t, cur in today_s.items():
                        prev = ps.get(t)
                        # Preserve None for missing past data (don't fillna(0))
                        # Cap extreme values at ±10 (1000%) to avoid misleading display
                        if prev and prev != 0:
                            raw_delta = (cur - prev) / abs(prev)
                            delta[t] = round(max(-10.0, min(10.0, raw_delta)), 4)
                        else:
                            delta[t] = None
                    score_deltas_by_period[n] = delta
                except Exception as e:
                    print(f"  {n}d score delta: ERROR — {e}")
                    score_deltas_by_period[n] = {}
            else:
                score_deltas_by_period[n] = {}

    # YTD delta
    days_since_ytd = (latest_date - ytd_start).days
    if days_since_ytd > 0:
        raw_ytd = raw_dt[raw_dt["Holdings_As_Of"] <= ytd_start]
        if not raw_ytd.empty:
            try:
                lb_ytd, _ = compute_leaderboard(raw_ytd, cfg)
                ps_ytd = lb_ytd.set_index("ticker")["final_score"].to_dict()
                today_s = leaderboard.set_index("ticker")["final_score"]
                ytd_delta = {}
                for t, cur in today_s.items():
                    prev = ps_ytd.get(t)
                    # Preserve None for missing past data
                    # Cap extreme values at ±10 (1000%)
                    if prev and prev != 0:
                        raw_delta = (cur - prev) / abs(prev)
                        ytd_delta[t] = round(max(-10.0, min(10.0, raw_delta)), 4)
                    else:
                        ytd_delta[t] = None
                score_deltas_by_period["YTD"] = ytd_delta
                print(f"  YTD score delta: {len(ytd_delta)} tickers (from {ytd_start.date()})")
            except Exception as e:
                print(f"  YTD score delta: ERROR — {e}")
                score_deltas_by_period["YTD"] = {}
        else:
            score_deltas_by_period["YTD"] = {}
    else:
        score_deltas_by_period["YTD"] = {}

    # ── VELOCITY signal — captures both steady accumulation AND burst moves ─────
    def _attach_velocity(leaderboard: pd.DataFrame,
                         deltas_by_period: dict,
                         historical: dict) -> pd.DataFrame:
        """Add velocity columns. Catches STX-style +55-ranks-in-12-days bursts
        that a naive 7d-only delta would miss."""

        # 1. Per-ETF rank/weight motion
        d7  = deltas_by_period.get(7)
        d30 = deltas_by_period.get(30)
        rank_avg_7  = d7.groupby("ticker")["rank_delta"].mean()  if d7  is not None and not d7.empty  else pd.Series(dtype=float)
        flow_avg_7  = d7.groupby("ticker")["weight_flow"].mean() if d7  is not None and not d7.empty  else pd.Series(dtype=float)
        rank_avg_30 = d30.groupby("ticker")["rank_delta"].mean() if d30 is not None and not d30.empty else pd.Series(dtype=float)

        # 2. Global leaderboard rank trajectory (requires leaderboard_rank col in historical snapshots)
        dates_sorted = sorted(historical.keys())
        today_date = dates_sorted[-1]
        window_start = today_date - pd.Timedelta(days=30)
        window_cols = [c for c in dates_sorted if c >= window_start]

        global_rank_delta_30 = pd.Series(dtype=float)
        peak_improvement_30  = pd.Series(dtype=float)
        best_in_window       = pd.Series(dtype=float)
        is_burst             = pd.Series(dtype=bool)

        if len(window_cols) >= 5:
            # Check historical snapshots have leaderboard_rank
            sample = historical[window_cols[0]]
            if "leaderboard_rank" in sample.columns:
                rank_panel_rows = {}
                for d in window_cols:
                    rank_panel_rows[d] = historical[d].set_index("ticker")["leaderboard_rank"]
                rank_panel = pd.DataFrame(rank_panel_rows)
                
                # Coverage check: require continuous presence (≥80% of window)
                nan_count = rank_panel.isna().sum(axis=1)
                coverage = (len(window_cols) - nan_count) / len(window_cols)
                
                first_col        = rank_panel.iloc[:, 0]
                last_col         = rank_panel.iloc[:, -1]
                worst_in_window  = rank_panel.max(axis=1)   # highest rank number = worst
                best_in_window   = rank_panel.min(axis=1)   # lowest rank number = best
                global_rank_delta_30 = (first_col - last_col).round(0)        # positive = improved
                peak_improvement_30  = (worst_in_window - best_in_window).round(0)
                
                # Sustained: rank must be better than within-window median for ≥5 of last 10 snapshots
                recent10 = rank_panel.iloc[:, -10:] if rank_panel.shape[1] >= 10 else rank_panel
                median_per_ticker = rank_panel.median(axis=1)
                is_better_than_median = recent10.lt(median_per_ticker, axis=0)
                sustained_count = is_better_than_median.sum(axis=1)
                
                # Burst qualifier: peak ≥ 40 AND coverage ≥ 80% AND sustained ≥ 8 days
                is_burst = (peak_improvement_30 >= 40) & (coverage >= 0.80) & (sustained_count >= 8)
        else:
            is_burst = pd.Series(dtype=bool)

        # 3. ETF count change vs ~30d ago
        past_counts = pd.Series(dtype=float)
        if len(dates_sorted) >= 2:
            target    = today_date - pd.Timedelta(days=30)
            past_date = min(dates_sorted, key=lambda d: abs((d - target).total_seconds()))
            if "etf_count" in historical[past_date].columns:
                past_counts = historical[past_date].set_index("ticker")["etf_count"]

        # 4. Attach all raw signals
        leaderboard["avg_rank_delta_7d"]     = leaderboard["ticker"].map(rank_avg_7).fillna(0).round(2)
        leaderboard["avg_weight_flow_7d"]    = leaderboard["ticker"].map(flow_avg_7).fillna(0).round(4)
        leaderboard["avg_rank_delta_30d"]    = leaderboard["ticker"].map(rank_avg_30).fillna(0).round(2)
        leaderboard["global_rank_delta_30d"] = leaderboard["ticker"].map(global_rank_delta_30).fillna(0).astype(int)
        leaderboard["global_rank_peak_30d"]  = leaderboard["ticker"].map(peak_improvement_30).fillna(0).astype(int)
        leaderboard["global_rank_best_30d"]  = leaderboard["ticker"].map(best_in_window).fillna(leaderboard["leaderboard_rank"]).astype(int)
        leaderboard["etf_count_30d_ago"]     = leaderboard["ticker"].map(past_counts).fillna(leaderboard["etf_count"]).astype(int)
        leaderboard["etf_count_delta_30d"]   = (leaderboard["etf_count"] - leaderboard["etf_count_30d_ago"]).astype(int)
        # Burst: peak improvement of >=40 global ranks at any point in last 30d, with sustained presence
        leaderboard["burst_30d"]             = leaderboard["ticker"].map(is_burst).fillna(False)

        # 5. Composite velocity score
        # Tuning: global rank Δ30d of +50 → +25; peak +50 → +12.5;
        #         per-ETF avg Δ7d of +5 → +5; weight flow +20% → +4;
        #         ETFs added 30d +1 → +5; score streak +2d → +2
        leaderboard["velocity_score"] = (
            leaderboard["global_rank_delta_30d"].fillna(0).clip(-200, 200) * 0.5 +
            leaderboard["global_rank_peak_30d"].fillna(0).clip(0, 200) * 0.25 +
            leaderboard["avg_rank_delta_7d"].fillna(0) * 1.0 +
            leaderboard["avg_weight_flow_7d"].fillna(0) * 20.0 +
            leaderboard["etf_count_delta_30d"].fillna(0) * 5.0 +
            leaderboard["score_streak"].fillna(0).clip(-10, 10) * 1.0
        ).round(2)
        return leaderboard

    if historical:
        leaderboard = _attach_velocity(leaderboard, deltas_by_period, historical)
        print(f"  velocity_score: range [{leaderboard['velocity_score'].min():.1f}, {leaderboard['velocity_score'].max():.1f}]")
        burst_count = int(leaderboard['burst_30d'].sum())
    else:
        print("  WARNING: No historical data — skipping velocity/burst computation")
        for col in ["avg_rank_delta_7d", "avg_weight_flow_7d", "avg_rank_delta_30d",
                    "global_rank_delta_30d", "global_rank_peak_30d", "global_rank_best_30d",
                    "etf_count_30d_ago", "etf_count_delta_30d", "burst_30d", "velocity_score"]:
            leaderboard[col] = 0 if col != "burst_30d" else False
        burst_count = 0
    print(f"  burst_30d:      {burst_count} tickers with >=40 peak rank improvement")

    # ── Conviction Divergence ─────────────────────────────────────────────
    # Score rising but rank falling = being crowded out; inverse = relative strength
    leaderboard["conviction_divergence"] = 0
    mask_up_score = leaderboard["score_delta_pct"].fillna(0) > 0
    mask_down_rank = leaderboard["global_rank_delta_30d"].fillna(0) < 0
    leaderboard.loc[mask_up_score & mask_down_rank, "conviction_divergence"] = -1  # crowded out
    mask_down_score = leaderboard["score_delta_pct"].fillna(0) < 0
    mask_up_rank = leaderboard["global_rank_delta_30d"].fillna(0) > 0
    leaderboard.loc[mask_down_score & mask_up_rank, "conviction_divergence"] = 1  # relative strength

    # ── Stealth Accumulation ──────────────────────────────────────────────
    # Weight growing in 3+ ETFs but rank NOT improving
    leaderboard["stealth_accumulation"] = (
        (leaderboard["avg_weight_flow_7d"].fillna(0) > 0.03) &
        (leaderboard["avg_rank_delta_7d"].fillna(0) < 1) &
        (leaderboard["etf_count"] >= 3)
    )

    # ── Momentum Regime ───────────────────────────────────────────────────
    def _classify_regime(row):
        streak = row.get("score_streak", 0) or 0
        vel = row.get("velocity_score", 0) or 0
        if vel > 15 and streak > 3:
            return "accelerating"
        elif vel > 5 and streak > 0:
            return "rising"
        elif vel < -15 and streak < -3:
            return "declining"
        elif vel < -5 and streak < 0:
            return "weakening"
        else:
            return "stable"

    leaderboard["momentum_regime"] = leaderboard.apply(_classify_regime, axis=1)
    print(f"  new signals:    conviction_divergence, stealth_accumulation ({int(leaderboard['stealth_accumulation'].sum())}), momentum_regime")

    # ── Tier Breadth — how many distinct strategy types co-hold this name ────
    # 5 = held by all five (Scout/Quant/Quality/Trend/Blob); 1 = mono-tier.
    # Higher breadth = more independent strategy types confirming the name.
    leaderboard["tier_breadth"] = leaderboard["tiers"].fillna("").apply(
        lambda s: len([t for t in s.split(" + ") if t.strip()])
    ).astype(int)

    # ── Quality Adoption / Defection (30d) ────────────────────────────────────
    # Quality ETFs (COWZ/CALF/SPHQ) screen on free-cash-flow & profitability.
    # When momentum/scout name picks up a Quality cosign, that's institutional
    # validation. When Quality drops a name, that's a fundamentals warning.
    QUALITY_ETFS = {"COWZ", "CALF", "SPHQ"}

    def _quality_change(historical: dict, leaderboard: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
        """Returns (adopted_mask, defected_mask) per ticker for Quality vs ~30d ago."""
        if not historical:
            return pd.Series(dtype=bool), pd.Series(dtype=bool)
        dates_sorted = sorted(historical.keys())
        if len(dates_sorted) < 2:
            return pd.Series(dtype=bool), pd.Series(dtype=bool)
        target_past = dates_sorted[-1] - pd.Timedelta(days=30)
        past_date = min(dates_sorted, key=lambda d: abs((d - target_past).total_seconds()))
        # held_by 30d ago — split on ", " to get a set of ETF tickers
        past_lb = historical[past_date]
        if "held_by" not in past_lb.columns:
            return pd.Series(dtype=bool), pd.Series(dtype=bool)
        past_held = past_lb.set_index("ticker")["held_by"].apply(
            lambda s: set(t.strip() for t in str(s).split(",") if t.strip())
        )
        today_held = leaderboard.set_index("ticker")["held_by"].apply(
            lambda s: set(t.strip() for t in str(s).split(",") if t.strip())
        )
        adopted, defected = {}, {}
        all_tickers = set(today_held.index) | set(past_held.index)
        for t in all_tickers:
            now = today_held.get(t, set())
            then = past_held.get(t, set())
            now_q = now & QUALITY_ETFS
            then_q = then & QUALITY_ETFS
            adopted[t] = bool(now_q - then_q)        # gained at least one Quality ETF
            defected[t] = bool(then_q - now_q)       # lost at least one Quality ETF
        return pd.Series(adopted), pd.Series(defected)

    if historical:
        q_adopt, q_defect = _quality_change(historical, leaderboard)
        leaderboard["quality_adopted_30d"] = leaderboard["ticker"].map(q_adopt).fillna(False).astype(bool)
        leaderboard["quality_defected_30d"] = leaderboard["ticker"].map(q_defect).fillna(False).astype(bool)
    else:
        leaderboard["quality_adopted_30d"] = False
        leaderboard["quality_defected_30d"] = False
    print(f"  tier_breadth:   max={int(leaderboard['tier_breadth'].max())} · "
          f"quality_adopted_30d={int(leaderboard['quality_adopted_30d'].sum())} · "
          f"quality_defected_30d={int(leaderboard['quality_defected_30d'].sum())}")

    # ── Attach metadata (sector, industry, country) for flow analysis ─────────
    def _attach_metadata(leaderboard: pd.DataFrame) -> pd.DataFrame:
        """Merge ticker metadata (sector, industry, country) from cached CSV."""
        try:
            meta = pd.read_csv("data/ticker_metadata.csv")
            leaderboard = leaderboard.merge(meta, on="ticker", how="left")
            for col in ["sector", "industry", "country"]:
                leaderboard[col] = leaderboard[col].fillna("Unknown")
        except FileNotFoundError:
            print("  WARNING: data/ticker_metadata.csv not found — skipping metadata")
            for col in ["sector", "industry", "country", "market_cap_usd"]:
                leaderboard[col] = "Unknown" if col != "market_cap_usd" else None
        return leaderboard

    leaderboard = _attach_metadata(leaderboard)

    # ── Compute flow aggregations by sector and country ──────────────────────
    def _compute_flow(leaderboard: pd.DataFrame, dim: str) -> list[dict]:
        """For each value of `dim` (sector or country), aggregate velocity-weighted exposure."""
        lb = leaderboard[leaderboard["etf_count"] >= 2].copy()
        if lb.empty or dim not in lb.columns:
            return []
        g = lb.groupby(dim).agg(
            net_velocity=("velocity_score", "sum"),
            avg_velocity=("velocity_score", "mean"),
            names=("ticker", "count"),
            total_weight=("total_weight", "sum"),
            burst_count=("burst_30d", "sum"),
            hc_count=("flag", lambda s: (s == "HIGH_CONVICTION").sum()),
        ).reset_index().rename(columns={dim: "label"}).sort_values("net_velocity", ascending=False)
        return g.round(2).to_dict(orient="records")

    flow = {
        "by_sector":  _compute_flow(leaderboard, "sector"),
        "by_country": _compute_flow(leaderboard, "country"),
    }
    (output_dir / "flow.json").write_text(_dumps(flow, separators=(",", ":")))
    print(f"  flow.json:      {len(flow['by_sector'])} sectors, {len(flow['by_country'])} countries")

    # ── ETF Overlap matrix ────────────────────────────────────────────────────
    # For each pair (A, B), what fraction of A's holdings are also in B?
    # Uses today's snapshot only (latest DataFrame from compute_leaderboard).
    # Output: { etfs: [...], jaccard: [[...]], shared: [[...]] }
    #   jaccard[i][j] = |A ∩ B| / |A ∪ B|  (symmetric, 0..1)
    #   shared[i][j]  = |A ∩ B|  (raw count)
    try:
        if not latest.empty:
            etf_holdings = {
                etf: set(g["ticker"].tolist())
                for etf, g in latest.groupby("ETF_Ticker")
            }
            etf_list = sorted(etf_holdings.keys())
            n = len(etf_list)
            jaccard = [[0.0] * n for _ in range(n)]
            shared = [[0] * n for _ in range(n)]
            for i, a in enumerate(etf_list):
                A = etf_holdings[a]
                for j, b in enumerate(etf_list):
                    B = etf_holdings[b]
                    inter = len(A & B)
                    union = len(A | B) or 1
                    jaccard[i][j] = round(inter / union, 4)
                    shared[i][j] = inter
            overlap = {
                "etfs": etf_list,
                "jaccard": jaccard,
                "shared": shared,
                "sizes": {e: len(etf_holdings[e]) for e in etf_list},
            }
            (output_dir / "etf_overlap.json").write_text(_dumps(overlap, separators=(",", ":")))
            print(f"  etf_overlap.json: {n}×{n} matrix (Jaccard + raw counts)")
    except Exception as e:
        print(f"  etf_overlap.json: ERROR — {e}")

    # ── leaderboard.json — main payload for the site ──────────────────────────
    lb_records = leaderboard.to_dict(orient="records")

    # Build per-ticker flag history from historical leaderboards
    # Schema: { ticker: [{d, flag, rank, vs, burst}] }
    # vs = velocity_score (only available if column exists in snapshot)
    # burst = burst_30d (only available if column exists in snapshot)
    flag_history: dict[str, list] = {}
    if historical:
        for d in sorted(historical.keys()):
            lb_snap = historical[d]
            has_vs = "velocity_score" in lb_snap.columns
            has_burst = "burst_30d" in lb_snap.columns
            for _, row in lb_snap.iterrows():
                t = row["ticker"]
                entry = {
                    "d": d.strftime("%Y-%m-%d"),
                    "flag": row.get("flag", ""),
                    "rank": int(row.get("leaderboard_rank", 0)),
                }
                if has_vs:
                    vs = row.get("velocity_score")
                    entry["vs"] = round(float(vs), 1) if vs is not None and vs == vs else 0
                if has_burst:
                    entry["burst"] = bool(row.get("burst_30d", False))
                if t not in flag_history:
                    flag_history[t] = []
                flag_history[t].append(entry)

    # Attach per-period score deltas to every record (flag_history written separately)
    for r in lb_records:
        t = r.get("ticker", "")
        r["score_deltas_by_period"] = {}
        for p in all_periods:
            v = score_deltas_by_period.get(p, {}).get(t)
            # Treat NaN, None, missing — all as null in JSON
            if v is None or (isinstance(v, float) and v != v):
                r["score_deltas_by_period"][str(p)] = None
            else:
                r["score_deltas_by_period"][str(p)] = round(float(v), 4)
    (output_dir / "leaderboard.json").write_text(_dumps(lb_records, separators=(",", ":")))
    # Write flag_history to separate file (keyed by ticker) to reduce leaderboard.json payload
    (output_dir / "flag_history.json").write_text(_dumps(flag_history, separators=(",", ":")))
    print(f"  flag_history:   {sum(1 for t in flag_history if flag_history[t])} tickers with history (separate file)")
    # ── holdings_latest.json — per-(ETF, ticker) detail with rank deltas ──────
    if not latest.empty:
        latest_out = latest[[
            "ETF_Ticker", "ticker", "name", "weight", "rank", "tier", "is_new",
            "rank_mult", "base_score", "new_bonus", "score", "Holdings_As_Of"
        ]].copy()
        latest_out["Holdings_As_Of"] = pd.to_datetime(latest_out["Holdings_As_Of"]).dt.strftime("%Y-%m-%d")
        # Merge primary period columns (backward compat: rank_delta, weight_flow)
        latest_out = latest_out.merge(
            deltas[["ETF_Ticker", "ticker", "rank_delta", "weight_flow"]],
            on=["ETF_Ticker", "ticker"], how="left"
        )
        # Merge additional periods (1d, 14d, 30d etc.)
        for n_days, d in deltas_by_period.items():
            if n_days == primary_period:
                continue  # already merged above
            rename_cols = {
                "rank_delta": f"rank_delta_{n_days}d",
                "weight_flow": f"weight_flow_{n_days}d",
            }
            latest_out = latest_out.merge(
                d[["ETF_Ticker", "ticker", "rank_delta", "weight_flow"]].rename(columns=rename_cols),
                on=["ETF_Ticker", "ticker"], how="left"
            )
        (output_dir / "holdings_latest.json").write_text(
            _dumps(latest_out.to_dict(orient="records"), separators=(",", ":"))
        )

    # Top velocity movers (15 names, held by 2+ ETFs)
    if 'velocity_score' in leaderboard.columns:
        top_vel = leaderboard[leaderboard['etf_count'] >= 2].sort_values('velocity_score', ascending=False).head(15)
        chg['top_velocity'] = [
            {
                'ticker':               str(r['ticker']),
                'company':              str(r.get('company', '')),
                'velocity_score':       float(r['velocity_score']),
                'avg_rank_delta_7d':    float(r['avg_rank_delta_7d']),
                'global_rank_delta_30d': int(r.get('global_rank_delta_30d', 0)),
                'global_rank_peak_30d': int(r.get('global_rank_peak_30d', 0)),
                'etf_count_delta_30d':  int(r['etf_count_delta_30d']),
                'burst_30d':            bool(r.get('burst_30d', False)),
                'final_score':          int(r['final_score']),
                'etf_count':            int(r['etf_count']),
                'tiers':                str(r.get('tiers', '')),
            }
            for _, r in top_vel.iterrows()
        ]

    # ── changelog.json — entries / exits / movers ─────────────────────────────
    (output_dir / "changelog.json").write_text(_dumps(chg, indent=2))

    # ── score_history.parquet + JSON — for sparklines ─────────────────────────
    if not score_pnl.empty:
        score_pnl.to_parquet(output_dir / "score_history.parquet")
        # Compact JSON: { ticker: [{ d, s }, ...] } limited to top-N by today's score
        top_n_for_spark = 200
        top_tickers = leaderboard.head(top_n_for_spark)["ticker"].tolist()
        spark = {}
        for t in top_tickers:
            if t in score_pnl.index:
                series = score_pnl.loc[t].dropna()
                spark[t] = [{"d": d.strftime("%Y-%m-%d"), "s": round(float(v), 2)}
                             for d, v in series.items()]
        (output_dir / "score_history.json").write_text(_dumps(spark, separators=(",", ":")))

    # ── leaderboard.parquet — for DuckDB-WASM time-travel queries ─────────────
    leaderboard.to_parquet(output_dir / "leaderboard.parquet", index=False)

    # ── holdings_history.parquet + JSON (Phase 2) — per-(ETF, ticker, date) ───
    print("\nBuilding holdings history…")
    sanitized_raw = cfg.sanitizer.apply(raw)
    sanitized_raw["Holdings_As_Of"] = pd.to_datetime(sanitized_raw["Holdings_As_Of"], errors="coerce")
    window_start = sanitized_raw["Holdings_As_Of"].max() - pd.Timedelta(days=cfg.history.leaderboard_lookback_days)
    hist_window = sanitized_raw[sanitized_raw["Holdings_As_Of"] >= window_start].copy()
    hist_window = hist_window.sort_values(
        ["ETF_Ticker", "Holdings_As_Of", "weight", "ticker"],
        ascending=[True, True, False, True]
    )
    hist_window["rank"] = hist_window.groupby(["ETF_Ticker", "Holdings_As_Of"]).cumcount() + 1
    out_hh = hist_window[["ETF_Ticker", "ticker", "Holdings_As_Of", "rank", "weight"]].copy()
    out_hh["Holdings_As_Of"] = out_hh["Holdings_As_Of"].dt.strftime("%Y-%m-%d")
    out_hh.to_parquet(output_dir / "holdings_history.parquet", index=False)
    print(f"  holdings_history.parquet: {len(out_hh):,} rows")
    # Compact JSON for top-300 tickers
    top_tickers_hh = set(leaderboard.head(300)["ticker"].tolist())
    hist_filtered = hist_window[hist_window["ticker"].isin(top_tickers_hh)]
    holdings_history_json: dict = {}
    for (t, etf), g in hist_filtered.groupby(["ticker", "ETF_Ticker"]):
        row_list = [
            {"d": str(d)[:10], "r": int(r), "w": round(float(w), 6)}
            for d, r, w in zip(g["Holdings_As_Of"], g["rank"], g["weight"])
        ]
        holdings_history_json.setdefault(t, {})[etf] = row_list
    (output_dir / "holdings_history.json").write_text(
        _dumps(holdings_history_json, separators=(",", ":"))
    )
    print(f"  holdings_history.json: {len(holdings_history_json)} tickers")

    # ── metadata.json — versioning, counts, config snapshot ───────────────────
    latest_holdings_date = pd.to_datetime(raw["Holdings_As_Of"]).max().strftime("%Y-%m-%d")
    latest_scrape_date = pd.to_datetime(raw["Date_Scraped"]).max().strftime("%Y-%m-%d")

    # Yesterday's counts for KPI deltas
    yday_hc = yday_spec = None
    if len(historical) >= 2:
        dates_sorted = sorted(historical.keys())
        prev_lb = historical[dates_sorted[-2]]
        yday_hc = int((prev_lb["flag"] == "HIGH_CONVICTION").sum())
        yday_spec = int((prev_lb["flag"] == "SPECULATIVE_BETA").sum())

    metadata = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source": source,
        "source_rows_raw": int(raw_rows),
        "source_rows_after_sanitize": int(sanitized_rows),
        "leaderboard_rows": int(len(leaderboard)),
        "latest_holdings_as_of": latest_holdings_date,
        "latest_scrape_date": latest_scrape_date,
        "etfs": sorted(raw["ETF_Ticker"].unique().tolist()),
        "snapshot_dates_in_window": [d.strftime("%Y-%m-%d") for d in sorted(historical.keys())],
        "flag_counts_today": {
            "HIGH_CONVICTION": int((leaderboard["flag"] == "HIGH_CONVICTION").sum()),
            "SPECULATIVE_BETA": int((leaderboard["flag"] == "SPECULATIVE_BETA").sum()),
            "NEW": int(leaderboard["any_new"].sum()) if "any_new" in leaderboard else 0,
            "NONE": int((leaderboard["flag"] == "").sum()),
        },
        "flag_counts_yesterday": {
            "HIGH_CONVICTION": yday_hc,
            "SPECULATIVE_BETA": yday_spec,
        },
        "config_snapshot": {
            "sanitizer": {
                "blocked_tickers": list(cfg.sanitizer.blocked_tickers),
                "blocked_name_patterns": list(cfg.sanitizer.blocked_name_patterns),
            },
            "etfs": [{"ticker": e.ticker, "tier": e.tier, "points": e.points} for e in cfg.etfs],
            "rank_breakpoints": [list(b) for b in cfg.rank_breakpoints],
            "new_lookback_days": cfg.new_lookback_days,
            "new_bonus_mult": cfg.new_bonus_mult,
            "high_conviction_min_etfs": cfg.high_conviction_min_etfs,
        },
    }
    (output_dir / "metadata.json").write_text(_dumps(metadata, indent=2))

    # Quick summary
    print(f"\n✓ Wrote outputs to {output_dir}/")
    for name in ["leaderboard.json", "holdings_latest.json", "changelog.json",
                 "score_history.json", "score_history.parquet", "leaderboard.parquet",
                 "holdings_history.parquet", "holdings_history.json", "metadata.json"]:
        p = output_dir / name
        if p.exists():
            print(f"  {name:<30} {p.stat().st_size:>10,} bytes")
    print(f"\nLatest holdings: {latest_holdings_date}")
    print(f"Today's flags: {metadata['flag_counts_today']}")
    if yday_hc is not None:
        print(f"Yesterday's flags: HC={yday_hc} (Δ {metadata['flag_counts_today']['HIGH_CONVICTION'] - yday_hc:+d}) "
              f"SPEC={yday_spec} (Δ {metadata['flag_counts_today']['SPECULATIVE_BETA'] - yday_spec:+d})")
    print(f"\nChangelog: {len(chg.get('entered_hc', []))} entries to HC, "
          f"{len(chg.get('exited_hc', []))} exits, "
          f"{len(chg.get('new_entrants', []))} new entrants this week")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Build Predator Protocol site artifacts")
    p.add_argument("--source", default=DEFAULT_SOURCE)
    p.add_argument("--output", default="docs/data")
    p.add_argument("--config", default="config.yaml")
    args = p.parse_args(argv)
    build(args.source, Path(args.output), Path(args.config))
    return 0


if __name__ == "__main__":
    sys.exit(main())
