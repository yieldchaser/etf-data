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
    python -m conviction.build
    python -m conviction.build --source path/to/all_history.csv
    python -m conviction.build --output docs/data --config config.yaml
"""
from __future__ import annotations
import argparse
import json
import math
import os
import re
import shutil
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


def _write_json_atomic(path: Path, text: str) -> None:
    """Write JSON atomically: write to .tmp then os.replace to avoid truncated files.

    Creates parent directories automatically so callers never need to mkdir
    themselves (mirrors the markets_history.py version of this helper).
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


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


# Repo root — anchored to this file's location so DEFAULT_SOURCE is correct
# regardless of the working directory the caller uses (CWD-independent).
_REPO_ROOT = Path(__file__).resolve().parent.parent
# Default: scraper writes here, build reads from here. Same repo.
DEFAULT_SOURCE = str(_REPO_ROOT / "data" / "all_history.csv")
# Fallback: pull live from GitHub if local file is missing (for first-time / local dev runs).
FALLBACK_SOURCE = "https://raw.githubusercontent.com/yieldchaser/etf-data/main/data/all_history.csv"
# Partitioned Parquet store (Tier 2). Tests / advanced callers can override
# the location via the CONVICTION_PARQUET_STORE / PREDATOR_PARQUET_STORE env var (e.g. point at an empty
# directory to force the CSV path) without monkeypatching this module.
_DEFAULT_PARQUET_STORE = Path(__file__).resolve().parent.parent / "data" / "history_parquet"
_parquet_env = os.environ.get("CONVICTION_PARQUET_STORE") or os.environ.get("PREDATOR_PARQUET_STORE")
PARQUET_STORE = Path(_parquet_env) if _parquet_env else _DEFAULT_PARQUET_STORE


def _read_parquet_store(store: Path, lookback_days: int = 180) -> pd.DataFrame | None:
    """
    Read from year-partitioned Parquet store.

    Only loads partitions needed for the lookback window (push date filter down).
    Returns None if the store doesn't exist or has no data.
    """
    if not store.exists():
        return None

    import datetime
    cutoff = pd.Timestamp.now() - pd.Timedelta(days=lookback_days)
    cutoff_year = cutoff.year

    # Collect relevant year partitions
    frames = []
    for year_dir in sorted(store.glob("year=*")):
        try:
            yr = int(year_dir.name.split("=")[1])
        except (ValueError, IndexError):
            continue
        if yr < cutoff_year:
            continue  # skip years entirely before the lookback window
        parquet_file = year_dir / "holdings.parquet"
        if parquet_file.exists():
            try:
                df = pd.read_parquet(parquet_file)
                frames.append(df)
            except Exception as e:
                print(f"  WARNING: Could not read {parquet_file}: {e}")

    if not frames:
        return None

    combined = pd.concat(frames, ignore_index=True)
    # Apply date filter
    combined["Holdings_As_Of"] = pd.to_datetime(combined["Holdings_As_Of"], errors="coerce")
    combined = combined[combined["Holdings_As_Of"] >= cutoff]
    print(f"  Parquet store: {len(combined):,} rows from {len(frames)} year partition(s) "
          f"(lookback {lookback_days}d)")
    return combined


def fetch_history(source: str) -> pd.DataFrame:
    # Try partitioned Parquet store first (Tier 2)
    #
    # YTD score deltas need a pre-Jan-1 baseline row; a fixed 180-day lookback
    # stops covering Dec-31 once "now" passes ~June 30, nullifying every
    # ticker's YTD delta for the rest of the year (bug-hunt Critical #2).
    # Size the window to always span the year-to-date range plus buffer.
    now = pd.Timestamp.now()
    days_into_year = (now - pd.Timestamp(year=now.year, month=1, day=1)).days
    ytd_safe_lookback = max(180, days_into_year + 45)
    parquet_df = _read_parquet_store(PARQUET_STORE, lookback_days=ytd_safe_lookback)
    if parquet_df is not None and not parquet_df.empty:
        print(f"Loading from partitioned Parquet store: {PARQUET_STORE}")
        print(f"  {len(parquet_df):,} rows · {parquet_df['ETF_Ticker'].nunique()} ETFs · "
              f"{parquet_df['Holdings_As_Of'].min().date()} → {parquet_df['Holdings_As_Of'].max().date()}")
        return parquet_df

    # Fall back to CSV
    p = Path(source)
    if not p.exists() and not source.startswith("http"):
        if os.environ.get("CI"):
            raise FileNotFoundError(
                f"CI build: {source} not found locally and fallback is disabled in CI. "
                f"Ensure the data file is committed or fetched before running the build."
            )
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
        if n == cfg.history.rank_delta_lookback_days and "score_delta_pct" in leaderboard.columns:
            # Fast path: the primary-period delta was already computed upstream
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
        raw_ytd = raw_dt[raw_dt["Holdings_As_Of"] < ytd_start]
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

    # ── MOTION signals — rank trajectory, burst/crater, ETF-count change ──────
    def _attach_motion(leaderboard: pd.DataFrame,
                       deltas_by_period: dict,
                       historical: dict) -> pd.DataFrame:
        """Add per-ticker motion columns. Catches STX-style +55-ranks-in-12-days
        bursts that a naive 7d-only delta would miss.

        The velocity composite formerly assembled here was retired
        (2026-08 signal study: its components cancel — top-quintile lift ≤1.0
        at every forward horizon). The underlying components remain, since the
        UI and Pre-HC/Stealth-Buy filters use them directly."""
        is_burst  = pd.Series(dtype=bool)
        is_crater = pd.Series(dtype=bool)

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

        if len(window_cols) >= 5:
            # Check historical snapshots have leaderboard_rank — guard per-snapshot
            rank_panel_rows = {}
            for d in window_cols:
                snap = historical[d]
                if "leaderboard_rank" in snap.columns:
                    rank_panel_rows[d] = snap.set_index("ticker")["leaderboard_rank"]
            if rank_panel_rows:
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
                
                # Burst qualifier: peak ≥ 40 AND (coverage ≥ 80% OR ticker has < 15 window
                # snapshots — newer names get a pass on the coverage gate since they can't
                # possibly reach 80% of a 30-day window; sustained_count ≥ 8 still guards
                # against noise from tickers with only a handful of appearances).
                #
                # Without this, any ticker added in the last ~6 days of the window fails
                # the 80% gate and can never qualify for burst regardless of rank movement.
                actual_obs = rank_panel.notna().sum(axis=1)
                coverage_ok = (coverage >= 0.80) | (actual_obs < 15)
                is_burst = (peak_improvement_30 >= 40) & coverage_ok & (sustained_count >= 8)

                is_worse_than_median = recent10.gt(median_per_ticker, axis=0)
                sustained_worse_count = is_worse_than_median.sum(axis=1)
                is_crater = (peak_improvement_30 >= 40) & coverage_ok & (sustained_worse_count >= 8)

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
        # Crater: peak drop of >=40 global ranks at any point in last 30d, currently near worst
        leaderboard["crater_30d"]            = leaderboard["ticker"].map(is_crater).fillna(False)
        leaderboard["global_rank_drop_30d"]  = leaderboard["ticker"].map(
            lambda t: int(peak_improvement_30.get(t, 0)) if (is_crater.get(t, False)) else 0
        ).fillna(0).astype(int)

        return leaderboard

    if historical:
        leaderboard = _attach_motion(leaderboard, deltas_by_period, historical)
        burst_count = int(leaderboard['burst_30d'].sum())
        crater_count = int(leaderboard['crater_30d'].sum())
    else:
        print("  WARNING: No historical data — skipping motion/burst/crater computation")
        for col in ["avg_rank_delta_7d", "avg_weight_flow_7d", "avg_rank_delta_30d",
                    "global_rank_delta_30d", "global_rank_peak_30d", "global_rank_best_30d",
                    "etf_count_30d_ago", "etf_count_delta_30d", "burst_30d", "crater_30d",
                    "global_rank_drop_30d"]:
            leaderboard[col] = 0 if col not in ("burst_30d", "crater_30d") else False
        burst_count = 0
        crater_count = 0
    print(f"  burst_30d:      {burst_count} tickers with >=40 peak rank improvement")
    print(f"  crater_30d:     {crater_count} tickers with >=40 peak rank drop")

    # ── Multi-period rank delta and ETF count delta ───────────────────────────
    # Mirrors score_deltas_by_period so the frontend can switch all Change sections
    # using a single changesPeriod picker (not just Score Gainers/Losers).
    RANK_DELTA_PERIODS: list[int] = list(cfg.history.delta_periods_days)
    global_rank_delta_by_period_map: dict[int, "pd.Series"] = {}
    etf_count_past_by_period_map:  dict[int, "pd.Series"] = {}

    if historical:
        _dates_sorted = sorted(historical.keys())
        _today_dt     = _dates_sorted[-1]

        for _n in RANK_DELTA_PERIODS:
            # ── Global rank delta ──────────────────────────────────────────
            _wstart = _today_dt - pd.Timedelta(days=_n)
            _wcols  = [c for c in _dates_sorted if c >= _wstart]
            if len(_wcols) >= 2:
                _rp_rows = {}
                for _d in _wcols:
                    _snap = historical[_d]
                    if "leaderboard_rank" in _snap.columns:
                        _rp_rows[_d] = _snap.set_index("ticker")["leaderboard_rank"]
                if len(_rp_rows) >= 2:
                    _panel = pd.DataFrame(_rp_rows)
                    global_rank_delta_by_period_map[_n] = (
                        _panel.iloc[:, 0] - _panel.iloc[:, -1]
                    ).round(0)  # positive = improved (rank number went down)

            # ── ETF count at period start (for delta computation) ──────────
            if len(_dates_sorted) >= 2:
                _target = _today_dt - pd.Timedelta(days=_n)
                _past   = min(_dates_sorted, key=lambda d: abs((d - _target).total_seconds()))
                _snap   = historical[_past]
                if "etf_count" in _snap.columns:
                    etf_count_past_by_period_map[_n] = _snap.set_index("ticker")["etf_count"]

        print(f"  multi-period rank delta: {len(global_rank_delta_by_period_map)} periods computed")


    # ── Momentum Regime ───────────────────────────────────────────────────
    # Rebuilt on non-cancelling components after the velocity retirement:
    # per-ETF mean 7d rank delta (positive = funds are improving their
    # positioning) combined with the score streak direction.
    def _classify_regime(row):
        streak = row.get("score_streak", 0) or 0
        rd7 = row.get("avg_rank_delta_7d", 0) or 0
        if streak > 3 and rd7 >= 2:
            return "accelerating"
        elif streak > 0 and rd7 > 0:
            return "rising"
        elif streak < -3 and rd7 <= -2:
            return "declining"
        elif streak < 0 and rd7 < 0:
            return "weakening"
        else:
            return "stable"

    leaderboard["momentum_regime"] = leaderboard.apply(_classify_regime, axis=1)
    print(f"  momentum_regime: {leaderboard['momentum_regime'].value_counts().to_dict()}")

    # ── Tier Breadth — how many distinct strategy types co-hold this name ────
    # 5 = held by all five (Scout/Quant/Quality/Trend/Core); 1 = mono-tier.
    # Higher breadth = more independent strategy types confirming the name.
    leaderboard["tier_breadth"] = leaderboard["tiers"].fillna("").apply(
        lambda s: len([t for t in s.split(" + ") if t.strip()])
    ).astype(int)

    # ── Quality Adoption (30d) ────────────────────────────────────────────
    # Quality-tier ETFs (from config.yaml) screen on free-cash-flow &
    # profitability. When a momentum/scout name picks up a Quality cosign,
    # that's institutional validation.
    # quality_defected_30d was retired (2026-08 signal study: its forward
    # lift flipped sign between evaluation grids — unreliable).
    QUALITY_ETFS = {
        t for t, e_info in cfg.etf_lookup().items() if getattr(e_info, "tier", None) == "Quality"
    }

    def _quality_change(historical: dict, leaderboard: pd.DataFrame) -> pd.Series:
        """Returns adopted_mask per ticker for Quality vs ~30d ago."""
        if not historical:
            return pd.Series(dtype=bool)
        dates_sorted = sorted(historical.keys())
        if len(dates_sorted) < 2:
            return pd.Series(dtype=bool)
        target_past = dates_sorted[-1] - pd.Timedelta(days=30)
        past_date = min(dates_sorted, key=lambda d: abs((d - target_past).total_seconds()))
        # held_by 30d ago — split on ", " to get a set of ETF tickers
        past_lb = historical[past_date]
        if "held_by" not in past_lb.columns:
            return pd.Series(dtype=bool)
        past_held = past_lb.set_index("ticker")["held_by"].apply(
            lambda s: set(t.strip() for t in str(s).split(",") if t.strip())
        )
        today_held = leaderboard.set_index("ticker")["held_by"].apply(
            lambda s: set(t.strip() for t in str(s).split(",") if t.strip())
        )
        adopted = {}
        all_tickers = set(today_held.index) | set(past_held.index)
        for t in all_tickers:
            now = today_held.get(t, set())
            then = past_held.get(t, set())
            now_q = now & QUALITY_ETFS
            then_q = then & QUALITY_ETFS
            adopted[t] = bool(now_q - then_q)        # gained at least one Quality ETF
        return pd.Series(adopted)

    if historical:
        q_adopt = _quality_change(historical, leaderboard)
        leaderboard["quality_adopted_30d"] = leaderboard["ticker"].map(q_adopt).fillna(False).astype(bool)
    else:
        leaderboard["quality_adopted_30d"] = False
    print(f"  tier_breadth:   max={int(leaderboard['tier_breadth'].max())} · "
          f"quality_adopted_30d={int(leaderboard['quality_adopted_30d'].sum())}")

    # ── §31 Apex predictive overlay — bounded temporal kicker (apex mode) ─────
    # apex_score = final_score × (1 + 0.25·tanh(velocity_z/2) + 0.10·tanh(ignition_z/2))
    # Does NOT touch leaderboard_rank or row sort order — apex_rank is a
    # separate column the UI can sort by. Reuses the score panel computed
    # above for streaks.
    if cfg.scoring_mode == "apex" and historical:
        _ax = cfg.apex
        _overlay_kwargs: dict = {}
        if _ax is not None:
            _overlay_kwargs = {
                "conv_floor": _ax.conv_floor,
                "conv_cap":   _ax.conv_cap,
                "conv_gamma": _ax.conv_gamma,
            }
        leaderboard = hist.predictive_overlay(leaderboard, score_pnl, **_overlay_kwargs)
        adjusted = int((leaderboard["apex_score"] != leaderboard["final_score"]).sum())
        print(f"  apex overlay:   velocity_z range "
              f"[{leaderboard['velocity_z'].min():.2f}, {leaderboard['velocity_z'].max():.2f}] · "
              f"{adjusted} scores adjusted")
    else:
        # Neutral columns — JSON schema stays stable across modes
        leaderboard["velocity_z"] = 0.0
        leaderboard["ignition_z"] = 0.0
        leaderboard["apex_score"] = leaderboard["final_score"]
        leaderboard["apex_rank"] = leaderboard["leaderboard_rank"]

    # ── §31.1 Conviction gate on apex_score (breadth-inflation guard) ─────────
    # A ticker held as a filler by many ETFs (low avg_conviction, mediocre ranks)
    # can outscore a true conviction name via raw breadth. The gate multiplies
    # apex_score by g = clip(avg_conviction, 0.40, 1.10), with an escape hatch
    # for names whose median per-ETF rank is ≤ 10 (g ≥ 1.0) so that genuine
    # top-of-book positions (e.g. MU #1/#1/#2, SNDK five #1 ranks) are protected.
    # final_score is NOT touched — only apex_score / apex_rank are affected.
    if cfg.scoring_mode == "apex":
        # Compute median per-ETF rank per ticker from today's latest snapshot
        _median_etf_rank = (
            latest.groupby("ticker")["rank"].median()
            .rename("median_etf_rank")
        )
        leaderboard = leaderboard.merge(
            _median_etf_rank.reset_index(), on="ticker", how="left"
        )
        leaderboard["median_etf_rank"] = leaderboard["median_etf_rank"].fillna(9999.0)

        # Base gate: clip avg_conviction to [0.40, 1.10]
        _conv = leaderboard["avg_conviction"].fillna(0.40)
        _g = _conv.clip(lower=0.40, upper=1.10)

        # Escape: if median_etf_rank ≤ 10, floor g at 1.0
        _top_of_book = leaderboard["median_etf_rank"] <= 10
        _g = _g.where(~_top_of_book, other=_g.clip(lower=1.0))

        leaderboard["conviction_gate"] = _g.round(2)
        leaderboard["apex_score"] = (
            leaderboard["apex_score"] * leaderboard["conviction_gate"]
        ).clip(lower=0.0).round().astype("int64")

        # Recompute apex_rank from gated scores
        _order = leaderboard.sort_values(
            ["apex_score", "etf_count", "total_weight"],
            ascending=[False, False, False],
        ).index
        leaderboard["apex_rank"] = (
            pd.Series(range(1, len(leaderboard) + 1), index=_order).astype("int64")
        )

        # Build log
        _gated_below_1 = int((_g < 1.0).sum())
        _g_min = float(_g.min())
        _g_median = float(_g.median())
        _top10 = (
            leaderboard.sort_values("apex_rank")
            .head(10)[["ticker", "apex_score", "conviction_gate", "etf_count"]]
            .to_string(index=False)
        )
        print(
            f"\n  conviction gate: {_gated_below_1} tickers gated below 1.0 · "
            f"g_min={_g_min:.2f} · g_median={_g_median:.2f}"
        )
        print(f"  apex top-10 after gate:\n{_top10}")
    else:
        leaderboard["conviction_gate"] = 1.0
        leaderboard["median_etf_rank"] = leaderboard["ticker"].map(
            latest.groupby("ticker")["rank"].median()
        ).fillna(9999.0)

    # ── Attach metadata (sector, industry, country) for flow analysis ─────────
    def _attach_metadata(leaderboard: pd.DataFrame) -> pd.DataFrame:
        """Merge ticker metadata (sector, industry, country) from cached CSV."""
        meta_path = Path(__file__).resolve().parent.parent / "data" / "ticker_metadata.csv"
        try:
            meta = pd.read_csv(meta_path)
            leaderboard = leaderboard.merge(meta, on="ticker", how="left")
            for col in ["sector", "industry", "country"]:
                leaderboard[col] = leaderboard[col].fillna("Unknown")
        except FileNotFoundError:
            print(f"  WARNING: {meta_path} not found — metadata unavailable (sector/country flow will be empty)")
            for col in ["sector", "industry", "country", "market_cap_usd"]:
                leaderboard[col] = "Unknown" if col != "market_cap_usd" else None
        return leaderboard

    leaderboard = _attach_metadata(leaderboard)

    # ── Compute flow aggregations by sector and country ──────────────────────
    def _compute_flow(leaderboard: pd.DataFrame, dim: str) -> list[dict]:
        """For each value of `dim` (sector or country), aggregate net fund-flow
        exposure: how many ETF positions were added/removed over 30d.
        (Formerly velocity-weighted; the velocity composite was retired 2026-08.)"""
        lb = leaderboard[leaderboard["etf_count"] >= 2].copy()
        if lb.empty or dim not in lb.columns:
            return []
        # Exclude tickers with no metadata — they'd show as a misleading "Unknown" bucket
        lb = lb[lb[dim] != "Unknown"]
        if lb.empty:
            return []
        g = lb.groupby(dim).agg(
            net_funds_delta=("etf_count_delta_30d", "sum"),
            avg_funds_delta=("etf_count_delta_30d", "mean"),
            names=("ticker", "count"),
            total_weight=("total_weight", "sum"),
            burst_count=("burst_30d", "sum"),
            crater_count=("crater_30d", "sum"),
            hc_count=("flag", lambda s: (s == "HIGH_CONVICTION").sum()),
        ).reset_index().rename(columns={dim: "label"}).sort_values("net_funds_delta", ascending=False)
        return g.round(2).to_dict(orient="records")

    flow = {
        "by_sector":  _compute_flow(leaderboard, "sector"),
        "by_country": _compute_flow(leaderboard, "country"),
    }
    (output_dir / "flow.json").parent.mkdir(parents=True, exist_ok=True)
    _write_json_atomic(output_dir / "flow.json", _dumps(flow, separators=(",", ":")))
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
                jaccard[i][i] = 1.0
                shared[i][i] = len(A)
                for j in range(i + 1, n):
                    b = etf_list[j]
                    B = etf_holdings[b]
                    inter = len(A & B)
                    union = len(A | B) or 1
                    jac = round(inter / union, 4)
                    jaccard[i][j] = jaccard[j][i] = jac
                    shared[i][j] = shared[j][i] = inter
            overlap = {
                "etfs": etf_list,
                "jaccard": jaccard,
                "shared": shared,
                "sizes": {e: len(etf_holdings[e]) for e in etf_list},
            }
            _write_json_atomic(output_dir / "etf_overlap.json", _dumps(overlap, separators=(",", ":")))
            print(f"  etf_overlap.json: {n}×{n} matrix (Jaccard + raw counts)")
    except Exception as e:
        print(f"  etf_overlap.json: ERROR — {e}")

    # ── leaderboard.json — main payload for the site ──────────────────────────
    lb_records = leaderboard.to_dict(orient="records")

    # Build per-ticker flag history — all signals historized via signal_history()
    # Schema: { ticker: [{d, flag, rank, vs?, burst?, n?, st?, dv?, qa?, qd?}] }
    print("\nComputing per-day signal history…")
    flag_history: dict[str, list] = hist.signal_history(raw, historical, cfg)

    # Consistency check: last-date signals vs leaderboard's own computed columns
    if flag_history and historical:
        last_d = max(historical.keys())
        last_d_str = last_d.strftime("%Y-%m-%d")
        # Use the ENRICHED leaderboard (carries today's burst_30d / crater_30d
        # added by _attach_velocity) rather than the raw historical snapshot, which
        # never carries those columns and would always report a false mismatch.
        lb_indexed = leaderboard.set_index("ticker") if "ticker" in leaderboard.columns else None
        mismatch_burst = 0
        mismatch_crater = 0
        n_checked = 0
        for tkr, entries in flag_history.items():
            if not entries:
                continue
            last_e = entries[-1]
            if last_e["d"] != last_d_str:
                continue
            n_checked += 1
            hist_burst = bool(last_e.get("burst", False))
            hist_crater = bool(last_e.get("crater", False))
            if lb_indexed is not None and tkr in lb_indexed.index:
                lb_row = lb_indexed.loc[tkr]
                if isinstance(lb_row, pd.DataFrame):
                    lb_row = lb_row.iloc[0]
                lb_burst = bool(lb_row.get("burst_30d", False))
                if lb_burst != hist_burst:
                    mismatch_burst += 1
                lb_crater = bool(lb_row.get("crater_30d", False))
                if lb_crater != hist_crater:
                    mismatch_crater += 1
        if n_checked > 0:
            print(f"  signal consistency: {n_checked} tickers checked · "
                  f"burst mismatches={mismatch_burst} ({100*mismatch_burst/n_checked:.1f}%) · "
                  f"crater mismatches={mismatch_crater} ({100*mismatch_crater/n_checked:.1f}%)")

    # Attach per-period score deltas AND multi-period rank/ETF-count deltas to every record
    for r in lb_records:
        t = r.get("ticker", "")
        # Score deltas (existing)
        r["score_deltas_by_period"] = {}
        for p in all_periods:
            v = score_deltas_by_period.get(p, {}).get(t)
            if v is None or (isinstance(v, float) and v != v):
                r["score_deltas_by_period"][str(p)] = None
            else:
                r["score_deltas_by_period"][str(p)] = round(float(v), 4)
        # Global rank delta by period — positive = climbed (rank number improved)
        r["global_rank_delta_by_period"] = {}
        for _n, _s in global_rank_delta_by_period_map.items():
            v = _s.get(t) if t in _s.index else None
            r["global_rank_delta_by_period"][str(_n)] = int(v) if v is not None and v == v else 0
        # ETF count delta by period
        r["etf_count_delta_by_period"] = {}
        current_etf = int(r.get("etf_count", 0) or 0)
        for _n, _ps in etf_count_past_by_period_map.items():
            past = int(_ps.get(t)) if t in _ps.index and not pd.isna(_ps.get(t)) else current_etf
            r["etf_count_delta_by_period"][str(_n)] = current_etf - past
    _write_json_atomic(output_dir / "leaderboard.json", _dumps(lb_records, separators=(",", ":")))
    # Write flag_history to separate file (keyed by ticker) to reduce leaderboard.json payload
    _write_json_atomic(output_dir / "flag_history.json", _dumps(flag_history, separators=(",", ":")))
    print(f"  flag_history:   {sum(1 for t in flag_history if flag_history[t])} tickers with history (separate file)")
    # ── holdings_latest.json — per-(ETF, ticker) detail with rank deltas ──────
    if not latest.empty:
        latest_out = latest[[
            "ETF_Ticker", "ticker", "name", "weight", "rank", "tier", "is_new",
            "base_score", "new_bonus", "score", "Holdings_As_Of"
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
        # Round numeric float columns to 4 decimals to bound JSON payload size
        # (Req 6.2). Do NOT round `rank` or any `rank_delta*` integer-ish columns.
        _float_round_cols = ["weight", "base_score", "new_bonus", "score"] + [
            c for c in latest_out.columns if c.startswith("weight_flow")
        ]
        for _c in _float_round_cols:
            if _c in latest_out.columns:
                latest_out[_c] = latest_out[_c].round(4)
        (output_dir / "holdings_latest.json").parent.mkdir(parents=True, exist_ok=True)
        _write_json_atomic(
            output_dir / "holdings_latest.json",
            _dumps(latest_out.to_dict(orient="records"), separators=(",", ":"))
        )

        # Compute exited holdings for each period and serialize
        print("\nComputing exited holdings for all periods…")
        holdings_exits = {}
        for n_days in cfg.history.delta_periods_days:
            try:
                exits_df = hist.compute_exits(raw, cfg, lookback_days=n_days)
                if not exits_df.empty:
                    for etf, g in exits_df.groupby("ETF_Ticker"):
                        etf_exits = holdings_exits.setdefault(etf, {})
                        records = g[["ticker", "name", "rank_then", "weight_then"]].to_dict(orient="records")
                        # Round weight_then to 4 decimals
                        for r in records:
                            if r["weight_then"] is not None and r["weight_then"] == r["weight_then"]:
                                r["weight_then"] = round(float(r["weight_then"]), 4)
                        etf_exits[str(n_days)] = records
            except Exception as e:
                print(f"  {n_days}d exits: ERROR — {e}")
        _write_json_atomic(
            output_dir / "holdings_exits.json",
            _dumps(holdings_exits, separators=(",", ":"))
        )
        print(f"  holdings_exits.json:  {len(holdings_exits)} ETFs with exit data")

    # Top accumulation movers (15 names, held by 2+ ETFs) — most ETF positions
    # added over 30d. Replaces the retired top_velocity panel (2026-08 study:
    # velocity had no forward lift; ETF accumulation is the cleaner
    # conviction-flow measure).
    if 'etf_count_delta_30d' in leaderboard.columns:
        top_acc = leaderboard[leaderboard['etf_count'] >= 2].sort_values(
            ['etf_count_delta_30d', 'global_rank_delta_30d'], ascending=False).head(15)
        chg['top_accumulation'] = [
            {
                'ticker':               str(r['ticker']),
                'company':              str(r.get('company', '')),
                'etf_count_delta_30d':  int(r['etf_count_delta_30d']),
                'avg_rank_delta_7d':    float(r['avg_rank_delta_7d']),
                'global_rank_delta_30d': int(r.get('global_rank_delta_30d', 0)),
                'burst_30d':            bool(r.get('burst_30d', False)),
                'crater_30d':           bool(r.get('crater_30d', False)),
                'final_score':          int(r['final_score']),
                'etf_count':            int(r['etf_count']),
                'tiers':                str(r.get('tiers', '')),
            }
            for _, r in top_acc.iterrows()
        ]

    # ── Multi-period universe exits ───────────────────────────────────────────
    # The 1-day universe_exits above is computed from full uncapped leaderboards,
    # but the frontend's multi-day "Dropped from Universe" table previously scanned
    # score_history.json (capped at 600 tickers → exited names truncated). Compute
    # exits for every period the Changes-tab picker exposes, using the same full
    # leaderboards, so 7d/30d/90d/YTD drops actually render.
    try:
        exit_periods = list(cfg.history.delta_periods_days) + ["YTD"]
        chg['universe_exits_by_period'] = hist.compute_universe_exits_by_period(
            historical, leaderboard, exit_periods, top_n=cfg.history.changelog_top_n,
        )
        total_exits = sum(len(v) for v in chg['universe_exits_by_period'].values())
        print(f"  universe_exits_by_period: {total_exits} exits across {len(exit_periods)} periods")
    except Exception as e:
        print(f"  universe_exits_by_period: ERROR — {e}")
        chg['universe_exits_by_period'] = {str(p): [] for p in exit_periods}

    # ── changelog.json — entries / exits / movers ─────────────────────────────
    _write_json_atomic(output_dir / "changelog.json", _dumps(chg, indent=2))

    # ── score_history.parquet + JSON — for sparklines ─────────────────────────
    if not score_pnl.empty:
        score_pnl.to_parquet(output_dir / "score_history.parquet")
        # Compact JSON: { ticker: [{ d, s }, ...] }
        # Include ALL tickers that ever appeared in any historical snapshot so that
        # the frontend universeExits getter can detect tickers that have since dropped
        # out of the universe (score > 0 on a past date, missing/zero today).
        # We cap at a generous limit to keep file size reasonable.
        all_historical_tickers = set(score_pnl.index.tolist())
        # Sort by today's score (descending) so the most relevant tickers come first,
        # but include every ticker ever seen — not just current top-N.
        current_tickers_ordered = leaderboard["ticker"].tolist()  # already sorted by score desc
        # Build ordered list: current tickers first (already sorted), then exited tickers
        exited_tickers = sorted(all_historical_tickers - set(current_tickers_ordered))
        all_tickers_ordered = current_tickers_ordered + exited_tickers
        max_history_tickers = 600  # generous cap; exited tickers have sparse series so file stays small
        spark = {}
        for t in all_tickers_ordered[:max_history_tickers]:
            if t in score_pnl.index:
                series = score_pnl.loc[t].dropna()
                if not series.empty:
                    spark[t] = [{"d": d.strftime("%Y-%m-%d"), "s": round(float(v), 2)}
                                 for d, v in series.items()]
        _write_json_atomic(output_dir / "score_history.json", _dumps(spark, separators=(",", ":")))

    # ── leaderboard.parquet — for DuckDB-WASM time-travel queries ─────────────
    leaderboard.to_parquet(output_dir / "leaderboard.parquet", index=False)

    # ── holdings_history.parquet + JSON (Phase 2) — per-(ETF, ticker, date) ───
    print("\nBuilding holdings history…")
    sanitized_raw = cfg.sanitizer.apply(raw)
    # Rank consistently with the leaderboard: configured ETFs only, positive
    # weights only (zero-weight rows previously sorted into the ranking).
    sanitized_raw = sanitized_raw[sanitized_raw["ETF_Ticker"].isin(cfg.etf_lookup())]
    sanitized_raw = sanitized_raw[pd.to_numeric(sanitized_raw["weight"], errors="coerce").fillna(0) > 0]
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
    _write_json_atomic(
        output_dir / "holdings_history.json",
        _dumps(holdings_history_json, separators=(",", ":"))
    )
    print(f"  holdings_history.json: {len(holdings_history_json)} tickers")

    # ── Write individual ticker history JSON files ────────────────────────────
    print("\nBuilding individual ticker history files...")
    history_dir = output_dir / "history"
    if history_dir.exists():
        shutil.rmtree(history_dir)
    history_dir.mkdir(parents=True, exist_ok=True)

    _UNSAFE_FN_RE = re.compile(r"[^A-Za-z0-9._-]")
    _WIN_RESERVED = {"CON", "PRN", "AUX", "NUL"} | {f"COM{i}" for i in range(1, 10)} | {f"LPT{i}" for i in range(1, 10)}

    def _safe_detail_stem(ticker: str) -> str:
        stem = _UNSAFE_FN_RE.sub("_", ticker).rstrip(". ")
        if not stem:
            stem = "_"
        if stem.upper() in _WIN_RESERVED:
            stem = stem + "_"
        return stem

    # Group all holdings history at once to avoid repeated queries
    ticker_holdings_history: dict[str, dict] = {}
    for (t, etf), g in hist_window.groupby(["ticker", "ETF_Ticker"]):
        row_list = [
            {"d": str(d)[:10], "r": int(r), "w": round(float(w), 6)}
            for d, r, w in zip(g["Holdings_As_Of"], g["rank"], g["weight"])
        ]
        ticker_holdings_history.setdefault(t, {})[etf] = row_list

    # Compute global rank history for each ticker from historical snapshots
    ticker_rank_history: dict[str, list] = {}
    if historical:
        for d in sorted(historical.keys()):
            lb_snap = historical[d]
            d_str = d.strftime("%Y-%m-%d")
            total_tracked = len(lb_snap)
            for row in lb_snap.to_dict(orient="records"):
                t = row["ticker"]
                rank = row.get("leaderboard_rank")
                if rank is not None and rank == rank:  # check not NaN
                    entry = {
                        "d": d_str,
                        "rank": int(rank),
                        "total": total_tracked
                    }
                    ticker_rank_history.setdefault(t, []).append(entry)

    # All unique tickers with history
    all_tickers = set(score_pnl.index.tolist()) | set(hist_window["ticker"].unique())

    for t in all_tickers:
        ticker_score_hist = []
        if t in score_pnl.index:
            series = score_pnl.loc[t].dropna()
            ticker_score_hist = [{"d": d.strftime("%Y-%m-%d"), "s": round(float(v), 2)}
                                 for d, v in series.items()]
                                 
        ticker_holdings = ticker_holdings_history.get(t, {})
        ticker_flag_hist = flag_history.get(t, [])
        ticker_rank_hist = ticker_rank_history.get(t, [])
        
        payload = {
            "ticker": t,
            "scoreHistory": ticker_score_hist,
            "holdingsHistory": ticker_holdings,
            "flagHistory": ticker_flag_hist,
            "rankHistory": ticker_rank_hist
        }
        
        stem = _safe_detail_stem(t)
        _write_json_atomic(history_dir / f"{stem}.json", _dumps(payload, separators=(",", ":")))
    print(f"  Wrote {len(all_tickers)} individual ticker history files.")

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

    # §28 Detect immature ETFs (< etf_maturity_days of history)
    etf_maturity_days = cfg.etf_maturity_days
    etf_first_seen = raw.groupby("ETF_Ticker")["Holdings_As_Of"].min()
    etf_last_seen  = raw.groupby("ETF_Ticker")["Holdings_As_Of"].max()
    etf_span = (pd.to_datetime(etf_last_seen) - pd.to_datetime(etf_first_seen)).dt.days
    immature_etfs = sorted(etf_span[etf_span < etf_maturity_days].index.tolist())

    # Per-ETF staleness alarm (Tier 3 exception)
    # Emit GitHub Actions ::warning:: for ETFs whose latest Holdings_As_Of is older than 5 trading days
    STALE_TRADING_DAYS = 5
    raw_dt2 = raw.copy()
    raw_dt2["Holdings_As_Of"] = pd.to_datetime(raw_dt2["Holdings_As_Of"], errors="coerce")
    etf_latest_as_of = raw_dt2.groupby("ETF_Ticker")["Holdings_As_Of"].max()
    today_ts = pd.Timestamp.now().normalize()
    # Approximate trading days as calendar days × 5/7
    stale_etfs = []
    for etf_ticker, last_as_of in etf_latest_as_of.items():
        if pd.isna(last_as_of):
            continue
        cal_days = (today_ts - last_as_of).days
        approx_trading_days = cal_days * 5 / 7
        if approx_trading_days > STALE_TRADING_DAYS:
            stale_etfs.append({
                "etf": etf_ticker,
                "last_as_of": last_as_of.strftime("%Y-%m-%d"),
                "calendar_days_old": cal_days,
            })
            if os.environ.get("GITHUB_ACTIONS"):
                print(f"::warning::ETF {etf_ticker} holdings are stale: last Holdings_As_Of={last_as_of.date()} ({cal_days} calendar days ago)")
    if stale_etfs:
        print(f"\n⚠ Stale ETFs ({len(stale_etfs)}): {[s['etf'] for s in stale_etfs]}")

    # ── Markets data freshness summary (charter v2 Part 2) ──────────────
    # After the workflow's ingest steps run, surface per-asset source +
    # last-month-date directly in metadata.json. A skeptic on the live site
    # can read this and confirm: (a) recent assets carry a live source
    # (fred:.. or yfinance:..), not Mega_Markets_Historical.xlsx; (b) asof
    # advances each build; (c) fx/cpi/rates sections are non-empty.
    markets_freshness: dict[str, Any] = {"available": False}
    market_returns_path = output_dir / "market_returns.json"
    if market_returns_path.exists():
        try:
            mr = json.loads(market_returns_path.read_text(encoding="utf-8"))
            assets   = mr.get("assets", {})
            fx       = mr.get("fx", {})
            cpi      = mr.get("cpi", {})
            rates    = mr.get("rates", {})

            # Derive the set of asset_ids that *should* carry a live source from
            # the canonical asset registry — never hardcoded.  An asset is
            # "live-eligible" when its registry entry declares a yfinance/FRED
            # source AND it isn't an FX pair (FX series live in the top-level
            # "fx" section, not in "assets"). Any registry asset still labelled
            # "Mega_Markets_Historical*" after the live merge is a holdout —
            # the verdict block below names it explicitly so the dashboard chip
            # tooltip can surface which assets are blocking LIVE_MERGE_HEALTHY.
            try:
                from conviction.markets_history import ASSET_REGISTRY as _MR_REGISTRY
                live_eligible_keys: set[str] = {
                    spec["key"]
                    for spec in _MR_REGISTRY
                    if spec.get("source_type") in {"yfinance", "fred"}
                    and spec.get("return_type") != "fx"
                }
            except Exception as _reg_err:
                # Registry import failure must not collapse the whole build —
                # fall back to an empty set (verdict will report 0 holdouts and
                # use the live/excel counts alone, matching the pre-fix
                # behaviour). Charter v2 Part 3: external-data failures must
                # never block the rest of the build.
                print(f"  WARN: could not import ASSET_REGISTRY for live-eligible derivation: {_reg_err}")
                live_eligible_keys = set()

            by_source: dict[str, int] = {}
            stale_assets: list[dict] = []
            holdout_keys: list[str] = []
            holdout_names: list[str] = []
            today_ym = datetime.now(timezone.utc).strftime("%Y-%m")
            for aid, av in assets.items():
                meta = av.get("meta", {}) or {}
                src = meta.get("source", "unknown")
                # Bucket by source family (fred / yfinance / Excel / unknown)
                family = ("fred" if src.startswith("fred:")
                          else "yfinance" if src.startswith("yfinance:")
                          else "excel" if "Mega_Markets_Historical" in src
                          else "other")
                by_source[family] = by_source.get(family, 0) + 1

                # Live-merge holdout: a registry-eligible asset whose live
                # fetch fell through to the Excel deep-history label. Keep the
                # honest Excel label on the asset (no fake live source) AND
                # name the asset in the verdict so the chip can call it out.
                if (
                    aid in live_eligible_keys
                    and isinstance(src, str)
                    and src.startswith("Mega_Markets_Historical")
                ):
                    holdout_keys.append(aid)
                    holdout_names.append(meta.get("name") or aid)

                last = meta.get("last", "")
                if last and last < today_ym[:7]:
                    # asset's last month-of-data is older than this calendar month
                    try:
                        last_ts = pd.Timestamp(last + "-01") + pd.offsets.MonthEnd(0)
                        age_days = (pd.Timestamp.utcnow().tz_localize(None) - last_ts).days
                        if age_days > 60:
                            stale_assets.append({"asset": aid, "last": last, "age_days": int(age_days)})
                    except Exception:
                        pass

            live_source_count = by_source.get("fred", 0) + by_source.get("yfinance", 0)
            excel_only_count  = by_source.get("excel", 0)

            # Verdict policy (design Fix Implementation #10):
            #   HEALTHY  — no holdouts AND live sources outnumber excel-only.
            #   DEGRADED — at least one holdout, OR live ≤ excel but live > 0.
            #   FAILED   — no live sources at all (cold start / total outage).
            if not holdout_keys and live_source_count > excel_only_count:
                verdict = "LIVE_MERGE_HEALTHY"
            elif live_source_count > 0:
                verdict = "LIVE_MERGE_DEGRADED"
            else:
                verdict = "LIVE_MERGE_FAILED"

            markets_freshness = {
                "available":     True,
                "asof":          mr.get("asof"),
                "asset_count":   len(assets),
                "by_source":     by_source,
                "fx_pairs":      sorted(fx.keys()),
                "cpi_series":    sorted(cpi.keys()),
                "rates_series":  sorted(rates.keys()),
                "stale_assets":  stale_assets,
                "self_living_check": {
                    "live_source_count": live_source_count,
                    "excel_only_count":  excel_only_count,
                    # Honest verdict — names every live-eligible asset that
                    # fell through to its Excel label so the chip tooltip can
                    # call them out by name (display) and by id (programmatic).
                    "holdouts":      sorted(holdout_names),
                    "holdout_keys":  sorted(holdout_keys),
                    "verdict":       verdict,
                },
            }
            print(f"\n  Markets freshness: asof={mr.get('asof')}  "
                  f"sources={by_source}  "
                  f"fx={len(fx)}  cpi={len(cpi)}  rates={len(rates)}  "
                  f"holdouts={len(holdout_keys)}  "
                  f"verdict={markets_freshness['self_living_check']['verdict']}")
        except Exception as e:
            print(f"  WARN: could not read market_returns.json for freshness summary: {e}")

    # ── Universe expansion detection (15→30 ETF migration, May 2026) ──────────
    # The tracked-ETF universe doubled in stages (16 → 21 → 23 → 29 → 30
    # around 2026-05-18..22). Absolute ranks are universe-relative, so they
    # are structurally depressed after each jump; score comparisons spanning
    # the boundary mix two different universes. Emit the per-date fund count
    # so charts can annotate the break and readers interpret history right.
    fund_count_by_date: list[dict] = []
    largest_jump: dict | None = None
    try:
        _raw_dt2 = raw_dt.dropna(subset=["Holdings_As_Of"])
        _fc = _raw_dt2.groupby("Holdings_As_Of")["ETF_Ticker"].nunique().sort_index()
        if len(_fc) >= 2:
            _jumps = _fc.diff()
            # Ignore near-empty partial snapshots (e.g. a scrape day where
            # only one fund reported before the rest) — the migration marker
            # must reflect real universe growth, not scrape timing artifacts.
            _real = _jumps[(_jumps.index.isin(_fc.index)) & (_fc > 3)]
            if not _real.empty:
                _jd = _real.idxmax()
                if _real.max() > 0:
                    largest_jump = {
                        "date": pd.Timestamp(_jd).strftime("%Y-%m-%d"),
                        "funds_before": int(_fc.iloc[_fc.index.get_loc(_jd) - 1]),
                        "funds_after": int(_fc.loc[_jd]),
                    }
            fund_count_by_date = [
                {"date": pd.Timestamp(d).strftime("%Y-%m-%d"), "funds": int(c)}
                for d, c in _fc.items()
            ]
    except Exception as e:
        print(f"  WARNING: universe-expansion detection failed: {e}")

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
        # §28 Maturity gate metadata — drives the UI banner
        "etf_maturity_days": etf_maturity_days,
        "immature_etfs": immature_etfs,
        "stale_etfs": stale_etfs,
        # Universe expansion annotation (15→30 ETF migration) — drives chart
        # markers and the methodology data-note
        "universe_expansion": {
            "largest_jump": largest_jump,
            "fund_count_by_date": fund_count_by_date,
        },
        # Charter v2 Part 2 — live-merge verification, surfaced on the site
        "markets_data_freshness": markets_freshness,
        "scoring_mode": cfg.scoring_mode,
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
    _write_json_atomic(output_dir / "metadata.json", _dumps(metadata, indent=2))

    # Quick summary
    print(f"\n✓ Wrote outputs to {output_dir}/")
    for name in ["leaderboard.json", "holdings_latest.json", "holdings_exits.json", "changelog.json",
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
    p = argparse.ArgumentParser(description="Build Conviction Labs site artifacts")
    p.add_argument("--source", default=DEFAULT_SOURCE)
    p.add_argument("--output", default="docs/data")
    p.add_argument("--config", default="config.yaml")
    args = p.parse_args(argv)
    build(args.source, Path(args.output), Path(args.config))
    return 0


if __name__ == "__main__":
    sys.exit(main())
