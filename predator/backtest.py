"""
Backtest the Predator Protocol scoring algorithm.

For each historical leaderboard snapshot date D:
  1. Identify buy signals: HC entrants, BURST triggers, top-N by score, top-N by velocity
  2. Look up each name's price on D (from data/markets/yf_<TICKER>.parquet)
     — skip if missing (international names without yfinance coverage)
  3. Look up price 30 days later
  4. Compute equal-weighted return

Strategies:
  - hc_entry:       buy each name on the day it first enters HC
  - burst_trigger:  buy each name on the day burst_30d flips True
  - top10_score:    buy top 10 by final_score each Monday
  - top10_velocity: buy top 10 by velocity_score each Monday
  - baseline:       equal-weighted SPX (S&P 500) for comparison

Output: docs/data/backtest.json
"""
from __future__ import annotations
import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
import numpy as np

if sys.platform.startswith("win"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except AttributeError:
        pass


class _SafeEncoder(json.JSONEncoder):
    def iterencode(self, o, _one_shot=False):
        return super().iterencode(self._sanitise(o), _one_shot)
    def _sanitise(self, obj):
        if isinstance(obj, float):
            return None if (math.isnan(obj) or math.isinf(obj)) else obj
        if isinstance(obj, dict):
            return {k: self._sanitise(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [self._sanitise(v) for v in obj]
        return obj


def _dumps(obj, **kw) -> str:
    kw.setdefault("cls", _SafeEncoder)
    return json.dumps(obj, **kw)


def _load_price_cache(markets_dir: Path) -> dict[str, pd.DataFrame]:
    """Load all yf_*.parquet files into a dict keyed by symbol."""
    cache: dict[str, pd.DataFrame] = {}
    if not markets_dir.exists():
        return cache
    for p in markets_dir.glob("yf_*.parquet"):
        sym = p.stem[3:]  # strip "yf_"
        try:
            df = pd.read_parquet(p)[["Date", "Close"]].dropna()
            df["Date"] = pd.to_datetime(df["Date"])
            df = df.sort_values("Date").set_index("Date")
            cache[sym] = df
        except Exception:
            pass
    return cache


def _get_price(cache: dict, ticker: str, date: pd.Timestamp) -> float | None:
    """Return the closing price for ticker on or just after date."""
    df = cache.get(ticker)
    if df is None:
        return None
    future = df[df.index >= date]
    if future.empty:
        return None
    return float(future["Close"].iloc[0])


def _compute_stats(trades: list[dict]) -> dict:
    """Compute summary stats from a list of trade dicts."""
    if not trades:
        return {"n_trades": 0, "win_rate": None, "avg_return": None,
                "max_drawdown": None, "sharpe": None}
    returns = [t["return_pct"] for t in trades if t["return_pct"] is not None]
    if not returns:
        return {"n_trades": len(trades), "win_rate": None, "avg_return": None,
                "max_drawdown": None, "sharpe": None}
    wins = sum(1 for r in returns if r > 0)
    avg_r = float(np.mean(returns))
    std_r = float(np.std(returns)) if len(returns) > 1 else 0.0
    sharpe = float(avg_r / std_r * math.sqrt(252 / 30)) if std_r > 0 else None
    # Max drawdown on cumulative equity curve
    cum = np.cumprod([1 + r for r in returns])
    peak = np.maximum.accumulate(cum)
    dd = (cum - peak) / peak
    max_dd = float(dd.min()) if len(dd) > 0 else None
    return {
        "n_trades": len(returns),
        "win_rate": round(wins / len(returns), 4),
        "avg_return": round(avg_r, 4),
        "max_drawdown": round(max_dd, 4) if max_dd is not None else None,
        "sharpe": round(sharpe, 3) if sharpe is not None else None,
    }


def _cumulative_returns(trades: list[dict]) -> list[dict]:
    """Build a date-sorted cumulative return series from trades."""
    if not trades:
        return []
    valid = [(t["exit_date"], t["return_pct"]) for t in trades
             if t["return_pct"] is not None and t["exit_date"] is not None]
    if not valid:
        return []
    valid.sort(key=lambda x: x[0])
    cum = 1.0
    out = []
    for date, r in valid:
        cum *= (1 + r)
        out.append({"date": date, "total_return": round(cum - 1, 4)})
    return out


def run_backtest(
    history_path: str,
    markets_dir: Path,
    output_path: Path,
    hold_days: int = 30,
) -> None:
    from .scoring import Config, compute_leaderboard
    from . import history as hist

    cfg = Config.from_yaml("config.yaml")
    print(f"Loading history from {history_path}…")
    raw = pd.read_csv(history_path)
    raw["Holdings_As_Of"] = pd.to_datetime(raw["Holdings_As_Of"], errors="coerce")

    print("Loading price cache…")
    price_cache = _load_price_cache(markets_dir)
    print(f"  {len(price_cache)} symbols loaded")

    print("Computing historical leaderboards…")
    historical = hist.historical_leaderboards(raw, cfg)
    dates_sorted = sorted(historical.keys())
    print(f"  {len(dates_sorted)} snapshots")

    if len(dates_sorted) < 2:
        print("ERROR: Need at least 2 snapshots for backtest")
        return

    # ── Strategy 1: HC Entry ──────────────────────────────────────────────
    print("Strategy: hc_entry…")
    hc_trades: list[dict] = []
    prev_hc: set[str] = set()
    for d in dates_sorted:
        lb = historical[d]
        curr_hc = set(lb.loc[lb["flag"] == "HIGH_CONVICTION", "ticker"])
        new_entries = curr_hc - prev_hc
        for ticker in new_entries:
            entry_price = _get_price(price_cache, ticker, d)
            if entry_price is None:
                continue
            exit_date = d + pd.Timedelta(days=hold_days)
            exit_price = _get_price(price_cache, ticker, exit_date)
            ret = (exit_price - entry_price) / entry_price if exit_price else None
            hc_trades.append({
                "date": d.strftime("%Y-%m-%d"),
                "ticker": ticker,
                "entry": round(entry_price, 4),
                "exit": round(exit_price, 4) if exit_price else None,
                "exit_date": exit_date.strftime("%Y-%m-%d"),
                "return_pct": round(ret, 4) if ret is not None else None,
                "days_held": hold_days,
            })
        prev_hc = curr_hc
    print(f"  {len(hc_trades)} trades")

    # ── Strategy 2: BURST Trigger ─────────────────────────────────────────
    print("Strategy: burst_trigger…")
    burst_trades: list[dict] = []
    prev_burst: set[str] = set()
    for d in dates_sorted:
        lb = historical[d]
        if "burst_30d" not in lb.columns:
            continue
        curr_burst = set(lb.loc[lb["burst_30d"] == True, "ticker"])
        new_burst = curr_burst - prev_burst
        for ticker in new_burst:
            entry_price = _get_price(price_cache, ticker, d)
            if entry_price is None:
                continue
            exit_date = d + pd.Timedelta(days=hold_days)
            exit_price = _get_price(price_cache, ticker, exit_date)
            ret = (exit_price - entry_price) / entry_price if exit_price else None
            burst_trades.append({
                "date": d.strftime("%Y-%m-%d"),
                "ticker": ticker,
                "entry": round(entry_price, 4),
                "exit": round(exit_price, 4) if exit_price else None,
                "exit_date": exit_date.strftime("%Y-%m-%d"),
                "return_pct": round(ret, 4) if ret is not None else None,
                "days_held": hold_days,
            })
        prev_burst = curr_burst
    print(f"  {len(burst_trades)} trades")

    # ── Strategy 3: Top-10 Score (weekly rebalance on Mondays) ───────────
    print("Strategy: top10_score…")
    score_trades: list[dict] = []
    mondays = [d for d in dates_sorted if d.weekday() == 0]
    for d in mondays:
        lb = historical[d].head(10)
        for _, row in lb.iterrows():
            ticker = row["ticker"]
            entry_price = _get_price(price_cache, ticker, d)
            if entry_price is None:
                continue
            exit_date = d + pd.Timedelta(days=hold_days)
            exit_price = _get_price(price_cache, ticker, exit_date)
            ret = (exit_price - entry_price) / entry_price if exit_price else None
            score_trades.append({
                "date": d.strftime("%Y-%m-%d"),
                "ticker": ticker,
                "entry": round(entry_price, 4),
                "exit": round(exit_price, 4) if exit_price else None,
                "exit_date": exit_date.strftime("%Y-%m-%d"),
                "return_pct": round(ret, 4) if ret is not None else None,
                "days_held": hold_days,
            })
    print(f"  {len(score_trades)} trades")

    # ── Strategy 4: Top-10 Velocity (weekly rebalance on Mondays) ────────
    print("Strategy: top10_velocity…")
    vel_trades: list[dict] = []
    for d in mondays:
        lb = historical[d]
        if "velocity_score" not in lb.columns:
            continue
        top10 = lb.nlargest(10, "velocity_score")
        for _, row in top10.iterrows():
            ticker = row["ticker"]
            entry_price = _get_price(price_cache, ticker, d)
            if entry_price is None:
                continue
            exit_date = d + pd.Timedelta(days=hold_days)
            exit_price = _get_price(price_cache, ticker, exit_date)
            ret = (exit_price - entry_price) / entry_price if exit_price else None
            vel_trades.append({
                "date": d.strftime("%Y-%m-%d"),
                "ticker": ticker,
                "entry": round(entry_price, 4),
                "exit": round(exit_price, 4) if exit_price else None,
                "exit_date": exit_date.strftime("%Y-%m-%d"),
                "return_pct": round(ret, 4) if ret is not None else None,
                "days_held": hold_days,
            })
    print(f"  {len(vel_trades)} trades")

    # ── Strategy 5: Baseline (SPX buy-and-hold) ───────────────────────────
    print("Strategy: baseline (SPX)…")
    baseline_trades: list[dict] = []
    for d in mondays:
        entry_price = _get_price(price_cache, "SPX", d)
        if entry_price is None:
            continue
        exit_date = d + pd.Timedelta(days=hold_days)
        exit_price = _get_price(price_cache, "SPX", exit_date)
        ret = (exit_price - entry_price) / entry_price if exit_price else None
        baseline_trades.append({
            "date": d.strftime("%Y-%m-%d"),
            "ticker": "SPX",
            "entry": round(entry_price, 4),
            "exit": round(exit_price, 4) if exit_price else None,
            "exit_date": exit_date.strftime("%Y-%m-%d"),
            "return_pct": round(ret, 4) if ret is not None else None,
            "days_held": hold_days,
        })
    print(f"  {len(baseline_trades)} trades")

    # ── Scatter: velocity_score vs realized 30d return ────────────────────
    print("Building velocity scatter…")
    scatter: list[dict] = []
    for d in dates_sorted:
        lb = historical[d]
        if "velocity_score" not in lb.columns:
            continue
        for _, row in lb.iterrows():
            ticker = row["ticker"]
            vs = row.get("velocity_score")
            if vs is None or not math.isfinite(float(vs)):
                continue
            entry_price = _get_price(price_cache, ticker, d)
            if entry_price is None:
                continue
            exit_date = d + pd.Timedelta(days=hold_days)
            exit_price = _get_price(price_cache, ticker, exit_date)
            if exit_price is None:
                continue
            ret = (exit_price - entry_price) / entry_price
            scatter.append({
                "velocity_score": round(float(vs), 2),
                "realized_return_30d": round(ret, 4),
                "ticker": ticker,
                "date": d.strftime("%Y-%m-%d"),
            })
    # Limit scatter to 2000 points for payload size
    if len(scatter) > 2000:
        import random
        random.shuffle(scatter)
        scatter = scatter[:2000]
    print(f"  {len(scatter)} scatter points")

    # ── Assemble output ───────────────────────────────────────────────────
    result = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "hold_days": hold_days,
        "strategies": {
            "hc_entry": {
                "label": "HC Entry",
                "description": "Buy on day ticker first enters HIGH_CONVICTION",
                "trades": hc_trades,
                "cumulative_returns": _cumulative_returns(hc_trades),
                "stats": _compute_stats(hc_trades),
            },
            "burst_trigger": {
                "label": "BURST Trigger",
                "description": "Buy on day burst_30d first flips True",
                "trades": burst_trades,
                "cumulative_returns": _cumulative_returns(burst_trades),
                "stats": _compute_stats(burst_trades),
            },
            "top10_score": {
                "label": "Top-10 Score",
                "description": "Buy top 10 by Final Alpha Score each Monday",
                "trades": score_trades,
                "cumulative_returns": _cumulative_returns(score_trades),
                "stats": _compute_stats(score_trades),
            },
            "top10_velocity": {
                "label": "Top-10 Velocity",
                "description": "Buy top 10 by Velocity Score each Monday",
                "trades": vel_trades,
                "cumulative_returns": _cumulative_returns(vel_trades),
                "stats": _compute_stats(vel_trades),
            },
            "baseline": {
                "label": "Baseline (SPX)",
                "description": "Buy S&P 500 each Monday (benchmark)",
                "trades": baseline_trades,
                "cumulative_returns": _cumulative_returns(baseline_trades),
                "stats": _compute_stats(baseline_trades),
            },
        },
        "scatter": scatter,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(_dumps(result, separators=(",", ":")))
    size_kb = output_path.stat().st_size / 1024
    print(f"\n✓ Wrote {output_path} ({size_kb:.0f} KB)")
    for name, strat in result["strategies"].items():
        s = strat["stats"]
        print(f"  {name:<18} trades={s['n_trades']:3}  win={s['win_rate'] or 0:.0%}  "
              f"avg={s['avg_return'] or 0:+.1%}  sharpe={s['sharpe'] or 0:.2f}")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Backtest Predator Protocol strategies")
    p.add_argument("--source",  default="data/all_history.csv")
    p.add_argument("--markets", default="data/markets")
    p.add_argument("--output",  default="docs/data/backtest.json")
    p.add_argument("--hold",    type=int, default=30, help="Hold period in days")
    args = p.parse_args(argv)
    run_backtest(args.source, Path(args.markets), Path(args.output), args.hold)
    return 0


if __name__ == "__main__":
    sys.exit(main())
