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
import sys
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd

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
    deltas = compute_rank_deltas(raw, cfg)

    sanitized_rows = len(latest) if not latest.empty else 0
    print(f"  leaderboard: {len(leaderboard)} unique tickers · "
          f"{(leaderboard['flag']=='HIGH_CONVICTION').sum()} HC · "
          f"{(leaderboard['flag']=='SPECULATIVE_BETA').sum()} SPEC β")

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

    # ── leaderboard.json — main payload for the site ──────────────────────────
    lb_records = leaderboard.to_dict(orient="records")
    (output_dir / "leaderboard.json").write_text(json.dumps(lb_records, separators=(",", ":"), default=str))

    # ── holdings_latest.json — per-(ETF, ticker) detail with rank deltas ──────
    if not latest.empty:
        latest_out = latest[[
            "ETF_Ticker", "ticker", "name", "weight", "rank", "tier", "is_new",
            "rank_mult", "base_score", "new_bonus", "score", "Holdings_As_Of"
        ]].copy()
        latest_out["Holdings_As_Of"] = pd.to_datetime(latest_out["Holdings_As_Of"]).dt.strftime("%Y-%m-%d")
        latest_out = latest_out.merge(
            deltas[["ETF_Ticker", "ticker", "rank_delta", "weight_flow"]],
            on=["ETF_Ticker", "ticker"], how="left"
        )
        (output_dir / "holdings_latest.json").write_text(
            json.dumps(latest_out.to_dict(orient="records"), separators=(",", ":"), default=str)
        )

    # ── changelog.json — entries / exits / movers ─────────────────────────────
    (output_dir / "changelog.json").write_text(json.dumps(chg, indent=2, default=str))

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
        (output_dir / "score_history.json").write_text(json.dumps(spark, separators=(",", ":")))

    # ── leaderboard.parquet — for DuckDB-WASM time-travel queries ─────────────
    leaderboard.to_parquet(output_dir / "leaderboard.parquet", index=False)

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
            "tiers": [{"name": t.name, "etfs": list(t.etfs), "points": t.points} for t in cfg.tiers],
            "rank_breakpoints": [list(b) for b in cfg.rank_breakpoints],
            "new_lookback_days": cfg.new_lookback_days,
            "new_bonus_mult": cfg.new_bonus_mult,
            "high_conviction_min_etfs": cfg.high_conviction_min_etfs,
        },
    }
    (output_dir / "metadata.json").write_text(json.dumps(metadata, indent=2))

    # Quick summary
    print(f"\n✓ Wrote outputs to {output_dir}/")
    for name in ["leaderboard.json", "holdings_latest.json", "changelog.json",
                 "score_history.json", "score_history.parquet", "leaderboard.parquet", "metadata.json"]:
        p = output_dir / name
        if p.exists():
            print(f"  {name:<28} {p.stat().st_size:>9,} bytes")
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
