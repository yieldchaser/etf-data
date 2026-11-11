"""
markets/build.py — Build markets.json and sim_underlyings.json from data/markets/*.parquet.

Usage:
    python -m markets.build --source data/markets --output docs/data

Outputs:
    docs/data/markets.json        — Phase 3 PRICE LOG data for markets.html
    docs/data/sim_underlyings.json — Phase 4 price history for sim.html
"""
from __future__ import annotations
import argparse
import json
import math
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd

from .metadata import FRED_META, LEVERAGED_UNDERLYINGS, YF_META
from .stats import series_stats, log_rows


def _write_atomic(path: Path, text: str) -> None:
    """Write text atomically: write to .tmp then os.replace.

    Prevents truncated/corrupt files if the process is interrupted mid-write.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)

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


# Metadata imported from .metadata (YF_META, FRED_META, LEVERAGED_UNDERLYINGS)


def build(source: Path, output: Path) -> dict[str, int]:
    output.mkdir(parents=True, exist_ok=True)
    series_list = []

    # ── yfinance series ───────────────────────────────────────────────────────
    print("Processing yfinance parquets…")
    for sym, meta in YF_META.items():
        p = source / f"yf_{sym}.parquet"
        if not p.exists():
            print(f"  {sym}: not found, skipping")
            continue
        try:
            df = pd.read_parquet(p)
            st = series_stats(df, value_col="Close")
            rows = log_rows(df, value_col="Close", n=200)
            series_list.append({
                "symbol": sym,
                "label":  meta["label"],
                "kind":   meta["kind"],
                "stats":  st,
                "log":    rows,
            })
            print(f"  {sym}: {st.get('sessions_observed', 0)} sessions")
        except Exception as e:
            print(f"  {sym}: ERROR — {e}")

    # ── FRED series ───────────────────────────────────────────────────────────
    print("Processing FRED parquets…")
    for sid, meta in FRED_META.items():
        p = source / f"fred_{sid}.parquet"
        if not p.exists():
            print(f"  {sid}: not found, skipping")
            continue
        try:
            df = pd.read_parquet(p)
            # FRED uses "value" column
            df = df.rename(columns={"value": "Close"})
            st = series_stats(df, value_col="Close")
            rows = log_rows(df, value_col="Close", n=200)
            series_list.append({
                "symbol": sid,
                "label":  meta["label"],
                "kind":   meta["kind"],
                "stats":  st,
                "log":    rows,
            })
            print(f"  {sid}: {st.get('sessions_observed', 0)} sessions")
        except Exception as e:
            print(f"  {sid}: ERROR — {e}")

    # Sort by kind then symbol
    series_list.sort(key=lambda x: (x["kind"], x["symbol"]))

    markets_json = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "series": series_list,
    }

    out_path = output / "markets.json"
    write_markets = True
    # Only overwrite if we got at least 90% as many series as before (prevents CI partial-fetch from wiping good data)
    if out_path.exists():
        try:
            existing_count = len(json.loads(out_path.read_text(encoding="utf-8")).get("series", []))
            if len(series_list) < existing_count * 0.9:
                print(f"\n⚠ Skipping markets.json write — new series count ({len(series_list)}) < 90% of existing ({existing_count}). Keeping existing file.")
                write_markets = False
        except Exception:
            pass
    if write_markets:
        text = _dumps(markets_json, separators=(",", ":"))
        _write_atomic(out_path, text)
        size_mb = len(text.encode()) / 1_048_576
        print(f"\n✓ markets.json: {len(series_list)} series · {size_mb:.2f} MB")

    # ── sim_underlyings.json (Phase 4) ────────────────────────────────────────
    print("\nBuilding sim_underlyings.json…")
    sim_inputs: dict = {}
    for sym in LEVERAGED_UNDERLYINGS:
        p = source / f"yf_{sym}.parquet"
        if not p.exists():
            print(f"  {sym}: not found, skipping")
            continue
        try:
            df = pd.read_parquet(p)[["Date", "Close"]].copy()
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
            df = df.dropna(subset=["Date", "Close"]).sort_values("Date")
            df = df[df["Close"].apply(lambda v: isinstance(v, (int, float)) and math.isfinite(float(v)))]
            df = df.tail(2520)   # last 10 years (~252 trading days/year × 10)
            sim_inputs[sym] = [
                {"d": str(d)[:10], "c": round(float(c), 4)}
                for d, c in zip(df["Date"], df["Close"])
            ]
            print(f"  {sym}: {len(sim_inputs[sym])} rows")
        except Exception as e:
            print(f"  {sym}: ERROR — {e}")

    sim_out = output / "sim_underlyings.json"
    write_sim = True
    if sim_out.exists():
        try:
            existing_sim_count = len(json.loads(sim_out.read_text(encoding="utf-8")))
            if len(sim_inputs) < existing_sim_count * 0.8:
                print(f"\n⚠ Skipping sim_underlyings.json write — new count ({len(sim_inputs)}) < 80% of existing ({existing_sim_count}). Keeping existing file.")
                write_sim = False
        except Exception:
            pass
    if write_sim:
        _write_atomic(sim_out, _dumps(sim_inputs, separators=(",", ":")))
        sim_size_kb = sim_out.stat().st_size / 1024
        print(f"✓ sim_underlyings.json: {len(sim_inputs)} underlyings · {sim_size_kb:.0f} KB")

    return {"markets_series": len(series_list), "sim_underlyings": len(sim_inputs)}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Build markets.json and sim_underlyings.json")
    ap.add_argument("--source", default="data/markets", help="Directory containing parquet files")
    ap.add_argument("--output", default="docs/data",   help="Output directory for JSON files")
    args = ap.parse_args(argv)
    summary = build(Path(args.source), Path(args.output))
    if summary["markets_series"] == 0 and summary["sim_underlyings"] == 0:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
