"""
markets/fetch_yf.py — Incremental yfinance fetcher for indices, commodities, FX, crypto, and equities.

Usage:
    python -m markets.fetch_yf --out data/markets
    python -m markets.fetch_yf --out data/markets --only NG SPX
"""
from __future__ import annotations
import argparse
import os
import sys
from pathlib import Path
import pandas as pd
import yfinance as yf

from .metadata import YF_SYMBOLS

# Re-exported alias for backwards compatibility
SYMBOLS: dict[str, str] = YF_SYMBOLS


def _normalise_yf_frame(raw: pd.DataFrame) -> pd.DataFrame:
    df = raw.reset_index()
    # Flatten multi-level columns that yfinance sometimes returns
    df.columns = [c if isinstance(c, str) else c[0] for c in df.columns]
    cols_wanted = ["Date", "Open", "High", "Low", "Close", "Volume"]
    cols_present = [c for c in cols_wanted if c in df.columns]
    df = df[cols_present].copy()
    if "Volume" not in df.columns:
        df["Volume"] = None
    df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
    return df


def fetch_one(symbol: str, yf_ticker: str, out_dir: Path, period: str = "max") -> None:
    """Fetch full history if missing or split/restatement detected, else incremental with 5-day overlap verification."""
    target = out_dir / f"yf_{symbol}.parquet"
    existing = pd.DataFrame()
    last_valid = pd.NaT
    if target.exists():
        existing = pd.read_parquet(target)
        if not existing.empty and "Date" in existing.columns:
            existing["Date"] = pd.to_datetime(existing["Date"], errors="coerce").dt.strftime("%Y-%m-%d")
            existing = existing.dropna(subset=["Date"]).sort_values("Date").reset_index(drop=True)
            last_valid = pd.to_datetime(existing["Date"], errors="coerce").dropna().max()

    if not existing.empty and not pd.isna(last_valid):
        # 5-day overlap window catches retroactive stock splits / data restatements
        start = (last_valid - pd.Timedelta(days=5)).strftime("%Y-%m-%d")
        raw_df = yf.download(yf_ticker, start=start, progress=False, auto_adjust=False)
        if raw_df is None or raw_df.empty:
            print(f"    no new rows; existing {len(existing)} rows kept")
            return
        new_df = _normalise_yf_frame(raw_df)

        merged_check = pd.merge(
            existing[["Date", "Close"]],
            new_df[["Date", "Close"]],
            on="Date",
            suffixes=("_old", "_new"),
        )
        needs_full_refetch = False
        for _, row in merged_check.iterrows():
            old_c, new_c = row["Close_old"], row["Close_new"]
            if pd.notna(old_c) and pd.notna(new_c) and float(old_c) > 0:
                if abs(float(new_c) / float(old_c) - 1.0) > 1e-4:
                    needs_full_refetch = True
                    break

        if needs_full_refetch:
            print(f"    split/restatement detected on overlap window -> full refetch")
            full_raw = yf.download(yf_ticker, period=period, progress=False, auto_adjust=False)
            if full_raw is not None and not full_raw.empty:
                df = _normalise_yf_frame(full_raw)
            else:
                df = pd.concat([existing, new_df]).drop_duplicates(subset=["Date"], keep="last")
        else:
            df = pd.concat([existing, new_df]).drop_duplicates(subset=["Date"], keep="last")
    else:
        raw_df = yf.download(yf_ticker, period=period, progress=False, auto_adjust=False)
        if raw_df is None or raw_df.empty:
            if not existing.empty:
                print(f"    no new rows; existing {len(existing)} rows kept")
            return
        df = _normalise_yf_frame(raw_df)

    df = df.sort_values("Date").reset_index(drop=True)
    # Atomic write: write to .tmp then os.replace — an interrupted write
    # leaves the previous good file intact rather than a truncated parquet.
    tmp = target.with_suffix(target.suffix + ".tmp")
    df.to_parquet(tmp, index=False)
    os.replace(tmp, target)
    added = len(df) - len(existing) if not existing.empty else len(df)
    print(f"    → {len(df)} total rows (delta {added:+d})")


def main() -> int:
    ap = argparse.ArgumentParser(description="Fetch market price data via yfinance")
    ap.add_argument("--out", default="data/markets", help="Output directory for parquet files")
    ap.add_argument("--only", nargs="*", default=None, help="Restrict to these symbol(s)")
    args = ap.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    symbols = {k: v for k, v in SYMBOLS.items() if args.only is None or k in args.only}
    print(f"Fetching {len(symbols)} symbols into {out_dir}/")
    failed = 0
    for sym, ticker in symbols.items():
        print(f"  {sym} ({ticker})…")
        try:
            fetch_one(sym, ticker, out_dir)
        except Exception as e:
            failed += 1
            print(f"    ERROR: {e}")

    print(f"Done. ({len(symbols) - failed}/{len(symbols)} succeeded)")
    if symbols and failed == len(symbols):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
