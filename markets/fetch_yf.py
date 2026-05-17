"""
markets/fetch_yf.py — Incremental yfinance fetcher for indices, commodities, FX, crypto, and equities.

Usage:
    python -m markets.fetch_yf --out data/markets
    python -m markets.fetch_yf --out data/markets --only NG SPX
"""
from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd
import yfinance as yf


# Phase 3: Market data symbols
SYMBOLS: dict[str, str] = {
    # Indices
    "SPX":    "^GSPC",
    "NDX":    "^NDX",
    "DJI":    "^DJI",
    "RUT":    "^RUT",
    "NIKKEI": "^N225",
    "DAX":    "^GDAXI",
    "FTSE":   "^FTSE",
    "SENSEX": "^BSESN",
    "NIFTY":  "^NSEI",
    "HSI":    "^HSI",
    # Commodities
    "GLD":    "GC=F",
    "SLV":    "SI=F",
    "PL":     "PL=F",
    "PA":     "PA=F",
    "CL":     "CL=F",
    "BZ":     "BZ=F",
    "NG":     "NG=F",
    "HG":     "HG=F",
    # FX
    "USDINR":  "INR=X",
    "USDSGD":  "SGD=X",
    "EURUSD":  "EURUSD=X",
    # Crypto
    "BTC":    "BTC-USD",
    "ETH":    "ETH-USD",
    # Phase 4: Leveraged ETN underlyings
    "NVDA":   "NVDA",
    "AAPL":   "AAPL",
    "TSLA":   "TSLA",
    "GOOG":   "GOOG",
    "GOOGL":  "GOOGL",
    "MSFT":   "MSFT",
    "AMZN":   "AMZN",
    "META":   "META",
}


def fetch_one(symbol: str, yf_ticker: str, out_dir: Path, period: str = "max") -> None:
    """Fetch full history if file is missing, otherwise only fetch since last close + 1 day."""
    target = out_dir / f"yf_{symbol}.parquet"
    existing = pd.DataFrame()
    if target.exists():
        existing = pd.read_parquet(target)
        last = pd.to_datetime(existing["Date"]).max()
        start = (last + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        df = yf.download(yf_ticker, start=start, progress=False, auto_adjust=False)
    else:
        df = yf.download(yf_ticker, period=period, progress=False, auto_adjust=False)

    if df is None or df.empty:
        if not existing.empty:
            print(f"    no new rows; existing {len(existing)} rows kept")
        return

    df = df.reset_index()
    # Flatten multi-level columns that yfinance sometimes returns
    df.columns = [c if isinstance(c, str) else c[0] for c in df.columns]
    # Select only the columns we care about; Volume may be missing for some instruments
    cols_wanted = ["Date", "Open", "High", "Low", "Close", "Volume"]
    cols_present = [c for c in cols_wanted if c in df.columns]
    df = df[cols_present].copy()
    if "Volume" not in df.columns:
        df["Volume"] = None
    df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")

    if not existing.empty:
        df = pd.concat([existing, df]).drop_duplicates(subset=["Date"], keep="last")
    df = df.sort_values("Date").reset_index(drop=True)
    df.to_parquet(target, index=False)
    print(f"    → {len(df)} total rows (added {len(df) - len(existing) if not existing.empty else len(df)})")


def main() -> None:
    ap = argparse.ArgumentParser(description="Fetch market price data via yfinance")
    ap.add_argument("--out", default="data/markets", help="Output directory for parquet files")
    ap.add_argument("--only", nargs="*", default=None, help="Restrict to these symbol(s)")
    args = ap.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    symbols = {k: v for k, v in SYMBOLS.items() if args.only is None or k in args.only}
    print(f"Fetching {len(symbols)} symbols into {out_dir}/")
    for sym, ticker in symbols.items():
        print(f"  {sym} ({ticker})…")
        try:
            fetch_one(sym, ticker, out_dir)
        except Exception as e:
            print(f"    ERROR: {e}")

    print("Done.")


if __name__ == "__main__":
    main()
