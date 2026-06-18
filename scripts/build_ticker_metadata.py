"""
Build ticker metadata CSV from yfinance.

Reads unique tickers from data/all_history.csv and fetches sector, industry,
country, and market cap from yfinance. Caches results to data/ticker_metadata.csv.

Usage:
    python scripts/build_ticker_metadata.py
"""
import pandas as pd
import yfinance as yf
from pathlib import Path
import time

def main():
    # Read unique tickers
    history_path = Path("data/all_history.csv")
    if not history_path.exists():
        print(f"ERROR: {history_path} not found")
        return 1

    df = pd.read_csv(history_path)
    tickers = sorted(df["ticker"].dropna().unique())
    print(f"Found {len(tickers)} unique tickers")

    # Cache check (BUG-15): load existing metadata and only hit yfinance for
    # tickers missing from the cache. Avoids re-fetching ~3,000 resolved rows
    # every run and tripping yfinance rate limits.
    out_path = Path("data/ticker_metadata.csv")
    cached: dict[str, dict] = {}
    if out_path.exists():
        try:
            cache_df = pd.read_csv(out_path)
            for _, row in cache_df.iterrows():
                cached[row["ticker"]] = {
                    "ticker":   row["ticker"],
                    "sector":   row.get("sector", "Unknown"),
                    "industry": row.get("industry", "Unknown"),
                    "country":  row.get("country", "Unknown"),
                    "market_cap_usd": row.get("market_cap_usd", None),
                }
            print(f"Loaded {len(cached)} cached entries from {out_path}")
        except Exception as e:
            print(f"WARNING: could not read cache ({e}); rebuilding from scratch")
            cached = {}

    missing = [t for t in tickers if t not in cached]
    print(f"Need to fetch: {len(missing)} tickers ({len(tickers) - len(missing)} served from cache)")

    # Build metadata (only for tickers not already cached)
    new_rows = []
    for i, ticker in enumerate(missing, 1):
        if i % 50 == 0:
            print(f"  Progress: {i}/{len(missing)}")

        try:
            info = yf.Ticker(ticker).info
            new_rows.append({
                "ticker": ticker,
                "sector": info.get("sector", "Unknown"),
                "industry": info.get("industry", "Unknown"),
                "country": info.get("country", "Unknown"),
                "market_cap_usd": info.get("marketCap", None),
            })
            time.sleep(0.1)  # Rate limiting
        except Exception as e:
            print(f"  WARNING: {ticker} failed — {e}")
            new_rows.append({
                "ticker": ticker,
                "sector": "Unknown",
                "industry": "Unknown",
                "country": "Unknown",
                "market_cap_usd": None,
            })

    # Merge cached + newly-fetched (cache first so new fetches overwrite
    # any ticker that's present in both — fresh data wins).
    metadata = {**cached, **{r["ticker"]: r for r in new_rows}}
    metadata = [metadata[t] for t in tickers if t in metadata]

    # Write CSV
    meta_df = pd.DataFrame(metadata)
    meta_df.to_csv(out_path, index=False)

    # Stats
    resolved = (meta_df["sector"] != "Unknown").sum()
    print(f"\n✓ Wrote {len(meta_df)} rows to {out_path}")
    print(f"  {resolved} tickers with resolved sector ({resolved/len(meta_df)*100:.1f}%)")

    return 0

if __name__ == "__main__":
    import sys
    sys.exit(main())
