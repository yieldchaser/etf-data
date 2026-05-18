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
    tickers = sorted(df["Holdings_Ticker_Symbol"].dropna().unique())
    print(f"Found {len(tickers)} unique tickers")
    
    # Build metadata
    metadata = []
    for i, ticker in enumerate(tickers, 1):
        if i % 50 == 0:
            print(f"  Progress: {i}/{len(tickers)}")
        
        try:
            info = yf.Ticker(ticker).info
            metadata.append({
                "ticker": ticker,
                "sector": info.get("sector", "Unknown"),
                "industry": info.get("industry", "Unknown"),
                "country": info.get("country", "Unknown"),
                "market_cap_usd": info.get("marketCap", None),
            })
            time.sleep(0.1)  # Rate limiting
        except Exception as e:
            print(f"  WARNING: {ticker} failed — {e}")
            metadata.append({
                "ticker": ticker,
                "sector": "Unknown",
                "industry": "Unknown",
                "country": "Unknown",
                "market_cap_usd": None,
            })
    
    # Write CSV
    out_path = Path("data/ticker_metadata.csv")
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
