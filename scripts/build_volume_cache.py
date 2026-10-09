"""Fetch and cache historical trading volume for the ETF flow universe."""
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
import time
import yfinance as yf

REPO_ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = REPO_ROOT / "docs" / "data" / "flows" / "catalog.json"
VOLUME_OUT = REPO_ROOT / "docs" / "data" / "flows" / "volume.json"

def fetch_and_save_volume(period: str = "1y", batch_size: int = 30) -> None:
    if not CATALOG_PATH.is_file():
        print(f"Catalog not found at {CATALOG_PATH}")
        return

    with open(CATALOG_PATH, "r", encoding="utf-8") as f:
        catalog = json.load(f)

    tickers = [it["ticker"] for it in catalog.get("instruments", [])]
    print(f"Fetching volume for {len(tickers)} tickers over period={period}...")

    volume_series: dict[str, dict[str, int]] = {}

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i : i + batch_size]
        print(f"Downloading batch {i // batch_size + 1}/{(len(tickers) + batch_size - 1) // batch_size}: {batch[:5]}...")
        try:
            df = yf.download(batch, period=period, progress=False, group_by="ticker", auto_adjust=False)
            for ticker in batch:
                series_for_ticker: dict[str, int] = {}
                try:
                    if len(batch) == 1:
                        vols = df["Volume"] if "Volume" in df else None
                    else:
                        vols = df[ticker]["Volume"] if ticker in df and "Volume" in df[ticker] else None
                    if vols is not None:
                        for d, v in vols.dropna().items():
                            val = int(v)
                            if val > 0:
                                d_str = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)[:10]
                                series_for_ticker[d_str] = val
                except Exception as ex:
                    pass
                if series_for_ticker:
                    volume_series[ticker] = series_for_ticker
        except Exception as e:
            print(f"Error in batch: {e}")
        time.sleep(0.5)

    payload = {
        "updated_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "period": period,
        "count": len(volume_series),
        "series": volume_series
    }

    VOLUME_OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(VOLUME_OUT, "w", encoding="utf-8") as f:
        json.dump(payload, f, separators=(",", ":"))

    print(f"Saved volume data for {len(volume_series)} tickers to {VOLUME_OUT} ({VOLUME_OUT.stat().st_size:,} bytes)")

if __name__ == "__main__":
    fetch_and_save_volume()
