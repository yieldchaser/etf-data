"""
Build universal 15,715 ETF search index for Conviction Labs Markets.
Reads trackinsight_all_etfs_rich.csv and compiles a compact, optimized
JSON index docs/data/etf_search_index.json for instant client-side autocomplete.
"""
import csv
import json
from pathlib import Path

SOURCE_CSV = Path(r"C:\Users\Dell\.gemini\antigravity\brain\b113b1d3-7776-41dd-90f5-bc61d3dff5b5\scratch\trackinsight_all_etfs_rich.csv")
OUT_JSON = Path("docs/data/etf_search_index.json")
FLOWS_DIR = Path("docs/data/flows")

def get_cached_tickers():
    if not FLOWS_DIR.exists():
        return set()
    return {p.stem.upper() for p in FLOWS_DIR.glob("*.json") if p.stem != "curated_manifest"}

def build_index():
    print(f"Reading source ETF universe from {SOURCE_CSV}...")
    cached_set = get_cached_tickers()
    
    rows = []
    seen = set()

    with open(SOURCE_CSV, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for r in reader:
            raw_ticker = (r.get("ticker") or "").strip()
            if not raw_ticker:
                continue
            
            # Clean symbol (strip exchange prefix for display/search)
            display_ticker = raw_ticker.split(":")[-1] if ":" in raw_ticker else raw_ticker
            
            if raw_ticker in seen:
                continue
            seen.add(raw_ticker)

            aum_val = None
            try:
                if r.get("aum"):
                    aum_val = int(float(r["aum"]))
            except ValueError:
                pass

            label = (r.get("label") or "").strip()
            provider = (r.get("provider") or "").strip()
            currency = (r.get("currency") or "USD").strip().upper()
            is_cached = 1 if display_ticker.upper() in cached_set else 0

            # Schema: [ticker, key, label, provider, aum, currency, is_cached]
            rows.append([
                display_ticker,
                raw_ticker,
                label,
                provider,
                aum_val,
                currency,
                is_cached
            ])

    # Sort descending by USD AUM first (for USD ETFs), then others
    def sort_key(item):
        is_usd = 1 if item[5] == "USD" else 0
        aum = item[4] or 0
        return (is_usd, aum)

    rows.sort(key=sort_key, reverse=True)
    print(f"Compiled {len(rows)} ETFs.")

    payload = {
        "cols": ["t", "k", "l", "p", "a", "c", "cached"],
        "rows": rows
    }

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(payload, f, separators=(",", ":"))

    size_kb = OUT_JSON.stat().st_size / 1024
    print(f"Saved to {OUT_JSON} ({size_kb:.1f} KB)")

if __name__ == "__main__":
    build_index()
