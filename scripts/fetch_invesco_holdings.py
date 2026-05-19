#!/usr/bin/env python3
"""
Invesco ETF Holdings Mega Scraper
Fetches daily holdings for 10 Invesco ETFs from the official Invesco API.

Source:  dng-api.invesco.com  |  GET, no auth required
Library: curl_cffi (Chrome TLS impersonation -- bypasses JA3 fingerprint block)

Install:
    pip install curl_cffi

Usage:
    python fetch_invesco_holdings.py                   # fetch all ETFs
    python fetch_invesco_holdings.py --tickers PIE SPMO  # fetch specific ETFs
    python fetch_invesco_holdings.py --dry-run         # preview, no save
    python fetch_invesco_holdings.py --workers 4       # parallel (default: 3)

Output:
    data/holdings/<TICKER>_holdings.csv   (one file per ETF, never mixed)

Each row includes both ticker AND cusip as identity anchors.
Idempotent: re-running on the same day will not double-write.

GitHub Actions cron: '30 22 * * 1-5'  (10:30 PM UTC, after US close)
"""

import csv
import sys
import time
import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from curl_cffi import requests
except ImportError:
    print(
        "ERROR: curl_cffi not installed.\n"
        "       Run: pip install curl_cffi",
        file=sys.stderr,
    )
    sys.exit(1)

# ─────────────────────────────────────────────────────────────────────────────
# ETF Registry
# CUSIP verified against Invesco.com, SEC filings, and Morningstar.
# DO NOT reorder or edit without re-verifying each CUSIP independently.
# ─────────────────────────────────────────────────────────────────────────────
ETF_REGISTRY = [
    {
        "ticker": "PIE",
        "cusip":  "46138E867",
        "name":   "Invesco DW Emerging Markets Momentum ETF",
    },
    {
        "ticker": "XSMO",
        "cusip":  "46137V498",
        "name":   "Invesco S&P SmallCap Momentum ETF",
    },
    {
        "ticker": "XMMO",
        "cusip":  "46137V464",
        "name":   "Invesco S&P MidCap Momentum ETF",
    },
    {
        "ticker": "XLG",
        "cusip":  "46137V233",
        "name":   "Invesco S&P 500 Top 50 ETF",
    },
    {
        "ticker": "SPMO",
        "cusip":  "46138E339",
        "name":   "Invesco S&P 500 Momentum ETF",
    },
    {
        "ticker": "SPHQ",
        "cusip":  "46137V241",
        "name":   "Invesco S&P 500 Quality ETF",
    },
    {
        "ticker": "SPHB",
        "cusip":  "46138E370",
        "name":   "Invesco S&P 500 High Beta ETF",
    },
    {
        "ticker": "RPG",
        "cusip":  "46137V266",
        "name":   "Invesco S&P 500 Pure Growth ETF",
    },
    {
        "ticker": "QQQM",
        "cusip":  "46138G649",
        "name":   "Invesco NASDAQ 100 ETF",
    },
    {
        "ticker": "CSD",
        "cusip":  "46137V159",
        "name":   "Invesco S&P Spin-Off ETF",
    },
]

# Build a lookup dict for --tickers filtering
REGISTRY_BY_TICKER = {etf["ticker"]: etf for etf in ETF_REGISTRY}

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
API_BASE    = "https://dng-api.invesco.com/cache/v1/accounts/en_US/shareclasses"
API_SUFFIX  = "holdings/fund?idType=cusip&productType=ETF"
OUTPUT_DIR  = Path("data") / "holdings"
DELAY_SEC   = 0.5   # polite pause between requests in sequential mode

REQUEST_HEADERS = {
    "Accept":           "*/*",
    "Accept-Encoding":  "gzip, deflate, br, zstd",
    "Accept-Language":  "en-US,en;q=0.9",
    "Origin":           "https://www.invesco.com",
    "Referer":          "https://www.invesco.com/",
    "Sec-Fetch-Dest":   "empty",
    "Sec-Fetch-Mode":   "cors",
    "Sec-Fetch-Site":   "same-site",
}

CSV_FIELDS = [
    "effective_date",     # date holdings are valid for (from API) -- sort key
    "effective_biz_date", # prior business date (from API)
    "fetched_at",         # UTC timestamp of this run
    "etf_ticker",         # e.g. PIE  -- identity anchor col 1
    "etf_cusip",          # e.g. 46138E867  -- identity anchor col 2
    "etf_name",           # full fund name
    "total_holdings",     # total count reported by API
    "rank",               # 1-based position in response
    "ticker",             # holding ticker
    "issuer_name",        # holding company name
    "shares",             # Share / Par value
    "pct_tna",            # % of Total Net Assets
    "security_type",
    "holding_cusip",      # CUSIP of the individual holding
    "market_value_usd",
]

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("invesco")


# ─────────────────────────────────────────────────────────────────────────────
# Fetch
# ─────────────────────────────────────────────────────────────────────────────
def build_url(cusip: str) -> str:
    return f"{API_BASE}/{cusip}/{API_SUFFIX}"


def fetch_one(etf: dict) -> dict:
    """Fetch holdings for a single ETF. Returns enriched data dict."""
    ticker = etf["ticker"]
    cusip  = etf["cusip"]
    url    = build_url(cusip)

    log.info(f"[{ticker}]  GET {url}")
    resp = requests.get(
        url,
        headers=REQUEST_HEADERS,
        impersonate="chrome",
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()

    # Sanity check: confirm the CUSIP in the response matches what we sent
    returned_cusip = data.get("cusip", "")
    if returned_cusip and returned_cusip != cusip:
        raise ValueError(
            f"[{ticker}] CUSIP MISMATCH! sent={cusip} got={returned_cusip} -- aborting"
        )

    return data


# ─────────────────────────────────────────────────────────────────────────────
# Parse
# ─────────────────────────────────────────────────────────────────────────────
def parse_one(etf: dict, data: dict) -> list[dict]:
    ticker         = etf["ticker"]
    cusip          = etf["cusip"]
    name           = etf["name"]
    effective_date = data.get("effectiveDate", "")
    effective_biz  = data.get("effectiveBusinessDate", "")
    total          = data.get("totalNumberOfHoldings", "")
    raw            = data.get("holdings", [])
    fetched_at     = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    rows = []
    for rank, h in enumerate(raw, start=1):
        rows.append({
            "effective_date":     effective_date,
            "effective_biz_date": effective_biz,
            "fetched_at":         fetched_at,
            "etf_ticker":         ticker,    # identity anchor
            "etf_cusip":          cusip,     # identity anchor
            "etf_name":           name,
            "total_holdings":     total,
            "rank":               rank,
            "ticker":             h.get("ticker", ""),
            "issuer_name":        h.get("issuerName", ""),
            "shares":             h.get("units", ""),
            "pct_tna":            h.get("percentageOfTotalNetAssets", ""),
            "security_type":      h.get("securityTypeName", ""),
            "holding_cusip":      h.get("cusip", ""),
            "market_value_usd":   h.get("marketValue", ""),
        })

    log.info(
        f"[{ticker}]  effectiveDate={effective_date} | "
        f"holdings={len(rows)} / reported={total}"
    )
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# Save  (idempotent per ETF per date)
# ─────────────────────────────────────────────────────────────────────────────
def save_one(etf: dict, rows: list[dict], dry_run: bool = False) -> str:
    """
    Save rows to <ticker>_holdings.csv.
    Returns one of: 'written', 'skipped', 'dry_run'
    """
    ticker = etf["ticker"]
    if not rows:
        log.warning(f"[{ticker}]  No rows returned -- nothing to save")
        return "empty"

    effective_date = rows[0]["effective_date"]
    out_file = OUTPUT_DIR / f"{ticker.lower()}_holdings.csv"

    if dry_run:
        log.info(f"[{ticker}]  DRY RUN -- {len(rows)} rows for {effective_date}")
        hdr = f"  {'RNK':>3}  {'TICKER':<10} {'ISSUER NAME':<45} {'%TNA':>6}  {'MKT VALUE':>16}"
        print(hdr)
        print("  " + "-" * (len(hdr) - 2))
        for r in rows[:10]:
            mv  = float(r["market_value_usd"]) if r["market_value_usd"] else 0
            pct = float(r["pct_tna"])          if r["pct_tna"]          else 0
            print(
                f"  {r['rank']:>3}. {r['ticker']:<10} {r['issuer_name']:<45} "
                f"{pct:>5.2f}%  ${mv:>15,.2f}"
            )
        if len(rows) > 10:
            print(f"  ... +{len(rows) - 10} more\n")
        return "dry_run"

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    file_exists = out_file.exists()

    # Idempotency: skip if this effective_date already recorded for this ETF
    if file_exists:
        with open(out_file, newline="", encoding="utf-8") as f:
            existing_dates = {row.get("effective_date", "") for row in csv.DictReader(f)}
        if effective_date in existing_dates:
            log.info(f"[{ticker}]  {effective_date} already in CSV -- skipping")
            return "skipped"

    mode = "a" if file_exists else "w"
    with open(out_file, mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)

    action = "Appended" if file_exists else "Created"
    log.info(f"[{ticker}]  {action} {len(rows)} rows -> {out_file}")
    return "written"


# ─────────────────────────────────────────────────────────────────────────────
# Worker (fetch + parse + save in one shot for thread pool)
# ─────────────────────────────────────────────────────────────────────────────
def process_etf(etf: dict, dry_run: bool) -> tuple[str, str]:
    """Returns (ticker, status) where status is written/skipped/dry_run/error."""
    ticker = etf["ticker"]
    try:
        data  = fetch_one(etf)
        rows  = parse_one(etf, data)
        status = save_one(etf, rows, dry_run=dry_run)
        return ticker, status
    except Exception as e:
        log.error(f"[{ticker}]  FAILED: {e}")
        return ticker, "error"


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Fetch daily holdings for Invesco ETFs."
    )
    parser.add_argument(
        "--tickers", nargs="+", metavar="TICKER",
        help=f"Subset of tickers to fetch. Available: {', '.join(REGISTRY_BY_TICKER)}"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Fetch and preview without writing to CSV"
    )
    parser.add_argument(
        "--workers", type=int, default=3,
        help="Parallel fetch workers (default: 3; use 1 for sequential)"
    )
    args = parser.parse_args()

    # Select ETFs
    if args.tickers:
        unknown = [t for t in args.tickers if t.upper() not in REGISTRY_BY_TICKER]
        if unknown:
            print(f"ERROR: Unknown tickers: {unknown}", file=sys.stderr)
            print(f"Available: {list(REGISTRY_BY_TICKER.keys())}", file=sys.stderr)
            sys.exit(1)
        etf_list = [REGISTRY_BY_TICKER[t.upper()] for t in args.tickers]
    else:
        etf_list = ETF_REGISTRY

    log.info(f"Fetching {len(etf_list)} ETF(s) with {args.workers} worker(s)")

    results = {}

    if args.workers == 1:
        # Sequential with polite delay
        for etf in etf_list:
            ticker, status = process_etf(etf, dry_run=args.dry_run)
            results[ticker] = status
            time.sleep(DELAY_SEC)
    else:
        # Parallel
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {
                pool.submit(process_etf, etf, args.dry_run): etf["ticker"]
                for etf in etf_list
            }
            for future in as_completed(futures):
                ticker, status = future.result()
                results[ticker] = status

    # Summary
    print("\n── Summary ──────────────────────────────────")
    for ticker in [e["ticker"] for e in etf_list]:
        status = results.get(ticker, "?")
        icon = {"written": "✓", "skipped": "=", "dry_run": "~",
                "error": "✗", "empty": "?"}.get(status, "?")
        print(f"  {icon}  {ticker:<6}  {status}")
    print("─────────────────────────────────────────────")

    errors = [t for t, s in results.items() if s == "error"]
    if errors:
        log.error(f"Failed ETFs: {errors}")
        sys.exit(1)


if __name__ == "__main__":
    main()
