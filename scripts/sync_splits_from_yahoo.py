#!/usr/bin/env python3
"""Automated ongoing corporate action and split-adjustment sync from Yahoo Finance.

This script:
1. Probes Yahoo Finance corporate actions (`events=splits`) for all 145 ETFs in parallel.
2. Compares against the canonical registry in `data/flows/splits.json`.
3. Detects any new forward split or reverse split (combine).
4. When a new split occurs:
   - Updates `data/flows/splits.json` with the new event.
   - Retroactively backward-adjusts historical NAV across individual CSVs, aggregate CSV,
     and the master Excel workbook.
   - Rebuilds deterministic JSON artifacts via `build_local_flow_artifacts.py`.
5. Returns a structured report and exits cleanly.
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

DATA_DIR = REPO_ROOT / "data" / "flows"
INDIVIDUAL_DIR = DATA_DIR / "individual"
SPLITS_JSON_PATH = DATA_DIR / "splits.json"
AGGREGATE_PATH = DATA_DIR / "all_leveraged_etf_flows.csv"
WORKBOOK_PATH = DATA_DIR / "Leveraged_ETF_Flows_Master.xlsx"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
}


def load_canonical_splits() -> dict[str, list[dict[str, Any]]]:
    if not SPLITS_JSON_PATH.is_file():
        return {}
    try:
        return json.loads(SPLITS_JSON_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"Warning: Could not read {SPLITS_JSON_PATH}: {e}", file=sys.stderr)
        return {}


def fetch_yahoo_splits_for_ticker(ticker: str) -> dict[str, Any]:
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=max&interval=1d&events=splits"
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=12) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            result = data["chart"]["result"][0]
            splits_raw = result.get("events", {}).get("splits", {})
            splits_list = []
            for ts_key, sinfo in splits_raw.items():
                split_dt = datetime.fromtimestamp(sinfo.get("date", int(ts_key))).strftime("%Y-%m-%d")
                num = float(sinfo.get("numerator", 1))
                den = float(sinfo.get("denominator", 1))
                splits_list.append({
                    "split_date": split_dt,
                    "split_ratio": sinfo.get("splitRatio", f"{int(num)}:{int(den)}"),
                    "numerator": num,
                    "denominator": den,
                    "factor": num / den,
                })
            splits_list.sort(key=lambda x: x["split_date"])
            return {"ticker": ticker, "status": "ok", "splits": splits_list}
    except Exception as exc:
        return {"ticker": ticker, "status": "error", "error": str(exc), "splits": []}


def scan_all_splits(tickers: list[str], max_workers: int = 15) -> dict[str, list[dict[str, Any]]]:
    live_splits: dict[str, list[dict[str, Any]]] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_yahoo_splits_for_ticker, t): t for t in tickers}
        for fut in as_completed(futures):
            res = fut.result()
            if res["status"] == "ok":
                live_splits[res["ticker"]] = res["splits"]
            else:
                live_splits[res["ticker"]] = []
    return live_splits


def is_known_event(s_date_str: str, known_events: list[dict[str, Any]]) -> bool:
    try:
        s_dt = datetime.strptime(s_date_str, "%Y-%m-%d")
    except ValueError:
        return False
    for ke in known_events:
        try:
            k_dt = datetime.strptime(ke["split_date"], "%Y-%m-%d")
            if abs((s_dt - k_dt).days) <= 7:
                return True
        except ValueError:
            pass
    return False


def detect_new_splits(
    canonical_splits: dict[str, list[dict[str, Any]]],
    live_splits: dict[str, list[dict[str, Any]]],
    tickers: list[str],
) -> list[dict[str, Any]]:
    new_events: list[dict[str, Any]] = []

    for ticker in tickers:
        known_events = canonical_splits.get(ticker, [])

        csv_path = INDIVIDUAL_DIR / f"{ticker}_flows.csv"
        if not csv_path.is_file():
            continue

        # Read date range from CSV
        with csv_path.open("r", encoding="utf-8") as f:
            lines = [l.strip() for l in f if l.strip()]
        if len(lines) < 2:
            continue
        first_date = lines[1].split(",")[0]
        last_date = lines[-1].split(",")[0]

        ticker_live = live_splits.get(ticker, [])
        for ls in ticker_live:
            s_date = ls["split_date"]
            # Must fall inside our active dataset range and be previously unknown
            if first_date <= s_date <= last_date and not is_known_event(s_date, known_events):
                new_events.append({
                    "ticker": ticker,
                    "split_date": s_date,
                    "split_ratio": ls["split_ratio"],
                    "factor": ls["factor"],
                })

    return new_events


def sync_and_adjust(force: bool = False, apply_changes: bool = True) -> dict[str, Any]:
    individual_files = sorted(INDIVIDUAL_DIR.glob("*_flows.csv"))
    tickers = [f.name.replace("_flows.csv", "") for f in individual_files]
    
    canonical_splits = load_canonical_splits()
    print(f"[splits] Querying Yahoo Finance corporate actions for {len(tickers)} ETFs...", flush=True)
    live_splits = scan_all_splits(tickers)
    
    new_events = detect_new_splits(canonical_splits, live_splits, tickers)
    
    if not new_events and not force:
        print(f"[splits] All {len(tickers)} ETFs are up to date. Zero new splits detected.", flush=True)
        return {
            "status": "up_to_date",
            "new_splits_count": 0,
            "new_splits": [],
            "total_canonical_tickers": len(canonical_splits),
        }

    if new_events:
        print(f"[splits] Detected {len(new_events)} NEW corporate actions:", flush=True)
        for ev in new_events:
            print(f"  -> {ev['ticker']}: {ev['split_date']} ({ev['split_ratio']})", flush=True)
            # Add to canonical splits
            if ev["ticker"] not in canonical_splits:
                canonical_splits[ev["ticker"]] = []
            canonical_splits[ev["ticker"]].append(ev)
            canonical_splits[ev["ticker"]].sort(key=lambda x: x["split_date"])

    if apply_changes:
        # Save updated splits.json
        print(f"[splits] Saving updated registry to {SPLITS_JSON_PATH}...", flush=True)
        SPLITS_JSON_PATH.write_text(json.dumps(canonical_splits, indent=2), encoding="utf-8")
        
        # Run apply_split_adjustments
        from scripts.apply_split_adjustments import apply_split_adjustments
        print("[splits] Applying retroactive adjustments across data sources...", flush=True)
        apply_split_adjustments(dry_run=False)
        
        # Rebuild local artifacts
        print("[splits] Rebuilding static flow artifacts...", flush=True)
        from scripts.build_local_flow_artifacts import build_local_artifacts
        build_res = build_local_artifacts(
            source_dir=DATA_DIR,
            output_dir=REPO_ROOT / "docs" / "data" / "flows",
            validate_workbook=True,
            validate_markdown=True,
        )
        print(f"[splits] Rebuild complete: {build_res.get('ticker_artifacts')} artifacts updated.", flush=True)

    return {
        "status": "updated",
        "new_splits_count": len(new_events),
        "new_splits": new_events,
        "total_canonical_tickers": len(canonical_splits),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync corporate actions and splits from Yahoo Finance")
    parser.add_argument("--check-only", action="store_true", help="Only check for new splits without applying")
    parser.add_argument("--force", action="store_true", help="Force re-applying adjustments even if no new splits")
    args = parser.parse_args()

    res = sync_and_adjust(force=args.force, apply_changes=not args.check_only)
    print(json.dumps(res, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
