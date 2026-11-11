#!/usr/bin/env python3
"""Automated ongoing corporate action and split-adjustment sync from Yahoo Finance.

This script:
1. Probes Yahoo Finance corporate actions (`events=splits`) for all 160 ETFs in parallel.
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
import csv
import json
import math
import os
import sys
import tempfile
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
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
                split_dt = datetime.fromtimestamp(sinfo.get("date", int(ts_key)), tz=timezone.utc).strftime("%Y-%m-%d")
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


def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise


def scan_all_splits(
    tickers: list[str], max_workers: int = 15
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, str]]:
    live_splits: dict[str, list[dict[str, Any]]] = {}
    live_errors: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_yahoo_splits_for_ticker, t): t for t in tickers}
        for fut in as_completed(futures):
            res = fut.result()
            if res["status"] == "ok":
                live_splits[res["ticker"]] = res["splits"]
            else:
                live_splits[res["ticker"]] = []
                live_errors[res["ticker"]] = str(res.get("error", "unknown error"))
    return live_splits, live_errors


def is_known_event(
    s_date_str: str,
    known_events: list[dict[str, Any]],
    factor: float | None = None,
) -> bool:
    try:
        s_dt = datetime.strptime(s_date_str, "%Y-%m-%d")
    except ValueError:
        return False
    for ke in known_events:
        try:
            k_dt = datetime.strptime(ke["split_date"], "%Y-%m-%d")
        except (ValueError, KeyError, TypeError):
            continue
        day_gap = abs((s_dt - k_dt).days)
        if day_gap <= 3:
            return True
        if day_gap <= 7:
            k_factor = ke.get("factor")
            if factor is None or k_factor is None:
                return True
            try:
                kf = float(k_factor)
                if kf > 0 and factor > 0 and (
                    abs(factor - kf) < 0.05 or abs(math.log(factor / kf)) < 0.08
                ):
                    return True
            except (TypeError, ValueError):
                return True
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

        dates: list[str] = []
        with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                d_val = str(row.get("date", "")).strip()[:10]
                if d_val:
                    dates.append(d_val)
        if not dates:
            continue
        first_date = min(dates)
        last_date = max(dates)

        ticker_live = live_splits.get(ticker, [])
        for ls in ticker_live:
            s_date = ls["split_date"]
            factor = float(ls["factor"])
            # Must fall inside our active dataset range and be previously unknown
            if first_date <= s_date <= last_date and not is_known_event(s_date, known_events, factor=factor):
                new_events.append({
                    "ticker": ticker,
                    "split_date": s_date,
                    "split_ratio": ls["split_ratio"],
                    "numerator": ls.get("numerator"),
                    "denominator": ls.get("denominator"),
                    "factor": factor,
                })

    return new_events


def sync_and_adjust(force: bool = False, apply_changes: bool = True) -> dict[str, Any]:
    individual_files = sorted(INDIVIDUAL_DIR.glob("*_flows.csv"))
    tickers = [f.name.replace("_flows.csv", "") for f in individual_files]

    canonical_splits = load_canonical_splits()
    print(f"[splits] Querying Yahoo Finance corporate actions for {len(tickers)} ETFs...", flush=True)
    live_splits, live_errors = scan_all_splits(tickers)
    if live_errors:
        print(
            f"[splits] Warning: Yahoo corporate-action fetch failed for {len(live_errors)}/{len(tickers)} ETFs: "
            + ", ".join(sorted(live_errors)[:8]),
            file=sys.stderr,
            flush=True,
        )

    new_events = detect_new_splits(canonical_splits, live_splits, tickers)

    if not new_events and not force:
        print(f"[splits] All {len(tickers)} ETFs are up to date. Zero new splits detected.", flush=True)
        return {
            "status": "up_to_date" if not live_errors else "up_to_date_with_fetch_warnings",
            "new_splits_count": 0,
            "new_splits": [],
            "live_error_count": len(live_errors),
            "live_errors": live_errors,
            "total_canonical_tickers": len(canonical_splits),
        }

    if new_events:
        print(f"[splits] Detected {len(new_events)} NEW corporate actions:", flush=True)
        for ev in new_events:
            print(f"  -> {ev['ticker']}: {ev['split_date']} ({ev['split_ratio']})", flush=True)
            if apply_changes:
                stored_ev = {
                    "split_date": ev["split_date"],
                    "split_ratio": ev["split_ratio"],
                    "factor": ev["factor"],
                }
                if ev.get("numerator") is not None:
                    stored_ev["numerator"] = ev["numerator"]
                if ev.get("denominator") is not None:
                    stored_ev["denominator"] = ev["denominator"]
                canonical_splits.setdefault(ev["ticker"], []).append(stored_ev)
                canonical_splits[ev["ticker"]].sort(key=lambda x: x["split_date"])

    if not apply_changes:
        return {
            "status": "splits_detected" if new_events else "checked",
            "new_splits_count": len(new_events),
            "new_splits": new_events,
            "live_error_count": len(live_errors),
            "live_errors": live_errors,
            "total_canonical_tickers": len(canonical_splits),
        }

    # Save updated splits.json atomically
    print(f"[splits] Saving updated registry to {SPLITS_JSON_PATH}...", flush=True)
    _atomic_write_text(SPLITS_JSON_PATH, json.dumps(canonical_splits, indent=2) + "\n")

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
        "live_error_count": len(live_errors),
        "live_errors": live_errors,
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
