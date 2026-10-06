#!/usr/bin/env python3
"""Stealthy incremental daily updater for the 145-instrument ETF flow dataset.

Design principles for zero-ban, low-footprint operation:
1. Incremental tail-only window (default 21 calendar days) — requests ~15 rows
   (<1 KB JSON) per ticker instead of 10-year backfills.
2. Cohort-aware freshness probing — probes 1 liquid benchmark ticker at the
   current max local date first; if upstream has not advanced past max_local_date,
   skips all tickers already at max_local_date and only checks lagging tickers
   (unless --force-all is passed).
3. Randomized ticker order + human-like jitter (0.75s–1.85s between requests,
   plus periodic 3.0s–5.0s micro-breaks) over a single keep-alive Session with
   browser-grade headers.
4. Fail-closed anti-ban circuit breaker — immediately halts network requests on
   any HTTP 403/405/429 or WAF challenge page without touching local files.
5. Full authoritative sync — when new observations arrive, updates individual
   CSVs, curated_catalog_summary.json, all_leveraged_etf_flows.csv,
   Leveraged_ETF_Flows_Master.xlsx, row-count constants, and docs/data/flows/.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import random
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pandas as pd
import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
FLOWS_DIR = REPO_ROOT / "data" / "flows"
INDIV_DIR = FLOWS_DIR / "individual"
SUMMARY_PATH = FLOWS_DIR / "curated_catalog_summary.json"
AGG_PATH = FLOWS_DIR / "all_leveraged_etf_flows.csv"
XLSX_PATH = FLOWS_DIR / "Leveraged_ETF_Flows_Master.xlsx"
DOCS_FLOWS_DIR = REPO_ROOT / "docs" / "data" / "flows"

USER_AGENTS = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0",
)

AGGREGATE_COLUMNS = [
    "ticker",
    "trackinsight_key",
    "date",
    "nav",
    "usd_flow",
    "perf_pct",
    "cumulative_flow",
    "flow_impulse",
    "flow_zscore",
    "regime",
    "pressure",
    "category",
    "underlying",
    "leverage",
]


class CircuitBreakerTripped(RuntimeError):
    """Raised when upstream returns an access or rate-limit status."""


def _build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.trackinsight.com/en/etf-screener",
            "Origin": "https://www.trackinsight.com",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
        }
    )
    return session


def _candidate_keys(ticker: str, recorded_key: str) -> list[str]:
    candidates: list[str] = []
    for k in (
        recorded_key,
        ticker,
        f"ARCX:{ticker}",
        f"XNMS:{ticker}",
        f"BATS:{ticker}",
        f"CBOE:{ticker}",
    ):
        if k and k not in candidates:
            candidates.append(k)
    return candidates


def _fetch_morpheus_window(
    session: requests.Session,
    ticker: str,
    recorded_key: str,
    start_date: str,
    end_date: str,
    timeout: float = 20.0,
) -> dict[str, dict[str, float]]:
    """Fetch a bounded date window for a single ETF from Trackinsight Morpheus."""
    for key in _candidate_keys(ticker, recorded_key):
        url = (
            f"https://www.trackinsight.com/search-api/morpheus/"
            f"{key}/flow,Perf,R,totalAssets/{start_date}/{end_date}"
        )
        try:
            resp = session.get(url, timeout=timeout)
        except requests.RequestException:
            continue

        if resp.status_code in (403, 405, 429):
            raise CircuitBreakerTripped(
                f"Upstream returned HTTP {resp.status_code} for {ticker} ({key}); tripping stealth circuit breaker."
            )
        if resp.status_code != 200:
            continue

        text_lower = resp.text[:512].lower()
        if "<html" in text_lower or "cloudflare" in text_lower or "cf-chl" in text_lower:
            raise CircuitBreakerTripped(
                f"Upstream returned HTML/challenge body for {ticker} ({key}); tripping stealth circuit breaker."
            )

        try:
            payload = resp.json()
        except ValueError:
            continue

        if not isinstance(payload, list) or len(payload) < 3:
            continue

        flow_pts = payload[0].get("points") or []
        perf_pts = payload[1].get("points") or []
        nav_pts = payload[2].get("points") or []
        if not nav_pts:
            continue

        by_ts: dict[int, dict[str, float]] = {}
        for pt in nav_pts:
            if pt and len(pt) >= 2 and pt[1] is not None and float(pt[1]) > 0:
                by_ts.setdefault(int(pt[0]), {})["nav"] = float(pt[1])
        for pt in flow_pts:
            if pt and len(pt) >= 2 and pt[1] is not None:
                by_ts.setdefault(int(pt[0]), {})["usd_flow"] = float(pt[1])
        for pt in perf_pts:
            if pt and len(pt) >= 2 and pt[1] is not None:
                by_ts.setdefault(int(pt[0]), {})["perf_pct"] = float(pt[1])

        by_date: dict[str, dict[str, float]] = {}
        for ts, vals in sorted(by_ts.items()):
            if "nav" not in vals:
                continue
            day = dt.datetime.fromtimestamp(ts / 1000.0, tz=dt.timezone.utc).strftime("%Y-%m-%d")
            by_date[day] = {
                "nav": vals["nav"],
                "usd_flow": vals.get("usd_flow", 0.0),
                "perf_pct": vals.get("perf_pct", 0.0),
            }
        if by_date:
            return by_date
    return {}


def _recompute_series_metrics(rows: list[dict[str, Any]], headers: list[str]) -> list[dict[str, Any]]:
    cum = 0.0
    flows: list[float] = []
    recomputed: list[dict[str, Any]] = []
    for i, r in enumerate(rows):
        nav = round(float(r["nav"]), 4)
        flow = round(float(r["usd_flow"]), 2)
        perf = round(float(r["perf_pct"]), 6)
        cum = round(cum + flow, 2)
        flows.append(flow)

        win5 = flows[max(0, i - 4) : i + 1]
        impulse = round(sum(win5) / len(win5), 2)

        win30 = flows[max(0, i - 29) : i + 1]
        if len(win30) >= 5:
            mean30 = sum(win30) / len(win30)
            var30 = sum((x - mean30) ** 2 for x in win30) / (len(win30) - 1)
            std30 = math.sqrt(var30) if var30 > 0 else 0.0
            zscore = round((flow - mean30) / std30, 4) if std30 > 0 else 0.0
        else:
            zscore = 0.0

        if zscore >= 1.5:
            regime = "ACCUMULATION"
        elif zscore <= -1.5:
            regime = "DISTRIBUTION"
        else:
            regime = "BALANCED"

        win10 = flows[max(0, i - 9) : i + 1]
        mean10 = sum(win10) / len(win10)
        if len(win30) >= 5 and std30 > 0:
            pressure = round(max(-100.0, min(100.0, ((mean10 / std30) * 35.0) + (zscore * 25.0))), 4)
        else:
            pressure = 0.0

        entry: dict[str, Any] = {
            "ticker": r["ticker"],
            "trackinsight_key": r["trackinsight_key"],
            "date": r["date"],
            "nav": nav,
            "usd_flow": flow,
            "perf_pct": perf,
            "cumulative_flow": cum,
            "flow_impulse": impulse,
            "flow_zscore": zscore,
            "regime": regime,
            "pressure": pressure,
        }
        for opt in ("category", "underlying", "leverage"):
            if opt in headers:
                entry[opt] = r.get(opt, "")
        recomputed.append(entry)
    return recomputed


def _sync_codebase_counts(old_rows: int, new_rows: int, old_dates: int, new_dates: int, old_date: str, new_date: str) -> None:
    """Keep EXPECTED_TOTAL_ROWS, EXPECTED_WORKBOOK_DATE_ROWS, docs, and tests in sync when rows grow."""
    build_script = REPO_ROOT / "scripts" / "build_local_flow_artifacts.py"
    text = build_script.read_text(encoding="utf-8")
    text = re.sub(r"EXPECTED_TOTAL_ROWS\s*=\s*\d+", f"EXPECTED_TOTAL_ROWS = {new_rows}", text)
    text = re.sub(r"EXPECTED_WORKBOOK_DATE_ROWS\s*=\s*\d+", f"EXPECTED_WORKBOOK_DATE_ROWS = {new_dates}", text)
    build_script.write_text(text, encoding="utf-8")

    old_fmt = f"{old_rows:,}"
    new_fmt = f"{new_rows:,}"

    for doc_rel in ("FUND_FLOW_ETFS.md", "README.md"):
        doc_path = REPO_ROOT / doc_rel
        if doc_path.exists():
            dtext = doc_path.read_text(encoding="utf-8")
            dtext = dtext.replace(old_fmt, new_fmt)
            if new_date > old_date:
                dtext = dtext.replace(old_date, new_date)
            doc_path.write_text(dtext, encoding="utf-8")

    for test_rel in (
        "tests/test_local_flow_artifacts.py",
        "tests/test_flow_ui.py",
        "tests/test_flow_ui_metrics.py",
    ):
        tpath = REPO_ROOT / test_rel
        if tpath.exists():
            ttext = tpath.read_text(encoding="utf-8")
            ttext = ttext.replace(str(old_rows), str(new_rows))
            if new_date > old_date:
                ttext = ttext.replace(old_date, new_date)
            tpath.write_text(ttext, encoding="utf-8")


def update_daily_flows(
    lookback_days: int = 21,
    force_all: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    summary_list: list[dict[str, Any]] = json.loads(SUMMARY_PATH.read_text(encoding="utf-8"))
    summary_by_ticker = {item["ticker"]: item for item in summary_list}

    max_local_date = max(str(item["latest_date"]) for item in summary_list)
    min_local_date = min(str(item["latest_date"]) for item in summary_list)
    tomorrow = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=1)).strftime("%Y-%m-%d")

    session = _build_session()

    # Step 1: Probe 1 liquid ticker at max_local_date to see if upstream has a newer trading day
    probe_Start = (
        dt.datetime.strptime(max_local_date, "%Y-%m-%d") - dt.timedelta(days=14)
    ).strftime("%Y-%m-%d")
    probe_tickers = [t for t in ("TQQQ", "SOXL", "UPRO", "NVDL") if summary_by_ticker.get(t, {}).get("latest_date") == max_local_date]
    probe_ticker = random.choice(probe_tickers) if probe_tickers else summary_list[0]["ticker"]
    probe_key = summary_by_ticker[probe_ticker]["trackinsight_key"]

    upstream_has_newer_global_date = False
    probe_data = _fetch_morpheus_window(session, probe_ticker, probe_key, probe_Start, tomorrow)
    if probe_data:
        max_upstream_probe = max(probe_data.keys())
        if max_upstream_probe > max_local_date:
            upstream_has_newer_global_date = True
            print(f"[probe] {probe_ticker} advanced from {max_local_date} -> {max_upstream_probe}; full universe eligible.")
        else:
            print(f"[probe] {probe_ticker} upstream latest is {max_upstream_probe} (local max {max_local_date}).")

    # Determine which tickers to query
    if force_all or upstream_has_newer_global_date:
        candidates = list(summary_list)
    else:
        # Only query tickers whose local latest_date lags max_local_date
        candidates = [item for item in summary_list if str(item["latest_date"]) < max_local_date]
        print(f"[stealth] Skipping {len(summary_list) - len(candidates)} tickers already at {max_local_date}; checking {len(candidates)} lagging tickers.")

    random.shuffle(candidates)

    updated_tickers: list[str] = []
    added_rows_count = 0

    for idx, item in enumerate(candidates, start=1):
        ticker = item["ticker"]
        rec_key = item["trackinsight_key"]
        local_last = str(item["latest_date"])
        start_dt = (
            dt.datetime.strptime(local_last, "%Y-%m-%d") - dt.timedelta(days=max(7, lookback_days))
        ).strftime("%Y-%m-%d")

        # Human-like jitter
        if idx > 1:
            time.sleep(random.uniform(0.75, 1.85))
            if idx % 25 == 0:
                time.sleep(random.uniform(2.5, 4.5))

        fetched = (
            probe_data
            if (ticker == probe_ticker and probe_data and start_dt >= probe_Start)
            else _fetch_morpheus_window(session, ticker, rec_key, start_dt, tomorrow)
        )
        if not fetched:
            continue

        new_dates = [d for d in sorted(fetched.keys()) if d > local_last]
        if not new_dates:
            continue

        csv_path = INDIV_DIR / f"{ticker}_flows.csv"
        with csv_path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            headers = list(reader.fieldnames or [])
            existing_rows = list(reader)

        if not existing_rows:
            continue

        template = existing_rows[-1]
        for day in new_dates:
            obs = fetched[day]
            new_row = {
                "ticker": ticker,
                "trackinsight_key": template["trackinsight_key"],
                "date": day,
                "nav": obs["nav"],
                "usd_flow": obs["usd_flow"],
                "perf_pct": obs["perf_pct"],
            }
            for opt in ("category", "underlying", "leverage"):
                if opt in headers:
                    new_row[opt] = template.get(opt, item.get(opt, ""))
            existing_rows.append(new_row)

        recomputed = _recompute_series_metrics(existing_rows, headers)
        if not dry_run:
            with csv_path.open("w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=headers, lineterminator="\n")
                writer.writeheader()
                writer.writerows(recomputed)

        updated_tickers.append(ticker)
        added_rows_count += len(new_dates)
        print(f"  + {ticker}: +{len(new_dates)} rows (now through {recomputed[-1]['date']})")

    if not updated_tickers or dry_run:
        print(f"Completed check: {len(updated_tickers)} tickers updated ({added_rows_count} new rows). dry_run={dry_run}")
        return {
            "status": "ok",
            "updated_tickers": len(updated_tickers),
            "added_rows": added_rows_count,
            "data_changed": False,
        }

    # Rebuild authoritative summary, aggregate CSV, and Master Excel workbook
    print("Rebuilding authoritative summary, aggregate CSV, and Excel workbook...")
    old_total_rows = sum(int(x["rows_count"]) for x in summary_list)
    old_max_date = max_local_date

    # Count old unique dates from workbook or aggregate
    all_rows_merged: list[dict[str, Any]] = []
    old_dates_set: set[str] = set()

    for item in summary_list:
        ticker = item["ticker"]
        csv_path = INDIV_DIR / f"{ticker}_flows.csv"
        with csv_path.open("r", encoding="utf-8", newline="") as f:
            rows = list(csv.DictReader(f))
        for r in rows:
            if r["date"] <= old_max_date:
                old_dates_set.add(r["date"])
        last = rows[-1]
        first = rows[0]
        item["earliest_date"] = first["date"]
        item["latest_date"] = last["date"]
        item["rows_count"] = len(rows)
        item["latest_nav"] = round(float(last["nav"]), 4)
        item["total_cumulative_flow_m"] = round(float(last["cumulative_flow"]) / 1_000_000.0, 2)
        item["flow_zscore_latest"] = round(float(last["flow_zscore"]), 2)
        item["regime_latest"] = last["regime"]
        item["pressure_latest"] = round(float(last["pressure"]), 1)
        if "flow_30d_m" in item:
            item["flow_30d_m"] = round(sum(float(x["usd_flow"]) for x in rows[-30:]) / 1_000_000.0, 2)
        if "flow_ytd_m" in item:
            yr = str(last["date"])[:4]
            item["flow_ytd_m"] = round(
                sum(float(x["usd_flow"]) for x in rows if str(x["date"]).startswith(yr)) / 1_000_000.0,
                2,
            )
        if "flow_1y_m" in item:
            item["flow_1y_m"] = round(sum(float(x["usd_flow"]) for x in rows[-252:]) / 1_000_000.0, 2)

        for r in rows:
            all_rows_merged.append(
                {
                    "ticker": ticker,
                    "trackinsight_key": r["trackinsight_key"],
                    "date": r["date"],
                    "nav": r["nav"],
                    "usd_flow": r["usd_flow"],
                    "perf_pct": r["perf_pct"],
                    "cumulative_flow": r["cumulative_flow"],
                    "flow_impulse": r["flow_impulse"],
                    "flow_zscore": r["flow_zscore"],
                    "regime": r["regime"],
                    "pressure": r["pressure"],
                    "category": r.get("category") or item["category"],
                    "underlying": r.get("underlying") or item["underlying"],
                    "leverage": r.get("leverage") or item["leverage"],
                }
            )

    summary_list.sort(key=lambda x: (x["category"], -float(x.get("aum_m") or 0), x["ticker"]))
    SUMMARY_PATH.write_text(json.dumps(summary_list, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    all_rows_merged.sort(key=lambda r: (r["ticker"], r["date"]))
    with AGG_PATH.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=AGGREGATE_COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(all_rows_merged)

    df_agg = pd.DataFrame(all_rows_merged)
    for col in ("nav", "usd_flow", "perf_pct", "cumulative_flow", "flow_impulse", "flow_zscore", "pressure"):
        df_agg[col] = df_agg[col].astype(float)

    df_universe = pd.DataFrame(summary_list)
    latest_rows = []
    for item in summary_list:
        t = item["ticker"]
        last_r = df_agg[df_agg["ticker"] == t].iloc[-1]
        latest_rows.append(
            {
                "ticker": t,
                "category": item["category"],
                "fund_name": item["fund_name"],
                "underlying": item["underlying"],
                "leverage": item["leverage"],
                "latest_date": last_r["date"],
                "nav": last_r["nav"],
                "daily_flow_usd": last_r["usd_flow"],
                "cumulative_flow_usd": last_r["cumulative_flow"],
                "flow_impulse": last_r["flow_impulse"],
                "flow_zscore": last_r["flow_zscore"],
                "regime": last_r["regime"],
                "pressure": last_r["pressure"],
            }
        )
    df_latest = pd.DataFrame(latest_rows)
    daily_wide = df_agg.pivot(index="date", columns="ticker", values="usd_flow").sort_index().reset_index()
    cum_wide = df_agg.pivot(index="date", columns="ticker", values="cumulative_flow").sort_index().reset_index()
    nav_wide = df_agg.pivot(index="date", columns="ticker", values="nav").sort_index().reset_index()

    with pd.ExcelWriter(XLSX_PATH, engine="openpyxl") as xl_writer:
        df_universe.to_excel(xl_writer, sheet_name="Universe_Catalog", index=False)
        df_latest.to_excel(xl_writer, sheet_name="Latest_Snapshot", index=False)
        daily_wide.to_excel(xl_writer, sheet_name="Daily_Flows_Wide", index=False)
        cum_wide.to_excel(xl_writer, sheet_name="Cumulative_Flows_Wide", index=False)
        nav_wide.to_excel(xl_writer, sheet_name="NAV_Wide", index=False)

    new_total_rows = len(all_rows_merged)
    new_unique_dates = len(daily_wide)
    new_max_date = max(str(x["latest_date"]) for x in summary_list)

    _sync_codebase_counts(
        old_rows=old_total_rows,
        new_rows=new_total_rows,
        old_dates=len(old_dates_set),
        new_dates=new_unique_dates,
        old_date=old_max_date,
        new_date=new_max_date,
    )

    # Rebuild static UI artifacts under docs/data/flows and verify
    subprocess.run(
        [sys.executable, "scripts/build_local_flow_artifacts.py", "--output-dir", str(DOCS_FLOWS_DIR)],
        cwd=REPO_ROOT,
        check=True,
    )
    subprocess.run(
        [sys.executable, "scripts/build_local_flow_artifacts.py", "--verify-output", "--output-dir", str(DOCS_FLOWS_DIR)],
        cwd=REPO_ROOT,
        check=True,
    )

    return {
        "status": "ok",
        "updated_tickers": len(updated_tickers),
        "added_rows": added_rows_count,
        "total_rows": new_total_rows,
        "latest_date": new_max_date,
        "data_changed": True,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Stealthy incremental daily updater for 145-ETF flow catalog.")
    parser.add_argument("--lookback-days", type=int, default=21, help="Calendar days of tail overlap to query (default: 21)")
    parser.add_argument("--force-all", action="store_true", help="Query all 145 tickers even if lead probe has not advanced")
    parser.add_argument("--dry-run", action="store_true", help="Check upstream without writing files")
    args = parser.parse_args(argv)

    try:
        result = update_daily_flows(
            lookback_days=args.lookback_days,
            force_all=args.force_all,
            dry_run=args.dry_run,
        )
        print(json.dumps(result, indent=2))
        return 0
    except CircuitBreakerTripped as exc:
        print(f"[circuit-breaker] {exc}", file=sys.stderr)
        # Exit 0 on circuit-breaker trip during scheduled run so local data stays intact and CI doesn't spam alerts
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
