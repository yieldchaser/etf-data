#!/usr/bin/env python3
"""Stealthy incremental daily updater for the 160-instrument ETF flow dataset.

Design principles for zero-ban, low-footprint operation:
1. Real Chromium session warmup on Trackinsight + batched POST requests to
   /search-api/snapshot/get_snapshots (10 funds per batch -> at most 15 HTTP
   requests for the entire 160-ETF universe, or 1 probe request when no new
   trading session has settled).
2. Incremental tail-only window (default 21 calendar days) — requests ~15 rows
   per ticker instead of 10-year backfills.
3. Cohort-aware freshness probing — probes 4 liquid benchmark tickers at the
   current max local date first; if upstream has not advanced past max_local_date,
   skips all tickers already at max_local_date and only checks lagging tickers
   (unless --force-all is passed).
4. Randomized batch order + human-like jitter (0.85s–1.95s between batches)
   and immediate fail-closed circuit breaker on HTTP 403/405/429 or WAF challenge.
5. Verbatim history preservation — appends only new dates > max_existing_date
   to data/flows/individual/*.csv, updates curated_catalog_summary.json,
   all_leveraged_etf_flows.csv, Leveraged_ETF_Flows_Master.xlsx, row-count
   constants, and docs/data/flows/.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import io
import json
import os
import random
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.flow_reversal import (
    apply_derived_metrics as _shared_apply_derived_metrics,
    net_reversal_pairs as _net_reversal_pairs,
)

FLOWS_DIR = REPO_ROOT / "data" / "flows"
INDIVIDUAL_DIR = FLOWS_DIR / "individual"
SUMMARY_PATH = FLOWS_DIR / "curated_catalog_summary.json"
AGGREGATE_PATH = FLOWS_DIR / "all_leveraged_etf_flows.csv"
WORKBOOK_PATH = FLOWS_DIR / "Leveraged_ETF_Flows_Master.xlsx"
DOCS_FLOWS_DIR = REPO_ROOT / "docs" / "data" / "flows"

ENDPOINT = "https://www.trackinsight.com/search-api/snapshot/get_snapshots"

REQUIRED_COLUMNS = [
    "date",
    "ticker",
    "trackinsight_key",
    "usd_flow",
    "nav",
    "perf_pct",
    "cumulative_flow",
    "daily_inflow",
    "daily_outflow",
    "flow_zscore",
    "flow_5d",
    "flow_20d",
    "regime",
    "pressure",
]
AGGREGATE_COLUMNS = REQUIRED_COLUMNS + ["category", "underlying", "leverage"]

FETCH_JS = """
async ({ endpoint, payload, timeoutMs }) => {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
        const resp = await fetch(endpoint, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-Requested-With': 'XMLHttpRequest'
            },
            credentials: 'include',
            body: JSON.stringify(payload),
            signal: controller.signal
        });
        const text = await resp.text();
        if (!resp.ok) {
            return { __error: true, status: resp.status, statusText: resp.statusText };
        }
        const lower = text.slice(0, 512).toLowerCase();
        if (lower.includes('human verification') || lower.includes('aws waf') || lower.includes('captcha') || lower.includes('<html')) {
            return { __error: true, status: 202, statusText: 'Challenge response', challenge: true };
        }
        return JSON.parse(text);
    } catch (e) {
        return { __error: true, status: 0, statusText: String(e) };
    } finally {
        clearTimeout(timer);
    }
}
"""


class CircuitBreakerTripped(RuntimeError):
    """Raised when upstream returns an access, rate-limit, or challenge status."""


def _parse_snapshot_item(item: Any) -> list[dict[str, Any]]:
    if not isinstance(item, dict):
        return []

    def unpack(field_data: Any) -> list[float | None]:
        if not isinstance(field_data, dict):
            return []
        scale = field_data.get("scale", 1) or 1
        raw = field_data.get("data", [])
        return [float(v) / scale if v is not None else None for v in raw]

    stamp_field = item.get("stamp", {})
    day_numbers = stamp_field.get("data", []) if isinstance(stamp_field, dict) else []
    dates = [
        dt.datetime.fromtimestamp(int(d) * 86400, tz=dt.timezone.utc).strftime("%Y-%m-%d")
        for d in day_numbers
    ]
    flow_vals = unpack(item.get("USD:flow", {}))
    nav_vals = unpack(item.get("nav", {}))
    perf_vals = unpack(item.get("perf", {}))
    n = len(dates)

    def pad(lst: list[float | None]) -> list[float | None]:
        return lst + [None] * (n - len(lst))

    rows: list[dict[str, Any]] = []
    for day, fl, nv, pf in zip(dates, pad(flow_vals), pad(nav_vals), pad(perf_vals)):
        if nv is None or nv <= 0:
            continue
        rows.append(
            {
                "date": day,
                "usd_flow": fl if fl is not None else 0.0,
                "nav": nv,
                "perf_pct": pf if pf is not None else 0.0,
            }
        )
    return rows


def _atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise


def _select_probe_entries(summary_rows: list[dict[str, Any]], max_local_date: str, count: int = 4) -> list[dict[str, Any]]:
    """Dynamically pick top-AUM active liquid ETFs at `max_local_date` for session probing."""
    at_latest = [
        item for item in summary_rows if str(item.get("latest_date", "")) == max_local_date
    ]
    pool = at_latest or list(summary_rows)
    ranked = sorted(
        pool,
        key=lambda item: (float(item.get("aum_m") or 0.0), str(item.get("ticker", ""))),
        reverse=True,
    )
    return ranked[:count]


def _apply_derived_metrics(df: pd.DataFrame) -> pd.DataFrame:
    return _shared_apply_derived_metrics(df, preserve_na=False, net_reversals=True, round_decimals=4)


def _sync_codebase_counts(old_rows: int, new_rows: int, old_dates: int, new_dates: int, old_date: str, new_date: str) -> None:
    """Keep anchored EXPECTED_* counts, docs, and local artifact tests in sync without corrupting prose dates."""
    build_script = REPO_ROOT / "scripts" / "build_local_flow_artifacts.py"
    text = build_script.read_text(encoding="utf-8")
    text = re.sub(r"EXPECTED_TOTAL_ROWS\s*=\s*\d+", f"EXPECTED_TOTAL_ROWS = {new_rows}", text)
    text = re.sub(r"EXPECTED_WORKBOOK_DATE_ROWS\s*=\s*\d+", f"EXPECTED_WORKBOOK_DATE_ROWS = {new_dates}", text)
    _atomic_write_bytes(build_script, text.encode("utf-8"))

    old_fmt = f"{old_rows:,}"
    new_fmt = f"{new_rows:,}"

    for doc_rel in ("FUND_FLOW_ETFS.md", "README.md"):
        doc_path = REPO_ROOT / doc_rel
        if doc_path.exists():
            dtext = doc_path.read_text(encoding="utf-8")
            dtext = dtext.replace(old_fmt, new_fmt)
            if new_date > old_date:
                dtext = re.sub(
                    rf"(Latest(?: Observation)? Date\*?\*?:\s*`?){re.escape(old_date)}(`?)",
                    rf"\g<1>{new_date}\g<2>",
                    dtext,
                )
            _atomic_write_bytes(doc_path, dtext.encode("utf-8"))

    tpath = REPO_ROOT / "tests" / "test_local_flow_artifacts.py"
    if tpath.exists():
        ttext = tpath.read_text(encoding="utf-8")
        ttext = re.sub(rf'("rows":\s*){old_rows}\b', rf"\g<1>{new_rows}", ttext)
        ttext = re.sub(rf'("total_rows":\s*){old_rows}\b', rf"\g<1>{new_rows}", ttext)
        if new_date > old_date:
            ttext = re.sub(
                rf'(actual\["latest_date"\]\s*==\s*"){re.escape(old_date)}(")',
                rf"\g<1>{new_date}\g<2>",
                ttext,
            )
        _atomic_write_bytes(tpath, ttext.encode("utf-8"))


def _evaluate_batch(page: Any, batch_entries: list[dict[str, Any]], start_date: str, end_date: str) -> dict[str, list[dict[str, Any]]]:
    payload = {
        "requests": [
            {
                "fund": entry["trackinsight_key"],
                "startDate": start_date,
                "endDate": end_date,
                "columns": ["stamp", "USD:flow", "nav", "perf"],
            }
            for entry in batch_entries
        ]
    }
    resp = page.evaluate(FETCH_JS, {"endpoint": ENDPOINT, "payload": payload, "timeoutMs": 25000})
    if isinstance(resp, dict) and resp.get("__error"):
        status = int(resp.get("status") or 0)
        if status in (202, 403, 405, 429) or resp.get("challenge"):
            raise CircuitBreakerTripped(
                f"Upstream returned status={status} ({resp.get('statusText')}); tripping stealth circuit breaker."
            )
        return {}
    if not isinstance(resp, list) or len(resp) != len(batch_entries):
        return {}
    out: dict[str, list[dict[str, Any]]] = {}
    for entry, item in zip(batch_entries, resp):
        out[entry["ticker"]] = _parse_snapshot_item(item)
    return out


def update_daily_flows(
    lookback_days: int = 21,
    force_all: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    from playwright.sync_api import sync_playwright

    summary_rows: list[dict[str, Any]] = json.loads(SUMMARY_PATH.read_text(encoding="utf-8"))
    summary_by_ticker = {item["ticker"]: item for item in summary_rows}

    max_local_date = max(str(item["latest_date"]) for item in summary_rows)
    start_date = (
        dt.datetime.strptime(max_local_date, "%Y-%m-%d") - dt.timedelta(days=max(14, lookback_days))
    ).strftime("%Y-%m-%d")
    end_date = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=1)).strftime("%Y-%m-%d")

    delta_map: dict[str, list[dict[str, Any]]] = {}

    probe_entries = _select_probe_entries(summary_rows, max_local_date, count=4)
    warmup_ticker = probe_entries[0]["ticker"] if probe_entries else summary_rows[0]["ticker"]
    warmup_url = f"https://www.trackinsight.com/en/fund/{warmup_ticker}/flows"

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-setuid-sandbox",
            ],
        )
        ctx = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1920, "height": 1080},
        )
        page = ctx.new_page()
        page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        page.goto(warmup_url, wait_until="domcontentloaded", timeout=60000)
        time.sleep(random.uniform(1.8, 2.6))

        # Step 1: Cohort-aware freshness probe on top-AUM liquid ETFs at local max date
        probe_results = _evaluate_batch(page, probe_entries, start_date, end_date)
        delta_map.update(probe_results)

        max_probe_date = max(
            (rows[-1]["date"] for rows in probe_results.values() if rows),
            default=max_local_date,
        )
        upstream_advanced = max_probe_date > max_local_date
        print(
            f"[probe] Checked {len(probe_entries)} benchmark funds; upstream latest={max_probe_date} "
            f"(local max={max_local_date}, advanced={upstream_advanced})"
        )

        probed_tickers = {e["ticker"] for e in probe_entries}
        if force_all or upstream_advanced:
            remaining = [item for item in summary_rows if item["ticker"] not in probed_tickers]
        else:
            remaining = [
                item
                for item in summary_rows
                if item["ticker"] not in probed_tickers and str(item["latest_date"]) < max_local_date
            ]
            print(
                f"[stealth] Upstream at {max_probe_date}; skipping {len(summary_rows) - len(remaining) - len(probed_tickers)} "
                f"up-to-date funds and checking {len(remaining)} lagging funds in batches of 10."
            )

        random.shuffle(remaining)
        batch_size = 10
        for i in range(0, len(remaining), batch_size):
            batch = remaining[i : i + batch_size]
            time.sleep(random.uniform(0.85, 1.85))
            batch_res = _evaluate_batch(page, batch, start_date, end_date)
            delta_map.update(batch_res)

        browser.close()

    # Check which tickers actually have new dates beyond their local latest_date
    updated_tickers: list[str] = []
    added_rows_count = 0

    for entry in summary_rows:
        ticker = entry["ticker"]
        fetched_rows = delta_map.get(ticker, [])
        if not fetched_rows:
            continue
        max_existing_date = str(entry["latest_date"])
        new_rows = [r for r in fetched_rows if r["date"] > max_existing_date]
        if new_rows:
            updated_tickers.append(ticker)
            added_rows_count += len(new_rows)
            print(f"  + {ticker}: +{len(new_rows)} new observations (through {new_rows[-1]['date']})")

    if not updated_tickers or dry_run:
        print(f"Completed stealth check: {len(updated_tickers)} tickers updated ({added_rows_count} new rows). dry_run={dry_run}")
        return {
            "status": "ok",
            "probed_max_date": max_probe_date,
            "updated_tickers": len(updated_tickers),
            "added_rows": added_rows_count,
            "data_changed": False,
        }

    # Apply updates preserving existing CSV lines verbatim
    old_total_rows = sum(int(x["rows_count"]) for x in summary_rows)
    old_max_date = max_local_date

    agg_meta: dict[str, tuple[str, str, str]] = {}
    with AGGREGATE_PATH.open("r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            t = row["ticker"]
            if t not in agg_meta:
                agg_meta[t] = (row["category"], row["underlying"], row["leverage"])

    all_dfs_by_ticker: dict[str, pd.DataFrame] = {}
    aggregate_out_rows: list[list[str]] = []
    old_dates_set: set[str] = set()

    for entry in summary_rows:
        ticker = entry["ticker"]
        ti_key = entry["trackinsight_key"]
        ind_path = INDIVIDUAL_DIR / f"{ticker}_flows.csv"

        orig_text = ind_path.read_text(encoding="utf-8", errors="replace")
        orig_lines = orig_text.splitlines()
        df_orig = pd.read_csv(ind_path)
        df_orig["date"] = df_orig["date"].astype(str).str.slice(0, 10)
        df_orig = df_orig.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
        for d in df_orig["date"].tolist():
            old_dates_set.add(d)
        orig_cols = list(df_orig.columns)
        max_existing_date = str(df_orig["date"].iloc[-1])

        new_delta_rows = sorted(
            (r for r in delta_map.get(ticker, []) if r["date"] > max_existing_date),
            key=lambda r: r["date"],
        )
        if new_delta_rows:
            df_new_raw = pd.DataFrame(new_delta_rows)
            df_new_raw["ticker"] = ticker
            df_new_raw["trackinsight_key"] = ti_key
            for col in ("category", "underlying", "leverage"):
                if col in orig_cols:
                    df_new_raw[col] = df_orig[col].iloc[-1]

            base_cols = ["date", "ticker", "trackinsight_key", "usd_flow", "nav", "perf_pct"] + [
                c for c in ["category", "underlying", "leverage"] if c in orig_cols
            ]
            df_combined = pd.concat([df_orig[base_cols], df_new_raw], ignore_index=True)
            df_combined = df_combined.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
            df_combined = _apply_derived_metrics(df_combined)
            df_combined = df_combined[orig_cols]

            # Find earliest historical row whose derived metrics changed (e.g. T-1 netted
            # by a consecutive-day reversal pair on T) and rewrite from that row onwards.
            n_orig = len(df_orig)
            rewrite_from = n_orig
            for check_idx in range(max(0, n_orig - 3), n_orig):
                changed = False
                for col in ("flow_zscore", "flow_5d", "flow_20d", "regime", "pressure"):
                    a_val = df_orig.iloc[check_idx][col]
                    b_val = df_combined.iloc[check_idx][col]
                    if col == "regime":
                        if str(a_val) != str(b_val):
                            changed = True
                            break
                    elif abs(float(a_val) - float(b_val)) > 1e-4:
                        changed = True
                        break
                if changed:
                    rewrite_from = check_idx
                    break

            rewrite_df = df_combined.iloc[rewrite_from:]
            buf = io.StringIO()
            rewrite_df.to_csv(buf, index=False, header=False, lineterminator="\n")
            rewritten_lines = [line for line in buf.getvalue().splitlines() if line.strip()]
            prefix_end = min(len(orig_lines), 1 + rewrite_from)
            final_lines = orig_lines[:prefix_end] + rewritten_lines
            _atomic_write_bytes(ind_path, ("\r\n".join(final_lines) + "\r\n").encode("utf-8"))
            df_final = pd.read_csv(ind_path)
        else:
            df_final = df_orig

        all_dfs_by_ticker[ticker] = df_final
        last = df_final.iloc[-1]
        entry["earliest_date"] = str(df_final["date"].iloc[0])
        entry["latest_date"] = str(last["date"])
        entry["rows_count"] = int(len(df_final))
        entry["latest_nav"] = round(float(last["nav"]), 4)
        entry["total_cumulative_flow_m"] = round(float(last["cumulative_flow"]) / 1_000_000.0, 2)
        entry["flow_zscore_latest"] = round(float(last["flow_zscore"]), 2)
        entry["regime_latest"] = str(last["regime"])
        entry["pressure_latest"] = round(float(last["pressure"]), 1)

        cat_val, und_val, lev_val = agg_meta.get(
            ticker, (entry["category"], entry["underlying"], entry["leverage"])
        )
        with ind_path.open("r", encoding="utf-8", errors="replace", newline="") as f:
            reader = csv.DictReader(f)
            for r in reader:
                out_r = [r[col] for col in REQUIRED_COLUMNS]
                out_r.extend(
                    [
                        r.get("category") or cat_val,
                        r.get("underlying") or und_val,
                        r.get("leverage") or lev_val,
                    ]
                )
                aggregate_out_rows.append(out_r)

    summary_json_str = json.dumps(summary_rows, indent=2, ensure_ascii=False)
    _atomic_write_bytes(
        SUMMARY_PATH,
        summary_json_str.replace("\r\n", "\n").replace("\n", "\r\n").encode("utf-8"),
    )

    agg_buf = io.StringIO()
    writer = csv.writer(agg_buf, lineterminator="\r\n")
    writer.writerow(AGGREGATE_COLUMNS)
    writer.writerows(aggregate_out_rows)
    _atomic_write_bytes(AGGREGATE_PATH, agg_buf.getvalue().encode("utf-8"))

    universe_df = pd.DataFrame(summary_rows)
    summary_latest_cols = [
        "ticker",
        "category",
        "fund_name",
        "issuer",
        "underlying",
        "leverage",
        "latest_date",
        "latest_nav",
        "total_cumulative_flow_m",
        "flow_zscore_latest",
        "regime_latest",
        "pressure_latest",
    ]
    summary_latest_df = universe_df[summary_latest_cols].copy()

    all_dates = sorted(
        {str(d) for df in all_dfs_by_ticker.values() for d in df["date"].tolist()}
    )
    sorted_tickers = sorted(all_dfs_by_ticker.keys())

    daily_wide = pd.concat(
        [
            pd.DataFrame({"date": all_dates}),
            pd.DataFrame({t: all_dfs_by_ticker[t].set_index("date")["usd_flow"].reindex(all_dates).values for t in sorted_tickers}),
        ],
        axis=1,
    )
    cum_wide = pd.concat(
        [
            pd.DataFrame({"date": all_dates}),
            pd.DataFrame({t: all_dfs_by_ticker[t].set_index("date")["cumulative_flow"].reindex(all_dates).values for t in sorted_tickers}),
        ],
        axis=1,
    )
    nav_wide = pd.concat(
        [
            pd.DataFrame({"date": all_dates}),
            pd.DataFrame({t: all_dfs_by_ticker[t].set_index("date")["nav"].reindex(all_dates).values for t in sorted_tickers}),
        ],
        axis=1,
    )

    fd, tmp_xlsx = tempfile.mkstemp(prefix=f".{WORKBOOK_PATH.name}.", suffix=".tmp.xlsx", dir=WORKBOOK_PATH.parent)
    os.close(fd)
    tmp_xlsx_path = Path(tmp_xlsx)
    try:
        with pd.ExcelWriter(tmp_xlsx_path, engine="openpyxl") as writer:
            universe_df.to_excel(writer, sheet_name="Universe_Catalog", index=False)
            summary_latest_df.to_excel(writer, sheet_name="Summary_Latest", index=False)
            daily_wide.to_excel(writer, sheet_name="Daily_Flows_Wide", index=False)
            cum_wide.to_excel(writer, sheet_name="Cumulative_Flows_Wide", index=False)
            nav_wide.to_excel(writer, sheet_name="NAV_Wide", index=False)
        os.replace(tmp_xlsx_path, WORKBOOK_PATH)
    except Exception:
        tmp_xlsx_path.unlink(missing_ok=True)
        raise

    new_total_rows = len(aggregate_out_rows)
    new_unique_dates = len(all_dates)
    new_max_date = all_dates[-1]

    _sync_codebase_counts(
        old_rows=old_total_rows,
        new_rows=new_total_rows,
        old_dates=len(old_dates_set),
        new_dates=new_unique_dates,
        old_date=old_max_date,
        new_date=new_max_date,
    )

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
        "probed_max_date": max_probe_date,
        "updated_tickers": len(updated_tickers),
        "added_rows": added_rows_count,
        "total_rows": new_total_rows,
        "latest_date": new_max_date,
        "data_changed": True,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Stealthy incremental daily updater for 160-ETF flow catalog.")
    parser.add_argument("--lookback-days", type=int, default=21, help="Calendar days of tail overlap to query (default: 21)")
    parser.add_argument("--force-all", action="store_true", help="Query all 160 tickers even if lead probe has not advanced")
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
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
