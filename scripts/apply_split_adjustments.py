#!/usr/bin/env python3
"""Strictly idempotent backward corporate action and split adjustment engine.

Design principles:
1. Strict Idempotency: Before applying any split multiplier from `splits.json`,
   the engine tests whether the step-discontinuity actually exists in the data.
   If `nav[t] / nav[t-1]` already aligns with `1 + perf_pct[t]`, the split is
   recognized as ALREADY ADJUSTED and safely skipped.
2. Zero Over-Adjustment: Can be executed 1 time or 1,000 times with identical output.
3. Multi-artifact Synchronization: When an unadjusted jump is detected, it
   updates the individual CSV, the master aggregate `all_leveraged_etf_flows.csv`,
   and `Leveraged_ETF_Flows_Master.xlsx` sheet `NAV_Wide`.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import tempfile
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
PLAN_PATH = REPO_ROOT / "data" / "flows" / "splits.json"
INDIVIDUAL_DIR = REPO_ROOT / "data" / "flows" / "individual"
AGGREGATE_PATH = REPO_ROOT / "data" / "flows" / "all_leveraged_etf_flows.csv"
WORKBOOK_PATH = REPO_ROOT / "data" / "flows" / "Leveraged_ETF_Flows_Master.xlsx"
SUMMARY_PATH = REPO_ROOT / "data" / "flows" / "curated_catalog_summary.json"


def _atomic_write_text(path: Path, text: str, *, line_ending: str = "\n") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    normalized = text.replace("\r\n", "\n").replace("\n", line_ending)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            handle.write(normalized)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise


def _atomic_write_xlsx(path: Path, sheets: dict[str, pd.DataFrame]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp.xlsx", dir=path.parent)
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        with pd.ExcelWriter(tmp_path, engine="openpyxl") as writer:
            for s_name, s_df in sheets.items():
                s_df.to_excel(writer, sheet_name=s_name, index=False)
        os.replace(tmp_path, path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise


def apply_split_adjustments(dry_run: bool = True) -> dict[str, Any]:
    if not PLAN_PATH.is_file():
        print(f"Error: plan file not found at {PLAN_PATH}", file=sys.stderr)
        return {"status": "error", "message": "splits.json not found"}

    with open(PLAN_PATH, "r", encoding="utf-8") as f:
        plan = json.load(f)

    print(f"Loaded plan with {len(plan)} tickers in registry.", flush=True)

    adjusted_nav_by_ticker_date: dict[tuple[str, str], float] = {}
    adjusted_tickers: list[str] = []

    # 1. Process individual files
    for ticker, splits in plan.items():
        csv_path = INDIVIDUAL_DIR / f"{ticker}_flows.csv"
        if not csv_path.is_file():
            continue

        df = pd.read_csv(csv_path)
        if df.empty or "date" not in df.columns or "nav" not in df.columns:
            continue
        df["date"] = df["date"].astype(str).str.slice(0, 10)
        df = df.sort_values("date").reset_index(drop=True)

        multipliers = np.ones(len(df), dtype=float)
        ticker_needed_adjustment = False

        for s in sorted(splits, key=lambda x: str(x.get("split_date", ""))):
            s_date = str(s.get("split_date", "")).strip()
            try:
                factor = float(s.get("factor", 1.0))
            except (TypeError, ValueError):
                continue
            if not s_date or factor <= 0 or abs(factor - 1.0) < 1e-6:
                continue

            sub = df[df["date"] >= s_date]
            if sub.empty:
                continue
            pos = int(sub.index[0])
            if pos <= 0:
                continue

            prev_nav = float(df.iloc[pos - 1]["nav"])
            curr_nav = float(df.iloc[pos]["nav"])
            if not (math.isfinite(prev_nav) and math.isfinite(curr_nav)) or prev_nav <= 0 or curr_nav <= 0:
                continue

            raw_perf = df.iloc[pos]["perf_pct"] if "perf_pct" in df.columns else None
            has_perf = raw_perf is not None and pd.notna(raw_perf) and float(raw_perf) > -1.0
            ln_exp = math.log1p(float(raw_perf)) if has_perf else 0.0
            ln_act = math.log(curr_nav / prev_nav)
            ln_expected_unadj = -math.log(factor)  # unadjusted NAV jumps by 1 / factor

            # Apply in log-space so fractional splits like 3:2 (ln=0.405) & 4:3 work reliably
            # and genuine market crashes aligned with perf_pct are ignored.
            log_residual = abs(ln_act - ln_exp)
            matches_split_jump = abs(ln_act - ln_expected_unadj) < 0.30
            if (not has_perf and matches_split_jump and abs(ln_act) > 0.22) or (
                has_perf and log_residual > 0.22 and matches_split_jump
            ):
                print(
                    f"  [{ticker}] Detected unadjusted split jump at {s_date} "
                    f"(curr={curr_nav}, prev={prev_nav}, factor={factor}, ratio={curr_nav / prev_nav:.4f})",
                    flush=True,
                )
                ticker_needed_adjustment = True
                mask = df["date"] < df.iloc[pos]["date"]
                multipliers[mask] *= (1.0 / factor)

        if ticker_needed_adjustment:
            old_nav = pd.to_numeric(df["nav"], errors="coerce").to_numpy(dtype=float)
            new_nav = np.round(old_nav * multipliers, 4)

            formatted_nav = []
            for val in new_nav:
                if abs(val - round(val, 2)) < 1e-4 and val >= 1.0:
                    formatted_nav.append(round(float(val), 2))
                else:
                    formatted_nav.append(round(float(val), 4))

            df["nav"] = formatted_nav

            for d_str, n_val in zip(df["date"], df["nav"]):
                adjusted_nav_by_ticker_date[(ticker, str(d_str))] = n_val

            adjusted_tickers.append(ticker)

            if not dry_run:
                _atomic_write_text(csv_path, df.to_csv(index=False, lineterminator="\n"), line_ending="\r\n")
                print(f"  [{ticker}] Saved adjusted individual CSV.", flush=True)

    if not adjusted_tickers:
        print("All ETF price series are already split-adjusted. Zero files modified.", flush=True)
        return {
            "status": "up_to_date",
            "adjusted_tickers_count": 0,
            "adjusted_tickers": [],
            "aggregate_rows_updated": 0,
            "workbook_cells_updated": 0,
        }

    print(f"Adjusted individual files: {len(adjusted_tickers)} tickers: {adjusted_tickers}", flush=True)

    # 2. Process aggregate file
    changed_count = 0
    if AGGREGATE_PATH.is_file():
        print("Processing aggregate all_leveraged_etf_flows.csv...", flush=True)
        df_agg = pd.read_csv(AGGREGATE_PATH)
        keys = list(zip(df_agg["ticker"], df_agg["date"].astype(str).str.slice(0, 10)))
        updated_nav = [adjusted_nav_by_ticker_date.get(k, orig) for k, orig in zip(keys, df_agg["nav"])]

        changed_count = sum(
            1
            for a, b in zip(updated_nav, df_agg["nav"])
            if not (pd.isna(a) and pd.isna(b)) and (pd.isna(a) or pd.isna(b) or abs(float(a) - float(b)) > 1e-9)
        )
        print(f"Aggregate rows updated: {changed_count} / {len(df_agg)}", flush=True)
        df_agg["nav"] = updated_nav
        if not dry_run:
            _atomic_write_text(AGGREGATE_PATH, df_agg.to_csv(index=False, lineterminator="\n"), line_ending="\r\n")
            print("Saved updated aggregate file.", flush=True)

    # 3. Sync summary latest_nav if any final row changed
    latest_nav_by_ticker = {}
    for ticker in adjusted_tickers:
        csv_dates = [d for (t, d) in adjusted_nav_by_ticker_date if t == ticker]
        if csv_dates:
            max_d = max(csv_dates)
            latest_nav_by_ticker[ticker] = adjusted_nav_by_ticker_date[(ticker, max_d)]

    if SUMMARY_PATH.is_file() and latest_nav_by_ticker and not dry_run:
        summary_rows = json.loads(SUMMARY_PATH.read_text(encoding="utf-8"))
        summary_changed = False
        for row in summary_rows:
            t = row.get("ticker")
            if t in latest_nav_by_ticker and abs(float(row.get("latest_nav", 0)) - latest_nav_by_ticker[t]) > 1e-4:
                row["latest_nav"] = round(float(latest_nav_by_ticker[t]), 4)
                summary_changed = True
        if summary_changed:
            _atomic_write_text(SUMMARY_PATH, json.dumps(summary_rows, indent=2, ensure_ascii=False) + "\n", line_ending="\r\n")

    # 4. Process workbook NAV_Wide
    updated_cells = 0
    if WORKBOOK_PATH.is_file():
        print("Processing workbook Leveraged_ETF_Flows_Master.xlsx sheet NAV_Wide...", flush=True)
        with pd.ExcelFile(WORKBOOK_PATH) as wb:
            sheets = {s: pd.read_excel(wb, sheet_name=s) for s in wb.sheet_names}

        nav_wide = sheets["NAV_Wide"]
        dates = nav_wide["date"].astype(str).str[:10].tolist()

        for ticker in adjusted_tickers:
            if ticker in nav_wide.columns:
                orig_col = nav_wide[ticker].tolist()
                new_col = [adjusted_nav_by_ticker_date.get((ticker, d), orig) for d, orig in zip(dates, orig_col)]
                updated_cells += sum(
                    1
                    for a, b in zip(new_col, orig_col)
                    if not (pd.isna(a) and pd.isna(b)) and (pd.isna(a) or pd.isna(b) or abs(float(a) - float(b)) > 1e-9)
                )
                nav_wide[ticker] = new_col

        print(f"Workbook NAV_Wide cells updated: {updated_cells}", flush=True)
        sheets["NAV_Wide"] = nav_wide

        if not dry_run:
            print("Writing updated Excel workbook atomically with openpyxl...", flush=True)
            _atomic_write_xlsx(WORKBOOK_PATH, sheets)
            print("Saved updated Excel workbook successfully.", flush=True)

    return {
        "status": "updated",
        "adjusted_tickers_count": len(adjusted_tickers),
        "adjusted_tickers": adjusted_tickers,
        "aggregate_rows_updated": changed_count,
        "workbook_cells_updated": updated_cells,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply split adjustments idempotently")
    parser.add_argument("--apply", action="store_true", help="Apply changes (default is dry-run)")
    args = parser.parse_args()

    res = apply_split_adjustments(dry_run=not args.apply)
    print(json.dumps(res, indent=2))
    return 1 if res.get("status") == "error" else 0


if __name__ == "__main__":
    raise SystemExit(main())
