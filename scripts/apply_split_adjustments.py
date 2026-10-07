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
import glob
import json
import os
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
PLAN_PATH = REPO_ROOT / "data" / "flows" / "splits.json"
INDIVIDUAL_DIR = REPO_ROOT / "data" / "flows" / "individual"
AGGREGATE_PATH = REPO_ROOT / "data" / "flows" / "all_leveraged_etf_flows.csv"
WORKBOOK_PATH = REPO_ROOT / "data" / "flows" / "Leveraged_ETF_Flows_Master.xlsx"


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
        multipliers = np.ones(len(df), dtype=float)
        ticker_needed_adjustment = False

        for s in splits:
            s_date = s["split_date"]
            factor = float(s["factor"])
            sub = df[df["date"] >= s_date]
            if sub.empty:
                continue
            idx = sub.index[0]
            if idx == 0:
                continue

            prev_nav = float(df.loc[idx - 1, "nav"])
            curr_nav = float(df.loc[idx, "nav"])
            perf_pct = float(df.loc[idx, "perf_pct"])
            exp_ratio = 1.0 + perf_pct
            act_ratio = curr_nav / prev_nav if prev_nav > 0 else 1.0
            div = act_ratio / exp_ratio if exp_ratio != 0 else 1.0

            # Only apply if an actual unadjusted jump exists
            if (act_ratio > 1.35 or act_ratio < 0.65) and abs(div - 1.0) > 0.25:
                print(
                    f"  [{ticker}] Detected unadjusted split jump at {s_date} "
                    f"(curr={curr_nav}, prev={prev_nav}, factor={factor}, ratio={act_ratio:.4f})",
                    flush=True,
                )
                ticker_needed_adjustment = True
                mask = df["date"] < df.loc[idx, "date"]
                multipliers[mask] *= (1.0 / factor)

        if ticker_needed_adjustment:
            old_nav = df["nav"].values
            new_nav = np.round(old_nav * multipliers, 4)

            formatted_nav = []
            for val in new_nav:
                if abs(val - round(val, 2)) < 1e-4 and val >= 1.0:
                    formatted_nav.append(round(val, 2))
                else:
                    formatted_nav.append(round(val, 4))

            df["nav"] = formatted_nav

            for d_str, n_val in zip(df["date"], df["nav"]):
                adjusted_nav_by_ticker_date[(ticker, str(d_str))] = n_val

            adjusted_tickers.append(ticker)

            if not dry_run:
                df.to_csv(csv_path, index=False)
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
        keys = list(zip(df_agg["ticker"], df_agg["date"].astype(str)))
        updated_nav = [adjusted_nav_by_ticker_date.get(k, orig) for k, orig in zip(keys, df_agg["nav"])]

        changed_count = sum(1 for a, b in zip(updated_nav, df_agg["nav"]) if a != b)
        print(f"Aggregate rows updated: {changed_count} / {len(df_agg)}", flush=True)
        df_agg["nav"] = updated_nav
        if not dry_run:
            df_agg.to_csv(AGGREGATE_PATH, index=False)
            print("Saved updated aggregate file.", flush=True)

    # 3. Process workbook NAV_Wide
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
                updated_cells += sum(1 for a, b in zip(new_col, orig_col) if a != b and not (pd.isna(a) and pd.isna(b)))
                nav_wide[ticker] = new_col

        print(f"Workbook NAV_Wide cells updated: {updated_cells}", flush=True)
        sheets["NAV_Wide"] = nav_wide

        if not dry_run:
            print("Writing updated Excel workbook with openpyxl...", flush=True)
            with pd.ExcelWriter(WORKBOOK_PATH, engine="openpyxl") as writer:
                for s_name, s_df in sheets.items():
                    s_df.to_excel(writer, sheet_name=s_name, index=False)
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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
