import os
import glob
import json
from pathlib import Path
import pandas as pd
import numpy as np

REPO_ROOT = Path(__file__).resolve().parents[1]
PLAN_PATH = REPO_ROOT / "data" / "flows" / "splits.json"
INDIVIDUAL_DIR = REPO_ROOT / "data" / "flows" / "individual"
AGGREGATE_PATH = REPO_ROOT / "data" / "flows" / "all_leveraged_etf_flows.csv"
WORKBOOK_PATH = REPO_ROOT / "data" / "flows" / "Leveraged_ETF_Flows_Master.xlsx"

def apply_split_adjustments(dry_run=True):
    with open(PLAN_PATH, "r", encoding="utf-8") as f:
        plan = json.load(f)
        
    print(f"Loaded plan with {len(plan)} tickers needing adjustment.", flush=True)
    
    adjusted_nav_by_ticker_date = {}
    
    # 1. Process individual files
    for ticker, splits in plan.items():
        csv_path = INDIVIDUAL_DIR / f"{ticker}_flows.csv"
        df = pd.read_csv(csv_path)
        df["date_dt"] = pd.to_datetime(df["date"])
        df = df.sort_values("date_dt").reset_index(drop=True)
        
        # Calculate multipliers backward
        multipliers = np.ones(len(df), dtype=float)
        for s in splits:
            s_date = pd.to_datetime(s["split_date"])
            factor = float(s["factor"])
            mask = df["date_dt"] < s_date
            multipliers[mask] *= (1.0 / factor)
            
        old_nav = df["nav"].values
        new_nav = np.round(old_nav * multipliers, 4)
        
        # Format: if rounded to 2 decimal places matches within 0.0001, round to 2 decimals, else 4 decimals
        formatted_nav = []
        for val in new_nav:
            if abs(val - round(val, 2)) < 1e-4 and val >= 1.0:
                formatted_nav.append(round(val, 2))
            else:
                formatted_nav.append(round(val, 4))
                
        df["nav"] = formatted_nav
        
        for d_str, n_val in zip(df["date"], df["nav"]):
            adjusted_nav_by_ticker_date[(ticker, str(d_str))] = n_val
            
        if not dry_run:
            df.drop(columns=["date_dt"]).to_csv(csv_path, index=False)
            
    print(f"Adjusted individual files: {len(plan)} tickers.", flush=True)
    
    # 2. Process aggregate file
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
    print("Processing workbook Leveraged_ETF_Flows_Master.xlsx sheet NAV_Wide...", flush=True)
    with pd.ExcelFile(WORKBOOK_PATH) as wb:
        sheets = {s: pd.read_excel(wb, sheet_name=s) for s in wb.sheet_names}
        
    nav_wide = sheets["NAV_Wide"]
    dates = nav_wide["date"].astype(str).str[:10].tolist()
    
    updated_cells = 0
    for ticker in plan:
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

if __name__ == "__main__":
    import sys
    dry = "--apply" not in sys.argv
    print(f"Running apply_split_adjustments (dry_run={dry})...", flush=True)
    apply_split_adjustments(dry_run=dry)
