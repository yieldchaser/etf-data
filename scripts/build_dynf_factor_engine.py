"""
Build the Factor Rotation Engine dataset for DYNF (iShares U.S. Equity Factor Rotation Active ETF).

Processes all daily holdings from data/history_parquet/ (2019 to present) and decomposes
portfolio exposure into 6 fundamental & macro factor sleeves:
1. Momentum (Semiconductors, Tech Hardware & High Beta Growth)
2. Quality (Mega-Cap Platforms, High ROE, Low Leverage)
3. Value & Free Cash Flow (Energy, Industrials & Materials)
4. Financials (Money Center Banks, Insurance & Credit)
5. Low Volatility (Healthcare, Consumer Staples & Utilities)
6. Cyclicals (Consumer Discretionary & Retail Beta)

Outputs: docs/data/dynf_factor_engine.json
"""
import os
import sys
import json
from datetime import datetime
from pathlib import Path
import pandas as pd
import numpy as np

# Encoding fix
if sys.platform.startswith("win"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except AttributeError:
        pass

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

# Factor Mapping Definitions
FACTOR_DEFINITIONS = {
    "momentum": {
        "label": "Momentum",
        "description": "High beta, price velocity & semiconductor hardware",
        "color": "#c084fc", # purple / --burst
        "tickers": {
            'NVDA', 'AVGO', 'AMAT', 'LRCX', 'AMD', 'MU', 'QCOM', 'TXN', 'INTC', 'KLAC',
            'MRVL', 'ADI', 'NXPI', 'ON', 'MCHP', 'MPWR', 'CDNS', 'SNPS', 'ARM', 'SMCI',
            'TER', 'SWKS', 'QRVO', 'CRUS', 'ENTG', 'WDC', 'STX', 'NFLX', 'CRM',
            'ADBE', 'NOW', 'PANW', 'CRWD', 'SNOW', 'FTNT', 'DDOG', 'ZS', 'PLTR', 'ANET'
        }
    },
    "quality": {
        "label": "Quality",
        "description": "Mega-cap platforms, fortress balance sheets & high ROE",
        "color": "#22d3ee", # cyan / --cyan
        "tickers": {
            'AAPL', 'MSFT', 'GOOGL', 'GOOG', 'META', 'V', 'MA', 'CSCO', 'ACN', 'ORCL',
            'IBM', 'INTU', 'ADP', 'PAYX', 'ISRG', 'SYK', 'TMO', 'DHR', 'ABT',
            'GILD', 'REGN', 'VRTX', 'BDX'
        }
    },
    "value_fcf": {
        "label": "Value & Energy FCF",
        "description": "High free cash flow yield, tangible assets & inflation hedge",
        "color": "#34d399", # emerald / --up
        "tickers": {
            'XOM', 'CVX', 'COP', 'EOG', 'SLB', 'VLO', 'MPC', 'PSX', 'OXY', 'HES', 'KMI',
            'WMB', 'HAL', 'BKR', 'CAT', 'DE', 'GE', 'UNP', 'HON', 'UPS', 'FDX', 'LMT',
            'RTX', 'BA', 'NOC', 'GD', 'EMR', 'ETN', 'ITW', 'PH', 'LIN', 'APD', 'SHW',
            'ECL', 'FCX', 'NEM', 'NUE', 'DOW'
        }
    },
    "financials": {
        "label": "Financials & Capital",
        "description": "Money center banks, insurance & credit spread sensitivity",
        "color": "#60a5fa", # blue / --blue
        "tickers": {
            'JPM', 'BAC', 'WFC', 'C', 'GS', 'MS', 'BLK', 'BRKB', 'BRK B', 'ALL', 'PGR',
            'CB', 'AXP', 'SCHW', 'USB', 'PNC', 'TFC', 'COF', 'BK', 'STT', 'MET', 'PRU',
            'AIG', 'TRV', 'MMC', 'AON', 'AJG', 'CME', 'ICE', 'MCO', 'SPGI'
        }
    },
    "low_volatility": {
        "label": "Low Volatility",
        "description": "Inelastic demand, non-cyclical healthcare & consumer staples",
        "color": "#fbbf24", # amber / --amber
        "tickers": {
            'JNJ', 'UNH', 'LLY', 'ABBV', 'MRK', 'PFE', 'BMY', 'AMGN', 'CVS', 'CI', 'ELV',
            'PG', 'KO', 'PEP', 'COST', 'WMT', 'TGT', 'MDLZ', 'CL', 'KMB', 'GIS', 'MO',
            'PM', 'T', 'VZ', 'CMCSA', 'NEE', 'DUK', 'SO', 'D', 'AEP', 'SRE', 'EXC',
            'XEL', 'ED', 'MDT', 'WM', 'RSG', 'AWK'
        }
    },
    "cyclicals": {
        "label": "Cyclical Discretionary",
        "description": "Consumer spending, retail & economic cycle beta",
        "color": "#fb923c", # orange
        "tickers": {
            'AMZN', 'TSLA', 'HD', 'LOW', 'MCD', 'SBUX', 'NKE', 'BKNG', 'TJX', 'MAR',
            'HLT', 'ORLY', 'AZO', 'ROST', 'LULU', 'CMG', 'DRI', 'YUM', 'EBAY', 'ETSY'
        }
    }
}

def load_all_dynf():
    parquet_dir = REPO_ROOT / "data/history_parquet"
    year_dirs = sorted(
        [d for d in parquet_dir.glob("year=*") if d.is_dir()],
        key=lambda d: int(d.name.split("=")[1]) if d.name.split("=")[1].isdigit() else 0
    )
    dfs = []
    for yd in year_dirs:
        p = yd / "holdings.parquet"
        if p.exists():
            df = pd.read_parquet(p)
            dynf = df[df['ETF_Ticker'] == 'DYNF'].copy()
            if not dynf.empty:
                dfs.append(dynf)
    if not dfs:
        raise RuntimeError("No DYNF data found in history_parquet")
    combined = pd.concat(dfs, ignore_index=True)
    combined['Holdings_As_Of'] = pd.to_datetime(combined['Holdings_As_Of'])
    combined.sort_values(by=['Holdings_As_Of', 'weight'], ascending=[True, False], inplace=True)
    return combined

def compute_factor_series(df):
    dates = sorted(df['Holdings_As_Of'].unique())
    print(f"Total distinct trading days: {len(dates)}")
    
    daily_records = []
    for dt in dates:
        day_df = df[df['Holdings_As_Of'] == dt]
        total_w = day_df['weight'].sum()
        if total_w <= 0:
            continue
            
        record = {
            'date': dt.strftime('%Y-%m-%d'),
            'holdings_count': int(len(day_df))
        }
        
        sum_factor_w = 0.0
        for f_id, f_meta in FACTOR_DEFINITIONS.items():
            f_w = float(day_df[day_df['ticker'].isin(f_meta['tickers'])]['weight'].sum() * 100)
            record[f_id] = round(f_w, 2)
            sum_factor_w += f_w
            
        record['other'] = round(max(0.0, 100.0 - sum_factor_w), 2)
        daily_records.append(record)
        
    res_df = pd.DataFrame(daily_records)
    res_df['Holdings_As_Of'] = pd.to_datetime(res_df['date'])
    res_df.set_index('Holdings_As_Of', inplace=True)
    return res_df

def determine_macro_regime(row):
    # Macro regime derived from factor dominance
    mom = row['momentum']
    qual = row['quality']
    val = row['value_fcf']
    lvol = row['low_volatility']
    
    if (mom + qual) > (val + lvol) * 1.5:
        if mom > qual:
            return "Expansion (High-Beta Momentum)"
        return "Expansion (Quality Leadership)"
    elif (val + lvol) > (mom + qual):
        if val > lvol:
            return "Recovery (Cyclical Value & FCF)"
        return "Contraction (Defensive Flight)"
    else:
        return "Slowdown (Barbell Rotation)"

def main():
    print("Loading DYNF history from Parquet store...")
    df = load_all_dynf()
    print(f"Loaded {len(df):,} holdings records.")
    
    factor_df = compute_factor_series(df)
    
    # Calculate rolling changes (30d and 90d delta)
    # Using ~21 business days for 30d, ~63 business days for 90d
    factor_keys = list(FACTOR_DEFINITIONS.keys()) + ['other']
    
    for f in factor_keys:
        factor_df[f'{f}_delta_30d'] = factor_df[f] - factor_df[f].shift(21)
        factor_df[f'{f}_delta_90d'] = factor_df[f] - factor_df[f].shift(63)
        
    latest_row = factor_df.iloc[-1]
    latest_date_str = latest_row['date']
    latest_holdings_df = df[df['Holdings_As_Of'] == factor_df.index[-1]]
    
    # Current factor states and top drivers
    current_factors = {}
    for f_id, f_meta in FACTOR_DEFINITIONS.items():
        curr_w = float(latest_row[f_id])
        d30 = float(latest_row.get(f'{f_id}_delta_30d', 0.0) or 0.0)
        d90 = float(latest_row.get(f'{f_id}_delta_90d', 0.0) or 0.0)
        
        # Acceleration logic
        if d30 > 0.5:
            if (d30 / 21) > (d90 / 63):
                accel_status = "Accelerating"
                accel_badge = "up"
            else:
                accel_status = "Expanding"
                accel_badge = "neutral"
        elif d30 < -0.5:
            if (d30 / 21) < (d90 / 63):
                accel_status = "Decelerating"
                accel_badge = "down"
            else:
                accel_status = "Contracting"
                accel_badge = "down"
        else:
            accel_status = "Neutral"
            accel_badge = "neutral"
            
        # Top holdings driving this factor
        f_stocks = latest_holdings_df[latest_holdings_df['ticker'].isin(f_meta['tickers'])].head(4)
        top_drivers = []
        for _, stock_row in f_stocks.iterrows():
            top_drivers.append({
                'ticker': stock_row['ticker'],
                'name': stock_row.get('name', ''),
                'weight': round(float(stock_row['weight'] * 100), 2)
            })
            
        current_factors[f_id] = {
            'label': f_meta['label'],
            'description': f_meta['description'],
            'color': f_meta['color'],
            'current_weight': round(curr_w, 2),
            'delta_30d': round(d30, 2),
            'delta_90d': round(d90, 2),
            'status': accel_status,
            'badge': accel_badge,
            'top_drivers': top_drivers
        }
        
    # Dynamic sampling for responsive browser rendering:
    # Within 365 days of latest trading day: sample every 2nd trading day (~2-3/week for fine detail)
    # Older historical periods: sample every 5th trading day (~weekly)
    sample_cutoff = factor_df.index[-1] - pd.Timedelta(days=365)
    sample_indices = []
    for i, dt in enumerate(factor_df.index):
        if dt < sample_cutoff:
            if i % 5 == 0 or i == 0:
                sample_indices.append(i)
        else:
            if i % 2 == 0 or i == len(factor_df) - 1:
                sample_indices.append(i)
                
    if (len(factor_df) - 1) not in sample_indices:
        sample_indices.append(len(factor_df) - 1)
        
    sampled_df = factor_df.iloc[sorted(list(set(sample_indices)))].copy()
    
    timeline = []
    for dt, row in sampled_df.iterrows():
        point = {
            'date': row['date'],
            'regime': determine_macro_regime(row)
        }
        for f in factor_keys:
            point[f] = round(float(row[f]), 2)
        timeline.append(point)
        
    macro_regime_current = determine_macro_regime(latest_row)
    
    # Milestone summaries (Annual snapshots)
    annual_snapshots = []
    for yr, grp in factor_df.groupby(factor_df.index.year):
        last_day = grp.iloc[-1]
        annual_snapshots.append({
            'year': int(yr),
            'date': last_day['date'],
            'regime': determine_macro_regime(last_day),
            'momentum': round(float(last_day['momentum']), 2),
            'quality': round(float(last_day['quality']), 2),
            'value_fcf': round(float(last_day['value_fcf']), 2),
            'financials': round(float(last_day['financials']), 2),
            'low_volatility': round(float(last_day['low_volatility']), 2),
            'cyclicals': round(float(last_day['cyclicals']), 2),
            'holdings_count': int(last_day['holdings_count'])
        })
        
    payload = {
        'fund': 'DYNF',
        'ticker': 'DYNF',
        'fund_name': 'iShares U.S. Equity Factor Rotation Active ETF',
        'as_of': latest_date_str,
        'inception_date': '2019-03-19',
        'macro_regime': macro_regime_current,
        'current_factors': current_factors,
        'factor_definitions': {
            f_id: {
                'label': m['label'],
                'description': m['description'],
                'color': m['color']
            } for f_id, m in FACTOR_DEFINITIONS.items()
        },
        'timeline': timeline,
        'milestones': annual_snapshots,
        'annual_snapshots': annual_snapshots
    }
    
    out_dir = REPO_ROOT / "docs" / "data"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "dynf_factor_engine.json"
    
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
        
    file_size_kb = out_file.stat().st_size / 1024
    print(f"\nSaved {out_file} ({file_size_kb:.1f} KB, {len(timeline)} timeline points, {len(annual_snapshots)} annual snapshots).")

if __name__ == "__main__":
    main()
