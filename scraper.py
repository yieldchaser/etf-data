import pandas as pd
import requests
import time
import random
import json
import os
from datetime import datetime
from io import StringIO
import urllib3

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIG ---
CONFIG_FILE = 'config.json'
DATA_DIR_LATEST = 'data/latest'
DATA_DIR_HISTORY = 'data/history'
TODAY = datetime.now().strftime('%Y-%m-%d')

# Header rotation
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
]

def get_session():
    session = requests.Session()
    session.headers.update({
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
    })
    return session

def find_holdings_table(dfs):
    """
    Smarter Table Finder: 
    1. Checks headers.
    2. Checks the first row of data (in case headers were missed).
    """
    if not dfs:
        return None
    
    valid_keywords = ['ticker', 'symbol', 'holding', 'identifier', 'weighting']
    
    for i, df in enumerate(dfs):
        # Strategy A: Check current Column Names
        cols = [str(c).strip().lower() for c in df.columns]
        if any(k in cols for k in valid_keywords):
            print(f"      -> Found valid table (Headers match) in Table #{i}")
            return df
        
        # Strategy B: Check Row 0 (Promote Header)
        # Sometimes pandas reads the header as the first row of data
        if not df.empty:
            first_row = [str(x).strip().lower() for x in df.iloc[0].values]
            if any(k in first_row for k in valid_keywords):
                print(f"      -> Found valid table (Row 0 matches) in Table #{i}. Promoting header.")
                new_header = df.iloc[0] # Grab the first row for the header
                df = df[1:] # Take the data less the header row
                df.columns = new_header # Set the header row as the df header
                return df

    print("      -> ⚠️ Could not identify holdings table. Fallback to Table #0.")
    return dfs[0]

def clean_dataframe(df, ticker):
    if df is None or df.empty:
        return None

    # 1. Standardize columns
    df.columns = [str(c).strip().lower() for c in df.columns]
    
    # 2. Rename columns to standard format
    col_map = {
        'stockticker': 'ticker', 'symbol': 'ticker', 'holding': 'ticker', 'ticker': 'ticker',
        'identifier': 'ticker', # First Trust Keyword
        'securityname': 'name', 'company': 'name', 'security name': 'name', 'security_name': 'name', 'security': 'name',
        'weightings': 'weight', '% tna': 'weight', 'weight': 'weight', '% of net assets': 'weight', 
        'weighting': 'weight', # First Trust Keyword
        '%_of_net_assets': 'weight', '% net assets': 'weight'
    }
    df.rename(columns=col_map, inplace=True)

    # 3. Validation
    if 'ticker' not in df.columns or 'weight' not in df.columns:
        print(f"      -> Missing columns in {ticker}. Found: {list(df.columns)}")
        return None

    # 4. Filter Garbage Rows
    stop_words = ["cash", "usd", "liquidity", "government", "treasury", "money market", "net other", "total"]
    df['name'] = df['name'].astype(str)
    df['ticker'] = df['ticker'].astype(str)
    
    pattern = '|'.join(stop_words)
    mask = df['name'].str.contains(pattern, case=False, na=False) | \
           df['ticker'].str.contains(pattern, case=False, na=False)
    df = df[~mask].copy()

    # 5. Clean Ticker
    df['ticker'] = df['ticker'].str.replace(' USD', '', regex=False)
    df['ticker'] = df['ticker'].str.replace('.UN', '', regex=False)
    df['ticker'] = df['ticker'].str.upper().str.strip()

    # 6. Clean Weight
    if df['weight'].dtype == object:
        df['weight'] = df['weight'].astype(str).str.replace('%', '').str.replace(',', '')
    
    df['weight'] = pd.to_numeric(df['weight'], errors='coerce')
    
    if df['weight'].max() > 1.0:
        df['weight'] = df['weight'] / 100.0

    df['ETF_Ticker'] = ticker
    df['Date_Scraped'] = TODAY
    
    return df[['ETF_Ticker', 'ticker', 'name', 'weight', 'Date_Scraped']]

def main():
    try:
        with open(CONFIG_FILE, 'r') as f:
            etfs = json.load(f)
    except:
        print("❌ Config file not found.")
        return

    session = get_session()
    
    os.makedirs(DATA_DIR_LATEST, exist_ok=True)
    archive_path = os.path.join(DATA_DIR_HISTORY, *TODAY.split('-'))
    os.makedirs(archive_path, exist_ok=True)

    master_list = []

    for etf in etfs:
        if not etf.get('enabled', True): continue
        
        ticker = etf['ticker']
        print(f"➳ Processing {ticker}...")
        
        try:
            df = None
            
            # --- PACER ---
            if etf['scraper_type'] == 'pacer_csv':
                r = session.get(etf['url'], timeout=20)
                if r.status_code == 200:
                    content = r.text.splitlines()
                    start = 0
                    for i, line in enumerate(content):
                        if "Ticker" in line or "StockTicker" in line:
                            start = i; break
                    df = pd.read_csv(StringIO('\n'.join(content[start:])))

            # --- FIRST TRUST / ALPHA ---
            elif etf['scraper_type'] in ['first_trust', 'alpha_architect']:
                r = session.get(etf['url'], timeout=20)
                # Force pandas to look for header in the first few rows
                dfs = pd.read_html(r.text, flavor='bs4') 
                df = find_holdings_table(dfs)

            # --- INVESCO ---
            elif etf['scraper_type'] == 'invesco':
                # Try Magic Link
                dl_link = f"https://www.invesco.com/us/en/financial-products/etfs/holdings/main/holdings/0?ticker={ticker}&action=download"
                try:
                    r = session.get(dl_link, timeout=15, verify=False)
                    if r.status_code == 200 and len(r.content) > 100:
                        df = pd.read_csv(StringIO(r.text))
                        print("      -> Magic Link worked!")
                    else:
                        raise Exception("Magic link empty")
                except:
                    print("      -> Magic link failed. Trying page scrape...")
                    r = session.get(etf['url'], timeout=15)
                    dfs = pd.read_html(r.text)
                    df = find_holdings_table(dfs)

            # --- SAVE ---
            clean_df = clean_dataframe(df, ticker)
            
            if clean_df is not None and not clean_df.empty:
                save_path = os.path.join(DATA_DIR_LATEST, f"{ticker}.csv")
                clean_df.to_csv(save_path, index=False)
                master_list.append(clean_df)
                print(f"    ✅ Success: {len(clean_df)} rows saved.")
            else:
                print(f"    ⚠️ Failed to extract data.")

        except Exception as e:
            print(f"    ❌ Error: {e}")
        
        time.sleep(random.uniform(2, 5))

    if master_list:
        full_df = pd.concat(master_list)
        full_df.to_csv(os.path.join(archive_path, 'master_archive.csv'), index=False)
        print("\n📜 Daily Archive Complete.")

if __name__ == "__main__":
    main()