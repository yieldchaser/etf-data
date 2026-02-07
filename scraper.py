import pandas as pd
import time
import json
import os
from datetime import datetime
from io import StringIO
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
import requests

# --- CONFIG ---
CONFIG_FILE = 'config.json'
DATA_DIR_LATEST = 'data/latest'
DATA_DIR_HISTORY = 'data/history'
TODAY = datetime.now().strftime('%Y-%m-%d')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
}

def setup_driver():
    """ Launches Headless Chrome """
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled") 
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    return webdriver.Chrome(options=chrome_options)

def find_first_trust_table(dfs):
    """ DO NOT TOUCH: Logic to handle First Trust missing headers """
    if not dfs: return None
    valid_keywords = ['ticker', 'symbol', 'holding', 'identifier', 'weighting', 'cusip']
    for i, df in enumerate(dfs):
        cols = [str(c).strip().lower() for c in df.columns]
        if any(k in cols for k in valid_keywords): return df
        if not df.empty:
            first_row = [str(x).strip().lower() for x in df.iloc[0].values]
            if any(k in first_row for k in valid_keywords):
                print(f"      -> Promoting header in Table #{i}")
                new_header = df.iloc[0]
                df = df[1:] 
                df.columns = new_header
                return df
    return None

def clean_dataframe(df, ticker):
    if df is None or df.empty: return None

    # --- CRITICAL DO NOT TOUCH: FIX FOR IMOM DUPLICATES ---
    # Forces unique column names before processing
    df = df.loc[:, ~df.columns.duplicated()]
    # ------------------------------------------------------

    df.columns = [str(c).strip().lower() for c in df.columns]
    
    col_map = {
        'stockticker': 'ticker', 'symbol': 'ticker', 'holding': 'ticker', 'ticker': 'ticker',
        'identifier': 'ticker', 'sedol': 'ticker',
        'securityname': 'name', 'company': 'name', 'security name': 'name', 'security_name': 'name', 'security': 'name', 'name': 'name',
        'weightings': 'weight', '% tna': 'weight', 'weight': 'weight', '% of net assets': 'weight', 
        'weighting': 'weight', '%_of_net_assets': 'weight', '% net assets': 'weight'
    }
    df.rename(columns=col_map, inplace=True)

    if 'ticker' not in df.columns:
        print(f"      -> ⚠️ Missing 'ticker' column. Found: {list(df.columns)}")
        return None

    stop_words = ["cash", "usd", "liquidity", "government", "treasury", "money market", "net other", "total"]
    df['name'] = df['name'].astype(str)
    
    # Force Ticker to String
    df['ticker'] = df['ticker'].astype(str)
    
    pattern = '|'.join(stop_words)
    mask = df['name'].str.contains(pattern, case=False, na=False) | \
           df['ticker'].str.contains(pattern, case=False, na=False)
    df = df[~mask].copy()

    df['ticker'] = df['ticker'].str.replace(' USD', '', regex=False)
    df['ticker'] = df['ticker'].str.replace('.UN', '', regex=False)
    df['ticker'] = df['ticker'].str.upper().str.strip()

    if 'weight' in df.columns:
        if df['weight'].dtype == object:
            df['weight'] = df['weight'].astype(str).str.replace('%', '').str.replace(',', '')
        df['weight'] = pd.to_numeric(df['weight'], errors='coerce')
        if df['weight'].max() > 1.0:
            df['weight'] = df['weight'] / 100.0
    else:
        df['weight'] = 0.0

    df['ETF_Ticker'] = ticker
    df['Date_Scraped'] = TODAY
    return df[['ETF_Ticker', 'ticker', 'name', 'weight', 'Date_Scraped']]

def main():
    try:
        with open(CONFIG_FILE, 'r') as f: etfs = json.load(f)
    except: return

    print("🚀 Launching Scraper v12 (Restored Stable Version)...")
    
    driver = None
    session = requests.Session()
    session.headers.update(HEADERS)
    
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
            
            # --- PACER (DO NOT TOUCH - WORKING) ---
            if etf['scraper_type'] == 'pacer_csv':
                r = session.get(etf['url'], timeout=20)
                if r.status_code == 200:
                    content = r.text.splitlines()
                    start = 0
                    for i, line in enumerate(content[:20]):
                        if "Ticker" in line or "Symbol" in line:
                            start = i; break
                    df = pd.read_csv(StringIO('\n'.join(content[start:])))

            # --- FIRST TRUST (DO NOT TOUCH - WORKING) ---
            elif etf['scraper_type'] == 'first_trust':
                r = session.get(etf['url'], timeout=20)
                dfs = pd.read_html(r.text)
                df = find_first_trust_table(dfs)

            # --- ALPHA ARCHITECT (DO NOT TOUCH - WORKING) ---
            elif 'alpha' in etf['url'] or etf['scraper_type'] == 'selenium_alpha':
                if driver is None: driver = setup_driver()
                driver.get(etf['url'])
                time.sleep(5)
                try:
                    selects = driver.find_elements(By.TAG_NAME, "select")
                    for s in selects:
                        try:
                            Select(s).select_by_visible_text("All")
                            time.sleep(2)
                        except: pass
                except: pass
                dfs = pd.read_html(StringIO(driver.page_source))
                for d in dfs:
                    if len(d) > 25: df = d; break

            # --- INVESCO (THE PROBLEM CHILD) ---
            # Current Status: Works but only gets Top 10 rows
            # Need: Full list via 'View All' or Export
            elif etf['scraper_type'] == 'selenium_invesco':
                if driver is None: driver = setup_driver()
                driver.get(etf['url'])
                time.sleep(8)
                
                # Currently just grabs what is visible (Top 10)
                dfs = pd.read_html(StringIO(driver.page_source))
                for d in dfs:
                    cols = [str(c).lower() for c in d.columns]
                    if 'ytd' in cols: continue 
                    if len(d) > 5:
                        df = d
                        break

            # --- SAVE ---
            clean_df = clean_dataframe(df, ticker)
            
            if clean_df is not None and not clean_df.empty:
                save_path = os.path.join(DATA_DIR_LATEST, f"{ticker}.csv")
                clean_df.to_csv(save_path, index=False)
                master_list.append(clean_df)
                print(f"    ✅ Success: {len(clean_df)} rows saved.")
            else:
                print(f"    ⚠️ Data not found.")

        except Exception as e:
            print(f"    ❌ Error: {e}")

    if driver: driver.quit()

    if master_list:
        full_df = pd.concat(master_list)
        full_df.to_csv(os.path.join(archive_path, 'master_archive.csv'), index=False)
        print("\n📜 Daily Archive Complete.")

if __name__ == "__main__":
    main()