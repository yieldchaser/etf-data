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

# Headers that worked for Pacer/First Trust
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
}

def setup_driver():
    """ Launches Headless Chrome for Alpha Architect & Invesco """
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled") 
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    return webdriver.Chrome(options=chrome_options)

def clean_dataframe(df, ticker):
    if df is None or df.empty: return None

    # FIX IMOM CRASH: Drop duplicate columns immediately
    df = df.loc[:, ~df.columns.duplicated()]

    # 1. Standardize columns
    df.columns = [str(c).strip().lower() for c in df.columns]
    
    # 2. Rename columns
    col_map = {
        'stockticker': 'ticker', 'symbol': 'ticker', 'holding': 'ticker', 'ticker': 'ticker',
        'identifier': 'ticker', 'sedol': 'ticker',
        'securityname': 'name', 'company': 'name', 'security name': 'name', 'security_name': 'name', 'security': 'name', 'name': 'name',
        'weightings': 'weight', '% tna': 'weight', 'weight': 'weight', '% of net assets': 'weight', 
        'weighting': 'weight', '%_of_net_assets': 'weight', '% net assets': 'weight'
    }
    df.rename(columns=col_map, inplace=True)

    # 3. Check critical columns
    if 'ticker' not in df.columns:
        print(f"      -> ⚠️ Missing 'ticker' column. Found: {list(df.columns)}")
        return None

    # 4. Filter Garbage
    stop_words = ["cash", "usd", "liquidity", "government", "treasury", "money market", "net other", "total"]
    df['name'] = df['name'].astype(str)
    df['ticker'] = df['ticker'].astype(str)
    
    # Ensure Ticker is clean before checking
    df['ticker'] = df['ticker'].str.upper().str.strip()
    
    pattern = '|'.join(stop_words)
    mask = df['name'].str.contains(pattern, case=False, na=False) | \
           df['ticker'].str.contains(pattern, case=False, na=False)
    df = df[~mask].copy()

    # 5. Clean Ticker Specifics
    df['ticker'] = df['ticker'].str.replace(' USD', '', regex=False)
    df['ticker'] = df['ticker'].str.replace('.UN', '', regex=False)

    # 6. Clean Weight
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
        with open(CONFIG_FILE, 'r') as f:
            etfs = json.load(f)
    except:
        print("❌ Config file not found.")
        return

    # Prepare Selenium Driver (Only used for Alpha & Invesco)
    print("🚀 Launching Scraper v11 (Restoration)...")
    driver = setup_driver()
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
            
            # --- STRATEGY 1: PACER (Reverted to Request/CSV) ---
            if etf['scraper_type'] == 'pacer_csv':
                try:
                    # Direct CSV download - Simple and Fast
                    r = session.get(etf['url'], timeout=20)
                    if r.status_code == 200:
                        content = r.text.splitlines()
                        start = 0
                        for i, line in enumerate(content[:20]):
                            if "Ticker" in line or "Symbol" in line:
                                start = i; break
                        df = pd.read_csv(StringIO('\n'.join(content[start:])))
                    else:
                        print(f"      -> HTTP {r.status_code} Error")
                except Exception as e:
                    print(f"      -> Pacer Error: {e}")

            # --- STRATEGY 2: FIRST TRUST (Reverted to Request/Pandas) ---
            elif etf['scraper_type'] == 'first_trust':
                try:
                    r = session.get(etf['url'], timeout=20)
                    dfs = pd.read_html(r.text)
                    # Find the table with 'Identifier' or 'Ticker'
                    for d in dfs:
                        cols = [str(c).lower() for c in d.columns]
                        if any(k in cols for k in ['ticker', 'identifier', 'symbol']):
                            df = d; break
                    if df is None and dfs: 
                        # Try header promotion (worked in v7)
                        d = dfs[0]
                        if not d.empty:
                            new_header = d.iloc[0]
                            d = d[1:]
                            d.columns = new_header
                            df = d
                except Exception as e:
                    print(f"      -> First Trust Error: {e}")

            # --- STRATEGY 3: ALPHA ARCHITECT (Selenium Click) ---
            elif 'alpha' in etf['url'] or etf['scraper_type'] == 'selenium_alpha':
                try:
                    page_url = f"https://funds.alphaarchitect.com/{ticker.lower()}/#fund-holdings"
                    driver.get(page_url)
                    time.sleep(5)
                    
                    # Click "All"
                    selects = driver.find_elements(By.TAG_NAME, "select")
                    for s in selects:
                        try:
                            Select(s).select_by_visible_text("All")
                            time.sleep(2)
                        except: pass
                    
                    # Scrape Table
                    dfs = pd.read_html(StringIO(driver.page_source))
                    for d in dfs:
                        if len(d) > 20: df = d; break
                except Exception as e:
                    print(f"      -> Alpha Architect Error: {e}")

            # --- STRATEGY 4: INVESCO (Selenium Simple) ---
            elif etf['scraper_type'] == 'selenium_invesco':
                try:
                    driver.get(etf['url'])
                    time.sleep(10) # Give it time
                    dfs = pd.read_html(StringIO(driver.page_source))
                    if dfs: df = dfs[0] # Take what we can get (usually top 10)
                except Exception as e:
                    print(f"      -> Invesco Error: {e}")

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
            print(f"    ❌ Critical Error: {e}")

    driver.quit()

    if master_list:
        full_df = pd.concat(master_list)
        full_df.to_csv(os.path.join(archive_path, 'master_archive.csv'), index=False)
        print("\n📜 Daily Archive Complete.")

if __name__ == "__main__":
    main()