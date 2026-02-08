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
    """ Launches Headless Chrome with Strict Timeouts (Fixed QMOM) """
    chrome_options = Options()
    chrome_options.add_argument("--headless=new") 
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    # Eager strategy: Access DOM as soon as possible
    chrome_options.page_load_strategy = 'eager'
    
    driver = webdriver.Chrome(options=chrome_options)
    
    # CRITICAL QMOM FIX: Hard limit on page loading. 
    driver.set_page_load_timeout(30) 
    
    return driver

def find_first_trust_table(dfs):
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

    # 3. CRITICAL IMOM FIX: Nuclear Deduplication
    # If we have two 'ticker' columns, keep the first one
    df = df.loc[:, ~df.columns.duplicated()]
    
    # Double Check: If 'ticker' is STILL a DataFrame (rare edge case), force select the first one
    if isinstance(df['ticker'], pd.DataFrame):
        print("      -> ⚠️ Found duplicate 'ticker' columns. Forcing selection of first column.")
        df['ticker'] = df['ticker'].iloc[:, 0]

    if 'ticker' not in df.columns:
        print(f"      -> ⚠️ Missing 'ticker' column. Found: {list(df.columns)}")
        return None

    stop_words = ["cash", "usd", "liquidity", "government", "treasury", "money market", "net other", "total"]
    df['name'] = df['name'].astype(str)
    
    # 4. Safe String Conversion
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
    except:
        print("❌ Config file not found.")
        return

    print("🚀 Launching Scraper V12.7 (The Convergence)...")
    
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
            
            # --- PACER ---
            if etf['scraper_type'] == 'pacer_csv':
                r = session.get(etf['url'], timeout=20)
                if r.status_code == 200:
                    content = r.text.splitlines()
                    start = 0
                    for i, line in enumerate(content[:20]):
                        if "Ticker" in line or "Symbol" in line:
                            start = i; break
                    df = pd.read_csv(StringIO('\n'.join(content[start:])))

            # --- FIRST TRUST ---
            elif etf['scraper_type'] == 'first_trust':
                r = session.get(etf['url'], timeout=20)
                dfs = pd.read_html(r.text)
                df = find_first_trust_table(dfs)

            # --- ALPHA ARCHITECT (QMOM/IMOM) ---
            elif 'alpha' in etf['url'] or etf['scraper_type'] == 'selenium_alpha':
                if driver is None: driver = setup_driver() 
                
                # TIMEOUT HANDLER (QMOM Fix):
                try:
                    driver.get(etf['url'])
                except:
                    print("      -> Page load timed out (Expected for QMOM). Stopping load...")
                    driver.execute_script("window.stop();")
                
                time.sleep(2) 
                
                # Expand "All"
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

            # --- INVESCO ---
            elif etf['scraper_type'] == 'selenium_invesco':
                if driver is None: driver = setup_driver()
                
                # TIMEOUT HANDLER:
                try:
                    driver.get(etf['url'])
                except:
                    print("      -> Page load timed out. Stopping load...")
                    driver.execute_script("window.stop();")
                
                time.sleep(5)
                
                # Attempt 1: Full Download
                try:
                    s = requests.Session()
                    for c in driver.get_cookies():
                        s.cookies.set(c['name'], c['value'])
                    s.headers.update({"User-Agent": driver.execute_script("return navigator.userAgent;")})
                    
                    dl_url = f"https://www.invesco.com/us/en/financial-products/etfs/holdings/main/holdings/0?ticker={ticker}&action=download"