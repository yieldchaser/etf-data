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
DATA_DIR_BACKUP = 'data/invesco_backup' # New backup folder
TODAY = datetime.now().strftime('%Y-%m-%d')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
}

def setup_driver():
    """ Launches Headless Chrome with Strict Timeouts """
    chrome_options = Options()
    chrome_options.add_argument("--headless=new") 
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    # Eager strategy for speed
    chrome_options.page_load_strategy = 'eager'
    
    driver = webdriver.Chrome(options=chrome_options)
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

def scrape_invesco_backup(driver, url, ticker):
    """ The Backup Track: Scrapes Top 10 from Official Invesco Site """
    try:
        print(f"      -> 🛡️ Running Backup Scraper for {ticker}...")
        driver.get(url)
        time.sleep(5)
        
        # Try full download first (Smart Cookie)
        try:
            s = requests.Session()
            for c in driver.get_cookies():
                s.cookies.set(c['name'], c['value'])
            s.headers.update({"User-Agent": driver.execute_script("return navigator.userAgent;")})
            dl_url = f"https://www.invesco.com/us/en/financial-products/etfs/holdings/main/holdings/0?ticker={ticker}&action=download"
            r = s.get(dl_url, timeout=10)
            if r.status_code == 200:
                lines = r.text.splitlines()
                start_row = 0
                found = False
                for i, line in enumerate(lines[:30]):
                    if "Ticker" in line or "Holding" in line or "Company" in line:
                        start_row = i
                        found = True
                        break
                if found:
                    df = pd.read_csv(StringIO("\n".join(lines[start_row:])))
                    return df
        except: pass

        # Fallback to visible table
        dfs = pd.read_html(StringIO(driver.page_source))
        for d in dfs:
            if 'ytd' in [str(c).lower() for c in d.columns]: continue
            if len(d) > 5: return d
            
    except Exception as e:
        print(f"      -> Backup Scraper Failed: {e}")
    return None

def clean_dataframe(df, ticker):
    if df is None or df.empty: return None

    # 1. Standardize columns
    df.columns = [str(c).strip().lower() for c in df.columns]
    
    # 2. Rename columns (Expanded for new sources)
    col_map = {
        'stockticker': 'ticker', 'symbol': 'ticker', 'holding': 'ticker', 'ticker': 'ticker',
        'identifier': 'ticker', 'sedol': 'ticker',
        'securityname': 'name', 'company': 'name', 'security name': 'name', 'security_name': 'name', 'security': 'name', 'name': 'name', 'company_name': 'name',
        'weightings': 'weight', '% tna': 'weight', 'weight': 'weight', '% of net assets': 'weight', 'weighting': 'weight', '%_of_net_assets': 'weight', '% net assets': 'weight', 'weight_%': 'weight', '%_weight': 'weight'
    }
    df.rename(columns=col_map, inplace=True)

    # 3. Deduplicate
    df = df.loc[:, ~df.columns.duplicated()]
    
    if 'ticker' not in df.columns:
        return None

    # 4. Filter Garbage
    stop_words = ["cash", "usd", "liquidity", "government", "treasury", "money market", "net other", "total"]
    df['name'] = df['name'].astype(str)
    df['ticker'] = df['ticker'].astype(str)
    
    pattern = '|'.join(stop_words)
    mask = (df['name'].str.contains(pattern, case=False, na=False) | 
            df['ticker'].str.contains(pattern, case=False, na=False))
    df = df[~mask].copy()

    # 5. Clean Ticker & Weight
    df['ticker'] = df['ticker'].str.replace(' USD', '', regex=False)
    df['ticker'] = df['ticker'].str.replace('.UN', '', regex=False)
    df['ticker'] = df['ticker'].str.upper().str.strip()

    if 'weight' in df.columns:
        if df['weight'].dtype == object:
            df['weight'] = df['weight'].astype(str).str.replace('%', '').str.replace(',', '')
        df['weight'] = pd.to_numeric(df['weight'], errors='coerce')
        # Handle cases where weight is 3.5 (percent) vs 0.035 (decimal)
        # Assuming if max > 1, it is a percentage
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

    print("🚀 Launching Scraper V13.0 (Dual-Track System)...")
    
    driver = None
    session = requests.Session()
    session.headers.update(HEADERS)
    
    os.makedirs(DATA_DIR_LATEST, exist_ok=True)
    os.makedirs(DATA_DIR_HISTORY, exist_ok=True)
    os.makedirs(DATA_DIR_BACKUP, exist_ok=True)
    
    archive_path = os.path.join(DATA_DIR_HISTORY, *TODAY.split('-'))
    os.makedirs(archive_path, exist_ok=True)
    master_list = []

    for etf in etfs:
        if not etf.get('enabled', True): continue
        ticker = etf['ticker']
        print(f"➳ Processing {ticker}...")
        
        try:
            df = None
            
            # --- TRACK A: PRIMARY SOURCES ---
            
            # 1. Pacer (CSV)
            if etf['scraper_type'] == 'pacer_csv':
                r = session.get(etf['url'], timeout=20)
                if r.status_code == 200:
                    content = r.text.splitlines()
                    start = 0
                    for i, line in enumerate(content[:20]):
                        if "Ticker" in line or "Symbol" in line: start = i; break
                    df = pd.read_csv(StringIO('\n'.join(content[start:])))

            # 2. First Trust (HTML)
            elif etf['scraper_type'] == 'first_trust':
                r = session.get(etf['url'], timeout=20)
                dfs = pd.read_html(r.text)
                df = find_first_trust_table(dfs)

            # 3. Alpha Architect (Selenium)
            elif etf['scraper_type'] == 'selenium_alpha':
                if driver is None: driver = setup_driver() 
                try: driver.get(etf['url'])
                except: driver.execute_script("window.stop();")
                time.sleep(2)
                try:
                    selects = driver.find_elements(By.TAG_NAME, "select")
                    for s in selects:
                        try: Select(s).select_by_visible_text("All"); time.sleep(2)
                        except: pass
                except: pass
                dfs = pd.read_html(StringIO(driver.page_source))
                for d in dfs: 
                    if len(d) > 25: df = d; break

            # 4. CompaniesMarketCap (New Third-Party)
            elif etf['scraper_type'] == 'companies_market_cap':
                r = session.get(etf['url'], timeout=20)
                # This site usually has a clean table class="table"
                dfs = pd.read_html(r.text)
                for d in dfs:
                    if len(d) > 30: # Look for the big table
                        df = d
                        # Rename specifically for this site if needed
                        if 'Name' in df.columns and 'Ticker' in df.columns:
                            break

            # 5. StockAnalysis (New Third-Party)
            elif etf['scraper_type'] == 'stock_analysis':
                r = session.get(etf['url'], timeout=20)
                dfs = pd.read_html(r.text)
                for d in dfs:
                    # StockAnalysis tables often have "No." "Symbol" "Name"
                    if 'Symbol' in d.columns and 'Name' in d.columns:
                        d.rename(columns={'Symbol': 'Ticker'}, inplace=True)
                        df = d
                        break

            # --- TRACK B: BACKUP FOR INVESCO ---
            if 'backup_url' in etf:
                if driver is None: driver = setup_driver()
                backup_df = scrape_invesco_backup(driver, etf['backup_url'], ticker)
                clean_backup = clean_dataframe(backup_df, ticker)
                if clean_backup is not None:
                    backup_path = os.path.join(DATA_DIR_BACKUP, f"{ticker}_official_backup.csv")
                    clean_backup.to_csv(backup_path, index=False)
                    print(f"      -> 🛡️ Backup saved to {backup_path}")

            # --- SAVE PRIMARY DATA ---
            clean_df = clean_dataframe(df, ticker)
            
            if clean_df is not None and not clean_df.empty:
                save_path = os.path.join(DATA_DIR_LATEST, f"{ticker}.csv")
                clean_df.to_csv(save_path, index=False)
                master_list.append(clean_df)
                print(f"    ✅ Success: {len(clean_df)} rows saved.")
            else:
                print(f"    ⚠️ Primary Data not found.")

        except Exception as e:
            print(f"    ❌ Error: {e}")
            if driver:
                try: driver.quit()
                except: pass
                driver = None

    if driver: driver.quit()

    if master_list:
        full_df = pd.concat(master_list)
        full_df.to_csv(os.path.join(archive_path, 'master_archive.csv'), index=False)
        print("\n📜 Daily Archive Complete.")

if __name__ == "__main__":
    main()