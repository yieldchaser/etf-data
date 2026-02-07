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
    """
    Specific logic for First Trust (FPX/FPXI) to handle missing headers.
    """
    if not dfs: return None
    
    valid_keywords = ['ticker', 'symbol', 'holding', 'identifier', 'weighting', 'cusip']
    
    for i, df in enumerate(dfs):
        # 1. Check existing headers
        cols = [str(c).strip().lower() for c in df.columns]
        if any(k in cols for k in valid_keywords):
            return df
        
        # 2. Check Row 0 (Header Promotion)
        # This fixes the "Missing columns: ['0']" error
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

    # 3. FIX IMOM CRASH: Remove Duplicate Columns
    # If we have two 'ticker' columns, keep the first one
    df = df.loc[:, ~df.columns.duplicated()]

    # 4. Check critical columns
    if 'ticker' not in df.columns:
        print(f"      -> ⚠️ Missing 'ticker' column. Found: {list(df.columns)}")
        return None

    # 5. Filter Garbage
    stop_words = ["cash", "usd", "liquidity", "government", "treasury", "money market", "net other", "total"]
    df['name'] = df['name'].astype(str)
    
    # Force Ticker to String
    df['ticker'] = df['ticker'].astype(str)
    
    pattern = '|'.join(stop_words)
    mask = df['name'].str.contains(pattern, case=False, na=False) | \
           df['ticker'].str.contains(pattern, case=False, na=False)
    df = df[~mask].copy()

    # 6. Clean Ticker
    df['ticker'] = df['ticker'].str.replace(' USD', '', regex=False)
    df['ticker'] = df['ticker'].str.replace('.UN', '', regex=False)
    df['ticker'] = df['ticker'].str.upper().str.strip()

    # 7. Clean Weight
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

    print("🚀 Launching Scraper v12 (The Fixer)...")
    
    # We only start Selenium if we need it (for Alpha Architect)
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
            
            # --- PACER (Requests/CSV) ---
            if etf['scraper_type'] == 'pacer_csv':
                r = session.get(etf['url'], timeout=20)
                if r.status_code == 200:
                    content = r.text.splitlines()
                    start = 0
                    for i, line in enumerate(content[:20]):
                        if "Ticker" in line or "Symbol" in line:
                            start = i; break
                    df = pd.read_csv(StringIO('\n'.join(content[start:])))

            # --- FIRST TRUST (Requests/Pandas + Header Fix) ---
            elif etf['scraper_type'] == 'first_trust':
                r = session.get(etf['url'], timeout=20)
                dfs = pd.read_html(r.text)
                df = find_first_trust_table(dfs)

            # --- ALPHA ARCHITECT (Selenium) ---
            elif 'alpha' in etf['url'] or etf['scraper_type'] == 'selenium_alpha':
                if driver is None: driver = setup_driver() # Lazy load driver
                
                driver.get(etf['url'])
                time.sleep(5)
                
                # Click "All"
                try:
                    selects = driver.find_elements(By.TAG_NAME, "select")
                    for s in selects:
                        try:
                            Select(s).select_by_visible_text("All")
                            time.sleep(2)
                        except: pass
                except: pass
                
                # Scrape
                dfs = pd.read_html(StringIO(driver.page_source))
                for d in dfs:
                    if len(d) > 25: df = d; break

                        # --- INVESCO FINAL (REAL BROWSER DOWNLOAD EMULATION) ---
            elif etf['scraper_type'] == 'selenium_invesco':
                if driver is None:
                    driver = setup_driver()

                driver.get(etf['url'])
                time.sleep(6)

                print("      -> Initiating real browser download...")

                # get cookies from real selenium session
                cookies = driver.get_cookies()
                s = requests.Session()

                for c in cookies:
                    s.cookies.set(c['name'], c['value'])

                download_url = f"https://www.invesco.com/us/en/financial-products/etfs/holdings/0?ticker={ticker}&action=download"

                headers = {
                    "User-Agent": driver.execute_script("return navigator.userAgent;"),
                    "Accept": "text/csv,application/vnd.ms-excel,application/octet-stream",
                    "Referer": etf['url'],
                    "Origin": "https://www.invesco.com",
                    "Connection": "keep-alive"
                }

                r = s.get(download_url, headers=headers)

                text = r.text

                # detect bot-block html
                if "<!doctype html" in text.lower():
                    print("      -> Blocked by WAF. Retrying with browser fetch...")

                    # use selenium to fetch via JS (bypasses WAF)
                    text = driver.execute_script(f"""
                        return fetch("{download_url}", {{
                            credentials: 'include'
                        }}).then(r => r.text());
                    """)

                if "Ticker" in text or "Holding" in text:
                    print("      -> CSV received")

                    lines = text.splitlines()
                    start = 0

                    for i, line in enumerate(lines[:40]):
                        if "Ticker" in line or "Holding" in line or "Security" in line:
                            start = i
                            break

                    csv_data = "\n".join(lines[start:])

                    df = pd.read_csv(
                        StringIO(csv_data),
                        engine="python",
                        on_bad_lines="skip"
                    )

                else:
                    print("      -> FAILED to retrieve holdings")
                    df = None




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