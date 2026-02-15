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
import re

# --- CONFIG ---
CONFIG_FILE = 'config.json'
DATA_DIR_LATEST = 'data/latest'
DATA_DIR_HISTORY = 'data/history'
DATA_DIR_BACKUP = 'data/invesco_backup'
TODAY = datetime.now().strftime('%Y-%m-%d')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
}

def extract_holdings_date(text):
    """
    Smart Date Hunter: Scans text for dates following 'as of', 'effective', etc.
    Future-proofed to handle various formats and text layouts.
    """
    if not text: return None
    
    # 1. Clean the text (remove extra spaces/newlines)
    text = " ".join(text.split())
    
    # 2. Look for keywords + a date pattern
    # Patterns: Month DD, YYYY | MM/DD/YYYY | DD-Mon-YYYY
    date_patterns = [
        r"(?:as of|effective|holdings as of|date of)\s+([A-Z][a-z]+\s+\d{1,2},?\s+\d{4})", # Feb 12, 2026
        r"(?:as of|effective|holdings as of|date of)\s+(\d{1,2}/\d{1,2}/\d{4})",         # 02/17/2026
        r"(?:as of|effective|holdings as of|date of)\s+(\d{4}-\d{2}-\d{2})"             # 2026-02-12
    ]
    
    for pattern in date_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            raw_date = match.group(1)
            try:
                # Standardize to YYYY-MM-DD
                for fmt in ("%B %d, %Y", "%m/%d/%Y", "%Y-%m-%d", "%b %d, %Y"):
                    try:
                        return datetime.strptime(raw_date.replace(',', ''), fmt.replace(',', '')).strftime('%Y-%m-%d')
                    except: continue
            except: pass
    return None

def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new") 
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.page_load_strategy = 'eager'
    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(30) 
    return driver

def clean_dataframe(df, ticker, holdings_date=None):
    if df is None or df.empty: return None

    df.columns = [str(c).strip().lower() for c in df.columns]
    
    col_map = {
        'stockticker': 'ticker', 'symbol': 'ticker', 'holding': 'ticker', 
        'identifier': 'ticker', 'securityname': 'name', 'company': 'name',
        '% net assets': 'weight', '% tna': 'weight', 'weighting': 'weight', 
        '% portfolio weight': 'weight', '% weight': 'weight'
    }
    df.rename(columns=col_map, inplace=True)

    for col in df.columns:
        if 'weight' in col and col != 'weight':
            df.rename(columns={col: 'weight'}, inplace=True)

    df = df.loc[:, ~df.columns.duplicated()]
    if 'ticker' not in df.columns: return None

    # Cleaning weights
    if 'weight' in df.columns:
        df['weight'] = df['weight'].astype(str).str.replace('%', '').str.replace(',', '')
        df['weight'] = pd.to_numeric(df['weight'], errors='coerce')
        if df['weight'].max() > 1.0: df['weight'] = df['weight'] / 100.0
    else:
        df['weight'] = 0.0

    df['ETF_Ticker'] = ticker
    df['Holdings_As_Of'] = holdings_date if holdings_date else "Unknown"
    df['Date_Scraped'] = TODAY
    
    return df[['ETF_Ticker', 'ticker', 'name', 'weight', 'Holdings_As_Of', 'Date_Scraped']]

def scrape_invesco_backup(driver, url, ticker):
    try:
        print(f"      -> 🛡️ Running Backup Scraper for {ticker}...")
        driver.get(url)
        time.sleep(5)
        
        # Extract Holdings Date from page text
        page_text = driver.find_element(By.TAG_NAME, "body").text
        h_date = extract_holdings_date(page_text)

        # Download logic
        dl_url = f"https://www.invesco.com/us/en/financial-products/etfs/holdings/main/holdings/0?ticker={ticker}&action=download"
        r = requests.get(dl_url, headers=HEADERS, timeout=10)
        if r.status_code == 200:
            lines = r.text.splitlines()
            for i, line in enumerate(lines[:30]):
                if "Ticker" in line or "Holding" in line:
                    return pd.read_csv(StringIO("\n".join(lines[i:]))), h_date
    except Exception as e:
        print(f"      -> Backup Failed: {e}")
    return None, None

def main():
    try:
        with open(CONFIG_FILE, 'r') as f: etfs = json.load(f)
    except: return

    print("🚀 Launching Scraper V13.3 (House Cleaning - Smart Dates)...")
    driver = None
    session = requests.Session()
    session.headers.update(HEADERS)
    
    os.makedirs(DATA_DIR_LATEST, exist_ok=True)
    os.makedirs(DATA_DIR_BACKUP, exist_ok=True)
    master_list = []

    for etf in etfs:
        if not etf.get('enabled', True): continue
        ticker = etf['ticker']
        print(f"➳ Processing {ticker}...")
        df, h_date = None, None
        
        try:
            # --- PRIMARY TRACK ---
            if etf['scraper_type'] == 'pacer_csv':
                r = session.get(etf['url'])
                h_date = extract_holdings_date(r.text[:500]) # Look in header of CSV
                df = pd.read_csv(StringIO(r.text), skiprows=3) # Standard Pacer skip

            elif etf['scraper_type'] in ['stock_analysis', 'companies_market_cap']:
                r = session.get(etf['url'])
                h_date = extract_holdings_date(r.text)
                dfs = pd.read_html(StringIO(r.text))
                for d in dfs:
                    if len(d) > 20: df = d; break

            elif etf['scraper_type'] == 'first_trust':
                r = session.get(etf['url'])
                h_date = extract_holdings_date(r.text)
                dfs = pd.read_html(StringIO(r.text))
                for d in dfs:
                    if 'Identifier' in str(d.columns): df = d; break

            elif etf['scraper_type'] == 'selenium_alpha':
                if driver is None: driver = setup_driver()
                driver.get(etf['url'])
                time.sleep(3)
                h_date = extract_holdings_date(driver.find_element(By.TAG_NAME, "body").text)
                dfs = pd.read_html(StringIO(driver.page_source))
                for d in dfs:
                    if len(d) > 20: df = d; break

            # --- BACKUP TRACK ---
            if 'backup_url' in etf:
                if driver is None: driver = setup_driver()
                b_df, b_date = scrape_invesco_backup(driver, etf['backup_url'], ticker)
                clean_b = clean_dataframe(b_df, ticker, b_date)
                if clean_b is not None:
                    clean_b.to_csv(os.path.join(DATA_DIR_BACKUP, f"{ticker}_official_backup.csv"), index=False)

            # --- CLEAN & SAVE ---
            clean_df = clean_dataframe(df, ticker, h_date)
            if clean_df is not None:
                clean_df.to_csv(os.path.join(DATA_DIR_LATEST, f"{ticker}.csv"), index=False)
                master_list.append(clean_df)
                print(f"    ✅ Success: {len(clean_df)} rows. Holdings Date: {h_date}")

        except Exception as e:
            print(f"    ❌ Error: {e}")

    if driver: driver.quit()
    if master_list:
        pd.concat(master_list).to_csv('data/master_latest.csv', index=False)

if __name__ == "__main__":
    main()
