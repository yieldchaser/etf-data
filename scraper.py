import pandas as pd
import time
import json
import os
import re
import requests
from datetime import datetime
from io import StringIO
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

# --- CONFIG ---
CONFIG_FILE = 'config.json'
DATA_DIR_LATEST = 'data/latest'
DATA_DIR_BACKUP = 'data/invesco_backup'
TODAY = datetime.now().strftime('%Y-%m-%d')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

def extract_date_from_text(text):
    """ Standardized date cleaner for all sources """
    if not text: return TODAY
    # Regex to hunt for 'February 12, 2026' or '02/12/2026'
    pattern = r"([A-Z][a-z]+\s+\d{1,2},?\s+\d{4})|(\d{1,2}/\d{1,2}/\d{4})"
    match = re.search(pattern, text)
    if match:
        raw = match.group(0).replace(',', '')
        for fmt in ("%B %d %Y", "%m/%d/%Y", "%b %d %Y"):
            try:
                return datetime.strptime(raw, fmt).strftime('%Y-%m-%d')
            except: continue
    return TODAY

def clean_dataframe(df, ticker, h_date=TODAY):
    """ Restores the reliable V12 formatting logic """
    if df is None or df.empty: return None
    df.columns = [str(c).strip().lower() for c in df.columns]

    # Standard column mapping
    mappings = {
        'ticker': ['symbol', 'identifier', 'stock ticker', 'ticker'],
        'name': ['security name', 'company', 'holding', 'description', 'name'],
        'weight': ['% weight', 'weighting', 'weight %', '% net assets', 'weight']
    }
    for target, keywords in mappings.items():
        for col in df.columns:
            if any(k in col for k in keywords):
                df.rename(columns={col: target}, inplace=True)
                break

    if 'ticker' not in df.columns: return None
    
    # Weight cleaning
    if 'weight' in df.columns:
        df['weight'] = df['weight'].astype(str).str.replace('%', '').str.replace(',', '')
        df['weight'] = pd.to_numeric(df['weight'], errors='coerce').fillna(0.0)
        if df['weight'].max() > 1.0: df['weight'] = df['weight'] / 100.0
    
    df['ETF_Ticker'] = ticker
    df['Holdings_As_Of'] = h_date
    df['Date_Scraped'] = TODAY
    return df[['ETF_Ticker', 'ticker', 'name', 'weight', 'Holdings_As_Of', 'Date_Scraped']]

def setup_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    return webdriver.Chrome(options=options)

def main():
    try:
        with open(CONFIG_FILE, 'r') as f: etfs = json.load(f)
    except: return

    print(f"🚀 Launching Scraper V14.0 (Reliability + CMC Date Fix) - {TODAY}")
    driver = setup_driver()
    os.makedirs(DATA_DIR_LATEST, exist_ok=True)
    os.makedirs(DATA_DIR_BACKUP, exist_ok=True)

    for etf in etfs:
        if not etf.get('enabled', True): continue
        ticker = etf['ticker']
        print(f"➳ {ticker}...")
        
        try:
            df, h_date = None, TODAY
            
            # --- DATE SATELLITE (CompaniesMarketCap for Invesco) ---
            if "companiesmarketcap" in etf['url']:
                driver.get(etf['url'])
                time.sleep(2)
                h_date = extract_date_from_text(driver.find_element(By.TAG_NAME, "body").text)

            # --- RELIABLE SCRAPING LOGIC (RESTORED FROM V12) ---
            if etf['scraper_type'] in ['pacer_csv', 'selenium_alpha', 'first_trust']:
                driver.get(etf['url'])
                time.sleep(5) # Simple V12-style sleep
                if etf['scraper_type'] == 'first_trust':
                    # First Trust date is usually right in the header text
                    h_date = extract_date_from_text(driver.find_element(By.TAG_NAME, "body").text)
                
                dfs = pd.read_html(StringIO(driver.page_source))
            else:
                r = requests.get(etf['url'], headers=HEADERS, timeout=15)
                # StockAnalysis Date Fix
                if "stockanalysis" in etf['url']:
                    h_date = extract_date_from_text(r.text)
                dfs = pd.read_html(StringIO(r.text))

            for d in dfs:
                if len(d) > 8: 
                    df = d; break

            clean_df = clean_dataframe(df, ticker, h_date)
            if clean_df is not None:
                clean_df.to_csv(os.path.join(DATA_DIR_LATEST, f"{ticker}.csv"), index=False)
                print(f"    ✅ Success: {len(clean_df)} rows | Date: {h_date}")
            else:
                print(f"    ⚠️ No valid data for {ticker}")

        except Exception as e:
            print(f"    ❌ Error: {e}")

    driver.quit()

if __name__ == "__main__":
    main()
