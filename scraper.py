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

def extract_holdings_date(text):
    """
    Scans text for dates following keywords like 'as of', 'effective', etc.
    Standardizes various formats (Feb 12, 2026, 02/12/2026, etc.) into YYYY-MM-DD.
    """
    if not text: return "Unknown"
    text = " ".join(text.split()) # Clean whitespace
    
    # regex patterns for date extraction
    patterns = [
        r"(?:as of|effective|holdings as of|date of)\s+([A-Z][a-z]+\s+\d{1,2},?\s+\d{4})", 
        r"(?:as of|effective|holdings as of|date of)\s+(\d{1,2}/\d{1,2}/\d{4})",
        r"(?:as of|effective)\s+date\s*[:\s]*(\d{1,2}/\d{1,2}/\d{4})"
    ]
    
    for p in patterns:
        match = re.search(p, text, re.IGNORECASE)
        if match:
            raw_date = match.group(1).replace(',', '')
            for fmt in ("%B %d %Y", "%m/%d/%Y", "%b %d %Y", "%d %b %Y"):
                try:
                    return datetime.strptime(raw_date, fmt).strftime('%Y-%m-%d')
                except: continue
    return "Unknown"

def clean_dataframe(df, ticker, h_date="Unknown"):
    if df is None or df.empty: return None
    df.columns = [str(c).strip().lower() for c in df.columns]

    # Broad keyword mapping to catch weights and tickers across families
    mappings = {
        'ticker': ['symbol', 'identifier', 'stock ticker', 'holding ticker', 'ticker'],
        'name': ['security name', 'company', 'holding', 'description', 'name'],
        'weight': ['% weight', 'weighting', 'weight %', '% net assets', '% tna', 'weight']
    }

    for target, keywords in mappings.items():
        for col in df.columns:
            if any(k in col for k in keywords):
                df.rename(columns={col: target}, inplace=True)
                break

    if 'ticker' not in df.columns: return None

    # Handle numeric weights correctly
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
    return webdriver.Chrome(options=options)

def main():
    try:
        with open(CONFIG_FILE, 'r') as f: etfs = json.load(f)
    except: return

    print(f"🚀 Running Scraper V13.4 - {TODAY}")
    driver = None
    os.makedirs(DATA_DIR_LATEST, exist_ok=True)
    os.makedirs(DATA_DIR_BACKUP, exist_ok=True)

    for etf in etfs:
        if not etf.get('enabled', True): continue
        ticker = etf['ticker']
        print(f"➳ {ticker}...")
        
        try:
            r = requests.get(etf['url'], headers=HEADERS, timeout=20)
            page_text = r.text
            h_date = extract_holdings_date(page_text)
            
            # Use pandas to read tables directly where possible
            dfs = pd.read_html(StringIO(page_text))
            df = None
            for d in dfs:
                if len(d) > 5: # Valid data tables usually have multiple rows
                    df = d
                    break
            
            # Special handling for Invesco Backups
            if 'backup_url' in etf:
                if driver is None: driver = setup_driver()
                driver.get(etf['backup_url'])
                time.sleep(3)
                backup_text = driver.find_element(By.TAG_NAME, "body").text
                b_date = extract_holdings_date(backup_text)
                # Invesco specific download logic can be added here
                print(f"      🛡️ Backup Date: {b_date}")

            clean_df = clean_dataframe(df, ticker, h_date)
            if clean_df is not None:
                clean_df.to_csv(os.path.join(DATA_DIR_LATEST, f"{ticker}.csv"), index=False)
                print(f"    ✅ Rows: {len(clean_df)} | As Of: {h_date}")

        except Exception as e:
            print(f"    ❌ Failed: {e}")

    if driver: driver.quit()

if __name__ == "__main__":
    main()
