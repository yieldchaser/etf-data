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
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

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
    Enhanced Date Hunter: Scans for multiple date formats across all families.
    """
    if not text: return "Unknown"
    text = " ".join(text.split())
    
    # Expanded patterns to catch Pacer, Invesco, and StockAnalysis variations
    patterns = [
        r"(?:as of|effective|holdings as of|date of|as of date)\s*[:\s]*([A-Z][a-z]+\s+\d{1,2},?\s+\d{4})", 
        r"(?:as of|effective|holdings as of|date of)\s*[:\s]*(\d{1,2}/\d{1,2}/\d{4})",
        r"(?:as of|effective|holdings as of)\s*[:\s]*(\d{4}-\d{2}-\d{2})"
    ]
    
    for p in patterns:
        match = re.search(p, text, re.IGNORECASE)
        if match:
            raw_date = match.group(1).replace(',', '')
            for fmt in ("%B %d %Y", "%m/%d/%Y", "%Y-%m-%d", "%b %d %Y"):
                try:
                    return datetime.strptime(raw_date, fmt).strftime('%Y-%m-%d')
                except: continue
    return "Unknown"

def clean_dataframe(df, ticker, h_date="Unknown"):
    if df is None or df.empty: return None
    df.columns = [str(c).strip().lower() for c in df.columns]

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
    driver = webdriver.Chrome(options=options)
    return driver

def main():
    try:
        with open(CONFIG_FILE, 'r') as f: etfs = json.load(f)
    except: return

    print(f"🚀 Running Scraper V13.5 (Pacer & Date Fixes) - {TODAY}")
    driver = None
    os.makedirs(DATA_DIR_LATEST, exist_ok=True)
    os.makedirs(DATA_DIR_BACKUP, exist_ok=True)

    for etf in etfs:
        if not etf.get('enabled', True): continue
        ticker = etf['ticker']
        print(f"➳ {ticker}...")
        
        try:
            df, h_date = None, "Unknown"

            # Use Selenium for PACER and any failing primary sources
            if etf['scraper_type'] in ['pacer_csv', 'selenium_alpha'] or ticker in ['COWZ', 'CALF']:
                if driver is None: driver = setup_driver()
                driver.get(etf['url'])
                # Wait for content to load for Pacer
                WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                page_source = driver.page_source
                page_text = driver.find_element(By.TAG_NAME, "body").text
                h_date = extract_holdings_date(page_text)
                dfs = pd.read_html(StringIO(page_source))
                for d in dfs:
                    if len(d) > 5: df = d; break
            else:
                # Standard Requests logic for everything else
                r = requests.get(etf['url'], headers=HEADERS, timeout=20)
                h_date = extract_holdings_date(r.text)
                dfs = pd.read_html(StringIO(r.text))
                for d in dfs:
                    if len(d) > 10: df = d; break

            # Process Data
            clean_df = clean_dataframe(df, ticker, h_date)
            if clean_df is not None:
                clean_df.to_csv(os.path.join(DATA_DIR_LATEST, f"{ticker}.csv"), index=False)
                print(f"    ✅ Rows: {len(clean_df)} | As Of: {h_date}")
            else:
                print(f"    ⚠️ No valid table found for {ticker}")

        except Exception as e:
            print(f"    ❌ Failed: {e}")

    if driver: driver.quit()

if __name__ == "__main__":
    main()
