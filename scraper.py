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
    """ Scans text for dates following keywords. Standardizes to YYYY-MM-DD. """
    if not text: return "Unknown"
    text = " ".join(text.split())
    patterns = [
        r"(?:as of|effective|holdings as of|date of)\s*[:\s]*([A-Z][a-z]+\s+\d{1,2},?\s+\d{4})", 
        r"(?:as of|effective|holdings as of|date of)\s*[:\s]*(\d{1,2}/\d{1,2}/\d{4})",
        r"(?:as of|effective|holdings as of|date of)\s*[:\s]*(\d{1,2}-[A-Z][a-z]+-\d{4})"
    ]
    for p in patterns:
        match = re.search(p, text, re.IGNORECASE)
        if match:
            raw_date = match.group(1).replace(',', '')
            for fmt in ("%B %d %Y", "%m/%d/%Y", "%d-%b-%Y", "%b %d %Y"):
                try:
                    return datetime.strptime(raw_date, fmt).strftime('%Y-%m-%d')
                except: continue
    return "Unknown"

def clean_dataframe(df, ticker, h_date="Unknown"):
    """ The missing function! Standardizes columns across all families. """
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
    options.add_argument("--window-size=1920,1080")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    return webdriver.Chrome(options=options)

def main():
    try:
        with open(CONFIG_FILE, 'r') as f: etfs = json.load(f)
    except Exception as e:
        print(f"Error loading config: {e}")
        return

    print(f"🚀 Launching Scraper V13.7 (Fixed Missing Function) - {TODAY}")
    driver = None
    os.makedirs(DATA_DIR_LATEST, exist_ok=True)

    for etf in etfs:
        if not etf.get('enabled', True): continue
        ticker = etf['ticker']
        print(f"➳ {ticker}...")
        
        try:
            df, h_date = None, "Unknown"
            # Route specific families through Selenium for rendering
            needs_selenium = etf['scraper_type'] in ['pacer_csv', 'selenium_alpha', 'first_trust'] or ticker in ['COWZ', 'CALF', 'FPX', 'FPXI', 'QMOM', 'IMOM']
            
            if needs_selenium:
                if driver is None: driver = setup_driver()
                driver.get(etf['url'])
                time.sleep(5) 
                body_element = driver.find_element(By.TAG_NAME, "body")
                h_date = extract_holdings_date(body_element.text)
                dfs = pd.read_html(StringIO(driver.page_source))
            else:
                r = requests.get(etf['url'], headers=HEADERS, timeout=15)
                h_date = extract_holdings_date(r.text)
                dfs = pd.read_html(StringIO(r.text))

            for d in dfs:
                if len(d) > 8: 
                    df = d
                    break

            clean_df = clean_dataframe(df, ticker, h_date)
            if clean_df is not None:
                clean_df.to_csv(os.path.join(DATA_DIR_LATEST, f"{ticker}.csv"), index=False)
                print(f"    ✅ Success: {len(clean_df)} rows | As Of: {h_date}")
            else:
                print(f"    ⚠️ No valid data found for {ticker}")

        except Exception as e:
            print(f"    ❌ Failed: {e}")

    if driver: driver.quit()

if __name__ == "__main__":
    main()
