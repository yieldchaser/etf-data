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
from selenium.webdriver.support.ui import Select

# --- CONFIG ---
CONFIG_FILE = 'config.json'
DATA_DIR_LATEST = 'data/latest'
TODAY = datetime.now().strftime('%Y-%m-%d')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

def extract_date_from_text(text):
    if not text: return TODAY
    pattern = r"(\d{1,2}/\d{1,2}/\d{4})|([A-Z][a-z]+\s+\d{1,2},?\s+\d{4})"
    match = re.search(pattern, text)
    if match:
        raw = match.group(0).replace(',', '')
        for fmt in ("%m/%d/%Y", "%B %d %Y", "%b %d %Y"):
            try:
                return datetime.strptime(raw, fmt).strftime('%Y-%m-%d')
            except: continue
    return TODAY

def find_first_trust_table(dfs):
    if not dfs: return None
    valid_keywords = ['ticker', 'symbol', 'holding', 'identifier', 'weighting', 'cusip']
    for df in dfs:
        cols = [str(c).strip().lower() for c in df.columns]
        if any(k in cols for k in valid_keywords): return df
        if not df.empty:
            first_row = [str(x).strip().lower() for x in df.iloc[0].values]
            if any(k in first_row for k in valid_keywords):
                new_header = df.iloc[0]
                df_clean = df[1:].copy() 
                df_clean.columns = new_header
                return df_clean
    return None

def clean_dataframe(df, ticker, h_date=TODAY):
    if df is None or df.empty: return None
    df = df.copy() 
    df.columns = [str(c).strip().lower() for c in df.columns]

    mappings = {
        'ticker': ['symbol', 'identifier', 'stock ticker', 'ticker'],
        'name': ['security name', 'company', 'holding', 'description', 'name'],
        'weight': ['weighting', '% weight', 'weight %', '% net assets', '% of net assets', 'weight']
    }
    for target, keywords in mappings.items():
        for col in df.columns:
            if any(k in col for k in keywords):
                df.rename(columns={col: target}, inplace=True)
                break

    if 'ticker' not in df.columns: return None
    
    if 'weight' not in df.columns:
        df['weight'] = 0.0
    
    if 'weight' in df.columns:
        df['weight'] = df['weight'].astype(str).str.replace('%', '').str.replace(',', '')
        df['weight'] = pd.to_numeric(df['weight'], errors='coerce').fillna(0.0)
        if df['weight'].max() > 1.0: df['weight'] = df['weight'] / 100.0
    
    df.loc[:, 'ETF_Ticker'] = ticker
    df.loc[:, 'Holdings_As_Of'] = h_date
    df.loc[:, 'Date_Scraped'] = TODAY
    return df[['ETF_Ticker', 'ticker', 'name', 'weight', 'Holdings_As_Of', 'Date_Scraped']]

def setup_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    return webdriver.Chrome(options=options)

def main():
    try:
        with open(CONFIG_FILE, 'r') as f: etfs = json.load(f)
    except: return

    print(f"🚀 Launching Scraper V14.6 (QMOM Keyword Fix) - {TODAY}")
    driver = setup_driver()
    os.makedirs(DATA_DIR_LATEST, exist_ok=True)

    for etf in etfs:
        if not etf.get('enabled', True): continue
        ticker = etf['ticker']
        print(f"➳ {ticker}...")
        
        try:
            df, h_date = None, TODAY
            
            # --- PACER ---
            if etf['scraper_type'] == 'pacer_csv':
                driver.get(etf['url'])
                time.sleep(3)
                h_date = extract_date_from_text(driver.find_element(By.TAG_NAME, "body").text)
                r = requests.get(etf['url'], headers=HEADERS, timeout=15)
                content = r.text.splitlines()
                start = 0
                for i, line in enumerate(content[:20]):
                    if "Ticker" in line or "Symbol" in line: start = i; break
                df = pd.read_csv(StringIO('\n'.join(content[start:])))

            # --- ALPHA ARCHITECT ---
            elif etf['scraper_type'] == 'selenium_alpha':
                driver.get(etf['url'])
                time.sleep(3)
                h_date = extract_date_from_text(driver.find_element(By.TAG_NAME, "body").text)
                try:
                    selects = driver.find_elements(By.TAG_NAME, "select")
                    for s in selects:
                        try: 
                            Select(s).select_by_visible_text("All")
                            time.sleep(1)
                        except: pass
                except: pass
                dfs = pd.read_html(StringIO(driver.page_source))
                for d in dfs: 
                    if len(d) > 25: df = d; break

            # --- FIRST TRUST ---
            elif etf['scraper_type'] == 'first_trust':
                driver.get(etf['url'])
                time.sleep(5) 
                h_date = extract_date_from_text(driver.find_element(By.TAG_NAME, "body").text)
                dfs = pd.read_html(StringIO(driver.page_source))
                df = find_first_trust_table(dfs)
            
            # --- OTHERS ---
            else:
                r = requests.get(etf['url'], headers=HEADERS, timeout=15)
                h_date = extract_date_from_text(r.text)
                dfs = pd.read_html(StringIO(r.text))
                for d in dfs:
                    if len(d) > 20: df = d; break

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
