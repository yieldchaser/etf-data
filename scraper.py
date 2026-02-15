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
TODAY = datetime.now().strftime('%Y-%m-%d')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

def extract_date_from_text(text):
    """ Standardized date hunter for Pacer (MM/DD/YYYY) and others """
    if not text: return TODAY
    # Focus on Pacer format: 02/17/2026 or Invesco format: February 12, 2026
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
    """ The reliable FPX/FPXI header promoter """
    if not dfs: return None
    valid_keywords = ['ticker', 'symbol', 'holding', 'identifier', 'weighting', 'cusip']
    for df in dfs:
        cols = [str(c).strip().lower() for c in df.columns]
        if any(k in cols for k in valid_keywords): return df
        if not df.empty:
            first_row = [str(x).strip().lower() for x in df.iloc[0].values]
            if any(k in first_row for k in valid_keywords):
                new_header = df.iloc[0]
                df = df[1:].copy() 
                df.columns = new_header
                return df
    return None

def clean_dataframe(df, ticker, h_date=TODAY):
    """ Standardizes columns and fixes SettingWithCopyWarnings """
    if df is None or df.empty: return None
    df = df.copy() # Avoid SettingWithCopyWarning
    df.columns = [str(c).strip().lower() for c in df.columns]

    mappings = {
        'ticker': ['symbol', 'identifier', 'stock ticker', 'ticker'],
        'name': ['security name', 'company', 'holding', 'description', 'name'],
        'weight': ['weighting', '% weight', 'weight %', '% net assets', 'weight']
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
    
    df.loc[:, 'ETF_Ticker'] = ticker
    df.loc[:, 'Holdings_As_Of'] = h_date
    df.loc[:, 'Date_Scraped'] = TODAY
    return df[['ETF_Ticker', 'ticker', 'name', 'weight', 'Holdings_As_Of', 'Date_Scraped']]

def setup_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,1080")
    return webdriver.Chrome(options=options)

def main():
    try:
        with open(CONFIG_FILE, 'r') as f: etfs = json.load(f)
    except: return

    print(f"🚀 Launching Scraper V14.3 (Pacer Restoration) - {TODAY}")
    driver = setup_driver()
    os.makedirs(DATA_DIR_LATEST, exist_ok=True)

    for etf in etfs:
        if not etf.get('enabled', True): continue
        ticker = etf['ticker']
        print(f"➳ {ticker}...")
        
        try:
            df, h_date = None, TODAY
            
            # --- PACER RESTORATION (The CSV Method) ---
            if etf['scraper_type'] == 'pacer_csv':
                # First, get the date from the website text
                driver.get(etf['url'])
                time.sleep(3)
                h_date = extract_date_from_text(driver.find_element(By.TAG_NAME, "body").text)
                
                # Second, grab the actual data via CSV (Reliable V12 style)
                # We assume the CSV link is the one provided in config
                r = requests.get(etf['url'], headers=HEADERS, timeout=15)
                content = r.text.splitlines()
                start = 0
                for i, line in enumerate(content[:20]):
                    if "Ticker" in line or "Symbol" in line: start = i; break
                df = pd.read_csv(StringIO('\n'.join(content[start:])))

            # --- FIRST TRUST & ALPHA ARCHITECT ---
            elif etf['scraper_type'] in ['selenium_alpha', 'first_trust']:
                driver.
