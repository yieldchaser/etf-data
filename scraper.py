import pandas as pd
import requests
import time
import random
import json
import os
from datetime import datetime
from io import StringIO
from bs4 import BeautifulSoup

# --- CONSTANTS & CONFIG ---
CONFIG_FILE = 'config.json'
DATA_DIR_LATEST = 'data/latest'
DATA_DIR_HISTORY = 'data/history'
TODAY = datetime.now().strftime('%Y-%m-%d')

# Stop-Words for Cleaning Engine
STOP_WORDS = [
    "cash", "usd", "liquidity", "invesco government", 
    "treasury", "net other assets", "united states treasury", "money market"
]

def setup_session():
    """
    Creates a session with a HARDCODED robust User-Agent.
    This avoids the 'fake_useragent' crash on GitHub servers.
    """
    session = requests.Session()
    
    # Use a standard, modern Chrome browser header
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
    }
    session.headers.update(headers)
    
    # Prime the session for Invesco 
    try:
        session.get("https://www.invesco.com/us/en/financial-products/etfs.html", timeout=10)
        time.sleep(2)
    except Exception as e:
        print(f"⚠️ Session priming warning: {e}")
    
    return session

def clean_dataframe(df, ticker):
    """
    Sanitizes data: Standardizes columns, removes 'Cash', formats weights.
    """
    if df.empty:
        return df

    # Standardize Column Names
    df.columns = [str(c).strip().lower() for c in df.columns]
    
    col_map = {
        'stockticker': 'ticker', 'symbol': 'ticker', 'holding': 'ticker',
        'securityname': 'name', 'company': 'name', 'security name': 'name',
        'weightings': 'weight', '% tna': 'weight', 'weight': 'weight', '% of net assets': 'weight'
    }
    df.rename(columns=col_map, inplace=True)

    if 'ticker' not in df.columns or 'weight' not in df.columns:
        print(f"  ⚠️ Warning: {ticker} missing columns. Found: {df.columns}")
        return pd.DataFrame() 

    # 1. Stop-Word Kill Switch
    pattern = '|'.join(STOP_WORDS)
    mask = df['name'].astype(str).str.contains(pattern, case=False, na=False) | \
           df['ticker'].astype(str).str.contains(pattern, case=False, na=False)
    df = df[~mask].copy()

    # 2. Ticker Normalization
    df['ticker'] = df['ticker'].astype(str).str.replace(' USD', '', regex=False)
    df['ticker'] = df['ticker'].str.replace('.UN', '', regex=False)
    df['ticker'] = df['ticker'].str.replace(' UW', '', regex=False)
    df['ticker'] = df['ticker'].str.upper().str.strip()

    # 3. Weight Formatting
    if df['weight'].dtype == object:
        df['weight'] = df['weight'].astype(str).str.replace('%', '').str.replace(',', '')
    
    df['weight'] = pd.to_numeric(df['weight'], errors='coerce')
    
    if df['weight'].max() > 1.0:
        df['weight'] = df['weight'] / 100.0

    df['ETF_Ticker'] = ticker
    df['Date_Scraped'] = TODAY
    
    return df[['ETF_Ticker', 'ticker', 'name', 'weight', 'Date_Scraped']]

def fetch_data(etf_config, session):
    ticker = etf_config['ticker']
    url = etf_config['url']
    sType = etf_config['scraper_type']
    
    print(f"➳ Scraping {ticker} via {sType}...")
    
    try:
        if sType == "pacer_csv":
            response = session.get(url, timeout=20)
            content = response.content.decode('utf-8')
            lines = content.splitlines()
            start_row = 0
            for i, line in enumerate(lines):
                if "Ticker" in line or "StockTicker" in line:
                    start_row = i
                    break
            df = pd.read_csv(StringIO('\n'.join(lines[start_row:])))
            
        elif sType == "first_trust" or sType == "alpha_architect":
            response = session.get(url, timeout=20)
            dfs = pd.read_html(response.text)
            df = dfs[0]

        elif sType == "invesco":
            response = session.get(url, timeout=20)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            download_link = None
            for a in soup.find_all('a', href=True):
                if "download" in a['href'] and "holdings" in a['href']:
                    download_link = a['href']
                    if not download_link.startswith('http'):
                        download_link = "https://www.invesco.com" + download_link
                    break
            
            if download_link:
                time.sleep(random.uniform(2, 5))
                csv_resp = session.get(download_link, timeout=20)
                df = pd.read_csv(StringIO(csv_resp.text))
            else:
                dfs = pd.read_html(response.text)
                df = dfs[0]
        else:
            return None

        return clean_dataframe(df, ticker)

    except Exception as e:
        print(f"  ❌ Error scraping {ticker}: {str(e)}")
        return None

def main():
    # Load Config
    try:
        with open(CONFIG_FILE, 'r') as f:
            etfs = json.load(f)
    except FileNotFoundError:
        print("❌ Error: config.json not found!")
        exit(1)

    session = setup_session()
    
    # Create Output Folders
    os.makedirs(DATA_DIR_LATEST, exist_ok=True)
    archive_path = os.path.join(DATA_DIR_HISTORY, *TODAY.split('-'))
    os.makedirs(archive_path, exist_ok=True)

    master_archive = []

    for etf in etfs:
        if not etf.get('enabled', True):
            continue

        delay = random.uniform(3, 8)
        print(f"⏳ Sleeping {delay:.2f}s...")
        time.sleep(delay)

        df = fetch_data(etf, session)

        if df is not None and not df.empty:
            latest_file = os.path.join(DATA_DIR_LATEST, f"{etf['ticker']}.csv")
            df.to_csv(latest_file, index=False)
            print(f"  ✅ Saved latest: {latest_file}")
            master_archive.append(df)
        else:
            print(f"  ⚠️ No data for {etf['ticker']}")

    if master_archive:
        full_archive = pd.concat(master_archive, ignore_index=True)
        archive_file = os.path.join(archive_path, 'master_archive.csv')
        full_archive.to_csv(archive_file, index=False)
        print(f"📜 Master Archive saved: {archive_file}")
    else:
        print("⚠️ No data collected. Scraper finished with empty results.")

if __name__ == "__main__":
    main()