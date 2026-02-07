import pandas as pd
import requests
import time
import random
import json
import os
from datetime import datetime
from io import StringIO
from bs4 import BeautifulSoup

# --- CONFIG ---
CONFIG_FILE = 'config.json'
DATA_DIR_LATEST = 'data/latest'
DATA_DIR_HISTORY = 'data/history'
TODAY = datetime.now().strftime('%Y-%m-%d')

# Headers to mimic a real Chrome browser on Windows
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://www.google.com/'
}

def clean_dataframe(df, ticker):
    if df is None or df.empty:
        return None

    # Standardize columns
    df.columns = [str(c).strip().lower() for c in df.columns]
    
    # Column mapping
    col_map = {
        'stockticker': 'ticker', 'symbol': 'ticker', 'holding': 'ticker', 'ticker': 'ticker',
        'securityname': 'name', 'company': 'name', 'security name': 'name', 'security_name': 'name',
        'weightings': 'weight', '% tna': 'weight', 'weight': 'weight', '% of net assets': 'weight', '%_of_net_assets': 'weight'
    }
    df.rename(columns=col_map, inplace=True)

    # Required columns check
    if 'ticker' not in df.columns or 'weight' not in df.columns:
        print(f"    ⚠️ Data found but missing columns. Found: {df.columns.tolist()}")
        return None

    # Cleaning rules
    stop_words = ["cash", "usd", "liquidity", "government", "treasury", "money market", "net other"]
    pattern = '|'.join(stop_words)
    mask = df['name'].astype(str).str.contains(pattern, case=False, na=False) | \
           df['ticker'].astype(str).str.contains(pattern, case=False, na=False)
    df = df[~mask].copy()

    # Normalize Ticker
    df['ticker'] = df['ticker'].astype(str).str.replace(' USD', '', regex=False)
    df['ticker'] = df['ticker'].str.replace('.UN', '', regex=False).str.upper().strip()

    # Format Weight
    if df['weight'].dtype == object:
        df['weight'] = df['weight'].astype(str).str.replace('%', '').str.replace(',', '')
    df['weight'] = pd.to_numeric(df['weight'], errors='coerce')
    if df['weight'].max() > 1.0:
        df['weight'] = df['weight'] / 100.0

    df['ETF_Ticker'] = ticker
    df['Date_Scraped'] = TODAY
    return df[['ETF_Ticker', 'ticker', 'name', 'weight', 'Date_Scraped']]

def get_invesco_data(session, ticker, url):
    """
    Strategy: Try the 'Magic' hidden download link first.
    If that fails, fall back to parsing the visible page.
    """
    # 1. Try "Magic" Direct Download Link
    magic_link = f"https://www.invesco.com/us/en/financial-products/etfs/holdings/main/holdings/0?ticker={ticker}&action=download"
    try:
        print(f"    Trying Magic Link for {ticker}...")
        r = session.get(magic_link, timeout=15)
        if r.status_code == 200 and len(r.content) > 50:
            return pd.read_csv(StringIO(r.text))
    except:
        pass

    # 2. Fallback: Scrape the main page for a dynamic link
    try:
        print(f"    Magic link failed. Scraping page for {ticker}...")
        r = session.get(url, timeout=15)
        soup = BeautifulSoup(r.text, 'html.parser')
        
        # Look for any link containing 'download'
        for a in soup.find_all('a', href=True):
            if 'download' in a['href'].lower():
                dl_url = a['href']
                if not dl_url.startswith('http'):
                    dl_url = 'https://www.invesco.com' + dl_url
                
                r_dl = session.get(dl_url, timeout=15)
                return pd.read_csv(StringIO(r_dl.text))
    except Exception as e:
        print(f"    ❌ Invesco scrape error: {e}")
    
    return None

def main():
    with open(CONFIG_FILE, 'r') as f:
        etfs = json.load(f)

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
            if etf['scraper_type'] == 'pacer_csv':
                r = session.get(etf['url'])
                # Skip garbage rows
                content = r.text.splitlines()
                start = 0
                for i, line in enumerate(content):
                    if "Ticker" in line or "StockTicker" in line:
                        start = i
                        break
                df = pd.read_csv(StringIO('\n'.join(content[start:])))

            elif etf['scraper_type'] == 'first_trust':
                r = session.get(etf['url'])
                dfs = pd.read_html(r.text)
                df = dfs[0]

            elif etf['scraper_type'] == 'alpha_architect':
                r = session.get(etf['url'])
                dfs = pd.read_html(r.text)
                df = dfs[0]

            elif etf['scraper_type'] == 'invesco':
                df = get_invesco_data(session, ticker, etf['url'])

            # Clean and Save
            clean_df = clean_dataframe(df, ticker)
            
            if clean_df is not None and not clean_df.empty:
                save_path = os.path.join(DATA_DIR_LATEST, f"{ticker}.csv")
                clean_df.to_csv(save_path, index=False)
                master_list.append(clean_df)
                print(f"    ✅ Success! Saved {len(clean_df)} rows.")
            else:
                print(f"    ⚠️ Failed to extract data for {ticker}")

        except Exception as e:
            print(f"    ❌ Critical Error on {ticker}: {e}")
        
        time.sleep(random.uniform(2, 5))

    if master_list:
        full_df = pd.concat(master_list)
        full_df.to_csv(os.path.join(archive_path, 'master_archive.csv'), index=False)
        print("📜 Master Archive Created.")

if __name__ == "__main__":
    main()