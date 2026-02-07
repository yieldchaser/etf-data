import pandas as pd
import cloudscraper
import time
import random
import json
import os
from datetime import datetime
from io import StringIO
import urllib3

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIG ---
CONFIG_FILE = 'config.json'
DATA_DIR_LATEST = 'data/latest'
DATA_DIR_HISTORY = 'data/history'
TODAY = datetime.now().strftime('%Y-%m-%d')

def get_scraper():
    """
    Creates a CloudScraper instance.
    This acts like a REAL browser (Chrome) to bypass anti-bot checks.
    """
    scraper = cloudscraper.create_scraper(
        browser={
            'browser': 'chrome',
            'platform': 'windows',
            'desktop': True
        }
    )
    return scraper

def find_holdings_table(dfs):
    """
    Smart Table Finder
    """
    if not dfs: return None
    valid_keywords = ['ticker', 'symbol', 'holding', 'identifier', 'weighting', 'cusip']
    
    for i, df in enumerate(dfs):
        if len(df) < 5: continue # Skip tiny tables
        
        # Check Header
        cols = [str(c).strip().lower() for c in df.columns]
        if any(k in cols for k in valid_keywords):
            return df
        
        # Check Row 0 (Header Promotion)
        if not df.empty:
            first_row = [str(x).strip().lower() for x in df.iloc[0].values]
            if any(k in first_row for k in valid_keywords):
                new_header = df.iloc[0]
                df = df[1:] 
                df.columns = new_header
                return df
    return None

def clean_dataframe(df, ticker):
    if df is None or df.empty: return None

    # 1. Standardize columns
    df.columns = [str(c).strip().lower() for c in df.columns]
    
    # 2. Rename columns to standard format
    col_map = {
        'stockticker': 'ticker', 'symbol': 'ticker', 'holding': 'ticker', 'ticker': 'ticker',
        'identifier': 'ticker', # First Trust
        'securityname': 'name', 'company': 'name', 'security name': 'name', 'security_name': 'name', 'security': 'name', 'name': 'name',
        'weightings': 'weight', '% tna': 'weight', 'weight': 'weight', '% of net assets': 'weight', 
        'weighting': 'weight', '%_of_net_assets': 'weight', '% net assets': 'weight'
    }
    df.rename(columns=col_map, inplace=True)

    # 3. Validation
    if 'ticker' not in df.columns:
        print(f"      -> ⚠️ Missing 'ticker' column. Found: {list(df.columns)}")
        return None

    # 4. Filter Garbage
    stop_words = ["cash", "usd", "liquidity", "government", "treasury", "money market", "net other", "total"]
    df['name'] = df['name'].astype(str)
    df['ticker'] = df['ticker'].astype(str)
    
    pattern = '|'.join(stop_words)
    mask = df['name'].str.contains(pattern, case=False, na=False) | \
           df['ticker'].str.contains(pattern, case=False, na=False)
    df = df[~mask].copy()

    # 5. Clean Ticker
    df['ticker'] = df['ticker'].str.replace(' USD', '', regex=False)
    df['ticker'] = df['ticker'].str.replace('.UN', '', regex=False)
    df['ticker'] = df['ticker'].str.upper().str.strip()

    # 6. Clean Weight
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

    # Initialize the "CloudScraper" (The Anti-Bot Key)
    scraper = get_scraper()
    
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
            
            # --- PACER ---
            if etf['scraper_type'] == 'pacer_csv':
                r = scraper.get(etf['url'])
                if r.status_code == 200:
                    content = r.text.splitlines()
                    start = 0
                    for i, line in enumerate(content[:20]):
                        if "Ticker" in line or "Symbol" in line:
                            start = i; break
                    df = pd.read_csv(StringIO('\n'.join(content[start:])))

            # --- FIRST TRUST ---
            elif etf['scraper_type'] == 'first_trust':
                r = scraper.get(etf['url'])
                dfs = pd.read_html(r.text, flavor='bs4') 
                df = find_holdings_table(dfs)

            # --- ALPHA ARCHITECT (The Fix) ---
            elif etf['scraper_type'] == 'direct_csv':
                # Use CloudScraper to hit the export link
                # This often bypasses the 403 that 'requests' gets
                r = scraper.get(etf['url'])
                if r.status_code == 200:
                    df = pd.read_csv(StringIO(r.text))
                    print("      -> Direct CSV Download Worked!")
                else:
                    print(f"      -> Direct CSV failed ({r.status_code}). Trying table scrape...")
                    # Fallback: Scrape the main visual table
                    # Note: We visit the main page, NOT the export link for this fallback
                    fallback_url = f"https://funds.alphaarchitect.com/{ticker.lower()}/"
                    r_page = scraper.get(fallback_url)
                    dfs = pd.read_html(r_page.text)
                    df = find_holdings_table(dfs)

            # --- INVESCO (The Fix) ---
            elif etf['scraper_type'] == 'invesco':
                # CloudScraper should fix the "Document is empty" error
                dl_link = f"https://www.invesco.com/us/en/financial-products/etfs/holdings/main/holdings/0?ticker={ticker}&action=download"
                try:
                    r = scraper.get(dl_link)
                    if r.status_code == 200 and len(r.content) > 500:
                        df = pd.read_csv(StringIO(r.text))
                        print("      -> Magic Link worked!")
                    else:
                        raise Exception("Link failed")
                except:
                    print("      -> Magic link failed. Trying page scrape...")
                    r = scraper.get(etf['url'])
                    dfs = pd.read_html(r.text)
                    df = find_holdings_table(dfs)

            # --- SAVE ---
            clean_df = clean_dataframe(df, ticker)
            
            if clean_df is not None and not clean_df.empty:
                save_path = os.path.join(DATA_DIR_LATEST, f"{ticker}.csv")
                clean_df.to_csv(save_path, index=False)
                master_list.append(clean_df)
                print(f"    ✅ Success: {len(clean_df)} rows saved.")
            else:
                print(f"    ⚠️ Failed to extract data.")

        except Exception as e:
            print(f"    ❌ Error: {e}")
        
        time.sleep(random.uniform(3, 8))

    if master_list:
        full_df = pd.concat(master_list)
        full_df.to_csv(os.path.join(archive_path, 'master_archive.csv'), index=False)
        print("\n📜 Daily Archive Complete.")

if __name__ == "__main__":
    main()