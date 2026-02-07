import pandas as pd
import time
import random
import json
import os
from datetime import datetime
from io import StringIO
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
import requests

# --- CONFIG ---
CONFIG_FILE = 'config.json'
DATA_DIR_LATEST = 'data/latest'
DATA_DIR_HISTORY = 'data/history'
TODAY = datetime.now().strftime('%Y-%m-%d')

def setup_driver():
    """ Launches Headless Chrome """
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled") 
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    return webdriver.Chrome(options=chrome_options)

def clean_dataframe(df, ticker):
    if df is None or df.empty: return None

    # 0. Remove Duplicate Columns (Fixes the IMOM crash)
    df = df.loc[:, ~df.columns.duplicated()]

    # 1. Standardize columns
    df.columns = [str(c).strip().lower() for c in df.columns]
    
    # 2. Rename columns
    col_map = {
        'stockticker': 'ticker', 'symbol': 'ticker', 'holding': 'ticker', 'ticker': 'ticker',
        'identifier': 'ticker', 'sedol': 'ticker',
        'securityname': 'name', 'company': 'name', 'security name': 'name', 'security_name': 'name', 'security': 'name', 'name': 'name',
        'weightings': 'weight', '% tna': 'weight', 'weight': 'weight', '% of net assets': 'weight', 
        'weighting': 'weight', '%_of_net_assets': 'weight', '% net assets': 'weight'
    }
    df.rename(columns=col_map, inplace=True)

    # 3. Check critical columns
    if 'ticker' not in df.columns:
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

    print("🚀 Launching Hybrid Scraper...")
    driver = setup_driver()
    
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
            
            # --- STRATEGY 1: PACER (Simple CSV) ---
            if etf['scraper_type'] == 'pacer_csv':
                # Pacer works best with standard requests
                try:
                    df = pd.read_csv(etf['url'], skiprows=lambda x: x < 10 and 'Ticker' not in str(x))
                    # Fallback if header missed
                    if 'Ticker' not in df.columns and 'Symbol' not in df.columns:
                         df = pd.read_csv(etf['url'])
                except Exception as e:
                    print(f"    ⚠️ Pacer CSV error: {e}")

            # --- STRATEGY 2: ALPHA ARCHITECT (Selenium Click) ---
            elif 'alpha' in etf['url'] or etf['scraper_type'] == 'direct_csv':
                page_url = f"https://funds.alphaarchitect.com/{ticker.lower()}/#fund-holdings"
                driver.get(page_url)
                time.sleep(5)
                
                # Click "All"
                try:
                    selects = driver.find_elements(By.TAG_NAME, "select")
                    for s in selects:
                        try:
                            Select(s).select_by_visible_text("All")
                            time.sleep(2)
                        except: pass
                except: pass
                
                # Scrape Table
                dfs = pd.read_html(StringIO(driver.page_source))
                for d in dfs:
                    if len(d) > 20: # Look for the big table
                        df = d; break

            # --- STRATEGY 3: INVESCO (Secret JSON API) ---
            elif etf['scraper_type'] == 'invesco':
                # Invesco loads data via this hidden API
                api_url = f"https://www.invesco.com/us/en/financial-products/etfs/holdings/main/holdings/0?ticker={ticker}"
                driver.get(api_url)
                time.sleep(2)
                
                # The browser will display raw JSON in the body
                raw_text = driver.find_element(By.TAG_NAME, "body").text
                try:
                    data = json.loads(raw_text)
                    # The JSON structure usually has 'holdings' or 'result'
                    # We convert the list of dicts to DataFrame
                    if isinstance(data, list):
                        df = pd.DataFrame(data)
                    elif 'holdings' in data:
                         df = pd.DataFrame(data['holdings'])
                    elif 'result' in data:
                         df = pd.DataFrame(data['result'])
                except:
                    print("    ⚠️ JSON parse failed, trying fallback...")
                    # Fallback to visual scrape if JSON fails
                    driver.get(etf['url'])
                    time.sleep(8)
                    dfs = pd.read_html(StringIO(driver.page_source))
                    df = dfs[0] if dfs else None

            # --- STRATEGY 4: FIRST TRUST (Selenium Wait) ---
            elif etf['scraper_type'] == 'first_trust' or 'ftportfolios' in etf['url']:
                driver.get(etf['url'])
                time.sleep(10) # Wait for table to render
                dfs = pd.read_html(StringIO(driver.page_source))
                for d in dfs:
                    cols = [str(c).lower() for c in d.columns]
                    if any(k in cols for k in ['ticker', 'identifier', 'symbol']):
                        df = d
                        break

            # --- SAVE ---
            clean_df = clean_dataframe(df, ticker)
            
            if clean_df is not None and not clean_df.empty:
                save_path = os.path.join(DATA_DIR_LATEST, f"{ticker}.csv")
                clean_df.to_csv(save_path, index=False)
                master_list.append(clean_df)
                print(f"    ✅ Success: {len(clean_df)} rows saved.")
            else:
                print(f"    ⚠️ Data not found / Table empty.")

        except Exception as e:
            print(f"    ❌ Error: {e}")

    driver.quit()

    if master_list:
        full_df = pd.concat(master_list)
        full_df.to_csv(os.path.join(archive_path, 'master_archive.csv'), index=False)
        print("\n📜 Daily Archive Complete.")

if __name__ == "__main__":
    main()