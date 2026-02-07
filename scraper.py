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
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select

# --- CONFIG ---
CONFIG_FILE = 'config.json'
DATA_DIR_LATEST = 'data/latest'
DATA_DIR_HISTORY = 'data/history'
TODAY = datetime.now().strftime('%Y-%m-%d')

def setup_driver():
    """
    Launches a HEADLESS Chrome Browser.
    This is a real browser, not a simulation.
    """
    chrome_options = Options()
    chrome_options.add_argument("--headless") # Run in background
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    # Hide automation flags to look like a human
    chrome_options.add_argument("--disable-blink-features=AutomationControlled") 
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    driver = webdriver.Chrome(options=chrome_options)
    return driver

def clean_dataframe(df, ticker):
    if df is None or df.empty: return None
    
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

def scrape_alpha_architect(driver, url):
    """
    Specific logic to click the "Show All" dropdown.
    """
    driver.get(url)
    time.sleep(5) # Let JS load
    
    try:
        # Try to find the "Show entries" dropdown
        # It usually has a name like 'table_id_length'
        select_elements = driver.find_elements(By.TAG_NAME, "select")
        for select in select_elements:
            try:
                # Select "All" (value usually '-1' or 'All')
                dropdown = Select(select)
                dropdown.select_by_visible_text("All")
                print("      -> Clicked 'Show All'...")
                time.sleep(3) # Wait for table to expand
                break
            except:
                continue
    except Exception as e:
        print(f"      -> Warning: Could not click dropdown ({str(e)})")

    # Now scrape the full page source
    dfs = pd.read_html(StringIO(driver.page_source))
    # Find the big table
    for df in dfs:
        if len(df) > 15: # If it has more than 15 rows, it's likely the full table now
            return df
    return dfs[0] if dfs else None

def main():
    try:
        with open(CONFIG_FILE, 'r') as f:
            etfs = json.load(f)
    except:
        print("❌ Config file not found.")
        return

    # LAUNCH BROWSER
    print("🚀 Launching Headless Chrome...")
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
            
            # --- STRATEGY 1: PACER (Keep using direct CSV, it works) ---
            if etf['scraper_type'] == 'pacer_csv':
                # Use pandas directly for CSVs
                try:
                    df = pd.read_csv(etf['url'], skiprows=lambda x: x < 10 and 'Ticker' not in str(x))
                    # Quick fix to find header if logic above fails
                    if 'Ticker' not in df.columns:
                         df = pd.read_csv(etf['url'])
                except:
                    driver.get(etf['url'])
                    df = pd.read_csv(StringIO(driver.find_element(By.TAG_NAME, 'pre').text))

            # --- STRATEGY 2: ALPHA ARCHITECT (Selenium Interaction) ---
            elif 'alpha' in etf['url'] or etf['scraper_type'] == 'direct_csv':
                # Revert to standard page URL for Selenium interaction
                page_url = f"https://funds.alphaarchitect.com/{ticker.lower()}/#fund-holdings"
                df = scrape_alpha_architect(driver, page_url)

            # --- STRATEGY 3: GENERIC / INVESCO / FIRST TRUST ---
            else:
                driver.get(etf['url'])
                time.sleep(8) # Wait generous time for Invesco JS to render
                
                # Check for "Magic" Invesco Download Link in DOM
                try:
                    links = driver.find_elements(By.TAG_NAME, "a")
                    for link in links:
                        href = link.get_attribute('href')
                        if href and "action=download" in href:
                            print("      -> Found hidden download link, grabbing...")
                            driver.get(href)
                            time.sleep(3)
                            df = pd.read_csv(StringIO(driver.page_source)) # Sometimes raw csv renders in browser
                            break
                except:
                    pass
                
                if df is None:
                    # Fallback: Parse visible table
                    dfs = pd.read_html(StringIO(driver.page_source))
                    # Smart Table Finder
                    for d in dfs:
                        cols = [str(c).lower() for c in d.columns]
                        if any(k in cols for k in ['ticker', 'symbol', 'identifier', 'holding']):
                            df = d
                            break
                    if df is None and dfs: df = dfs[0]

            # --- SAVE ---
            clean_df = clean_dataframe(df, ticker)
            
            if clean_df is not None and not clean_df.empty:
                save_path = os.path.join(DATA_DIR_LATEST, f"{ticker}.csv")
                clean_df.to_csv(save_path, index=False)
                master_list.append(clean_df)
                print(f"    ✅ Success: {len(clean_df)} rows saved.")
            else:
                print(f"    ⚠️ Data not found.")

        except Exception as e:
            print(f"    ❌ Error: {e}")

    driver.quit()

    if master_list:
        full_df = pd.concat(master_list)
        full_df.to_csv(os.path.join(archive_path, 'master_archive.csv'), index=False)
        print("\n📜 Daily Archive Complete.")

if __name__ == "__main__":
    main()