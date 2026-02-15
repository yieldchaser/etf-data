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
from selenium.webdriver.support.ui import Select

# --- CONFIG ---
CONFIG_FILE = 'config.json'
DATA_DIR_LATEST = 'data/latest'
DATA_DIR_HISTORY = 'data/history'
DATA_DIR_BACKUP = 'data/invesco_backup'
GIANT_HISTORY_FILE = 'data/all_history.csv' # <--- NEW GIANT FILE
TODAY = datetime.now().strftime('%Y-%m-%d')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

def clean_date_string(date_text):
    """ Converts various date formats to YYYY-MM-DD """
    if not date_text: return None
    clean = re.sub(r"(?i)as of|date|[:,-]", " ", date_text).strip()
    match = re.search(r"(\d{1,2}/\d{1,2}/\d{4})|([A-Z][a-z]+\s+\d{1,2},?\s+\d{4})", clean)
    if match: clean = match.group(0).replace(',', '')

    for fmt in ("%m/%d/%Y", "%B %d %Y", "%b %d %Y", "%m %d %Y"):
        try:
            return datetime.strptime(clean, fmt).strftime('%Y-%m-%d')
        except: continue
    return None

def extract_invesco_nuclear_date(driver):
    """ Nuclear Strategy: Scrolls, grabs Source, Regexes """
    try:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3) 
        html = driver.page_source
        match = re.search(r"# of holdings\s*\(as of\s*(\d{1,2}/\d{1,2}/\d{4})\)", html, re.IGNORECASE)
        if match:
            print(f"      -> Nuclear Hit: {match.group(1)}")
            return clean_date_string(match.group(1))
    except Exception as e:
        print(f"      -> Nuclear Failed: {e}")
    return TODAY

def scrape_invesco_backup(driver, url, ticker):
    try:
        print(f"      -> 🛡️ Running Backup Scraper for {ticker}...")
        driver.get(url)
        h_date = extract_invesco_nuclear_date(driver)
        
        print(f"      -> Downloading from visible table...")
        df = None
        dfs = pd.read_html(StringIO(driver.page_source))
        for d in dfs:
            valid_keywords = ['ticker', 'symbol', 'holding', 'identifier', 'weight', '% of net assets']
            cols = [str(c).strip().lower() for c in d.columns]
            if any(k in cols for k in valid_keywords):
                df = d; break
            for i in range(min(5, len(d))):
                row_values = [str(x).strip().lower() for x in d.iloc[i].values]
                if any(k in row_values for k in valid_keywords):
                    new_header = d.iloc[i]
                    d = d[i+1:].copy()
                    d.columns = new_header
                    df = d
                    break
            if df is not None: break
        return df, h_date
    except Exception as e:
        print(f"      -> Backup Failed: {e}")
    return None, TODAY

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
    if 'weight' not in df.columns: df['weight'] = 0.0
    
    if 'weight' in df.columns:
        df['weight'] = df['weight'].astype(str).str.replace('%', '').str.replace(',', '')
        df['weight'] = pd.to_numeric(df['weight'], errors='coerce').fillna(0.0)
        if df['weight'].max() > 1.0: df['weight'] = df['weight'] / 100.0
    
    df['ETF_Ticker'] = ticker
    df['Holdings_As_Of'] = h_date
    df['Date_Scraped'] = TODAY
    return df[['ETF_Ticker', 'ticker', 'name', 'weight', 'Holdings_As_Of', 'Date_Scraped']]

def check_if_new_data(ticker, new_date):
    """ Smart Deduplication """
    file_path = os.path.join(DATA_DIR_LATEST, f"{ticker}.csv")
    if not os.path.exists(file_path):
        return True 
    try:
        existing_df = pd.read_csv(file_path, nrows=1)
        if 'Holdings_As_Of' in existing_df.columns:
            old_date = str(existing_df['Holdings_As_Of'].iloc[0])
            if old_date == str(new_date):
                return False 
    except: pass
    return True

def update_giant_history(new_dfs):
    """ Updates the single GIANT history file with new data """
    if not new_dfs: return
    
    print("\n🦕 Updating Giant History File...")
    
    # 1. Combine new data
    new_data = pd.concat(new_dfs)
    
    # 2. Load existing giant file if it exists
    if os.path.exists(GIANT_HISTORY_FILE):
        try:
            existing_data = pd.read_csv(GIANT_HISTORY_FILE)
            # Combine
            combined = pd.concat([existing_data, new_data])
        except:
            combined = new_data
    else:
        combined = new_data
    
    # 3. Deduplicate based on content, NOT scrap date
    # If we have the same Ticker + Holdings Date + Holding Ticker, keep only one.
    before_len = len(combined)
    combined.drop_duplicates(subset=['ETF_Ticker', 'ticker', 'Holdings_As_Of'], keep='last', inplace=True)
    after_len = len(combined)
    
    # 4. Save
    combined.to_csv(GIANT_HISTORY_FILE, index=False)
    print(f"    ✅ Giant History Updated: {after_len} rows total (Added {after_len - (len(existing_data) if os.path.exists(GIANT_HISTORY_FILE) else 0)} unique rows)")

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

    print(f"🚀 Launching Scraper V17.0 (Giant History) - {TODAY}")
    driver = setup_driver()
    os.makedirs(DATA_DIR_LATEST, exist_ok=True)
    os.makedirs(DATA_DIR_BACKUP, exist_ok=True)
    
    archive_path = os.path.join(DATA_DIR_HISTORY, *TODAY.split('-'))
    master_list = []
    new_data_list = [] # List specifically for Giant History (only new stuff)

    for etf in etfs:
        if not etf.get('enabled', True): continue
        ticker = etf['ticker']
        print(f"➳ {ticker}...")
        
        try:
            # --- PRIMARY TRACK ---
            df, h_date = None, TODAY
            
            if etf['scraper_type'] == 'pacer_csv':
                driver.get(etf['url'])
                time.sleep(3)
                text = driver.find_element(By.TAG_NAME, "body").text
                h_date = clean_date_string(text) or TODAY
                r = requests.get(etf['url'], headers=HEADERS, timeout=15)
                content = r.text.splitlines()
                start = 0
                for i, line in enumerate(content[:20]):
                    if "Ticker" in line or "Symbol" in line: start = i; break
                df = pd.read_csv(StringIO('\n'.join(content[start:])))

            elif etf['scraper_type'] == 'selenium_alpha':
                driver.get(etf['url'])
                time.sleep(3)
                text = driver.find_element(By.TAG_NAME, "body").text
                h_date = clean_date_string(text) or TODAY
                try:
                    selects = driver.find_elements(By.TAG_NAME, "select")
                    for s in selects:
                        try: Select(s).select_by_visible_text("All"); time.sleep(1)
                        except: pass
                except: pass
                dfs = pd.read_html(StringIO(driver.page_source))
                for d in dfs: 
                    if len(d) > 25: df = d; break

            elif etf['scraper_type'] == 'first_trust':
                driver.get(etf['url'])
                time.sleep(5) 
                text = driver.find_element(By.TAG_NAME, "body").text
                h_date = clean_date_string(text) or TODAY
                dfs = pd.read_html(StringIO(driver.page_source))
                df = find_first_trust_table(dfs)
            
            else: 
                r = requests.get(etf['url'], headers=HEADERS, timeout=15)
                h_date = clean_date_string(r.text) or TODAY
                dfs = pd.read_html(StringIO(r.text))
                for d in dfs:
                    if len(d) > 20: df = d; break

            # --- PROCESS PRIMARY ---
            clean_df = clean_dataframe(df, ticker, h_date)
            
            # --- BACKUP TRACK (Invesco) ---
            if 'backup_url' in etf:
                b_df, b_date = scrape_invesco_backup(driver, etf['backup_url'], ticker)
                if b_df is not None:
                    clean_backup = clean_dataframe(b_df, ticker, b_date)
                    if clean_backup is not None:
                        clean_backup['Holdings_As_Of'] = b_date
                        clean_backup.to_csv(os.path.join(DATA_DIR_BACKUP, f"{ticker}_official_backup.csv"), index=False)
                        print(f"      -> 🛡️ Backup Saved: {len(clean_backup)} rows | Date: {b_date}")
                    
                        if clean_df is None or (b_date > h_date):
                             clean_df = clean_backup
                             h_date = b_date

            # --- SMART SAVE LOGIC ---
            if clean_df is not None:
                # Always add to daily snapshot list
                master_list.append(clean_df)

                # Check if it's actually NEW
                if check_if_new_data(ticker, h_date):
                    clean_df.to_csv(os.path.join(DATA_DIR_LATEST, f"{ticker}.csv"), index=False)
                    print(f"    ✅ New Data Saved: {len(clean_df)} rows | Date: {h_date}")
                    new_data_list.append(clean_df) # Only append NEW data to giant history
                else:
                    print(f"    💤 Data unchanged ({h_date}). Skipping individual save.")

            else:
                 print(f"    ⚠️ No valid data found.")

        except Exception as e:
            print(f"    ❌ Error: {e}")

    driver.quit()

    # --- SAVE DAILY ARCHIVE ---
    if master_list:
        os.makedirs(archive_path, exist_ok=True)
        full_df = pd.concat(master_list)
        full_df.to_csv(os.path.join(archive_path, 'master_archive.csv'), index=False)
        print(f"\n📜 Daily Archive Created: {len(master_list)} ETFs in snapshot.")
    
    # --- UPDATE GIANT HISTORY ---
    if new_data_list:
        update_giant_history(new_data_list)
    else:
        print("\n🦕 Giant History: No new data to append.")

if __name__ == "__main__":
    main()
