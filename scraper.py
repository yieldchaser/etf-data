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
DATA_DIR_BACKUP = 'data/invesco_backup'
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

def scrape_invesco_backup(driver, url, ticker):
    try:
        print(f"      -> 🛡️ Running Backup Scraper for {ticker}...")
        driver.get(url)
        
        # 1. Wait for page load & Grab Date
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        page_text = driver.find_element(By.TAG_NAME, "body").text
        h_date = extract_date_from_text(page_text)
        
        # 2. Find the REAL Download Link
        download_url = None
        try:
            # Look for "Export data" or "Download" link
            links = driver.find_elements(By.TAG_NAME, "a")
            for link in links:
                href = link.get_attribute("href")
                if href and ("action=download" in href or "download" in href.lower()):
                    download_url = href
                    break
        except: pass
        
        # If no link found, use the standard guess
        if not download_url:
            download_url = f"https://www.invesco.com/us/en/financial-products/etfs/holdings/main/holdings/0?ticker={ticker}&action=download"

        # 3. Download using Requests (With Session Cookies)
        s = requests.Session()
        for c in driver.get_cookies():
            s.cookies.set(c['name'], c['value'])
        s.headers.update({"User-Agent": driver.execute_script("return navigator.userAgent;")})
        
        r = s.get(download_url, timeout=15)
        
        if r.status_code == 200:
            lines = r.text.splitlines()
            start_row = 0
            for i, line in enumerate(lines[:30]):
                if "Ticker" in line or "Holding" in line or "Company" in line:
                    start_row = i; break
            
            df = pd.read_csv(StringIO("\n".join(lines[start_row:])))
            return df, h_date

        # 4. Fallback: If download fails, grab the visible table
        print(f"      -> Download failed ({r.status_code}), trying visible table...")
        dfs = pd.read_html(StringIO(driver.page_source))
        for d in dfs:
            if len(d) > 5: return d, h_date

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
    
    # FORCE THE DATE
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
    return webdriver.Chrome(options=options)

def main():
    try:
        with open(CONFIG_FILE, 'r') as f: etfs = json.load(f)
    except: return

    print(f"🚀 Launching Scraper V14.9 (Invesco Backup Fix) - {TODAY}")
    driver = setup_driver()
    os.makedirs(DATA_DIR_LATEST, exist_ok=True)
    os.makedirs(DATA_DIR_BACKUP, exist_ok=True)

    for etf in etfs:
        if not etf.get('enabled', True): continue
        ticker = etf['ticker']
        print(f"➳ {ticker}...")
        
        try:
            # --- PRIMARY SCRAPER ---
            df, h_date = None, TODAY
            
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

            elif etf['scraper_type'] == 'selenium_alpha':
                driver.get(etf['url'])
                time.sleep(3)
                h_date = extract_date_from_text(driver.find_element(By.TAG_NAME, "body").text)
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
                h_date = extract_date_from_text(driver.find_element(By.TAG_NAME, "body").text)
                dfs = pd.read_html(StringIO(driver.page_source))
                df = find_first_trust_table(dfs)
            
            else: 
                r = requests.get(etf['url'], headers=HEADERS, timeout=15)
                h_date = extract_date_from_text(r.text)
                dfs = pd.read_html(StringIO(r.text))
                for d in dfs:
                    if len(d) > 20: df = d; break

            clean_df = clean_dataframe(df, ticker, h_date)
            if clean_df is not None:
                clean_df.to_csv(os.path.join(DATA_DIR_LATEST, f"{ticker}.csv"), index=False)
                print(f"    ✅ Primary: {len(clean_df)} rows | Date: {h_date}")

            # --- BACKUP SCRAPER (DYNAMIC LINK) ---
            if 'backup_url' in etf:
                b_df, b_date = scrape_invesco_backup(driver, etf['backup_url'], ticker)
                
                if b_df is not None:
                    clean_backup = clean_dataframe(b_df, ticker, b_date)
                    if clean_backup is not None:
                        clean_backup.to_csv(os.path.join(DATA_DIR_BACKUP, f"{ticker}_official_backup.csv"), index=False)
                        print(f"      -> 🛡️ Backup Saved: {len(clean_backup)} rows | Date: {b_date}")
                    else:
                        print(f"      -> Backup Data Invalid")
                else:
                    print(f"      -> Backup Returned No Data")

        except Exception as e:
            print(f"    ❌ Error: {e}")

    driver.quit()

if __name__ == "__main__":
    main()
