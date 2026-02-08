import pandas as pd
import time
import json
import os
import shutil
import tempfile
from datetime import datetime
from io import StringIO
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
import requests

# --- CONFIG ---
CONFIG_FILE = 'config.json'
DATA_DIR_LATEST = 'data/latest'
DATA_DIR_HISTORY = 'data/history'
TODAY = datetime.now().strftime('%Y-%m-%d')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
}

# Quality thresholds
MIN_INVESCO_ROWS = 30  # Fail if we get less than this
MIN_OTHER_ROWS = 5

# Failure reason codes
class FailureReason:
    WAF_HTML = "WAF_HTML_BLOCK"
    EXPORT_NOT_FOUND = "EXPORT_BUTTON_NOT_FOUND"
    DOWNLOAD_TIMEOUT = "DOWNLOAD_TIMEOUT"
    PARSE_ERROR = "PARSE_ERROR"
    INSUFFICIENT_ROWS = "INSUFFICIENT_ROWS"
    NETWORK_ERROR = "NETWORK_ERROR"
    UNKNOWN = "UNKNOWN_ERROR"

def setup_driver_deterministic(download_dir=None):
    """
    PRODUCTION-READY: Deterministic Chrome driver with webdriver-manager
    """
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    # Download preferences if needed
    if download_dir:
        prefs = {
            "download.default_directory": download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
        }
        chrome_options.add_experimental_option("prefs", prefs)
    
    # USE WEBDRIVER-MANAGER for deterministic driver installation
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    # Anti-detection
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    return driver

def find_first_trust_table(dfs):
    """Header promotion logic for First Trust"""
    if not dfs:
        return None
    
    valid_keywords = ['ticker', 'symbol', 'holding', 'identifier', 'weighting', 'cusip']
    
    for i, df in enumerate(dfs):
        cols = [str(c).strip().lower() for c in df.columns]
        if any(k in cols for k in valid_keywords):
            return df
        
        if not df.empty:
            first_row = [str(x).strip().lower() for x in df.iloc[0].values]
            if any(k in first_row for k in valid_keywords):
                print(f"   -> Promoting header in Table #{i}")
                new_header = df.iloc[0]
                df = df[1:].copy()
                df.columns = new_header
                return df
    return None

def clean_dataframe(df, ticker):
    """Clean and standardize DataFrame"""
    if df is None or df.empty:
        return None

    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    
    col_map = {
        'stockticker': 'ticker', 'symbol': 'ticker', 'holding': 'ticker', 'ticker': 'ticker',
        'identifier': 'ticker', 'sedol': 'ticker',
        'securityname': 'name', 'company': 'name', 'security name': 'name', 
        'security_name': 'name', 'security': 'name', 'name': 'name',
        'weightings': 'weight', '% tna': 'weight', 'weight': 'weight', 
        '% of net assets': 'weight', 'weighting': 'weight', 
        '%_of_net_assets': 'weight', '% net assets': 'weight'
    }
    df.rename(columns=col_map, inplace=True)
    df = df.loc[:, ~df.columns.duplicated()].copy()

    if 'ticker' not in df.columns:
        print(f"   -> ⚠️ Missing 'ticker' column. Found: {list(df.columns)}")
        return None

    stop_words = ["cash", "usd", "liquidity", "government", "treasury", 
                  "money market", "net other", "total"]
    
    df.loc[:, 'name'] = df['name'].astype(str)
    df.loc[:, 'ticker'] = df['ticker'].astype(str)
    
    pattern = '|'.join(stop_words)
    mask = df['name'].str.contains(pattern, case=False, na=False) | \
           df['ticker'].str.contains(pattern, case=False, na=False)
    df = df[~mask].copy()

    df.loc[:, 'ticker'] = df['ticker'].str.replace(' USD', '', regex=False)
    df.loc[:, 'ticker'] = df['ticker'].str.replace('.UN', '', regex=False)
    df.loc[:, 'ticker'] = df['ticker'].str.upper().str.strip()

    if 'weight' in df.columns:
        if df['weight'].dtype == object:
            df.loc[:, 'weight'] = df['weight'].astype(str).str.replace('%', '').str.replace(',', '')
        df.loc[:, 'weight'] = pd.to_numeric(df['weight'], errors='coerce')
        if df['weight'].max() > 1.0:
            df.loc[:, 'weight'] = df['weight'] / 100.0
    else:
        df.loc[:, 'weight'] = 0.0

    df.loc[:, 'ETF_Ticker'] = ticker
    df.loc[:, 'Date_Scraped'] = TODAY
    
    return df[['ETF_Ticker', 'ticker', 'name', 'weight', 'Date_Scraped']].copy()

def scrape_invesco_export_button(ticker, download_dir):
    """
    PRIMARY STRATEGY: Click Export button and download file
    """
    print(f"   -> Primary: Export Button Download")
    
    driver = None
    try:
        driver = setup_driver_deterministic(download_dir=download_dir)
        
        url = f"https://www.invesco.com/us/financial-products/etfs/product-detail?audienceType=Investor&ticker={ticker}"
        driver.get(url)
        print(f"      ✓ Loaded {ticker} page")
        time.sleep(8)
        
        # Try to find Export button
        export_selectors = [
            "//button[contains(text(), 'Export data')]",
            "//a[contains(text(), 'Export data')]",
            "//button[contains(@aria-label, 'Export')]",
            "//*[contains(text(), 'Export data')]",
        ]
        
        clicked = False
        for selector in export_selectors:
            try:
                element = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, selector))
                )
                driver.execute_script("arguments[0].scrollIntoView(true);", element)
                time.sleep(1)
                driver.execute_script("arguments[0].click();", element)
                print(f"      ✓ Clicked Export button")
                clicked = True
                break
            except:
                continue
        
        if not clicked:
            driver.quit()
            return None, FailureReason.EXPORT_NOT_FOUND
        
        # Wait for download (30s timeout)
        print(f"      ⏳ Waiting for download...")
        start_time = time.time()
        downloaded_file = None
        
        while time.time() - start_time < 30:
            files = [f for f in os.listdir(download_dir) 
                    if f.endswith(('.xlsx', '.xls', '.csv')) and not f.endswith('.crdownload')]
            
            if files:
                files.sort(key=lambda x: os.path.getmtime(os.path.join(download_dir, x)), reverse=True)
                downloaded_file = os.path.join(download_dir, files[0])
                print(f"      ✓ Downloaded: {files[0]}")
                break
            time.sleep(1)
        
        driver.quit()
        
        if not downloaded_file:
            return None, FailureReason.DOWNLOAD_TIMEOUT
        
        # Parse file
        try:
            if downloaded_file.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(downloaded_file)
                print(f"      ✓ Parsed Excel: {len(df)} rows")
            else:
                # CSV with smart metadata skipping
                try:
                    df = pd.read_csv(downloaded_file)
                except:
                    with open(downloaded_file, 'r') as f:
                        lines = f.readlines()
                    start = 0
                    for i, line in enumerate(lines[:20]):
                        if any(k in line for k in ['Ticker', 'Symbol', 'Holding']):
                            start = i
                            break
                    df = pd.read_csv(StringIO(''.join(lines[start:])))
                print(f"      ✓ Parsed CSV: {len(df)} rows")
            
            # Check for WAF block (HTML content in file)
            if df.empty or '@doctype' in str(df.columns).lower():
                return None, FailureReason.WAF_HTML
            
            return df, None
            
        except Exception as e:
            print(f"      ✗ Parse error: {e}")
            return None, FailureReason.PARSE_ERROR
            
    except Exception as e:
        print(f"      ✗ Strategy failed: {e}")
        if driver:
            driver.quit()
        return None, FailureReason.UNKNOWN

def scrape_invesco_fallback(ticker):
    """
    FALLBACK: Just get the visible table (top 10)
    """
    print(f"   -> Fallback: Visible Table")
    
    driver = None
    try:
        driver = setup_driver_deterministic()
        
        url = f"https://www.invesco.com/us/financial-products/etfs/product-detail?audienceType=Investor&ticker={ticker}"
        driver.get(url)
        time.sleep(8)
        
        dfs = pd.read_html(StringIO(driver.page_source))
        for d in dfs:
            cols = [str(c).lower() for c in d.columns]
            if 'ytd' in cols or '1y' in cols:
                continue
            if len(d) > 5:
                driver.quit()
                print(f"      ✓ Got {len(d)} rows from visible table")
                return d, None
        
        driver.quit()
        return None, FailureReason.PARSE_ERROR
        
    except Exception as e:
        if driver:
            driver.quit()
        return None, FailureReason.UNKNOWN

def scrape_invesco(ticker, download_dir):
    """
    Simplified 2-strategy approach for Invesco
    """
    print(f"   📡 Scraping Invesco {ticker}...")
    
    # Strategy 1: Export button
    df, error = scrape_invesco_export_button(ticker, download_dir)
    if df is not None and len(df) >= MIN_INVESCO_ROWS:
        return df, None
    
    if error:
        print(f"   ⚠️ Primary failed: {error}")
    
    # Strategy 2: Fallback
    df, error = scrape_invesco_fallback(ticker)
    if df is not None:
        if len(df) < MIN_INVESCO_ROWS:
            print(f"   ⚠️ Only got {len(df)} rows (threshold: {MIN_INVESCO_ROWS})")
            return df, FailureReason.INSUFFICIENT_ROWS
        return df, None
    
    return None, error or FailureReason.UNKNOWN

def main():
    try:
        with open(CONFIG_FILE, 'r') as f:
            etfs = json.load(f)
    except:
        print("❌ Config file not found.")
        return

    print("🚀 Launching Scraper v15 (Production-Ready)")
    print(f"📅 Date: {TODAY}\n")
    
    driver = None
    session = requests.Session()
    session.headers.update(HEADERS)
    
    os.makedirs(DATA_DIR_LATEST, exist_ok=True)
    archive_path = os.path.join(DATA_DIR_HISTORY, *TODAY.split('-'))
    os.makedirs(archive_path, exist_ok=True)
    
    # Temp download directory
    download_dir = tempfile.mkdtemp()
    
    master_list = []
    failures = []

    for etf in etfs:
        if not etf.get('enabled', True):
            continue
            
        ticker = etf['ticker']
        print(f"➳ Processing {ticker}...")
        
        try:
            df = None
            error = None
            
            # --- PACER (Requests/CSV) ---
            if etf['scraper_type'] == 'pacer_csv':
                try:
                    r = session.get(etf['url'], timeout=20)
                    if r.status_code == 200:
                        content = r.text.splitlines()
                        start = 0
                        for i, line in enumerate(content[:20]):
                            if "Ticker" in line or "Symbol" in line:
                                start = i
                                break
                        df = pd.read_csv(StringIO('\n'.join(content[start:])))
                except Exception as e:
                    error = FailureReason.NETWORK_ERROR
                    print(f"   ❌ Error: {e}")

            # --- FIRST TRUST (Requests/HTML) ---
            elif etf['scraper_type'] == 'first_trust':
                try:
                    r = session.get(etf['url'], timeout=20)
                    dfs = pd.read_html(StringIO(r.text))
                    df = find_first_trust_table(dfs)
                except Exception as e:
                    error = FailureReason.NETWORK_ERROR
                    print(f"   ❌ Error: {e}")

            # --- ALPHA ARCHITECT (Selenium) ---
            elif 'alpha' in etf['url'] or etf['scraper_type'] == 'selenium_alpha':
                try:
                    if driver is None:
                        driver = setup_driver_deterministic()
                    
                    driver.get(etf['url'])
                    time.sleep(5)
                    
                    # Click "All"
                    try:
                        selects = driver.find_elements(By.TAG_NAME, "select")
                        for s in selects:
                            try:
                                Select(s).select_by_visible_text("All")
                                time.sleep(2)
                            except:
                                pass
                    except:
                        pass
                    
                    dfs = pd.read_html(StringIO(driver.page_source))
                    for d in dfs:
                        if len(d) > 25:
                            df = d
                            break
                except Exception as e:
                    error = FailureReason.UNKNOWN
                    print(f"   ❌ Error: {e}")

            # --- INVESCO (Simplified 2-strategy) ---
            elif etf['scraper_type'] == 'selenium_invesco':
                df, error = scrape_invesco(ticker, download_dir)

            # --- SAVE ---
            clean_df = clean_dataframe(df, ticker)
            
            if clean_df is not None and not clean_df.empty:
                # Quality gate
                row_count = len(clean_df)
                threshold = MIN_INVESCO_ROWS if 'invesco' in etf['scraper_type'] else MIN_OTHER_ROWS
                
                if row_count < threshold:
                    print(f"   ⚠️ Quality gate: {row_count} rows < {threshold} threshold")
                    failures.append({
                        'ticker': ticker,
                        'rows': row_count,
                        'reason': error or FailureReason.INSUFFICIENT_ROWS
                    })
                
                save_path = os.path.join(DATA_DIR_LATEST, f"{ticker}.csv")
                clean_df.to_csv(save_path, index=False)
                master_list.append(clean_df)
                print(f"   ✅ Success: {row_count} rows saved.\n")
            else:
                print(f"   ❌ Data not found.\n")
                failures.append({
                    'ticker': ticker,
                    'rows': 0,
                    'reason': error or FailureReason.PARSE_ERROR
                })

        except Exception as e:
            print(f"   ❌ Unexpected error: {e}\n")
            failures.append({
                'ticker': ticker,
                'rows': 0,
                'reason': FailureReason.UNKNOWN
            })

    # Cleanup
    if driver:
        driver.quit()
    
    try:
        shutil.rmtree(download_dir)
    except:
        pass

    # Save results
    if master_list:
        full_df = pd.concat(master_list, ignore_index=True)
        full_df.to_csv(os.path.join(archive_path, 'master_archive.csv'), index=False)
        full_df.to_csv(os.path.join(DATA_DIR_LATEST, 'master_latest.csv'), index=False)
        print(f"\n📜 Daily Archive Complete: {len(master_list)} ETFs processed")
        print(f"📊 Total holdings: {len(full_df)} rows")
    
    # Report failures
    if failures:
        print(f"\n⚠️ Failures: {len(failures)}")
        for f in failures:
            print(f"   • {f['ticker']}: {f['rows']} rows - {f['reason']}")
        
        # Save failure report
        failure_df = pd.DataFrame(failures)
        failure_df.to_csv(os.path.join(DATA_DIR_LATEST, 'failures.csv'), index=False)

if __name__ == "__main__":
    main()