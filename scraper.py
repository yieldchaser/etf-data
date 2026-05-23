import pandas as pd
import time
import json
import os
import re
import html as html_lib
import requests
from datetime import datetime
from io import StringIO
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select

# Import curl_cffi at module level (before Selenium starts) to avoid
# mid-session TLS library conflicts with ChromeDriver.
try:
    from curl_cffi import requests as cffi_requests
except ImportError:
    cffi_requests = None

# --- CONFIG ---
CONFIG_FILE = 'config.json'
DATA_DIR_LATEST = 'data/latest'
DATA_DIR_HISTORY = 'data/history'
DATA_DIR_BACKUP = 'data/invesco_backup'
DATA_DIR_LEGACY_BACKUP = 'data/legacy_backup'
GIANT_HISTORY_FILE = 'data/all_history.csv' 
TODAY = datetime.now().strftime('%Y-%m-%d')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

# ── Invesco API ───────────────────────────────────────────────────────────────
INVESCO_API_BASE   = "https://dng-api.invesco.com/cache/v1/accounts/en_US/shareclasses"
INVESCO_API_SUFFIX = "holdings/fund?idType=cusip&productType=ETF"
INVESCO_API_HEADERS = {
    "Accept": "*/*", "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-US,en;q=0.9", "Origin": "https://www.invesco.com",
    "Referer": "https://www.invesco.com/", "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors", "Sec-Fetch-Site": "same-site",
}
INVESCO_SKIP_TYPES = {
    "cash & equivalents", "cash", "cash equivalent", "fx forward",
    "futures", "option", "swap", "repurchase agreement", "treasury bill",
    "money market fund, taxable", "money market fund",
}

def fetch_invesco_api(etf_ticker, etf_cusip):
    """Fetch holdings from official Invesco API. Returns (df, date) or (None, TODAY)."""
    if cffi_requests is None:
        print(f"      -> curl_cffi not installed — skipping {etf_ticker}")
        return None, TODAY
    url = f"{INVESCO_API_BASE}/{etf_cusip}/{INVESCO_API_SUFFIX}"
    print(f"      -> Invesco API: {url}")
    try:
        resp = cffi_requests.get(url, headers=INVESCO_API_HEADERS, impersonate="chrome", timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"      -> API failed: {e}")
        return None, TODAY
    holdings_as_of = data.get("effectiveBusinessDate") or data.get("effectiveDate") or TODAY
    try: holdings_as_of = datetime.strptime(holdings_as_of[:10], "%Y-%m-%d").strftime("%Y-%m-%d")
    except: holdings_as_of = TODAY
    rows = []
    for h in data.get("holdings", []):
        sec_type = str(h.get("securityTypeName", "")).strip().lower()
        if sec_type in INVESCO_SKIP_TYPES: continue
        raw_ticker = str(h.get("ticker", "")).strip()
        if not raw_ticker or raw_ticker.lower() in ("none", "n/a", "", "usd", "agpxx"): continue
        pct = h.get("percentageOfTotalNetAssets")
        try: weight = float(pct) / 100.0
        except: weight = 0.0
        if weight <= 0: continue
        rows.append({"ETF_Ticker": etf_ticker, "ticker": raw_ticker,
                     "name": html_lib.unescape(str(h.get("issuerName", "")).strip()),
                     "weight": round(weight, 6), "Holdings_As_Of": holdings_as_of, "Date_Scraped": TODAY})
    if not rows:
        print(f"      -> 0 equity rows returned")
        return None, TODAY
    df = pd.DataFrame(rows, columns=["ETF_Ticker", "ticker", "name", "weight", "Holdings_As_Of", "Date_Scraped"])
    print(f"      -> {len(df)} equity rows | as_of={holdings_as_of}")
    return df, holdings_as_of

def clean_date_string(date_text):
    if not date_text: return None
    clean = re.sub(r"(?i)as of|date|[:,-]", " ", date_text).strip()
    match = re.search(r"(\d{1,2}/\d{1,2}/\d{4})|([A-Z][a-z]+\s+\d{1,2},?\s+\d{4})", clean)
    if match: clean = match.group(0).replace(',', '')
    for fmt in ("%m/%d/%Y", "%B %d %Y", "%b %d %Y", "%m %d %Y"):
        try: return datetime.strptime(clean, fmt).strftime('%Y-%m-%d')
        except: continue
    return None

def extract_invesco_nuclear_date(driver):
    try:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3) 
        html = driver.page_source
        match = re.search(r"# of holdings\s*\(as of\s*(\d{1,2}/\d{1,2}/\d{4})\)", html, re.IGNORECASE)
        if match: return clean_date_string(match.group(1))
    except: pass
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
            valid_keywords = ['ticker', 'symbol', 'holding', 'identifier', 'weight', '% of net assets', '% tna', '% market value']
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
    except: return None, TODAY

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
        'weight': ['weighting', '% weight', 'weight %', '% net assets', '% of net assets', 'weight', '% tna', '% market value']
    }
    for target, keywords in mappings.items():
        for col in df.columns:
            if any(k in col for k in keywords):
                df.rename(columns={col: target}, inplace=True)
                break
    if 'ticker' not in df.columns: return None
    if 'weight' not in df.columns: df['weight'] = 0.0
    
    if 'weight' in df.columns:
        df['weight'] = df['weight'].astype(str).str.replace('%', '', regex=False).str.replace(',', '', regex=False)
        df['weight'] = pd.to_numeric(df['weight'], errors='coerce').fillna(0.0)
        if df['weight'].max() > 1.0: df['weight'] = df['weight'] / 100.0
    
    df['ETF_Ticker'] = ticker
    df['Holdings_As_Of'] = h_date
    df['Date_Scraped'] = TODAY
    return df[['ETF_Ticker', 'ticker', 'name', 'weight', 'Holdings_As_Of', 'Date_Scraped']]

def check_if_new_data(ticker, new_date):
    file_path = os.path.join(DATA_DIR_LATEST, f"{ticker}.csv")
    if not os.path.exists(file_path): return True 
    try:
        existing_df = pd.read_csv(file_path, nrows=1)
        if 'Holdings_As_Of' in existing_df.columns:
            old_date = str(existing_df['Holdings_As_Of'].iloc[0])
            if old_date == str(new_date): return False 
    except: pass
    return True

def update_giant_history(new_dfs):
    if not new_dfs: return
    print(f"\n🦕 Updating Giant History File with {len(new_dfs)} datasets...")
    new_data = pd.concat(new_dfs)
    if os.path.exists(GIANT_HISTORY_FILE):
        try:
            existing_data = pd.read_csv(GIANT_HISTORY_FILE)
            combined = pd.concat([existing_data, new_data])
        except: combined = new_data
    else:
        combined = new_data
    combined.drop_duplicates(subset=['ETF_Ticker', 'ticker', 'Holdings_As_Of'], keep='last', inplace=True)
    combined.to_csv(GIANT_HISTORY_FILE, index=False)
    print(f"    ✅ Giant History Saved: {len(combined)} total rows.")

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

    print(f"🚀 Launching Scraper V17.4 (Anti-Trojan Enabled) - {TODAY}")
    driver = setup_driver()
    os.makedirs(DATA_DIR_LATEST, exist_ok=True)
    os.makedirs(DATA_DIR_BACKUP, exist_ok=True)
    os.makedirs(DATA_DIR_LEGACY_BACKUP, exist_ok=True)
    
    archive_path = os.path.join(DATA_DIR_HISTORY, *TODAY.split('-'))
    master_list = []
    backup_list = []  # <--- NEW LIST FOR BACKUP HISTORY
    new_data_list = []

    for etf in etfs:
        if not etf.get('enabled', True): continue
        ticker = etf['ticker']
        print(f"➳ {ticker}...")
        
        try:
            df, h_date = None, TODAY
            
            # --- SCRAPER SELECTION ---
            if etf['scraper_type'] == 'invesco_api':
                cusip = etf.get('cusip', '')
                if not cusip:
                    print(f"    invesco_api requires cusip — skipping {ticker}")
                    continue
                clean_df, h_date = fetch_invesco_api(ticker, cusip)
                if clean_df is not None:
                    master_list.append(clean_df)
                    if check_if_new_data(ticker, h_date):
                        clean_df.to_csv(os.path.join(DATA_DIR_LATEST, f"{ticker}.csv"), index=False)
                        print(f"    New Data Saved: {len(clean_df)} rows | Date: {h_date}")
                        new_data_list.append(clean_df)
                    else:
                        clean_df.to_csv(os.path.join(DATA_DIR_LATEST, f"{ticker}.csv"), index=False)
                        print(f"    (Forced Update) Saved: {len(clean_df)} rows | Date: {h_date}")
                else:
                    print(f"    No data for {ticker}.")
                continue

            elif etf['scraper_type'] == 'pacer_csv':
                driver.get(etf['url']); time.sleep(3)
                text = driver.find_element(By.TAG_NAME, "body").text
                h_date = clean_date_string(text) or TODAY
                r = requests.get(etf['url'], headers=HEADERS, timeout=15)
                content = r.text.splitlines()
                start = 0
                for i, line in enumerate(content[:20]):
                    if "Ticker" in line or "Symbol" in line: start = i; break
                df = pd.read_csv(StringIO('\n'.join(content[start:])))

            elif etf['scraper_type'] == 'selenium_alpha':
                driver.get(etf['url']); time.sleep(3)
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
                driver.get(etf['url']); time.sleep(5) 
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

            # --- CLEAN PRIMARY ---
            clean_df = clean_dataframe(df, ticker, h_date)
            
            # --- RUN BACKUP SCRAPER ---
            if 'backup_url' in etf:
                b_df, b_date = scrape_invesco_backup(driver, etf['backup_url'], ticker)
                if b_df is not None:
                    clean_backup = clean_dataframe(b_df, ticker, b_date)
                    if clean_backup is not None:
                        clean_backup['Holdings_As_Of'] = b_date
                        
                        # 1. SAVE TO LATEST BACKUP (Overwrites daily)
                        clean_backup.to_csv(os.path.join(DATA_DIR_BACKUP, f"{ticker}_official_backup.csv"), index=False)
                        print(f"      -> 🛡️ Backup Saved: {len(clean_backup)} rows")
                        
                        # 2. ADD TO BACKUP HISTORY LIST (New!)
                        backup_list.append(clean_backup)

                        # 3. DECIDE: Use Backup or Primary?
                        if clean_df is None or clean_df.empty:
                             clean_df = clean_backup
                             h_date = b_date
                        elif len(clean_df) < 5 and len(clean_backup) > 5:
                             clean_df = clean_backup
                             h_date = b_date
                        elif b_date > h_date:
                             # TROJAN HORSE FIX: Don't let a Top-10 preview overwrite a full list!
                             if len(clean_backup) <= 15 and len(clean_df) > 15:
                                 print(f"      -> ⚠️ Backup newer ({b_date}) but only {len(clean_backup)} rows. Keeping Primary ({len(clean_df)} rows).")
                             else:
                                 clean_df = clean_backup
                                 h_date = b_date

            if clean_df is not None:
                master_list.append(clean_df) 
                
                if check_if_new_data(ticker, h_date):
                    clean_df.to_csv(os.path.join(DATA_DIR_LATEST, f"{ticker}.csv"), index=False)
                    print(f"    ✅ New Data Saved: {len(clean_df)} rows | Date: {h_date}")
                    new_data_list.append(clean_df)
                else:
                    clean_df.to_csv(os.path.join(DATA_DIR_LATEST, f"{ticker}.csv"), index=False)
                    print(f"    ✅ (Forced Update) Saved: {len(clean_df)} rows | Date: {h_date}")

            else: print(f"    ⚠️ No valid data found.")

        except Exception as e: print(f"    ❌ Error: {e}")

    driver.quit()

    # --- SAVE HISTORY FILES ---
    if master_list:
        os.makedirs(archive_path, exist_ok=True)
        
        # 1. Save the MASTER Archive (Used by Excel)
        full_df = pd.concat(master_list)
        full_df.to_csv(os.path.join(archive_path, 'master_archive.csv'), index=False)
        print(f"\n📜 Daily Master Archive Created.")
        
        # 2. Save the BACKUP Archive (Safety Net - NEW!)
        if backup_list:
            backup_df = pd.concat(backup_list)
            backup_df.to_csv(os.path.join(archive_path, 'raw_invesco_backups.csv'), index=False)
            print(f"🛡️ Daily Backup Archive Created (Just in case).")

    # --- UPDATE GIANT HISTORY ---
    if new_data_list:
        update_giant_history(new_data_list)
    elif not os.path.exists(GIANT_HISTORY_FILE) and master_list:
        print("\n🦕 Initializing Giant History File...")
        update_giant_history(master_list)
    else:
        print("\n🦕 Giant History: No new data.")

    # --- EXTENDED SCRAPERS (Playwright / PDF / XLS sources) ---
    run_extended_scrapers()

if __name__ == "__main__":
    main()


# ── Extended scraper bridge ────────────────────────────────────────────────────
def run_extended_scrapers():
    """
    Calls the extended ETF scraper (scripts/etf_holdings_scraper_v42.py),
    normalises its canonical output CSV to the pipeline schema, and merges
    into data/all_history.csv via the standard update_giant_history() path.

    Schema mapping:
        etf          → ETF_Ticker
        ticker       → ticker
        name         → name
        weight_pct   → weight  (divide by 100 to get decimal)
        as_of_date   → Holdings_As_Of
        scrape_date  → Date_Scraped  (already YYYYMMDD → reformatted)
    """
    import sys
    import importlib.util

    scraper_path = os.path.join(os.path.dirname(__file__), 'scripts', 'etf_holdings_scraper_v42.py')
    if not os.path.exists(scraper_path):
        print("\n⚠️  Extended scraper not found — skipping.")
        return

    # ── Output file the extended scraper writes ──────────────────────────────
    from datetime import date
    today_8 = date.today().strftime('%Y%m%d')
    csv_out = f'etf_holdings_{today_8}.csv'

    # Run the extended scraper if the output doesn't already exist for today
    if not os.path.exists(csv_out):
        print(f"\n🔌 Running extended ETF scrapers (scripts/etf_holdings_scraper_v42.py)…")
        try:
            spec   = importlib.util.spec_from_file_location("etf_scraper_ext", scraper_path)
            module = importlib.util.module_from_spec(spec)
            # Temporarily suppress the module-level print() that runs on import
            import io as _io, contextlib
            with contextlib.redirect_stdout(_io.StringIO()):
                spec.loader.exec_module(module)
            module.run_all()
            print(f"  Extended scrapers done → {csv_out}")
        except Exception as e:
            print(f"  ❌ Extended scraper failed: {e}")
            return
    else:
        print(f"\n🔌 Extended scraper output already exists for today ({csv_out}) — using it.")

    # ── Read canonical CSV and normalise ─────────────────────────────────────
    if not os.path.exists(csv_out):
        print("  ⚠️  No extended scraper output file found — skipping bridge.")
        return

    try:
        raw = pd.read_csv(csv_out)
        print(f"  Extended CSV: {len(raw)} rows, ETFs: {sorted(raw['etf'].unique())}")
    except Exception as e:
        print(f"  ❌ Could not read extended CSV: {e}")
        return

    # ── Bridge: normalise to pipeline schema ─────────────────────────────────
    required = {'etf', 'ticker', 'name', 'weight_pct', 'as_of_date'}
    if not required.issubset(set(raw.columns)):
        missing = required - set(raw.columns)
        print(f"  ❌ Extended CSV missing columns: {missing} — skipping bridge.")
        return

    bridge = raw.copy()

    # Drop non-equity rows (cash, derivatives, etc.)
    if 'security_type' in bridge.columns:
        non_equity_types = {
            'cash', 'cash equivalent', 'money market', 'derivative',
            'futures', 'option', 'swap', 'fx', 'currency',
        }
        mask = bridge['security_type'].fillna('').str.lower()
        bridge = bridge[~mask.str.contains('|'.join(non_equity_types), na=False)]

    # Drop rows with no usable ticker
    bridge = bridge[bridge['ticker'].notna() & bridge['ticker'].astype(str).str.strip().ne('')]
    bridge = bridge[~bridge['ticker'].astype(str).str.startswith('$')]

    # Weight: weight_pct is stored as percentage (e.g. 5.25 = 5.25%) → divide by 100
    bridge['weight'] = pd.to_numeric(bridge['weight_pct'], errors='coerce').fillna(0) / 100.0
    bridge = bridge[bridge['weight'] > 0]

    # Date normalisation
    bridge['Holdings_As_Of'] = pd.to_datetime(bridge['as_of_date'], errors='coerce').dt.strftime('%Y-%m-%d')
    bridge['Date_Scraped']   = pd.to_datetime(bridge['scrape_date'].astype(str), format='%Y%m%d', errors='coerce').dt.strftime('%Y-%m-%d')
    bridge['Date_Scraped']   = bridge['Date_Scraped'].fillna(TODAY)

    # Final schema
    bridge = bridge.rename(columns={'etf': 'ETF_Ticker', 'name': 'name'})
    pipeline_df = bridge[['ETF_Ticker', 'ticker', 'name', 'weight', 'Holdings_As_Of', 'Date_Scraped']].copy()
    pipeline_df = pipeline_df.dropna(subset=['Holdings_As_Of'])

    if pipeline_df.empty:
        print("  ⚠️  Extended bridge produced 0 valid rows — skipping.")
        return

    print(f"  Bridge: {len(pipeline_df)} rows across {pipeline_df['ETF_Ticker'].nunique()} ETFs")
    for etf, grp in pipeline_df.groupby('ETF_Ticker'):
        aod = grp['Holdings_As_Of'].iloc[0]
        print(f"    {etf:6} {len(grp):4} rows | as_of={aod}")

    # Merge into giant history
    update_giant_history([pipeline_df])
