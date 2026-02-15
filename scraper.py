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

# --- CONFIG ---
CONFIG_FILE = 'config.json'
DATA_DIR_LATEST = 'data/latest'
TODAY = datetime.now().strftime('%Y-%m-%d')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

def extract_holdings_date(text):
    """
    Advanced Date Extraction: Targets specific text patterns used by Pacer and Invesco.
    """
    if not text: return "Unknown"
    # Clean text to single line for easier regex matching
    text = " ".join(text.split())
    
    # Patterns for: Feb 12, 2026 | 02/12/2026 | 12-Feb-2026
    patterns = [
        r"(?:as of|effective|holdings as of|date of)\s*[:\s]*([A-Z][a-z]+\s+\d{1,2},?\s+\d{4})", 
        r"(?:as of|effective|holdings as of|date of)\s*[:\s]*(\d{1,2}/\d{1,2}/\d{4})",
        r"(?:as of|effective|holdings as of|date of)\s*[:\s]*(\d{1,2}-[A-Z][a-z]+-\d{4})"
    ]
    
    for p in patterns:
        match = re.search(p, text, re.IGNORECASE)
        if match:
            raw_date = match.group(1).replace(',', '')
            for fmt in ("%B %d %Y", "%m/%d/%Y", "%d-%b-%Y", "%b %d %Y"):
                try:
                    return datetime.strptime(raw_date, fmt).strftime('%Y-%m-%d')
                except: continue
    return "Unknown"

def setup_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    # Mask headless status to bypass First Trust/Alpha Architect blocks
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    return webdriver.Chrome(options=options)

def main():
    try:
        with open(CONFIG_FILE, 'r') as f: etfs = json.load(f)
    except: return

    print(f"🚀 Launching Scraper V13.6 (Deep Plumbing) - {TODAY}")
    driver = None
    os.makedirs(DATA_DIR_LATEST, exist_ok=True)

    for etf in etfs:
        if not etf.get('enabled', True): continue
        ticker = etf['ticker']
        print(f"➳ {ticker}...")
        
        try:
            df, h_date = None, "Unknown"

            # Use Selenium for families known to fail with simple requests
            if etf['scraper_type'] in ['pacer_csv', 'selenium_alpha', 'first_trust'] or ticker in ['COWZ', 'CALF', 'FPX', 'FPXI', 'QMOM', 'IMOM']:
                if driver is None: driver = setup_driver()
                driver.get(etf['url'])
                
                # Dynamic Wait: specifically for Pacer/First Trust tables
                time.sleep(5) # Allow JS to execute
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                
                # Scrape Date from visible text
                h_date = extract_holdings_date(driver.find_element(By.TAG_NAME, "body").text)
                
                # Fetch tables
                dfs = pd.read_html(StringIO(driver.page_source))
                for d in dfs:
                    if len(d) > 8: # Filter for the actual holdings table
                        df = d
                        break
            else:
                # Optimized Requests for Invesco/Third-Party Sources
                r = requests.get(etf['url'], headers=HEADERS, timeout=15)
                h_date = extract_holdings_date(r.text)
                dfs = pd.read_html(StringIO(r.text))
                for d in dfs:
                    if len(d) > 20: df = d; break

            # Standardized Cleaning
            clean_df = clean_dataframe(df, ticker, h_date)
            if clean_df is not None:
                clean_df.to_csv(os.path.join(DATA_DIR_LATEST, f"{ticker}.csv"), index=False)
                print(f"    ✅ Success: {len(clean_df)} rows | As Of: {h_date}")
            else:
                print(f"    ⚠️ Data Missing for {ticker}")

        except Exception as e:
            print(f"    ❌ Error: {e}")

    if driver: driver.quit()

if __name__ == "__main__":
    main()
