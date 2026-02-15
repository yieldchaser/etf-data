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

def extract_invesco_date_v15_7(driver):
    """ 
    Priority 1: Look for '# of holdings (as of DATE)' -> From 'Fund details' section
    Priority 2: Look for 'Etf holdings as of DATE' -> From Table Header
    Priority 3: Look for 'Holdings' anchor and scan forward
    """
    try:
        text = driver.find_element(By.TAG_NAME, "body").text
        
        # 1. THE USER'S KEY: "# of holdings (as of 02/12/2026)"
        # This is extremely specific and avoids YTD/Price confusion
        match_details = re.search(r"# of holdings\s*\(as of\s*(\d{1,2}/\d{1,2}/\d{4})\)", text, re.IGNORECASE)
        if match_details:
            return clean_date_string(match_details.group(1))

        # 2. Table Header Strategy: "Etf holdings as of February 12, 2026"
        match_header = re.search(r"Etf holdings as of\s*([A-Z][a-z]+\s+\d{1,2},?\s+\d{4})", text, re.IGNORECASE)
        if match_header:
            return clean_date_string(match_header.group(1))

        # 3. Fallback: Anchor Strategy (Holdings... as of)
        start_index = text.find("Holdings")
        if start_index != -1:
            scan_text = text[start_index : start_index + 500]
            match_anchor = re.search(r"(?:as of|date)[\s:]*([A-Z][a-z]+\s+\d{1,2},?\s+\d{4}|\d{1,2}/\d{1,2}/\d{4})", scan_text, re.IGNORECASE)
            if match_anchor:
                return clean_date_string(match_anchor.group(1))

    except: pass
    return TODAY

def scrape_invesco_backup(driver, url, ticker):
    try:
        print(f"      -> 🛡️ Running Backup Scraper for {ticker}...")
        driver.get(url)
        time.sleep(5)
        
        # 1. Grab Date using the V15.7 Logic
        h_date = extract_invesco_date_v15_7(driver)

        # 2. Extract Visible Table
        print(f"      -> Downloading from visible table...")
        df = None
        dfs = pd.read_html(StringIO(driver.page_source))
        
        for d in dfs:
            valid_keywords = ['ticker', 'symbol', 'holding', 'identifier', 'weight', '% of net assets']
            cols = [str(c).strip().lower() for c in d.columns]
            if any(k in cols for
