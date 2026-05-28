import pandas as pd
import time
import json
import os
import re
import html as html_lib
import requests
from datetime import datetime
from io import StringIO

# Selenium is only needed at runtime (scraper execution), not for tests/build.
# Lazy-import so the module can be imported without selenium installed.
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import Select
    _SELENIUM_AVAILABLE = True
except ImportError:
    webdriver = None  # type: ignore
    Options = None    # type: ignore
    By = None         # type: ignore
    WebDriverWait = None  # type: ignore
    EC = None         # type: ignore
    Select = None     # type: ignore
    _SELENIUM_AVAILABLE = False

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

# ── US trading-day helper ─────────────────────────────────────────────────────
# Build the holiday set once at import time so every caller shares the same
# object (no repeated calendar construction inside hot loops).
import datetime as _dt
from pandas.tseries.holiday import USFederalHolidayCalendar as _USCal
_US_HOLIDAYS: frozenset = frozenset(
    _USCal().holidays(start='2010-01-01', end='2040-12-31')
    .strftime('%Y-%m-%d')
    .tolist()
)

def roll_to_prev_biz_day(d_str: str) -> str:
    """Return the most recent US business day <= d_str.

    Rolls back through weekends and US federal holidays.  Safe to call
    on already-valid trading days (returns the input unchanged).
    """
    try:
        d = _dt.date.fromisoformat(d_str)
    except (ValueError, TypeError):
        return d_str
    while d.weekday() >= 5 or d.isoformat() in _US_HOLIDAYS:
        d -= _dt.timedelta(days=1)
    return d.isoformat()

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

# ── v42 Extended Scraper Bridge constants ─────────────────────────────────────
# V42_ETFS: ownership manifest — these tickers are scraped by
# scripts/etf_holdings_scraper_v42.py via the Bridge, NOT by the primary loop.
# REQUIRED_CANONICAL_COLUMNS: schema validation gate for the Bridge.
# NON_EQUITY_SECURITY_TYPES: substring filters applied to security_type.
V42_ETFS: frozenset = frozenset({
    "VLUE", "AVSC", "GRIN", "JHMM", "JHEM", "JHSC", "MFEM", "JOET",
})

REQUIRED_CANONICAL_COLUMNS: frozenset = frozenset({
    "etf", "ticker", "name", "weight_pct", "as_of_date",
})

NON_EQUITY_SECURITY_TYPES: tuple = (
    "cash", "cash equivalent", "money market", "derivative",
    "futures", "option", "swap", "fx", "currency",
)


def check_v42_ownership_collisions(primary_config: list) -> list:
    """
    Return the sorted list of tickers appearing in BOTH config.json and V42_ETFS.

    Logs a single-line warning per collision in the form:
        ⚠️  Ownership collision: '<ticker>' appears in both config.json and V42_ETFS

    An empty intersection is the healthy case (returns []). The function does
    not raise: malformed config items (e.g. missing the 'ticker' key, non-dict
    entries) are skipped rather than blowing up startup. Only side effect is
    logging, so this is safe to call once from main() at startup.

    Args:
        primary_config: parsed contents of config.json — a list of dicts each
            expected to carry a 'ticker' key.

    Returns:
        Sorted list of colliding ticker strings.
    """
    primary_tickers = set()
    for item in primary_config:
        try:
            ticker = item["ticker"]
        except (KeyError, TypeError):
            # Defensive: skip malformed entries instead of raising.
            continue
        if isinstance(ticker, str) and ticker:
            primary_tickers.add(ticker)

    collisions = sorted(primary_tickers & V42_ETFS)
    for ticker in collisions:
        print(f"⚠️  Ownership collision: '{ticker}' appears in both config.json and V42_ETFS")
    return collisions


# Pipeline_Schema column order — every Bridge output must conform to this.
PIPELINE_SCHEMA_COLS = ["ETF_Ticker", "ticker", "name", "weight", "Holdings_As_Of", "Date_Scraped"]


def clean_canonical_csv(csv_path):
    """
    Read and validate the v42 Canonical_CSV at `csv_path`, returning a
    (cleaned_df, errors) tuple.

    cleaned_df: pandas DataFrame in Pipeline_Schema column order
                ["ETF_Ticker", "ticker", "name", "weight", "Holdings_As_Of", "Date_Scraped"].
                Empty (zero rows, but with Pipeline_Schema columns) if any validation
                step fails or no rows survive cleaning.
    errors:     list of human-readable failure descriptions. Empty on success.

    Validation steps (executed in order):
      1. Read CSV via pd.read_csv. On parse exception → (empty_df, ["read failed: <msg>"]).
      2. Verify REQUIRED_CANONICAL_COLUMNS ⊆ df.columns. On miss → (empty_df,
         ["missing columns: {set}"]).
      3. Drop rows whose `security_type` (lower-cased) contains any substring in
         NON_EQUITY_SECURITY_TYPES.
      4. Drop rows whose `ticker` is null/empty/leading-`$`.
      5. Coerce `weight_pct` via pd.to_numeric(errors='coerce'), divide by 100;
         drop rows where the result is ≤ 0 or > 1. Log identifier when
         weight_pct > 100 (Req 14.2).
      6. Parse `as_of_date` via pd.to_datetime(errors='coerce'); drop NaT rows.
      7. Build `Date_Scraped` from `scrape_date` (YYYYMMDD) when present, else
         fall back to module-level TODAY.
      8. Rename `etf` → `ETF_Ticker` and project to Pipeline_Schema column order.
      9. When `as_of_date > Date_Scraped`, log a warning naming the ETF but still
         include the row (Req 14.3).

    Pure function. The only I/O is reading `csv_path` (and stdout logging).
    """
    empty_df = pd.DataFrame(columns=PIPELINE_SCHEMA_COLS)

    # Step 1: Read CSV.
    # NOTE: keep_default_na=False, na_values=[] preserves ticker strings such
    # as "NULL", "NA", "NaN", "None", "N/A", "null", "n/a", "nan" verbatim.
    # Pandas' default na_values would coerce these to NaN, breaking the
    # bridge's idempotence on byte level (see test_bridge_idempotent_on_repeat_invocation).
    try:
        raw = pd.read_csv(csv_path, keep_default_na=False, na_values=[])
    except Exception as e:
        return empty_df, [f"read failed: {e}"]

    # Step 2: Required-column gate.
    missing = REQUIRED_CANONICAL_COLUMNS - set(raw.columns)
    if missing:
        return empty_df, [f"missing columns: {set(missing)}"]

    df = raw.copy()

    # Step 3: Filter non-equity security types (substring, case-insensitive).
    if "security_type" in df.columns:
        sec_lower = df["security_type"].fillna("").astype(str).str.lower()
        non_equity_pattern = "|".join(re.escape(s) for s in NON_EQUITY_SECURITY_TYPES)
        df = df[~sec_lower.str.contains(non_equity_pattern, na=False, regex=True)]

    # Step 4: Drop null/empty/leading-$ tickers.
    df = df[df["ticker"].notna()]
    df = df.assign(ticker=df["ticker"].astype(str).str.strip())
    df = df[df["ticker"].ne("") & ~df["ticker"].str.startswith("$")]

    # Step 5: weight_pct → weight (÷100); drop rows outside (0, 1].
    # Some sources (MFEM/PIMCO XLS, AVSC/Avantis) emit weight_pct as a string with
    # a trailing '%' (e.g. "2.62%"). Strip non-numeric characters before coercion
    # so those rows are not silently dropped by pd.to_numeric. Also strips
    # commas which appear in some locale-formatted CSVs (e.g. "1,234.56").
    weight_pct_str = df["weight_pct"].astype(str).str.strip().str.replace("%", "", regex=False).str.replace(",", "", regex=False)
    weight_pct_numeric = pd.to_numeric(weight_pct_str, errors="coerce")
    df = df.assign(weight=weight_pct_numeric / 100.0)

    # Log offending rows where weight_pct > 100 before dropping (Req 14.2).
    over_mask = weight_pct_numeric > 100
    if over_mask.any():
        for _, row in df[over_mask].iterrows():
            etf_ident = row.get("etf", "")
            tkr_ident = row.get("ticker", "")
            wp_value = row.get("weight_pct", "")
            print(f"  ⚠️  weight_pct > 100: {etf_ident}/{tkr_ident} weight_pct={wp_value}")

    df = df[(df["weight"] > 0) & (df["weight"] <= 1)]

    # Step 6: Parse as_of_date; drop NaT.
    as_of_dt = pd.to_datetime(df["as_of_date"], errors="coerce")
    df = df.assign(_as_of_dt=as_of_dt)
    df = df[df["_as_of_dt"].notna()]
    df = df.assign(Holdings_As_Of=df["_as_of_dt"].dt.strftime("%Y-%m-%d"))

    # Step 7: Date_Scraped from scrape_date (YYYYMMDD) or fall back to TODAY.
    if "scrape_date" in df.columns:
        scraped_dt = pd.to_datetime(
            df["scrape_date"].astype(str), format="%Y%m%d", errors="coerce"
        )
        scraped_str = scraped_dt.dt.strftime("%Y-%m-%d")
        df = df.assign(Date_Scraped=scraped_str.fillna(TODAY))
    else:
        df = df.assign(Date_Scraped=TODAY)

    # Step 9: §28 date bug fix — clamp Holdings_As_Of to ≤ today (build date).
    # The Invesco API returns T+1 effectiveBusinessDate which can be in the future
    # relative to the build. Clamp to today so Holdings date ≤ Built date.
    if not df.empty:
        today_clamp = TODAY
        future_mask = df["Holdings_As_Of"] > today_clamp
        if future_mask.any():
            for _, row in df[future_mask].iterrows():
                print(
                    f"  ⚠️  Future Holdings_As_Of clamped for {row.get('etf', '')}: "
                    f"{row.get('Holdings_As_Of', '')} → {today_clamp}"
                )
            df.loc[future_mask, "Holdings_As_Of"] = today_clamp
        # Belt-and-suspenders: roll back any weekend/holiday dates that slipped
        # through (e.g. GRIN's DOM scraper picking up a non-trading date).
        nonbiz_mask = df["Holdings_As_Of"].apply(
            lambda s: pd.Timestamp(s).weekday() >= 5 or s in _US_HOLIDAYS
        )
        if nonbiz_mask.any():
            for _, row in df[nonbiz_mask].iterrows():
                print(
                    f"  ⚠️  Non-trading Holdings_As_Of rolled back for "
                    f"{row.get('etf', '')}: {row.get('Holdings_As_Of', '')}"
                )
            df.loc[nonbiz_mask, "Holdings_As_Of"] = (
                df.loc[nonbiz_mask, "Holdings_As_Of"].apply(roll_to_prev_biz_day)
            )

    # Step 8: Rename etf → ETF_Ticker, round weight, project to Pipeline_Schema.
    df = df.rename(columns={"etf": "ETF_Ticker"})
    df = df.assign(weight=df["weight"].round(6))

    cleaned = df[PIPELINE_SCHEMA_COLS].copy().reset_index(drop=True)
    return cleaned, []


def write_v42_latest_snapshots(cleaned_df):
    """
    For each distinct ETF_Ticker group in `cleaned_df`, write
    `data/latest/{ETF_Ticker}.csv` containing only that ETF's rows in
    Pipeline_Schema column order. Overwrites any pre-existing per-ETF
    latest file (matches the primary loop's per-ETF latest-write
    semantics).

    Each per-ETF write is wrapped in try/except so that one ETF's I/O
    failure does not abort writes for the others.

    No-op if `cleaned_df` is empty (immediate return; no directory side
    effects beyond the defensive `os.makedirs`).

    Validates Property 6 part (b); implements Requirement 4.2.
    """
    if cleaned_df.empty:
        return

    os.makedirs(DATA_DIR_LATEST, exist_ok=True)

    for etf_ticker, group in cleaned_df.groupby("ETF_Ticker"):
        path = os.path.join(DATA_DIR_LATEST, f"{etf_ticker}.csv")
        try:
            group[PIPELINE_SCHEMA_COLS].to_csv(path, index=False)
            print(f"🛰️  Latest snapshot saved: {etf_ticker:6} ({len(group)} rows)")
        except Exception as e:
            print(f"❌ Failed to write latest/{etf_ticker}.csv: {e}")


def append_v42_to_master_archive(cleaned_df, today):
    """
    Append `cleaned_df` to `data/history/{YYYY}/{MM}/{DD}/master_archive.csv`,
    deriving the dated directory from the `today` argument (YYYY-MM-DD).

    Behaviour:
      - No-op (immediate return) if `cleaned_df` is empty.
      - If `master_archive.csv` does not exist at the dated path, create it
        with `cleaned_df[PIPELINE_SCHEMA_COLS]` content.
      - If it exists, read it, `pd.concat` with `cleaned_df`, deduplicate on
        (ETF_Ticker, ticker, Holdings_As_Of) keeping the last occurrence,
        project to PIPELINE_SCHEMA_COLS, and write back.
      - The entire read+concat+dedupe+write pipeline is wrapped in a single
        try/except. On any I/O exception, the error is logged and the
        existing file is left untouched (the function returns without
        raising).

    Atomicity note:
        This implementation provides only **best-effort atomicity**. A real
        atomic write would require a temp file + os.replace. Because the
        write is the last step (read+concat+dedup must succeed first) the
        existing archive is preserved across most expected failure modes,
        but a crash mid-`to_csv` can still leave a partially-written file.

    Args:
        cleaned_df: pandas DataFrame in Pipeline_Schema column order.
        today: YYYY-MM-DD string identifying the archive date.

    Returns:
        None.

    Validates Property 6 part (c); implements Requirements 4.3, 4.4.
    """
    if cleaned_df.empty:
        return

    archive_dir = os.path.join(DATA_DIR_HISTORY, *today.split("-"))
    os.makedirs(archive_dir, exist_ok=True)
    archive_path = os.path.join(archive_dir, "master_archive.csv")

    try:
        if os.path.exists(archive_path):
            # keep_default_na=False, na_values=[] preserves ticker strings
            # such as "NULL"/"NA"/"NaN" verbatim so the dedup composite key
            # (ETF_Ticker, ticker, Holdings_As_Of) stays stable across runs.
            existing = pd.read_csv(archive_path, keep_default_na=False, na_values=[])
            combined = pd.concat([existing, cleaned_df], ignore_index=True)
            combined = combined.drop_duplicates(
                subset=["ETF_Ticker", "ticker", "Holdings_As_Of"], keep="last"
            )
            combined = combined[PIPELINE_SCHEMA_COLS]
            combined.to_csv(archive_path, index=False)
            print(
                f"📜  Master archive appended: {len(cleaned_df)} v42 rows merged "
                f"at {today} (final size: {len(combined)} rows)"
            )
        else:
            cleaned_df[PIPELINE_SCHEMA_COLS].to_csv(archive_path, index=False)
            print(
                f"📜  Master archive created: {len(cleaned_df)} v42 rows at {today}"
            )
    except Exception as e:
        print(f"❌ Failed to update master_archive at {today}: {e}")


def bridge_write_all_sinks(cleaned_df):
    """
    Bridge fan-out helper: write `cleaned_df` to all three downstream sinks
    in a fixed order, matching the data-path obligations of the primary loop.

    Behaviour:
      - No-op if `cleaned_df` is empty (logs `⚠️  Bridge: 0 valid rows —
        no sinks updated` and returns).
      - Otherwise, before any write, logs per-ETF row counts in the same
        format the primary loop uses:
              `{TICKER:6} {rows:4} rows | as_of={YYYY-MM-DD}`
      - Then fans `cleaned_df` out to all three sinks, in this order:
          1. `update_giant_history([cleaned_df])`  — append + dedupe into
             `data/all_history.csv` (Giant_History).
          2. `write_v42_latest_snapshots(cleaned_df)` — overwrite the
             per-ETF snapshot at `data/latest/{ETF_Ticker}.csv`.
          3. `append_v42_to_master_archive(cleaned_df, TODAY)` — append-or-
             create `data/history/{Y}/{M}/{D}/master_archive.csv` (the
             dated Master_Archive).

    Ordering note:
        This helper MUST be invoked AFTER the primary loop has written the
        primary Master_Archive for the current date (Req 4.5). The Bridge
        is a pipeline stage that runs at the end of `main()`, not in
        parallel with the primary loop.

    Post-condition (when called on a non-empty DataFrame):
        - Every `(ETF_Ticker, ticker, Holdings_As_Of)` triple in
          `cleaned_df` is present in Giant_History.
        - `data/latest/{e}.csv` exists for every distinct `ETF_Ticker`
          value `e` in `cleaned_df`.
        - The dated Master_Archive contains every row in `cleaned_df`
          (modulo dedupe on the composite key).

    Args:
        cleaned_df: pandas DataFrame in Pipeline_Schema column order.

    Returns:
        None.

    Implements Requirements 4.1, 4.5; validates Property 6 (full).
    """
    if cleaned_df.empty:
        print("⚠️  Bridge: 0 valid rows — no sinks updated")
        return

    print("\n🌉  Bridge fan-out — writing v42 rows to all three sinks...")
    for etf, group in cleaned_df.groupby("ETF_Ticker"):
        as_of = group["Holdings_As_Of"].iloc[0]
        print(f"    {etf:6} {len(group):4} rows | as_of={as_of}")

    update_giant_history([cleaned_df])
    write_v42_latest_snapshots(cleaned_df)
    append_v42_to_master_archive(cleaned_df, TODAY)


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
    # §28 date bug fix: clamp Holdings_As_Of to ≤ TODAY so future-dated T+1
    # API responses don't produce holdings dated after the build date.
    if holdings_as_of > TODAY:
        print(f"      -> ⚠️  Future date clamped: {holdings_as_of} → {TODAY}")
        holdings_as_of = TODAY
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
        try:
            parsed = datetime.strptime(clean, fmt).strftime('%Y-%m-%d')
            # §28 date bug fix: clamp future dates to TODAY
            if parsed > TODAY:
                return TODAY
            return parsed
        except: continue
    return None

def extract_invesco_nuclear_date(driver):
    try:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3) 
        html = driver.page_source
        match = re.search(r"# of holdings\s*\(as of\s*(\d{1,2}/\d{1,2}/\d{4})\)", html, re.IGNORECASE)
        if match:
            parsed = clean_date_string(match.group(1))
            # clamp future dates
            if parsed and parsed > TODAY:
                return TODAY
            return parsed
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
            # keep_default_na=False, na_values=[] preserves ticker strings
            # such as "NULL"/"NA"/"NaN" verbatim so dedup composite keys
            # remain stable across consecutive bridge invocations.
            existing_data = pd.read_csv(GIANT_HISTORY_FILE, keep_default_na=False, na_values=[])
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

def run_extended_scrapers():
    """
    Bridge entry point. Composes the helpers ``clean_canonical_csv`` and
    ``bridge_write_all_sinks`` to integrate v42's Canonical_CSV output into
    the primary pipeline's three sinks (Giant_History, per-ETF latest
    snapshots, dated Master_Archive).

    Flow (matches design.md → "Components and Interfaces → run_extended_scrapers"):

      1. Resolve ``scraper_path``. If missing, log ``❌`` and return.
      2. Resolve ``csv_out`` path (``etf_holdings_YYYYMMDD.csv`` at repo root).
      3. If ``csv_out`` exists at start: log
         ``Bridge: reusing existing canonical CSV at <path>`` and skip the
         subprocess (Req 2.7, 14.5).
      4. Else invoke v42 via ``subprocess.run(... timeout=600,
         capture_output=True, text=True)``. Catch ``subprocess.TimeoutExpired``
         and generic ``Exception``. In all subprocess outcomes, print the last
         3000 chars of stdout to the run log (Req 2.6); on non-zero return
         code, also print the last 1000 chars of stderr (Req 2.3).
      5. After subprocess: if ``csv_out`` is still missing, log
         ``❌ No extended output`` and return.
      6. ``cleaned_df, errors = clean_canonical_csv(csv_out)``. If ``errors``
         is non-empty, log each ``⚠️ `` line and return.
      7. If ``cleaned_df`` is empty, log ``⚠️  Bridge: 0 valid rows`` and
         return (short-circuit; ``bridge_write_all_sinks`` would also handle
         this case, but the early return keeps the call site symmetric).
      8. Else call ``bridge_write_all_sinks(cleaned_df)``.

    The reuse-existing-CSV branch (step 3) MUST NOT re-invoke the subprocess
    (Req 14.5) — this preserves same-day-rerun idempotency.

    Implements Requirements 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8.
    """
    import subprocess
    import sys

    repo_root = os.path.dirname(os.path.abspath(__file__))
    scraper_path = os.path.join(repo_root, 'scripts', 'etf_holdings_scraper_v42.py')
    if not os.path.exists(scraper_path):
        print("\n❌ Extended scraper not found — skipping.")
        return

    today_8 = TODAY.replace('-', '')
    csv_out = os.path.join(repo_root, f'etf_holdings_{today_8}.csv')

    if os.path.exists(csv_out):
        # Step 3 — reuse branch. MUST NOT re-invoke subprocess (Req 14.5).
        print(f"\n🔌 Bridge: reusing existing canonical CSV at {csv_out}")
    else:
        # Step 4 — invoke v42 as an isolated subprocess.
        print("\n🔌 Running extended ETF scrapers (isolated subprocess)…")
        try:
            result = subprocess.run(
                [sys.executable, scraper_path],
                timeout=600,
                capture_output=True, text=True,
                cwd=repo_root,
            )
            if result.stdout:
                print(result.stdout[-3000:])
            if result.returncode != 0:
                print(f"  ⚠️  Extended scraper exited {result.returncode} — continuing anyway.")
                if result.stderr:
                    print(f"  STDERR: {result.stderr[-1000:]}")
            else:
                print("  Extended scrapers done.")
        except subprocess.TimeoutExpired as e:
            print("  ⚠️  Extended scraper timed out — continuing anyway.")
            if getattr(e, "stdout", None):
                tail = e.stdout if isinstance(e.stdout, str) else e.stdout.decode("utf-8", errors="replace")
                print(tail[-3000:])
        except Exception as e:
            print(f"  ⚠️  Could not run extended scraper: {e} — continuing anyway.")

    # Step 5 — verify CSV exists after subprocess.
    if not os.path.exists(csv_out):
        print("❌ No extended output")
        return

    # Step 6 — clean and validate.
    cleaned_df, errors = clean_canonical_csv(csv_out)
    if errors:
        for err in errors:
            print(f"⚠️  {err}")
        return

    # Step 7 — short-circuit on empty.
    if cleaned_df.empty:
        print("⚠️  Bridge: 0 valid rows")
        return

    # Step 8 — fan out to all three sinks.
    bridge_write_all_sinks(cleaned_df)


def main():
    try:
        with open(CONFIG_FILE, 'r') as f: etfs = json.load(f)
    except: return

    # Startup invariant: surface ownership collisions between config.json
    # (Primary_ETFs) and V42_ETFS. Warning-only — does not abort (Req 1.3).
    collisions = check_v42_ownership_collisions(etfs)
    if collisions:
        print(f"⚠️  V42 ownership collision summary: {collisions} appear in both config.json and V42_ETFS")

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
