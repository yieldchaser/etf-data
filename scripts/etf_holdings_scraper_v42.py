"""
ETF Holdings Scraper — v27 (Ironclad Programmatic Event Integration)
=====================================================================
Sources & methods (all confirmed via browser network recon):

  GRIN  → advisor.vcm.com  Playwright download (href always blob:)
  JHMM  → JH PDF  fund_id=2Y7Q
  JHEM  → JH PDF  fund_id=2Y7Z
  JHSC  → JH PDF  fund_id=2Y7Y
  VLUE  → BlackRock CSV API, rolling prev-biz-day date
  MFEM  → PIMCO  Playwright session → cookie+API → local file
  JOET  → Virtus direct XLS, dynamic header row detection
  AVSC  → Avantis Playwright
"""

import sys
try:
    sys.stdout.reconfigure(errors='replace')
    sys.stderr.reconfigure(errors='replace')
except AttributeError:
    pass

import requests, pandas as pd, io, re, warnings, traceback, os
from datetime import date, timedelta
from typing import Optional
from bs4 import BeautifulSoup
import pdfplumber

warnings.filterwarnings('ignore')

# ── Dates ─────────────────────────────────────────────────────────────────────
def prev_biz_day(d=None, n=1):
    d = d or date.today()
    for _ in range(n):
        d -= timedelta(days=1)
        while d.weekday() >= 5:
            d -= timedelta(days=1)
    return d

# Module-level holiday cache (audit LOW): _clamp_to_prev_biz_day used to
# construct a USFederalHolidayCalendar on every call — O(rows) waste when
# applied per-row over thousands of holdings.
_HOLIDAYS_CACHE: frozenset | None = None


def _clamp_to_prev_biz_day(date_str: str) -> str:
    """
    Clamp a date string to the most recent US business day <= today.
    Rolls back through weekends and US federal holidays so that
    Holdings_As_Of never lands on a Saturday, Sunday, or market holiday.
    """
    global _HOLIDAYS_CACHE
    try:
        d = pd.Timestamp(date_str).date()
    except Exception:
        return date_str
    today = date.today()
    if d > today:
        d = today
    if _HOLIDAYS_CACHE is None:
        from pandas.tseries.holiday import USFederalHolidayCalendar
        _HOLIDAYS_CACHE = frozenset(
            USFederalHolidayCalendar()
            .holidays(start='2020-01-01', end='2035-12-31')
            .strftime('%Y-%m-%d')
            .tolist()
        )
    _holidays = _HOLIDAYS_CACHE
    while d.weekday() >= 5 or d.strftime('%Y-%m-%d') in _holidays:
        d -= timedelta(days=1)
    return d.strftime('%Y-%m-%d')

TODAY   = date.today()
TODAY_8 = TODAY.strftime('%Y%m%d')
print(f"Run date: {TODAY_8}  |  Prev biz day: {prev_biz_day().strftime('%Y%m%d')}")

# ── Shared session ─────────────────────────────────────────────────────────────
S = requests.Session()
S.headers.update({
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
    ),
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
})



# ═══════════════════════════════════════════════════════════════════════════════
# CANONICAL OUTPUT SCHEMA
# ═══════════════════════════════════════════════════════════════════════════════
# Every ETF is normalised to this column order before CSV export.
# Source-specific extras are appended after the canonical columns.
_CANON_COLS = [
    'etf', 'as_of_date', 'scrape_date',
    'name', 'ticker', 'cusip', 'sedol', 'isin',
    'weight_pct', 'shares', 'market_value', 'price',
    'security_type', 'country', 'exchange', 'currency',
]

# Maps every known raw column name → canonical name.
# Keys are case-sensitive; add variants as encountered.
_COL_MAP: dict[str, str] = {
    # name
    'Name': 'name',         'name': 'name',           'Security Name': 'name',
    'Description': 'name',  'NAME': 'name',            'Security': 'name',
    'HoldingName': 'name',  'securityName': 'name',    'holding_name': 'name',
    'Company': 'name',      'COMPANY': 'name',         'company': 'name',
    'Holding': 'name',      'Holding Name': 'name',    'Issuer': 'name',
    'Company Name': 'name', 'Security Description': 'name',
    # ticker
    'Ticker': 'ticker',     'ticker': 'ticker',        'TICKER': 'ticker',
    'Symbol': 'ticker',     'symbol': 'ticker',
    # identifiers
    'CUSIP': 'cusip',       'Cusip': 'cusip',          'cusip': 'cusip',
    'Security Id': 'cusip',                             # Virtus/JOET
    'SEDOL': 'sedol',       'Sedol': 'sedol',          'sedol': 'sedol',
    'ISIN': 'isin',         'Isin': 'isin',            'isin': 'isin',
    # weight / % of portfolio
    'Weight (%)': 'weight_pct',          'PCT_OF_MV': 'weight_pct',
    '% of Net Assets': 'weight_pct',     '% of assets': 'weight_pct',
    'Weight': 'weight_pct',              'weight': 'weight_pct',
    'Weightings': 'weight_pct',          '% of Portfolio': 'weight_pct',
    'weight_pct': 'weight_pct',          'portfolioWeight': 'weight_pct',
    'Portfolio Weight (%)': 'weight_pct',
    # shares
    'Shares': 'shares',     'SHARES': 'shares',        'Shares Held': 'shares',
    'Quantity': 'shares',   'shares': 'shares',        'sharesHeld': 'shares',
    # market value
    'Market Value ($)': 'market_value',  'MKT_VAL': 'market_value',
    'Market Value': 'market_value',      'Value': 'market_value',
    'market_value': 'market_value',      'marketValue': 'market_value',
    # price
    'Price': 'price',       'price': 'price',
    # security type
    'Asset Class': 'security_type',      'Security Type': 'security_type',
    'Type': 'security_type',             'security_type': 'security_type',
    'assetClass': 'security_type',       'AssetClass': 'security_type',
    # country / exchange / currency
    'Location': 'country',  'Country': 'country',      'country': 'country',
    'Exchange': 'exchange',  'exchange': 'exchange',
    'Currency': 'currency',  'currency': 'currency',
    # as_of_date embedded in data (JH family)
    'AS_OF_DATE': 'as_of_date',
    # AVSC (Avantis CSV — exact headers from downloaded file)
    'COMPANY':                              'name',
    'SECURITY TYPE':                        'security_type',
    'SHARES/PRINCIPAL/ NOTIONAL AMOUNT':    'shares',
    'MARKET VALUE ($)':                     'market_value',
    'WEIGHT':                               'weight_pct',
    # GRIN correct-format CSV (#allholdingsCSVExport)
    # Columns: Date, Stock Symbol, ETF Name, ISIN, Holding, Security Type, Shares, Market Value, Portfolio %
    'Stock Symbol':  'ticker',   # Bloomberg ticker (e.g. "SGE LN", "8035 JP")
    'Holding':       'name',     # Company / holding name
    'Portfolio %':   'weight_pct',
}


def _canonicalize(df: pd.DataFrame, etf: str, as_of_date: str = '') -> pd.DataFrame:
    """
    Normalise a raw holdings DataFrame to the universal schema.

    Rules
    ─────
    • Known raw column names are renamed via _COL_MAP.
    • Duplicate canonical targets (two raw cols both rename to 'weight_pct')
      keep only the first occurrence.
    • 'etf' and 'scrape_date' are injected/overwritten unconditionally.
    • 'as_of_date':
        – If the df already has the column (JH's AS_OF_DATE, VLUE's injected date,
          MFEM's injected date), it is normalised to YYYY-MM-DD and kept.
        – Otherwise the caller-supplied `as_of_date` string is used.
    • All _CANON_COLS that are still absent are added as pd.NA.
    • Column order: canonical cols first, then any ETF-specific extras.
    """
    df = df.copy()

    # Step 1 — rename
    rename_map = {c: _COL_MAP[c] for c in df.columns if c in _COL_MAP}
    df.rename(columns=rename_map, inplace=True)

    # Step 2 — deduplicate (keep first if two raw cols map to same canonical name)
    df = df.loc[:, ~df.columns.duplicated(keep='first')]

    # Step 3 — etf + scrape_date
    df['etf']        = etf
    df['scrape_date'] = TODAY_8

    # Step 4 — as_of_date
    if 'as_of_date' in df.columns and df['as_of_date'].notna().any():
        # Normalise 8-digit YYYYMMDD (JH) → YYYY-MM-DD
        def _norm(v):
            v = str(v).strip()
            return f'{v[:4]}-{v[4:6]}-{v[6:]}' if re.match(r'^\d{8}$', v) else v
        df['as_of_date'] = df['as_of_date'].apply(_norm)
    else:
        df['as_of_date'] = as_of_date
    # Clamp to most recent US business day <= today (catches weekends & holidays)
    df['as_of_date'] = df['as_of_date'].apply(_clamp_to_prev_biz_day)

    # Step 5 — fill missing canonical cols
    for col in _CANON_COLS:
        if col not in df.columns:
            df[col] = pd.NA

    # Step 6 — reorder
    extras = [c for c in df.columns if c not in _CANON_COLS]
    return df[_CANON_COLS + extras]


# ══════════════════════════════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════════════════════════════
def fetch_grin() -> pd.DataFrame:
    page_url = (
        'https://advisor.vcm.com/products/victoryshares-etfs/'
        'victoryshares-etfs-list/victoryshares-international-free-cash-flow-growth-etf'
    )
    r = S.get(page_url, headers={'Accept': 'text/html,*/*',
                                  'Referer': 'https://advisor.vcm.com/'}, timeout=30)
    print(f"  GRIN page: {r.status_code} | {len(r.content)//1024} KB")
    r.raise_for_status()
    print("  GRIN: using Playwright download capture...")
    return _grin_playwright(page_url)


def _launch_playwright_browser(p):
    args = [
        '--no-sandbox',
        '--disable-blink-features=AutomationControlled',
        '--disable-dev-shm-usage',
    ]
    is_ci = os.environ.get('CI') is not None or os.environ.get('GITHUB_ACTIONS') is not None
    headless = True if is_ci else False

    try:
        return p.chromium.launch(headless=headless, args=args)
    except Exception:
        try:
            return p.chromium.launch(channel='chrome', headless=headless, args=args)
        except Exception:
            return p.chromium.launch(headless=True, args=args)


def _grin_playwright(page_url):
    from playwright.sync_api import sync_playwright
    captured = {}
    with sync_playwright() as p:
        browser = _launch_playwright_browser(p)
        ctx  = browser.new_context(accept_downloads=True,
                                   viewport={'width': 1280, 'height': 900})
        page = ctx.new_page()
        page.add_init_script(
            "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
        )

        def on_response(resp):
            ct = resp.headers.get('content-type', '')
            # CSV / Excel binary
            if resp.status in (200, 201) and ('csv' in ct or 'excel' in ct or 'spreadsheet' in ct):
                try:
                    body = resp.body()
                    if len(body) > 500:
                        captured.update({'bytes': body, 'ct': ct})
                        print(f"  GRIN PW response: {len(body)} bytes")
                except Exception:
                    pass
            # JSON API — capture the MOST RECENT date seen across all API calls.
            # Multiple endpoints fire (Fees, Characteristics, TopHoldings, etc.)
            # each with different dates. We want the holdings date specifically
            # (TopHoldings / AllHoldings), which is always the most recent.
            # Use the same max-date logic as the DOM heuristic.
            if 'json' in ct and resp.status == 200:
                try:
                    data = resp.json()
                    def _find_date(obj, depth=0):
                        if depth > 3: return None
                        if isinstance(obj, dict):
                            for key in ('asOfDate','asofdate','as_of_date','date',
                                        'AsOfDate','holdingsDate','tradeDate'):
                                val = obj.get(key)
                                if val and re.match(r'\d{4}-\d{2}-\d{2}|\d{1,2}/\d{1,2}/\d{4}', str(val)):
                                    return str(val)
                            for v in obj.values():
                                r2 = _find_date(v, depth+1)
                                if r2: return r2
                        elif isinstance(obj, list) and obj:
                            return _find_date(obj[0], depth+1)
                        return None
                    found = _find_date(data)
                    if found:
                        m2 = re.match(r'(\d{1,2})/(\d{1,2})/(\d{4})', found)
                        if m2:
                            found = f"{m2.group(3)}-{m2.group(1).zfill(2)}-{m2.group(2).zfill(2)}"
                        # Keep the most recent date seen — not last-written.
                        # TopHoldings/AllHoldings (05/22) must beat Characteristics (04/30).
                        existing = captured.get('grin_api_aod', '0000-00-00')
                        if found > existing:
                            captured['grin_api_aod'] = found
                            print(f"  GRIN PW: date from API JSON = {found} ← {resp.url[:70]}")
                except Exception:
                    pass

        page.on('response', on_response)
        page.goto(page_url, timeout=90000, wait_until='domcontentloaded')
        page.wait_for_timeout(4000)

        # ── Wait for the Portfolio section to fully render ────────────────────
        # The blob attached to #allholdingsCSVExport is generated lazily from
        # the page's JS state.  If we click before the Portfolio section's API
        # call completes, the blob has no Date column (stale/partial data).
        # Waiting for "Top 10 Holdings" in the DOM means the holdings data is
        # loaded and the blob will contain the correct Date="MM/DD/YYYY" rows.
        try:
            page.wait_for_selector(
                'text="Top 10 Holdings"', timeout=15000
            )
            page.wait_for_timeout(1500)   # give the blob time to update
            print("  GRIN PW: Portfolio section loaded — blob data ready")
        except Exception:
            print("  GRIN PW: ⚠ Portfolio section wait timed out — proceeding anyway")

        # ── Download the holdings CSV ─────────────────────────────────────────
        if not captured.get('bytes'):
            for sel in [
                '#allholdingsCSVExport',
                'a:has-text("All Holdings")',
                '[download*="Holdings"]',
            ]:
                try:
                    with page.expect_download(timeout=15000) as dl_info:
                        page.locator(sel).first.click()
                    dl = dl_info.value
                    captured.update({'bytes': open(dl.path(), 'rb').read(), 'ct': 'text/csv'})
                    print(f"  GRIN PW download: {len(captured['bytes'])} bytes via {sel}")
                    break
                except Exception as e:
                    print(f"  GRIN PW {sel}: {e!s:.80}")

        # ── Extract date from the Portfolio section heading ────────────────────
        # "Top 10 Holdings (As of 05/22/2026)" — most specific date source.
        # Only falls back to the page-wide latest-date heuristic if not found.
        grin_dom_aod = ''
        try:
            # Try the specific Portfolio heading first
            for sel in [
                'text=/Top 10 Holdings.*As of/',
                '*:has-text("Top 10 Holdings")',
            ]:
                try:
                    loc = page.locator(sel).first
                    txt = loc.inner_text(timeout=3000)
                    m   = re.search(r'As of\s+(\d{1,2}/\d{1,2}/\d{4})', txt, re.I)
                    if m:
                        p_d = m.group(1).split('/')
                        grin_dom_aod = f"{p_d[2]}-{p_d[0].zfill(2)}-{p_d[1].zfill(2)}"
                        print(f"  GRIN PW: date from Portfolio heading = {grin_dom_aod}")
                        break
                except Exception:
                    pass

            # Fallback: latest-date heuristic across whole page
            if not grin_dom_aod:
                grin_dom_aod = page.evaluate("""
                    () => {
                        const els = [...document.querySelectorAll('p,span,div,small,em,li')];
                        const hits = [];
                        for (const el of els) {
                            if (el.children.length > 0) continue;
                            const m = el.textContent.match(
                                /As of\\s+(\\d{1,2}\\/\\d{1,2}\\/\\d{4})/i
                            );
                            if (m) hits.push(m[1]);
                        }
                        if (!hits.length) return '';
                        const toNum = s => {
                            const [mo,d,y] = s.split('/');
                            return parseInt(y)*10000+parseInt(mo)*100+parseInt(d);
                        };
                        hits.sort((a,b) => toNum(b)-toNum(a));
                        const [mo,d,y] = hits[0].split('/');
                        return y+'-'+mo.padStart(2,'0')+'-'+d.padStart(2,'0');
                    }
                """)
                if grin_dom_aod:
                    print(f"  GRIN PW: date from page heuristic = {grin_dom_aod}")
        except Exception as e:
            print(f"  GRIN PW: DOM date error: {e!s:.60}")
        if not grin_dom_aod:
            print("  GRIN PW: ⚠ DOM date not found")

        browser.close()
    captured['dom_aod'] = grin_dom_aod
    # API JSON date takes priority over DOM scrape (more reliable)
    if captured.get('grin_api_aod'):
        captured['dom_aod'] = captured['grin_api_aod']

    if not captured.get('bytes'):
        raise RuntimeError("GRIN: could not capture download")
    raw, ct = captured['bytes'], captured.get('ct', '')
    df = (pd.read_csv(io.StringIO(raw.decode('utf-8', errors='replace')))
          if 'csv' in ct or raw[:2] != b'PK'
          else pd.read_excel(io.BytesIO(raw)))
    df.insert(0, 'etf', 'GRIN')

    # Prefer the DOM date (scraped from Portfolio tab) as the authoritative
    # as_of_date.  Only fall back to the file's "Date" column if the DOM
    # scrape failed AND the column actually contains date-formatted values.
    _DATE_RE = re.compile(r'^\d{1,2}[/\-]\d{1,2}[/\-]\d{4}$|^\d{4}-\d{2}-\d{2}$')
    grin_aod = captured.get('dom_aod', '')

    if 'Date' in df.columns:
        first_val = str(df['Date'].dropna().iloc[0]).strip() if not df['Date'].dropna().empty else ''
        if _DATE_RE.match(first_val):
            # Date column has real dates — extract and drop
            if not grin_aod:
                parts = first_val.replace('-', '/').split('/')
                if len(parts[2]) == 4:
                    grin_aod = f"{parts[2]}-{parts[0].zfill(2)}-{parts[1].zfill(2)}"
                else:
                    grin_aod = first_val
                print(f"  GRIN: as_of_date from Date column = {grin_aod}")
            df.drop(columns=['Date'], inplace=True)
        else:
            # The "All Holdings" CSV is missing the Date column; every column
            # is shifted one position left.  Apply the correct semantic names.
            #
            # Original header → actual content
            # Date            → Bloomberg ticker (e.g. "SGE LN", "8035 JP")
            # Stock Symbol    → ETF display name
            # ETF Name        → ISIN
            # ISIN            → Company / holding name
            # Holding         → Security type
            # Security Type   → Shares held
            # Shares          → Market value
            # Market Value    → Portfolio weight
            # Portfolio %     → (empty / NaN)
            shift_rename = {
                'Date':           'ticker',
                'ETF Name':       'isin',
                'ISIN':           'name',
                'Holding':        'security_type',
                'Security Type':  'shares',
                'Shares':         'market_value',
                'Market Value':   'weight_pct',
            }
            df.rename(columns={k: v for k, v in shift_rename.items() if k in df.columns},
                      inplace=True)
            # Drop the now-redundant shifted columns we can't reliably use
            for drop_col in ['Stock Symbol', 'Portfolio %']:
                if drop_col in df.columns:
                    df.drop(columns=[drop_col], inplace=True)
            print(f"  GRIN: corrected shifted CSV column headers")

    if grin_aod:
        df['as_of_date'] = grin_aod
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# 2. JH Family — PDF parse
# ═══════════════════════════════════════════════════════════════════════════════
FUND_MAP = {'2Y7Q': 'JHMM', '2Y7Z': 'JHEM', '2Y7Y': 'JHSC'}

def parse_jh_row(row_str: str) -> Optional[dict]:
    s = re.sub(r"\s*\n.*", "", row_str).strip()
    if len(s) < 20:
        return None
    fid = s[:4].strip()
    if fid not in FUND_MAP:
        return None

    m_ids = re.search(
        r'\s(?=[A-Z0-9]*\d)([A-Z0-9]{8,10})\s+(?=[A-Z0-9]*\d)([A-Z0-9]{6,8})\s+', s
    )
    if not m_ids:
        return None

    m_nums = re.search(
        r'([\d,]+\.?\d*)\s+([\d,]+\.?\d*)\s+([\d\.]+)\s+(\d{8})$', s
    )
    if not m_nums:
        return None

    prefix = s[:m_ids.start()].split()
    ticker = prefix[-1] if prefix else ''
    name   = s[m_ids.end():m_nums.start()].strip()

    return {
        'FUND_ID':    fid,
        'TICKER':     ticker,
        'CUSIP':      m_ids.group(1),
        'SEDOL':      m_ids.group(2),
        'NAME':       name,
        'SHARES':     float(m_nums.group(1).replace(',', '') or 0),
        'MKT_VAL':    float(m_nums.group(2).replace(',', '') or 0),
        'PCT_OF_MV':  float(m_nums.group(3) or 0),
        'AS_OF_DATE': m_nums.group(4),
    }


def fetch_jh_family() -> dict:
    S.get('https://www.jhinvestments.com/etfs', headers={
        'Accept': 'text/html,*/*', 'Referer': 'https://www.google.com/',
        'Sec-Fetch-Mode': 'navigate',
    }, timeout=30)

    pdf_url = (
        'https://www.jhinvestments.com/content/dam/jhi-investments/JHINV/public/ETFs/'
        'Documents/TradeDateHoldingFiles/Trade_Date_Holdings.pdf'
    )
    r = S.get(pdf_url, headers={
        'Accept': 'application/pdf,*/*',
        'Referer': 'https://www.jhinvestments.com/etfs',
        'Sec-Fetch-Mode': 'navigate', 'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Site': 'same-origin',
    }, timeout=120)
    print(f"  JH PDF: {r.status_code} | {len(r.content)//1024} KB")
    r.raise_for_status()

    frames      = {v: [] for v in FUND_MAP.values()}
    total       = 0
    skipped     = 0

    with pdfplumber.open(io.BytesIO(r.content)) as pdf:
        print(f"  JH PDF: {len(pdf.pages)} pages")
        for pg in pdf.pages:
            for tbl in (pg.extract_tables() or []):
                for row in tbl:
                    if not row:
                        continue
                    cell = ' '.join(str(c or '').strip() for c in row if c).strip()
                    parsed = parse_jh_row(cell)
                    if parsed:
                        frames[FUND_MAP[parsed['FUND_ID']]].append(parsed)
                        total += 1
                    elif cell[:4] in FUND_MAP:
                        skipped += 1

    print(f"  JH: parsed {total} rows | failed parse: {skipped}")
    result = {}
    for t, rows in frames.items():
        if rows:
            df = pd.DataFrame(rows)
            df.insert(0, 'etf', t)
            result[t] = df
        else:
            result[t] = pd.DataFrame()
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# 3. VLUE — BlackRock CSV API
# ═══════════════════════════════════════════════════════════════════════════════
def fetch_vlue() -> pd.DataFrame:
    for n in range(1, 8):
        d    = prev_biz_day(TODAY, n)
        dstr = d.strftime('%Y%m%d')
        url  = (
            'https://www.blackrock.com/varnish-api/blk-one01-product-data/product-data/api/v1/'
            f'get-fund-document?appType=PRODUCT_PAGE&appSubType=ISHARES&targetSite=us-ishares'
            f'&locale=en_US&portfolioId=251616&userType=individual&asOfDate={dstr}&component=holdings'
        )
        r = S.get(url, headers={
            'Accept': '*/*',
            'Referer': 'https://www.blackrock.com/',
            'Origin':  'https://www.blackrock.com',
        }, timeout=30)
        print(f"  VLUE: {dstr} -> {r.status_code} | {len(r.content)} bytes")
        if r.status_code != 200 or len(r.content) < 5000:
            continue
        lines = r.text.splitlines()
        hidx  = next((i for i, ln in enumerate(lines) if ln.strip().startswith('Ticker,')), None)
        if hidx is None:
            continue
        df = pd.read_csv(io.StringIO(r.text), skiprows=hidx)
        df = df[df['Ticker'].notna() & df['Ticker'].str.match(r'^[A-Z0-9\-\.]{1,12}$', na=False)]
        if len(df) > 0:
            df.insert(0, 'etf', 'VLUE')
            # Parse the exact holdings date from the BlackRock CSV metadata.
            # The format varies slightly across fund pages; we try several
            # known field names and fall back to a regex scan for any line
            # that contains a spelled-out month date.
            aod = ''
            for meta_line in lines[:hidx]:
                # Known field names (case-insensitive)
                if re.search(r'fund holdings date|holdings date|as of date|as-of date|asofdate', meta_line, re.I):
                    parts = meta_line.split(',', 1)
                    if len(parts) == 2:
                        raw_d = parts[1].strip().strip('"')
                        try:
                            import datetime
                            for fmt in ('%B %d, %Y', '%b %d, %Y', '%m/%d/%Y', '%Y-%m-%d'):
                                try:
                                    aod = datetime.datetime.strptime(raw_d, fmt).strftime('%Y-%m-%d')
                                    break
                                except ValueError:
                                    pass
                        except Exception:
                            aod = raw_d
                    if aod:
                        break
            # Fallback: scan every metadata line for "Month DD, YYYY" pattern
            if not aod:
                for meta_line in lines[:hidx]:
                    m2 = re.search(r'([A-Za-z]+ \d{1,2},? \d{4})', meta_line)
                    if m2:
                        raw_d = m2.group(1)
                        try:
                            import datetime
                            for fmt in ('%B %d, %Y', '%B %d %Y', '%b %d, %Y'):
                                try:
                                    aod = datetime.datetime.strptime(raw_d, fmt).strftime('%Y-%m-%d')
                                    break
                                except ValueError:
                                    pass
                        except Exception:
                            pass
                        if aod:
                            print(f"  VLUE: as_of_date from metadata regex = {aod}  (raw: '{meta_line.strip()}')")
                            break
            if not aod:
                aod = d.strftime('%Y-%m-%d')
                print(f"  VLUE: ⚠ metadata date not found — using loop date {aod}")
            df['as_of_date'] = aod
            print(f"  VLUE: ✅ {len(df)} rows | as_of_date={aod}")
            return df
    raise RuntimeError("VLUE: no data found for recent dates")


# ═══════════════════════════════════════════════════════════════════════════════
# 4. MFEM — PIMCO
# ═══════════════════════════════════════════════════════════════════════════════
MFEM_PAGE_URL = (
    'https://www.pimco.com/us/en/investments/etf/'
    'pimco-rafi-dynamic-multi-factor-emerging-markets-equity-etf/usetf-usd'
)

def fetch_mfem() -> pd.DataFrame:
    """
    PIMCO MFEM holdings — four-tier waterfall:
      Tier 0  Direct API (no session) — fund-detail-api with public headers only.
              No Playwright, no modal interaction.  Works if PIMCO serves the
              XLSX with Client/Username headers alone.  Tried first, fast.
      Tier 1  Playwright: real mouse events to satisfy React 17+ synthetic event system,
              then response intercept + download capture for the XLSX.
      Tier 2  Cookie API: replay session cookies against fund-detail-api.
      Tier 3  Local file fallback.
    """
    from playwright.sync_api import sync_playwright
    import glob

    # ── Tier 0: Direct API call — no Playwright, no modal ─────────────────────
    # fund-detail-api may return the XLSX with just the required custom headers
    # (Client, Username, etc.) without a JSESSIONID cookie.  If this works we
    # skip the modal entirely — much faster and more reliable for daily runs.
    _api_hdrs_public = {
        'Accept':      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*',
        'Client':      'WEB',
        'Countrycode': 'US',
        'langcode':    'en',
        'userrole':    'FI',
        'Origin':      'https://www.pimco.com',
        'Referer':     'https://www.pimco.com/',
        'User-Agent':  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    }
    print("  MFEM: trying direct API (no session)...")
    for n in range(1, 10):
        d    = prev_biz_day(TODAY, n)
        dstr = d.strftime('%Y-%m-%d')
        for endpoint in ['allHoldings', 'topTenHoldings']:
            url = (
                f'https://fund-ui.pimco.com/fund-detail-api/api/funds/72202L389/'
                f'{endpoint}/export?asOfDate={dstr}'
            )
            try:
                r = S.get(url, headers=_api_hdrs_public, timeout=30)
                print(f"  MFEM T0 ({endpoint}) {dstr} -> {r.status_code} | {len(r.content):,} B")
                if r.status_code in (200, 201) and r.content[:4] == b'PK\x03\x04':
                    df, a1_aod = _parse_mfem_excel(r.content)
                    df['as_of_date'] = a1_aod or dstr
                    print(f"  MFEM: ✅ {len(df)} rows via direct API | as_of_date={df['as_of_date'].iloc[0]}")
                    return df
            except Exception as e:
                print(f"  MFEM T0 error: {e!s:.60}")
    print("  MFEM: direct API didn't yield data — falling back to Playwright modal...")

    captured:       dict = {}
    all_cookie_str: str  = ''
    jsession:       Optional[str] = None

    # ── helpers ────────────────────────────────────────────────────────────────

    def _fire_real_click(page, locator, label: str) -> bool:
        """
        Try three escalating strategies, each firing real browser mouse events:
          1. Bounding-box → page.mouse.click()      (most trusted)
          2. locator.click()                        (standard Playwright)
          3. locator.click(force=True)              (bypasses pointer-events:none)
        Returns True on first success.
        """
        # Strategy 1 — absolute coordinates via page.mouse
        try:
            locator.scroll_into_view_if_needed(timeout=3000)
            box = locator.bounding_box(timeout=3000)
            if box:
                cx, cy = box['x'] + box['width'] / 2, box['y'] + box['height'] / 2
                page.mouse.move(cx, cy)
                page.wait_for_timeout(120)
                page.mouse.click(cx, cy)
                print(f"  MFEM PW: mouse.click({cx:.0f},{cy:.0f}) -> {label}")
                return True
        except Exception as e:
            print(f"  MFEM PW: bbox click failed for {label}: {e!s:.70}")

        # Strategy 2 — standard Playwright click
        try:
            locator.click(timeout=4000)
            print(f"  MFEM PW: locator.click() -> {label}")
            return True
        except Exception as e:
            print(f"  MFEM PW: locator.click failed for {label}: {e!s:.70}")

        # Strategy 3 — force click (overrides pointer-events:none on hidden inputs)
        try:
            locator.click(force=True, timeout=4000)
            print(f"  MFEM PW: force-click -> {label}")
            return True
        except Exception as e:
            print(f"  MFEM PW: force-click failed for {label}: {e!s:.70}")

        return False

    # ── Playwright tier ─────────────────────────────────────────────────────────

    with sync_playwright() as p:
        browser = _launch_playwright_browser(p)
        ctx = browser.new_context(
            user_agent=(
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/124.0.0.0 Safari/537.36'
            ),
            accept_downloads=True,
            viewport={'width': 1280, 'height': 900},
        )
        page = ctx.new_page()
        page.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )

        def on_response(resp):
            if captured:
                return
            url = resp.url
            # Only match the specific holdings-export paths on fund-ui.pimco.com.
            # The broad domain-only check (v28) caused distribution-times — a small
            # XLSX also served from fund-ui.pimco.com — to be captured first.
            # Require BOTH the correct subdomain AND the specific endpoint path.
            is_holdings_export = (
                'fund-ui.pimco.com' in url
                and 'export' in url
                and ('allHoldings' in url or 'topTenHoldings' in url)
            )
            if resp.status in (200, 201) and is_holdings_export:
                try:
                    body = resp.body()
                    if len(body) > 3000 and body[:4] == b'PK\x03\x04':
                        # Parse asOfDate=YYYY-MM-DD from the URL query string
                        m = re.search(r'asOfDate=(\d{4}-\d{2}-\d{2})', url)
                        captured['bytes']      = body
                        captured['as_of_date'] = m.group(1) if m else ''
                        print(f"  MFEM PW: intercepted XLSX {len(body):,} bytes ← {url[:80]}")
                except Exception:
                    pass

        page.on('response', on_response)

        print("  MFEM: loading page...")
        page.goto(MFEM_PAGE_URL, timeout=90000, wait_until='domcontentloaded')
        # Ensure the browser window has focus before any clicks.
        # After GRIN's browser closes, the new window can open in background
        # on Windows, causing click events to misfire silently.
        page.bring_to_front()
        page.wait_for_timeout(500)

        # ── Capture JSESSIONID immediately after page load ─────────────────────
        # The server sets JSESSIONID in the Set-Cookie header of the initial
        # page response — before ANY modal interaction.  We can use it immediately
        # to call the fund-detail-api, completely bypassing the modal.
        all_cookies    = ctx.cookies()
        jsession       = next((c['value'] for c in all_cookies if c['name'] == 'JSESSIONID'), None)
        all_cookie_str = '; '.join(f"{c['name']}={c['value']}" for c in all_cookies)
        if jsession:
            print(f"  MFEM PW: JSESSIONID acquired on page load ({jsession[:8]}...) — trying API directly...")
            api_hdrs = {
                'Accept':      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*',
                'Client':      'WEB',
                'Countrycode': 'US',
                'Language':    'en',
                'Username':    'FL',
                'Origin':      'https://www.pimco.com',
                'Referer':     MFEM_PAGE_URL,
                'Cookie':      all_cookie_str,
            }
            for n in range(1, 10):
                d    = prev_biz_day(TODAY, n)
                dstr = d.strftime('%Y-%m-%d')
                for endpoint in ['allHoldings', 'topTenHoldings']:
                    url = (
                        f'https://fund-ui.pimco.com/fund-detail-api/api/funds/72202L389/'
                        f'{endpoint}/export?asOfDate={dstr}'
                    )
                    try:
                        r = S.get(url, headers=api_hdrs, timeout=30)
                        print(f"  MFEM early-API ({endpoint}) {dstr} -> {r.status_code} | {len(r.content):,} B")
                        if r.status_code in (200, 201) and r.content[:4] == b'PK\x03\x04':
                            browser.close()
                            df, a1_aod = _parse_mfem_excel(r.content)
                            df['as_of_date'] = a1_aod or dstr
                            print(f"  MFEM: ✅ {len(df)} rows via early session API | as_of_date={df['as_of_date'].iloc[0]}")
                            return df
                    except Exception as e:
                        print(f"  MFEM early-API error: {e!s:.60}")
            print("  MFEM: early session API returned no data — proceeding with modal flow...")
        else:
            print("  MFEM: no JSESSIONID on page load — proceeding with modal flow...")

        # ── Step 1: Dismiss the gating modal ───────────────────────────────────
        try:
            page.wait_for_selector('text=Financial Advisor', timeout=15000)
            print("  MFEM PW: modal detected")

            # --- 1a. Select investor type card ---
            # Selector priority:
            #   1. label:has-text — the semantic radio target; React onClick is on the label
            #   2. li:has-text with radio — wider target but may miss the label handler
            #   3. class-based / generic fallbacks
            # On each retry, cycle to the NEXT selector so we don't keep
            # hitting the same element that already fired without effect.
            card_selectors = [
                'label:has-text("Financial Advisor")',                           # semantic radio label — primary
                'li:has-text("Financial Advisor"):has(input[type="radio"])',
                'li:has-text("Financial Advisor")',
                '[class*="card"]:has-text("Financial Advisor")',
            ]
            # Fallback investor types if Financial Advisor card fails completely
            alt_card_selectors = [
                'label:has-text("Individual Investor")',
                'li:has-text("Individual Investor"):has(input[type="radio"])',
                'label:has-text("Institutional Investor")',
                'li:has-text("Institutional Investor"):has(input[type="radio"])',
            ]

            card_clicked = False
            sel_index = 0   # cycle through selectors on each retry
            for sel in card_selectors:
                loc = page.locator(sel).first
                try:
                    if loc.count() == 0:
                        continue
                except Exception:
                    continue
                if _fire_real_click(page, loc, f"FA card ({sel})"):
                    card_clicked = True
                    break

            if not card_clicked:
                # Last-resort: fire a full trusted MouseEvent chain from within JS
                # using the hidden radio input — React's root listener will see it.
                print("  MFEM PW: falling back to MouseEvent chain on radio input...")
                page.evaluate("""
                    () => {
                        const fire = (el) => {
                            ['pointerover','pointerenter','mouseover','mouseenter',
                             'pointermove','mousemove',
                             'pointerdown','mousedown','pointerup','mouseup','click'
                            ].forEach(t => el.dispatchEvent(
                                new MouseEvent(t, {bubbles:true, cancelable:true,
                                                   view:window, buttons:1})
                            ));
                        };
                        // Prefer the hidden radio so React's onChange fires
                        for (const r of document.querySelectorAll('input[type="radio"]')) {
                            const c = r.closest('li') || r.closest('label') || r.parentElement;
                            if (c && c.textContent.includes('Financial Advisor')) {
                                r.checked = true;
                                fire(r);
                                fire(c);
                                return;
                            }
                        }
                        // Fallback: click the visible label text node's parent
                        for (const l of document.querySelectorAll('label,li,div')) {
                            if (l.childNodes.length &&
                                [...l.childNodes].some(n =>
                                    n.textContent && n.textContent.trim() === 'Financial Advisor')) {
                                fire(l);
                                return;
                            }
                        }
                    }
                """)

            # React needs a tick to reconcile; give it time to expand the modal
            page.wait_for_timeout(2500)

            # --- 1b. Confirm T&C section appeared — retry the card click up to
            #         3 times if React doesn't register the first attempt.
            # Intermittent failure mode: mouse.click() fires but React's synthetic
            # event dispatcher silently drops it on that frame.  Retrying works
            # because the second click lands on a stable React fiber state.
            tc_appeared = False
            for tc_attempt in range(3):
                try:
                    page.wait_for_selector(
                        'label:has-text("I acknowledge"), input[type="checkbox"]',
                        timeout=5000
                    )
                    tc_appeared = True
                    print(f"  MFEM PW: T&C section expanded ✓ (attempt {tc_attempt+1})")
                    break
                except Exception:
                    if tc_attempt < 2:
                        # Cycle to the NEXT card selector — the previous one fired
                        # but React didn't process it (wrong sub-element hit).
                        sel_index = (sel_index + 1) % len(card_selectors)
                        next_sel = card_selectors[sel_index]
                        print(f"  MFEM PW: T&C not yet visible (attempt {tc_attempt+1}) — trying '{next_sel}'...")
                        loc = page.locator(next_sel).first
                        try:
                            if loc.count() > 0:
                                _fire_real_click(page, loc, f"FA card retry ({next_sel})")
                        except Exception:
                            pass
                        page.wait_for_timeout(2500)

            # If FA card failed entirely, try an alternative investor type
            if not tc_appeared:
                print("  MFEM PW: FA card exhausted — trying alternative investor types...")
                for alt_sel in alt_card_selectors:
                    try:
                        loc = page.locator(alt_sel).first
                        if loc.count() == 0:
                            continue
                        _fire_real_click(page, loc, f"alt card ({alt_sel})")
                        page.wait_for_timeout(2500)
                        page.wait_for_selector(
                            'label:has-text("I acknowledge"), input[type="checkbox"]',
                            timeout=5000
                        )
                        tc_appeared = True
                        print(f"  MFEM PW: T&C appeared via alt card '{alt_sel}' ✓")
                        break
                    except Exception:
                        pass

            if not tc_appeared:
                # All card attempts exhausted → force-click the radio input directly
                print("  MFEM PW: T&C not visible after all attempts — force-clicking radio input...")
                try:
                    radio = page.locator('input[type="radio"]').first
                    radio.click(force=True, timeout=4000)
                    page.wait_for_timeout(2000)
                    page.wait_for_selector(
                        'label:has-text("I acknowledge"), input[type="checkbox"]',
                        timeout=5000
                    )
                    tc_appeared = True
                    print("  MFEM PW: T&C appeared after radio force-click ✓")
                except Exception as re2:
                    print(f"  MFEM PW: radio force-click: {re2!s:.80}")

            # --- 1c. Tick the acknowledgment checkbox ---
            if tc_appeared:
                print("  MFEM PW: clicking acknowledgment checkbox...")
                cb_clicked = False

                # Try the label first — its `for` attribute wires it to the checkbox
                for cb_sel in [
                    'label:has-text("I acknowledge")',
                    'label:has-text("acknowledge")',
                    'input[type="checkbox"]',
                ]:
                    loc = page.locator(cb_sel).first
                    try:
                        if loc.count() == 0:
                            continue
                    except Exception:
                        continue
                    if _fire_real_click(page, loc, f"checkbox ({cb_sel})"):
                        cb_clicked = True
                        break

                if not cb_clicked:
                    # MouseEvent chain on the checkbox itself
                    page.evaluate("""
                        () => {
                            const cb = document.querySelector('input[type="checkbox"]');
                            if (!cb) return;
                            cb.checked = true;
                            ['pointerdown','mousedown','pointerup','mouseup','click'].forEach(t =>
                                cb.dispatchEvent(new MouseEvent(t, {
                                    bubbles:true, cancelable:true, view:window
                                }))
                            );
                            cb.dispatchEvent(new Event('change', {bubbles:true}));
                            cb.dispatchEvent(new Event('input',  {bubbles:true}));
                        }
                    """)
                    print("  MFEM PW: dispatched MouseEvent chain on checkbox (fallback)")

                page.wait_for_timeout(1500)
            else:
                print("  MFEM PW: ⚠ T&C never appeared — proceeding anyway")

            # --- 1d. Click Accept (only after React has naturally enabled it) ---
            print("  MFEM PW: clicking Accept button...")
            accept_loc = page.locator('button:has-text("Accept"), button[type="submit"]').first
            # Wait up to 4 s for React to enable it before trying force
            enabled = False
            try:
                accept_loc.wait_for(state='visible', timeout=4000)
                if accept_loc.is_enabled(timeout=2000):
                    enabled = True
            except Exception:
                pass

            if enabled:
                _fire_real_click(page, accept_loc, "Accept (enabled)")
            else:
                # React state may not have fully updated; force-click still fires
                # the event chain so the submit handler executes
                print("  MFEM PW: Accept not yet enabled — force-clicking...")
                _fire_real_click(page, accept_loc, "Accept (forced)")

            print("  MFEM PW: modal submission complete.")
            page.wait_for_timeout(5000)

        except Exception as e:
            print(f"  MFEM PW: compliance flow exception: {e!s:.120}")

        # ── Step 2: Wait for page to settle and intercept XLSX ─────────────────
        try:
            page.wait_for_load_state('networkidle', timeout=20000)
        except Exception:
            pass
        page.wait_for_timeout(3000)

        # ── Step 3: If still empty, click the "All Holdings" button ────────────
        # DevTools recon confirmed: "All Holdings ↓" button on the Portfolio
        # Composition section triggers topTenHoldings/export (despite the name,
        # PIMCO returns full holdings) → PimcoHoldingsReport_*.xlsx (~67 KB).
        # We use expect_download so Playwright captures even Content-Disposition:
        # attachment files that Chrome would otherwise save to disk silently.
        # Size gate of 10 KB filters out small ancillary XLSXs (e.g. the
        # distribution-times response that tripped up v28 at 5,311 bytes).
        if not captured:
            print("  MFEM PW: scanning for All Holdings download trigger...")
            dl_selectors = [
                'button:has-text("All Holdings")',   # primary — confirmed in DevTools
                'a:has-text("All Holdings")',
                'button:has-text("All Holding")',    # partial match safety net
                'a:has-text("All Holding")',
                'a[href*="allHoldings"]',
                'a[href*="topTenHoldings"]',
                'a[href*="export"]',
                'button:has-text("Download")',
                'a:has-text("Download")',
            ]
            for sel in dl_selectors:
                if captured:
                    break
                try:
                    loc = page.locator(sel).first
                    if not loc.is_visible(timeout=3000):
                        continue
                    print(f"  MFEM PW: clicking '{sel}'...")
                    try:
                        with page.expect_download(timeout=15000) as dl_info:
                            loc.click()
                        raw = open(dl_info.value.path(), 'rb').read()
                        if len(raw) > 10000 and raw[:4] == b'PK\x03\x04':
                            captured['bytes'] = raw
                            print(f"  MFEM PW: ✓ download via '{sel}': {len(raw):,} bytes")
                        else:
                            print(f"  MFEM PW: '{sel}' -> {len(raw):,} bytes -- too small, skipping")
                    except Exception:
                        if captured:  # might have landed in on_response
                            break
                except Exception:
                    pass

        # ── Harvest session cookies for Tier 2 ─────────────────────────────────
        all_cookies    = ctx.cookies()
        jsession       = next((c['value'] for c in all_cookies if c['name'] == 'JSESSIONID'), None)
        all_cookie_str = '; '.join(f"{c['name']}={c['value']}" for c in all_cookies)
        if jsession:
            print(f"  MFEM PW: JSESSIONID={jsession[:8]}...")

        browser.close()

    # ── Tier 1 result ───────────────────────────────────────────────────────────
    if captured:
        df, a1_aod = _parse_mfem_excel(captured['bytes'])
        # Prefer A1 date (declared by PIMCO in the file itself) over the URL param;
        # use URL param only if A1 parsing failed.
        df['as_of_date'] = a1_aod or captured.get('as_of_date', '')
        print(f"  MFEM: ✅ {len(df)} rows via Playwright intercept | as_of_date={df['as_of_date'].iloc[0]}")
        return df

    # ── Tier 2: Cookie API replay ───────────────────────────────────────────────
    if jsession:
        print("  MFEM: intercept missed — replaying session cookie against API...")
        api_hdrs = {
            'Accept':      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*',
            'Client':      'WEB',
            'Countrycode': 'US',
            'langcode':    'en',
            'userrole':    'FI',
            'Origin':      'https://www.pimco.com',
            'Referer':     'https://www.pimco.com/',
            'User-Agent':  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            'Cookie':      all_cookie_str,
        }
        for n in range(1, 10):
            d    = prev_biz_day(TODAY, n)
            dstr = d.strftime('%Y-%m-%d')
            for endpoint in ['allHoldings', 'topTenHoldings']:
                url = (
                    f'https://fund-ui.pimco.com/fund-detail-api/api/funds/72202L389/'
                    f'{endpoint}/export?asOfDate={dstr}'
                )
                r = S.get(url, headers=api_hdrs, timeout=30)
                print(f"  MFEM API ({endpoint}) {dstr} -> {r.status_code} | {len(r.content):,} bytes")
                if r.status_code in (200, 201) and r.content[:4] == b'PK\x03\x04':
                    df, a1_aod = _parse_mfem_excel(r.content)
                    df['as_of_date'] = a1_aod or dstr   # A1 > URL param
                    print(f"  MFEM: ✅ {len(df)} rows via cookie+API | as_of_date={df['as_of_date'].iloc[0]}")
                    return df

    # ── Tier 3: Local file fallback ─────────────────────────────────────────────
    candidates = (
        ['mfem_holdings.xlsx', 'MFEM_holdings.xlsx', f'PIMCO_ETF_MFEM_Holdings_{TODAY_8}.xlsx']
        + sorted(glob.glob('PIMCO_ETF_MFEM_Holdings_*.xlsx'), reverse=True)
    )
    for fname in candidates:
        if os.path.exists(fname):
            print(f"  MFEM: reading local file {fname}")
            df, a1_aod = _parse_mfem_excel(open(fname, 'rb').read())
            df['as_of_date'] = a1_aod  # local file: trust A1 only, no URL param fallback
            if not a1_aod:
                print(f"  MFEM: ⚠ local file has no parseable A1 date")
            print(f"  MFEM: ✅ {len(df)} rows from local file | as_of_date={a1_aod}")
            return df

    raise RuntimeError("MFEM: all tiers failed — Playwright, cookie API, and local file all missed.")


def _parse_mfem_excel(raw: bytes) -> tuple[pd.DataFrame, str]:
    """
    Parse the PIMCO holdings XLSX.
    Returns (df, as_of_date_str) where as_of_date_str is read directly from
    cell A1 ("AsOfDate: MM/DD/YYYY" or "AsOfDate: YYYY-MM-DD").
    Never infer the date — if A1 is unreadable, return '' so the caller
    can use the URL-param date instead.
    """
    wb_raw = pd.read_excel(io.BytesIO(raw), header=None, nrows=1, engine='openpyxl')
    a1 = str(wb_raw.iloc[0, 0]).strip() if not wb_raw.empty else ''
    mfem_aod = ''
    # Format: "AsOfDate: 05/21/2026" or "AsOfDate: 2026-05-21"
    m = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', a1)
    if m:
        parts = m.group(1).split('/')
        mfem_aod = f"{parts[2]}-{parts[0].zfill(2)}-{parts[1].zfill(2)}"
    else:
        m2 = re.search(r'(\d{4}-\d{2}-\d{2})', a1)
        if m2:
            mfem_aod = m2.group(1)
    if mfem_aod:
        print(f"  MFEM parse: as_of_date from A1 = {mfem_aod}  (raw: '{a1}')")
    else:
        print(f"  MFEM parse: ⚠ could not parse date from A1='{a1}'")

    df = pd.read_excel(io.BytesIO(raw), skiprows=1, engine='openpyxl')
    df.insert(0, 'etf', 'MFEM')
    df.dropna(how='all', inplace=True)
    return df, mfem_aod


# ═══════════════════════════════════════════════════════════════════════════════
# 5. JOET — Virtus direct XLS
# ═══════════════════════════════════════════════════════════════════════════════
def fetch_joet() -> pd.DataFrame:
    r = S.get('https://www.virtus.com/assets/files/3op/positions_joet.xls', timeout=30)
    r.raise_for_status()
    raw  = pd.read_excel(io.BytesIO(r.content), engine='xlrd', header=None)

    # Cell A1 contains "Positions as of M/D/YYYY" — extract the exact source date
    # before we skip past the metadata rows to reach the column headers.
    joet_aod = ''
    a1_val = str(raw.iloc[0, 0]).strip() if not raw.empty else ''
    m = re.search(r'as of\s+(\d{1,2}/\d{1,2}/\d{4})', a1_val, re.IGNORECASE)
    if m:
        parts = m.group(1).split('/')
        joet_aod = f"{parts[2]}-{parts[0].zfill(2)}-{parts[1].zfill(2)}"
        print(f"  JOET: as_of_date from A1 = {joet_aod}  (raw: '{a1_val}')")
    else:
        print(f"  JOET: ⚠ could not parse date from A1='{a1_val}'")

    hrow = next(
        (i for i, row in raw.iterrows()
         # Containment match (audit LOW): exact 'Ticker' equality silently
         # defaulted to header row 1 when the issuer renamed the column to
         # e.g. "Ticker Symbol", producing 0 bridged rows.
         if any('Ticker' in str(v) or 'Security Id' in str(v) for v in row.values)), 1
    )
    print(f"  JOET: header at row {hrow}")
    df = pd.read_excel(io.BytesIO(r.content), engine='xlrd', skiprows=hrow, header=0)
    df.insert(0, 'etf', 'JOET')
    if joet_aod:
        df['as_of_date'] = joet_aod
    df.dropna(how='all', inplace=True)
    if 'Ticker' in df.columns:
        df = df[df['Ticker'].notna() & (df['Ticker'].astype(str).str.strip() != '')]
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# 6. AVSC — Avantis Playwright
# ═══════════════════════════════════════════════════════════════════════════════
def fetch_avsc() -> pd.DataFrame:
    """
    Avantis U.S. Small Cap Equity ETF — three-tier waterfall:

    Tier 1  Adobe Analytics beacon intercept.
            When "All Holdings" is clicked, the site sends an Adobe Analytics
            POST to wa?medium=fetch with the COMPLETE CSV embedded in the
            `pev1` form field as a data URI (data:text/csv;charset=utf-8,...).
            We intercept this request via page.on('request', ...), decode pev1
            with urllib.parse.unquote(), and get all ~1,500 rows in one shot.
            The CSV metadata row "As of date,05/21/2026" is the authoritative
            holdings date and is parsed from the same intercepted payload.

    Tier 2  "View all" click + full DOM table scrape (no pagination needed
            after the button expands all rows).  Date from first "As of" text
            that's a sibling of the holdings table heading (not the stale
            Characteristics section date lower on the page).

    Tier 3  Static HTTP fallback.

    Why not blob download / expect_download?
            The "All Holdings" link generates the CSV client-side as a blob URL
            and triggers the download entirely in-browser.  There is no static
            server-side URL to GET and no network response to intercept.
            The analytics beacon is the only reliable extraction hook.
    """
    from playwright.sync_api import sync_playwright
    import urllib.parse, datetime

    holdings_url = 'https://www.avantisinvestors.com/avantis-investments/total-holdings/457/?type=etf'
    beacon_csv   = ''   # raw CSV text from analytics pev1
    all_rows: list = []
    avsc_aod  = ''

    with sync_playwright() as p:
        browser = _launch_playwright_browser(p)
        ctx     = browser.new_context(accept_downloads=True,
                                      viewport={'width': 1280, 'height': 900})
        page    = ctx.new_page()
        page.add_init_script(
            "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
        )

        # ── Tier 1 setup: intercept the Adobe Analytics POST ─────────────────
        # The beacon URL starts with "wa?" and the POST body contains
        # "pev1=data%3Atext%2Fcsv..." (URL-encoded "data:text/csv...").
        def on_request(req):
            nonlocal beacon_csv
            if beacon_csv:
                return
            if req.method != 'POST':
                return
            if 'wa?' not in req.url:
                return
            try:
                post_raw = req.post_data or ''
                if 'pev1=' not in post_raw:
                    return
                params   = urllib.parse.parse_qs(post_raw, keep_blank_values=True)
                pev1_val = params.get('pev1', [''])[0]
                # pev1 starts with "data:text/csv;charset=utf-8,"
                if 'text/csv' not in pev1_val and 'COMPANY' not in pev1_val:
                    return
                # Strip the data-URI prefix and URL-decode
                csv_encoded = pev1_val.split(',', 1)[-1] if ',' in pev1_val else pev1_val
                beacon_csv  = urllib.parse.unquote(csv_encoded)
                print(f"  AVSC: intercepted CSV from analytics beacon "
                      f"({len(beacon_csv):,} chars)")
            except Exception as e:
                print(f"  AVSC beacon parse error: {e!s:.60}")

        page.on('request', on_request)

        page.goto(holdings_url, timeout=60000, wait_until='networkidle')
        page.wait_for_timeout(3000)

        # ── Tier 1 trigger: click "All Holdings" span ─────────────────────────
        # The element is <span role="button" ...>All Holdings</span> — NOT an
        # <a> or <button> tag.  That's why every prior selector missed it.
        # Correct selector: span[role="button"]:has-text("All Holdings")
        if not beacon_csv:
            for dl_sel in [
                'span[role="button"]:has-text("All Holdings")',
                '[role="button"]:has-text("All Holdings")',
                'span.cursor-pointer:has-text("All Holdings")',
            ]:
                try:
                    loc = page.locator(dl_sel).first
                    loc.scroll_into_view_if_needed(timeout=5000)
                    loc.click(force=True, timeout=5000)
                    print(f"  AVSC: clicked '{dl_sel}'")
                    page.wait_for_timeout(3000)  # let beacon fire
                    if beacon_csv:
                        break
                except Exception as e:
                    print(f"  AVSC click {dl_sel}: {e!s:.60}")

        # ── Tier 2: "View all" expansion + full DOM table scrape ─────────────
        # Clicking "View all" from the holdings-count dropdown loads ALL rows
        # on a single page (pagination disappears). One scrape gets everything.
        if not beacon_csv:
            print("  AVSC: beacon miss — trying View all + DOM scrape")

            # Step 1: open the count dropdown ("50 holdings ∨")
            # Wait for it explicitly — it renders after JS hydration
            expanded = False
            for dropdown_sel in [
                'button:has-text("50 holdings")',
                'button:has-text("100 holdings")',
                'button:has-text("holdings")',
            ]:
                try:
                    page.wait_for_selector(dropdown_sel, timeout=8000)
                    page.locator(dropdown_sel).first.click(timeout=5000)
                    page.wait_for_timeout(1000)
                    expanded = True
                    print(f"  AVSC: opened dropdown via '{dropdown_sel}'")
                    break
                except Exception as e:
                    print(f"  AVSC dropdown {dropdown_sel}: {e!s:.60}")

            # Step 2: click "View all" from the open dropdown
            if expanded:
                try:
                    # The option renders as a button/li/span inside the dropdown
                    for va_sel in [
                        'button:has-text("View all")',
                        'li:has-text("View all")',
                        'span:has-text("View all")',
                        '[role="option"]:has-text("View all")',
                        'text=View all',
                    ]:
                        try:
                            loc = page.locator(va_sel).first
                            if loc.is_visible(timeout=2000):
                                loc.click(timeout=3000)
                                print(f"  AVSC: clicked 'View all' via '{va_sel}'")
                                # Wait for all 457 rows to render
                                page.wait_for_timeout(5000)
                                break
                        except Exception:
                            pass
                except Exception as e:
                    print(f"  AVSC View all click: {e!s:.60}")

            # Scrape ALL table rows from the expanded DOM in one evaluate call
            all_rows = page.evaluate("""
                () => {
                    const ths = [...document.querySelectorAll('table thead th, table th')];
                    const headers = ths.map(th => th.innerText.trim());
                    return [...document.querySelectorAll('table tbody tr')].map(tr => {
                        const cells = [...tr.querySelectorAll('td')];
                        const obj   = {};
                        cells.forEach((td, i) => {
                            obj[headers[i] || 'c' + i] = td.innerText.trim();
                        });
                        return obj;
                    }).filter(r => Object.keys(r).length > 0);
                }
            """)
            print(f"  AVSC: DOM scraped {len(all_rows)} rows after View all")

            # DOM date: use the same latest-date heuristic as GRIN.
            # Two "As of" dates on the page: holdings (05/21) > characteristics (04/30).
            # The holdings date is always the most recent — sort all matches descending.
            try:
                avsc_aod = page.evaluate("""
                    () => {
                        const els = [...document.querySelectorAll('p,span,div,small,em,li')];
                        const hits = [];
                        for (const el of els) {
                            if (el.children.length > 0) continue;
                            const m = el.textContent.match(
                                /As of\\s+(\\d{1,2}\\/\\d{1,2}\\/\\d{4})/i
                            );
                            if (m) hits.push(m[1]);
                        }
                        if (!hits.length) return '';
                        const toNum = s => {
                            const [mo,d,y] = s.split('/');
                            return parseInt(y)*10000+parseInt(mo)*100+parseInt(d);
                        };
                        hits.sort((a,b) => toNum(b)-toNum(a));
                        const [mo,d,y] = hits[0].split('/');
                        return y+'-'+mo.padStart(2,'0')+'-'+d.padStart(2,'0');
                    }
                """)
                if avsc_aod:
                    print(f"  AVSC: DOM date (most recent on page) = {avsc_aod}")
            except Exception as e:
                print(f"  AVSC DOM date error: {e!s:.60}")

        browser.close()

    # ── Process Tier 1 result (analytics beacon CSV) ─────────────────────────
    if beacon_csv:
        lines = beacon_csv.splitlines()

        # Parse "As of date" from CSV metadata (row 6: "As of date,05/21/2026")
        for meta_line in lines[:15]:
            if re.search(r'as of date', meta_line, re.I):
                parts = meta_line.split(',', 1)
                if len(parts) == 2:
                    raw_d = parts[1].strip().strip('"')
                    m = re.match(r'(\d{1,2})/(\d{1,2})/(\d{4})', raw_d)
                    if m:
                        avsc_aod = f"{m.group(3)}-{m.group(1).zfill(2)}-{m.group(2).zfill(2)}"
                        print(f"  AVSC: as_of_date from CSV metadata = {avsc_aod}")
                        break

        # Column headers are on the line starting with "COMPANY"
        hidx = next(
            (i for i, ln in enumerate(lines) if re.match(r'(?i)^company\b', ln.strip())),
            None
        )
        if hidx is not None:
            df = pd.read_csv(io.StringIO(beacon_csv), skiprows=hidx)
            df.insert(0, 'etf', 'AVSC')
            if avsc_aod:
                df['as_of_date'] = avsc_aod
            df.dropna(how='all', inplace=True)
            print(f"  AVSC: ✅ {len(df)} rows from beacon | as_of_date={avsc_aod}")
            return df

    # ── Process Tier 2 result (DOM table rows) ────────────────────────────────
    if all_rows:
        df = pd.DataFrame(all_rows)
        df.insert(0, 'etf', 'AVSC')
        if avsc_aod:
            df['as_of_date'] = avsc_aod
        if 'Ticker' in df.columns:
            # Allow class-share separators (BRK.B, BF-B) — the old strict
            # ^[A-Z]{1,5}$ silently dropped them from the DOM fallback path.
            df = df[df['Ticker'].notna() & df['Ticker'].str.match(r'^[A-Z]{1,5}[.\-]?[A-Z]?$', na=False)]
        print(f"  AVSC: ✅ {len(df)} rows from DOM | as_of_date={avsc_aod}")
        return df

    # ── Tier 3: static HTTP fallback ─────────────────────────────────────────
    r    = S.get(holdings_url, timeout=30, allow_redirects=True)
    soup = BeautifulSoup(r.text, 'lxml')
    for tbl in soup.find_all('table'):
        df = pd.read_html(str(tbl))[0]
        if len(df) > 5:
            df.insert(0, 'etf', 'AVSC')
            print(f"  AVSC: static HTML fallback — {len(df)} rows")
            return df
    return pd.DataFrame()


# ═══════════════════════════════════════════════════════════════════════════════
# RUNNER
# ═══════════════════════════════════════════════════════════════════════════════
def run_all():
    tasks = [
        ('GRIN', fetch_grin),
        ('JH',   fetch_jh_family),
        ('VLUE', fetch_vlue),
        ('MFEM', fetch_mfem),
        ('JOET', fetch_joet),
        ('AVSC', fetch_avsc),
    ]

    # Fallback as_of_date for ETFs that don't embed it in their data
    default_aod = prev_biz_day().strftime('%Y-%m-%d')

    raw_frames:   list[pd.DataFrame] = []   # original columns  → per-tab XLSX
    canon_frames: list[pd.DataFrame] = []   # canonical columns → consolidated CSV
    summary: list[dict] = []

    for name, fn in tasks:
        print(f"\n{'='*60}\n  {name}\n{'='*60}")
        try:
            result = fn()

            # JH returns {ticker: df, ...}; everything else returns a single df
            items: list[tuple[str, pd.DataFrame]] = (
                list(result.items()) if isinstance(result, dict)
                else [(name, result)]
            )

            for ticker, df in items:
                rows = len(df)
                print(f"  {'✅' if rows else '⚠️ '} {ticker}: {rows} rows")
                if rows:
                    print(df.head(2).to_string(index=False))
                    raw_frames.append(df)
                    # Canonicalize — each fetch fn embeds as_of_date where known;
                    # default_aod is the fallback for GRIN / AVSC / JOET.
                    canon_frames.append(_canonicalize(df, ticker, default_aod))
                summary.append({
                    'ETF':    ticker,
                    'Status': '✅ OK' if rows else '⚠️ 0 rows',
                    'Rows':   rows,
                    'Cols':   df.shape[1],
                })

        except Exception as e:
            print(f"  ❌ FAILED: {e!s:.120}")
            traceback.print_exc()
            summary.append({'ETF': name, 'Status': f'❌ {e!s:.80}', 'Rows': 0, 'Cols': 0})

    # ── 1. Per-ETF raw XLSX (preserves every source-specific column) ─────────
    xlsx_out = f'etf_holdings_{TODAY_8}.xlsx'
    if raw_frames:
        with pd.ExcelWriter(xlsx_out, engine='openpyxl') as w:
            for df in raw_frames:
                sheet = str(df['etf'].iloc[0])[:31] if 'etf' in df.columns else 'unknown'
                df.to_excel(w, sheet_name=sheet, index=False)
        print(f"\n  📁 Raw XLSX saved:  {xlsx_out}")

    # ── 2. Unified canonical CSV (standardised schema, all ETFs stacked) ─────
    csv_out = f'etf_holdings_{TODAY_8}.csv'
    if canon_frames:
        combined = pd.concat(canon_frames, ignore_index=True)
        # Sort for deterministic ordering and downstream join-friendliness
        combined.sort_values(['etf', 'as_of_date'], inplace=True, ignore_index=True)
        combined.to_csv(csv_out, index=False)
        print(f"  📄 Canonical CSV saved: {csv_out}")
        print(f"     Columns: {list(combined.columns[:16])}")
        print(f"     Shape:   {combined.shape}")

    print(f"\n\n{'='*60}\nFINAL SUMMARY\n{'='*60}")
    print(pd.DataFrame(summary).to_string(index=False))

    # Fail loud on TOTAL failure (audit P0: silent-success exits). run_all()
    # used to swallow every exception and exit 0, so an issuer-wide outage
    # yielded a green run with zero rows. Partial failures remain exit 0 —
    # fault isolation across funds is by design.
    if summary and all(s['Rows'] == 0 for s in summary):
        print("\n❌ FATAL: every extended scraper returned 0 rows — failing run (exit 1)")
        sys.exit(1)


if __name__ == '__main__':
    run_all()