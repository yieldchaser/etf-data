"""
Conviction Labs — Stock Details Builder (Self-Healing Coverage Engine)
========================================================================
Fetches per-ticker price history + company description, writing each to
docs/data/details/{TICKER}.json

Coverage grows automatically each CI run via a bounded batch approach.
Pending tickers get stub files so the UI shows honest state (no 404s).

Rules:
  - Descriptions: yfinance longBusinessSummary ONLY — never hallucinated
  - Descriptions cached forever once fetched (write-once)
  - Prices refreshed on each resolved run
  - Batch size default 150 — rate-limit safe
"""
from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

if sys.platform.startswith("win"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except AttributeError:
        pass

# Monkeypatch yfinance to prevent AttributeError: 'NoneType' object has no attribute 'text'
# when a 404 occurs on the quoteSummary endpoint.
try:
    import yfinance as yf
    import yfinance.scrapers.quote as yfq
    old_fetch = yfq.Quote._fetch
    def _patched_fetch(self, modules):
        try:
            return old_fetch(self, modules)
        except Exception:
            return None
    yfq.Quote._fetch = _patched_fetch
except Exception:
    pass

REPO_ROOT           = Path(__file__).resolve().parent.parent
LEADERBOARD_PATH    = REPO_ROOT / "docs" / "data" / "leaderboard.json"
DESC_CACHE_PATH     = REPO_ROOT / "data" / "company_descriptions.json"
DETAILS_DIR         = REPO_ROOT / "docs" / "data" / "details"
COVERAGE_STATE_PATH = REPO_ROOT / "data" / "coverage_state.json"
SYMBOL_MAP_PATH     = REPO_ROOT / "data" / "symbol_map.json"
UNRESOLVED_PATH     = REPO_ROOT / "data" / "stocks_unresolved.json"
CURRENCY_CACHE_PATH = REPO_ROOT / "data" / "currency_cache.json"
TICKER_META_PATH    = REPO_ROOT / "data" / "ticker_metadata.csv"

PRICE_PERIOD        = "2y"
BATCH_SIZE_DEFAULT  = 150
DESC_DELAY          = 0.5
PROBE_DELAY         = 0.3
MAX_RETRY           = 5

KNOWN_SYMBOL_MAP: dict[str, str] = {
    "BRK.B": "BRK-B", "BRK.A": "BRK-A", "BF.B": "BF-B", "BF.A": "BF-A",
    "005930": "005930.KS", "A005930": "005930.KS",
    "000660": "000660.KS", "A000660": "000660.KS",
    "005380": "005380.KS", "005935": "005935.KS",
    "055550": "055550.KS", "035420": "035420.KS",
    "000270": "000270.KS", "034730": "034730.KS",
    "012330": "012330.KS", "017670": "017670.KS",
    "051910": "051910.KS", "003550": "003550.KS",
    "015760": "015760.KS", "030200": "030200.KS",
    "096770": "096770.KS", "032830": "032830.KS",
    "018260": "018260.KS", "009150": "009150.KS",
    "028260": "028260.KS", "010130": "010130.KS",
    "086790": "086790.KS", "105560": "105560.KS",
    "066570": "066570.KS", "009540": "009540.KS",
    "000830": "000830.SZ", "011200": "011200.KS",
    "042660": "042660.KS", "033780": "033780.KS",
    "2330": "2330.TW", "2317": "2317.TW", "2454": "2454.TW",
    "2308": "2308.TW", "2303": "2303.TW", "2382": "2382.TW",
    "2412": "2412.TW", "2881": "2881.TW", "2882": "2882.TW",
    "3711": "3711.TW", "2395": "2395.TW", "2886": "2886.TW",
    "2891": "2891.TW", "1303": "1303.TW", "1301": "1301.TW",
    "2002": "2002.TW", "2207": "2207.TW", "3045": "3045.TW",
    "VALE3": "VALE3.SA", "PETR4": "PETR4.SA", "PETR3": "PETR3.SA",
    "ITUB4": "ITUB4.SA", "BBDC4": "BBDC4.SA", "ABEV3": "ABEV3.SA",
    "WEGE3": "WEGE3.SA", "BBAS3": "BBAS3.SA", "RENT3": "RENT3.SA",
    "EGIE3": "EGIE3.SA", "EMBR3": "EMBR3.SA", "MGLU3": "MGLU3.SA",
    "RADL3": "RADL3.SA", "SUZB3": "SUZB3.SA", "LREN3": "LREN3.SA",
    "HAPV3": "HAPV3.SA", "RAIL3": "RAIL3.SA", "CPLE6": "CPLE6.SA",
    "VIVT3": "VIVT3.SA", "TAEE11": "TAEE11.SA", "TRPL4": "TRPL4.SA",
    "7203": "7203.T", "6758": "6758.T", "9984": "9984.T",
    "6501": "6501.T", "8306": "8306.T", "9432": "9432.T",
    "7974": "7974.T", "4502": "4502.T", "6902": "6902.T",
    "8001": "8001.T", "7267": "7267.T", "6954": "6954.T",
    "4661": "4661.T", "8411": "8411.T", "9983": "9983.T",
    "700":  "0700.HK", "0700": "0700.HK",
    "9988": "9988.HK", "1299": "1299.HK",
    "0005": "0005.HK", "2318": "2318.HK",
    "0388": "0388.HK", "1398": "1398.HK",
    "3690": "3690.HK", "2020": "2020.HK",
    "INFY": "INFY", "WIT": "WIT", "HDB": "HDB", "IBN": "IBN",
    "ATGL": "ATGL.NS",
    "STX": "STX", "ALV": "ALV", "FTI": "FTI", "LYB": "LYB",
    "NXPI": "NXPI", "GTX": "GTX", "ARM": "ARM", "ITC": "ITC.NS",
    "REP.SM": "REP.MC", "EVO.SS": "EVO.ST", "SAND.SS": "SAND.ST",
    "TOP.F": "TOP.BK", "TRE.SM": "TRE.MC", "VWS.DC": "VWS.CO",
}

COUNTRY_SUFFIX: dict[str, str] = {
    "South Korea": ".KS", "Korea": ".KS",
    "Taiwan": ".TW",
    "Japan": ".T",
    "Hong Kong": ".HK",
    "Australia": ".AX",
    "Canada": ".TO",
    "Germany": ".DE",
    "France": ".PA",
    "Sweden": ".ST",
    "Switzerland": ".SW",
    "Netherlands": ".AS",
    "United Kingdom": ".L",
    "Brazil": ".SA",
    "China": ".SS",
    "Mexico": ".MX",
    "Singapore": ".SI",
}

PROBE_SUFFIXES = [
    ".KS", ".TW", ".T", ".HK", ".SS", ".SZ", ".SA", ".NS", ".AX", ".L", ".DE", ".PA",
    ".MX", ".KL", ".JK", ".BK", ".IS", ".WA", ".SR", ".SN", ".PS", ".TA", ".JO", ".AT",
    ".AD", ".AE"
]

# Exchange-suffix → native trading currency. Used to infer the price currency
# when yfinance info doesn't return one (or returned a wrong default).
SUFFIX_CURRENCY: dict[str, str] = {
    "KS": "KRW", "KQ": "KRW",
    "T": "JPY",
    "TW": "TWD", "TWO": "TWD",
    "HK": "HKD",
    "SS": "CNY", "SZ": "CNY",
    "SA": "BRL",
    # ".L" (LSE) deliberately omitted: yfinance quotes pence (GBp); inferring "GBP" would mislabel by 100x. London names rely on yfinance info currency.
    "TO": "CAD", "V": "CAD",
    "AX": "AUD",
    "NS": "INR", "BO": "INR",
    "PA": "EUR", "AS": "EUR", "BR": "EUR", "MC": "EUR", "MI": "EUR",
    "DE": "EUR", "F": "EUR", "VI": "EUR", "HE": "EUR", "LS": "EUR",
    "SW": "CHF",
    "ST": "SEK",
    "OL": "NOK",
    "CO": "DKK",
    "MX": "MXN",
    "JO": "ZAR",
    "BK": "THB",
    "IS": "TRY",
    "WA": "PLN",
    "JK": "IDR",
    "KL": "MYR",
    "SI": "SGD",
    "NZ": "NZD",
    "TA": "ILS",
    "SN": "CLP",
    "KW": "KWD",
    "QA": "QAR",
    "SR": "SAR",
    "AD": "AED",
    "AE": "AED",
}


def infer_currency_from_symbol(symbol: str) -> str | None:
    """Infer trading currency from a yfinance symbol's exchange suffix.

    Case-insensitive match on the part after the last dot ('000660.KS' → 'KRW').
    Symbols without a dot (plain US listings) return None.
    """
    if not symbol or "." not in symbol:
        return None
    suffix = symbol.rsplit(".", 1)[1].upper()
    return SUFFIX_CURRENCY.get(suffix)


# Currency codes that appear as ETF "holdings" but are not equities.
# Skip these from the stock-details universe to avoid wasting batch budget.
KNOWN_CURRENCY_CODES = {
    "AED", "ARS", "AUD", "BRL", "CAD", "CHF", "CLP", "CNY", "COP", "CZK",
    "DKK", "EGP", "EUR", "GBP", "HKD", "HUF", "IDR", "ILS", "INR", "ISK",
    "JPY", "KRW", "KWD", "MXN", "MYR", "NOK", "NZD", "PEN", "PHP", "PLN",
    "QAR", "RON", "SAR", "SEK", "SGD", "THB", "TRY", "TWD", "USD", "ZAR",
}

# Matches the auto-generated placeholder description so stale template
# entries get refreshed. Flexible across tier phrasings ("smart-beta ETFs",
# "quant ETFs", etc.) rather than pinning one literal string.
_TEMPLATE_DESC_RE = re.compile(r"is a holding tracked across[\w\s-]*ETFs", re.IGNORECASE)


class _SafeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, float) and (math.isnan(o) or math.isinf(o)):
            return None
        return super().default(o)


def _sanitise(obj: Any) -> Any:
    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    if isinstance(obj, dict):
        return {k: _sanitise(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitise(v) for v in obj]
    return obj


def _dumps(obj: Any, **kw) -> str:
    kw.setdefault("cls", _SafeEncoder)
    return json.dumps(_sanitise(obj), **kw)


def _write_json_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def _load_leaderboard_tickers() -> dict[str, str]:
    if not LEADERBOARD_PATH.exists():
        print(f"  ERROR: {LEADERBOARD_PATH} not found.")
        return {}
    try:
        lb = json.loads(LEADERBOARD_PATH.read_text(encoding="utf-8"))
        # `company` is null for ~800 rows in leaderboard.json; coalesce to ""
        # so the map never holds None (None[:40] crashed the whole batch run
        # before coverage_state could be saved — blocking coverage accumulation).
        return {r["ticker"]: (r.get("company") or "") for r in lb if r.get("ticker")}
    except Exception as e:
        print(f"  ERROR reading leaderboard: {e}")
        return {}


def _load_leaderboard_ranks() -> dict[str, int]:
    """{ticker: leaderboard_rank} — lower rank = higher conviction. Drives
    batch prioritisation so the most-viewed top names (e.g. MU #34) get real
    price/description data before the long tail, rather than being starved
    behind an alphabetical/US-first ordering."""
    if not LEADERBOARD_PATH.exists():
        return {}
    try:
        lb = json.loads(LEADERBOARD_PATH.read_text(encoding="utf-8"))
        out: dict[str, int] = {}
        for i, r in enumerate(lb):
            t = r.get("ticker")
            if t:
                # Prefer the explicit field; fall back to row order (the file
                # is emitted in rank order) if it's missing/null.
                rank = r.get("leaderboard_rank")
                out[t] = int(rank) if isinstance(rank, (int, float)) else i + 1
        return out
    except Exception as e:
        print(f"  WARNING: could not load leaderboard ranks: {e}")
        return {}


def _load_ticker_metadata() -> dict[str, dict]:
    if not TICKER_META_PATH.exists():
        return {}
    try:
        df = pd.read_csv(TICKER_META_PATH, dtype=str).fillna("")
        result: dict[str, dict] = {}
        for _, row in df.iterrows():
            t = str(row.get("ticker", "")).strip()
            if t:
                result[t] = {
                    "sector":  str(row.get("sector", "Unknown")),
                    "country": str(row.get("country", "Unknown")),
                }
        return result
    except Exception as e:
        print(f"  WARNING: Could not load ticker_metadata: {e}")
        return {}


def _load_coverage_state() -> dict:
    default: dict = {"resolved": [], "pending": [], "unresolved": {}, "last_run_utc": None, "coverage_pct": 0.0}
    if COVERAGE_STATE_PATH.exists():
        try:
            state = json.loads(COVERAGE_STATE_PATH.read_text(encoding="utf-8"))
            for key, val in default.items():
                state.setdefault(key, val)
            return state
        except Exception as e:
            print(f"  WARNING: Bad coverage_state.json ({e}), starting fresh.")
    return dict(default)


def _save_coverage_state(state: dict) -> None:
    _write_json_atomic(COVERAGE_STATE_PATH, _dumps(state, indent=2, sort_keys=True))


def _load_symbol_map() -> dict[str, str]:
    base = dict(KNOWN_SYMBOL_MAP)
    if SYMBOL_MAP_PATH.exists():
        try:
            from_file = json.loads(SYMBOL_MAP_PATH.read_text(encoding="utf-8"))
            base.update(from_file)
        except Exception as e:
            print(f"  WARNING: Bad symbol_map.json ({e})")
    return base


def _save_symbol_map(m: dict[str, str]) -> None:
    _write_json_atomic(SYMBOL_MAP_PATH, _dumps(m, indent=2, sort_keys=True))


def _load_desc_cache() -> dict[str, str]:
    if DESC_CACHE_PATH.exists():
        try:
            return json.loads(DESC_CACHE_PATH.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"  WARNING: Bad description cache ({e})")
    return {}


def _save_desc_cache(cache: dict[str, str]) -> None:
    _write_json_atomic(DESC_CACHE_PATH, _dumps(cache, indent=2, sort_keys=True))


def _load_currency_cache() -> dict[str, str]:
    if CURRENCY_CACHE_PATH.exists():
        try:
            return json.loads(CURRENCY_CACHE_PATH.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"  WARNING: Bad currency cache ({e})")
    return {}


def _save_currency_cache(cache: dict[str, str]) -> None:
    _write_json_atomic(CURRENCY_CACHE_PATH, _dumps(cache, indent=2, sort_keys=True))


def _is_us_ticker(ticker: str, country: str) -> bool:
    if re.fullmatch(r"[A-Z]{1,5}", ticker):
        return True
    if country and country not in ("Unknown", ""):
        return country == "United States"
    return False


def _resolve_yf_symbol(ticker: str, meta: dict, symbol_map: dict) -> str:
    if ticker in symbol_map:
        return symbol_map[ticker]

    # Normalize share classes (e.g. "BBD.B" -> "BBD-B", "BBD.B.C" -> "BBD-B-C")
    ticker = re.sub(r"\.(A|B|C|U|W)(?=\.|$)", r"-\1", ticker)

    # Translate Bloomberg/exchange suffixes to Yahoo Finance suffixes
    # E.g. "285A.JP" -> "285A.T", "VALE3.BZ" -> "VALE3.SA"
    if "." in ticker:
        base, suffix = ticker.rsplit(".", 1)
        suffix_map = {
            "JP": "T",
            "JT": "T",
            "TT": "TW",
            "CN": "SS",
            "CT": "TO",
            "GY": "DE",
            "GR": "DE",
            "BB": "BR",
            "SE": "ST",
            "LN": "L",
            "NO": "OL",
            "FP": "PA",
            "NA": "AS",
            "BZ": "SA",
            "IM": "MI",
            "AU": "AX",
            "TB": "BK",
            "SJ": "JO",
            "JN": "JO",
            "AB": "SR",
            "TI": "IS",
            "PW": "WA",
            "IJ": "JK",
            "MK": "KL",
            "GA": "AT",
            "CI": "SN",
            "IT": "TA",
        }
        if suffix in suffix_map:
            suffix = suffix_map[suffix]
        if suffix == "US":
            return base
        if suffix == "HK" and base.isdigit():
            return f"{base.zfill(4)}.HK"
        return f"{base}.{suffix}"

    country = (meta.get("country") or "").strip()
    if _is_us_ticker(ticker, country):
        return ticker
    if country and country in COUNTRY_SUFFIX:
        return ticker + COUNTRY_SUFFIX[country]
    if re.fullmatch(r"A?\d{6}", ticker):
        return ticker.lstrip("A") + ".KS"
    if re.fullmatch(r"\d{4}", ticker):
        return ticker + ".TW"
    if re.fullmatch(r"[A-Z]{4}(3|4|11)", ticker):
        # Only apply the Brazilian .SA suffix when country is unknown/unset.
        # If country is known and is NOT Brazil, skip this pattern — it could
        # false-positive on a coincidental 4-letter+digit ticker from another
        # exchange (e.g. an Indian or Chinese name that the metadata doesn't map).
        # Brazil-specific names should already have been caught by COUNTRY_SUFFIX
        # above; this heuristic is only for tickers with no country metadata.
        if not country or country == "Brazil":
            return ticker + ".SA"
    if re.fullmatch(r"\d{1,4}", ticker):
        return ticker.zfill(4) + ".HK"
    return ticker


def _company_name_matches(lb_company: str, yf_company: str) -> bool:
    if not lb_company or not yf_company:
        return False
    # Common stop words in company names
    stop_words = {
        "sa", "plc", "ltd", "inc", "corp", "co", "holdings", "holding", "common", "stock",
        "class", "ord", "ordinary", "bhd", "as", "sab", "de", "cv", "group", "the", "and",
        "of", "in", "sa-registered", "shs", "sponsored", "adr", "foreign", "forgn", "sh",
        "units", "unit", "trust", "investment", "financial", "bank", "banking", "sae", "ksc"
    }
    
    def tokenize(name: str) -> set[str]:
        # Split by non-alphanumeric, convert to lower, filter out stop words and short tokens
        tokens = re.split(r"[^A-Za-z0-9]", name.lower())
        return {t for t in tokens if t and t not in stop_words and len(t) > 1}

    lb_tokens = tokenize(lb_company)
    yf_tokens = tokenize(yf_company)
    
    # If no significant tokens in either, fallback to basic comparison
    if not lb_tokens or not yf_tokens:
        return lb_company.lower().strip() == yf_company.lower().strip()
        
    # Return True if there is at least one overlapping token
    return bool(lb_tokens & yf_tokens)


def _probe_yf_symbol(ticker: str, company: str, symbol_map: dict) -> str | None:
    try:
        import urllib.request
        import urllib.parse
        import json
        import yfinance as yf
    except ImportError:
        return None
    base = ticker.lstrip("A") if re.fullmatch(r"A\d{6}", ticker) else ticker
    # If ticker contains a dot suffix (e.g. "285A.JP"), strip it to probe the base ticker
    if "." in base:
        base = base.split(".")[0]
    base = re.sub(r"[^A-Za-z0-9]", "", base)
    if not base:
        return None

    # Clean the company name for a better Search API query
    def clean_company_for_query(co: str) -> str:
        import html
        name = html.unescape(co).upper()
        # Remove currency valuation stuff like INR2.0, TRY1.0
        name = re.sub(r'\b(INR|TRY|MXN|AED|KWD|PHP|THB|USD|EUR|KWD)\d+(\.\d+)?\b', '', name)
        name = re.sub(r'\bCOMMON\s+STOCK\b', '', name)
        # Suffixes to drop
        suffixes = [
            "LTD INDIA", "LTD", "LIMITED", "INC", "INCORPORATED", "PLC", "PUBLIC LIMITED COMPANY",
            "SA", "S.A.", "S.A.B. DE C.V.", "SAB DE CV", "SER B", "SER A", "SER C",
            "BHD MALAYSIA", "MALAYSIA", "SOUTH AFRICA", "CO", "CORP", "CORPORATION",
            "GROUP", "HOLDINGS", "HOLDING", "SPONS", "ADR", "SPONSORED", "GDR",
            "SHS", "UNIT", "UNITS", "CLASS B", "CLASS A", "SERIES B", "SERIES A",
            "NET OTHER ASSETS", "OTHER ASSETS", "INDIA", "MEXICO", "INDONESIA", "MALAYSIA",
            "THAILAND", "CHILE", "PORTUGAL", "GEORGIA", "TURKEY", "KOREA", "TAIWAN", "JAPAN",
            "BRAZIL", "SAUDI ARABIA", "SPAIN", "ITALY", "GERMANY", "FRANCE", "UK", "UNITED KINGDOM",
            "CANADA", "AUSTRALIA", "USA", "UNITED STATES"
        ]
        for suf in suffixes:
            name = re.sub(r'\b' + re.escape(suf) + r'\b', ' ', name)
        name = re.sub(r'[^A-Z0-9\s]', ' ', name)
        return " ".join(name.split())

    cleaned_company = clean_company_for_query(company)

    # Step 1: Hit Yahoo Finance Search API for both ticker base and company name
    queries = [base]
    if cleaned_company and cleaned_company.upper() != base.upper():
        queries.append(cleaned_company)

    candidates = []
    seen_syms = set()
    for q in queries:
        url = f"https://query2.finance.yahoo.com/v1/finance/search?q={urllib.parse.quote(q)}"
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        try:
            with urllib.request.urlopen(req, timeout=5) as response:
                res = json.loads(response.read().decode('utf-8'))
                for quote in res.get('quotes', []):
                    sym = quote.get('symbol')
                    name = quote.get('shortname', '') or quote.get('longname', '') or ''
                    if sym and sym not in seen_syms:
                        seen_syms.add(sym)
                        candidates.append((sym, name))
        except Exception as e:
            print(f"    Search API error for query '{q}': {e}")

    # Prefer candidates from Search API, sort them to prefer local/clean symbols over NVDR/Foreign
    def candidate_sort_key(item: tuple[str, str]) -> tuple[int, int, int, int, int]:
        sym, name = item
        sym_base = sym.split(".")[0].replace("-", "")
        # Priority 1: company name matches (0 is better)
        matches = 0 if _company_name_matches(company, name) else 1
        # Priority 2: base ticker matches exactly (0 is better)
        base_match = 0 if sym_base.upper() == base.upper() else 1
        # Priority 3: local board preferred over NVDR (-R) or Foreign (-F)
        has_r = 1 if "-R." in sym or sym.endswith("-R") else 0
        has_f = 1 if "-F." in sym or sym.endswith("-F") else 0
        dots = sym.count(".")
        return (matches, base_match, has_r, has_f, dots)

    # Filter candidates to only include those matching base ticker as a prefix or in the company name
    valid_candidates = []
    for sym, name in candidates:
        sym_base = sym.split(".")[0].replace("-", "")
        # If searched by company name, it's fine if the base ticker doesn't match the symbol's base ticker,
        # as long as the company name matches!
        if base.upper() in sym_base.upper() or base.upper() in name.upper() or _company_name_matches(company, name):
            valid_candidates.append((sym, name))
    valid_candidates.sort(key=candidate_sort_key)
    
    cand_name_map = {sym: name for sym, name in valid_candidates}

    # Extract just the symbols to test
    symbols_to_test = [sym for sym, name in valid_candidates]

    # Fall back to PROBE_SUFFIXES if search API returned no valid candidates
    if not symbols_to_test:
        for suffix in PROBE_SUFFIXES:
            symbols_to_test.append(base + suffix)

    tested = set()
    for sym in symbols_to_test:
        if sym in tested:
            continue
        tested.add(sym)
        # If candidate name is known from search API, ensure it matches company
        sym_co_name = cand_name_map.get(sym)
        if company and sym_co_name and not _company_name_matches(company, sym_co_name):
            continue
        try:
            df = yf.download(sym, period="5d", progress=False, threads=False)
            if df is not None and not df.empty:
                # Check if we have at least one valid price point
                if "Close" in df.columns:
                    close = df["Close"].dropna()
                else:
                    close = df.iloc[:, 0].dropna()
                if not close.empty:
                    val = float(close.values.flatten()[-1])
                    if val > 0:
                        # If probed from PROBE_SUFFIXES without pre-checked name, verify info
                        if not sym_co_name and company:
                            try:
                                t_info = yf.Ticker(sym).info
                                probed_co = t_info.get("longName") or t_info.get("shortName") or ""
                                if probed_co and not _company_name_matches(company, probed_co):
                                    continue
                            except Exception:
                                pass
                        symbol_map[ticker] = sym
                        print(f"    PROBE HIT: {ticker} → {sym} (price={val:.2f})")
                        return sym
        except Exception:
            pass
        time.sleep(PROBE_DELAY)
    return None



def _fetch_price_one(symbol: str) -> list[list] | None:
    try:
        import yfinance as yf
        df = yf.download(
            symbol, period=PRICE_PERIOD, interval="1d",
            auto_adjust=True, progress=False, threads=False,
        )
        if df is None or df.empty:
            return None
        if isinstance(df.columns, pd.MultiIndex):
            level0 = df.columns.get_level_values(0)
            if "Close" in level0:
                close = df["Close"]
                if isinstance(close, pd.DataFrame):
                    close = close.iloc[:, 0]
            else:
                close = df.iloc[:, 0]
        else:
            close = df["Close"] if "Close" in df.columns else df.iloc[:, 0]
        close = close.dropna()
        if close.empty:
            return None
        prices = [
            [pd.Timestamp(dt).strftime("%Y-%m-%d"), round(float(val), 4)]
            for dt, val in close.items()
            if not (isinstance(val, float) and (math.isnan(val) or math.isinf(val)))
        ]
        prices.sort(key=lambda x: x[0])
        return prices if prices else None
    except Exception as e:
        print(f"    Price fetch error ({symbol}): {e}")
        return None


def _fetch_description_one(symbol: str) -> tuple[str | None, str | None]:
    """Fetch real description + trading currency from yfinance.
    Returns (description, currency). NEVER fabricates descriptions."""
    try:
        import yfinance as yf
        info = yf.Ticker(symbol).info
        # .upper() also normalizes London's 'GBp' (pence) to 'GBP' — display
        # currency code only; prices are NOT rescaled.
        currency = (info.get("currency") or info.get("financialCurrency") or "").strip().upper() or None
        desc = (info.get("longBusinessSummary") or info.get("description") or "").strip()
        if len(desc) < 20:
            desc = None
        elif len(desc) > 620:
            sentences = desc.split(". ")
            out, length = "", 0
            for s in sentences:
                if length + len(s) > 600:
                    break
                out += s + ". "
                length += len(s) + 2
            desc = out.strip() or desc[:600]
        return desc, currency
    except Exception:
        return None, None


# Map a raw ticker to a filesystem- and URL-safe detail-file stem. Some
# leaderboard tickers carry scraper junk — footnote asterisks, HTML entities,
# encoding damage (e.g. "WALMEX.*", "PE&amp;OLES*", "�"). '*' and friends
# are invalid in Windows/NTFS paths and break `git checkout`/clone on Windows.
# stock.html applies the IDENTICAL regex when building the fetch URL, so the
# written name and the requested name stay in sync.
_UNSAFE_FN_RE = re.compile(r"[^A-Za-z0-9._-]")
# Windows reserved device names — invalid as a filename base regardless of the
# characters used (e.g. ticker "CON" → CON.json can't exist on Windows).
_WIN_RESERVED = {"CON", "PRN", "AUX", "NUL"} | {f"COM{i}" for i in range(1, 10)} | {f"LPT{i}" for i in range(1, 10)}


def _safe_detail_stem(ticker: str) -> str:
    """Map a raw ticker to a stem that is a valid filename on every OS.

    Mirrored verbatim in docs/stock.html so the written name and the requested
    URL stay in sync. Handles: illegal chars, trailing dot/space (invalid on
    Windows), empty result, and Windows reserved device names.
    """
    stem = _UNSAFE_FN_RE.sub("_", ticker).rstrip(". ")
    if not stem:
        stem = "_"
    if stem.upper() in _WIN_RESERVED:
        stem = stem + "_"
    return stem


def _cleanup_unsafe_detail_files() -> int:
    """Delete legacy detail files whose names are not already in canonical safe
    form — covers illegal chars (WALMEX.*.json), trailing dot/space, AND Windows
    reserved device names (CON.json). Returns the count removed. The commit-back
    step stages these deletions so the repo is checkout-safe on every OS."""
    removed = 0
    if not DETAILS_DIR.exists():
        return 0
    for f in DETAILS_DIR.iterdir():
        if not f.is_file() or not f.name.endswith(".json"):
            continue
        stem = f.name[:-len(".json")]
        if _safe_detail_stem(stem) + ".json" != f.name:
            try:
                f.unlink()
                removed += 1
            except OSError as e:
                print(f"  WARNING: could not remove legacy file {f.name}: {e}")
    if removed:
        print(f"  Removed {removed} legacy non-canonical detail file(s).")
    return removed


def _cleanup_stale_detail_files(universe_stems: set[str]) -> int:
    """Delete detail files that do not correspond to any ticker in the current
    leaderboard universe. Covers old tickers, and blocked tickers like EUR."""
    removed = 0
    if not DETAILS_DIR.exists():
        return 0
    for f in DETAILS_DIR.iterdir():
        if not f.is_file() or not f.name.endswith(".json"):
            continue
        stem = f.name[:-len(".json")]
        if stem not in universe_stems:
            try:
                f.unlink()
                removed += 1
            except OSError as e:
                print(f"  WARNING: could not remove stale file {f.name}: {e}")
    if removed:
        print(f"  Removed {removed} stale detail file(s) not in current universe (e.g. cash/old tickers).")
    return removed



def _write_detail_file(
    ticker: str, company: str,
    desc: str | None, prices: list[list] | None,
    pending: bool = False,
    currency: str | None = None,
) -> None:
    generated = datetime.now(timezone.utc).isoformat(timespec="seconds")
    if pending:
        payload: dict = {"ticker": ticker, "company": company, "pending": True, "generated": generated}
    else:
        payload = {
            "ticker": ticker, "company": company,
            "description": desc or "", "prices": prices or [],
            "currency": currency or "USD",
            "generated": generated,
        }
    _write_json_atomic(DETAILS_DIR / f"{_safe_detail_stem(ticker)}.json", _dumps(payload, separators=(",", ":")))


def _write_pending_stubs(pending_tickers: list[str], lb_map: dict[str, str]) -> int:
    count = 0
    for ticker in pending_tickers:
        out_path = DETAILS_DIR / f"{_safe_detail_stem(ticker)}.json"
        if out_path.exists():
            try:
                existing = json.loads(out_path.read_text(encoding="utf-8"))
                if not existing.get("pending", False) and existing.get("prices"):
                    continue
            except Exception:
                pass
        _write_detail_file(ticker, lb_map.get(ticker, ticker), None, None, pending=True)
        count += 1
    return count


def _prioritize_batch(
    pending: list[str], metadata: dict,
    symbol_map: dict, batch_size: int,
    lb_ranks: dict[str, int] | None = None,
) -> list[str]:
    """Order the pending tickers so the highest-conviction names resolve first.

    Primary key is leaderboard rank (lower = better) so the top-of-board names
    users actually open (e.g. MU #34) get real data before the long tail.
    Secondary key keeps the original yfinance-resolvability tiering as a
    tiebreak among equal/unknown ranks (already-mapped, then US, then known
    country, then unknown), with ticker as a final stable tiebreak.
    """
    lb_ranks = lb_ranks or {}

    def resolvability(t: str) -> int:
        if t in symbol_map:
            return 0
        country = (metadata.get(t) or {}).get("country", "")
        if _is_us_ticker(t, country):
            return 1
        if country and country not in ("Unknown", ""):
            return 2
        return 3

    # Unranked tickers sort after every ranked one.
    def key(t: str) -> tuple[int, int, str]:
        return (lb_ranks.get(t, 10**9), resolvability(t), t)

    return sorted(pending, key=key)[:batch_size]


def build_details(
    batch_size: int = BATCH_SIZE_DEFAULT,
    refresh_size: int = 100,
    tickers_filter: list[str] | None = None,
    fill_pending_only: bool = False,
    dry_run: bool = False,
    refresh_descriptions: bool = False,
) -> None:
    print("\n" + "═" * 62)
    print("  Conviction Labs — Stock Details (Self-Healing Coverage)")
    print("═" * 62)

    DETAILS_DIR.mkdir(parents=True, exist_ok=True)
    _cleanup_unsafe_detail_files()

    lb_map     = _load_leaderboard_tickers()
    if not lb_map:
        print("  No tickers found — run conviction.build first.")
        return

    lb_ranks   = _load_leaderboard_ranks()
    metadata   = _load_ticker_metadata()
    state      = _load_coverage_state()
    symbol_map = _load_symbol_map()
    desc_cache = _load_desc_cache() if not refresh_descriptions else {}
    currency_cache = _load_currency_cache()

    total_universe = set(lb_map.keys())
    # Filter out currency codes — they appear as ETF cash positions, not equities
    currency_tickers = total_universe & KNOWN_CURRENCY_CODES
    if currency_tickers:
        print(f"  Filtered {len(currency_tickers)} currency codes: {', '.join(sorted(currency_tickers)[:10])}…")
        total_universe -= currency_tickers
    _cleanup_stale_detail_files({_safe_detail_stem(t) for t in total_universe})
    # Discover resolved tickers by scanning all files in the details directory.
    # This recovers any tickers that were successfully processed in previous runs or interrupted runs.
    resolved       = set()
    for t in total_universe:
        detail_path = DETAILS_DIR / f"{_safe_detail_stem(t)}.json"
        if detail_path.exists():
            try:
                existing = json.loads(detail_path.read_text(encoding="utf-8"))
                if not existing.get("pending", False) and existing.get("prices"):
                    resolved.add(t)
            except Exception:
                pass
    unresolved     = {k: v for k, v in (state.get("unresolved") or {}).items() if k in total_universe}
    pending_set    = (set(state.get("pending") or []) | (total_universe - resolved - set(unresolved.keys()))) & total_universe

    # Guarantee top-N tickers (by leaderboard rank) are retried even if they
    # previously exceeded MAX_RETRY. Transient yfinance failures must not
    # permanently exclude high-priority names from coverage.
    if lb_ranks:
        top_n_guaranteed = batch_size
        top_tickers_by_rank = sorted(lb_ranks, key=lambda t: lb_ranks[t])[:top_n_guaranteed]
        for t in top_tickers_by_rank:
            if t in total_universe and t not in resolved and t in unresolved:
                unresolved.pop(t)
                pending_set.add(t)

    prev_coverage = len(resolved) / max(len(total_universe), 1) * 100
    print(f"\n  Universe:   {len(total_universe)} tickers")
    print(f"  Resolved:   {len(resolved)} ({prev_coverage:.1f}%)")
    print(f"  Pending:    {len(pending_set)}")
    print(f"  Unresolved: {len(unresolved)}")

    if fill_pending_only:
        print("\n  --fill-pending: writing stub files only…")
        if not dry_run:
            n = _write_pending_stubs(sorted(pending_set | set(unresolved.keys())), lb_map)
            print(f"  Wrote {n} pending stub files.")
        return

    if tickers_filter:
        batch = tickers_filter
        print(f"\n  Filter mode: {len(batch)} tickers")
    else:
        batch = _prioritize_batch(sorted(pending_set), metadata, symbol_map, batch_size, lb_ranks)
        print(f"\n  Batch: {len(batch)} of {len(pending_set)} pending (top by leaderboard rank)")

    # ── Resolved-ticker refresh: prioritized refresh of stale prices ──
    refresh_batch: list[str] = []
    if not tickers_filter:
        # Decouple refresh budget from pending batch:
        # Default dedicated refresh budget + any leftover batch_size budget
        leftover_budget = max(0, batch_size - len(batch))
        refresh_budget = refresh_size + leftover_budget
        
        if refresh_budget > 0:
            stale_resolved = []
            today_dt = datetime.now(timezone.utc).date()
            
            for t in sorted(resolved):
                detail_path = DETAILS_DIR / f"{_safe_detail_stem(t)}.json"
                last_dt = datetime(1970, 1, 1).date()
                if detail_path.exists():
                    try:
                        existing = json.loads(detail_path.read_text(encoding="utf-8"))
                        prices = existing.get("prices", [])
                        is_pending = existing.get("pending", False)
                        if prices and not is_pending:
                            last_dt = datetime.strptime(prices[-1][0], "%Y-%m-%d").date()
                    except Exception:
                        pass
                
                # Priority calculation
                age_days = (today_dt - last_dt).days
                rank = lb_ranks.get(t, 10**9)
                
                if rank <= 250:
                    target = 1
                elif rank <= 1000:
                    target = 3
                else:
                    target = 7
                    
                overdue_ratio = age_days / target
                stale_resolved.append((overdue_ratio, rank, t))
                
            # Sort by overdue_ratio descending (most overdue first), then rank ascending (higher conviction first)
            stale_resolved.sort(key=lambda x: (-x[0], x[1], x[2]))
            
            # Select up to refresh_budget tickers
            refresh_batch = [t for _, _, t in stale_resolved[:refresh_budget]]
            if refresh_batch:
                print(f"  Refresh: {len(refresh_batch)} prioritized resolved tickers (overdue first)")

    if dry_run:
        print("\n  --dry-run plan:")
        for t in batch[:30]:
            sym = _resolve_yf_symbol(t, metadata.get(t, {}), symbol_map)
            country = (metadata.get(t) or {}).get("country", "?")
            print(f"    {t:15s} → {sym:20s}  [{country}]")
        if len(batch) > 30:
            print(f"    … and {len(batch)-30} more")
        return

    newly_resolved = 0
    errors = 0
    newly_unresolved: list[str] = []

    for i, ticker in enumerate(batch, 1):
        company = lb_map.get(ticker) or ticker
        meta    = metadata.get(ticker, {})
        print(f"\n  [{i}/{len(batch)}] {ticker} ({company[:40]})")

        symbol = _resolve_yf_symbol(ticker, meta, symbol_map)
        prices = _fetch_price_one(symbol)

        if not prices:
            print(f"    No data for {symbol} — probing…")
            probed = _probe_yf_symbol(ticker, company, symbol_map)
            if probed and probed != symbol:
                symbol = probed
                prices = _fetch_price_one(symbol)

        current_desc = desc_cache.get(ticker, "")
        current_currency = currency_cache.get(ticker)
        needs_desc = not current_desc or _TEMPLATE_DESC_RE.search(current_desc)
        if needs_desc and symbol:
            time.sleep(DESC_DELAY)
            desc, fetched_currency = _fetch_description_one(symbol)
            if desc:
                desc_cache[ticker] = desc
                print(f"    Description: {len(desc)} chars")
            else:
                print(f"    Description: not found")
            if fetched_currency:
                currency_cache[ticker] = fetched_currency
        elif not current_currency and symbol:
            # Have description cached but no currency yet — quick fetch
            time.sleep(DESC_DELAY)
            _, fetched_currency = _fetch_description_one(symbol)
            if fetched_currency:
                currency_cache[ticker] = fetched_currency
            desc = current_desc or None
        else:
            desc = current_desc or None

        if prices:
            # Currency priority: yfinance info > exchange-suffix inference > "USD" (writer default)
            currency = currency_cache.get(ticker) or infer_currency_from_symbol(symbol)
            _write_detail_file(ticker, company, desc, prices, pending=False, currency=currency)
            resolved.add(ticker)
            pending_set.discard(ticker)
            unresolved.pop(ticker, None)
            newly_resolved += 1
            print(f"    ✓ {len(prices)} price points")
        else:
            fail_count = unresolved.get(ticker, 0) + 1
            unresolved[ticker] = fail_count
            if fail_count >= MAX_RETRY:
                pending_set.discard(ticker)
                newly_unresolved.append(ticker)
                print(f"    ✗ Unresolved after {fail_count} attempts")
            else:
                print(f"    ✗ Failed (attempt {fail_count}/{MAX_RETRY})")
            errors += 1

    # ── Refresh loop for stale resolved tickers ──
    refresh_failures = state.get("refresh_failures", {})
    for i, ticker in enumerate(refresh_batch, 1):
        company = lb_map.get(ticker) or ticker
        meta    = metadata.get(ticker, {})
        print(f"\n  [refresh {i}/{len(refresh_batch)}] {ticker} ({company[:40]})")

        symbol = _resolve_yf_symbol(ticker, meta, symbol_map)
        prices = _fetch_price_one(symbol)

        if not prices:
            probed = _probe_yf_symbol(ticker, company, symbol_map)
            if probed and probed != symbol:
                symbol = probed
                prices = _fetch_price_one(symbol)

        desc = desc_cache.get(ticker) or None
        if prices:
            # Currency priority: yfinance info > exchange-suffix inference > "USD" (writer default)
            currency = currency_cache.get(ticker) or infer_currency_from_symbol(symbol)
            _write_detail_file(ticker, company, desc, prices, pending=False, currency=currency)
            print(f"    ✓ refreshed {len(prices)} price points")
            refresh_failures.pop(ticker, None)  # reset consecutive failure counter on success
        else:
            fail_n = refresh_failures.get(ticker, 0) + 1
            refresh_failures[ticker] = fail_n
            if fail_n >= 3:
                # Ticker has failed refresh 3 times in a row — likely delisted or URL changed.
                # Move back to pending so the full resolution pipeline (probe, symbol_map
                # update) gets another chance rather than silently serving stale prices forever.
                resolved.discard(ticker)
                pending_set.add(ticker)
                refresh_failures.pop(ticker, None)
                print(f"    ✗ refresh failed {fail_n}x — moving back to pending for re-resolution")
            else:
                print(f"    ✗ refresh failed ({fail_n}/3 before re-queue)")
    state["refresh_failures"] = refresh_failures

    all_pending = sorted(pending_set | {t for t in unresolved if t not in resolved})
    print(f"\n  Writing pending stubs for {len(all_pending)} tickers…")
    stubs = _write_pending_stubs(all_pending, lb_map)
    print(f"  Stubs written: {stubs}")

    new_coverage = len(resolved) / max(len(total_universe), 1) * 100
    state["resolved"]     = sorted(resolved)
    state["pending"]      = sorted(pending_set)
    state["unresolved"]   = unresolved
    state["last_run_utc"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    state["coverage_pct"] = round(new_coverage, 2)
    _save_coverage_state(state)
    _save_symbol_map(symbol_map)
    _save_desc_cache(desc_cache)
    _save_currency_cache(currency_cache)

    unresolved_report = {
        "generated_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "count": len(unresolved),
        "tickers": {t: {"attempts": c, "company": lb_map.get(t, "")} for t, c in unresolved.items()},
    }
    _write_json_atomic(UNRESOLVED_PATH, _dumps(unresolved_report, indent=2))

    print(f"\n{'═'*62}")
    print(f"  Coverage: {prev_coverage:.1f}% → {new_coverage:.1f}% (+{newly_resolved} this run)")
    print(f"  Resolved: {len(resolved)} / {len(total_universe)}")
    print(f"  Pending:  {len(pending_set)}  |  Unresolved: {len(unresolved)}")
    if newly_unresolved:
        print(f"  Newly unresolved: {', '.join(newly_unresolved[:10])}")
    remaining = math.ceil(len(pending_set) / batch_size) if batch_size > 0 else "?"
    print(f"  Projected runs to full coverage: ~{remaining}")
    print(f"{'═'*62}\n")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="conviction.fetch_stock_details")
    p.add_argument("--batch-size",   type=int, default=BATCH_SIZE_DEFAULT)
    p.add_argument("--refresh-size", type=int, default=100)
    p.add_argument("--fill-pending", action="store_true")
    p.add_argument("--dry-run",      action="store_true")
    p.add_argument("--tickers",      type=str, default=None)
    p.add_argument("--refresh-desc", action="store_true")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    tickers_filter = None
    if args.tickers:
        tickers_filter = [t.strip().upper() for t in args.tickers.split(",")]
    build_details(
        batch_size=args.batch_size,
        refresh_size=args.refresh_size,
        tickers_filter=tickers_filter,
        fill_pending_only=args.fill_pending,
        dry_run=args.dry_run,
        refresh_descriptions=args.refresh_desc,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
