"""Canonical ETF Instrument Taxonomy, Time-Versioned Leverage & Pair Classification.

Single source of truth unifying all 160 catalog instruments across:
- Canonical underlying identifiers (`underlying_id`) & descriptive labels (`underlying_label`).
- Time-versioned leverage tracking corporate transitions (e.g. UVXY, SVXY on 2018-02-28).
- Signed Market Beta Polarity tracking economic market exposure direction.
- Strict Symmetric vs Asymmetric Hedge pair predicates.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = REPO_ROOT / "docs" / "data" / "flows" / "catalog.json"

# Canonical Underlying Mapping: ticker -> (underlying_id, underlying_label)
TAXONOMY_MAP: dict[str, tuple[str, str]] = {
    # 1. Broad Market Equity Index
    "MIDU": ("SP400", "S&P MidCap 400"),
    "QLD": ("NDX", "NASDAQ-100"),
    "QQQM": ("NDX", "NASDAQ-100"),
    "SPHQ": ("SPX_QUALITY", "S&P 500 Quality"),
    "SPMO": ("SPX_MOMENTUM", "S&P 500 Momentum"),
    "SPXL": ("SPX", "S&P 500"),
    "SPXS": ("SPX", "S&P 500"),
    "SQQQ": ("NDX", "NASDAQ-100"),
    "SSO": ("SPX", "S&P 500"),
    "TNA": ("RUT", "Russell 2000"),
    "TQQQ": ("NDX", "NASDAQ-100"),
    "TZA": ("RUT", "Russell 2000"),
    "UDOW": ("DJIA", "Dow Jones Industrial Average"),
    "UPRO": ("SPX", "S&P 500"),
    "URTY": ("RUT", "Russell 2000"),

    # 2. Technology, Semiconductor & Thematic
    "BIB": ("BIOTECH_NDX", "Nasdaq Biotechnology"),
    "CWEB": ("CHINA_INET", "China Internet"),
    "DFEN": ("AERO_DEF", "Aerospace & Defense"),
    "HIBL": ("SPX_HIGHBETA", "S&P 500 High Beta"),
    "LABU": ("BIOTECH_SP", "S&P Biotechnology"),
    "MAGS": ("MAG7", "Magnificent Seven"),
    "MAGX": ("MAG7", "Magnificent Seven"),
    "MSOX": ("CANNABIS", "U.S. Cannabis"),
    "NAIL": ("HOMEBUILDERS", "Homebuilders"),
    "QQQU": ("MAG7", "Magnificent Seven"),
    "ROM": ("US_TECH", "U.S. Technology"),
    "SMH": ("SEMIS", "Semiconductors"),
    "SMHX": ("SEMIS", "Semiconductors"),
    "SOXL": ("SEMIS", "Semiconductors"),
    "SOXS": ("SEMIS", "Semiconductors"),
    "TARK": ("ARKK_INNOV", "ARK Innovation Strategy"),
    "TECL": ("US_TECH", "U.S. Technology"),
    "USD": ("SEMIS", "Semiconductors"),
    "WEBL": ("DJ_INTERNET", "Dow Jones Internet"),
    "XBI": ("BIOTECH_SP", "S&P Biotechnology"),

    # 3. Sector Specific Leveraged
    "CURE": ("HEALTHCARE", "Health Care Select"),
    "DIG": ("OIL_GAS", "Oil & Gas"),
    "DPST": ("US_REG_BANKS", "Regional Banking"),
    "DRN": ("US_REIT", "U.S. Real Estate"),
    "ERX": ("ENERGY_SELECT", "Energy Select"),
    "FAS": ("US_FIN", "Financials Select"),
    "PILL": ("PHARMA", "Pharmaceuticals"),
    "RETL": ("RETAIL", "Retail"),
    "URE": ("US_REIT", "U.S. Real Estate"),
    "UYG": ("US_FIN", "Financials Select"),
    "WANT": ("CONS_DISC", "Consumer Discretionary"),

    # 4. Single-Stock Leveraged — AI, Semis & High-Beta Tech
    "AAOX": ("AAOI", "Applied Optoelectronics"),
    "AMDL": ("AMD", "Advanced Micro Devices"),
    "APLX": ("APLD", "Applied Digital"),
    "APPX": ("APP", "AppLovin"),
    "ARMG": ("ARM", "ARM Holdings"),
    "ASMG": ("ASML", "ASML Holding"),
    "AVGX": ("AVGO", "Broadcom"),
    "AXTX": ("AXTI", "AXT Inc"),
    "COHX": ("COHR", "Coherent Corp"),
    "CRDU": ("CRDO", "Credo Technology"),
    "CRML": ("CRM", "Salesforce"),
    "CRWL": ("CRWD", "CrowdStrike"),
    "CSEX": ("CLS", "Celestica"),
    "CWVX": ("CRWV", "CoreWeave"),
    "DLLL": ("DELL", "Dell Technologies"),
    "GLWG": ("GLW", "Corning"),
    "INTW": ("INTC", "Intel"),
    "LABX": ("ALAB", "Astera Labs"),
    "LITX": ("LITE", "Lumentum Holdings"),
    "LRCU": ("LRCX", "Lam Research"),
    "MULL": ("MU", "Micron Technology"),
    "MUU": ("MU", "Micron Technology"),
    "MVLL": ("MRVL", "Marvell Technology"),
    "NEBX": ("NBIS", "Nebius Group"),
    "NOWL": ("NOW", "ServiceNow"),
    "NVD": ("NVDA", "NVIDIA"),
    "NVDL": ("NVDA", "NVIDIA"),
    "NVDS": ("NVDA", "NVIDIA"),
    "NVDU": ("NVDA", "NVIDIA"),
    "NVDX": ("NVDA", "NVIDIA"),
    "NVTX": ("NVTS", "Navitas Semiconductor"),
    "ORCX": ("ORCL", "Oracle"),
    "PANG": ("PANW", "Palo Alto Networks"),
    "PLTD": ("PLTR", "Palantir Technologies"),
    "PLTU": ("PLTR", "Palantir Technologies"),
    "PTIR": ("PLTR", "Palantir Technologies"),
    "QBTX": ("QBTS", "D-Wave Quantum"),
    "QCML": ("QCOM", "Qualcomm"),
    "RGTU": ("RGTI", "Rigetti Computing"),
    "RKLX": ("RKLB", "Rocket Lab"),
    "SMCX": ("SMCI", "Super Micro Computer"),
    "SNOU": ("SNOW", "Snowflake"),
    "SNXX": ("SNDK", "SanDisk"),
    "STXX": ("STX", "Seagate Technology"),
    "TEMT": ("TEM", "Tempus AI"),
    "TSMU": ("TSM", "Taiwan Semiconductor"),
    "WDCX": ("WDC", "Western Digital"),

    # 5. Single-Stock Leveraged — Mega-Cap Giants, Crypto & Consumer
    "AAPU": ("AAPL", "Apple"),
    "AMZU": ("AMZN", "Amazon"),
    "ASTX": ("ASTS", "AST SpaceMobile"),
    "BABX": ("BABA", "Alibaba"),
    "BEX": ("BE", "Bloom Energy"),
    "CONI": ("COIN", "Coinbase"),
    "CONL": ("COIN", "Coinbase"),
    "COTG": ("COST", "Costco Wholesale"),
    "FBL": ("META", "Meta Platforms"),
    "GEVX": ("GEV", "GE Vernova"),
    "GGLL": ("GOOGL", "Alphabet"),
    "GOOX": ("GOOGL", "Alphabet"),
    "HOOG": ("HOOD", "Robinhood Markets"),
    "IREX": ("IREN", "IREN"),
    "LLYX": ("LLY", "Eli Lilly"),
    "LULG": ("LULU", "Lululemon Athletica"),
    "METU": ("META", "Meta Platforms"),
    "MSFL": ("MSFT", "Microsoft"),
    "MSFU": ("MSFT", "Microsoft"),
    "MSTU": ("MSTR", "MicroStrategy"),
    "MSTX": ("MSTR", "MicroStrategy"),
    "MSTZ": ("MSTR", "MicroStrategy"),
    "NFXL": ("NFLX", "Netflix"),
    "SHPU": ("SHOP", "Shopify"),
    "SMST": ("MSTR", "MicroStrategy"),
    "SMU": ("SMR", "NuScale Power"),
    "SOFX": ("SOFI", "SoFi Technologies"),
    "TSLL": ("TSLA", "Tesla"),
    "TSLQ": ("TSLA", "Tesla"),
    "TSLR": ("TSLA", "Tesla"),
    "TSLT": ("TSLA", "Tesla"),
    "TSLZ": ("TSLA", "Tesla"),
    "UBRL": ("UBER", "Uber Technologies"),
    "UPSX": ("UPST", "Upstart Holdings"),
    "WULX": ("WULF", "TeraWulf"),

    # 6. Commodities, Energy & Volatility
    "AGQ": ("SILVER", "Silver"),
    "GDXU": ("GOLD_MINERS", "Gold Miners"),
    "JNUG": ("JR_GOLD_MINERS", "Junior Gold Miners"),
    "NUGT": ("GOLD_MINERS", "Gold Miners"),
    "SCO": ("CRUDE_OIL", "Crude Oil"),
    "SVIX": ("VIX_ST_FUTURES", "S&P 500 VIX Short-Term Futures"),
    "SVXY": ("VIX_ST_FUTURES", "S&P 500 VIX Short-Term Futures"),
    "UCO": ("CRUDE_OIL", "Crude Oil"),
    "UGL": ("GOLD", "Gold"),
    "UVIX": ("VIX_ST_FUTURES", "S&P 500 VIX Short-Term Futures"),
    "UVXY": ("VIX_ST_FUTURES", "S&P 500 VIX Short-Term Futures"),

    # 7. Fixed Income, Currencies & Crypto
    "BITU": ("BTC", "Bitcoin"),
    "BITX": ("BTC", "Bitcoin"),
    "ETHU": ("ETH", "Ether"),
    "IBIT": ("BTC", "Bitcoin"),
    "TLT": ("UST_20Y", "20+ Year Treasury"),
    "TMF": ("UST_20Y", "20+ Year Treasury"),
    "TMV": ("UST_20Y", "20+ Year Treasury"),
    "TYD": ("UST_7_10Y", "7-10 Year Treasury"),
    "UBT": ("UST_20Y", "20+ Year Treasury"),
    "YCL": ("JPY_USD", "JPY/USD"),

    # 8. International / Country
    "BRZU": ("BRAZIL", "Brazil"),
    "CHAU": ("CHINA_A", "China A-Shares"),
    "EDC": ("EMERGING_MKTS", "MSCI Emerging Markets"),
    "EURL": ("EUROPE", "MSCI Europe"),
    "INDL": ("INDIA", "India"),
    "KORU": ("SOUTH_KOREA", "South Korea"),
    "MEXX": ("MEXICO", "Mexico"),
    "YANG": ("CHINA_50", "China 50"),
    "YINN": ("CHINA_50", "China 50"),

    # 10. Core Benchmarks & Anchors (already listed under relevant underlyings)
    "ARKK": ("ARKK_INNOV", "ARK Innovation Strategy"),
    "COWZ": ("CASH_COWS", "Pacer US Cash Cows 100"),
}

# Volatility ETP corporate action transition date (Volmageddon)
VOLMAGEDDON_CUTOFF = "2018-02-28"

# Time-versioned leverage schedules
LEVERAGE_SCHEDULES: dict[str, list[dict[str, Any]]] = {
    "UVXY": [
        {"until": "2018-02-27", "leverage": 2.0},
        {"since": "2018-02-28", "leverage": 1.5},
    ],
    "SVXY": [
        {"until": "2018-02-27", "leverage": -1.0},
        {"since": "2018-02-28", "leverage": -0.5},
    ],
}


def get_underlying_id(ticker: str) -> str:
    """Return the canonical underlying ID for an instrument."""
    entry = TAXONOMY_MAP.get(ticker)
    if entry:
        return entry[0]
    return ticker


def get_underlying_label(ticker: str) -> str:
    """Return the descriptive underlying label for an instrument."""
    entry = TAXONOMY_MAP.get(ticker)
    if entry:
        return entry[1]
    return ticker


def get_taxonomy_record(ticker: str) -> dict[str, Any]:
    """Return full taxonomy record for an instrument."""
    entry = TAXONOMY_MAP.get(ticker)
    uid = entry[0] if entry else ticker
    lbl = entry[1] if entry else ticker
    return {
        "underlying_id": uid,
        "underlying_label": lbl,
        "leverage_schedule": LEVERAGE_SCHEDULES.get(ticker),
    }


def leverage_at_date(ticker: str, date_str: str = "", default_lev: float = 1.0) -> float:
    """Return time-versioned stated leverage for a ticker on a given date.

    Handles the Feb 28, 2018 Volmageddon transition:
    - UVXY: +2.0x pre-2018-02-28, +1.5x on/after.
    - SVXY: -1.0x pre-2018-02-28, -0.5x on/after.
    """
    if ticker == "UVXY":
        if date_str and date_str < VOLMAGEDDON_CUTOFF:
            return 2.0
        return 1.5
    if ticker == "SVXY":
        if date_str and date_str < VOLMAGEDDON_CUTOFF:
            return -1.0
        return -0.5
    return default_lev


def market_beta_polarity(ticker: str, leverage_value: float = 1.0) -> int:
    """Return signed economic market beta polarity (+1 or -1).

    Long VIX products (UVIX, UVXY) have positive leverage on the VIX futures
    index, but represent SHORT market beta (-1).
    Short VIX products (SVIX, SVXY) have negative leverage on the VIX futures
    index, but represent LONG market beta carry (+1).
    All other ETFs: +1 if bull (leverage_value > 0), else -1.
    """
    if ticker in ("UVIX", "UVXY"):
        return -1
    if ticker in ("SVIX", "SVXY"):
        return +1
    return 1 if leverage_value > 0 else -1


def classify_pair(
    inst_a: dict[str, Any],
    inst_b: dict[str, Any],
    date_str: str = "",
) -> str | None:
    """Classify the pair relationship between two instruments.

    Returns:
    - 'symmetric': same underlying_id, opposite signs, and abs(|L_a| - |L_b|) < 1e-9.
    - 'asymmetric_hedge': same underlying_id, opposite signs, and |L_a| != |L_b|.
    - None: different underlyings or same sign exposure.
    """
    ticker_a = inst_a.get("ticker", "")
    ticker_b = inst_b.get("ticker", "")
    if not ticker_a or not ticker_b or ticker_a == ticker_b:
        return None

    uid_a = get_underlying_id(ticker_a)
    uid_b = get_underlying_id(ticker_b)
    if uid_a != uid_b:
        return None

    lev_a_default = float(inst_a.get("leverage_value", 1.0))
    lev_b_default = float(inst_b.get("leverage_value", 1.0))

    la = leverage_at_date(ticker_a, date_str, lev_a_default)
    lb = leverage_at_date(ticker_b, date_str, lev_b_default)

    # Must be opposite exposure signs
    if la * lb >= 0:
        return None

    if abs(abs(la) - abs(lb)) < 1e-9:
        return "symmetric"
    return "asymmetric_hedge"


def are_symmetric_pair(
    inst_a: dict[str, Any],
    inst_b: dict[str, Any],
    date_str: str = "",
) -> bool:
    """Return True iff instruments form an exact magnitude-matched symmetric pair."""
    return classify_pair(inst_a, inst_b, date_str) == "symmetric"


def are_asymmetric_hedge_pair(
    inst_a: dict[str, Any],
    inst_b: dict[str, Any],
    date_str: str = "",
) -> bool:
    """Return True iff instruments form an asymmetric hedge pair (opposite signs, unequal magnitudes)."""
    return classify_pair(inst_a, inst_b, date_str) == "asymmetric_hedge"


def get_leverage_schedule(ticker: str) -> list[dict[str, Any]] | None:
    """Return corporate leverage transition schedule if instrument is time-varying."""
    return LEVERAGE_SCHEDULES.get(ticker)


def get_all_tickers_for_underlying(underlying_id: str) -> list[str]:
    """Return all tickers tracking a given underlying ID."""
    return [t for t, (uid, _) in TAXONOMY_MAP.items() if uid == underlying_id]


def get_underlying_complexes() -> dict[str, list[str]]:
    """Return all underlying complexes mapped to their constituent tickers."""
    complexes: dict[str, list[str]] = {}
    for ticker, (uid, _) in TAXONOMY_MAP.items():
        complexes.setdefault(uid, []).append(ticker)
    return complexes
