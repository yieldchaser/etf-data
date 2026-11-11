"""
markets/metadata.py — Canonical metadata for yfinance symbols, FRED series, and sim underlyings.
"""
from __future__ import annotations

YF_SYMBOLS: dict[str, str] = {
    # Indices
    "SPX":    "^GSPC",
    "NDX":    "^NDX",
    "DJI":    "^DJI",
    "RUT":    "^RUT",
    "NIKKEI": "^N225",
    "DAX":    "^GDAXI",
    "FTSE":   "^FTSE",
    "SENSEX": "^BSESN",
    "NIFTY":  "^NSEI",
    "HSI":    "^HSI",
    # Commodities
    "GLD":    "GC=F",
    "SLV":    "SI=F",
    "PL":     "PL=F",
    "PA":     "PA=F",
    "CL":     "CL=F",
    "BZ":     "BZ=F",
    "NG":     "NG=F",
    "HG":     "HG=F",
    # FX
    "USDINR": "INR=X",
    "USDSGD": "SGD=X",
    "EURUSD": "EURUSD=X",
    # Crypto
    "BTC":    "BTC-USD",
    "ETH":    "ETH-USD",
    # Phase 4: Leveraged ETN underlyings
    "NVDA":   "NVDA",
    "AAPL":   "AAPL",
    "TSLA":   "TSLA",
    "GOOG":   "GOOG",
    "GOOGL":  "GOOGL",
    "MSFT":   "MSFT",
    "AMZN":   "AMZN",
    "META":   "META",
}

YF_META: dict[str, dict] = {
    "SPX":    {"label": "S&P 500 (SPX)",            "kind": "indices"},
    "NDX":    {"label": "Nasdaq 100 (NDX)",          "kind": "indices"},
    "DJI":    {"label": "Dow Jones (DJI)",           "kind": "indices"},
    "RUT":    {"label": "Russell 2000 (RUT)",        "kind": "indices"},
    "NIKKEI": {"label": "Nikkei 225 (NIKKEI)",      "kind": "indices"},
    "DAX":    {"label": "DAX (DAX)",                 "kind": "indices"},
    "FTSE":   {"label": "FTSE 100 (FTSE)",           "kind": "indices"},
    "SENSEX": {"label": "BSE Sensex (SENSEX)",       "kind": "indices"},
    "NIFTY":  {"label": "Nifty 50 (NIFTY)",         "kind": "indices"},
    "HSI":    {"label": "Hang Seng (HSI)",           "kind": "indices"},
    "GLD":    {"label": "Gold (GC=F)",               "kind": "commodities"},
    "SLV":    {"label": "Silver (SI=F)",             "kind": "commodities"},
    "PL":     {"label": "Platinum (PL=F)",           "kind": "commodities"},
    "PA":     {"label": "Palladium (PA=F)",          "kind": "commodities"},
    "CL":     {"label": "WTI Crude (CL=F)",         "kind": "commodities"},
    "BZ":     {"label": "Brent Crude (BZ=F)",       "kind": "commodities"},
    "NG":     {"label": "Natural Gas (NG=F)",        "kind": "commodities"},
    "HG":     {"label": "Copper (HG=F)",             "kind": "commodities"},
    "USDINR": {"label": "USD/INR (INR=X)",           "kind": "fx"},
    "USDSGD": {"label": "USD/SGD (SGD=X)",           "kind": "fx"},
    "EURUSD": {"label": "EUR/USD (EURUSD=X)",        "kind": "fx"},
    "BTC":    {"label": "Bitcoin (BTC-USD)",         "kind": "crypto"},
    "ETH":    {"label": "Ethereum (ETH-USD)",        "kind": "crypto"},
}

FRED_SERIES: dict[str, str] = {
    "VIXCLS":  "CBOE VIX (1-month implied vol)",
    "VXNCLS":  "CBOE Nasdaq VIX",
    "VIX9D":   "9-day VIX",
    "VIX3M":   "3-month VIX",
    "VIX6M":   "6-month VIX",
    "VVIX":    "VIX of VIX (vol of vol)",
    "MOVE":    "ICE BofA MOVE (treasury vol)",
    "DGS10":   "10Y Treasury constant maturity rate",
    "DGS2":    "2Y Treasury",
    "T10Y2Y":  "10Y–2Y spread",
    "SOFR":    "Secured Overnight Financing Rate",
    "DFF":     "Effective Fed Funds Rate",
    "DEXINUS": "India–US exchange rate",
    "DEXSIUS": "Singapore–US exchange rate",
}

FRED_META: dict[str, dict] = {
    "VIXCLS":  {"label": "VIX (1M implied vol)",     "kind": "vol"},
    "VXNCLS":  {"label": "Nasdaq VIX",               "kind": "vol"},
    "VIX9D":   {"label": "VIX 9-day",                "kind": "vol"},
    "VIX3M":   {"label": "VIX 3-month",              "kind": "vol"},
    "VIX6M":   {"label": "VIX 6-month",              "kind": "vol"},
    "VVIX":    {"label": "VVIX (vol of vol)",         "kind": "vol"},
    "MOVE":    {"label": "MOVE Index (Treasury vol)", "kind": "vol"},
    "DGS10":   {"label": "10Y Treasury Yield",        "kind": "rates"},
    "DGS2":    {"label": "2Y Treasury Yield",         "kind": "rates"},
    "T10Y2Y":  {"label": "10Y–2Y Spread",             "kind": "rates"},
    "SOFR":    {"label": "SOFR",                      "kind": "rates"},
    "DFF":     {"label": "Effective Fed Funds Rate",  "kind": "rates"},
    "DEXINUS": {"label": "India–US FX (DEXINUS)",     "kind": "rates"},
    "DEXSIUS": {"label": "Singapore–US FX (DEXSIUS)", "kind": "rates"},
}

LEVERAGED_UNDERLYINGS: list[str] = [
    "NVDA", "AAPL", "TSLA", "GOOG", "GOOGL", "MSFT", "AMZN", "META", "SPX", "NDX"
]
