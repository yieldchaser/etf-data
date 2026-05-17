# Phase 3 — Markets Pipeline (yfinance + FRED) and PRICE LOG Widget

Working in `yieldchaser/etf-data`. Phases 1-2 are deployed; bug-fix sweep is merged. Now add a second data pipeline (markets prices, indices, FX, VIX family) alongside the existing ETF holdings pipeline and surface it as a new tab on the dashboard.

## Goals

1. New `markets/` Python package that fetches daily OHLC for indices, commodities, FX, and the VIX family. Writes parquet files into `data/markets/`.
2. Markets build step that consumes `data/markets/*.parquet` and produces `docs/data/markets.json` for the dashboard.
3. New `docs/markets.html` page with a "PRICE LOG" widget — matches the aesthetic of the reference screenshot the user provided (label "PRICE LOG — `<symbol>` · `<N>` sessions"; columns DATE / CLOSE / DAY Δ% / STREAK / LEVEL; ▲N/▼N streak indicators; percentile-of-history bars on the right).
4. Daily GitHub Actions workflow that runs after US close and commits to `data/markets/`. Existing `build_site.yml` already triggers on `data/**` so the site rebuilds automatically.
5. New "MARKETS" tab in `docs/index.html` nav linking to `markets.html`.

## Repo recap

```
.
├── scraper.py                           # untouched
├── config.json                          # untouched (scraper config)
├── data/
│   ├── all_history.csv                  # ETF holdings (existing)
│   └── markets/                         # NEW — created by this phase
│       ├── yf_SPX.parquet
│       ├── yf_NDX.parquet
│       ├── ...
│       ├── fred_VIXCLS.parquet
│       └── ...
├── predator/                            # ETF scoring (Phase 1-2)
├── markets/                             # NEW — this phase
│   ├── __init__.py
│   ├── fetch_yf.py
│   ├── fetch_fred.py
│   ├── build.py
│   └── stats.py
├── tests/
│   ├── test_scoring.py                  # existing
│   └── test_markets.py                  # NEW
├── docs/
│   ├── index.html                       # add MARKETS tab
│   ├── stock.html                       # from Phase 2
│   └── markets.html                     # NEW
└── .github/workflows/
    ├── daily_scrape.yml                 # untouched
    ├── build_site.yml                   # untouched (already triggers on data/**)
    └── fetch_markets.yml                # NEW
```

## Step 1 — yfinance fetcher (`markets/fetch_yf.py`)

Symbols to cover (canonical, used as filenames `yf_<SYMBOL>.parquet`):

| Symbol     | Description                          | yfinance ticker  |
|------------|--------------------------------------|------------------|
| `SPX`      | S&P 500 Index                        | `^GSPC`          |
| `NDX`      | Nasdaq 100                           | `^NDX`           |
| `DJI`      | Dow Jones Industrial Average         | `^DJI`           |
| `RUT`      | Russell 2000                         | `^RUT`           |
| `NIKKEI`   | Nikkei 225                           | `^N225`          |
| `DAX`      | DAX                                  | `^GDAXI`         |
| `FTSE`     | FTSE 100                             | `^FTSE`          |
| `SENSEX`   | BSE Sensex                           | `^BSESN`         |
| `NIFTY`    | Nifty 50                             | `^NSEI`          |
| `HSI`      | Hang Seng                            | `^HSI`           |
| `GLD`      | Gold spot (LBMA)                     | `GC=F`           |
| `SLV`      | Silver spot                          | `SI=F`           |
| `PL`       | Platinum                             | `PL=F`           |
| `PA`       | Palladium                            | `PA=F`           |
| `CL`       | WTI crude oil                        | `CL=F`           |
| `BZ`       | Brent crude                          | `BZ=F`           |
| `NG`       | Natural gas (the user's reference)   | `NG=F`           |
| `HG`       | Copper                               | `HG=F`           |
| `USDINR`   | Dollar–rupee                         | `INR=X`          |
| `USDSGD`   | Dollar–Sing                          | `SGD=X`          |
| `EURUSD`   | Euro–dollar                          | `EURUSD=X`       |
| `BTC`      | Bitcoin                              | `BTC-USD`        |
| `ETH`      | Ethereum                             | `ETH-USD`        |

Required behavior:

```python
# markets/fetch_yf.py
from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd
import yfinance as yf

SYMBOLS = {
    "SPX": "^GSPC", "NDX": "^NDX", "DJI": "^DJI", "RUT": "^RUT",
    "NIKKEI": "^N225", "DAX": "^GDAXI", "FTSE": "^FTSE",
    "SENSEX": "^BSESN", "NIFTY": "^NSEI", "HSI": "^HSI",
    "GLD": "GC=F", "SLV": "SI=F", "PL": "PL=F", "PA": "PA=F",
    "CL": "CL=F", "BZ": "BZ=F", "NG": "NG=F", "HG": "HG=F",
    "USDINR": "INR=X", "USDSGD": "SGD=X", "EURUSD": "EURUSD=X",
    "BTC": "BTC-USD", "ETH": "ETH-USD",
}

def fetch_one(symbol: str, yf_ticker: str, out_dir: Path, period: str = "max") -> None:
    """Fetch full history if file is missing, otherwise only fetch since last close + 1 day."""
    target = out_dir / f"yf_{symbol}.parquet"
    if target.exists():
        existing = pd.read_parquet(target)
        last = pd.to_datetime(existing["Date"]).max()
        start = (last + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        df = yf.download(yf_ticker, start=start, progress=False, auto_adjust=False)
    else:
        existing = pd.DataFrame()
        df = yf.download(yf_ticker, period=period, progress=False, auto_adjust=False)
    if df is None or df.empty:
        return
    df = df.reset_index()
    df.columns = [c if isinstance(c, str) else c[0] for c in df.columns]  # flatten if multi-index
    df = df[["Date", "Open", "High", "Low", "Close", "Volume"]].copy()
    df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
    if not existing.empty:
        df = pd.concat([existing, df]).drop_duplicates(subset=["Date"], keep="last")
    df = df.sort_values("Date")
    df.to_parquet(target, index=False)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/markets")
    ap.add_argument("--only", nargs="*", default=None, help="restrict to these symbols")
    args = ap.parse_args()
    out_dir = Path(args.out); out_dir.mkdir(parents=True, exist_ok=True)
    for sym, ticker in SYMBOLS.items():
        if args.only and sym not in args.only:
            continue
        print(f"  {sym} ({ticker})...")
        try:
            fetch_one(sym, ticker, out_dir)
        except Exception as e:
            print(f"    error: {e}")

if __name__ == "__main__":
    main()
```

## Step 2 — FRED fetcher (`markets/fetch_fred.py`)

Series (requires `FRED_API_KEY` as a GitHub secret — the user should grab one free from `https://fred.stlouisfed.org/docs/api/api_key.html`):

| Series   | Description                                |
|----------|--------------------------------------------|
| `VIXCLS` | CBOE VIX (1-month implied vol)             |
| `VXNCLS` | CBOE Nasdaq VIX                            |
| `VIX9D`  | 9-day VIX                                  |
| `VIX3M`  | 3-month VIX                                |
| `VIX6M`  | 6-month VIX                                |
| `VVIX`   | VIX of VIX (vol of vol)                    |
| `MOVE`   | ICE BofA MOVE (treasury vol)               |
| `DGS10`  | 10Y Treasury constant maturity rate        |
| `DGS2`   | 2Y Treasury                                |
| `T10Y2Y` | 10Y–2Y spread                              |
| `SOFR`   | Secured Overnight Financing Rate           |
| `DFF`    | Effective Fed Funds Rate                   |
| `DEXINUS`| India–US exchange rate                     |
| `DEXSIUS`| Singapore–US exchange rate                 |

Use `requests` (already in `requirements.txt`). Pull JSON from `https://api.stlouisfed.org/fred/series/observations?series_id=...&api_key=...&file_type=json&observation_start=YYYY-MM-DD`. Same incremental-since-last-date pattern as yfinance. Save as `fred_<SERIES>.parquet` with columns `Date`, `value`.

Skip silently with a warning if `FRED_API_KEY` env var is missing (so local runs without the key still work for the yfinance side).

## Step 3 — Stats module (`markets/stats.py`)

Pure function library, no I/O. For a single time series:

```python
def series_stats(df: pd.DataFrame, value_col: str = "Close") -> dict:
    """
    Returns:
        close_today, close_yesterday, day_pct, week_pct, month_pct,
        ytd_pct, all_time_range (low, high), all_time_low_date, all_time_high_date,
        sessions_observed, current_streak (signed int — positive = up days),
        last_20_up, last_20_down,
        percentile_all_time (0-1, where 1 = highest ever)
    """
```

Required logic for `current_streak`: walk back from latest, count consecutive same-direction daily-pct-change days. Output sign matches direction (positive int for up streak, negative for down).

## Step 4 — Markets build (`markets/build.py`)

```python
"""
Markets build — consumes data/markets/*.parquet, produces docs/data/markets.json.
Schema:
  {
    "generated_at_utc": "...",
    "series": [
      {
        "symbol": "NG",
        "label": "Natural Gas (NG=F)",
        "kind": "commodity",
        "stats": { ...series_stats... },
        "log": [{d, close, pct, streak, level}]   # last 200 sessions, page-able client-side
      },
      ...
    ]
  }
"""
```

Sort `series` array by `kind` then `symbol`. Output one JSON file. Total payload should be under 2MB even with 200 rows × 25 symbols.

## Step 5 — `docs/markets.html` PRICE LOG widget

Single-file Alpine.js + Tailwind, same aesthetic as `docs/index.html`. Reuse CSS variables and tier-chip patterns by copy-pasting the `<style>` block (no shared CSS file).

### Layout

```
┌──────────────────────────────────────────────────────────┐
│ PREDATOR PROTOCOL  ›  MARKETS                ← back       │   ← header (reuses index.html style)
│                                                            │
│ ┌──────────┬──────────┬──────────┬──────────┬──────────┐ │   ← KPI strip (4 highlight series)
│ │ SPX      │ VIX      │ GLD      │ NG       │ USDINR    │ │
│ │ 5840 +0.4│ 14.8 -2  │ 2618 +0.8│ 2.96 +2.3│ 84.7 +0.1 │ │
│ └──────────┴──────────┴──────────┴──────────┴──────────┘ │
│                                                            │
│ Categories: [Indices] [Commodities] [FX] [Vol] [Crypto]   │   ← filter chips
│ Search: [...........]                                      │
│                                                            │
│ ┌──── PRICE LOG — NG · 9070+ sessions ─────── Page 1/91 ┐ │   ← one widget per series
│ │ All-time range: $1.04 – $15.38 · Streak: ▲3 up         │ │
│ │                                                          │ │
│ │ DATE         CLOSE   DAY Δ%   STREAK   LEVEL            │ │
│ │ May 15 2026  $2.961  +2.32%   ▲3       ─────● 13%       │ │
│ │ May 14 2026  $2.894  +1.05%   ▲2       ─────● 13%       │ │
│ │ ...                                                       │ │
│ └────────────────────────────────────────────────────────┘ │
│ ┌──── PRICE LOG — SPX · 24800+ sessions ────────────────┐ │
│ │ ...                                                      │ │
│ └────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────┘
```

### PRICE LOG widget behavior

- Header line: `PRICE LOG — <symbol> · <N>+ sessions` (cyan label-style)
- Subheader: `All-time range: $<low> – $<high> · Last 20 sessions: <up>/20 up · Current streak: ▲N up | ▼N down`
- Table columns: DATE, CLOSE (mono), DAY Δ% (green ≥0, red <0), STREAK (▲N green / ▼N red), LEVEL (4px-tall percentile bar showing where today's price sits in the all-time range)
- Pagination: 12 rows per page, Prev/Next at top right
- Hover any row → tooltip with `<date> · <close> · <pct> · <percentile_at_that_date>th percentile all-time`
- Same colour palette as Phase 1 (var(--up), var(--down), var(--cyan), var(--text-3))

### Category filters

- `Indices`: SPX, NDX, DJI, RUT, NIKKEI, DAX, FTSE, SENSEX, NIFTY, HSI
- `Commodities`: GLD, SLV, PL, PA, CL, BZ, NG, HG
- `FX`: USDINR, USDSGD, EURUSD
- `Vol`: VIXCLS, VXNCLS, VIX9D, VIX3M, VIX6M, VVIX, MOVE
- `Rates`: DGS10, DGS2, T10Y2Y, SOFR, DFF
- `Crypto`: BTC, ETH

## Step 6 — GitHub Actions (`/.github/workflows/fetch_markets.yml`)

```yaml
name: Fetch markets data

on:
  schedule:
    - cron: '0 22 * * 1-5'   # 22:00 UTC, weekdays (after US close)
  workflow_dispatch:

permissions:
  contents: write             # to commit data/markets/

jobs:
  fetch:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: '3.12', cache: pip }
      - run: pip install -r requirements.txt
      - run: python -m markets.fetch_yf --out data/markets
      - run: python -m markets.fetch_fred --out data/markets
        env:
          FRED_API_KEY: ${{ secrets.FRED_API_KEY }}
      - name: Commit if changed
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add data/markets
          if git diff --staged --quiet; then
            echo "No changes"
          else
            git commit -m "data: refresh markets"
            git push
          fi
```

This triggers `build_site.yml` (which already watches `data/**`), which now needs to also run the markets build. Update `build_site.yml`'s build step:

```yaml
- name: Build site artifacts
  run: |
    python -m predator.build --source data/all_history.csv --output docs/data --config config.yaml
    python -m markets.build --source data/markets --output docs/data
```

## Step 7 — Wire MARKETS tab into `docs/index.html` navigation

In the `<nav>` tabs block (around line ~205), add a link:

```html
<a href="markets.html" class="px-3 py-1.5 rounded-md border font-medium tracking-wider uppercase transition" style="color: var(--text-2); border-color: var(--border)">
  Markets
</a>
```

Same for `docs/stock.html` if Phase 2 added a nav.

## Step 8 — Update `requirements.txt`

Append:
```
yfinance>=0.2.40
```

`pandas`, `pyarrow`, `requests` are already pinned from Phase 1.

## Definition of Done — Phase 3

1. `python -m markets.fetch_yf --only NG` produces `data/markets/yf_NG.parquet` with at least 1000 rows.
2. `python -m markets.fetch_fred --only VIXCLS` (with `FRED_API_KEY` set) produces `data/markets/fred_VIXCLS.parquet`.
3. `python -m markets.build --source data/markets --output docs/data` produces `docs/data/markets.json` under 2 MB.
4. Navigating to `https://yieldchaser.github.io/etf-data/markets.html` shows a PRICE LOG widget for at least NG, SPX, VIX, and GLD with real data and working pagination.
5. The MARKETS link appears in the nav of the main dashboard.
6. New test `tests/test_markets.py` covers `markets.stats.series_stats` and the streak computation.
7. The `fetch_markets.yml` workflow runs end-to-end manually via `workflow_dispatch` and commits a non-empty `data/markets/` to the repo.

## What you must NOT do

- Do not call yfinance from within the dashboard JS at runtime. All fetching is server-side cron only.
- Do not commit your FRED API key. It goes in repo secrets only.
- Do not introduce a JS bundler. Continue the no-build-step constraint.
- Do not regress the ETF holdings pipeline.
