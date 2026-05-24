
# 📈 Predator Protocol: Automated ETF Holdings Pipeline

**Automated Financial Data Pipeline & Institutional Conviction Scanner**
*Tracks daily holdings for 30 Smart-Beta ETFs across Pacer, First Trust, Alpha Architect, Invesco, BlackRock, John Hancock, PIMCO, Avantis, VictoryShares, and Virtus to detect early institutional accumulation. The ETF count is derived from `config.yaml::etfs[]` — when that list changes, this README must be updated in the same commit.*

---

## 🏗️ System Architecture

This system operates as a **five-component pipeline**:
1. **Data Ingestion (Python/GitHub Actions):** Automated scraping, cleaning, and archival of daily ETF holdings across 30 Smart-Beta ETFs.
2. **Scoring Engine (Excel/Power Query):** A multi-factor scoring engine that ranks stocks based on cross-ETF conviction, factor weighting, and accumulation signals.
3. **Live Dashboard (GitHub Pages):** Institutional-grade interactive frontend with leaderboard, velocity engine, burst detection, and structural intelligence.
4. **Extended Scraper (Playwright/Bridge):** Browser-automated ingestion for 8 ETFs requiring JS rendering, with a fault-isolated bridge pipeline.
5. **Markets Intelligence Platform (FRED/yfinance/Excel):** Monthly EOP close data for 25+ global assets (gold from 1833, S&P 500 from 1871) and 7 CBOE vol indices, powering a multi-view analytical dashboard.

---

## 🚀 Component 1: Data Ingestion (GitHub Actions)

### Capabilities
This project runs automatically via **GitHub Actions** to:
1. **Scrape** official ETF issuer websites for daily holdings data.
2. **Standardize** columns across different issuers (Ticker, Name, Weight, Date).
3. **Intelligent Deduplication:** Prevents redundant commits if data hasn't changed.
4. **Dual Archiving:**
   * **Daily Snapshots:** `data/history/YYYY/MM/DD/master_archive.csv`
   * **Giant History:** `data/all_history.csv` (Single append-only file for backtesting).

### Tech Stack
* **Python 3.x**
* **Selenium & ChromeDriver:** For navigating complex JS-heavy sites (Pacer, First Trust). Lazy-imported so the module can be used without Selenium installed (tests and site build don't need it).
* **Pandas:** For data cleaning and CSV management.
* **"Nuclear" Date Hunter:** Custom Regex logic to find hidden "As Of" dates in raw HTML source code.

### Automated Schedule
* **Frequency:** Runs automatically at **14:00 UTC** and **22:00 UTC on weekdays** (cron: `0 14,22 * * 1-5`) via `.github/workflows/daily_scrape.yml`.
* **Data Structure:**
  ```text
  data/
  ├── latest/               # The most recent raw CSV for each ticker
  │   ├── COWZ.csv
  │   ├── SPMO.csv
  │   └── ...
  ├── history/              # Daily snapshots organized by date
  │   └── 2026/
  │       └── 02/
  │           └── 15/
  │               └── master_archive.csv
  └── all_history.csv       # 🌟 THE MASTER FILE: All historical data concatenated
  ```

---

## 📊 Component 2: Scoring Engine (Excel / Power Query)

The Excel dashboard connects directly to `raw.githubusercontent.com` to fetch the latest CSVs, merging them into a single "Master Leaderboard" that scores stocks based on institutional conviction.

### 1. ETF Tier Weights (`ETF_Config` Table)

ETFs are grouped into five tiers based on the strategy signal they emit. Each ticker carries a `Points` weight reflecting the rarity and conviction of that signal. Tickers, tiers, and points below mirror `config.yaml::etfs[]` exactly — that file is the source of truth.

#### 🏷️ Scout tier — Spinoffs & IPOs. Captures structural market inefficiencies.

| Ticker | Name | Points |
| --- | --- | --- |
| `CSD`  | Invesco S&P Spin-Off ETF                          | **40** |
| `FPX`  | First Trust US Equity Opportunities ETF           | **40** |
| `FPXI` | First Trust IPOX International Equity ETF         | **60** |

#### 🏷️ Quant tier — Factor-based momentum (US + International, Large/Mid/Small Cap).

| Ticker | Name | Points |
| --- | --- | --- |
| `QMOM` | Alpha Architect US Quantitative Momentum ETF              | **40** |
| `IMOM` | Alpha Architect International Quantitative Momentum ETF   | **60** |
| `XMMO` | Invesco S&P MidCap Momentum ETF                           | **40** |
| `XSMO` | Invesco S&P SmallCap Momentum ETF                         | **40** |
| `PIE`  | Invesco DWA Emerging Markets Momentum ETF                 | **40** |
| `PDP`  | Invesco DWA Momentum ETF                                  | **40** |
| `DWAS` | Invesco DWA SmallCap Momentum ETF                         | **40** |
| `EEMO` | Invesco S&P Emerging Markets Momentum ETF                 | **60** |
| `PIZ`  | Invesco DWA Developed Markets ex-US Momentum ETF          | **60** |
| `JHMM` | John Hancock Multifactor Mid Cap ETF                      | **40** |
| `JHEM` | John Hancock Multifactor Emerging Markets ETF             | **60** |
| `JHSC` | John Hancock Multifactor Small Cap ETF                    | **40** |
| `MFEM` | PIMCO RAFI Dynamic Multi-Factor Emerging Markets Equity ETF | **60** |
| `JOET` | Virtus Terranova US Quality Momentum ETF                  | **40** |

#### 🏷️ Quality tier — High Free Cash Flow, value, and profitability screening.

| Ticker | Name | Points |
| --- | --- | --- |
| `COWZ` | Pacer US Cash Cows 100 ETF                                | **30** |
| `CALF` | Pacer US Small Cap Cash Cows 100 ETF                      | **30** |
| `SPHQ` | Invesco S&P 500 Quality ETF                               | **30** |
| `IVAL` | Alpha Architect International Quantitative Value ETF      | **30** |
| `QVAL` | Alpha Architect US Quantitative Value ETF                 | **30** |
| `VLUE` | iShares MSCI USA Value Factor ETF                         | **30** |
| `AVSC` | Avantis US Small Cap Value ETF                            | **30** |
| `GRIN` | VictoryShares International Free Cash Flow Growth ETF     | **30** |

#### 🏷️ Trend tier — Broad momentum and high-beta validation.

| Ticker | Name | Points |
| --- | --- | --- |
| `SPMO` | Invesco S&P 500 Momentum ETF                              | **10** |
| `SPHB` | Invesco S&P 500 High Beta ETF                             | **10** |
| `RPG`  | Invesco S&P 500 Pure Growth ETF                           | **10** |

#### 🏷️ Blob tier — Market-cap weighted benchmarks (confirmation only).

| Ticker | Name | Points |
| --- | --- | --- |
| `XLG`  | Invesco S&P 500 Top 50 ETF                                | **2** |
| `QQQM` | Invesco NASDAQ 100 ETF                                    | **2** |

### 2. The Algorithm

**`Final Score = Σ (Single ETF Scores)`**

For each holding, the score is calculated as:

* **Rank Multiplier:**
  * Top 10 Rank: `1.5x`
  * Top 30 Rank: `1.2x`
  * Rank > 30: `1.0x`

* **New Entrant Bonus:**
  * If `Status = "NEW"` AND `Category` is **Scout** or **Quant**:
  * **Bonus = Points × 5** (e.g., +200 pts).

---

## 🚩 Output Flags

* **HIGH CONVICTION:** `Count of Unique ETFs` ≥ 4. Broad consensus across multiple strategies.
* **SPECULATIVE BETA:** Present in Trend tier but absent in Quality or Scout tiers. High volatility momentum without fundamental support.

---

## 🔧 Maintenance & Operations

### Daily Usage
1. **Check Status:** Verify the GitHub Action run is green (Success).
2. **Update View:** Open Excel and click **Data > Refresh All**.

### Troubleshooting
* **Ghost Rows:** Filter the `Ticker` column to exclude `(null)` values.
* **Privacy Errors:** Select **"Ignore Privacy Levels"** in Excel to allow merging of GitHub data with the local Config table.

---

## 🦅 Component 3: Live Dashboard (GitHub Pages)

The site at **https://yieldchaser.github.io/etf-data/** runs the Predator Protocol scoring algorithm directly on `data/all_history.csv` and renders a premium, institutional-grade interactive dashboard.

### Features

- **Global Leaderboard & Insights:** ~3,950 unique tickers ranked by Final Alpha Score with day-over-day deltas, HC streaks, percentile bars, velocity/burst badges, and auto-generated explainer lines.
- **Per-ETF Telemetry:** 50-slice SVG donut charts, holdings sorted by rank/weight/delta, tier pie chart.
- **ETF Sidebar Metadata:** Rebalance/reconstitution schedule, expandable rows, holdings pie chart.
- **Deep-Dive Stock Analytics (`stock.html`):** Score history, rank history, per-ETF rank history, signal timeline (Gantt), score decomposition bar, tier breadth, quality adoption/defection chips.
- **Velocity Engine:** Composite velocity score, 4σ burst detection, sector/country flow overlay (Unknown entries excluded), watchlist with compare mode.
- **Strategy Backtest (`/backtest.html`):** 5 strategies with cumulative returns, stats table, velocity-vs-return scatter.
- **Sector & Country Flow:** Aggregated velocity-weighted exposure by sector and country. Foreign tickers and currency codes with no metadata are excluded from flow charts.

### Architecture

```
scraper.py → data/all_history.csv
    → predator/build.py (sanitizer + scoring + velocity + flow + overlap)
    → docs/data/*.json
    → GitHub Pages (docs/)
```

Auto-rebuilds within ~10 min of every push via `.github/workflows/build_site.yml`.

### Pages

| Page | URL | Description |
|------|-----|-------------|
| Dashboard | `/` | Leaderboard, ETFs, Changes, Watchlist tabs |
| Stock Detail | `/stock.html?t=GEV` | Per-ticker deep dive with charts |
| Markets | `/markets.html` | Multi-asset returns intelligence |
| Simulator | `/sim.html` | Leveraged ETN NAV simulator |
| Backtest | `/backtest.html` | Strategy performance comparison |

### Local development

```bash
pip install -r requirements.txt
python -m pytest tests/ -v                         # 33 tests
python -m predator.build                           # builds docs/data/*
python -m predator.markets_history --full-refresh  # fetch + write market_returns.json
python -m predator.ingest_mega_xl                  # merge Excel historical data
python -m predator.vol_history --full-refresh      # fetch + write vol_history.json
python -m http.server -d docs 8000                 # preview at http://localhost:8000
```

### Tuning

Edit `config.yaml`:
- `etfs[].points` — Scout/Quant=40 (FPXI/IMOM=60), Quality=30, Trend=10, Blob=2
- `rank_breakpoints` — top-10 = 1.5×, top-30 = 1.2× multipliers
- `new_lookback_days` — how long a ticker must have been absent to count as NEW
- `high_conviction_min_etfs` — threshold for HIGH CONVICTION flag

---

## 🔌 Component 4: Extended Scraper

The primary `scraper.py` handles 21 ETFs from issuers exposing stable HTML, CSV, or JSON endpoints. The remaining 8 ETFs require Playwright browser automation, PDF parsing, or cookie-bound APIs — these live in `scripts/etf_holdings_scraper_v42.py` and run as an isolated subprocess.

### V42_ETFS Ownership Manifest

`V42_ETFS = frozenset({VLUE, AVSC, GRIN, JHMM, JHEM, JHSC, MFEM, JOET})`

### Bridge Pipeline

```
v42 subprocess → etf_holdings_YYYYMMDD.csv (Canonical_CSV)
    → clean_canonical_csv() → (cleaned_df, errors)
    → bridge_write_all_sinks(cleaned_df)
        ├── data/all_history.csv                       (Giant_History — append + dedupe)
        ├── data/latest/{TICKER}.csv                   (one per V42 ETF)
        └── data/history/YYYY/MM/DD/master_archive.csv (dated daily snapshot)
```

Idempotency enforced on composite key `(ETF_Ticker, ticker, Holdings_As_Of)`. The 14:00 and 22:00 UTC reruns are safe to repeat.

---

## 🌍 Component 5: Markets Intelligence Platform

A standalone data pipeline and multi-view dashboard providing cross-asset return analytics for 25 global instruments with deep historical data (gold from 1833, S&P 500 from 1871, Dow Jones from 1885).

### Data Pipelines

| Module | Output | Description |
|--------|--------|-------------|
| `predator/markets_history.py` | `docs/data/market_returns.json` | Fetches monthly EOP close for ~25 assets from yfinance. |
| `predator/ingest_mega_xl.py` | `docs/data/market_returns.json` | Merges `Mega_Markets_Historical.xlsx` (5 sheets, 25 series) — Excel data takes priority for overlapping months, extending history back to 1833. |
| `predator/vol_history.py` | `docs/data/vol_history.json` | Fetches daily close for 7 CBOE volatility indices (VIX, GVZ, OVX, VXN, RVX, VXD, VXEEM) from FRED. |

### Asset Coverage

| Category | Assets |
|----------|--------|
| Equity | S&P 500 (1871), DJIA (1885), NASDAQ, NASDAQ-100, Nikkei 225, BSE Sensex, DAX, FTSE 100, Hang Seng, ASX 200, Bovespa, Shanghai Composite |
| Precious Metals | Gold (1833), Silver, Platinum, Palladium |
| Energy | WTI Crude, Brent Crude |
| Commodities | Copper, Corn, Wheat, Soybeans, Sugar, Coffee, Cotton |

### Markets Page (`/markets.html`) — 9 Tabs

| Tab | Description |
|-----|-------------|
| **Return Matrix** | Annual returns heatmap (newest year left), monthly drilldown, currency lens (Local/USD/INR/Gold/Silver), real returns, z-score color mode, event annotations, CSV export. Click any asset name → annual returns bar chart with CAGR reference line + price history. |
| **Price Log** | Daily price log for 23 tracked instruments |
| **Yield History** | US rate history line chart + annual yield heatmap |
| **Periodic Table** | Callan-style rank rotation table |
| **Drawdowns** | Underwater curves for all assets, grouped category sidebar with search, time range filter (All/50Y/30Y/20Y/10Y), max drawdown table with current DD column |
| **Hold Lab** | Rolling N-year returns — best/worst/median |
| **Seasonality** | Month-of-year average return heatmap |
| **Correlation** | Correlation matrix + Growth of $100 log-scale chart |
| **Volatility** | 4 sub-panels: current level tiles, monthly z-score heatmap, historical chart, cross-asset bar |

### Asset Detail Panel

Click any asset name in the Return Matrix to open a detail panel with:
- **Annual Returns tab (default):** Year-by-year bar chart (green/red), CAGR dashed reference line, partial-year bars at 40% opacity, hover shows exact return %. Respects active currency lens.
- **Price History tab:** Full price history line chart with log/linear toggle.
- Stats strip: CAGR, annualised vol, best/worst year, % positive years, data since.

---

## 🧪 Testing

**33 tests** (27 scoring + 6 bridge) with full coverage of the scoring algorithm, temporal analytics, and bridge pipeline contract.

- **Property-based testing** via [Hypothesis](https://hypothesis.readthedocs.io/) validates the bridge contract against randomized inputs.
- **Deterministic unit tests** cover scoring math, rank multipliers, NEW-entrant bonuses, velocity calculations, and burst detection thresholds.

```bash
python -m pytest tests/ -v    # runs all 33 tests
```

---

## ⚙️ CI/CD

| Workflow | Trigger | What it does |
|----------|---------|--------------|
| `daily_scrape.yml` | 14:00 + 22:00 UTC weekdays | Scrapes all 30 ETFs, commits new data, triggers site rebuild |
| `build_site.yml` | After scraper, on code push, manual | Runs tests, builds `docs/data/*.json`, fetches market data (FRED + yfinance + Excel), deploys to GitHub Pages |

Build time: ~10–15 min (market data fetch) on scraper-triggered runs. Code-only pushes run the same pipeline but market data steps complete quickly since data is already cached in CI.

Selenium, curl_cffi, and pdfplumber are **not** installed in the build CI — they're scraper-only dependencies. The `scraper.py` module lazy-imports Selenium so it can be imported without it installed.
