# 📈 Predator Protocol

> **Automated ETF Holdings Intelligence Platform** — tracks daily holdings across 30 Smart-Beta ETFs, detects early institutional accumulation signals, and delivers a full-stack analytical dashboard with 150+ years of cross-asset market history.

**Live Dashboard:** https://yieldchaser.github.io/etf-data/

---

## Table of Contents

1. [System Overview](#system-overview)
2. [Architecture](#architecture)
3. [Component 1 — Data Ingestion](#component-1--data-ingestion)
4. [Component 2 — Scoring Engine](#component-2--scoring-engine)
5. [Component 3 — Live Dashboard](#component-3--live-dashboard)
6. [Component 4 — Extended Scraper Bridge](#component-4--extended-scraper-bridge)
7. [Component 5 — Markets Intelligence Platform](#component-5--markets-intelligence-platform)
8. [CI/CD Pipeline](#cicd-pipeline)
9. [Testing](#testing)
10. [Local Development](#local-development)
11. [Configuration Reference](#configuration-reference)

---

## System Overview

Predator Protocol is a fully automated financial intelligence system built around a single thesis: **institutional conviction is detectable before it becomes consensus**. Smart-Beta ETFs — momentum, quality, value, spinoff — are run by systematic strategies that rebalance on rules. When multiple independent strategies simultaneously accumulate the same name, that convergence is a signal.

The system operates as five tightly integrated components:

| Component | Role |
|-----------|------|
| **Data Ingestion** | Scrapes 30 ETF issuers daily, normalises holdings, archives to CSV |
| **Scoring Engine** | Multi-factor algorithm: tier weights × rank multipliers × new-entrant bonuses |
| **Live Dashboard** | GitHub Pages SPA — leaderboard, velocity engine, burst detection, structural analytics |
| **Extended Scraper Bridge** | Fault-isolated subprocess for 8 ETFs requiring Playwright/PDF/XLS ingestion |
| **Markets Intelligence Platform** | 150+ years of cross-asset return data, 9-tab analytical dashboard |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│  GitHub Actions — Daily ETF Scrape (14:00 + 22:00 UTC weekdays)     │
│                                                                     │
│  scraper.py (21 primary ETFs)                                       │
│  └── scripts/etf_holdings_scraper_v42.py (8 extended ETFs)          │
│       └── Bridge: clean → dedupe → write                            │
│                                                                     │
│  Output: data/all_history.csv  (append-only, ~100k rows)            │
└──────────────────────────┬──────────────────────────────────────────┘
                           │ triggers
┌──────────────────────────▼──────────────────────────────────────────┐
│  GitHub Actions — Build Site                                        │
│                                                                     │
│  predator/build.py                                                  │
│  ├── Sanitizer (blocked tickers, name patterns, ticker renames)     │
│  ├── Scoring (tier weights × rank mult × new bonus)                 │
│  ├── Temporal analytics (streaks, percentiles, deltas 1/7/14/30/60/90d) │
│  ├── Velocity engine (composite score + 4σ burst detection)         │
│  ├── Structural signals (concentration, tier breadth, quality Δ)    │
│  ├── Flow aggregation (sector + country, Unknown excluded)          │
│  └── ETF overlap matrix (30×30 Jaccard)                             │
│                                                                     │
│  predator/markets_history + ingest_markets_xl.py + vol_history.py   │
│  └── docs/data/market_returns.json (25 assets, gold from 1833)      │
│                                                                     │
│  Output: docs/data/*.json → GitHub Pages                            │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Component 1 — Data Ingestion

### What it does

`scraper.py` runs twice daily on GitHub Actions and scrapes official ETF issuer websites for current holdings. Each run produces a dated snapshot and appends to the master history file.

### ETF Universe (30 ETFs)

The ETF count is derived from `config.yaml::etfs[]` — that file is the source of truth.

| Tier | ETFs | Signal |
|------|------|--------|
| **Scout** (40–60 pts) | CSD, FPX, FPXI | Spinoffs & IPOs — structural market inefficiencies |
| **Quant** (40–60 pts) | QMOM, IMOM, XMMO, XSMO, PIE, PDP, DWAS, EEMO, PIZ, JHMM, JHEM, JHSC, MFEM, JOET | Factor-based momentum (US + International) |
| **Quality** (30 pts) | COWZ, CALF, SPHQ, IVAL, QVAL, VLUE, AVSC, GRIN | Free cash flow, value, profitability |
| **Trend** (10 pts) | SPMO, SPHB, RPG | Broad momentum confirmation |
| **Core** (2 pts) | XLG, QQQM | Mega-cap benchmarks |

International ETFs (FPXI, IMOM, EEMO, PIZ, JHEM, MFEM) carry 60 points instead of their tier default to level the playing field — global names naturally appear in fewer US-focused ETFs.

### Scraper Architecture

- **Selenium + ChromeDriver** — JS-heavy sites (Pacer, First Trust). Lazy-imported so the module can be used without Selenium installed (tests and build CI don't need it).
- **curl_cffi** — TLS fingerprint spoofing for API endpoints that block standard requests.
- **Pandas** — data normalisation, deduplication, CSV management.
- **"Nuclear" Date Hunter** — custom regex that extracts hidden `As Of` dates from raw HTML source, handling every issuer's idiosyncratic date format.

### Data Flow

```
Issuer website
    → scraper.py (fetch + parse + normalise)
    → data/latest/{TICKER}.csv          (current snapshot per ETF)
    → data/history/YYYY/MM/DD/master_archive.csv  (dated daily snapshot, gitignored)
    → data/all_history.csv              (transient working buffer, hydrated from Parquet)
```

**History isolation architecture (2026-06):** `data/all_history.csv` is no longer committed to Git — the daily append was the single biggest driver of `.git` growth (~11 MB blob per commit, poorly compressing). The durable history is now the **year-partitioned Parquet store** at `data/history_parquet/year=YYYY/holdings.parquet`, guarded by a SHA-256 manifest (`CHECKSUMS.json`) with an immutability contract enforced by `tests/test_parquet_immutability.py`. Each scrape run:

1. `scripts/hydrate_csv_from_parquet.py` reconstructs `data/all_history.csv` from the Parquet store (byte-identical to what used to be committed — verified by `scripts/validate_zero_loss.py`).
2. `scraper.py` runs unchanged, appending today's scrape to the CSV.
3. `scripts/migrate_to_parquet.py` folds the new rows into the Parquet store (mandatory, fail-loud — past-year partitions are immutable, current-year is append-only).
4. The local CSV is wiped so `git add data/` never stages it.

The committed daily scrape is now a ~1-line manifest refresh + small Parquet partition delta, down from a multi-megabyte CSV blob. `predator/build.py` reads the Parquet store directly (Parquet-first `fetch_history`), so the build never depends on the transient CSV.

Idempotency is enforced on the composite key `(ETF_Ticker, ticker, Holdings_As_Of)` — reruns within the same UTC day are safe.

### Schedule

```
cron: '0 14,22 * * 1-5'   # 14:00 UTC (10:00 AM ET) + 22:00 UTC (6:00 PM ET), weekdays
```

The 14:00 run targets the primary window after all issuers publish T+1 data. The 22:00 run catches late publishers and serves as a retry.

---

## Component 2 — Scoring Engine

### Score Formula

```
Single Score = (weight% × 100) × tier_points × rank_multiplier + new_entrant_bonus

Final Alpha Score = Σ Single Scores across all ETFs holding this ticker
```

### Tier Points

| Tier | Default Points | Override |
|------|---------------|---------|
| Scout | 40 | FPXI → 60 (international) |
| Quant | 40 | IMOM, EEMO, PIZ, JHEM, MFEM → 60 (international) |
| Quality | 30 | — |
| Trend | 10 | — |
| Core | 2 | — |

### Rank Multiplier

Applied per-ETF based on the ticker's rank within that ETF's holdings:

| Rank | Multiplier |
|------|-----------|
| Top 10 | 1.5× |
| Top 30 | 1.2× |
| > 30 | 1.0× |

### New Entrant Bonus

When a ticker is absent from the 14-day lookback window and re-enters a Scout or Quant ETF:

```
New Bonus = tier_points × 5.0
```

This amplifies early detection of names entering high-signal ETFs for the first time.

### Conviction-Quality Layer (§31.2)

Apex (v3) display ranking multiplies in a conviction-quality term so breadth-filler names (held underweight in many funds) no longer outrank concentrated conviction:

```
m_conv = clip(avg_conviction, 0.50, 1.25) ^ conv_gamma
```

Plus a conviction gate (clip 0.40–1.10) with an escape hatch: the gate is bypassed when the median per-ETF rank is ≤ 10. Raw `final_score` is unchanged — this layer affects display ranking only. Parameters `conv_floor` / `conv_cap` / `conv_gamma` live in `config.yaml`; full math on the methodology page, §3.3–3.4.

### ETF Overlap Discount & Tier Synergy Scaling (Option 1)

To prevent duplicate holdings within highly correlated ETF strategy clusters (e.g. `JHEM` + `MFEM` + `EEMO`) from artificially inflating leaderboard scores:
1. **Jaccard Overlap Matrix**: Pairwise holding similarities are computed across all active ETFs (`similarity[etf1][etf2]`).
2. **Marginal Overlap Discount**: For an asset held by multiple ETFs, subsequent ETF score contributions are scaled down by `max(min_mult, 1.0 - max_sim * 1.5)` relative to previously processed higher-conviction ETFs for that asset.
3. **Multi-Tier Synergy Multiplier**: Multi-factor consensus spanning distinct strategy tiers (e.g. *Quality + Trend + Scout*) earns a `+15%` multiplier per unique tier (`1.0 + (num_unique_tiers - 1) * 0.15`), rewarding cross-strategy alignment over duplicate single-factor stacking.

### Output Flags

| Flag | Condition | Interpretation |
|------|-----------|---------------|
| **HIGH_CONVICTION** | Held by ≥ 4 distinct ETFs | Broad consensus across independent strategies |
| **SPECULATIVE_BETA** | In Trend tier, absent from Quality + Scout | High-volatility momentum without fundamental support |

### Sanitizer

Before scoring, the pipeline filters:
- **Blocked tickers**: currency placeholders (`$USD`, `$EUR`, `$JPY`, etc.), money market funds (`AGPXX`, `FGXXX`), cash instruments
- **Blocked name patterns**: "Money Market", "Securities Lending"
- **Ticker normalisation**: `BRK-B → BRK.B`, `BF/B → BF.B`, `GOOG → GOOGL` (dual-class consolidation)

---

## Component 3 — Live Dashboard

**URL:** https://yieldchaser.github.io/etf-data/

Built with Tailwind CSS + Alpine.js. Zero build step — static HTML/JS served directly from `docs/`. All computation runs client-side from pre-built JSON payloads.

### Pages

| Page | Path | Description |
|------|------|-------------|
| Dashboard | `/` | Main leaderboard, ETFs tab, Changes tab, Watchlist |
| Stock Detail | `/stock.html?t=TICKER` | Per-ticker deep dive |
| Markets | `/markets.html` | Cross-asset returns intelligence |
| Backtest | `/backtest.html` | Strategy performance comparison |
| Simulator | `/sim.html` | Leveraged ETN NAV simulator |

### Dashboard Tabs

#### Leaderboard
- ~3,950 unique tickers ranked by Final Alpha Score
- Sortable columns: score, rank, velocity, burst, concentration, tier breadth, ticker, company (3-way click sort toggle: desc → asc → default, with alphabetical columns and rank defaulting to ascending on first click)
- Day-over-day score deltas with honest `—` display when no comparable past data exists
- HC streaks, percentile-of-own-history progress bars
- **VELO** and **BURST** badges with micro-breakdown tooltips
- Filter chips: HC only, BURST, VELO, Quality+, Concentration ≤80%
- Auto-generated explainer line per row: compresses tier breadth, score delta, burst state, HC streak, quality signals, concentration, stealth, divergence into one scannable sentence
- **Expanded Row Detail Drawer**:
  - **Auto-Explainer Grid**: A frosted glassmorphic card container (`STRUCTURE`, `FLOW`, `RISK`) featuring dynamic left-border colored callout bars (`border-l-2 pl-2 border-current`) matching alert severity.
  - **Metadata Card**: Displays score breakdown, HC streak, best rank, score streak, and total weight separated by vertical dividers (`xl:border-l xl:border-white/[0.04]`).
  - **Holdings Table**: Polished layout with clean padding, soft borders, and standardized column alignments.
  - **Interactive Price Card (Right Panel)**: Includes pre-loaded price charts, capsule timeframe controls (1M to ALL), 2x2 key metrics grid (Latest Price, Avg Volatility, Period Low, Period High), a horizontal range slider track showing current position, and a horizontally expandable full-width company description.

#### ETFs Tab
- Per-ETF 50-slice SVG donut chart (20-color institutional palette)
- Tier pie chart (fixed Alpine SVG template limitation via `x-html`)
- Holdings sorted by rank, weight, rank delta, weight flow (supporting 3-way click sort toggle: desc → asc → default)
- **Official Strategy Descriptions**: Integrated investment objectives and systematic index mechanics for all 30 ETFs, cleanly parsed from research and embedded right below the detail header.
- Sidebar metadata: rebalance/reconstitution schedule from `ETF DATA.csv`
- ETF overlap heatmap: 30×30 Jaccard similarity matrix

#### Changes Tab
- Daily turnover: HC entries/exits, biggest score movers, new discoveries
- Top 15 velocity movers panel
- Sector flow: velocity-weighted exposure by GICS sector (Unknown excluded)
- Country flow: velocity-weighted exposure by domicile (Unknown excluded)
- Click a sector/country to filter the leaderboard
- **Interactive Sorting**: All 10 tables, sector flow, and country flow tables support 3-way toggle sorting (descending → ascending → default/clear) with visual indicators.
- **Dynamic Lookbacks**: Climbers and Fallers tables dynamically respect the selected lookback changesPeriod.
- **Color-Coded Card Accents**: Category-matched left-border highlights (green for gainers/climbers, red/rose for losers/fallers/crashed, cyan for pickups, purple for burst, blue for entrants, grey for exits).

#### Watchlist Tab
- Pin tickers with ★, persisted to localStorage
- "Since last visit" changelog: HC entries/exits, new bursts among pinned names
- Compare mode: side-by-side cards for top 4 pinned tickers

### Stock Detail (`stock.html`)

- **Score History**: sparkline area chart, score accumulation over time
- **Global Rank History**: inverted Y-axis line chart, O(1) pre-computed lookup
- **Per-ETF Rank History**: multi-line chart, tier-based coloring, crosshair tooltips
- **Signal Timeline**: Gantt-style multi-lane chart — per-day signal history now includes all signals (velocity, burst, NEW, stealth, divergence, quality adopt/defect) stored compactly per day in `flag_history.json`, with rank overlay, hover crosshair, duration counters
- **Score Decomposition Bar**: stacked bar showing each ETF's contribution, colored by tier
- **Tier Breadth chip**: how many distinct strategy types co-hold this name (1–5)
- **Quality+/Quality− chips**: gained/lost a Quality ETF in last 30 days
- **Momentum Gauge**: ↗ accelerating / → stable / ↘ weakening

### Analytical Signals

| Signal | Definition |
|--------|-----------|
| `velocity_score` | `0.5×GlobalRankΔ30d + 0.25×PeakImprovement30d + 1.0×AvgRankΔ7d + 20.0×AvgWeightFlow7d + 5.0×ETFsAdded30d + 1.0×ScoreStreak` |
| `burst_30d` | Peak rank improvement ≥ 40 positions in 30d, with ≥80% continuous presence AND sustained improvement in ≥8 of last 10 snapshots |
| `conviction_divergence` | Score rising but rank falling (crowded out) or score falling but rank rising (relative strength) |
| `stealth_accumulation` | Weight growing in 3+ ETFs without rank improvement |
| `momentum_regime` | accelerating / rising / stable / weakening / declining |
| `tier_breadth` | Count of distinct strategy tiers (Scout/Quant/Quality/Trend/Core) co-holding this name |
| `concentration_score` | % of final score from single top ETF (100 = mono-ETF, 25 = perfectly diversified across 4) |
| `quality_adopted_30d` | Gained a Quality ETF (COWZ/CALF/SPHQ) in last 30 days |
| `quality_defected_30d` | Lost a Quality ETF in last 30 days |

---

## Component 4 — Extended Scraper Bridge

### Why it exists

Eight ETFs require ingestion methods that can't run in the primary Selenium loop: Playwright browser automation (JS-rendered tables), PDF parsing, XLS downloads, and cookie-bound APIs. These run as a fault-isolated subprocess.

### Ownership Manifest

```python
V42_ETFS = frozenset({"VLUE", "AVSC", "GRIN", "JHMM", "JHEM", "JHSC", "MFEM", "JOET"})
```

At startup, `scraper.py` calls `check_v42_ownership_collisions()` and logs a warning if any ticker appears in both `config.json` (primary) and `V42_ETFS` (extended). In healthy state the intersection is empty.

### Bridge Pipeline

```
scripts/etf_holdings_scraper_v42.py (subprocess, timeout=600s)
    → etf_holdings_YYYYMMDD.csv  (Canonical_CSV at repo root)
         │
         ▼
    clean_canonical_csv()
    ├── Required columns: etf, ticker, name, weight_pct, as_of_date, security_type, scrape_date
    ├── Filters: non-equity security types, currency tickers ($USD, $BRL...), invalid weights
    ├── Normalises: weight_pct strings with % suffix ("2.62%" → 0.0262)
    └── Returns: (cleaned_df, errors)
         │
         ▼
    bridge_write_all_sinks(cleaned_df)
    ├── data/all_history.csv                        (Giant History — append + dedupe)
    ├── data/latest/{TICKER}.csv                    (one snapshot per V42 ETF)
    └── data/history/YYYY/MM/DD/master_archive.csv  (dated daily snapshot — append + dedupe)
```

### Failure Isolation Contract

Three-layer isolation ensures no v42 failure can corrupt the primary pipeline output:

1. **Subprocess crash / timeout** — logged, Bridge returns. Primary sinks already written remain intact.
2. **Canonical CSV validation failure** — `clean_canonical_csv()` returns empty DataFrame + error list. `bridge_write_all_sinks()` is skipped entirely; no sink is touched.
3. **Per-sink write failure** — each ETF's `data/latest/` write and the master_archive append are wrapped individually so one ETF's I/O failure does not abort the others.

### Reuse-Existing-CSV Recovery

If `etf_holdings_YYYYMMDD.csv` already exists when the Bridge starts (partial-run recovery or same-day rerun), the subprocess is skipped and the existing canonical CSV is reused. This avoids re-incurring Playwright cost while still fanning the data into all three sinks.

---

## Component 5 — Markets Intelligence Platform

A standalone data pipeline and 9-tab analytical dashboard providing cross-asset return analytics with deep historical coverage.

### Data Pipelines

#### `predator/markets_history.py`

Fetches monthly end-of-period (EOP) close data from FRED (primary) and yfinance (fallback for international indices). Supports incremental updates (trailing 3 months) and full refresh. Writes `docs/data/market_returns.json`.

Asset registry covers 50+ series across:
- **Equities**: S&P 500, DJIA, NASDAQ, NASDAQ-100, Nikkei 225, BSE Sensex, DAX, FTSE 100, Hang Seng, ASX 200, Bovespa, Shanghai Composite, Wilshire 5000
- **Precious Metals**: Gold, Silver, Platinum, Palladium
- **Energy**: WTI Crude, Brent Crude, Natural Gas, Coal
- **Base Metals**: Copper, Aluminum, Nickel, Zinc, Iron Ore, Tin, Lead
- **Agriculture**: Wheat, Corn, Soybeans, Cotton, Sugar, Coffee, Cocoa, Rice, Palm Oil
- **FX Rates**: USD/INR, USD/JPY, EUR/USD, GBP/USD, USD/CNY, USD/CAD, USD/AUD, USD/SGD, USD/BRL, USD/MXN, USD/KRW, USD/CHF
- **Real Estate**: US Home Prices (Case-Shiller)
- **Rates**: 3M T-Bill, 2Y/10Y/30Y Treasury, Fed Funds, Moody's BAA/AAA, 10Y–2Y Spread
- **Auxiliary**: US CPI (for real-return adjustment), USD/INR (for currency lens)

#### `predator/ingest_markets_xl.py`

Reads `data/Mega_Markets_Historical.xlsx` (and optionally `Markets_1_.xlsx` when present) and merges its deep historical data into the `market_returns.json` payload in the canonical shape. Excel data takes priority for overlapping months, extending history back to:

| Asset | History from |
|-------|-------------|
| Gold | **1833** |
| S&P 500 | **1871** |
| Dow Jones | **1885** |
| BSE Sensex | 1979 |
| NASDAQ | 1971 |

#### `predator/vol_history.py`

Fetches daily close for 7 CBOE volatility indices from FRED. Writes `docs/data/vol_history.json`.

| Index | FRED Series | Coverage |
|-------|-------------|---------|
| VIX | VIXCLS | 1990+ |
| GVZ (Gold VIX) | GVZCLS | 2008+ |
| OVX (Oil VIX) | OVXCLS | 2007+ |
| VXN (NASDAQ VIX) | VXNCLS | 2001+ |
| RVX (Russell VIX) | RVXCLS | 2004+ |
| VXD (DJIA VIX) | VXDCLS | 2005+ |
| VXEEM (EM VIX) | VXEEMCLS | 2011+ |

### Markets Dashboard (`/markets.html`) — 9 Tabs

#### Return Matrix
Annual returns heatmap for all assets. **Newest year on the left**, oldest on the right.

- Click any cell → monthly drilldown row with 12 colored cells
- Click any asset name → **Asset Detail Panel** (see below)
- **Currency lens**: Local / USD / INR / Gold / Silver — all returns recomputed client-side using monthly FX rates from the aux registry
- **Real returns toggle**: CPI-adjusted using CPIAUCSL (US CPI)
- **Z-score color mode**: color = how unusual this year was vs the asset's own history
- **Event annotations**: ⚡ markers on crisis years (1973, 1987, 2008, 2020, 2022...)
- **CSV export**: downloads current matrix view with active lens applied

#### Asset Detail Panel
Opens below the matrix when you click an asset name. Two tabs:

**Annual Returns (default)**
- Year-by-year vertical bar chart — green positive, red negative
- Partial years (first/last) shown at 40% opacity
- Amber dashed CAGR reference line
- Hover any bar → bottom strip shows exact year + return % to 2dp
- Respects active currency lens — Gold priced in INR shows INR-denominated gold returns

**Price History**
- Full price history line chart (log/linear toggle)
- Stats strip: CAGR, annualised vol, best/worst year, % positive years, data since

#### Price Log
Daily price log for 23 tracked instruments with ATH, ATL, streaks, and percentile stats.

#### Yield History
US interest rate history — line chart + annual yield heatmap. Series: 3M T-Bill, 2Y, 10Y, 30Y Treasury, Fed Funds, Moody's BAA, 10Y–2Y Spread.

#### Periodic Table
Callan-style rank rotation table — assets sorted best-to-worst return for each year, colored by asset.

#### Drawdowns (Drawdown Studio)
- Underwater curves for **all assets** (not a hardcoded subset)
- Left sidebar: grouped by category, search filter, Select All / Reset
- Time range filter: All / 50Y / 30Y / 20Y / 10Y
- Chart: dark `#111` background, 35% opacity fill, bright 2px line, 0% at top, negative going down
- Hover crosshair with per-asset drawdown values
- Max Drawdown table: Peak date, Trough date, Recovery date, Months to recover, Current DD

#### Hold Lab
Rolling N-year returns — best/worst/median for each holding period.

#### Seasonality
Month-of-year average return heatmap — which months historically perform best/worst per asset.

#### Correlation
Correlation matrix + Growth of $100 log-scale chart for selected assets.

#### Volatility Intelligence
4 sub-panels:
1. Current level tiles with z-score vs own history
2. Monthly z-score heatmap (VIX regime calendar)
3. Historical line chart with lookback filter
4. Cross-asset bar chart (current vs 1Y avg)

---

## CI/CD Pipeline

### Workflows

#### `daily_scrape.yml` — Data Collection
```
Trigger: cron 14:00 + 22:00 UTC weekdays (plus weekend runs), workflow_dispatch
Runner:  ubuntu-latest
Steps:
  1. Install libraries: pandas, selenium, playwright, curl_cffi, pdfplumber, xlrd, pyarrow
  2. Install Chromium (playwright) + xvfb
  3. Hydrate transient all_history.csv from Parquet store (hydrate_csv_from_parquet.py)
  4. Run scraper: xvfb-run -a python scraper.py
  5. Update parquet archive: fold new rows into data/history_parquet/ (migrate_to_parquet.py)
  6. Wipe local transient data/all_history.csv
  7. Commit and push Parquet partition deltas + CHECKSUMS.json (only if data changed)
  8. Trigger site rebuild (Workflow dispatch)
```

#### `build_site.yml` — Site Build & Deploy
```
Trigger: workflow_run (after scraper), push to main (predator/**, docs/**, scraper.py, tests/**...), workflow_dispatch
Runner:  ubuntu-latest
Steps:
  1. Install: pandas, pyyaml, pyarrow, pytest, hypothesis, yfinance, openpyxl, fredapi, python-dotenv
     (No selenium/curl_cffi/pdfplumber — scraper-only, not needed for build)
  2. pytest tests/ -v  (325 tests)
  3. predator.build  → docs/data/*.json
  4. predator.fetch_prices  → Portfolio Lab prices (yfinance adjusted-close)
  5. predator.fetch_stock_details  → descriptions + 2-year price history
  6. Persist stock-detail coverage  → commit-back coverage state to main
  7. predator.ingest_markets_xl  → 1/2: Markets Excel deep-history seed (backfill only)
  8. predator.markets_history  → 2/2: Live FRED + yfinance merge (current months, fx, cpi, rates)
  9. markets.fetch_yf + markets.fetch_fred + markets.build  → docs/data/markets.json + sim_underlyings.json
  10. predator.vol_history --full-refresh  → docs/data/vol_history.json
  11. Verify required outputs exist
  12. Upload Pages artifact → Deploy to GitHub Pages
```

### Key Design Decisions

- **Selenium lazy-import**: `scraper.py` wraps all selenium imports in `try/except ImportError`. The module can be imported without Selenium installed — tests and build CI don't need it, only the runtime scraper does.
- **Concurrency**: `cancel-in-progress: true` — newer builds cancel older ones, always deploying freshest data.
- **workflow_run chaining**: Build triggers off scraper completion, bypassing GitHub's limitation where bot-authored pushes don't trigger other workflows.
- **continue-on-error**: All market data fetch steps use `continue-on-error: true` — a FRED rate limit or yfinance outage doesn't fail the entire build.

---

## Testing

**325 tests** across 21 files — property-based (Hypothesis) and deterministic coverage of scoring, sanitization, the v42 bridge contract, Parquet immutability, markets engine, signal history, multi-period universe exits, and CI config.

```bash
python -m pytest tests/ -v
```

### Test Suite

The suite covers the full pipeline end-to-end. Key areas:

| Area | Test file(s) | Coverage |
|------|-------------|----------|
| Scoring & sanitizer | `test_scoring.py`, `test_apex_scoring.py`, `test_apex_gate.py` | Score formula, tier weights, rank multipliers, conviction layer, **sanitizer memoization correctness** |
| Temporal analytics | `test_scoring.py`, `test_signal_history.py` | Rank deltas (1/7/14/30/60/90d), velocity, burst detection, signal timeline |
| v42 bridge contract | `test_bridge.py`, `test_fault_isolation.py` | Property-based (Hypothesis) validation of the bridge pipeline, failure isolation |
| History isolation | `test_parquet_immutability.py` | SHA-256 manifest integrity, past-year immutability, zero-loss reconstruction, append-only contract |
| Markets engine | `test_markets_engine.py`, `test_markets_unit_conversion.py`, `test_self_living_merge.py`, `test_unit_gaps.py` | Market returns pipeline, currency conversion, partial-year merge |
| CI config | `test_ci_config.py`, `test_pipeline_automation.py` | Workflow structure invariants (e.g. `continue-on-error` policy) |

The sanitizer tests in `test_scoring.py` (44 tests) exercise `cfg.sanitizer.apply()` with distinct synthetic inputs, validating blocked tickers, name patterns, ticker normalization (BRK-B → BRK.B), GOOG → GOOGL dedup, and KRX cross-listing collapse — and prove the build-time memoization cache returns byte-identical results via deep copies.

| Test | Properties Covered |
|------|-------------------|
| `test_bridge_cleans_canonical_csv_to_pipeline_schema` | Column projection, weight normalisation, row count preservation, CSV round-trip |
| `test_bridge_rejects_csv_missing_required_columns` | Required-column enforcement halts pipeline before any sink write |
| `test_bridge_excludes_currency_and_money_market_rows` | Non-equity filtering (currency tickers, money market funds) |
| `test_bridge_idempotent_on_repeat_invocation` | Giant History + Latest Snapshots + Master Archive are byte-identical on second call |
| `test_metadata_json_contains_all_configured_etfs` | `metadata.json::etfs` equals sorted distinct ETF set from Giant History |
| `test_bridge_handles_weight_pct_with_percent_sign` | `"2.62%"` → `0.0262` normalisation (MFEM/AVSC regression) |

Hypothesis generates randomised inputs (column ordering, edge-case dates, malformed rows, locale-formatted numbers) to validate invariants that deterministic tests can't cover.

---

## Local Development

### Prerequisites

```bash
pip install -r requirements.txt
# For scraper runtime (not needed for build/tests):
pip install selenium curl_cffi pdfplumber xlrd
```

### Commands

```bash
# Reconstruct data/all_history.csv from Parquet store (required for local development/queries)
python scripts/hydrate_csv_from_parquet.py

# Run all 325 tests
python -m pytest tests/ -v

# Build site artifacts (leaderboard, holdings, changelog, flow, overlap)
# Note: --source data/all_history.csv is ignored when the Parquet store is
# populated (Parquet-first fetch_history). The path is retained for compat.
python -m predator.build --source data/all_history.csv --output docs/data --config config.yaml

# Fetch market returns (FRED + yfinance) — requires FRED_API_KEY in .env
python -m predator.markets_history --full-refresh

# Preview fetch plan without writing
python -m predator.markets_history --dry-run

# Seed deep history from data/Mega_Markets_Historical.xlsx (run first before markets_history)
python -m predator.ingest_markets_xl

# Fetch CBOE vol indices — requires FRED_API_KEY
python -m predator.vol_history --full-refresh

# Run the scraper locally (requires Chrome + ChromeDriver)
python scraper.py

# Preview the site
python -m http.server -d docs 8000
# → http://localhost:8000
```

### Environment Variables

```bash
# Required for FRED data (markets_history, vol_history)
FRED_API_KEY=your_key_here

# Or create a .env file in the repo root:
echo "FRED_API_KEY=your_key_here" > .env
```

### File Structure

```
etf-data/
├── scraper.py                    # Primary scraper (21 ETFs)
├── config.yaml                   # Scoring config — source of truth for ETF universe
├── config.json                   # Per-ETF scraper routing (URL, scraper_type, CUSIP)
├── requirements.txt
│
├── data/
│   ├── Mega_Markets_Historical.xlsx  # Historical market data (gold 1833+, S&P 1871+)
│   ├── all_history.csv           # Transient master holdings CSV (hydrate via scripts/hydrate_csv_from_parquet.py)
│   ├── history_parquet/          # Append-only Parquet store (sole durable source of truth)
│   ├── latest/                   # Current snapshot per ETF
│   ├── history/                  # Dated daily snapshots (gitignored)
│   └── ticker_metadata.csv       # Sector/industry/country/market_cap per ticker
│
├── docs/                         # GitHub Pages root
│   ├── index.html                # Main dashboard
│   ├── markets.html              # Markets Intelligence Platform
│   ├── stock.html                # Per-ticker deep dive
│   ├── backtest.html             # Strategy backtest
│   ├── sim.html                  # Leveraged ETN simulator
│   └── data/                     # Pre-built JSON payloads
│       ├── leaderboard.json      # Main leaderboard (~3.8MB)
│       ├── holdings_latest.json  # Per-(ETF, ticker) detail
│       ├── changelog.json        # Daily turnover
│       ├── flag_history.json     # Per-ticker flag/rank history (90d)
│       ├── score_history.json    # Score sparkline data
│       ├── flow.json             # Sector + country flow
│       ├── etf_overlap.json      # 30×30 Jaccard matrix
│       ├── market_returns.json   # Cross-asset monthly close (~940KB)
│       ├── vol_history.json      # CBOE vol indices
│       ├── markets.json          # Daily price log
│       └── metadata.json         # Build info + config snapshot
│
├── predator/
│   ├── build.py                  # Main build pipeline
│   ├── scoring.py                # Score formula + sanitizer
│   ├── history.py                # Temporal analytics
│   ├── markets_history.py        # FRED + yfinance market data
│   ├── ingest_markets_xl.py      # Active Excel deep-history backfill ingestion
│   ├── ingest_mega_xl.py         # Backward-compat shim (delegates to ingest_markets_xl)
│   ├── vol_history.py            # CBOE vol indices
│   └── backtest.py               # Strategy backtest engine
│
├── scripts/
│   ├── etf_holdings_scraper_v42.py  # Extended scraper (8 ETFs)
│   └── hydrate_csv_from_parquet.py # Reconstructs transient CSV from Parquet store
│
├── tests/                        # pytest suite (20 files, 316 tests)
│   ├── test_scoring.py           # 44 scoring & sanitizer tests
│   └── test_bridge.py            # 6 bridge PBT tests
│
└── .github/workflows/
    ├── daily_scrape.yml          # Data collection (14:00 + 22:00 UTC)
    └── build_site.yml            # Site build + deploy
```

---

## Configuration Reference

### `config.yaml`

```yaml
etfs:
  - {ticker: COWZ, tier: Quality, points: 30}
  # ... 30 ETFs total

rank_breakpoints:
  - {rank_max: 10,  multiplier: 1.5}
  - {rank_max: 30,  multiplier: 1.2}
  - {rank_max: 999, multiplier: 1.0}

new_lookback_days: 14       # absent for 14d = NEW entrant
new_bonus_mult: 5.0         # NEW bonus = tier_points × 5
new_bonus_tiers: [Scout, Quant]

high_conviction_min_etfs: 4  # ≥4 ETFs = HIGH_CONVICTION

history:
  delta_periods_days: [1, 7, 14, 30, 60, 90]
  leaderboard_lookback_days: 120
  changelog_top_n: 15
```

Commit a change to `config.yaml` → site auto-rebuilds with new scores in ~10 minutes.

### `config.json`

Per-ETF scraper routing: URL, `scraper_type` (invesco_api, pacer_csv, selenium_alpha, first_trust, etc.), CUSIP where needed. Consumed by `scraper.py` at runtime.

---

*Predator Protocol — built to find institutional conviction before it becomes consensus.*
