
# 📈 Predator Protocol: Automated ETF Holdings Pipeline

**Automated Financial Data Pipeline & Institutional Conviction Scanner**
*Tracks daily holdings for 16 Smart-Beta ETFs from Pacer, First Trust, Alpha Architect, and Invesco to detect early institutional accumulation.*

---

## 🏗️ System Architecture

This system operates as a two-stage pipeline:
1.  **Data Ingestion (Python/GitHub):** Automated scraping, cleaning, and archival of daily ETF holdings.
2.  **Data Processing (Excel Power Query):** A multi-factor scoring engine that ranks stocks based on cross-ETF conviction, factor weighting, and accumulation signals.

---

## 🚀 Component 1: Data Ingestion (GitHub Actions)

### Capabilities
This project runs automatically via **GitHub Actions** to:
1.  **Scrape** official ETF issuer websites for daily holdings data.
2.  **Standardize** columns across different issuers (Ticker, Name, Weight, Date).
3.  **Intelligent Deduplication:** Prevents redundant commits if data hasn't changed.
4.  **Dual Archiving:**
    * **Daily Snapshots:** `data/history/YYYY/MM/DD/master_archive.csv`
    * **Giant History:** `data/all_history.csv` (Single append-only file for backtesting).

### Tech Stack
* **Python 3.x**
* **Selenium & ChromeDriver:** For navigating complex JS-heavy sites (Pacer, First Trust).
* **Pandas:** For data cleaning and CSV management.
* **"Nuclear" Date Hunter:** Custom Regex logic to find hidden "As Of" dates in raw HTML source code.

### Automated Schedule
* **Frequency:** Runs automatically at **01:30 UTC** and **12:30 UTC** via `.github/workflows/daily_scrape.yml`.
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

ETFs are categorized by strategy type. Each category is assigned a weight (`Points`) based on the rarity and value of the signal.

| Category | Ticker | Points | Strategy Description |
| --- | --- | --- | --- |
| **Scout** | `CSD`, `FPX`, `FPXI` | **40** | Spinoffs & IPOs. Captures structural market inefficiencies. |
| **Quant** | `QMOM`, `IMOM`, `XMMO`, `XSMO`, `PIE` | **40** | Factor-based momentum (Mid/Small Cap). |
| **Quality** | `COWZ`, `CALF`, `SPHQ` | **30** | High Free Cash Flow & Profitability screening. |
| **Trend** | `SPMO`, `SPHB`, `RPG` | **10** | Broad momentum and high-beta validation. |
| **Blob** | `QQQM`, `XLG` | **2** | Market-cap weighted benchmarks (Confirmation only). |

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

The dashboard generates specific flags based on the composition of holdings:

* **HIGH CONVICTION:**
* **Condition:** `Count of Unique ETFs` ≥ 4.
* **Implication:** Broad consensus across multiple strategies (e.g., Spin-off + Quality + Momentum).


* **SPECULATIVE BETA:**
* **Condition:** Present in **Trend** (Tier D) but **ABSENT** in **Quality** (Tier C) or **Scouts** (Tier A).
* **Implication:** High volatility momentum without fundamental cash-flow support.



---

## 🔧 Maintenance & Operations

### Daily Usage

1. **Check Status:** Verify the GitHub Action run is green (Success).
2. **Update View:** Open Excel and click **Data > Refresh All**.

### Troubleshooting

* **Ghost Rows:** If blank rows appear (e.g., Row 888), use the filter on the `Ticker` column to exclude `(null)` values.
* **Privacy Errors:** If Excel prompts for Privacy Levels, select **"Ignore Privacy Levels"** to allow merging of GitHub data with the local Config table.
* **Modifying Weights:** Edit the `ETF_Config` table (Columns AA:AC) in the Excel file and refresh to update scoring logic immediately.

---

## 🦅 Component 3: Live Dashboard (GitHub Pages)

The site at **https://yieldchaser.github.io/etf-data/** runs the Predator Protocol
scoring algorithm directly on `data/all_history.csv` and renders a premium, institutional-grade
interactive dashboard. It augments the legacy Excel workflow with a high-performance,
mobile-friendly Web UI.

### 🌟 Phase 2.5 Institutional UX Features

The frontend has been completely modernized (Tailwind CSS + Alpine.js) with zero build dependencies, featuring:

- **Global Leaderboard & Insights:**
  - View all ~920 unique tickers ranked by Final Alpha Score, with day-over-day score deltas, HC streaks, and percentile-of-own-history progress bars.
  - **STATUS ⓘ Column:** Dynamic badging for `HC` (High Conviction) and `NEW` entrants with singleton-driven rich tooltips replacing legacy indicators.
  - **Daily Turnover:** Track who entered/exited HIGH CONVICTION, biggest score movers, and new discoveries (not seen in 7+ days).
- **Per-ETF Telemetry:**
  - Dynamic **50-slice SVG Donut Charts** supporting a 20-color institutional palette and "remaining weight" calculations for tail-end holdings.
  - Real-time visualization of current holdings sorted by rank, weight, and 7-day rank delta.
- **Deep-Dive Stock Analytics (`stock.html`):**
  - **Score History:** Sparkline area charts showing score accumulation over time.
  - **Global Leaderboard Rank History:** Clean, inverted Y-axis line chart tracking the stock's rank across the entire internal universe, utilizing O(1) pre-computed lookup maps for high-cardinality data.
  - **Per-ETF Rank History:** Multi-line charts with tier-based coloring (Scout, Quant, Quality, Trend), drop-shadows, and precise crosshair tooltips to track performance isolated to specific ETFs.
- **Singleton Tooltip Infrastructure:** A highly optimized, centralized DOM tooltip engine (`#tt` + `x-tooltip`) powering rich hover interactions across the entire dashboard without polluting the DOM tree.

### 🚀 Phase 2.6 Institutional Velocity Engine & Burst Detection

The platform now computes, filters, and surfaces institutional accumulation acceleration (**Velocity** and **Bursts**), catching the earliest actionable signals of institutional accumulation:

- **Composite Velocity Score (`velocity_score`):** A robust, multi-factor acceleration signal mapping institutional rate-of-change. Calculated as:
  $$\text{Velocity} = 0.5 \times \text{GlobalRankDelta}_{30d} + 0.25 \times \text{GlobalRankPeak}_{30d} + 1.0 \times \text{AvgRankDelta}_{7d} + 20.0 \times \text{AvgWeightFlow}_{7d} + 5.0 \times \text{ETFsAdded}_{30d} + 1.0 \times \text{ScoreStreak}$$
  This composite catches both steady, low-noise accumulators and sudden high-conviction institutional entries.
- **Institutional Burst Detection (`burst_30d`):** A $4\sigma$ event detector that flags any ticker achieving an improvement of $\ge 40$ positions in its global leaderboard rank at any point during a rolling 30-day window.
- **Leadboards & Visual Filtering:**
  - **Velocity Columns:** Sortable, color-coded leaderboard columns surfacing composite velocity. 
  - **VELO & BURST Badges:** Live badges on matching tickers with custom hover tooltips showing micro-breakdowns of all underlying signals.
  - **Interactive Filter Chips:** Click-to-filter controls to quickly isolate current `VELO` accumulators or `BURST` movers.
  - **Top Velocity Movers Panel:** An integrated 5th panel in the Changes tab showcasing the 15 fastest-accumulating tickers with real-time stats.
- **Hero Analytics & Detail Panels:**
  - Stock detail pages (`stock.html`) now feature dedicated hero KPI cards displaying real-time `VELOCITY` and `ETFs ADDED (30d)` metrics.
  - A dynamic, unified calendar-date X-axis for the per-ETF Rank History chart, aligning mismatched ETF histories and resolving previous rendering overlapping bugs.
- **Analytical Stability & Verification:** Complete test suite mapping rank deltas, rolling velocity calculations, and burst threshold detection with 100% automated test coverage.

### Architecture

`scraper.py` writes `data/all_history.csv` → `predator/build.py` reads it,
runs sanitizer + scoring + temporal analytics → writes `docs/data/*.json` →
GitHub Pages serves `docs/` using static HTML/JS. Auto-rebuilds within ~2 min of every scraper commit
via `.github/workflows/build_site.yml`. The existing `daily_scrape.yml` is untouched.

### Algorithm

All knobs live in `config.yaml`. Push a change, site rebuilds, no code edits needed.

The sanitizer mirrors the `ArchiveToDatabase_Production` VBA sub:
- Blocked tickers (exact, case-insensitive): `USD`, `$USD`, `$CAD`, `AGPXX`
- Blocked name substrings: `CASH &`, `CASH COLLATERAL`, `TREASURY`
- Ticker standardization: `BRK-B → BRK.B`, `BF/B → BF.B`

Scoring is the documented Predator Protocol v1 (Component 2 table above).

### Local development

```bash
pip install -r requirements.txt
python -m pytest tests/ -v           # 17 tests
python -m predator.build             # builds docs/data/*
python -m http.server -d docs 8000   # preview at http://localhost:8000
```

### Tuning

Edit `config.yaml`:
- `tiers[].points` — Scout/Quant=40, Quality=30, Trend=10, Blob=2
- `rank_breakpoints` — top-10 = 1.5×, top-30 = 1.2× multipliers
- `new_lookback_days` — how long a ticker must have been absent to count as NEW
- `new_bonus_mult` — NEW-entrant bonus multiplier on tier_points
- `high_conviction_min_etfs` — threshold for HIGH CONVICTION flag
- `history.leaderboard_lookback_days` — drives streaks and percentile bars

Commit, push — site rebuilds with new scores in ~2 min.

