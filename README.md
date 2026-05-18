
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

### 🔬 Phase 2.7 Institutional Decision Engine

The platform now provides **prescriptive** intelligence beyond descriptive analytics:

- **BURST False-Positive Fix:** Burst detection now requires ≥80% continuous presence on the leaderboard AND sustained improvement for ≥8 of the last 10 snapshots. Eliminates re-entry and single-touch false positives.
- **Honest Δ% Display:** Score deltas show em-dash (—) when no comparable past data exists, instead of misleading `+0.0%`.
- **Sector & Country Flow Overlay (`flow.json`):** Aggregates velocity-weighted exposure by sector and country. Two new panels in the Changes tab with horizontal bar visualizations. Click a sector to filter the leaderboard.
- **Watchlist (localStorage):** Pin tickers with ★, view them in a dedicated WATCHLIST tab. "Since last visit" changelog shows HC entries/exits and new bursts among pinned names.
- **Concentration Risk Score:** Per-ticker metric showing what fraction of the score comes from a single ETF. Tooltip warns when conviction is fragile (>70% from one ETF). Sortable column + ≤80% filter chip.
- **Strategy Backtest (`/backtest.html`):** Quantified performance of 5 strategies (HC Entry, BURST Trigger, Top-10 Score, Top-10 Velocity, SPX Baseline) with cumulative returns chart, stats table, and velocity-vs-return scatter plot with R².
- **Signal Timeline (`stock.html`):** Per-ticker Gantt-style chart showing HC/SPEC/VELO/BURST state history over 90 days with rank line overlay. Shows when a stock entered/exited each state and how long it stayed. Duration stats (e.g., "HC: 34d · VELO: 12d").
- **Momentum Gauge:** Real-time indicator (↗ strengthening / → stable / ↘ weakening) based on score streak direction.
- **New Derived Signals:**
  - `momentum_regime` — accelerating / rising / stable / weakening / declining
  - `conviction_divergence` — detects when score rises but rank falls (being crowded out)
  - `stealth_accumulation` — weight growing in 3+ ETFs without rank improvement
- **Chart Upgrades:** Y-axis rank labels, current-rank pill badges, removed drop-shadow rendering bug, better tier color contrast, thicker lines with glow effect.
- **Performance Fixes:** Debounced search (300ms), binary search in backtest hover, eliminated O(n²) array copies in holdings table.

### 🧬 Phase 2.8 Structural Intelligence & Comparison Tools

The platform now surfaces **structural** relationships between ETFs and tickers, enabling deeper conviction analysis:

- **Smooth Chart Curves:** All line charts (Score History, Leaderboard Rank, Per-ETF Rank, Signal Timeline rank overlay) now use Catmull-Rom → cubic Bézier smoothing instead of jagged polylines.
- **Signal Timeline Overhaul (`stock.html`):**
  - Interactive hover crosshair with tooltip showing date, state (HC/VELO/BURST/SPEC/Neutral), rank, and velocity at each snapshot.
  - X-axis date tick labels (5 evenly spaced).
  - Current-state pill badge in the panel header.
  - Duration counters for all states including SPEC and Neutral.
  - Highlighted bar outline + rank dot on hover.
- **Tier Breadth (`tier_breadth`):** Counts how many distinct strategy types (Scout/Quant/Quality/Trend/Blob) co-hold a name (1–5). Higher breadth = more independent confirmation. Displayed as a chip on stock detail.
- **Quality Adoption / Defection (30d):**
  - `quality_adopted_30d` — a Quality ETF (COWZ/CALF/SPHQ) added this name in the last 30 days. Momentum + fundamentals confirmation.
  - `quality_defected_30d` — a Quality ETF dropped this name. Possible fundamentals warning.
  - Surfaced as `Quality+` / `Quality−` chips on stock detail and leaderboard rows. New `Quality+` filter chip on the leaderboard.
- **ETF Overlap Heatmap (`etf_overlap.json`):**
  - 16×16 Jaccard similarity matrix showing pairwise holdings overlap between all ETFs.
  - Interactive heatmap panel on the ETFs tab with hover details (shared count + Jaccard %).
  - "Top overlap pairs" summary line (e.g., RPG↔SPMO 24%, RPG↔SPHB 20%).
  - Helps distinguish which ETFs provide independent signal vs which echo each other.
- **Score Decomposition Bar (`stock.html`):**
  - Horizontal stacked bar showing each ETF's contribution to the final score, colored by tier.
  - Per-ETF legend with rank, weight, and percentage contribution.
  - "Concentrated" / "Diversified" pill badge based on top-ETF share.
- **Compare Mode (Watchlist tab):**
  - Side-by-side cards for the top 4 pinned tickers (by score).
  - Each card shows: ticker, rank, state badge, score + delta, sparkline, ETFs, velocity, concentration.
  - Click any card to jump to full stock detail.
- **Auto-Generated Explainer Line:**
  - One-sentence narrative auto-generated for each leaderboard row on expansion.
  - Compresses: tier breadth, score delta, BURST/VELO state, HC streak, Quality+/−, concentration, stealth, divergence into a single scannable line.
  - Example: "Held by 5 ETFs across 3 tiers · Score +12% 30d · BURST +47 ranks (best #14) · Quality+ in 30d."
- **Hash Routing:** `index.html#etf=COWZ` auto-opens the ETFs tab for that ETF (linked from stock detail decomposition).

### Architecture

`scraper.py` writes `data/all_history.csv` → `predator/build.py` reads it,
runs sanitizer + scoring + temporal analytics + velocity + concentration + flow + overlap →
writes `docs/data/*.json` → GitHub Pages serves `docs/` using static HTML/JS.
Auto-rebuilds within ~2 min of every scraper commit via `.github/workflows/build_site.yml`.

### Local development

```bash
pip install -r requirements.txt
python -m pytest tests/ -v           # 27 tests
python -m predator.build             # builds docs/data/*
python -m predator.backtest          # builds docs/data/backtest.json
python -m http.server -d docs 8000   # preview at http://localhost:8000
```

### Pages

| Page | URL | Description |
|------|-----|-------------|
| Dashboard | `/` | Leaderboard, ETFs, Changes, Watchlist tabs |
| Stock Detail | `/stock.html?t=GEV` | Per-ticker deep dive with charts |
| Markets | `/markets.html` | Price log for indices, commodities, FX, vol |
| Simulator | `/sim.html` | Leveraged ETN NAV simulator |
| Backtest | `/backtest.html` | Strategy performance comparison |

### Tuning

Edit `config.yaml`:
- `etfs[].points` — Scout/Quant=40 (FPXI/IMOM=60), Quality=30, Trend=10, Blob=2
- `rank_breakpoints` — top-10 = 1.5×, top-30 = 1.2× multipliers
- `new_lookback_days` — how long a ticker must have been absent to count as NEW
- `new_bonus_mult` — NEW-entrant bonus multiplier on tier_points
- `high_conviction_min_etfs` — threshold for HIGH CONVICTION flag
- `history.leaderboard_lookback_days` — drives streaks and percentile bars

Commit, push — site rebuilds with new scores in ~2 min.

