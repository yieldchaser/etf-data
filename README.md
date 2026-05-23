
# 📈 Predator Protocol: Automated ETF Holdings Pipeline

**Automated Financial Data Pipeline & Institutional Conviction Scanner**
*Tracks daily holdings for 29 Smart-Beta ETFs across Pacer, First Trust, Alpha Architect, Invesco, BlackRock, John Hancock, PIMCO, Avantis, VictoryShares, and Virtus to detect early institutional accumulation. The ETF count is derived from `config.yaml::etfs[]` — when that list changes, this README must be updated in the same commit.*

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


---

## 🔌 Component 4: Extended Scraper

The primary `scraper.py` handles 21 ETFs from issuers exposing stable HTML, CSV, or JSON endpoints (Pacer, First Trust, Alpha Architect, Invesco). The remaining 8 ETFs require Playwright browser automation, PDF parsing, or cookie-bound APIs — these live in `scripts/etf_holdings_scraper_v42.py` and run as an isolated subprocess. The Bridge fans the v42 output into the same canonical sinks the primary loop writes to.

### V42_ETFS Ownership Manifest

`V42_ETFS = frozenset({VLUE, AVSC, GRIN, JHMM, JHEM, JHSC, MFEM, JOET})` is the set of 8 tickers owned by the extended scraper. At startup, `scraper.py` calls `check_v42_ownership_collisions()` and logs a warning if any ticker appears in both `config.json` (primary) and `V42_ETFS` (extended). In healthy state the intersection is empty.

### Bridge Pipeline

```
v42 subprocess  ──►  etf_holdings_YYYYMMDD.csv   (Canonical_CSV at repo root)
                              │
                              ▼
                   clean_canonical_csv()  ──► (cleaned_df, errors)
                              │
                              ▼
                   bridge_write_all_sinks(cleaned_df)
                       ├──►  data/all_history.csv                       (Giant_History — append + dedupe)
                       ├──►  data/latest/{TICKER}.csv                   (one per V42 ETF)
                       └──►  data/history/YYYY/MM/DD/master_archive.csv (dated daily snapshot — append + dedupe)
```

The Bridge runs **after** the primary loop has already written its master_archive, so v42 rows are merged into the existing dated file rather than overwriting it. Idempotency is enforced on the composite key `(ETF_Ticker, ticker, Holdings_As_Of)` keep-last on every write — the 14:00 and 22:00 UTC reruns plus any manual `workflow_dispatch` triggers are safe to repeat.

### Failure Isolation

The extended scraper is invoked via `subprocess.run(..., timeout=600, capture_output=True, text=True)`. The Bridge is designed so no failure mode can corrupt the primary output that has already been written:

- **Subprocess crash, non-zero exit, or `TimeoutExpired`** — the last 3000 chars of stdout (and last 1000 of stderr on failure) are logged and the Bridge returns. `data/all_history.csv` and the dated `master_archive.csv` from the primary loop remain intact.
- **Canonical_CSV validation failure** — missing required columns, unparseable rows, or zero valid rows after filtering cause `clean_canonical_csv()` to return an empty DataFrame plus an error list. `bridge_write_all_sinks()` is skipped entirely; no sink is touched.
- **Per-sink write failure** — each per-ETF `data/latest/` write and the master_archive append are wrapped individually so one ETF's I/O failure does not abort the others.

This three-layer **failure isolation** contract is what allows the extended scraper to be flaky without ever degrading the 21-ETF primary pipeline.

### Reuse-Existing-CSV Recovery

If `etf_holdings_YYYYMMDD.csv` already exists when the Bridge starts (e.g. partial-run recovery or a rerun within the same UTC day), the subprocess is skipped and the existing canonical CSV is reused. This avoids re-incurring Playwright cost while still fanning the data into all three sinks.
