# 📈 ETF Holdings Scraper & Historian

**Automated Financial Data Pipeline**
*Tracks daily holdings for Pacer (COWZ, CALF), First Trust (FPX, FPXI), Alpha Architect (QMOM, IMOM), and Invesco (SPMO, XSMO, etc.) ETFs.*

---

## 🚀 Capabilities
This project runs automatically via **GitHub Actions** to:
1.  **Scrape** official ETF issuer websites for daily holdings data.
2.  **Standardize** columns across different issuers (Ticker, Name, Weight, Date).
3.  **Intelligent Deduplication:** Prevents redundant commits if data hasn't changed.
4.  **Dual Archiving:**
    * **Daily Snapshots:** `data/history/YYYY/MM/DD/master_archive.csv`
    * **Giant History:** `data/all_history.csv` (Single append-only file for backtesting).

## 🛠️ Tech Stack
* **Python 3.x**
* **Selenium & ChromeDriver:** For navigating complex JS-heavy sites (Pacer, First Trust).
* **Pandas:** For data cleaning and CSV management.
* **"Nuclear" Date Hunter:** Custom Regex logic to find hidden "As Of" dates in raw HTML source code.

## 📂 Data Structure
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
├── invesco_backup/       # Raw backup files for Invesco (debugging)
└── all_history.csv       # 🌟 THE MASTER FILE: All historical data concatenated



📈 Predator Protocol: Automated ETF Holdings PipelineOverviewThis system is an automated end-to-end data pipeline that aggregates daily holdings from 16 Smart-Beta ETFs. It consists of two components:Data Ingestion (Python/GitHub): Automated scraping, cleaning, and archival of daily ETF holdings.Data Processing (Excel Power Query): A multi-factor scoring engine that ranks stocks based on cross-ETF conviction, factor weighting, and accumulation signals.🏗️ System ArchitectureComponent 1: Data Ingestion (GitHub Actions)Source: Official issuer websites (Pacer, First Trust, Alpha Architect, Invesco).Frequency: Runs automatically at 01:30 UTC and 12:30 UTC via .github/workflows/daily_scrape.yml.Tech Stack: Python 3.9, Selenium, Pandas, BeautifulSoup.Data Structure:data/latest/*.csv: The most recent holdings for each ticker.data/history/YYYY/MM/DD/: Daily snapshots for archival.data/all_history.csv: Append-only master file for backtesting.Component 2: Scoring Engine (Excel / Power Query)Connection: Connects directly to raw.githubusercontent.com to fetch the latest CSVs.Processing: Merges 16 individual datasets, applies factor weights, and calculates a composite score.Update Mechanism: Manual Refresh (Data tab -> Refresh All).⚙️ Scoring Logic & ConfigurationThe system assigns a composite score to each stock based on the specific ETF holding it.1. ETF Tier Weights (ETF_Config Table)ETFs are categorized by strategy type. Each category is assigned a weight (Points) based on the rarity and value of the signal.CategoryTickerPointsStrategy DescriptionScoutCSD, FPX, FPXI40Spinoffs & IPOs. Captures structural market inefficiencies.QuantQMOM, IMOM, XMMO, XSMO, PIE40Factor-based momentum (Mid/Small Cap).QualityCOWZ, CALF, SPHQ30High Free Cash Flow & Profitability screening.TrendSPMO, SPHB, RPG10Broad momentum and high-beta validation.BlobQQQM, XLG2Market-cap weighted benchmarks (Confirmation only).2. The AlgorithmFinal Score = Σ (Single ETF Scores)For each holding, the score is calculated as:$$Score = (Weight \% \times Points \times RankMultiplier) \times 100 + NewBonus$$Rank Multiplier:Top 10 Rank: 1.5xTop 30 Rank: 1.2xRank > 30: 1.0xNew Entrant Bonus:If Status = "NEW" AND Category is Scout or Quant:Bonus = Points × 5 (e.g., +200 pts).🚩 Output FlagsThe dashboard generates specific flags based on the composition of holdings:HIGH CONVICTION:Condition: Count of Unique ETFs ≥ 4.Implication: Broad consensus across multiple strategies (e.g., Spin-off + Quality + Momentum).SPECULATIVE BETA:Condition: Present in Trend (Tier D) but ABSENT in Quality (Tier C) or Scouts (Tier A).Implication: High volatility momentum without fundamental cash-flow support.🔧 Maintenance & OperationsDaily UsageCheck Status: Verify the GitHub Action run is green (Success).Update View: Open Excel and click Data > Refresh All.TroubleshootingGhost Rows: If blank rows appear (e.g., Row 888), use the filter on the Ticker column to exclude (null) values.Privacy Errors: If Excel prompts for Privacy Levels, select "Ignore Privacy Levels" to allow merging of GitHub data with the local Config table.Modifying Weights: Edit the ETF_Config table (Columns AA:AC) in the Excel file and refresh to update scoring logic immediately.
