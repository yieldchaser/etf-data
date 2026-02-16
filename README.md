
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

```

```
