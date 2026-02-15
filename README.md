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
