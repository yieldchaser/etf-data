# DYNF Factor Rotation Engine: Comprehensive Architecture, Mathematical Model, and Implementation Specification

---

## 1. Executive Summary and Problem Statement

### 1.1 The Problem
Institutional active ETFs frequently shift their asset allocations in response to changing macroeconomic regimes, interest rate cycles, inflation pressures, and market sentiment. Among these funds, the **iShares U.S. Equity Factor Rotation Active ETF (`DYNF`)** is BlackRock's flagship active vehicle for systematically timing and rotating between fundamental style factors:
- Momentum
- Quality
- Value
- Low Volatility
- Size / Cyclical Beta

Traditional ETF dashboards treat funds as monolithic static blocks, displaying only current top 10 holdings or backward-looking trailing returns. Such views mask the crucial dynamic information embedded in daily portfolio shifts:
1. Which equity style factors are institutional quants currently accumulating or dumping?
2. Did the fund detect turning points (such as the 2026 semiconductor slowdown or the 2022 energy rotation) ahead of broad market declines?
3. How fast is factor rotation occurring (factor velocity and acceleration)?
4. What macroeconomic regime (Expansion, Slowdown, Contraction, Recovery) does the current portfolio posture reflect?

### 1.2 Objective
To build a fully automated, quantitative Factor Rotation Engine that:
1. Ingests the complete daily historical holdings of `DYNF` from inception to present without lookahead bias.
2. Decomposes daily constituent weights into quantifiable factor sleeves.
3. Calculates factor velocity, 30-day and 90-day rates of change, and acceleration/deceleration state.
4. Identifies macroeconomic regime transitions across historical cycles.
5. Surfaces an interactive, high-performance visualization in the web dashboard with zero external charting dependencies, dynamic time horizons, timeline scrubbing, and playback animation.
6. Integrates seamlessly into the automated CI/CD pipeline so daily scraper runs refresh the engine automatically.

---

## 2. Target ETF and Historical Data Ingestion

### 2.1 Target Instrument Profile
- **Fund Name**: iShares U.S. Equity Factor Rotation Active ETF
- **Ticker**: `DYNF`
- **Issuer**: BlackRock / iShares
- **Manager**: BlackRock Systematic Active Equity (SAE) Team
- **Benchmark / Baseline**: Russell 1000 Index
- **Inception Date**: March 19, 2019
- **Stated Investment Strategy**: Dynamically allocates across U.S. large- and mid-cap equities by overweighting factors perceived to have the most favorable risk/return outlook based on macroeconomic, valuation, and sentiment indicators.

### 2.2 Historical Data Store Specifications
- **Data Source**: Daily published BlackRock institutional holding records obtained via BlackRock Varnish CDN API (`scraper_type: blackrock_api`, `portfolio_id: 307283`).
- **Storage Format**: Apache Parquet store partitioned by year:
  `data/history_parquet/year=YYYY/holdings.parquet`
- **Checksum Verification**: SHA-256 cryptographic manifest in `data/history_parquet/CHECKSUMS.json`.
- **Temporal Coverage**: March 19, 2019 to November 5, 2026.
- **Distinct Trading Days**: 1,905 trading days.
- **Total Holding Records Processed**: 625,035 individual position rows.
- **Unique Historical Tickers**: 932 individual stock constituents.
- **Fund Weight Coverage**: Categorized factor sleeves account for 85% to 95% of aggregate fund equity weight on every single trading day.

---

## 3. Factor Classification Architecture

### 3.1 Quantitative Factor Sleeves vs. Industrial Sectors
In quantitative portfolio management, a company's sector describes its product line (e.g. Information Technology, Energy, Healthcare), whereas a style factor describes its empirical risk premia and statistical behavior (e.g. relative price momentum, return on equity, valuation multiple, beta).

The Factor Engine organizes DYNF holdings into 6 distinct factor sleeves that bridge quantitative risk factors with real-world market drivers:

```
+----------------------------------------------------------------------------------------------------+
|                                    DYNF PORTFOLIO DECOMPOSITION                                    |
+----------------------------------+----------------------------------+------------------------------+
| 1. MOMENTUM                      | 2. QUALITY                       | 3. VALUE & FREE CASH FLOW    |
| - High price velocity (12m rel.) | - High ROE (>30%), Low debt      | - Low EV/EBITDA, high FCF    |
| - High beta growth               | - Recurring revenue platforms    | - Tangible asset capital     |
| - Semiconductors & AI Hardware   | - Mega-cap software ecosystems   | - Energy & Heavy Industrials |
| (NVDA, AVGO, AMD, MU, LRCX)      | (AAPL, MSFT, GOOGL, META, V, MA) | (XOM, CVX, CAT, DE, GE, UNP) |
+----------------------------------+----------------------------------+------------------------------+
| 4. FINANCIALS & CAPITAL          | 5. LOW VOLATILITY                | 6. CYCLICAL DISCRETIONARY    |
| - Net interest margin exposure   | - Low market beta (<0.80)        | - Consumer spending beta     |
| - Credit spread sensitivity      | - Inelastic demand               | - Retail & discretionary     |
| - Money center banks & insurance | - Healthcare, Staples, Utilities | (AMZN, TSLA, HD, LOW, MCD)   |
| (JPM, BAC, GS, MS, BLK, CB)      | (UNH, LLY, JNJ, PG, KO, WMT, NEE)|                              |
+----------------------------------+----------------------------------+------------------------------+
```

### 3.2 Detailed Sleeve Constituent Mapping

1. **Momentum (Semiconductors, Tech Hardware & High Beta Growth)**:
   - *Rationale*: Captures stocks exhibiting the highest 6-to-12 month price velocity and earnings revisions. During market expansion phases, this factor is dominated by semiconductor and hardware infrastructure providers.
   - *Primary Constituents*: `NVDA`, `AVGO`, `AMAT`, `LRCX`, `AMD`, `MU`, `QCOM`, `TXN`, `INTC`, `KLAC`, `MRVL`, `ADI`, `NXPI`, `ON`, `MCHP`, `MPWR`, `CDNS`, `SNPS`, `ARM`, `SMCI`, `TER`, `SWKS`, `QRVO`, `CRUS`, `ENTG`, `WDC`, `STX`, `NFLX`, `CRM`, `ADBE`, `NOW`, `PANW`, `CRWD`, `SNOW`, `FTNT`, `DDOG`, `ZS`, `PLTR`, `ANET`.

2. **Quality (Mega-Cap Platforms, High ROE, Fortress Balance Sheets)**:
   - *Rationale*: Captures businesses with high profitability, wide economic moats, low leverage, and resilient recurring revenues.
   - *Primary Constituents*: `AAPL`, `MSFT`, `GOOGL`, `GOOG`, `META`, `V`, `MA`, `CSCO`, `ACN`, `ORCL`, `IBM`, `INTU`, `ADP`, `PAYX`, `ISRG`, `SYK`, `TMO`, `DHR`, `ABT`, `GILD`, `REGN`, `VRTX`, `BDX`.

3. **Value & Free Cash Flow (Energy, Industrials & Materials)**:
   - *Rationale*: Companies trading at low valuation multiples with strong tangible asset backing, high dividend yields, and inflation hedging capabilities.
   - *Primary Constituents*: `XOM`, `CVX`, `COP`, `EOG`, `SLB`, `VLO`, `MPC`, `PSX`, `OXY`, `HES`, `KMI`, `WMB`, `HAL`, `BKR`, `CAT`, `DE`, `GE`, `UNP`, `HON`, `UPS`, `FDX`, `LMT`, `RTX`, `BA`, `NOC`, `GD`, `EMR`, `ETN`, `ITW`, `PH`, `LIN`, `APD`, `SHW`, `ECL`, `FCX`, `NEM`, `NUE`, `DOW`.

4. **Financials & Capital Strength (Money Center Banks, Insurance, Credit)**:
   - *Rationale*: Financial institutions sensitive to the yield curve slope, credit spreads, and capital market velocity.
   - *Primary Constituents*: `JPM`, `BAC`, `WFC`, `C`, `GS`, `MS`, `BLK`, `BRKB`, `ALL`, `PGR`, `CB`, `AXP`, `SCHW`, `USB`, `PNC`, `TFC`, `COF`, `BK`, `STT`, `MET`, `PRU`, `AIG`, `TRV`, `MMC`, `AON`, `AJG`, `CME`, `ICE`, `MCO`, `SPGI`.

5. **Low Volatility (Healthcare, Consumer Staples, Regulated Utilities)**:
   - *Rationale*: Non-cyclical businesses with inelastic product demand, stable cash flows, and low sensitivity to economic downturns.
   - *Primary Constituents*: `JNJ`, `UNH`, `LLY`, `ABBV`, `MRK`, `PFE`, `BMY`, `AMGN`, `CVS`, `CI`, `ELV`, `PG`, `KO`, `PEP`, `COST`, `WMT`, `TGT`, `MDLZ`, `CL`, `KMB`, `GIS`, `MO`, `PM`, `T`, `VZ`, `CMCSA`, `NEE`, `DUK`, `SO`, `D`, `AEP`, `SRE`, `EXC`, `XEL`, `ED`, `MDT`, `WM`, `RSG`, `AWK`.

6. **Cyclical Discretionary (Consumer Discretionary, Retail Beta)**:
   - *Rationale*: Highly sensitive to consumer confidence, discretionary disposable income, and credit expansion.
   - *Primary Constituents*: `AMZN`, `TSLA`, `HD`, `LOW`, `MCD`, `SBUX`, `NKE`, `BKNG`, `TJX`, `MAR`, `HLT`, `ORLY`, `AZO`, `ROST`, `LULU`, `CMG`, `DRI`, `YUM`, `EBAY`, `ETSY`.

---

## 4. Mathematical Model and Computational Methodology

### 4.1 Daily Factor Weight Aggregation
For each trading day $t \in T$:
Let $H_t$ be the set of all holding records for `DYNF` on day $t$.
Let $w_{i,t}$ be the portfolio weight of stock $i$ on day $t$, where $\sum_{i \in H_t} w_{i,t} = 1.0$.

For each factor sleeve $F_k$ with constituent ticker set $S_k$:
$$W_{k,t} = \sum_{i \in H_t \cap S_k} w_{i,t} \times 100$$

The unclassified residual weight is captured by:
$$W_{\text{other},t} = \max\left(0, 100 - \sum_{k=1}^6 W_{k,t}\right)$$

### 4.2 Factor Velocity and Acceleration Mechanics
Institutional rotations do not happen overnight; they unfold over monthly and quarterly rebalancing cycles. To detect the speed and direction of capital reallocation, the engine computes:

1. **30-Day Factor Delta ($\Delta_{30d}$)** (using approximately 21 trading days):
   $$\Delta_{30d,k,t} = W_{k,t} - W_{k,t-21}$$

2. **90-Day Factor Delta ($\Delta_{90d}$)** (using approximately 63 trading days):
   $$\Delta_{90d,k,t} = W_{k,t} - W_{k,t-63}$$

3. **Kinematic Velocity Comparison**:
   - Short-term velocity: $V_{\text{short}} = \frac{\Delta_{30d}}{21}$
   - Medium-term velocity: $V_{\text{medium}} = \frac{\Delta_{90d}}{63}$

4. **Acceleration / Deceleration Classification**:
   - If $\Delta_{30d} > +0.5\%$:
     - If $V_{\text{short}} > V_{\text{medium}}$: Status = **Accelerating** (Accumulation is gaining speed).
     - Else: Status = **Expanding** (Accumulation continues but at a moderating pace).
   - If $\Delta_{30d} < -0.5\%$:
     - If $V_{\text{short}} < V_{\text{medium}}$: Status = **Decelerating** (Dumping is intensifying).
     - Else: Status = **Contracting** (Outflow continues at a steady pace).
   - If $|\Delta_{30d}| \le 0.5\%$: Status = **Neutral** (Steady state).

### 4.3 Macroeconomic Regime State Machine
The algorithm infers the macroeconomic posture of the fund on each trading day by evaluating the relative balance between offensive (pro-cyclical) and defensive (counter-cyclical) factor clusters:

- **Offensive Cluster**: $O_t = W_{\text{momentum},t} + W_{\text{quality},t}$
- **Defensive Cluster**: $D_t = W_{\text{value},t} + W_{\text{low\_vol},t}$

**Decision Tree**:
1. **Expansion**:
   - Condition: $O_t > 1.5 \times D_t$
   - Sub-state:
     - If $W_{\text{momentum},t} > W_{\text{quality},t}$: `Expansion (High-Beta Momentum)`
     - Else: `Expansion (Quality Leadership)`
2. **Contraction / Recovery**:
   - Condition: $D_t > O_t$
   - Sub-state:
     - If $W_{\text{value},t} > W_{\text{low\_vol},t}$: `Recovery (Cyclical Value & FCF)`
     - Else: `Contraction (Defensive Flight)`
3. **Slowdown / Transition**:
   - Condition: Neither condition met (Offensive and defensive forces are balanced)
   - State: `Slowdown (Barbell Rotation)`

---

## 5. Dynamic Sampling and Performance Optimization

Transmitting 1,905 daily observations with 6 factor series across the wire would result in large JSON payloads (~700 KB) and sluggish SVG rendering on mobile devices.

The engine implements **Adaptive Kinematic Sampling**:
- **Recent Window (Trailing 365 Days)**: Sampled every 2nd trading day (~2 to 3 observations per week). This preserves every turning point, intra-month rotation, and sudden shift with high precision.
- **Historical Archive (> 365 Days)**: Sampled every 5th trading day (~weekly).
- **Boundary Guarantee**: The very first date (`2019-03-19`) and the latest date (`as_of`) are strictly included.

**Result**:
- Payload reduced from 700 KB to **134.4 KB** (456 points).
- Browser SVG renders smoothly in under 16ms (60 FPS).

---

## 6. Frontend Dashboard Implementation (`docs/index.html`)

### 6.1 Architectural Constraints
- **Zero External Charting Libraries**: Built entirely using Alpine.js and procedural inline SVG (`viewBox="0 0 960 210"`).
- **Design System Alignment**: Strictly uses existing CSS variables (`--surface`, `--surface-2`, `--border`, `--text`, `--cyan`, `--up`, `--down`, `--amber`, `--blue`, `--burst`), font-mono tabular numerals, and pill/capsule styling.
- **Zero Emojis**: All indicators use pure typography, geometric badges, and vector elements.

### 6.2 Interactive Component Hierarchy
1. **Header Bar**:
   - Fund indicator chip (`DYNF`), fund title, and dynamic Macro Regime badge.
   - Dynamic horizon selector: `ALL`, `5Y`, `3Y`, `1Y`, `YTD` (dynamically computed relative to `dynfEngine.as_of`).
2. **Factor Acceleration & Deceleration KPI Grid (6 Cards)**:
   - Factor label, color marker, status chip (`Accelerating`, `Decelerating`, `Expanding`, `Neutral`).
   - Large tabular weight percentage and signed 30-day delta (`+X.X% 30d` / `-X.X% 30d`).
   - Factor description and top 4 constituent stocks linking to `stock.html?t=TICKER`.
   - Card click interaction: Isolates that factor across the entire dashboard.
3. **Real-Time Inspection Strip**:
   - On cursor hover or slider drag: Shows exact date, regime badge, and all 6 factor allocations with color markers.
   - On idle: Shows interactive navigation instructions.
4. **Procedural SVG Multi-Line Chart**:
   - Horizontal gridlines at 0%, 10%, 20%, 30%, 40% with clean Y-axis labels.
   - Adaptive X-axis tick labels (years for `ALL`/`5Y`, half-years for `3Y`, months for `1Y`/`YTD`).
   - 6 factor polylines with rounded joins.
   - Dimming effect: Non-highlighted lines fade to 0.18 opacity when a factor is isolated, while the active line thickens to 2.8px and renders on top.
   - Vertical dashed crosshair line with colored intersection dots on hover/scrub.
5. **Timeline Scrubber & Animation Controls**:
   - HTML5 range slider synchronized with the SVG canvas.
   - Automated "Play / Pause" timeline playback button (75ms per step) to visualize rotations over time.
   - "Reset to Latest" button.
6. **Collapsible Historical Regime Rotations Drawer**:
   - Complete multi-year milestone table of annual regime transitions, holdings count, and factor weight breakdowns.
   - Clickable rows that instantly jump the chart and scrubber to that year's snapshot.

---

## 7. Automated CI/CD Data Pipeline

### 7.1 Pipeline Workflow
1. **Daily Scrape (`.github/workflows/daily_scrape.yml`)**:
   - Triggered 3 times daily (02:00, 14:00, 22:00 UTC) on weekdays and once daily on weekends.
   - Scrapes BlackRock CDN API for DYNF (`config.json` entry with `portfolio_id: 307283`).
   - Appends and folds new records into `data/history_parquet/year=YYYY/holdings.parquet`.
   - Commits Parquet partition to `main` and dispatches `build_site.yml`.
2. **Build Site (`.github/workflows/build_site.yml`)**:
   - Triggered by pushes touching `data/**` or `scripts/build_dynf_factor_engine.py`.
   - Executes `python scripts/build_dynf_factor_engine.py`.
   - Validates outputs: `test -f docs/data/dynf_factor_engine.json`.
   - Deploys static site to GitHub Pages.

---

## 8. Verification and Quality Assurance

1. **Parquet Immutability**: All past-year Parquet partitions verified with zero byte drift.
2. **Core System Stability**: 95 regression tests executed (`pytest tests/test_parquet_immutability.py tests/test_scoring.py tests/test_ci_config.py tests/test_bridge.py`); 94 passed, 1 skipped. Core conviction ranking and official 30 ETF universe remain 100% untouched.
3. **JavaScript Execution**: Evaluated in Node.js VM across 35 combinations of horizons and factor focus states; 0 errors, 0 NaNs, 0 undefined values.
4. **Emoji Audit**: 0 emojis present across all code, tests, documentation, and commits.
