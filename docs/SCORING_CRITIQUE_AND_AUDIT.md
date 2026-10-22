# Conviction Labs — Scoring Engine Empirical Critique & Audit Report

**Date:** October 2026  
**Dataset Scope:** 3,912 stocks across 30 Smart-Beta ETFs; comprehensive evaluation across the Top 1,000 securities with 3,838 historical price series.  
**Audited Engine:** `conviction/scoring.py` (`scoring_mode: apex`), `config.yaml`, and `docs/index.html`.

---

## 1. Executive Summary

This audit was initiated following an empirical market anomaly observed in October 2026: **high-conviction market leaders (such as GE Vernova `GEV` and SanDisk `SNDK`) experienced steep stock price corrections (-17% to -33%) and noticeable weight trimming across their holding ETFs, yet remained virtually immoveable at the absolute summit of the Global Leaderboard (#1 and #2).**

A systematic, quantitative investigation across the top 1,000 stocks reveals eight core structural dynamics, shortcomings, and mathematical artifacts in the scoring system:

1. **The "Sticky Giant / Zombie Conviction" Artifact:** Stocks with massive institutional multi-ETF breadth remain pinned in the top 10–20 even during 20–30% price crashes because multi-tier additive points create an insurmountable point moat over smaller, fast-rising names.
2. **The "Trimming Penalty / Lagging Momentum" Artifact:** High-flying stocks (+20% to +100% rallies) often see their conviction scores fall or stagnate because equal-weight and factor ETFs mechanically trim them back to target weights.
3. **The "Breadth Filler" Distortion:** Broad-market large caps (e.g., `AMAT`, `WDC`, `QCOM`, `APP`, `KLAC`) with very low individual conviction (underweight tail-holdings < 0.40 conviction) crowd out true alpha bets on the primary leaderboard because they happen to appear in 6–8 funds.
4. **Statistical Decoupling:** Across the entire universe, 30-day price return and 30-day score delta have a correlation of **`0.0041`** (effectively zero), meaning score movements operate almost independently of near-term price reality.
5. **UI Disconnect:** The engine already possesses a mathematical solution for breadth-filler distortion (`apex_rank` and `conviction_gate`), but the live user interface (`docs/index.html`) defaults strictly to `leaderboard_rank` (`final_score`).
6. **Share-Class & Dual-Listing Fragmentation:** Multiple listings of the same firm (Samsung Common vs Preferred, Petrobras ON vs PN, Siemens Energy Xetra vs ADR) are scored separately, splitting conviction mass and artificially depressing their rankings.
7. **The Single-Fund Sovereign Inversion:** Global sovereign giants (Alibaba, Reliance, HDFC Bank, Constellation Software) held with massive overweight conviction in a single fund are buried around rank #200–#490 on the default leaderboard despite having apex conviction scores worthy of the Top 50.
8. **ETF Scraping Desynchronization:** 18 out of 30 ETFs lag behind by 1–2 days, creating a temporally skewed matrix that injects artificial delta noise into daily rankings.

---

## 2. Methodology & Audit Dataset

The audit cross-referenced:
- **Leaderboard Metrics:** `docs/data/leaderboard.json` (3,912 stocks, including `final_score`, `leaderboard_rank`, `apex_score`, `apex_rank`, `avg_conviction`, `conviction_gate`, `etf_count`, `tier_breadth`, `m_breadth`, `m_conc`).
- **Historical Daily Prices:** 3,838 per-stock detail files (`docs/data/details/*.json`), computing 7-day, 30-day, and 90-day realized price returns.
- **ETF Holdings Snapshots:** `docs/data/holdings_history.parquet` and `data/latest/*.csv`, tracking intra-fund rank shifts and weight flows.

---

## 3. Finding 1: The "Sticky Giant / Zombie Conviction" Artifact

### Empirical Evidence
In a healthy alpha-tracking system, a persistent price breakdown accompanied by institutional selling should visibly erode a stock's ranking. 

However, in the current engine, **31 stocks in the Top 200 plummeted by > 15% (with several down > 25% to 31%) over 30 days, yet their Global Leaderboard rank remained frozen or actually advanced**:

| Ticker | Company | 30d Price Return | Global Leaderboard Rank | 30d Rank Δ | 30d Score Δ | ETF Count | Tier Breadth |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **`SNDK`** | SanDisk Corp | **`-26.96%`** | **#1** | **`+1`** | `+13.48%` | 9 | 5 |
| **`GEV`** | GE Vernova Inc | **`-17.76%`** | **#2** | **`-1`** | `-9.12%` | 8 | 5 |
| **`000660`** | SK hynix Inc | **`-26.49%`** | **#3** | **`0`** | `0.00%` | 4 | 1 |
| **`STX`** | Seagate Technology | **`-24.18%`** | **#6** | **`+2`** | `+11.71%` | 7 | 5 |
| **`992`** | Lenovo Group Ltd | **`-30.81%`** | **#36** | **`+9`** | `+8.00%` | 3 | 1 |
| **`034020`** | Doosan Enerbility | **`-29.59%`** | **#41** | **`+12`** | `+10.43%` | 3 | 1 |
| **`COHR`** | Coherent Corp | **`-26.89%`** | **#105** | **`+9`** | `+5.75%` | 5 | 3 |
| **`SPHR`** | Sphere Entertainment | **`-19.62%`** | **#73** | **`+37`** | `+15.91%` | 4 | 3 |
| **`028260`** | Samsung C&T Corp | **`-17.96%`** | **#53** | **`+21`** | `+7.62%` | 3 | 1 |

### The Mathematical Shortcoming
1. **Point Moat from Additive Sums:**  
   $$\text{Raw Mass } B = \sum_{i} \text{Points}_i \times C_i$$  
   When a stock is held across 8–9 ETFs, its raw score sums to 250–350+ points. Even if weights inside those ETFs drop by 15% due to price decay, the stock still has ~280 points. Meanwhile, a high-momentum emerging stock held with massive conviction by 2 ETFs can only reach ~120 points.
2. **The Multi-Tier Synergy Multiplier:**  
   $$M_{\text{breadth}} = 1 + \alpha \times \frac{E}{\ln(N_{\text{tiers}})}$$  
   A stock present in all 5 tiers (Core, Quality, Quant, Scout, Trend) receives an unbending **`+25% to +27%` score uplift** (`1.27x`). This multiplier is completely invariant to stock price drops.
3. **Rank Bracket Resilience:**  
   The rank multiplier uses discrete step brackets (`rank <= 10 -> 1.5x`, `rank <= 30 -> 1.2x`). If an ETF holding slips from rank #1 to rank #4, its multiplier remains unchanged at `1.5x`.

---

## 4. Finding 2: The "Trimming Penalty / Lagging Momentum" Artifact

### Empirical Evidence
Conversely, several stocks experiencing aggressive bull runs saw their conviction scores drop or stagnate:

| Ticker | Company | 30d Price Return | Global Leaderboard Rank | 30d Score Δ | ETF Count | Tier Breadth |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: |
| **`CCL`** | Carnival Corp | **`+22.53%`** | #232 | **`-7.35%`** | 6 | 3 |
| **`SBSP3`** | Sabesp | **`+20.60%`** | #461 | **`-27.59%`** | 3 | 1 |
| **`PETR3`** | Petrobras (ON) | **`+22.45%`** | #450 | **`-4.44%`** | 1 | 1 |
| **`PETR4`** | Petrobras (PN) | **`+20.31%`** | #63 | **`-2.70%`** | 2 | 1 |
| **`4958`** | Zhen Ding Tech | **`+21.43%`** | #141 | **`-5.88%`** | 3 | 1 |

### The Mathematical Shortcoming
- **Mechanical Rebalance Bias:** In smart-beta funds that follow equal-weighting (e.g. `JOET`) or fixed-tier sizing (e.g. `CALF`, `COWZ`, `RPG`), outperforming stocks naturally drift up in weight until the scheduled reconstitution date. At reconstitution, the fund manager or index algorithm **sells shares to reset the weight back to baseline**.
- The scoring engine calculates `weight_flow` and `conviction_multiplier` purely from observed holding weights. It cannot distinguish between:
  - *Bearish institutional exit* (fund manager dumping a deteriorating company).
  - *Mechanical rebalancing profit-taking* (fund trimming a successful 100% winner to preserve portfolio risk limits).
- Consequently, great momentum winners get penalized with negative score deltas right after rebalancing.

---

## 5. Finding 3: The Multi-Fund "Breadth Filler" Distortion

### Empirical Evidence
One of the most consequential issues is the presence of **"Breadth Fillers"** — mega-cap or benchmark components held in 5–8 ETFs at minuscule, underweight allocations (often < 0.10% per fund) that nevertheless outrank genuine concentrated alpha bets:

| Ticker | Company | Leaderboard Rank (`final_score`) | Apex Rank (`apex_score`) | Rank Discrepancy | Average Conviction | Top ETF Share |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: |
| **`AMAT`** | Applied Materials | **#38** | **#125** | **`-87` spots** | `0.451` | 31.7% |
| **`WDC`** | Western Digital | **#42** | **#132** | **`-90` spots** | `0.472` | 33.8% |
| **`INCY`** | Incyte Corp | **#44** | **#138** | **`-94` spots** | `0.338` | 36.0% |
| **`EXEL`** | Exelixis Inc | **#46** | **#148** | **`-102` spots** | `0.357` | 35.7% |
| **`QCOM`** | QUALCOMM Inc | **#48** | **#147** | **`-99` spots** | `0.385` | 30.3% |
| **`APP`** | AppLovin Corp | **#58** | **#154** | **`-96` spots** | `0.307` | 44.8% |
| **`KLAC`** | KLA Corp | **#68** | **#184** | **`-116` spots** | `0.383` | 36.4% |
| **`CF`** | CF Industries | **#74** | **#175** | **`-101` spots** | `0.335` | 39.9% |
| **`FANG`** | Diamondback Energy | **#77** | **#203** | **`-126` spots** | `0.349` | 43.5% |

### Analysis: The Dual-Engine Disconnect
- The codebase **already contains the mathematical cure** for this problem:
  In `conviction/scoring.py` (§30/§31), the `apex_score` engine applies a conviction-quality discount:
  $$M_{\text{conv}} = \text{clip}(\text{avg\_conviction}, \text{conv\_floor}, \text{conv\_cap})^{\gamma}$$
  which penalizes filler breadth (< 0.50 conviction) and knocks names like `AMAT`, `QCOM`, and `KLAC` down by 80 to 120 ranks!
- **The Shortcoming:** In `docs/index.html`, the default sort and primary display column presented to users is still **`leaderboard_rank`** (based on unadjusted `final_score`). The superior, debiased `apex_score` is relegated to a secondary column.

---

## 6. Finding 4: Statistical Decoupling (Near-Zero Price Correlation)

A regression analysis comparing 30-day price returns with 30-day conviction score deltas across all 975 active stocks yields:
* **30-Day Correlation:** **`+0.0041`**
* **7-Day Correlation:** **`+0.0515`**

```
30-Day Price Return vs. Score Delta Correlation:
+-------------------------------------------------------------+
| R = 0.0041  (Pure Orthogonality / Near-Zero Correlation)    |
+-------------------------------------------------------------+
```

### Why this happens:
1. **Reconstitution Asynchrony:** Smart-Beta ETFs only update their portfolios on fixed schedules (monthly, quarterly, or semi-annually). Between rebalances, a stock's price can move dramatically without *any* trade occurring in the ETF.
2. **Passive Drift:** When a stock falls 30%, its dollar weight in an ETF drops purely due to equity market decline, but its share count is unchanged until the next rebalance. The engine reflects this as subtle weight shrinkage rather than an active exit.

---

## 7. Finding 5: High-Rank Stickiness & Invariance

* **Top 100 Rank Stability:** **`26.3%` of the Top 100 stocks moved by $\le 2$ rank spots over 30 days**, despite immense equity volatility during the same window.
* Because the gap between #1 (362 pts) and #50 (115 pts) is so wide (~247 points), established institutional giants are essentially immune to normal day-to-day factor rotations.

---

## 8. Concrete & Actionable Recommendations

To resolve these shortcomings without destabilizing the core strengths of the platform, the following three enhancements are recommended:

### Recommendation A: Promote `apex_rank` to Primary UI Sort (Zero Model Risk)
- **Action:** In `docs/index.html`, set the default sorting key to `apex_rank` (or `apex_score`) instead of `leaderboard_rank`.
- **Impact:** Immediately eliminates the 22 "breadth filler" distortions (pushing underweight large-caps down to their realistic rank ~130–180), while placing true conviction bets at the top.

### Recommendation B: Dynamic Asymmetric Flow Decay (Weight Flow Responsiveness)
- **Action:** If a stock registers negative weight flow across 3 or more ETFs in the same 7-to-14 day window (as observed with `GEV`), accelerate its conviction multiplier decay.
- **Formula Suggestion:**
  $$\text{Penalty} = 1.0 - \beta \times \max(0, -\text{avg\_weight\_flow\_7d})$$
- **Impact:** Reflects multi-fund institutional trimming before the stock is fully liquidated.

### Recommendation C: Drawdown-Aware Conviction Dampener (Trailing Price Sanity Check)
- **Action:** If a stock's 30-day price return drops into severe drawdown territory (e.g. $< -20\%$), scale down its `m_breadth` uplift.
- **Impact:** Prevents "falling knife" situations (like `SNDK` -27% or `STX` -24%) from maintaining an untouchable #1 rank when institutional confidence is actively undergoing market stress.

---

## 9. Finding 6: Dual-Class & Cross-Listing Fragmentation (Split-Score Penalty)

### Empirical Evidence
While the sanitizer in `conviction/scoring.py` merges `GOOG` into `GOOGL` and `BRK-B` into `BRK.B`, it completely misses foreign share classes, preferred shares, and dual-listed ADRs:

| Company | Split Tickers | Ranks | Individual Scores | Holding ETFs | Combined Impact If Consolidated |
| :--- | :--- | :---: | :---: | :--- | :--- |
| **Samsung Electronics** | `005930` (Ord)<br>`005935` (Pref) | #4<br>#209 | 265<br>67 | EEMO, JHEM, MFEM, PIZ<br>EEMO | **Combined score: 332+** &rarr; Would vault Samsung to **#2 Globally**, beating GE Vernova! |
| **Petrobras** | `PETR4` (Pref)<br>`PETR3` (Ord) | #63<br>#450 | 108<br>43 | EEMO, JHEM<br>EEMO | **Combined score: 151+** &rarr; Would vault Petrobras into **Top 25 Globally**. |
| **Hyundai Motor** | `005380` (Ord)<br>`005387` (2nd Pref)<br>`005385` (Pref) | #15<br>#2675<br>#3013 | 187<br>2<br>1 | EEMO, JHEM, MFEM<br>EEMO<br>EEMO | Artificially dilutes total institutional capital across 3 entries. |
| **Itaú Unibanco** | `ITUB4` (PN)<br>`ITUB3` (ON) | #134<br>#2474 | 82<br>3 | EEMO, JHEM<br>EEMO | Splits voting and non-voting shares into separate rows. |
| **Siemens Energy** | `ENR.GR` (Xetra)<br>`ENR` (US ADR) | #157<br>#2234 | 76<br>5 | FPXI, GRIN<br>AVSC | European and US listings of the exact same firm treated as strangers. |

### Mathematical Flaw:
The multi-tier breadth bonus ($M_{\text{breadth}}$) and Herfindahl concentration penalty ($M_{\text{conc}}$) assume each ticker is an independent corporate entity. When one ETF holds preferred shares and another holds common shares, the algorithm treats them as completely separate, diluting their combined conviction mass.

---

## 10. Finding 7: The Single-Fund Sovereign Inversion (Massive Rank Discrepancies)

Comparing `leaderboard_rank` (`final_score`) against `apex_rank` (`apex_score`) reveals immense, 200-to-400 spot dislocations for single-fund high-conviction institutional positions:

| Ticker | Company | Leaderboard Rank (`final_score`) | Apex Rank (`apex_score`) | Inversion (Spots Gained) | Average Conviction | Held By |
| :--- | :--- | :---: | :---: | :---: | :---: | :--- |
| **`CSU`** | Constellation Software | **#490** | **#94** | **`+396` spots** | `0.919` | GRIN |
| **`GMEXICO.B`** | Grupo Mexico | **#470** | **#91** | **`+379` spots** | `0.938` | EEMO |
| **`1120`** | Al Rajhi Bank | **#479** | **#100** | **`+379` spots** | `0.929` | EEMO |
| **`PETR3`** | Petrobras (Ord) | **#450** | **#92** | **`+358` spots** | `0.965` | EEMO |
| **`105560`** | KB Financial Group | **#430** | **#76** | **`+354` spots** | `0.990` | EEMO |
| **`3750.HK`** | CATL (Class H) | **#413** | **#69** | **`+344` spots** | `1.019` | EEMO |
| **`2303`** | United Microelectronics | **#390** | **#57** | **`+333` spots** | `1.054` | EEMO |
| **`HDFCB`** | HDFC Bank Limited | **#343** | **#50** | **`+293` spots** | `1.136` | EEMO |
| **`ICICIBC`** | ICICI Bank Ltd | **#318** | **#48** | **`+270` spots** | `1.177` | EEMO |
| **`RELIANCE`** | Reliance Industries | **#286** | **#41** | **`+245` spots** | `1.246` | EEMO |
| **`9988`** | Alibaba Group | **#208** | **#34** | **`+174` spots** | `1.494` | EEMO |

### Why This is a Serious Shortcoming
In `final_score`, if a massive global sovereign giant (like Alibaba, Reliance, or HDFC Bank) is held by only 1 international ETF — even if it is that fund's **single largest overweight position with an astronomical conviction multiplier of 1.49** — it gets buried around rank #200 to #400. 
At the same time, a mediocre US stock held at tiny 0.04% weights by 5 funds ranks in the top 50. 
`apex_score` fixes this, but because `leaderboard_rank` is the default view, users miss these high-conviction institutional positions.

---

## 11. Finding 8: ETF Snapshot Desynchronization (Temporal Latency Mismatch)

### Empirical Evidence
An inspection of `data/latest/*.csv` shows that holdings snapshots are **not synchronized on the same calendar day**:
* **Oldest Snapshot Date in Active Set:** `2026-10-20` (18 ETFs, including `QQQM`, `SPHQ`, `SPMO`, `FPX`, `CSD`, `EEMO`, `PDP`, `XMMO`, `XSMO`)
* **Newest Snapshot Date in Active Set:** `2026-10-21` (12 ETFs, including `JOET`, `COWZ`, `CALF`, `AVSC`, `GRIN`, `IVAL`, `QVAL`)

### Mathematical Risk:
When computing day-over-day score deltas (`score_delta`):
* A stock held in both `JOET` (updated today) and `SPHQ` (updated yesterday) has its score computed across a **hybrid, temporally skewed matrix**.
* This introduces artificial delta spikes and false "burst" or "crater" signals caused purely by scraper arrival timing rather than actual portfolio trades.

---

## 12. Summary of Actionable Architectural Fixes

| Issue | Severity | Proposed Fix |
| :--- | :---: | :--- |
| **1. UI Sort Disconnect** | **Critical** | Switch default sort in `docs/index.html` from `leaderboard_rank` to `apex_rank`. Instantly fixes filler breadth and elevates true conviction bets like Alibaba, Reliance, and Constellation Software. |
| **2. Dual-Class Fragmentation** | **High** | Expand `sanitizer.ticker_renames` and `cross_listing_patterns` to map common preferred/dual shares (`005935` &rarr; `005930`, `PETR4` &rarr; `PETR3`, `ENR.GR` &rarr; `ENR`). Prevents score splitting for Samsung, Petrobras, and Siemens. |
| **3. Asymmetric Flow Decay** | **Medium** | Add a weight-flow decay factor when an asset experiences multi-fund outflows (e.g. `avg_rank_delta_7d < -2.0` across 3+ funds). Breaks the "sticky zombie" effect for declining stocks like `GEV` and `SNDK`. |
| **4. Trailing Drawdown Dampener** | **Medium** | Dampen $M_{\text{breadth}}$ if 30-day realized price return is $< -20\%$, preventing falling knives from holding the #1 spot. |

---

*Report generated and checked against repository data: `docs/data/leaderboard.json`, `docs/data/details/*.json`, and `docs/data/holdings_history.parquet`.*

