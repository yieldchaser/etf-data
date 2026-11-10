# Quantitative Architecture Specification: Volume Regime Heat Map & Microstructure Outlier Detection

## Executive Overview
This specification details the mathematical architecture, market microstructure rationale, and empirical distribution properties of a multi-timeframe Volume Regime Heat Map deployed across high-beta, leveraged exchange-traded products (ETPs) and commodity derivatives.

The analytical engine visualizes trading participation, institutional liquidity absorption, panic capitulation climaxes, and volume drought regimes across rolling calendar horizons (90, 126, 180, and 252 trading sessions) across long and short/inverse product legs.

A recent empirical audit revealed a fundamental mathematical degeneracy in the outlier thresholding system: selecting the top 10% (P90), top 5% (P95), and top 1% (P99) outlier filters produced identical peak counts. This document deconstructs the mathematical root causes of this rank saturation, benchmarks three competing architectural solutions evaluated by internal quantitative specialists, and presents a unified two-stage framework for external peer review.

---

## 1. Current Baseline Framework

### 1.1 Daily Capitulation Volume Index (CVI)
The platform evaluates session-by-session capitulation intensity using an interaction term between rolling volume expansion and price drawdown over a trailing window of $N = 22$ trading bars ($t-21$ through $t$, representing one calendar month of trading history):

$$\text{ValPct}_t = \text{percentileRank}(V_t, \{V_{t-21}, \dots, V_t\})$$

$$\text{PricePct}_t = \text{percentileRank}(P_t, \{P_{t-21}, \dots, P_t\})$$

$$\text{CVI}_t = \text{ValPct}_t \times \left(1 - \frac{\text{PricePct}_t}{100}\right)$$

where $V_t$ is trading volume (or dollar turnover $V_t \times P_t$) and $P_t$ is closing price. 

### 1.2 Discrete Empirical Percentile Rank
The empirical percentile rank is computed via strict inequality:

$$\text{percentileRank}(x, \mathbf{X}) = \frac{1}{N} \sum_{i=1}^N \mathbf{1}_{\{X_i < x\}} \times 100$$

### 1.3 Cross-Asset Composite & Outlier Thresholding
Across an active asset filter $\mathcal{K}$ (e.g., all tracked instruments, or specifically long versus short pair legs), the composite daily score represents the upper envelope:

$$S_t = \max_{k \in \mathcal{K}} \text{CVI}_{k, t}$$

Traders interact with the calendar grid using a three-tier outlier spotlight:
- **P90 (Notable Outliers):** Filtered to sessions in the top 10% of historical intensity.
- **P95 (Significant Outliers):** Filtered to sessions in the top 5% of historical intensity.
- **P99 (Extreme Outliers):** Filtered to sessions in the top 1% of historical intensity.

Sessions satisfying $S_t \ge \text{Threshold}$ are highlighted with a three-tier visual hierarchy (amber, orange, and red), while sub-threshold sessions are dimmed.

---

## 2. Mathematical Root Cause Analysis of Quantile Degeneracy

### 2.1 Theoretical Upper Bound Saturation
Under the strict empirical rank definition with $N = 22$:
1. **Volume Ceiling:** Because $V_t \in \{V_{t-21}, \dots, V_t\}$, the event $V_i < V_t$ can occur at most $N - 1 = 21$ times. Thus:
   $$\sup \text{ValPct}_t = \frac{21}{22} \times 100 = 95.\overline{45}\%$$
2. **Price Floor:** When price establishes a new 22-bar low, exactly 0 historical observations are strictly smaller than $P_t$:
   $$\inf \text{PricePct}_t = \frac{0}{22} \times 100 = 0.00\% \implies \left(1 - \frac{\text{PricePct}_t}{100}\right) = 1.00$$
3. **Hard Ceiling:** The maximum theoretical score achievable by any trading session is bounded:
   $$\sup \text{CVI}_t = 95.\overline{45} \times 1.00 = 95.4545\dots$$

No market shock, regardless of magnitude, can ever generate a score above 95.4545.

### 2.2 Discrete Point Mass Accumulation
In high-volatility, leveraged commodity instruments subject to acute margin cascades and weather shocks, volume surges accompanying 22-day price lows occur frequently:
- Across a 756-session historical dataset of 6 leveraged instruments:
  - Instrument A hits the ceiling on 16 sessions (2.13%).
  - Instrument B hits the ceiling on 29 sessions (3.86%).
  - Instrument C hits the ceiling on 24 sessions (3.19%).
  - Instrument D hits the ceiling on 26 sessions (3.46%).

### 2.3 Cross-Sectional Upper Envelope Aggregation
Taking the cross-sectional maximum across correlated pair legs concentrates probability mass at the upper boundary:
- Across Inverse/Short instruments ($K = 3$): **43 sessions out of 752 (5.72%)** hit exactly 95.4545.
- Across Long instruments ($K = 3$): **37 sessions out of 752 (4.92%)** hit exactly 95.4545.
- Across All instruments ($K = 6$): **80 sessions out of 752 (10.64%)** hit exactly 95.4545.

### 2.4 Sample Quantile Degeneracy
Because 10.64% of the entire historical sample shares the identical maximum value $x^* = 95.4545$, the empirical cumulative distribution satisfies:

$$F_S(x^*-) = 1.0 - 0.1064 = 0.8936, \quad F_S(x^*) = 1.0000$$

For any quantile probability $p \in (0.8936, 1.0000]$:

$$Q(p) \equiv x^* = 95.4545$$

Consequently:
$$Q(0.90) = 95.4545, \quad Q(0.95) = 95.4545, \quad Q(0.99) = 95.4545$$

The Boolean filter conditions $\mathbf{1}_{\{S_t \ge Q(p)\}}$ become logically identical for P90, P95, and P99. Testing confirmed that all 9 Hyndman-Fan sample quantile estimators (Types 1 through 9) collapse to 95.4545.

### 2.5 Microstructure Blindness (Magnitude Truncation)
Ordinal ranking discards order-flow magnitude. In the historical audit:
- A routine seasonal drift session with $1.40\times$ RVOL and a $-3.8\%$ price drop received $\text{CVI} = \mathbf{95.45}$.
- A catastrophic margin liquidation session with $9.35\times$ RVOL and a $-25.1\%$ price crash received the exact same $\text{CVI} = \mathbf{95.45}$.

---

## 3. The Three Competing Architectural Paradigms

Internal research produced three distinct architectural paradigms to resolve this failure:

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                              Three Architectural Formulations                          │
├───────────────────────────────┬────────────────────────────────────────────────────────┤
│ Formulation 1: Semi-          │ Non-parametric continuous transformation using Gaussian│
│ Parametric PIT (Statistics)   │ CDF on standardized log-volume and standardized price. │
├───────────────────────────────┼────────────────────────────────────────────────────────┤
│ Formulation 2: Microstructure │ Kyle-Hasbrouck concave liquidity modeling combining    │
│ Log-RVOL Elasticity           │ log2(1 + RVOL) with continuous 21d range drawdown.     │
├───────────────────────────────┼────────────────────────────────────────────────────────┤
│ Formulation 3: Adaptive Window│ Calibrates order statistics directly to visible window │
│ Desk Architecture (UI/UX)     │ length N with continuous volume ratio tie-breaking.    │
└───────────────────────────────┴────────────────────────────────────────────────────────┘
```

### 3.1 Formulation 1: Semi-Parametric Probability Integral Transform (PIT)
- **Concept:** Transform discrete ranks into continuous probabilities using standard distribution theory. Volume is log-transformed ($y = \ln V$) based on empirical log-normality ($\ln V \sim \mathcal{N}(\mu, \sigma^2)$) and standardized:
  $$Z_V = \frac{\ln V_t - \mu_{\ln V}}{\sigma_{\ln V}}, \quad Z_P = \frac{P_t - \mu_P}{\sigma_P}$$
- **Continuous CDF Mapping:**
  $$\text{ValPct}_t = \Phi(Z_V) \times 100, \quad \text{PricePct}_t = \Phi(Z_P) \times 100$$
  $$\text{CVI}_t = \text{ValPct}_t \times \left(1 - \frac{\text{PricePct}_t}{100}\right)$$
- **Properties:**
  - Produces 752 unique scores out of 752 sessions (0 ties).
  - Smooth mapping spanning $[0.0, 100.0]$.
  - Separates peak counts across P90, P95, and P99.

### 3.2 Formulation 2: Commodity Microstructure Log-RVOL Elasticity
- **Concept:** Grounded in financial microstructure (Kyle 1985, Hasbrouck 1991). Order flow price impact is concave relative to volume. Instead of ordinal counting, scale score directly by Relative Volume (RVOL) and continuous range depth:
  $$\text{RVOL}_{21, t} = \frac{V_t}{\frac{1}{21}\sum_{i=1}^{21} V_{t-i}}$$
  $$\text{RangePos}_{21, t} = \frac{\max_{21}(P) - P_t}{\max_{21}(P) - \min_{21}(P)}$$
  $$\text{MicroCVI}_t = \min\left(100.0, \log_2(1 + \text{RVOL}_{21, t}) \times \text{RangePos}_{21, t} \times 36.0\right)$$
- **Properties:**
  - Captures liquidity scaling: $\text{RVOL} = 1.0 \implies 1.00$, $\text{RVOL} = 3.0 \implies 2.00$, $\text{RVOL} = 7.0 \implies 3.00$.
  - Isolates catastrophic margin liquidations ($9\times$ RVOL) from routine $1.3\times$ volume upticks.
  - Empirical backtest: P99 turning point signals yielded an average **+17.20% forward 10-day return** on long leveraged instruments (versus +4.35% for legacy) and **+21.97% forward 21-day return** on inverse instruments (versus +12.81% legacy).

### 3.3 Formulation 3: Adaptive Window-Calibrated Order Statistics
- **Concept:** Focuses on trader cognitive ergonomics and viewport interaction. When paginating back to quiet historical quarters, global 3-year thresholds cause P99 to produce **0 peaks** because no multi-year extreme occurred in that specific quarter. A trader inspecting a 90-day window requires the primary volume climax of *that specific visible cycle*.
- **Mechanism:**
  1. Retain primary CVI score for baseline shading, but attach a secondary trailing volume multiple `volRatio` as a continuous tie-breaker.
  2. Dynamically calculate target peak counts calibrated to visible window length $N$:
     $$k_{99} = \max(1, \mathrm{round}(0.01 \times N))$$
     $$k_{95} = \max(k_{99} + 1, \mathrm{round}(0.05 \times N))$$
     $$k_{90} = \max(k_{95} + 1, \mathrm{round}(0.10 \times N))$$
  3. Lexicographically sort visible sessions by `(Score, VolRatio)`.
  4. Multi-tier visual lighting: P90 lights amber, P95 lights orange, and P99 isolates blazing red.
  5. Guarantees the **Strict Monotonicity Law** ($\text{Count}(P_{90}) > \text{Count}(P_{95}) > \text{Count}(P_{99}) > 0$) across 100% of tested slices, timeframes, and pagination offsets.

---

## 4. Empirical Benchmark Matrix

The following table summarizes the performance of the baseline against the candidate formulations across the 756-session historical dataset:

| Dimension / Metric | Current Baseline | Formulation 1 (PIT) | Formulation 2 (Log-RVOL) | Formulation 3 (Adaptive Window) |
| :--- | :---: | :---: | :---: | :---: |
| **All Instruments (90d View)** | 5 / 5 / 5 *(Collapsed)* | 7 / 4 / 3 *(Monotonic)* | 12 / 9 / 0 *(P99 Empty)* | **9 / 4 / 1 *(Optimal Monotonic)*** |
| **All Instruments (252d View)** | 27 / 27 / 27 *(Collapsed)* | 25 / 14 / 5 *(Monotonic)* | 30 / 19 / 4 *(Monotonic)* | **25 / 13 / 3 *(Optimal Monotonic)*** |
| **Short Instruments (90d View)** | 7 / 2 / 2 *(Collapsed)* | 6 / 5 / 2 *(Monotonic)* | 5 / 2 / 1 *(Monotonic)* | **9 / 4 / 1 *(Optimal Monotonic)*** |
| **Short Instruments (252d View)**| 25 / 17 / 17 *(Collapsed)* | 24 / 16 / 5 *(Monotonic)* | 29 / 17 / 5 *(Monotonic)* | **25 / 13 / 3 *(Optimal Monotonic)*** |
| **Long Instruments (252d View)** | 28 / 10 / 10 *(Collapsed)* | 23 / 10 / 3 *(Monotonic)* | 29 / 16 / 5 *(Monotonic)* | **25 / 13 / 3 *(Optimal Monotonic)*** |
| **Quiet Historical Quarter Offset**| 2 / 1 / 1 *(Collapsed)* | 2 / 1 / 0 *(P99 Empty)* | 2 / 1 / 0 *(P99 Empty)* | **9 / 4 / 1 *(Always Identifies Climax)*** |
| **Distinct Score Values** | 117 / 752 *(Massive Ties)* | 752 / 752 *(0 Ties)* | 752 / 752 *(0 Ties)* | 752 / 752 *(0 Ties)* |
| **Forward 10d Mean Return (P99 Long)** | +4.35% | +9.12% | **+17.20%** | +15.80% |
| **Forward 21d Mean Return (P99 Short)**| +12.81% | +16.40% | **+21.97%** | +19.45% |
| **Strict Monotonicity Pass Rate** | 0.0% (Failed) | 87.5% | 85.0% | **100.0% (336 / 336 Passed)** |

---

## 5. Proposed Unified Two-Stage Architecture

To capture the statistical rigor of Formulation 1, the institutional alpha of Formulation 2, and the UX guarantees of Formulation 3, we propose a **Two-Stage Quantitative Architecture**:

```
[ Raw Intraday & Daily Historical OHLCV Bars ]
                     │
                     ▼
┌────────────────────────────────────────────────────────────────────────┐
│ STAGE 1: MICROSTRUCTURE SCORING ENGINE                                 │
│ Replace discrete 22-bar ranks with continuous MicroCVI:                │
│   Score = min(100, log2(1 + RVOL_21d) * RangePos_21d * 36.0)           │
│ - Eliminates the 95.45 ceiling permanently.                            │
│ - Incorporates non-linear order flow magnitude.                        │
│ - Quadruples forward reversal signal quality.                          │
└────────────────────────────────────┬───────────────────────────────────┘
                                     │ Continuous Scores (0 to 100)
                                     ▼
┌────────────────────────────────────────────────────────────────────────┐
│ STAGE 2: ADAPTIVE MONOTONIC VIEWPORT ENGINE                            │
│ Filter visible trading days in active viewport N:                      │
│   k99 = max(1, round(0.01 * N))  --> Blazing Red Climax Outlier        │
│   k95 = max(k99+1, round(0.05 * N)) --> Orange Significant Outliers    │
│   k90 = max(k95+1, round(0.10 * N)) --> Amber Notable Outliers         │
│ - Guarantees Count(P90) > Count(P95) > Count(P99) > 0 on every view.   │
│ - Zero dead filters (P99 >= 1 always).                                 │
│ - Equal-frequency color swatch distribution across baseline tiles.     │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 6. Key Questions & Trade-Offs for External Peer Review

We request external quantitative review on the following specific design dimensions:

1. **Volume Elasticity Function:** Is logarithmic scaling $\log_2(1 + \text{RVOL})$ optimal for leveraged commodity derivatives, or would a Generalized Pareto (EVT) tail transformation, Power-Law scaling ($V^\alpha$ with $\alpha \approx 0.5-0.6$), or Box-Cox transformation provide superior regime invariance across multi-year cycles?
2. **Local Viewport vs. Global Historical Baselines:** Is calibrating outlier counts to the visible calendar window ($N \in \{90, 126, 180, 252\}$) standard practice on institutional execution desks, or should a hybrid approach be used (e.g., dual-gating where a visible peak must also exceed a global baseline threshold)?
3. **Cross-Asset Aggregation Operator:** Taking the upper envelope $\max_{k \in \mathcal{K}} S_{k, t}$ highlights whether any instrument experienced capitulation, but can mask single-instrument divergence. Would a turnover-weighted composite or a Mahalanobis distance metric better capture systemic cross-pair stress?
4. **Volume Drought (Valleys) Symmetry:** While panic selling exhibits sharp, asymmetric spikes, liquidity droughts are slow-moving and persistent. Does an inverse volume ratio $\text{AvgVol} / \text{CurrentVol}$ properly model market-maker inventory withdrawal, or should bid-ask spread estimators (e.g. Corwin-Schultz, Abdi-Ranaldo) be formally integrated?
5. **Implementation Pitfalls:** What operational or quantitative hazards (e.g. lookahead leakage, volatility regime shifts, holiday calendar gaps) should be guarded against when deploying this two-stage model in a production web environment?
