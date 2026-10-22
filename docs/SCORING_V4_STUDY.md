# Scoring Engine v4 — Walk-Forward Study

**Date:** 2026-10-22 · **Engine:** `conviction/scoring.py` (`scoring_mode: apex`) · **Reproduce:** `scripts/scoring_study.py`

This study responds to `docs/SCORING_CRITIQUE_AND_AUDIT.md`. That audit reported anomalies (sticky giants, breadth fillers, price decoupling, fragmented share classes). This study tests each claim by replaying the **real engine** on every holdings snapshot in the store, and then scores candidate changes on forward outcomes. The result is scoring engine v4. Section 7 answers each of the audit's findings.

---

## 1. Verdict

1. **The score had two double counts and one non-monotone correction, and the audit caught none of them.**
   - **Breadth was rewarded twice.** A tier-synergy bonus `1 + 0.15·(tiers−1)` sat on top of the entropy multiplier `M_breadth`, so a 5-tier name earned up to 1.60 × 1.35 = **2.16×** from breadth alone. This is the "point moat".
   - **International names were levelled twice.** A 60-point bump for six funds sat on top of `M_universe`, which exists to do the same job.
   - **The filler discount averaged conviction.** Two stacked `clip(avg_conviction)` factors (`m_conv` and the conviction gate) did the discounting. Averaging makes the score **non-monotone**: adding a low-conviction holder could lower a name's score (57.6 → 45 in a worked example). The mean was also unweighted, so a 2-point Core fund counted as much as a 40-point Scout fund.
2. **Symbol identity was corrupting the board directly.**
   - **Collisions.** International funds report bare local-exchange codes, and 50 of them collided with a different company under the same symbol (42 against a US ticker, 8 between two international exchanges). `MTN` was Vail Resorts (AVSC, JHSC) *and* MTN Group (JHEM, MFEM) on one row, a **fabricated 4-fund HIGH_CONVICTION name ranked #9 on the October leaderboard**. Others include `A` (Agilent + Kimberly-Clark de México), `TEL` (TE Connectivity + PLDT), `PEP` (PepsiCo + PPB Group) and `BA` (Boeing + BAE Systems).
   - **Fragmentation.** ~135 securities were split across exchange-suffix dialects (`6857` / `6857.JP`, `ADVANC` / `ADVANC.R`). Share classes were split too (Samsung common/pref, Petrobras ON/PN).
3. **The score cannot be "fixed" into a better return predictor by reshaping it.**
   - **Momentum regime.** Its forward-return IC follows the momentum regime: +0.045 in Feb–May, −0.032 in May–Oct. Plain 12-1 price momentum moves the same way.
   - **Little residual signal.** After controlling for 12-1 momentum the residual IC is about ±0.015 and flips sign between periods.
   - **No candidate moved it.** Every formula change tested moved forward-return IC by less than a tenth of its standard error. That includes the audit's drawdown dampener and outflow decay.
4. **v4 fixes the defects in (1) and (2).** The changes are κ = 2 per-holding convexity, one breadth reward, one regional levelling term, and name-verified identity resolution. Forward-return IC is unchanged; the internal outcomes improve in both universes:

| 30-ETF universe (May–Oct, 22 weekly dates) | pre-v4 | **v4** |
|---|---:|---:|
| Top-50 breadth-filler share (avg conviction < 0.5) | 17.6% | **4.8%** |
| Forward fund-accumulation IC (score vs 30-day change in holder count) | −0.009 | **+0.018** |
| Top-50 names that lose a holder within ~30 days | 18.8% | **14.4%** |
| Top-50 still in top-100 after ~28 days | 92.1% | **93.7%** |
| 20-day forward-return IC (t, Newey-West) | −0.031 (−1.22) | −0.031 (−1.28) |

| 16-ETF universe (Feb–May, 13 weekly dates, out-of-sample) | pre-v4 | **v4** |
|---|---:|---:|
| Top-50 breadth-filler share | 53.2% | **26.8%** |
| Forward fund-accumulation IC | +0.002 | **+0.046** |
| Top-50 names that lose a holder within ~30 days | 15.9% | **11.2%** |
| Top-50 still in top-100 after ~28 days | 79.9% | 79.9% |
| 20-day forward-return IC (t) | +0.042 (3.13) | +0.050 (3.12) |

*(Engine replay: `scripts/scoring_study.py engine`. The pre-v4 cache truncates scores to integers, which ties the bottom of the board and slightly depresses its all-name IC; this has no effect on the top-50 metrics.)*

---

## 2. Data and method

- **Holdings.** `data/history_parquet` holds 753,872 rows, 217 snapshot dates from 2026-02-12 to 2026-10-21, and 30 ETFs since late May (16 before that).
- **Prices.** 3,813 daily local-currency series from `docs/data/details/*.json`. Daily moves above 60% are masked as bad ticks or unadjusted splits.
- **Engine replay.** `compute_leaderboard(history, cfg, as_of=D)` runs for every snapshot date D. Variants are pure functions of the replayed per-holding frames. **V0 re-implements the pre-v4 `final_score` and is verified to match the engine exactly (max |diff| = 0)** before anything else is trusted.
- **Sampling.** Weekly sample dates. The **30-ETF universe** (2026-05-25 →) is the main sample; the **16-ETF universe** (Feb → mid-May) is an out-of-sample robustness check.
- **Returns.**
  - **Entry:** the T+2 close after the holdings as-of date. Median publication lag is 1 day; 87% of snapshots are scraped within 2 days.
  - **Horizons:** forward 5, 10, 20 and 40 trading days.
  - **Cleaning:** winsorized at 1/99% and **demeaned within currency groups**, so the study measures stock selection rather than which country rallied.
- **Metrics.**
  - **Return metrics:** cross-sectional Spearman IC with Newey-West t-stats on the weekly series, both for all names and for names with ≥ 2 holders, plus the top-50 demeaned return.
  - **Internal metrics:** what the board consists of (filler share, single-fund share, non-US share), what it predicts about fund behaviour (accumulation IC, top-50 exit rate), and stability (retention, median daily top-100 rank change).
- **Decision rule, fixed before results.**
  - **Structural fixes** (removing a double count, data hygiene) ship unless they cost more than one standard error of forward-return IC.
  - **Heuristic or price-based additions** ship only if they *improve* IC by more than one standard error with a consistent sign across horizons and both universes.
- **Statistical power is limited.** Five months of the 30-ETF universe amount to roughly 4–5 independent 20-day windows. The standard error of the mean 20-day IC is about 0.025, so return differences between variants below ~0.025 are noise.

---

## 3. Variant ladder (`scripts/scoring_study.py variants`)

Each row changes one thing relative to the row it builds on. All rows start from the replayed pre-v4 per-holding conviction.

**30-ETF universe**

| Variant | IC 20d | t | Top-50 ret 20d | Filler top-50 | Single-fund top-50 | Non-US top-50 | Accum. IC | Exit top-50 | Retain | Churn |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| V0 pre-v4 `final_score` | −0.032 | −1.26 | −1.8% | 17.2% | 0.0% | 56% | −0.008 | 18.6% | 92.1% | 2.7 |
| V0a pre-v4 `apex_score` | −0.035 | −1.34 | −1.5% | 0.4% | 9.9% | 77% | 0.000 | 10.2% | 95.4% | 3.1 |
| V1 no tier synergy | −0.032 | −1.27 | −1.3% | 9.3% | 0.0% | 67% | −0.006 | 17.8% | 93.3% | 2.7 |
| V3 conviction once (point-weighted) | −0.033 | −1.30 | −1.3% | 1.5% | 4.0% | 80% | −0.002 | 13.0% | 97.0% | 2.5 |
| V5 V3 + audit drawdown dampener | −0.033 | −1.30 | −1.3% | 1.5% | 4.0% | 80% | −0.002 | 13.0% | 96.5% | 2.5 |
| V7 V3 + APEX kicker | −0.034 | −1.31 | −1.3% | 1.8% | 3.7% | 79% | −0.002 | 12.7% | 95.5% | 3.3 |
| K2 κ = 2 | −0.033 | −1.29 | −1.3% | 2.6% | 3.8% | 76% | +0.019 | 14.1% | 96.2% | 2.4 |
| **K2 + uniform tier points (v4)** | −0.033 | −1.33 | −1.6% | 4.9% | 2.6% | 62% | **+0.021** | 14.5% | 94.1% | 2.7 |
| *Reference: 12-1 price momentum* | −0.051 | −0.95 | −2.7% | 58% | 44% | 51% | +0.078 | 12.2% | 90.1% | 4.5 |
| *Reference: 1-month reversal* | +0.028 | +1.16 | +0.8% | 93% | 62% | 29% | −0.041 | 11.2% | 8.5% | 22.5 |

**16-ETF universe**

| Variant | IC 20d | t | Top-50 ret 20d | Filler top-50 | Single-fund top-50 | Non-US top-50 | Accum. IC | Exit top-50 | Retain | Churn |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| V0 pre-v4 `final_score` | +0.045 | 3.23 | +4.0% | 52.9% | 3.5% | 10% | +0.001 | 16.0% | 79.2% | 4.2 |
| V0a pre-v4 `apex_score` | +0.046 | 2.94 | +3.4% | 15.5% | 37.2% | 15% | +0.007 | 10.5% | 69.7% | 5.4 |
| V1 no tier synergy | +0.045 | 3.24 | +3.6% | 50.3% | 6.5% | 10% | 0.000 | 14.5% | 77.2% | 4.4 |
| V3 conviction once (point-weighted) | +0.044 | 3.13 | +3.2% | 30.6% | 17.5% | 11% | +0.003 | 11.9% | 76.0% | 5.3 |
| V5 V3 + audit drawdown dampener | +0.044 | 3.12 | +3.2% | 30.2% | 17.9% | 11% | +0.003 | 11.9% | 75.9% | 5.4 |
| V7 V3 + APEX kicker | +0.045 | 3.16 | +3.5% | 30.5% | 18.5% | 11% | +0.003 | 12.2% | 74.3% | 6.5 |
| K2 κ = 2 | +0.045 | 3.03 | +3.0% | 25.9% | 16.9% | 10% | +0.028 | 11.2% | 76.9% | 4.9 |
| **K2 + uniform tier points (v4)** | +0.045 | 3.05 | +2.9% | 26.3% | 16.0% | 9% | **+0.042** | 11.2% | 79.5% | 5.1 |
| *Reference: 12-1 price momentum* | +0.085 | 2.64 | +5.7% | 69% | 63% | 20% | +0.195 | 6.9% | 76.0% | 4.7 |
| *Reference: 1-month reversal* | −0.005 | −0.12 | −0.2% | 94% | 83% | 4% | −0.057 | 13.4% | 9.2% | 18.4 |

**How to read it.**

- **Forward-return IC never moves.** Its column is flat to within ±0.003 across every engine variant. The standard error is ~0.025.
- **The internal columns separate the variants.**
- **Pre-v4 `apex_score` was not a better ranking.** It already "fixed" fillers, which is why the audit recommended promoting it. But it did so by averaging, with three costs:
  - **Single-fund names:** it filled the 16-ETF top 50 with single-fund names (37%).
  - **Stability:** it had the worst rank stability (churn 5.4, retention 69.7%).
  - **Accumulation:** it predicted fund accumulation no better than `final_score`.
- **V3 vs K2.** "Conviction once, point-weighted" (V3) is the cleanest version of the averaging idea. It lowers filler share, but it does nothing for the accumulation IC (−0.002 / +0.003). κ = 2 achieves similar filler and exit improvements **and** turns the accumulation IC positive in both universes, without the monotonicity flaw.
- **Uniform tier points.** κ = 2 amplifies whatever the points table favours. With the 60-point bump it pushed the 30-ETF top 50 to 76% non-US names. Removing the redundant bump brings that back to 62% (60% in the shipped engine), close to the pre-v4 56%. It also gives the best accumulation IC of any variant (+0.021 / +0.042).
- **Momentum as a benchmark.** Price momentum predicts future ETF additions far better than any holdings-based score (+0.08 / +0.20), because momentum ETFs buy past winners. That is the honest ceiling on "conviction before consensus" from price alone. It is not a holdings signal, and the score doesn't import it (§6).

---

## 4. Tested and rejected

| Proposal | Result | Decision |
|---|---|---|
| **Audit Rec. C — drawdown dampener** (drop `M_breadth` when 21-day return < −20%) | IC change ≤ 0.0003, retention slightly worse. Top-100 names down > 20% lost a holder **less** often than other top-100 names (5.9% vs 19.5%, n = 85). Their forward 20-day demeaned return was −2.4% vs −0.5% in May–Oct but +0.5% vs +0.9% in Feb–May (n = 25). | Rejected — the "falling knife" premise doesn't hold in holdings data, and the effect on forward returns is noise. |
| **Audit Rec. B — multi-fund outflow decay** (penalise names with weight outflows in ≥ 3 funds) | Once a holding's weight change is measured *net of its own price drift relative to the fund* (passive drift isn't selling), the trigger almost never fires. Score and metric changes were ≤ 0.0005. | Rejected — most of the "trimming" the audit saw was passive drift. |
| **APEX velocity kicker** (± 35% momentum overlay) | IC change < 0.001 at every horizon; churn 2.5 → 3.3 / 5.3 → 6.5. | Retired from the ranking. `velocity_z` / `ignition_z` stay as diagnostics. |
| **Diminishing returns on breadth** (k-th largest contribution × 0.85ᵏ⁻¹ or 0.7ᵏ⁻¹) | Filler / exit gains similar to κ = 2, weaker accumulation IC, worse retention in the 16-ETF universe. | Rejected in favour of κ. |
| **Soft-threshold convexity** (`C·min(1, C/c₀)`, which only shrinks below c₀) | Indistinguishable from κ = 2 once tier points are uniform; slightly higher churn. | Rejected — κ is simpler. |
| **Removing the Jaccard overlap discount** | No measurable difference. | Kept — its rationale (duplicate EM allocators) is sound and it costs nothing. |
| **Removing M_conc / M_breadth / M_universe** | No measurable difference on returns; slightly lower filler share. | Kept — they encode the product thesis (independent styles, levelling), and the evidence doesn't contradict them. |
| **Staleness guard at 10 days** | Identical metrics, but COWZ/CALF once had a 21-day scrape gap, so a 10-day guard would fake exits. | Shipped at **30 days**. |

---

## 5. What v4 changes

**Scoring** (`conviction/scoring.py`, `config.yaml`)

- **Convexity.** `Single_i = points_i × C_iᵏ` with **κ = 2** (`conviction.kappa`, apex only).
- **One breadth reward.** The tier-synergy bonus is ignored in apex mode; `M_breadth` is the single breadth reward.
- **Uniform tier points.** The 60-point international exception is retired, so every fund earns its tier's points. `region: intl` marks international funds for identity resolution only.
- **One canonical score.** `final_score` / `leaderboard_rank` are the only score and rank. `apex_score` / `apex_rank` are aliases kept for schema stability; `conviction_gate` is gone. Apex scores keep one decimal: integer truncation would have collapsed 46% of the board to 0.
- **Dominance diagnostics on linear mass.** `max_etf_share` (which drives CONCENTRATED) and `top_etf_share` are computed on linear conviction mass Σ points × C, so the calibrated 0.85 threshold keeps its meaning (≈ 200 flags before and after).
- **Staleness guard** (`stale_snapshot_days: 30`).

**Security identity** (`Sanitizer._resolve_identities`, `config.yaml`)

- **Collision split.** A ticker splits when its holders' reported company names form ≥ 2 clusters with no distinctive word in common, and exactly one cluster includes a US fund. The international cluster becomes `<ticker>.<TAG>` (`MTN.MTN`, `KTB.KRUNG`, `TEL.PLDT`). **42 splits** on the live store, all reviewed by hand.
- **Suffix merge.** `ROOT.SUF` (SUF an exchange or board code) merges into ROOT when exactly one identity of ROOT shares a name word with it. **134 merges**, all reviewed. Name matching needs an exact distinctive word or two abbreviation prefixes ("IND + COMM BK" ↔ "Industrial & Commercial Bank"), so `NXT.LN` (NEXT plc) stays apart from `NXT` (Nextpower) and `ENR.GR` (Siemens Energy) stays apart from `ENR` (Energizer). Siemens Energy now resolves to one 4-fund identity: PIZ's bare `ENR` is split from Energizer, then FPXI/IMOM/GRIN's `ENR.GR` merges into it.
- **Curated overrides.** Eight row-level, name-matched overrides cover collisions *between two international exchanges*, where no rule is safe: `2313` Compeq (TW) vs Shenzhou (HK), which was a fake 4-fund HC; `2382`, `1519`, `6269`, `8210`, `JSW`, `LTM`, `SAP`.
- **Share-class renames.** `005935→005930`, `PETR3→PETR4`, `ITUB3→ITUB4`, Hyundai prefs, `BBDC3→BBDC4`, `FOX→FOXA`, `NWS→NWSA` and others.
- **Price symbol map.** `data/symbol_map.json` entries that pointed a bare ticker at the international listing (e.g. `MTN → MTN.JO`) moved to the new identity (`MTN.MTN → MTN.JO`). The 15 affected detail files were removed so the coverage loop regenerates them under the corrected mapping.

**UI** (`docs/index.html`): the redundant Apex column, sort option, CSV columns and "apex vs raw" explainer line are removed, and the Score tooltip describes v4.

---

## 6. Does it catch risers early and drop fallers? (motion signals)

The product's purpose is to catch names before they run and drop them as conviction unwinds, so motion was tested separately from the score level. Universe: top-300 names; outcome: 20-day forward currency-demeaned return.

| Motion signal (IC, NW t) | 30-ETF (Jun–Oct) | 16-ETF (Mar–May) |
|---|---:|---:|
| Score change over 10 snapshots — pre-v4 | −0.023 (−0.7) | +0.055 (1.3) |
| Score change over 10 snapshots — v4 | −0.028 (−0.8) | +0.050 (1.0) |
| Linear conviction-mass change (v4, pre-κ) | −0.027 (−0.8) | +0.056 (1.2) |
| **Change in number of holding funds** | **+0.014 (1.2)** | **+0.078 (3.3)** |

- **Score and rank motion behave like the score level.** They help in a momentum-friendly regime and hurt in a momentum unwind. Pre-v4 and v4 are statistically indistinguishable; κ does not amplify drift noise.
- **Fund-count change is the only motion signal positive in both regimes.** Calendar-window events for top-200 names over the next 20 days:
  - *Lost a fund in the last 14 days:* −5.5% vs peers in Feb–May and −5.9% in May–Oct. This is **the steadiest warning in the data.**
  - *Gained a fund:* +16.9% in Feb–May, but no edge during the May–Oct unwind.
  - The 30-day window predicts as well as 14 days. The UI therefore keeps its existing 30-day `etf_count_delta_30d` column, and v4 adds **Funds ↑ / Funds ↓ filter chips** to screen on it directly.
- **v4 moves deteriorating giants.** The old engine held GEV at #2 and SNDK at #1 through the whole Aug–Oct decline. Under v4 their ranks drift down as their holdings soften (GEV 5 → 11, SNDK 2 → 9), and STX falls 16 → 82 (vs 4 → 34 before). The rank now reflects the trimming, while the score stays a holdings measure, not a price filter.
- **Follow-up, not in v4.** BURST and CRATER use a fixed 40-rank threshold. On a 3,800-name board most hits are low-ranked names whose near-tie scores churn: ~800–950 flags both before and after v4, median rank ~1,200–1,800. A rank-relative definition needs its own validation.

## 7. Known limitations (unchanged by v4)

- **Overweight is measured against equal weight, not a benchmark.** In cap-weighted funds, part of C is company size. That is why EM mega-caps (TSMC, MediaTek, Samsung, SK hynix) top the v4 board; they also ranked #1–#7 under pre-v4 `apex_score`. A size-neutral measure needs market caps for the whole universe; `data/ticker_metadata.csv` covers 1,224 of ~3,900 names.
- **Forward-return evidence is short and regime-dependent.** See §2. v4 was chosen on structural grounds plus internal outcomes that agree in both universes, not on a return backtest.
- **Price is not in the score.** Momentum predicts future ETF additions better than holdings do (§3), but importing it would turn a holdings-convergence product into a momentum screen. That is a product decision, not a scoring fix, so it is left out deliberately.
- **Identity resolution depends on reported names.** Holder-reported names must share a distinctive word. A same-exchange collision with matching names, or a company reported under two unrelated names (e.g. "Antam" vs "Aneka Tambang"), is left as is. Each build logs every split, merge and override for audit.

---

## 8. The audit's findings, answered

| # | Audit finding | Verdict | Resolution |
|---|---|---|---|
| 1 | Sticky giants (SNDK, GEV pinned at #1/#2 through −20–30% drawdowns) | **Partly right, wrong mechanism.** Their holdings didn't change: SNDK is still 6–7% of CSD, FPX and RPG. What pinned them was the **2.16× breadth double count** plus low-conviction Core/Quality positions adding up. The audit's "rank-bracket resilience" argument (1.5× / 1.2× brackets) applies to *legacy* mode only; apex uses a continuous rank percentile. | Double count removed and κ = 2: SNDK #1 → #9, GEV #2 → #11, STX #6 → #82. Price-based dampening was tested and rejected (§4). |
| 2 | Trimming penalty on rallying names | **Real but mostly passive.** Weight changes are mostly price drift, and rebalance trims are real portfolio actions that a holdings signal should record. Forward data shows no penalty worth correcting. | No change (Rec. B tested, §4). |
| 3 | Breadth fillers (AMAT, QCOM, KLAC…) | **Confirmed.** | κ = 2: AMAT #38 → #264, KLAC #68 → #339, APP #58 → #876, WDC #42 → #400. |
| 4 | Score Δ uncorrelated with price Δ (r = 0.004) | **Expected, not a defect.** ETFs rebalance on schedules, so holdings lag price. The relevant test is forward-looking (§3): the score has no reliable return information beyond momentum, and no reshaping changes that. | Documented as a limitation. |
| 5 | UI sorts by `leaderboard_rank`, not `apex_rank` | **Correct observation, wrong fix.** Pre-v4 `apex_score` swapped breadth inflation for single-fund inflation and instability (§3). | One canonical rank. The Apex column is removed and `apex_*` are aliases. |
| 6 | Dual-class / cross-listing fragmentation | **Confirmed, and much larger than reported** (~135 suffix fragments). The audit's proposed `ENR.GR → ENR` merge would have been a bug, because `ENR` is Energizer. The audit missed the reverse failure: **50 collisions** that merged *different* companies, fabricating HC names such as MTN. | Name-verified identity resolution + curated share-class renames (§5). |
| 7 | Single-fund sovereigns buried (#200–#490) | **Confirmed.** | κ = 2 lifts genuine single-fund overweights: Alibaba #208 → #43, Reliance #286 → #110, ICICI #318 → #117, HDFC #343 → #138. CSU fell instead (#490 → #1363) because PIZ exited it on 2026-09-30 and GRIN now holds it *below* equal weight. The audit's 0.92 conviction was stale. |
| 8 | 18 of 30 ETFs a day behind | **Not a defect.** Each date uses each fund's latest snapshot as of that date, which is the correct as-of semantics. The real risk was unbounded staleness, which the audit didn't flag. | Staleness guard (30 days). |

---

## 9. Reproduce

```bash
pip install -r requirements.txt
python scripts/scoring_study.py cache --config <pre-v4 config.yaml> --out /tmp/study/v3   # ~6 min
python scripts/scoring_study.py cache --config config.yaml           --out /tmp/study/v4
python scripts/scoring_study.py variants --cache /tmp/study/v3     # §3 tables (+ V0 == engine check)
python scripts/scoring_study.py engine   --cache /tmp/study/v3 /tmp/study/v4   # §1 tables
```
The pre-v4 config is `config.yaml` at commit `4f79d3dd`.
