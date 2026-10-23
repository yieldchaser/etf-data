# HIGH_CONVICTION, BURST and CRATER — Signal Study

**Date:** 2026-10-23 · **Reproduce:** `scripts/scoring_study.py cache` → `scripts/signals_study.py` · **Builds on:** `docs/SCORING_V4_STUDY.md`

These three flags are the product's trading signals:

- **HIGH_CONVICTION:** "these are the names independent strategies agree on."
- **BURST:** "this name is starting to go up."
- **CRATER:** "get out."

This study checks each flag against what happened next and rebuilds the ones that failed. It uses the v4 engine replayed over every snapshot (Feb–Oct 2026), with a data fix (§1) applied first.

**Outcome measures:** forward 10- and 20-trading-day returns from T+2, winsorized and **demeaned within each currency group** (so a flag is measured against peers, not against its own market); whether a flagged name gained or lost a holding fund within 30 days; and how many names each rule flags.

**Two regimes:**

- **16-ETF** (Mar–mid-May, momentum-friendly);
- **30-ETF** (Jun–Oct, a momentum unwind).

A rule is only credited with what holds in both.

---

## 1. Root cause of the one-day rank glitches: partial ETF scrapes

A rank that jumps from #100 to #140 for a day and comes straight back is almost always a **partial scrape**.

**The example:** on 2026-08-29 EEMO's snapshot held 231 of its ~261 names, with weights summing to **0.72**. SK hynix, its largest position (~13%), was missing. The engine took that snapshot as EEMO's current book, so SK hynix "lost" a fund and fell **#3 → #34** for two days.

**Across the store:** 15 such snapshots out of 4,210. They are EEMO on 06-03, 08-29, 10-05 and 10-06; JHEM on 06-16 and 06-29 (half the book missing); PIE on 02-25; and eight funds on 02-23 that scraped only their top 10 rows.

**Fix — partial-snapshot guard** (`sanitizer.partial_snapshot_min_ratio: 0.90`):

- **The rule:** a fund snapshot whose *sanitized* weight sum is below 90% of the median of that fund's previous 10 snapshots is skipped, and the fund's last complete book stays in force.
- **As-of safe:** it looks only backwards.
- **Reconstitutions pass:** a genuine reconstitution passes untouched. EEMO's late-September cut from 261 to 211 names kept weights at ≈100%.
- **Cash rows excluded:** it runs after cash rows are removed. EEMO's 09-21 snapshot summed to 1.33 only because of a 31% cash line.

SK hynix now stays at #1–2 through 08-29. Every downstream consumer reads the same sanitized holdings, so the guard also fixes the fake exits, score deltas and changelog entries those scrapes produced.

---

## 2. HIGH_CONVICTION

**Problem.** The rule was "held by ≥ 4 distinct ETFs."

- **16-fund universe:** it flagged ~38 names.
- **30-fund universe:** ~200. Several of the new funds hold 500–1,300 names each (AVSC, JHEM, JHMM, MFEM, JHSC), so four holders stopped being a meaningful bar.

| Candidate (weekly averages) | Names (30-ETF) | Lost a holder within 30d | 20d vs peers (30-ETF) | Names (16-ETF) | Lost a holder | 20d vs peers (16-ETF) |
|---|---:|---:|---:|---:|---:|---:|
| ≥ 4 holders (old) | 200 | 27.1% | −0.1% | 38 | 12.1% | +5.3% |
| ≥ 5 holders | 76 | 26.9% | −0.3% | 14 | 12.9% | +4.6% |
| ≥ 2 holders at conviction ≥ 0.7 (any fund) | 64 | 10.7% | −1.3% | 13 | 12.3% | +8.2% |
| ≥ 4 holders, ≥ 2 at C ≥ 0.6 (any fund), ≥ 2 tiers | 30 | 16.0% | −1.3% | 12 | 10.6% | +9.4% |
| **≥ 4 holders, ≥ 2 non-benchmark at C ≥ 0.6, ≥ 2 non-benchmark tiers (shipped)** | **22** | **15.3%** | −2.3% | **5** | **0.0%** | **+7.3%** |

*"Vs peers" is the flagged names' average 20-day currency-demeaned return minus the average of all names held by ≥ 2 funds. The 30-ETF rows cover 16 weekly dates and the 16-ETF rows 11.*

**Why exclude the benchmark tier.** Without the exclusion, NVDA (#330) and AVGO (#1,293) made the list. The only "overweight" holders were QQQM and XLG, the cap-weighted Core benchmarks, which overweight the largest companies by construction. That is size, not conviction. Core holdings now count neither as conviction holders nor as an independent strategy.

**The shipped rule:** HIGH_CONVICTION = broad **and** convinced **and** independent:

- **Broad:** ≥ 4 holders.
- **Convinced:** ≥ 2 non-benchmark funds hold the name at conviction ≥ 0.6, roughly 2× equal weight near the top of the book.
- **Independent:** those holders span ≥ 2 non-benchmark strategy tiers.

**Today:** 24 names. SNDK, DINO, GEV, MU and **AAPL** qualify; Apple is overweight in PDP, SPHQ and SPMO. TSMC and Samsung lead the score but are held by EM-Quant funds only, which is one lens, so they are not HC. NVDA and MSFT are not HC, because only the benchmark funds overweight them.

**Trade-off.** Genuine conviction in momentum-run funds carries more momentum exposure. HC names did better than the old set in the momentum-friendly regime and worse in the unwind (−2.3% vs −0.1%). The stable gain is that **the consensus holds**: HC names lose a holder half as often.

**Side effects, deliberately contained:**

- **SPECULATIVE_BETA:** keeps its old population (names with fewer than 4 holders). Broad names that miss the new HC bar, such as GOOGL and NVDA, are not relabelled "speculative beta".
- **Pre-HC chip:** now means one step from the new bar: ≥ 3 holders, ≥ 1 conviction holder, ≥ 2 tiers, ranks and score improving.
- **Legacy scoring mode:** keeps the raw-count rule.

---

## 3. BURST and CRATER

### What was wrong

The old rule over a 30-day window:

```
peak = worst rank − best rank            # RANGE, no direction
BURST  = peak ≥ 40 AND rank better than the window median in ≥ 8 of the last 10 snapshots
CRATER = peak ≥ 40 AND rank worse  than the window median in ≥ 8 of the last 10 snapshots
```

1. **No direction.** A name at #100 that glitches to #140 for one day has a range of 40, which passes the size test for *both* flags. A name that only falls has a large "peak improvement" too.
2. **An absolute threshold on a 3,800-name board.** 40 positions is noise at rank #1,700. The flags fired on **~870 (BURST) and ~810 (CRATER) names a day**, with a median rank of ~1,700–1,900.
3. **No link to what funds did.** Rank alone also moves with the price drift of other names.
4. **Measured result: no edge.** CRATER names **outperformed**.

The general `global_rank_peak_30d` column used the same direction-blind range, so a pure faller displayed its fall as an "improvement".

A second copy of the rule lived in `history.signal_history`. The backtest's BURST/CRATER strategies read a `burst_30d` column that the historical leaderboards never had, so they reported no trades.

### Candidates (onset events: the first day a name is flagged, no re-onset within 20 snapshots)

**BURST**

| Rule | Flags/day (30 / 16) | 20d vs peers, 30-ETF | 20d vs peers, 16-ETF |
|---|---:|---:|---:|
| Old (range ≥ 40, 30 days) | 872 / 151 | −0.5% (t −3.8) | +0.1% (t 0.1) |
| Smoothed, log-rank ≥ 0.5 over 10 snapshots, top-300 | 19 / 20 | −2.6% (t −3.5) | +3.1% (t 2.3) |
| … same, no smoothing | 19 / 18 | −2.5% | +2.9% |
| … log-rank ≥ 0.7 | 11 / 13 | −1.0% | +1.5% |
| … 20-snapshot lookback, ≥ 0.7 | 24 / 21 | −1.9% | +4.3% |
| **… ≥ 0.5 over 10 snapshots, top-300, + gained a fund (shipped)** | **8 / 6** | **−0.9% (t −0.7)** | **+5.2% (t 2.0)** |

**CRATER**

| Rule | Flags/day (30 / 16) | 20d vs peers, 30-ETF | 20d vs peers, 16-ETF |
|---|---:|---:|---:|
| Old (range ≥ 40, 30 days) | 811 / 174 | **+0.4% (t +2.7)** (wrong sign) | +0.2% |
| Smoothed, log-rank ≥ 0.5 over 10 snapshots, from top-300 | 22 / 19 | −1.7% (t −2.3) | +0.8% |
| … log-rank ≥ 0.7 | 13 / 12 | −3.9% (t −4.1) | −0.1% |
| … 20-snapshot lookback | 34 / 22 | −1.9% | 0.0% |
| **… ≥ 0.5 over 10 snapshots, from top-300, + lost a fund** (shipped first; see the CRATER window follow-up) | **9 / 6** | **−2.0% (t −2.0)** | **−1.7% (t −0.9)** |

### Why these rules

- **Fund confirmation is what makes both flags work.** Without it, BURST and CRATER behave like rank momentum: right in one regime and wrong in the other. This matches the v4 motion study: a change in the number of holding funds is the only motion signal that held up across both regimes.
- **BURST.** Fund-confirmed BURST is the best variant in the momentum-friendly regime (+5.2%). In the unwind it does not lose meaningfully (−0.9%, not significant), whereas the unconfirmed rank surge loses −2.6%.
- **CRATER.** Fund-confirmed CRATER is the only variant negative in **both** regimes. That makes it the "get out" signal it was meant to be.
- **Smoothing.** Once the partial-snapshot guard removes glitches at the source, the 3-snapshot median makes little measurable difference: 19.3 vs 19.4 flags a day, similar returns. It is kept as a cheap backstop for glitches the weight-sum guard cannot see, such as a scrape that drops one name but still sums to ~100%.

### The shipped rule (`config.yaml → motion`, `conviction/history.burst_crater_panels`)

On the leaderboard rank smoothed by a rolling median of 3 snapshots, over the last 10 snapshots (~2 weeks) for BURST and 20 (~4 weeks) for CRATER:

- **BURST:**
  - the rank improved by at least a factor of 1.65 (log 0.5, e.g. #250 → #150);
  - it is now inside the top 300;
  - it is within 15% (log) of its best in the window, so the move is still in force;
  - the name **gained ≥ 1 holding fund**.
- **CRATER:**
  - the rank worsened by at least a factor of **1.35** (log 0.3);
  - it started inside the top 300;
  - it is still within 15% of its worst;
  - the name **lost ≥ 1 holding fund**;
  - all measured over **20 snapshots** (~4 weeks), not BURST's 10 — see the two follow-ups below.
- **SURGE / SLIDE** (momentum tags, not signals): BURST's rank move (×1.65 over 10 snapshots, top 300, in force) up or down, with **no** fund change. A name that is BURST / CRATER is not also tagged.

### CRATER window (follow-up 1)

Yageo (2327) fell 55% from its June peak while its rank slid from #6 to #27, and no CRATER fired. PIE sold it at the start of July, but the rank only fell ×1.65 about 13 snapshots later. By then the exit was outside the 10-snapshot window. A fund's exit often comes *before* the slide, so CRATER windows were re-tested (`scripts/signals_study.py <cache> bc`):

| CRATER variant | 20d vs peers, 30-ETF | 16-ETF | Yageo |
|---|---:|---:|---|
| 10-snapshot move + fund exit over the same 10 (previous) | −2.0% (t −2.0) | −1.7% (t −0.9) | never flagged |
| 10-snapshot move + fund exit within 20 | −2.3% (t −2.4) | −0.5% (t −0.2) | 2026-07-18 |
| 10-snapshot move + fund exit within 30 | −2.1% | −0.5% | 2026-07-18 |
| **20-snapshot move + fund exit over the same 20** (then loosened to ×1.35, follow-up 2) | **−2.4% (t −2.2)** | **−1.3% (t −0.7)** | **2026-07-22** (~650 TWD; low 502 on 2026-08-02) |

The shipped variant is the only one at least as strong as the previous rule averaged across both regimes, and it catches the fund-first case. The same widening **hurt BURST** (30-ETF: −0.9% → −1.6% to −2.8%), so BURST keeps its 10-snapshot window: an addition long before a rank surge is weak evidence; an exit long before a slide is not. Note that Yageo was also on the Funds ↓ list for 14 days after PIE's exit, in early July near the top.

### Motion study (follow-up 2): what BURST and CRATER should be

The redesign cut BURST from ~580 flags a day to ~9 (zero on 41% of days) and CRATER from ~590 to ~15, and the user's experience of the old flags was that they moved with price. Both observations are right. The question is which behaviour carries information.

**What the old flags were** (replay, Mar–Oct; returns vs currency peers):

| | flags/day | days with none | median rank | flags below #300 | prior 10d | next 20d |
|---|---:|---:|---:|---:|---:|---:|
| Old BURST (range ≥ 40) | 581 | 0% | #1,372 | 88% | **+4.0%** | +0.6% |
| Old CRATER | 588 | 0% | #1,619 | 93% | **−3.5%** | +0.1% |
| BURST (fund-confirmed) | 9 (median 2) | 41% | #124 | 2% | +4.8% | +1.7% |
| CRATER (fund-confirmed, ×1.65 over 20 snapshots) | 15 (median 7) | 8% | #406 | 65% | −3.3% | −3.0% |

The old flags described the move that had just happened across most of the board, which is why they felt in sync with price. They predicted nothing.

**Twenty rules, both regimes** (`scripts/motion_features.py`, `scripts/motion_study.py … rules`). "Leans in / cuts" is a holding fund changing a name's weight by ≥ 20% **net of price drift**. The fund return is computed from its own book; the six funds that publish weights a day stale are lagged a day, and the four whose weights never drift (JHEM, JHMM, MFEM, AVSC) count any change as deliberate. Onset events, 20-day return vs currency peers:

| BURST rule | flags/day | Jun–Oct (30 ETFs) | Feb–May (16 ETFs) |
|---|---:|---:|---:|
| **×1.65 rank rise + a fund joined (shipped)** | 8 / 6 | **−0.9% (t −0.7)** | **+5.2% (t 2.0)** |
| ×1.65 rank rise, no fund needed | 19 / 19 | −2.6% (t −3.5) | +2.9% (t 2.1) |
| ×1.35 rank rise, no fund needed | 41 / 31 | −1.7% | +2.6% |
| 1+ fund leans in (net of price) | 28 / 12 | −2.9% (t −4.5) | +2.1% |
| 2+ funds lean in | 3 / 1 | −4.4% | +5.6% (10 events) |
| ×1.35 rise + (fund joined or leaned in) | 22 / 12 | −2.4% | +2.5% |
| own-book rank up in ⅔ of holders | 20 / 4 | −1.8% | +7.6% |
| price: top 5% 10-day return in the top 300 | 15 / 13 | −2.3% | +4.8% |
| ×1.35 / ×1.2 / any rise + a fund joined | 11–18 / 7–9 | −1.6% to −1.7% | +4.4% to +5.4% |

| CRATER rule | flags/day | Jun–Oct | Feb–May |
|---|---:|---:|---:|
| ×1.65 rank fall over 20 + a fund left (previous) | 18 / 11 | −2.4% (t −2.2) | −1.3% (t −0.7) |
| **×1.35 rank fall over 20 + a fund left (shipped)** | 22 / 13 | **−2.9% (t −2.8)** | **−1.0% (t −0.6)** |
| ×1.2 / any fall over 20 + a fund left | 24–31 / 13–15 | −2.7% | −0.5% to −0.8% |
| ×1.65 rank fall over 10, no fund needed | 22 / 18 | −1.7% (t −2.3) | +0.9% |
| 1+ fund cuts (net of price) | 23 / 8 | −1.5% | **+3.1%** |
| ×1.35 fall + (fund left or cut) | 24 / 11 | −1.5% | +0.1% |
| own-book rank down in ⅔ of holders | 17 / 5 | −0.7% | +3.4% |
| price: bottom 5% 10-day return | 15 / 13 | −0.8% | **+2.4%** |

Every rule built from price, rank or re-weighting alone is momentum. It paid in Feb–May and reversed in Jun–Oct, with t-stats of 2–4 in **both** directions. Active re-weighting looked like the natural "fund leaning in" signal, but it is mostly rules-based momentum funds buying recent winners, so it carries the same sign flip. Only a fund **joining or leaving** kept its sign in both regimes.

**Can the momentum regime be timed?** (`… regime`) Each momentum rule's onsets were split by how the same rule's onsets had done over a trailing window whose 20-day outcomes were already known (30–60 and 30–90 days back). The trailing record was **anti-predictive**: correlation −0.06 to −0.17. Own-book-rank onsets returned −4.4% (t −3.4) exactly when their trailing record looked good. Momentum turns at its peak, so a real-time switch would have bought at the turn.

**Decision.**

- **BURST** stays as shipped. It is the only rise rule without a significant loss in either regime, and every way of widening it (lower threshold, longer window, re-weighting as confirmation) doubled its loss in the unwind.
- **CRATER** loosens to ×1.35 over its 20-snapshot window: at least as strong in both regimes, about 20% more coverage.
- **SURGE / SLIDE** carry the rank move without a fund change. They give back the always-on, in-sync-with-price view (~19 a day each, a day with none is rare) and are labelled as momentum with both regimes' numbers, never as a forecast.

**One implementation everywhere.** This function now drives:

- today's flags (`build.py`), which carry the move size (`burst_rank_gain`, `crater_rank_loss`, `motion_rank_then`) and the confirming fund change (`motion_funds_delta`) for the UI;
- the per-ticker flag history (`signal_history`);
- the backtest's BURST and CRATER trigger strategies.

`global_rank_peak_30d` is now directional: the largest rise from an earlier point to a later one, on the smoothed rank.

**Behaviour on recent data:**

- **Bursts and craters cluster around real fund events.** EEMO's late-September reconstitution added Taiwan financials (BURST) and dropped several Korean names (CRATER).
- **Quiet weeks produce no flags.**
- **Mild drift is not a CRATER.** GEV's drift from #8 to #11 with no fund leaving is not flagged.

---

## 4. Every other signal — audit

`scripts/signal_audit.py` replays each remaining signal with the definition the site used, on every snapshot, and scores it on weekly sample dates. The measure is the members' mean 20-day forward return vs currency peers, with a t-statistic over the weekly series. A signal is kept only if that sign agrees across both regimes, or if it is a descriptor rather than a call.

| Signal (old definition) | Members/day (30 / 16) | 20d vs peers, 30-ETF | 20d vs peers, 16-ETF | Decision |
|---|---:|---:|---:|---|
| Pre-HC (exactly 3 funds, ranks improving) | 25 / 4 | −0.4% | +0.9% | **Redefined** |
| Pre-HC (new: one step from the HC bar) | 13 / 5 | +0.1% | **+7.4% (t 3.5)** | Shipped |
| Stealth-Buy (≤ 2 funds, +10% weight in 7d) | **251** / 54 | −0.3% | +2.1% | **Redefined** — mostly price drift in small positions |
| Stealth (new: 2nd fund joined in 14d, top 500) | 4 / 12 | +1.7% | **+4.9% (t 2.0)** | Shipped |
| Funds ↓ (any name, 30d) | 157 / 32 | **+2.0%** (wrong sign) | −2.0% | **Redefined** |
| Funds ↓ (new: top-200, lost a fund in 14d) | 8 / 5 | **−7.0% (t −2.8)** | −2.7% | Shipped |
| Funds ↑ (any name, 30d) | 707 / 127 | +0.2% | +2.1% | **Redefined** |
| Funds ↑ (new: top-200, gained a fund in 30d) | 48 / 37 | +1.5% | **+6.2% (t 5.3)** | Shipped |
| CONCENTRATED | 195 / 71 | 0.0% | −2.0% (t −2.6) | **Kept as a risk descriptor.** The "best risk-adjusted flag" claim is withdrawn. |
| SPECULATIVE_BETA | 70 / 112 | −0.3% | +0.4% | **Removed** — no edge, and its premise (momentum without fundamental support is speculative) is not borne out |
| Quality+ | 296 / 95 | +0.8% (t 1.7) | +0.4% | Kept — weak but consistent |
| Q+Q chip (Quant + Quality + recent pickup) | 153 / 9 | +0.9% | −2.3% | **Removed** — sign flips |
| ACCUM badge (≥ 2 funds added in 30d) | 168 / 4 | −1.6% | +3.5% | **Removed** — pure momentum; Funds ↑ covers it |
| EARLY badge (2–4 funds, any weight rise) | **822** / 111 | −0.2% | +1.7% | **Removed** — flags half the board |
| CROWDED badge | 0 / 0 | — | — | **Removed** — never fired |
| NEW | 136 / 66 | −2.3% | +6.2% | Kept — a factual event, momentum-exposed |
| Momentum regime "declining" | 14 / 6 | −3.6% | −1.1% | Kept as a display label; the other regimes carry no edge |
| "Extreme conviction moves" ribbon (top \|z\| ≥ 2) | 8 / 7 | ~0% / −0.9% | −0.7% / −3.3% | **Replaced** by a strip of today's validated signals |

The deep-board rank churn that drove the ribbon (e.g. "+1,902 ranks") is the same artifact that broke the old BURST/CRATER.

**What made the redefinitions work:**

- **A relevance floor.** The top-200 or top-500 restriction carries most of the gain. Deep-board names churn without consequence.
- **Funds, not weights.** A fund adding or dropping a name is a decision. A weight change is mostly price drift.

**Where the definitions now live.** Every chip is now a build-time column (`funds_up`, `funds_down`, `stealth_buy`, `pre_hc`, `etf_count_delta_14d` — `history.screening_signals`). That gives each one a single tested definition instead of browser-side logic.

**Bug fixed along the way.** Holder-count deltas used to treat a name absent from the board N days ago as "no change". It is now counted as zero holders, so a brand-new two-fund name shows +2.

## 5. Fund flows on leveraged ETFs (2026-10)

**Question:** can a leveraged ETF's own creations and redemptions tell you when to buy its lows or sell its highs?

**Data and method** (`scripts/flow_study.py`, reproducible):
- Every long-side fund in `data/flows/all_leveraged_etf_flows.csv`: 137 funds, 175k fund-days, 2016–2026.
- Price z = 20-day NAV return / (60-day return std × √20). Flow z = 5-day net flow / (60-day flow std × √5).
- Outcome: the next 20 days' NAV return, entered the day after the signal.
- Each fund-day is compared with all funds **in the same price state on the same date**. This isolates what the flow adds beyond "the price fell / rose", which on its own already moves forward returns.

**20-day return vs same-date, same-price-state funds:**

| Setup | 2016–20 | 2021–23 | 2024–26 | Reading |
|---|---|---|---|---|
| Inflows after a drop | +0.26% | +0.16% | +0.92% | The only setup with the same sign in all three eras. Small next to 20-day swings of ±15–30%. |
| No unusual flow after a drop | −0.27% | −0.17% | −0.38% | The mirror of the row above. |
| Outflows after a drop ("washout") | +0.51% | +0.31% | −0.06% | Fading. |
| Inflows after a rally ("climax") | +0.58% | −0.85% | +1.16% | Flips sign. |
| Outflows after a rally ("distribution") | −0.38% | +0.50% | +0.45% | Wrong way round in two of three eras. Not a sell warning. |

Flows also lag price a little: corr(1-day flow z, prior-day return) = −0.03. Buyers arrive after drops.

**Outflow streaks.** `docs/research_alpha_findings.md` reported 67–80% win rates after four-day redemption streaks. Win rates there are absolute, so they include the base rate: TQQQ's next 20 days were positive 56–72% of the time in any case. Against same-date, same-price-quintile peers, a streak of exactly four days did come out positive in all three eras (+1.15% / +0.65% / +1.67%). Streaks of three, five and six days flip sign (e.g. five days: +1.37% / −0.15% / +0.25%), so the "four" looks like a threshold picked in-sample, not a mechanism.

**What changed on the Fund Flows tab:**
- **Scanner.** The scanner's per-fund "Win %" counted any positive 20-day return, mostly the base rate of a bull fund. Its episodes were only 5 days apart, so their 20-day windows overlapped and the counts were inflated. Funds with no episodes showed a placeholder 50%. The scanner now shows a fund's record against its own typical 20 days, with episodes at least 20 days apart, and only when there are ≥ 8 of them.
- **Opportunity score.** The arbitrary "opportunity score /100" and the 1Y/3Y/All slices of those tiny samples are removed. The setups are named for what they are (inflows / outflows after a drop / rally), and each column says what the pooled history above shows.
- **Strategy signals view.** The "Systematic Strategy Signals & Conviction Basket" view (+72.8% CAGR, 2.84 Sharpe) is removed. It was a file generated once on 2026-10-05 by a script that read inputs from a local scratch directory outside the repo, so it could not be rebuilt or checked. It was still displayed as live ("Next rebalance 2026-10-09"), and its claimed edge is far beyond anything the pooled data supports.

**Bottom line:** flows are context, not a trading edge, on this data. "Money is buying the dip" is the one consistent positive, and it is small.

## 6. Limitations

- **A short window.** About eight months and two regimes. The BURST and CRATER effects have t-statistics of 0.7–2.0 per regime, so they are directionally consistent rather than proven. The old rules showed no edge at all, and one pointed the wrong way.
- **HIGH_CONVICTION carries momentum exposure.** It is a statement about consensus and its durability, not a return forecast. Its returns follow the momentum regime.
- **Fund confirmation means BURST/CRATER wait for a fund to act.** Most tracked funds rebalance monthly or quarterly, so flags cluster around rebalance dates and can be absent for a week or two.
