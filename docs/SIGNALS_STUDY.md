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
| **… ≥ 0.5 over 10 snapshots, from top-300, + lost a fund (shipped)** | **9 / 6** | **−2.0% (t −2.0)** | **−1.7% (t −0.9)** |

### Why these rules

- **Fund confirmation is what makes both flags work.** Without it, BURST and CRATER behave like rank momentum: right in one regime and wrong in the other. This matches the v4 motion study: a change in the number of holding funds is the only motion signal that held up across both regimes.
- **BURST.** Fund-confirmed BURST is the best variant in the momentum-friendly regime (+5.2%). In the unwind it does not lose meaningfully (−0.9%, not significant), whereas the unconfirmed rank surge loses −2.6%.
- **CRATER.** Fund-confirmed CRATER is the only variant negative in **both** regimes. That makes it the "get out" signal it was meant to be.
- **Smoothing.** Once the partial-snapshot guard removes glitches at the source, the 3-snapshot median makes little measurable difference: 19.3 vs 19.4 flags a day, similar returns. It is kept as a cheap backstop for glitches the weight-sum guard cannot see, such as a scrape that drops one name but still sums to ~100%.

### The shipped rule (`config.yaml → motion`, `conviction/history.burst_crater_panels`)

On the leaderboard rank smoothed by a rolling median of 3 snapshots, over the last 10 snapshots (~2 weeks):

- **BURST:**
  - the rank improved by at least a factor of 1.65 (log 0.5, e.g. #250 → #150);
  - it is now inside the top 300;
  - it is within 15% (log) of its best in the window, so the move is still in force;
  - the name **gained ≥ 1 holding fund**.
- **CRATER:**
  - the rank worsened by at least a factor of 1.65;
  - it started inside the top 300;
  - it is still within 15% of its worst;
  - the name **lost ≥ 1 holding fund**.

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

## 4. Limitations

- **A short window.** About eight months and two regimes. The BURST and CRATER effects have t-statistics of 0.7–2.0 per regime, so they are directionally consistent rather than proven. The old rules showed no edge at all, and one pointed the wrong way.
- **HIGH_CONVICTION carries momentum exposure.** It is a statement about consensus and its durability, not a return forecast. Its returns follow the momentum regime.
- **Fund confirmation means BURST/CRATER wait for a fund to act.** Most tracked funds rebalance monthly or quarterly, so flags cluster around rebalance dates and can be absent for a week or two.
