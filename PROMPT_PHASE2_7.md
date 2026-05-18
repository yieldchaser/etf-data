# Phase 2.7 — BURST False-Positive Fix + Honesty in Δ% + Four 75→100 Upgrades

Working in `yieldchaser/etf-data`. Phase 2.6 is deployed. Three real bugs need surgical fixes, then four substantial upgrades that turn the platform from "good dashboard" into "institutional-grade decision system".

**Run Part A first** (bug fixes, low blast radius). Then **Part B** (the 4 upgrades, each independent).

---

## Part A — Three Bug Fixes

### A1. BURST is overfitting (395/920 names flagged ≈ 43% of universe)

#### Confirmed in live data

`python -c "import json; lb=json.load(open('docs/data/leaderboard.json')); print(sum(1 for r in lb if r.get('burst_30d')))"` returns **395**. A flag triggering on 43% of the universe is noise, not signal.

Two distinct failure modes confirmed in `leaderboard.json`:

**Mode 1 — Re-entry false positives.** `$CNY` has `global_rank_peak_30d: 901, global_rank_delta_30d: -900, etf_count: 1`. The ticker dropped off the leaderboard entirely then came back. NaN handling in `min()`/`max()` reads "absence" as "huge improvement", producing a phantom +900 swing.

**Mode 2 — Single-touch false positives.** `FLEX` has `global_rank_peak_30d: 40` exactly — it briefly touched +40 better than baseline for **one snapshot** then reverted. That's a one-day spike, not a burst.

#### Required fix in `predator/build.py` `_attach_velocity()`

Two changes to the burst computation:

1. **Require continuous presence** on the leaderboard throughout the 30d window (coverage ≥ 80%).
2. **Require the improved rank to be sustained** for ≥5 of the last 10 snapshots.

Replace the burst block:

```python
window_start = today_date - pd.Timedelta(days=30)
window_cols = [c for c in rank_panel.columns if c >= window_start]
if len(window_cols) >= 5:
    win = rank_panel[window_cols]
    nan_count = win.isna().sum(axis=1)
    coverage = (len(window_cols) - nan_count) / len(window_cols)

    first_col = win.iloc[:, 0]
    last_col  = win.iloc[:, -1]
    worst_in_window = win.max(axis=1)   # highest rank number = worst
    best_in_window  = win.min(axis=1)   # lowest rank number = best
    global_rank_delta_30 = (first_col - last_col).round(0)        # + = improved
    peak_improvement_30  = (worst_in_window - best_in_window).round(0)

    # Sustained: rank must be better than within-window median for ≥5 of last 10 snapshots
    recent10 = win.iloc[:, -10:] if win.shape[1] >= 10 else win
    median_per_ticker = win.median(axis=1)
    is_better_than_median = recent10.lt(median_per_ticker, axis=0)
    sustained_count = is_better_than_median.sum(axis=1)

    # Burst qualifier: peak ≥ 40 AND coverage ≥ 80% AND sustained ≥ 5 days
    is_burst = (peak_improvement_30 >= 40) & (coverage >= 0.80) & (sustained_count >= 5)
else:
    global_rank_delta_30 = pd.Series(dtype=float)
    peak_improvement_30  = pd.Series(dtype=float)
    best_in_window       = pd.Series(dtype=float)
    is_burst             = pd.Series(dtype=bool)

leaderboard["global_rank_delta_30d"] = leaderboard["ticker"].map(global_rank_delta_30).fillna(0).astype(int)
leaderboard["global_rank_peak_30d"]  = leaderboard["ticker"].map(peak_improvement_30).fillna(0).astype(int)
leaderboard["global_rank_best_30d"]  = leaderboard["ticker"].map(best_in_window).fillna(leaderboard["leaderboard_rank"]).astype(int)
leaderboard["burst_30d"]             = leaderboard["ticker"].map(is_burst).fillna(False)
```

#### DoD — A1

- After rebuild, `python -c "import json; lb=json.load(open('docs/data/leaderboard.json')); print(sum(1 for r in lb if r.get('burst_30d')))"` returns **between 15 and 60**.
- `$CNY`, `EUR`, `$JPY`, `HII`, `FGXXX` no longer have `burst_30d: True`.
- `STX` (the original motivating case from Phase 2.6) still has `burst_30d: True`.
- 2 new tests:
  ```python
  def test_burst_requires_sustained_presence(self, cfg):
      """A ticker that dropped off the leaderboard and returned shouldn't BURST."""
      # 30-day synthetic history where TICKER is rank-5 days 0-5, NaN days 5-20, rank-5 days 20-30
      # Assert burst_30d is False (coverage check fails — 30% gap)

  def test_burst_requires_sustained_improvement(self, cfg):
      """A ticker that touched +50 ranks for one day shouldn't BURST."""
      # 30-day synthetic history: rank 80 for 28 days, rank 20 for 1 day, rank 80 again
      # Assert burst_30d is False (sustained_count = 0)
  ```

### A2. `+0.0%` is silently lying when there's no past data

#### Confirmed in live data

RAL, NVT, SOLV, MU, STRL, KO, SANM all show `score_deltas_by_period.7d = 0.0` despite velocity scores of 35-117. These tickers weren't on the leaderboard 7 days ago — their score went from "absent" to (e.g.) 316. The build's `.fillna(0)` renders `+0.0%` to the user, which reads as "no change" when reality is "no comparable past data".

#### Required fix in `predator/build.py`

Find `score_deltas_by_period` (around line 144). Remove the `.fillna(0)`:

```python
# Before:
score_deltas_by_period[n] = leaderboard.set_index("ticker")[col].fillna(0).to_dict()

# After: preserve NaN → null so the UI can distinguish "no data" from "0%"
score_deltas_by_period[n] = leaderboard.set_index("ticker")[col].to_dict()
```

Same for the YTD block.

Per-row attachment:

```python
# Before:
r["score_deltas_by_period"] = {
    str(p): round(float(score_deltas_by_period.get(p, {}).get(t, 0)), 4)
    for p in [1, 7, 14, 30, 60, 90, "YTD"]
}

# After:
r["score_deltas_by_period"] = {}
for p in [1, 7, 14, 30, 60, 90, "YTD"]:
    v = score_deltas_by_period.get(p, {}).get(t)
    # Treat NaN, None, missing — all as null in JSON
    if v is None or (isinstance(v, float) and v != v):
        r["score_deltas_by_period"][str(p)] = None
    else:
        r["score_deltas_by_period"][str(p)] = round(float(v), 4)
```

#### Required fix in `docs/index.html` `fmtPct`

```javascript
fmtPct(n) {
  if (n === null || n === undefined) return '—';
  if (!isFinite(n)) return '—';
  const v = Number(n) * 100;
  return (v >= 0 ? '+' : '') + v.toFixed(1) + '%';
}
```

#### DoD — A2

- RAL, NVT, SOLV, MU, STRL, SANM render `—` under the 7d column (not `+0.0%`).
- Toggling 1d/30d/60d/90d/YTD: any period where the ticker wasn't on the leaderboard at the reference date shows `—`.
- SNDK, GEV (continuously present) still show numeric deltas.

### A3. Rank History chart collapses flat lines to a single pixel

#### What I saw

On `stock.html?t=FLEX`, the COWZ line is essentially a flat horizontal line because COWZ rank has been 43-44 for all 64 snapshots. The chart looks broken when it's actually correct — it's just uninformative.

#### Required fix in `docs/stock.html`

In the `rankLines` getter:

```javascript
const observedRange = maxRank - minRank;
const visualRange = Math.max(observedRange + 2, 4);  // +2 padding, min 4
const yRange = visualRange;
const rankToY = r => yTop + ((r - minRank) / yRange) * (yBot - yTop);

// In each line object:
flat: (lineMaxRank - lineMinRank) === 0 ? rows[0].r : null,
```

In the legend row template:

```html
<span x-show="line.flat !== null" class="text-[10px]" style="color: var(--text-3)"
      x-text="`(flat at #${line.flat})`"></span>
```

#### DoD — A3

- COWZ line on FLEX page renders with visible vertical space.
- "(flat at #44)" annotation appears next to COWZ in the legend.
- Active mover lines (QMOM, XMMO on FLEX) still render full-range, no compression.

---

## Part B — Four Upgrades That Take This 75 → 100

The platform today gives **descriptive** intelligence: who's HC, who's bursting, who's accelerating. To make it **prescriptive** — actually decision-enabling at institutional caliber — add these four.

### B1. Sector & geography flow overlay

#### Why this matters more than another metric

The dashboard tracks tickers, not sectors. But "ETFs rotating into semis, out of energy, toward Japan" is exactly the kind of insight that **leads price by 4-8 weeks** in the institutional flow literature (it's called "sector dispersion" in the Bloomberg world). Build it from data we already have.

#### Implementation

**1. Static `data/ticker_metadata.csv`:**

Generate via one-off `scripts/build_ticker_metadata.py`:
- Read unique tickers from `data/all_history.csv`
- For each: `yfinance.Ticker(t).info.get('sector')`, `industry`, `country`
- Cache, default `(Unknown, Unknown, Unknown)` on failure
- Commit the CSV (~920 rows)

Schema:
```
ticker,sector,industry,country,market_cap_usd
NVDA,Technology,Semiconductors,US,3500000000000
6857.JP,Technology,Semiconductor Equipment,JP,...
GEV,Industrials,Heavy Electrical Equipment,US,...
```

**2. Extend `predator/build.py`:**

```python
def _attach_metadata(leaderboard: pd.DataFrame) -> pd.DataFrame:
    try:
        meta = pd.read_csv("data/ticker_metadata.csv")
        leaderboard = leaderboard.merge(meta, on="ticker", how="left")
        for col in ["sector", "industry", "country"]:
            leaderboard[col] = leaderboard[col].fillna("Unknown")
    except FileNotFoundError:
        for col in ["sector", "industry", "country"]:
            leaderboard[col] = "Unknown"
    return leaderboard

leaderboard = _attach_metadata(leaderboard)

def _compute_flow(leaderboard: pd.DataFrame, dim: str) -> list[dict]:
    """For each value of `dim` (sector or country), aggregate velocity-weighted exposure."""
    lb = leaderboard[leaderboard["etf_count"] >= 2].copy()
    g = lb.groupby(dim).agg(
        net_velocity=("velocity_score", "sum"),
        avg_velocity=("velocity_score", "mean"),
        names=("ticker", "count"),
        total_weight=("total_weight", "sum"),
        burst_count=("burst_30d", "sum"),
        hc_count=("flag", lambda s: (s == "HIGH_CONVICTION").sum()),
    ).reset_index().rename(columns={dim: "label"}).sort_values("net_velocity", ascending=False)
    return g.round(2).to_dict(orient="records")

flow = {
    "by_sector":  _compute_flow(leaderboard, "sector"),
    "by_country": _compute_flow(leaderboard, "country"),
}
(output_dir / "flow.json").write_text(json.dumps(flow, separators=(",", ":")))
```

**3. Two new panels on the Changes tab in `docs/index.html`:**

```html
<div class="rounded-lg border" style="background: var(--surface); border-color: var(--border)">
  <div class="px-3 py-2 flex items-center justify-between" style="border-bottom: 1px solid var(--border)">
    <div class="label-cyan label">SECTOR FLOW — net velocity by sector</div>
    <div class="text-xs num font-mono" style="color: var(--text-3)">sum of velocity scores · 2+ ETFs only</div>
  </div>
  <template x-for="r in (flow?.by_sector || []).slice(0, 12)" :key="r.label">
    <div class="px-3 py-2 flex items-center gap-3 text-xs hover:bg-white/5 transition"
         style="border-bottom: 1px solid rgba(255,255,255,0.03)">
      <span class="font-mono flex-1 truncate" x-text="r.label"></span>
      <div class="flex-1 h-1.5 rounded" style="background: rgba(255,255,255,0.05)">
        <div class="h-full rounded"
             :style="`width: ${Math.min(100, Math.abs(r.net_velocity) / sectorMax * 100)}%; background: ${r.net_velocity >= 0 ? 'var(--up)' : 'var(--down)'}`"></div>
      </div>
      <span class="num font-mono w-20 text-right"
            :style="`color: ${r.net_velocity >= 0 ? 'var(--up)' : 'var(--down)'}`"
            x-text="`${r.net_velocity >= 0 ? '+' : ''}${r.net_velocity.toFixed(0)}`"></span>
      <span class="num font-mono w-12 text-right" style="color: var(--text-3)"
            x-text="`${r.names}n`"></span>
    </div>
  </template>
</div>
```

Mirror for `by_country`.

State:
```javascript
flow: {},
async load() {
  // ...existing...
  try { this.flow = await fetch('data/flow.json').then(r => r.json()); } catch (e) {}
},
get sectorMax() {
  return Math.max(1, ...(this.flow?.by_sector || []).map(r => Math.abs(r.net_velocity)));
}
```

**Bonus** — clicking a sector row filters the leaderboard to that sector:

```html
@click="selectedSector = r.label; activeTab = 'leaderboard'"
```

Plus a sector filter in the leaderboard's filter bar.

#### DoD — B1

- `data/ticker_metadata.csv` has at least 600 of 920 rows with non-"Unknown" sector.
- `docs/data/flow.json` exists, non-empty.
- Changes tab renders two panels with horizontal bar viz.
- Clicking a sector row jumps to leaderboard filtered to that sector.

### B2. Watchlist — convert read-only dashboard into a working tool

#### Why

Every visit today is read-only. You scan, you remember names, you switch to your broker. Add localStorage-backed pins so the dashboard remembers what you care about, and surface changes since your last visit.

#### Implementation

**1. Pin button as first cell on every leaderboard row:**

```html
<td class="px-3 py-2 text-center">
  <button @click.stop="togglePin(row.ticker)" class="text-base leading-none transition"
          :style="`color: ${isPinned(row.ticker) ? 'var(--cyan)' : 'var(--text-3)'}`"
          x-tooltip="isPinned(row.ticker) ? 'Pinned — click to unpin' : 'Pin to watchlist'">
    <span x-text="isPinned(row.ticker) ? '★' : '☆'"></span>
  </button>
</td>
```

```javascript
pins: JSON.parse(localStorage.getItem('predator_pins') || '[]'),

togglePin(t) {
  const i = this.pins.indexOf(t);
  if (i >= 0) this.pins.splice(i, 1);
  else this.pins.push(t);
  localStorage.setItem('predator_pins', JSON.stringify(this.pins));
},
isPinned(t) { return this.pins.includes(t); }
```

**2. WATCHLIST tab in nav:**

```html
<button @click="activeTab = 'watchlist'"
        :class="activeTab === 'watchlist' ? 'tab-active' : ''"
        class="px-3 py-1.5 ...">
  Watchlist <span class="num font-mono ml-1 opacity-60" x-text="`(${pins.length})`"></span>
</button>
```

When `activeTab === 'watchlist'`, the leaderboard table reuses the same component, filtered:

```javascript
if (this.activeTab === 'watchlist') r = r.filter(x => this.pins.includes(x.ticker));
```

**3. Watchlist changelog strip at the top of the watchlist tab:**

```javascript
get watchlistDelta() {
  const last = JSON.parse(localStorage.getItem('predator_pin_snapshot') || '{}');
  const out = { entered_hc: [], exited_hc: [], new_burst: [], no_longer_burst: [] };
  for (const t of this.pins) {
    const row = this.leaderboard.find(r => r.ticker === t);
    if (!row) continue;
    const before = last[t] || {};
    if (row.flag === 'HIGH_CONVICTION' && before.flag !== 'HIGH_CONVICTION') out.entered_hc.push(t);
    if (before.flag === 'HIGH_CONVICTION' && row.flag !== 'HIGH_CONVICTION') out.exited_hc.push(t);
    if (row.burst_30d && !before.burst_30d) out.new_burst.push(t);
    if (before.burst_30d && !row.burst_30d) out.no_longer_burst.push(t);
  }
  return out;
},

snapshotPins() {
  const snap = {};
  for (const t of this.pins) {
    const row = this.leaderboard.find(r => r.ticker === t);
    if (row) snap[t] = { flag: row.flag, burst_30d: row.burst_30d };
  }
  localStorage.setItem('predator_pin_snapshot', JSON.stringify(snap));
}
```

Show as a strip above the table:

```html
<div x-show="activeTab === 'watchlist' && (watchlistDelta.entered_hc.length || watchlistDelta.exited_hc.length || watchlistDelta.new_burst.length)"
     class="mb-3 px-3 py-2 rounded-lg border text-xs"
     style="background: rgba(34, 211, 238, 0.04); border-color: rgba(34, 211, 238, 0.20)">
  <span class="label-cyan label">SINCE LAST VISIT</span>
  <div class="flex flex-wrap gap-x-4 gap-y-1 mt-1">
    <span x-show="watchlistDelta.entered_hc.length" style="color: var(--up)">
      ▲ <span x-text="watchlistDelta.entered_hc.length"></span> entered HC: <span class="font-mono" x-text="watchlistDelta.entered_hc.join(', ')"></span>
    </span>
    <span x-show="watchlistDelta.exited_hc.length" style="color: var(--down)">
      ▼ <span x-text="watchlistDelta.exited_hc.length"></span> exited HC: <span class="font-mono" x-text="watchlistDelta.exited_hc.join(', ')"></span>
    </span>
    <span x-show="watchlistDelta.new_burst.length" style="color: #c084fc">
      ⚡ <span x-text="watchlistDelta.new_burst.length"></span> new BURST: <span class="font-mono" x-text="watchlistDelta.new_burst.join(', ')"></span>
    </span>
  </div>
  <button @click="snapshotPins()" class="chip border mt-2 px-2 py-0.5" style="border-color: var(--border-2)">mark all as read</button>
</div>
```

#### DoD — B2

- ☆ on any row turns into ★, persists across reloads.
- WATCHLIST tab shows only pinned tickers; count in tab label.
- Empty state: "pin a ticker to start" message.
- "Since last visit" strip appears when pinned tickers changed flag/burst status; "Mark all as read" snapshots the new state.

### B3. Concentration risk score

#### Why

A HIGH_CONVICTION score of 1400 spread across 6 ETFs is genuinely diversified institutional conviction. A score of 1400 where 90% comes from a single ETF is **single-ETF concentration risk** — if that ETF rebalances, the conviction evaporates. The dashboard treats both equally today. Fix it.

#### Implementation

In `predator/build.py`, after `compute_leaderboard`:

```python
def _compute_concentration(latest: pd.DataFrame) -> pd.DataFrame:
    """Per ticker: what fraction of the score comes from its single top ETF?
    100 = entirely one ETF; 25 = perfectly diversified across 4 ETFs."""
    grouped = latest.groupby("ticker")["score"]
    totals = grouped.sum()
    maxes  = grouped.max()
    top_share = (maxes / totals).fillna(1.0)
    return pd.DataFrame({
        "ticker": totals.index,
        "top_etf_share": top_share.values.round(3),
        "concentration_score": (top_share * 100).round(0).astype(int).values,
    })

conc = _compute_concentration(latest)
leaderboard = leaderboard.merge(conc, on="ticker", how="left")
```

In `docs/index.html`, enrich the Score cell tooltip:

```html
<td x-tooltip="`Score breakdown:<br>• Final: ${row.final_score}<br>• Concentration: ${row.concentration_score}% from top ETF<br>• ${row.concentration_score < 40 ? '✓ Well-diversified across ETFs' : row.concentration_score < 70 ? 'Moderate concentration' : '⚠ Highly concentrated — fragile if top ETF rebalances'}`">
```

Add to `sortableCols`:
```javascript
{ key: 'concentration_score', label: 'Concentration risk' },
```

Add a filter chip:
```html
<button @click="excludeConcentrated = !excludeConcentrated"
        :class="excludeConcentrated ? 'border-emerald-500/40 text-emerald-300' : ''"
        class="chip border px-2 py-1"
        style="border-color: var(--border-2)"
        x-tooltip="'Hide names where 80%+ of score comes from one ETF'">
  ≤80% conc
</button>
```

```javascript
excludeConcentrated: false,

// In filtered getter:
if (this.excludeConcentrated) r = r.filter(x => (x.concentration_score || 0) <= 80);
```

#### DoD — B3

- Score cell tooltip on GEV (held by 7 ETFs) shows "✓ Well-diversified".
- Score cell tooltip on a single-ETF name shows "⚠ Highly concentrated".
- Sort by concentration_score asc surfaces the most diversified-conviction names first.
- ≤80% filter chip hides single-ETF dominant names.

### B4. Backtest the algorithm

#### Why this is the killer feature

The whole platform rests on the assumption that the Predator Protocol scoring identifies winners. Has anyone *checked*? With 81k holding rows over 3 months plus daily score history, you can answer: **"if I bought every HIGH_CONVICTION entrant on day-0 and held 30 days, what would my returns be?"**

This turns the platform from "dashboard" to "research platform with quantified edge".

#### Implementation

New file `predator/backtest.py`:

```python
"""
Backtest the Predator Protocol scoring algorithm.

For each historical leaderboard snapshot date D:
  1. Identify buy signals: HC entrants, BURST triggers, or top-N by velocity_score
  2. Look up each name's price on D (from data/markets/yf_<TICKER>.parquet)
     — skip if missing (international names without yfinance coverage)
  3. Look up price 30 days later
  4. Compute equal-weighted return

Strategies:
  - hc_entry:      buy each name on the day it first enters HC
  - burst_trigger: buy each name on the day burst_30d flips True
  - top10_score:   buy top 10 by final_score each Monday
  - top10_velocity: buy top 10 by velocity_score each Monday
  - baseline:      equal-weighted basket of all 920 names (for comparison)

Output: docs/data/backtest.json
{
  "strategies": {
    "hc_entry": {
      "trades": [{date, ticker, entry, exit, return_pct, days_held}],
      "cumulative_returns": [{date, total_return}],
      "stats": {n_trades, win_rate, avg_return, max_drawdown, sharpe}
    },
    ...
  },
  "scatter": [{velocity_score, realized_return_30d, ticker, date}]
}
"""
```

`docs/backtest.html` — new page, single file:

- Line chart: cumulative returns by strategy (5 lines), x = date, y = cumulative %.
- Summary table: rows = strategies, columns = total trades, win rate, avg return, max drawdown, Sharpe.
- Scatter plot: x = velocity_score at signal time, y = realized 30-day return. Print R² value at corner.

Use inline SVG; no charting library.

For V1, run as a one-off script. Schedule it weekly via `.github/workflows/backtest.yml` later.

#### DoD — B4

- `python -m predator.backtest --output docs/data/backtest.json` produces JSON with at least 30 days of trades per strategy.
- `docs/backtest.html` renders the cumulative-returns chart and stats table.
- Scatter plot shows correlation between velocity_score and realized returns with R² label.
- README updated with link to `/backtest.html`.

---

## Overall Definition of Done

- Part A: existing tests stay green; 2 new tests pass (burst false-positive cases).
- BURST count drops from 395 to 15-60.
- `+0.0%` only appears for legitimate zero-change cases; otherwise em-dash.
- Part B: 4 new features deployed, each independently shippable.
- New tabs: WATCHLIST.
- New panels in Changes tab: SECTOR FLOW, COUNTRY FLOW.
- New leaderboard column/filter: concentration_score.
- New page: `backtest.html` with quantified strategy performance.

## What you must NOT do

- Do not touch the scoring formula. All upgrades layered on top.
- Do not introduce a JS bundler.
- Do not lower the new burst thresholds without re-checking the count stays under ~80.
- Do not commit `data/ticker_metadata.csv` with under 600 successfully-resolved tickers — that breaks the sector aggregations.
