# Phase 2.6 — Rank History Bug Fix + Velocity Column

Working in `yieldchaser/etf-data`. Phase 2.5 shipped (per-ETF calibration + tooltips + multi-sort + pie chart). Two follow-ups now:

**Part A** — Rank History chart on `stock.html` is broken. Root cause confirmed below. Fix it.

**Part B** — Add a new **Velocity** column to the leaderboard that surfaces the fastest-moving names by rank. This is the big new signal we've been missing — the leaderboard currently shows static conviction (Score) and direction (SC.Δ%), but not **acceleration of conviction**. Velocity fills that gap.

---

## Part A — Rank History chart bug fix

### Confirmed root cause (verified against live `holdings_history.json`)

For SNDK, the live data has these per-ETF series lengths:

| ETF  | Snapshots | First date  | Last date   |
|------|-----------|-------------|-------------|
| CSD  | 44        | 2026-03-16  | 2026-05-15  |
| FPX  | 44        | 2026-03-16  | 2026-05-15  |
| QQQM | 13        | 2026-04-21  | 2026-05-13  |
| RPG  | 38        | 2026-03-16  | 2026-05-13  |
| SPMO | 33        | 2026-03-23  | 2026-05-13  |

The current `rankLines` getter (`docs/stock.html` line ~593) computes `x = (i / (n-1)) × 1000` **per ETF independently**. So QQQM's 13 points spread across the full 0→1000 width, putting its first observation (2026-04-21) at the same x as CSD's first observation (2026-03-16). Lines are time-misaligned and overlap nonsensically — which is why the chart area looks empty (lines render off-canvas or stacked under each other).

The crosshair-vertical also uses `Math.max(...rankLines.map(l => l.series.length))` as the x-axis denominator, but the tooltip indexes into `line.series[rci]` for each ETF — different ETFs at the same `rci` show different *calendar* dates, which is misleading.

### Required fix

Build a **shared time axis** from the union of all dates across all ETFs, then map each ETF's observations onto that shared axis. Where an ETF has no observation on a shared-axis date, the line simply doesn't have a vertex there — but the x-coordinate of every plotted vertex is consistent across ETFs.

Replace the entire `rankLines` getter with this:

```javascript
get rankLines() {
  const hh = this.holdingsHistory;
  if (!hh || !Object.keys(hh).length) return [];

  // 1. Build the shared time axis — union of all dates, sorted ascending
  const dateSet = new Set();
  for (const series of Object.values(hh)) {
    for (const p of series) dateSet.add(p.d);
  }
  const allDates = Array.from(dateSet).sort();
  if (allDates.length < 2) return [];
  const dateToX = {};
  allDates.forEach((d, i) => {
    dateToX[d] = (i / (allDates.length - 1)) * 1000;
  });

  // 2. Compute Y scale from the global rank range
  const allRanks = Object.values(hh).flat().map(p => p.r).filter(Boolean);
  const minRank = Math.min(...allRanks);
  const maxRank = Math.max(...allRanks);
  const rangeFloor = Math.max(maxRank, minRank + 4);  // avoid pancake-flat chart for HC top names
  const yRange = Math.max(1, rangeFloor - minRank);
  const yTop = 10, yBot = 210;
  const rankToY = r => yTop + ((r - minRank) / yRange) * (yBot - yTop);

  // 3. Build one polyline per ETF, vertices placed on the shared x-axis
  const lines = [];
  for (const etf of Object.keys(hh).sort()) {
    const rows = hh[etf];
    if (!rows || !rows.length) continue;
    const tier = this.tierMap[etf] || 'Blob';
    const color = this.tierLineColor(tier);
    // Map each observation to {x, y, r, d} — preserves date alignment across ETFs
    const series = rows
      .filter(p => dateToX[p.d] !== undefined)
      .map(p => ({ r: p.r, d: p.d, x: dateToX[p.d], y: rankToY(p.r) }));
    if (!series.length) continue;
    const path = series
      .map((p, i) => `${i === 0 ? 'M' : 'L'}${p.x.toFixed(1)},${p.y.toFixed(1)}`)
      .join(' ');
    lines.push({
      etf, path, color, tier, series,
      endX: series[series.length - 1].x,
      endY: series[series.length - 1].y,
    });
  }
  // Expose the shared date axis on the first line so the template can read it
  if (lines.length) lines[0].sharedDates = allDates;
  return lines;
}
```

Also add a getter for the shared axis so other parts of the template (crosshair, tooltip) can use it without duplicating the union logic:

```javascript
get rankSharedDates() {
  return this.rankLines[0]?.sharedDates || [];
}
```

### Update the crosshair logic and tooltip

The crosshair currently uses each line's own length. Switch to the shared axis. Replace the `@mousemove` handler on the chart wrapper (around line 287):

```html
@mousemove="
  const svg = $el.querySelector('svg');
  if (!svg || !rankSharedDates.length) return;
  const rect = svg.getBoundingClientRect();
  const rx = $event.clientX - rect.left;
  const n = rankSharedDates.length;
  if (n < 2) return;
  rci = Math.max(0, Math.min(n - 1, Math.round((rx / rect.width) * (n - 1))));
  rcx = (rci / (n - 1)) * rect.width;
"
```

The crosshair vertical line and the dot-per-ETF needs to switch from `line.series[rci]` to looking up by date. Replace the `<template x-if>` for crosshair dots (around line 318):

```html
<template x-if="rci !== null">
  <g>
    <template x-for="line in rankLines" :key="line.etf">
      <template x-for="pt in [line.series.find(p => p.d === rankSharedDates[rci])]" :key="pt?.d">
        <circle x-show="pt"
                :cx="(rci / Math.max(1, rankSharedDates.length - 1)) * 1000"
                :cy="pt?.y" r="3" :fill="line.color" stroke="#000" stroke-width="1.5"/>
      </template>
    </template>
  </g>
</template>
```

Replace the crosshair vertical line block (around line 326):

```html
<template x-if="rci !== null">
  <line class="crosshair-line"
        :x1="(rci / Math.max(1, rankSharedDates.length - 1)) * 1000"
        y1="0"
        :x2="(rci / Math.max(1, rankSharedDates.length - 1)) * 1000"
        y2="220"/>
</template>
```

Replace the hover tooltip body (around line 334) — show the shared date once, then one row per ETF with the rank at that date (or "—" if the ETF didn't have data on that day):

```html
<div x-show="rci !== null" class="chart-tooltip" :style="`top: 4px; left: ${Math.min((rcx || 0) + 8, 300)}px`">
  <div class="font-mono text-[10px] mb-1" style="color: var(--text-3)"
       x-text="rankSharedDates[rci] || ''"></div>
  <template x-for="line in rankLines" :key="line.etf">
    <div class="flex items-center gap-2 text-[10px]">
      <span class="font-mono" :style="`color: ${line.color}`" x-text="line.etf"></span>
      <span class="num font-mono" style="color: var(--text-2)"
            x-text="(() => { const p = line.series.find(p => p.d === rankSharedDates[rci]); return p ? '#' + p.r : '—'; })()"></span>
    </div>
  </template>
</div>
```

### Add x-axis date labels (so the chart isn't just floating lines)

Currently there are no date markers below the rank-history SVG. Add three: first date, middle date, last date. Insert just before the closing `</svg>`:

```html
<!-- X-axis date labels -->
<template x-if="rankSharedDates.length >= 2">
  <g style="font-family: ui-monospace; font-size: 9px" fill="var(--text-3)">
    <text x="0"   y="232" text-anchor="start"  x-text="rankSharedDates[0]"></text>
    <text x="500" y="232" text-anchor="middle" x-text="rankSharedDates[Math.floor(rankSharedDates.length / 2)]"></text>
    <text x="1000" y="232" text-anchor="end"   x-text="rankSharedDates[rankSharedDates.length - 1]"></text>
  </g>
</template>
```

### Definition of Done — Part A

1. Navigate to `https://yieldchaser.github.io/etf-data/stock.html?t=SNDK`. The Rank History chart shows **5 distinct polylines** (CSD purple, FPX purple, QQQM grey, RPG yellow, SPMO yellow), each starting at its own first observation date — i.e., QQQM's line begins around the middle of the chart (its first snapshot is 2026-04-21), not at x=0.
2. Hovering anywhere on the chart shows a tooltip with the **calendar date** at the cursor's x-position, and below it one line per ETF showing the rank at that date or "—" if the ETF didn't have a snapshot on that day.
3. Three x-axis date labels render below the chart: first, midpoint, and last date.
4. Test on other tickers with mismatched ETF date ranges (e.g., `GEV` in 7 ETFs) — every line renders correctly aligned.

---

## Part B — Velocity column (fastest movers)

### Why this matters

Today's leaderboard shows **conviction** (Score) and **direction** (SC.Δ%). What it doesn't show is **how fast that conviction is concentrating or dispersing across ETFs** — which is the earliest tradeable signal in the entire pipeline. A stock whose **average rank across all holding ETFs is improving rapidly week-over-week** is being actively accumulated. That's exactly the institutional-rotation signal the Predator Protocol was designed to catch.

**Concrete motivating case — STX (Seagate)**. From the global rank history chart on `stock.html?t=STX`:

- 2026-04-05: global rank **#76 out of 177 tracked**
- 2026-04-17: global rank **#21 out of 179 tracked** — a **+55 rank improvement in 12 days**
- 2026-05-15: holding around **#18**

That's a textbook burst — institutional capital concentrating into a name fast. A naive 7-day rank delta misses this completely because the surge already happened. The formula below catches both **steady accumulation** (slow climb week-over-week) AND **burst moves** (the entire jump that's mostly already in the rear-view).

A few signals we derive from data the build already produces:

1. **Rank velocity (`avg_rank_delta_7d`, `avg_rank_delta_30d`)**: average of per-ETF `rank_delta` across every ETF holding the stock. Positive = rank improving (good).
2. **Global rank acceleration (`global_rank_delta_30d`, `global_rank_best_30d`)**: how much the stock's **leaderboard rank** (across all 920+ names, not per-ETF) has improved, AND the best (peak) rank achieved in the last 30 days. This is what catches STX-style bursts that already peaked.
3. **Cross-ETF concentration (`etf_count_delta_30d`)**: how many ETFs added/dropped the name in the last 30 days. A stock going from 1 ETF to 4 ETFs in 30 days **is** HIGH_CONVICTION forming.
4. **Weight flow (`avg_weight_flow_7d`)**: average position-size growth across ETFs.
5. **Burst flag (`burst_30d`)**: boolean — global rank improved by ≥ 40 ranks at any point in the last 30 days, even if it's since stabilized. This is the discriminator that separates "moving fast right now" from "moved fast recently and held".

### Compute the velocity signal in `predator/build.py`

After the per-period deltas are computed (around line 220, where `deltas_by_period` is iterated), add a new section:

```python
# ── VELOCITY signal — captures both steady accumulation AND burst moves ─────────
def _attach_velocity(leaderboard: pd.DataFrame,
                    deltas_by_period: dict,
                    historical: dict) -> pd.DataFrame:
    """Add velocity columns. Catches the STX-style +55-ranks-in-12-days burst
    that a naive 7d-only delta would miss."""

    # ── 1. Per-ETF rank/weight motion ──────────────────────────────────
    d7  = deltas_by_period.get(7)
    d30 = deltas_by_period.get(30)
    rank_avg_7  = d7.groupby("ticker")["rank_delta"].mean()  if d7  is not None and not d7.empty  else pd.Series(dtype=float)
    flow_avg_7  = d7.groupby("ticker")["weight_flow"].mean() if d7  is not None and not d7.empty  else pd.Series(dtype=float)
    rank_avg_30 = d30.groupby("ticker")["rank_delta"].mean() if d30 is not None and not d30.empty else pd.Series(dtype=float)

    # ── 2. Global leaderboard rank trajectory ──────────────────────────
    # For every snapshot date, every ticker has a leaderboard_rank (their global rank
    # out of all tickers). We track that trajectory over the last 30 days to catch
    # both the current delta AND the peak improvement achieved within the window.
    dates_sorted = sorted(historical.keys())
    today_date = dates_sorted[-1]
    today_lb = historical[today_date].set_index("ticker")
    today_rank = today_lb["leaderboard_rank"]

    # Build a panel: rows = tickers, cols = dates, values = global rank
    rank_panel_rows = {}
    for d in dates_sorted:
        rank_panel_rows[d] = historical[d].set_index("ticker")["leaderboard_rank"]
    rank_panel = pd.DataFrame(rank_panel_rows)  # NaN where ticker not on leaderboard that day

    # 30-day window for global rank signals
    window_start = today_date - pd.Timedelta(days=30)
    window_cols = [c for c in rank_panel.columns if c >= window_start]
    if len(window_cols) >= 2:
        win = rank_panel[window_cols]
        first_col = win.iloc[:, 0]
        # worst (highest number = lowest position) within the window — i.e., starting position
        worst_in_window = win.max(axis=1)
        # best (lowest number) within the window — i.e., peak achieved
        best_in_window  = win.min(axis=1)
        # current
        current = win.iloc[:, -1]
        # 30-day delta: positive = improved (rank number got smaller)
        global_rank_delta_30 = (first_col - current).round(0)
        # Peak improvement at any point in window vs the starting position
        peak_improvement_30 = (worst_in_window - best_in_window).round(0)
    else:
        global_rank_delta_30 = pd.Series(dtype=float)
        peak_improvement_30 = pd.Series(dtype=float)
        best_in_window = pd.Series(dtype=float)

    # ── 3. ETF count change ────────────────────────────────────────────
    if len(dates_sorted) >= 2:
        target = today_date - pd.Timedelta(days=30)
        past_date = min(dates_sorted, key=lambda d: abs((d - target).total_seconds()))
        past_counts = historical[past_date].set_index("ticker")["etf_count"]
    else:
        past_counts = pd.Series(dtype=float)

    # ── 4. Attach all raw signals to the leaderboard ───────────────────
    leaderboard["avg_rank_delta_7d"]      = leaderboard["ticker"].map(rank_avg_7).fillna(0).round(2)
    leaderboard["avg_weight_flow_7d"]     = leaderboard["ticker"].map(flow_avg_7).fillna(0).round(4)
    leaderboard["avg_rank_delta_30d"]     = leaderboard["ticker"].map(rank_avg_30).fillna(0).round(2)
    leaderboard["global_rank_delta_30d"]  = leaderboard["ticker"].map(global_rank_delta_30).fillna(0).astype(int)
    leaderboard["global_rank_peak_30d"]   = leaderboard["ticker"].map(peak_improvement_30).fillna(0).astype(int)
    leaderboard["global_rank_best_30d"]   = leaderboard["ticker"].map(best_in_window).fillna(leaderboard["leaderboard_rank"]).astype(int)
    leaderboard["etf_count_30d_ago"]      = leaderboard["ticker"].map(past_counts).fillna(leaderboard["etf_count"]).astype(int)
    leaderboard["etf_count_delta_30d"]    = (leaderboard["etf_count"] - leaderboard["etf_count_30d_ago"]).astype(int)
    # Burst flag: peak improvement of ≥ 40 global ranks at any point in last 30d
    leaderboard["burst_30d"]              = leaderboard["global_rank_peak_30d"] >= 40

    # ── 5. Composite velocity score ────────────────────────────────────
    # Tuning rationale (each component should contribute ~comparable magnitude):
    #   Global rank Δ30d of +50 ranks  → +25
    #   Peak improvement 30d of +50    → +12.5 (rewards bursts that haven't fully reverted)
    #   Per-ETF avg rank Δ 7d of +5    → +5
    #   Per-ETF weight flow 7d +20%    → +4
    #   ETFs added 30d: +1             → +5
    #   Score streak: +2 days          → +2
    leaderboard["velocity_score"] = (
        leaderboard["global_rank_delta_30d"].fillna(0).clip(-200, 200) * 0.5 +
        leaderboard["global_rank_peak_30d"].fillna(0).clip(0, 200) * 0.25 +
        leaderboard["avg_rank_delta_7d"].fillna(0) * 1.0 +
        leaderboard["avg_weight_flow_7d"].fillna(0) * 20.0 +
        leaderboard["etf_count_delta_30d"].fillna(0) * 5.0 +
        leaderboard["score_streak"].fillna(0).clip(-10, 10) * 1.0
    ).round(2)

    return leaderboard

leaderboard = _attach_velocity(leaderboard, deltas_by_period, historical)
```

This adds 9 new columns to `leaderboard.json` per row. Total payload delta: ~70KB across 920 names. Acceptable.

### STX worked example with the new formula

Plugging STX's actual data through the formula:

```
global_rank_delta_30d  = 76 → 18  = +58 ranks  → × 0.5 = +29.0
global_rank_peak_30d   = 76 → 18  = +58 peak   → × 0.25 = +14.5
avg_rank_delta_7d      ≈ +2 ranks (modest, post-burst)  → +2.0
avg_weight_flow_7d     ≈ +5%                            → +1.0
etf_count_delta_30d    = +1 (maybe)                     → +5.0
score_streak           = +3 (steady up after surge)     → +3.0
                                              total   = ≈ +54.5
```

STX would surface near the **top** of a velocity-sorted leaderboard — exactly the behavior you want. A steady accumulator at +5 ranks/week with no burst might score ~15-20; STX's burst gets it to 50+.

### Surface velocity in `docs/index.html`

Add a new sortable column to the leaderboard table, right after `SC.Δ%`. The cell displays the composite velocity_score as a colored chip, plus a small `▲N rank / +N ETFs` micro-breakdown on hover.

In the `<thead>` row, insert after the SC.Δ% header:

```html
<th @click="sort('velocity_score')" :class="sortClass('velocity_score')"
    class="text-right px-3 py-2 cursor-pointer hover:text-zinc-300 hidden lg:table-cell"
    x-tooltip="'Velocity — composite rate-of-change signal across all ETFs holding this name. <br>Sums: avg rank improvement (7d) + avg weight flow (7d) + new ETF additions (30d) + score streak. <br>High positive = institutional accumulation accelerating. Negative = distribution.'">
  Velocity
</th>
```

In the `<tbody>` row template, insert a new `<td>` after the SC.Δ% cell:

```html
<td class="px-3 py-2 text-right num font-mono hidden lg:table-cell"
    :style="`color: ${velocityColor(row.velocity_score)}`"
    x-tooltip="velocityTip(row)"
    x-text="fmtVelocity(row.velocity_score)"></td>
```

Component methods (add to the Alpine `app()` object):

```javascript
fmtVelocity(v) {
  if (v == null || !isFinite(v)) return '—';
  const n = Number(v);
  if (Math.abs(n) < 0.5) return '0';
  return (n > 0 ? '+' : '') + n.toFixed(1);
},

velocityColor(v) {
  if (v == null || !isFinite(v)) return 'var(--text-3)';
  const n = Number(v);
  if (n >= 15) return 'var(--up)';        // strong positive
  if (n >= 5)  return '#86efac';          // mild positive
  if (n <= -15) return 'var(--down)';     // strong negative
  if (n <= -5)  return '#fda4af';         // mild negative
  return 'var(--text-2)';
},

velocityTip(row) {
  const rd7  = row.avg_rank_delta_7d ?? 0;
  const wf7  = (row.avg_weight_flow_7d ?? 0) * 100;
  const ed30 = row.etf_count_delta_30d ?? 0;
  const sst  = row.score_streak ?? 0;
  const grd30 = row.global_rank_delta_30d ?? 0;
  const grp30 = row.global_rank_peak_30d ?? 0;
  return `<div class="font-mono text-[10px]">
    <div style="color: var(--cyan)">VELOCITY ${this.fmtVelocity(row.velocity_score)}</div>
    <div>global rank Δ (30d): ${grd30 >= 0 ? '+' : ''}${grd30} positions</div>
    <div>peak improvement (30d): +${grp30} positions</div>
    <div>avg per-ETF rank Δ (7d): ${rd7 >= 0 ? '+' : ''}${rd7.toFixed(1)}</div>
    <div>avg weight flow (7d): ${wf7 >= 0 ? '+' : ''}${wf7.toFixed(1)}%</div>
    <div>ETFs added (30d): ${ed30 >= 0 ? '+' : ''}${ed30}</div>
    <div>score streak: ${sst >= 0 ? '+' : ''}${sst} days</div>
    ${row.burst_30d ? '<div style="color: #c084fc; margin-top: 4px">⚡ BURST detected</div>' : ''}
  </div>`;
}
```

### Add a "VELO" and "BURST" flag chip

Two new chips, both surfacing different sub-cases of acceleration:

**`VELO`** — current top-quintile composite velocity. Steady accumulators land here.
**`BURST`** — at any point in the last 30 days, the stock improved its global rank by 40+ positions. This is the STX-style move that's "mostly already played out" but still tradeable.

In the flag cell:

```html
<span x-show="row.burst_30d" class="chip"
      x-tooltip="`BURST move — global rank improved by ${row.global_rank_peak_30d}+ positions at some point in last 30 days. Currently ranked #${row.leaderboard_rank} (best in window: #${row.global_rank_best_30d}).`"
      style="background: rgba(168, 85, 247, 0.12); color: #c084fc; border: 1px solid rgba(168, 85, 247, 0.30)">
  BURST
</span>
<span x-show="row.velocity_score >= 25 && row.etf_count >= 2 && !row.burst_30d" class="chip"
      x-tooltip="`Top-velocity composite (${row.velocity_score}). Conviction is concentrating — ETFs are adding the name and/or rank is climbing.`"
      style="background: rgba(34, 211, 238, 0.10); color: var(--cyan); border: 1px solid rgba(34, 211, 238, 0.25)">
  VELO
</span>
```

The `!row.burst_30d` on the VELO chip prevents stacking — a single name gets the more specific BURST label when it qualifies for both. Threshold of 25 (raised from 15 in the earlier draft because the new formula produces larger numbers thanks to the global rank delta term).

Also add filter chips in the filter bar:

```html
<button @click="velocityOnly = !velocityOnly"
        :class="velocityOnly ? 'border-cyan-500/40 text-cyan-300' : ''"
        class="chip border px-2 py-1 transition hover:border-cyan-500/30"
        style="border-color: var(--border-2)"
        x-tooltip="'Show only top-velocity names (velocity ≥ 25, held by 2+ ETFs)'"
        x-text="`VELO ${counts.VELO || 0}`"></button>
<button @click="burstOnly = !burstOnly"
        :class="burstOnly ? 'border-purple-500/40 text-purple-300' : ''"
        class="chip border px-2 py-1 transition hover:border-purple-500/30"
        style="border-color: var(--border-2)"
        x-tooltip="'Show only burst movers — stocks that improved global rank by 40+ in the last 30 days'"
        x-text="`BURST ${counts.BURST || 0}`"></button>
```

State + counts update:

```javascript
velocityOnly: false,
burstOnly: false,

get counts() {
  return {
    HIGH_CONVICTION: this.leaderboard.filter(r => r.flag === 'HIGH_CONVICTION').length,
    SPECULATIVE_BETA: this.leaderboard.filter(r => r.flag === 'SPECULATIVE_BETA').length,
    NEW: this.leaderboard.filter(r => r.any_new).length,
    VELO: this.leaderboard.filter(r => (r.velocity_score || 0) >= 25 && (r.etf_count || 0) >= 2 && !r.burst_30d).length,
    BURST: this.leaderboard.filter(r => r.burst_30d).length,
  };
}

// in filtered getter:
if (this.velocityOnly) {
  r = r.filter(x => (x.velocity_score || 0) >= 25 && (x.etf_count || 0) >= 2 && !x.burst_30d);
}
if (this.burstOnly) {
  r = r.filter(x => x.burst_30d);
}
```

Add `velocity_score`, `global_rank_delta_30d`, and `global_rank_peak_30d` to `sortableCols` for the multi-level sort dialog (from Phase 2.5):

```javascript
{ key: 'velocity_score', label: 'Velocity (composite)' },
{ key: 'global_rank_delta_30d', label: 'Global rank Δ (30d)' },
{ key: 'global_rank_peak_30d', label: 'Peak rank improvement (30d)' },
{ key: 'avg_rank_delta_7d', label: 'Avg per-ETF rank Δ (7d)' },
{ key: 'etf_count_delta_30d', label: 'ETFs added (30d)' },
```

### Surface velocity on the per-stock detail page

In `docs/stock.html`, add two KPI cards to the hero strip (between RANK and SCORE Δ%):

```html
<div class="rounded-lg border px-3 py-2.5" style="background: var(--surface); border-color: var(--border)">
  <div class="label" x-tooltip="'Composite velocity score across all ETFs holding this name'">VELOCITY</div>
  <div class="num font-mono text-2xl mt-0.5" :style="`color: ${velocityColor(leaderboardRow?.velocity_score)}`"
       x-text="fmtVelocity(leaderboardRow?.velocity_score)"></div>
</div>
<div class="rounded-lg border px-3 py-2.5" style="background: var(--surface); border-color: var(--border)">
  <div class="label" x-tooltip="'Net change in number of ETFs holding this name over the last 30 days'">ETFs ADDED (30d)</div>
  <div class="num font-mono text-2xl mt-0.5"
       :style="leaderboardRow?.etf_count_delta_30d > 0 ? 'color: var(--up)' : (leaderboardRow?.etf_count_delta_30d < 0 ? 'color: var(--down)' : '')"
       x-text="leaderboardRow?.etf_count_delta_30d > 0 ? `+${leaderboardRow.etf_count_delta_30d}` : (leaderboardRow?.etf_count_delta_30d ?? 0)"></div>
</div>
```

Copy the `fmtVelocity` and `velocityColor` helpers into the `stock.html` Alpine app.

### Add a Changes tab section for top movers

In `docs/index.html`'s Changes tab (4-panel grid), add a 5th panel: "TOP VELOCITY MOVERS — last 7 days". Show the 15 highest `velocity_score` names, sorted desc, with a sparkline of rank-delta-7d.

In `predator/build.py`, append to the changelog dict produced by `history.py`:

```python
# After chg = hist.changelog(...)
top_velocity = leaderboard[leaderboard["etf_count"] >= 2].sort_values("velocity_score", ascending=False).head(15)
chg["top_velocity"] = [
    {
        "ticker": r["ticker"],
        "company": r["company"],
        "velocity_score": float(r["velocity_score"]),
        "avg_rank_delta_7d": float(r["avg_rank_delta_7d"]),
        "etf_count_delta_30d": int(r["etf_count_delta_30d"]),
        "final_score": int(r["final_score"]),
        "etf_count": int(r["etf_count"]),
        "tiers": r["tiers"],
    }
    for _, r in top_velocity.iterrows()
]
```

Render the panel in `index.html`'s Changes tab. Mirror the existing "BIGGEST SCORE GAINERS" panel structure.

### Definition of Done — Part B

1. `python -m predator.build` adds the 9 new columns (`avg_rank_delta_7d`, `avg_weight_flow_7d`, `avg_rank_delta_30d`, `global_rank_delta_30d`, `global_rank_peak_30d`, `global_rank_best_30d`, `etf_count_30d_ago`, `etf_count_delta_30d`, `velocity_score`, `burst_30d`) to every row in `leaderboard.json`.
2. **STX validation**: in the rebuilt `leaderboard.json`, STX shows `global_rank_delta_30d ≥ 50`, `global_rank_peak_30d ≥ 55`, `burst_30d: true`, and `velocity_score ≥ 40`. Sorting the leaderboard by `velocity_score` desc should put STX in the top 20.
3. The leaderboard table has a new sortable **Velocity** column with color-coded values (`+` green = improving, `−` red = deteriorating). Hovering a cell shows the full breakdown tooltip including global rank delta, peak, ETFs added, and a "⚡ BURST detected" line when applicable.
4. The `BURST` chip appears on STX and any other row with `burst_30d: true`. The `VELO` chip appears on rows with `velocity_score >= 25` AND `etf_count >= 2` AND `!burst_30d` (no stacking). Both filter chips work in the filter bar.
5. `velocity_score`, `global_rank_delta_30d`, and `global_rank_peak_30d` are all selectable as sort keys in the multi-level sort dialog from Phase 2.5.
6. The per-stock page (`stock.html?t=STX`) shows the two new hero KPIs: VELOCITY and ETFs ADDED (30d), plus the BURST chip in the flag strip.
7. The Changes tab shows a 5th panel: TOP VELOCITY MOVERS with the top 15 names by velocity_score.
8. Two new tests in `tests/test_scoring.py`:
   ```python
   def test_velocity_score_aggregates_rank_deltas(self, cfg):
       """A ticker improving by 5 ranks in each of 2 ETFs should have positive velocity."""
       # Build synthetic 14-day history where ticker X drops 5 ranks (improves)
       # in both QMOM and SPMO. Assert velocity_score > 0.
       ...

   def test_burst_flag_triggers_on_global_rank_jump(self, cfg):
       """A ticker that improved global rank by 40+ in last 30d gets burst_30d=True."""
       # Synthetic 30-day history. Ticker X starts at global rank #80, climbs to #20.
       # Assert burst_30d is True and global_rank_peak_30d >= 50.
       ...
   ```

---

## Overall Definition of Done

- All existing tests still pass (`pytest tests/ -v`).
- Two new tests pass (rank-history shared-axis path, velocity aggregate).
- `https://yieldchaser.github.io/etf-data/stock.html?t=SNDK` shows a working Rank History chart with 5 polylines on a shared time axis.
- Leaderboard has a new Velocity column with sort, filter, and VELO chip.
- Hover tooltips on the new column show: avg rank Δ (7d), avg weight flow (7d), ETFs added (30d), score streak.
- Top of leaderboard sorted by Velocity desc surfaces names where institutional conviction is **concentrating fastest** — these are the highest-quality forward-looking signals the platform can produce.

## What you must NOT do

- Do not change the scoring formula (`weight × points × rank_mult × 100 + new_bonus`). Velocity is layered *on top* of scoring; it doesn't alter what Score means.
- Do not introduce a build step. CDN Tailwind + Alpine.js only.
- Do not regress Part A's rank-history chart on tickers where all ETFs have the same date range (e.g., GEV in 7 ETFs that all span the full window — chart should still look correct).
- Do not lower the BURST threshold below 40 peak-rank improvement. Bursts of <40 ranks are common noise; 40+ ranks across the global universe is a 4-5σ event.
- Do not lower the VELO threshold below 25 (the new formula produces larger numbers than the earlier draft thanks to the global rank delta term).
