# Design Document

## Overview

This design fixes the Yield History "Annual Averages" rendering bug, polishes the Hold Lab tab (responsive KPI grid, axis chrome on the rolling chart, axis chrome and hover readout on the distribution chart), and applies a consistency pass to the Drawdown and Correlation Growth charts so all four charts share the same institutional axis treatment.

All work is confined to `docs/markets.html`. The page is a single-file Alpine.js 3.x SPA. Charts are inline SVG strings returned by Alpine getters. No new runtime dependencies.

The Yield bug is a single root cause: `rates[*].values` in `data/market_returns.json` is emitted by `predator/markets_history.py` as a sorted array of `[YYYY-MM, value]` tuples, but two getters — `yieldYears` (line ~3468) and `getYieldValue` (line ~3492) — read it as a `{YYYY-MM: value}` map. `Object.keys()` over a tuple array returns the string indices `"0", "1", ...`, which is why every year header is `"0"`, `"1"`, ... `"25"` and every cell is blank. The fix is to treat `values` as `Array<[ym, value]>` everywhere.

The Hold Lab grid bug is a single Tailwind class change: `grid-cols-2 sm:grid-cols-5` becomes a responsive ladder ending at `xl:grid-cols-7`.

The chart polish work follows a single shared pattern already used by the existing yield and drawdown charts: explicit axis lines at the plot edges, monospace tick labels at `--text-3`, faint horizontal gridlines, an emphasised zero gridline, and a hover crosshair driven by a getter that mirrors the existing `yieldChartHover` / `drawdownHover` / `growthHover` shape.

## Architecture

### Files Modified

- `docs/markets.html` — single file, single SPA.

### Touched Symbols

| Symbol | Approx. line | Change |
|---|---|---|
| `yieldYears` getter | 3468–3477 | Iterate `values` as tuple array; pull year from `tuple[0].slice(0,4)`. |
| `getYieldValue` method | 3492–3504 | Scan tuple array for `${year}-12`; fall back to last in-year tuple. |
| `yieldChartMeta` getter | 3479–3490 | Iterate `values` as tuple array (uses same map-style access today). |
| `yieldChartSvg` getter | 3506–3608 | Replace `Object.keys(r.values || {})` and `vals[m]` with tuple iteration. |
| `yieldChartHover` method | 3611–3651 | Same tuple-array fix for month collection and value lookup. |
| Hold Lab KPI strip template | 1257 | Class change to responsive ladder. |
| `hpChartSvg` getter | ~4150 | Add y-axis line, x-axis line, formatted tick labels, gridlines, emphasised zero, crosshair element. |
| `hpDistSvg` getter | ~4185 | Add y-axis line, x-axis line, count tick labels, gridlines. |
| Hold Lab template (rolling chart) | ~1267 | Wire `@mousemove="hpChartHover($event)"`, `@mouseleave="hpHoverInfo = null"`, hover bar markup. |
| Hold Lab template (distribution chart) | ~1277 | Wire bar-level `@mouseenter="hpDistHover(i)"`, `@mouseleave="hpDistHoverInfo = null"`, hover bar markup. |
| `hpHoverInfo`, `hpDistHoverInfo` state | new | Two new Alpine reactive fields, both default `null`. |
| `hpChartHover`, `hpDistHover` methods | new | New methods mirroring existing hover handler shape. |
| `drawdownChartSvg` getter | 3849–3935 | Add explicit y-axis line at `x=PL` and x-axis line at `y=H-PB`. Tick labels and gridlines already present — keep. |
| `growthChartSvg` getter | 4270–4308 | Add explicit y-axis line at `x=PL` and x-axis line at `y=H-PB`. |

No symbols are removed. No method signatures change. Existing hover state bindings (`yieldHoverInfo`, `ddHoverInfo`, `growthHoverInfo`) are untouched.

## Data Models

### Rate series shape (read-only, comes from pipeline)

```jsonc
// data/market_returns.json → rates[<key>]
{
  "meta": { "name": "...", "category": "rates", "native_ccy": "USD", "first": "1934-01", "last": "2025-10", ... },
  "values": [
    ["1934-01", 0.81],
    ["1934-02", 0.79],
    // ... sorted ascending by YYYY-MM
    ["2025-10", 4.21]
  ]
}
```

The page must treat `values` as `Array<[string, number]>`. The pipeline (`predator/markets_history.py`) emits this shape and the legacy migration path also coerces any old `{YYYY-MM: value}` dicts to the array form on save (see `markets_history.py` ~line 854). The page never sees the dict form.

### Hold Lab hover state (new)

```js
// Alpine state additions
hpHoverInfo: null,       // { date: 'YYYY-MM', val: number } | null
hpDistHoverInfo: null,   // { lo: number, hi: number, count: number } | null
```

Both shapes mirror the existing `yieldHoverInfo` / `ddHoverInfo` / `growthHoverInfo` pattern: a flat object the template reads, set in the hover handler, cleared on `mouseleave`.

## Components and Interfaces

### 1. Yield Annual Averages — tuple-array fix

**Current (broken)**:

```js
get yieldYears() {
  const rates = this.returnsData?.rates || {};
  const yearSet = new Set();
  for (const s of Object.values(rates)) {
    for (const k of Object.keys(s.values || {})) {   // BUG: Object.keys on array → "0","1","2"...
      yearSet.add(parseInt(k.slice(0, 4)));
    }
  }
  return Array.from(yearSet).sort((a, b) => a - b);
},

getYieldValue(seriesKey, year) {
  const s = (this.returnsData?.rates || {})[seriesKey];
  if (!s) return null;
  const vals = s.values || {};
  const decKey = `${year}-12`;
  if (vals[decKey] != null) return vals[decKey];   // BUG: vals[ym] on array → undefined
  const yearKeys = Object.keys(vals).filter(k => k.startsWith(`${year}-`)).sort();
  if (yearKeys.length) return vals[yearKeys[yearKeys.length - 1]];
  return null;
},
```

**Fixed**:

```js
get yieldYears() {
  const rates = this.returnsData?.rates || {};
  const yearSet = new Set();
  for (const s of Object.values(rates)) {
    const vals = s.values || [];
    for (const [ym] of vals) {                     // tuple destructure
      yearSet.add(parseInt(ym.slice(0, 4)));
    }
  }
  return Array.from(yearSet).sort((a, b) => a - b);
},

getYieldValue(seriesKey, year) {
  const s = (this.returnsData?.rates || {})[seriesKey];
  if (!s) return null;
  const vals = s.values || [];
  const prefix = `${year}-`;
  let dec = null;
  let lastInYear = null;
  for (const [ym, v] of vals) {
    if (!ym.startsWith(prefix)) continue;
    if (ym === `${year}-12`) dec = v;
    if (lastInYear === null || ym > lastInYear[0]) lastInYear = [ym, v];
  }
  if (dec != null) return dec;
  return lastInYear ? lastInYear[1] : null;
},
```

The same `Object.keys(r.values || {})` / `vals[m]` pattern in `yieldChartMeta`, `yieldChartSvg`, and `yieldChartHover` must also switch to tuple iteration. Implementation pattern:

```js
// Build a Map once per render; O(n) preprocessing, O(1) lookup
const valMap = new Map(s.values || []);
// Then use valMap.get(ym) instead of vals[ym], and Array.from(valMap.keys()) instead of Object.keys(vals).
```

This keeps the chart-render and hover-handler hot paths O(1) per month.

### 2. Hold Lab KPI strip — responsive ladder

**Current**:

```html
<div class="grid grid-cols-2 sm:grid-cols-5 gap-3 mb-5">
```

**Fixed**:

```html
<div class="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-6 xl:grid-cols-7 gap-3 mb-5">
```

Breakpoint mapping (Tailwind defaults):
- `< 640px` → `grid-cols-2` (2 columns)
- `640–1023px` → `sm:grid-cols-4` (4 columns)
- `1024–1279px` → `lg:grid-cols-6` (6 columns)
- `≥ 1280px` → `xl:grid-cols-7` (7 columns; the optional 8th "Never Lost" tile drops to a second row, then fills the next cell on the same row when it would fit — Grid auto-flow handles this naturally without manual `col-span` overrides).

No tile styling changes. The inner `<div class="rounded-lg border px-3 py-2.5" ...>` is preserved verbatim.

### 3. HoldLab_Rolling_Chart — `hpChartSvg` polish

The current implementation already has gridlines and y-tick labels; it lacks an explicit axis line, x-axis year labels, an emphasised zero gridline, and a crosshair. The polished output mirrors `yieldChartSvg` and `drawdownChartSvg`.

```js
get hpChartSvg() {
  const rolls = this._rollingReturns(this.hpAsset, this.hpHorizon);
  if (!rolls.length) return '<div style="padding:40px;text-align:center;color:var(--text-3)">Not enough data for this horizon</div>';

  const W = 1200, H = 240, PL = 52, PR = 20, PT = 15, PB = 35;
  const chartW = W - PL - PR, chartH = H - PT - PB;
  const rets = rolls.map(r => r.ret);
  const rawMin = Math.min(...rets), rawMax = Math.max(...rets);
  // Pad and force zero into the range when crossing
  const pad = (rawMax - rawMin) * 0.05 || 0.01;
  const minR = Math.min(rawMin - pad, 0);
  const maxR = Math.max(rawMax + pad, 0);

  const xS = (i) => PL + (i / Math.max(rolls.length - 1, 1)) * chartW;
  const yS = (v) => PT + chartH - ((v - minR) / (maxR - minR)) * chartH;

  // Y ticks: 5 evenly spaced across [minR, maxR], emphasised at v=0
  let yTicks = '';
  for (let i = 0; i <= 4; i++) {
    const v = minR + (maxR - minR) * i / 4;
    const y = yS(v);
    const isZero = Math.abs(v) < 1e-9;
    const stroke = isZero ? 'rgba(255,255,255,0.20)' : 'rgba(255,255,255,0.04)';
    const fill   = isZero ? '#a1a1aa' : '#52525b';
    const sign   = v > 0 ? '+' : '';
    yTicks += `<line x1="${PL}" y1="${y.toFixed(1)}" x2="${W-PR}" y2="${y.toFixed(1)}" stroke="${stroke}" stroke-width="1"/>`;
    yTicks += `<text x="${PL-4}" y="${(y+4).toFixed(1)}" text-anchor="end" style="font-size:9px;fill:${fill};font-family:ui-monospace">${sign}${(v*100).toFixed(0)}%</text>`;
  }
  // Snap zero gridline if not on a tick
  const hasZeroTick = false; // we always include zero via min/max forcing — keep simple
  const y0 = yS(0);
  const zl = `<line x1="${PL}" y1="${y0.toFixed(1)}" x2="${W-PR}" y2="${y0.toFixed(1)}" stroke="rgba(255,255,255,0.20)" stroke-width="1"/>`;

  // X ticks: every 5 calendar years
  let xLabels = '';
  const yrSeen = new Set();
  for (let i = 0; i < rolls.length; i++) {
    const yr = parseInt(rolls[i].ym.slice(0, 4));
    if (yr % 5 !== 0 || yrSeen.has(yr)) continue;
    if (rolls[i].ym.slice(5, 7) !== '01') continue; // anchor on Jan
    yrSeen.add(yr);
    const x = xS(i);
    xLabels += `<text x="${x.toFixed(1)}" y="${H-8}" text-anchor="middle" style="font-size:9px;fill:#52525b;font-family:ui-monospace">${yr}</text>`;
  }

  // Axis lines (new)
  const yAxis = `<line x1="${PL}" y1="${PT}" x2="${PL}" y2="${H-PB}" stroke="#52525b" stroke-width="1" opacity="0.6"/>`;
  const xAxis = `<line x1="${PL}" y1="${H-PB}" x2="${W-PR}" y2="${H-PB}" stroke="#52525b" stroke-width="1" opacity="0.6"/>`;

  // Area + line
  let area = `M${PL},${y0.toFixed(1)} `, line = '';
  for (let i = 0; i < rolls.length; i++) {
    const x = xS(i).toFixed(1), y = yS(rolls[i].ret).toFixed(1);
    area += `L${x},${y} `;
    line += i === 0 ? `M${x},${y} ` : `L${x},${y} `;
  }
  area += `L${xS(rolls.length-1).toFixed(1)},${y0.toFixed(1)} Z`;
  const fc = rolls[rolls.length-1].ret >= 0 ? '#34d399' : '#fb7185';

  // Crosshair placeholder (controlled by hpChartHover)
  const crosshair = `<line id="hp-crosshair" x1="0" y1="${PT}" x2="0" y2="${H-PB}" stroke="rgba(255,255,255,0.25)" stroke-width="1" stroke-dasharray="3,3" opacity="0"/>`;

  return `<svg viewBox="0 0 ${W} ${H}" style="width:100%;min-width:600px;height:${H}px;display:block">${yTicks}${zl}${xLabels}${yAxis}${xAxis}<path d="${area}" fill="${fc}" opacity="0.15"/><path d="${line.trim()}" fill="none" stroke="${fc}" stroke-width="1.5"/>${crosshair}</svg>`;
}
```

**Hover handler** (new):

```js
hpChartHover(e) {
  const svg = e.currentTarget.querySelector('svg');
  if (!svg) return;
  const rolls = this._rollingReturns(this.hpAsset, this.hpHorizon);
  if (!rolls.length) return;
  const rect = svg.getBoundingClientRect();
  const W = 1200, PL = 52, PR = 20, chartW = W - PL - PR;
  const scaleX = rect.width / W;
  const relX = (e.clientX - rect.left) / scaleX;
  const frac = Math.max(0, Math.min(1, (relX - PL) / chartW));
  const idx = Math.round(frac * (rolls.length - 1));
  const pt = rolls[idx];
  this.hpHoverInfo = { date: pt.ym, val: pt.ret };
  const ch = svg.getElementById('hp-crosshair');
  if (ch) {
    const x = PL + (idx / Math.max(rolls.length - 1, 1)) * chartW;
    ch.setAttribute('x1', x); ch.setAttribute('x2', x); ch.setAttribute('opacity', '1');
  }
}
```

**Template wiring** (replaces existing `<div x-html="hpChartSvg" ...>`):

```html
<div x-html="hpChartSvg"
     @mousemove="hpChartHover($event)"
     @mouseleave="hpHoverInfo = null"
     style="width:100%;overflow-x:auto;"></div>
<div x-show="hpHoverInfo" class="px-4 py-2 flex flex-wrap gap-x-6 gap-y-1 text-xs num font-mono"
     style="border-top: 1px solid var(--border); background: var(--surface-2)">
  <span style="color: var(--text-3)" x-text="hpHoverInfo?.date || ''"></span>
  <span :style="`color: ${(hpHoverInfo?.val ?? 0) >= 0 ? 'var(--up)' : 'var(--down)'}`"
        x-text="hpHoverInfo ? `Rolling ${hpHorizon}Y: ${(hpHoverInfo.val >= 0 ? '+' : '') + (hpHoverInfo.val * 100).toFixed(1)}%` : ''"></span>
</div>
```

### 4. HoldLab_Distribution_Chart — `hpDistSvg` polish

Replace the current minimal output with axis chrome, count y-ticks, signed-percentage x-ticks, and bar-level hover indices.

```js
get hpDistSvg() {
  const rolls = this._rollingReturns(this.hpAsset, this.hpHorizon);
  if (!rolls.length) return '';
  const rets = rolls.map(r => r.ret);
  const min = Math.min(...rets), max = Math.max(...rets);
  const nB = 20;
  const step = (max - min) / nB || 0.01;
  const buckets = Array(nB).fill(0);
  for (const r of rets) buckets[Math.min(Math.floor((r - min) / step), nB - 1)]++;
  const maxC = Math.max(...buckets);

  const W = 700, H = 160, PL = 36, PR = 12, PT = 12, PB = 28;
  const chartW = W - PL - PR, chartH = H - PT - PB;
  const bw = chartW / nB;

  // Y ticks: 4 evenly spaced integer counts from 0 to maxC
  let yTicks = '';
  for (let i = 0; i <= 3; i++) {
    const c = Math.round((maxC * i) / 3);
    const y = PT + chartH - (c / Math.max(maxC, 1)) * chartH;
    yTicks += `<line x1="${PL}" y1="${y.toFixed(1)}" x2="${W-PR}" y2="${y.toFixed(1)}" stroke="rgba(255,255,255,0.04)" stroke-width="1"/>`;
    yTicks += `<text x="${PL-4}" y="${(y+3.5).toFixed(1)}" text-anchor="end" style="font-size:9px;fill:#52525b;font-family:ui-monospace">${c}</text>`;
  }

  // Bars + x-tick labels at every 4th midpoint
  let bars = '', xLabels = '';
  for (let i = 0; i < nB; i++) {
    const bh = (buckets[i] / Math.max(maxC, 1)) * chartH;
    const x = PL + i * bw, y = PT + chartH - bh;
    const lo = min + i * step, hi = lo + step, mid = (lo + hi) / 2;
    bars += `<rect x="${x.toFixed(1)}" y="${y.toFixed(1)}" width="${(bw-1).toFixed(1)}" height="${bh.toFixed(1)}" fill="${mid >= 0 ? '#34d399' : '#fb7185'}" opacity="0.7" rx="1" data-bucket="${i}"/>`;
    if (i % 4 === 0) {
      const sign = mid > 0 ? '+' : '';
      xLabels += `<text x="${(x+bw/2).toFixed(1)}" y="${H-8}" text-anchor="middle" style="font-size:9px;fill:#52525b;font-family:ui-monospace">${sign}${(mid*100).toFixed(0)}%</text>`;
    }
  }

  const yAxis = `<line x1="${PL}" y1="${PT}" x2="${PL}" y2="${H-PB}" stroke="#52525b" stroke-width="1" opacity="0.6"/>`;
  const xAxis = `<line x1="${PL}" y1="${H-PB}" x2="${W-PR}" y2="${H-PB}" stroke="#52525b" stroke-width="1" opacity="0.6"/>`;

  return `<svg viewBox="0 0 ${W} ${H}" style="width:100%;max-width:700px;height:${H}px;display:block">${yTicks}${bars}${xLabels}${yAxis}${xAxis}</svg>`;
}
```

**Hover handler** (new) — bucket-level rather than mouse-x scan, since bars are discrete:

```js
hpDistHover(e) {
  const target = e.target.closest('rect[data-bucket]');
  if (!target) { this.hpDistHoverInfo = null; return; }
  const i = parseInt(target.getAttribute('data-bucket'));
  const rolls = this._rollingReturns(this.hpAsset, this.hpHorizon);
  if (!rolls.length) return;
  const rets = rolls.map(r => r.ret);
  const min = Math.min(...rets), max = Math.max(...rets);
  const nB = 20, step = (max - min) / nB || 0.01;
  const lo = min + i * step, hi = lo + step;
  const buckets = Array(nB).fill(0);
  for (const r of rets) buckets[Math.min(Math.floor((r - min) / step), nB - 1)]++;
  this.hpDistHoverInfo = { lo, hi, count: buckets[i] };
}
```

**Template wiring** (replaces existing `<div x-html="hpDistSvg" ...>`):

```html
<div x-html="hpDistSvg"
     @mousemove="hpDistHover($event)"
     @mouseleave="hpDistHoverInfo = null"
     style="width:100%;overflow-x:auto;"></div>
<div x-show="hpDistHoverInfo" class="px-4 py-2 flex flex-wrap gap-x-6 gap-y-1 text-xs num font-mono"
     style="border-top: 1px solid var(--border); background: var(--surface-2)">
  <span style="color: var(--text-3)"
        x-text="hpDistHoverInfo ? `${(hpDistHoverInfo.lo>=0?'+':'') + (hpDistHoverInfo.lo*100).toFixed(1)}% → ${(hpDistHoverInfo.hi>=0?'+':'') + (hpDistHoverInfo.hi*100).toFixed(1)}%` : ''"></span>
  <span style="color: var(--text)"
        x-text="hpDistHoverInfo ? `${hpDistHoverInfo.count} window${hpDistHoverInfo.count === 1 ? '' : 's'}` : ''"></span>
</div>
```

### 5. Drawdown_Chart and Correlation_Growth_Chart — consistency pass

Both charts already have tick labels and gridlines but lack explicit axis lines.

**`drawdownChartSvg`** — append two lines before the final `return` template:

```js
const yAxis = `<line x1="${PL}" y1="${PT}" x2="${PL}" y2="${H-PB}" stroke="#52525b" stroke-width="1" opacity="0.6"/>`;
const xAxis = `<line x1="${PL}" y1="${H-PB}" x2="${W-PR}" y2="${H-PB}" stroke="#52525b" stroke-width="1" opacity="0.6"/>`;
return `<svg ...>${yTicks}${xLabels}${areas}${yAxis}${xAxis}${crosshair}</svg>`;
```

The existing zero-line emphasis at `rgba(255,255,255,0.20)` and `#a1a1aa` label fill is already correct (line 3886 — already in line with Requirement 5.2). No change to underwater curves, table, or hover behaviour.

**`growthChartSvg`** — same pattern: append two axis lines and inject signed-percentage formatting is not needed here (dollar labels stay as-is). The existing horizontal gridlines at `rgba(255,255,255,0.04)` already meet Requirement 5.5.

```js
const yAxis = `<line x1="${PL}" y1="${PT}" x2="${PL}" y2="${H-PB}" stroke="#52525b" stroke-width="1" opacity="0.6"/>`;
const xAxis = `<line x1="${PL}" y1="${H-PB}" x2="${W-PR}" y2="${H-PB}" stroke="#52525b" stroke-width="1" opacity="0.6"/>`;
return `<svg ...>${ticks}${xLabels}${lines}${yAxis}${xAxis}${ch}</svg>`;
```

`growthHover`, `drawdownHover`, and `growthHoverInfo` / `ddHoverInfo` bindings are unchanged.

## Visual Tokens

| Element | Value |
|---|---|
| Axis line stroke | `#52525b` (matches `--text-3`), opacity `0.6`, stroke-width `1` |
| Tick label font | `font-family: ui-monospace`, `font-size: 9px` |
| Tick label fill (non-zero) | `#52525b` |
| Tick label fill (zero) | `#a1a1aa` |
| Gridline stroke (non-zero) | `rgba(255,255,255,0.04)` |
| Gridline stroke (zero) | `rgba(255,255,255,0.20)` |
| Crosshair stroke | `rgba(255,255,255,0.25)`, dasharray `3,3` |
| Hover bar background | `var(--surface-2)` |
| Hover bar border-top | `1px solid var(--border)` |

These match the values already in `yieldChartSvg` and `drawdownChartSvg` exactly. No new tokens are introduced. The literal hex values used (`#52525b`, `#a1a1aa`) are the same ones already inlined throughout existing SVG getters; switching to `var(--text-3)` inside SVG attribute strings is not feasible because Alpine inlines these getters as raw HTML strings outside any CSS scope.

## Error Handling

- **Empty rate series**: `yieldYears` returns `[]`; the table renders zero year columns and zero data rows. No exception.
- **Missing year**: `getYieldValue` returns `null`; the cell renders empty.
- **Insufficient rolling-return data**: `hpChartSvg` returns the existing "Not enough data" placeholder div; `hpDistSvg` returns `''`. Hover handlers early-return on empty `rolls`.
- **Hover before render**: `e.currentTarget.querySelector('svg')` returns `null`; handler early-returns.
- **Bucket hover when target is the SVG background**: `e.target.closest('rect[data-bucket]')` returns `null`; handler clears `hpDistHoverInfo`.

No new error conditions introduced; all paths follow existing handler conventions.

## Testing Strategy

The existing repo has no JS test runner. The PBT properties below are written as standalone Node scripts that exercise the pure functions in isolation. The Hold Lab and chart-polish work is also covered by manual verification on the live page at common viewport widths.

### Pure Functions Under Test

These four functions can be lifted out and tested in isolation. Each takes plain data and returns plain data — no DOM, no Alpine.

- `yieldYearsFor(rates)` — returns deduped ascending integer years from `rates[*].values` tuple arrays.
- `getYieldValueFor(rates, seriesKey, year)` — returns December value, else latest in-year value, else `null`.
- `rollingHoverIndex(rolls, frac, W, PL, PR)` — returns the index `round(frac*(n-1))` clamped to `[0, n-1]`.
- `bucketize(rets, nB)` — returns `{ buckets, min, step }` for the histogram.

The test harness uses `fast-check` (added as a dev-only dependency in a separate `tests/` folder if a harness is set up; if no harness is set up, properties are documented and a manual one-shot Node script under `tests/markets-yield-holdlab-polish.test.mjs` runs the assertions with hand-rolled generators). Per Requirement 6.2, no new runtime dependencies enter `docs/markets.html` itself.

### Manual Verification

1. **Yield bug**: open `docs/markets.html` → Yield History tab → confirm Annual Averages headers show real years (e.g. `1934 ... 2025`) and cells render percentages.
2. **KPI grid**: resize window from 320px → 1600px and confirm column count steps through 2 → 4 → 6 → 7.
3. **Rolling chart**: hover sweep left-to-right; crosshair tracks; readout bar shows `YYYY-MM` and signed pct.
4. **Distribution chart**: hover each bar; readout shows bucket range and count.
5. **Drawdown / Growth**: confirm visible y-axis line at left edge and x-axis line at bottom edge; existing hover behaviour still works.

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system — essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

### Property 1: yieldYears covers union of tuple-array years

For any `rates` object whose every series stores `values` as a sorted array of `[YYYY-MM, value]` tuples, `yieldYears(rates)` returns a deduplicated, ascending list of integers equal to the set of all four-digit year prefixes present across the union of all tuple arrays.

**Validates: Requirements 1.1, 1.2**

### Property 2: getYieldValue lookup precedence

For any `rates` object, any series key, and any integer year, `getYieldValue(rates, key, year)`:
- returns the second element of the tuple whose first element equals `${year}-12` if such a tuple exists in that series;
- otherwise returns the second element of the lexicographically-greatest tuple whose first element starts with `${year}-` if any such tuple exists;
- otherwise returns `null`.

**Validates: Requirements 1.3, 1.4, 1.5**

### Property 3: hpChartSvg axis chrome completeness

For any non-empty rolling-return series produced by `_rollingReturns`, the string returned by `hpChartSvg` contains:
- exactly one y-axis line element with `x1=PL` and `x2=PL`,
- exactly one x-axis line element with `y1=H-PB` and `y2=H-PB`,
- five y-tick `<text>` elements with monospace styling,
- one gridline `<line>` per y-tick at stroke `rgba(255,255,255,0.04)` (non-zero) or `rgba(255,255,255,0.20)` (zero),
- one `<line id="hp-crosshair">` element.

**Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7**

### Property 4: hpDistSvg axis chrome completeness

For any non-empty rolling-return series, the string returned by `hpDistSvg` contains:
- exactly one y-axis line at `x=PL`,
- exactly one x-axis line at `y=H-PB`,
- four y-tick `<text>` elements showing integer counts,
- one gridline `<line>` per y-tick at stroke `rgba(255,255,255,0.04)`,
- x-tick `<text>` elements at midpoints of every fourth bucket formatted as signed percentages.

**Validates: Requirements 4.1, 4.2, 4.3, 4.4, 4.5**

### Property 5: hpChartHover index mapping

For any non-empty rolling-return series of length `n` and any `frac ∈ [0, 1]`, `hpChartHover` with a synthetic mouse event placing `clientX` at fraction `frac` of the SVG width sets `hpHoverInfo` to `{ date: rolls[idx].ym, val: rolls[idx].ret }` where `idx = round(frac * (n-1))`.

**Validates: Requirements 3.8**

### Property 6: hpDistHover bucket mapping

For any non-empty rolling-return series and any bucket index `i ∈ [0, nB)`, `hpDistHover` with a synthetic mouse event whose target is the rect with `data-bucket="${i}"` sets `hpDistHoverInfo` to `{ lo: min + i*step, hi: min + (i+1)*step, count: buckets[i] }` where `min`, `step`, `buckets` are computed identically to `hpDistSvg`.

**Validates: Requirements 4.6**

### Property 7: Drawdown and Growth charts include explicit axis lines

For any non-empty asset selection that produces a non-empty `drawdownChartSvg` or `growthChartSvg`, the returned SVG string contains:
- exactly one `<line>` element with `x1=PL` and `x2=PL` (y-axis),
- exactly one `<line>` element with `y1=H-PB` and `y2=H-PB` (x-axis at bottom).

**Validates: Requirements 5.1, 5.4**
