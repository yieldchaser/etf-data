# Design Document: Markets Asset Detail Polish

## Overview

Fix and polish the Markets page asset detail panel in the Predator Protocol dashboard. This covers: correcting the returns heatmap to render all historical years (including pre-1900 data), adding decade navigation, polishing the heatmap and multi-currency table UI to institutional-grade quality, and replacing the buggy dual native range slider with a custom Alpine.js dual-handle slider.

All changes are scoped to `docs/markets.html`, a single-file SPA (~4883 lines) using Alpine.js for reactivity and Tailwind CSS for styling. The architecture follows the existing pattern: computed getters build HTML strings that Alpine renders via `x-html` bindings. No new files or build steps are introduced.

## Architecture

### Component Map

```
marketsApp() Alpine.js x-data
├── get assetMonthlyHeatmapHtml()   ← Requirement 1, 2, 3
├── get assetMultiCcyTableHtml()    ← Requirement 5
├── Dual-Handle Slider component    ← Requirement 4
│   ├── _sliderPointerDown/Move/Up()
│   ├── _sliderStartPct / _sliderEndPct (computed)
│   └── _trcInit() / _trcSyncWindow()
├── cellColor(ret)                  ← shared color scale
└── cellTextColor(ret)              ← shared text contrast
```

### Design Principles

- **Single-file constraint**: All logic remains in `docs/markets.html`
- **Computed getter pattern**: HTML is generated as strings in Alpine.js getters, rendered via `x-html`
- **Shared color scale**: `cellColor(ret)` is reused across heatmap, multi-ccy table, and return matrix
- **Pointer events**: Unified mouse/touch handling via the Pointer Events API
- **No external dependencies**: Only Alpine.js and Tailwind CSS (already loaded via CDN)

## Components and Interfaces

### Component 1: Heatmap Year Iteration Fix (Requirement 1)

### Root Cause Analysis

The current `assetMonthlyHeatmapHtml` getter extracts years correctly using:

```javascript
const years = [...new Set(adjusted.map(a => parseInt(a.ym.slice(0,4))))].sort((a,b) => b - a);
```

This approach derives years from the `adjusted` array (output of `_getAdjustedClose`). The bug occurs when `_getAdjustedClose` filters out months where the FX multiplier returns `null`. For assets with pre-1900 data, if the FX lookup has no data for those early months, the multiplier returns `null` and those months are excluded from `adjusted`, causing entire early years to disappear.

### Fix Strategy

The heatmap should always operate on the **raw monthly data** for the selected asset when the active currency lens is "Local" (no FX conversion needed). When a non-Local lens is active, the heatmap already correctly shows only months where FX data exists.

The fix ensures the year extraction loop iterates over all years present in the source data:

```javascript
get assetMonthlyHeatmapHtml() {
  if (!this.selectedAsset || !this.returnsData) return '';
  const adjusted = this._getAdjustedClose(this.selectedAsset);
  if (adjusted.length < 2) return '...';

  // Build monthly returns from consecutive pairs in adjusted array
  const monthlyRet = {};
  for (let i = 1; i < adjusted.length; i++) {
    const prev = adjusted[i - 1].close, curr = adjusted[i].close;
    if (prev > 0 && curr != null) {
      monthlyRet[adjusted[i].ym] = curr / prev - 1;
    }
  }

  // Extract ALL years from adjusted data — no off-by-one
  const years = [...new Set(adjusted.map(a => parseInt(a.ym.slice(0, 4))))]
    .sort((a, b) => b - a);

  // ... render table with all years
}
```

The key insight: the return for month M is computed from the pair `(adjusted[i-1], adjusted[i])` where `adjusted[i].ym === M`. This means the first month of the entire series has no return (no predecessor), but its year still appears in the year list. The fix ensures we don't skip years that only have the "first month" entry (no computable return yet) — those years render with empty cells.

### Cross-Year Boundary Handling

Returns across year boundaries (e.g., Dec 2020 → Jan 2021) are handled naturally because `adjusted` is sorted chronologically and the loop pairs consecutive entries regardless of year. No special year-boundary logic is needed.

## Component 2: Decade Navigation (Requirement 2)

### Architecture

The decade navigator is rendered conditionally above the heatmap when `years.length > 30`. It consists of:

1. **Decade buttons** — one per distinct decade in the data
2. **Scroll-to behavior** — each button scrolls the heatmap container to an anchor element
3. **Active state tracking** — IntersectionObserver highlights the currently visible decade

### Implementation Design

```javascript
// Inside assetMonthlyHeatmapHtml getter:
// 1. Compute decades from years
const decades = [...new Set(years.map(y => Math.floor(y / 10) * 10))].sort((a, b) => b - a);

// 2. Render decade nav bar (only if years.length > 30)
if (years.length > 30) {
  html = `<div class="heatmap-decade-nav flex gap-1 flex-wrap mb-2 sticky top-0 z-10 py-1"
               style="background:var(--surface)">` +
    decades.map(d => `<button class="decade-btn px-2 py-0.5 rounded border text-[10px] font-mono"
      style="color:var(--text-3);border-color:var(--border)"
      data-decade="${d}"
      onclick="this.closest('[x-data]').__x.$data._scrollToDecade(${d})">${d}s</button>`
    ).join('') + `</div>` + html;
}

// 3. Each year row gets an id for scroll targeting
html += `<tr id="heatmap-yr-${yr}">...`;
```

### Scroll-to Method

```javascript
_scrollToDecade(decade) {
  // Find first year in this decade
  const target = document.getElementById(`heatmap-yr-${decade + 9}`);
  // Descending order: decade 1990 starts at row for 1999
  // Actually find the first row of the decade (highest year in that decade present)
  const container = target?.closest('.overflow-auto');
  if (target && container) {
    target.scrollIntoView({ block: 'start', behavior: 'smooth' });
  }
}
```

### IntersectionObserver for Active State

After the heatmap HTML is rendered, an `$nextTick` callback sets up an IntersectionObserver on decade boundary rows. When a decade boundary row enters the viewport top, the corresponding button gets the `tab-active` class.

```javascript
_initDecadeObserver() {
  const container = this.$el.querySelector('.heatmap-scroll-container');
  if (!container) return;
  if (this._decadeObserver) this._decadeObserver.disconnect();

  const buttons = container.parentElement.querySelectorAll('.decade-btn');
  const rows = container.querySelectorAll('tr[id^="heatmap-yr-"]');

  this._decadeObserver = new IntersectionObserver((entries) => {
    for (const entry of entries) {
      if (entry.isIntersecting) {
        const yr = parseInt(entry.target.id.replace('heatmap-yr-', ''));
        const decade = Math.floor(yr / 10) * 10;
        buttons.forEach(b => {
          b.classList.toggle('tab-active', parseInt(b.dataset.decade) === decade);
        });
      }
    }
  }, { root: container, rootMargin: '0px 0px -90% 0px', threshold: 0 });

  rows.forEach(row => this._decadeObserver.observe(row));
}
```

## Component 3: Heatmap UI Polish (Requirement 3)

### CSS Changes

All styling is applied inline within the HTML string generated by `assetMonthlyHeatmapHtml`:

| Feature | Implementation |
|---------|---------------|
| Color scale | `background: ${this.cellColor(ret)}` — reuses existing function |
| Cell borders | `border: 1px solid rgba(255,255,255,0.04)` on every `<td>` |
| Row striping | `background: rgba(255,255,255,0.02)` on even-indexed rows |
| Sticky header | `position:sticky; top:0; z-index:3; background:var(--bg)` on `<thead>` |
| Sticky year col | `position:sticky; left:0; z-index:2; background:var(--bg)` on year `<td>` |
| Cell centering | `text-align:center; vertical-align:middle; padding:2px 0` |
| Annual total | Extra `<td>` at end of each row with compound return |

### Annual Total Column

```javascript
// After rendering 12 month cells, compute annual total
let annualCompound = 1;
let hasAny = false;
for (let m = 1; m <= 12; m++) {
  const ym = `${yr}-${String(m).padStart(2, '0')}`;
  const ret = monthlyRet[ym];
  if (ret !== undefined) {
    annualCompound *= (1 + ret);
    hasAny = true;
  }
}
const annualRet = hasAny ? annualCompound - 1 : null;
// Render annual total cell with same color scale
if (annualRet !== null) {
  const bg = this.cellColor(annualRet);
  const fg = this.cellTextColor(annualRet);
  const pct = (annualRet * 100).toFixed(1);
  const sign = annualRet >= 0 ? '+' : '';
  html += `<td style="...;background:${bg};color:${fg};font-weight:600">${sign}${pct}%</td>`;
} else {
  html += `<td style="..."></td>`;
}
```

### Sticky Header with Annual Column

The header row adds a "Total" label in the 14th column:

```javascript
html += `<th style="position:sticky;top:0;z-index:3;...">Total</th>`;
```

## Component 4: Custom Dual-Handle Slider (Requirement 4)

### Architecture

Replace the two stacked `<input type="range">` elements with a custom Alpine.js component using pointer events. The component renders:

1. **Track background** — full-width bar (height: 6px, rounded, dark background)
2. **Active segment** — colored bar between the two handles (cyan)
3. **Start handle** — circular draggable element (left)
4. **End handle** — circular draggable element (right)

### HTML Template

```html
<!-- Custom dual-handle slider -->
<div class="relative flex-1 select-none" style="height:32px; touch-action:none"
     @pointerdown="_sliderPointerDown($event)"
     @pointermove="_sliderPointerMove($event)"
     @pointerup="_sliderPointerUp($event)"
     @pointercancel="_sliderPointerUp($event)">
  <!-- Track background -->
  <div class="absolute top-1/2 -translate-y-1/2 w-full rounded-full"
       style="height:4px; background:var(--border-2)"></div>
  <!-- Active segment -->
  <div class="absolute top-1/2 -translate-y-1/2 rounded-full"
       style="height:4px; background:var(--cyan)"
       :style="`left:${_sliderStartPct}%; width:${_sliderEndPct - _sliderStartPct}%`"></div>
  <!-- Start handle -->
  <div class="absolute top-1/2 -translate-y-1/2 rounded-full border-2 cursor-grab"
       style="width:16px; height:16px; background:var(--bg); border-color:var(--cyan); margin-left:-8px"
       :style="`left:${_sliderStartPct}%`"></div>
  <!-- End handle -->
  <div class="absolute top-1/2 -translate-y-1/2 rounded-full border-2 cursor-grab"
       style="width:16px; height:16px; background:var(--bg); border-color:var(--cyan); margin-left:-8px"
       :style="`left:${_sliderEndPct}%`"></div>
</div>
```

### Reactive Computed Properties

```javascript
get _sliderStartPct() {
  const total = this._trcAllMonths.length - 1;
  return total > 0 ? (this.trcStartIdx / total) * 100 : 0;
},

get _sliderEndPct() {
  const total = this._trcAllMonths.length - 1;
  return total > 0 ? (this.trcEndIdx / total) * 100 : 100;
},
```

### Pointer Event Handlers

```javascript
_sliderDragging: null, // 'start' | 'end' | null

_sliderPointerDown(e) {
  const rect = e.currentTarget.getBoundingClientRect();
  const pct = (e.clientX - rect.left) / rect.width;
  const total = this._trcAllMonths.length - 1;
  const idx = Math.round(pct * total);

  // Determine which handle is closer
  const distStart = Math.abs(idx - this.trcStartIdx);
  const distEnd = Math.abs(idx - this.trcEndIdx);
  this._sliderDragging = distStart <= distEnd ? 'start' : 'end';

  e.currentTarget.setPointerCapture(e.pointerId);
  this._sliderPointerMove(e); // apply immediately
},

_sliderPointerMove(e) {
  if (!this._sliderDragging) return;
  const rect = e.currentTarget.getBoundingClientRect();
  const pct = Math.max(0, Math.min(1, (e.clientX - rect.left) / rect.width));
  const total = this._trcAllMonths.length - 1;
  const idx = Math.round(pct * total);

  if (this._sliderDragging === 'start') {
    if (idx < this.trcEndIdx) {
      this.trcStartIdx = idx;
      this.trcPreset = 'custom';
      this._trcSyncWindow();
    }
  } else {
    if (idx > this.trcStartIdx) {
      this.trcEndIdx = idx;
      this.trcPreset = 'custom';
      this._trcSyncWindow();
    }
  }
},

_sliderPointerUp(e) {
  this._sliderDragging = null;
},
```

### Touch Support

The `touch-action: none` CSS property on the container prevents browser scroll interference. Pointer events (`pointerdown`, `pointermove`, `pointerup`) unify mouse and touch handling — no separate touch event listeners needed. `setPointerCapture` ensures moves are tracked even if the pointer leaves the element.

## Component 5: Multi-Currency Table Polish (Requirement 5)

### Styling Approach

The `assetMultiCcyTableHtml` getter is enhanced with:

| Feature | Implementation |
|---------|---------------|
| Sticky header | `position:sticky; top:0; z-index:3; background:var(--bg)` on `<th>` |
| Row striping | `background: rgba(255,255,255,0.02)` on even-indexed rows |
| Color tint | `background: ${this.cellColor(ret)}20` (hex alpha for subtle tint) |
| Cell padding | `padding: 4px 10px` |
| Right-aligned | `text-align:right; font-variant-numeric:tabular-nums` |
| Column borders | `border-left: 1px solid var(--border)` on non-first data cells |
| Sticky year col | `position:sticky; left:0; z-index:2; background:var(--bg)` |
| CAGR row | Inserted after `<thead>` as a summary row |

### CAGR Calculation

```javascript
// For each lens, compute CAGR from first to last available year
const cagrRow = {};
for (const lens of lenses) {
  const annual = annualByLens[lens];
  const yrs = Object.keys(annual).map(Number).sort((a, b) => a - b);
  if (yrs.length < 2) { cagrRow[lens] = null; continue; }

  // Compound all annual returns
  let compound = 1;
  for (const yr of yrs) {
    compound *= (1 + annual[yr].ret);
  }
  const n = yrs[yrs.length - 1] - yrs[0]; // number of years spanned
  cagrRow[lens] = n > 0 ? Math.pow(compound, 1 / n) - 1 : null;
}
```

### Color Tint Strategy

Instead of full-saturation backgrounds (which would overwhelm the table), use the color scale at reduced opacity:

```javascript
const tintBg = this.cellColor(ret); // e.g., '#3b82f6'
// Apply as semi-transparent background
const style = `background: ${tintBg}20;`; // 20 = ~12% opacity in hex
```

This provides visual heat while keeping text readable against the dark theme.

---

## Data Models

### Monthly Close Array Format

```javascript
// market_returns.json → assets[key].monthly
// Array of [ym, close] tuples sorted chronologically
[
  ["1835-01", 4.52],
  ["1835-02", 4.61],
  // ...
  ["2025-01", 5842.10]
]
```

### Adjusted Close Entry

```javascript
// Output of _getAdjustedClose(assetKey)
{ ym: "2024-06", close: 5432.10 }  // close adjusted for FX lens
```

### Monthly Return Map

```javascript
// Built inside assetMonthlyHeatmapHtml
// Key: "YYYY-MM", Value: decimal return (e.g., 0.05 = +5%)
{ "2024-01": 0.032, "2024-02": -0.011, ... }
```

### Annual Return by Lens

```javascript
// Output of _computeAnnualForLens(assetKey, ccy, real)
{ 2024: { ret: 0.24, partial: false }, 2023: { ret: 0.12, partial: false }, ... }
```

### Slider State

```javascript
{
  trcStartIdx: 0,           // index into _trcAllMonths
  trcEndIdx: 240,           // index into _trcAllMonths
  _trcAllMonths: ["1835-01", ...],  // sorted month strings
  _sliderDragging: null,    // 'start' | 'end' | null
  trcPreset: 'max',         // '1m'|'6m'|'1y'|'2y'|'5y'|'10y'|'max'|'custom'
}
```

## Data Flow

```
market_returns.json
  └── load() → this.returnsData
        ├── _getAdjustedClose(assetKey) → [{ym, close}, ...]
        │     └── applies FX multiplier based on activeCcy
        ├── assetMonthlyHeatmapHtml (getter)
        │     ├── computes monthlyRet from consecutive pairs
        │     ├── extracts ALL years from adjusted array
        │     ├── renders decade nav (if >30 years)
        │     ├── renders table with sticky headers, striping, borders
        │     └── renders annual total column
        ├── assetMultiCcyTableHtml (getter)
        │     ├── _computeAnnualForLens() for each lens
        │     ├── renders CAGR summary row
        │     └── renders styled year rows with color tints
        └── Dual-Handle Slider
              ├── _sliderStartPct / _sliderEndPct (computed)
              ├── pointer event handlers
              └── updates trcStartIdx / trcEndIdx → _trcSyncWindow()
```

---

## Error Handling

| Scenario | Handling |
|----------|----------|
| Asset has < 2 monthly data points | Return "Not enough data" message |
| FX multiplier returns null for a month | Month excluded from adjusted array; cell renders empty |
| No data for a month within a year | Cell renders as empty (no background, no text) |
| Division by zero (prev close = 0) | Skip return calculation; cell renders empty |
| Slider dragged beyond bounds | Clamped to [0, total-1] via Math.max/Math.min |
| Start handle dragged past end | Rejected (idx < endIdx check) |
| Touch event on slider | Handled by pointer events + touch-action:none |
| IntersectionObserver not supported | Decade buttons still work for click-to-scroll; no active highlighting |

---

## Testing Strategy

### Unit Tests (Example-Based)

- Verify decade navigator visibility threshold (30 rows boundary)
- Verify sticky CSS properties are present in generated HTML
- Verify slider rejects crossing handles (specific index pairs)
- Verify CAGR calculation with known return series

### Property Tests

- Year completeness: random monthly arrays → all years present in output
- Return calculation: random close pairs → correct return formula
- Sort invariant: random year sets → descending order
- Decade completeness: random year sets → correct decade buttons
- Annual total: random monthly returns → correct compound
- Handle ordering: random drag sequences → invariant holds
- Color consistency: random returns → cellColor applied

### Integration Tests

- IntersectionObserver decade highlighting (requires DOM)
- Pointer event drag sequences on slider (requires DOM)
- Reactive chart update on handle drag (requires Alpine.js lifecycle)

---

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system — essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

### Property 1: Year completeness

*For any* Monthly_Close_Array containing data points across N distinct years, the Heatmap_Generator SHALL produce exactly N year rows in its output, one for each distinct year present in the input data.

**Validates: Requirements 1.1**

### Property 2: Monthly return calculation correctness

*For any* two consecutive entries `(ym_prev, close_prev)` and `(ym_curr, close_curr)` in the sorted adjusted close array where `close_prev > 0`, the computed monthly return for `ym_curr` SHALL equal `close_curr / close_prev - 1`, regardless of whether `ym_prev` and `ym_curr` span a year boundary.

**Validates: Requirements 1.2, 1.3**

### Property 3: Descending year sort invariant

*For any* heatmap output with multiple year rows, each year value SHALL be strictly less than the year value in the preceding row (i.e., the sequence of years is strictly descending from top to bottom).

**Validates: Requirements 1.4**

### Property 4: Decade button completeness

*For any* set of years present in the heatmap data, the Decade_Navigator SHALL render exactly one button for each distinct decade (floor(year/10)*10) that contains at least one year from the data set.

**Validates: Requirements 2.2**

### Property 5: Annual total equals compound of monthly returns

*For any* year row in the heatmap, the annual total column value SHALL equal the product of `(1 + monthly_return)` for all months in that year that have a computable return, minus 1. If no months have computable returns, the annual total cell SHALL be empty.

**Validates: Requirements 3.7**

### Property 6: Slider handle ordering invariant

*For any* sequence of drag operations on the dual-handle slider, the invariant `trcStartIdx < trcEndIdx` SHALL hold after every operation completes. No drag operation SHALL result in the start index being equal to or greater than the end index.

**Validates: Requirements 4.4, 4.5**

### Property 7: Heatmap color scale consistency

*For any* monthly return value `r` displayed in the heatmap, the cell background color SHALL equal `cellColor(r)` — the same color scale function used by the Return_Matrix.

**Validates: Requirements 3.1**

### Property 8: Multi-CCY color scale consistency

*For any* annual return value `r` displayed in the Multi_CCY_Table, the cell background tint SHALL be derived from `cellColor(r)` — the same color scale function used by the Return_Matrix.

**Validates: Requirements 5.3**

### Property 9: CAGR calculation correctness

*For any* lens in the Multi_CCY_Table with annual returns spanning N years from year_first to year_last, the displayed CAGR SHALL equal `(compound_of_all_annual_returns)^(1/N) - 1` where N = year_last - year_first and the compound is the product of `(1 + annual_return)` for each year.

**Validates: Requirements 5.7**
