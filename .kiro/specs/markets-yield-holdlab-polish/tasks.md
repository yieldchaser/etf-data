# Implementation Plan: Markets Yield & Hold Lab Polish

## Overview

All work is confined to `docs/markets.html` (single-file Alpine.js 3.x SPA). The implementation proceeds in five sections, ordered to minimise edit conflicts in the single file:

1. **Yield bug fix** — pure data-access correction in five getters/methods (lines ~3468–3651). Independent of every other section.
2. **Hold Lab KPI grid** — single-line Tailwind class change at line ~1257.
3. **Hold Lab Rolling chart** — polish `hpChartSvg`, add `hpHoverInfo` state and `hpChartHover` handler, wire template at ~line 1267.
4. **Hold Lab Distribution chart** — polish `hpDistSvg`, add `hpDistHoverInfo` state and `hpDistHover` handler, wire template at ~line 1277.
5. **Drawdown + Growth consistency pass** — append explicit axis lines to `drawdownChartSvg` (~line 3935) and `growthChartSvg` (~line 4308).

Property tests live under `tests/markets-yield-holdlab-polish/` as standalone `*.test.mjs` Node scripts, one per property, using hand-rolled generators (no new runtime dependencies enter `docs/markets.html`).

## Tasks

- [x] 1. Fix Yield History Annual Averages tuple-array bug
  - [x] 1.1 Fix `yieldYears` getter to iterate tuple array
    - In `docs/markets.html`, locate the `yieldYears` getter (~line 3468)
    - Replace `Object.keys(s.values || {})` iteration with tuple destructuring `for (const [ym] of (s.values || []))`
    - Extract year via `ym.slice(0, 4)`; keep dedup-and-sort logic
    - _Requirements: 1.1, 1.2, 1.6_

  - [x] 1.2 Fix `getYieldValue` method to scan tuple array
    - In `docs/markets.html`, locate `getYieldValue(seriesKey, year)` (~line 3492)
    - Iterate `s.values || []` as `[ym, v]` tuples; track December (`${year}-12`) match and lexicographically-greatest in-year tuple
    - Return December match if present, else last in-year value, else `null`
    - _Requirements: 1.3, 1.4, 1.5, 1.7_

  - [x] 1.3 Fix `yieldChartMeta` getter to iterate tuple array
    - In `docs/markets.html`, locate `yieldChartMeta` getter (~line 3479)
    - Replace `Object.keys(r.values || {})` with tuple iteration; preserve min/max/first/last computation semantics
    - _Requirements: 1.1, 1.2_

  - [x] 1.4 Fix `yieldChartSvg` getter to iterate tuple array
    - In `docs/markets.html`, locate `yieldChartSvg` getter (~line 3506)
    - For each rate series, build `const valMap = new Map(s.values || [])` once per render
    - Replace `Object.keys(vals)` with `Array.from(valMap.keys())` and `vals[m]` with `valMap.get(m)`
    - Keep all existing visual output (axis chrome, gridlines, lines) byte-identical for non-empty data
    - _Requirements: 1.1, 1.2, 1.6_

  - [x] 1.5 Fix `yieldChartHover` method to iterate tuple array
    - In `docs/markets.html`, locate `yieldChartHover` method (~line 3611)
    - Build the same `valMap` pattern; replace `Object.keys`/`vals[m]` accesses
    - Preserve `yieldHoverInfo` shape exactly so existing template bindings continue to work
    - _Requirements: 1.1, 1.2_

  - [ ]* 1.6 Write property test for `yieldYears`
    - Create `tests/markets-yield-holdlab-polish/yield-years.test.mjs`
    - Lift the fixed `yieldYears` logic into a pure helper `yieldYearsFor(rates)` for testing
    - **Property 1: yieldYears covers union of tuple-array years**
    - Generate random `rates` objects whose `values` arrays contain random `[YYYY-MM, value]` tuples; assert returned list equals the deduped, ascending union of year prefixes
    - **Validates: Requirements 1.1, 1.2**

  - [ ]* 1.7 Write property test for `getYieldValue` lookup precedence
    - Create `tests/markets-yield-holdlab-polish/yield-cell-lookup.test.mjs`
    - Lift the fixed lookup into a pure helper `getYieldValueFor(rates, key, year)` for testing
    - **Property 2: getYieldValue lookup precedence**
    - Generate series with random presence/absence of December tuples and partial-year coverage; assert December-first, then lexicographic-latest-in-year, then `null`
    - **Validates: Requirements 1.3, 1.4, 1.5**

- [x] 2. Update Hold Lab KPI strip to responsive ladder
  - [x] 2.1 Change KPI grid Tailwind classes
    - In `docs/markets.html`, locate the KPI strip `<div class="grid grid-cols-2 sm:grid-cols-5 gap-3 mb-5">` (~line 1257)
    - Replace with `class="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-6 xl:grid-cols-7 gap-3 mb-5"`
    - Do not modify any inner tile markup
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6_

- [x] 3. Polish Hold Lab Rolling chart with axis chrome and hover
  - [x] 3.1 Add `hpHoverInfo` reactive state field
    - In `docs/markets.html`, in the Alpine component data section, add `hpHoverInfo: null,` near the other Hold Lab state fields
    - _Requirements: 3.7, 3.8_

  - [x] 3.2 Polish `hpChartSvg` getter with axis lines, gridlines, ticks, crosshair placeholder
    - In `docs/markets.html`, locate `hpChartSvg` getter (~line 4150)
    - Add explicit y-axis line at `x=PL` and x-axis line at `y=H-PB`, both `stroke="#52525b"` opacity `0.6`
    - Add 5 evenly spaced y-tick `<line>` + `<text>` pairs with monospace `font-size:9px`, signed-percentage labels, zero gridline emphasised at `rgba(255,255,255,0.20)` and label fill `#a1a1aa`
    - Add x-tick year labels every 5 calendar years anchored on January
    - Add `<line id="hp-crosshair" ... opacity="0">` element controlled by hover handler
    - Preserve existing area/line path output and "Not enough data" placeholder
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7_

  - [x] 3.3 Add `hpChartHover` method
    - In `docs/markets.html`, in the Alpine methods section near other hover handlers, add `hpChartHover(e)` method
    - Compute fractional x within the SVG using `getBoundingClientRect()`; derive `idx = round(frac * (rolls.length - 1))`
    - Set `this.hpHoverInfo = { date: rolls[idx].ym, val: rolls[idx].ret }`
    - Update `#hp-crosshair` element's `x1`/`x2` and set `opacity="1"`
    - Early-return on missing svg or empty rolls
    - _Requirements: 3.7, 3.8_

  - [x] 3.4 Wire Rolling chart template with mouse events and Hover_Readout_Bar
    - In `docs/markets.html`, locate the `<div x-html="hpChartSvg" ...>` block (~line 1267)
    - Add `@mousemove="hpChartHover($event)"` and `@mouseleave="hpHoverInfo = null"`
    - Append a sibling `<div x-show="hpHoverInfo" ...>` Hover_Readout_Bar matching existing yield/drawdown structure (border-top `var(--border)`, background `var(--surface-2)`, `text-xs num font-mono`)
    - Show `hpHoverInfo.date` (text-3) and signed-pct rolling return coloured by sign
    - _Requirements: 3.7, 3.8, 3.9_

  - [ ]* 3.5 Write property test for `hpChartSvg` axis chrome completeness
    - Create `tests/markets-yield-holdlab-polish/hp-chart-svg.test.mjs`
    - Lift `hpChartSvg` logic into a pure helper that takes a `rolls` array and returns the SVG string
    - **Property 3: hpChartSvg axis chrome completeness**
    - For random non-empty `rolls`, assert the output string contains exactly one y-axis line at `x1=PL,x2=PL`, exactly one x-axis line at `y1=H-PB,y2=H-PB`, five monospace y-tick `<text>` elements, one gridline per tick at the correct stroke (zero highlighted), and exactly one `id="hp-crosshair"` line
    - **Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7**

  - [ ]* 3.6 Write property test for `hpChartHover` index mapping
    - Create `tests/markets-yield-holdlab-polish/hp-chart-hover.test.mjs`
    - Lift index mapping into a pure helper `rollingHoverIndex(n, frac)` that returns `clamp(round(frac * (n-1)), 0, n-1)`
    - **Property 5: hpChartHover index mapping**
    - For random `n ≥ 1` and random `frac ∈ [0, 1]`, assert the helper equals `Math.max(0, Math.min(n-1, Math.round(frac * (n-1))))`
    - **Validates: Requirements 3.8**

- [x] 4. Polish Hold Lab Distribution chart with axis chrome and hover
  - [x] 4.1 Add `hpDistHoverInfo` reactive state field
    - In `docs/markets.html`, in the Alpine component data section, add `hpDistHoverInfo: null,` near `hpHoverInfo`
    - _Requirements: 4.6_

  - [x] 4.2 Polish `hpDistSvg` getter with axis lines, count y-ticks, signed-pct x-ticks, gridlines, bucket data attribute
    - In `docs/markets.html`, locate `hpDistSvg` getter (~line 4185)
    - Add y-axis line at `x=PL` and x-axis line at `y=H-PB`
    - Add 4 evenly spaced count y-ticks with monospace integer labels and gridlines at `rgba(255,255,255,0.04)`
    - Add x-tick labels at every fourth bucket midpoint formatted as signed percentages, monospace, `#52525b`
    - Add `data-bucket="${i}"` to each `<rect>` so the hover handler can resolve indices
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5_

  - [x] 4.3 Add `hpDistHover` method
    - In `docs/markets.html`, in the Alpine methods section, add `hpDistHover(e)` method
    - Resolve target via `e.target.closest('rect[data-bucket]')`; clear `hpDistHoverInfo` when null
    - Recompute `min`, `step`, `buckets` identically to `hpDistSvg`; set `this.hpDistHoverInfo = { lo, hi, count }`
    - _Requirements: 4.6_

  - [x] 4.4 Wire Distribution chart template with mouse events and Hover_Readout_Bar
    - In `docs/markets.html`, locate the `<div x-html="hpDistSvg" ...>` block (~line 1277)
    - Add `@mousemove="hpDistHover($event)"` and `@mouseleave="hpDistHoverInfo = null"`
    - Append a sibling `<div x-show="hpDistHoverInfo" ...>` Hover_Readout_Bar matching existing readout structure
    - Show bucket bounds `lo → hi` (signed pct, 1 decimal) and count formatted as `N window(s)`
    - _Requirements: 4.6, 4.7_

  - [ ]* 4.5 Write property test for `hpDistSvg` axis chrome completeness
    - Create `tests/markets-yield-holdlab-polish/hp-dist-svg.test.mjs`
    - Lift `hpDistSvg` logic into a pure helper taking `rolls` and returning the SVG string
    - **Property 4: hpDistSvg axis chrome completeness**
    - Assert exactly one y-axis line at `x=PL`, exactly one x-axis line at `y=H-PB`, four integer-count y-tick `<text>` elements, one gridline per y-tick at the correct stroke, and signed-pct x-tick labels at every fourth bucket midpoint
    - **Validates: Requirements 4.1, 4.2, 4.3, 4.4, 4.5**

  - [ ]* 4.6 Write property test for `hpDistHover` bucket mapping
    - Create `tests/markets-yield-holdlab-polish/hp-dist-hover.test.mjs`
    - Lift bucketisation into a pure helper `bucketize(rets, nB)` returning `{ buckets, min, step }`
    - **Property 6: hpDistHover bucket mapping**
    - For random non-empty `rets` and any `i ∈ [0, nB)`, assert that `lo = min + i*step`, `hi = min + (i+1)*step`, and `count = buckets[i]` match the hover output exactly
    - **Validates: Requirements 4.6**

- [x] 5. Drawdown and Correlation Growth axis-line consistency pass
  - [x] 5.1 Add explicit axis lines to `drawdownChartSvg`
    - In `docs/markets.html`, locate `drawdownChartSvg` getter (~line 3849)
    - Before the final `return` template literal, define `yAxis` and `xAxis` `<line>` strings (`stroke="#52525b"` opacity `0.6`)
    - Inject both into the returned SVG string after `${yTicks}${xLabels}${areas}` and before `${crosshair}`
    - Preserve all existing tick label, gridline, underwater curve, and crosshair output
    - _Requirements: 5.1, 5.2, 5.3, 5.7_

  - [x] 5.2 Add explicit axis lines to `growthChartSvg`
    - In `docs/markets.html`, locate `growthChartSvg` getter (~line 4270)
    - Before the final `return` template literal, define `yAxis` and `xAxis` `<line>` strings (`stroke="#52525b"` opacity `0.6`)
    - Inject both into the returned SVG string after `${ticks}${xLabels}${lines}` and before `${ch}`
    - Preserve dollar tick labels, year tick labels, log-scale curves, and existing hover behaviour
    - _Requirements: 5.4, 5.5, 5.6, 5.7_

  - [ ]* 5.3 Write property test for explicit axis lines in Drawdown_Chart and Correlation_Growth_Chart
    - Create `tests/markets-yield-holdlab-polish/dd-growth-axes.test.mjs`
    - Lift the SVG-emission logic into pure helpers that accept a series array and return the SVG string
    - **Property 7: Drawdown and Growth charts include explicit axis lines**
    - For random non-empty input, assert each output contains exactly one `<line>` with `x1=PL,x2=PL` (y-axis) and exactly one `<line>` with `y1=H-PB,y2=H-PB` (x-axis at bottom)
    - **Validates: Requirements 5.1, 5.4**

- [x] 6. Final checkpoint
  - Ensure all tests pass, ask the user if questions arise.
  - Manual smoke check at common viewport widths (320px, 768px, 1024px, 1280px, 1600px): Yield Annual Averages headers show real years and cells render percentages; Hold Lab KPI strip steps through 2 → 4 → 6 → 7 columns; Rolling and Distribution charts show axis lines, ticks, gridlines, and hover readouts; Drawdown and Growth charts show new axis lines without regressing existing hover behaviour.

## Notes

- All file edits are scoped to `docs/markets.html`; no new runtime dependencies enter the page (Requirement 6.1, 6.2, 6.3, 6.4).
- Tasks marked with `*` are optional property tests and can be skipped for a faster MVP.
- Each property test is a standalone Node `*.test.mjs` script under `tests/markets-yield-holdlab-polish/`, using hand-rolled generators rather than introducing a new test runner.
- Section ordering minimises edit conflicts in the single file: yield (lines ~3468–3651) → KPI grid (line ~1257) → rolling chart (~1267 + ~4150) → distribution chart (~1277 + ~4185) → drawdown/growth (~3935, ~4308).
- Because every implementation sub-task edits `docs/markets.html`, each is placed in its own wave; property tests live in separate files and run in parallel within a single trailing wave.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1"] },
    { "id": 1, "tasks": ["1.2"] },
    { "id": 2, "tasks": ["1.3"] },
    { "id": 3, "tasks": ["1.4"] },
    { "id": 4, "tasks": ["1.5"] },
    { "id": 5, "tasks": ["2.1"] },
    { "id": 6, "tasks": ["3.1"] },
    { "id": 7, "tasks": ["3.2"] },
    { "id": 8, "tasks": ["3.3"] },
    { "id": 9, "tasks": ["3.4"] },
    { "id": 10, "tasks": ["4.1"] },
    { "id": 11, "tasks": ["4.2"] },
    { "id": 12, "tasks": ["4.3"] },
    { "id": 13, "tasks": ["4.4"] },
    { "id": 14, "tasks": ["5.1"] },
    { "id": 15, "tasks": ["5.2"] },
    { "id": 16, "tasks": ["1.6", "1.7", "3.5", "3.6", "4.5", "4.6", "5.3"] }
  ]
}
```
