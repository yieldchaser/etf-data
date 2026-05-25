# Implementation Plan: Markets Asset Detail Polish

## Overview

Fix and polish the asset detail panel in `docs/markets.html`. The implementation is ordered to minimize conflicts in this single-file SPA: first fix data correctness (heatmap year bug), then layer on UI polish, then add navigation features, then replace the slider component, and finally polish the multi-currency table.

All changes target `docs/markets.html` (~4883 lines), a single-file SPA using Alpine.js and Tailwind CSS.

## Tasks

- [x] 1. Fix heatmap year iteration bug
  - [x] 1.1 Fix the `assetMonthlyHeatmapHtml` getter to render all historical years
    - Modify the getter (around line 2886) to ensure the year extraction from `adjusted` array includes every distinct year present in the Monthly_Close_Array
    - Ensure monthly returns are computed from consecutive pairs in the sorted adjusted array, correctly handling cross-year boundaries (Dec→Jan)
    - Verify that years with only one data point (no computable return) still render as a row with empty cells
    - Ensure year rows are sorted in descending order (most recent first)
    - _Requirements: 1.1, 1.2, 1.3, 1.4_

  - [ ]* 1.2 Write property tests for year completeness and return calculation
    - **Property 1: Year completeness** — for any monthly close array with N distinct years, the heatmap produces exactly N year rows
    - **Property 2: Monthly return calculation correctness** — for consecutive entries (ym_prev, close_prev) and (ym_curr, close_curr) where close_prev > 0, return equals close_curr / close_prev - 1
    - **Property 3: Descending year sort invariant** — year sequence is strictly descending
    - **Validates: Requirements 1.1, 1.2, 1.3, 1.4**

- [x] 2. Add heatmap UI polish
  - [x] 2.1 Enhance heatmap styling with cell borders, row striping, sticky headers, and annual total column
    - Apply `border: 1px solid rgba(255,255,255,0.04)` to each data cell for visual separation
    - Add alternating row backgrounds (`background: rgba(255,255,255,0.02)` on even-indexed rows)
    - Make the header row (month labels) sticky with `position:sticky; top:0; z-index:3; background:var(--bg)`
    - Make the year column sticky with `position:sticky; left:0; z-index:2; background:var(--bg)`
    - Ensure cell content is vertically and horizontally centered with consistent padding
    - Add an annual total column at the end of each row: compound all monthly returns for that year `(∏(1+r_i) - 1)`, render with same `cellColor()` scale, bold font-weight
    - Add "Total" header label in the sticky header row
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7_

  - [ ]* 2.2 Write property test for annual total calculation
    - **Property 5: Annual total equals compound of monthly returns** — for any year row, the annual total equals product of (1 + monthly_return) for all months with data, minus 1
    - **Validates: Requirements 3.7**

- [x] 3. Checkpoint - Verify heatmap correctness and styling
  - Ensure all tests pass, ask the user if questions arise.

- [x] 4. Add decade navigation for heatmap
  - [x] 4.1 Implement decade navigator bar and scroll-to-decade functionality
    - Compute decades from the years array: `[...new Set(years.map(y => Math.floor(y/10)*10))].sort((a,b) => b-a)`
    - Render a decade nav bar above the heatmap table only when `years.length > 30`
    - Each button shows the decade label (e.g., "1990s") and has a `data-decade` attribute
    - On click, scroll the heatmap container so the first row of that decade is visible at the top
    - Add `id="heatmap-yr-${yr}"` to each year row `<tr>` for scroll targeting
    - Wrap the heatmap table in a container with class `heatmap-scroll-container` for observer targeting
    - Add `_scrollToDecade(decade)` method to the Alpine component
    - _Requirements: 2.1, 2.2, 2.3, 2.5_

  - [x] 4.2 Implement IntersectionObserver for active decade highlighting
    - Add `_initDecadeObserver()` method that sets up an IntersectionObserver on year rows
    - When a decade boundary row enters the viewport top area, highlight the corresponding decade button with `tab-active` class
    - Call `_initDecadeObserver()` via `$nextTick` after heatmap HTML is rendered
    - Disconnect previous observer when asset changes or heatmap re-renders
    - _Requirements: 2.4_

  - [ ]* 4.3 Write property test for decade button completeness
    - **Property 4: Decade button completeness** — for any set of years, the navigator renders exactly one button per distinct decade containing at least one year
    - **Validates: Requirements 2.2**

- [x] 5. Replace price range slider with custom dual-handle component
  - [x] 5.1 Implement custom dual-handle slider using pointer events
    - Replace the two overlapping `<input type="range">` elements (around line 572-583) with a custom slider component
    - Render: track background (full-width, 4px height, rounded, `var(--border-2)` color), active segment (cyan, positioned between handles), two circular handle elements (16px, cyan border, dark fill)
    - Add `_sliderDragging: null` state property to the Alpine component
    - Implement `_sliderPointerDown(e)`: determine closest handle, set `_sliderDragging`, call `setPointerCapture`
    - Implement `_sliderPointerMove(e)`: compute index from pointer position, update `trcStartIdx` or `trcEndIdx` with crossing prevention, call `_trcSyncWindow()`
    - Implement `_sliderPointerUp(e)`: clear `_sliderDragging`
    - Add computed getters `_sliderStartPct` and `_sliderEndPct` for handle positioning
    - Set `touch-action: none` on the container for touch support
    - Bind `@pointerdown`, `@pointermove`, `@pointerup`, `@pointercancel` events
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8_

  - [ ]* 5.2 Write property test for slider handle ordering invariant
    - **Property 6: Slider handle ordering invariant** — for any sequence of drag operations, `trcStartIdx < trcEndIdx` always holds
    - **Validates: Requirements 4.4, 4.5**

- [x] 6. Polish multi-currency table
  - [x] 6.1 Enhance `assetMultiCcyTableHtml` with institutional-grade styling and CAGR row
    - Make column headers sticky: `position:sticky; top:0; z-index:3; background:var(--bg)`
    - Add alternating row backgrounds on even-indexed rows
    - Apply color tint to return cells: `background: ${cellColor(ret)}20` (hex alpha ~12% opacity)
    - Set consistent cell padding (`4px 10px`), right-aligned numeric values, `font-variant-numeric: tabular-nums`
    - Add subtle column borders: `border-left: 1px solid var(--border)` on non-first data cells
    - Make year column sticky on the left
    - Add a CAGR summary row after `<thead>`: compute `(compound)^(1/N) - 1` for each lens across full history
    - Style CAGR row distinctly (e.g., slightly different background, bold values)
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7_

  - [ ]* 6.2 Write property tests for multi-ccy table
    - **Property 8: Multi-CCY color scale consistency** — for any annual return value, the cell tint is derived from `cellColor(r)`
    - **Property 9: CAGR calculation correctness** — CAGR equals `(compound)^(1/N) - 1` where N = year_last - year_first
    - **Validates: Requirements 5.3, 5.7**

- [x] 7. Final checkpoint - Ensure all changes work together
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- All changes are in a single file (`docs/markets.html`) — tasks are ordered to modify different sections and minimize merge conflicts
- The existing `cellColor(ret)` and `cellTextColor(ret)` functions are reused across heatmap, multi-ccy table, and return matrix
- The heatmap getter starts at ~line 2886, the multi-ccy getter at ~line 3028, and the slider HTML at ~line 568
- Pointer Events API provides unified mouse/touch handling without separate event listeners
- Each task references specific requirements for traceability
- Property tests validate universal correctness properties from the design document

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1"] },
    { "id": 1, "tasks": ["1.2", "2.1"] },
    { "id": 2, "tasks": ["2.2", "4.1", "5.1"] },
    { "id": 3, "tasks": ["4.2", "4.3", "5.2", "6.1"] },
    { "id": 4, "tasks": ["6.2"] }
  ]
}
```
