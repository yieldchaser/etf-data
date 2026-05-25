# Requirements Document

## Introduction

Fix and polish the Yield History tab and Hold Lab tab in `docs/markets.html`, then apply a consistency pass on the Drawdown chart and Correlation Growth chart so all four charts share the same institutional-grade axis, gridline, and crosshair treatment.

The Yield History "Annual Averages" table is currently broken because the renderer treats `rates[*].values` as an object map of `{YYYY-MM: value}` when in reality the pipeline emits a sorted array of `[YYYY-MM, value]` tuples. As a result, the year header row contains the string indices `"0", "1", ..., "25"` and every data cell renders blank. The fix is to iterate the tuple array, derive years from the first element of each tuple, and look up December (year-end) values via tuple scan.

The Hold Lab KPI strip currently uses `grid-cols-2 sm:grid-cols-5`, which forces the 6th and 7th KPI tiles onto a second row even on wide displays. The fix is a responsive ladder ending at `xl:grid-cols-7` so that all seven KPIs sit on a single row at ≥1280px viewport width, with the optional 8th "Never Lost" tile wrapping cleanly.

The Hold Lab Rolling chart and Return Distribution histogram lack the axis lines, formatted tick labels, gridlines, and hover crosshair that the existing Yield History and Drawdown charts already use. This polish pass adds those treatments and brings the Drawdown chart and Correlation Growth chart in line with the same patterns where they differ.

All changes are scoped to `docs/markets.html` only. The page is a single-file SPA built with Alpine.js 3.x and Tailwind CSS, with charts rendered as inline SVG strings returned from Alpine getters. No new runtime dependencies. Color tokens come from existing CSS custom properties: `--cyan`, `--up`, `--down`, `--amber`, `--text-3`. Tick labels use `font-family: ui-monospace` at 9–10px.

## Glossary

- **Yield_Annual_Table**: The "Annual Averages" table rendered inside the Yield History tab, listing one row per rate series and one column per calendar year.
- **Yield_Year_List**: The computed array of calendar years used as column headers in the Yield_Annual_Table, exposed as the `yieldYears` Alpine getter.
- **Yield_Cell_Lookup**: The function `getYieldValue(seriesKey, year)` that returns the year-end (December) yield value for a series and year, falling back to the last available month within that year when December is missing.
- **Rate_Values_Tuples**: The serialised representation of a rate series in `market_returns.json`, where each series stores its monthly values as a sorted array of `[YYYY-MM, value]` tuples under the key `values` (per `predator/markets_history.py`).
- **HoldLab_KPI_Strip**: The horizontal strip of summary tiles at the top of the Hold Lab tab, rendered from the `hpKpis` Alpine getter, containing 7 standard KPIs plus an optional 8th "Never Lost" badge.
- **HoldLab_Rolling_Chart**: The line/area chart rendered from the `hpChartSvg` Alpine getter showing rolling N-year annualised returns over time for the selected asset.
- **HoldLab_Distribution_Chart**: The histogram rendered from the `hpDistSvg` Alpine getter showing the frequency distribution of rolling N-year returns.
- **Drawdown_Chart**: The underwater curve chart rendered from the `drawdownChartSvg` Alpine getter, with hover handler `drawdownHover`.
- **Correlation_Growth_Chart**: The "Growth of $100 (log scale)" chart rendered from the `growthChartSvg` Alpine getter, with hover handler `growthHover`.
- **Hover_Readout_Bar**: The horizontal bar of formatted values rendered below a chart when the user hovers, matching the existing pattern used by `yieldHoverInfo`, `ddHoverInfo`, and `growthHoverInfo`.
- **Crosshair_Line**: A vertical SVG `<line>` element inside a chart SVG that follows the mouse x-position and indicates the currently hovered data point.

## Requirements

### Requirement 1: Yield Annual Averages reads tuple array correctly

**User Story:** As an analyst viewing the Yield History tab, I want the Annual Averages table to show real year columns and yield values, so that I can compare year-end interest rate levels across decades.

#### Acceptance Criteria

1. THE Yield_Year_List SHALL be computed by iterating the Rate_Values_Tuples array of each rate series, extracting the year from the first four characters of each tuple's `YYYY-MM` string, and returning a deduplicated ascending list of integer years.
2. WHEN a rate series stores its `values` as an array of `[YYYY-MM, value]` tuples, THE Yield_Year_List SHALL include every calendar year for which at least one tuple exists across the union of all rate series.
3. WHEN the Yield_Cell_Lookup is invoked for a series key and a year, THE Yield_Cell_Lookup SHALL search the Rate_Values_Tuples array of that series for a tuple whose first element equals `${year}-12` and SHALL return the second element of that tuple when found.
4. IF no December tuple exists for the requested year, THEN THE Yield_Cell_Lookup SHALL return the value of the latest tuple within that year, where "latest" is determined by lexicographic comparison of the tuple's `YYYY-MM` string.
5. IF no tuple exists for the requested year in the requested series, THEN THE Yield_Cell_Lookup SHALL return `null`.
6. WHEN the Yield_Annual_Table renders its header row, THE Yield_Annual_Table SHALL display each integer year from the Yield_Year_List as a column header in ascending order from left to right.
7. WHEN the Yield_Annual_Table renders a data row for a series, THE Yield_Annual_Table SHALL display the value returned by the Yield_Cell_Lookup for that series and year in each year column, formatted as a percentage with one decimal place, or render an empty cell when the lookup returns `null`.

### Requirement 2: Hold Lab KPI strip fits seven tiles on one row at ≥1280px

**User Story:** As a user viewing the Hold Lab tab on a wide display, I want all seven core KPI tiles to sit on a single row, so that I can scan the rolling-return summary without vertical scrolling.

#### Acceptance Criteria

1. WHILE the viewport width is at least 1280 pixels, THE HoldLab_KPI_Strip SHALL arrange the seven core KPI tiles (Best Window, 75th %ile, Median, 25th %ile, Worst Window, % Positive, Windows) in a single row of seven equal-width columns.
2. WHILE the viewport width is between 1024 pixels and 1279 pixels inclusive, THE HoldLab_KPI_Strip SHALL arrange the KPI tiles in a grid of at most six columns per row.
3. WHILE the viewport width is between 640 pixels and 1023 pixels inclusive, THE HoldLab_KPI_Strip SHALL arrange the KPI tiles in a grid of at most four columns per row.
4. WHILE the viewport width is below 640 pixels, THE HoldLab_KPI_Strip SHALL arrange the KPI tiles in a grid of two columns per row.
5. WHEN the optional eighth "Never Lost" KPI tile is present, THE HoldLab_KPI_Strip SHALL wrap the eighth tile to a new row at viewport widths below 1280 pixels and SHALL allow the eighth tile to occupy the next available cell at viewport widths of 1280 pixels or wider without breaking the seven-column alignment of the first row.
6. THE HoldLab_KPI_Strip SHALL preserve the existing tile styling (background `var(--surface)`, border `var(--border)`, label/value typography) without visual regression.

### Requirement 3: HoldLab_Rolling_Chart institutional axis and crosshair polish

**User Story:** As an analyst inspecting rolling returns, I want a labelled y-axis with percentage ticks, a labelled x-axis with year ticks, faint gridlines, and a hover crosshair with a date+value readout, so that I can read exact values off the chart and compare it visually with the other charts on the page.

#### Acceptance Criteria

1. THE HoldLab_Rolling_Chart SHALL render a visible y-axis line at the left edge of the plot area, drawn in colour `var(--text-3)` at the standard chart axis opacity used elsewhere on the page.
2. THE HoldLab_Rolling_Chart SHALL render y-axis tick labels at five evenly spaced values across the visible return range, with each label formatted as a signed percentage with no decimal places (e.g. `+10%`, `0%`, `-5%`) using `font-family: ui-monospace` at 9–10px and fill `var(--text-3)`.
3. THE HoldLab_Rolling_Chart SHALL render the y-axis tick label and gridline at the zero-return value with higher visual emphasis than non-zero ticks, using fill `#a1a1aa` for the label and stroke `rgba(255,255,255,0.20)` for the gridline.
4. THE HoldLab_Rolling_Chart SHALL render a visible x-axis line at the bottom edge of the plot area, drawn in colour `var(--text-3)` at the standard chart axis opacity used elsewhere on the page.
5. THE HoldLab_Rolling_Chart SHALL render x-axis tick labels approximately every five calendar years across the visible date range, with each label formatted as a four-digit year using `font-family: ui-monospace` at 9–10px and fill `var(--text-3)`.
6. THE HoldLab_Rolling_Chart SHALL render faint horizontal gridlines aligned with each y-axis tick at stroke `rgba(255,255,255,0.04)` and stroke-width 1.
7. THE HoldLab_Rolling_Chart SHALL render a vertical Crosshair_Line that follows the mouse x-position while the cursor is over the chart and is hidden when the cursor leaves the chart.
8. WHEN the user hovers the HoldLab_Rolling_Chart, THE HoldLab_Rolling_Chart SHALL display a Hover_Readout_Bar below the SVG showing the hovered date in `YYYY-MM` format and the rolling return at that date formatted as a signed percentage with one decimal place.
9. THE Hover_Readout_Bar for the HoldLab_Rolling_Chart SHALL match the visual structure of the existing yield and drawdown hover bars (border-top `var(--border)`, background `var(--surface-2)`, monospace numeric values).

### Requirement 4: HoldLab_Distribution_Chart institutional axis and hover polish

**User Story:** As an analyst inspecting the rolling-return distribution, I want labelled axes, gridlines, and a hover readout showing the bucket range and count, so that I can read frequencies precisely instead of estimating bar heights.

#### Acceptance Criteria

1. THE HoldLab_Distribution_Chart SHALL render a visible y-axis line at the left edge of the plot area, drawn in colour `var(--text-3)` at the standard chart axis opacity used elsewhere on the page.
2. THE HoldLab_Distribution_Chart SHALL render y-axis tick labels showing integer frequency counts at four evenly spaced values from zero up to the maximum bucket count, using `font-family: ui-monospace` at 9–10px and fill `var(--text-3)`.
3. THE HoldLab_Distribution_Chart SHALL render a visible x-axis line at the bottom edge of the plot area, drawn in colour `var(--text-3)` at the standard chart axis opacity used elsewhere on the page.
4. THE HoldLab_Distribution_Chart SHALL render x-axis tick labels at the midpoints of evenly spaced buckets, with each label formatted as a signed percentage with no decimal places, using `font-family: ui-monospace` at 9–10px and fill `var(--text-3)`.
5. THE HoldLab_Distribution_Chart SHALL render faint horizontal gridlines aligned with each y-axis tick at stroke `rgba(255,255,255,0.04)` and stroke-width 1.
6. WHEN the user hovers a bar in the HoldLab_Distribution_Chart, THE HoldLab_Distribution_Chart SHALL display a Hover_Readout_Bar below the SVG showing the bucket's lower and upper bounds formatted as signed percentages with one decimal place and the integer count of windows in that bucket.
7. THE Hover_Readout_Bar for the HoldLab_Distribution_Chart SHALL match the visual structure of the existing yield and drawdown hover bars (border-top `var(--border)`, background `var(--surface-2)`, monospace numeric values).

### Requirement 5: Drawdown_Chart and Correlation_Growth_Chart consistency pass

**User Story:** As a user viewing the Markets page, I want the Drawdown chart and Correlation Growth chart to share the same axis lines, gridline density, tick label styling, and crosshair-readout pattern as the polished Yield and Hold Lab charts, so that all four charts feel cohesive.

#### Acceptance Criteria

1. THE Drawdown_Chart SHALL render a visible y-axis line at the left edge of the plot area and a visible x-axis line at the bottom edge of the plot area, drawn in colour `var(--text-3)` at the standard chart axis opacity used elsewhere on the page.
2. THE Drawdown_Chart SHALL render y-axis tick labels and x-axis tick labels using `font-family: ui-monospace` at 9–10px and fill `var(--text-3)`, with the zero-percent gridline visually emphasised at stroke `rgba(255,255,255,0.20)` and label fill `#a1a1aa`.
3. THE Drawdown_Chart SHALL preserve its existing underwater curves, area fills, asset selector, max-drawdown table, and hover behaviour without functional regression.
4. THE Correlation_Growth_Chart SHALL render a visible y-axis line at the left edge of the plot area and a visible x-axis line at the bottom edge of the plot area, drawn in colour `var(--text-3)` at the standard chart axis opacity used elsewhere on the page.
5. THE Correlation_Growth_Chart SHALL render y-axis dollar tick labels and x-axis year tick labels using `font-family: ui-monospace` at 9–10px and fill `var(--text-3)`, with horizontal gridlines aligned to each y-axis tick at stroke `rgba(255,255,255,0.04)`.
6. THE Correlation_Growth_Chart SHALL preserve its existing log-scale growth curves, asset selector, and hover behaviour without functional regression.
7. WHERE a chart already provides a Hover_Readout_Bar, THE chart SHALL retain the existing readout content and SHALL NOT introduce structural changes that break the existing hover state bindings (`yieldHoverInfo`, `ddHoverInfo`, `growthHoverInfo`).

### Requirement 6: Single-file scope and dependency constraints

**User Story:** As a maintainer, I want all changes confined to `docs/markets.html` and free of new runtime dependencies, so that the static-site deployment pipeline remains unchanged.

#### Acceptance Criteria

1. THE implementation SHALL modify only the file `docs/markets.html` within the repository.
2. THE implementation SHALL NOT add any new `<script>` tag, CDN reference, or npm package to the page.
3. THE implementation SHALL render all chart visuals using inline SVG strings returned from Alpine.js getters, consistent with the existing chart implementations in the file.
4. THE implementation SHALL reuse the existing CSS custom properties `--cyan`, `--up`, `--down`, `--amber`, and `--text-3` for colour values and SHALL NOT introduce hard-coded hex values that duplicate these tokens.
