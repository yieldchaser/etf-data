# Requirements Document

## Introduction

Fix and polish the Markets page asset detail panel in the Predator Protocol dashboard. This covers four areas: correcting the returns heatmap to render all historical years (including pre-1900 data), adding decade navigation; polishing the heatmap and multi-currency table UI to institutional-grade quality; replacing the buggy dual native range slider with a custom Alpine.js dual-handle slider; and improving the multi-currency comparison table styling.

All changes are scoped to `docs/markets.html`, a single-file SPA using Alpine.js and Tailwind CSS with a dark theme (orange/cyan accents). The target aesthetic is a professional financial terminal.

## Glossary

- **Heatmap_Generator**: The `get assetMonthlyHeatmapHtml()` computed getter that produces the year × month HTML table of monthly returns for the selected asset.
- **Decade_Navigator**: A set of jump buttons rendered above the heatmap that allow the user to scroll to a specific decade within the heatmap table.
- **Dual_Handle_Slider**: The custom Alpine.js component that replaces the two overlapping native `<input type="range">` elements for selecting a time window on the price chart.
- **Multi_CCY_Table**: The `get assetMultiCcyTableHtml()` computed getter that produces the year × currency comparison table showing returns across Local, USD, INR, Gold, and Real lenses.
- **Asset_Detail_Panel**: The expandable panel below the return matrix that shows charts, heatmap, and multi-currency data for the selected asset.
- **Return_Matrix**: The main annual return matrix table displayed in the Return Matrix tab.
- **Monthly_Close_Array**: The `monthly` field in `market_returns.json` containing `[ym, close]` tuples for each asset, where `ym` is a `YYYY-MM` string.

## Requirements

### Requirement 1: Heatmap renders all historical years

**User Story:** As an analyst, I want the monthly returns heatmap to display every year of available data for an asset, so that I can examine the full historical record including pre-1900 data.

#### Acceptance Criteria

1. WHEN an asset with historical data spanning multiple centuries is selected, THE Heatmap_Generator SHALL produce a row for every year present in the Monthly_Close_Array without omitting any year.
2. WHEN the Heatmap_Generator computes monthly returns, THE Heatmap_Generator SHALL correctly pair consecutive months across year boundaries to calculate the return for each month.
3. WHEN a year contains at least one month with a computable return, THE Heatmap_Generator SHALL render that year's row with the available monthly cells populated and remaining cells displayed as empty.
4. THE Heatmap_Generator SHALL sort year rows in descending order from the most recent year to the earliest year.

### Requirement 2: Decade navigation for heatmap

**User Story:** As an analyst viewing an asset with 190+ years of data, I want decade jump buttons so that I can quickly navigate to any decade without manually scrolling through hundreds of rows.

#### Acceptance Criteria

1. WHEN the heatmap contains more than 30 year rows, THE Asset_Detail_Panel SHALL display a Decade_Navigator bar above the heatmap table.
2. THE Decade_Navigator SHALL render one button for each decade that contains at least one year of data for the selected asset.
3. WHEN the user clicks a decade button, THE Decade_Navigator SHALL scroll the heatmap container so that the first row of the selected decade is visible at the top of the viewport.
4. WHILE the user scrolls the heatmap, THE Decade_Navigator SHALL visually highlight the button corresponding to the decade currently visible at the top of the scrollable area.
5. WHEN the heatmap contains 30 or fewer year rows, THE Asset_Detail_Panel SHALL hide the Decade_Navigator bar.

### Requirement 3: Heatmap UI polish

**User Story:** As a user, I want the monthly returns heatmap to look professional and institutional-grade, so that the dashboard conveys credibility and is easy to read.

#### Acceptance Criteria

1. THE Heatmap_Generator SHALL render each data cell with a background color derived from the return magnitude using the same color scale as the Return_Matrix.
2. THE Heatmap_Generator SHALL apply a subtle border to each cell to create visual separation between adjacent cells.
3. THE Heatmap_Generator SHALL apply alternating row backgrounds (row striping) at every other year row to improve scanability.
4. THE Heatmap_Generator SHALL render the header row (month labels) as sticky so that month labels remain visible while scrolling vertically.
5. THE Heatmap_Generator SHALL render the year column as sticky so that year labels remain visible while scrolling horizontally.
6. THE Heatmap_Generator SHALL apply consistent padding and spacing so that cell content is vertically and horizontally centered.
7. THE Heatmap_Generator SHALL render an annual total column at the end of each row showing the full-year return for that year.

### Requirement 4: Custom dual-handle price range slider

**User Story:** As a user, I want a clear, non-overlapping dual-handle slider for selecting the price chart time range, so that I can intuitively drag start and end handles without confusion.

#### Acceptance Criteria

1. THE Dual_Handle_Slider SHALL replace the two overlapping native `<input type="range">` elements with a single custom component built using Alpine.js.
2. THE Dual_Handle_Slider SHALL render a visible track bar showing the full range, with a highlighted segment between the two handles indicating the selected window.
3. THE Dual_Handle_Slider SHALL render two distinct circular handle elements that the user can drag independently along the track.
4. WHEN the user drags the start handle, THE Dual_Handle_Slider SHALL update `trcStartIdx` and prevent the start handle from crossing past the end handle.
5. WHEN the user drags the end handle, THE Dual_Handle_Slider SHALL update `trcEndIdx` and prevent the end handle from crossing before the start handle.
6. WHEN either handle is dragged, THE Dual_Handle_Slider SHALL update the time window display and price chart in real time.
7. THE Dual_Handle_Slider SHALL use the cyan accent color (`var(--cyan)`) for the active track segment and handle elements, consistent with the dashboard theme.
8. THE Dual_Handle_Slider SHALL support mouse drag interaction on desktop and touch drag interaction on touch devices.

### Requirement 5: Multi-currency table UI polish

**User Story:** As a user, I want the multi-currency comparison table to look polished and consistent with the Return Matrix styling, so that the dashboard has a cohesive professional appearance.

#### Acceptance Criteria

1. THE Multi_CCY_Table SHALL render column headers with a sticky top position so that lens labels remain visible while scrolling vertically.
2. THE Multi_CCY_Table SHALL apply alternating row backgrounds (row striping) at every other year row to improve scanability.
3. THE Multi_CCY_Table SHALL render return values with a background tint derived from the return magnitude, matching the color scale used in the Return_Matrix.
4. THE Multi_CCY_Table SHALL apply consistent cell padding, right-aligned numeric values, and tabular-nums font-variant for proper digit alignment.
5. THE Multi_CCY_Table SHALL render a subtle border between columns to visually separate the different currency lenses.
6. THE Multi_CCY_Table SHALL render the year column as sticky on the left so that year labels remain visible while scrolling horizontally.
7. THE Multi_CCY_Table SHALL display a summary row at the top showing the CAGR (compound annual growth rate) for each lens across the full available history.
