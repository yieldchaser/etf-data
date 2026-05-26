# Bugfix Requirements Document

## Introduction

Two coordinated defects in the markets pipeline (predator/markets_history.py, predator/ingest_mega_xl.py, predator/vol_history.py) and the Markets dashboard (docs/markets.html) are corrupting the integrity of the Return Matrix and the live-data verdict. Both ship in the same pass and are treated here as one fix.

**Bug 1 — Partial-year returns rendering as catastrophic fake losses.** The 2026 column of the Return Matrix shows impossible saturated values for the agriculture rows (Wheat −96%, Corn −98%, Sugar −99%, Soybeans −97%, Cotton −99%, Coffee −99%). The tooltip confirms the cause: "⚠ Partial year (not all months available)". Partial-year handling is inconsistent across asset classes — metals partial cells (Copper +12%, Aluminum +17%) render correctly with a dim/grey honest treatment, while equity and agriculture partial cells render at full saturation, and the ag math additionally produces broken near-total-loss numbers. A saturated −99% that is actually a partial-year artifact reads as a real crash and is worse than showing nothing. This is the same defect class as the earlier DAX −96% incident.

**Bug 2 — Live-merge incomplete on dual-registry equity names.** The live merge is partially working: Dow Jones now correctly shows `yfinance:^DJI`, Aluminum shows `fred:PALUMUSDM`, and the header verdict honestly reports "LIVE MERGE DEGRADED." However, S&P 500 still shows `Source: Mega_Markets_Historical.xlsx:Equities` despite Dow Jones (same registry) having flipped to the live source, and the same defect likely affects NASDAQ and NASDAQ-100. The verdict cannot reach LIVE_MERGE_HEALTHY while these holdouts persist.

Scope is strictly the markets pipeline and `docs/markets.html`. `scraper.py` and all scoring code are out of scope.

## Bug Analysis

### Current Behavior (Defect)

The Return Matrix and the live-merge verdict in the markets dashboard exhibit the following observed defects on the live site:

1.1 WHEN a year column (e.g. 2026) is missing months for an agriculture series (Wheat, Corn, Sugar, Soybeans, Cotton, Coffee) THEN the system renders a saturated full-year-style return cell with a near-total-loss value (−96% to −99%) that is mathematically broken and not a real return.

1.2 WHEN a year column is missing months for an equity series THEN the system renders a saturated full-strength full-year-style return cell rather than a dimmed/greyed honest partial-year treatment.

1.3 WHEN a year column is missing months for a metals series THEN the system renders a dimmed/greyed cell with a partial-year flag, demonstrating that the "honest" rendering path exists but is not applied uniformly.

1.4 WHEN a year column is missing months for any series THEN the partial-year tooltip flag ("⚠ Partial year (not all months available)") is attached, but the visual treatment and the underlying numeric computation differ by asset class, producing inconsistent and in some cases mathematically invalid output.

1.5 WHEN a single-year return cell carries an extreme magnitude (beyond roughly ±70%) that is not a known real market event THEN the system renders the value blind without validating it against the source data, allowing partial-year and unit/scale artifacts to surface as apparent catastrophic moves.

1.6 WHEN the live merge runs on the S&P 500 entry THEN the Source field in the tooltip remains `Mega_Markets_Historical.xlsx:Equities` rather than a live source (yfinance/fred), even though Dow Jones in the same registry successfully flipped to `yfinance:^DJI`.

1.7 WHEN the live merge runs on NASDAQ and NASDAQ-100 entries (the other dual-registry equity names) THEN the Source field is expected to remain on the Excel label for the same root cause as 1.6.

1.8 WHEN one or more equity symbols fail to flip to a live source THEN the header verdict reports "LIVE MERGE DEGRADED" without identifying which specific asset is the holdout, leaving the operator unable to tell from the verdict alone whether the degradation is benign (a known un-fetchable symbol) or a regression.

### Expected Behavior (Correct)

The fix applies one consistent partial-year rule across every asset class and brings the dual-registry equity names onto live sources where possible:

2.1 WHEN a year column is missing months for an agriculture series THEN the system SHALL render that cell using the same honest partial-year treatment as metals (dimmed/greyed visual style, partial-year tooltip flag, and a numerically correct partial-to-date return) OR suppress the cell, and SHALL NOT emit a saturated near-total-loss number.

2.2 WHEN a year column is missing months for an equity series THEN the system SHALL render that cell using the same honest partial-year treatment as metals (dimmed/greyed visual style, partial-year tooltip flag, and a numerically correct partial-to-date return) OR suppress the cell, and SHALL NOT emit a saturated full-strength full-year-style number.

2.3 WHEN a year column is missing months for any asset class (equity, metals, agriculture, energy) THEN the system SHALL apply one single partial-year rule uniformly: either (a) render dim/grey with the partial-year flag and a correct partial-to-date return, or (b) suppress the cell — and SHALL NOT vary the rule by asset class.

2.4 WHEN computing the partial-to-date return for any series THEN the system SHALL handle missing or zero base months, missing current months, NaN propagation, and unit/scale mismatches in a way that never produces a saturated artifact value, and SHALL identify and document which of these candidate root causes was responsible for the agriculture −96%/−99% artifact (missing/zero base divisor, Dec-of-prior versus non-existent current month, FRED ag series unit/scale mismatch, or NaN propagation through annualization).

2.5 WHEN a single-year return cell would render with a magnitude beyond roughly ±70% AND the value is not a known real market event THEN the system SHALL validate the value against its source data before rendering, and SHALL either render the validated value or suppress/flag it rather than emit it blind.

2.6 WHEN the live merge runs on S&P 500 THEN the system SHALL either flip the Source field to a live source (yfinance/fred) OR honestly retain the Excel label and report S&P 500 by name as a known holdout in the verdict, with the diagnosis (e.g. yfinance symbol mismatch, fetch failure) documented.

2.7 WHEN the live merge runs on NASDAQ and NASDAQ-100 THEN the system SHALL either flip the Source field to a live source OR honestly retain the Excel label and report each affected symbol by name as a known holdout in the verdict.

2.8 WHEN every dual-registry equity symbol successfully carries a live source THEN the header verdict SHALL read LIVE_MERGE_HEALTHY; otherwise the verdict SHALL remain LIVE_MERGE_DEGRADED and SHALL name the specific asset(s) preventing the healthy state.

### Unchanged Behavior (Regression Prevention)

The fix must preserve all currently-correct behavior in the Return Matrix, the source-preservation logic, and every other markets tab:

3.1 WHEN a year column has all months available for any series THEN the system SHALL CONTINUE TO compute and render the full-year return at full saturation with no partial-year flag.

3.2 WHEN a metals series has a partial year (e.g. Copper +12%, Aluminum +17% in 2026) THEN the system SHALL CONTINUE TO render the dim/grey honest partial-year treatment with the correct partial-to-date value, since this is the target behavior being generalized.

3.3 WHEN Dow Jones is processed by the live merge THEN the system SHALL CONTINUE TO show Source `yfinance:^DJI`.

3.4 WHEN Aluminum is processed by the live merge THEN the system SHALL CONTINUE TO show Source `fred:PALUMUSDM`.

3.5 WHEN any non-dual-registry asset that already carries a live source is processed THEN the system SHALL CONTINUE TO retain that live source.

3.6 WHEN the markets dashboard renders any tab other than the Return Matrix (e.g. detail views, yield/holdlab, volatility, asset detail) THEN those tabs SHALL CONTINUE TO render with no regression in layout, data, or tooltips.

3.7 WHEN the build runs end-to-end THEN the full build SHALL CONTINUE TO complete cleanly with all existing tests green.

3.8 WHEN `scraper.py` or any scoring code path executes THEN those code paths SHALL CONTINUE TO behave identically; they are out of scope and SHALL NOT be modified.

3.9 WHEN a year column has all months available and produces a legitimate large move (a known real market event with magnitude beyond ±70%) THEN the system SHALL CONTINUE TO render that value, since the ±70% sanity check applies only to unvalidated cells.

3.10 WHEN the partial-year tooltip flag is attached to a cell that already renders correctly today (e.g. metals 2026) THEN the tooltip text and the flag mechanism SHALL CONTINUE TO function as they do now.

## Deriving the Bug Conditions

This bugfix has two coordinated bug conditions. Both must hold under the fixed pipeline.

### Bug Condition 1 — Partial-year rendering

```pascal
FUNCTION isBugCondition1(Cell)
  INPUT: Cell of type ReturnMatrixCell
         Cell.assetClass    : {equity, metals, agriculture, energy}
         Cell.year          : integer
         Cell.monthsPresent : integer  // count of months with data in Cell.year
         Cell.monthsExpected: integer  // count of months expected for Cell.year
         Cell.renderedValue : float    // the return rendered in the cell
         Cell.renderStyle   : {saturated, dimmed, suppressed}
         Cell.partialFlag   : boolean  // tooltip "Partial year" flag attached
  OUTPUT: boolean

  // Cell triggers the bug when it is a partial-year cell that is either
  // rendered inconsistently with the canonical metals treatment, or carries
  // a numerically broken value, or is an unvalidated extreme single-year cell.

  isPartial   := (Cell.monthsPresent < Cell.monthsExpected)
  inconsistent:= isPartial AND (Cell.renderStyle = saturated)
  broken      := isPartial AND (abs(Cell.renderedValue) > 0.90)
                            AND (Cell.assetClass IN {agriculture, equity})
  extreme     := (NOT isPartial) AND (abs(Cell.renderedValue) > 0.70)
                                 AND (NOT isKnownRealEvent(Cell))

  RETURN inconsistent OR broken OR extreme
END FUNCTION
```

```pascal
// Property: Fix Checking — Partial-year rendering
FOR ALL Cell WHERE isBugCondition1(Cell) DO
  result := renderReturnMatrixCell'(Cell)
  ASSERT (result.renderStyle IN {dimmed, suppressed})
     AND (result.renderStyle = dimmed IMPLIES result.partialFlag = true)
     AND (result.renderStyle = dimmed IMPLIES isCorrectPartialToDate(result.renderedValue, Cell))
     AND (NOT isSaturatedArtifact(result))
END FOR
```

### Bug Condition 2 — Live-merge source on dual-registry equities

```pascal
FUNCTION isBugCondition2(Asset)
  INPUT: Asset of type RegistryEntry
         Asset.name           : string
         Asset.source         : string   // e.g. "yfinance:^DJI", "Mega_Markets_Historical.xlsx:Equities"
         Asset.liveFetchable  : boolean  // true if a live fetch can succeed
  OUTPUT: boolean

  // Cell triggers the bug when the asset is a dual-registry equity name
  // that retains the Excel source label even though a live fetch is feasible.

  dualRegistryEquity := Asset.name IN {"S&P 500", "NASDAQ", "NASDAQ-100"}
  excelLabelled      := startsWith(Asset.source, "Mega_Markets_Historical.xlsx")

  RETURN dualRegistryEquity AND excelLabelled AND Asset.liveFetchable
END FUNCTION
```

```pascal
// Property: Fix Checking — Live-merge source on dual-registry equities
FOR ALL Asset WHERE isBugCondition2(Asset) DO
  result := liveMerge'(Asset)
  ASSERT isLiveSource(result.source)   // yfinance:* or fred:*
END FOR

// Property: Honest reporting when live fetch is genuinely infeasible
FOR ALL Asset WHERE Asset.name IN {"S&P 500", "NASDAQ", "NASDAQ-100"}
                 AND NOT Asset.liveFetchable DO
  result  := liveMerge'(Asset)
  verdict := buildVerdict'()
  ASSERT startsWith(result.source, "Mega_Markets_Historical.xlsx")
     AND verdict.degraded = true
     AND Asset.name IN verdict.holdouts
END FOR
```

### Preservation Goal

```pascal
// Property: Preservation Checking
FOR ALL Cell WHERE NOT isBugCondition1(Cell) DO
  ASSERT renderReturnMatrixCell(Cell) = renderReturnMatrixCell'(Cell)
END FOR

FOR ALL Asset WHERE NOT isBugCondition2(Asset) DO
  ASSERT liveMerge(Asset) = liveMerge'(Asset)
END FOR
```

This ensures that for every full-year cell, every metals partial-year cell already rendering correctly, every non-dual-registry asset, and every other markets tab, the fixed pipeline behaves identically to the original.

### Definitions

- **F**: the original markets pipeline (predator/markets_history.py, predator/ingest_mega_xl.py, predator/vol_history.py) and `docs/markets.html` rendering as they exist before the fix.
- **F'**: the same files after applying the partial-year rule unification and the live-merge source-preservation extension to S&P 500 / NASDAQ / NASDAQ-100.
- **Counterexamples observed on the live site**:
  - Wheat 2026 −96%, Corn 2026 −98%, Sugar 2026 −99%, Soybeans 2026 −97%, Cotton 2026 −99%, Coffee 2026 −99% (broken partial-year math, saturated rendering).
  - S&P 500 Source `Mega_Markets_Historical.xlsx:Equities` while Dow Jones is `yfinance:^DJI` (live-merge holdout).
