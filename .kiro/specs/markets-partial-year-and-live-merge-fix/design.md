# Markets Partial-Year and Live-Merge Fix — Bugfix Design

## Overview

Two coordinated defects in the markets pipeline corrupt the integrity of the Return Matrix and the live-merge verdict on `docs/markets.html`. They ship as one fix because they share the same code paths and the same dashboard surface.

**Bug 1 — Partial-year cells.** The Return Matrix renders the current incomplete year (2026) inconsistently across asset classes. Metals cells (Copper +12%, Aluminum +17%) render with the canonical dim/italic partial-year treatment. Equity cells render at apparent full saturation. Agriculture cells render with both full saturation and mathematically broken near-total-loss values (Wheat −96%, Corn −98%, Sugar −99%, Soybeans −97%, Cotton −99%, Coffee −99%). The partial-year math, the partial-year visual treatment, and the absence of any sanity check on extreme single-year cells are all in scope.

**Bug 2 — Live-merge dual-registry equity holdouts.** The Excel→FRED/yfinance merge correctly flips Dow Jones (`yfinance:^DJI`) and Aluminum (`fred:PALUMUSDM`) to live sources, but S&P 500, NASDAQ Composite, and NASDAQ-100 (also dual-registry equity entries reachable via yfinance/FRED) still carry `Mega_Markets_Historical.xlsx:Equities` as their `Source` label. The live-merge verdict honestly reports `LIVE_MERGE_DEGRADED`, but does not name which assets are holding it back, and the verdict cannot reach `LIVE_MERGE_HEALTHY` while these specific assets persist.

The fix:

1. Unify the partial-year rule across every asset class as one rendering policy applied uniformly in `docs/markets.html` (the matrix consumer of `_buildAnnualCache`).
2. Harden `_buildAnnualCache` so that the per-cell return is computed from a stable price reference at the year boundary (Dec-of-prior versus latest-available month), with explicit guards against missing/zero divisor, NaN propagation, and unit/scale mismatch between Excel deep-history and FRED/yfinance current months.
3. Add a single-cell sanity check that flags or suppresses any unvalidated single-year return whose magnitude exceeds approximately ±70%.
4. Diagnose the live-fetch path for `sp500` (`yfinance:^GSPC`), `nasdaq` (`fred:NASDAQCOM`), and `nasdaq100` (`yfinance:^NDX`) in `predator/markets_history.py` so the live source label is set whenever a fresh fetch succeeds.
5. Extend the verdict block in `predator/build.py` (which reads `market_returns.json` and is rendered by `docs/markets.html`) so the verdict names specific holdouts when degraded, and only reaches `LIVE_MERGE_HEALTHY` when every dual-registry equity name carries a live source.

Scope is strictly `predator/markets_history.py`, `predator/ingest_mega_xl.py`, `predator/vol_history.py`, and `docs/markets.html`. `scraper.py` and all scoring code are out of scope.

## Glossary

- **Bug_Condition (C)**: The condition that triggers the bug. There are two: `C1` covers partial-year matrix cells that render inconsistently, broken, or unvalidated-extreme; `C2` covers dual-registry equity assets (S&P 500, NASDAQ, NASDAQ-100) that retain the Excel source label after the live merge.
- **Property (P)**: The desired behavior. `P1` requires partial-year cells to render under one uniform rule (dim/italic with partial-year flag and a numerically correct partial-to-date return, OR suppressed) and requires extreme cells to be validated. `P2` requires dual-registry equity assets to either flip to a live source or be honestly reported as named holdouts in the verdict.
- **Preservation**: Existing behavior that must remain unchanged. Full-year cells, metals partial-year cells, the Dow Jones live source, the Aluminum live source, every other markets tab, all unrelated tests, `scraper.py`, and all scoring code.
- **`_buildAnnualCache`**: The function in `docs/markets.html` (around line 2620) that builds `{year → {ret, partial, count}}` per asset for the Return Matrix. It compounds monthly returns and stamps `partial = (year === firstYear || year === lastYear)`.
- **`activeCellColor` / `cellText` / `.matrix-cell-partial`**: The rendering pipeline for matrix cells in `docs/markets.html`. The CSS class applies `opacity: 0.40` and italic; `cellText` adds a trailing `·` glyph; `activeCellColor` returns a diverging hue/lightness based on return magnitude.
- **`build_output`** (markets_history.py, ~line 721): Builds `market_returns.json` from FRED/yfinance results and merges with the Excel-seeded JSON, setting `meta.source = f"{source_type}:{series_id}"` whenever live data is present.
- **`_merge_monthly`** / **source-label honesty rule** (ingest_markets_xl.py, ~line 410): When the Excel ingester runs after a live source is set, it preserves the live source label and only supplies deep-history backfill for missing months.
- **`self_living_check`**: The verdict block built in `predator/build.py` (~line 810) and rendered as a chip in `docs/markets.html` (~line 100). Verdict values: `LIVE_MERGE_HEALTHY`, `LIVE_MERGE_DEGRADED`, `LIVE_MERGE_FAILED`.
- **Dual-registry equity name**: An equity asset whose `asset_id` appears in both the Excel `SERIES_MAP` (Equities sheet) and the live `ASSET_REGISTRY` in `markets_history.py`. The set is `{sp500, djia, nasdaq, nasdaq100, nikkei, sensex, dax}`. The bug holdouts are `{sp500, nasdaq, nasdaq100}`.
- **Partial year**: A year for which `monthsPresent < monthsExpected`, where `monthsExpected = 12` for any past year and `monthsExpected = currentMonth(asof)` for the asof year.

## Bug Details

### Bug Condition

The bug condition has two coordinated halves. Both must be satisfied by the fixed pipeline.

**Bug Condition 1 — Partial-year matrix cells.** A return-matrix cell triggers the bug when it is a partial-year cell rendered inconsistently with the canonical metals treatment, OR carries a numerically broken value, OR is an unvalidated extreme single-year cell.

```
FUNCTION isBugCondition1(cell)
  INPUT: cell of type ReturnMatrixCell
         cell.assetClass     : {equity, precious_metals, base_metals, agriculture, energy, ...}
         cell.year           : integer
         cell.monthsPresent  : integer  // count of months with data in cell.year
         cell.monthsExpected : integer  // 12 for past years; current_month(asof) for asof year
         cell.renderedValue  : float    // the return rendered in the cell
         cell.renderStyle    : {saturated, dimmed, suppressed}
         cell.partialFlag    : boolean  // whether the cell carries the partial-year tooltip
  OUTPUT: boolean

  isPartial    := cell.monthsPresent < cell.monthsExpected
  inconsistent := isPartial AND cell.renderStyle = saturated
  broken       := isPartial AND abs(cell.renderedValue) > 0.90
                              AND cell.assetClass IN {agriculture, equity}
  extreme      := (NOT isPartial) AND abs(cell.renderedValue) > 0.70
                                  AND (NOT isKnownRealEvent(cell))

  RETURN inconsistent OR broken OR extreme
END FUNCTION
```

**Bug Condition 2 — Dual-registry equity live-merge holdouts.** An asset triggers the bug when it is a dual-registry equity name that retains the Excel source label after the live merge runs and a live fetch is feasible.

```
FUNCTION isBugCondition2(asset)
  INPUT: asset of type RegistryEntry
         asset.name          : string                        // display name, e.g. "S&P 500"
         asset.assetId       : string                        // canonical key, e.g. "sp500"
         asset.source        : string                        // e.g. "yfinance:^GSPC", "Mega_Markets_Historical.xlsx:Equities"
         asset.liveFetchable : boolean                       // a live fetch could succeed for this run
  OUTPUT: boolean

  dualRegistryEquity := asset.assetId IN {"sp500", "nasdaq", "nasdaq100"}
  excelLabelled      := startsWith(asset.source, "Mega_Markets_Historical.xlsx")

  RETURN dualRegistryEquity AND excelLabelled AND asset.liveFetchable
END FUNCTION
```

### Examples

Counterexamples observed on the live site at the time of this design:

- **Wheat 2026 = −96%** (FRED `PWHEAMTUSDM`, agriculture). Partial year — only the first months of 2026 are present in the merged series — yet the cell renders saturated with a near-total-loss value that is not a real market move.
- **Corn 2026 = −98%**, **Sugar 2026 = −99%**, **Soybeans 2026 = −97%**, **Cotton 2026 = −99%**, **Coffee 2026 = −99%**. Same pattern across the agriculture sheet. All FRED-backed, all dual-registered with an Excel deep-history series.
- **Equity 2026 cells render at apparent full saturation** despite being partial-year cells, while the canonical treatment (used by metals) is dim/italic with the trailing `·` glyph.
- **Copper 2026 = +12%, Aluminum 2026 = +17%** (control). Partial year, dim/italic, partial flag set, numerically plausible — this is the target rendering.
- **S&P 500 → `Mega_Markets_Historical.xlsx:Equities`** (expected: `yfinance:^GSPC`). The asset_id `sp500` exists in both the Excel `SERIES_MAP` (Equities sheet) and the live `ASSET_REGISTRY` (yfinance ^GSPC), and a live fetch should be feasible — Dow Jones (^DJI), in the same registry, succeeded.
- **NASDAQ → `Mega_Markets_Historical.xlsx:Equities`** (expected: `fred:NASDAQCOM`).
- **NASDAQ-100 → `Mega_Markets_Historical.xlsx:Equities`** (expected: `yfinance:^NDX`).

## Expected Behavior

### Preservation Requirements

**Unchanged Behaviors:**

- Full-year cells in the Return Matrix continue to render at full saturation, with no partial-year flag and no `·` glyph, exactly as today.
- Metals partial-year cells (Copper 2026, Aluminum 2026, etc.) continue to render dim/italic with the partial-year flag and the same numeric value — this is the canonical treatment that the fix generalises.
- Dow Jones continues to carry `Source: yfinance:^DJI` after the merge.
- Aluminum continues to carry `Source: fred:PALUMUSDM` after the merge.
- Every non-dual-registry asset that already carries a live source today continues to retain that live source.
- Every markets-dashboard tab other than the Return Matrix (asset detail, yield/holdlab, volatility, multi-currency, seasonality, drawdown, etc.) renders with no regression in layout, data, or tooltips.
- All currently-green tests in `tests/` continue to pass. The full build `python -m predator.build … && python -m predator.ingest_markets_xl … && python -m predator.markets_history --full-refresh && python -m predator.vol_history --full-refresh` continues to complete cleanly.
- The freshness/verdict chip continues to display the same three states (`LIVE_MERGE_HEALTHY`, `LIVE_MERGE_DEGRADED`, `LIVE_MERGE_FAILED`) with the same colour palette; only the holdout naming and the threshold for `HEALTHY` change.
- `scraper.py` and every scoring code path execute identically — they are out of scope and SHALL NOT be modified.
- A legitimate full-year move with magnitude beyond ±70% (a known real market event) continues to render — the ±70% sanity check applies only to unvalidated cells.

**Scope of the fix:**

All inputs that do NOT satisfy `isBugCondition1` or `isBugCondition2` should be completely unaffected. This includes:

- Every full-year cell (`monthsPresent === monthsExpected`).
- Every metals partial-year cell that already renders correctly today.
- Every asset that is not in `{sp500, nasdaq, nasdaq100}`, regardless of source label.
- Every non-Return-Matrix view in `docs/markets.html`.

## Hypothesized Root Cause

### Bug 1 — Partial-year rendering

The most likely root causes, ranked from most to least likely based on the observed counterexamples:

1. **Excel↔FRED unit/scale mismatch on agriculture asset_ids.** The Excel sheet supplies wheat as `USD/bushel`, while FRED `PWHEAMTUSDM` supplies wheat as `USD/mt`. Both series are merged under the same `asset_id = "wheat"` by `SERIES_MAP` (in `predator/ingest_mega_xl.py` and `predator/ingest_markets_xl.py`) and `ASSET_REGISTRY` (in `predator/markets_history.py`). The same collision exists for corn (USD/bushel ↔ USD/mt), soybeans (USD/bushel ↔ USD/mt), sugar (USD/lb ↔ USD/kg), cotton (USD/lb ↔ USD/kg), and coffee (USD/lb ↔ USD/kg). When the merged monthly series crosses the boundary from Excel deep-history to FRED current-month, a single monthly return spikes by the unit ratio (≈36× for bushel→mt, ≈2.2× for lb→kg). Compounded across the partial year, this can produce extreme negatives (a ~−96% to −99% compound) or extreme positives, depending on which side wins the overlap month and on the sign of the ratio inversion in the merge.

2. **NaN-or-zero base divisor at the year boundary.** `_buildAnnualCache` skips `monthlyRet[ym] === undefined` but does not guard against `prev === 0` or `prev` being a unit-mismatched outlier. A near-zero or unit-misaligned `prev` produces a near-±100% monthly return and a saturated annual.

3. **`asof`-year `monthsExpected` mismatch.** The current `partial = (year === firstYear || year === lastYear)` rule is correct for flagging, but the visual treatment depends on the rendered magnitude. A large-magnitude partial cell still reads as "saturated" because the diverging palette is driven by `ret`, not by `partial`. The 40% opacity from `.matrix-cell-partial` is not enough to make a deep-color cell read as dim. This explains the equity case where the cell IS partial-flagged but VISUALLY reads saturated.

4. **`activeCellColor` z-score path.** When `colorMode === 'zscore'` and the year is partial, `getZscore` returns `null` and `activeCellColor` falls back to a dimmed return color. When `colorMode === 'return'`, there is no dimming at all beyond the CSS class — the cell takes the full diverging color. So the inconsistency between asset classes may also be partly a `colorMode` interaction.

5. **`firstYear`/`lastYear` derived from the merged series.** When deep-history Excel adds an early year that has only one month, that year's first cell is correctly flagged partial. When the live merge appends a partial 2026, the lastYear is correctly flagged partial. But if the merge drops the boundary month (because Excel `last < FRED first`), the partial-year span shifts and `monthsPresent` for the asof year is artificially small — driving a tiny compound base.

### Bug 2 — Live-merge dual-registry equity holdouts

The most likely root causes, ranked from most to least likely:

1. **Live fetch returns empty for `^GSPC`, `^NDX`, `NASDAQCOM`.** In `markets_history.fetch_all`, a per-series try/except catches every exception and degrades to cache. If the cache is also empty (cold-start, or cache invalidated), `data` remains empty. In `build_output`, the early return `if data is None or data.empty: ... assets_out[key] = existing_assets[key]` then preserves the Excel-labelled entry verbatim — no live source, no holdout signal. This is the most likely explanation for the asymmetry with `^DJI` (which fetched fine).

2. **Cache freshness drift.** A previous successful fetch wrote the parquet cache, but a subsequent failure plus a `--full-refresh` invocation that rebuilt only some series leaves the cache present but the live fetch empty. The current preserve-existing path then re-emits the Excel label.

3. **yfinance MultiIndex column shape change.** `_fetch_yfinance_series` handles MultiIndex columns with a fallback to `data["Close"].iloc[:, 0]`. If the response shape changes again for specific tickers (rate-limited or shape-coerced), the fallback may produce an empty Series silently for `^GSPC` / `^NDX` but not for `^DJI`.

4. **FRED rate limit on `NASDAQCOM`.** `_fetch_fred_series` retries internally but a sustained 429 produces an empty Series, which then falls through the same preserve-existing path.

5. **Verdict aggregation ignores per-asset names.** Even when `sp500` falls through to Excel, the verdict logic in `predator/build.py` only counts `by_source.get("excel", 0)` and `by_source.get("fred", 0) + by_source.get("yfinance", 0)`. It cannot name the holdouts because it never iterates per-asset to record which assets are excel-labelled. This is a downstream defect that prevents `LIVE_MERGE_HEALTHY` from ever being honest about the cause.

## Correctness Properties

Property 1: Bug Condition — Partial-year cells render uniformly and never broken-or-extreme

_For any_ Return Matrix cell where the bug condition holds (`isBugCondition1` returns true), the fixed `_buildAnnualCache` and matrix render pipeline SHALL produce a cell with `renderStyle ∈ {dimmed, suppressed}`. If `renderStyle = dimmed`, the cell SHALL carry the partial-year flag, and `renderedValue` SHALL equal a numerically correct partial-to-date compound return computed from a non-zero, NaN-free base price. The cell SHALL NOT carry a saturated near-total-loss artifact. For full-year cells whose magnitude exceeds approximately ±70%, the cell SHALL be either validated against `isKnownRealEvent` (and rendered) or marked as suppressed/flagged.

**Validates: Requirements 2.1, 2.2, 2.3, 2.4, 2.5**

Property 2: Bug Condition — Dual-registry equity live-merge

_For any_ dual-registry equity asset where the bug condition holds (`isBugCondition2` returns true), the fixed `markets_history.build_output` SHALL set `meta.source = isLiveSource(...)` (i.e. starts with `yfinance:` or `fred:`) when a live fetch succeeds. _For any_ such asset where a live fetch genuinely cannot succeed in this run, `meta.source` MAY remain the Excel label, AND the verdict block SHALL include the asset's display name in `self_living_check.holdouts`, AND the verdict SHALL be `LIVE_MERGE_DEGRADED` (not `LIVE_MERGE_HEALTHY`).

**Validates: Requirements 2.6, 2.7, 2.8**

Property 3: Preservation — Full-year, metals partial-year, and non-dual-registry assets unchanged

_For any_ Return Matrix cell where `isBugCondition1` returns false, the fixed render pipeline SHALL produce exactly the same `{ret, partial, renderStyle, partialFlag}` quadruple as the original pipeline. _For any_ asset where `isBugCondition2` returns false, the fixed `build_output` SHALL produce exactly the same `meta.source` as the original. This preserves every full-year cell, every metals partial cell that renders correctly today, every non-Return-Matrix tab, the Dow Jones live source, the Aluminum live source, and every other asset that already carries a live source.

**Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 3.9, 3.10**

## Fix Implementation

### Changes Required

Assuming the root-cause hypotheses are correct (the testing strategy below confirms or refutes each before code lands).

#### File: `docs/markets.html` — `_buildAnnualCache` and matrix render

1. **Numerically robust partial-to-date return.** In `_buildAnnualCache` (around line 2624), when computing the year compound, guard against:
   - `prev <= 0` or `prev` outside a sanity envelope (the existing series' median ratio over the trailing window).
   - A monthly return whose magnitude exceeds approximately 0.50 in a single step on a non-equity series — flag the year as `boundaryAnomaly: true` so the rendering layer can suppress the cell rather than show an artifact.
   - NaN propagation: if `compound` becomes NaN/Inf, drop the year.

2. **Compute `monthsExpected`.** Replace the implicit `partial = (year === firstYear || year === lastYear)` rule with an explicit comparison:
   ```
   monthsExpected = (year < asofYear) ? 12 : asofMonth
   partial        = (count < monthsExpected) OR (year === firstYear AND count < 12)
   ```
   The `firstYear` clause preserves the deep-history-edge case.

3. **Uniform partial-year rendering policy.** In the matrix render template (around line 397), introduce one rendering policy applied to every asset class regardless of category. The policy is the existing metals treatment: `.matrix-cell-partial` (40% opacity, italic) plus the trailing `·` glyph plus the partial tooltip. The diverging color palette continues to drive hue/lightness, but the cell-level opacity is the single signal of partiality.

4. **Extreme-cell sanity check.** Add a `validateExtreme(ret, year, asset, count, monthsExpected)` helper that returns `{ok, reason}`:
   - `ok = true` when `abs(ret) <= 0.70`.
   - `ok = true` when the cell is in the curated `KNOWN_REAL_EVENTS` map (e.g. `{ ('sp500', 2008): -0.38, ('nasdaq', 2000): -0.39 }` — small set of known real crashes/booms; entries are added on a value-by-value basis).
   - `ok = false` otherwise, with `reason = 'unvalidated_extreme'`.
   When `ok = false`, the matrix renderer SHALL apply the dimmed style AND a warning glyph (e.g. `⚠`) AND a tooltip line "unvalidated extreme — verify against source data."

5. **Tooltip honesty.** The cell tooltip MUST show: month count, expected count, partial flag, computed return value, and (when applicable) the boundary-anomaly or unvalidated-extreme reason.

#### File: `predator/markets_history.py` — Live fetch hardening

6. **Diagnose the empty-fetch path for `sp500`, `nasdaq`, `nasdaq100`.** In `_fetch_yfinance_series`, after the empty-result check, add a one-line `print(f"    EMPTY: yfinance returned 0 rows for {ticker}")` when `data is None or data.empty`. Same for `_fetch_fred_series` on empty results. This provides log evidence the testing strategy can use to confirm the root cause.

7. **`build_output` honest-source path.** When `data is None or data.empty` AND there is no cache AND `key in existing_assets` AND `existing_assets[key].meta.source` starts with `Mega_Markets_Historical.xlsx`, set a sentinel field `existing_assets[key].meta._live_holdout = True` before re-emitting. The downstream verdict block reads this sentinel.
   - This SHALL NOT modify the `source` label itself — the Excel label is honest when no live data is available — but it SHALL surface the asset to the verdict block.

8. **Per-symbol retry with backoff.** Replace the unconditional fall-through with one retry after a short delay (1–2 seconds) for the three known-flaky tickers, before degrading to cache. Bound retries to a single extra attempt per series to keep the run fast.

#### File: `predator/build.py` — Verdict block

9. **Verdict naming.** In the `markets_freshness.self_living_check` block (around line 810), iterate per-asset and build a `holdouts` list:
   ```
   holdouts = sorted([
     v["meta"]["name"]
     for aid, v in assets.items()
     if aid in DUAL_REGISTRY_EQUITIES
       AND v["meta"].get("source", "").startswith("Mega_Markets_Historical")
   ])
   ```
   where `DUAL_REGISTRY_EQUITIES = {"sp500", "nasdaq", "nasdaq100", "djia", "nikkei", "sensex", "dax"}`.

10. **Verdict threshold.** The verdict SHALL be `LIVE_MERGE_HEALTHY` only when `len(holdouts) == 0` AND `live_source_count > excel_only_count`. When `len(holdouts) > 0`, the verdict SHALL be `LIVE_MERGE_DEGRADED` AND the verdict object SHALL carry `holdouts` so the dashboard chip tooltip can name them.

#### File: `docs/markets.html` — Verdict chip tooltip

11. **Holdout naming in the chip tooltip.** In the verdict-chip `@mouseenter` handler (around line 105), append the holdouts list when present:
    ```
    `${baseTooltip} · holdouts: ${holdouts.join(', ')}`
    ```
    The chip itself continues to read `● LIVE MERGE OK` / `◐ LIVE MERGE DEGRADED` / `◌ LIVE MERGE FAILED` as today.

#### File: `predator/ingest_mega_xl.py`

12. **No code change in the deprecated path.** The active Excel ingester is `predator/ingest_markets_xl.py`, which already preserves live source labels (the source-label honesty rule, around line 410). `ingest_mega_xl.py` is a backward-compat shim that delegates to `ingest_markets_xl`. The shim is in scope of this spec only as a no-touch — confirm that the existing behaviour (delegating without re-introducing the Excel label clobber) is preserved by the test suite, and add no new logic.

#### File: `predator/vol_history.py`

13. **No code change.** `vol_history.py` writes its own JSON (`docs/data/vol_history.json`) and does not participate in the Return Matrix or the live-merge verdict. It is in scope only because the bugfix.md listed the markets pipeline files; verify with a test that the file is not modified by the fix and that `vol_history` continues to set its sources as `fred:{series_id}` (which is already correct).

## Testing Strategy

### Validation Approach

The strategy follows the two-phase bug-condition methodology: first surface counterexamples that demonstrate each bug on the unfixed code (and discriminate between the candidate root causes), then verify the fix produces the expected behavior for buggy inputs and produces identical behavior for non-buggy inputs.

### Exploratory Bug Condition Checking

**Goal**: Surface concrete counterexamples for both bugs and discriminate between the candidate root causes BEFORE writing the fix. If the observed counterexamples do not match the leading hypothesis, re-hypothesize before coding.

**Test Plan**:

For Bug 1, build a focused harness that reads the live `docs/data/market_returns.json`, replays `_buildAnnualCache` in Node (or a Python port), and asserts the bug-condition predicate on every cell. The harness should also probe the unit-mismatch hypothesis directly by inspecting the merged monthly series for each agriculture `asset_id` and reporting any month-over-month price ratio outside `[0.5, 2.0]` — this isolates the boundary jump signature.

For Bug 2, run `python -m predator.markets_history --full-refresh --assets sp500,nasdaq,nasdaq100,djia` against the live JSON and inspect `meta.source` for each. Capture stdout to verify whether each yfinance/FRED call returns rows or empties, which discriminates between root-cause candidates 1 (live fetch empty) and 2 (cache freshness drift).

**Test Cases**:

1. **Wheat 2026 partial-year value** (Bug 1, broken): Replay `_buildAnnualCache` for `wheat`. ASSERT the 2026 cell return is in a plausible range (within ±50% of a sanity envelope derived from the trailing 24 months) and `partial = true`. EXPECTED on unfixed code: return ≈ −0.96, value broken.

2. **Equity 2026 partial-year render style** (Bug 1, inconsistent): Render the matrix with the test harness for `sp500`, `nasdaq`, `nasdaq100`. ASSERT the 2026 cell carries `.matrix-cell-partial` AND has effective opacity ≤ 0.5 in the rendered DOM. EXPECTED on unfixed code: cell is partial-flagged but visually saturates because the diverging color overrides the 40% opacity at full magnitude.

3. **Metals 2026 partial-year render style** (Bug 1, control / preservation): Same harness for `copper`, `aluminum`. ASSERT cell renders dim/italic with partial flag. EXPECTED on unfixed code: passes (this is the canonical treatment being generalised).

4. **Unit-mismatch boundary scan** (Bug 1, root-cause discrimination): For each agriculture asset_id, scan the merged monthly series and report any month-over-month price ratio outside `[0.5, 2.0]`. EXPECTED on unfixed code: a single boundary month around the Excel→FRED join shows a ratio of ≈36× (bushel↔mt) or ≈2.2× (lb↔kg). If absent, re-hypothesize toward root causes 2 or 5.

5. **Extreme single-year cell scan** (Bug 1, extreme): Scan every cell in the matrix and report any with `monthsPresent === monthsExpected` and `abs(ret) > 0.70` that is not in `KNOWN_REAL_EVENTS`. EXPECTED on unfixed code: a small number of unvalidated extremes will surface (potentially DAX 1922-style or other Curvo-synthetic edge cases).

6. **S&P 500 live fetch** (Bug 2, holdout): Run `markets_history --full-refresh --assets sp500`. ASSERT post-run `meta.source = "yfinance:^GSPC"`. EXPECTED on unfixed code: Either the fetch succeeds (suggesting cache freshness drift) or returns empty (suggesting live fetch failure) — discriminating between root causes 1 and 2.

7. **NASDAQ Composite live fetch** (Bug 2, holdout): Same with `nasdaq`. EXPECTED: same discrimination for FRED `NASDAQCOM`.

8. **NASDAQ-100 live fetch** (Bug 2, holdout): Same with `nasdaq100`. EXPECTED: same discrimination for yfinance `^NDX`.

9. **Verdict naming gap** (Bug 2, verdict): Read `docs/data/metadata.json` and inspect `markets_data_freshness.self_living_check`. ASSERT presence of a `holdouts` list. EXPECTED on unfixed code: key is absent — verdict cannot name what is preventing healthy state.

**Expected Counterexamples**:

- Wheat / Corn / Sugar / Soybeans / Cotton / Coffee 2026 cells with `abs(ret) > 0.90`, partial-flagged.
- Equity 2026 cells partial-flagged but visually saturated due to magnitude-driven color.
- S&P 500, NASDAQ, NASDAQ-100 with `meta.source` starting with `Mega_Markets_Historical.xlsx`.
- `metadata.json` `self_living_check` block missing a `holdouts` field, or with the field present but empty even though the by-source bucket shows `excel > 0`.
- Possible causes (to be discriminated by the harness): unit/scale collision at the Excel↔FRED boundary (most likely for ag), magnitude-driven color overpowering the 40% opacity for equity, empty live-fetch result preserving the Excel label.

### Fix Checking

**Goal**: Verify that for all inputs where the bug condition holds, the fixed pipeline produces the expected behavior.

**Pseudocode:**

```
FOR ALL cell WHERE isBugCondition1(cell) DO
  result := buildAnnualCache_fixed(cell.asset).get(cell.year)
  ASSERT result.partialFlag = true
     AND result.renderStyle IN {dimmed, suppressed}
     AND (result.renderStyle = dimmed IMPLIES isCorrectPartialToDate(result.ret, cell))
     AND NOT isSaturatedArtifact(result)
END FOR

FOR ALL asset WHERE isBugCondition2(asset) AND asset.liveFetchable DO
  result := buildOutput_fixed(asset)
  ASSERT isLiveSource(result.meta.source)   // starts with yfinance: or fred:
END FOR

FOR ALL asset WHERE asset.assetId IN {"sp500", "nasdaq", "nasdaq100"} AND NOT asset.liveFetchable DO
  result  := buildOutput_fixed(asset)
  verdict := buildVerdict_fixed()
  ASSERT startsWith(result.meta.source, "Mega_Markets_Historical.xlsx")
     AND verdict.value = "LIVE_MERGE_DEGRADED"
     AND asset.name IN verdict.holdouts
END FOR
```

### Preservation Checking

**Goal**: Verify that for all inputs where the bug condition does NOT hold, the fixed pipeline produces the same result as the original pipeline.

**Pseudocode:**

```
FOR ALL cell WHERE NOT isBugCondition1(cell) DO
  ASSERT buildAnnualCache_original(cell.asset).get(cell.year)
       = buildAnnualCache_fixed   (cell.asset).get(cell.year)
END FOR

FOR ALL asset WHERE NOT isBugCondition2(asset) DO
  ASSERT buildOutput_original(asset).meta.source
       = buildOutput_fixed   (asset).meta.source
END FOR
```

**Testing Approach**: Property-based testing is recommended for preservation checking because:

- It generates many synthetic monthly-close series across categories (equity, metals, agriculture, energy) and many full-year and partial-year combinations, catching edge cases manual unit tests miss.
- It generates synthetic registry entries with random assignments of source labels (excel / yfinance / fred / unknown) and random `live_fetchable` flags, exercising the verdict logic broadly.
- It provides strong guarantees that behavior is unchanged for all non-buggy inputs, far beyond what a few hand-picked fixtures can verify.

**Test Plan**: Capture the original behavior as a snapshot (baseline JSON of `_buildAnnualCache` outputs across every asset, plus a baseline `meta.source` map) BEFORE applying the fix. Then run the fixed code against the same inputs and assert pointwise equality on the non-buggy subset.

**Test Cases**:

1. **Full-year cells unchanged**: Snapshot every full-year cell from the live `market_returns.json` before the fix; assert pointwise equality (`{ret, partial, count}`) after the fix.
2. **Metals partial cells unchanged**: Snapshot the 2026 cells for `copper`, `aluminum`, `gold`, `silver`, `platinum`, `palladium`, `nickel`, `zinc`, `iron_ore`, `tin`, `lead` before the fix; assert pointwise equality after.
3. **Dow Jones source unchanged**: ASSERT `meta.source = "yfinance:^DJI"` after the fix.
4. **Aluminum source unchanged**: ASSERT `meta.source = "fred:PALUMUSDM"` after the fix.
5. **Non-dual-registry asset sources unchanged**: For every asset_id NOT in `{sp500, nasdaq, nasdaq100}`, ASSERT `meta.source` is unchanged.
6. **Other markets-dashboard tabs unchanged**: Visual snapshot test (or DOM-text snapshot) of asset detail, yield/holdlab, volatility, multi-currency, seasonality, drawdown — assert no diff.
7. **`scraper.py` unchanged**: ASSERT `git diff scraper.py` is empty.
8. **Scoring code unchanged**: ASSERT `git diff predator/scoring.py predator/build.py` (excluding the verdict block at the documented line range) is empty.

### Unit Tests

- `_buildAnnualCache` numerical robustness: zero-base divisor → year suppressed (not −∞ or NaN).
- `_buildAnnualCache` boundary anomaly: synthetic series with one month-of-+3500% return → year flagged `boundaryAnomaly` and suppressed.
- `validateExtreme(ret, ...)`: returns `{ok: true}` for `|ret| ≤ 0.70`; `{ok: true}` for entries in `KNOWN_REAL_EVENTS`; `{ok: false, reason: 'unvalidated_extreme'}` otherwise.
- `_fetch_yfinance_series` empty-response logging: triggers the `EMPTY:` log line for an empty Series.
- `build_output` `_live_holdout` sentinel: when live fetch + cache both empty, sentinel is set on the preserved entry.
- `self_living_check.holdouts`: contains exactly the dual-registry equity assets whose source starts with `Mega_Markets_Historical`.
- `verdict = LIVE_MERGE_HEALTHY` only when `len(holdouts) == 0`.

### Property-Based Tests

- **Generator**: synthetic per-asset monthly-close series (year range `[1970, 2026]`, monthly cadence, prices in `[1, 1e6]`, occasional NaN, occasional unit-jump months). For each generated asset, compute `_buildAnnualCache` under both the original and the fixed code. ASSERT preservation for the non-buggy subset, and ASSERT the fix's correctness predicate for the buggy subset.
- **Generator**: synthetic registry entries with `(name ∈ all_equity_names, source ∈ {excel, yfinance:*, fred:*}, live_fetchable ∈ {true, false})`. ASSERT the verdict logic produces `HEALTHY` ↔ `holdouts.empty AND live_source_count > excel_only_count`, and ASSERT `holdouts` always contains exactly the dual-registry entries with the Excel label.
- **Generator**: random matrix cells `(year ∈ [1970, 2026], monthsPresent ∈ [1, 12], monthsExpected ∈ [1, 12], ret ∈ [-1, 5])`. ASSERT the rendering policy is the SAME function across asset classes (single render path).

### Integration Tests

- **Full pipeline run**: `python -m predator.ingest_markets_xl --no-fail-on-stale && python -m predator.markets_history --full-refresh && python -m predator.build --source data/all_history.csv --output docs/data --config config.yaml`. ASSERT the resulting `market_returns.json` and `metadata.json`:
  - Every dual-registry equity asset that was reachable via live fetch carries a `yfinance:` or `fred:` source.
  - Any holdouts are named in `markets_data_freshness.self_living_check.holdouts`.
  - The verdict is `LIVE_MERGE_HEALTHY` if and only if `holdouts` is empty AND `live_source_count > excel_only_count`.
- **Render integration**: Open `docs/markets.html` against the resulting JSON in a headless browser. ASSERT the 2026 column for `wheat`, `corn`, `sugar`, `soybeans`, `cotton`, `coffee` either renders dim/italic with a numerically plausible partial-to-date return, or is suppressed. ASSERT the 2026 column for `sp500`, `nasdaq`, `nasdaq100` renders dim/italic with a numerically plausible partial-to-date return. ASSERT the 2026 column for `copper`, `aluminum` continues to render exactly as it does today.
- **Verdict chip integration**: ASSERT the verdict chip's tooltip names any holdouts when degraded, and the chip text is `● LIVE MERGE OK` only when `holdouts` is empty.
- **Other markets tabs**: Click through asset detail, yield/holdlab, volatility, multi-currency, seasonality, drawdown. ASSERT no visual or data regressions.
- **Test suite**: `python -m pytest tests/ -v` — all currently green tests SHALL remain green.
