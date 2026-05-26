# Implementation Plan: Markets Partial-Year and Live-Merge Fix

## Overview

Two coordinated defects in the markets pipeline ship as one bugfix:

- **Bug 1 — Partial-year matrix cells:** the 2026 column renders inconsistently across asset classes (metals correct, equity saturated-but-partial-flagged, agriculture saturated AND mathematically broken at −96%/−99%).
- **Bug 2 — Dual-registry equity live-merge holdouts:** S&P 500, NASDAQ Composite, and NASDAQ-100 retain `Mega_Markets_Historical.xlsx:Equities` as `Source` even though Dow Jones (same registry) flips to `yfinance:^DJI`; the verdict cannot reach `LIVE_MERGE_HEALTHY` and does not name the holdouts.

The plan follows the bugfix exploratory methodology: write a Bug Condition exploration test that fails on unfixed code, write Preservation property tests that pass on unfixed code, apply the fix across `docs/markets.html`, `predator/markets_history.py`, and `predator/build.py`, then re-run both test sets.

Scope is strictly the markets pipeline files and `docs/markets.html`. `scraper.py` and all scoring code are out of scope.

## Tasks

- [x] 1. Write bug condition exploration test
  - **Property 1: Bug Condition** - Partial-Year Inconsistency, Broken Ag Math, and Dual-Registry Equity Live-Merge Holdouts
  - **CRITICAL**: This test MUST FAIL on unfixed code - failure confirms the bugs exist
  - **DO NOT attempt to fix the test or the code when it fails**
  - **NOTE**: This test encodes the expected behavior - it will validate the fix when it passes after implementation
  - **GOAL**: Surface counterexamples that demonstrate (a) the partial-year render inconsistency / broken-ag math / unvalidated-extreme cases and (b) the dual-registry equity live-merge holdouts
  - **Scoped PBT Approach**: Bug 1 is partly deterministic (the live `docs/data/market_returns.json` already contains the broken ag cells), so scope Property 1 to the concrete failing cases and reinforce with a property over synthesized monthly-close series; Bug 2 is deterministic against the live registry, so scope to the concrete dual-registry equity asset_ids
  - **Bug Condition C1 — Partial-year matrix cells (encoded from `isBugCondition1` in design):**
    - `isPartial    := cell.monthsPresent < cell.monthsExpected`
    - `inconsistent := isPartial AND cell.renderStyle = saturated`
    - `broken       := isPartial AND abs(cell.renderedValue) > 0.90 AND cell.assetClass IN {agriculture, equity}`
    - `extreme      := (NOT isPartial) AND abs(cell.renderedValue) > 0.70 AND (NOT isKnownRealEvent(cell))`
    - `C1(cell) = inconsistent OR broken OR extreme`
  - **Bug Condition C2 — Dual-registry equity live-merge (encoded from `isBugCondition2` in design):**
    - `dualRegistryEquity := asset.assetId IN {"sp500", "nasdaq", "nasdaq100"}`
    - `excelLabelled      := startsWith(asset.source, "Mega_Markets_Historical.xlsx")`
    - `C2(asset) = dualRegistryEquity AND excelLabelled AND asset.liveFetchable`
  - Test 1a (Bug 1, broken ag — concrete): replay `_buildAnnualCache` (Python port) against `docs/data/market_returns.json` for `wheat`, `corn`, `sugar`, `soybeans`, `cotton`, `coffee`; ASSERT for the 2026 cell `abs(ret) <= 0.50` AND `partial = true` AND `renderStyle IN {dimmed, suppressed}`
  - Test 1b (Bug 1, equity inconsistency — concrete): replay `_buildAnnualCache` for `sp500`, `nasdaq`, `nasdaq100`, `djia`, `nikkei`, `sensex`, `dax`; ASSERT the 2026 cell carries `partialFlag = true` AND `renderStyle ∈ {dimmed, suppressed}` (i.e. not `saturated`)
  - Test 1c (Bug 1, unvalidated extreme — property): for every cell in the matrix where `monthsPresent === monthsExpected` and `abs(ret) > 0.70`, ASSERT the cell is in `KNOWN_REAL_EVENTS` OR is suppressed/flagged
  - Test 1d (Bug 1, property over synthesized series): generate synthetic monthly-close series (1970–2026, occasional NaN, occasional unit-jump month, prices in [1, 1e6]); for each `(asset, year)` cell, ASSERT `NOT isSaturatedArtifact(result)` AND (`isPartial IMPLIES result.renderStyle ∈ {dimmed, suppressed}`) — encodes Property 1 from design Section "Correctness Properties"
  - Test 1e (Bug 2, concrete): run `python -m predator.markets_history --full-refresh --assets sp500,nasdaq,nasdaq100,djia` then read `docs/data/market_returns.json`; ASSERT `meta.source` for `sp500` starts with `yfinance:` OR `fred:`; same for `nasdaq` and `nasdaq100`
  - Test 1f (Bug 2, verdict naming — concrete): read `docs/data/metadata.json` `markets_data_freshness.self_living_check`; ASSERT presence of `holdouts` field, AND if `verdict == "LIVE_MERGE_DEGRADED"` then `holdouts` is non-empty AND names the offending dual-registry equity assets by display name
  - Run all tests on UNFIXED code
  - **EXPECTED OUTCOME**: Tests FAIL (this is correct - it proves the bugs exist)
  - Document counterexamples found, e.g. `wheat 2026 = -0.96 (broken)`, `sp500 meta.source = "Mega_Markets_Historical.xlsx:Equities"`, `metadata.self_living_check.holdouts missing`
  - Use the unit-mismatch boundary scan (design Test Plan #4) to discriminate Bug 1 root cause: report any month-over-month price ratio outside `[0.5, 2.0]` for ag asset_ids around the Excel↔FRED boundary; record which root-cause candidate the evidence supports (unit-mismatch ≈36× / ≈2.2×, vs zero-base, vs NaN propagation, vs magnitude-driven-color)
  - Use the live-fetch logging (design Fix Implementation #6) to discriminate Bug 2 root cause: capture stdout of the `--full-refresh` run; record whether `^GSPC`/`^NDX`/`NASDAQCOM` returned 0 rows (live-fetch empty) or whether the cache is being preferred (cache freshness drift)
  - Mark task complete when tests are written, run, failures are documented, AND the root-cause discrimination evidence is recorded
  - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8_

- [x] 2. Write preservation property tests (BEFORE implementing fix)
  - **Property 2: Preservation** - Full-Year Cells, Metals Partial Cells, Existing Live Sources, and Other Markets Tabs Unchanged
  - **IMPORTANT**: Follow observation-first methodology - observe behavior on UNFIXED code for non-buggy inputs, then write property-based tests asserting that observed behavior is preserved across the input domain
  - Property-based testing generates many test cases for stronger guarantees that the fix touches only `C1`/`C2` inputs
  - **Preservation Predicate (encoded from design "Preservation Checking"):**
    - `FOR ALL cell WHERE NOT isBugCondition1(cell): buildAnnualCache_original(cell.asset).get(cell.year) = buildAnnualCache_fixed(cell.asset).get(cell.year)`
    - `FOR ALL asset WHERE NOT isBugCondition2(asset): buildOutput_original(asset).meta.source = buildOutput_fixed(asset).meta.source`
  - **Step 1 — Capture baselines on unfixed code:**
    - Snapshot every full-year cell `{ret, partial, count}` from the live `docs/data/market_returns.json` for every asset (covers Requirement 3.1)
    - Snapshot 2026 cells for `copper`, `aluminum`, `gold`, `silver`, `platinum`, `palladium`, `nickel`, `zinc`, `iron_ore`, `tin`, `lead` (covers Requirement 3.2)
    - Snapshot `meta.source` for every asset_id, especially `djia → yfinance:^DJI`, `aluminum → fred:PALUMUSDM`, and every non-dual-registry live asset (covers Requirements 3.3, 3.4, 3.5)
    - Snapshot DOM text of every non-Return-Matrix tab in `docs/markets.html` (asset detail, yield/holdlab, volatility, multi-currency, seasonality, drawdown) (covers Requirement 3.6)
    - Snapshot `git diff scraper.py` and `git diff predator/scoring.py` as empty-baseline (covers Requirement 3.8)
  - **Step 2 — Write preservation property tests:**
    - Test 2a (full-year cells, property): for synthesized assets, generate full-year cell inputs (`monthsPresent === monthsExpected`); ASSERT pointwise equality of `{ret, partial, count}` between original and fixed `_buildAnnualCache`
    - Test 2b (metals partial cells, concrete): for snapshotted metals 2026 cells, ASSERT pointwise equality of `{ret, partial, count, renderStyle, partialFlag}`
    - Test 2c (Dow Jones / Aluminum live source, concrete): ASSERT `meta.source` for `djia` is `yfinance:^DJI` AND `meta.source` for `aluminum` is `fred:PALUMUSDM`
    - Test 2d (non-dual-registry sources, property): generate synthetic registry entries with `(asset_id ∈ all_assets \ {sp500, nasdaq, nasdaq100}, source ∈ {excel, yfinance:*, fred:*}, live_fetchable ∈ {true, false})`; ASSERT `meta.source` is unchanged after the fixed `build_output` runs
    - Test 2e (other tabs, snapshot): replay each non-Return-Matrix tab against the snapshotted DOM text; ASSERT no diff
    - Test 2f (out-of-scope files unchanged): ASSERT `git diff scraper.py` is empty AND `git diff predator/scoring.py` is empty (Requirement 3.8) AND `predator/vol_history.py` continues to set `fred:{series_id}` sources unchanged (design Fix Implementation #13)
    - Test 2g (legitimate extreme full-year cells preserved): for any cell with `monthsPresent === monthsExpected` AND `abs(ret) > 0.70` that is in `KNOWN_REAL_EVENTS`, ASSERT it continues to render at full saturation (Requirement 3.9)
    - Test 2h (verdict states & palette unchanged): ASSERT the verdict chip continues to expose the three states `LIVE_MERGE_HEALTHY` / `LIVE_MERGE_DEGRADED` / `LIVE_MERGE_FAILED` with the same colour palette (Requirement 3.10 implicit, design Preservation Requirements)
    - Test 2i (currently-green tests stay green): run `python -m pytest tests/ -v` and ASSERT same pass set as baseline (Requirement 3.7)
  - Run tests on UNFIXED code
  - **EXPECTED OUTCOME**: Tests PASS (this confirms baseline behavior to preserve)
  - Mark task complete when tests are written, run, and passing on unfixed code
  - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 3.9, 3.10_

- [x] 3. Fix for partial-year rendering and dual-registry equity live-merge

  - [x] 3.1 Harden `_buildAnnualCache` numerical robustness in `docs/markets.html`
    - At ~line 2624, when computing the year compound, guard against `prev <= 0` and `prev` outside the trailing-window sanity envelope
    - On a non-equity series, flag any single monthly return with `|ret| > 0.50` as `boundaryAnomaly: true` so the renderer can suppress
    - Drop the year cleanly if the running compound becomes NaN/Inf (no `-0.99` artifact)
    - Replace the implicit `partial = (year === firstYear || year === lastYear)` rule with explicit `monthsExpected = (year < asofYear) ? 12 : asofMonth` and `partial = (count < monthsExpected) OR (year === firstYear AND count < 12)`
    - _Bug_Condition: `isBugCondition1(cell)` — specifically the `broken` and zero-base / NaN-propagation root causes_
    - _Expected_Behavior: `result.partialFlag = true AND result.renderStyle ∈ {dimmed, suppressed} AND (renderStyle = dimmed IMPLIES isCorrectPartialToDate(result.ret, cell)) AND NOT isSaturatedArtifact(result)`_
    - _Preservation: full-year cells unchanged; metals partial cells unchanged; deep-history-edge `firstYear` partial behavior preserved (Preservation Requirements §3.1, §3.2)_
    - _Requirements: 2.1, 2.4_

  - [x] 3.2 Apply one uniform partial-year rendering policy in `docs/markets.html`
    - At ~line 397 (matrix render template), drive partial styling by the `partial` flag alone, identical across every asset class (the canonical metals treatment)
    - Apply `.matrix-cell-partial` (40% opacity, italic) plus the trailing `·` glyph plus the partial tooltip whenever `partial = true`
    - Decouple cell-level dimming from the diverging color palette so a high-magnitude partial cell still reads visually dim (fixes the "equity partial-flagged but visually saturated" inconsistency)
    - _Bug_Condition: `isBugCondition1(cell)` — specifically the `inconsistent` clause and the magnitude-driven-color root cause (#3 from design)_
    - _Expected_Behavior: `result.partialFlag = true AND result.renderStyle ∈ {dimmed, suppressed}` uniformly across `{equity, metals, agriculture, energy}`_
    - _Preservation: full-year cells continue at full saturation with no partial flag (Preservation Requirement §3.1); metals partial cells render exactly as today (§3.2)_
    - _Requirements: 2.1, 2.2, 2.3_

  - [x] 3.3 Add `validateExtreme` sanity check in `docs/markets.html`
    - Implement `validateExtreme(ret, year, asset, count, monthsExpected) → {ok, reason}`
    - `ok = true` when `|ret| <= 0.70`
    - `ok = true` when the cell is in the curated `KNOWN_REAL_EVENTS` map (e.g. `('sp500', 2008): -0.38`, `('nasdaq', 2000): -0.39`)
    - Otherwise `ok = false, reason = 'unvalidated_extreme'`; renderer applies dimmed style + `⚠` glyph + tooltip line "unvalidated extreme — verify against source data"
    - Make the cell tooltip honest: month count, expected count, partial flag, computed return, and any `boundaryAnomaly` / `unvalidated_extreme` reason
    - _Bug_Condition: `isBugCondition1(cell)` — specifically the `extreme` clause_
    - _Expected_Behavior: extreme cells either validated (rendered) or marked suppressed/flagged_
    - _Preservation: legitimate full-year extreme moves in `KNOWN_REAL_EVENTS` continue to render (Requirement 3.9); existing tooltip mechanism preserved (Requirement 3.10)_
    - _Requirements: 2.5_

  - [x] 3.4 Diagnose and harden live fetch in `predator/markets_history.py`
    - In `_fetch_yfinance_series` and `_fetch_fred_series`, log `EMPTY: {provider} returned 0 rows for {series_id}` when the result is empty so the testing strategy can confirm the root-cause hypothesis
    - For the three known-flaky tickers (`^GSPC`, `^NDX`, `NASDAQCOM`), add one bounded retry with 1–2s backoff before degrading to cache
    - In `build_output` (~line 721), when `data is None or data.empty` AND no cache AND `key in existing_assets` AND `existing_assets[key].meta.source` starts with `Mega_Markets_Historical.xlsx`, set sentinel `existing_assets[key].meta._live_holdout = True` before re-emitting; do NOT modify the `source` label (Excel label is honest when no live data is available)
    - _Bug_Condition: `isBugCondition2(asset)` — specifically the live-fetch-empty and cache-freshness-drift root causes (#1, #2 from design)_
    - _Expected_Behavior: `meta.source ∈ {yfinance:*, fred:*}` when live fetch succeeds; otherwise `_live_holdout = true` is surfaced to the verdict block_
    - _Preservation: Dow Jones continues to carry `yfinance:^DJI` (§3.3); Aluminum continues to carry `fred:PALUMUSDM` (§3.4); every non-dual-registry asset retains its live source (§3.5)_
    - _Requirements: 2.6, 2.7_

  - [x] 3.5 Extend verdict block in `predator/build.py`
    - In `markets_freshness.self_living_check` (~line 810), iterate per-asset and build `holdouts = sorted([v["meta"]["name"] for aid, v in assets.items() if aid in DUAL_REGISTRY_EQUITIES AND v["meta"].get("source", "").startswith("Mega_Markets_Historical")])`, where `DUAL_REGISTRY_EQUITIES = {"sp500", "nasdaq", "nasdaq100", "djia", "nikkei", "sensex", "dax"}`
    - Set verdict `LIVE_MERGE_HEALTHY` only when `len(holdouts) == 0 AND live_source_count > excel_only_count`; otherwise `LIVE_MERGE_DEGRADED` and emit the `holdouts` array
    - In `docs/markets.html` verdict-chip `@mouseenter` handler (~line 105), append `· holdouts: ${holdouts.join(', ')}` to the chip tooltip when `holdouts` is non-empty
    - Confirm `predator/ingest_mega_xl.py` is no-touch (delegating shim, design Fix Implementation #12) and `predator/vol_history.py` is no-touch (design Fix Implementation #13)
    - _Bug_Condition: `isBugCondition2(asset)` — specifically the verdict-aggregation-ignores-per-asset-names root cause (#5 from design)_
    - _Expected_Behavior: `verdict.value = "LIVE_MERGE_DEGRADED" AND asset.name ∈ verdict.holdouts` for any genuine live-fetch holdout; `verdict.value = "LIVE_MERGE_HEALTHY"` only when every dual-registry equity carries a live source_
    - _Preservation: chip continues to expose the same three states with the same colour palette (Preservation Requirements); only the holdout naming and the threshold for HEALTHY change_
    - _Requirements: 2.6, 2.7, 2.8_

  - [x] 3.6 Verify bug condition exploration test now passes
    - **Property 1: Expected Behavior** - Partial-Year Inconsistency, Broken Ag Math, and Dual-Registry Equity Live-Merge Holdouts
    - **IMPORTANT**: Re-run the SAME test from task 1 - do NOT write a new test
    - The test from task 1 encodes the expected behavior; when it passes, both `C1` and `C2` are satisfied
    - Run all sub-tests 1a–1f from step 1
    - **EXPECTED OUTCOME**: Test PASSES (confirms ag 2026 cells render dim/correct, equity 2026 cells render dim, sp500/nasdaq/nasdaq100 carry live sources OR are named in `holdouts`, and the verdict reaches `LIVE_MERGE_HEALTHY` only when there are no holdouts)
    - _Requirements: Expected Behavior Properties from design (Property 1, Property 2)_

  - [x] 3.7 Verify preservation tests still pass
    - **Property 2: Preservation** - Full-Year Cells, Metals Partial Cells, Existing Live Sources, and Other Markets Tabs Unchanged
    - **IMPORTANT**: Re-run the SAME tests from task 2 - do NOT write new tests
    - Run all sub-tests 2a–2i from step 2 against the fixed code
    - **EXPECTED OUTCOME**: Tests PASS (confirms full-year cells unchanged, metals partial cells unchanged, Dow Jones / Aluminum / non-dual-registry live sources unchanged, all other markets tabs unchanged, `scraper.py` / scoring code / `vol_history.py` / `ingest_mega_xl.py` untouched, all currently-green tests still green)
    - Confirm all tests still pass after fix (no regressions)

- [x] 4. Checkpoint - Ensure all tests pass
  - Run the full integration pipeline: `python -m predator.ingest_markets_xl --no-fail-on-stale && python -m predator.markets_history --full-refresh && python -m predator.build --source data/all_history.csv --output docs/data --config config.yaml`
  - Run `python -m pytest tests/ -v` and confirm all tests are green (Requirement 3.7)
  - Open `docs/markets.html` against the resulting JSON and visually confirm: 2026 ag cells render dim/correct or are suppressed; 2026 equity cells render dim; 2026 metals cells unchanged; verdict chip names holdouts when degraded
  - Click through asset detail, yield/holdlab, volatility, multi-currency, seasonality, drawdown — confirm no regressions (Requirement 3.6)
  - Ensure all tests pass; ask the user if questions arise

## Notes

- Tasks 1 and 2 MUST be completed BEFORE task 3 — Property 1 must fail on unfixed code and Property 2 must pass on unfixed code; this confirms the bugs exist and captures baseline behavior to preserve.
- Task 1 (Property 1: Bug Condition) and task 3.6 (Property 1: Expected Behavior) re-run the SAME test — the bug-condition exploration test encodes the expected behavior; failure on unfixed code confirms the bug, passing after the fix confirms resolution.
- Task 2 (Property 2: Preservation) and task 3.7 re-run the SAME tests — they pass on unfixed code (capturing baseline) and must still pass after the fix (no regressions).
- Sub-tasks 3.1–3.5 modify three files (`docs/markets.html`, `predator/markets_history.py`, `predator/build.py`); they are sequenced so the rendering layer is hardened before the source/verdict layer to keep the dashboard renderable at every checkpoint.
- `predator/ingest_mega_xl.py` and `predator/vol_history.py` are explicitly no-touch in this fix (design Fix Implementation #12 and #13); preservation test 2f enforces this.
- `scraper.py` and all scoring code are out of scope (Requirement 3.8); preservation test 2f enforces empty `git diff` on those paths.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1", "2"] },
    { "id": 1, "tasks": ["3.1", "3.2", "3.3", "3.4", "3.5"] },
    { "id": 2, "tasks": ["3.6", "3.7"] },
    { "id": 3, "tasks": ["4"] }
  ]
}
```
