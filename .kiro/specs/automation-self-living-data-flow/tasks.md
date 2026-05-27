# Implementation Plan: Automation — Self-Living Data Flow

## Overview

Four targeted changes: fix a `sys.exit(1)` in `vol_history.py`, add a FRED success log in `markets_history.py`, verify `build.py` verdict logic against requirements, and write the full test suite (CI config smoke tests + 8 property-based tests + example-based unit tests).

## Tasks

- [x] 1. Fix `vol_history.py` — replace `sys.exit(1)` with `return {}`
  - In `predator/vol_history.py`, locate the `fetch_all()` function
  - Replace the block `print("\n  Cannot proceed without FRED API key."); sys.exit(1)` with `print("\n  Cannot proceed without FRED API key — returning empty results (soft-skip)."); return {}`
  - Confirm `main()` already handles an empty `results` dict gracefully (it does — it exits early without writing)
  - No other changes to `vol_history.py`
  - _Requirements: 4.7_

  - [ ]* 1.1 Write unit test `test_vol_history_no_sys_exit` in `tests/test_unit_gaps.py`
    - Patch `predator.vol_history._get_fred_client` to return `None`
    - Call `vol_history.fetch_all()` and assert it returns `{}` without raising `SystemExit`
    - _Requirements: 4.7_

- [x] 2. Add FRED client success log to `markets_history.py`
  - In `predator/markets_history.py`, locate `_get_fred_client()`
  - After `return Fred(api_key=api_key)`, split into: `client = Fred(api_key=api_key)`, then a `try/except` block that calls `print("FRED client initialised.")` and silently passes on any exception, then `return client`
  - _Requirements: 1.5_

  - [ ]* 2.1 Write unit test `test_fred_client_logs_success` in `tests/test_unit_gaps.py`
    - Set `FRED_API_KEY` in env, patch `fredapi.Fred` to return a mock object
    - Capture stdout and assert `"FRED client initialised"` appears in output
    - _Requirements: 1.5_

  - [ ]* 2.2 Write unit test `test_fred_client_soft_skip` in `tests/test_unit_gaps.py`
    - Unset `FRED_API_KEY` from env
    - Call `_get_fred_client()` and assert it returns `None` without raising `SystemExit`
    - Assert `"ERROR: FRED_API_KEY environment variable not set"` appears in captured stdout
    - _Requirements: 1.6_

- [x] 3. Verify `build.py` verdict logic against requirements 2.1–2.8
  - Read `predator/build.py` lines 800–957 (the `self_living_check` block)
  - Confirm the verdict policy exactly matches: `HEALTHY` when `not holdout_keys and live_source_count > excel_only_count`; `DEGRADED` when `live_source_count > 0`; `FAILED` otherwise
  - Confirm `ASSET_REGISTRY` is imported with a graceful `ImportError` fallback
  - Confirm `live_eligible_keys` is derived from the registry (not a hardcoded list)
  - Confirm `holdouts` (display names) and `holdout_keys` (asset IDs) are both written
  - If any gap is found, fix it in `predator/build.py`; if all correct, no change needed
  - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8_

  - [ ]* 3.1 Write unit test `test_verdict_failed_no_live_sources` in `tests/test_unit_gaps.py`
    - Construct a minimal `market_returns.json`-shaped dict where all assets have `meta.source` starting with `Mega_Markets_Historical`
    - Call the verdict computation logic and assert verdict is `LIVE_MERGE_FAILED` and `live_source_count == 0`
    - _Requirements: 2.6_

  - [ ]* 3.2 Write unit test `test_build_without_market_returns` in `tests/test_unit_gaps.py`
    - Run `predator.build` with a temp output dir and no `market_returns.json` present
    - Assert `metadata.json` is written with `markets_data_freshness.available == False`
    - Assert the build does not raise an exception
    - _Requirements: 7.6_

- [x] 4. Write `tests/test_unit_gaps.py` — remaining example-based unit tests
  - Add `test_yfinance_flaky_ticker_retry`: patch `yfinance.download` to return empty on first call and non-empty on second; assert one retry fires for a ticker in `_YFINANCE_FLAKY_TICKERS`
  - Add `test_stale_etf_warning_annotation`: construct a `leaderboard` DataFrame with a stale `Holdings_As_Of` date; assert `::warning::` appears in captured stdout from the build step
  - Ensure all tests in this file are importable and pass with `python -m pytest tests/test_unit_gaps.py -v`
  - _Requirements: 6.2, 5.6_

- [x] 5. Checkpoint — ensure tasks 1–4 pass
  - Run `python -m pytest tests/test_unit_gaps.py -v` and confirm all tests pass
  - Ask the user if any questions arise before proceeding to property-based tests

- [x] 6. Write `tests/test_ci_config.py` — YAML smoke tests for workflow files
  - Load `.github/workflows/build_site.yml` and `.github/workflows/daily_scrape.yml` with `yaml.safe_load`
  - Implement the following 9 tests:
    - `test_markets_history_step_has_fred_key`: assert the `2/2 — Live FRED + yfinance merge` step has `env.FRED_API_KEY`
    - `test_vol_history_step_has_fred_key`: assert the `Fetch vol history` step has `env.FRED_API_KEY`
    - `test_fetch_fred_step_has_fred_key`: assert the `Fetch FRED data` step has `env.FRED_API_KEY`
    - `test_external_steps_have_continue_on_error`: assert all five external-data steps carry `continue-on-error: true`
    - `test_build_step_no_continue_on_error`: assert the `Build site artifacts` step does NOT carry `continue-on-error: true`
    - `test_run_tests_step_no_continue_on_error`: assert the `Run tests` step does NOT carry `continue-on-error: true`
    - `test_daily_scrape_has_cron_schedule`: assert `daily_scrape.yml` has a `schedule` trigger with at least one cron entry
    - `test_build_site_has_workflow_run_trigger`: assert `build_site.yml` has a `workflow_run` trigger
    - `test_git_pull_rebase_present`: assert `daily_scrape.yml` contains `git pull --rebase` in a run step
  - _Requirements: 1.1, 1.2, 1.3, 4.1, 4.2, 4.3, 4.4, 4.5, 7.4, 7.5, 3.7, 3.8, 7.2_

- [x] 7. Write `tests/test_verdict_logic.py` — PBT for Properties 1, 2, 3
  - Define Hypothesis strategies: `asset_with_live_source_strategy()` (generates asset dicts with `meta.source` starting with `yfinance:` or `fred:`), `asset_with_excel_source_strategy()` (generates asset dicts with `meta.source` starting with `Mega_Markets_Historical`), `asset_strategy()` (union of both)
  - Extract or inline the verdict computation logic from `build.py` as a pure function `compute_self_living_check(market_returns_dict, live_eligible_keys)` for testability
  - Define `LIVE_ELIGIBLE_KEYS` as a fixed set of 5–10 asset keys drawn from `ASSET_REGISTRY` (those with `source_type in {yfinance, fred}` and `return_type != fx`)

  - [x]* 7.1 Write property test for Property 1 (Verdict HEALTHY invariant)
    - **Property 1: Verdict HEALTHY invariant**
    - **Validates: Requirements 2.1, 2.2, 2.3**
    - `@given(st.lists(asset_with_live_source_strategy(), min_size=1))` with `@settings(max_examples=100)`
    - Assert `verdict == "LIVE_MERGE_HEALTHY"`, `holdouts == []`, `live_source_count > excel_only_count`

  - [x]* 7.2 Write property test for Property 2 (Verdict DEGRADED invariant)
    - **Property 2: Verdict DEGRADED invariant**
    - **Validates: Requirements 2.4, 2.5**
    - `@given(st.lists(asset_with_live_source_strategy(), min_size=1), st.lists(asset_with_excel_source_strategy(), min_size=1))` with `@settings(max_examples=100)`
    - Assert `verdict == "LIVE_MERGE_DEGRADED"`, `holdout_keys` equals the set of Excel-source asset keys that are in `LIVE_ELIGIBLE_KEYS`

  - [x]* 7.3 Write property test for Property 3 (Source-label honesty) — verdict side
    - **Property 3: Source-label honesty**
    - **Validates: Requirements 3.4, 5.5**
    - `@given(asset_strategy())` with `@settings(max_examples=100)`
    - Assert: if `meta.source` starts with `yfinance:` or `fred:`, asset key is NOT in `holdout_keys`; if `meta.source` starts with `Mega_Markets_Historical` and key is in `LIVE_ELIGIBLE_KEYS`, key IS in `holdout_keys`

- [x] 8. Write `tests/test_source_labels.py` — PBT for Properties 3 (ingest side) and 4
  - Import `ingest_markets_xl.build_output` (or the relevant merge function) for Property 4
  - Define `monthly_data_strategy()` generating lists of `["YYYY-MM", float]` pairs

  - [x]* 8.1 Write property test for Property 4 (Excel ingest preserves live source labels)
    - **Property 4: Excel ingest preserves live source labels**
    - **Validates: Requirements 3.5**
    - `@given(asset_with_live_source_strategy(), monthly_data_strategy())` with `@settings(max_examples=100)`
    - Construct an existing `market_returns.json`-shaped dict with the live-source asset; call `build_output_with_merge(new_monthly, existing, merge=True)`; assert the output asset's `meta.source` is unchanged and does not start with `Mega_Markets_Historical`

- [x] 9. Write `tests/test_asof.py` — PBT for Property 5
  - Define `asset_strategy()` generating asset dicts with a `meta.last` field set to a valid `YYYY-MM` string

  - [x]* 9.1 Write property test for Property 5 (`asof` advances to maximum last date)
    - **Property 5: asof advances to maximum last date**
    - **Validates: Requirements 3.6**
    - `@given(st.lists(asset_strategy(), min_size=1))` with `@settings(max_examples=100)`
    - Build a `market_returns.json`-shaped dict from the generated assets; assert `mr["asof"] == max(a["meta"]["last"] for a in assets)`

- [x] 10. Write `tests/test_fred_retry.py` — PBT for Property 6
  - Import `_fetch_fred_series` from `predator.markets_history`
  - Use `unittest.mock.patch` to replace `time.sleep` (no actual sleeping) and mock `fred.get_series`

  - [x]* 10.1 Write property test for Property 6 (FRED retry exponential backoff)
    - **Property 6: FRED retry exponential backoff**
    - **Validates: Requirements 6.1, 6.3**
    - `@given(st.integers(min_value=1, max_value=5))` with `@settings(max_examples=100)`
    - Mock `get_series` to raise `Exception("429 Too Many Requests")` for the first `n_failures` calls, then return a valid Series; assert total call count equals `n_failures + 1` and result is non-empty

- [x] 11. Write `tests/test_cache_fallback.py` — PBT for Property 7
  - Import `_write_cache`, `_read_cache`, and `build_output` from `predator.markets_history`
  - Use a `tmp_path` fixture (pytest) to isolate cache writes from the real `CACHE_DIR`

  - [x]* 11.1 Write property test for Property 7 (Cache fallback data appears in output)
    - **Property 7: Cache fallback data appears in output**
    - **Validates: Requirements 6.4**
    - `@given(asset_key_strategy(), monthly_data_strategy())` with `@settings(max_examples=100)`
    - Write a parquet cache for the asset key; call `build_output({})` (empty live results); assert the asset key appears in `output["assets"]` with the cached monthly data

- [x] 12. Write `tests/test_fault_isolation.py` — PBT for Property 8
  - Import `fetch_all` and `ASSET_REGISTRY` from `predator.markets_history`
  - Define `exception_strategy()` generating common exception types (`ValueError`, `RuntimeError`, `OSError`, `TimeoutError`)

  - [x]* 12.1 Write property test for Property 8 (Per-series exception isolation)
    - **Property 8: Per-series exception isolation**
    - **Validates: Requirements 4.8**
    - `@given(st.sampled_from(list(ASSET_REGISTRY)), exception_strategy())` with `@settings(max_examples=100)`
    - Patch the fetch dispatch in `fetch_all` so the selected series raises the given exception; call `fetch_all(full_refresh=True)`; assert no exception propagates and the function returns a dict (possibly empty for the failing series)

- [x] 13. Final checkpoint — run full test suite
  - Run `python -m pytest tests/ -v` and confirm all tests pass (or document any expected skips)
  - Ensure all tests in `test_unit_gaps.py`, `test_ci_config.py`, `test_verdict_logic.py`, `test_source_labels.py`, `test_asof.py`, `test_fred_retry.py`, `test_cache_fallback.py`, and `test_fault_isolation.py` are collected and pass
  - Ask the user if any questions arise

## Notes

- Tasks marked with `*` are optional and can be skipped for a faster MVP
- Tasks 1 and 2 are the only production code changes; all other tasks are tests
- Task 3 is a read-and-verify step — the verdict logic in `build.py` is already correct per the design document, but must be confirmed before writing tests that depend on it
- Property tests use Hypothesis (already installed — `.hypothesis/` directory present in repo)
- All property tests use `@settings(max_examples=100)` explicitly
- Each property test file carries the tag comment `# Feature: automation-self-living-data-flow, Property N: <property_text>` at the top of each test function
- The `compute_self_living_check` helper extracted for testing in task 7 should be a pure function that mirrors the logic in `build.py` exactly — do not modify `build.py` to extract it; test it by replicating the logic in the test file

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1", "2", "3"] },
    { "id": 1, "tasks": ["1.1", "2.1", "2.2", "3.1", "3.2"] },
    { "id": 2, "tasks": ["4"] },
    { "id": 3, "tasks": ["6", "7", "8", "9", "10", "11", "12"] },
    { "id": 4, "tasks": ["7.1", "7.2", "7.3", "8.1", "9.1", "10.1", "11.1", "12.1"] },
    { "id": 5, "tasks": ["13"] }
  ]
}
```
