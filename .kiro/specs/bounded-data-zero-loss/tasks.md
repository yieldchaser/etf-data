# Implementation Plan: Bounded Data, Zero Loss

## Overview

This plan hardens the storage contract of the Predator Protocol ETF data repository in four parts:

- **Part B (foundation)**: a one-line `encoding="utf-8"` fix to the `_run_migrate` subprocess helper unblocks the two existing failing tests on Windows. This must land first because every Part A test depends on it.
- **Part A**: three new example-based tests (`test_year_rollover_simulation`, `test_zero_loss_reconstruction`, `test_append_only_contract`) plus three Hypothesis property tests (Properties 1–3) that machine-verify the byte-stability, zero-loss, and append-only contracts of the year-partitioned Parquet store.
- **Part C.1**: track `*.xlsx` via Git LFS by creating `.gitattributes` and adding an `_is_lfs_pointer` detection helper to `predator/ingest_markets_xl.py` so a clone without LFS fails fast with a clear LFS-specific error rather than a confusing `BadZipFile`.
- **Part C.2**: shrink `holdings_latest.json` (~45% reduction) by rounding numeric precision to 4 decimal places and dropping the unreferenced `rank_mult` column, plus tighten `flag_history.json` by omitting zero-valued `vs` entries; Properties 4–6 pin precision, key allowlists, and consumer-field presence.
- **Part D**: documentation-only update to the `.gitignore` comment block to reference the design doc as the authoritative size analysis (Req 7.5, 8.7) and note that `.gitattributes` now exists (Req 5.4).

Sacred guards from Requirement 9 are honored throughout: no task touches `scraper.py`, `scripts/etf_holdings_scraper_v42.py`, `predator/scoring.py`, or the `Dedup_Key` tie-break logic in `migrate_to_parquet.py`.

The one-time `git lfs migrate import --include="*.xlsx" --everything` step (Req 5.5) is a destructive history rewrite that MUST be performed manually by the maintainer — it is intentionally NOT a coding task and is documented in the Notes section below.

## Tasks

- [x] 1. Part B — Fix subprocess encoding for Windows compatibility
  - [x] 1.1 Add `encoding="utf-8"` to `_run_migrate` in `tests/test_parquet_immutability.py`
    - Insert `encoding="utf-8"` as a keyword argument to the `subprocess.run(...)` call inside the existing `_run_migrate` helper alongside `text=True`
    - Do NOT modify `scripts/migrate_to_parquet.py` (script already reconfigures stdout to UTF-8 on Windows; fix is consumer-side only)
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5_

- [x] 2. Part A — Year-rollover proof example tests
  - [x] 2.1 Add `test_year_rollover_simulation` to `tests/test_parquet_immutability.py`
    - Use fixed years 2026 and 2027 in a `tmp_path` sandbox
    - Step 1: write CSV with only `year=2026` rows; run migrate; assert manifest records SHA-256 for `year=2026`
    - Step 2: record SHA-256 and byte size of `year=2026/holdings.parquet`
    - Step 3: extend CSV with `year=2027` rows; run migrate again
    - Step 4: assert SHA-256 and byte size of `year=2026/holdings.parquet` are byte-identical
    - Step 5: assert `"LOCKED"` appears in the second run's stdout for `year=2026`
    - Step 6: assert `year=2027/holdings.parquet` was created
    - Skip with clear message when wall-clock year < 2027 (immutability guard requires `year < current_year`)
    - On SHA mismatch, emit partition year + expected/actual SHA-256[:16] before failing (Req 1.4)
    - On success, emit a confirmation message that the partition is byte-identical (Req 1.5)
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5_

  - [x] 2.2 Add `test_zero_loss_reconstruction` to `tests/test_parquet_immutability.py`
    - Use a `tmp_path` sandbox with a multi-year source CSV (`year=2025`: 5+ rows, `year=2026`: 7+ rows)
    - Run migrate to populate the sandboxed Parquet store
    - Concatenate every `year=*/holdings.parquet` file into a single DataFrame
    - Apply `Dedup_Key` `(ETF_Ticker, ticker, Holdings_As_Of)` with `keep="last"` to BOTH source CSV and reconstructed DataFrame
    - Assert deduplicated row counts are equal
    - Assert set of Dedup_Key tuples in reconstructed equals set in source (every key present)
    - On missing keys, list up to 20 examples in the failure message (Req 2.4)
    - Fail loudly with explicit empty-side identification if either dataset is empty (Req 2.6)
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6_

  - [x] 2.3 Add `test_append_only_contract` and `_real_store_snapshot` helper to `tests/test_parquet_immutability.py`
    - Add module-level helper `_real_store_snapshot() -> dict[Path, float]` that returns mtime map of all files under the real `PARQUET_STORE` glob (returns `{}` when store does not exist)
    - In the test: bootstrap with past-year rows; assert past-year manifest entry exists, fail immediately with "Bootstrap failed to create past-year partition" if not (Req 3.1)
    - Record past-year SHA-256, byte size, row count, and manifest entry
    - Snapshot real store mtimes before sandboxed work; assert unchanged at teardown (Req 3.5)
    - Append current-year rows; re-run migrate
    - Run TWO independent checks: SHA-256 unchanged AND row count unchanged; report distinct `FAILED: sha256` vs `FAILED: row-count` (Req 3.3)
    - Assert manifest `generated_at` advanced after the append (Req 3.4)
    - Assert current-year partition row count grew by the count of net-new unique Dedup_Key tuples (Req 3.2)
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5_

- [ ] 3. Part A — Property-based tests for the storage contract
  - [ ]* 3.1 Add Hypothesis property test for past-year byte-stability under year-rollover append
    - **Property 1: Past-year partition byte-stability under year-rollover append**
    - **Validates: Requirements 1.1, 1.2, 1.3**
    - Add to `tests/test_parquet_immutability.py` using a `@composite` strategy that draws `R_past` (rows for past year) and `R_new` (rows for a strictly-greater year)
    - Use `@hypothesis.settings(max_examples=100, deadline=None)` (subprocess + parquet round-trip exceeds default deadline)
    - Tag with comment: `# Feature: bounded-data-zero-loss, Property 1: Past-year partition byte-stability under year-rollover append`
    - Assert past-year `holdings.parquet` SHA-256 and byte size identical between run 1 and run 2
    - Assert `"LOCKED"` substring appears in run-2 stdout referencing the past year
    - Assert real `data/history_parquet/` mtimes unchanged (Req 1.3)

  - [ ]* 3.2 Add Hypothesis property test for zero-loss reconstruction from Parquet partitions
    - **Property 2: Zero-loss reconstruction from Parquet partitions**
    - **Validates: Requirements 2.1, 2.2, 2.3, 2.5, 9.5**
    - Add to `tests/test_parquet_immutability.py` using the `multi_year_csv` `@composite` strategy from the design (≥2 distinct years, ≥5 rows/year)
    - Use `@hypothesis.settings(max_examples=100, deadline=None)`
    - Tag with comment: `# Feature: bounded-data-zero-loss, Property 2: Zero-loss reconstruction from Parquet partitions`
    - For every drawn CSV, run sandboxed migrate, concatenate partitions, dedup both sides on Dedup_Key with `keep="last"`
    - Assert deduplicated row counts equal AND set of Dedup_Key tuples equal
    - Assert real `data/history_parquet/` mtimes unchanged

  - [ ]* 3.3 Add Hypothesis property test for the append-only contract
    - **Property 3: Append-only contract (past-year immutability + current-year growth + manifest progression)**
    - **Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5, 9.5**
    - Add to `tests/test_parquet_immutability.py` using `@composite` strategies that draw non-empty `R_past` and non-empty `R_current`
    - Use `@hypothesis.settings(max_examples=100, deadline=None)`
    - Tag with comment: `# Feature: bounded-data-zero-loss, Property 3: Append-only contract`
    - Assert past-year SHA-256 unchanged AND past-year row count unchanged after current-year append
    - Assert current-year partition rows == count of unique Dedup_Key tuples in `R_current`
    - Assert manifest `generated_at` advanced AND manifest's current-year `rows` matches on-disk row count
    - Assert real `data/history_parquet/` mtimes unchanged

- [x] 4. Checkpoint — Parts A and B verified
  - Ensure all tests pass, ask the user if questions arise.

- [x] 5. Part C.1 — Track Mega_Markets_Historical.xlsx via Git LFS
  - [x] 5.1 Create `.gitattributes` at the repository root
    - File contents: `*.xlsx filter=lfs diff=lfs merge=lfs -text`
    - Single line; pattern intentionally matches all future `.xlsx` files (not just the one current blob)
    - _Requirements: 5.1, 5.5_

  - [x] 5.2 Add `_is_lfs_pointer` helper and pointer-detection error to `predator/ingest_markets_xl.py`
    - Add module-level helper `_is_lfs_pointer(path: Path) -> bool` that returns `True` when the file is ≤1024 bytes AND its first 64 bytes start with `b"version https://git-lfs.github.com/spec/"`
    - Wire the check into the Excel-loading path BEFORE `pandas.read_excel` is invoked
    - On detection, raise `RuntimeError("Mega_Markets_Historical.xlsx is a Git LFS pointer, not a real Excel file. Run 'git lfs install && git lfs pull'.")`
    - This is the single error path for clone-without-LFS, LFS-server-unreachable, and LFS-quota-exhausted scenarios (all leave a pointer file in place)
    - Do NOT modify any path references; the file path remains `Mega_Markets_Historical.xlsx`
    - _Requirements: 5.2, 5.3_

  - [ ]* 5.3 Add unit tests for LFS pointer detection in `tests/test_lfs_pointer.py`
    - New file `tests/test_lfs_pointer.py`
    - Test 1: `_is_lfs_pointer` returns `True` for a synthetic 130-byte file starting with `b"version https://git-lfs.github.com/spec/v1\n..."` written to `tmp_path`
    - Test 2: `_is_lfs_pointer` returns `False` for a real binary blob (e.g., a 2 KB random-bytes file)
    - Test 3: `_is_lfs_pointer` returns `False` for a small text file that does NOT start with the pointer header
    - Test 4: When `_is_lfs_pointer` returns `True`, the ingest entry point raises `RuntimeError` whose message contains `"Git LFS pointer"` and the remediation hint `"git lfs install && git lfs pull"`
    - _Requirements: 5.2, 5.3_

- [x] 6. Part C.2 — Reduce JSON payload size in `predator/build.py`
  - [x] 6.1 Trim `holdings_latest.json` emit: drop `rank_mult` and round numeric precision to 4 decimal places
    - In the `holdings_latest.json` emit block (around lines 622–649 of `predator/build.py`), remove `"rank_mult"` from the column projection list
    - Before the JSON write, apply `.round(4)` to columns: `weight`, `base_score`, `new_bonus`, `score`, and every `weight_flow*` column (e.g., `weight_flow`, `weight_flow_7d`, `weight_flow_30d`)
    - Do NOT round `rank` or any `rank_delta*` integer columns
    - Do NOT remove any column referenced by `docs/index.html`, `docs/stock.html`, `docs/markets.html`, `docs/sim.html`, or any JS file under `docs/`
    - Verify with a quick `grep` over `docs/` that `rank_mult` is not referenced before deletion
    - _Requirements: 6.2, 6.4, 6.5, 6.6_

  - [x] 6.2 Tighten `flag_history.json` emit: omit zero-valued `vs` entries
    - In the `flag_history` build block (around lines 582–604 of `predator/build.py`), only assign `entry["vs"]` when `has_vs` AND the rounded value is non-zero
    - Preserve the always-present keys `{"d", "flag", "rank"}` (Property 5 minimum allowlist)
    - `entry["burst"]` continues to be assigned only when `has_burst`
    - Do NOT add any new keys to the entry dict (Property 5 maximum allowlist is `{"d", "flag", "rank", "vs", "burst"}`)
    - _Requirements: 6.3, 6.5, 6.6_

  - [ ]* 6.3 Add Hypothesis property test for `holdings_latest.json` weight precision in `tests/test_docs_payload.py`
    - **Property 4: Holdings JSON weight precision bound**
    - **Validates: Requirements 6.2**
    - New file `tests/test_docs_payload.py`
    - Use a `@composite` strategy generating a small DataFrame of holdings rows with arbitrary float `weight` values in `[0.0, 1.0]`
    - Invoke the `holdings_latest.json` emit pathway against `tmp_path` (extract or import the emit logic; or write the DataFrame through `_dumps` mirroring `build.py`)
    - For every record in the resulting JSON, parse `weight` and assert `weight == round(weight, 4)` within `1e-9` floating-point tolerance
    - Use `@hypothesis.settings(max_examples=100)`
    - Tag with comment: `# Feature: bounded-data-zero-loss, Property 4: Holdings JSON weight precision bound`

  - [ ]* 6.4 Add Hypothesis property test for `flag_history.json` key allowlist in `tests/test_docs_payload.py`
    - **Property 5: Flag-history JSON key allowlist**
    - **Validates: Requirements 6.3**
    - Use a `@composite` strategy generating synthetic historical leaderboard snapshots
    - Invoke the `flag_history.json` emit pathway against `tmp_path`
    - For every entry in every per-ticker array, assert `set(entry.keys()) <= {"d", "flag", "rank", "vs", "burst"}` AND `{"d", "flag", "rank"} <= set(entry.keys())`
    - Use `@hypothesis.settings(max_examples=100)`
    - Tag with comment: `# Feature: bounded-data-zero-loss, Property 5: Flag-history JSON key allowlist`

  - [ ]* 6.5 Add Hypothesis property test for docs JSON consumer-field presence in `tests/test_docs_payload.py`
    - **Property 6: Docs JSON consumer-field presence**
    - **Validates: Requirements 6.6**
    - Statically extract the set of field names referenced from `docs/**/*.html` and `docs/**/*.js` matched by patterns `r\.\w+`, `row\.\w+`, `entry\.\w+`, destructuring assignments, and template-literal interpolations
    - For a generated synthetic input (one record sufficient), invoke each emit pathway (`leaderboard.json`, `holdings_latest.json`, `flag_history.json`) and assert every referenced field name appears as a key in at least one record of the corresponding output file
    - Use `@hypothesis.settings(max_examples=25)` (the static field set is the variation surface; generation is mainly for edge cases)
    - Tag with comment: `# Feature: bounded-data-zero-loss, Property 6: Docs JSON consumer-field presence`

- [x] 7. Part D — Update `.gitignore` comment block to reference the storage contract
  - [x] 7.1 Update the `.gitignore` comment block describing the Parquet_Store commit status
    - Preserve the existing comment that `data/history_parquet/` is committed (NOT ignored) — it is the permanent archive (Req 7.2)
    - Verify `.gitignore` lists `data/markets_history/`, `data/vol_history/`, `data/markets/` as ignored rebuild caches (Req 7.1); add any that are missing
    - Update the comment block to reference `.kiro/specs/bounded-data-zero-loss/design.md` as the authoritative source of the storage contract and 5-year size analysis (Req 7.5, 8.7)
    - Update the existing `Mega_Markets_Historical.xlsx` LFS comment to note that `.gitattributes` now exists at the repo root (Req 5.4)
    - Do NOT remove any existing ignore patterns
    - _Requirements: 5.4, 7.1, 7.2, 7.5, 8.7_

- [x] 8. Final checkpoint — Full test suite green and storage contract verified
  - Ensure all tests pass, ask the user if questions arise.
  - Run `python -m pytest tests/ -v` and confirm exit code 0 (Req 9.4)
  - Confirm no individual test file produced setup, collection, or configuration errors (Req 9.4)
  - Confirm the implementation modified none of: `scraper.py`, `scripts/etf_holdings_scraper_v42.py`, `predator/scoring.py`, the `Dedup_Key` tie-break in `migrate_to_parquet.py` (Req 9.1, 9.2, 9.3)
  - Confirm no new mechanism drops historical rows for size bounding (Req 9.5)

## Notes

- Tasks marked with `*` are optional property/unit tests and can be skipped for a faster MVP; core implementation tasks are never optional.
- Each task references specific requirement clauses for traceability; property tests additionally cite the property they validate from the design document.
- Properties 1–3 live alongside the example tests in `tests/test_parquet_immutability.py` so a single file pins the storage contract; Properties 4–6 live in the new `tests/test_docs_payload.py` so the dashboard payload contract has its own narrow test home.
- Hypothesis tests use `deadline=None` because subprocess invocations of `migrate_to_parquet.py` plus parquet round-trips can exceed the default 200 ms deadline on cold caches.
- Sacred guards: no task touches `scraper.py`, `scripts/etf_holdings_scraper_v42.py`, `predator/scoring.py`, or the `Dedup_Key` tie-break logic in `migrate_to_parquet.py`. The encoding fix is intentionally consumer-side (`_run_migrate` helper only).
- **Final test-suite status (post-implementation)**: 236 passed / 6 skipped / 0 failed (exit code 0). Req 9.4 satisfied. Sacred guards (`scraper.py`, `etf_holdings_scraper_v42.py`, `predator/scoring.py`, Dedup_Key tie-break) all confirmed untouched.
- **Manual maintainer step (NOT a coding task)**: After task 5.1 lands, the maintainer must perform the one-time destructive history rewrite from Req 5.5:
  ```bash
  git lfs track "*.xlsx"           # already done by task 5.1 via .gitattributes
  git add .gitattributes
  git lfs migrate import --include="*.xlsx" --everything
  git push --force-with-lease origin main
  git lfs push --all origin
  ```
  This rewrites every commit that touched the Excel blob and requires `--force-with-lease`. It is intentionally excluded from the task list because it is a destructive git history operation that must be reviewed and executed manually with explicit approval.
- Part D (the 5-year growth projection) is documented in `design.md` (the "Data Models → 5-year footprint model" section). No additional documentation file is created; the design document IS the deliverable for Req 8.6.
- **Future-pass note — current-year partition write atomicity**: The current-year partition write in `scripts/migrate_to_parquet.py` is not specified as atomic (no temp-file + rename). Out of scope for this spec because the migrate write logic is pre-existing and frozen-adjacent under the Req 9 sacred guards, but flagged here as a known soft spot for a future pass. A crash mid-write could theoretically corrupt the current-year partition; the past-year immutability guarantee is unaffected.
- **Future-pass note — Hypothesis property layer remains optional**: Tasks 3.1, 3.2, 3.3 (Properties 1–3 in `tests/test_parquet_immutability.py`), tasks 6.3, 6.4, 6.5 (Properties 4–6 in `tests/test_docs_payload.py`), and task 5.3 (`tests/test_lfs_pointer.py`) remain optional MVP-deferrable items. Implementation can land without them — and has, per the test-suite status above — but they are the natural next layer of test coverage and should be the first things picked up in a follow-up pass that wants stronger machine-verified guarantees.
- **Cross-spec note — `predator/vol_history.py` baseline drift**: A pre-existing baseline drift in `predator/vol_history.py` (the `sys.exit(1)` → `return {}` soft-skip introduced by `automation-self-living-data-flow` task 1, Req 4.7) was tripping the `test_2f_vol_history_unchanged` guard in `markets-partial-year-and-live-merge-fix`. During this spec's session, the guard test was updated to a surgical carve-out that accepts ONLY that exact two-line swap and fails loudly on any other diff. `predator/vol_history.py` itself was NOT modified. This note is preserved here so future readers of either spec understand why the guard test has a carve-out and that the carve-out is intentional and tightly scoped.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "5.1", "5.2", "6.1"] },
    { "id": 1, "tasks": ["2.1", "5.3", "6.2", "7.1"] },
    { "id": 2, "tasks": ["2.2", "6.3"] },
    { "id": 3, "tasks": ["2.3", "6.4"] },
    { "id": 4, "tasks": ["3.1", "6.5"] },
    { "id": 5, "tasks": ["3.2"] },
    { "id": 6, "tasks": ["3.3"] }
  ]
}
```
