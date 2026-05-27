# Requirements Document

## Introduction

The Predator Protocol ETF data repository must remain healthy indefinitely: bounded in committed size, provably zero-loss in historical holdings data, and sustainable at a 5-year growth horizon. This spec hardens the data layer storage contract across four concerns:

- **Part A — Year-Rollover Proof**: The immutability guard in `scripts/migrate_to_parquet.py` has never been exercised across a real year boundary. Three new tests prove the contract holds: a synthetic year-rollover simulation, a zero-loss reconstruction test, and an append-only contract test.
- **Part B — Subprocess Encoding Fix**: Two existing tests in `test_parquet_immutability.py` fail on Windows because `subprocess.run(text=True)` uses the system codepage (cp1252), which cannot decode the UTF-8 emoji in the migrate script's stdout. The fix is to pass `encoding='utf-8'` explicitly.
- **Part C — Bounded Footprint**: The committed `docs/data/` payload is ~13.6 MB today. Large JSON files (`leaderboard.json` 3.88 MB, `flag_history.json` 3.52 MB, `holdings_latest.json` 3.38 MB) are candidates for size reduction without dropping any data the dashboard needs. The 10.62 MB Excel file must be moved to Git LFS.
- **Part D — Growth Projection**: At ~1,107 rows/day, the parquet archive grows ~2.5 MB/year per partition. The 5-year projection (~12.5 MB parquet + ~7.8 MB rolling CSV + bounded JSON) is confirmed sustainable within GitHub's 1 GB repo and Pages limits.

This spec is complementary to `automation-self-living-data-flow`, which covers pipeline resilience. This spec covers only the storage contract, test coverage, and committed-size management.

---

## Glossary

- **Parquet_Store**: The year-partitioned archive at `data/history_parquet/year=YYYY/holdings.parquet`, committed to Git and never gitignored.
- **Manifest**: `data/history_parquet/CHECKSUMS.json` — the SHA-256 manifest recording the hash and row count of every partition.
- **Past_Year_Partition**: Any partition where `year < current_year`. Contractually immutable once the Manifest records it.
- **Current_Year_Partition**: The partition where `year == current_year`. Mutable; grows daily as new scrapes land.
- **Immutability_Guard**: The logic in `migrate_to_parquet.py` that refuses to overwrite a Past_Year_Partition unless `--allow-historical-rewrite` is passed.
- **Dedup_Key**: The composite key `(ETF_Ticker, ticker, Holdings_As_Of)` with `keep="last"` tie-break used to deduplicate holdings rows.
- **Rolling_CSV**: `data/all_history.csv` — a rolling recent window (120-day lookback) used as the migration source. Not the permanent archive.
- **Reconstruction**: Concatenating all Parquet_Store partitions to reproduce the full holdings history without touching the Rolling_CSV.
- **Append_Only_Contract**: The invariant that new scrapes only add rows to the Current_Year_Partition via the Dedup_Key; they never rewrite, prune, truncate, or reorder a Past_Year_Partition.
- **Git_LFS**: Git Large File Storage — stores large binary blobs outside the main object store so they do not bloat every clone.
- **Migrate_Script**: `scripts/migrate_to_parquet.py` — the sole writer of the Parquet_Store.
- **Test_Suite**: `python -m pytest tests/ -v` — the full test suite that must remain green after all changes.
- **Docs_Data_Payload**: The set of files under `docs/data/` that are committed to Git and served by GitHub Pages.
- **Per_Ticker_Detail**: The lazy-loaded per-ticker JSON files under `docs/data/details/` (already implemented for `stock.html`).

---

## Requirements

### Requirement 1: Year-Rollover Simulation Test

**User Story:** As the repository maintainer, I want a test that synthetically simulates a year rollover and proves the 2026 partition is byte-identical after 2027 data is appended, so that the immutability contract is proven rather than merely asserted.

#### Acceptance Criteria

1. WHEN the year-rollover simulation test runs, THE Test_Suite SHALL: (a) run the Migrate_Script with a source CSV containing only `year=2026` rows and assert that the Manifest exists and records a SHA-256 for the `year=2026` partition; (b) compute the SHA-256 of the `year=2026` partition file on disk; (c) run the Migrate_Script again with the same source CSV extended with `year=2027` rows; (d) assert that the SHA-256 of the `year=2026` partition file on disk after step (c) equals the SHA-256 computed in step (b).
2. WHEN the Migrate_Script processes a source CSV containing both `year=2026` and `year=2027` rows and a `year=2026` partition already exists in the Manifest, THE Migrate_Script SHALL emit a line containing `LOCKED` in stdout for the `year=2026` partition, and the `year=2026` partition file's SHA-256 and byte size SHALL be unchanged after the second run.
3. THE year-rollover simulation test SHALL use a `tmp_path` sandbox; THE test SHALL NOT write to or read from the real `data/history_parquet/` store path during execution.
4. WHEN the year-rollover simulation test detects that the `year=2026` partition SHA-256 has changed, THE Test_Suite SHALL emit to stdout the partition year, the expected SHA-256 (first 16 hex chars), and the actual SHA-256 (first 16 hex chars), then fail the test.
5. WHEN the year-rollover simulation test confirms that the `year=2026` partition SHA-256 is unchanged, THE Test_Suite SHALL emit to stdout a confirmation message stating that the partition is byte-identical.

---

### Requirement 2: Zero-Loss Reconstruction Test

**User Story:** As the repository maintainer, I want a test that reconstructs the full holdings history from Parquet partitions alone and proves zero row loss versus the source CSV, so that I can confirm the archive is the authoritative record.

#### Acceptance Criteria

1. WHEN the reconstruction test runs, THE Test_Suite SHALL: (a) run the Migrate_Script against a multi-year source CSV to populate the sandbox Parquet_Store; (b) concatenate all partition files in the sandbox Parquet_Store; (c) apply the Dedup_Key to both the concatenated partitions and the source CSV independently; (d) assert that the deduplicated row count of the concatenated partitions equals the deduplicated row count of the source CSV.
2. WHEN the reconstruction test compares the reconstructed dataset to the source CSV, THE Test_Suite SHALL compare on the Dedup_Key columns `(ETF_Ticker, ticker, Holdings_As_Of)` and assert that every unique key present in the deduplicated source CSV is present in the deduplicated reconstructed dataset.
3. THE reconstruction test SHALL use a `tmp_path` sandbox with at least two synthetic year partitions (e.g., `year=2025` and `year=2026`), each containing at least 5 rows, to exercise the multi-partition concatenation path.
4. IF any Dedup_Key present in the source CSV is absent from the reconstructed dataset, THEN THE Test_Suite SHALL fail immediately with a message listing the missing Dedup_Key values (up to 20 examples).
5. THE reconstruction test SHALL NOT depend on the real `data/all_history.csv` or `data/history_parquet/` store; it SHALL be fully self-contained in `tmp_path`.
6. IF the source CSV or the reconstructed dataset is empty when the comparison is performed, THEN THE Test_Suite SHALL fail immediately with a message identifying which dataset is empty, rather than passing vacuously.

---

### Requirement 3: Append-Only Contract Test

**User Story:** As the repository maintainer, I want a test that proves new scrapes only add rows to the current-year partition and never mutate any prior-year partition, so that the append-only invariant is machine-verified.

#### Acceptance Criteria

1. WHEN the append-only contract test runs AND the Past_Year_Partition has been successfully created in the bootstrapping step, THE Test_Suite SHALL record the Past_Year_Partition's SHA-256 and row count, then run the Migrate_Script with additional rows for the current year only, and assert that the Past_Year_Partition's SHA-256 and row count are unchanged; IF the bootstrapping step fails (i.e., the Past_Year_Partition was not successfully created before the append test runs), THE Test_Suite SHALL fail immediately rather than pass vacuously.
2. WHEN the Migrate_Script appends new current-year rows, THE Migrate_Script SHALL increase the Current_Year_Partition's row count by the number of net-new unique Dedup_Key values in the new rows.
3. WHEN the append-only contract test verifies the Past_Year_Partition, THE Test_Suite SHALL check both SHA-256 (byte-identity) and row count (no silent truncation) independently, and SHALL report an explicit `FAILED` status for each check that detected a mutation, distinguishing between a check that detected a problem and a check that failed to run.
4. WHEN the Migrate_Script appends one or more net-new rows to the Current_Year_Partition, THE append-only contract test SHALL verify that the Manifest is updated (the `generated_at` timestamp advances and the current-year partition's `rows` count reflects the new total); IF zero net-new rows are appended (i.e., all input rows are duplicates of existing Dedup_Key values), THEN the Manifest SHALL NOT be updated and the `generated_at` timestamp SHALL remain unchanged.
5. THE append-only contract test SHALL use a `tmp_path` sandbox and SHALL NOT touch the real `data/history_parquet/` store; IF the test detects any file access outside the `tmp_path` sandbox (e.g., reads or writes to the real store path), THE Test_Suite SHALL fail immediately.

---

### Requirement 4: Subprocess Encoding Fix

**User Story:** As the repository maintainer, I want the two failing subprocess-based tests to pass on Windows, so that the full test suite is green on all platforms.

#### Acceptance Criteria

1. WHEN `_run_migrate` in `test_parquet_immutability.py` invokes `subprocess.run`, THE call SHALL include `encoding='utf-8'` as an explicit keyword argument, and `result.stdout` SHALL be a `str` object (not `None` and not `bytes`).
2. IF the Migrate_Script emits a `LOCKED` message in stdout, THEN `"LOCKED" in result.stdout` SHALL evaluate to `True` without raising `UnicodeDecodeError` or `TypeError`.
3. IF the Migrate_Script emits a `REWRITING` message in stdout, THEN `"REWRITING" in result.stdout` SHALL evaluate to `True` without raising `UnicodeDecodeError` or `TypeError`.
4. THE encoding fix SHALL be applied only to the `_run_migrate` helper in `test_parquet_immutability.py`; THE Migrate_Script itself already reconfigures `sys.stdout` to UTF-8 on Windows and SHALL NOT be modified for this fix.
5. WHEN the full Test_Suite is run on Windows after the fix, THE Test_Suite SHALL report a PASS exit code (0) for all tests in `test_parquet_immutability.py` with no test-level failures.

---

### Requirement 5: Git LFS for Mega_Markets_Historical.xlsx

**User Story:** As the repository maintainer, I want `Mega_Markets_Historical.xlsx` tracked via Git LFS, so that its 10.62 MB binary blob does not bloat every clone of the repository.

#### Acceptance Criteria

1. THE repository SHALL contain a `.gitattributes` file at the root that includes the pattern `*.xlsx filter=lfs diff=lfs merge=lfs -text`, so that all Excel files are tracked via Git LFS.
2. WHEN a developer clones the repository without Git LFS installed, THE repository SHALL clone successfully (Git LFS pointer file is checked out instead of the binary), and THE build SHALL fail with a clear error message indicating that Git LFS is required rather than silently producing incorrect output; THE build SHALL also be allowed to fail for any other LFS-related issue, including misconfigured LFS remotes or inaccessible LFS storage.
3. WHEN Git LFS is installed and the repository is cloned, THE `Mega_Markets_Historical.xlsx` file SHALL be available at its existing path (`Mega_Markets_Historical.xlsx`) so that no path references in `predator/ingest_markets_xl.py` or any other script need to change; IF the LFS fetch fails due to network issues or server problems, THE build SHALL emit a clear error message identifying the LFS fetch failure as the cause rather than a generic file-not-found error.
4. THE `.gitignore` SHALL retain the comment noting that `Mega_Markets_Historical.xlsx` should be tracked via Git LFS, updated to reflect that the `.gitattributes` file now exists.
5. THE Git LFS migration SHALL be performed with `git lfs track "*.xlsx"` followed by `git add .gitattributes`, and the existing committed blob SHALL be migrated with `git lfs migrate import --include="*.xlsx"` so that historical commits do not retain the large binary in the main object store.

---

### Requirement 6: JSON Payload Size Reduction

**User Story:** As the repository maintainer, I want the large JSON files in `docs/data/` reduced in committed size without dropping any data the dashboard needs, so that the repository stays lean and GitHub Pages load times improve.

#### Acceptance Criteria

1. THE Docs_Data_Payload analysis SHALL identify which fields in `leaderboard.json` (3.88 MB), `flag_history.json` (3.52 MB), and `holdings_latest.json` (3.38 MB) are consumed by the dashboard JavaScript and which are redundant or over-precise.
2. WHEN numeric weight fields in `holdings_latest.json` are written, THE Build_Step SHALL round weight values to 4 decimal places (from the current 6–8 decimal places), reducing payload size without affecting dashboard rendering fidelity.
3. WHEN `flag_history.json` is written, THE Build_Step SHALL include only the fields required by the dashboard's flag-history rendering logic; any fields present in the file but not referenced by any dashboard JavaScript SHALL be omitted.
4. WHERE the dashboard already implements Per_Ticker_Detail lazy-loading for `stock.html`, THE Build_Step SHALL assess and document whether `holdings_latest.json` could be split into a slim summary file (top-level ticker list with score and rank) and Per_Ticker_Detail files; the assessment findings SHALL be recorded in the design document, and no split SHALL be implemented as part of this spec.
5. WHEN any JSON size-reduction change is applied, THE Test_Suite SHALL pass and the dashboard SHALL render correctly in a local build (`predator.build` completes without errors and all required output files are present).
6. THE size-reduction changes SHALL NOT drop any field that is referenced by `docs/index.html`, `docs/stock.html`, `docs/markets.html`, `docs/sim.html`, or any JavaScript file under `docs/`.

---

### Requirement 7: Gitignore and Archive Commit Status Verification

**User Story:** As the repository maintainer, I want the gitignore and commit status of every data directory confirmed correct, so that rebuild caches are never committed and the permanent archive is never accidentally ignored.

#### Acceptance Criteria

1. THE `.gitignore` SHALL list `data/markets_history/`, `data/vol_history/`, and `data/markets/` as ignored paths, confirming these rebuild caches are never committed.
2. THE `.gitignore` SHALL NOT list `data/history_parquet/` as an ignored path; THE Parquet_Store SHALL be committed to Git as the permanent archive.
3. WHEN a developer runs `git status` after a fresh build, THE Git_Status SHALL show no untracked or modified files under `data/markets_history/`, `data/vol_history/`, or `data/markets/`; this verification rule applies only to the post-fresh-build state and does not constrain the working tree at other times.
4. WHEN a developer runs `git status` after the Migrate_Script writes a new partition, THE Git_Status SHALL immediately show the new or modified partition file under `data/history_parquet/` as a staged or unstaged change (not ignored); IF the Migrate_Script writes a new partition and `git status` does not detect the file as a change, THEN THE Migrate_Script SHALL fail with an explicit error message identifying the gitignore misconfiguration.
5. THE `.gitignore` comment block describing the Parquet_Store's commit status SHALL be preserved and updated to reference this spec as the authoritative source of the storage contract.

---

### Requirement 8: 5-Year Growth Projection and Sustainability Confirmation

**User Story:** As the repository maintainer, I want a documented growth projection confirming the repository stays within GitHub's limits at the 5-year horizon, so that I can commit to this storage architecture with confidence.

#### Acceptance Criteria

1. THE growth projection SHALL use the measured ingestion rate of exactly 1,107 rows/day and the measured Parquet compression ratio of exactly 6.2 bytes/row (98,527 rows → 0.61 MB) as fixed constants; THE projection SHALL NOT be parameterized to accept alternative ingestion rates or compression ratios.
2. THE 1-year projection SHALL enforce both of the following calculations: (a) the annual Parquet partition size as ~404,000 rows/year × 6.2 bytes/row ≈ 2.5 MB; AND (b) the Rolling_CSV size at 120-day lookback as ≈ 133,000 rows × 59 bytes/row ≈ 7.8 MB.
3. THE 5-year projection SHALL state and enforce the following bounds: 5 annual Parquet partitions × ~2.5 MB ≈ 12.5 MB total Parquet archive; Rolling_CSV stays bounded at ~8 MB (rolling window, not cumulative) and SHALL be rejected as a configuration error if it exceeds ~8 MB; JSON payload grows only with the unique ticker universe (bounded at ~5,000 tickers) and SHALL be rejected as a configuration error if it exceeds the ticker-based limit.
4. THE projection SHALL state the 5-year total committed size as approximately 32 MB (12.5 MB Parquet archive + 7.8 MB Rolling_CSV + ~11 MB Docs_Data_Payload + negligible Git LFS pointer files), and SHALL confirm this total does not exceed 100 MB, which is well within GitHub's 1 GB repository soft limit and 1 GB GitHub Pages limit.
5. THE projection SHALL identify the Rolling_CSV as the only unbounded risk if the 120-day lookback window is ever removed, and SHALL state that removing the lookback window would invalidate the 7.8 MB CSV bound in the projection; the lookback window SHALL be preserved to keep the projection valid.
6. THE projection SHALL be documented in the design document for this spec.
7. THE `.gitignore` comment block describing the Parquet_Store's commit status SHALL reference the design document for this spec as the authoritative size analysis.

---

### Requirement 9: Scope Guards — Scraper and Scoring Are Immutable

**User Story:** As the repository maintainer, I want the scraper logic and scoring math to remain exactly as they are, so that the data-layer hardening work cannot accidentally break the core ETF analysis.

#### Acceptance Criteria

1. THE implementation SHALL NOT modify `scraper.py` or `etf_holdings_scraper_v42.py` in any way.
2. THE implementation SHALL NOT modify `predator/scoring.py` in any way.
3. THE Dedup_Key `(ETF_Ticker, ticker, Holdings_As_Of)` with `keep="last"` tie-break SHALL remain unchanged in the Migrate_Script; no alternative deduplication strategy SHALL be introduced.
4. WHEN the full Test_Suite (`python -m pytest tests/ -v`) is run after all changes, THE Test_Suite SHALL pass with zero failures and zero regressions against the baseline passing count; THE Test_Suite is considered passing only when the overall pytest result is PASS (exit code 0), not merely when zero individual tests fail, and setup errors, collection errors, or configuration problems that cause pytest to exit non-zero SHALL count as a failure.
5. THE implementation SHALL NOT introduce any mechanism that bounds repository size by dropping historical rows; the Append_Only_Contract is sacred and any size-bounding approach that removes history is a contract violation.
