# Requirements Document

## Introduction

The v42 ETF Holdings Scraper (`scripts/etf_holdings_scraper_v42.py`) was developed iteratively over 42 versions and scrapes 8 additional ETFs (VLUE, AVSC, GRIN, JHMM, JHEM, JHSC, MFEM, JOET) using sources that the primary `scraper.py` cannot handle: Playwright-driven sites, JH PDFs, BlackRock CSV API, PIMCO XLS via cookie-bound API, Virtus direct XLS, and Avantis Playwright download. It was integrated into the daily pipeline via `scraper.py::run_extended_scrapers()`, which runs v42 as a subprocess and bridges its canonical CSV (`etf_holdings_YYYYMMDD.csv`) into `data/all_history.csv`.

The integration was rushed: it shipped over four commits and required three hot-fixes within hours of merge (missing `xvfb`, function-order `NameError`, Python 3.9 syntax error), indicating it was never run end-to-end in CI before merge. Several integration gaps remain:

- `config.json` does not list the 8 new ETFs, so the primary scraper loop is unaware of them
- The bridge writes only to `data/all_history.csv`; it does not produce `data/latest/{TICKER}.csv` snapshots or append to `data/history/YYYY/MM/DD/master_archive.csv`, making the bridge data-path asymmetric with the primary loop
- `README.md` still advertises "16 Smart-Beta ETFs" while `config.yaml` enumerates 29
- `docs/data/metadata.json` still reports only 16 ETFs, indicating the bridge has not yet written successful v42 data into `all_history.csv` in CI
- No automated test verifies the end-to-end contract from v42 canonical CSV → bridge → `all_history.csv` → leaderboard
- v42 uses `headless=False` Playwright that depends on Xvfb in CI; failure modes (one source down, browser crash, subprocess timeout) must not destroy primary scraper output

This feature delivers a properly bounded, observable, idempotent, and tested integration of the v42 scraper into the existing pipeline so that all 29 configured ETFs flow end-to-end through scrape → archive → score → publish on every CI run, with failure of any single source isolated from the rest.

## Glossary

- **Main_Scraper**: The script `scraper.py` executed by the daily workflow; runs primary per-ETF scrapers and invokes the Bridge at the end of `main()`.
- **Extended_Scraper**: The script `scripts/etf_holdings_scraper_v42.py` containing source-specific fetchers for VLUE, AVSC, GRIN, JHMM, JHEM, JHSC, MFEM, JOET; entry point is `run_all()`.
- **Bridge**: The function `scraper.py::run_extended_scrapers()` that invokes the Extended_Scraper as a subprocess, reads its Canonical_CSV, and merges rows into the Giant_History.
- **Canonical_CSV**: The file `etf_holdings_YYYYMMDD.csv` written by the Extended_Scraper at the repository root, with the canonical schema `[etf, as_of_date, scrape_date, name, ticker, cusip, sedol, isin, weight_pct, shares, market_value, price, security_type, country, exchange, currency, ...]`.
- **Pipeline_Schema**: The 6-column schema used by every downstream consumer: `[ETF_Ticker, ticker, name, weight, Holdings_As_Of, Date_Scraped]`. `weight` is a fraction in `[0, 1]`. `Holdings_As_Of` and `Date_Scraped` are `YYYY-MM-DD` strings.
- **Giant_History**: The append-only file `data/all_history.csv` consumed by the Predator_Builder.
- **Latest_Snapshot**: The per-ETF file `data/latest/{TICKER}.csv` containing the most recent scrape for that ticker.
- **Master_Archive**: The daily snapshot file `data/history/YYYY/MM/DD/master_archive.csv` containing all ETF rows scraped on that date.
- **Predator_Builder**: The module `predator.build` invoked by the Build_Site_Workflow; reads `data/all_history.csv` and writes `docs/data/*.json` (including `metadata.json` and `leaderboard.json`).
- **Dashboard**: The static site under `docs/` published to GitHub Pages.
- **Daily_Scrape_Workflow**: `.github/workflows/daily_scrape.yml` — runs `python scraper.py` on a cron and pushes data changes.
- **Build_Site_Workflow**: `.github/workflows/build_site.yml` — runs `predator.build`, builds JSON artifacts, and deploys to Pages.
- **V42_ETFs**: The set `{VLUE, AVSC, GRIN, JHMM, JHEM, JHSC, MFEM, JOET}` — the 8 ETFs owned by the Extended_Scraper.
- **Primary_ETFs**: The set of ETFs listed in `config.json` (currently 21) — the ETFs owned by the Main_Scraper's primary loop.
- **Configured_ETFs**: The complete set of ETFs listed in `config.yaml` under `etfs:` (currently 29) — the single source of truth for the ETF universe.
- **Bridge_Failure**: Any condition where the Extended_Scraper subprocess does not produce a valid Canonical_CSV: nonzero exit, timeout, crash, missing file, missing required columns, or zero valid rows after cleaning.

## Requirements

### Requirement 1: Single Source of Truth for ETF Universe

**User Story:** As a maintainer, I want `config.yaml` to be the authoritative list of every ETF the system tracks, so that adding or removing an ETF does not require edits across multiple files that can drift.

#### Acceptance Criteria

1. THE Main_Scraper SHALL treat `config.yaml::etfs[].ticker` as the complete authoritative ETF universe.
2. WHEN a ticker appears in `config.yaml::etfs[]`, THE Main_Scraper SHALL ensure that exactly one of (a) the primary loop in `scraper.py::main()` or (b) the Extended_Scraper produces rows for that ticker on each run.
3. IF a ticker appears in both `config.json` and the Extended_Scraper's task list, THEN THE Main_Scraper SHALL treat this as a configuration error and log a duplicate-ownership warning naming the ticker.
4. THE Configured_ETFs set SHALL equal the union of Primary_ETFs and V42_ETFs with empty intersection.
5. WHEN ownership of a configured ticker must be determined, THE Main_Scraper SHALL use a single explicit V42_ETFs ownership manifest defined as a module-level constant in `scraper.py`, and that manifest SHALL be the only authority used for both duplicate detection (1.3) and downstream routing in Requirement 4.

### Requirement 2: Extended Scraper Subprocess Isolation

**User Story:** As an operator, I want the Extended_Scraper to run in a subprocess so that any crash, hang, or unhandled exception inside v42 cannot terminate the Main_Scraper or destroy primary scrape output.

#### Acceptance Criteria

1. THE Bridge SHALL invoke the Extended_Scraper using `subprocess.run` with the same Python interpreter as the parent process.
2. THE Bridge SHALL set a timeout of 600 seconds on the subprocess.
3. IF the Extended_Scraper subprocess exits with a nonzero return code, THEN THE Bridge SHALL log the return code and the last 1000 characters of stderr and continue without raising.
4. IF the Extended_Scraper subprocess raises `subprocess.TimeoutExpired`, THEN THE Bridge SHALL log a timeout message and continue without raising.
5. IF any other exception occurs while launching or waiting on the subprocess, THEN THE Bridge SHALL log the exception type and message and continue without raising.
6. WHEN the Bridge invokes the Extended_Scraper subprocess, THE Bridge SHALL print the last 3000 characters of subprocess stdout to the run log regardless of the subprocess return code or any subsequent processing outcome.
7. WHEN a Canonical_CSV for the current date already exists at the time the Bridge starts, THE Bridge SHALL reuse it without re-invoking the subprocess.
8. WHEN the Bridge reuses an existing Canonical_CSV, THE Bridge SHALL log a single line stating the reuse decision and the path of the file being reused.

### Requirement 3: Canonical CSV Schema Validation

**User Story:** As a maintainer, I want the Bridge to validate the Canonical_CSV schema before merging, so that a malformed or partially-written file from v42 cannot corrupt `data/all_history.csv`.

#### Acceptance Criteria

1. WHEN the Bridge reads the Canonical_CSV, THE Bridge SHALL verify the presence of the required columns `{etf, ticker, name, weight_pct, as_of_date}`.
2. IF any required column is missing, THEN THE Bridge SHALL log the missing column names and SHALL NOT write any rows to the Giant_History.
3. IF the Canonical_CSV cannot be read by `pandas.read_csv` (file truncated, encoding error, malformed rows), THEN THE Bridge SHALL log the exception and SHALL NOT write any rows to the Giant_History.
4. THE Bridge SHALL exclude rows whose `security_type` matches any of `{cash, cash equivalent, money market, derivative, futures, option, swap, fx, currency}` (case-insensitive substring).
5. THE Bridge SHALL exclude rows whose `ticker` is null, empty, or begins with the character `$`.
6. THE Bridge SHALL coerce `weight_pct` via `pandas.to_numeric(errors='coerce')`, divide by 100 to obtain a fraction, and exclude rows whose resulting `weight` is not strictly greater than 0.
7. THE Bridge SHALL parse `as_of_date` to `YYYY-MM-DD` via `pandas.to_datetime` and SHALL exclude rows where parsing produces `NaT`.
8. WHEN cleaning is complete, THE Bridge SHALL produce a DataFrame matching Pipeline_Schema with `ETF_Ticker` (renamed from `etf`), `ticker`, `name`, `weight`, `Holdings_As_Of`, `Date_Scraped`.
9. WHEN the cleaned DataFrame is empty, THE Bridge SHALL log "Bridge: 0 valid rows" and SHALL NOT modify the Giant_History.

### Requirement 4: Data-Path Parity Between Primary Loop and Bridge

**User Story:** As a downstream consumer (Predator_Builder, Excel Power Query users, ad-hoc analysts), I want v42-sourced ETFs to appear in every output the primary loop writes, so that I do not have to know which scraper produced a row to find it.

#### Acceptance Criteria

1. WHEN the Bridge produces a non-empty cleaned DataFrame, THE Bridge SHALL append its rows to the Giant_History via the same `update_giant_history` function used by the primary loop.
2. WHEN the Bridge produces a non-empty cleaned DataFrame, THE Bridge SHALL write a Latest_Snapshot at `data/latest/{ETF_Ticker}.csv` for each ticker in V42_ETFs, containing only that ETF's rows in Pipeline_Schema column order.
3. WHEN the Bridge produces a non-empty cleaned DataFrame on a date when a Master_Archive exists for that date, THE Bridge SHALL append its rows to the existing `data/history/YYYY/MM/DD/master_archive.csv`.
4. WHEN the Bridge produces a non-empty cleaned DataFrame on a date when no Master_Archive exists for that date, THE Bridge SHALL create `data/history/YYYY/MM/DD/master_archive.csv` containing its rows in Pipeline_Schema column order.
5. THE Bridge SHALL write Latest_Snapshot and Master_Archive contributions only AFTER the primary loop has returned from its per-ETF iteration and the Main_Scraper has finished writing the primary Master_Archive for the current date.

### Requirement 5: Giant History Idempotency

**User Story:** As an operator, I want re-running the scraper on the same day to be idempotent, so that retried runs and 14:00 + 22:00 UTC double-runs do not duplicate rows.

#### Acceptance Criteria

1. THE Main_Scraper SHALL deduplicate the Giant_History on the composite key `(ETF_Ticker, ticker, Holdings_As_Of)` retaining the last occurrence.
2. WHEN the Main_Scraper is invoked twice on the same calendar date with identical source data, THE Giant_History SHALL contain the same set of rows after the second invocation as after the first.
3. WHEN the Bridge contributes the same `(ETF_Ticker, ticker, Holdings_As_Of)` triple as already present in the Giant_History, THE Main_Scraper SHALL retain exactly one row for that triple.
4. WHEN any source contributes rows that will be written to the Giant_History, THE Main_Scraper SHALL deduplicate the combined output on the composite key before writing to disk, regardless of how many sources contributed.
5. WHEN two rows share `(ETF_Ticker, ticker)` but have different `Holdings_As_Of` values, THE Main_Scraper SHALL retain both rows.

### Requirement 6: Failure Isolation

**User Story:** As an operator, I want a failure in any single Extended_Scraper source to leave every other source's output intact, so that a transient outage at one issuer does not zero out the entire daily run.

#### Acceptance Criteria

1. WHEN the Extended_Scraper's `run_all` function processes one of its tasks, THE Extended_Scraper SHALL wrap that task in a try/except that catches all exceptions.
2. IF a single task raises, THEN THE Extended_Scraper SHALL log the failure with traceback and continue to the next task.
3. WHEN the Extended_Scraper completes, THE Canonical_CSV SHALL contain rows from every task that produced at least one row.
4. IF every task in the Extended_Scraper fails, THEN THE Extended_Scraper SHALL still exit with return code 0 and the Canonical_CSV SHALL not be written.
5. IF the Bridge encounters a Bridge_Failure, THEN the Main_Scraper SHALL still produce a Master_Archive and Giant_History containing all primary-loop rows.

### Requirement 7: CI Workflow Correctness

**User Story:** As a maintainer, I want the daily workflow to install every dependency v42 needs and to run successfully on a clean GitHub Actions runner, so that the integration cannot fail because of a missing system package.

#### Acceptance Criteria

1. THE Daily_Scrape_Workflow SHALL install `xvfb` via `apt-get` before invoking the Main_Scraper.
2. THE Daily_Scrape_Workflow SHALL install the Python packages `pandas, requests, lxml, openpyxl, beautifulsoup4, html5lib, selenium, curl_cffi, playwright, pdfplumber` via pip.
3. THE Daily_Scrape_Workflow SHALL run `python -m playwright install chromium --with-deps` before invoking the Main_Scraper.
4. THE Daily_Scrape_Workflow SHALL invoke the Main_Scraper under `xvfb-run -a` so that v42's `headless=False` Playwright sessions have a virtual display.
5. THE Daily_Scrape_Workflow SHALL execute on the schedule `0 14,22 * * 1-5` and on `workflow_dispatch`.
6. WHEN the Main_Scraper produces no data changes under `git diff --staged`, THE Daily_Scrape_Workflow SHALL set output `data_changed=false` and SHALL NOT dispatch the Build_Site_Workflow.
7. WHEN the Main_Scraper produces data changes, THE Daily_Scrape_Workflow SHALL commit, rebase, push, and dispatch the Build_Site_Workflow.

### Requirement 8: Observability

**User Story:** As an operator, I want every CI run to log a per-ETF row count so that I can diagnose silent regressions (e.g., a source that returns zero rows but does not error).

#### Acceptance Criteria

1. WHILE the Main_Scraper iterates primary ETFs, THE Main_Scraper SHALL log per primary ETF the row count and `Holdings_As_Of` of the saved snapshot.
2. WHEN the Bridge finishes cleaning, THE Bridge SHALL log per V42 ETF the row count and `Holdings_As_Of` of the cleaned DataFrame.
3. WHEN the Extended_Scraper finishes `run_all`, THE Extended_Scraper SHALL log a summary table containing one row per task with columns `ETF, Status, Rows, Cols`.
4. WHEN a Bridge_Failure occurs, THE Bridge SHALL log a single line beginning with `⚠️ ` or `❌ ` that names the failure mode.
5. THE Predator_Builder SHALL include in `docs/data/metadata.json` the field `etfs` containing the sorted list of all ETFs present in the Giant_History.

### Requirement 9: Frontend Reflects All Configured ETFs

**User Story:** As a Dashboard user, I want the site to surface every configured ETF, so that holdings of VLUE, AVSC, GRIN, JHMM, JHEM, JHSC, MFEM, and JOET contribute to leaderboard scoring and appear in per-ETF panels.

#### Acceptance Criteria

1. WHEN the Build_Site_Workflow completes successfully on a date when the Bridge produced rows, THE `metadata.json::etfs` field SHALL contain every ticker in Configured_ETFs that has at least one row in `data/all_history.csv`.
2. WHEN the Predator_Builder runs, THE `etf_overlap.json` SHALL contain a square Jaccard matrix of size N × N where N equals the number of distinct `ETF_Ticker` values in the latest snapshot of `data/all_history.csv`.
3. WHEN the Build_Site_Workflow runs `predator.build`, THE Predator_Builder SHALL apply the points and tier defined in `config.yaml::etfs` to every ETF that contributes rows.
4. WHEN a ticker is held by a V42 ETF, THE Dashboard SHALL include that ETF's contribution in the ticker's `final_score`, `tiers`, `held_by`, and `tier_breadth` fields.

### Requirement 10: Documentation Accuracy

**User Story:** As a new contributor, I want `README.md` to accurately describe what the system tracks today, so that I do not have to read the code to learn the actual ETF universe.

#### Acceptance Criteria

1. THE `README.md` SHALL state the ETF count as the count derived from `config.yaml::etfs[]`, not a hard-coded historical number.
2. THE `README.md::ETF Tier Weights` table SHALL list every ticker in Configured_ETFs grouped by tier and SHALL show its `points` value matching `config.yaml`.
3. WHEN `config.yaml::etfs[]` changes, THE `README.md` ETF count and tier table SHALL be updated in the same commit.
4. THE `README.md` SHALL describe the existence of the Extended_Scraper, the V42_ETFs it owns, and the Bridge that merges its output into the Giant_History.

### Requirement 11: End-to-End Test Coverage

**User Story:** As a maintainer, I want an automated test that exercises the bridge contract, so that a future regression in the Canonical_CSV schema is caught in CI before merge.

#### Acceptance Criteria

1. THE test suite SHALL include a test that takes a fixture Canonical_CSV containing rows for at least two V42 ETFs and verifies that the Bridge's cleaning logic produces a DataFrame matching Pipeline_Schema.
2. THE test suite SHALL include a test that verifies a Canonical_CSV missing a required column produces zero rows written to the Giant_History.
3. THE test suite SHALL include a test that verifies cleaning a Canonical_CSV containing currency tickers (e.g., `$USD`, `$BRL`) and money-market `security_type` rows excludes those rows.
4. THE test suite SHALL include a test that verifies running the Bridge twice with the same Canonical_CSV produces the same Giant_History row set after the second run as after the first.
5. THE test suite SHALL include a test that verifies a Giant_History containing rows for all Configured_ETFs produces a leaderboard via `predator.build` whose `metadata.json::etfs` list equals the sorted Configured_ETFs.

### Requirement 12: Round-Trip Property for the Canonical-to-Pipeline Translation

**User Story:** As a maintainer, I want the Canonical_CSV → Pipeline_Schema translation to be a faithful projection, so that no information needed downstream is lost or corrupted by the Bridge.

#### Acceptance Criteria

1. FOR every row of a Canonical_CSV that survives the cleaning rules in Requirement 3, THE Bridge SHALL produce exactly one row in the Pipeline_Schema output.
2. FOR every surviving row, THE Bridge SHALL preserve `etf` as `ETF_Ticker`, `ticker` as `ticker`, `name` as `name`, and `as_of_date` as `Holdings_As_Of`.
3. FOR every surviving row, THE Bridge SHALL produce `weight` equal to `weight_pct / 100` rounded to no fewer than 6 decimal places.
4. FOR every surviving row whose `weight_pct` is in the open interval `(0, 100]`, THE Bridge SHALL produce a `weight` in the open interval `(0, 1]`.
5. WHEN the Pipeline_Schema output is serialized to CSV and re-read by `pandas.read_csv`, THE re-read DataFrame SHALL equal the pre-serialization DataFrame on the columns `[ETF_Ticker, ticker, Holdings_As_Of]` row-set.

### Requirement 13: Build Site Workflow Inputs and Outputs

**User Story:** As a Dashboard user, I want the build workflow to verify every required artifact before deploying, so that a partial build cannot replace a working dashboard with a broken one.

#### Acceptance Criteria

1. THE Build_Site_Workflow SHALL run the test suite via `python -m pytest tests/ -v` before invoking `predator.build`.
2. IF the test suite fails, THEN THE Build_Site_Workflow SHALL block the deploy job and SHALL NOT publish the Pages artifact.
3. THE Build_Site_Workflow SHALL verify the existence of `docs/data/leaderboard.json`, `docs/data/holdings_latest.json`, `docs/data/changelog.json`, `docs/data/metadata.json`, and `docs/data/leaderboard.parquet` before uploading the Pages artifact.
4. WHEN the Build_Site_Workflow is triggered by `workflow_run` from the Daily_Scrape_Workflow, THE Build_Site_Workflow SHALL check out the exact commit pushed by the scraper.
5. WHEN the Daily_Scrape_Workflow concludes with status other than `success`, THE Build_Site_Workflow SHALL skip the build job.

### Requirement 14: Edge Cases and Defensive Constraints

**User Story:** As a maintainer, I want known edge cases to be handled explicitly, so that operational surprises (timeouts, ticker collisions, 14:00 vs 22:00 reruns) do not require manual intervention.

#### Acceptance Criteria

1. IF the same `ticker` appears in both a Primary_ETF's holdings and a V42_ETF's holdings on the same date, THEN THE Main_Scraper SHALL retain both rows, distinguished by the `ETF_Ticker` column.
2. IF the Extended_Scraper produces a row whose `weight_pct` exceeds 100, THEN THE Bridge SHALL log the offending row identifier and SHALL exclude that row from the Pipeline_Schema output.
3. IF the Extended_Scraper produces a Canonical_CSV with `as_of_date` in the future relative to `Date_Scraped`, THEN THE Bridge SHALL log a warning naming the ETF and SHALL still include the row in the Pipeline_Schema output.
4. IF the Daily_Scrape_Workflow's 14:00 UTC run successfully scrapes V42_ETFs and the 22:00 UTC run on the same date scrapes them again with the same `as_of_date`, THEN THE Giant_History SHALL contain exactly one row per `(ETF_Ticker, ticker, Holdings_As_Of)` triple.
5. IF the Extended_Scraper subprocess writes the Canonical_CSV but the Main_Scraper process is terminated before invoking the Bridge, THEN the next Main_Scraper invocation on the same calendar date SHALL reuse the existing Canonical_CSV and complete the Bridge step.
