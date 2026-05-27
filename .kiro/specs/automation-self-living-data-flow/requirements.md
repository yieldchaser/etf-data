# Requirements Document

## Introduction

The Predator Protocol site must operate indefinitely with zero human intervention. New monthly market data (May 2026 → June 2026 → July 2026 → …) must flow in automatically, merge cleanly onto the historical baseline, and keep the live site fresh and correct — even if the owner is permanently unavailable. This spec covers four concerns: (A) proving the self-living merge actually reaches `LIVE_MERGE_HEALTHY`; (B) guaranteeing the monthly roll-forward is truly automatic; (C) maximising pipeline resilience so no single failure can take the site down or require manual recovery; and (D) preserving all existing guards (scraper logic, scoring math, and the verified partial-year rendering fix).

## Glossary

- **Pipeline**: The full sequence of CI steps that produces the live site: scrape → Excel backfill → live FRED/yfinance merge → build → deploy.
- **Live_Merge**: The step in `predator/markets_history.py` that fetches current monthly data from FRED and yfinance and merges it onto the Excel deep-history baseline.
- **Verdict**: The `self_living_check.verdict` field in `metadata.json`, one of `LIVE_MERGE_HEALTHY`, `LIVE_MERGE_DEGRADED`, or `LIVE_MERGE_FAILED`.
- **Holdout**: A registry-eligible asset whose live fetch failed and whose source label therefore remains `Mega_Markets_Historical.xlsx:*` after the Live_Merge step.
- **FRED_API_KEY**: The GitHub repository secret that authorises calls to the FRED API. Read by `os.environ.get("FRED_API_KEY")` in `markets_history.py` and `vol_history.py`.
- **Excel_Backfill**: The `predator/ingest_markets_xl.py` step that seeds deep historical data from `Mega_Markets_Historical.xlsx`. It is run once per build, before the Live_Merge step, and never re-run after it.
- **Asof**: The `asof` field in `market_returns.json` — the latest YYYY-MM month for which data exists across all assets.
- **Staleness_Badge**: A per-asset "as of \<date\>" label rendered on the site when a source is unavailable and the displayed data is from a prior run.
- **Fault_Boundary**: An isolated try/except block with `continue-on-error` that prevents a single external-data failure from propagating to the rest of the build.
- **Cache_Fallback**: The on-disk parquet cache in `data/markets_history/` and `data/vol_history/` that is served when a live fetch fails.
- **Build_Step**: The `predator.build` invocation in `build_site.yml` that reads `market_returns.json` and writes `metadata.json`, `leaderboard.json`, and all other site artifacts.
- **Daily_Scrape**: The `daily_scrape.yml` workflow that runs on a cron schedule, scrapes ETF holdings, commits new data, and triggers `build_site.yml`.
- **ASSET_REGISTRY**: The list of asset specifications in `predator/markets_history.py` that declares the canonical source type (`yfinance` or `fred`) and series ID for each asset.

---

## Requirements

### Requirement 1: FRED API Key Reaches the Fetch

**User Story:** As the owner, I want proof that `FRED_API_KEY` is actually injected into the correct CI step's environment, so that FRED series are fetched live and not silently skipped.

#### Acceptance Criteria

1. WHEN `build_site.yml` runs the `predator.markets_history --full-refresh` step, THE Pipeline SHALL expose `FRED_API_KEY` in that step's `env:` block under the exact name `FRED_API_KEY`.
2. WHEN `build_site.yml` runs the `predator.vol_history --full-refresh` step, THE Pipeline SHALL expose `FRED_API_KEY` in that step's `env:` block under the exact name `FRED_API_KEY`.
3. WHEN `build_site.yml` runs the `markets.fetch_fred` step, THE Pipeline SHALL expose `FRED_API_KEY` in that step's `env:` block under the exact name `FRED_API_KEY`.
4. THE Pipeline SHALL NOT rely on a job-level or workflow-level `environment:` block to scope the secret, because repository-level secrets are not automatically available inside named GitHub Environments unless explicitly passed.
5. WHEN `predator.markets_history` initialises the FRED client and `FRED_API_KEY` is present in the environment, THE Markets_History_Module SHALL log a confirmation line (e.g. `FRED client initialised`) so the CI build log provides auditable proof the key was received; IF the logging call itself fails, THE Markets_History_Module SHALL continue with FRED operations regardless.
6. IF `FRED_API_KEY` is absent from the environment when `predator.markets_history` runs, THEN THE Markets_History_Module SHALL log `ERROR: FRED_API_KEY environment variable not set` and continue without FRED fetches (soft-skip, never `sys.exit`).

---

### Requirement 2: Live-Merge Verdict Reaches HEALTHY

**User Story:** As the owner, I want the live-merge verdict to reach `LIVE_MERGE_HEALTHY` on every normal CI run, so that I can confirm all eligible assets carry live sources.

#### Acceptance Criteria

1. WHEN a CI build completes with `FRED_API_KEY` in scope and all external sources reachable, THE Build_Step SHALL write `metadata.json` with `markets_data_freshness.self_living_check.verdict = "LIVE_MERGE_HEALTHY"`.
2. WHEN the verdict is `LIVE_MERGE_HEALTHY`, THE Build_Step SHALL write `self_living_check.holdouts` as an empty list; IF any holdouts are present, THE Build_Step SHALL NOT set the verdict to `LIVE_MERGE_HEALTHY`.
3. WHEN the verdict is `LIVE_MERGE_HEALTHY`, THE Build_Step SHALL write `self_living_check.live_source_count` greater than `self_living_check.excel_only_count`.
4. WHEN the verdict is `LIVE_MERGE_DEGRADED`, THE Build_Step SHALL write `self_living_check.holdouts` as a non-empty list naming every asset whose live fetch failed, by display name.
5. WHEN the verdict is `LIVE_MERGE_DEGRADED`, THE Build_Step SHALL write `self_living_check.holdout_keys` as a non-empty list of the corresponding asset IDs.
6. WHEN the verdict is `LIVE_MERGE_FAILED`, THE Build_Step SHALL write `self_living_check.live_source_count = 0`, indicating a total live-source outage.
7. THE Build_Step SHALL derive the set of live-eligible assets exclusively from `ASSET_REGISTRY` in `predator/markets_history.py` — never from a hardcoded list — so that adding a new asset to the registry automatically includes it in the verdict calculation.
8. IF `ASSET_REGISTRY` cannot be imported during the Build_Step, THEN THE Build_Step SHALL log a warning and continue with an empty live-eligible set rather than failing the build; THE Build_Step SHALL only use an empty live-eligible set when the `ASSET_REGISTRY` import actually fails — it SHALL NOT substitute an empty set for any other condition.

---

### Requirement 3: Monthly Roll-Forward Is Automatic

**User Story:** As the owner, I want new monthly data to appear on the site without any manual action, so that the site stays current indefinitely after my death.

#### Acceptance Criteria

1. THE Pipeline SHALL fetch every month of data after `Mega_Markets_Historical.xlsx`'s last month exclusively from live sources (yfinance and FRED), never by editing the Excel file.
2. WHEN a new calendar month's data becomes available from FRED or yfinance, THE Live_Merge SHALL append that month to the relevant asset's `monthly` array in `market_returns.json` on the next scheduled build, without any config edit, date bump in code, or manual trigger.
3. THE Excel_Backfill step SHALL run before the Live_Merge step in every build, and THE Live_Merge step SHALL NOT be re-run after the Excel_Backfill step, so that live source labels are never overwritten by the Excel filename.
4. WHEN the Live_Merge step writes a live-fetched month for an asset, THE Markets_History_Module SHALL set `meta.source` to `yfinance:<series_id>` or `fred:<series_id>` for that asset, not to the Excel filename.
5. WHEN the Excel_Backfill step runs and an asset already carries a `meta.source` starting with `yfinance:` or `fred:`, THE Excel_Backfill_Module SHALL preserve that source label and SHALL NOT overwrite it with the Excel filename; IF the preservation mechanism fails to apply the Excel filename label as a fallback, THE Excel_Backfill_Module SHALL halt the build; IF the preservation succeeds, THE build SHALL continue normally.
6. WHEN a new month is appended, THE Build_Step SHALL advance the `asof` field in `market_returns.json` to the new latest YYYY-MM, the matrix's newest column SHALL reflect the new month, and each affected asset's `meta.last` SHALL equal the new month.
7. THE `daily_scrape.yml` workflow SHALL run on a cron schedule (at minimum once per weekday) without requiring any human trigger, so that new scrape data is committed and `build_site.yml` is dispatched automatically.
8. THE `build_site.yml` workflow SHALL trigger automatically on completion of `daily_scrape.yml` (via `workflow_run`) and on direct pushes to `main`, so that a fresh month requires no manual dispatch.

---

### Requirement 4: Full Fault Isolation for External Data Steps

**User Story:** As the owner, I want every external-data step to be isolated so that a failure in any one of them — or all of them at once — never blocks the core build or takes the site down.

#### Acceptance Criteria

1. THE `build_site.yml` step that runs `predator.markets_history --full-refresh` SHALL carry `continue-on-error: true`.
2. THE `build_site.yml` step that runs `predator.vol_history --full-refresh` SHALL carry `continue-on-error: true`.
3. THE `build_site.yml` step that runs `markets.fetch_yf` SHALL carry `continue-on-error: true`.
4. THE `build_site.yml` step that runs `markets.fetch_fred` SHALL carry `continue-on-error: true`.
5. THE `build_site.yml` step that runs `predator.ingest_markets_xl` SHALL carry `continue-on-error: true`.
6. WHEN all external-data steps fail simultaneously, THE Build_Step SHALL still complete and produce valid `leaderboard.json`, `metadata.json`, and all other required site artifacts using the last-good cached data; IF the cached data itself is missing or corrupted, THE Build_Step SHALL always generate minimal valid artifacts (empty asset list, `LIVE_MERGE_FAILED` verdict) and complete the build successfully rather than crashing.
7. THE `predator.vol_history` module SHALL NOT call `sys.exit(1)` when the FRED client fails to initialise; instead, THE Vol_History_Module SHALL log the error and return gracefully so the `continue-on-error` wrapper is not bypassed by a hard exit.
8. WHEN `predator.markets_history` encounters a per-series fetch exception, THE Markets_History_Module SHALL catch it within the per-series loop, log the error, and continue to the next series without propagating the exception.

---

### Requirement 5: Graceful Degradation with Honest Staleness Badges

**User Story:** As the owner, I want the site to show honest staleness information when a source is down, so that visitors never see stale data presented as fresh.

#### Acceptance Criteria

1. WHEN a live fetch fails and the Cache_Fallback is used, THE Site SHALL display a per-asset staleness badge showing "as of \<last-good-date\>" rather than the current date.
2. THE Site SHALL never display a blank or a crash page when any external data source is unavailable.
3. WHEN the verdict is `LIVE_MERGE_DEGRADED`, THE Site SHALL display the verdict chip with the degraded state and SHALL include the holdout asset names in the chip tooltip; IF the degraded state conditions are not met, THE Site SHALL display the verdict chip with the failed state as a fallback.
4. WHEN the verdict is `LIVE_MERGE_FAILED`, THE Site SHALL display the verdict chip with the failed state.
5. THE Site SHALL never display a live-source label (e.g. `yfinance:^GSPC`) for an asset that is actually being served from the Excel deep-history cache.
6. WHEN an ETF's holdings data is stale beyond the configured threshold, THE Build_Step SHALL emit a `::warning::` annotation in the CI log naming the ETF and its last-known date.

---

### Requirement 6: Self-Healing on Transient Failures

**User Story:** As the owner, I want transient failures (rate limits, timeouts, network blips) to auto-recover on the next scheduled run without any intervention.

#### Acceptance Criteria

1. WHEN a FRED series fetch returns a 429 (rate limit) response, THE Shared_FRED_Retry_Utility SHALL retry with exponential backoff (up to 5 attempts, capped at 30 seconds per wait) before degrading to cache; both `predator.markets_history` and `predator.vol_history` SHALL use this shared utility.
2. WHEN a yfinance fetch returns empty for a ticker in the known-flaky set (`^GSPC`, `^NDX`, `^DJI`, `NASDAQCOM`), THE Markets_History_Module SHALL perform one bounded retry with a short backoff before degrading to cache.
3. WHEN a FRED series fetch returns a 429 response in `predator.vol_history`, THE Vol_History_Module SHALL use the Shared_FRED_Retry_Utility with exponential backoff (up to 5 attempts, capped at 30 seconds per wait) before degrading to cache.
4. WHEN a live fetch fails and the Cache_Fallback is used, THE Markets_History_Module SHALL write the cached data to `market_returns.json` so the next build has a valid baseline to merge onto; IF this write operation fails, THE Markets_History_Module SHALL log the error and continue the build rather than failing.
5. WHEN the next scheduled build runs after a transient failure, THE Pipeline SHALL attempt all live fetches again from scratch (no permanent skip flag), so that a recovered source is automatically picked up.
6. THE Cache_Fallback SHALL be a local parquet file on disk (in `data/markets_history/` or `data/vol_history/`) that persists across CI runs via the repository's committed data directory.

---

### Requirement 7: No Single Point of Failure Requiring Human Intervention

**User Story:** As the owner, I want every potential single point of failure in the pipeline to be either self-recovering or explicitly documented, so that the system can run forever without a human.

#### Acceptance Criteria

1. THE Pipeline SHALL be audited for every step that, if it failed, would require manual intervention to recover; each such step SHALL either be made self-recovering (via `continue-on-error`, retry logic, or cache fallback) or documented as a known manual-touch point.
2. WHEN the `daily_scrape.yml` workflow fails to commit new data (e.g. due to a merge conflict), THE Scrape_Workflow SHALL attempt a `git pull --rebase` before pushing as the sole recovery mechanism, so that concurrent commits do not permanently block the scrape.
3. WHEN the `build_site.yml` workflow fails the `Verify outputs` step (a required artifact is missing), THE Pipeline SHALL surface the failure as a CI error so the owner is notified, rather than silently deploying an incomplete site.
4. THE `predator.build` step SHALL NOT carry `continue-on-error: true`, because it is the core build step whose failure must be surfaced as a hard CI error.
5. THE `Run tests` step SHALL NOT carry `continue-on-error: true`, because test failures must block deployment.
6. WHEN `market_returns.json` does not exist at the time `predator.build` runs (e.g. all upstream data steps failed), THE Build_Step SHALL complete with `markets_data_freshness.available = false` and `verdict = "LIVE_MERGE_FAILED"` rather than crashing, and THE Pipeline SHALL allow deployment to proceed whenever the build step completes successfully, regardless of what caused the degraded verdict.

---

### Requirement 8: Scope Guards — Scraper and Scoring Are Immutable

**User Story:** As the owner, I want the scraper logic and scoring math to remain exactly as they are, so that the automation work cannot accidentally break the core ETF analysis.

#### Acceptance Criteria

1. THE implementation SHALL NOT modify `scraper.py` beyond additive resilience wrappers (e.g. wrapping an existing call in a try/except that was not previously wrapped).
2. THE implementation SHALL NOT modify `predator/scoring.py` in any way, including additive resilience wrappers.
3. THE implementation SHALL NOT modify `etf_holdings_scraper_v42.py` beyond additive resilience wrappers.
4. WHEN the full test suite (`python -m pytest tests/ -v`) is run after all changes, THE Test_Suite SHALL be executed and SHALL pass with the same set of passing tests as before the changes.
5. THE implementation SHALL verify each change with a clean full build (`predator.ingest_markets_xl --no-fail-on-stale && predator.markets_history --full-refresh && predator.build`) before committing.
