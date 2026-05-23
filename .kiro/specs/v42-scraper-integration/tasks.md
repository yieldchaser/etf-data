# Implementation Plan: v42 Scraper Integration

## Overview

Convert the feature design into a series of prompts for a code-generation LLM that will implement each step with incremental progress. Make sure that each prompt builds on the previous prompts, and ends with wiring things together. There should be no hanging or orphaned code that isn't integrated into a previous step. Focus ONLY on tasks that involve writing, modifying, or testing code.

The plan is divided into four phases. **Phase 1** refactors `scraper.py` into composable helpers and is **strictly sequential** because every sub-task edits the same file. **Phase 2** introduces the bridge test suite and depends on Phase 1's helpers being importable. **Phase 3** updates `README.md` documentation and runs in parallel with Phase 2 (different files). **Phase 4** verifies the integration end-to-end and depends on Phases 2 and 3 completing.

Implementation language: **Python** (matches existing codebase and design).

## Tasks

- [x] 1. Refactor `scraper.py` into composable bridge helpers
  - All sub-tasks below edit `scraper.py` and MUST execute strictly sequentially (1.1 → 1.2 → ... → 1.8) to avoid file-conflict races.
  - Design reference: "Components and Interfaces → Module: `scraper.py`" (lines 162–280 of design.md).

  - [x] 1.1 Add module-level constants `V42_ETFS`, `REQUIRED_CANONICAL_COLUMNS`, `NON_EQUITY_SECURITY_TYPES` to `scraper.py`
    - Define `V42_ETFS = frozenset({"VLUE", "AVSC", "GRIN", "JHMM", "JHEM", "JHSC", "MFEM", "JOET"})`.
    - Define `REQUIRED_CANONICAL_COLUMNS = frozenset({"etf", "ticker", "name", "weight_pct", "as_of_date"})`.
    - Define `NON_EQUITY_SECURITY_TYPES = ("cash", "cash equivalent", "money market", "derivative", "futures", "option", "swap", "fx", "currency")`.
    - Place constants at module top, after imports, before any function definitions.
    - **Done-when:** `python -c "from scraper import V42_ETFS, REQUIRED_CANONICAL_COLUMNS, NON_EQUITY_SECURITY_TYPES; assert len(V42_ETFS) == 8 and len(REQUIRED_CANONICAL_COLUMNS) == 5 and len(NON_EQUITY_SECURITY_TYPES) == 9"` exits 0.
    - _Design: "New module-level constants"_
    - _Requirements: 1.5_

  - [x] 1.2 Add `check_v42_ownership_collisions(primary_config: list[dict]) -> list[str]` to `scraper.py`
    - Function returns `sorted(set(t["ticker"] for t in primary_config) & V42_ETFS)`.
    - For every collision, emit a single-line warning: `⚠️  Ownership collision: '<ticker>' appears in both config.json and V42_ETFS`.
    - Function MUST NOT raise; an empty intersection is the healthy case (returns `[]`).
    - Pure-ish: only side effect is logging. Suitable to call from `main()` once at startup.
    - **Done-when:** Unit-callable from REPL with a synthetic config list; returns the expected sorted intersection; logs one warning per collision; never raises.
    - _Design: "Components and Interfaces → check_v42_ownership_collisions"_
    - _Requirements: 1.3, 1.5_
    - _Validates: Property 1_

  - [x] 1.3 Add `clean_canonical_csv(csv_path: str) -> tuple[pd.DataFrame, list[str]]` to `scraper.py`
    - Pure function. Only I/O is reading `csv_path` via `pd.read_csv`.
    - Validation order (must match design):
      1. Read CSV; on parse exception return `(empty_df, ["read failed: <message>"])`.
      2. Verify `REQUIRED_CANONICAL_COLUMNS ⊆ df.columns`; on miss return `(empty_df, ["missing columns: {set}"])`.
      3. Drop rows whose `security_type` (lower-cased) contains any substring in `NON_EQUITY_SECURITY_TYPES`.
      4. Drop rows whose `ticker` is null/empty/leading-`$`.
      5. Coerce `weight_pct` via `pd.to_numeric(errors='coerce')`, divide by 100; drop rows where the result is `≤ 0` or `> 1` (logs offending row identifier when `weight_pct > 100` per Req 14.2).
      6. Parse `as_of_date` via `pd.to_datetime(errors='coerce')`; drop NaT rows.
      7. Build `Date_Scraped` from `scrape_date` (`YYYYMMDD`) when present, else fall back to `TODAY`.
      8. Rename `etf → ETF_Ticker` and project columns to Pipeline_Schema order: `[ETF_Ticker, ticker, name, weight, Holdings_As_Of, Date_Scraped]`.
      9. When `as_of_date > Date_Scraped`, log warning naming the ETF but still include the row (Req 14.3).
    - Returned DataFrame MUST be empty (zero rows) whenever any validation step short-circuits.
    - **Done-when:** Function returns Pipeline_Schema column order on a valid fixture CSV; returns empty DataFrame plus errors list on a CSV missing `as_of_date`; excludes a `$USD` row and a `Money Market Fund` row from a mixed fixture.
    - _Design: "Components and Interfaces → clean_canonical_csv" + "Error Handling → Canonical_CSV Validation Failures" table_
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8, 3.9, 12.1, 12.2, 12.3, 12.4, 14.2, 14.3_
    - _Validates: Properties 2, 3, 4_

  - [x] 1.4 Add `write_v42_latest_snapshots(cleaned_df: pd.DataFrame) -> None` to `scraper.py`
    - For every distinct `ETF_Ticker` value in `cleaned_df`, write `data/latest/{ETF_Ticker}.csv` containing only that ETF's rows in Pipeline_Schema column order.
    - Overwrite any pre-existing per-ETF latest file (matches primary loop semantics).
    - Wrap each per-ETF write in try/except so one ETF's I/O failure does not abort writes for the others.
    - No-op on empty `cleaned_df`.
    - **Done-when:** Calling on a DataFrame with two ETFs creates exactly two files under `data/latest/`, each containing the correct subset; calling on an empty DataFrame creates no files and does not raise.
    - _Design: "Components and Interfaces → write_v42_latest_snapshots" + "Error Handling → Sink-Write Failures" table_
    - _Requirements: 4.2_
    - _Validates: Property 6 part (b)_

  - [x] 1.5 Add `append_v42_to_master_archive(cleaned_df: pd.DataFrame, today: str) -> None` to `scraper.py`
    - Derive the archive directory from `today` (`YYYY-MM-DD`) → `data/history/YYYY/MM/DD/`.
    - If `master_archive.csv` does not exist at that path, create it with `cleaned_df` content in Pipeline_Schema column order.
    - If it exists, read it, `pd.concat` with `cleaned_df`, deduplicate on `(ETF_Ticker, ticker, Holdings_As_Of)` keep-last, write back atomically.
    - On any I/O exception, log error and leave the existing file untouched.
    - No-op on empty `cleaned_df`.
    - **Done-when:** Two consecutive calls with the same DataFrame leave the master_archive byte-identical after the second call (idempotent); a call when the file does not exist creates it; a call when the file exists appends without duplicating any composite key.
    - _Design: "Components and Interfaces → append_v42_to_master_archive" + "Error Handling → Sink-Write Failures" table_
    - _Requirements: 4.3, 4.4_
    - _Validates: Property 6 part (c)_

  - [x] 1.6 Add `bridge_write_all_sinks(cleaned_df: pd.DataFrame) -> None` to `scraper.py`
    - Single fan-out helper that, in this fixed order, calls:
      1. `update_giant_history([cleaned_df])` (existing function — append + dedupe).
      2. `write_v42_latest_snapshots(cleaned_df)` (1.4).
      3. `append_v42_to_master_archive(cleaned_df, TODAY)` (1.5).
    - Before fan-out, log per-ETF row counts in the format `{TICKER:6} {rows:4} rows | as_of={YYYY-MM-DD}` (matches primary-loop format per design "Logging Conventions").
    - No-op on empty `cleaned_df` (but still log `⚠️  Bridge: 0 valid rows`).
    - This helper MUST be invoked AFTER the primary loop has written the primary master_archive (Req 4.5).
    - **Done-when:** Calling on a non-empty DataFrame produces (a) all triples present in `data/all_history.csv`, (b) one `data/latest/{ETF}.csv` per distinct ETF, and (c) the dated `master_archive.csv` containing the rows; calling on an empty DataFrame leaves all three sinks unchanged.
    - _Design: "Components and Interfaces → bridge_write_all_sinks" + "Architecture → Bridge as a Pipeline Stage"_
    - _Requirements: 4.1, 4.5_
    - _Validates: Property 6 (full)_

  - [x] 1.7 Refactor `run_extended_scrapers()` in `scraper.py` to compose the helpers from 1.2–1.6
    - Replace the existing inline body with the flow documented in design "Components and Interfaces → run_extended_scrapers":
      1. Resolve `scraper_path`. If missing, log `❌` and return.
      2. Resolve `csv_out` path (`etf_holdings_YYYYMMDD.csv` at repo root).
      3. If `csv_out` exists at start: log `Bridge: reusing existing canonical CSV at <path>` and skip subprocess (Req 2.7, 14.5).
      4. Else invoke v42 via `subprocess.run(... timeout=600, capture_output=True, text=True)`. Catch `subprocess.TimeoutExpired` and generic `Exception`. In all subprocess outcomes, print the last 3000 chars of stdout to the run log (Req 2.6); on non-zero return code, also print the last 1000 chars of stderr (Req 2.3).
      5. After subprocess: if `csv_out` still missing, log `❌ No extended output` and return.
      6. `cleaned_df, errors = clean_canonical_csv(csv_out)` (1.3). If `errors` non-empty, log each `⚠️ ` line and return.
      7. If `cleaned_df` empty, log `⚠️  Bridge: 0 valid rows` and return.
      8. Else call `bridge_write_all_sinks(cleaned_df)` (1.6).
    - Reuse-existing-CSV branch MUST NOT re-invoke the subprocess (Req 14.5).
    - The existing call site at the end of `main()` stays as-is.
    - **Done-when:** Touching `etf_holdings_YYYYMMDD.csv` (using a valid fixture) and running `python scraper.py` causes the bridge to skip the subprocess, log the reuse line, and write all three sinks; deleting the CSV causes the subprocess to be invoked.
    - _Design: "Components and Interfaces → run_extended_scrapers" + "Error Handling → v42 Subprocess Failures"_
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8_

  - [x] 1.8 Add startup ownership-collision check at the top of `main()` in `scraper.py`
    - Immediately after `etfs = json.load(f)` loads `config.json`, call `collisions = check_v42_ownership_collisions(etfs)`.
    - On non-empty result, log a single warning line listing the collided tickers (the function from 1.2 already logs per-collision details).
    - MUST NOT abort execution — collisions are warnings only (Req 1.3).
    - **Done-when:** Running `python scraper.py` on a config.json containing a v42 ticker (synthetic test) prints the warning and continues; running on a clean config produces no warning.
    - _Design: "Components and Interfaces → Changed call site in `main()`"_
    - _Requirements: 1.3_

- [x] 2. Build the bridge test suite
  - Phase 2 starts after Phase 1 is fully complete (specifically once 1.7 is finished and the helpers are importable).
  - Tests live in `tests/test_bridge.py` (new file). Existing tests in `tests/test_scoring.py` are unaffected.

  - [x] 2.1 Add `hypothesis` to `requirements.txt` as a test dependency
    - Append a single line `hypothesis>=6.0` (or pin to a recent stable minor) under the existing requirements list.
    - Verify locally with `pip install -r requirements.txt` that the install resolves cleanly.
    - **Done-when:** `python -c "import hypothesis"` exits 0 in the project venv; `requirements.txt` diff shows exactly one added line.
    - _Design: "Testing Strategy → Property-Based Testing → Library"_
    - _Requirements: 11.1, 11.3, 11.4_

  - [x] 2.2 Create `tests/test_bridge.py` with five test functions
    - All PBT tests use `@given` strategies and `@settings(max_examples=100, deadline=2000)`.
    - Each PBT function carries a header comment of the form `# Feature: v42-scraper-integration, Property N: <property text>`.
    - Tests must monkeypatch the module-level `GIANT_HISTORY_FILE`, `DATA_DIR_LATEST`, and any history-dir constants in `scraper.py` so no test touches the real `data/` directory; use `tmp_path` for all fixtures.
    - The five required functions:
      1. **`test_bridge_cleans_canonical_csv_to_pipeline_schema(tmp_path)`** — PBT.
         - `# Feature: v42-scraper-integration, Property 2`
         - `# Feature: v42-scraper-integration, Property 4`
         - `# Feature: v42-scraper-integration, Property 5`
         - Fixture CSV with rows for ≥ 2 V42 ETFs (e.g. VLUE + JHMM), all canonical columns populated. Assert `clean_canonical_csv` returns Pipeline_Schema columns in correct order, row count equals input, weight values lie in `(0, 1]`, and a CSV round-trip preserves the `(ETF_Ticker, ticker, Holdings_As_Of)` row set (Property 5).
         - _Validates: Properties 2, 4, 5 — Requirements 11.1, 12.1–12.5_
      2. **`test_bridge_rejects_csv_missing_required_columns(tmp_path)`** — example-based.
         - `# Feature: v42-scraper-integration, Property 3`
         - Hand-crafted CSV missing `as_of_date`. Pre-seed `GIANT_HISTORY_FILE` with a known row set. Assert (a) `clean_canonical_csv` returns `(empty_df, errors_listing_as_of_date)`, (b) calling `bridge_write_all_sinks` on the empty DataFrame leaves the pre-seeded `GIANT_HISTORY_FILE` byte-identical.
         - _Validates: Property 3 — Requirements 3.1, 3.2, 3.9, 11.2_
      3. **`test_bridge_excludes_currency_and_money_market_rows(tmp_path)`** — PBT.
         - `# Feature: v42-scraper-integration, Property 2`
         - Fixture CSV containing one `$USD` ticker, one `$BRL` ticker, one `security_type='Money Market Fund'` row, one `security_type='Cash & Equivalents'` row, and one valid equity row. Assert `clean_canonical_csv` returns exactly one row (the equity). Hypothesis strategy randomises the equity row's fields and the order of the rows.
         - _Validates: Property 2 — Requirements 3.4, 3.5, 11.3_
      4. **`test_bridge_idempotent_on_repeat_invocation(tmp_path)`** — PBT.
         - `# Feature: v42-scraper-integration, Property 6`
         - `# Feature: v42-scraper-integration, Property 7`
         - Hypothesis strategy generates a non-empty Pipeline_Schema DataFrame. Run `bridge_write_all_sinks` twice. Assert (a) the row set in `GIANT_HISTORY_FILE` is identical after both runs (composite-key idempotence) and (b) `data/latest/{ETF}.csv` and `master_archive.csv` are unchanged byte-for-byte after the second call.
         - _Validates: Properties 6, 7 — Requirements 5.1–5.4, 11.4, 14.4_
      5. **`test_metadata_json_contains_all_configured_etfs(tmp_path)`** — example-based.
         - `# Feature: v42-scraper-integration, Property 9`
         - Build a minimal Giant_History fixture with one row per Configured_ETF (read from `config.yaml`). Run `predator.build` (or its loader subset) against it. Assert `metadata.json::etfs` equals `sorted(Configured_ETFs)`.
         - _Validates: Property 9 — Requirements 8.5, 9.1, 11.5_
    - **Done-when:** `python -m pytest tests/test_bridge.py -v` reports 5 passed, 0 failed; each PBT test runs ≥ 100 examples (hypothesis output visible with `-v`); each test header carries the `# Feature: v42-scraper-integration, Property N` comment.
    - _Design: "Components and Interfaces → Module: `tests/test_bridge.py`" + "Testing Strategy"_
    - _Requirements: 11.1, 11.2, 11.3, 11.4, 11.5_
    - _Validates: Properties 2, 3, 4, 5, 6, 7, 9_

- [x] 3. Update `README.md` to reflect the 29-ETF universe
  - Phase 3 starts after Phase 1 is complete. All Phase 3 sub-tasks edit `README.md` and MUST execute sequentially (3.1 → 3.2 → 3.3) to avoid file-conflict races.
  - Phase 3 runs in parallel with Phase 2 (different files: `README.md` vs `tests/test_bridge.py` + `requirements.txt`).

  - [x] 3.1 Update `README.md` header — replace the "16 Smart-Beta ETFs" line with the 29-ETF formulation
    - Replace the historical count and the issuer-list bullet with: "29 Smart-Beta ETFs across Pacer, First Trust, Alpha Architect, Invesco, BlackRock, John Hancock, PIMCO, Avantis, VictoryShares, and Virtus."
    - Per Req 10.1, the count itself MUST be derived from `len(config.yaml::etfs[])` — call this out in the prose so future config edits trigger a README update.
    - **Done-when:** `grep -c "29 Smart-Beta" README.md` ≥ 1; `grep -c "16 Smart-Beta" README.md` == 0; the issuer list contains all 10 names listed above.
    - _Design: "Components and Interfaces → README" point 1_
    - _Requirements: 10.1, 10.3_

  - [x] 3.2 Update `README.md` "ETF Tier Weights" table to list all 29 ETFs grouped by tier with points
    - Read `config.yaml::etfs[]` and group entries by `tier` (S/A/B/C or whatever tiers exist).
    - For each tier, produce a sub-table with columns `Ticker | Name | Points`. Points MUST match `config.yaml` exactly.
    - Include all 8 V42_ETFs (VLUE, AVSC, GRIN, JHMM, JHEM, JHSC, MFEM, JOET) and all Quant additions called out in design point 2 (PDP, DWAS, EEMO, PIZ, IMOM-bumped, FPXI-bumped, IVAL).
    - **Done-when:** Every ticker in `config.yaml::etfs[]` appears in the README tier table exactly once; for each ticker, the README points value matches the `config.yaml` value.
    - _Design: "Components and Interfaces → README" point 2_
    - _Requirements: 10.2, 10.3_

  - [x] 3.3 Add a new "🔌 Component 4: Extended Scraper" section to `README.md`
    - Place it immediately after the existing "Component 1" section (or wherever Components 1–3 currently sit).
    - Section MUST cover: the V42_ETFS set; the `subprocess.run(... timeout=600)` invocation pattern; the `etf_holdings_YYYYMMDD.csv` Canonical_CSV; the Bridge's three sinks (`data/all_history.csv`, `data/latest/{TICKER}.csv`, `data/history/YYYY/MM/DD/master_archive.csv`); the failure-isolation contract (subprocess crash / timeout / per-source failure does not corrupt primary output).
    - **Done-when:** A `## 🔌 Component 4: Extended Scraper` header exists in `README.md`; the section text contains the strings "V42_ETFS", "subprocess", "etf_holdings_", "all_history.csv", "latest/", "master_archive.csv", and "failure isolation".
    - _Design: "Components and Interfaces → README" point 3_
    - _Requirements: 10.4_

- [x] 4. Verification — confirm the integration is wired correctly end-to-end
  - Phase 4 starts only after both Phase 2 (2.2) and Phase 3 (3.3) are complete.

  - [x] 4.1 Run `python -m pytest tests/ -v` locally and confirm all existing tests + 5 new tests pass
    - Run from repository root in the project venv with `requirements.txt` installed.
    - All previously-passing tests (e.g. `tests/test_scoring.py`) MUST still pass.
    - The 5 new tests in `tests/test_bridge.py` MUST pass with no warnings other than hypothesis statistics.
    - **Done-when:** Pytest summary line reports `passed` for every test, `failed` count is 0, and the new `test_bridge.py` file contributes exactly 5 passing tests. Capture the pytest output in the verification log for Phase 4.3.
    - _Design: "Testing Strategy → Configuration"_
    - _Requirements: 11.1, 11.2, 11.3, 11.4, 11.5, 13.1_

  - [x] 4.2 Confirm `scraper.py` imports cleanly via `python -c "import scraper"`
    - Run `python -c "import scraper"` from repository root; exit code MUST be 0 with no stderr output.
    - This catches NameError / function-order regressions of the kind seen in the original v42 hot-fix series.
    - Optionally also run `python -c "from scraper import V42_ETFS, REQUIRED_CANONICAL_COLUMNS, NON_EQUITY_SECURITY_TYPES, check_v42_ownership_collisions, clean_canonical_csv, write_v42_latest_snapshots, append_v42_to_master_archive, bridge_write_all_sinks, run_extended_scrapers"` to verify every helper is exported.
    - **Done-when:** Both commands exit 0 with no stderr; the symbol-import command resolves all 9 names without ImportError.
    - _Design: "Components and Interfaces → Module: `scraper.py`"_
    - _Requirements: 1.5, 2.1, 4.1_

  - [x] 4.3 Document the manual `workflow_dispatch` verification checklist
    - Create `docs/runbooks/v42-rollout-verification.md` (new file) documenting Step 4 of the design's Migration Plan.
    - Section MUST list, as a checkable runbook, every observable an operator should confirm in the first manual workflow run:
      1. **Ownership collision warning** — should be empty (no collisions between `config.json` and `V42_ETFS`).
      2. **Per-ETF row counts** in primary-loop log lines, format `{TICKER:6} {rows:4} rows | as_of={YYYY-MM-DD}` (Req 8.1).
      3. **v42 summary table** with columns `ETF, Status, Rows, Cols`, one row per task (Req 8.3).
      4. **Bridge log lines** — `Bridge: N rows across 8 ETFs` on success, or a `⚠️ ` / `❌ ` line on failure (Req 8.2, 8.4).
      5. **`docs/data/metadata.json::etfs`** — array of length 29 after the build_site workflow runs, containing all V42_ETFS members (Req 8.5, 9.1).
      6. **Dashboard surfacing** — V42 ETFs appear in the ETFs tab and contribute to leaderboard scoring (Req 9.4).
    - Include the exact GitHub Actions navigation path: `Actions → "Daily ETF Scrape" → "Run workflow" (workflow_dispatch)`.
    - This is a docs-only deliverable; it does not modify code or workflows. The new file MUST live under `docs/runbooks/` to avoid conflicts with `README.md` (Phase 3) and code files (Phase 1).
    - **Done-when:** `docs/runbooks/v42-rollout-verification.md` exists; the file contains all six checklist items above with corresponding requirement IDs in parentheses; the file is committable (no template placeholders left).
    - _Design: "Migration Plan → Step 4: Trigger first full run"_
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 9.1, 9.4_

## Notes

- Phase 1 sub-tasks 1.1–1.8 share `scraper.py` and are encoded as a strict chain in the dependency graph. Do not parallelise them.
- Phase 3 sub-tasks 3.1–3.3 share `README.md` and are similarly chained.
- The bridge tests in 2.2 are listed as a single sub-task (per the user-supplied spec) but the file MUST contain five distinct test functions, each carrying the `# Feature: v42-scraper-integration, Property N` tag(s) shown above. The orchestrator can drive 2.2 as a single unit because all five tests live in the same file and share fixtures.
- No task in this plan touches `.github/workflows/daily_scrape.yml` or `.github/workflows/build_site.yml` — design Section "Workflows" confirms both are already correct (xvfb, playwright install, test gate, artifact verification). If a workflow regression is found during Phase 4 verification, raise it as a separate task rather than silently widening this plan.
- Each task references specific requirements for traceability and (where applicable) the property number(s) it implements or validates.
- The plan deliberately omits implementation tasks for `predator/build.py` and `scripts/etf_holdings_scraper_v42.py`. The design confirmed both already satisfy their relevant requirements (predator reads the ETF universe from live data; v42's `run_all` already wraps tasks in try/except and prints a summary table).

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1"] },
    { "id": 1, "tasks": ["1.2"] },
    { "id": 2, "tasks": ["1.3"] },
    { "id": 3, "tasks": ["1.4"] },
    { "id": 4, "tasks": ["1.5"] },
    { "id": 5, "tasks": ["1.6"] },
    { "id": 6, "tasks": ["1.7"] },
    { "id": 7, "tasks": ["1.8"] },
    { "id": 8, "tasks": ["2.1", "3.1"] },
    { "id": 9, "tasks": ["2.2", "3.2"] },
    { "id": 10, "tasks": ["3.3"] },
    { "id": 11, "tasks": ["4.1", "4.2", "4.3"] }
  ]
}
```
