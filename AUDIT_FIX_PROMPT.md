# Predator Protocol — Audit Remediation Prompt (for the coding agent)

You produced two audit reports. This prompt tells you **what to fix, in what order, and what NOT to touch.** Most findings are real; some are future-proofing the owner has explicitly chosen to defer; a few may be false positives. **Your job is not to fix all 50 findings — it is to fix the ones that crash, corrupt, or silently degrade, while breaking nothing that currently works.**

## Prime directives (violating any of these is a failure)

1. **VERIFY BEFORE FIXING.** For every bug below, first write/run a tiny reproduction (a focused test or a one-off script) that demonstrates the bug on the *current* code. If you cannot reproduce it, mark it `UNCONFIRMED` and do **not** change code for it. We do not "fix" things that aren't broken — that's how working code dies.
2. **THE SCRAPER IS NEAR-SACROSANCT.** `scraper.py` and `scripts/etf_holdings_scraper_v42.py` currently work against live issuer sites. Make the **minimum** change required for each confirmed scraper bug. No refactors, no renaming, no "while I'm here" cleanup. After ANY change to scraper code, you MUST pass the dry-run gate (see §Dry-Run) before committing.
3. **EVERY fix gets a test** that fails before and passes after. No fix is "done" until `pytest tests/ -v` is green and the new test exists.
4. **ATOMIC, REVERSIBLE COMMITS.** One logical fix per commit, with the finding ID in the message (e.g. `fix(history): #2A pd.Series.get crash`). Do not squash unrelated fixes.
5. **DEFER what is marked DEFER.** Do not touch it even if it offends you. Adding code for it is out of scope and risks regressions.
6. **When in doubt, ask — do not guess.** A wrong "fix" to working code is worse than an open bug.

---

## TIER 1 — Confirmed-class crashes & silent corruption (FIX FIRST)

> Verify each with a reproduction before editing. If the build is currently green, some of these may not be hit on the current data — confirm the trigger condition, then fix defensively.

**#1A — `history.py` `pd.Series.get()` crash.** `score_yday = s.get(yday, float("nan"))`. `pd.Series.get()` does not behave like dict.get for label lookups the way used here. Repro: call `streaks_and_deltas` on a small history fixture. Fix: `s.loc[yday] if yday in s.index else float("nan")`. Test the temporal path on a 2-snapshot fixture.

**#1B — `build.py` `leaderboard_rank` checked only in first snapshot.** The loop assumes every snapshot has the column because only `window_cols[0]` was checked. Repro: feed a history list where one older snapshot lacks `leaderboard_rank`. Fix: guard per-snapshot (`if "leaderboard_rank" in historical[d].columns`) and skip/fill missing ones; never let it raise.

**#1C — `ticker_metadata.csv` referenced but never generated → sector/country flow permanently empty.** Confirm `flow.json` is empty in the current build output. Fix options (pick the simplest that works): (a) generate `ticker_metadata.csv` as a committed build input with a documented refresh script, or (b) make `_attach_metadata` resolve the path via `Path(__file__).resolve().parent.parent/"data"/...` AND make the flow builder degrade gracefully with a visible "metadata unavailable" state instead of silently empty. Either way: the flow tabs must either show data or honestly say why not. Also fix the relative-path fragility (#build.py:423) in the same commit.

**#1D — `asof` derived only from Excel, so freshness gate false-flags everything stale.** Repro: stale Excel + fresh FRED → `asof` is old. Fix: compute `asof = max(last date across ALL sources merged into market_returns.json)`, not just Excel. Re-run `check_freshness` to confirm fresh FRED no longer trips it.

**#1E — No atomic writes anywhere → truncated JSON poisons next build.** Add a single `write_json_atomic(path, obj)` helper (write to `path.tmp`, `os.replace`). Route the JSON/parquet writers in `build.py`, `markets_history.py`, `ingest_markets_xl.py`, `vol_history.py`, `fetch_prices.py` through it. This is low-risk and high-value. Test: simulate a write that raises mid-stream → original file intact.

**#1F — `fetch_history` GitHub fallback hides missing local file with stale data.** Keep the fallback (useful for local dev) but: log loudly, and if the fallback is used in CI (detect `CI` env var), **fail the build** rather than deploy silently. Stale-but-green is the worst outcome.

**#1G — `markets_history.build_output` imports private internals from `ingest_markets_xl` (`_load_existing`, `_merge_monthly`) → breaks tests that patch `OUTPUT_PATH`, and couples two modules writing the same file.** Decide one authoritative owner of `market_returns.json` merge logic, expose it as a public function, import that. Confirm `test_markets_history_build_output_preserves_excel_history` passes against the patched path.

**#1H — FX never updated by `markets_history.py` (the `invert` branch is unreachable for FX; `fx_out` only carries forward existing).** This means the **currency lens (INR etc.) in markets.html silently has no data on a fresh build** — directly relevant to the planned Asset Detail work. Repro: fresh build → inspect `market_returns.json::fx`. Fix: populate the FX section from the fetched/Excel FX series; apply inversion in the FX path. Test that `fx.USDINR` is non-empty after build.

---

## TIER 2 — File-size / unbounded growth (the owner's flagged concern — DESIGN + IMPLEMENT)

**#2 — `all_history.csv` and `data/history/` grow unbounded; `build.py` loads the whole CSV every build.**

Implement **year-partitioned Parquet** as the historical store:
- New layout: `data/history_parquet/year=YYYY/holdings.parquet` (one file per year, append within the current year, never rewrite past years).
- Keep `all_history.csv` as a **rolling recent window** (e.g. last 120–180 days) that the scraper appends to cheaply — preserves current scraper behavior and the dedup key `(ETF_Ticker, ticker, Holdings_As_Of)`.
- A small migration script converts the existing `all_history.csv` into the partitioned store once.
- `build.py` reads from the partitioned store (push date filters down so it only loads the window it needs — e.g. the leaderboard 120-day lookback doesn't load 2 years).
- Parquet is compressed + columnar → smaller on disk and faster to load than the CSV; old years are immutable so git stops re-diffing them.
- Add `data/markets_history/` and `data/vol_history/` (parquet caches) to `.gitignore` so caches are never committed (audit #1.3 of report 2). Move the one large `.xlsx` to **Git LFS**.

Acceptance: build reads partitioned store; a simulated 2-year dataset loads in similar time to today's; `git status` shows no parquet cache files staged; old-year partitions are byte-stable across builds.

> NOTE: This is the one place a bigger change is justified. Do it behind a clear seam, keep the CSV path working as a fallback until the partitioned path is proven, and migrate—don't delete—the existing CSV.

---

## TIER 3 — Scraper fragility (DEFER logic changes; add ONLY a cheap staleness alarm)

The owner's decision, which is correct: issuer-site fragility (Invesco undocumented API, Alpha Architect DOM, ChromeDriver drift, Pacer cookies, yfinance breakage) **cannot be hardened against in advance**. When a scraper breaks, a new scraper is written. **Do NOT** add retries, configurable sleeps, UA rotation, or driver-version pinning now — out of scope.

**The ONE thing to add (no scraper-logic change):** a **per-ETF staleness check** that runs *after* scraping and emits a loud warning (GitHub Actions `::warning::` annotation, and a `stale_etfs` list in `metadata.json`) for any ETF whose latest `Holdings_As_Of` is older than N trading days. This touches only output inspection, not scrape logic, so it can't break scraping. Surface the same list as a small badge in the dashboard header. This is how the owner learns a scraper died in 1 day instead of 3 weeks.

Also DEFER (do not implement now, just note): hardcoded sleeps, UA string, retry logic, ChromeDriver pinning, race-condition hardening on git push, CDN SRI hashes. These are real but low-priority and several risk breaking the working pipeline.

---

## TIER 4 — Safe papercuts (batch into one or two commits)

- **`requirements.txt` (+ `requirements-scraper.txt`)** with pinned versions for reproducibility and working pip cache. Build deps: pandas, pyyaml, pyarrow, yfinance, openpyxl, fredapi, python-dotenv, requests, lxml, html5lib, beautifulsoup4, pytest, hypothesis. Scraper-only: selenium, curl_cffi, playwright, pdfplumber, xlrd. Switch CI to `pip install -r requirements.txt`.
- **`stock.html` Promise.all has no `.catch()` on `score_history.json` / `holdings_history.json`** → one missing file blanks the whole page. Add `.catch(() => ({}))` (or `[]`) to both, and render an empty-state.
- **CI verify step doesn't check the files `stock.html` requires.** Add `test -f` for `score_history.json`, `holdings_history.json`, `flag_history.json`, `vol_history.json`, `prices.json` to the verify step.
- **`leaderboard.json` sanity check in CI:** valid JSON + non-zero entry count, so an empty leaderboard can never deploy silently.
- **Wrong command in `markets.html` error string:** `python predator/markets_history.py` → `python -m predator.markets_history`.
- **`ingest_mega_xl` shim forwards `--series`→`--assets` but `ingest_markets_xl` has no `--assets` arg** (report 2 #1 critical). Either add the arg or fix the shim; add a test that the documented CLI invocation runs.
- **Zero-weight rows pass `dropna` and inflate `etf_count`/`held_by`** (`scoring.py`). Filter `weight > 0` (or a tiny epsilon) before counting breadth. Add a test.
- **`markets.json` partial-fetch guard uses `<` not a meaningful drop.** Change to skip only on a meaningful drop (e.g. `< existing_count * 0.9`) so one failed series doesn't block all updates.
- **yfinance tz-naive/aware concat risk:** add `.tz_localize(None)` after extracting monthly close in `markets_history._fetch_yfinance_series` (report 2 medium). Cheap insurance against a known regression.

---

## Dry-Run gate (MANDATORY before committing ANY scraper-adjacent change)

If a fix touches `scraper.py` or `scripts/etf_holdings_scraper_v42.py`:
1. Run the scraper in a **dry-run / limited mode** against 2–3 representative ETFs (one `invesco_api`, one `selenium_alpha`, one `pacer_csv`) — do NOT write to the real `all_history.csv`; write to a temp path.
2. Confirm row counts, weight normalization, and `Holdings_As_Of` parsing match a pre-change baseline for those ETFs.
3. Only if identical (modulo the intended fix) may you commit. If the scraper has no dry-run flag, add a minimal `--dry-run --etfs TICKER,TICKER` that writes to a temp dir — that's an allowed scraper change because it's additive and inert by default.

---

## The loop (how to run this)

1. **Pass 1:** Work Tier 1 → Tier 2 → Tier 4, each with verify-repro → fix → test. Skip/annotate UNCONFIRMED. Leave Tier 3 except the staleness alarm.
2. **Green gate:** `pytest tests/ -v` all green; build runs end-to-end producing all expected JSON; scraper dry-run unchanged.
3. **Re-audit:** run a fresh full audit (same depth as before). Produce a new findings list.
4. **Triage the new findings against these same Tier rules** — do NOT auto-fix everything. Real crashes/corruption → fix. New future-proofing → defer & list. Report what you're deferring and why.
5. **Repeat** until a re-audit surfaces no Tier-1/Tier-2 issues. Then stop and hand back a summary: fixed, deferred (with reasons), and any UNCONFIRMED findings for human review.

**Stop conditions / ask-the-human triggers:** any change that would alter scraper scrape logic beyond the minimum; any fix that changes leaderboard *scores* (that's a separate, deliberate workstream — do not touch scoring math here); any finding you cannot reproduce but the audit rates critical; any schema change to `all_history.csv` columns.

---

### Out of scope for this prompt (do not start here)
The Markets/Portfolio-Lab/Leaderboard-scoring redesigns live in a separate spec (`MARKETS_SECTION_OVERHAUL.md`). This prompt is **bug remediation only** — make the existing system correct and self-sustaining without changing what it computes.
