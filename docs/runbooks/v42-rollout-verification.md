# v42 Scraper Integration — Rollout Verification

## When to run this

After deploying the v42 integration to `main`, run the daily scraper manually
to verify the bridge wires through end-to-end before relying on the scheduled
cron. Re-run on any deploy that touches `scraper.py`, `config.yaml`,
`config.json`, `conviction/`, or `.github/workflows/daily_scrape.yml`.

## How to trigger

Navigation path: `Actions → "Daily ETF Scrape" → "Run workflow" (workflow_dispatch)`.

1. Open `<repo>/actions`, select **Daily ETF Scrape**, click **Run workflow**
   (top-right), keep branch `main`, click **Run workflow**.
2. Watch the run; expected duration ~6–10 minutes.
3. Once green, confirm **build_site** auto-triggers and completes green before
   checking observable 5 below.

## Six observables to confirm

Each item below is independently checkable in the run log (or, for 5–6, in
GitHub Pages output). All six MUST pass before declaring the rollout healthy.

### 1. Ownership collision warning (Req 1.3, healthy = silent)

- [ ] Search the `Run scraper.py` step for `Ownership collision`.
- **Expected:** zero matches. The intersection of `config.json` and `V42_ETFS`
  is empty, so no warning lines are printed.
- **If wrong:** A ticker is double-owned. Decide which owner keeps it, remove it
  from the other, redeploy. Do not proceed.

### 2. Per-ETF row counts in primary loop (Req 8.1)

- [ ] Confirm one log line per primary-config ticker, exact format
  `{TICKER:6} {rows:4} rows | as_of={YYYY-MM-DD}`.
- **Expected:** rows ≥ 1 for every ticker; `as_of` within the last 7 calendar days.
- **If wrong:** A primary scraper regressed. Inspect that ETF's source URL
  before relying on this run.

### 3. v42 summary table (Req 8.3)

- [ ] Confirm a table with columns `ETF, Status, Rows, Cols` printed by
  `scripts/etf_holdings_scraper_v42.py`, one row per v42 task.
- **Expected:** every row has `Status=ok` and `Rows ≥ 1`. `Cols` matches the
  canonical schema width (5+).
- **If wrong:** A v42 task failed in isolation. Bridge will skip that ETF; the
  primary output is still safe to keep.

### 4. Bridge log lines (Req 8.2, 8.4)

- [ ] On success: a line of the form `Bridge: N rows across 8 ETFs`.
- [ ] On failure: a `⚠️ ` line (validation rejected the canonical CSV) or a
  `❌ ` line (subprocess crash / missing output).
- **Expected:** exactly one success line, no `⚠️ ` / `❌ ` lines.
- **If wrong:** Capture the warning text and triage against
  `clean_canonical_csv` validation steps in `scraper.py`.

### 5. `docs/data/metadata.json::etfs` after build_site (Req 8.5, 9.1)

- [ ] After the `build_site` workflow finishes, fetch the deployed
  `docs/data/metadata.json` and confirm `etfs` is an array of length **29**.
- [ ] All 8 V42_ETFS members are present:
  `VLUE, AVSC, GRIN, JHMM, JHEM, JHSC, MFEM, JOET`.
- **Expected:** `len(metadata.etfs) == 29`; `set(V42_ETFS) ⊆ set(metadata.etfs)`.
- **If wrong:** `conviction.build` did not see the v42 rows. Confirm
  `data/all_history.csv` contains rows for the missing ETFs and that the
  build_site workflow ran after the scrape workflow.

### 6. Dashboard surfacing (Req 9.4)

- [ ] Open the deployed dashboard. Confirm V42 ETFs appear in the **ETFs** tab.
- [ ] Confirm V42 holdings contribute to **leaderboard** scoring (a non-zero
  score row exists for at least one stock held only by a V42 ETF).
- **If wrong:** The data layer is healthy (observable 5 passed) but the
  frontend is filtering them out. Inspect the dashboard's ETF-allowlist code.

## Rollback

If any observable fails and cannot be triaged in the same session, revert the
deploy commit. The bridge writes are append-only and idempotent, so the giant
history file and per-ETF latest snapshots are safe to leave in place; the next
clean run will reconcile them.
