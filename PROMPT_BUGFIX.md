# Predator Protocol — Critical Bug Fix Sweep

You are working in the repo `yieldchaser/etf-data` (Phase 1 of the Predator Protocol site is deployed at `https://yieldchaser.github.io/etf-data/`). Four bugs have been confirmed in the live dashboard. Fix all four. Do not stop until every "Definition of Done" passes.

## Repo layout (do not invent paths — these exist)

```
.
├── scraper.py                              # V17.4, untouched, writes data/all_history.csv
├── config.json                             # scraper config, untouched
├── config.yaml                             # scoring & sanitizer config (Phase 1 — edit this)
├── data/all_history.csv                    # 81k+ rows, 16 ETFs, untouched
├── predator/
│   ├── __init__.py
│   ├── scoring.py                          # Sanitizer + Config + compute_leaderboard + compute_rank_deltas
│   ├── history.py                          # snapshot_dates, historical_leaderboards, streaks_and_deltas, changelog
│   └── build.py                            # orchestrator → docs/data/*.json|*.parquet
├── tests/test_scoring.py                   # 19 passing tests covering Sanitizer + scoring formula
├── docs/
│   ├── index.html                          # Alpine.js + Tailwind dashboard
│   └── data/                               # generated; do NOT edit by hand
└── .github/workflows/
    ├── daily_scrape.yml                    # existing scraper schedule, untouched
    └── build_site.yml                      # runs predator.build + deploys Pages
```

## How to verify locally before pushing

```bash
pip install -r requirements.txt
python -m pytest tests/ -v          # MUST be all green
python -m predator.build            # builds docs/data/*
python -m http.server -d docs 8000  # preview at http://localhost:8000
```

---

## Bug 1 — ETF Holdings panel renders empty after clicking any ETF

### Confirmed root cause

`docs/data/holdings_latest.json` contains duplicate `(ETF_Ticker, ticker)` rows after the GOOG→GOOGL rename. Specifically (verified against live JSON):
- QQQM has GOOGL 8× — 4 rows at rank 6, 4 rows at rank 8
- RPG has GOOGL 8× — 4 rows at rank 48, 4 rows at rank 55
- SPMO has GOOGL 8× — 4 rows at rank 4, 4 rows at rank 5
- XLG has GOOGL 8× — 4 rows at rank 5, 4 rows at rank 7

The 4× multiplier is a Cartesian product: `predator/build.py` does `latest_out.merge(deltas[...], on=["ETF_Ticker", "ticker"], how="left")`. Both sides have 2 rows for `(QQQM, GOOGL)`, the merge produces 2 × 2 = 4. Then the same 4 duplicate-ranked rows appear in the ETFs tab.

The current Alpine `x-for` key in `docs/index.html` (line 348) is `\`${h.ETF_Ticker}-${h.rank}\``, which is *still* not unique. Alpine fails its first render of the ETFs tab (default `activeETF: 'QQQM'`), and the reactivity is broken for all subsequent ETF clicks — that's why PIE (which has no duplicates of its own) renders empty.

### Required fix

Fix at the data layer in `predator/scoring.py`, not the UI. After `GOOG→GOOGL` rename in `Sanitizer.apply()`, dedupe by `(ETF_Ticker, ticker, Holdings_As_Of)` by **summing `weight`** and keeping the most recent metadata. Then `compute_leaderboard` and `compute_rank_deltas` produce one row per (ETF, ticker) per snapshot date.

Add a private method `_dedupe_after_renames()` on `Sanitizer`:

```python
def _dedupe_after_renames(self, df: pd.DataFrame) -> pd.DataFrame:
    """After GOOG→GOOGL rename, sum weights for duplicate (ETF_Ticker, ticker, Holdings_As_Of) keys.

    Without this, dual-class shares that get renamed to the same ticker create
    Cartesian-product explosions in downstream merges (build.py line ~75:
    latest_out.merge(deltas, on=['ETF_Ticker', 'ticker'])).
    """
    if df.empty or not self.ticker_renames:
        return df
    key = ["ETF_Ticker", "ticker", "Holdings_As_Of"]
    # Group and aggregate. Keep first-seen name and latest Date_Scraped.
    agg = df.groupby(key, as_index=False).agg(
        weight=("weight", "sum"),
        name=("name", "first"),
        Date_Scraped=("Date_Scraped", "max"),
    )
    # Restore column order (HOLDINGS_COLUMNS contract)
    return agg[["ETF_Ticker", "ticker", "name", "weight", "Holdings_As_Of", "Date_Scraped"]]
```

Call it at the end of `Sanitizer.apply()`, after step 4 (ticker renames).

### Also strengthen the UI key as defense-in-depth

In `docs/index.html`, line 348, change:
```html
<template x-for="h in etfHoldings" :key="`${h.ETF_Ticker}-${h.rank}`">
```
to:
```html
<template x-for="(h, i) in etfHoldings" :key="`${h.ETF_Ticker}-${h.ticker}-${i}`">
```
The `i` index guarantees uniqueness even if the data layer ever regresses.

### Definition of Done — Bug 1

1. `python -c "import json, collections; h = json.load(open('docs/data/holdings_latest.json')); c = collections.Counter((r['ETF_Ticker'], r['ticker']) for r in h); print({k:v for k,v in c.items() if v > 1})"` prints `{}`
2. Loading `https://yieldchaser.github.io/etf-data/`, clicking the ETFs tab, and clicking through CALF → COWZ → CSD → FPX → FPXI → IMOM → PIE → QMOM → QQQM → RPG → SPHB → SPHQ → SPMO → XLG → XMMO → XSMO **shows holdings in the right panel for every ETF**.
3. Rapidly clicking ETFs in succession (5 clicks in 2 seconds) does not produce an empty panel.
4. New test in `tests/test_scoring.py`:
   ```python
   def test_googl_dedupe_after_rename(self, cfg):
       """Sanitizer must collapse GOOG+GOOGL rows from the same ETF/date into one row."""
       df = _h([
           ("SPMO", "GOOG",  "Alphabet Inc", 0.04, "2026-05-01", "2026-05-01"),
           ("SPMO", "GOOGL", "Alphabet Inc", 0.05, "2026-05-01", "2026-05-01"),
           ("SPMO", "AAPL",  "Apple Inc",    0.07, "2026-05-01", "2026-05-01"),
       ])
       out = cfg.sanitizer.apply(df)
       googl = out[out["ticker"] == "GOOGL"]
       assert len(googl) == 1
       assert googl["weight"].iloc[0] == pytest.approx(0.09)
   ```

---

## Bug 2 — `Cash&Other` placeholder leaks into the leaderboard

### Confirmed in live data

`docs/data/leaderboard.json` contains: `{"ticker": "Cash&Other", "company": "Cash & Other", "final_score": 27, ...}`. This shows up in the Changes tab as a "biggest gainer" at +22.7%. It's a holding placeholder used by some ETF issuers, not a tradeable security.

### Required fix

Edit `config.yaml`. Under `sanitizer:`, expand `blocked_tickers` to include the placeholder, and expand `blocked_name_exact` for the canonical name. **Do not** uncomment `blocked_name_patterns` — that's the aggressive VBA-style filter the user explicitly opted out of for Power Query parity.

```yaml
sanitizer:
  blocked_tickers:
    - USD
    - $USD
    - $CAD            # add: VBA blocks it; harmless to add since none should slip through
    - Cash&Other      # add: the placeholder ticker seen in live data
    - "-"             # add: some scrapes emit literal "-" for unidentified rows

  blocked_name_exact:
    - "Short-Term Investment Trust - Invesco Government & Agency Portfolio"
    - "Cash & Other"  # add: exact name match
    - "Cash"          # add
    - "Other"         # add
```

### Definition of Done — Bug 2

1. `python -c "import json; lb = json.load(open('docs/data/leaderboard.json')); print([r for r in lb if 'cash' in r['ticker'].lower() or 'other' in r['ticker'].lower() or 'cash' in (r.get('company') or '').lower()])"` prints `[]`
2. Existing test `test_partial_company_name_match_does_not_block` still passes (the new entries are *exact* matches, not substrings).

---

## Bug 3 — Same company under multiple ticker formats not merged (TPRO IM vs TPRO.IM)

### Confirmed in live data

- `TPRO IM` (IMOM, Quant tier, score 153) and `TPRO.IM` (FPXI, Scout tier, score 70) are both Technoprobe SpA. Combined score would be ~223 in Quant+Scout (would flag as multi-tier but not HIGH_CONVICTION).
- Similar pattern for `5020 JP`, `8031 JP`, `8058 JP`, `6963 JP`, `6269 JP`, `1605 JP`, `7735 JP`, `REP SM`, `WDS AU`, `PLS AU`, `SAND SS`, `6146 JP`, `6920 JP` — international tickers with whitespace from IMOM. The dotted variants (e.g., `6857.JP`) come from FPXI.

The current `ticker_replacements` only normalizes `-` and `/` to `.`. Whitespace is not normalized, so `8058 JP` and `8058.JP` are treated as different tickers.

### Required fix

In `config.yaml`, extend `sanitizer.ticker_replacements`:

```yaml
sanitizer:
  ticker_replacements:
    "-": "."
    "/": "."
    " ": "."          # add: collapse whitespace inside tickers (e.g., "8058 JP" → "8058.JP")
```

And in `predator/scoring.py`, `Sanitizer.apply()` step 3, strip leading/trailing whitespace *before* applying replacements and collapse any multiple-dot artifacts after:

```python
# 3. Ticker punctuation standardization
cleaned = out["ticker"].astype(str).str.strip()
for old, new in self.ticker_replacements:
    cleaned = cleaned.str.replace(old, new, regex=False)
# Collapse any "..".. artifacts from chained replaces (e.g., "BF / B" → "BF.."B → "BF.B")
cleaned = cleaned.str.replace(r"\.+", ".", regex=True).str.strip(".")
out["ticker"] = cleaned
```

### Definition of Done — Bug 3

1. New test in `tests/test_scoring.py`:
   ```python
   def test_whitespace_ticker_normalization(self, cfg):
       df = _h([
           ("IMOM", "8058 JP", "Mitsubishi Corp", 0.022, "2026-05-01", "2026-05-01"),
           ("FPXI", "8058.JP", "Mitsubishi Corp", 0.015, "2026-05-01", "2026-05-01"),
           ("IMOM", "TPRO IM", "Technoprobe SpA", 0.020, "2026-05-01", "2026-05-01"),
           ("FPXI", "TPRO.IM", "Technoprobe SpA", 0.016, "2026-05-01", "2026-05-01"),
       ])
       out = cfg.sanitizer.apply(df)
       assert set(out["ticker"]) == {"8058.JP", "TPRO.IM"}
   ```
2. After rebuild, `python -c "import json; lb = json.load(open('docs/data/leaderboard.json')); print([r['ticker'] for r in lb if 'TPRO' in r['ticker']])"` returns `['TPRO.IM']` (single entry), not `['TPRO IM', 'TPRO.IM']`.

---

## Bug 4 — General UI/UX polish & performance sweep

### What's wrong

1. No loading state: `docs/index.html` shows blank KPI strip and tables while `Promise.all([fetch(...)])` runs in `app().load()`. First paint looks broken.
2. `etfHoldings` getter recomputes a full `Array.filter().sort()` on every state read (Alpine reads getters many times per render). For 1300+ rows this is fine, but with rapid ETF switching it stutters on slower devices.
3. No empty state in the ETFs panel — if a future ETF has zero holdings, the user sees just headers.
4. No error retry button in the error toast — only console-debuggable.
5. The Score column should be sortable by Day Δ% as a tiebreaker (currently sorts by score alone, score 0 ties cluster alphabetically).

### Required fixes (all in `docs/index.html`)

#### 4a. Loading state

Add this above the KPI strip (before `<!-- ── KPI STRIP ── -->`):

```html
<div x-show="loading" class="rounded-lg border p-8 mb-5 text-center pulse-load" style="background: var(--surface); border-color: var(--border); color: var(--text-3)">
  <div class="text-xs label">LOADING</div>
  <div class="text-sm mt-1 font-mono">Fetching leaderboard, holdings, changelog…</div>
</div>
```

Wrap the existing KPI strip, tabs, and tabs content with `x-show="!loading"`.

#### 4b. Memoize the ETF holdings index

In the Alpine component, after `await Promise.all(...)` in `load()`, build an index:

```javascript
this.holdingsByETF = {};
for (const h of this.holdings) {
  (this.holdingsByETF[h.ETF_Ticker] ||= []).push(h);
}
for (const etf of Object.keys(this.holdingsByETF)) {
  this.holdingsByETF[etf].sort((a, b) => a.rank - b.rank);
}
```

Replace the `etfHoldings` getter:

```javascript
get etfHoldings() {
  return this.holdingsByETF[this.activeETF] || [];
}
```

This turns O(N) filter + O(N log N) sort per render into O(1) lookup.

#### 4c. Empty state for ETF panel

Inside the ETF detail `<tbody>`, add right above the `<template x-for...>`:

```html
<tr x-show="etfHoldings.length === 0">
  <td colspan="8" class="text-center py-12 text-xs" style="color: var(--text-3)">
    No holdings available for this ETF in the current data.
  </td>
</tr>
```

#### 4d. Error retry

Replace the error toast block (search for `Error toast` comment) with:

```html
<div x-show="error" x-cloak class="fixed bottom-4 right-4 max-w-md text-sm rounded-lg px-4 py-3 flex items-center gap-3" style="background: rgba(251, 113, 133, 0.12); border: 1px solid rgba(251, 113, 133, 0.30); color: var(--down)">
  <div class="flex-1">
    <div class="font-medium mb-1">Could not load data</div>
    <div class="text-xs opacity-90" x-text="error"></div>
  </div>
  <button @click="error = ''; loading = true; load()" class="px-3 py-1.5 rounded border border-current text-xs hover:opacity-80 transition">Retry</button>
</div>
```

#### 4e. Score sort tiebreaker

In `filtered` getter's sort callback, when `av === bv` for numeric keys, fall back to `score_delta_pct` desc then `etf_count` desc:

```javascript
r = [...r].sort((a, b) => {
  const av = a[k], bv = b[k];
  if (av == null && bv == null) return 0;
  if (av == null) return 1;
  if (bv == null) return -1;
  if (typeof av === 'number' && typeof bv === 'number') {
    if (av !== bv) return (av - bv) * dir;
    // Tiebreaker chain
    const dpA = a.score_delta_pct ?? 0, dpB = b.score_delta_pct ?? 0;
    if (dpA !== dpB) return dpB - dpA;
    return (b.etf_count || 0) - (a.etf_count || 0);
  }
  return String(av).localeCompare(String(bv)) * dir;
});
```

### Definition of Done — Bug 4

1. Initial page load shows a "LOADING" placeholder, then transitions cleanly to the dashboard.
2. Clicking through all 16 ETFs in rapid succession (use Cypress, Playwright, or just spam-click) shows holdings render for every one with no flicker or empty panel.
3. Disconnecting the network during fetch then hitting "Retry" recovers the dashboard.
4. Sorting by "Day Δ%" puts +20% gainers above 0% movers above -20% losers, with secondary sort by score_delta_pct producing a deterministic order.

---

## Overall Definition of Done

- All 19 existing tests in `tests/test_scoring.py` still pass.
- 3 new tests added and pass: `test_googl_dedupe_after_rename`, `test_whitespace_ticker_normalization`, plus one for the build-step deduplication.
- `python -m predator.build` runs clean and prints non-zero counts for `HIGH_CONVICTION`, `SPECULATIVE_BETA`, `NEW`.
- `docs/data/holdings_latest.json` has zero duplicate `(ETF_Ticker, ticker)` rows.
- `docs/data/leaderboard.json` has zero rows whose ticker or company contains "Cash" or "Other".
- Live dashboard renders all 16 ETFs' holdings without an empty panel.
- Commit message: `fix: dedupe GOOGL rows, normalize whitespace tickers, expand sanitizer, polish UI`. Push to `main`, wait for `Build site` workflow to go green, verify on `https://yieldchaser.github.io/etf-data/`.

## What you must NOT do

- Do not change `scraper.py`, `config.json`, `data/`, `.github/workflows/daily_scrape.yml`.
- Do not change the scoring formula (`weight × points × rank_mult × 100 + new_bonus`) — it is calibrated against the Excel Power Query and any change requires user sign-off.
- Do not add a build step (Webpack, Vite, etc.) to the dashboard. The site is single-file Tailwind CDN + Alpine.js by design.
- Do not introduce a Python web framework. The pipeline is `scraper → all_history.csv → build.py → static JSON → Pages`.
