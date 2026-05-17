# Phase 2 — Per-Stock Detail Page, Sparklines, Time Travel & Period Selector

You are working in `yieldchaser/etf-data`. Bug-fix sweep (PROMPT_BUGFIX) is already merged. Phase 1 site is live at `https://yieldchaser.github.io/etf-data/`. The scoring algorithm is calibrated to the Excel Power Query — do not touch it.

## Goals (all four)

1. Per-stock detail page at `docs/stock.html?ticker=GEV` with hero header, 60-day score sparkline, and per-ETF rank/weight history.
2. Make ticker cells in `docs/index.html` clickable → opens `stock.html?ticker=XXX`.
3. Custom period selector for rank/score deltas: weekly, bi-weekly, monthly, and user-defined date range. Currently only fixed 7-day delta exists.
4. New `docs/data/holdings_history.parquet` produced by `predator/build.py` containing per-(ETF, ticker, snapshot_date) rows.

## Repo recap (exact paths)

```
predator/scoring.py          — Config, Sanitizer, compute_leaderboard, compute_rank_deltas
predator/history.py          — snapshot_dates, historical_leaderboards, score_panel,
                               flag_panel, streaks_and_deltas, changelog
predator/build.py            — orchestrator → docs/data/*.json|*.parquet
config.yaml                  — scoring + sanitizer + history config (the only knob file)
docs/index.html              — Alpine.js + Tailwind, single-file
docs/data/                   — generated artifacts
tests/test_scoring.py        — pytest, 19+ passing
```

Existing outputs you can consume from in `docs/data/`:

- `leaderboard.json` — array of `{ticker, company, final_score, etf_count, total_weight, held_by, tiers, any_new, best_rank, flag, leaderboard_rank, score_delta, score_delta_pct, score_streak, hc_streak, score_percentile, days_observed}`
- `holdings_latest.json` — per (ETF_Ticker, ticker): `{rank, weight, tier, is_new, rank_mult, base_score, new_bonus, score, Holdings_As_Of, rank_delta, weight_flow}`
- `score_history.json` — `{ticker: [{d, s}]}` for the top 200 names, ~60 daily snapshots
- `changelog.json` — `{today, yesterday, entered_hc, exited_hc, biggest_gainers, biggest_losers, new_entrants}`
- `metadata.json` — config snapshot, ETFs list, snapshot_dates_in_window

## Step 1 — Add `holdings_history.parquet` output

In `predator/build.py`, after the `historical = hist.historical_leaderboards(raw, cfg)` line, build a per-(ETF, ticker, date) frame:

```python
# Build holdings_history: ranked per ETF for every snapshot date in the lookback window
sanitized_raw = cfg.sanitizer.apply(raw)
sanitized_raw["Holdings_As_Of"] = pd.to_datetime(sanitized_raw["Holdings_As_Of"], errors="coerce")
window_start = sanitized_raw["Holdings_As_Of"].max() - pd.Timedelta(days=cfg.history.leaderboard_lookback_days)
hist_window = sanitized_raw[sanitized_raw["Holdings_As_Of"] >= window_start].copy()

# Rank within each (ETF, date)
hist_window = hist_window.sort_values(["ETF_Ticker", "Holdings_As_Of", "weight", "ticker"],
                                     ascending=[True, True, False, True])
hist_window["rank"] = hist_window.groupby(["ETF_Ticker", "Holdings_As_Of"]).cumcount() + 1

# Compact for parquet (drop name to save space — we have it in holdings_latest)
out = hist_window[["ETF_Ticker", "ticker", "Holdings_As_Of", "rank", "weight"]].copy()
out["Holdings_As_Of"] = out["Holdings_As_Of"].dt.strftime("%Y-%m-%d")
out.to_parquet(output_dir / "holdings_history.parquet", index=False)
```

Also produce a JSON view limited to the top 300 leaderboard tickers (so `stock.html` can load without DuckDB-WASM for most queries):

```python
# JSON view: { "<ticker>": { "<etf>": [{d, r, w}, ...] } } for top 300 names
top_tickers_for_history = set(leaderboard.head(300)["ticker"].tolist())
hist_filtered = hist_window[hist_window["ticker"].isin(top_tickers_for_history)]
holdings_history_json = {}
for (t, etf), g in hist_filtered.groupby(["ticker", "ETF_Ticker"]):
    holdings_history_json.setdefault(t, {})[etf] = [
        {"d": d.strftime("%Y-%m-%d"), "r": int(r), "w": round(float(w), 6)}
        for d, r, w in zip(g["Holdings_As_Of"], g["rank"], g["weight"])
    ]
(output_dir / "holdings_history.json").write_text(json.dumps(holdings_history_json, separators=(",", ":")))
```

## Step 2 — Custom period selector for deltas

In `config.yaml`, replace single `rank_delta_lookback_days: 7` under `history:` with a list:

```yaml
history:
  delta_periods_days: [1, 7, 14, 30]      # daily, weekly, bi-weekly, monthly
  leaderboard_lookback_days: 60
  changelog_top_n: 15
```

In `predator/scoring.py`:
- `HistoryConfig` dataclass: replace `rank_delta_lookback_days: int` with `delta_periods_days: tuple[int, ...]`.
- `compute_rank_deltas(history, cfg, lookback_days=None)` already accepts `lookback_days`. Keep it, default to `cfg.history.delta_periods_days[1]` (the 7-day) for backward compat.

In `predator/build.py`, compute deltas for every period and emit:

```python
deltas_by_period = {}
for n_days in cfg.history.delta_periods_days:
    d = compute_rank_deltas(raw, cfg, lookback_days=n_days)
    deltas_by_period[n_days] = d
    # Optional: write to parquet for power users
    # d.to_parquet(output_dir / f"rank_deltas_{n_days}d.parquet", index=False)

# In holdings_latest_json output, add columns for each period:
for n_days, d in deltas_by_period.items():
    latest_out = latest_out.merge(
        d[["ETF_Ticker", "ticker", "rank_delta", "weight_flow"]].rename(columns={
            "rank_delta": f"rank_delta_{n_days}d",
            "weight_flow": f"weight_flow_{n_days}d",
        }),
        on=["ETF_Ticker", "ticker"], how="left"
    )
```

In `docs/index.html`:

a) Add a period selector chip-group to the leaderboard filter bar (right before the search input):

```html
<div class="flex gap-1 items-center">
  <span class="label mr-1">period</span>
  <template x-for="p in [1, 7, 14, 30]" :key="p">
    <button @click="period = p"
            :class="period === p ? 'border-cyan-500/40 text-cyan-300' : ''"
            class="chip border px-2 py-1 transition hover:border-cyan-500/30"
            style="border-color: var(--border-2)"
            x-text="`${p}d`"></button>
  </template>
  <button @click="openCustomRange = !openCustomRange"
          :class="openCustomRange ? 'border-cyan-500/40 text-cyan-300' : ''"
          class="chip border px-2 py-1 transition" style="border-color: var(--border-2)">
    custom
  </button>
</div>
```

b) Custom range popover (date inputs for start/end):

```html
<div x-show="openCustomRange" x-transition class="absolute z-30 mt-1 rounded-lg p-3 border" style="background: var(--surface-2); border-color: var(--border-2)">
  <div class="grid grid-cols-2 gap-2 text-xs">
    <label class="label">From <input type="date" x-model="customFrom" class="block mt-1 bg-transparent border rounded px-2 py-1" style="border-color: var(--border-2)"></label>
    <label class="label">To   <input type="date" x-model="customTo"   class="block mt-1 bg-transparent border rounded px-2 py-1" style="border-color: var(--border-2)"></label>
  </div>
  <button @click="applyCustomRange()" class="mt-2 chip border px-2 py-1 hover:border-cyan-500/30" style="border-color: var(--border-2)">apply</button>
</div>
```

c) Component state:

```javascript
period: 7,              // active period in days
openCustomRange: false,
customFrom: '', customTo: '',
applyCustomRange() {
  if (!this.customFrom || !this.customTo) return;
  this.period = `${this.customFrom}_${this.customTo}`;
  this.openCustomRange = false;
}
```

d) Wherever `rank_delta` / `weight_flow` are read in the row template, dispatch on `period`:

```javascript
deltaFor(h, kind) {
  // kind = 'rank' | 'weight'
  const p = this.period;
  if (typeof p === 'number') {
    return h[`${kind}_delta_${p}d`] ?? h[`${kind === 'rank' ? 'rank_delta' : 'weight_flow'}_${p}d`];
  }
  // Custom range: need client-side computation against holdings_history.json
  return this.computeCustomDelta(h, this.customFrom, this.customTo, kind);
}
```

For the custom range branch, `computeCustomDelta` reads `holdings_history.json` (top 300 tickers) and looks up the (ETF, ticker)'s rank/weight at the closest snapshots ≤ customFrom and customTo, then returns `(now − then)` or `(now / then − 1)`. If the ticker isn't in the top 300, fall back to "—".

## Step 3 — Per-stock detail page

Create `docs/stock.html` as a single file matching the index.html aesthetic exactly. Reuse the same `<style>` block (copy/paste — no shared CSS files; we keep the no-build-step constraint).

### Layout

```
┌─────────────────────────────────────────────────────────────┐
│ PREDATOR PROTOCOL  ›  GEV                          ← back │   ← header
├─────────────────────────────────────────────────────────────┤
│ GEV                                                          │
│ GE Vernova Inc.                                              │
│                                                              │
│  ┌──────────┬──────────┬──────────┬──────────┬──────────┐  │   ← KPI strip
│  │ score    │ day Δ    │ HC streak│ ETFs     │ pct       │  │
│  │  1353    │ -1.2%    │  55 days │   7      │  82%      │  │
│  └──────────┴──────────┴──────────┴──────────┴──────────┘  │
│                                                              │
│  60-DAY SCORE HISTORY                                        │   ← sparkline (full-width SVG)
│  [inline SVG path, hover shows date + score tooltip]        │
│                                                              │
│  PER-ETF HISTORY                                             │   ← per-ETF table
│  ETF   Tier    Rank   7d Δ   30d Δ   Weight   Flow   Mini  │
│  CSD   Scout   #2    ▲3     ▲1      8.4%    +12%   ▁▂▃▅█│
│  FPX   Scout   #1    —      ▼2      9.1%    +8%    ▂▃▅█▇│
│  ...                                                         │
│                                                              │
│  HELD BY                                                     │
│  [Scout]  CSD · FPX                                          │
│  [Quant]  QMOM                                               │
│  ...                                                         │
└─────────────────────────────────────────────────────────────┘
```

### Implementation requirements

a) **URL parsing**: read `?ticker=XXX` on load. If missing or unknown, show a search box and exit.

b) **Sparkline**: build an inline SVG path from `score_history.json[ticker]`. Width 100% of container, height 120px. Plot points as small dots. On hover, show a tooltip with date and exact score. Use the colors `--up` (green) for last value if > first value, `--down` (red) otherwise. Do not use a charting library — pure SVG path math.

```javascript
function buildSparkPath(points, width, height) {
  if (!points || points.length < 2) return '';
  const ys = points.map(p => p.s);
  const min = Math.min(...ys), max = Math.max(...ys);
  const range = max - min || 1;
  const stepX = width / (points.length - 1);
  return points.map((p, i) => {
    const x = i * stepX;
    const y = height - ((p.s - min) / range) * height;
    return `${i === 0 ? 'M' : 'L'}${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(' ');
}
```

c) **Per-ETF history table**: from `holdings_history.json[ticker]`, for each ETF present, compute current rank, 7d/30d rank deltas, current weight, weight flow, and build a tiny inline SVG sparkline of the rank time series (inverted Y: lower rank = higher = better).

d) **Mini sparklines per ETF row**: 80px × 18px inline SVG, same path logic. Color by trend: up over the window = `--up`, down = `--down`.

e) **Quick switcher**: small autocomplete input in the header that lets the user type a ticker and jump. Populate suggestions from `leaderboard.json` (already cached).

f) **Back link**: top-left, navigates to `./` with the leaderboard tab restored.

### Wire up `docs/index.html` ticker clicks

Find the ticker cell in the leaderboard table (around line 380 in the current file):

```html
<td class="px-3 py-2 font-medium font-mono" x-text="row.ticker"></td>
```

Replace with:

```html
<td class="px-3 py-2 font-medium font-mono">
  <a :href="`stock.html?ticker=${encodeURIComponent(row.ticker)}`"
     @click.stop
     class="hover:underline" style="color: var(--text); text-decoration-color: var(--cyan)" x-text="row.ticker"></a>
</td>
```

The `@click.stop` prevents the row-expand toggle from firing when the user clicks the ticker link.

Do the same for the ETF tab's ticker column (around line 380 again, ETF tab section).

## Definition of Done — Phase 2

1. `python -m pytest tests/ -v` is all green with at least 2 new tests:
   - `test_delta_periods_config` — verifies `cfg.history.delta_periods_days` is a tuple of ints from YAML.
   - `test_holdings_history_parquet_shape` — runs build on synthetic 30-day data and asserts the parquet has one row per (ETF, ticker, date).
2. `python -m predator.build` produces `docs/data/holdings_history.parquet` and `docs/data/holdings_history.json`.
3. Navigating to `https://yieldchaser.github.io/etf-data/stock.html?ticker=GEV` renders the hero, sparkline, and at least 4 ETF rows (CSD, FPX, QMOM, SPMO at minimum).
4. Period chips (1d/7d/14d/30d) on the leaderboard change all Δ% and Δrank columns. Custom range with valid dates produces deltas from `holdings_history.json` lookups; out-of-range tickers show "—".
5. Clicking a ticker on the leaderboard navigates to the detail page; clicking the rest of the row still expands the inline drill-down.
6. Lighthouse performance score on the detail page ≥ 90 on desktop.

## What you must NOT do

- Do not add a JS bundler. Continue using CDN Tailwind + Alpine.
- Do not regress the existing 19+ test count or any current rendering.
- Do not change the scoring formula or sanitizer behavior — Phase 2 is purely additive UI + temporal queries.
