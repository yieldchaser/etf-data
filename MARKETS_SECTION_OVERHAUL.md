# Predator Protocol — World-Class Overhaul Spec & Build Prompt
**Project:** Predator Protocol · `yieldchaser/etf-data`
**Scope:** **Part I — Markets Intelligence Platform** (`docs/markets.html`, 9 tabs + data layer + Asset Detail). **Part II — Portfolio Lab & Simulator** (`docs/sim.html` → expanded, Portfolio-Visualizer + IBKR-PortfolioAnalyst-grade analytics + Kelly sizing).
**Bar:** Bloomberg-module / elite-quant grade. No "good enough." Every pixel and every number must be defensible and tie out.

> **Two philosophies, on purpose.** Part I (Markets) is **simplicity-first** — a clean, instantly-legible yearly-returns product for *everyone*. Part II (Portfolio Lab) is the **power-user playground** — deep, complex analytics for someone running real money. Don't let Part II's complexity bleed into Part I's clean core, and don't dumb Part II down. Both reuse the same foundations (§2): data contract, unified state, asset-identity colors, the §2.5 time-range control, the §2.4 currency/real engine, and the Drawdown engine.

---

## 0. How to use this prompt

Hand this to the builder (Claude Code / yourself) as the single source of truth for the Markets section. Work **strictly in the sequence in §8**. Do not start cosmetic tab work until the two foundation fixes (data pipeline + return-matrix render) are green, because every other tab inherits from them.

Four rules that override convenience at every step:

1. **Simplicity-first — yearly returns are the hero.** The product's spine is one dead-simple question answered beautifully: *which asset class is doing well or badly, year by year.* The Return Matrix (annual returns, color = good/bad) is the **default landing view** and must be instantly legible to someone who never reads a manual. Every advanced module (term structure, variance risk premium, ulcer index, rolling correlation) is **progressive disclosure** — one click deeper, never cluttering the default. We are not competing on feature count; we win on a clean core that is impossible to misread. When in doubt, cut.
2. **Every displayed number must tie out to a recomputable formula from the raw series.** If a return, CAGR, z-score, or drawdown can't be reproduced from the JSON by hand, it's wrong.
3. **One shared state object drives all tabs** — currency lens, real/nominal, time window, selected assets, asset-identity colors. Picking INR once changes *everything*. This is the "symmetry" the platform is missing today.
4. **Fail loud in the pipeline, fail graceful in the UI.** The build must error when a series is stale; the UI must render an honest "no data" state instead of a silent blank.

---

## 1. Root-cause diagnosis of the three reported failures

### 1.1 Return Matrix renders empty — **client-side render bug, NOT a data bug**
Proof: the category group rows draw, and the Asset Statistics cards below the matrix populate from the *same* `market_returns.json`. Data is loaded; only the grid body fails to emit asset rows + cells.

Fix in this priority order; instrument each before moving on:
- **(a) Collapsed-group toggle never expands.** If category rows are accordion headers, default them to **expanded** and verify the toggle handler mutates the right reactive key.
- **(b) Year-key type mismatch.** Cell lookup is almost certainly `returns[year]` where `year` is an int from the header loop but the data is keyed by string (or vice-versa). Normalize both sides to string at the data-contract boundary (§2.3) and the cells will populate.
- **(c) Category-label mismatch.** The per-category asset array is empty because the group label in code (`"Precious Metals"`) doesn't match the data (`"precious_metals"`). Drive grouping off a single enum, never off display strings.

**Acceptance:** matrix shows ≥ 44 asset rows under their category groups; every cell either shows a return or an explicit `·` for a missing year; no console errors.

### 1.2 "Still showing old data" — **pipeline never sees your new history**
The build ingests **`Mega_Markets_Historical.xlsx`** via `ingest_mega_xl.py` (5 fixed sheets). The history you just collected lives in **`Markets_1_.xlsx`** — different sheet names (`S&P 500`, `Gold`, `Nikkei 225`, `USD_INR`, `Fred`, `1-Month Implied Volatility`, `1-Day Implied Volatility`…) and a different layout (a daily `Date/Close` block, a separate "Historic Data" monthly block, and a `Year / % Change` annual block per sheet). The build literally cannot read it.

Secondary cause: FX, agriculture, base metals beyond a few, and rates are pulled live from FRED/yfinance under `continue-on-error: true`. A silent rate-limit or outage leaves those series frozen with **no failure signal** — the build still "succeeds" and stamps today's date, so the header says `RETURNS DATA: 2026-05-24` while the underlying series are weeks old.

Fix: rebuild the ingest around your canonical Excel (see §3) and add a freshness gate that **fails the build** if any series' last point is older than its expected cadence.

### 1.3 Volatility "just says random things" — **presentation problem, data is already there**
I-3 already holds full history (VIX/GVZ/OVX since 1990–2008), I-1 already shows all seven indices with z-scores. The single-number card (`VIX 17.6`) feels useless because it's *presented* as a number instead of a *regime*. And you have something most platforms don't: **1-Day AND 1-Month implied vol levels for all 7 indices** (`Markets_1_.xlsx`, 1,533 daily rows) — i.e. a vol **term structure**. The fix is a redesign that leads with percentile + regime + term structure, not new data (see §5.9).

---

## 2. Architectural foundations (cross-cutting — build first, reuse everywhere)

### 2.1 Unified app state
A single reactive store (`marketState`) consumed by every tab:
```
marketState = {
  lens:      'local' | 'usd' | 'inr' | 'gold' | 'silver',   // currency/denominator
  basis:     'nominal' | 'real',                            // CPI-adjusted or not
  window:    { start: ISODate, end: ISODate },              // EXACT visible range — single source of truth (§2.5)
  preset:    '1m'|'6m'|'1y'|'2y'|'5y'|'10y'|'max'|'custom', // shortcut that derived `window`; 'custom' when brushed
  selected:  [assetId, ...],                                // shared multi-select
  granularity:'monthly' | 'daily',
  asof:      ISODate                                         // global data date
}
```
Changing `lens` to `inr` must instantly re-denominate the Return Matrix, Asset Detail, Periodic Table, Correlation growth chart, Seasonality, and Drawdowns — all from one toggle.

### 2.2 Asset-identity color system
One canonical color per asset, defined once, used in **every** chart (matrix heat is the only exception, which uses a diverging return scale). Gold is always the same amber everywhere; S&P always the same blue. Build a `COLORS[assetId]` map and forbid ad-hoc colors. This single change is what makes the platform feel "symmetric."

### 2.3 The data contract (normalize at the boundary)
Define the JSON shape once and validate it on load. Every consumer reads this, nothing else:
```jsonc
{
  "asof": "2026-05-24",
  "assets": {
    "GOLD": {
      "name": "Gold",
      "category": "precious_metals",   // enum, lowercase_snake — single source of grouping
      "native_ccy": "USD",
      "source": "markets_1.xlsx:Gold", // provenance, shown in UI
      "first": "1968-01-02",
      "last":  "2026-05-22",
      "monthly": [ ["1968-01", 35.5], ... ],   // [YYYY-MM, native-ccy EOP price]
      "daily":   [ ["2026-05-22", 4523.2], ... ] // optional, for detail/price-log
    }
  },
  "fx": { "USDINR": [["1973-01", 8.02], ...], "USDJPY": [...], "EURUSD": [...] },
  "cpi": { "US": [["1968-01", 34.1], ...], "IN": [...] },   // for real returns
  "events": [ {"year":2008,"label":"GFC"}, {"year":2020,"label":"COVID"}, ... ]
}
```
- **Years are strings everywhere.** Kills bug §1.1(b) permanently.
- **Returns are computed from prices, never stored** (single source of truth → no drift). Store the precomputed Gold-INR columns from your Excel only as a *test oracle* to validate the client math, not as the display value.

### 2.4 The currency/real engine (the core of your Asset Detail ask)
To render asset `A` in lens `C`, real or nominal, do it **on the price series**, not by chaining `(1+r)(1+fx)`:

```
price_native(t)                          // as stored
price_usd(t)    = price_native(t) * fx(native→USD, t)
price_C(t)      = price_usd(t)    * fx(USD→C, t)        // C ∈ {INR, ...}
price_gold(t)   = price_usd(t)    / gold_usd(t)         // "priced in ounces of gold"
price_real(t)   = price_C(t) / cpi_C(t) * cpi_C(base)   // real, in lens currency
annual_return(y)= price(Dec y) / price(Dec y-1) - 1
CAGR            = (price_last / price_first)^(12/months) - 1
```
Why price-based, not return-chaining: identical answer when done right, but immune to compounding/rounding drift and handles missing months cleanly. **Acceptance test:** Gold 2025 return in INR computed this way must equal your Excel's precomputed `Change % (INR)` for 2025 within 0.1pp.

Edge rules to decide (see §9): what is "Local" for Sensex/Nikkei/DAX (their native INR/JPY/EUR); what to do for INR before 1973 (clamp vs n/a); and whether real-INR uses Indian CPI or US CPI.

### 2.5 ⭐ Time-Range Control — preset bar + draggable brush, perfectly synced
**One reusable component (`<TimeRangeControl>`) wraps every time-series chart** (Asset Detail, Price History, Yield, Drawdowns, Volatility, Correlation growth). It has three elements that are **bound to a single `window = {start, end}` value** — never three separate states:

1. **Preset bar** (top): `1M · 6M · 1Y · 2Y · 5Y · 10Y · Max`. For deep-history assets (Gold 1833+, S&P 1871+) Max means full series. Clicking a preset sets `window = [last − N, last]` and the brush handles snap to match exactly.
2. **Brush / range slider** (bottom): a thin **overview mini-chart of the full series**, with two draggable handles + a draggable middle band. Dragging the handles resizes the window; dragging the band pans it. The main chart zooms to exactly the brushed window as you drag.
3. **Main chart**: x-axis domain `=== window`, always. No off-by-one, no independent zoom.

**Synchronization contract (this is the "no error anywhere" you asked for — treat as law):**
- **Single source of truth.** Presets and brush both *write* `window`; the chart only *reads* it. Three views, one value.
- **Bidirectional, lossless.** Click `1Y` → brush handles land exactly on `[last−1y, last]`. Drag the brush → the active preset clears to `custom` (no preset stays falsely highlighted). If a drag happens to land exactly on a preset span, re-highlight that preset.
- **Snap to data points.** Handles snap to real months (or days in daily mode), so a window edge never falls between points and renders a half/empty bar.
- **No inversion, min width.** Enforce `start < end` and a minimum window (≥ 1 month monthly / ≥ 5 days daily); clamp both edges to `[series.first, series.last]`.
- **Short-series tolerance.** `1M`/`6M` on a monthly-only or young series shows what exists and disables presets longer than the available history — it must **never** error or blank.
- **Smooth + exact.** Re-render on `requestAnimationFrame` during drag (no jank), but commit the *exact* snapped value on release. Debounce any recompute, never the visual.
- **Year mode for grids.** The Return Matrix and Periodic Table use the same mental model but in **years**: the preset bar becomes year-spans and the brush is a horizontal year-window over the columns. Same `window`, same rules.
- **Scope:** the control reads/writes `marketState.window` so a range chosen on one chart can carry across tabs; a chart may also keep a local brush for fine detail without breaking the global value.

**Acceptance:** drag the brush from 200y → 5y and the main chart zooms live with zero lag/desync; click `2Y` and the brush handles snap precisely to a 2-year span; brushing clears the preset to `custom`; window edges always sit on real data points; `1M` on a 3-month-old series shows 3 months, no crash.

### 2.6 Formatting, density, performance, a11y (non-negotiable polish)
- **Number format:** returns to 1dp with sign and color; prices with locale grouping; CAGR/vol to 1dp; never mix `+0.0%` and `0.00%` in the same view.
- **Tooltips are first-class.** Every chart has a single crosshair tooltip showing date + exact value(s) in the active lens. No naked dots.
- **Performance:** 150+ years × 44 assets × monthly = render with canvas or virtualized SVG; precompute per-lens series once on lens change, not per-frame. Target < 16ms interaction frames, < 1.5s first paint.
- **Accessibility & legibility:** WCAG-AA contrast on the dark theme, keyboard-navigable tabs, focus rings, `prefers-reduced-motion` respected, color-blind-safe diverging scale (the current orange↔blue is fine; verify).
- **Empty/partial states:** explicit "no data for this range/lens," partial years at 40% opacity with a clear "partial" tag.

---

## 3. Data layer & pipeline (the staleness fix — gates everything)

### 3.1 Decide the canonical source (DESIGN DECISION — §9.1)
**Recommended:** make `Markets_1_.xlsx` (your live working file) the canonical input and write a new `ingest_markets_xl.py` that understands its real layout, replacing/superseding `ingest_mega_xl.py`. Rationale: you maintain that file anyway; forcing you to re-export into the old `Mega_…` shape is exactly the friction that produced the staleness.

### 3.2 New ingest: `predator/ingest_markets_xl.py`
Parse each price sheet's three blocks robustly (the layout has a daily `Date/Close` block in cols A–C, a "Historic/Historical Data" monthly block, and a `Year/% Change` annual block — column positions vary per sheet, so **locate blocks by header text, not by fixed index**). Emit the §2.3 contract. Specifically:
- Map sheet → assetId + category + native_ccy via an explicit registry (no guessing).
- Build `monthly` from EOP of the daily block (preferred) or the monthly block where daily is absent.
- Pull `USD_INR` sheet → `fx.USDINR`; `Fred` sheet + the two implied-vol sheets → vol pipeline (§3.4).
- Treat your precomputed `% Change` / `Change % (INR)` columns as **validation oracles** only.

### 3.3 Freshness gate (fail loud)
Add a build step that asserts each series' `last` date is within its expected cadence (daily series ≤ 4 calendar days old on a weekday; monthly ≤ 35 days). On violation: **fail the build** and name the stale series. Remove blanket `continue-on-error: true` from the data-fetch steps, or downgrade it to "warn + mark series stale in metadata" so the UI can badge it — never silent.

### 3.4 Volatility pipeline upgrade
You have, per index (VIX, GVZ/Gold, OVX/Crude, VXN/NQ, RVX/RUT, VXD/DJIA, VXEEM/EM):
- Daily **and** monthly **levels** (the `.1` columns) → history + term structure.
- Precomputed z-scores → validate the client's own z-score math.
Emit `vol_history.json` with: per-index daily level series, monthly-avg series, full-history mean/std for z, and a **term-structure pair** (1-Day vs 1-Month) so §5.9 can chart contango/backwardation.

### 3.5 Provenance & freshness in the UI
Replace the bare `RETURNS DATA: 2026-05-24` with a small **provenance popover**: per-series `source`, `first→last`, and a green/amber/red freshness dot. This is how you'll catch the next staleness incident in two seconds instead of two weeks.

---

## 4. ⭐ FLAGSHIP: Asset Detail deep-dive (`?asset=GOLD`)

This is the feature you described: click Gold → a fully symmetric, year-wise, multi-currency deep dive. It is the centerpiece; spend the most polish here. Reachable by clicking any asset name in the Return Matrix, Periodic Table, Price Log, Seasonality, Correlation, or Drawdowns.

**Layout (single scrollable page, sticky header):**

1. **Header bar** — Asset name · native denomination · current price + today's % (in active lens) · data range · source dot. The global lens/basis/range toggles live here and are the same controls as every other tab (synced state).

2. **Year-by-year returns chart** (the thing you asked for) — vertical bars, green/red, one per year, CAGR dashed reference line, partial first/last years at 40% opacity. Hover → exact year + return to 2dp in the active lens. Switching lens to INR **re-renders these bars as INR returns** instantly.

3. **Year × currency table** — rows = years; columns = **Local · USD · INR · Gold-denom · Real**, all shown at once so you can read gold's 2025 as `+63% USD / +X% INR / +Y% real` in one glance. Sortable, exportable to CSV. (This is strictly better than only showing the active lens — it answers "vs USD vs INR" without toggling.)

4. **Stats strip** — CAGR, annualized vol, best year, worst year, % positive years, max drawdown, current drawdown, return/vol ratio, data since. All recomputed in the active lens.

5. **Monthly heatmap** — year (rows) × month (cols), colored by monthly return. This is the asset's *entire* history at a glance and doubles as per-asset seasonality.

6. **Price history chart** — log/linear toggle; optional overlays: drawdown shading, the asset's own vol index if one exists (Gold→GVZ, Crude→OVX, equity→VIX), and crisis-year shading from `events`.

7. **Rolling N-year return** (Hold-Lab inline) — best/worst/median for the selected holding period, with the rolling line.

8. **"Compare with"** — add 1–2 other assets to overlay on the returns bars and growth-of-$100, in the same lens.

**Acceptance:** clicking Gold opens this page; lens=INR changes every number on it; the Gold-2025-INR cell ties out to the Excel oracle within 0.1pp; deep-link `markets.html?asset=GOLD&lens=inr` restores the exact view.

---

## 5. Tab-by-tab overhaul

### 5.1 Return Matrix
- **Fix the empty render first** (§1.1).
- **Sticky** first column (asset) **and** sticky year header — both must pin on scroll.
- **Summary columns** appended to the right: CAGR · Vol · Best · Worst · %+ · a 12-pt sparkline. These let the matrix double as a screener.
- **Sortable** by any year column or any summary column (click header).
- **Click asset name → Asset Detail** (§4). **Click cell → monthly drilldown** (keep, polish the 12-cell row).
- Keep nominal/real, return/z-score, lens, CSV — but they now read/write `marketState` so they persist across tabs.
- **Event markers** (⚡) get hover labels (`2008 — GFC`) and clicking one scrolls the matrix to that year column.

### 5.2 Price Log
- **Remove the green/red micro-bars / streak sparklines on the cards** (your explicit ask) — replace with a single clean number block: price, day %, and a thin neutral 30-day line (no candle-ish bars).
- Keep the detail drawer (Close, Day%, 7D, 1M, YTD, ATH, ATL, %-ile, streak, 20D-up) — that table is genuinely useful; just de-noise the cards above it.
- Make the category chips (ALL/COMMODITIES/CRYPTO/FX/INDICES) and search drive a clean responsive grid; cards link to Asset Detail.

### 5.3 Yield History
- Already strong. Add: **recession shading**, a **2s10s inversion highlight** (shade periods where 10Y–2Y < 0 — the single most-watched recession signal), and a **real-yield series** (10Y − CPI YoY). Keep the annual heatmap; make it share the matrix's sticky-header treatment.

### 5.4 Periodic Table (Callan) — **fix the overflow**
The staircase bug = it renders **every** year at fixed cell width, overflowing the viewport (screenshot 9). Fixes:
- **Cap default to last ~20–25 years**, with a range control to extend.
- **Responsive cells** that shrink to fit container width; cap the table to viewport height with internal scroll, not page-blowout.
- **Sticky year header**, **asset legend** docked beside it.
- **Hover an asset → highlight its rank path** across all years (the whole point of a Callan chart). **Click a year → that year's full ranked column** enlarged.
- Category filter (Equity/Metals/Energy/…) already exists — wire it to `marketState.selected`.

### 5.5 Drawdowns (Drawdown Studio)
Already good (underwater curves, max-DD table). Push to institutional:
- **Duration, not just depth:** add "time underwater" and a **recovery-time distribution**. Depth tells you the pain; duration tells you the patience required.
- **Current-DD percentile:** "today's drawdown is deeper than 78% of this asset's history."
- **Ulcer Index / pain index** per asset (captures depth × duration in one number) in the max-DD table.
- **Click a drawdown event → zoom** to that window with peak/trough/recovery markers and crisis labels.
- **Regime overlay:** optionally shade the underwater chart by VIX regime or rate regime, so you can see *why* the drawdown happened.
- Conditional stat: average forward 1Y return *given* you bought at −20% / −30% / −40% drawdown (the "should I buy the dip" table).

### 5.6 Hold Lab
Strong. Add: **percentile bands** (10/25/50/75/90) on the rolling-return chart, not just best/worst/median; a **"never lost money over N years"** callout; and **comparison across assets** for the same holding period (S&P vs Gold vs Sensex rolling-10Y side by side).

### 5.7 Seasonality
Strong. Add: **hit-rate already on hover → also encode it** (e.g., a small dot/opacity) so you see *consistency* not just average; **sample size** (n years) per cell; a **"current month" highlight** (we're in May → frame May's history); and per-asset **monthly box-plots** in Asset Detail. Flag low-n cells (< 10 years) as statistically thin.

### 5.8 Correlation — the real upgrade is **rolling correlation**
Static correlation is table-stakes. The alpha is that **correlation is not constant**:
- Add a **rolling correlation chart** (e.g., 1Y rolling corr of Gold vs S&P over time) — this is how you *see* regime shifts (gold-equity correlation flipping positive in 2022 was the whole macro story).
- Let the user pick a **base asset**; show every other asset's rolling corr to it.
- Keep the static matrix and Growth-of-$100 (good), but denominate growth in `marketState.lens`.
- Add **correlation-to-rates** and **correlation-to-VIX** rows — cross-asset macro linkages are exactly the "non-obvious interconnections" worth surfacing.

### 5.9 Volatility Intelligence — redesign around regime + term structure
Rename the cryptic I-1…I-4. New default view leads with **meaning, not a number**:
- **Regime banner (auto-generated):** "Crude vol (OVX) is in the 97th percentile of its history at +2.07σ — the market is pricing significant energy stress. Equity vol (VIX) is benign (38th percentile)." Generated from the data, updated daily.
- **Per-index card:** current level · **percentile rank** (more intuitive than z) · regime word (Calm/Normal/Elevated/Stress/Crisis from percentile buckets) · 1Y sparkline · days-since-last-spike.
- **History chart** (your I-3) becomes primary, not buried — with crisis annotations and a percentile band shaded behind the line.
- **★ Term structure (you have the data):** plot **1-Day vs 1-Month implied vol** per index. 1D > 1M = backwardation = acute stress *now*; 1M > 1D = contango = calm. Almost no public dashboard shows this — it's a genuine differentiator and a real trading signal.
- **Variance risk premium (if realized vol is computable):** implied (VIX) minus 30-day realized S&P vol — the single best "is fear over- or under-priced" gauge.
- **Cross-asset vol matrix:** which markets are stressed *relative to their own history* (percentile, not raw level — raw OVX 73 vs VIX 17 isn't comparable; percentile is).

---

## 6. New data & features to add (grounded in your files + macro reality)

- **FX lens expansion:** you have USD/INR from 1973; surface JPY/EUR/CNY (already in the registry) so Nikkei-in-USD, DAX-in-USD, "everything in your home currency" all work. The "what did this *actually* return to an Indian investor" view is your differentiator given the BSE/USD-INR focus.
- **Gold-denominated everything:** "the S&P costs X ounces of gold" — a favorite of macro/structural thinkers; you already have the gold + FX series to compute it.
- **Real (inflation-adjusted) everywhere**, not just the matrix — extend CPI deflation into Asset Detail, Hold Lab, Periodic Table.
- **Cross-asset ratio charts:** Gold/Silver ratio, Copper/Gold (growth vs fear), Stocks/Bonds, Oil/Gold — these *ratios* are the structural-regime signals elite macro accounts live by. One small "Ratios" sub-tab.
- **Yield-curve regime tape:** a compact strip showing curve shape (steep/flat/inverted) over time, tied to the recession overlay in §5.3.
- **Seasonality significance & "this month" module** (§5.7).
- **Event study:** pick an event (rate shock, COVID) → table of each asset's drawdown, recovery time, and 1Y-forward return from that event. Turns history into a decision tool.
- **From `Corporate_CAGR.xlsx`:** the `% Returns`, `Return Weightage`, and `Trades` sheets look like a personal performance ledger — candidate for a separate "Portfolio vs Benchmarks" view, *not* the public markets tab. Confirm before wiring (§9.5).

---

## 7. QA, edge cases, acceptance criteria

- **Tie-out tests** (block release): Gold-INR-2025 == Excel oracle ±0.1pp; S&P CAGR matches a hand calc from first/last monthly; every drawdown's peak/trough dates reproduce from the price series.
- **Lens invariance:** switching lens and switching back returns identical numbers (no drift).
- **Partial years** never silently averaged into CAGR; explicitly excluded or flagged.
- **Missing data:** an asset with no data in the selected range shows an explicit empty state, never a blank cell that looks like 0%.
- **Pre-FX history:** INR/JPY lens before the FX series starts → explicit "n/a (no FX history)," not a wrong number.
- **Performance budget:** matrix + periodic table interactive < 16ms/frame at full history; first paint < 1.5s.
- **Cross-tab state:** set lens=INR on Matrix → navigate to Drawdowns → still INR. Set window=30Y → persists. Deep links restore full state.
- **Range-control sync:** brush and preset bar and chart never disagree; dragging the brush clears the preset to `custom`; clicking a preset snaps handles to the exact span; window edges always land on real data points; live zoom during a 200y→5y drag stays smooth with zero desync.
- **Freshness:** force a stale series in a test → build fails (or UI shows red dot), never silent.

---

## 8. Sequencing (ship in this order — do not reorder)

1. **Data pipeline** — new `ingest_markets_xl.py` from `Markets_1_.xlsx` + freshness gate + provenance metadata. (Fixes staleness; unblocks everything.)
2. **Data contract + unified state + color system + Time-Range Control** (§2, incl. §2.5 brush+presets). (Foundation — every chart consumes the range control, so build it once here.)
3. **Return Matrix render fix** (§1.1) + sticky + summary columns + click-through. (Your "first thing not working." This is the default landing / yearly-returns hero.)
4. **Currency/real engine** (§2.4) with tie-out tests. (Gates the flagship.)
5. **⭐ Asset Detail deep-dive** (§4). (The feature you actually want.)
6. **Periodic Table overflow fix** (§5.4) + **Price Log de-noise** (§5.2). (Quick, high-visibility wins.)
7. **Volatility redesign** (§5.9) — regime + percentile + term structure.
8. **Drawdowns / Correlation / Seasonality / Hold Lab** upgrades (§5.5–5.8).
9. **New ratio/event-study modules** (§6).

Each step ships independently and leaves the site in a working state (auto-researcher loop: build → verify tie-outs → keep what beats current).

---

## 9. Open design decisions (need your call — these gate the build)

1. **Canonical data source:** Rebuild the ingest around `Markets_1_.xlsx` (recommended), or keep `Mega_Markets_Historical.xlsx` and you'll export into its shape?
2. **Currency math:** Compute all-currency returns client-side from USD price × FX series (recommended, flexible), trusting your Excel's precomputed INR only as a test oracle — yes/no?
3. **"Local" for non-USD assets:** Is Sensex's "Local" = INR, Nikkei = JPY, DAX = EUR (i.e. true native), with USD/INR/etc. as conversions on top?
4. **Real-INR returns:** Use **Indian CPI** when lens=INR + basis=real (correct but needs the series), or US CPI as a stopgap?
5. **`Corporate_CAGR.xlsx`:** Is that your personal trade ledger? Should it become a private "Portfolio vs Benchmarks" view, or stay out of the public Markets section entirely?
6. **Currencies to support in the lens:** Local · USD · INR · Gold · Silver — add JPY/EUR/CNY now or later?
7. **Scope of this pass:** Ship steps 1–5 first (pipeline + matrix + flagship), then iterate — or spec-and-build the full §5 in one sweep?

---

# PART II — Portfolio Lab & Simulator (`docs/sim.html`, expanded)

**Goal:** replicate — then surpass — Portfolio Visualizer's *Backtest Portfolio* and Interactive Brokers *PortfolioAnalyst* (the David Orr / Militia Capital screenshots), and add Kelly/position-sizing. Your current `sim.html` (Leveraged ETN NAV simulator) becomes **one tool** inside this lab, not the whole thing. This is the deep-analytics counterweight to the simple Markets section; reuse all Part I foundations (§2).

> **DECIDED — primary mode is a HYPOTHETICAL backtester.** Build *any* portfolio from scratch (tickers + weights) and simulate it historically against a benchmark. This is the anchor of the lab. Importing your own real track record (`Corporate_CAGR.xlsx`) is a **secondary, optional** convenience built *after* the hypothetical core works — not the foundation. Consequence: arbitrary-ticker total-return price history is now a **hard dependency**, not an open question (see §22.1).

## 10. Information architecture
Rename `sim.html` → **Portfolio Lab** with sub-tabs (left rail or top tabs):
`Builder · Backtest · Risk vs Benchmark · Growth · Drawdowns · Monte Carlo · Position Sizing · Leverage Decay (ETN)`
Top-level controls shared across sub-tabs: benchmark selector (default S&P 500 TR), date window (§2.5 control), currency/real lens (§2.4), rebalance frequency, risk-free source. One portfolio + one benchmark drive every panel — the "symmetry" rule applies here too.

## 11. Builder (replicates the Sample-Portfolio screen)
- Table: **Ticker · Name · Allocation%**, editable rows, add/remove, **weights may exceed 100%, go negative (short/inverse), and include cash/borrow** (e.g. `SPY 150%`, `DWSH 50%`, `^CASHUS −100%` = 100% margin). Show a live "net exposure / gross exposure / leverage" readout.
- **Save / Load / Edit** portfolios (persist to a JSON the user can commit; do **not** use localStorage in artifacts — keep in-memory + export/import file).
- **Multiple portfolios** comparable side-by-side (P1 vs P2 vs benchmark), like the Militia "Consolidated vs SPY/IWM/EWJ" comparison.
- **Excel/CSV import** (the "drop-in" — see §22).

## 12. Backtest engine — methodology (match Portfolio Visualizer exactly, then extend)
Confirmed PV conventions to mirror so numbers tie out to what people expect:
```
• Monthly total-return series per asset (dividends + distributions reinvested).
• Portfolio monthly return r_p,t = Σ_i w_i · r_i,t  (weights reset to target at each rebalance).
• Default rebalance = annual; selectable: none / monthly / quarterly / annual / threshold-band.
• Risk-free = FRED 3M T-Bill (you already ingest it); used for Sharpe/Sortino/Treynor excess returns.
• Inflation = CPI-U (you have CPI); drives the "inflation-adjusted" (real) toggle.
• CAGR from start/end balance; risk stats from monthly returns, annualized (×√12); maxDD from the monthly equity curve.
• Leverage/borrow: a negative cash weight accrues borrow cost = (3M T-Bill + spread) on the borrowed notional each period; subtract.
• Fees: optional per-asset TER (%/yr) and a portfolio-level advisory fee; subtract pro-rata monthly.
```
Surpass PV: add **daily** backtest mode (uses your daily price series) for accurate leverage-decay and drawdown timing; add **transaction-cost** modeling on rebalance turnover.

## 13. Performance Summary vs Benchmark (the Portfolio Visualizer table)
Two columns (Portfolio | Benchmark), every number with an info-tooltip stating its formula. Exact metric set + formulas (monthly basis, annualized):
```
Start / End Balance        equity curve endpoints (lens + real aware)
CAGR                       (End/Start)^(12/N) − 1
Std Dev (ann.)             stdev(r_t) · √12
Best / Worst Year          max/min of calendar-year compounded returns
Max Drawdown               min over t of (equity_t / running_peak_t − 1)
Sharpe                     mean(r_t − rf_t) / stdev(r_t − rf_t) · √12
Sortino                    mean(r_t − rf_t) / downsideDev · √12        (downsideDev = √mean(min(r_t−MAR,0)²))
Calmar                     CAGR / |MaxDD|
Treynor                    [mean(r_t − rf_t)·12] / Beta
Active Return              mean(r_p,t − r_b,t) · 12
Tracking Error             stdev(r_p,t − r_b,t) · √12
Information Ratio          Active Return / Tracking Error
Beta                       cov(r_p−rf, r_b−rf) / var(r_b−rf)
Alpha (ann.)               [mean(r_p−rf) − Beta·mean(r_b−rf)] · 12
R²                         corr(r_p, r_b)²
Benchmark Correlation      corr(r_p, r_b)
Skewness, Excess Kurtosis  of monthly returns
Upside / Downside Capture  geo-mean(port | bench up/down months) ÷ geo-mean(bench | those months)
```
Surpass: add **Omega ratio** (Σ gains above MAR ÷ Σ |losses below MAR|), **Ulcer Index** (√mean(DD_t²)) + **Martin ratio**, **% Positive Periods**, and a **MAR control** for Sortino/Omega.

## 14. Risk vs Benchmark panel (the IBKR PortfolioAnalyst layout, screenshots 4 & 6)
Reproduce the exact four-quadrant layout, restyled to your dark theme:
- **VAMI chart** — Value Added Monthly Index: `VAMI_0 = 1000; VAMI_t = VAMI_{t−1}·(1+r_t)`. One line per portfolio + benchmarks, asset-identity colors.
- **Distribution of Returns** — histogram of monthly returns bucketed (e.g. −4..−2, −2..0, 0..2, 2..4%), one bar group per series. Overlay a normal curve to show fat tails.
- **Risk Measures table** — Ending VAMI · Max Drawdown · Peak-To-Valley (dates) · Recovery (or "Ongoing") · Sharpe · Sortino · Std Dev · Downside Deviation · Mean Return · Positive/Negative Periods (count + %). One column per series, "Consolidated" highlighted.
- **Risk Measures Relative to Benchmark** — Correlation · Beta · Alpha vs each benchmark (SPY/IWM/EWJ pattern).
- Header notes the **return method (TWR)** and analysis period, exactly like IBKR.

## 15. Growth, Drawdowns, Rolling (reuse Part I engines)
- **Portfolio Growth** chart: log/linear toggle + inflation-adjusted toggle (§2.4) + §2.5 range control. Portfolio vs benchmark(s), Growth-of-$10k.
- **Drawdowns**: feed the portfolio equity curve into the Part I Drawdown Studio engine (§5.5) — underwater curve, max-DD event table, recovery, current-DD percentile.
- **Rolling metrics**: rolling CAGR, rolling Sharpe, rolling vol, rolling beta, rolling correlation-to-benchmark (the regime story, like §5.8).

## 16. Monte Carlo / forward simulation (to top PV)
- Methods: **historical bootstrap** (resample monthly returns), **block bootstrap** (preserve autocorrelation/regimes — superior to PV's iid), and **parametric** (mean/cov, optional Student-t fat tails).
- Outputs: **fan chart** of terminal wealth percentiles (5/25/50/75/95), terminal-wealth histogram, **probability of loss**, **max-DD distribution**, and — with withdrawals — **success rate** + safe-withdrawal-rate solver.
- Inputs: horizon, contributions/withdrawals (nominal or inflation-adjusted), # paths.

## 17. Position Sizing — Kelly (the Militia Capital tool, screenshot 5)
- **Continuous Kelly:** optimal leverage `f* = (μ − rf) / σ²` (per-period arithmetic mean/variance); growth rate `g(f) = rf + f(μ−rf) − ½ f² σ²`, maximized at `f*`, and `g(2f*) = rf` (overbetting gives back all edge).
- **Discrete Kelly:** for win prob `p`, payoff `b:1`, loss `a`: `f* = (p·b − (1−p)·a) / (a·b)` (simplifies to `p − (1−p)/b` for `a=1`).
- **Kelly curve chart** (reproduce Orr's exactly): x-axis Kelly fraction 0→2.0; blue **Return/growth** curve (inverted-U, peak at 1.0×); red **Volatility** curve (rises with f); markers at full-Kelly and **half-Kelly** (the recommended default).
- **Net-worth sizing matrix** (editable, replicates his table): rows = net-worth tiers (`<$1M / $1–5M / $5M+`), columns = `Low / Moderate / High` risk tolerance, cells = max % of net worth per position. Let the user edit and save it as their sizing policy.
- Pull `μ, σ` straight from the Backtest of the selected portfolio so Kelly is grounded in real numbers, not guesses.

## 18. Leverage Decay (ETN) — keep + upgrade
Your existing daily-compounding leveraged/inverse NAV simulator is good and stays as a sub-tab. Upgrades: drive "underlying" from a **real selected asset's daily series** (not just the manual vol slider); add **borrow/financing cost** and **TER** to the decay; chart **ETN vs naive-leverage vs underlying**; quantify **volatility drag = ½·(L²−L)·σ²** annualized and show it ties out to the simulated path.

## 19. Stress tests / scenario analysis (to top PV)
Apply historical crisis windows (2008 GFC, 2020 COVID, 2022 rate shock — from Part I `events`) to the **current** portfolio weights and report each scenario's drawdown, vol, and recovery. "If today's portfolio had lived through 2008, it would have drawn down X%." This turns history into a forward-looking risk gate.

## 20. Excel / data drop-in (the "figure out how it works" ask)
Three import modes, parser locates columns by header text (robust to layout), validates, and shows a preview before committing:
1. **Weights** — `[Ticker, Name, Weight%]` → static backtest portfolio. (Fastest to replicate the Sample-Portfolio screen.)
2. **Returns ledger** — `[Date, Return%]` per strategy (your `Corporate_CAGR.xlsx::% Returns`) → compute *all* §13–14 metrics + VAMI directly from your real track record vs benchmark. **This is the one-step path to reproduce the Militia VAMI/risk panels from your own numbers.**
3. **Trades ledger** — `[Date, Ticker, Side, Qty, Price, Fees]` (your `Corporate_CAGR.xlsx::Trades`) → reconstruct positions over time → **TWR** (matches IBKR) + **money-weighted IRR**, then the full risk panel.

## 21. Part II — acceptance & tie-outs
- Recreate PV's Sample Portfolio (`DWSH 50% / SPY 150% / ^CASHUS −100%`) and **match PV's CAGR, Sharpe, Sortino, Max DD within rounding** on the same date range.
- VAMI starts at exactly 1000 and `VAMI_end / 1000 − 1 == total return`.
- Information Ratio `== Active Return / Tracking Error` exactly; Beta from regression matches `corr·(σ_p/σ_b)`.
- Importing your `% Returns` sheet reproduces a VAMI/risk panel that visually matches the IBKR screenshots' structure.
- Kelly: `g(f)` peaks at `f*` and `g(2f*) ≈ rf` in the chart.
- Leverage: simulated ETN decay ties out to the closed-form `½(L²−L)σ²` drag.
- Every metric has a formula tooltip; nothing is a black box.

## 22. Part II — decisions
**RESOLVED:**
- **(1) Backtest price source — LOCKED.** Hypothetical-first makes arbitrary-ticker total-return history a hard dependency. Build a curated **`prices.json` via yfinance adjusted-close at build time** (dividends reinvested). This is foundational — build it first, before the backtest engine. Cache to `docs/data/prices.json`; refresh under the same freshness gate (§3.3).
- **(3) Real track record — SECONDARY.** Importing `Corporate_CAGR.xlsx` (`% Returns` / `Trades`) into Excel-import modes 2/3 is an *optional* feature built **after** the hypothetical core ships. Do not let it shape the core architecture. (Also resolves Part I §9.5: keep it out of the public Markets section; it lives only as an optional Portfolio-Lab import.)

**STILL OPEN (sensible defaults chosen so the agent can build now):**
- **(2) Backtestable universe:** default day-one = your full ETF universe (from `config.yaml`) **+** every index/commodity/metal/FX already in `market_returns.json`. Confirm or extend.
- **(4) Granularity:** default = **monthly** backtest first (Portfolio-Visualizer parity, simplest tie-out), with **daily** as a follow-on mode for accurate leverage decay. Override if you want daily from day one.
- **(5) Default benchmark:** default = **S&P 500 TR**, with a selectable multi-benchmark set (SPY / IWM / EWJ / Sensex) for the IBKR-style comparison. Confirm.

---

# PART III — Leaderboard & Scoring Engine (`docs/index.html` + `predator/scoring.py`)

**Context:** the Cross-ETF Conviction Scanner is the core product and works well at finding institutionally-targeted momentum names. But the scoring has a structural bias and is currently distorted by the 15→30 ETF migration. This part fixes the *meaning* of the rank, not its looks. Read the project README for the current formula before changing anything.

## 23. Diagnosis — two problems, one structural, one transient

### 23.1 Structural: breadth-of-filler beats depth-of-conviction (the ITUB4 > Samsung bug)
Worked from live numbers (holdings counts from the ETFs tab: EEMO 263, JHEM 865, MFEM 706):
```
Samsung A005930 (2 ETFs, real conviction):  JHEM #2 @5.51% → 795.9   MFEM #4 @1.76% → 458.4
ITUB4   Itaú     (4 ETFs, mostly filler):   EEMO #9 @1.28% → 414.8   MFEM #30 @0.64% → 346.1
                                            JHEM #67 @0.28% → 316.8   PIE #82 @0.48% → 219.3
```
ITUB4 (1297) outranks Samsung (1254) purely on **breadth**, even though Samsung's positions show vastly higher conviction. Root causes:
- **Step rank multiplier saturates at #30** (1.5/1.2/1.0×) — #31 and #500 score identically; rank carries zero signal exactly where filler lives.
- **Additive Σ across ETFs rewards breadth**, and each ETF adds a large tier-points floor, so 4 shallow ≫ 2 deep.
- **International 60-pt override** inflates every EM holding 50% regardless of depth — double-amplifying tail positions.

### 23.2 Transient: the NEW flood + a date bug (the 15→30 migration)
- **3,746 / 3,950 names flagged NEW** because doubling the universe reset the 14-day lookback. The new-entrant bonus (×5 tier points) is firing near-universally and rewards breadth-heavy names most — compounding 23.1. 7-day deltas all read +0.0%, velocity/burst are mostly dead, Changes Today = 0.
- **Date bug:** header shows `Holdings 2026-05-26` but `Built 2026-05-24` — holdings dated in the *future* vs build. Date-parse / "As Of" extraction error; likely the same reason day-over-day diffs are empty.

## 24. The core fix — conviction-weighted scoring
Replace the `weight%×100` term **and** the step `rank_multiplier` with one continuous, size-normalized **conviction multiplier** per holding. Two ingredients:
```
n_i   = holdings in ETF i        w_i = weight (fraction)        r_i = rank (1 = top)
Overweight   OW_i = w_i / (1/n_i) = w_i · n_i      # 1 = equal-weight, >1 conviction, <1 filler  (PRIMARY)
RankPct      RP_i = 1 − (r_i − 1)/n_i              # 0..1, top of book = 1                        (SECONDARY)

Conviction   C_i = Wovr(OW_i) · Grank(RP_i)
  Wovr(OW) = clamp( ln(1+OW) / ln(1+OW★) , 0 , Cmax )     # OW★≈4 → ~1.0 ; Cmax≈1.5
  Grank(RP)= RP_floor + (1−RP_floor)·RP                    # RP_floor≈0.35

Single Score_i = TierPoints_i · C_i        # tier points kept; weight%×100 and step mult REMOVED (de-double-counted)
Final          = Σ_i Single Score_i
```
**Why overweight, not just rank percentile (you asked for percentiles — here's the upgrade):** in JHEM (865 names) Itaú's #67 is "top 8%" by rank but only 2.4× equal-weight; Samsung's #2 is 47× equal-weight. Rank percentile would wrongly bless the filler; overweight exposes it. Use OW as primary, RP as a gentle gate.

**Worked tie-out (target behavior):** under this model Samsung ≈ 177 conviction-pts (both positions heavily overweight, top of book) vs Itaú ≈ 151 — and Itaú's PIE #82 (which is *underweight* equal-weight) collapses from 219 → ~7. **Samsung now ranks above Itaú; tail positions stop mattering.** Constants (OW★, Cmax, RP_floor) are tunable knobs — expose them in `config.yaml` and let the auto-research loop sweep them.

## 25. Aggregation, breadth, and the HIGH_CONVICTION redefinition
- **Conviction breadth** `k_c = count of ETFs where C_i ≥ θ` (θ≈0.8). This is "how many strategies hold it *with conviction*," distinct from raw ETF count.
- **Redefine HIGH_CONVICTION = k_c ≥ 4** (≥4 ETFs holding with conviction), not ≥4 ETFs holding at all. This is the single most important flag fix — it's what the thesis actually means.
- Keep Final additive initially; optionally test a **concave breadth** term (marginal contribution of the k-th ETF decays) since 1→2 independent strategies is a bigger signal than 8→9. A/B it; keep only if it improves the ranking.

## 26. Deprecate the international 60-pt override
With within-ETF normalization (OW + RP), ETF size and US/intl differences are already leveled — a #9 in EEMO is genuinely high *in EEMO's book*. The flat 60-pt override now over-corrects. **Recommended:** drop it, or replace with a small, explicit "hard-to-find" bonus (e.g. +10% for names in ≤2 ETFs) so genuine rare conviction still surfaces without inflating EM tails. (DECISION §29.1.)

## 27. Score normalization for universe-size stability
Absolute Σ-scores balloon as the universe grows (15→30 already doubled them), so scores aren't comparable over time. **Display a normalized score** = percentile (0–100) or z-score of Final within the day's universe; keep raw Final for sorting. This makes the leaderboard scale invariant to adding ETFs — essential now and every future expansion.

## 28. Fix the transient distortions
- **Per-ETF maturity gate (kills the NEW flood):** track `first_tracked_date` per ETF. A ticker is "NEW-in-ETF" only if `(today − first_tracked_date_i) ≥ lookback` AND the ticker was absent in that ETF's *own* prior lookback window. Suppress the new-entrant bonus and NEW flag for not-yet-mature ETFs. Optional: backfill historical holdings for the 15 new ETFs where issuers publish them.
- **Universe-expansion banner:** while any ETF is immature, show a small honest note ("12 ETFs still building 14-day baselines — velocity/NEW signals partial") instead of a silently broken leaderboard.
- **Date bug:** fix the "As Of" parse so `Holdings` date ≤ `Built` date; re-verify Changes / 7-day deltas come alive afterward.

## 29. New features (analysis from my end — make it better, stay clean)
Core stays simple; these are progressive disclosure, off by default:
- **Conviction column + sort** — surface avg OW / avg RP so two 4-ETF names are distinguishable at a glance. Extend the auto-explainer: "held with conviction by 2 (47×/12× overweight), filler in 1."
- **Early-vs-Crowded axis (serves your stated edge directly).** The thesis is "before it becomes consensus." Plot breadth (× ETFs) against velocity/weight-flow: **rising weight in 2–3 ETFs = EARLY (the alpha); flat weight in 9 ETFs = CROWDED (late).** A quadrant chip (Emerging / Building / Consensus / Fading) is the most decision-useful thing you can add.
- **Accumulation vs distribution** — sign the weight-flow: managers *adding* (weight↑ rank↑) vs *trimming* (weight↓). You already compute flow per-ETF and stealth accumulation; elevate the *sign* into a primary signal.
- **Flow normalization (sector/country tabs):** current "sum of velocity" is dominated by 1–2 names (Chile +141 from 1 name). Add per-name normalization **and** flow-vs-own-history z-score so "+141" is contextualized. Inherits the conviction fix automatically.
- **Liquidity/size sanity:** optional ADV / market-cap floor or a micro-cap tag so illiquid EM tail names aren't surfaced as "conviction." You already track JHSC small-cap — use it.
- **Factor attribution chip:** which tier is driving the score (momentum / quality / value / spinoff) — a compact stacked bar on the row (you already have full decomposition on the stock page).
- **Data-maturity indicator** per temporal signal (velocity/burst/7d) so users know which signals are still warming up post-migration.

## 30. Part III — acceptance & tie-outs (block release)
- **Samsung > ITUB4** after the conviction fix (the canonical regression).
- A holding that is **underweight equal-weight contributes < 5%** of what a top-of-book overweight position in the same ETF contributes.
- **NEW count drops** from ~3,746 to a sane level once the maturity gate is on (only genuinely-new institutional positions flagged).
- **Display scores stable** across a simulated universe change (add a dummy ETF → normalized leaderboard order barely moves; raw Σ would have jumped).
- `Holdings` date ≤ `Built` date; Changes / 7-day deltas populate.
- HIGH_CONVICTION count reflects conviction-breadth (k_c≥4), not raw holding count.
- Every constant (OW★, Cmax, RP_floor, θ) lives in `config.yaml`; one commit re-scores in ~10 min (existing behavior preserved).

## 31. Part III — open decisions
1. **International override:** drop entirely (rely on within-ETF normalization), or keep a small "hard-to-find" bonus for ≤2-ETF names? (Recommend: drop, add small bonus.)
2. **Conviction shape:** OW★≈4 / Cmax≈1.5 / RP_floor≈0.35 are starting values — want me to sweep them against a hand-labeled "good vs filler" set you provide, or ship defaults and tune in the back-and-forth?
3. **Breadth aggregation:** additive (simple, current) or concave/diminishing? (Recommend additive first, A/B concave.)
4. **NEW backfill:** can the 15 new ETFs' historical holdings be sourced, or do we rely solely on the maturity gate and wait out the 14 days?
5. **Display score:** percentile (0–100) or z-score for the normalized leaderboard number? (Recommend percentile — most legible.)

---

*Built to the standard: institutional-grade, every number defensible, every interaction purposeful. Part I ships simple; Part II ships deep; Part III makes the rank mean what it says. — Predator Protocol Overhaul v3*
