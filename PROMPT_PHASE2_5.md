# Phase 2.5 — Per-ETF Calibration Fix + UX Enhancement Sweep

Working in `yieldchaser/etf-data`. Phases 1, 2, 3, 4 prompts have been run (bug-fix, per-stock detail, markets, simulator). The user has refreshed their Excel `Equity_Indices_.xlsb` and verified the actual ETF point assignments. The current live site does not match the Excel because two ETFs have **per-ETF point overrides** that the current `config.yaml` cannot express.

This prompt does two things:

**Part A** — Critical scoring calibration. Restructure the tier config to support per-ETF point overrides. Match the Excel exactly.

**Part B** — Eight UX enhancements, all grounded in specific user feedback on the live dashboard.

Do them in order: Part A first (small, high-impact), then Part B.

---

## Part A — Per-ETF Point Calibration (CRITICAL)

### Confirmed discrepancy

Excel `ETF_Config` table (verified from the user's screenshot):

| ETF   | Points | Category |
|-------|--------|----------|
| CSD   | 40     | Scout    |
| FPX   | 40     | Scout    |
| **FPXI** | **60** | **Scout** ← override |
| QMOM  | 40     | Quant    |
| **IMOM** | **60** | **Quant** ← override |
| XMMO  | 40     | Quant    |
| XSMO  | 40     | Quant    |
| PIE   | 40     | Quant    |
| COWZ  | 30     | Quality  |
| CALF  | 30     | Quality  |
| SPHQ  | 30     | Quality  |
| SPMO  | 10     | Trend    |
| SPHB  | 10     | Trend    |
| RPG   | 10     | Trend    |
| XLG   | 2      | Blob     |
| QQQM  | 2      | Blob     |

Math validation (using `Single_Score = weight × points × rank_mult × 100`):

| Ticker | ETF | Weight | Rank | Excel Score | FPXI=40 yields | FPXI=60 yields |
|---|---|---|---|---|---|---|
| 6857.JP | FPXI rank 1 | 9.90% | 1 | **891** | 594 (current site) | **891** ✓ |
| 3750.HK | FPXI rank 2 | 9.21% | 2 | **829** | 553 | **829** ✓ |
| ENR.GR  | FPXI rank 5 | 6.31% | 5 | **568** | 378 | **568** ✓ |

The 60-point hypothesis matches Excel to the integer.

### Required change to `config.yaml`

Replace the existing `tiers:` block with a flat `etfs:` list that supports per-ETF points. The current format groups by tier; the new format is one row per ETF.

```yaml
# =============================================================================
# ETF UNIVERSE — per-ETF tier and point assignment.
# Matches Excel ETF_Config table exactly. FPXI and IMOM have per-ETF point
# overrides (60) that differ from their tier's default — this is the whole
# reason we moved away from grouping by tier.
# =============================================================================
etfs:
  # Scout — spinoffs and IPOs
  - {ticker: CSD,  tier: Scout,   points: 40}
  - {ticker: FPX,  tier: Scout,   points: 40}

  # Quant — factor-based momentum
  - {ticker: QMOM, tier: Quant,   points: 40}
  - {ticker: XMMO, tier: Quant,   points: 40}
  - {ticker: XSMO, tier: Quant,   points: 40}
  - {ticker: PIE,  tier: Quant,   points: 40}

  # ─── International Exception ────────────────────────────────────────────
  # FPXI and IMOM are international funds with naturally lower overlap with
  # US-only ETFs. Bumping their points to 60 levels the playing field for
  # global names that otherwise wouldn't accumulate score across multiple ETFs.
  # They stay in their conceptual tier (Scout / Quant) so NEW-entry boost and
  # SPEC-β logic continue to apply correctly.
  - {ticker: FPXI, tier: Scout,   points: 60}
  - {ticker: IMOM, tier: Quant,   points: 60}

  # Quality — free cash flow and profitability
  - {ticker: COWZ, tier: Quality, points: 30}
  - {ticker: CALF, tier: Quality, points: 30}
  - {ticker: SPHQ, tier: Quality, points: 30}

  # Trend — momentum confirmation
  - {ticker: SPMO, tier: Trend,   points: 10}
  - {ticker: SPHB, tier: Trend,   points: 10}
  - {ticker: RPG,  tier: Trend,   points: 10}

  # Blob — mega-cap benchmarks
  - {ticker: XLG,  tier: Blob,    points: 2}
  - {ticker: QQQM, tier: Blob,    points: 2}

# Keep the rest of the file (sanitizer, rank_breakpoints, new_*, history, etc.) unchanged.
```

Remove the old `tiers:` block entirely.

### Required change to `predator/scoring.py`

The `Tier` dataclass loses its `etfs` and `points` fields (they're now per-ETF). Replace with two clean dataclasses:

```python
@dataclass(frozen=True)
class ETF:
    """One row from the ETF universe — ticker, tier name, point weight."""
    ticker: str
    tier: str
    points: int


@dataclass(frozen=True)
class Config:
    sanitizer: Sanitizer
    etfs: tuple[ETF, ...]                              # was: tiers
    rank_breakpoints: tuple[tuple[int, float], ...]
    new_lookback_days: int
    new_bonus_mult: float
    new_bonus_tiers: tuple[str, ...]
    high_conviction_min_etfs: int
    history: HistoryConfig

    @classmethod
    def from_yaml(cls, path: str | Path) -> "Config":
        cfg = yaml.safe_load(Path(path).read_text())
        san = cfg.get("sanitizer", {})
        sanitizer = Sanitizer(...)   # unchanged

        etfs = tuple(
            ETF(ticker=e["ticker"], tier=e["tier"], points=int(e["points"]))
            for e in cfg["etfs"]
        )
        return cls(
            sanitizer=sanitizer,
            etfs=etfs,
            # ...rest unchanged...
        )

    def etf_lookup(self) -> dict[str, ETF]:
        """O(1) lookup from ticker to ETF metadata."""
        return {e.ticker: e for e in self.etfs}

    def etfs_in_tier(self, tier: str) -> tuple[str, ...]:
        return tuple(e.ticker for e in self.etfs if e.tier == tier)

    def all_tier_names(self) -> tuple[str, ...]:
        # Order-preserving unique
        seen, out = set(), []
        for e in self.etfs:
            if e.tier not in seen:
                seen.add(e.tier); out.append(e.tier)
        return tuple(out)
```

In `compute_leaderboard`, replace `etf_to_tier = cfg.etf_tier_map()` with `etf_lookup = cfg.etf_lookup()`. Replace tier and points lookups:

```python
# Before (broken — tier groups all share a single point value):
latest["tier"] = latest["ETF_Ticker"].map(lambda e: etf_to_tier[e].name)
latest["tier_points"] = latest["ETF_Ticker"].map(lambda e: etf_to_tier[e].points)

# After (correct — per-ETF points):
latest["tier"] = latest["ETF_Ticker"].map(lambda e: etf_lookup[e].tier)
latest["tier_points"] = latest["ETF_Ticker"].map(lambda e: etf_lookup[e].points)
```

Same substitution in `compute_rank_deltas` for any membership checks.

### Required change to `tests/test_scoring.py`

Update `test_all_16_etfs_in_config` to use the new shape:

```python
def test_all_16_etfs_in_config(self, cfg):
    expected = {"CSD", "FPX", "FPXI", "QMOM", "IMOM", "XMMO", "XSMO", "PIE",
                "COWZ", "CALF", "SPHQ", "SPMO", "SPHB", "RPG", "QQQM", "XLG"}
    assert set(cfg.etf_lookup().keys()) == expected

def test_fpxi_and_imom_have_60_points(self, cfg):
    """Per-ETF overrides verified against Excel ETF_Config table."""
    lookup = cfg.etf_lookup()
    assert lookup["FPXI"].points == 60
    assert lookup["FPXI"].tier == "Scout"
    assert lookup["IMOM"].points == 60
    assert lookup["IMOM"].tier == "Quant"
    # And the others stay at 40
    assert lookup["FPX"].points == 40
    assert lookup["QMOM"].points == 40
```

Update `test_score_formula_matches_power_query` — the GEV scenario in that test relies on tier points. Re-verify with the new lookup; the math doesn't change because GEV is in tiers that don't override (Scout via FPX=40, Quant via QMOM=40, Quality via SPHQ=30, Trend via SPMO=10).

### Definition of Done — Part A

1. `python -m pytest tests/ -v` passes including 2 new tests above.
2. After `python -m predator.build`, the live `leaderboard.json` shows:
   - `6857.JP` (Advantest) at score **891** (currently 594)
   - `3750.HK` (CATL) at score **829** (currently 552)
   - `ENR.GR` (Siemens Energy) at score **568** (currently 378)
   - `285A.JP` (Kioxia) at score **509** (currently 339)
3. Top of leaderboard still ranks `SNDK` #1 (~1388–1418), `GEV` #2 (~1338–1353).

---

## Part B — Eight UX Enhancements

Each enhancement targets a specific gap the user identified. All work happens in `docs/index.html` and `docs/stock.html` unless otherwise noted. Keep the no-build-step constraint (CDN Tailwind + Alpine.js).

### B1. Universal tooltip system (do this first — everything else depends on it)

#### What's wrong

The existing `[data-tip]` CSS attribute renders static strings only. There's no way to:
- pass dynamic content (interpolated values, multi-line)
- delay show / smooth fade
- position-aware (flip above/below if it would clip)
- richer formatting (bold labels, color-coded numbers)

#### Required: build an Alpine directive `x-tooltip`

Add to `docs/index.html` (and copy to `stock.html`):

```html
<!-- Singleton tooltip element, lives at body level -->
<div id="tt" class="fixed z-[100] pointer-events-none opacity-0 transition-opacity duration-150"
     style="background: var(--surface-2); border: 1px solid var(--border-2); color: var(--text);
            padding: 6px 10px; border-radius: 6px; font-size: 11px; max-width: 280px;
            box-shadow: 0 4px 20px rgba(0,0,0,0.6); line-height: 1.5"></div>

<script>
// Alpine directive: x-tooltip="'plain string'" or x-tooltip="{ html: '...', side: 'top' }"
document.addEventListener('alpine:init', () => {
  Alpine.directive('tooltip', (el, { expression }, { evaluate, cleanup }) => {
    const tt = document.getElementById('tt');
    let timer;
    const show = (e) => {
      const v = evaluate(expression);
      const cfg = typeof v === 'string' ? { html: v, side: 'top' } : v || {};
      if (!cfg.html) return;
      timer = setTimeout(() => {
        tt.innerHTML = cfg.html;
        tt.style.opacity = '1';
        positionTip(tt, el, cfg.side || 'top');
      }, 120);   // 120ms delay = doesn't fire on accidental hover
    };
    const hide = () => {
      clearTimeout(timer);
      tt.style.opacity = '0';
    };
    const move = (e) => positionTip(tt, el, 'top');
    el.addEventListener('mouseenter', show);
    el.addEventListener('mouseleave', hide);
    el.addEventListener('click', hide);          // close on click
    cleanup(() => {
      el.removeEventListener('mouseenter', show);
      el.removeEventListener('mouseleave', hide);
    });
  });
});

function positionTip(tt, el, side) {
  const r = el.getBoundingClientRect();
  const tr = tt.getBoundingClientRect();
  let x = r.left + r.width / 2 - tr.width / 2;
  let y = r.top - tr.height - 8;
  // Flip below if not enough room above
  if (y < 8) y = r.bottom + 8;
  // Clamp horizontally
  x = Math.max(8, Math.min(window.innerWidth - tr.width - 8, x));
  tt.style.left = x + 'px';
  tt.style.top = y + 'px';
}
</script>
```

Replace **all** existing `data-tip="..."` attributes with `x-tooltip="'...'"` for plain strings, or `x-tooltip="{html: '...'}"` for rich content.

#### Tooltips to add (grounded in actual UI elements)

For each column header in the leaderboard table:

| Header | Tooltip HTML |
|---|---|
| `#` | `Rank by Final Alpha Score (1 = highest conviction)` |
| `Ticker` | `Stock ticker. Click to open per-stock detail page.` |
| `Company` | `Issuer's legal name as reported by the ETF.` |
| `Score` | `Final Alpha Score = sum of (weight % × ETF points × rank multiplier × 100) across all ETFs holding this name. Higher = stronger cross-ETF conviction.` |
| `SC.Δ%` | `Score change vs the prior reference date. Use the period chips to pick 1d, 7d, 14d, 30d, 60d, 90d, YTD, or a custom range.` |
| `ETFs` | `Count of distinct ETFs currently holding this ticker.` |
| `Weight` | `Sum of position weights across all ETFs holding this name.` |
| `Strategies` | `Tiers represented across the ETFs holding this name. <br><b>Scout</b> = spinoffs/IPOs · <b>Quant</b> = factor momentum · <b>Quality</b> = FCF/profit · <b>Trend</b> = momentum confirmation · <b>Blob</b> = mega-cap benchmarks` |
| `Streak` | `Consecutive days the score moved in the same direction (▲ up · ▼ down).` |
| `Level` | `Today's score as a percentile of this ticker's own score history in the past 60 days. <br>33% bar = score is lower than 67% of recent days.` |
| `Flag` | `Conviction marker. <br><b>HC</b> = held by 4+ ETFs <br><b>SPEC</b> = in Trend tier without Quality or Scout backing <br><b>NEW</b> = first appearance in any ETF within the last 14 days` |

For each row chip (HC, SPEC, NEW, tier chips):

```html
<!-- HC chip -->
<span class="chip ..." x-tooltip="`HIGH CONVICTION — held by ${row.etf_count} ETFs<br>HC streak: ${row.hc_streak || 0} days`">HC</span>

<!-- Tier chip -->
<span class="chip" :style="tierChipStyle(t)"
      x-tooltip="`${t} tier — ${tierExplain(t)}`" x-text="t"></span>
```

Add a helper function:

```javascript
tierExplain(t) {
  const m = {
    Scout: 'Spinoffs and IPOs · CSD, FPX, FPXI (60 pts)',
    Quant: 'Factor-based momentum · QMOM, IMOM (60 pts), XMMO, XSMO, PIE',
    Quality: 'Free cash flow & profitability · COWZ, CALF, SPHQ',
    Trend: 'Broad momentum confirmation · SPMO, SPHB, RPG',
    Blob: 'Mega-cap benchmarks · QQQM, XLG',
  };
  return m[t] || '';
}
```

For the KPI strip cards (`names tracked`, `high conviction`, etc.):

```html
<div class="rounded-lg..." x-tooltip="kpi.tooltip">
```

Add `tooltip` to each kpi object in the getter:

```javascript
{ label: 'high conviction', value: c.HIGH_CONVICTION, color: 'var(--up)',
  tooltip: `Tickers held by 4 or more ETFs. <br>Δ vs yesterday: ${this.deltaSign(c.HIGH_CONVICTION - (yc.HIGH_CONVICTION || 0))}` },
```

For period selector chips (1d / 7d / 14d / 30d / 60d / 90d / YTD / custom):

```html
<button x-tooltip="`Score change vs ${periodLabel(p)}`" ...>
```

with:

```javascript
periodLabel(p) {
  return {1:'yesterday',7:'a week ago',14:'two weeks ago',30:'a month ago',60:'two months ago',90:'three months ago','YTD':'Jan 1'}[p] || 'a custom date';
}
```

#### DoD — B1
- Hovering any leaderboard column header for 120ms shows a positioned tooltip explaining what that column means.
- Tooltips flip below when near the top edge of the viewport.
- Tooltips disappear smoothly (no flicker, no clipping at the right edge).
- All chips on a row (HC, SPEC, NEW, tier) have their own context-specific tooltips with interpolated values.

---

### B2. Score history chart — hover values + add a Rank History chart

#### What's wrong

On `stock.html` (e.g., for SNDK), the score-history SVG sparkline shows a smooth trend but no way to know what the exact score was on any specific day. There's also no visualization of how the stock's **rank** moved over time per ETF — that's a critical signal independent of score.

#### Required changes (in `docs/stock.html`)

**1. Add an interactive crosshair tooltip to the score history SVG.**

Currently the score chart is a single `<path>`. Wrap it in a `<g>` with an invisible overlay `<rect>` that captures mousemove events:

```html
<div class="rounded-lg border" style="background: var(--surface); border-color: var(--border)">
  <div class="px-3 py-2 flex items-center justify-between" style="border-bottom: 1px solid var(--border)">
    <div class="label-cyan label" x-text="`SCORE HISTORY — ${ticker.toUpperCase()} · ${scoreHistory.length} snapshots`"></div>
    <div class="text-xs num font-mono" style="color: var(--text-3)" x-show="scoreHistory.length"
         x-text="`${scoreHistory[0]?.d} → ${scoreHistory[scoreHistory.length-1]?.d}`"></div>
  </div>
  <div class="relative">
    <svg x-ref="scoreChart" viewBox="0 0 1000 240" class="w-full"
         style="height: 240px; cursor: crosshair"
         @mousemove="handleChartHover($event, 'score')"
         @mouseleave="hoverIdx = null">
      <!-- gradient fill -->
      <defs>
        <linearGradient id="scoreFill" x1="0" x2="0" y1="0" y2="1">
          <stop offset="0%"  stop-color="var(--cyan)" stop-opacity="0.25"/>
          <stop offset="100%" stop-color="var(--cyan)" stop-opacity="0"/>
        </linearGradient>
      </defs>
      <!-- y-axis gridlines (every 25%) -->
      <template x-for="y in [0.25, 0.5, 0.75]" :key="y">
        <line :x1="0" :x2="1000" :y1="y * 240" :y2="y * 240" stroke="rgba(255,255,255,0.05)" stroke-width="1"/>
      </template>
      <!-- area + line -->
      <path :d="scorePathFilled" fill="url(#scoreFill)"/>
      <path :d="scorePath" stroke="var(--cyan)" stroke-width="1.5" fill="none"/>
      <!-- crosshair -->
      <g x-show="hoverIdx !== null">
        <line :x1="hoverX" :x2="hoverX" :y1="0" :y2="240" stroke="rgba(255,255,255,0.20)" stroke-dasharray="2,3" stroke-width="1"/>
        <circle :cx="hoverX" :cy="hoverY" r="3.5" fill="var(--cyan)" stroke="var(--bg)" stroke-width="2"/>
      </g>
    </svg>
    <!-- Tooltip card pinned to crosshair -->
    <div x-show="hoverIdx !== null" x-cloak class="absolute pointer-events-none rounded-md px-3 py-2 text-xs"
         :style="`left: ${tipLeft}px; top: 8px; background: var(--surface-2); border: 1px solid var(--border-2)`">
      <div class="num font-mono" x-text="scoreHistory[hoverIdx]?.d"></div>
      <div class="num font-mono text-sm" style="color: var(--cyan)" x-text="`Score: ${scoreHistory[hoverIdx]?.s}`"></div>
      <div class="num font-mono text-[10px]" style="color: var(--text-3)"
           x-text="hoverIdx > 0 ? deltaPctText(scoreHistory[hoverIdx].s, scoreHistory[hoverIdx-1].s) : '—'"></div>
    </div>
  </div>
</div>
```

Component state and methods:

```javascript
hoverIdx: null, hoverX: 0, hoverY: 0, tipLeft: 0,

handleChartHover(e, kind) {
  const svg = e.currentTarget;
  const rect = svg.getBoundingClientRect();
  const pxX = ((e.clientX - rect.left) / rect.width) * 1000;   // viewBox x coord
  const series = kind === 'score' ? this.scoreHistory : this.rankHistoryPoints(kind);
  if (!series.length) return;
  const stepX = 1000 / Math.max(1, series.length - 1);
  const idx = Math.max(0, Math.min(series.length - 1, Math.round(pxX / stepX)));
  this.hoverIdx = idx;
  this.hoverX = idx * stepX;
  this.hoverY = this._yForValue(series, idx);
  // Position tip in DOM pixels, clamped to chart
  this.tipLeft = Math.max(8, Math.min(rect.width - 180, (idx * stepX / 1000) * rect.width + 12));
},

deltaPctText(now, then) {
  if (!then) return '—';
  const pct = ((now / then) - 1) * 100;
  return `${pct >= 0 ? '+' : ''}${pct.toFixed(1)}% vs prev`;
},
```

**2. Add a Rank History chart below the Score History chart.**

The user explicitly asked for "I want to see how the rank moves up and down over time." Use `docs/data/holdings_history.json` (produced by Phase 2). For the active ticker, one line per ETF showing rank over time. Y-axis is **inverted** so rank 1 is at the top, rank 100 at the bottom — visually intuitive (up = better).

```html
<div class="rounded-lg border mt-4" style="background: var(--surface); border-color: var(--border)">
  <div class="px-3 py-2 flex items-center justify-between" style="border-bottom: 1px solid var(--border)">
    <div class="label-cyan label">RANK HISTORY — across ETFs holding this name</div>
    <div class="text-xs" style="color: var(--text-3)">lower = better</div>
  </div>
  <div class="relative px-2 py-2">
    <svg viewBox="0 0 1000 220" class="w-full" style="height: 220px; cursor: crosshair"
         @mousemove="handleChartHover($event, 'rank')" @mouseleave="hoverIdx = null">
      <!-- One <path> per ETF, color-coded by tier -->
      <template x-for="line in rankLines" :key="line.etf">
        <g>
          <path :d="line.path" :stroke="line.color" stroke-width="1.5" fill="none" stroke-linecap="round"/>
          <!-- ETF label at end of line -->
          <text :x="line.endX + 4" :y="line.endY + 3" :fill="line.color"
                style="font-family: ui-monospace; font-size: 10px"
                x-text="line.etf"></text>
        </g>
      </template>
    </svg>
  </div>
</div>
```

Computed properties:

```javascript
get rankLines() {
  // For each ETF in holdings_history[ticker], produce a polyline path with inverted Y
  const hh = this.holdingsHistory[this.ticker] || {};
  const allRanks = Object.values(hh).flat().map(p => p.r);
  const maxRank = Math.max(...allRanks, 10);   // floor at 10 for sensible scale
  const lines = [];
  for (const etf of Object.keys(hh).sort()) {
    const series = hh[etf];
    if (!series.length) continue;
    const stepX = 1000 / Math.max(1, series.length - 1);
    const tier = this.tierForETF(etf);
    const color = this.tierLineColor(tier);
    const pts = series.map((p, i) => {
      const x = i * stepX;
      const y = ((p.r - 1) / (maxRank - 1)) * 200 + 10;   // inverted: rank 1 at top
      return { x, y };
    });
    const path = pts.map((p, i) => `${i === 0 ? 'M' : 'L'}${p.x.toFixed(1)},${p.y.toFixed(1)}`).join(' ');
    lines.push({
      etf, path, color,
      endX: pts[pts.length - 1].x,
      endY: pts[pts.length - 1].y,
    });
  }
  return lines;
},

tierLineColor(tier) {
  return {
    Scout: '#c084fc', Quant: '#a5b4fc', Quality: '#5eead4',
    Trend: '#fcd34d', Blob: '#a1a1aa',
  }[tier] || '#a1a1aa';
},
```

#### DoD — B2
- Hovering the score chart on `stock.html?t=SNDK` shows a dashed vertical line, a cyan dot on the line, and a tooltip card showing date, exact score, and Δ% vs previous snapshot.
- Tooltip card stays inside the chart bounds (no horizontal clipping).
- The new Rank History chart renders one polyline per ETF that holds the stock, with tier-colored lines and inverted Y-axis (rank 1 at the top).
- The ETF label appears at the right end of each line.

---

### B3. Multi-level sorting (Excel-style)

#### What's wrong

The leaderboard sorts by clicking a column header. There's no way to say "sort by Score desc, then by ETF Count desc, then by Day Δ% desc" — which is exactly the kind of multi-key sort the user does in Excel.

#### Required: sort dialog

Add a "Sort..." button to the filter bar (next to the existing chips):

```html
<button @click="sortDialog = true" class="chip border px-2 py-1 hover:border-cyan-500/30"
        style="border-color: var(--border-2)"
        x-tooltip="'Multi-level sort. Click to configure up to 3 levels.'">
  Sort ▾
  <span class="ml-1 num font-mono opacity-60" x-show="sortLevels.length > 1"
        x-text="`(${sortLevels.length})`"></span>
</button>
```

Dialog (modal-ish positioning, click-outside-to-close):

```html
<div x-show="sortDialog" x-cloak @click="sortDialog = false"
     class="fixed inset-0 z-50 flex items-start justify-center pt-16 backdrop-blur-sm"
     style="background: rgba(0,0,0,0.5)">
  <div @click.stop class="rounded-lg border p-4 max-w-md w-full mx-4"
       style="background: var(--surface); border-color: var(--border-2)">
    <div class="flex items-center justify-between mb-3">
      <h3 class="text-sm font-medium" style="color: var(--cyan)">MULTI-LEVEL SORT</h3>
      <button @click="sortDialog = false" class="text-zinc-500 hover:text-zinc-300">✕</button>
    </div>
    <template x-for="(level, i) in sortLevels" :key="i">
      <div class="flex items-center gap-2 mb-2 text-xs">
        <span class="label w-12" x-text="i === 0 ? 'Sort by' : 'Then by'"></span>
        <select x-model="level.key" class="flex-1 bg-transparent border rounded px-2 py-1" style="border-color: var(--border-2)">
          <template x-for="col in sortableCols" :key="col.key">
            <option :value="col.key" x-text="col.label" style="background: #000"></option>
          </template>
        </select>
        <select x-model="level.dir" class="bg-transparent border rounded px-2 py-1" style="border-color: var(--border-2)">
          <option value="desc" style="background: #000">↓ desc</option>
          <option value="asc"  style="background: #000">↑ asc</option>
        </select>
        <button @click="sortLevels.splice(i, 1)" x-show="sortLevels.length > 1"
                class="text-zinc-500 hover:text-rose-400">−</button>
      </div>
    </template>
    <button @click="sortLevels.length < 3 && sortLevels.push({key: 'final_score', dir: 'desc'})"
            x-show="sortLevels.length < 3"
            class="text-xs chip border px-2 py-1 mt-2" style="border-color: var(--border-2)">+ add level</button>
    <div class="flex justify-end gap-2 mt-4 pt-3" style="border-top: 1px solid var(--border)">
      <button @click="sortLevels = [{key:'leaderboard_rank', dir:'asc'}]; sortDialog = false"
              class="chip border px-3 py-1" style="border-color: var(--border-2)">reset</button>
      <button @click="sortDialog = false"
              class="chip border px-3 py-1" style="border-color: rgba(34,211,238,0.4); color: var(--cyan)">apply</button>
    </div>
  </div>
</div>
```

State:

```javascript
sortDialog: false,
sortLevels: [{ key: 'leaderboard_rank', dir: 'asc' }],
sortableCols: [
  { key: 'leaderboard_rank',  label: '# (default rank)' },
  { key: 'ticker',            label: 'Ticker' },
  { key: 'company',           label: 'Company' },
  { key: 'final_score',       label: 'Score' },
  { key: 'score_delta_pct',   label: 'Score Δ% (active period)' },
  { key: 'etf_count',         label: 'ETF count' },
  { key: 'total_weight',      label: 'Total weight' },
  { key: 'score_streak',      label: 'Score streak' },
  { key: 'hc_streak',         label: 'HC streak' },
  { key: 'score_percentile',  label: 'Percentile (level)' },
],
```

Replace the existing single-key sort in the `filtered` getter:

```javascript
get filtered() {
  // ...existing filters: flag, newOnly, tier, q...
  return [...r].sort((a, b) => {
    for (const lvl of this.sortLevels) {
      const av = a[lvl.key], bv = b[lvl.key];
      const dir = lvl.dir === 'asc' ? 1 : -1;
      if (av == null && bv == null) continue;
      if (av == null) return 1;
      if (bv == null) return -1;
      let cmp;
      if (typeof av === 'number' && typeof bv === 'number') cmp = av - bv;
      else cmp = String(av).localeCompare(String(bv));
      if (cmp !== 0) return cmp * dir;
    }
    return 0;
  });
}
```

Column-header clicks still work — but they now **replace** the top sort level:

```javascript
sort(k) {
  const top = this.sortLevels[0];
  if (top.key === k) top.dir = top.dir === 'asc' ? 'desc' : 'asc';
  else this.sortLevels[0] = { key: k, dir: (k === 'leaderboard_rank') ? 'asc' : 'desc' };
  this.page = 1;
}
```

#### DoD — B3
- A "Sort ▾" chip on the filter bar opens a dialog with up to 3 sort levels.
- Adding "Score desc, then ETF count desc, then Score Δ% desc" produces a leaderboard where ties on score are broken by ETF count, then by Δ%.
- Clicking the Score column header replaces level 1 and toggles direction; level 2 and 3 are preserved.
- A "(2)" counter on the Sort button indicates multi-level state.

---

### B4. Extended period selector — 60d, 90d, YTD

#### What's wrong

The period chips are `1d / 7d / 14d / 30d / custom`. The user wants longer ranges to see medium-term conviction trends.

#### Required changes

**1. `config.yaml`** — extend `delta_periods_days`:

```yaml
history:
  delta_periods_days: [1, 7, 14, 30, 60, 90]   # add 60, 90
  leaderboard_lookback_days: 120                # bump to cover 90d period
  changelog_top_n: 15
```

**2. `predator/build.py`** — already iterates `cfg.history.delta_periods_days`. No code change needed; just emits `rank_delta_60d`, `weight_flow_60d`, `rank_delta_90d`, `weight_flow_90d` automatically.

Also add a **YTD** computation. YTD is special (variable length, depends on calendar). In `build.py` after the existing deltas loop:

```python
# YTD delta — compare latest to closest snapshot on/after Jan 1 of current year
latest_date = pd.to_datetime(raw["Holdings_As_Of"]).max()
ytd_start = pd.Timestamp(year=latest_date.year, month=1, day=1)
days_since_ytd = (latest_date - ytd_start).days
if days_since_ytd > 0:
    ytd_deltas = compute_rank_deltas(raw, cfg, lookback_days=days_since_ytd)
    deltas_by_period['YTD'] = ytd_deltas
    # in holdings_latest merge, rename to *_ytd suffix
```

For the leaderboard's `score_delta_pct` field, compute one per period using `historical[date_N_days_ago]` from `history.py`. Currently only one is computed (the default 7d). Loop over `[1, 7, 14, 30, 60, 90, 'YTD']`, store as a dict per row:

```python
# In build.py, after streaks_and_deltas:
score_deltas_by_period = {}
for n in [1, 7, 14, 30, 60, 90]:
    target_date = latest_date - pd.Timedelta(days=n)
    closest = min(historical.keys(), key=lambda d: abs((d - target_date).days))
    past_lb = historical[closest].set_index('ticker')['final_score']
    today_lb = leaderboard.set_index('ticker')['final_score']
    delta_pct = (today_lb / past_lb - 1).fillna(0)
    score_deltas_by_period[n] = delta_pct.to_dict()
# YTD
ytd_closest = min(historical.keys(), key=lambda d: abs((d - ytd_start).days))
score_deltas_by_period['YTD'] = ((leaderboard.set_index('ticker')['final_score'] /
                                   historical[ytd_closest].set_index('ticker')['final_score']) - 1).fillna(0).to_dict()

# Attach to leaderboard records
for r in lb_records:
    r['score_deltas_by_period'] = {str(p): round(score_deltas_by_period[p].get(r['ticker'], 0), 4)
                                    for p in [1, 7, 14, 30, 60, 90, 'YTD']}
```

**3. `docs/index.html`** — add chips:

```html
<template x-for="p in [1, 7, 14, 30, 60, 90, 'YTD']" :key="p">
  <button @click="period = p"
          :class="period === p ? 'border-cyan-500/40 text-cyan-300' : ''"
          class="chip border px-2 py-1 transition hover:border-cyan-500/30"
          style="border-color: var(--border-2)"
          x-tooltip="`Score change vs ${periodLabel(p)}`"
          x-text="p === 'YTD' ? 'YTD' : `${p}d`"></button>
</template>
```

And in the `paged` rendering, change the `Day Δ%` column header to dynamic:

```html
<th @click="sort('score_delta_pct')" :class="sortClass('score_delta_pct')"
    x-text="`SC.Δ% (${period === 'YTD' ? 'YTD' : period + 'd'})`"></th>
```

The cell reads from the per-period map:

```html
<td x-text="fmtPct(row.score_deltas_by_period?.[String(period)])"></td>
```

#### DoD — B4
- 60d, 90d, YTD chips appear in the period selector.
- Clicking 60d updates the `SC.Δ%` column header to "SC.Δ% (60d)" and every row recalculates.
- YTD recompute uses Jan 1 as the start date for the current year.
- Tooltips on chips describe the comparison reference date.

---

### B5. Pie chart for ETF holdings

#### What's wrong

The user's Excel `QQQM` sheet has a Nasdaq-100 pie chart showing position weights. The site's ETFs tab shows holdings as a table only.

#### Required: SVG donut chart in the ETFs tab

In `docs/index.html`, inside the ETF detail panel (right side, where the table currently is), add a chart above the table:

```html
<div class="px-3 py-3" style="border-bottom: 1px solid var(--border)">
  <div class="flex flex-col md:flex-row gap-4">
    <!-- Donut chart -->
    <div class="flex-shrink-0">
      <svg viewBox="0 0 200 200" style="width: 200px; height: 200px">
        <template x-for="(slice, i) in donutSlices" :key="slice.ticker">
          <path :d="slice.path" :fill="slice.color"
                @mouseenter="hoveredSlice = i" @mouseleave="hoveredSlice = null"
                x-tooltip="`<b>${slice.ticker}</b> · ${slice.name}<br>Weight: ${(slice.weight*100).toFixed(2)}%<br>Rank #${slice.rank}`"
                style="cursor: pointer; transition: opacity 0.15s"
                :style="hoveredSlice !== null && hoveredSlice !== i ? 'opacity: 0.3' : ''"/>
        </template>
        <!-- Inner ring (donut hole) -->
        <circle cx="100" cy="100" r="55" fill="var(--surface)"/>
        <!-- Center label -->
        <text x="100" y="96" text-anchor="middle" style="font-family: ui-monospace; font-size: 18px; fill: var(--cyan)" x-text="activeETF"></text>
        <text x="100" y="112" text-anchor="middle" style="font-family: ui-monospace; font-size: 10px; fill: var(--text-3)" x-text="`${etfHoldings.length} names`"></text>
      </svg>
    </div>
    <!-- Top 10 legend -->
    <div class="flex-1 min-w-0">
      <div class="label mb-2">TOP 10 BY WEIGHT</div>
      <template x-for="(slice, i) in donutSlices.slice(0, 10)" :key="slice.ticker">
        <div class="flex items-center gap-2 text-xs py-1 hover:bg-white/3 transition"
             @mouseenter="hoveredSlice = i" @mouseleave="hoveredSlice = null">
          <span class="w-2 h-2 rounded-sm flex-shrink-0" :style="`background: ${slice.color}`"></span>
          <span class="font-mono font-medium w-16" x-text="slice.ticker"></span>
          <span class="flex-1 truncate" style="color: var(--text-2)" x-text="slice.name"></span>
          <span class="num font-mono" x-text="`${(slice.weight*100).toFixed(2)}%`"></span>
        </div>
      </template>
      <div class="text-xs mt-2" style="color: var(--text-3)" x-show="donutSlices.length > 10"
           x-text="`+ ${donutSlices.length - 10} more names (${(remainingWeight*100).toFixed(1)}%)`"></div>
    </div>
  </div>
</div>
```

Computed properties for donut math:

```javascript
hoveredSlice: null,

get donutSlices() {
  const holdings = this.etfHoldings.filter(h => h.weight > 0).slice(0, 50);  // cap at 50 visible slices
  const total = holdings.reduce((s, h) => s + h.weight, 0) || 1;
  const slices = [];
  let cumAngle = -Math.PI / 2;   // start at top
  const palette = this.donutPalette();
  for (let i = 0; i < holdings.length; i++) {
    const h = holdings[i];
    const frac = h.weight / total;
    const angle = frac * 2 * Math.PI;
    const start = cumAngle;
    const end = cumAngle + angle;
    const large = angle > Math.PI ? 1 : 0;
    const x1 = 100 + 90 * Math.cos(start), y1 = 100 + 90 * Math.sin(start);
    const x2 = 100 + 90 * Math.cos(end),   y2 = 100 + 90 * Math.sin(end);
    slices.push({
      ticker: h.ticker, name: h.name, weight: h.weight, rank: h.rank,
      path: `M100,100 L${x1.toFixed(2)},${y1.toFixed(2)} A90,90 0 ${large} 1 ${x2.toFixed(2)},${y2.toFixed(2)} Z`,
      color: palette[i % palette.length],
    });
    cumAngle = end;
  }
  return slices;
},

get remainingWeight() {
  return this.etfHoldings.slice(10).reduce((s, h) => s + h.weight, 0);
},

donutPalette() {
  // Distinct, dark-bg-friendly colors. Avoid pure red/green to keep flag colors meaningful.
  return ['#22d3ee', '#a5b4fc', '#c084fc', '#fcd34d', '#5eead4', '#fb923c', '#f472b6',
          '#60a5fa', '#34d399', '#a78bfa', '#f59e0b', '#06b6d4', '#84cc16', '#ec4899',
          '#0ea5e9', '#10b981', '#facc15', '#8b5cf6', '#14b8a6', '#f97316'];
},
```

#### DoD — B5
- Switching to the ETFs tab and clicking any ETF (default QQQM) renders a 200×200 donut chart with the top-50 holdings as slices.
- Hovering a slice highlights it (other slices fade to 30% opacity) and shows a tooltip with ticker, name, weight, and rank.
- A top-10 legend appears to the right of the donut with color-matched swatches.
- A "+ N more names" footer shows total weight outside the top 10.

---

### B6. Clarify the "FLAGS" column on stock detail page

#### What's wrong

The user reports: "Clarification on the 'Flags' column in Holdings — I need to understand what it represents."

Looking at the screenshot of `stock.html?t=SNDK`, the per-ETF table has columns: ETF / TIER / RANK / 7D Δ / WEIGHT / FLOW / SCORE / FLAGS. For SNDK, the FLAGS column is empty for every ETF because nothing about SNDK is `NEW` right now. The user can't tell what the column is for.

#### Required changes (in `docs/stock.html`)

1. **Rename "FLAGS" to "STATUS"** in the column header.
2. Add an `x-tooltip` to the header explaining: `<b>NEW</b> = ticker first appeared in this ETF within the last 14 days. <br><b>HC</b> = this ticker is held by 4+ ETFs overall (cross-ETF conviction).<br><b>—</b> = no special flag.`
3. Always render at least an em-dash when the cell would be empty so the column doesn't feel broken:

```html
<td class="py-1.5">
  <span x-show="h.is_new" class="chip" style="..." x-tooltip="'First appearance in this ETF within the last 14 days'">NEW</span>
  <span x-show="!h.is_new" class="text-xs" style="color: var(--text-3)">—</span>
</td>
```

4. Add an HC chip to per-ETF rows when the ticker is HIGH_CONVICTION overall, so the user sees that signal from inside the ETF row too:

```html
<span x-show="leaderboardRow?.flag === 'HIGH_CONVICTION'" class="chip"
      style="background: rgba(52,211,153,0.10); color: var(--up); border: 1px solid rgba(52,211,153,0.25)"
      x-tooltip="`Held by ${leaderboardRow.etf_count} ETFs · HC streak ${leaderboardRow.hc_streak || 0} days`">HC</span>
```

Where `leaderboardRow` is the matching ticker's record from `leaderboard.json`.

#### DoD — B6
- The column header reads "STATUS" with a tooltip on hover explaining NEW, HC, and em-dash meanings.
- Every cell renders either a chip (NEW / HC) or an em-dash — no empty cells.
- The HC chip with hover-tooltip showing `etf_count` and `hc_streak` appears on every ETF row of an HC-flagged stock.

---

## Overall Definition of Done

1. `python -m pytest tests/ -v` — at least the 19 prior tests stay green, plus the 2 new Part A tests.
2. After `python -m predator.build`, `leaderboard.json` matches the Excel reference within ±2% for `6857.JP`, `3750.HK`, `ENR.GR`, `285A.JP`, and the Japanese IMOM names (`8058 JP`, `8031 JP`, `5020 JP`).
3. All eight enhancements function on the live site at `https://yieldchaser.github.io/etf-data/`.
4. Lighthouse performance score on `index.html` and `stock.html` stays ≥ 85 (the new charts and tooltips shouldn't regress paint times noticeably).
5. Single commit per enhancement, prefixed: `feat(b1): ...`, `feat(b2): ...`, etc. Part A goes first as `fix(scoring): per-ETF point overrides for FPXI=60, IMOM=60 to match Excel`.

## What you must NOT do

- Do not add a JS bundler. CDN Tailwind + Alpine.js only.
- Do not change the sanitizer behavior or any other scoring constants.
- Do not regress the existing period selector (1d/7d/14d/30d/custom). Add to it; don't replace it.
- Do not remove the column-header click-to-sort behavior — make it coexist with multi-level sort.
- Do not pull data via runtime API calls. Everything still comes from the pre-built `docs/data/*.json` artifacts.
