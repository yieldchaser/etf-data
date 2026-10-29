# Conviction Labs

> **Automated ETF Holdings Intelligence Platform** — tracks daily holdings across 30 Smart-Beta ETFs, detects early institutional accumulation signals, and delivers a full-stack analytical dashboard with 150+ years of cross-asset market history.

**Live Dashboard:** https://yieldchaser.github.io/etf-data/

---

## Table of Contents

1. [System Overview](#system-overview)
2. [Architecture](#architecture)
3. [Component 1 — Data Ingestion](#component-1--data-ingestion)
4. [Component 2 — Scoring Engine](#component-2--scoring-engine)
5. [Component 3 — Live Dashboard](#component-3--live-dashboard)
6. [Component 4 — Extended Scraper Bridge](#component-4--extended-scraper-bridge)
7. [Component 5 — Markets Intelligence Platform](#component-5--markets-intelligence-platform)
8. [CI/CD Pipeline](#cicd-pipeline)
9. [Testing](#testing)
10. [Local Development](#local-development)
11. [Configuration Reference](#configuration-reference)

---

## System Overview

Conviction Labs is a fully automated financial intelligence system built around a single thesis: **institutional conviction is detectable before it becomes consensus**. Smart-Beta ETFs — momentum, quality, value, spinoff — are run by systematic strategies that rebalance on rules. When multiple independent strategies simultaneously accumulate the same name, that convergence is a signal.

The system operates as five tightly integrated components:

| Component | Role |
|-----------|------|
| **Data Ingestion** | Scrapes 30 ETF issuers daily, normalises holdings, archives to CSV |
| **Scoring Engine** | Multi-factor algorithm: tier weights × rank multipliers × new-entrant bonuses |
| **Live Dashboard** | GitHub Pages SPA — leaderboard, motion signals (burst/crater), structural analytics |
| **Extended Scraper Bridge** | Fault-isolated subprocess for 8 ETFs requiring Playwright/PDF/XLS ingestion |
| **Markets Intelligence Platform** | 150+ years of cross-asset return data, 10-tab analytical dashboard |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│  GitHub Actions — Daily ETF Scrape (02:00/14:00/22:00 UTC weekdays) │
│                                                                     │
│  scraper.py (22 primary ETFs)                                       │
│  └── scripts/etf_holdings_scraper_v42.py (8 extended ETFs)          │
│       └── Bridge: clean → dedupe → write                            │
│                                                                     │
│  Output: data/all_history.csv  (append-only, ~100k rows)            │
└──────────────────────────┬──────────────────────────────────────────┘
                           │ triggers
┌──────────────────────────▼──────────────────────────────────────────┐
│  GitHub Actions — Build Site                                        │
│                                                                     │
│  conviction/build.py                                                │
│  ├── Sanitizer (blocked tickers, name patterns, ticker renames)     │
│  ├── Scoring (tier weights × rank mult × new bonus)                 │
│  ├── Temporal analytics (streaks, percentiles, deltas 1/7/14/30/60/90d + YTD) │
│  ├── Motion signals (rank trajectory, burst/crater, NEW, Pre-HC/Stealth-Buy) │
│  ├── Structural signals (concentration, tier breadth, quality Δ)    │
│  ├── Flow aggregation (sector + country, Unknown excluded)          │
│  └── ETF overlap matrix (30×30 Jaccard)                             │
│                                                                     │
│  conviction/markets_history + ingest_markets_xl.py + vol_history.py │
│  └── docs/data/market_returns.json (35 assets; count = len(assets))         │
│                                                                     │
│  Output: docs/data/*.json → GitHub Pages                            │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Component 1 — Data Ingestion

### What it does

`scraper.py` runs on the scheduled GitHub Actions cron (three weekday windows and one weekend window) and scrapes official ETF issuer websites for current holdings. Each run produces a dated snapshot and appends to the durable Parquet history store.

### ETF Universe (30 ETFs)

The ETF count is derived from `config.yaml::etfs[]` — that file is the source of truth.

| Tier | ETFs | Signal |
|------|------|--------|
| **Scout** (40 pts) | CSD, FPX, FPXI | Spinoffs & IPOs — structural market inefficiencies |
| **Quant** (40 pts) | QMOM, IMOM, XMMO, XSMO, PIE, PDP, DWAS, EEMO, PIZ, JHMM, JHEM, JHSC, MFEM, JOET | Factor-based momentum (US + International) |
| **Quality** (30 pts) | COWZ, CALF, SPHQ, IVAL, QVAL, VLUE, AVSC, GRIN | Free cash flow, value, profitability |
| **Trend** (10 pts) | SPMO, SPHB, RPG | Broad momentum confirmation |
| **Core** (2 pts) | XLG, QQQM | Mega-cap benchmarks |

Every fund earns its tier's points. International funds (FPXI, IMOM, EEMO, PIZ, PIE, JHEM, MFEM, IVAL, GRIN) are marked `region: intl` in `config.yaml`; regional levelling is done by the apex universe multiplier. The former 60-point international exception was retired in v4 (2026-10) because it duplicated that job — see [docs/SCORING_V4_STUDY.md](docs/SCORING_V4_STUDY.md).

### Data note — universe expansion (May 2026)

The tracked universe grew in stages: **11 → 16** in February 2026, stable at 16 through mid-May, then **21** (May 18) → **23** (May 21) → **29** (May 22) → **30** by late May. Because absolute ranks are universe-relative, each jump mechanically depresses rank numbers — rank deltas that span late May mix two different universes. Scores are additive per holding and less affected, but breadth-dependent terms shift; the §28 maturity gate suppresses NEW-flag floods from freshly added ETFs. Builds emit `metadata.universe_expansion` (largest jump + per-date fund counts) so charts can annotate the break — full story on the [methodology page](https://yieldchaser.github.io/etf-data/methodology.html#data-notes).

### Scraper Architecture

- **Selenium + ChromeDriver** — JS-heavy sites (Pacer, First Trust). Lazy-imported so the module can be used without Selenium installed (tests and build CI don't need it).
- **curl_cffi** — TLS fingerprint spoofing for API endpoints that block standard requests.
- **Pandas** — data normalisation, deduplication, CSV management.
- **"Nuclear" Date Hunter** — custom regex that extracts hidden `As Of` dates from raw HTML source, handling every issuer's idiosyncratic date format.

### Data Flow

```
Issuer website
    → scraper.py (fetch + parse + normalise)
    → data/latest/{TICKER}.csv          (current snapshot per ETF)
    → data/history/YYYY/MM/DD/master_archive.csv  (dated daily snapshot, gitignored)
    → data/all_history.csv              (transient working buffer, hydrated from Parquet)
```

**History isolation architecture (2026-06):** `data/all_history.csv` is no longer committed to Git — the daily append was the single biggest driver of `.git` growth (~11 MB blob per commit, poorly compressing). The durable history is now the **year-partitioned Parquet store** at `data/history_parquet/year=YYYY/holdings.parquet`, guarded by a SHA-256 manifest (`CHECKSUMS.json`) with an immutability contract enforced by `tests/test_parquet_immutability.py`. Each scrape run:

1. `scripts/hydrate_csv_from_parquet.py` reconstructs `data/all_history.csv` from the Parquet store (byte-identical to what used to be committed — verified by `scripts/validate_zero_loss.py`).
2. `scraper.py` runs unchanged, appending today's scrape to the CSV.
3. `scripts/migrate_to_parquet.py` folds the new rows into the Parquet store (mandatory, fail-loud — past-year partitions are immutable, current-year is append-only).
4. The local CSV is wiped so `git add data/` never stages it.

**Year-rollover zero-loss guarantee (2026-08):** around January 1, scrapes carry T−1 as-of dates in the *prior* year, which the immutable-partition guard refuses to write. Those rows are spilled to a committed sidecar (`data/all_history_refused.csv`) and the CSV-wipe step holds off while it is non-empty — no day is ever silently erased. Fold them with:

```
python scripts/migrate_to_parquet.py --source data/all_history_refused.csv --allow-historical-rewrite
```

**Ingress placeholder filter (2026-08):** `clean_dataframe` drops non-positive weights, `$`-prefixed currency lines, and `Cash&Other` aggregates at ingestion on every path — matching what the Invesco-API and v42 bridge paths always did. A table that filters to nothing returns no rows rather than recording a fake as-of day. Heuristic-path funds also declare `"weight_unit": "percent"` in `config.json`, replacing the old infer-from-data scale heuristic (one truncated page away from a silent 100× batch error).

The committed daily scrape is now a ~1-line manifest refresh + small Parquet partition delta, down from a multi-megabyte CSV blob. `conviction/build.py` reads the Parquet store directly (Parquet-first `fetch_history`), so the build never depends on the transient CSV.

Idempotency is enforced on the composite key `(ETF_Ticker, ticker, Holdings_As_Of)` — reruns within the same UTC day are safe.

### Schedule

```
cron: '0 2,14,22 * * 1-5'  # 02:00 + 14:00 + 22:00 UTC weekdays
cron: '0 14 * * 0,6'       # 14:00 UTC weekends (Invesco T+1 catch-up)
```

The 14:00 run targets the primary window after all issuers publish T+1 data. The 02:00 run catches Invesco's late-evening publish; the 22:00 run catches late publishers and serves as a retry. Weekend runs capture the Friday `effectiveDate` that is structurally missing from weekday scrapes.

### Fail-loud contract

The scraper fails loudly instead of exiting silent-success (audit P0):

- **Missing/corrupt `config.json`** → prints the cause and **exits 1**.
- **Total primary failure** (0 of N enabled ETFs scraped) → **exits 1**, evaluated *after* `run_extended_scrapers()` so a v42 collection is never skipped by a primary-only outage.
- **v42 total failure** (every extended fund returns 0 rows) → **exits 1**.
- **Giant-history read failure** → refuses to overwrite and exits non-zero instead of silently replacing 400k+ rows of history with today's rows; writes are atomic (temp + rename).
- **Partial failures stay warnings (exit 0)** — fault isolation across funds is by design, and cron redundancy covers transient blips.

Additional tripwires (warn-only): the Invesco path logs `equity rows vs reported holdings` on every response (loud warning below 50% — catches truncated/paginated responses), and the bridge warns when any `V42_ETFS` fund is missing from canonical output (a single source failing used to freeze its ETFs silently).

A green build therefore means data actually flowed.

### Stock-detail fetcher contract

`conviction/fetch_stock_details.py` (feeds the per-ticker price/description cards):

- Minor-unit quotes (`GBp`/`GBX`/`ZAc`/`ILA`) are converted at write time — prices stored in major units with an honest ISO currency code, so London names render ~100× correctly.
- All detail/coverage JSON writes are atomic with a NaN/Inf sanitiser (invalid bare `NaN` tokens cannot reach parsers).
- `coverage_state.json` updates run under an exclusive lockfile with a three-way merge, so concurrent builds can't lose coverage batches.
- If **zero** tickers resolve in a run, the step exits non-zero (previously exit 0 with green CI while every detail file went stale).

---

## Component 2 — Scoring Engine

### Score Formula (apex mode, v4)

```
C_i      = Wovr(OW_i) × Grank(RP_i)            per-holding conviction
           OW_i = weight × n_holdings  (1 = equal weight);  Wovr = clamp(ln(1+OW)/ln(5), 0, 1.5)
           RP_i = rank percentile in the fund;              Grank = 0.35 + 0.65 × RP
Single_i = tier_points_i × C_i^κ               κ = 2 — filler positions can't add up to a high score
B        = Σ Single_i over holding ETFs        Jaccard overlap-discounted across similar funds
Score    = B × M_conc × M_breadth × M_universe
```

| Multiplier | Range | Role |
|---|---|---|
| `M_conc` | 0.60–1.00 | Concentration guard — 1/HHI effective fund count; single-fund bets dampened, never hidden |
| `M_breadth` | 1.00–1.35 | Strategy dispersal — entropy of score across the 5 tiers (the only breadth reward) |
| `M_universe` | 0.80–1.25 | Opportunity normalization — levels international names (the only regional levelling) |

`final_score` / `leaderboard_rank` is the single canonical score and rank (one decimal). `apex_score` / `apex_rank` remain in the payload as aliases. The score-velocity diagnostics (`velocity_z` / `ignition_z`) were removed with the APEX kicker.

### Tier Points

| Tier | Points |
|------|--------|
| Scout | 40 |
| Quant | 40 |
| Quality | 30 |
| Trend | 10 |
| Core | 2 |

### What v4 changed (2026-10)

A walk-forward replay of the real engine over every snapshot (Feb–Oct 2026) drove these changes. Full evidence and every rejected alternative are in [docs/SCORING_V4_STUDY.md](docs/SCORING_V4_STUDY.md); reproduce with `scripts/scoring_study.py`.

- **Added κ = 2 convexity.** It replaces the `m_conv` factor and the conviction gate. Both averaged conviction, applied it twice, and made the score non-monotone: an extra low-conviction holder could lower a name's score.
- **Removed the tier-synergy bonus in apex mode.** It stacked on `M_breadth`, giving up to 2.16× for breadth alone.
- **Removed the 60-point international exception.** It stacked on `M_universe`.
- **Retired the APEX momentum kicker from the ranking.** Its effect on forward-return IC was below 0.001.
- **Added name-verified security identity resolution** (sanitizer):
  - splits cross-market ticker collisions (`MTN` was Vail Resorts *and* MTN Group on one fake 4-fund HC row);
  - merges exchange-suffix fragments (`6857` / `6857.JP`);
  - adds curated share-class renames (`005935→005930`, `PETR3→PETR4`, …).
- **Added a 30-day staleness guard** for dead ETF feeds.

Result (30-ETF universe):

| | Before v4 | After v4 |
|---|---|---|
| Top-50 breadth-filler share | 17.6% | 4.8% |
| Forward fund-accumulation IC | −0.009 | +0.018 |
| Top-50 30-day exit rate | 18.8% | 14.4% |
| Forward-return IC | unchanged | unchanged |

Forward-return IC tracks the momentum regime either way; the score is not a return forecast.

**Motion.**

- **Score and rank changes follow the momentum regime.** They behave the same before and after v4.
- **Change in the number of holding funds is the most consistent forward signal.** Top-200 names that lost a fund underperformed peers by about 5.5–6% over the next 20 days in both the Feb–May and May–Oct regimes. Use the **Funds ↑ / Funds ↓** chips on the leaderboard to screen on it.

Legacy (`weight% × 100 × tier_points × rank_multiplier + new bonus`) and conviction (§24, linear) modes remain selectable via `scoring_mode` for comparison.

### Output Flags

| Flag | Condition | Interpretation |
|------|-----------|---------------|
| **HIGH_CONVICTION** | ≥ 4 holders, ≥ 2 non-benchmark holders at conviction ≥ 0.6, ≥ 2 non-benchmark strategy tiers (Core excluded) | Broad, convinced, independent consensus (~20–25 names; raw "≥ 4 holders" flagged ~200) |
| **CONCENTRATED** | One fund supplies ≥ 85% of linear conviction mass (Σ points × C), or a lone fund holds it at ≥ 4× equal-weight | Single-fund story — a risk descriptor, not a buy signal (no consistent return edge in the 2026-10 audit) |
| **NEW** | First appearance within the 14-day lookback; suppressed for ETFs younger than the 30-day maturity baseline | Short-horizon entrant (1.38× accumulation lift ≤ 7d, 0.65× by 30d) |

Per-name motion signals `BURST` / `CRATER`, their price-driven grade (`SURGE` / `SLIDE` in the data), and the screening chips (Funds ↓, Funds ↑, Pre-HC, Stealth, Quality+) are documented under [Analytical Signals](#analytical-signals). `SPECULATIVE_BETA` was retired in the 2026-10 signal audit (no edge in either regime).

### Sanitizer

Before scoring, the pipeline filters:
- **Blocked tickers**: currency placeholders (`$USD`, `$EUR`, `$JPY`, `$AED`, `$CHF`, `$HKD`, `$SEK`, `$ZAR`, …), money market funds (`AGPXX`, `FGXXX`), cash instruments
- **Ingestion-side filtering** (2026-08): non-positive weights, `$`-prefixed currency lines, and `Cash&Other` rows are dropped at scrape time on every path — the sanitizer below is the second layer, not the only one. Legacy rows were purged once via `scripts/cleanup_holdings_rows.py`.
- **Blocked name patterns**: "Money Market", "Securities Lending"
- **Ticker normalisation**: `BRK-B → BRK.B`, `BF/B → BF.B`, `GOOG → GOOGL` (dual-class consolidation)

---

## Component 3 — Live Dashboard

**URL:** https://yieldchaser.github.io/etf-data/

Built with Tailwind CSS + Alpine.js. Zero build step — static HTML/JS served directly from `docs/`. All computation runs client-side from pre-built JSON payloads.

### Pages

| Page | Path | Description |
|------|------|-------------|
| Dashboard | `/` | Main leaderboard, ETFs tab, Changes tab, Watchlist |
| Stock Detail | `/stock.html?t=TICKER` | Per-ticker deep dive |
| Markets | `/markets.html` | Cross-asset returns intelligence |
| Backtest | `/backtest.html` | Strategy performance comparison |
| Portfolio Lab | `/sim.html` | Leveraged ETN NAV simulator |

### Dashboard Tabs

#### Leaderboard
- KPI strip: names tracked, High Conviction (with Pre-HC count), BURST and CRATER (with the date each last fired when none are live), names losing funds
- Today's signals strip: warnings (CRATER, Funds ↓) beside building names (BURST, HC entries, Pre-HC, Stealth), up to four each
- ~3,800 unique tickers ranked by the v4 conviction score (`final_score`, one decimal)
- Sortable columns: score, rank, rank delta, burst, concentration, tier breadth, ticker, company (3-way click sort toggle: desc → asc → default, with alphabetical columns and rank defaulting to ascending on first click)
- Day-over-day score deltas with honest `—` display when no comparable past data exists
- HC streaks, percentile-of-own-history progress bars
- **BURST** and **CRATER** badges with micro-breakdown tooltips
- Filter chips in three groups — Flags (HC, CONC, NEW), Signals (BURST, CRATER, Funds ↓, Funds ↑, Pre-HC, Stealth, Quality+); BURST and CRATER include their price-driven grade (dashed badge)
- Auto-generated explainer line per row: compresses tier breadth, score delta, burst state, HC streak, quality signals, concentration, NEW-entrant state into one scannable sentence
- **Expanded Row Detail Drawer**:
  - **Auto-Explainer Grid**: A frosted glassmorphic card container (`STRUCTURE`, `FLOW`, `RISK`) featuring dynamic left-border colored callout bars (`border-l-2 pl-2 border-current`) matching alert severity.
  - **Metadata Card**: Displays score breakdown, HC streak, best rank, score streak, and total weight separated by vertical dividers (`xl:border-l xl:border-white/[0.04]`).
  - **Holdings Table**: Polished layout with clean padding, soft borders, and standardized column alignments.
  - **Interactive Price Card (Right Panel)**: Includes pre-loaded price charts, capsule timeframe controls (1M to ALL), 2x2 key metrics grid (Latest Price, Avg Volatility, Period Low, Period High), a horizontal range slider track showing current position, and a horizontally expandable full-width company description.

#### ETFs Tab
- Per-ETF 50-slice SVG donut chart (20-color institutional palette)
- Tier pie chart (fixed Alpine SVG template limitation via `x-html`)
- Holdings sorted by rank, weight, rank delta, weight flow (supporting 3-way click sort toggle: desc → asc → default)
- **Official Strategy Descriptions**: Integrated investment objectives and systematic index mechanics for all 30 ETFs, cleanly parsed from research and embedded right below the detail header.
- Sidebar metadata: rebalance/reconstitution schedule from `ETF DATA.csv`
- ETF overlap heatmap: 30×30 Jaccard similarity matrix

#### Changes Tab
Everything is computed per lookback period (1d / 7d / 14d / 30d / 60d / 90d / YTD, default 30d) by `history.changelog` from the full daily leaderboards, and read as "building" on the left, "breaking" on the right:

| Row | Left | Right |
|-----|------|-------|
| Fund positions | Funds adding — gained a holding fund, now in the top 500 | Funds leaving — lost a holding fund, was in the top 500 |
| High Conviction | Entered HC | Left HC |
| Motion (in force now) | BURST | CRATER — with the date each last fired when none are live |
| Rank | Climbers — largest rank multiple, now in the top 300 (≥ 10 positions) | Fallers — largest rank multiple, was in the top 300 |
| Board | New on the board | Left the board |
| Flow (30d) | Sector flow | Country flow |

Every table shows rank then → now, funds then → now and score, with the name's strongest current signal as a badge. A summary strip at the top gives the up / down count for each row. Fund-count change leads because it is the most robust signal in the 2026-10 audit; score-% movers were dropped (dominated by near-zero scores) and raw rank deltas were replaced by rank multiples (a #3,000 → #1,000 move is noise; #300 → #120 is not).

#### Watchlist Tab
- Pin tickers with ★, persisted to localStorage
- "Since last visit" changelog: HC entries/exits, new bursts among pinned names
- Compare mode: side-by-side cards for top 4 pinned tickers

### Stock Detail (`stock.html`)

- **KPI cards**: ten, in two rows — where the name stands (score, rank, funds, convinced holders, top-fund share) and how it is moving (score Δ 1d, rank Δ 30d, funds Δ 30d, score streak, HC streak)

- **Score History**: sparkline area chart, score accumulation over time
- **Global Rank History**: inverted Y-axis line chart, O(1) pre-computed lookup
- **Per-ETF Rank History**: multi-line chart, tier-based coloring, crosshair tooltips
- **Signal Timeline**: Gantt-style multi-lane chart — per-day signal history (`flag`, `burst`, `crater`, price-driven burst / crater, quality adoption, rank, holder count) stored compactly per day in `flag_history.json`, with rank overlay, hover crosshair, duration counters
- **Score Decomposition Bar**: stacked bar showing each ETF's contribution, colored by tier
- **Tier Breadth chip**: how many distinct strategy types co-hold this name (1–5)
- **Quality+ chip**: gained a Quality-tier ETF in last 30 days (set derived from config)
- **Momentum Gauge**: ↗ accelerating / → stable / ↘ weakening
- **Lazy-loaded history**: the per-ticker history file loads first; the aggregate `score_history.json` / `holdings_history.json` payloads (~4.1 MB combined) are fetched only as a fallback for tickers whose per-ticker file is incomplete

### Analytical Signals

| Signal | Definition |
|--------|-----------|
| `BURST` | On the 3-snapshot rolling-median global rank: improved by ≥ ×1.65 (log move ≥ 0.5) over the last 10 snapshots, is now inside the top 300, is still within 15% (log) of its best rank in the window, **and** gained at least one holding fund over the same window. A one-day rank glitch cannot fire it. |
| `CRATER` | Over ~4 weeks (20 snapshots): smoothed rank worsened by ≥ ×1.35 from a starting position inside the top 300, still near its worst, **and** lost at least one holding fund over those 20 snapshots. The longer window catches funds that sell before the slide (Yageo, July 2026). The "get out" signal: −2.9% / −1.0% vs peers over 20 days in the two regimes. |
| `surge_10d` / `slide_10d` | Price-driven grade of BURST / CRATER (dashed badge on the site): smoothed rank ×1.4 better / worse over ~2 weeks in the top 300 with **no** fund change — the price move showing through fund weights (~25 / ~40 names on a typical day). Confirms a move, doesn't predict one. Not set on names already fund-backed BURST / CRATER. |
| `funds_down` (chip) | Inside the top 200 and lost a holding fund in the last 14 days — the most robust single exit warning in the audit |
| `funds_up` (chip) | Inside the top 200 and gained a holding fund over 30 days |
| `pre_hc` (chip) | Not yet HC, ≥ 3 funds, ≥ 1 non-Core conviction holder, ≥ 2 non-Core tiers, improving 7-day per-fund ranks and a rising score streak — one step from HIGH_CONVICTION |
| `stealth_buy` (chip) | A second fund joined in the last 14 days (2 holders now), inside the top 500 — the earliest form of independent confirmation |
| `quality_adopted_30d` | Gained a Quality-tier ETF in the last 30 days (Quality set derived from `config.yaml`) |
| `momentum_regime` | accelerating / rising / stable / weakening / declining — `score_streak` direction combined with the mean 7-day per-ETF rank delta |
| `etf_count_delta_30d` | Net ETF positions added/removed over 30 days; a name absent 30 days ago counts as 0 holders. Sector/country flow aggregates the same measure in `flow.json` |
| `tier_breadth` | Count of distinct strategy tiers (Scout/Quant/Quality/Trend/Core) co-holding this name |
| `concentration_score` | % of linear conviction mass from the single top ETF (100 = mono-ETF) |

BURST, CRATER and the backtest's BURST/CRATER strategies share one implementation (`conviction/history.burst_crater_panels`), so the site, the per-day signal history and the backtest cannot drift apart. Thresholds live under `motion:` and `signals:` in `config.yaml`.

### Signal evaluation (2026-10)

Every signal was replayed on each snapshot with the site's definition and scored on weekly dates by 20-day forward return vs currency peers, plus whether members went on to gain or lose a holding fund. Full tables: [`docs/SIGNALS_STUDY.md`](docs/SIGNALS_STUDY.md); reproduce with `scripts/signals_study.py` and `scripts/signal_audit.py`.

| Signal | Verdict |
|--------|---------|
| HIGH_CONVICTION | Redesigned: ≥ 4 holders was ~200 names; the conviction + independence rule is ~20–25 and loses a holder half as often |
| BURST / CRATER | Redesigned: the old 40-position range rule fired on ~580 names a day, mostly below #300, describing moves that had already happened. 20 rules tested in both regimes: only fund-confirmed rules kept their sign. The unconfirmed rank move (×1.4) is shown as BURST / CRATER's price-driven grade |
| Funds ↓ / Funds ↑ | Added — fund-count change is the most robust signal in the data |
| Pre-HC / Stealth | Redefined to the forms that carried information |
| Quality+, NEW, regime | Kept |
| CONCENTRATED | Kept as a risk descriptor only |
| SPECULATIVE_BETA, Q+Q chip, EARLY/CROWDED/ACCUM badges, ≤80% concentration chip, extreme-moves ribbon | Retired — no edge, or duplicated another signal |

Earlier retirements (2026-08: `velocity_score`, `stealth_accumulation`, `conviction_divergence`, `quality_defected_30d`) still stand.

Retired signals were removed from the engine, payloads, and UI; the full transparency log lives on the [methodology page](https://yieldchaser.github.io/etf-data/methodology.html#retired-signals).

---

## Component 4 — Extended Scraper Bridge

### Why it exists

Eight ETFs require ingestion methods that can't run in the primary Selenium loop: Playwright browser automation (JS-rendered tables), PDF parsing, XLS downloads, and cookie-bound APIs. These run as a fault-isolated subprocess.

### Ownership Manifest

```python
V42_ETFS = frozenset({"VLUE", "AVSC", "GRIN", "JHMM", "JHEM", "JHSC", "MFEM", "JOET"})
```

At startup, `scraper.py` calls `check_v42_ownership_collisions()` and logs a warning if any ticker appears in both `config.json` (primary) and `V42_ETFS` (extended). In healthy state the intersection is empty.

### Bridge Pipeline

```
scripts/etf_holdings_scraper_v42.py (subprocess, timeout=600s)
    → etf_holdings_YYYYMMDD.csv  (Canonical_CSV at repo root)
         │
         ▼
    clean_canonical_csv()
    ├── Required columns: etf, ticker, name, weight_pct, as_of_date, security_type, scrape_date
    ├── Filters: non-equity security types, currency tickers ($USD, $BRL...), invalid weights
    ├── Normalises: weight_pct strings with % suffix ("2.62%" → 0.0262)
    └── Returns: (cleaned_df, errors)
         │
         ▼
    bridge_write_all_sinks(cleaned_df)
    ├── data/all_history.csv                        (Giant History — append + dedupe)
    ├── data/latest/{TICKER}.csv                    (one snapshot per V42 ETF)
    └── data/history/YYYY/MM/DD/master_archive.csv  (dated daily snapshot — append + dedupe)
```

### Failure Isolation Contract

Three-layer isolation ensures no v42 failure can corrupt the primary pipeline output:

1. **Subprocess crash / timeout** — logged, Bridge returns. Primary sinks already written remain intact.
2. **Canonical CSV validation failure** — `clean_canonical_csv()` returns empty DataFrame + error list. `bridge_write_all_sinks()` is skipped entirely; no sink is touched.
3. **Per-sink write failure** — each ETF's `data/latest/` write and the master_archive append are wrapped individually so one ETF's I/O failure does not abort the others.

### Reuse-Existing-CSV Recovery

If `etf_holdings_YYYYMMDD.csv` already exists when the Bridge starts (partial-run recovery or same-day rerun), the subprocess is skipped and the existing canonical CSV is reused. This avoids re-incurring Playwright cost while still fanning the data into all three sinks.

---

## Component 5 — Markets Intelligence Platform

A standalone data pipeline and 10-tab analytical dashboard providing cross-asset return analytics with deep historical coverage.

### Data Pipelines

#### `conviction/markets_history.py`

Fetches monthly end-of-period (EOP) close data from FRED (primary) and yfinance (fallback for international indices). Supports incremental updates (trailing 3 months) and full refresh. Writes `docs/data/market_returns.json`.

The source registry covers 50+ candidate series; the checked-in `docs/data/market_returns.json` currently contains 35 assets, derived from its `assets` map and assembled by `conviction/markets_history.py` from Excel, FRED, and yfinance sources. The current output covers:
- **Equities**: S&P 500, DJIA, NASDAQ, NASDAQ-100, MSCI Emerging Markets, Nikkei 225, BSE Sensex, DAX, FTSE 100, Hang Seng, ASX 200, Bovespa, Shanghai Composite
- **Precious Metals**: Gold, Silver, Platinum, Palladium
- **Energy**: WTI Crude, Brent Crude, Natural Gas
- **Base Metals**: Copper, Aluminum, Nickel, Zinc, Iron Ore, Tin, Lead
- **Agriculture**: Wheat, Corn, Soybeans, Cotton, Sugar, Coffee, Cocoa, Rice

FX, rates, real-estate, and auxiliary series remain in companion metadata/auxiliary payloads and are not counted in `market_returns.json["assets"]`.

#### `conviction/ingest_markets_xl.py`

Reads `data/Mega_Markets_Historical.xlsx` (and optionally `Markets_1_.xlsx` when present) and merges its deep historical data into the `market_returns.json` payload in the canonical shape. Excel data takes priority for overlapping months, extending history back to:

| Asset | History from |
|-------|-------------|
| Gold | **1833** |
| S&P 500 | **1871** |
| Dow Jones | **1885** |
| BSE Sensex | 1979 |
| NASDAQ | 1971 |

#### `conviction/vol_history.py`

Fetches daily close for 7 CBOE volatility indices from FRED. Writes `docs/data/vol_history.json`.

| Index | FRED Series | Coverage |
|-------|-------------|---------|
| VIX | VIXCLS | 1990+ |
| GVZ (Gold VIX) | GVZCLS | 2008+ |
| OVX (Oil VIX) | OVXCLS | 2007+ |
| VXN (NASDAQ VIX) | VXNCLS | 2001+ |
| RVX (Russell VIX) | RVXCLS | 2004+ |
| VXD (DJIA VIX) | VXDCLS | 2005+ |
| VXEEM (EM VIX) | VXEEMCLS | 2011+ |

### Markets Dashboard (`/markets.html`) — 10 Tabs

#### Return Matrix
Annual returns heatmap for all assets. **Newest year on the left**, oldest on the right.

- Click any cell → monthly drilldown row with 12 colored cells
- Click any asset name → **Asset Detail Panel** (see below)
- **Currency lens**: Local / USD / INR / Gold / Silver — all returns recomputed client-side using monthly FX rates from the aux registry
- **Real returns toggle**: CPI-adjusted using CPIAUCSL (US CPI)
- **Z-score color mode**: color = how unusual this year was vs the asset's own history
- **Event annotations**: Crisis regime markers on key historical years (1973, 1987, 2008, 2020, 2022...)
- **CSV export**: downloads current matrix view with active lens applied

#### Asset Detail Panel
Opens below the matrix when you click an asset name. Two tabs:

**Annual Returns (default)**
- Year-by-year vertical bar chart — green positive, red negative
- Partial years (first/last) shown at 40% opacity
- Amber dashed CAGR reference line
- Hover any bar → bottom strip shows exact year + return % to 2dp
- Respects active currency lens — Gold priced in INR shows INR-denominated gold returns

**Price History**
- Full price history line chart (log/linear toggle)
- Stats strip: CAGR, annualised vol, best/worst year, % positive years, data since

#### Price Log
Daily price log for 23 tracked instruments with ATH, ATL, streaks, and percentile stats.

#### Yield History
US interest rate history — line chart + annual yield heatmap. Series: 3M T-Bill, 2Y, 10Y, 30Y Treasury, Fed Funds, Moody's BAA, 10Y–2Y Spread.

#### Periodic Table
Callan-style rank rotation table — assets sorted best-to-worst return for each year, colored by asset.

#### Drawdowns (Drawdown Studio)
- Underwater curves for **all assets** (not a hardcoded subset)
- Left sidebar: grouped by category, search filter, Select All / Reset
- Time range filter: All / 50Y / 30Y / 20Y / 10Y
- Chart: dark `#111` background, 35% opacity fill, bright 2px line, 0% at top, negative going down
- Hover crosshair with per-asset drawdown values
- Max Drawdown table: Peak date, Trough date, Recovery date, Months to recover, Current DD

#### Hold Lab
Rolling N-year returns — best/worst/median for each holding period.

#### Seasonality
Month-of-year average return heatmap — which months historically perform best/worst per asset.

#### Correlation
Correlation matrix + Growth of $100 log-scale chart for selected assets.

#### Volatility Intelligence
4 sub-panels:
1. Current level tiles with z-score vs own history
2. Monthly z-score heatmap (VIX regime calendar)
3. Historical line chart with lookback filter
4. Cross-asset bar chart (current vs 1Y avg)

#### Fund Flows
- Searches the generated 160-instrument local catalog by default; explicit filters expose the 24-name featured subset.
- Loads one selected static ticker file at a time, normalizes v1 and v2 payloads, and caches data by ticker and source revision.
- Uses one shared native range control, daily ETF estimated net-flow bars, a complete 20-observation rolling mean, selected-window cumulative source-reported aggregate net flow, complete-window/tie-aware percentile, and a clearly labeled prior-only z-score.
- Provides an optional two-axis price-and-flow view, an accessible selected-window data table, status definitions, and catalog alternative metadata.
- **Flow setups today** (scanner): funds whose 20-day price move and 5-day flow are both unusual for that fund, in four setups (inflows / outflows after a drop / rally). Under each column, the pooled evidence for the setup (`catalog.json` → `flow_evidence`, recomputed on every build: next-20-day return vs funds with the same price move, per era) and a verdict worded from those numbers, and each fund shows its own record (next 20 days vs its typical 20 days, episodes ≥ 20 days apart) only when it has ≥ 8 episodes. Flows are context, not a trade signal: only inflows after a drop kept a small, consistent edge (`scripts/flow_study.py`, `docs/SIGNALS_STUDY.md` §5).
- **Studio cards** describe price vs flow plainly ("Inflows near low", "Down with inflows"); the flow-spike card compares each spike's next 5 days with the fund's typical 5 days (≥ 8 spikes, ≥ 5 days apart). **Bull vs bear** nets bear-fund inflows against bull-fund inflows (`20D Net Bull` = bull − bear) and names the fund with the biggest 20-day flow.
- **Reversal pairs:** when a fund's latest 1-day flow undoes the previous day's (opposite sign, both ≥ 5× its typical daily flow, netting to within 20%), the catalog flags it (`flow_reversal`). These are source reporting errors, e.g. NVDL +$1.02B then −$0.99B. The leaderboard marks them ⚠ and ranks them as zero, and 1-day totals and the top-mover lists leave them out. The 5- and 20-day sums net them out on their own. Upstream, `update_daily_flows.py` nets these pairs before computing z, 5/20-day sums, regime and pressure (raw `usd_flow` kept). A 1-day flow of ≥ 50% of AUM is also marked ⚠ (seed money or a source error more often than a real move).
- Keeps legacy, stale, pending, empty, unavailable, loading, and error states explicit; missing observations are never rendered as zero.
- Keeps ticker, range, and optional price state in the URL and responds to browser history navigation. The browser cannot start a source refresh.
- Uses optional CDN enhancement with a local static-data fallback; the fallback removes `x-cloak`, loads the local catalog/history files, and exposes an accessible table when enhancement is unavailable.

---

## ETF Flow Data Runbook

### Source and catalog

The authoritative flow dataset is the local, read-only collection under `data/flows/`: **160 leveraged, inverse and benchmark instruments, 215,604 daily rows from 2016-01-04 through 2026-10-28**, with zero duplicate ticker-dates and zero missing required values. `scripts/build_local_flow_artifacts.py` validates those sources and deterministically regenerates every published artifact under `docs/data/flows/` (145 per-ticker JSON files, `catalog.json`, `manifest.json`); it refuses any output directory inside `data/flows/`.

`docs/data/flows/catalog.json` is the generated UI catalog (schema 1, 160 instruments of which 24 are featured) and `docs/data/flows/manifest.json` is the coverage manifest (schema 2, status `complete`, 160 files, 215,604 rows). Both are marked `network_fetch: false`. The Markets Fund Flows interface reads only these static files; the runtime fallback enforces the same 145/24 cardinality before rendering, and the browser cannot start a source refresh.

Trackinsight remains recorded as the historical provider in the metadata (`source_provider: Trackinsight`, `source_mode: historical_local`), but no runtime path performs a Trackinsight or other network request. External ingestion is disabled: `scripts/fetch_etf_flow.py` exits with code 2 in every mode unless the explicit `--unsafe-external-fetch` manual opt-in is passed, and `daily_etf_flows.yml` has no schedule — its only job prints the local-only policy and exits 1.

### Validation and rebuild

```bash
# Validate every local source (offline, no network access)
python scripts/build_local_flow_artifacts.py --validate-only

# Verify the checked-in docs artifacts against a fresh validation
python scripts/build_local_flow_artifacts.py --verify-output --output-dir docs/data/flows

# Rebuild artifacts (use a scratch output dir; build_site.yml runs the real one in CI)
python scripts/build_local_flow_artifacts.py --output-dir /tmp/flows-out
```

Validation enforces the 145-instrument universe, the 177,699-row total, required columns per individual file, aggregate and workbook agreement, and identity with `FUND_FLOW_ETFS.md`. The CI gate in `build_site.yml` runs `--verify-output` plus explicit 145-instrument / 24-featured / no-watch-tier / no-network-fetch assertions before any artifact upload.

### Legacy external ingestion (disabled)

`scripts/fetch_etf_flow.py` still contains the legacy Trackinsight scraper plus its retry, pacing, budget, manifest, and checkpoint-planning machinery (all covered by tests), but every mode is gated behind `--unsafe-external-fetch` and returns exit 2 without it. Legacy budget and request semantics: explicit ticker/endpoint/source-window/runtime budgets (defaults 3 tickers, 3 endpoint calls, 100 source windows, 900 seconds), calendar-quarter windows, one canonical fund per POST with `windows_per_request` 1–4 (default 3), 5–9 second pacing from response completion, 403/405/challenge as immediate hard stops, capped `Retry-After` handling, atomic per-ticker merges, and deterministic SHA-256 `content_sha256` manifest validation. Do not run the opt-in commands in CI:

```bash
python scripts/fetch_etf_flow.py --unsafe-external-fetch --dry-run --mode resume
```

The browser does not use the broad identity index, and the committed identity index exists only for the offline `--validate-keys` diagnostic. The UI normalizes v1 and v2 ticker payloads without changing null, zero, or available-history semantics.

---

## CI/CD Pipeline

### Workflows

#### `daily_scrape.yml` — Data Collection
```
Trigger: cron 02:00 + 14:00 + 22:00 UTC weekdays, 14:00 UTC weekends, workflow_dispatch
Runner:  ubuntu-latest   timeout-minutes: 90
Guards:  concurrency group "daily-scrape" with cancel-in-progress: false (serialized runs)
Steps:
  1. Install libraries: pandas, selenium, playwright, curl_cffi, pdfplumber, xlrd, pyarrow
  2. Install Chromium (playwright) + xvfb
  3. Hydrate transient all_history.csv from Parquet store (hydrate_csv_from_parquet.py)
  4. Run scraper: xvfb-run -a python scraper.py   (fail-loud exits — see Component 1)
  5. Update parquet archive: fold new rows into data/history_parquet/ (migrate_to_parquet.py)
  6. Wipe local transient data/all_history.csv
  7. Commit and push Parquet partition deltas + CHECKSUMS.json (only if data changed)
  8. If data changed, explicitly dispatch `build_site.yml`; no-change runs do not dispatch
```

#### `daily_etf_flows.yml` — Disabled Flow Workflow
```
Trigger: workflow_dispatch only, with a required `acknowledgement` input; no schedule and no cron.
Runner: one `disabled` job (ubuntu-latest, 5 minutes) that prints the local-only policy and exits 1.
Policy: no network request, no git push, no build call — external ingestion is disabled and the
        authoritative data is the read-only local dataset under data/flows/.
```

#### `build_site.yml` — Site Build & Deploy
```
Trigger: push to `main` on the declared site/data paths, manual `workflow_dispatch`, and reusable
         `workflow_call` (declared for external callers; the disabled flow workflow does not invoke it);
         no scraper-completion chaining trigger is used.
Runner:  ubuntu-latest   timeout-minutes: 60 (build) / 20 (deploy)
Steps:
  1. Install: pandas, pyyaml, pyarrow, pytest, hypothesis, yfinance, openpyxl, fredapi, python-dotenv
     (No selenium/curl_cffi/pdfplumber — scraper-only, not needed for build)
  2. pytest tests/ -v
  3. conviction.build  → docs/data/*.json
  4. conviction.fetch_prices  → Portfolio Lab prices (yfinance adjusted-close)
  5. conviction.fetch_stock_details  → descriptions + 2-year price history
  6. Persist stock-detail coverage  → commit-back coverage state to main
  7. conviction.ingest_markets_xl  → 1/2: Markets Excel deep-history seed (backfill only)
  8. conviction.markets_history  → 2/2: Live FRED + yfinance merge (current months, fx, cpi, rates)
  9. markets.fetch_yf + markets.fetch_fred + markets.build  → docs/data/markets.json + sim_underlyings.json
  10. conviction.vol_history --full-refresh  → docs/data/vol_history.json
  11. Verify outputs: required files exist; market_returns.json has the monthly-array contract;
      leaderboard.json and prices.json are non-empty
  12. Build local ETF flow artifacts → docs/data/flows/ (145 ticker files, catalog.json, manifest.json)
  13. Verify complete local ETF flow artifacts: --verify-output plus 145-instrument, 24-featured,
      no-watch-tier, and network_fetch=false assertions
  14. Upload Pages artifact → Deploy to GitHub Pages
```

### Key Design Decisions

- **Selenium lazy-import**: `scraper.py` wraps all selenium imports in `try/except ImportError`. The module can be imported without Selenium installed — tests and build CI don't need it, only the runtime scraper does.
- **Serialized scrapes**: `daily-scrape` uses `cancel-in-progress: false` for its scheduled and manual runs. A queued run rehydrates from the immutable Parquet store, then uses normal rebase semantics; a conflict aborts and requires a safe rerun. `build_site.yml` uses its own queued `pages` group with `cancel-in-progress: false`.
- **Build trigger paths**: the scraper explicitly dispatches `build_site.yml` after a data-changing run; the build workflow also supports its filtered `main` push, manual dispatch, and a reusable `workflow_call` for external callers. No completion-chaining trigger is used.
- **LFS checkout**: build checkout runs with `lfs: true` — `data/Mega_Markets_Historical.xlsx` lives in Git LFS, and the default pointer-file download would make the deep-history seed silently ingest a stub.
- **Timeouts everywhere**: scrape job 90 min (a hung Playwright/yfinance must not burn the 360-min default), site build 60 min, deploy 20 min.
- **Real verify gate**: the build fails unless every required payload exists, `market_returns.json` has the monthly-array contract shape, and `leaderboard.json` / `prices.json` are non-empty.
- **continue-on-error**: All market data fetch steps use `continue-on-error: true` — a FRED rate limit or yfinance outage doesn't fail the entire build. The verify gate above is what keeps a fully-empty payload from ever shipping.
- **YTD deltas stay computable year-round**: the Parquet fetch lookback is sized to `max(180, days_into_year + 45)` days so the December-31 YTD baseline never falls outside the loaded window (previously a fixed 180-day window shipped `YTD: null` for all tickers after ~June).
- **History window = the current fund universe**: per-stock histories, own-history stats, the Changes tab lookbacks and the YTD baseline start at `history.universe_start` (2026-05-22, the 30-fund universe), growing to `leaderboard_lookback_days` (400) and then rolling. In 2026 "YTD" therefore means since 2026-05-22.

---

## Testing

The pytest suite provides property-based (Hypothesis) and deterministic coverage of scoring, sanitization, the v42 bridge contract, Parquet immutability, markets engine, signal history, multi-period universe exits, short-side screening, CI configuration, flow ingestion gating, local flow artifact integrity, and Fund Flows UI contracts. The current collection contains 483 tests across 35 test files (482 passing, 1 manifest-gated skip).

```bash
python -m pytest tests/ -v
```

### Test Suite

The suite covers the full pipeline end-to-end. Key areas:

| Area | Test file(s) | Coverage |
|------|-------------|----------|
| Scoring & sanitizer | `test_scoring.py`, `test_apex_scoring.py`, `test_apex_gate.py` | Score formula, tier weights, rank multipliers, conviction layer, **sanitizer memoization correctness** |
| Temporal analytics | `test_scoring.py`, `test_signal_history.py` | Rank deltas (1/7/14/30/60/90d), burst/crater detection, motion signals, signal timeline schema |
| v42 bridge contract | `test_bridge.py`, `test_fault_isolation.py` | Property-based (Hypothesis) validation of the bridge pipeline, failure isolation |
| History isolation | `test_parquet_immutability.py` | SHA-256 manifest integrity, past-year immutability, zero-loss reconstruction, append-only contract |
| Markets engine | `test_markets_engine.py`, `test_markets_unit_conversion.py`, `test_self_living_merge.py`, `test_unit_gaps.py` | Market returns pipeline, currency conversion, partial-year merge |
| CI config | `test_ci_config.py`, `test_pipeline_automation.py` | Workflow structure invariants (e.g. `continue-on-error` policy) |
| ETF flow catalog | `test_etf_flow_catalog.py` | Local 145-instrument leveraged/inverse dataset contract for the disabled ingestion pipeline (no synthetic primary catalog, no watch tier), Trackinsight parser validation, retry/pacing, transactional merge, resumable checkpoint planning/diagnostics, rebase conflict handling, disabled-workflow and external-ingestion gating safety |
| Local flow artifacts | `test_local_flow_artifacts.py` | 145-instrument/177,699-row local dataset validation, per-file schema and SHA-256 spot-checks, catalog/manifest determinism, temp-dir builds that never touch `data/flows/`, output-path safety, UI/workflow cardinality sync, the `data/flows/` git-ignore regression guard, and the no-legacy-YAML scan |
| Fund Flows UI | `test_flow_ui.py`, `test_flow_ui_metrics.py` | Local catalog integrity (145/24), parent/child history reconciliation, shared native range, accessibility, dual-axis percentile output, v1/v2 normalization, neutral zero/null handling, prior-only z-score, and cumulative-window behavior |

The sanitizer tests in `test_scoring.py` (46 tests) exercise `cfg.sanitizer.apply()` with distinct synthetic inputs, validating blocked tickers, name patterns, ticker normalization (BRK-B → BRK.B), GOOG → GOOGL dedup, and KRX cross-listing collapse — and prove the build-time memoization cache returns byte-identical results via deep copies.

| Test | Properties Covered |
|------|-------------------|
| `test_bridge_cleans_canonical_csv_to_pipeline_schema` | Column projection, weight normalisation, row count preservation, CSV round-trip |
| `test_bridge_rejects_csv_missing_required_columns` | Required-column enforcement halts pipeline before any sink write |
| `test_bridge_excludes_currency_and_money_market_rows` | Non-equity filtering (currency tickers, money market funds) |
| `test_bridge_idempotent_on_repeat_invocation` | Giant History + Latest Snapshots + Master Archive are byte-identical on second call |
| `test_metadata_json_contains_all_configured_etfs` | `metadata.json::etfs` equals sorted distinct ETF set from Giant History |
| `test_bridge_handles_weight_pct_with_percent_sign` | `"2.62%"` → `0.0262` normalisation (MFEM/AVSC regression) |

Hypothesis generates randomised inputs (column ordering, edge-case dates, malformed rows, locale-formatted numbers) to validate invariants that deterministic tests can't cover.

---

## Local Development

### Prerequisites

```bash
pip install -r requirements.txt
# For scraper runtime (not needed for build/tests):
pip install selenium curl_cffi pdfplumber xlrd
```

### Commands

```bash
# Reconstruct data/all_history.csv from Parquet store (required for local development/queries)
python scripts/hydrate_csv_from_parquet.py

# Run all 483 tests
python -m pytest tests/ -v

# Build site artifacts (leaderboard, holdings, changelog, flow, overlap)
# Note: --source data/all_history.csv is ignored when the Parquet store is
# populated (Parquet-first fetch_history). The path is retained for compat.
python -m conviction.build --source data/all_history.csv --output docs/data --config config.yaml

# Fetch market returns (FRED + yfinance) — requires FRED_API_KEY in .env
python -m conviction.markets_history --full-refresh

# Preview fetch plan without writing
python -m conviction.markets_history --dry-run

# Seed deep history from data/Mega_Markets_Historical.xlsx (run first before markets_history)
python -m conviction.ingest_markets_xl

# Fetch CBOE vol indices — requires FRED_API_KEY
python -m conviction.vol_history --full-refresh

# Run the scraper locally (requires Chrome + ChromeDriver)
python scraper.py

# Preview the site
python -m http.server -d docs 8000
# → http://localhost:8000
```

### Environment Variables

```bash
# Required for FRED data (markets_history, vol_history)
FRED_API_KEY=your_key_here

# Or create a .env file in the repo root:
echo "FRED_API_KEY=your_key_here" > .env
```

### File Structure

```
etf-data/
├── scraper.py                    # Primary scraper (22 ETFs)
├── config.yaml                   # Scoring config — source of truth for ETF universe
├── config.json                   # Per-ETF scraper routing (URL, scraper_type, CUSIP)
├── requirements.txt
│
├── data/
│   ├── Mega_Markets_Historical.xlsx  # Historical market data (gold 1833+, S&P 1871+)
│   ├── all_history.csv           # Transient master holdings CSV (hydrate via scripts/hydrate_csv_from_parquet.py)
│   ├── history_parquet/          # Append-only Parquet store (sole durable source of truth)
│   ├── latest/                   # Current snapshot per ETF
│   ├── history/                  # Dated daily snapshots (gitignored)
│   ├── flows/                    # Authoritative local flow dataset (145 instruments, read-only)
│   └── ticker_metadata.csv       # Sector/industry/country/market_cap per ticker
│
├── docs/                         # GitHub Pages root
│   ├── index.html                # Main dashboard
│   ├── markets.html              # Markets Intelligence Platform
│   ├── flow-ui.js                # Curated Fund Flows state, metrics, and SVG charts
│   ├── flow-ui.css               # Scoped Fund Flows research interface
│   ├── stock.html                # Per-ticker deep dive
│   ├── backtest.html             # Strategy backtest
│   ├── sim.html                  # Portfolio Lab (leveraged ETN simulator)
│   └── data/                     # Pre-built JSON payloads
│       ├── leaderboard.json      # Main leaderboard (~3.8MB)
│       ├── holdings_latest.json  # Per-(ETF, ticker) detail
│       ├── changelog.json        # Changes tab: per-period fund adds/exits, HC moves, rank movers, new/exits
│       ├── flag_history.json     # Per-ticker flag/rank history (90d)
│       ├── score_history.json    # Score sparkline data
│       ├── flow.json             # Sector + country flow (net_funds_delta / avg_funds_delta)
│       ├── flows/                # Local flow export (145 ticker files + catalog.json + manifest.json)
│       │   ├── catalog.json      # Generated static UI catalog (145 instruments, 24 featured)
│       │   └── manifest.json     # Complete local flow coverage manifest (schema 2)
│       ├── etf_overlap.json      # 30×30 Jaccard matrix
│       ├── market_returns.json   # Cross-asset monthly close (~940KB)
│       ├── vol_history.json      # CBOE vol indices
│       ├── markets.json          # Daily price log
│       └── metadata.json         # Build info + config snapshot
│
├── conviction/
│   ├── build.py                  # Main build pipeline
│   ├── scoring.py                # Score formula + sanitizer
│   ├── history.py                # Temporal analytics
│   ├── markets_history.py        # FRED + yfinance market data
│   ├── ingest_markets_xl.py      # Active Excel deep-history backfill ingestion
│   ├── ingest_mega_xl.py         # Backward-compat shim (delegates to ingest_markets_xl)
│   ├── vol_history.py            # CBOE vol indices
│   ├── backtest.py               # Strategy backtest engine
│   └── short_screener.py         # Diagnostic: short-side signal classification (mandate vs factor handoff vs consensus exit)
│
├── scripts/
│   ├── etf_holdings_scraper_v42.py  # Extended scraper (8 ETFs)
│   └── hydrate_csv_from_parquet.py # Reconstructs transient CSV from Parquet store
│
├── tests/                        # pytest suite (35 files, 483 tests)
│   ├── test_scoring.py           # 46 scoring & sanitizer tests
│   └── test_bridge.py            # 6 bridge PBT tests
│
└── .github/workflows/
     ├── daily_scrape.yml          # Scheduled/manual holdings scrape
     ├── daily_etf_flows.yml       # Disabled flow workflow (dispatch-only, exits 1, no network)
     └── build_site.yml            # Site build + deploy, including workflow_call
```

---

## Configuration Reference

### `config.yaml`

```yaml
etfs:
  - {ticker: COWZ, tier: Quality, points: 30}
  # ... 30 ETFs total

rank_breakpoints:
  - {rank_max: 10,  multiplier: 1.5}
  - {rank_max: 30,  multiplier: 1.2}
  - {rank_max: 999, multiplier: 1.0}

new_lookback_days: 14       # absent for 14d = NEW entrant
new_bonus_mult: 5.0         # NEW bonus = tier_points × 5
new_bonus_tiers: [Scout, Quant]

high_conviction_min_etfs: 4  # ≥4 ETFs = HIGH_CONVICTION

history:
  delta_periods_days: [1, 7, 14, 30, 60, 90]
  universe_start: "2026-05-22"     # history never reaches back past the current 30-fund universe
  leaderboard_lookback_days: 400   # window grows from universe_start up to this cap, then rolls
  changelog_top_n: 15
```

Commit a change to `config.yaml` → site auto-rebuilds with new scores in ~10 minutes.

### `config.json`

Per-ETF scraper routing: URL, `scraper_type` (invesco_api, pacer_csv, selenium_alpha, first_trust, etc.), CUSIP where needed. Consumed by `scraper.py` at runtime.

---

*Conviction Labs — built to find institutional conviction before it becomes consensus.*
