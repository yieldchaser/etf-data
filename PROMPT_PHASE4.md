# Phase 4 — Leveraged ETN Simulator

Working in `yieldchaser/etf-data`. Phases 1-3 deployed. Now port the leveraged-ETN math from the user's `Markets_1_.xlsx` "Simulations" sheet into an interactive React-on-React-UMD widget on the site.

## Goal

A `docs/sim.html` page where the user picks an underlying ticker, leverage factor, expense ratio, and overnight financing rate, then sees the daily NAV evolution of a leveraged ETN side-by-side against the unleveraged underlying.

## Why this matters

The user holds **NVDX (2× NVDA), AAPX (2× AAPL), TSLT (2× TSLA), GOOX (2× GOOG)** in their `Corporate_CAGR.xlsx`. The Simulations sheet does the daily math for these including fee drag and financing cost on the borrowed exposure. Porting it lets the dashboard model "what would my NAV be today if I bought N shares of NVDX on date D" with the same accuracy as the spreadsheet.

## Repo recap

```
data/markets/                          # from Phase 3 — has price history
  yf_NVDA.parquet, yf_AAPL.parquet, ...
predator/                              # Phase 1-2 — leave untouched
markets/                               # Phase 3
docs/index.html, stock.html, markets.html
```

## Step 1 — Extend `markets/fetch_yf.py` to cover the leveraged underlyings

The user holds 2× products on NVDA, AAPL, TSLA, GOOG. Add these to the SYMBOLS dict:

```python
SYMBOLS = {
    ...existing...,
    "NVDA": "NVDA", "AAPL": "AAPL", "TSLA": "TSLA",
    "GOOG": "GOOG", "GOOGL": "GOOGL", "MSFT": "MSFT", "AMZN": "AMZN", "META": "META",
}
```

These are individual equities, not indices, so the existing fetch logic works as-is. Run the workflow once to populate the parquets.

## Step 2 — Simulator math module (`markets/sim.py`)

Pure functions, no I/O.

```python
"""
Leveraged ETN NAV evolution.

Daily mechanics for an Nx leveraged ETN:
    daily_return       = N × underlying_daily_return
    fee_drag           = expense_ratio / 252
    financing_cost     = (N-1) × overnight_rate / 252      # only the borrowed sleeve pays this
    NAV_t+1 / NAV_t    = 1 + daily_return − fee_drag − financing_cost

Returns are computed on Close-to-Close. No intraday path-dependence modeling.
"""
from __future__ import annotations
import pandas as pd
from dataclasses import dataclass


@dataclass(frozen=True)
class SimParams:
    leverage: float          # e.g., 2.0 for 2x
    expense_ratio: float     # annualized, e.g., 0.0095 for 95 bps
    overnight_rate: float    # annualized, e.g., 0.043 for 4.3% SOFR
    start_nav: float = 100.0
    trading_days_per_year: int = 252


def simulate(prices: pd.DataFrame, params: SimParams) -> pd.DataFrame:
    """
    prices: columns ["Date", "Close"], sorted ascending, daily
    Returns: DataFrame with columns:
        Date, Close, underlying_return, lev_return, fee, financing,
        nav_unleveraged, nav_leveraged, cumulative_drag
    """
    df = prices.copy()
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.sort_values("Date").reset_index(drop=True)
    df["underlying_return"] = df["Close"].pct_change().fillna(0)
    df["lev_return"] = params.leverage * df["underlying_return"]
    df["fee"] = params.expense_ratio / params.trading_days_per_year
    df["financing"] = (params.leverage - 1) * params.overnight_rate / params.trading_days_per_year
    df["nav_unleveraged"] = (1 + df["underlying_return"]).cumprod() * params.start_nav
    df["nav_leveraged"] = (1 + df["lev_return"] - df["fee"] - df["financing"]).cumprod() * params.start_nav
    df["cumulative_drag"] = df["nav_leveraged"] - (1 + df["lev_return"]).cumprod() * params.start_nav  # negative = drag
    return df


def stats(sim_df: pd.DataFrame) -> dict:
    """Summary: total_return_unlev, total_return_lev, max_drawdown_lev,
    cumulative_fee, cumulative_financing, theoretical_lev_return_no_drag."""
    last = sim_df.iloc[-1]
    first = sim_df.iloc[0]
    return {
        "total_return_unlev": (last["nav_unleveraged"] / first["nav_unleveraged"]) - 1,
        "total_return_lev":   (last["nav_leveraged"]   / first["nav_leveraged"])   - 1,
        "max_drawdown_lev":   ((sim_df["nav_leveraged"] / sim_df["nav_leveraged"].cummax()) - 1).min(),
        "cumulative_fee":     sim_df["fee"].sum(),
        "cumulative_financing": sim_df["financing"].sum(),
    }
```

## Step 3 — Build step emits parquets the front-end can read

In `markets/build.py`, add a small step that writes each leveraged-underlying's price history as a JSON the simulator can consume client-side:

```python
# After markets.json is written
import json
LEVERAGED_UNDERLYINGS = ["NVDA", "AAPL", "TSLA", "GOOG", "GOOGL", "MSFT", "AMZN", "META", "SPX", "NDX"]
sim_inputs = {}
for sym in LEVERAGED_UNDERLYINGS:
    p = Path(args.source) / f"yf_{sym}.parquet"
    if not p.exists():
        continue
    df = pd.read_parquet(p)[["Date", "Close"]].copy()
    df = df.tail(2520)   # last 10 years
    sim_inputs[sym] = [{"d": d, "c": round(float(c), 4)} for d, c in zip(df["Date"], df["Close"])]
(Path(args.output) / "sim_underlyings.json").write_text(json.dumps(sim_inputs, separators=(",", ":")))
```

Result: `docs/data/sim_underlyings.json` — a small (~500KB) file containing 10 years of daily close for each leveraged-product underlying.

## Step 4 — Simulator UI (`docs/sim.html`)

Single file, React-on-UMD pattern. Use:

```html
<script crossorigin src="https://unpkg.com/react@18/umd/react.production.min.js"></script>
<script crossorigin src="https://unpkg.com/react-dom@18/umd/react-dom.production.min.js"></script>
<script crossorigin src="https://unpkg.com/@babel/standalone/babel.min.js"></script>
```

(No bundler. Babel transpiles in the browser. We accept the ~50ms cost on first paint for the simplicity tradeoff. Wrap the script tag in `type="text/babel"`.)

### Layout

```
┌────────────────────────────────────────────────────────────┐
│ PREDATOR PROTOCOL  ›  SIMULATOR             ← back          │
├────────────────────────────────────────────────────────────┤
│                                                              │
│ ┌── INPUTS ──────────────────────────┐                       │
│ │ Underlying:    [NVDA  ▾]            │                       │
│ │ Leverage:      [───●────────] 2.0x  │                       │
│ │ Expense ratio: [────●───────] 0.95% │                       │
│ │ Overnight rate:[─────●──────] 4.30% │                       │
│ │ Start date:    [2024-01-02 ▾]       │                       │
│ │ Position:      [$10,000.00]         │                       │
│ │                                      │                       │
│ │ [PRESETS]  NVDX  AAPX  TSLT  GOOX   │                       │
│ └────────────────────────────────────┘                       │
│                                                              │
│ ┌── RESULT ──────────────────────────────────────────────┐ │
│ │  Unleveraged total return: +43.2%  · NAV: $14,320      │ │
│ │  Leveraged total return:   +71.8%  · NAV: $17,180      │ │
│ │  Theoretical 2x (no drag): +86.4%  · NAV: $18,640      │ │
│ │  Cumulative drag:          –7.5%   ↳ fee + financing   │ │
│ │  Max drawdown (lev):       –32.4%                       │ │
│ └────────────────────────────────────────────────────────┘ │
│                                                              │
│ [Inline SVG chart: NAV path of unleveraged vs leveraged,    │
│  with the gap shaded — this is the compounding-decay viz]   │
│                                                              │
└────────────────────────────────────────────────────────────┘
```

### Implementation requirements

1. Load `docs/data/sim_underlyings.json` once on mount.
2. Re-run `simulate()` (JS port of the Python `markets/sim.py`) every time any input changes. Use `useMemo` so re-renders don't recompute unnecessarily.
3. Chart: inline SVG, no charting library. Width 100%, height 320px. Two paths — unleveraged in `var(--text-2)` (subdued), leveraged in `var(--cyan)` (highlight). Shade the area between them in `rgba(34, 211, 238, 0.06)`. X-axis: date ticks every 60 sessions. Y-axis: NAV ticks at $start, $start*1.5, $start*2.0, etc.
4. Presets buttons (NVDX, AAPX, TSLT, GOOX) set the underlying, leverage = 2.0, expense ratio = 0.95%, overnight rate = current SOFR (hard-coded fallback if no FRED data).
5. Reuse the dark / mono / cyan aesthetic from `docs/index.html`. Copy the `<style>` block verbatim.

### JS port of `simulate()`

```javascript
function simulate(prices, { leverage, expenseRatio, overnightRate, startNav, tradingDaysPerYear }) {
  const out = [];
  let navUnlev = startNav, navLev = startNav, prevClose = null;
  const feePerDay = expenseRatio / tradingDaysPerYear;
  const finPerDay = (leverage - 1) * overnightRate / tradingDaysPerYear;
  for (const row of prices) {
    const close = row.c;
    let r = 0;
    if (prevClose != null) r = close / prevClose - 1;
    navUnlev *= (1 + r);
    navLev *= (1 + leverage * r - feePerDay - finPerDay);
    out.push({ d: row.d, close, navUnlev, navLev });
    prevClose = close;
  }
  return out;
}
```

## Step 5 — Python tests for the simulator (`tests/test_sim.py`)

Add unit tests:

```python
import pandas as pd
import pytest
from markets.sim import SimParams, simulate, stats


def test_one_x_is_underlying():
    """1× leverage with zero fees should match underlying exactly."""
    prices = pd.DataFrame({"Date": pd.date_range("2024-01-01", periods=5),
                            "Close": [100, 102, 101, 105, 110]})
    s = simulate(prices, SimParams(leverage=1.0, expense_ratio=0, overnight_rate=0))
    pd.testing.assert_series_equal(s["nav_leveraged"], s["nav_unleveraged"], check_names=False)


def test_constant_return_compounds_correctly():
    """For constant daily return r, 2× ETN with zero fees should compound (1+2r)^N."""
    r = 0.01
    prices = pd.DataFrame({"Date": pd.date_range("2024-01-01", periods=11),
                            "Close": [100 * (1 + r) ** i for i in range(11)]})
    s = simulate(prices, SimParams(leverage=2.0, expense_ratio=0, overnight_rate=0))
    expected_final = 100 * (1 + 2*r) ** 10
    assert s.iloc[-1]["nav_leveraged"] == pytest.approx(expected_final, rel=1e-9)


def test_fee_and_financing_drag():
    """At zero return, 2× ETN with 1% fee and 4% financing should lose ~5%/yr."""
    prices = pd.DataFrame({"Date": pd.date_range("2024-01-01", periods=253),
                            "Close": [100] * 253})
    s = simulate(prices, SimParams(leverage=2.0, expense_ratio=0.01, overnight_rate=0.04))
    final = s.iloc[-1]["nav_leveraged"]
    # Daily drag = (0.01 + (2-1)*0.04)/252 = 0.05/252 per day, compounded 252 days ≈ 4.88% decay
    assert 94.5 < final < 95.5
```

## Step 6 — Link from main nav

In `docs/index.html` and others, add a "SIMULATOR" link to the top nav next to "MARKETS".

## Definition of Done — Phase 4

1. `python -m pytest tests/test_sim.py -v` is green.
2. `python -m markets.build` produces `docs/data/sim_underlyings.json`.
3. Navigating to `https://yieldchaser.github.io/etf-data/sim.html` shows the simulator UI with NVDA loaded by default.
4. Changing leverage from 2× to 3× immediately updates the chart and the result block.
5. Clicking the "NVDX" preset sets leverage=2.0, expense=0.95%, overnight=4.3%, underlying=NVDA.
6. The result block correctly attributes leveraged ETN underperformance vs theoretical to fee + financing (positive drag breakdown).
7. The chart visibly shows the compounding-decay gap — when the underlying ends close to flat after volatility, the leveraged NAV is meaningfully below the unleveraged NAV.

## What you must NOT do

- Do not use a JS charting library. Inline SVG only.
- Do not pull yfinance from the browser. All underlyings come from the pre-built `sim_underlyings.json`.
- Do not change Phases 1-3 outputs or scoring.
