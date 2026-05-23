# Design Document

## Overview

This design hardens the v42 Extended_Scraper integration so that all 29 Configured_ETFs flow through a single, observable, idempotent pipeline. Today the integration is asymmetric: the primary loop writes three sinks (`data/latest/{TICKER}.csv`, `data/history/YYYY/MM/DD/master_archive.csv`, `data/all_history.csv`) but the Bridge only writes the last one. There is no automated test exercising the contract from Canonical_CSV to leaderboard, no module-level manifest of which scraper owns which ticker, and no `README.md` text matching the actual ETF universe.

The design treats the Bridge as a first-class pipeline stage with the same data-path obligations as the primary loop, isolates v42 as a subprocess whose failure modes cannot corrupt primary output, and adds a focused test file that pins the cleaning contract. It also formalises a single ownership manifest (`V42_ETFS`) used both for duplicate-ownership detection at startup and for downstream sink routing.

The design is deliberately conservative: existing function signatures stay where they don't conflict with the new structure, the v42 subprocess invocation is essentially the current code re-organised, and the dashboard requires no changes — `predator/build.py` already reads the ETF universe from `data/all_history.csv` via `cfg.etf_lookup()`, so new ETFs render automatically once their rows reach Giant_History.

## Architecture

### Pipeline Stages

The daily run is a four-stage pipeline. Each stage has well-defined inputs, outputs, and failure semantics.

```
Stage 1: Primary Loop
  Input:  config.json (21 Primary_ETFs)
  Output: master_list[] in memory (pandas DataFrames in Pipeline_Schema)

Stage 2: Primary Sink Writes
  Input:  master_list[]
  Output: data/latest/{TICKER}.csv  (per ETF)
          data/history/YYYY/MM/DD/master_archive.csv  (concatenated)
          data/all_history.csv  (deduped append via update_giant_history)

Stage 3: Extended Scraper (subprocess)
  Input:  config.yaml ETF universe (8 V42_ETFs implicit in v42 task list)
  Output: ./etf_holdings_YYYYMMDD.csv  (Canonical_CSV)
          ./etf_holdings_YYYYMMDD.xlsx (raw per-tab archive, not consumed downstream)

Stage 4: Bridge
  Input:  ./etf_holdings_YYYYMMDD.csv
  Output: data/latest/{TICKER}.csv  (per V42 ETF — NEW)
          data/history/YYYY/MM/DD/master_archive.csv  (append — NEW)
          data/all_history.csv  (deduped append via update_giant_history)
```

### Data Flow Diagram

```
                      ┌────────────────────────────────────────────┐
                      │         scraper.py::main()                 │
                      └────────────────────────────────────────────┘
                                          │
                                          ▼
        ┌─────────────────────────────────────────────────────────────────┐
        │  Startup invariant check: V42_ETFS ∩ config.json tickers == ∅   │
        │  (logs duplicate-ownership warning if violated; does not abort) │
        └─────────────────────────────────────────────────────────────────┘
                                          │
                                          ▼
   ┌──────────────────────────────────┐
   │  PRIMARY LOOP (21 ETFs)          │
   │  Pacer / First Trust / Selenium  │
   │  Alpha Architect / Invesco API   │
   └──────────────────────────────────┘
              │
              ├──► data/latest/{TICKER}.csv          (per-iteration)
              ▼
        master_list[]
              │
              ▼
   ┌──────────────────────────────────┐
   │  PRIMARY SINK WRITES             │
   │   • master_archive.csv  (write)  │
   │   • all_history.csv     (dedupe) │
   └──────────────────────────────────┘
              │
              ▼
   ┌──────────────────────────────────┐         ┌────────────────────────────────────┐
   │  run_extended_scrapers()         │────────►│  subprocess: v42                    │
   │  (BRIDGE entry point)            │         │  scripts/etf_holdings_scraper_v42   │
   │  • timeout 600s                  │◄────────│  writes etf_holdings_YYYYMMDD.csv   │
   │  • capture stdout/stderr         │         └────────────────────────────────────┘
   │  • reuse CSV if exists           │
   └──────────────────────────────────┘
              │
              ▼
   ┌──────────────────────────────────┐
   │  clean_canonical_csv(path)       │
   │   Returns (cleaned_df, errors)   │
   │   • required cols check          │
   │   • security_type filter         │
   │   • $ ticker filter              │
   │   • weight_pct → weight (÷100)   │
   │   • as_of_date parse             │
   │   • rename to Pipeline_Schema    │
   └──────────────────────────────────┘
              │
              ▼ (cleaned_df, non-empty)
   ┌──────────────────────────────────────────────────────────────────────┐
   │  bridge_write_all_sinks(cleaned_df)                                  │
   │   • update_giant_history([cleaned_df])  (append + dedupe)            │
   │   • write_v42_latest_snapshots(cleaned_df)  (per-ETF)                │
   │   • append_v42_to_master_archive(cleaned_df, today)  (create-or-app) │
   └──────────────────────────────────────────────────────────────────────┘
              │
              ▼
   ┌──────────────────────────────────┐
   │  Output state:                   │
   │   • all_history.csv  (29 ETFs)   │
   │   • latest/*.csv     (29 files)  │
   │   • master_archive.csv (29 ETFs) │
   └──────────────────────────────────┘

[Downstream — separate workflow]

   data/all_history.csv ──► predator.build ──► docs/data/{leaderboard,metadata,...}.json
                                                       │
                                                       ▼
                                                GitHub Pages
```

### Bridge as a Pipeline Stage

The Bridge is structurally identical to a primary scraper task: it produces a DataFrame in Pipeline_Schema and contributes to the same three sinks. The asymmetry today exists because the Bridge was bolted on after the primary loop's sink-writing pattern was already inlined inside `main()`. This design keeps that inline structure (no risky main-loop refactor) and puts the Bridge's three sink writes into a single helper, `bridge_write_all_sinks`, called once at the end of `main()`. Sink-write ordering is fixed: primary writes complete before any Bridge sink write begins (Req 4.5).

### Ownership Manifest

`scraper.py` declares `V42_ETFS` as a module-level frozenset:

```python
V42_ETFS = frozenset({"VLUE", "AVSC", "GRIN", "JHMM", "JHEM", "JHSC", "MFEM", "JOET"})
```

This single constant is the only authority for two things:
1. Startup ownership-collision check against `config.json` ticker list.
2. Routing in `bridge_write_all_sinks` (Latest_Snapshot is written for every `ETF_Ticker` value present in the cleaned DataFrame; we do not whitelist via `V42_ETFS` because the Canonical_CSV's `etf` column is itself authoritative — but `V42_ETFS` is asserted equal to the set of distinct `etf` values v42 emits when the run is healthy, surfacing drift between this constant and the v42 task list as a logged warning).

The Configured_ETFs invariant is `Primary_ETFs ∪ V42_ETFS == Configured_ETFs` and `Primary_ETFs ∩ V42_ETFS == ∅`. The startup check enforces only the disjointness clause; coverage (every Configured_ETF owned by exactly one scraper) is a CI-level concern surfaced through metadata.json's `etfs` field.

## Components and Interfaces

### Module: `scraper.py`

**New module-level constants**

```python
V42_ETFS: frozenset[str] = frozenset({
    "VLUE", "AVSC", "GRIN", "JHMM", "JHEM", "JHSC", "MFEM", "JOET",
})

REQUIRED_CANONICAL_COLUMNS: frozenset[str] = frozenset({
    "etf", "ticker", "name", "weight_pct", "as_of_date",
})

NON_EQUITY_SECURITY_TYPES: tuple[str, ...] = (
    "cash", "cash equivalent", "money market", "derivative",
    "futures", "option", "swap", "fx", "currency",
)
```

**New / refactored functions**

```python
def check_v42_ownership_collisions(primary_config: list[dict]) -> list[str]:
    """
    Return the sorted list of tickers appearing in BOTH config.json and V42_ETFS.
    Logs a warning per collision. Empty list = healthy. Does not raise.
    Called once at the top of main() after config.json is loaded.
    """

def clean_canonical_csv(csv_path: str) -> tuple[pd.DataFrame, list[str]]:
    """
    Read and validate the v42 Canonical_CSV at `csv_path`, returning a
    (cleaned_df, errors) tuple.

    cleaned_df: pandas DataFrame in Pipeline_Schema column order.
                Empty (zero rows) if any validation step fails or no rows
                survive cleaning.
    errors:     list of human-readable failure descriptions. Empty on success.

    Validation steps (in order):
      1. Read file via pd.read_csv. On any parse exception → ([], ["read failed: ..."]).
      2. Verify REQUIRED_CANONICAL_COLUMNS ⊆ df.columns → on missing → empty.
      3. Filter security_type substring matches against NON_EQUITY_SECURITY_TYPES.
      4. Drop ticker null/empty/leading-$ rows.
      5. Coerce weight_pct → numeric → /100; drop rows with weight ≤ 0 or > 1.
      6. Parse as_of_date via pd.to_datetime; drop NaT rows.
      7. Build Date_Scraped from scrape_date (YYYYMMDD) or fallback to TODAY.
      8. Rename etf → ETF_Ticker; project to Pipeline_Schema column order.

    Pure function. No I/O other than reading csv_path. Suitable for unit testing
    with a fixture CSV path.
    """

def write_v42_latest_snapshots(cleaned_df: pd.DataFrame) -> None:
    """
    For each ETF_Ticker group in cleaned_df, write
    data/latest/{ETF_Ticker}.csv containing only that ETF's rows in
    Pipeline_Schema column order. Overwrites any existing file (matches
    the primary loop's per-ETF latest-write semantics).

    No-op if cleaned_df is empty.
    """

def append_v42_to_master_archive(cleaned_df: pd.DataFrame, today: str) -> None:
    """
    Append cleaned_df to data/history/{today_ymd_path}/master_archive.csv.

    If the file does not exist (Bridge ran before the primary loop's
    archive write — should not happen given Req 4.5 ordering, but
    defensive), create it with cleaned_df contents only.

    If the file exists, read it, concat with cleaned_df, deduplicate on
    (ETF_Ticker, ticker, Holdings_As_Of) keep='last', write back.

    `today` is the YYYY-MM-DD string used to derive the archive directory
    (data/history/YYYY/MM/DD/).
    """

def bridge_write_all_sinks(cleaned_df: pd.DataFrame) -> None:
    """
    Single entry point that fans cleaned_df out to all three downstream sinks:
      1. update_giant_history([cleaned_df])
      2. write_v42_latest_snapshots(cleaned_df)
      3. append_v42_to_master_archive(cleaned_df, TODAY)

    Logs per-ETF row counts before writing. No-op if cleaned_df is empty.
    """

def run_extended_scrapers() -> None:
    """
    Bridge entry point. Refactored to compose the helpers above.

    Flow:
      1. Resolve scraper_path. If missing, log + return.
      2. Resolve csv_out path (etf_holdings_YYYYMMDD.csv at repo root).
      3. If csv_out exists: log "reusing" and skip subprocess (Req 2.7, 14.5).
         Else: invoke v42 via subprocess.run with timeout=600, capture both
         streams, log last 3000 chars stdout regardless of return code,
         log last 1000 chars stderr if returncode != 0. Catch
         TimeoutExpired and generic Exception; log and return on either.
      4. If csv_out still does not exist: log Bridge_Failure + return.
      5. cleaned_df, errors = clean_canonical_csv(csv_out).
      6. If errors: log each + return.
      7. If cleaned_df empty: log "Bridge: 0 valid rows" + return.
      8. bridge_write_all_sinks(cleaned_df).
    """
```

**Changed function**

`update_giant_history` keeps its signature and its dedup key `(ETF_Ticker, ticker, Holdings_As_Of)`. The existing implementation already satisfies Req 5 — no change required. Confirmed by reading lines 188–200 of scraper.py.

**Changed call site in `main()`**

The current `main()` ends with:
```python
if new_data_list:
    update_giant_history(new_data_list)
elif not os.path.exists(GIANT_HISTORY_FILE) and master_list:
    update_giant_history(master_list)

run_extended_scrapers()
```

This stays as-is. The startup check is added at the top of `main()` immediately after `etfs = json.load(f)`:

```python
collisions = check_v42_ownership_collisions(etfs)
if collisions:
    print(f"⚠️  Ownership collision: {collisions} appear in both config.json and V42_ETFS")
```

### Module: `scripts/etf_holdings_scraper_v42.py`

**Already partially compliant.** The current `run_all()` (lines 1432–1502) wraps each task in try/except and emits a summary table. The remaining gap is per-ticker isolation inside `fetch_jh_family` (one PDF download produces JHMM, JHEM, JHSC; if the PDF fetch fails, all three are zeroed at once — current behaviour). This is acceptable: the JH PDF is a single source, so per-ticker isolation inside it would be cosmetic.

**Required changes** are minimal:

1. **Confirm `try`/`except` wraps every task** — already present at lines 1450–1483.
2. **Confirm summary table prints final** — already present at line 1500–1501.
3. **Document the contract** as a module docstring addition: the script must always exit 0 even if every task fails (Req 6.4); it must omit the canonical CSV write on zero canonical frames (already done at line 1494's `if canon_frames:`).

The summary already has columns `ETF, Status, Rows, Cols` (Req 8.3, line 1474–1479). No code change needed; only a comment documenting the format as the public contract.

### Module: `predator/build.py`

**No changes.** Confirmed by reading the file: the ETF universe is derived from `cfg.etf_lookup()` (line 195 area) and from distinct `ETF_Ticker` values in `data/all_history.csv`. No ticker is hardcoded. The `metadata.json::etfs` field already comes from the live data. New ETFs surface automatically once they have rows in Giant_History.

### Module: `tests/test_bridge.py` (new file)

Five test functions exercising the Bridge contract end-to-end against fixture Canonical_CSVs.

```python
def test_bridge_cleans_canonical_csv_to_pipeline_schema(tmp_path):
    """
    Fixture CSV has rows for two V42 ETFs (VLUE + JHMM) with all canonical
    columns populated. Verify clean_canonical_csv returns a DataFrame whose
    columns equal the Pipeline_Schema list and whose row count equals the
    fixture row count (no rows dropped — all valid).
    """

def test_bridge_rejects_csv_missing_required_columns(tmp_path):
    """
    Fixture CSV is missing 'as_of_date'. Verify clean_canonical_csv returns
    (empty_df, errors) where errors names the missing column. Verify that
    invoking bridge_write_all_sinks on the empty DataFrame does not modify
    a pre-seeded GIANT_HISTORY_FILE (using tmp_path-rooted constants).
    """

def test_bridge_excludes_currency_and_money_market_rows(tmp_path):
    """
    Fixture CSV contains:
      - One $USD currency ticker
      - One $BRL currency ticker
      - One row with security_type='Money Market Fund'
      - One row with security_type='Cash & Equivalents'
      - One valid equity row
    Verify cleaned DataFrame has exactly one row (the equity).
    """

def test_bridge_idempotent_on_repeat_invocation(tmp_path):
    """
    Run bridge_write_all_sinks twice with the same cleaned DataFrame.
    Verify the resulting Giant_History row set after run 2 equals the
    row set after run 1 (composite key (ETF_Ticker, ticker, Holdings_As_Of)).
    """

def test_metadata_json_contains_all_configured_etfs(tmp_path):
    """
    Build a minimal Giant_History fixture containing one row per
    Configured_ETF. Run predator.build (or its loader subset) against it
    and assert that metadata.json::etfs equals sorted(Configured_ETFs).
    """
```

Tests use `tmp_path` and monkeypatch the module-level `GIANT_HISTORY_FILE`, `DATA_DIR_LATEST`, and `DATA_DIR_HISTORY` constants where needed, so no test touches the real `data/` directory.

### Workflows

**`.github/workflows/daily_scrape.yml`** — already correct. Confirmed:
- `xvfb` installed via apt-get (line 32) ✓
- All Python deps including `playwright` and `pdfplumber` installed (line 33) ✓
- `playwright install chromium --with-deps` runs (line 34) ✓
- Scraper invoked under `xvfb-run -a` (line 37) ✓
- Schedule `0 14,22 * * 1-5` ✓
- `data_changed=false` short-circuits the dispatch (line 49) ✓

No changes required.

**`.github/workflows/build_site.yml`** — already correct. Confirmed:
- Test suite runs before `predator.build` (line 65) ✓
- Verifies presence of `leaderboard.json`, `holdings_latest.json`, `changelog.json`, `metadata.json`, `leaderboard.parquet` (lines 86–91) ✓
- Checks out the exact commit pushed by the scraper via `head_sha` (line 50) ✓
- `if: github.event.workflow_run.conclusion == 'success'` skip clause (line 45) ✓

No changes required.

### README

Three localised edits:

1. **Header line** — replace `"16 Smart-Beta ETFs from Pacer, First Trust, Alpha Architect, and Invesco"` with `"29 Smart-Beta ETFs across Pacer, First Trust, Alpha Architect, Invesco, BlackRock, John Hancock, PIMCO, Avantis, VictoryShares, and Virtus"`. The count itself is derived from `config.yaml::etfs[]` length.

2. **ETF Tier Weights table** — extend to list all 29 tickers grouped by tier. Add the Quant additions (PDP, DWAS, EEMO, PIZ, JHMM, JHEM, JHSC, MFEM, JOET, IMOM-bumped, FPXI-bumped) and the Quality additions (IVAL, VLUE, AVSC, GRIN). Show points matching `config.yaml`.

3. **New section "🔌 Component 4: Extended Scraper"** placed after the existing Component 1 section. Documents the V42_ETFS set, the subprocess invocation pattern, the Canonical_CSV at `etf_holdings_YYYYMMDD.csv`, the Bridge's three sinks, and the failure-isolation contract.

## Data Models

### Pipeline_Schema

The 6-column DataFrame schema consumed by every downstream sink and by `predator.build`. All Bridge output must conform.

| Column           | Type     | Constraints                                          |
|------------------|----------|------------------------------------------------------|
| `ETF_Ticker`     | string   | Non-empty, ∈ Configured_ETFs                          |
| `ticker`         | string   | Non-empty, no leading `$`                             |
| `name`           | string   | May be empty (some sources omit issuer name)          |
| `weight`         | float    | (0, 1]; precision ≥ 6 decimal places                  |
| `Holdings_As_Of` | string   | `YYYY-MM-DD`, parseable by `pd.to_datetime`           |
| `Date_Scraped`   | string   | `YYYY-MM-DD`, defaults to `TODAY` when v42 omits it   |

### Canonical_CSV (v42 output, input to Bridge)

The 16-column canonical schema written by `_canonicalize` in `scripts/etf_holdings_scraper_v42.py` (lines 121–168). Required subset for Bridge cleaning:

| Column         | Required | Notes                                                  |
|----------------|----------|--------------------------------------------------------|
| `etf`          | Y        | Source ETF ticker. Renamed to `ETF_Ticker`.            |
| `ticker`       | Y        | Holding ticker. May be `$USD`, money-market — filtered. |
| `name`         | Y        | Issuer name. Optional content; column required.         |
| `weight_pct`   | Y        | Numeric, percentage form (0–100). Divided by 100.       |
| `as_of_date`   | Y        | YYYY-MM-DD. Renamed to `Holdings_As_Of`.                |
| `scrape_date`  | N        | YYYYMMDD. Used to derive `Date_Scraped`; falls back.    |
| `security_type`| N        | Substring-filtered against NON_EQUITY_SECURITY_TYPES.   |

Other canonical columns (cusip, sedol, isin, shares, market_value, price, country, exchange, currency) are tolerated and ignored. They are preserved in the v42-side raw XLSX but not propagated to Pipeline_Schema.

### Master_Archive

`data/history/YYYY/MM/DD/master_archive.csv` — same Pipeline_Schema as above. Created by the primary loop's `pd.concat(master_list).to_csv(...)` and appended-with-dedupe by the Bridge.

### Giant_History

`data/all_history.csv` — same Pipeline_Schema as above. Append-only with dedup key `(ETF_Ticker, ticker, Holdings_As_Of)` retaining last occurrence. Already implemented in `update_giant_history`.

### V42_ETFS

```python
frozenset({"VLUE", "AVSC", "GRIN", "JHMM", "JHEM", "JHSC", "MFEM", "JOET"})
```

This is the ownership manifest. Defined in `scraper.py` as a module-level constant. Used:
- At startup, intersected with the ticker list from `config.json` to detect ownership collisions.
- Implicitly for routing — the Bridge writes Latest_Snapshots for whatever ETFs appear in the cleaned DataFrame's `ETF_Ticker` column, which is expected to be a subset of `V42_ETFS`.

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system — essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

### Reflection on Consolidation

The 14 requirements yielded ~30 candidate testable claims after prework. Many overlap because the cleaning pipeline is a single pure function whose behavior factors into independent filters and a projection, and because the dedup contract on Giant_History is invoked from multiple call sites with the same semantics. Consolidation rules applied:

- **Row-exclusion filters consolidated.** `3.4` (security_type), `3.5` (ticker), `3.6` (weight_pct), `3.7` (as_of_date) all match the structure "for any canonical row matching predicate P, the row is excluded." Combined into a single "cleaning filter correctness" property whose statement quantifies over all four predicates simultaneously, which is strictly stronger than any individual predicate test.
- **Column projection consolidated.** `12.1` (1:1 projection cardinality), `12.2` (etf/ticker/name/as_of_date preservation), `12.3` (weight arithmetic), and `12.4` (range mapping) all describe the per-row projection contract. Combined into one property covering both cardinality and per-row value mapping.
- **Sink-write contracts consolidated.** `4.1` (Giant_History), `4.2` (Latest_Snapshot), `4.3+4.4` (Master_Archive append-or-create) all describe post-conditions of `bridge_write_all_sinks`. Combined into one property quantifying over all three sinks.
- **Idempotence properties consolidated.** `5.1` (dedup key), `5.2` (re-invocation), `5.3` (Bridge same triple), `5.4` (combined output), and `14.4` (14:00 + 22:00 reruns) are all the same dedup contract. Combined into one property.
- **Empty-input no-op consolidated.** `3.2` (missing column → no write) and `3.9` (zero valid rows → no write) both ensure an empty cleaned_df leaves Giant_History unchanged. Combined into the "required-column enforcement" property.
- **metadata.json properties consolidated.** `8.5` (sorted list of all ETFs in Giant_History) and `9.1` (Configured_ETFs in metadata) describe the same field. Combined.

After consolidation, nine properties remain. Each carries unique validation value: the cleaning filters, the column projection, the round-trip, and the sink contracts test independent code paths; the idempotence and the negation-of-idempotence (distinct Holdings_As_Of preserved) test the same dedup logic from opposite sides.

### Property 1: Ownership-collision detection is a pure set intersection

**For any** list of ticker strings `primary_tickers`, `check_v42_ownership_collisions(primary_tickers)` returns `sorted(set(primary_tickers) ∩ V42_ETFS)` and produces no side effects beyond logging.

**Validates: Requirements 1.3, 1.5**

### Property 2: Cleaning filters correctly exclude non-equity, invalid-ticker, out-of-range-weight, and unparseable-date rows

**For any** Canonical_CSV input, `clean_canonical_csv` returns a DataFrame containing exactly the rows of the input where ALL of the following hold simultaneously: (a) `security_type` does not contain any substring in NON_EQUITY_SECURITY_TYPES (case-insensitive), (b) `ticker` is non-null, non-empty, and does not begin with `$`, (c) the result of `pd.to_numeric(weight_pct) / 100` lies in the open-closed interval (0, 1], and (d) `pd.to_datetime(as_of_date)` is not NaT.

**Validates: Requirements 3.4, 3.5, 3.6, 3.7, 12.1, 14.2**

### Property 3: Required-column enforcement halts the pipeline before any sink write

**For any** Canonical_CSV input whose column set does not contain the full REQUIRED_CANONICAL_COLUMNS set, `clean_canonical_csv` returns an empty DataFrame and a non-empty errors list, AND for any pre-existing Giant_History `H`, invoking `bridge_write_all_sinks` with that empty DataFrame leaves `H` byte-identical.

**Validates: Requirements 3.1, 3.2, 3.9**

### Property 4: Column projection preserves source values and computes weight faithfully

**For any** surviving canonical row `r` (one that passes the filters in Property 2), the corresponding row `r'` in the cleaned DataFrame satisfies: `r'.ETF_Ticker == r.etf`, `r'.ticker == r.ticker`, `r'.name == r.name`, `r'.Holdings_As_Of == strftime(parse(r.as_of_date), '%Y-%m-%d')`, and `r'.weight == round(r.weight_pct / 100, 6)`. Furthermore, `r.weight_pct ∈ (0, 100]` implies `r'.weight ∈ (0, 1]`.

**Validates: Requirements 3.8, 12.2, 12.3, 12.4**

### Property 5: Pipeline_Schema CSV round-trip preserves the composite-key row set

**For any** Pipeline_Schema DataFrame `D`, writing `D` to CSV via `pandas.to_csv` and reading the result back via `pandas.read_csv` produces a DataFrame `D'` such that `set(zip(D.ETF_Ticker, D.ticker, D.Holdings_As_Of)) == set(zip(D'.ETF_Ticker, D'.ticker, D'.Holdings_As_Of))`.

**Validates: Requirements 12.5**

### Property 6: Bridge writes all three sinks when given a non-empty cleaned DataFrame

**For any** non-empty Pipeline_Schema DataFrame `cleaned_df`, after `bridge_write_all_sinks(cleaned_df)` returns: (a) every `(ETF_Ticker, ticker, Holdings_As_Of)` triple in `cleaned_df` appears in Giant_History, (b) for every distinct `ETF_Ticker` value `e` in `cleaned_df`, the file `data/latest/{e}.csv` exists and contains exactly the subset of `cleaned_df` rows where `ETF_Ticker == e` in Pipeline_Schema column order, and (c) the dated Master_Archive file at `data/history/{Y}/{M}/{D}/master_archive.csv` exists and its row set is a superset of `cleaned_df`'s row set under the composite key.

**Validates: Requirements 4.1, 4.2, 4.3, 4.4**

### Property 7: Giant_History is idempotent under the composite-key dedup

**For any** sequence of DataFrame contributions `[D_1, D_2, ..., D_n]`, applying `update_giant_history` to each sequentially produces a Giant_History whose rows are exactly `concat([D_1, ..., D_n]).drop_duplicates(subset=['ETF_Ticker', 'ticker', 'Holdings_As_Of'], keep='last')`. In particular, `update_giant_history` is idempotent: calling it twice with the same DataFrame leaves Giant_History byte-identical to the state after the first call.

**Validates: Requirements 5.1, 5.2, 5.3, 5.4, 14.4**

### Property 8: Distinct Holdings_As_Of values for the same (ETF_Ticker, ticker) pair are all retained

**For any** pair of rows `r_1, r_2` in Giant_History sharing `ETF_Ticker` and `ticker` but with `r_1.Holdings_As_Of ≠ r_2.Holdings_As_Of`, both rows survive deduplication. Equivalently: dedup must not collapse rows whose composite key triples differ. The same property holds for two rows with the same `ticker` but different `ETF_Ticker` (the primary-vs-V42 ticker collision case).

**Validates: Requirements 5.5, 14.1**

### Property 9: metadata.json::etfs equals the sorted distinct ETF set in Giant_History

**For any** Giant_History DataFrame `H`, after `predator.build` writes `docs/data/metadata.json`, the field `etfs` in that file equals `sorted(set(H.ETF_Ticker.unique()))`.

**Validates: Requirements 8.5, 9.1**

## Error Handling

The integration distinguishes three classes of failure, each with its own logging convention and recovery path.

### 1. v42 Subprocess Failures

Caught at the `run_extended_scrapers` boundary. None escalates to the Main_Scraper.

| Failure mode               | Detection                              | Action                                                                                    |
|----------------------------|----------------------------------------|-------------------------------------------------------------------------------------------|
| Nonzero exit code          | `result.returncode != 0`              | Log `⚠️ Extended scraper exited <code>`. Log last 1000 chars of stderr. Continue.        |
| Process timeout            | `subprocess.TimeoutExpired`           | Log `⚠️ Extended scraper timed out after 600s`. Continue.                                |
| Other subprocess exception | generic `except Exception`            | Log `⚠️ Could not run extended scraper: <type>: <message>`. Continue.                    |
| stdout always logged       | (any outcome)                         | Print last 3000 chars of stdout regardless of return code (Req 2.6).                     |

### 2. Canonical_CSV Validation Failures

Caught inside `clean_canonical_csv`. The function returns `(empty_df, errors)`; the caller short-circuits without writing.

| Failure mode                       | Detection                                              | Action                                                              |
|------------------------------------|--------------------------------------------------------|---------------------------------------------------------------------|
| File missing                       | `os.path.exists(csv_path) == False`                   | Skip subprocess (reuse). After subprocess: `❌ No extended output`. |
| Unreadable (parse error)           | `pd.read_csv` raises                                  | Append `read failed: <message>` to errors; return empty.            |
| Missing required column            | `not REQUIRED_CANONICAL_COLUMNS.issubset(df.columns)` | Append `missing columns: {set}`; return empty.                      |
| Zero rows after cleaning           | `len(cleaned_df) == 0`                                | Caller logs `⚠️  Bridge: 0 valid rows`. No sink writes.             |
| `weight_pct > 100` for a row       | per-row coerce                                        | Drop row, log identifier (`{etf}/{ticker}: weight_pct={value}`).    |
| `as_of_date` in the future         | `pd.to_datetime(as_of_date) > today`                  | Log warning naming the ETF; row is still included (Req 14.3).       |

### 3. Sink-Write Failures

Each sink is an independent file write. A failure on one sink is logged but does not block the others.

| Sink                          | Failure handling                                                              |
|-------------------------------|-------------------------------------------------------------------------------|
| `update_giant_history`        | Existing function logs and continues; preserves existing file on read error.  |
| `write_v42_latest_snapshots`  | Per-ETF write inside try/except; failure of one ETF does not abort others.    |
| `append_v42_to_master_archive`| Single write; on exception logs error and leaves existing file untouched.     |

### Logging Conventions

- **Bridge_Failure** lines begin with `⚠️ ` (recoverable) or `❌ ` (terminal for this run).
- **Per-ETF row counts** are emitted by both the primary loop and the Bridge in the same format: `{TICKER:6} {rows:4} rows | as_of={YYYY-MM-DD}`.
- **Subprocess output** is always tailed (3000 chars stdout, 1000 chars stderr on nonzero) so a CI run log captures enough context to diagnose v42 internals without re-running.

## Testing Strategy

### Test Categories

The integration is a thin orchestration layer over file I/O, subprocess launch, and pandas transformations. The high-value tests pin the cleaning contract; the rest of the orchestration is exercised by the existing `test_scoring.py` suite (which uses `compute_leaderboard` directly on Pipeline_Schema DataFrames) plus the workflow-level CI run.

| Concern                                | Test type            | Where                          |
|----------------------------------------|----------------------|--------------------------------|
| Canonical → Pipeline projection        | Property-based       | `tests/test_bridge.py`         |
| Schema validation rejection            | Example-based        | `tests/test_bridge.py`         |
| Currency/money-market exclusion        | Property-based       | `tests/test_bridge.py`         |
| Idempotency on repeat invocation       | Property-based       | `tests/test_bridge.py`         |
| metadata.json universe coverage        | Example-based        | `tests/test_bridge.py`         |
| Scoring formula                        | Already covered       | `tests/test_scoring.py`        |
| End-to-end CI run                      | Workflow execution   | `daily_scrape.yml` + manual    |

### Property-Based Testing

PBT applies to three of the five `tests/test_bridge.py` functions because the cleaning logic is a pure DataFrame transformation with universally quantified properties (round-trip, exclusion-set membership, idempotence). The remaining two (missing-column rejection, full-universe metadata) are example-based because they verify a specific failure path or a fixture-driven end-to-end shape, not a universal property over a varied input space.

**Library**: `hypothesis` (already pulled in transitively through pandas testing utilities, but adding it as an explicit test-only dep is acceptable). PBT runs use `@given` strategies generating canonical-shape rows. Minimum 100 iterations per property test, configured via `@settings(max_examples=100)`.

**Tag format**: each PBT test carries a comment `# Feature: v42-scraper-integration, Property N: <property text>`.

**Generators**:
- `canonical_row_strategy`: builds a single canonical-shape dict with `etf` ∈ `V42_ETFS`, `ticker` from a mix of plausible equity tickers and currency tickers, `weight_pct` ∈ `[0, 105]` (covering the >100 edge case), `as_of_date` as recent business days, `security_type` from a mix of equity and non-equity strings.
- `canonical_csv_strategy`: builds a list of 1–50 canonical rows, materialised as a DataFrame and written to `tmp_path / 'canonical.csv'`.

### Unit Testing

The two example-based tests pin specific contracts:
- Missing-column rejection — fixture is a hand-crafted CSV missing `as_of_date`. Asserts on the errors list and on the absence of writes.
- Full-universe metadata — fixture is a minimal Giant_History with one row per Configured_ETF; runs `predator.build` against it and asserts `metadata.json::etfs == sorted(Configured_ETFs)`.

### Configuration

- `pytest tests/ -v` is already part of `build_site.yml` (line 65). Adding `tests/test_bridge.py` automatically picks it up.
- No watch mode in CI (per project convention).
- PBT tests use `@settings(max_examples=100, deadline=2000)` so a CI run completes in seconds.

### Out-of-Scope Tests

We deliberately do **not** test:
- v42's per-source fetchers (Playwright, PDF, BlackRock API) — they are integration-bound to live external services and verified at the workflow level.
- Subprocess invocation mechanics — `subprocess.run` is a stdlib boundary; mocking it would test the mock.
- Predator scoring math — already covered by 27 tests in `test_scoring.py`.


## Risks and Mitigations

| Risk                                              | Likelihood | Impact | Mitigation                                                                                                                                     |
|---------------------------------------------------|------------|--------|------------------------------------------------------------------------------------------------------------------------------------------------|
| Playwright fragility (one source breaks)          | Medium     | Low    | v42's `run_all` already wraps each task in try/except; failures emit `❌ FAILED` in summary table and are logged with traceback. Bridge_Failure on whole subprocess is also isolated. |
| Ticker collision (same ticker in primary + V42)   | Low        | None   | Dedup key `(ETF_Ticker, ticker, Holdings_As_Of)` distinguishes them. Property 8 pins this. No real collision risk: V42 ETFs hold distinct issuers (intl + small-cap value) from Primary ETFs (mega-cap momentum). |
| CSV write race (two parallel runs)                | Low        | Low    | Workflows are sequential by design (cron at 14:00 + 22:00; manual dispatch is operator-gated). `concurrency: pages` in build_site.yml prevents concurrent builds. Same-day reruns are idempotent (Property 7). |
| v42 PDF parser regression (JH source format change) | Medium    | Medium | Bridge_Failure isolates impact to the 3 JH ETFs (JHMM, JHEM, JHSC). Other 5 V42 ETFs and 21 Primary ETFs unaffected. CI logs the per-ETF row count so silent regression is observable in metadata.json drift. |
| `xvfb` not available on runner                    | Low        | High   | Pinned in `daily_scrape.yml` step "Install libraries". A missing `xvfb` causes immediate workflow failure (visible) rather than silent v42 hang. |
| Disk-full or permission error on sink write       | Very Low   | Medium | Each sink write is independent; failure of one doesn't block the others. update_giant_history and append_v42_to_master_archive both swallow read errors and continue. |
| Module-level state on `TODAY` (UTC vs local)      | Low        | Low    | `TODAY` in scraper.py uses `datetime.now()` which is local time on the runner (UTC on GitHub Actions). v42 also uses `date.today()`. They will agree on the same calendar day. Documented as an accepted edge case at midnight UTC boundaries. |

## Migration Plan

The integration is already partially deployed (the current `run_extended_scrapers` writes Giant_History only). The migration is additive — no destructive changes, no data backfill required.

### Step 1: Code merge

1. Add `V42_ETFS` and `REQUIRED_CANONICAL_COLUMNS` constants to `scraper.py`.
2. Refactor `run_extended_scrapers` into the four helpers documented above.
3. Add `check_v42_ownership_collisions` startup check.
4. Create `tests/test_bridge.py` with all five tests.
5. Update `README.md` (header, tier table, new Component 4 section).
6. Confirm `daily_scrape.yml` and `build_site.yml` need no changes (already verified above).

### Step 2: Local verification

1. Run `python -m pytest tests/ -v` locally. All 27 existing tests + 5 new tests pass.
2. Manually run `python scraper.py` against a stale Canonical_CSV (touch `etf_holdings_YYYYMMDD.csv` so subprocess is skipped). Verify all three sinks update.

### Step 3: Deploy to main

1. Push to `main`. Build_Site workflow runs tests + builds; deploys to Pages.
2. Verify `docs/data/metadata.json::etfs` does not regress (should still contain the same set as before — no new data yet).

### Step 4: Trigger first full run

1. Go to Actions → "Daily ETF Scrape" → "Run workflow" (`workflow_dispatch`).
2. Watch the run log for:
   - Ownership collision warning (should be empty).
   - Per-ETF row counts for all 21 Primary ETFs.
   - v42 summary table with 9 entries (8 V42 ETFs + GRIN appears once but JH expands to 3 in items loop; JOET/AVSC/MFEM/VLUE one each → ~9 summary rows).
   - Bridge log lines: `✅ Bridge: N rows across 8 ETFs`.
3. After commit + push triggers Build_Site, verify `metadata.json::etfs` has length 29 and includes all V42_ETFS members.
4. Open the dashboard, confirm V42 ETFs appear in the ETFs tab and contribute to leaderboard scoring.

### Step 5: Steady-state monitoring

For the first week of scheduled runs (10 weekday runs, 14:00 + 22:00):

- Daily check: `metadata.json::etfs` length stays at 29.
- Weekly check: each V42 ETF has fresh `Holdings_As_Of` ≤ 7 days old.
- If any V42 ETF goes stale (no new Holdings_As_Of for >7 days), inspect the v42 summary table in the latest workflow run for that ETF's status row.

### Rollback

Should the deploy regress dashboard rendering, revert is a single commit (`git revert <merge>`). Giant_History is append-only and idempotent, so no data corruption risk; rolling back the code does not require rolling back data.
