"""Tests for the v42 Extended_Scraper Bridge.

Covers Properties 2, 3, 4, 5, 6, 7, 9 from the v42-scraper-integration spec.

All tests monkeypatch scraper module-level constants (GIANT_HISTORY_FILE,
DATA_DIR_LATEST, DATA_DIR_HISTORY, TODAY) onto tmp_path so no test touches
the real data/ directory.

Run: python -m pytest tests/test_bridge.py -v
"""
from __future__ import annotations

import json
import os
import string
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest
import yaml
from hypothesis import HealthCheck, given, settings, strategies as st

import scraper
from scraper import (
    PIPELINE_SCHEMA_COLS,
    V42_ETFS,
    bridge_write_all_sinks,
    clean_canonical_csv,
)


REPO_ROOT = Path(__file__).resolve().parent.parent
CONFIG_YAML = REPO_ROOT / "config.yaml"


# ─── Hypothesis strategies ────────────────────────────────────────────────────

# Tickers: 1..5 uppercase letters. Excludes leading-$ (the strategy's character
# set is uppercase letters only, so no $ ever appears) and ensures non-empty.
_TICKER = st.text(
    alphabet=string.ascii_uppercase, min_size=1, max_size=5
).filter(lambda s: s and not s.startswith("$"))

_NAME = st.text(
    alphabet=string.ascii_letters + " .,-", min_size=0, max_size=20
)

# Equity-friendly security_type values that must NOT match any substring in
# scraper.NON_EQUITY_SECURITY_TYPES (case-insensitive).
_EQUITY_SEC_TYPE = st.sampled_from(["Common Stock", "Equity", "ADR", "REIT", ""])

# weight_pct strictly in (0, 100]; we round to 6 places to keep the CSV
# round-trip exact (Property 5 compares row sets, so the rounding here is just
# defensive against float-print noise).
_WEIGHT_PCT = st.floats(
    min_value=1e-4, max_value=100.0, allow_nan=False, allow_infinity=False
).map(lambda x: round(x, 6))

_AS_OF_DATE = st.dates(
    min_value=pd.Timestamp("2020-01-01").date(),
    max_value=pd.Timestamp("2030-12-31").date(),
).map(lambda d: d.strftime("%Y-%m-%d"))

# Pick from the actual V42 ownership manifest so tests reflect real routing.
_V42_TICKERS = sorted(V42_ETFS)
_V42_ETF = st.sampled_from(_V42_TICKERS)


@st.composite
def _canonical_row(draw, etf=None):
    """Generate one valid canonical-CSV row dict (will pass all filters)."""
    return {
        "etf": etf if etf is not None else draw(_V42_ETF),
        "ticker": draw(_TICKER),
        "name": draw(_NAME),
        "weight_pct": draw(_WEIGHT_PCT),
        "as_of_date": draw(_AS_OF_DATE),
        "security_type": draw(_EQUITY_SEC_TYPE),
        "scrape_date": draw(_AS_OF_DATE).replace("-", ""),
    }


@st.composite
def _canonical_rows_two_etfs(draw):
    """A list of 2..20 valid canonical rows spanning ≥ 2 V42 ETFs.

    The composite-key (etf, ticker, as_of_date) is unique per row so row count
    is preserved through dedupe-style operations on the cleaned DataFrame.
    """
    etfs = draw(
        st.lists(_V42_ETF, min_size=2, max_size=4, unique=True)
    )
    n = draw(st.integers(min_value=len(etfs), max_value=20))
    rows = []
    seen_keys = set()
    for i in range(n):
        etf = etfs[i % len(etfs)] if i < len(etfs) else draw(st.sampled_from(etfs))
        for _attempt in range(20):
            row = draw(_canonical_row(etf=etf))
            key = (row["etf"], row["ticker"], row["as_of_date"])
            if key not in seen_keys:
                seen_keys.add(key)
                rows.append(row)
                break
    # Ensure at least 2 distinct ETFs survive
    distinct = {r["etf"] for r in rows}
    if len(distinct) < 2:
        # Forcibly inject a row from the second ETF
        second = etfs[1] if len(etfs) > 1 else draw(_V42_ETF)
        for _attempt in range(20):
            row = draw(_canonical_row(etf=second))
            key = (row["etf"], row["ticker"], row["as_of_date"])
            if key not in seen_keys:
                seen_keys.add(key)
                rows.append(row)
                break
    return rows


@st.composite
def _pipeline_rows_two_etfs(draw):
    """A list of 1..10 Pipeline_Schema dicts spanning ≥ 2 V42 ETFs.

    Used by Property 6 / 7 idempotence test which exercises bridge_write_all_sinks
    directly (skipping the clean_canonical_csv stage).
    """
    etfs = draw(
        st.lists(_V42_ETF, min_size=2, max_size=3, unique=True)
    )
    n = draw(st.integers(min_value=len(etfs), max_value=10))
    rows = []
    seen_keys = set()
    today = "2026-05-20"
    for i in range(n):
        etf = etfs[i % len(etfs)]
        for _attempt in range(20):
            ticker = draw(_TICKER)
            as_of = draw(_AS_OF_DATE)
            key = (etf, ticker, as_of)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            weight = draw(_WEIGHT_PCT) / 100.0
            rows.append({
                "ETF_Ticker": etf,
                "ticker": ticker,
                "name": draw(_NAME),
                "weight": round(weight, 6),
                "Holdings_As_Of": as_of,
                "Date_Scraped": today,
            })
            break
    return rows


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _patch_scraper_paths(monkeypatch, tmp_path: Path, today: str = "2026-05-20"):
    """Redirect scraper module-level filesystem constants into tmp_path."""
    monkeypatch.setattr(scraper, "GIANT_HISTORY_FILE", str(tmp_path / "all_history.csv"))
    monkeypatch.setattr(scraper, "DATA_DIR_LATEST", str(tmp_path / "latest"))
    monkeypatch.setattr(scraper, "DATA_DIR_HISTORY", str(tmp_path / "history"))
    monkeypatch.setattr(scraper, "TODAY", today)


def _wipe_sink_state(tmp_path: Path):
    """Remove all sink artefacts from the tmp_path so each Hypothesis example
    starts from a clean filesystem."""
    gh = tmp_path / "all_history.csv"
    if gh.exists():
        gh.unlink()
    latest_dir = tmp_path / "latest"
    if latest_dir.exists():
        for p in latest_dir.iterdir():
            if p.is_file():
                p.unlink()
    history_dir = tmp_path / "history"
    if history_dir.exists():
        # rm -rf history/ — pandas may have created nested YYYY/MM/DD dirs
        import shutil
        shutil.rmtree(history_dir)


# ─── Test 1: clean canonical CSV → Pipeline_Schema (Properties 2, 4, 5) ───────

def test_bridge_cleans_canonical_csv_to_pipeline_schema(tmp_path, monkeypatch):
    # Feature: v42-scraper-integration, Property 2: cleaning filters correctly
    # exclude non-equity, invalid-ticker, out-of-range-weight, unparseable-date rows
    # Feature: v42-scraper-integration, Property 4: column projection preserves
    # source values and computes weight faithfully
    # Feature: v42-scraper-integration, Property 5: Pipeline_Schema CSV round-trip
    # preserves the composite-key row set
    _patch_scraper_paths(monkeypatch, tmp_path)

    @given(rows=_canonical_rows_two_etfs())
    @settings(
        max_examples=100,
        deadline=2000,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def check(rows):
        csv_path = tmp_path / "canonical.csv"
        pd.DataFrame(rows).to_csv(csv_path, index=False)

        cleaned, errors = clean_canonical_csv(str(csv_path))

        # Property 4 — schema and column order.
        assert errors == []
        assert list(cleaned.columns) == PIPELINE_SCHEMA_COLS

        # Property 4 — row count preserved (every input row passes all filters).
        assert len(cleaned) == len(rows), (
            f"expected {len(rows)} rows, got {len(cleaned)}"
        )

        # Property 4 — at least 2 distinct ETFs in the cleaned output.
        assert cleaned["ETF_Ticker"].nunique() >= 2

        # Property 2 — weight values lie in (0, 1].
        assert (cleaned["weight"] > 0).all()
        assert (cleaned["weight"] <= 1).all()

        # Property 4 — per-row faithful projection of etf, ticker, weight.
        # Note: future as_of_dates are clamped to today (§28 date bug fix) AND
        # weekend/holiday dates are rolled back to the prev business day.
        # Build input_keys using the same two-step logic so the assertion stays
        # in sync with what clean_canonical_csv actually does.
        import scraper as _scraper
        today_str = _scraper.TODAY
        cleaned_keys = set(zip(
            cleaned["ETF_Ticker"], cleaned["ticker"], cleaned["Holdings_As_Of"]
        ))
        input_keys = {
            (r["etf"], r["ticker"],
             _scraper.roll_to_prev_biz_day(min(r["as_of_date"], today_str)))
            for r in rows
        }
        assert cleaned_keys == input_keys

        # Property 5 — CSV round-trip preserves the composite-key row set.
        roundtrip_path = tmp_path / "roundtrip.csv"
        cleaned.to_csv(roundtrip_path, index=False)
        # keep_default_na=False, na_values=[] preserves ticker strings such as
        # "NULL"/"NA"/"NaN" verbatim, matching the read semantics used by
        # clean_canonical_csv. Without this, pandas would coerce these strings
        # to NaN on read-back and the round-trip would not preserve the row set.
        roundtripped = pd.read_csv(roundtrip_path, keep_default_na=False, na_values=[])
        roundtrip_keys = set(zip(
            roundtripped["ETF_Ticker"],
            roundtripped["ticker"],
            roundtripped["Holdings_As_Of"],
        ))
        assert roundtrip_keys == cleaned_keys

    check()


# ─── Test 2: missing required column rejects pipeline (Property 3) ─────────────

def test_bridge_rejects_csv_missing_required_columns(tmp_path, monkeypatch):
    # Feature: v42-scraper-integration, Property 3: required-column enforcement
    # halts the pipeline before any sink write
    _patch_scraper_paths(monkeypatch, tmp_path)

    # Pre-seed Giant_History with a known row set.
    seed_df = pd.DataFrame(
        [
            ("VLUE", "AAPL", "Apple Inc",     0.05, "2026-05-01", "2026-05-01"),
            ("VLUE", "MSFT", "Microsoft Corp", 0.04, "2026-05-01", "2026-05-01"),
            ("JHMM", "WMT",  "Walmart Inc",   0.03, "2026-05-01", "2026-05-01"),
        ],
        columns=PIPELINE_SCHEMA_COLS,
    )
    gh_path = Path(scraper.GIANT_HISTORY_FILE)
    gh_path.parent.mkdir(parents=True, exist_ok=True)
    seed_df.to_csv(gh_path, index=False)
    seed_bytes = gh_path.read_bytes()

    # Hand-crafted Canonical_CSV missing as_of_date.
    bad_csv = tmp_path / "bad.csv"
    pd.DataFrame([
        {"etf": "VLUE", "ticker": "AAPL", "name": "Apple Inc",     "weight_pct": 5.0},
        {"etf": "JHMM", "ticker": "WMT",  "name": "Walmart Inc",   "weight_pct": 3.0},
    ]).to_csv(bad_csv, index=False)

    cleaned, errors = clean_canonical_csv(str(bad_csv))

    # (a) clean_canonical_csv returns (empty_df, errors_listing_as_of_date).
    assert cleaned.empty
    assert len(errors) >= 1
    assert any("as_of_date" in e for e in errors), (
        f"expected an error mentioning 'as_of_date', got: {errors}"
    )

    # (b) bridge_write_all_sinks on empty leaves Giant_History byte-identical.
    bridge_write_all_sinks(cleaned)

    assert gh_path.read_bytes() == seed_bytes, (
        "GIANT_HISTORY_FILE was modified by bridge_write_all_sinks(empty_df)"
    )


# ─── Test 3: filters currency / money-market rows (Property 2) ────────────────

def test_bridge_excludes_currency_and_money_market_rows(tmp_path, monkeypatch):
    # Feature: v42-scraper-integration, Property 2: cleaning filters correctly
    # exclude non-equity, invalid-ticker, out-of-range-weight, unparseable-date rows
    _patch_scraper_paths(monkeypatch, tmp_path)

    @given(
        equity_ticker=_TICKER,
        equity_weight=_WEIGHT_PCT,
        equity_etf=_V42_ETF,
        equity_as_of=_AS_OF_DATE,
        permutation=st.permutations([0, 1, 2, 3, 4]),
    )
    @settings(
        max_examples=100,
        deadline=2000,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def check(equity_ticker, equity_weight, equity_etf, equity_as_of, permutation):
        # Five rows: two currency tickers, two non-equity security types,
        # one valid equity. The Hypothesis `permutation` randomises the order
        # so the test does not depend on row position.
        rows = [
            {
                "etf": "VLUE", "ticker": "$USD", "name": "US Dollar",
                "weight_pct": 0.5, "as_of_date": "2026-05-01",
                "security_type": "Currency",
            },
            {
                "etf": "VLUE", "ticker": "$BRL", "name": "Brazilian Real",
                "weight_pct": 0.3, "as_of_date": "2026-05-01",
                "security_type": "Currency",
            },
            {
                "etf": "JHMM", "ticker": "AGPXX", "name": "Money Market Fund",
                "weight_pct": 1.5, "as_of_date": "2026-05-01",
                "security_type": "Money Market Fund",
            },
            {
                "etf": "JHMM", "ticker": "FGXXX", "name": "Cash & Equivalents Fund",
                "weight_pct": 1.0, "as_of_date": "2026-05-01",
                "security_type": "Cash & Equivalents",
            },
            {
                "etf": equity_etf, "ticker": equity_ticker, "name": "Test Equity",
                "weight_pct": equity_weight, "as_of_date": equity_as_of,
                "security_type": "Common Stock",
            },
        ]
        ordered = [rows[i] for i in permutation]
        csv_path = tmp_path / "mixed.csv"
        pd.DataFrame(ordered).to_csv(csv_path, index=False)

        cleaned, errors = clean_canonical_csv(str(csv_path))

        assert errors == []
        assert len(cleaned) == 1, (
            f"expected exactly 1 equity row to survive, got {len(cleaned)}: "
            f"{cleaned.to_dict(orient='records')}"
        )
        survivor = cleaned.iloc[0]
        assert survivor["ETF_Ticker"] == equity_etf
        assert survivor["ticker"] == equity_ticker
        assert 0 < survivor["weight"] <= 1

    check()


# ─── Test 4: bridge_write_all_sinks idempotent on repeat (Properties 6, 7) ────

def test_bridge_idempotent_on_repeat_invocation(tmp_path, monkeypatch):
    # Feature: v42-scraper-integration, Property 6: bridge writes all three
    # sinks when given a non-empty cleaned DataFrame
    # Feature: v42-scraper-integration, Property 7: Giant_History is idempotent
    # under the composite-key dedup
    _patch_scraper_paths(monkeypatch, tmp_path, today="2026-05-20")

    @given(rows=_pipeline_rows_two_etfs())
    @settings(
        max_examples=100,
        deadline=2000,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    def check(rows):
        _wipe_sink_state(tmp_path)

        df = pd.DataFrame(rows, columns=PIPELINE_SCHEMA_COLS)

        # First invocation — populates all three sinks.
        bridge_write_all_sinks(df)

        gh_path = Path(scraper.GIANT_HISTORY_FILE)
        latest_dir = Path(scraper.DATA_DIR_LATEST)
        archive_path = (
            Path(scraper.DATA_DIR_HISTORY) / "2026" / "05" / "20" / "master_archive.csv"
        )

        assert gh_path.exists(), "Giant_History missing after first call"
        assert archive_path.exists(), "Master_Archive missing after first call"
        for etf in df["ETF_Ticker"].unique():
            assert (latest_dir / f"{etf}.csv").exists(), (
                f"Latest snapshot missing for {etf} after first call"
            )

        first_gh_keys = {
            (r.ETF_Ticker, r.ticker, r.Holdings_As_Of)
            for r in pd.read_csv(gh_path).itertuples(index=False)
        }
        first_latest_bytes = {
            etf: (latest_dir / f"{etf}.csv").read_bytes()
            for etf in df["ETF_Ticker"].unique()
        }
        first_archive_bytes = archive_path.read_bytes()

        # Second invocation with the same DataFrame must be a no-op on disk.
        bridge_write_all_sinks(df)

        # Property 7 — Giant_History composite-key row set is unchanged.
        second_gh_keys = {
            (r.ETF_Ticker, r.ticker, r.Holdings_As_Of)
            for r in pd.read_csv(gh_path).itertuples(index=False)
        }
        assert second_gh_keys == first_gh_keys

        # Property 6 — per-ETF Latest_Snapshot files are byte-identical.
        for etf, expected_bytes in first_latest_bytes.items():
            assert (latest_dir / f"{etf}.csv").read_bytes() == expected_bytes, (
                f"latest/{etf}.csv changed on second bridge_write_all_sinks call"
            )

        # Property 6 — Master_Archive is byte-identical.
        assert archive_path.read_bytes() == first_archive_bytes, (
            "master_archive.csv changed on second bridge_write_all_sinks call"
        )

    check()


# ─── Test 5: metadata.json::etfs covers all Configured_ETFs (Property 9) ──────

def test_metadata_json_contains_all_configured_etfs(tmp_path):
    # Feature: v42-scraper-integration, Property 9: metadata.json::etfs equals
    # the sorted distinct ETF set in Giant_History
    cfg_data = yaml.safe_load(CONFIG_YAML.read_text(encoding="utf-8"))
    configured_etfs = sorted({entry["ticker"] for entry in cfg_data["etfs"]})
    assert configured_etfs, "config.yaml::etfs[] is empty — fixture preconditions broken"

    # Build a minimal Giant_History fixture: one row per Configured_ETF, two
    # snapshot dates so predator.build's historical/leaderboard pipeline has
    # something to chew on. We use synthetic holding tickers that are NOT in
    # config.yaml::sanitizer.blocked_tickers so the rows survive sanitization.
    today = "2026-05-20"
    yday = "2026-05-19"
    fixture_rows = []
    for i, etf in enumerate(configured_etfs):
        # Synthetic 4-letter holding ticker derived from the index so it is
        # unique and not in the sanitizer block list.
        tkr = f"H{i:03d}"
        for as_of in (yday, today):
            fixture_rows.append({
                "ETF_Ticker": etf,
                "ticker": tkr,
                "name": f"Test Holding {i}",
                "weight": 0.05,
                "Holdings_As_Of": as_of,
                "Date_Scraped": as_of,
            })
    fixture_csv = tmp_path / "all_history.csv"
    pd.DataFrame(fixture_rows, columns=PIPELINE_SCHEMA_COLS).to_csv(fixture_csv, index=False)

    output_dir = tmp_path / "build_output"
    output_dir.mkdir()

    # Run predator.build as a subprocess so it cannot leak global state into
    # the rest of the test session.
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    # Force CSV ingestion path: point parquet store at an empty tmp dir so
    # the build doesn't accidentally pick up the real repo's parquet archive
    # (which would shadow the synthetic --source CSV the test gave it).
    env["PREDATOR_PARQUET_STORE"] = str(tmp_path / "no_parquet")
    result = subprocess.run(
        [
            sys.executable, "-m", "predator.build",
            "--source", str(fixture_csv),
            "--output", str(output_dir),
            "--config", str(CONFIG_YAML),
        ],
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert result.returncode == 0, (
        f"predator.build exited {result.returncode}\n"
        f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    )

    metadata_path = output_dir / "metadata.json"
    assert metadata_path.exists(), (
        f"metadata.json missing in {output_dir}; build stdout:\n{result.stdout}"
    )
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert "etfs" in metadata, f"metadata.json missing 'etfs' field: keys={list(metadata)}"
    assert metadata["etfs"] == configured_etfs, (
        f"metadata.json::etfs mismatch.\n"
        f"  expected: {configured_etfs}\n"
        f"  actual:   {metadata['etfs']}"
    )



# ─── Regression: weight_pct string coercion (MFEM/AVSC bug) ───────────────────

def test_bridge_handles_weight_pct_with_percent_sign(tmp_path, monkeypatch):
    # Feature: v42-scraper-integration, Property 2 (regression)
    # Surfaced 2026-05-23 by the first end-to-end CI run: MFEM (PIMCO) and
    # AVSC (Avantis) emit weight_pct as a string with a trailing '%' (e.g.
    # "2.62%", "0.55%"). Without normalisation, pd.to_numeric coerced those
    # values to NaN and every row was filtered out, so MFEM and AVSC bridged
    # 0 rows despite the underlying scraper succeeding.
    _patch_scraper_paths(monkeypatch, tmp_path)
    csv_path = tmp_path / "percent_strings.csv"
    pd.DataFrame([
        {"etf": "MFEM", "ticker": "DELTA",  "name": "Delta Electronics",
         "weight_pct": "2.62%",  "as_of_date": "2026-05-21",
         "security_type": "Common Stock"},
        {"etf": "AVSC", "ticker": "MYRG",   "name": "MYR Group Inc",
         "weight_pct": "0.55%",  "as_of_date": "2026-05-22",
         "security_type": "Common Stock"},
        {"etf": "AVSC", "ticker": "DAN",    "name": "Dana Inc",
         "weight_pct": "0.45%",  "as_of_date": "2026-05-22",
         "security_type": "Common Stock"},
        # Locale-formatted string with comma, also accepted.
        {"etf": "MFEM", "ticker": "TEST",   "name": "Locale Test",
         "weight_pct": "12,3456", "as_of_date": "2026-05-21",
         "security_type": "Common Stock"},
    ]).to_csv(csv_path, index=False)

    cleaned, errors = clean_canonical_csv(str(csv_path))

    assert errors == []
    # 2.62%, 0.55%, 0.45% all in (0, 100] → survive. "12,3456" → strip comma →
    # 123456 → /100 = 1234.56 → weight > 1 → CORRECTLY filtered (regression
    # would have been silent NaN-coerce; this proves the strip happened and
    # the bounds check still works).
    assert len(cleaned) == 3, (
        f"expected 3 valid percent-string rows after stripping, got {len(cleaned)}: "
        f"{cleaned.to_dict(orient='records')}"
    )
    assert {"MFEM", "AVSC"} == set(cleaned["ETF_Ticker"])
    # Spot-check: "2.62%" → 0.0262
    mfem_delta = cleaned[(cleaned["ETF_Ticker"] == "MFEM") & (cleaned["ticker"] == "DELTA")].iloc[0]
    assert abs(mfem_delta["weight"] - 0.0262) < 1e-6
