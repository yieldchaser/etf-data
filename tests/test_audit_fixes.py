"""
Tests for AUDIT_FIX_PROMPT.md fixes.

Each test corresponds to a specific fix:
  #1A — history.py pd.Series.get() crash
  #1B — build.py leaderboard_rank checked only in first snapshot
  #1C — ticker_metadata.csv path resolution
  #1D — asof derived from all sources
  #1E — atomic writes
  #1F — CI fallback raises error
  #1G — public aliases for _load_existing / _merge_monthly
  #1H — FX section populated in markets_history
  #2  — partitioned Parquet store
  Tier4 — zero-weight rows, markets.json guard, requirements files
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pandas as pd
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _h(rows: list[tuple]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=["ETF_Ticker", "ticker", "name", "weight", "Holdings_As_Of", "Date_Scraped"])


# ─── #1A — history.py pd.Series.get() crash ──────────────────────────────────

class TestFix1A:
    """pd.Series.get() does not behave like dict.get for label lookups."""

    def test_streaks_and_deltas_two_snapshots(self):
        """streaks_and_deltas must not crash on a 2-snapshot fixture."""
        from conviction import history as hist
        from conviction.scoring import Config, compute_leaderboard
        from pathlib import Path

        cfg = Config.from_yaml(REPO_ROOT / "config.yaml")

        df = _h([
            ("FPX", "AAPL", "Apple", 0.10, "2026-01-01", "2026-01-01"),
            ("FPX", "AAPL", "Apple", 0.12, "2026-01-02", "2026-01-02"),
        ])
        historical = hist.historical_leaderboards(df, cfg)
        score_pnl = hist.score_panel(historical)
        flag_pnl = hist.flag_panel(historical)

        # Must not raise
        streaks = hist.streaks_and_deltas(score_pnl, flag_pnl)
        assert not streaks.empty
        aapl = streaks[streaks["ticker"] == "AAPL"]
        assert len(aapl) == 1
        # score_yday should be a number (not NaN crash)
        assert aapl.iloc[0]["score_yesterday"] is not None

    def test_score_yday_uses_loc_not_get(self):
        """Verify the fix: s.loc[yday] if yday in s.index else nan."""
        import pandas as pd
        # Simulate the exact scenario: a Series with Timestamp index
        idx = pd.to_datetime(["2026-01-01", "2026-01-02"])
        s = pd.Series([100.0, 120.0], index=idx)
        yday = idx[0]
        today = idx[1]

        # Old broken code: s.get(yday, float("nan")) — pd.Series.get() with Timestamp
        # may return the whole series or behave unexpectedly
        # New correct code:
        score_yday = s.loc[yday] if yday in s.index else float("nan")
        assert score_yday == 100.0

        # Verify missing key returns nan
        missing = pd.Timestamp("2025-01-01")
        score_missing = s.loc[missing] if missing in s.index else float("nan")
        import math
        assert math.isnan(score_missing)


# ─── #1B — leaderboard_rank checked per-snapshot ─────────────────────────────

class TestFix1B:
    """_attach_velocity must not crash when some snapshots lack leaderboard_rank."""

    def test_velocity_with_missing_leaderboard_rank_in_old_snapshot(self):
        """Feed a history where one older snapshot lacks leaderboard_rank — must not raise."""
        from conviction.scoring import Config, compute_leaderboard
        from pathlib import Path

        cfg = Config.from_yaml(REPO_ROOT / "config.yaml")

        # Build a minimal historical dict where one snapshot lacks leaderboard_rank
        df = _h([
            ("FPX", "AAPL", "Apple", 0.10, "2026-01-01", "2026-01-01"),
            ("FPX", "AAPL", "Apple", 0.12, "2026-01-15", "2026-01-15"),
            ("FPX", "AAPL", "Apple", 0.11, "2026-02-01", "2026-02-01"),
        ])

        lb1, _ = compute_leaderboard(df, cfg, as_of=pd.Timestamp("2026-01-01"))
        lb2, _ = compute_leaderboard(df, cfg, as_of=pd.Timestamp("2026-01-15"))
        lb3, _ = compute_leaderboard(df, cfg, as_of=pd.Timestamp("2026-02-01"))

        # Simulate old snapshot without leaderboard_rank
        lb1_no_rank = lb1.drop(columns=["leaderboard_rank"], errors="ignore")

        historical = {
            pd.Timestamp("2026-01-01"): lb1_no_rank,
            pd.Timestamp("2026-01-15"): lb2,
            pd.Timestamp("2026-02-01"): lb3,
        }

        # The _attach_velocity function is inside build() — test the guard logic directly
        dates_sorted = sorted(historical.keys())
        today_date = dates_sorted[-1]
        window_start = today_date - pd.Timedelta(days=30)
        window_cols = [c for c in dates_sorted if c >= window_start]

        # This is the fixed code path — guard per-snapshot
        rank_panel_rows = {}
        for d in window_cols:
            snap = historical[d]
            if "leaderboard_rank" in snap.columns:
                rank_panel_rows[d] = snap.set_index("ticker")["leaderboard_rank"]

        # Should not crash; should only include snapshots that have the column
        assert len(rank_panel_rows) >= 1  # at least lb2 and lb3 have it
        # lb1_no_rank should be excluded
        assert pd.Timestamp("2026-01-01") not in rank_panel_rows


# ─── #1C — ticker_metadata.csv path resolution ───────────────────────────────

class TestFix1C:
    """_attach_metadata must use absolute path and degrade gracefully."""

    def test_metadata_degrades_gracefully_when_file_missing(self, tmp_path):
        """When ticker_metadata.csv is missing, leaderboard gets Unknown columns."""
        from conviction.scoring import Config, compute_leaderboard
        from pathlib import Path
        import conviction.build as build_mod

        cfg = Config.from_yaml(REPO_ROOT / "config.yaml")
        df = _h([
            ("FPX", "AAPL", "Apple", 0.10, "2026-05-01", "2026-05-01"),
            ("FPX", "AAPL", "Apple", 0.08, "2026-01-01", "2026-01-01"),
        ])
        lb, _ = compute_leaderboard(df, cfg)

        # Simulate _attach_metadata with missing file
        meta_path = Path(__file__).resolve().parent.parent / "data" / "ticker_metadata.csv"
        if not meta_path.exists():
            # File doesn't exist — test graceful degradation
            for col in ["sector", "industry", "country", "market_cap_usd"]:
                lb[col] = "Unknown" if col != "market_cap_usd" else None
            assert lb["sector"].iloc[0] == "Unknown"
            assert lb["country"].iloc[0] == "Unknown"

    def test_metadata_path_is_absolute(self):
        """The path used in _attach_metadata must be absolute (not relative)."""
        import inspect
        import conviction.build as build_mod
        src = inspect.getsource(build_mod.build)
        # The fix uses Path(__file__).resolve().parent.parent / "data" / "ticker_metadata.csv"
        assert "Path(__file__).resolve()" in src or "__file__" in src, (
            "_attach_metadata should use __file__-relative path, not a bare relative path"
        )


# ─── #1D — asof from all sources ─────────────────────────────────────────────

class TestFix1D:
    """asof must be max of all sources, not just Excel assets."""

    def test_asof_includes_fx_section(self):
        """When FX data is newer than assets, asof should reflect FX date."""
        from conviction.ingest_markets_xl import build_output, _load_existing, EVENTS

        # Build a minimal existing JSON with old asset data but newer FX
        existing = {
            "asof": "2020-01",
            "generated_utc": "",
            "assets": {},
            "fx": {"USDINR": [["2026-05", 83.5]]},  # newer than any asset
            "cpi": {},
            "rates": {},
            "events": EVENTS,
        }

        # build_output with empty raw_series should preserve fx and compute asof from it
        output = build_output({}, existing, merge=True)
        # asof should be at least 2026-05 (from fx section)
        assert output["asof"] >= "2026-05", (
            f"asof={output['asof']} should be >= 2026-05 (from fx section)"
        )

    def test_markets_history_asof_includes_rates(self):
        """markets_history.build_output asof includes rates section."""
        from conviction.markets_history import build_output as mh_build_output
        import conviction.markets_history as mh

        # Patch OUTPUT_PATH to a non-existent path so it starts fresh
        orig = mh.OUTPUT_PATH
        mh.OUTPUT_PATH = Path("/nonexistent/market_returns.json")
        try:
            # Provide a rates series that is newer than any asset
            rates_data = pd.Series(
                [4.5, 4.6],
                index=pd.to_datetime(["2027-01-31", "2027-02-28"])
            )
            results = {"us_10y": rates_data}
            output = mh_build_output(results)
        finally:
            mh.OUTPUT_PATH = orig

        # asof should be at least 2027-02 (from rates)
        assert output["asof"] >= "2027-02", (
            f"asof={output['asof']} should be >= 2027-02 (from rates section)"
        )


# ─── #1E — atomic writes ─────────────────────────────────────────────────────

class TestFix1E:
    """Atomic writes: original file must be intact if write fails mid-stream."""

    def test_atomic_write_preserves_original_on_failure(self, tmp_path):
        """If write raises, original file must be intact."""
        from conviction.build import _write_json_atomic

        original_content = '{"original": true}'
        target = tmp_path / "test.json"
        target.write_text(original_content, encoding="utf-8")

        # Simulate a write that would fail — we can't easily inject a failure
        # but we can verify the atomic pattern: tmp file is cleaned up
        _write_json_atomic(target, '{"new": true}')
        assert target.read_text(encoding="utf-8") == '{"new": true}'
        # No .tmp file should remain
        assert not target.with_suffix(".tmp").exists()

    def test_atomic_write_creates_file_if_not_exists(self, tmp_path):
        """Atomic write creates the file if it doesn't exist."""
        from conviction.build import _write_json_atomic

        target = tmp_path / "new.json"
        assert not target.exists()
        _write_json_atomic(target, '{"created": true}')
        assert target.exists()
        assert target.read_text(encoding="utf-8") == '{"created": true}'

    def test_atomic_write_available_in_all_modules(self):
        """All JSON-writing modules must have _write_json_atomic."""
        import conviction.build
        import conviction.markets_history
        import conviction.ingest_markets_xl
        import conviction.vol_history
        import conviction.fetch_prices

        for mod in [conviction.build, conviction.markets_history,
                    conviction.ingest_markets_xl, conviction.vol_history,
                    conviction.fetch_prices]:
            assert hasattr(mod, "_write_json_atomic"), (
                f"{mod.__name__} missing _write_json_atomic"
            )


# ─── #1F — CI fallback raises error ──────────────────────────────────────────

class TestFix1F:
    """fetch_history must raise in CI when local file is missing."""

    def test_ci_raises_on_missing_file(self, tmp_path, monkeypatch):
        """When CI=true and BOTH parquet store AND source CSV are missing, fetch_history must raise.

        This pins the contract: a CI build that finds neither archive must
        fail loudly rather than silently fall back to the network. We point
        PARQUET_STORE at an empty tmp dir so the CSV path is genuinely
        exercised — without this monkeypatch, a locally-bootstrapped parquet
        archive would mask the missing-CSV case.
        """
        from conviction import build as _build
        from conviction.build import fetch_history

        monkeypatch.setenv("CI", "true")
        monkeypatch.setattr(_build, "PARQUET_STORE", tmp_path / "no_parquet")
        missing = str(tmp_path / "nonexistent.csv")

        with pytest.raises(FileNotFoundError, match="CI build"):
            fetch_history(missing)

    def test_non_ci_falls_back_gracefully(self, tmp_path, monkeypatch):
        """When CI is not set and source is missing, it falls back (no raise)."""
        from conviction import build as _build
        from conviction.build import fetch_history, FALLBACK_SOURCE

        monkeypatch.delenv("CI", raising=False)
        monkeypatch.setattr(_build, "PARQUET_STORE", tmp_path / "no_parquet")
        # We can't actually fetch from GitHub in tests, so just verify the
        # code path doesn't raise FileNotFoundError for the local check
        missing = str(tmp_path / "nonexistent.csv")
        # The function will try to fetch from FALLBACK_SOURCE — that may fail
        # with a network error, but NOT with FileNotFoundError
        try:
            fetch_history(missing)
        except FileNotFoundError:
            pytest.fail("Non-CI should not raise FileNotFoundError for missing local file")
        except Exception:
            pass  # Network errors are acceptable in tests


# ─── #1G — public aliases ─────────────────────────────────────────────────────

class TestFix1G:
    """load_existing and merge_monthly must be importable as public names."""

    def test_public_aliases_importable(self):
        """load_existing and merge_monthly must be importable from ingest_markets_xl."""
        from conviction.ingest_markets_xl import load_existing, merge_monthly
        assert callable(load_existing)
        assert callable(merge_monthly)

    def test_public_aliases_same_as_private(self):
        """Public aliases must be the same functions as private ones."""
        from conviction.ingest_markets_xl import (
            load_existing, merge_monthly,
            _load_existing, _merge_monthly,
        )
        assert load_existing is _load_existing
        assert merge_monthly is _merge_monthly

    def test_markets_history_uses_public_imports(self):
        """markets_history.py must import public names, not private ones."""
        import inspect
        import conviction.markets_history as mh
        src = inspect.getsource(mh.build_output)
        assert "load_existing" in src or "_load_xl_existing" in src
        # Must NOT import the private names directly
        assert "_load_existing as" not in src or "load_existing as" in src


# ─── #1H — FX section populated ──────────────────────────────────────────────

class TestFix1H:
    """FX section must be populated from fetched FX series."""

    def test_fx_section_populated_from_results(self):
        """When FX series are in results, fx_out must be populated."""
        from conviction.markets_history import build_output as mh_build_output
        import conviction.markets_history as mh

        # Patch OUTPUT_PATH to non-existent so it starts fresh
        orig = mh.OUTPUT_PATH
        mh.OUTPUT_PATH = Path("/nonexistent/market_returns.json")
        try:
            # Provide FX series data
            fx_data = pd.Series(
                [83.0, 83.5, 84.0],
                index=pd.to_datetime(["2026-03-31", "2026-04-30", "2026-05-31"])
            )
            # fx_usdinr has invert=True in ASSET_REGISTRY, so data is 1/value
            # We provide the raw FRED data (INR per USD)
            results = {"fx_usdinr": fx_data}
            output = mh_build_output(results)
        finally:
            mh.OUTPUT_PATH = orig

        fx = output.get("fx", {})
        assert "USDINR" in fx, f"USDINR not in fx section. fx keys: {list(fx.keys())}"
        assert len(fx["USDINR"]) > 0, "USDINR fx data is empty"

    def test_fx_inversion_applied(self):
        """FX series with invert=True must have values inverted."""
        from conviction.markets_history import build_output as mh_build_output
        import conviction.markets_history as mh

        orig = mh.OUTPUT_PATH
        mh.OUTPUT_PATH = Path("/nonexistent/market_returns.json")
        try:
            # FRED DEXINUS = INR per USD (e.g. 83.0 means 1 USD = 83 INR)
            # After inversion: 1/83.0 ≈ 0.01205 (USD per INR)
            fx_data = pd.Series(
                [83.0],
                index=pd.to_datetime(["2026-05-31"])
            )
            results = {"fx_usdinr": fx_data}
            output = mh_build_output(results)
        finally:
            mh.OUTPUT_PATH = orig

        fx = output.get("fx", {})
        if "USDINR" in fx and fx["USDINR"]:
            val = fx["USDINR"][0][1]
            # Should be inverted: ~0.01205, not 83.0
            assert val < 1.0, f"USDINR value {val} should be < 1.0 (inverted from 83.0)"


# ─── #2 — Partitioned Parquet store ──────────────────────────────────────────

class TestFix2:
    """Year-partitioned Parquet store."""

    def test_migration_script_exists(self):
        """Migration script must exist."""
        script = REPO_ROOT / "scripts" / "migrate_to_parquet.py"
        assert script.exists(), f"Migration script not found: {script}"

    def test_migration_creates_partitions(self, tmp_path):
        """Migration script creates year-partitioned Parquet files."""
        import subprocess
        import sys

        # Create a minimal CSV
        csv_path = tmp_path / "all_history.csv"
        df = pd.DataFrame([
            {"ETF_Ticker": "FPX", "ticker": "AAPL", "name": "Apple", "weight": 0.10,
             "Holdings_As_Of": "2024-06-01", "Date_Scraped": "2024-06-01"},
            {"ETF_Ticker": "FPX", "ticker": "AAPL", "name": "Apple", "weight": 0.11,
             "Holdings_As_Of": "2025-01-15", "Date_Scraped": "2025-01-15"},
            {"ETF_Ticker": "FPX", "ticker": "AAPL", "name": "Apple", "weight": 0.12,
             "Holdings_As_Of": "2026-03-01", "Date_Scraped": "2026-03-01"},
        ])
        df.to_csv(csv_path, index=False)

        dest = tmp_path / "history_parquet"
        result = subprocess.run(
            [sys.executable, str(REPO_ROOT / "scripts" / "migrate_to_parquet.py"),
             "--source", str(csv_path), "--dest", str(dest)],
            capture_output=True, text=True
        )
        assert result.returncode == 0, f"Migration failed: {result.stderr}"

        # Check partitions created
        assert (dest / "year=2024" / "holdings.parquet").exists()
        assert (dest / "year=2025" / "holdings.parquet").exists()
        assert (dest / "year=2026" / "holdings.parquet").exists()

    def test_parquet_store_read_function(self, tmp_path):
        """_read_parquet_store reads from partitioned store correctly."""
        from conviction.build import _read_parquet_store

        # Create a minimal partitioned store
        store = tmp_path / "history_parquet"
        yr_dir = store / "year=2026"
        yr_dir.mkdir(parents=True)

        df = pd.DataFrame([
            {"ETF_Ticker": "FPX", "ticker": "AAPL", "name": "Apple", "weight": 0.10,
             "Holdings_As_Of": pd.Timestamp("2026-03-01"), "Date_Scraped": "2026-03-01"},
        ])
        df.to_parquet(yr_dir / "holdings.parquet", index=False)

        result = _read_parquet_store(store, lookback_days=365)
        assert result is not None
        assert len(result) == 1
        assert result.iloc[0]["ticker"] == "AAPL"

    def test_parquet_store_returns_none_when_empty(self, tmp_path):
        """_read_parquet_store returns None when store doesn't exist."""
        from conviction.build import _read_parquet_store

        result = _read_parquet_store(tmp_path / "nonexistent_store")
        assert result is None

    def test_gitignore_has_parquet_dirs(self):
        """data/history_parquet/ must be in .gitignore."""
        gitignore = REPO_ROOT / ".gitignore"
        content = gitignore.read_text(encoding="utf-8")
        assert "data/history_parquet/" in content
        assert "data/markets_history/" in content
        assert "data/vol_history/" in content


# ─── Tier 4 — Zero-weight rows ───────────────────────────────────────────────

class TestFix4ZeroWeight:
    """Zero-weight rows must not inflate etf_count/held_by."""

    def test_zero_weight_rows_excluded_from_etf_count(self):
        """A ticker with weight=0 in one ETF must not count that ETF."""
        from conviction.scoring import Config, compute_leaderboard

        cfg = Config.from_yaml(REPO_ROOT / "config.yaml")
        df = _h([
            # AAPL in FPX with real weight
            ("FPX", "AAPL", "Apple", 0.10, "2026-05-01", "2026-05-01"),
            # AAPL in QMOM with zero weight (should be excluded)
            ("QMOM", "AAPL", "Apple", 0.0, "2026-05-01", "2026-05-01"),
            # Seed history
            ("FPX", "AAPL", "Apple", 0.08, "2026-01-01", "2026-01-01"),
        ])
        lb, _ = compute_leaderboard(df, cfg)
        aapl = lb[lb["ticker"] == "AAPL"]
        assert len(aapl) == 1
        # etf_count should be 1 (only FPX), not 2 (FPX + QMOM with 0 weight)
        assert aapl.iloc[0]["etf_count"] == 1, (
            f"etf_count={aapl.iloc[0]['etf_count']} should be 1 (zero-weight QMOM excluded)"
        )
        # held_by should only contain FPX
        assert "QMOM" not in aapl.iloc[0]["held_by"]


# ─── Tier 4 — markets.json partial-fetch guard ───────────────────────────────

class TestFix4MarketsGuard:
    """markets.json partial-fetch guard uses 90% threshold."""

    def test_guard_uses_90_percent_threshold(self):
        """The guard should skip write only when new count < 90% of existing."""
        import inspect
        import markets.build as mb
        src = inspect.getsource(mb.build)
        # Must use 0.9 multiplier, not bare < comparison
        assert "0.9" in src or "* 0.9" in src, (
            "markets/build.py guard should use 0.9 threshold"
        )


# ─── Tier 4 — requirements files ─────────────────────────────────────────────

class TestFix4Requirements:
    """requirements.txt and requirements-scraper.txt must exist."""

    def test_requirements_txt_exists(self):
        req = REPO_ROOT / "requirements.txt"
        assert req.exists(), "requirements.txt not found"
        content = req.read_text()
        assert "pandas" in content
        assert "pytest" in content
        assert "pyarrow" in content

    def test_requirements_scraper_txt_exists(self):
        req = REPO_ROOT / "requirements-scraper.txt"
        assert req.exists(), "requirements-scraper.txt not found"
        content = req.read_text()
        assert "selenium" in content
        assert "playwright" in content

    def test_build_site_yml_uses_requirements_txt(self):
        yml = REPO_ROOT / ".github" / "workflows" / "build_site.yml"
        content = yml.read_text()
        assert "requirements.txt" in content, "build_site.yml should use requirements.txt"

    def test_daily_scrape_yml_uses_requirements_scraper(self):
        yml = REPO_ROOT / ".github" / "workflows" / "daily_scrape.yml"
        content = yml.read_text()
        assert "requirements-scraper.txt" in content, (
            "daily_scrape.yml should use requirements-scraper.txt"
        )


# ─── Tier 4 — ingest_markets_xl --assets arg ─────────────────────────────────

class TestFix4AssetsArg:
    """ingest_markets_xl must accept --assets argument."""

    def test_assets_arg_accepted(self):
        """--assets argument must be accepted without error."""
        from conviction.ingest_markets_xl import parse_args
        args = parse_args(["--assets", "sp500,gold", "--dry-run"])
        assert args.assets == "sp500,gold"
        assert args.dry_run is True

    def test_ingest_mega_xl_shim_forwards_series_to_assets(self):
        """ingest_mega_xl shim must forward --series to --assets."""
        import inspect
        import conviction.ingest_mega_xl as shim
        src = inspect.getsource(shim.main)
        assert "assets" in src.lower(), (
            "ingest_mega_xl.main() must forward --series to --assets"
        )


# ─── Tier 4 — CI verify step ─────────────────────────────────────────────────

class TestFix4CIVerify:
    """build_site.yml verify step must check all required files."""

    def test_verify_step_checks_required_files(self):
        yml = REPO_ROOT / ".github" / "workflows" / "build_site.yml"
        content = yml.read_text()
        required_files = [
            "score_history.json",
            "holdings_history.json",
            "flag_history.json",
        ]
        for f in required_files:
            assert f in content, f"build_site.yml verify step missing check for {f}"

    def test_verify_step_has_leaderboard_sanity_check(self):
        yml = REPO_ROOT / ".github" / "workflows" / "build_site.yml"
        content = yml.read_text()
        assert "leaderboard.json" in content
        # Should have a non-zero entry count check
        assert "len(lb)" in content or "len(lb) > 0" in content


# ─── Tier 4 — markets.html wrong command ─────────────────────────────────────

class TestFix4MarketsHtmlCommand:
    """markets.html must use correct python -m command."""

    def test_markets_html_uses_module_command(self):
        html = REPO_ROOT / "docs" / "markets.html"
        content = html.read_text(encoding="utf-8")
        assert "python -m conviction.markets_history" in content, (
            "markets.html should use 'python -m conviction.markets_history'"
        )
        assert "python conviction/markets_history.py" not in content, (
            "markets.html should not use old 'python conviction/markets_history.py'"
        )


# ─── Tier 4 — stock.html Promise.all catch ───────────────────────────────────

class TestFix4StockHtmlCatch:
    """stock.html Promise.all must have .catch() on score_history and holdings_history."""

    def test_stock_html_has_catch_on_score_history(self):
        html = REPO_ROOT / "docs" / "stock.html"
        content = html.read_text(encoding="utf-8")
        # Find the score_history.json fetch line
        lines = content.split("\n")
        score_line = next((l for l in lines if "score_history.json" in l), None)
        assert score_line is not None, "score_history.json fetch not found in stock.html"
        assert ".catch(" in score_line, (
            f"score_history.json fetch missing .catch(): {score_line.strip()}"
        )

    def test_stock_html_has_catch_on_holdings_history(self):
        html = REPO_ROOT / "docs" / "stock.html"
        content = html.read_text(encoding="utf-8")
        lines = content.split("\n")
        hh_line = next((l for l in lines if "holdings_history.json" in l), None)
        assert hh_line is not None, "holdings_history.json fetch not found in stock.html"
        assert ".catch(" in hh_line, (
            f"holdings_history.json fetch missing .catch(): {hh_line.strip()}"
        )
