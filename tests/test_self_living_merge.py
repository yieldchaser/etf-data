"""
Tests for the self-living markets data contract (charter v2 Part 2).

Pin three invariants:

  1. SOURCE-LABEL HONESTY: when ingest_markets_xl merges with an existing
     JSON whose assets carry live FRED/yfinance source labels, those labels
     are PRESERVED. Excel only sets the source label for assets it
     introduces; it never overwrites a live label back to Excel.

  2. RESILIENCE — NO SYS.EXIT: markets_history.fetch_all() must NEVER
     sys.exit() when FRED is unavailable. It degrades to per-series cache
     fallback instead.

  3. LIVE SECTIONS POPULATED: when markets_history.fetch_all() does have
     fresh data, build_output() emits non-empty fx, cpi, and rates sections.
     The dashboard's currency lens & real-returns toggles depend on these
     being populated; an empty `fx: {}` is the bug we're testing against.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest


REPO_ROOT = Path(__file__).resolve().parent.parent


# ─── (1) Source-label honesty — Excel must not clobber live labels ──────────

class TestSourceLabelHonesty:
    """ingest_markets_xl preserves live source labels on existing assets."""

    def test_existing_yfinance_source_is_preserved(self, tmp_path, monkeypatch):
        """
        Simulate the production pipeline: markets_history wrote yfinance:^GSPC
        as sp500's source. Then ingest_markets_xl runs with --merge-existing.
        The post-merge source label MUST still say 'yfinance:^GSPC', not
        'Mega_Markets_Historical.xlsx:Equities'.

        Without S2 (the source-honesty patch), this test fails because the
        old code unconditionally overwrote source on every merge.
        """
        from conviction.ingest_markets_xl import build_output

        existing = {
            "asof": "2026-04",
            "assets": {
                "sp500": {
                    "meta": {
                        "name": "S&P 500", "category": "equity", "native_ccy": "USD",
                        "source": "yfinance:^GSPC", "first": "1871-01", "last": "2026-04",
                    },
                    "monthly": [["1871-01", 4.44], ["2026-03", 6500.0], ["2026-04", 7100.0]],
                },
            },
            "fx": {}, "cpi": {}, "rates": {}, "events": [],
        }

        # raw_series provides the Excel-side data for sp500 — same months,
        # but Excel claims it's the source. The merge must keep yfinance.
        raw = {
            "sp500": pd.DataFrame({
                "Date":  pd.to_datetime(["1871-01-31", "2026-03-31"]),
                "Close": [4.44, 6500.0],
            }),
        }

        out = build_output(raw, existing, merge=True)
        assert "sp500" in out["assets"]
        actual_source = out["assets"]["sp500"]["meta"]["source"]
        assert actual_source == "yfinance:^GSPC", (
            f"Excel must NOT overwrite live source label.\n"
            f"  expected: 'yfinance:^GSPC'\n"
            f"  got:      {actual_source!r}\n"
            f"  This is the regression that made the live JSON claim Excel as the\n"
            f"  source for SP500 / NASDAQ / etc. when yfinance was actually the\n"
            f"  source of recent months."
        )

    def test_new_asset_gets_excel_source(self, tmp_path):
        """When Excel introduces a brand-new asset, its source label is Excel."""
        from conviction.ingest_markets_xl import build_output

        existing = {"asof": "", "assets": {}, "fx": {}, "cpi": {}, "rates": {}}
        raw = {
            "gold": pd.DataFrame({
                "Date":  pd.to_datetime(["1833-01-31", "2026-04-30"]),
                "Close": [18.93, 4900.0],
            }),
        }
        out = build_output(raw, existing, merge=True)
        assert "gold" in out["assets"]
        src = out["assets"]["gold"]["meta"]["source"]
        assert "Mega_Markets_Historical.xlsx" in src, (
            f"New (Excel-introduced) asset should get Excel source. Got: {src!r}"
        )

    def test_existing_excel_source_is_overwritten_by_re_run(self):
        """
        If the existing source IS Excel (e.g. from a previous Excel run that
        also set it), it's OK to re-set it to the new Excel sheet/file label.
        The honesty rule only protects LIVE (non-Excel) sources.
        """
        from conviction.ingest_markets_xl import build_output

        existing = {
            "asof": "2026-01",
            "assets": {
                "gold": {
                    "meta": {
                        "name": "Gold", "category": "precious_metals", "native_ccy": "USD",
                        "source": "Mega_Markets_Historical.xlsx:OldSheet",
                        "first": "1833-01", "last": "2026-01",
                    },
                    "monthly": [["1833-01", 18.93]],
                },
            },
            "fx": {}, "cpi": {}, "rates": {}, "events": [],
        }
        raw = {
            "gold": pd.DataFrame({
                "Date":  pd.to_datetime(["1833-01-31", "2026-04-30"]),
                "Close": [18.93, 4900.0],
            }),
        }
        out = build_output(raw, existing, merge=True)
        # Either the new Excel sheet name OR the old one is acceptable — what
        # MUST hold is that it remains an Excel-family label.
        assert "Mega_Markets_Historical.xlsx" in out["assets"]["gold"]["meta"]["source"]


# ─── (2) Resilience — never sys.exit on FRED unavailability ─────────────────

class TestFetchAllResilience:
    """markets_history.fetch_all degrades gracefully when FRED is unavailable."""

    def test_fetch_all_does_not_sys_exit_when_fred_missing(self, monkeypatch):
        """
        Drop the FRED API key. Restrict assets to FRED-only series so there
        is genuinely nothing live to fetch. The function must complete
        normally (returning an empty dict or a cache-served dict), NOT
        sys.exit.
        """
        from conviction import markets_history as mh

        monkeypatch.delenv("FRED_API_KEY", raising=False)
        monkeypatch.setattr(mh, "_get_fred_client", lambda: None)
        # No-op cache helpers so we don't read or write the real cache dir
        monkeypatch.setattr(mh, "_read_cache",  lambda key: None)
        monkeypatch.setattr(mh, "_write_cache", lambda key, data: None)

        # FRED-only asset, no cache — used to trigger sys.exit(1) path
        try:
            result = mh.fetch_all(assets=["us_cpi"], full_refresh=True)
        except SystemExit as e:
            pytest.fail(
                f"fetch_all raised SystemExit({e.code}) when FRED was unavailable. "
                f"Charter v2 Part 2: external-data failures must NEVER kill the build. "
                f"Per-series soft-skip is the contract."
            )

        # Must return a dict (possibly empty), not raise.
        assert isinstance(result, dict), f"Expected dict, got {type(result).__name__}"
        # us_cpi specifically should be absent (no live, no cache)
        assert "us_cpi" not in result or result.get("us_cpi", pd.Series()).empty

    def test_fetch_all_per_series_exception_isolated(self, monkeypatch):
        """A single series throwing an exception must not abort the loop."""
        from conviction import markets_history as mh

        monkeypatch.delenv("FRED_API_KEY", raising=False)
        monkeypatch.setattr(mh, "_get_fred_client", lambda: object())  # truthy
        monkeypatch.setattr(mh, "_read_cache",  lambda key: None)
        monkeypatch.setattr(mh, "_write_cache", lambda key, data: None)

        def _boom(*args, **kwargs):
            raise RuntimeError("simulated FRED outage")

        monkeypatch.setattr(mh, "_fetch_fred_series", _boom)

        # Should complete, returning empty dict (no sys.exit, no propagation)
        try:
            result = mh.fetch_all(assets=["us_cpi", "us_2y"], full_refresh=True)
        except RuntimeError as e:
            pytest.fail(f"Per-series exception leaked out of fetch_all: {e}")
        assert isinstance(result, dict)


# ─── (3) Live JSON shape — fx/cpi/rates non-empty when data flows ───────────

class TestLiveJsonShape:
    """When live data is fetched, build_output emits non-empty fx/cpi/rates."""

    def test_build_output_populates_fx_section(self, monkeypatch):
        """fx_usdinr → fx['USDINR'] = [[YYYY-MM, val], ...] (non-empty)."""
        from conviction.markets_history import build_output, _read_cache

        # Stub _read_cache so the function doesn't read parquet from disk
        monkeypatch.setattr("conviction.markets_history._read_cache", lambda key: None)

        idx = pd.to_datetime(["2024-01-31", "2024-02-29", "2024-03-31"])
        results = {
            "fx_usdinr": pd.Series([83.0, 83.5, 83.2], index=idx),
            "us_cpi":    pd.Series([309.0, 310.0, 311.0], index=idx),
            "us_10y":    pd.Series([4.10, 4.20, 4.30], index=idx),
        }
        out = build_output(results)
        # fx
        assert "USDINR" in out["fx"], f"USDINR missing from fx: {list(out['fx'].keys())}"
        # fx_usdinr was inverted (USD per INR) — verify it's a list of pairs
        usdinr = out["fx"]["USDINR"]
        assert isinstance(usdinr, list) and len(usdinr) > 0
        assert isinstance(usdinr[0], list) and len(usdinr[0]) == 2, \
            f"fx entries must be [YYYY-MM, val] pairs, got {usdinr[0]!r}"
        # cpi
        assert "US" in out["cpi"], f"US CPI missing from cpi: {list(out['cpi'].keys())}"
        # rates (us_10y should be in rates section)
        assert "us_10y" in out["rates"], f"us_10y missing from rates: {list(out['rates'].keys())}"


# ─── (4) Workflow: Excel runs once before FRED, never again ─────────────────

class TestWorkflowOrder:
    """build_site.yml runs Excel ONCE as backfill, then FRED. No second Excel."""

    def test_excel_runs_exactly_once_before_fred(self):
        """
        Pin the new ordering. Charter v2 Part 2:
          - Excel ingest_markets_xl --no-fail-on-stale (backfill seed)
          - Then markets_history --full-refresh (live merge)
          - NO second Excel run after FRED
        """
        yml = (REPO_ROOT / ".github" / "workflows" / "build_site.yml").read_text()
        # Count occurrences. ingest_markets_xl should appear once (the seed).
        # markets_history --full-refresh appears once.
        excel_runs = yml.count("python -m conviction.ingest_markets_xl")
        fred_runs  = yml.count("python -m conviction.markets_history --full-refresh")
        assert excel_runs == 1, (
            f"Expected exactly 1 Excel ingest run (backfill seed), got {excel_runs}.\n"
            f"Charter v2 Part 2: Excel is backfill-only. The previous pipeline ran it\n"
            f"twice (once as seed, once after FRED with --merge-existing) which clobbered\n"
            f"live source labels."
        )
        assert fred_runs == 1, f"Expected exactly 1 FRED+yfinance run, got {fred_runs}"
