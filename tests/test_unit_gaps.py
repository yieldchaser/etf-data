"""
Unit tests for the two code gaps identified in the automation-self-living-data-flow spec.

Task 3 subtests:
  3.1  test_verdict_failed_no_live_sources  — Req 2.6
  3.2  test_build_without_market_returns    — Req 7.6

The verdict computation logic is replicated here as a pure function that
mirrors the logic in build.py exactly (per design doc: do not modify build.py
to extract it; test it by replicating the logic in the test file).
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))


# ─── Pure verdict computation (mirrors build.py logic exactly) ───────────────

def compute_self_living_check(
    assets: dict[str, Any],
    live_eligible_keys: set[str],
) -> dict[str, Any]:
    """
    Replicate the self_living_check verdict logic from conviction/build.py.

    This is intentionally a copy — not an import — so that the test is
    independent of build.py's internal structure and can verify the logic
    in isolation.
    """
    by_source: dict[str, int] = {}
    holdout_keys: list[str] = []
    holdout_names: list[str] = []

    for aid, av in assets.items():
        meta = av.get("meta", {}) or {}
        src = meta.get("source", "unknown")
        family = (
            "fred" if src.startswith("fred:")
            else "yfinance" if src.startswith("yfinance:")
            else "excel" if "Mega_Markets_Historical" in src
            else "other"
        )
        by_source[family] = by_source.get(family, 0) + 1

        if (
            aid in live_eligible_keys
            and isinstance(src, str)
            and src.startswith("Mega_Markets_Historical")
        ):
            holdout_keys.append(aid)
            holdout_names.append(meta.get("name") or aid)

    live_source_count = by_source.get("fred", 0) + by_source.get("yfinance", 0)
    excel_only_count = by_source.get("excel", 0)

    if not holdout_keys and live_source_count > excel_only_count:
        verdict = "LIVE_MERGE_HEALTHY"
    elif live_source_count > 0:
        verdict = "LIVE_MERGE_DEGRADED"
    else:
        verdict = "LIVE_MERGE_FAILED"

    return {
        "live_source_count": live_source_count,
        "excel_only_count": excel_only_count,
        "holdouts": sorted(holdout_names),
        "holdout_keys": sorted(holdout_keys),
        "verdict": verdict,
    }


# ─── Task 3.1 — Req 2.6: FAILED verdict when all sources are Excel ───────────

class TestVerdictFailedNoLiveSources:
    """
    Req 2.6: WHEN the verdict is LIVE_MERGE_FAILED, THE Build_Step SHALL write
    self_living_check.live_source_count = 0, indicating a total live-source outage.
    """

    def test_verdict_failed_when_all_assets_are_excel(self):
        """
        All assets carry Mega_Markets_Historical source labels.
        Verdict must be LIVE_MERGE_FAILED and live_source_count must be 0.
        """
        # Minimal market_returns.json-shaped assets dict — all Excel sources
        assets = {
            "sp500": {
                "meta": {
                    "name": "S&P 500",
                    "source": "Mega_Markets_Historical.xlsx:Equities",
                    "last": "2026-04",
                }
            },
            "gold": {
                "meta": {
                    "name": "Gold",
                    "source": "Mega_Markets_Historical.xlsx:Commodities",
                    "last": "2026-04",
                }
            },
            "us_10y": {
                "meta": {
                    "name": "US 10Y Treasury",
                    "source": "Mega_Markets_Historical.xlsx:Rates",
                    "last": "2026-04",
                }
            },
        }
        # All three are live-eligible (would normally come from FRED/yfinance)
        live_eligible_keys = {"sp500", "gold", "us_10y"}

        result = compute_self_living_check(assets, live_eligible_keys)

        assert result["verdict"] == "LIVE_MERGE_FAILED", (
            f"Expected LIVE_MERGE_FAILED when all assets are Excel-only, "
            f"got {result['verdict']!r}"
        )
        assert result["live_source_count"] == 0, (
            f"Expected live_source_count=0, got {result['live_source_count']}"
        )

    def test_verdict_failed_with_empty_assets(self):
        """
        Edge case: no assets at all → live_source_count=0 → LIVE_MERGE_FAILED.
        """
        result = compute_self_living_check({}, live_eligible_keys={"sp500"})
        assert result["verdict"] == "LIVE_MERGE_FAILED"
        assert result["live_source_count"] == 0

    def test_verdict_failed_holdout_keys_populated(self):
        """
        When verdict is FAILED, holdout_keys should list all registry-eligible
        Excel assets (they are all holdouts).
        """
        assets = {
            "sp500": {
                "meta": {
                    "name": "S&P 500",
                    "source": "Mega_Markets_Historical.xlsx:Equities",
                }
            },
        }
        live_eligible_keys = {"sp500"}

        result = compute_self_living_check(assets, live_eligible_keys)

        assert result["verdict"] == "LIVE_MERGE_FAILED"
        assert "sp500" in result["holdout_keys"], (
            "sp500 should be in holdout_keys when it's Excel-only and registry-eligible"
        )
        assert "S&P 500" in result["holdouts"], (
            "S&P 500 display name should be in holdouts"
        )


# ─── Task 3.2 — Req 7.6: build completes with available=false when market_returns.json missing ──

def _simulate_markets_freshness_block(output_dir: Path) -> dict[str, Any]:
    """
    Simulate the markets_freshness block from build.py in isolation.

    This replicates the exact logic from build.py:
        markets_freshness: dict[str, Any] = {"available": False}
        market_returns_path = output_dir / "market_returns.json"
        if market_returns_path.exists():
            try:
                ...full computation...
            except Exception as e:
                print(f"  WARN: could not read market_returns.json for freshness summary: {e}")

    Returns the markets_freshness dict that would be written to metadata.json.
    """
    markets_freshness: dict[str, Any] = {"available": False}
    market_returns_path = output_dir / "market_returns.json"

    if market_returns_path.exists():
        try:
            mr = json.loads(market_returns_path.read_text(encoding="utf-8"))
            assets = mr.get("assets", {})
            fx = mr.get("fx", {})
            cpi = mr.get("cpi", {})
            rates = mr.get("rates", {})

            # Import ASSET_REGISTRY with graceful fallback
            try:
                from conviction.markets_history import ASSET_REGISTRY as _MR_REGISTRY
                live_eligible_keys: set[str] = {
                    spec["key"]
                    for spec in _MR_REGISTRY
                    if spec.get("source_type") in {"yfinance", "fred"}
                    and spec.get("return_type") != "fx"
                }
            except Exception as _reg_err:
                print(f"  WARN: could not import ASSET_REGISTRY: {_reg_err}")
                live_eligible_keys = set()

            slc = compute_self_living_check(assets, live_eligible_keys)

            markets_freshness = {
                "available": True,
                "asof": mr.get("asof"),
                "asset_count": len(assets),
                "by_source": {},
                "fx_pairs": sorted(fx.keys()),
                "cpi_series": sorted(cpi.keys()),
                "rates_series": sorted(rates.keys()),
                "stale_assets": [],
                "self_living_check": slc,
            }
        except Exception as e:
            print(f"  WARN: could not read market_returns.json for freshness summary: {e}")
            # markets_freshness stays {"available": False}

    return markets_freshness


class TestBuildWithoutMarketReturns:
    """
    Req 7.6: WHEN market_returns.json does not exist at the time conviction.build runs,
    THE Build_Step SHALL complete with markets_data_freshness.available = false
    rather than crashing.

    These tests verify the markets_freshness logic in isolation (the logic is
    replicated from build.py as a pure function above). The full build() call
    is too slow for a unit test (it reads and scores the full holdings CSV).
    """

    def test_available_false_when_market_returns_missing(self, tmp_path):
        """
        When market_returns.json does not exist in output_dir, the freshness
        block must return {"available": False} without raising.
        """
        # Confirm no market_returns.json in tmp_path
        assert not (tmp_path / "market_returns.json").exists()

        try:
            result = _simulate_markets_freshness_block(tmp_path)
        except Exception as exc:
            pytest.fail(
                f"markets_freshness block raised {type(exc).__name__}: {exc}\n"
                f"Req 7.6: must complete gracefully when market_returns.json is absent."
            )

        assert result.get("available") is False, (
            f"Expected available=False when market_returns.json is absent, "
            f"got: {result.get('available')!r}"
        )

    def test_no_self_living_check_when_market_returns_missing(self, tmp_path):
        """
        When market_returns.json is absent, self_living_check must not be present
        in the freshness block (there is nothing to compute it from).
        """
        result = _simulate_markets_freshness_block(tmp_path)

        assert result.get("available") is False
        assert "self_living_check" not in result, (
            f"self_living_check should not be present when market_returns.json is absent, "
            f"got keys: {list(result.keys())}"
        )

    def test_available_true_when_market_returns_present(self, tmp_path):
        """
        Positive control: when market_returns.json IS present, available must be True.
        """
        # Write a minimal market_returns.json
        mr = {
            "asof": "2026-04",
            "assets": {
                "sp500": {
                    "meta": {
                        "name": "S&P 500",
                        "source": "yfinance:^GSPC",
                        "last": "2026-04",
                    },
                    "monthly": [["2026-04", 5200.0]],
                }
            },
            "fx": {},
            "cpi": {},
            "rates": {},
        }
        (tmp_path / "market_returns.json").write_text(
            json.dumps(mr), encoding="utf-8"
        )

        result = _simulate_markets_freshness_block(tmp_path)

        assert result.get("available") is True, (
            f"Expected available=True when market_returns.json is present, "
            f"got: {result.get('available')!r}"
        )
        assert "self_living_check" in result, (
            "self_living_check must be present when market_returns.json is present"
        )

    def test_build_py_initialises_available_false(self):
        """
        Verify that build.py's source code initialises markets_freshness with
        available=False BEFORE the if-block that reads market_returns.json.
        This is a static code check that the guard is in place.
        """
        build_py = REPO_ROOT / "conviction" / "build.py"
        source = build_py.read_text(encoding="utf-8")

        # The initialisation line must appear before the if-block
        init_pos = source.find('markets_freshness: dict[str, Any] = {"available": False}')
        if_pos = source.find("if market_returns_path.exists():")

        assert init_pos >= 0, (
            'build.py must initialise markets_freshness = {"available": False} '
            "before the market_returns.json existence check"
        )
        assert if_pos >= 0, (
            "build.py must have an 'if market_returns_path.exists():' guard"
        )
        assert init_pos < if_pos, (
            "markets_freshness must be initialised to available=False BEFORE "
            "the if market_returns_path.exists() block"
        )


# ─── Task 4 — Remaining example-based unit tests ─────────────────────────────


# ─── Task 4 / Req 4.7: vol_history.fetch_all() returns {} without SystemExit ─

class TestVolHistoryNoSysExit:
    """
    Req 4.7: WHEN the FRED client is unavailable, vol_history.fetch_all()
    SHALL return {} without calling sys.exit().
    """

    def test_vol_history_no_sys_exit(self, monkeypatch):
        """
        Patch _get_fred_client to return None.
        fetch_all() must return {} and must not raise SystemExit.
        """
        import pandas as pd
        import conviction.vol_history as vol_history

        monkeypatch.setattr(vol_history, "_get_fred_client", lambda: None)
        monkeypatch.setattr(vol_history, "_fetch_yfinance_vol", lambda *args, **kwargs: pd.Series(dtype=float))
        monkeypatch.setattr(vol_history, "_read_cache", lambda key: None)

        try:
            result = vol_history.fetch_all()
        except SystemExit as exc:
            pytest.fail(
                f"vol_history.fetch_all() raised SystemExit({exc.code}) when "
                f"_get_fred_client() returned None — Req 4.7 requires a graceful return."
            )

        assert result == {}, (
            f"Expected fetch_all() to return {{}} when FRED client is None, "
            f"got: {result!r}"
        )


# ─── Task 4 / Req 1.5: _get_fred_client() logs success ───────────────────────

class TestFredClientLogsSuccess:
    """
    Req 1.5 (rev 2026-08): WHEN FRED_API_KEY is present, _get_fred_client()
    SHALL return the API key string (the urllib port replaced the fredapi
    client object; callers pass it straight into _fetch_fred_series).
    """

    def test_fred_client_returns_key(self, monkeypatch, capsys):
        import conviction.markets_history as mh

        # Ensure the key is present
        monkeypatch.setenv("FRED_API_KEY", "test-key-12345")

        client = mh._get_fred_client()

        assert client == "test-key-12345", (
            f"Expected _get_fred_client() to return the API key string, got: {client!r}"
        )


# ─── Task 4 / Req 1.6: _get_fred_client() soft-skips when key absent ─────────

class TestFredClientSoftSkip:
    """
    Req 1.6: WHEN FRED_API_KEY is not set, _get_fred_client() SHALL return None
    without raising SystemExit, and SHALL log an error message.
    """

    def test_fred_client_soft_skip(self, monkeypatch, capsys):
        """
        Unset FRED_API_KEY from env.
        Call _get_fred_client() and assert it returns None without raising SystemExit.
        Assert "ERROR: FRED_API_KEY environment variable not set" appears in stdout.
        """
        import conviction.markets_history as mh

        # Ensure the key is absent
        monkeypatch.delenv("FRED_API_KEY", raising=False)

        try:
            result = mh._get_fred_client()
        except SystemExit as exc:
            pytest.fail(
                f"_get_fred_client() raised SystemExit({exc.code}) when FRED_API_KEY "
                f"was absent — Req 1.6 requires a graceful None return."
            )

        assert result is None, (
            f"Expected _get_fred_client() to return None when FRED_API_KEY is absent, "
            f"got: {result!r}"
        )

        captured = capsys.readouterr()
        assert "ERROR: FRED_API_KEY environment variable not set" in captured.out, (
            f"Expected error message in stdout, got: {captured.out!r}\n"
            "Req 1.6: _get_fred_client() must log an error when the key is absent."
        )


# ─── Task 4 / Req 6.2: yfinance flaky ticker retry ───────────────────────────

class TestYfinanceFlakeyTickerRetry:
    """
    Req 6.2: FOR tickers in the known-flaky set (^GSPC, ^NDX, ^DJI, NASDAQCOM),
    _fetch_yfinance_series() SHALL fire one bounded retry when the first
    yfinance.download() call returns an empty DataFrame.
    """

    def test_yfinance_flaky_ticker_retry(self, monkeypatch):
        """
        Patch yfinance.download to return empty DataFrame on first call and
        non-empty on second. Assert one retry fires for a flaky ticker.
        """
        import pandas as pd
        import conviction.markets_history as mh

        call_count = 0

        def mock_download(ticker, period, interval, progress):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # First call: return empty DataFrame (simulates transient CI failure)
                return pd.DataFrame()
            else:
                # Second call (retry): return a valid DataFrame with Close column
                idx = pd.to_datetime(["2024-01-31", "2024-02-29"])
                return pd.DataFrame({"Close": [4800.0, 4900.0]}, index=idx)

        # Patch time.sleep to avoid actual waiting
        monkeypatch.setattr("time.sleep", lambda s: None)

        # Patch yfinance.download inside the markets_history module
        import yfinance as yf
        monkeypatch.setattr(yf, "download", mock_download)

        # Use a known-flaky ticker
        flaky_ticker = "^GSPC"
        assert flaky_ticker in mh._YFINANCE_FLAKY_TICKERS, (
            f"{flaky_ticker} must be in _YFINANCE_FLAKY_TICKERS"
        )

        result = mh._fetch_yfinance_series(flaky_ticker, full_refresh=True)

        assert call_count == 2, (
            f"Expected exactly 2 yfinance.download calls (1 initial + 1 retry) "
            f"for flaky ticker {flaky_ticker}, got {call_count}"
        )
        assert not result.empty, (
            f"Expected non-empty result after retry for {flaky_ticker}, got empty Series"
        )


# ─── Task 4 / Req 5.6: stale ETF ::warning:: annotation ─────────────────────

def _simulate_stale_etf_warning(raw: "pd.DataFrame") -> list[dict]:
    """
    Replicate the stale ETF warning logic from conviction/build.py.

    This mirrors the exact logic in build.py lines 740–764:
        STALE_TRADING_DAYS = 5
        raw_dt2 = raw.copy()
        raw_dt2["Holdings_As_Of"] = pd.to_datetime(...)
        etf_latest_as_of = raw_dt2.groupby("ETF_Ticker")["Holdings_As_Of"].max()
        ...
        if approx_trading_days > STALE_TRADING_DAYS:
            stale_etfs.append(...)
            if os.environ.get("GITHUB_ACTIONS"):
                print(f"::warning::ETF ...")

    Returns the list of stale ETF dicts (same as build.py's stale_etfs).
    """
    import pandas as pd

    STALE_TRADING_DAYS = 5
    raw_dt2 = raw.copy()
    raw_dt2["Holdings_As_Of"] = pd.to_datetime(raw_dt2["Holdings_As_Of"], errors="coerce")
    etf_latest_as_of = raw_dt2.groupby("ETF_Ticker")["Holdings_As_Of"].max()
    today_ts = pd.Timestamp.now().normalize()
    stale_etfs = []
    for etf_ticker, last_as_of in etf_latest_as_of.items():
        if pd.isna(last_as_of):
            continue
        cal_days = (today_ts - last_as_of).days
        approx_trading_days = cal_days * 5 / 7
        if approx_trading_days > STALE_TRADING_DAYS:
            stale_etfs.append({
                "etf": etf_ticker,
                "last_as_of": last_as_of.strftime("%Y-%m-%d"),
                "calendar_days_old": cal_days,
            })
            if os.environ.get("GITHUB_ACTIONS"):
                print(
                    f"::warning::ETF {etf_ticker} holdings are stale: "
                    f"last Holdings_As_Of={last_as_of.date()} ({cal_days} calendar days ago)"
                )
    return stale_etfs


class TestStaleEtfWarningAnnotation:
    """
    Req 5.6: WHEN an ETF's latest Holdings_As_Of is older than 5 approximate
    trading days AND the build runs in GitHub Actions, the Build_Step SHALL
    emit a GitHub Actions ::warning:: annotation to stdout.
    """

    def _make_stale_raw(self) -> "pd.DataFrame":
        """
        Build a minimal raw DataFrame with one ETF whose Holdings_As_Of is
        30 calendar days ago (≈ 21 trading days — well above the 5-day threshold).
        """
        import pandas as pd
        from datetime import timedelta

        stale_date = (pd.Timestamp.now() - pd.Timedelta(days=30)).strftime("%Y-%m-%d")
        return pd.DataFrame({
            "ETF_Ticker": ["TEST_ETF", "TEST_ETF"],
            "Holdings_As_Of": [stale_date, stale_date],
            "Ticker": ["AAPL", "MSFT"],
            "Weight": [0.05, 0.04],
        })

    def test_stale_etf_warning_annotation(self, monkeypatch, capsys):
        """
        Construct a raw DataFrame with a stale Holdings_As_Of date.
        Set GITHUB_ACTIONS env var to simulate CI environment.
        Assert ::warning:: appears in captured stdout.
        """
        monkeypatch.setenv("GITHUB_ACTIONS", "true")

        raw = self._make_stale_raw()
        stale_etfs = _simulate_stale_etf_warning(raw)

        captured = capsys.readouterr()

        assert len(stale_etfs) >= 1, (
            "Expected at least one stale ETF to be detected with a 30-day-old Holdings_As_Of"
        )
        assert "::warning::" in captured.out, (
            f"Expected '::warning::' in stdout when GITHUB_ACTIONS is set and ETF is stale, "
            f"got: {captured.out!r}\n"
            "Req 5.6: build must emit a GitHub Actions warning annotation for stale ETFs."
        )

    def test_stale_etf_no_warning_outside_ci(self, monkeypatch, capsys):
        """
        Negative control: without GITHUB_ACTIONS set, ::warning:: must NOT appear.
        """
        monkeypatch.delenv("GITHUB_ACTIONS", raising=False)

        raw = self._make_stale_raw()
        stale_etfs = _simulate_stale_etf_warning(raw)

        captured = capsys.readouterr()

        assert len(stale_etfs) >= 1, (
            "Expected at least one stale ETF to be detected (precondition for this test)"
        )
        assert "::warning::" not in captured.out, (
            f"Expected no '::warning::' in stdout when GITHUB_ACTIONS is not set, "
            f"got: {captured.out!r}"
        )

    def test_fresh_etf_no_warning(self, monkeypatch, capsys):
        """
        Negative control: a fresh ETF (Holdings_As_Of = today) must not trigger a warning.
        """
        import pandas as pd

        monkeypatch.setenv("GITHUB_ACTIONS", "true")

        today = pd.Timestamp.now().strftime("%Y-%m-%d")
        raw = pd.DataFrame({
            "ETF_Ticker": ["FRESH_ETF"],
            "Holdings_As_Of": [today],
            "Ticker": ["AAPL"],
            "Weight": [0.05],
        })

        stale_etfs = _simulate_stale_etf_warning(raw)
        captured = capsys.readouterr()

        assert len(stale_etfs) == 0, (
            f"Expected no stale ETFs for a fresh Holdings_As_Of=today, got: {stale_etfs}"
        )
        assert "::warning::" not in captured.out, (
            f"Expected no '::warning::' for a fresh ETF, got: {captured.out!r}"
        )
