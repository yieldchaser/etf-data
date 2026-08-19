"""
Property-based tests for per-series exception isolation in fetch_all().

Feature: automation-self-living-data-flow, Property 8: Per-series exception isolation

An exception raised during the fetch of a single series in
``markets_history.fetch_all()`` SHALL be caught within the per-series loop,
and ``fetch_all()`` SHALL continue to fetch all remaining series, returning
results for every series that did not raise an exception.

**Validates: Requirements 4.8**
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from conviction.markets_history import ASSET_REGISTRY, fetch_all


# ─── Strategies ──────────────────────────────────────────────────────────────


def exception_strategy():
    """Generate common exception types that a series fetch might raise."""
    return st.sampled_from([
        ValueError("test"),
        RuntimeError("test"),
        OSError("test"),
        TimeoutError("test"),
    ])


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _make_dummy_series() -> pd.Series:
    """Return a minimal non-empty Series to stand in for a successful fetch."""
    return pd.Series([1.0], index=[pd.Timestamp("2024-01-31")])


# ─── Property 8: Per-series exception isolation ───────────────────────────────


# Feature: automation-self-living-data-flow, Property 8: Per-series exception isolation
@given(st.sampled_from(list(ASSET_REGISTRY)), exception_strategy())
@settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
def test_per_series_exception_isolation(failing_spec: dict, exc: Exception) -> None:
    """
    **Validates: Requirements 4.8**

    For any asset spec drawn from ASSET_REGISTRY and any common exception type:
    - Patch the fetch dispatch so that the selected asset's fetch raises the
      given exception.
    - All other series fetches return a dummy non-empty Series.
    - Call fetch_all(full_refresh=True).
    - Assert no exception propagates out of fetch_all().
    - Assert the return value is a dict (possibly empty for the failing series,
      but the function must complete and return).
    """
    failing_key = failing_spec["key"]
    failing_source_type = failing_spec["source_type"]

    dummy = _make_dummy_series()

    def mock_fred_fetch(fred_client, series_id, full_refresh=False, existing=None):
        # Find which spec this series_id belongs to
        # We raise for the failing spec's series_id when source_type matches
        if series_id == failing_spec["series_id"] and failing_source_type == "fred":
            raise exc
        return dummy

    def mock_yf_fetch(ticker, full_refresh=False, existing=None):
        if ticker == failing_spec["series_id"] and failing_source_type == "yfinance":
            raise exc
        return dummy

    # Mock FRED client so no real network calls are made
    mock_fred_client = MagicMock()

    with (
        patch("conviction.markets_history._get_fred_client", return_value=mock_fred_client),
        patch("conviction.markets_history._fetch_fred_series", side_effect=mock_fred_fetch),
        patch("conviction.markets_history._fetch_yfinance_series", side_effect=mock_yf_fetch),
        patch("conviction.markets_history._write_cache"),
        patch("conviction.markets_history._read_cache", return_value=None),
        patch("conviction.markets_history.time.sleep"),
    ):
        # This must not raise — the per-series try/except must absorb the exception
        result = fetch_all(full_refresh=True)

    # fetch_all must return a dict regardless of per-series failures
    assert isinstance(result, dict), (
        f"fetch_all() must return a dict even when {failing_key!r} raises "
        f"{type(exc).__name__}. Got {type(result).__name__!r} instead."
    )
