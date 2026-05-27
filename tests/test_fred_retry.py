"""
tests/test_fred_retry.py

# Feature: automation-self-living-data-flow, Property 6: FRED retry exponential backoff

Property 6: FRED retry exponential backoff
  For N 429 responses (N ≤ 5), the fetch function must make exactly N+1 total
  attempts before succeeding on the (N+1)th attempt.

Validates: Requirements 6.1, 6.3
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from predator.markets_history import _fetch_fred_series


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_valid_series() -> pd.Series:
    """Return a minimal non-empty Series that _fetch_fred_series would accept."""
    return pd.Series([1.0, 2.0], index=pd.to_datetime(["2024-01-31", "2024-02-29"]))


def _make_mock_fred(n_failures: int) -> tuple[MagicMock, list[int]]:
    """
    Build a mock FRED client whose get_series raises Exception("429 Too Many Requests")
    for the first *n_failures* calls, then returns a valid Series.

    Returns (mock_fred, call_log) where call_log is a mutable list that records
    each call number so the test can assert on total call count.
    """
    call_log: list[int] = []

    def _get_series(*args, **kwargs):
        call_log.append(len(call_log) + 1)
        if len(call_log) <= n_failures:
            raise Exception("429 Too Many Requests")
        return _make_valid_series()

    mock_fred = MagicMock()
    mock_fred.get_series.side_effect = _get_series
    return mock_fred, call_log


# ---------------------------------------------------------------------------
# Property 6 — FRED retry exponential backoff
# ---------------------------------------------------------------------------

@given(st.integers(min_value=1, max_value=5))
@settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow])
def test_fred_retry_backoff(n_failures: int) -> None:
    """
    # Feature: automation-self-living-data-flow, Property 6: FRED retry exponential backoff

    **Validates: Requirements 6.1, 6.3**

    For N 429 responses (N ≤ 5), _fetch_fred_series must make exactly N+1 total
    attempts before succeeding, and the returned Series must be non-empty.

    The mock raises Exception("429 Too Many Requests") for the first n_failures
    calls, then returns a valid Series on the (n_failures+1)th call.
    time.sleep is patched to avoid actual waiting.
    """
    mock_fred, call_log = _make_mock_fred(n_failures)

    with patch("predator.markets_history.time.sleep"):
        result = _fetch_fred_series(mock_fred, "TEST", full_refresh=True)

    # Exactly N+1 total attempts: N failures + 1 success
    assert len(call_log) == n_failures + 1, (
        f"Expected {n_failures + 1} total attempts for {n_failures} 429 failures, "
        f"got {len(call_log)}"
    )

    # Result must be non-empty — the successful (N+1)th call returned data
    assert not result.empty, (
        f"Expected non-empty result after {n_failures} 429 failures and 1 success, "
        f"got empty Series"
    )


# ---------------------------------------------------------------------------
# Edge-case: exactly 5 failures (boundary — last retry succeeds)
# ---------------------------------------------------------------------------

def test_fred_retry_backoff_five_failures() -> None:
    """
    Boundary case: 5 consecutive 429s followed by a success on attempt 6.
    max_retries == 5, so attempt indices 0..4 are the retries; attempt 5 is
    the final try. With n_failures=5 the 6th call (attempt index 5) succeeds.
    """
    mock_fred, call_log = _make_mock_fred(5)

    with patch("predator.markets_history.time.sleep"):
        result = _fetch_fred_series(mock_fred, "TEST", full_refresh=True)

    assert len(call_log) == 6
    assert not result.empty


# ---------------------------------------------------------------------------
# Edge-case: zero failures (first call succeeds immediately)
# ---------------------------------------------------------------------------

def test_fred_retry_no_failures() -> None:
    """
    When the first call succeeds, exactly 1 attempt is made and result is non-empty.
    """
    mock_fred, call_log = _make_mock_fred(0)

    with patch("predator.markets_history.time.sleep"):
        result = _fetch_fred_series(mock_fred, "TEST", full_refresh=True)

    assert len(call_log) == 1
    assert not result.empty


# ---------------------------------------------------------------------------
# Edge-case: 6 failures (exceeds max_retries=5) → empty result, 6 attempts
# ---------------------------------------------------------------------------

def test_fred_retry_exhausted() -> None:
    """
    When all 6 attempts (initial + 5 retries) return 429, the function gives up
    and returns an empty Series after exactly 6 total calls.
    """
    mock_fred, call_log = _make_mock_fred(6)

    with patch("predator.markets_history.time.sleep"):
        result = _fetch_fred_series(mock_fred, "TEST", full_refresh=True)

    # max_retries=5 means the loop runs for attempt in range(6): 0,1,2,3,4,5
    # All 6 raise 429; on attempt==5 (== max_retries) it returns empty.
    assert len(call_log) == 6
    assert result.empty


# ---------------------------------------------------------------------------
# Verify time.sleep is called with exponential backoff values
# ---------------------------------------------------------------------------

def test_fred_retry_sleep_durations() -> None:
    """
    For 3 consecutive 429s, time.sleep must be called with the correct
    exponential backoff values: min(2^attempt * 0.5, 30) for attempt 0,1,2.
    """
    mock_fred, _ = _make_mock_fred(3)
    sleep_calls: list[float] = []

    def _record_sleep(secs: float) -> None:
        sleep_calls.append(secs)

    with patch("predator.markets_history.time.sleep", side_effect=_record_sleep):
        _fetch_fred_series(mock_fred, "TEST", full_refresh=True)

    # attempt 0 → wait = min(2^0 * 0.5, 30) = 0.5
    # attempt 1 → wait = min(2^1 * 0.5, 30) = 1.0
    # attempt 2 → wait = min(2^2 * 0.5, 30) = 2.0
    expected = [0.5, 1.0, 2.0]
    assert sleep_calls == expected, (
        f"Expected sleep durations {expected}, got {sleep_calls}"
    )
