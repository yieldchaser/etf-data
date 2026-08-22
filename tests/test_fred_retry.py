"""
tests/test_fred_retry.py

# Feature: automation-self-living-data-flow, Property 6: FRED retry exponential backoff

Property 6: FRED retry exponential backoff
  For N 429 responses (N ≤ 5), the fetch function must make exactly N+1 total
  attempts before succeeding on the (N+1)th attempt.

Validates: Requirements 6.1, 6.3

2026-08: _fetch_fred_series moved from the fredapi library to the FRED JSON
REST API via urllib (fredapi raises opaque ValueError(None) under CI's
pandas/numpy stack). These tests now mock urllib.request.urlopen.
"""
from __future__ import annotations

import io
import json
import urllib.error
from unittest.mock import patch

import pandas as pd
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from conviction.markets_history import _fetch_fred_series


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_OK_PAYLOAD = json.dumps({
    "observations": [
        {"date": "2024-01-31", "value": "1.0"},
        {"date": "2024-02-29", "value": "2.0"},
    ]
}).encode()


def _make_mock_urlopen(n_failures: int):
    """
    Build a urlopen side_effect that raises HTTPError(429) for the first
    *n_failures* calls, then serves a valid FRED observations payload.

    Returns (side_effect, call_log).
    """
    call_log: list[int] = []

    def _side_effect(*args, **kwargs):
        call_log.append(len(call_log) + 1)
        if len(call_log) <= n_failures:
            raise urllib.error.HTTPError(
                "https://api.stlouisfed.org/fred/series/observations",
                429, "Too Many Requests", None, None,
            )
        return io.BytesIO(_OK_PAYLOAD)

    return _side_effect, call_log


def _run_fetch(n_failures: int):
    """Run _fetch_fred_series against a mocked urlopen; returns (result, call_log)."""
    side_effect, call_log = _make_mock_urlopen(n_failures)
    with patch("conviction.markets_history.time.sleep"), \
         patch("conviction.markets_history.urllib.request.urlopen", side_effect=side_effect):
        result = _fetch_fred_series("TESTKEY", "TEST", full_refresh=True)
    return result, call_log


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
    """
    result, call_log = _run_fetch(n_failures)

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
    """5 consecutive 429s followed by a success on attempt 6."""
    result, call_log = _run_fetch(5)

    assert len(call_log) == 6
    assert not result.empty


# ---------------------------------------------------------------------------
# Edge-case: zero failures (first call succeeds immediately)
# ---------------------------------------------------------------------------

def test_fred_retry_no_failures() -> None:
    """When the first call succeeds, exactly 1 attempt is made and result is non-empty."""
    result, call_log = _run_fetch(0)

    assert len(call_log) == 1
    assert not result.empty


# ---------------------------------------------------------------------------
# Edge-case: 6 failures (exceeds max_retries=5) → empty result, 6 attempts
# ---------------------------------------------------------------------------

def test_fred_retry_exhausted() -> None:
    """All 6 attempts return 429 → gives up, empty Series, exactly 6 calls."""
    result, call_log = _run_fetch(6)

    # max_retries=5 means the loop runs for attempt in range(6): 0,1,2,3,4,5
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
    side_effect, _ = _make_mock_urlopen(3)
    sleep_calls: list[float] = []

    def _record_sleep(secs: float) -> None:
        sleep_calls.append(secs)

    with patch("conviction.markets_history.time.sleep", side_effect=_record_sleep), \
         patch("conviction.markets_history.urllib.request.urlopen", side_effect=side_effect):
        _fetch_fred_series("TESTKEY", "TEST", full_refresh=True)

    # attempt 0 → wait = min(2^0 * 0.5, 30) = 0.5
    # attempt 1 → wait = min(2^1 * 0.5, 30) = 1.0
    # attempt 2 → wait = min(2^2 * 0.5, 30) = 2.0
    expected = [0.5, 1.0, 2.0]
    assert sleep_calls == expected, (
        f"Expected sleep durations {expected}, got {sleep_calls}"
    )


# ---------------------------------------------------------------------------
# REST parsing specifics (new with the urllib port)
# ---------------------------------------------------------------------------

def test_fred_missing_value_marker_dropped() -> None:
    """FRED marks missing data as '.' — those rows must be dropped, not NaN-poison."""
    payload = json.dumps({
        "observations": [
            {"date": "2024-01-31", "value": "."},
            {"date": "2024-02-29", "value": "2.0"},
        ]
    }).encode()

    def _ok(*args, **kwargs):
        return io.BytesIO(payload)

    with patch("conviction.markets_history.time.sleep"), \
         patch("conviction.markets_history.urllib.request.urlopen", side_effect=_ok):
        result = _fetch_fred_series("TESTKEY", "TEST", full_refresh=True)

    assert len(result) == 1
    assert result.iloc[0] == 2.0
