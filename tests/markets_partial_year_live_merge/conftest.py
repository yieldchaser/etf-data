"""Pytest config and shared fixtures for the markets partial-year & live-merge bugfix suite.

Hypothesis settings: keep deadlines generous and runs deterministic.
Shared fixtures: load `market_returns.json` / `metadata.json` once per session.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from hypothesis import HealthCheck, settings


# A "fast" profile (used by default) with conservative health checks.
settings.register_profile(
    "preservation",
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
    max_examples=50,
)
settings.load_profile("preservation")


REPO_ROOT = Path(__file__).resolve().parents[2]
MARKET_RETURNS_PATH = REPO_ROOT / "docs" / "data" / "market_returns.json"
METADATA_PATH = REPO_ROOT / "docs" / "data" / "metadata.json"


@pytest.fixture(scope="session")
def market_returns():
    """The full `docs/data/market_returns.json` payload."""
    if not MARKET_RETURNS_PATH.exists():
        pytest.skip(f"{MARKET_RETURNS_PATH} not found — run the markets pipeline first")
    return json.loads(MARKET_RETURNS_PATH.read_text(encoding="utf-8"))


@pytest.fixture(scope="session")
def metadata():
    """The full `docs/data/metadata.json` payload."""
    if not METADATA_PATH.exists():
        pytest.skip(f"{METADATA_PATH} not found — run the build first")
    return json.loads(METADATA_PATH.read_text(encoding="utf-8"))


@pytest.fixture(scope="session")
def asof_year_month(market_returns):
    """``(asof_year, asof_month)`` parsed from ``market_returns.asof``."""
    asof = market_returns.get("asof", "")
    if not asof or len(asof) < 7:
        pytest.skip("market_returns.json missing asof")
    return int(asof[:4]), int(asof[5:7])
