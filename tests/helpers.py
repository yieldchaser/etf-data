"""
Shared test utilities for the Predator Protocol test suite.

Import from here instead of duplicating across test files.
"""
from __future__ import annotations


def make_monthly_list(start_ym: str, n_months: int, start_val: float = 100.0, step: float = 1.0) -> list[list]:
    """Generate n_months of [[YYYY-MM, val], ...] starting from start_ym."""
    result = []
    yr, mo = int(start_ym[:4]), int(start_ym[5:7])
    val = start_val
    for _ in range(n_months):
        result.append([f"{yr:04d}-{mo:02d}", round(val, 4)])
        val += step
        mo += 1
        if mo > 12:
            mo = 1
            yr += 1
    return result


def make_asset(name: str, category: str, monthly: list[list]) -> dict:
    """Build a §2.3 asset dict."""
    return {
        "meta": {
            "name": name,
            "category": category,
            "native_ccy": "USD",
            "source": "test",
            "first": monthly[0][0],
            "last": monthly[-1][0],
        },
        "monthly": monthly,
    }


def make_json(assets: dict, asof: str = "2026-05") -> dict:
    """Build a minimal §2.3 contract JSON."""
    return {
        "asof": asof,
        "generated_utc": "2026-05-24T10:00:00+00:00",
        "assets": assets,
        "fx": {},
        "cpi": {},
        "rates": {},
        "events": [],
    }
