"""Regression tests for the two defects surfaced by the 2026-09-26 audit.

1. MONEY-MARKET SWEEP LEAK (scraper ingress)
   Pacer's holdings CSV carries a `MoneyMarketFlag` column. Rows flagged `Y`
   are cash instruments, not equity holdings — the CALF/COWZ files were
   ingesting `USBFS03` ("U.S. Bank Money Market Deposit Account 06/01/2031")
   as a holding. The build-time sanitizer hid it from the UI, so 293 poisoned
   rows accumulated in the durable parquet store between 2026-02-13 and
   2026-09-25 while every dashboard looked correct.

   Fix: drop flagged rows at ingress, in `clean_dataframe`, so no path can
   write them. Kept deliberately generic (flag == 'Y') rather than a
   USBFS03 blocklist entry: a sweep ticker renames over time, and the issuer
   already labels it.

2. LOCALE-DEPENDENT NUMBER FORMATTING (dashboard)
   `docs/index.html`, `docs/markets.html` and `docs/stock.html` call bare
   `toLocaleString()`, which formats in the *visitor's* locale. A visitor
   on an en-IN browser sees the holdings-row count as `6,40,738` instead of
   `640,738`. Sibling call sites in the same files already pin `'en-US'`;
   these were missed.

TDD: both tests are RED before the fix.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import pandas as pd
import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

import scraper as scr  # noqa: E402


# --------------------------------------------------------------------------
# 1. Money-market sweep rows must never be ingested
# --------------------------------------------------------------------------

# Shape taken verbatim from the live Pacer CALF file (2026-09-28).
PACER_ROWS = [
    {"Date": "09/28/2026", "Account": "CALF", "StockTicker": "ADNT",
     "SecurityName": "Adient PLC", "Weightings": "0.23%",
     "MoneyMarketFlag": ""},
    {"Date": "09/28/2026", "Account": "CALF", "StockTicker": "USBFS03",
     "SecurityName": "U.S. Bank Money Market Deposit Account 06/01/2031",
     "Weightings": "0.07%", "MoneyMarketFlag": "Y"},
    {"Date": "09/28/2026", "Account": "CALF", "StockTicker": "Cash&Other",
     "SecurityName": "Cash & Other", "Weightings": "0.05%",
     "MoneyMarketFlag": "Y"},
]


def test_money_market_flagged_rows_are_dropped():
    df = scr.clean_dataframe(pd.DataFrame(PACER_ROWS), "CALF",
                             "2026-09-28", weight_unit="percent")
    assert df is not None
    assert "USBFS03" not in set(df["ticker"]), \
        "money-market sweep row leaked into holdings"
    assert "Cash&Other" not in set(df["ticker"]), \
        "cash aggregate must also be dropped"
    assert set(df["ticker"]) == {"ADNT"}, "only the real equity row survives"


def test_unflagged_positive_weight_rows_are_kept():
    """Regression guard: the filter must not eat legitimate holdings."""
    rows = [
        {"StockTicker": "AAA", "SecurityName": "Alpha", "Weightings": "1.00%",
         "MoneyMarketFlag": ""},
        {"StockTicker": "PEN", "SecurityName": "Penumbra", "Weightings": "0.50%",
         "MoneyMarketFlag": ""},
    ]
    df = scr.clean_dataframe(pd.DataFrame(rows), "X", "2026-09-28",
                             weight_unit="percent")
    assert set(df["ticker"]) == {"AAA", "PEN"}


def test_money_market_filter_tolerates_missing_flag_column():
    """Issuers without a MoneyMarketFlag column must not crash or drop all."""
    rows = [{"StockTicker": "AAA", "SecurityName": "Alpha",
             "Weightings": "1.00%"}]
    df = scr.clean_dataframe(pd.DataFrame(rows), "X", "2026-09-28",
                             weight_unit="percent")
    assert set(df["ticker"]) == {"AAA"}


# --------------------------------------------------------------------------
# 2. Number formatting must not depend on the visitor's locale
# --------------------------------------------------------------------------

PAGES = ["index.html", "markets.html", "stock.html"]

# `.toLocaleString()` with no locale argument (option bag counts as pinned).
_BARE = re.compile(r"\.toLocaleString\(\s*\)")


@pytest.mark.parametrize("page", PAGES)
def test_no_bare_to_localestring_in_dashboard_pages(page: str):
    """Bare toLocaleString() renders in the visitor's locale (en-IN → 6,40,738)."""
    html = (REPO / "docs" / page).read_text(encoding="utf-8")
    offenders = [
        f"line {i}: {line.strip()}"
        for i, line in enumerate(html.splitlines(), 1)
        if _BARE.search(line)
    ]
    assert not offenders, (
        f"{page}: bare toLocaleString() must pin 'en-US' so digits group "
        f"consistently for every visitor:\n" + "\n".join(offenders)
    )


def test_number_formatting_sibling_stays_pinned():
    """The already-correct sibling call must not regress."""
    html = (REPO / "docs" / "index.html").read_text(encoding="utf-8")
    assert "toLocaleString('en-US'" in html
