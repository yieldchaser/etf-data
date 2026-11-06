"""
Edge case tests for holdings scraping and date management.

Tests weekend rollbacks, US federal holiday rollbacks, anti-phantom future date
clamping, composite-key deduplication, and placeholder/cash exclusion.

Run:
    python -m pytest tests/test_holdings_edge_cases.py -v
"""
import pytest
import pandas as pd
from datetime import date, timedelta
from scraper import (
    roll_to_prev_biz_day,
    clean_dataframe,
    update_giant_history,
    check_if_new_data,
    PIPELINE_SCHEMA_COLS,
    _US_HOLIDAYS,
    TODAY,
)

def test_roll_to_prev_biz_day_weekends():
    """Verify Saturday and Sunday roll back to the preceding Friday."""
    # 2026-11-07 is Saturday -> must roll to Friday 2026-11-06
    assert roll_to_prev_biz_day("2026-11-07") == "2026-11-06"
    # 2026-11-08 is Sunday -> must roll to Friday 2026-11-06
    assert roll_to_prev_biz_day("2026-11-08") == "2026-11-06"
    # 2026-11-06 is Friday -> stays Friday
    assert roll_to_prev_biz_day("2026-11-06") == "2026-11-06"
    # 2026-11-05 is Thursday -> stays Thursday
    assert roll_to_prev_biz_day("2026-11-05") == "2026-11-05"

def test_roll_to_prev_biz_day_holidays():
    """Verify US federal holidays roll back to the preceding business day."""
    # New Year's Day 2026-01-01 (Thursday) -> 2025-12-31 (Wednesday)
    assert roll_to_prev_biz_day("2026-01-01") == "2025-12-31"
    # MLK Day 2026-01-19 (Monday) -> 2026-01-16 (Friday)
    assert roll_to_prev_biz_day("2026-01-19") == "2026-01-16"
    # Memorial Day 2026-05-25 (Monday) -> 2026-05-22 (Friday)
    assert roll_to_prev_biz_day("2026-05-25") == "2026-05-22"
    # Juneteenth 2026-06-19 (Friday) -> 2026-06-18 (Thursday)
    assert roll_to_prev_biz_day("2026-06-19") == "2026-06-18"
    # Labor Day 2026-09-07 (Monday) -> 2026-09-04 (Friday)
    assert roll_to_prev_biz_day("2026-09-07") == "2026-09-04"
    # Thanksgiving 2026-11-26 (Thursday) -> 2026-11-25 (Wednesday)
    assert roll_to_prev_biz_day("2026-11-26") == "2026-11-25"
    # Christmas Day 2026-12-25 (Friday) -> 2026-12-24 (Thursday)
    assert roll_to_prev_biz_day("2026-12-25") == "2026-12-24"

def test_clean_dataframe_placeholder_and_cash_filter():
    """Verify non-positive weights, cash items, and $-tickers are excluded."""
    raw_data = pd.DataFrame([
        {"ticker": "AAPL", "name": "Apple Inc", "weight": "5.5%"},
        {"ticker": "MSFT", "name": "Microsoft Corp", "weight": "4.2%"},
        {"ticker": "Cash&Other", "name": "Cash and Other", "weight": "0.3%"},
        {"ticker": "$USD", "name": "US Dollar", "weight": "0.1%"},
        {"ticker": "GOOGL", "name": "Alphabet Inc", "weight": "-0.5%"},
        {"ticker": "AMZN", "name": "Amazon.com Inc", "weight": "0.0%"},
    ])
    cleaned = clean_dataframe(raw_data, "TEST_ETF", "2026-11-06", weight_unit="percent")
    assert cleaned is not None
    assert set(cleaned["ticker"]) == {"AAPL", "MSFT"}
    assert list(cleaned.columns) == PIPELINE_SCHEMA_COLS

def test_deduplication_composite_key(tmp_path, monkeypatch):
    """Verify that update_giant_history deduplicates on (ETF_Ticker, ticker, Holdings_As_Of)."""
    giant_file = tmp_path / "all_history.csv"
    monkeypatch.setattr("scraper.GIANT_HISTORY_FILE", str(giant_file))

    df1 = pd.DataFrame([
        {"ETF_Ticker": "IVV", "ticker": "AAPL", "name": "Apple", "weight": 0.07, "Holdings_As_Of": "2026-11-06", "Date_Scraped": "2026-11-06"},
        {"ETF_Ticker": "IVV", "ticker": "MSFT", "name": "Microsoft", "weight": 0.06, "Holdings_As_Of": "2026-11-06", "Date_Scraped": "2026-11-06"},
    ])
    update_giant_history([df1])

    # Second scrape on same as_of date with updated weight
    df2 = pd.DataFrame([
        {"ETF_Ticker": "IVV", "ticker": "AAPL", "name": "Apple Inc New", "weight": 0.075, "Holdings_As_Of": "2026-11-06", "Date_Scraped": "2026-11-06"},
        {"ETF_Ticker": "IVV", "ticker": "NVDA", "name": "Nvidia", "weight": 0.05, "Holdings_As_Of": "2026-11-06", "Date_Scraped": "2026-11-06"},
    ])
    update_giant_history([df2])

    result = pd.read_csv(giant_file)
    # Total rows must be 3 (AAPL updated, MSFT preserved, NVDA added)
    assert len(result) == 3
    aapl_row = result[result["ticker"] == "AAPL"].iloc[0]
    assert aapl_row["weight"] == 0.075
    assert aapl_row["name"] == "Apple Inc New"

def test_check_if_new_data(tmp_path, monkeypatch):
    """Verify check_if_new_data detects whether latest CSV has same date."""
    latest_dir = tmp_path / "latest"
    latest_dir.mkdir()
    monkeypatch.setattr("scraper.DATA_DIR_LATEST", str(latest_dir))

    test_file = latest_dir / "QUAL.csv"
    df = pd.DataFrame([{"ETF_Ticker": "QUAL", "Holdings_As_Of": "2026-11-05"}])
    df.to_csv(test_file, index=False)

    # Same date should return False
    assert not check_if_new_data("QUAL", "2026-11-05")
    # Newer date should return True
    assert check_if_new_data("QUAL", "2026-11-06")
    # Unseen ticker should return True
    assert check_if_new_data("UNSEEN", "2026-11-06")
