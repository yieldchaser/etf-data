"""Live-fetch diagnostic for ^DJI / ^GSPC / ^NDX / NASDAQCOM.

The orchestrator's Wave 0 surfaced the following question:
  - Is the bug in the merge relabel logic (yfinance returns rows but build_output
    forgets to write the live source label), OR
  - Is yfinance/FRED genuinely returning empty for these tickers right now?

This script mirrors the production fetch path exactly:
  - yfinance: yf.download(ticker, period="max", interval="1mo", progress=False)
  - FRED:     Fred(...).get_series(series_id, frequency="m", aggregation_method="eop")

It writes the result to `tests/markets_partial_year_live_merge/live_fetch_diagnosis.json`
and prints a one-line verdict per ticker. It does NOT touch any production data.
"""
from __future__ import annotations

import json
import os
import sys
import traceback
from pathlib import Path

OUT = Path(__file__).parent / "live_fetch_diagnosis.json"

YFINANCE_TICKERS = ["^DJI", "^GSPC", "^NDX"]
FRED_SERIES = ["NASDAQCOM"]


def _try_yfinance(ticker: str) -> dict:
    try:
        import yfinance as yf
        import pandas as pd
    except Exception as e:
        return {"ticker": ticker, "provider": "yfinance",
                "ok": False, "rows": 0, "error": f"import: {type(e).__name__}: {e}"}

    info: dict = {"ticker": ticker, "provider": "yfinance"}
    try:
        data = yf.download(ticker, period="max", interval="1mo", progress=False)
    except Exception as e:
        info.update({"ok": False, "rows": 0,
                     "error": f"download: {type(e).__name__}: {e}",
                     "traceback": traceback.format_exc(limit=2)})
        return info

    if data is None or data.empty:
        info.update({"ok": False, "rows": 0, "error": "yf.download returned empty DataFrame"})
        return info

    # extract Close column with multiindex handling (mirror _fetch_yfinance_series)
    try:
        if isinstance(data.columns, pd.MultiIndex):
            close = data[("Close", ticker)] if ("Close", ticker) in data.columns else data["Close"].iloc[:, 0]
        else:
            close = data["Close"]
        close = close.dropna()
    except Exception as e:
        info.update({"ok": False, "rows": 0,
                     "error": f"extract Close: {type(e).__name__}: {e}",
                     "raw_columns": [str(c) for c in list(data.columns)[:6]]})
        return info

    if close.empty:
        info.update({"ok": False, "rows": 0, "error": "Close column dropna empty"})
        return info

    info.update({
        "ok": True,
        "rows": int(len(close)),
        "first": str(close.index.min()),
        "last": str(close.index.max()),
        "last_value": float(close.iloc[-1]),
    })
    return info


def _try_fred(series_id: str) -> dict:
    info: dict = {"ticker": series_id, "provider": "fred"}
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except Exception:
        pass
    api_key = os.environ.get("FRED_API_KEY")
    if not api_key:
        info.update({"ok": False, "rows": 0, "error": "FRED_API_KEY not set in env"})
        return info
    try:
        from fredapi import Fred
    except Exception as e:
        info.update({"ok": False, "rows": 0, "error": f"import fredapi: {e}"})
        return info
    try:
        fred = Fred(api_key=api_key)
        data = fred.get_series(series_id, frequency="m", aggregation_method="eop")
    except Exception as e:
        info.update({"ok": False, "rows": 0,
                     "error": f"get_series: {type(e).__name__}: {e}",
                     "traceback": traceback.format_exc(limit=2)})
        return info
    if data is None or data.empty:
        info.update({"ok": False, "rows": 0, "error": "fred.get_series returned empty"})
        return info
    info.update({
        "ok": True,
        "rows": int(len(data)),
        "first": str(data.index.min()),
        "last": str(data.index.max()),
        "last_value": float(data.iloc[-1]),
    })
    return info


def main() -> None:
    results = []
    print("=" * 70)
    print("LIVE-FETCH DIAGNOSTIC")
    print("=" * 70)
    for tk in YFINANCE_TICKERS:
        print(f"\n[yfinance] {tk} ...")
        r = _try_yfinance(tk)
        results.append(r)
        if r["ok"]:
            print(f"  OK rows={r['rows']} first={r['first']} last={r['last']} last_value={r['last_value']:.4f}")
        else:
            print(f"  EMPTY/FAIL — {r.get('error')}")
    for sid in FRED_SERIES:
        print(f"\n[fred] {sid} ...")
        r = _try_fred(sid)
        results.append(r)
        if r["ok"]:
            print(f"  OK rows={r['rows']} first={r['first']} last={r['last']} last_value={r['last_value']:.4f}")
        else:
            print(f"  EMPTY/FAIL — {r.get('error')}")

    payload = {"results": results}

    # verdict
    yf_ok = all(r["ok"] for r in results if r["provider"] == "yfinance")
    fred_ok = all(r["ok"] for r in results if r["provider"] == "fred")
    if yf_ok and fred_ok:
        verdict = "all live fetches return rows — bug is in build_output relabel logic; fix the merge so all four carry live sources"
    else:
        empty = [r["ticker"] for r in results if not r["ok"]]
        verdict = f"genuine live-fetch holdouts: {empty} — keep honest Excel label and name them in metadata.markets_data_freshness.self_living_check.holdouts"
    payload["verdict"] = verdict
    print(f"\nVERDICT: {verdict}")

    OUT.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
