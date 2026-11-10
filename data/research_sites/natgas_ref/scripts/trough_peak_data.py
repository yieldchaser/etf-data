#!/usr/bin/env python3
"""
Trough-to-Peak Historical Data Builder
=======================================
Fetches full daily close prices from 2020-01-01 for all 6 NAT GAS ETFs
and writes docs/data/trough_peak_data.json for the client-side analyzer.
Reuses the same Yahoo Finance v8 fetch pattern as data_pipeline.py.
"""

import json
import logging
import math
import ssl
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("trough_peak_data")

PROJECT_ROOT     = Path(__file__).resolve().parent.parent
OUT_FILE         = PROJECT_ROOT / "docs" / "data" / "trough_peak_data.json"
KNOWN_SPLITS_JSON = PROJECT_ROOT / "data" / "known_splits.json"

YAHOO_URL  = "https://query1.finance.yahoo.com/v8/finance/chart/"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
START_TS   = int(datetime(2008, 1, 1).timestamp())
TICKERS    = ["KOLD", "BOIL", "HNU.TO", "HND.TO", "3NGL.L", "3NGS.L"]
MAX_TRIES  = 3

# Any single-day price ratio >= this is treated as an unregistered corporate action.
SPLIT_ANOMALY_THRESHOLD = 4.0

# Fallback seed used only when known_splits.json is absent (e.g. fresh clone).
_SPLITS_SEED: Dict[str, List[Tuple[str, float]]] = {
    "3NGL.L": [
        ("2016-03-18", 10.0), ("2019-02-25", 10.0), ("2020-04-17", 10.0),
        ("2023-03-27", 10.0), ("2024-01-12", 10.0), ("2024-07-22", 420.0),
        ("2024-09-09", 10.0), ("2026-03-03", 10.0),
    ],
    "3NGS.L": [
        ("2019-06-04", 10.0), ("2021-09-15", 10.0), ("2022-05-30", 10.0),
        ("2022-09-12", 10.0), ("2022-12-19", 17000.0), ("2024-07-22", 1.0 / 17),
    ],
}


def _load_known_splits() -> Dict[str, List[Tuple[str, float]]]:
    """Load split history from shared data/known_splits.json.

    data_pipeline.py runs first in the workflow, so by the time this script
    executes any newly auto-detected splits are already in the file.
    Falls back to hardcoded seed if the file is absent or corrupt.
    """
    if KNOWN_SPLITS_JSON.exists():
        try:
            with open(KNOWN_SPLITS_JSON) as f:
                raw = json.load(f)
            return {
                ticker: [(e["date"], float(e["ratio"])) for e in entries]
                for ticker, entries in raw.get("splits", {}).items()
            }
        except Exception as exc:
            log.warning("Could not load %s (%s) — using seed", KNOWN_SPLITS_JSON.name, exc)
    return {k: list(v) for k, v in _SPLITS_SEED.items()}


def _save_known_splits(splits: Dict[str, List[Tuple[str, float]]]) -> None:
    """Persist updated splits back to data/known_splits.json."""
    KNOWN_SPLITS_JSON.parent.mkdir(parents=True, exist_ok=True)
    payload: dict = {
        "note": (
            "Auto-managed by data_pipeline.py. New corporate actions are appended "
            "automatically when detected from price data. "
            "'source': 'manual' = verified from WisdomTree/LSE filings; "
            "'auto_detected' = inferred from price discontinuity."
        ),
        "splits": {
            ticker: [{"date": d, "ratio": r} for d, r in sorted(entries)]
            for ticker, entries in sorted(splits.items())
        },
    }
    with open(KNOWN_SPLITS_JSON, "w") as f:
        json.dump(payload, f, indent=2)
    log.info("Saved known splits → %s", KNOWN_SPLITS_JSON.name)


def _apply_split_adjustments(
    dates: List[str],
    closes: List[float],
    volumes: List[int],
    ticker: str,
    known_splits: Dict[str, List[Tuple[str, float]]],
) -> tuple:
    """Apply known split adjustments from known_splits.json to price/volume lists.

    For each event we inspect the price ratio at the boundary in log-space to
    detect whether Yahoo has already reflected it.  Only missing adjustments are
    applied, preventing double-counting.
    """
    splits = known_splits.get(ticker)
    if not splits:
        return dates, closes, volumes

    closes  = list(closes)
    volumes = list(volumes)

    for date_str, ratio in splits:
        # Find index boundary: last index where date < split_date
        boundary = None
        for i, d in enumerate(dates):
            if d < date_str:
                boundary = i
            else:
                break
        if boundary is None or boundary < 0:
            continue  # No data before this split
        post_idx = boundary + 1
        if post_idx >= len(dates):
            continue

        price_before = closes[boundary]
        price_after  = closes[post_idx]
        if not price_before or price_before <= 0:
            continue

        observed_ratio = price_after / price_before

        # Log-space detection: closer to 1.0 → already applied; closer to ratio → not applied
        log_observed = abs(math.log(observed_ratio))
        log_expected = abs(math.log(ratio))
        already_applied = abs(log_observed) < abs(log_observed - log_expected)

        if not already_applied:
            log.info(
                "Applying split adjustment for %s: ×%.4g on %s "
                "(observed jump ×%.4g vs expected ×%.4g)",
                ticker, ratio, date_str, observed_ratio, ratio,
            )
            for i in range(boundary + 1):
                closes[i]  = round(closes[i] * ratio, 4)
                volumes[i] = int(volumes[i] / ratio) if volumes[i] else 0
        else:
            log.debug(
                "Split %s ×%.4g on %s already reflected in Yahoo data — skipping",
                ticker, ratio, date_str,
            )

    return dates, closes, volumes


def _detect_and_apply_unknown_splits(
    dates: List[str],
    closes: List[float],
    volumes: List[int],
    ticker: str,
) -> Tuple[List[str], List[float], List[int], List[dict]]:
    """Scan for large price discontinuities not in MANUAL_SPLITS and auto-apply them.

    Returns adjusted (dates, closes, volumes) plus a list of detection dicts.
    """
    closes  = list(closes)
    volumes = list(volumes)
    detections: List[dict] = []

    lo = 1.0 / SPLIT_ANOMALY_THRESHOLD
    hi = SPLIT_ANOMALY_THRESHOLD

    for i in range(1, len(closes)):
        if closes[i - 1] <= 0:
            continue
        try:
            ratio = closes[i] / closes[i - 1]
        except ZeroDivisionError:
            continue
        if not (ratio >= hi or ratio <= lo):
            continue

        date_str = dates[i]
        log.warning(
            "AUTO-DETECTED unregistered split for %s on %s: observed ×%.4g — "
            "auto-applying and persisting to known_splits.json.",
            ticker, date_str, ratio,
        )
        for j in range(i):
            closes[j]  = round(closes[j] * ratio, 4)
            volumes[j] = int(volumes[j] / ratio) if volumes[j] else 0

        detections.append({
            "ticker": ticker,
            "date": date_str,
            "observed_ratio": round(float(ratio), 6),
            "direction": "reverse_split" if ratio > 1 else "forward_split",
            "auto_applied": True,
            "action_required": (
                f"Add (\"{date_str}\", {round(float(ratio), 4)}) "
                f"to MANUAL_SPLITS[\"{ticker}\"] in data_pipeline.py and trough_peak_data.py"
            ),
        })

    return dates, closes, volumes, detections


def fetch_ticker_data(ticker: str, known_splits: Dict[str, List[Tuple[str, float]]]):
    period2 = int(datetime.now().timestamp())
    url = (
        f"{YAHOO_URL}{urllib.request.quote(ticker)}"
        f"?period1={START_TS}&period2={period2}&interval=1d&includePrePost=false"
    )
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    for attempt in range(1, MAX_TRIES + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            resp = urllib.request.urlopen(req, context=ctx, timeout=30)
            raw = json.loads(resp.read())
            
            if not raw.get("chart", {}).get("result"):
                log.warning("No result found for %s", ticker)
                return None
                
            result = raw["chart"]["result"][0]
            timestamps = result.get("timestamp", [])
            indicators = result.get("indicators", {})
            quote = indicators.get("quote", [{}])[0]
            
            # Prefer adjclose (adjusted for splits/dividends)
            adj_indicators = indicators.get("adjclose", [{}])[0]
            adj_closes = adj_indicators.get("adjclose", [])
            
            # Fallback to standard close if adjclose is empty or missing
            closes = adj_closes if adj_closes else quote.get("close", [])
            volumes = quote.get("volume", [])

            dates, prices, vols = [], [], []
            for i in range(len(timestamps)):
                c = closes[i] if i < len(closes) else None
                v = volumes[i] if i < len(volumes) else None
                ts = timestamps[i]
                
                # Cleaning/Forward-fill logic: if c is None, skip row
                if c is None or c <= 0:
                    continue
                
                # Volume can be None or 0, we treat it as 0
                vol_val = int(v) if v is not None else 0
                
                date_str = datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d")
                # Deduplicate: Yahoo sometimes returns two records for the same date
                # (intraday estimate + official close). Keep the last (most up-to-date).
                if dates and dates[-1] == date_str:
                    prices[-1] = round(float(c), 4)
                    vols[-1] = vol_val
                else:
                    dates.append(date_str)
                    prices.append(round(float(c), 4))
                    vols.append(vol_val)

            log.info("%s: %d rows fetched (adjusted prices)", ticker, len(dates))

            # Apply known split adjustments (from data/known_splits.json)
            dates, prices, vols = _apply_split_adjustments(dates, prices, vols, ticker, known_splits)

            # Auto-detect and apply any remaining unregistered splits
            dates, prices, vols, detected = _detect_and_apply_unknown_splits(
                dates, prices, vols, ticker
            )

            return {
                "dates": dates,
                "closes": prices,
                "volumes": vols,
                "_detected_splits": detected,   # internal; stripped before writing JSON
            }

        except Exception as e:
            log.warning("Attempt %d/%d for %s failed: %s", attempt, MAX_TRIES, ticker, e)
            if attempt < MAX_TRIES:
                time.sleep(2 * attempt)

    log.error("All attempts failed for %s", ticker)
    return None


def main():
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    # Load shared known_splits.json (data_pipeline.py runs first in the workflow
    # and may have already added newly detected splits to this file)
    known_splits = _load_known_splits()

    output = {"tickers": {}}
    all_detected: List[dict] = []

    for ticker in TICKERS:
        data = fetch_ticker_data(ticker, known_splits)
        if data:
            # Strip internal key before writing to JSON
            detected = data.pop("_detected_splits", [])
            if detected:
                # Merge into the in-memory dict for persistence
                for event in detected:
                    bucket = known_splits.setdefault(event["ticker"], [])
                    existing = {d for d, _ in bucket}
                    if event["date"] not in existing:
                        bucket.append((event["date"], event["observed_ratio"]))
                        bucket.sort()
            all_detected.extend(detected)
            output["tickers"][ticker] = data
        time.sleep(1)  # polite delay between requests

    with open(OUT_FILE, "w") as f:
        json.dump(output, f, separators=(",", ":"))
    log.info("Written to %s", OUT_FILE)

    # Persist any newly discovered splits so future runs treat them as known
    if all_detected:
        _save_known_splits(known_splits)
        log.warning(
            "SPLIT WARNINGS: %d unregistered split(s) auto-applied and saved to %s",
            len(all_detected), KNOWN_SPLITS_JSON.name,
        )


if __name__ == "__main__":
    main()
