#!/usr/bin/env python3
"""Build and maintain daily ETF OHLCV history cache from Yahoo Finance v8 Chart API.

This script:
1. Dynamically discovers active instruments from `docs/data/flows/catalog.json`.
2. Fetches full or incremental daily OHLCV bars from Yahoo Finance Chart API.
3. Cleans, sanitizes, and geometrically validates bars (deduplication, price bounds, positive volume).
4. Merges with existing cache in `docs/data/flows/etf_history.json` when present.
5. Saves a compact, high-performance JSON artifact for client-side Canvas 2D studies.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import ssl
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CATALOG_PATH = REPO_ROOT / "docs" / "data" / "flows" / "catalog.json"
DEFAULT_OUTPUT_PATH = REPO_ROOT / "docs" / "data" / "flows" / "etf_history.json"
DEFAULT_SPLITS_PATH = REPO_ROOT / "data" / "flows" / "splits.json"
DEFAULT_INDIVIDUAL_DIR = REPO_ROOT / "data" / "flows" / "individual"

DEFAULT_PERIOD1 = 1451606400  # 2016-01-01 00:00:00 UTC
SPLIT_ANOMALY_THRESHOLD = 3.5
BAD_TICK_JUMP_THRESHOLD = 2.2
BAD_TICK_REVERT_LOG_TOL = 0.25
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}


def round_price(val: float) -> float:
    """Format price to sensible precision without float noise."""
    if val >= 1.0:
        return round(val, 2)
    return round(val, 4)


def _normalize_bar_geometry(bar: dict[str, Any]) -> dict[str, Any]:
    """Ensure OHLC geometric invariants hold after scaling or tick repair."""
    o = round_price(float(bar["o"]))
    h = round_price(float(bar["h"]))
    l = round_price(float(bar["l"]))
    c = round_price(float(bar["c"]))
    real_high = round_price(max(h, o, c, l))
    real_low = round_price(min(l, o, c, h))
    return {
        "d": bar["d"],
        "o": o,
        "h": real_high,
        "l": real_low,
        "c": c,
        "v": max(0, int(round(float(bar["v"])))),
    }


def load_known_splits(splits_path: Path = DEFAULT_SPLITS_PATH) -> dict[str, list[dict[str, Any]]]:
    """Load corporate split registry from data/flows/splits.json."""
    if not splits_path.is_file():
        return {}
    try:
        raw = json.loads(splits_path.read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else {}
    except Exception as exc:
        print(f"Notice: Could not parse splits registry ({exc}), proceeding without it.")
        return {}


def load_ticker_nav_map(
    ticker: str,
    individual_dir: Path = DEFAULT_INDIVIDUAL_DIR,
) -> dict[str, dict[str, float]]:
    """Load authoritative daily NAV and perf_pct cross-check map for a ticker."""
    csv_path = individual_dir / f"{ticker}_flows.csv"
    if not csv_path.is_file():
        return {}
    nav_map: dict[str, dict[str, float]] = {}
    try:
        with csv_path.open("r", encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                date_str = str(row.get("date", "")).strip()
                if not date_str:
                    continue
                try:
                    nav_val = float(row.get("nav", "") or 0)
                except ValueError:
                    continue
                if nav_val <= 0:
                    continue
                try:
                    perf_val = float(row.get("perf_pct", "") or 0)
                except ValueError:
                    perf_val = 0.0
                nav_map[date_str] = {"nav": nav_val, "perf_pct": perf_val}
    except Exception:
        return {}
    return nav_map


def apply_split_adjustments_to_bars(
    bars: list[dict[str, Any]],
    ticker: str,
    known_splits: dict[str, list[dict[str, Any]]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Idempotently apply missing known split adjustments in log-space.

    Inspects the boundary price ratio around each split_date using signed log
    distances to verify whether the bar series is already adjusted (~1.0) or
    exhibits an unadjusted / over-adjusted step matching `1/factor` or `factor`.
    """
    events = known_splits.get(ticker)
    if not events or len(bars) < 2:
        return [dict(b) for b in bars], []

    out = [dict(b) for b in bars]
    applied: list[dict[str, Any]] = []

    for ev in sorted(events, key=lambda x: str(x.get("split_date", ""))):
        split_date = str(ev.get("split_date", "")).strip()
        try:
            factor = float(ev.get("factor", 1.0))
        except (TypeError, ValueError):
            continue
        if not split_date or factor <= 0 or abs(factor - 1.0) < 1e-6:
            continue

        boundary = None
        for i, b in enumerate(out):
            if b["d"] < split_date:
                boundary = i
            else:
                break
        if boundary is None or boundary + 1 >= len(out):
            continue

        c_before = float(out[boundary]["c"])
        c_after = float(out[boundary + 1]["c"])
        if c_before <= 0 or c_after <= 0:
            continue

        observed_ratio = c_after / c_before
        log_obs = math.log(observed_ratio)
        # Skip normal daily fluctuations well below any 2:1 / 1:2 split jump
        if abs(log_obs) < 0.50:
            continue

        # Standard Yahoo unadjusted jump is 1/factor; also check `factor` to
        # heal cases where Yahoo over-applied an extra factor to pre-split bars.
        candidates = [1.0 / factor, factor]
        best_ratio = None
        best_err = float("inf")
        for cand in candidates:
            err = abs(log_obs - math.log(cand))
            if err < best_err:
                best_err = err
                best_ratio = cand

        if best_ratio is not None and best_err < 0.35 and best_err < abs(log_obs):
            for i in range(boundary + 1):
                b = out[i]
                b["o"] = float(b["o"]) * best_ratio
                b["h"] = float(b["h"]) * best_ratio
                b["l"] = float(b["l"]) * best_ratio
                b["c"] = float(b["c"]) * best_ratio
                if b["v"] and float(b["v"]) > 0:
                    b["v"] = max(1, int(round(float(b["v"]) / best_ratio)))
                out[i] = _normalize_bar_geometry(b)

            applied.append({
                "ticker": ticker,
                "split_date": split_date,
                "applied_multiplier": round(best_ratio, 6),
                "observed_ratio": round(observed_ratio, 6),
            })

    return out, applied


def sanitize_bad_ticks(
    bars: list[dict[str, Any]],
    ticker: str,
    nav_map: dict[str, dict[str, float]] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Heal 3-bar V-shaped single-day CTA bad ticks without affecting real splits.

    A bar at index `t` is a single-day bad tick when `close[t]` jumps >= 2.2x
    away from `close[t-1]` and `close[t+1]` immediately reverts within 25% of
    `close[t-1]` in log-space. When available, `nav_map` provides the exact
    authoritative replacement price; otherwise midpoint interpolation is used.
    """
    if len(bars) < 3:
        return [dict(b) for b in bars], []

    out = [dict(b) for b in bars]
    healed: list[dict[str, Any]] = []

    for i in range(1, len(out) - 1):
        c_prev = float(out[i - 1]["c"])
        c_curr = float(out[i]["c"])
        c_next = float(out[i + 1]["c"])
        if c_prev <= 0 or c_curr <= 0 or c_next <= 0:
            continue

        jump = max(c_curr / c_prev, c_prev / c_curr)
        if jump < BAD_TICK_JUMP_THRESHOLD:
            continue
        if abs(math.log(c_next / c_prev)) >= BAD_TICK_REVERT_LOG_TOL:
            continue

        # Determine authoritative replacement close
        d_curr = out[i]["d"]
        nav_entry = nav_map.get(d_curr) if nav_map else None
        nav_val = nav_entry.get("nav") if nav_entry else None
        if (
            nav_val
            and nav_val > 0
            and abs(math.log(nav_val / c_prev)) < 0.20
            and abs(math.log(nav_val / c_next)) < 0.20
        ):
            target_c = float(nav_val)
            method = "nav"
        else:
            target_c = (c_prev + c_next) / 2.0
            method = "midpoint"

        b = dict(out[i])
        # Preserve clean O/H/L fields if consistent with target_c, replace outliers
        def _clean_component(val: float, fallback: float) -> float:
            if val <= 0 or abs(math.log(val / target_c)) > 0.25:
                return fallback
            return val

        b["c"] = target_c
        b["o"] = _clean_component(float(b["o"]), target_c)
        b["h"] = _clean_component(float(b["h"]), max(b["o"], target_c))
        b["l"] = _clean_component(float(b["l"]), min(b["o"], target_c))
        out[i] = _normalize_bar_geometry(b)

        healed.append({
            "ticker": ticker,
            "date": d_curr,
            "old_close": c_curr,
            "new_close": out[i]["c"],
            "method": method,
        })

    return out, healed


def _nav_confirms_continuous_scale(
    bars: list[dict[str, Any]],
    idx: int,
    nav_map: dict[str, dict[str, float]] | None,
) -> bool:
    """Return True if authoritative NAV matches OHLCV both before and after `idx`.

    Used to distinguish genuine extreme single-session volatility events (where
    both Trackinsight NAV and Yahoo Close sit on the exact same scale) from
    unadjusted corporate stock splits.
    """
    if not nav_map:
        return False

    curr_d = bars[idx]["d"]
    curr_nav = nav_map.get(curr_d, {}).get("nav")
    if not curr_nav or curr_nav <= 0:
        return False
    if abs(math.log(float(bars[idx]["c"]) / curr_nav)) >= 0.25:
        return False

    # Check up to two prior bars (accounts for T+0 evening futures marks like Volmageddon)
    for lookback in (1, 2):
        if idx - lookback < 0:
            break
        prev_d = bars[idx - lookback]["d"]
        prev_nav = nav_map.get(prev_d, {}).get("nav")
        if prev_nav and prev_nav > 0:
            if abs(math.log(float(bars[idx - lookback]["c"]) / prev_nav)) < 0.25:
                return True
    return False


def detect_and_apply_unknown_splits(
    bars: list[dict[str, Any]],
    ticker: str,
    nav_map: dict[str, dict[str, float]] | None = None,
    threshold: float = SPLIT_ANOMALY_THRESHOLD,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Detect residual permanent price cliffs >= `threshold` and scale pre-split bars."""
    if len(bars) < 2:
        return [dict(b) for b in bars], []

    out = [dict(b) for b in bars]
    detections: list[dict[str, Any]] = []
    lo = 1.0 / threshold
    hi = threshold

    for i in range(1, len(out)):
        c_prev = float(out[i - 1]["c"])
        c_curr = float(out[i]["c"])
        if c_prev <= 0 or c_curr <= 0:
            continue
        ratio = c_curr / c_prev
        if not (ratio >= hi or ratio <= lo):
            continue
        if _nav_confirms_continuous_scale(out, i, nav_map):
            continue

        for j in range(i):
            b = out[j]
            b["o"] = float(b["o"]) * ratio
            b["h"] = float(b["h"]) * ratio
            b["l"] = float(b["l"]) * ratio
            b["c"] = float(b["c"]) * ratio
            if b["v"] and float(b["v"]) > 0:
                b["v"] = max(1, int(round(float(b["v"]) / ratio)))
            out[j] = _normalize_bar_geometry(b)

        detections.append({
            "ticker": ticker,
            "date": out[i]["d"],
            "observed_ratio": round(ratio, 6),
        })

    return out, detections


def has_overlapping_scale_mismatch(
    cached_bars: list[dict[str, Any]],
    new_bars: list[dict[str, Any]],
    log_tol: float = 0.35,
) -> bool:
    """Detect whether incremental Yahoo bars differ in split scale from cached bars."""
    if not cached_bars or not new_bars:
        return False
    cached_by_date = {b["d"]: b for b in cached_bars}
    for nb in new_bars:
        ob = cached_by_date.get(nb["d"])
        if ob and ob.get("c", 0) > 0 and nb.get("c", 0) > 0:
            if abs(math.log(float(nb["c"]) / float(ob["c"]))) > log_tol:
                return True
    return False


def sanitize_ticker_bars(
    bars: list[dict[str, Any]],
    ticker: str,
    known_splits: dict[str, list[dict[str, Any]]] | None = None,
    nav_map: dict[str, dict[str, float]] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Run full 3-stage OHLCV hygiene pipeline (splits -> bad ticks -> unknown splits)."""
    splits = known_splits or {}
    step1, applied_splits = apply_split_adjustments_to_bars(bars, ticker, splits)
    step2, healed_ticks = sanitize_bad_ticks(step1, ticker, nav_map=nav_map)
    step3, unknown_splits = detect_and_apply_unknown_splits(step2, ticker, nav_map=nav_map)
    summary = {
        "applied_splits": applied_splits,
        "healed_ticks": healed_ticks,
        "unknown_splits": unknown_splits,
    }
    return step3, summary


def fetch_yahoo_chart_data(
    ticker: str,
    period1: int,
    period2: int,
    retries: int = 3,
    backoff: float = 2.0,
) -> list[dict[str, Any]]:
    """Fetch and parse daily OHLCV bars for a single ticker with exponential backoff."""
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        f"?period1={period1}&period2={period2}&interval=1d&includePrePost=false"
    )
    req = urllib.request.Request(url, headers=HEADERS)
    ctx = ssl.create_default_context()

    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(req, context=ctx, timeout=15) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                result = data.get("chart", {}).get("result")
                if not result:
                    error_info = data.get("chart", {}).get("error", {})
                    msg = error_info.get("description", "No result array in chart response")
                    raise ValueError(f"Yahoo Chart API error: {msg}")

                chart_obj = result[0]
                timestamps = chart_obj.get("timestamp", [])
                quotes = chart_obj.get("indicators", {}).get("quote", [{}])[0]
                opens = quotes.get("open", [])
                highs = quotes.get("high", [])
                lows = quotes.get("low", [])
                closes = quotes.get("close", [])
                volumes = quotes.get("volume", [])

                bars_by_date: dict[str, dict[str, Any]] = {}
                for idx, ts in enumerate(timestamps):
                    if ts is None:
                        continue
                    o = opens[idx] if idx < len(opens) else None
                    h = highs[idx] if idx < len(highs) else None
                    l = lows[idx] if idx < len(lows) else None
                    c = closes[idx] if idx < len(closes) else None
                    v = volumes[idx] if idx < len(volumes) else None

                    # Strict null / non-finite / non-positive checks
                    if (
                        c is None
                        or v is None
                        or not isinstance(c, (int, float))
                        or not isinstance(v, (int, float))
                        or not math.isfinite(c)
                        or not math.isfinite(v)
                        or c <= 0
                        or v <= 0
                    ):
                        continue

                    # Fallbacks for missing or non-finite partial fields
                    o = o if (isinstance(o, (int, float)) and math.isfinite(o) and o > 0) else c
                    h = h if (isinstance(h, (int, float)) and math.isfinite(h) and h > 0) else max(o, c)
                    l = l if (isinstance(l, (int, float)) and math.isfinite(l) and l > 0) else min(o, c)

                    # Geometric sanity repair
                    real_high = max(h, o, c, l)
                    real_low = min(l, o, c, h)

                    date_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
                    bars_by_date[date_str] = {
                        "d": date_str,
                        "o": round_price(o),
                        "h": round_price(real_high),
                        "l": round_price(real_low),
                        "c": round_price(c),
                        "v": int(round(v)),
                    }

                # Return sorted chronologically
                clean_bars = sorted(bars_by_date.values(), key=lambda x: x["d"])
                return clean_bars

        except Exception as exc:
            last_exc = exc
            if attempt < retries:
                time.sleep(backoff * attempt)

    raise RuntimeError(f"Failed to fetch {ticker} after {retries} attempts: {last_exc}")


def load_catalog_tickers(catalog_path: Path) -> list[str]:
    """Dynamically load tickers from catalog.json without hardcoding."""
    if not catalog_path.is_file():
        raise FileNotFoundError(f"Catalog file not found at {catalog_path}")
    raw = json.loads(catalog_path.read_text(encoding="utf-8"))
    instruments = raw.get("instruments", [])
    tickers = [item["ticker"] for item in instruments if "ticker" in item]
    if not tickers:
        raise ValueError(f"No instruments found in catalog at {catalog_path}")
    return tickers


def load_existing_history(output_path: Path) -> dict[str, list[dict[str, Any]]]:
    """Load existing history cache if present."""
    if not output_path.is_file():
        return {}
    try:
        content = json.loads(output_path.read_text(encoding="utf-8"))
        return content.get("history", {})
    except Exception as exc:
        print(f"Notice: Could not parse existing cache ({exc}), will rebuild from scratch.")
        return {}


def _has_recent_split(ticker: str, known_splits: dict[str, list[dict[str, Any]]], cutoff_date: str) -> bool:
    """Return True if a known split for `ticker` occurred on or after `cutoff_date`."""
    for ev in known_splits.get(ticker, []):
        sd = str(ev.get("split_date", "")).strip()
        if sd and sd >= cutoff_date:
            return True
    return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Build daily ETF OHLCV history cache from Yahoo Finance.")
    parser.add_argument("--catalog-path", type=Path, default=DEFAULT_CATALOG_PATH, help="Path to catalog.json")
    parser.add_argument("--output-path", type=Path, default=DEFAULT_OUTPUT_PATH, help="Path to etf_history.json")
    parser.add_argument("--splits-path", type=Path, default=DEFAULT_SPLITS_PATH, help="Path to splits.json")
    parser.add_argument("--individual-dir", type=Path, default=DEFAULT_INDIVIDUAL_DIR, help="Path to individual NAV CSVs")
    parser.add_argument("--force-all", action="store_true", help="Force complete refetch from 2016 for all tickers")
    parser.add_argument("--repair-cache", action="store_true", help="Sanitize existing cache offline without fetching")
    parser.add_argument("--lookback-days", type=int, default=30, help="Incremental lookback days if cache exists")
    parser.add_argument("--workers", type=int, default=6, help="Concurrent worker threads")
    parser.add_argument("--min-bars", type=int, default=10, help="Minimum bars required for valid instrument")
    args = parser.parse_args()

    tickers = load_catalog_tickers(args.catalog_path)
    print(f"Loaded {len(tickers)} dynamic ETF tickers from {args.catalog_path}")

    known_splits = load_known_splits(args.splits_path)
    existing_payload: dict[str, Any] = {}
    if not args.force_all and args.output_path.is_file():
        try:
            existing_payload = json.loads(args.output_path.read_text(encoding="utf-8"))
        except Exception:
            existing_payload = {}
    existing_history = {} if args.force_all else existing_payload.get("history", {})
    now_ts = int(datetime.now(timezone.utc).timestamp()) + 86400  # Buffer 1 day

    results: dict[str, list[dict[str, Any]]] = {}
    errors: dict[str, str] = {}
    repair_logs: list[str] = []

    def process_ticker(ticker: str) -> tuple[str, list[dict[str, Any]], str | None]:
        cached_bars = existing_history.get(ticker, [])
        nav_map = load_ticker_nav_map(ticker, args.individual_dir)

        if args.repair_cache:
            if not cached_bars:
                return ticker, [], "No cached bars present for offline repair"
            final_bars, summary = sanitize_ticker_bars(cached_bars, ticker, known_splits, nav_map=nav_map)
        else:
            use_incremental = bool(cached_bars and not args.force_all)
            if use_incremental:
                last_date_str = cached_bars[-1]["d"]
                try:
                    last_dt = datetime.strptime(last_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                    start_ts = max(DEFAULT_PERIOD1, int(last_dt.timestamp()) - (args.lookback_days * 86400))
                except Exception:
                    start_ts = now_ts - (args.lookback_days * 86400)
                cutoff_str = datetime.fromtimestamp(start_ts, tz=timezone.utc).strftime("%Y-%m-%d")
                if _has_recent_split(ticker, known_splits, cutoff_str):
                    use_incremental = False
                    start_ts = DEFAULT_PERIOD1
            else:
                start_ts = DEFAULT_PERIOD1

            try:
                new_bars = fetch_yahoo_chart_data(ticker, period1=start_ts, period2=now_ts)
                if use_incremental:
                    if has_overlapping_scale_mismatch(cached_bars, new_bars):
                        print(f"Notice: Split scale mismatch detected on overlap for {ticker}; refetching full history.")
                        final_bars = fetch_yahoo_chart_data(ticker, period1=DEFAULT_PERIOD1, period2=now_ts)
                    else:
                        merged: dict[str, dict[str, Any]] = {b["d"]: b for b in cached_bars}
                        for b in new_bars:
                            merged[b["d"]] = b
                        final_bars = sorted(merged.values(), key=lambda x: x["d"])
                else:
                    final_bars = new_bars
            except Exception as exc:
                if cached_bars:
                    print(f"Warning: Fetch failed for {ticker} ({exc}), retaining {len(cached_bars)} cached bars.")
                    final_bars = cached_bars
                else:
                    return ticker, [], str(exc)

            final_bars, summary = sanitize_ticker_bars(final_bars, ticker, known_splits, nav_map=nav_map)

        for ev in summary["applied_splits"]:
            repair_logs.append(
                f"SPLIT {ev['ticker']} {ev['split_date']} mult={ev['applied_multiplier']} (obs={ev['observed_ratio']})"
            )
        for tk in summary["healed_ticks"]:
            repair_logs.append(
                f"BAD_TICK {tk['ticker']} {tk['date']} {tk['old_close']}->{tk['new_close']} ({tk['method']})"
            )
        for uk in summary["unknown_splits"]:
            repair_logs.append(
                f"UNKNOWN_SPLIT {uk['ticker']} {uk['date']} obs={uk['observed_ratio']}"
            )
        return ticker, final_bars, None

    t0 = time.time()
    mode_label = "Repairing cached OHLCV" if args.repair_cache else "Fetching OHLCV"
    print(f"{mode_label} for {len(tickers)} ETFs with {args.workers} workers...")
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(process_ticker, t): t for t in tickers}
        done_count = 0
        for fut in as_completed(futures):
            t, bars, err = fut.result()
            done_count += 1
            if err:
                errors[t] = err
                print(f"[{done_count}/{len(tickers)}] [FAIL] {t}: {err}")
            else:
                results[t] = bars
                if done_count % 20 == 0 or done_count == len(tickers):
                    print(f"[{done_count}/{len(tickers)}] [OK] Processed {t} ({len(bars)} bars)")

    elapsed = time.time() - t0
    print(f"\nCompleted in {elapsed:.1f}s. Success: {len(results)}, Errors: {len(errors)}")
    if repair_logs:
        print(f"OHLCV Hygiene Actions ({len(repair_logs)}):")
        for msg in sorted(repair_logs):
            print(f"  - {msg}")

    if errors:
        print(f"Warning: {len(errors)} tickers had fetch errors: {list(errors.keys())}")
        if len(results) < len(tickers) * 0.9:
            print("Error: Too many tickers failed to fetch OHLCV data.", file=sys.stderr)
            return 1

    # Preserve catalog ticker order for deterministic output
    ordered_results = {t: results[t] for t in tickers if t in results}
    updated_utc = (
        existing_payload.get("updated_utc")
        if args.repair_cache and existing_payload.get("updated_utc")
        else datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    )

    payload = {
        "schema_version": 1,
        "updated_utc": updated_utc,
        "instrument_count": len(ordered_results),
        "history": ordered_results,
    }

    args.output_path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(payload, separators=(",", ":"))
    args.output_path.write_text(serialized, encoding="utf-8")
    file_size_mb = args.output_path.stat().st_size / (1024 * 1024)
    print(f"Wrote {len(ordered_results)} instrument histories to {args.output_path} ({file_size_mb:.2f} MB)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
