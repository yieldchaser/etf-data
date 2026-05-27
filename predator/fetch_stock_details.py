"""
Predator Protocol — Stock Details Builder (Self-Healing Coverage Engine)
========================================================================
Fetches per-ticker price history + company description, writing each to
docs/data/details/{TICKER}.json

Coverage grows automatically each CI run via a bounded batch approach.
Pending tickers get stub files so the UI shows honest state (no 404s).

Rules:
  - Descriptions: yfinance longBusinessSummary ONLY — never hallucinated
  - Descriptions cached forever once fetched (write-once)
  - Prices refreshed on each resolved run
  - Batch size default 150 — rate-limit safe
"""
from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

if sys.platform.startswith("win"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except AttributeError:
        pass

REPO_ROOT           = Path(__file__).resolve().parent.parent
LEADERBOARD_PATH    = REPO_ROOT / "docs" / "data" / "leaderboard.json"
DESC_CACHE_PATH     = REPO_ROOT / "data" / "company_descriptions.json"
DETAILS_DIR         = REPO_ROOT / "docs" / "data" / "details"
COVERAGE_STATE_PATH = REPO_ROOT / "data" / "coverage_state.json"
SYMBOL_MAP_PATH     = REPO_ROOT / "data" / "symbol_map.json"
UNRESOLVED_PATH     = REPO_ROOT / "data" / "stocks_unresolved.json"
TICKER_META_PATH    = REPO_ROOT / "data" / "ticker_metadata.csv"

PRICE_PERIOD        = "2y"
BATCH_SIZE_DEFAULT  = 150
DESC_DELAY          = 0.5
PROBE_DELAY         = 0.3
MAX_RETRY           = 5

KNOWN_SYMBOL_MAP: dict[str, str] = {
    "BRK.B": "BRK-B", "BRK.A": "BRK-A", "BF.B": "BF-B", "BF.A": "BF-A",
    "SNDK": "WDC",
    "005930": "005930.KS", "A005930": "005930.KS",
    "000660": "000660.KS", "A000660": "000660.KS",
    "005380": "005380.KS", "005935": "005935.KS",
    "055550": "055550.KS", "035420": "035420.KS",
    "000270": "000270.KS", "034730": "034730.KS",
    "012330": "012330.KS", "017670": "017670.KS",
    "051910": "051910.KS", "003550": "003550.KS",
    "015760": "015760.KS", "030200": "030200.KS",
    "096770": "096770.KS", "032830": "032830.KS",
    "018260": "018260.KS", "009150": "009150.KS",
    "028260": "028260.KS", "010130": "010130.KS",
    "086790": "086790.KS", "105560": "105560.KS",
    "066570": "066570.KS", "009540": "009540.KS",
    "000830": "000830.KS", "011200": "011200.KS",
    "042660": "042660.KS", "033780": "033780.KS",
    "2330": "2330.TW", "2317": "2317.TW", "2454": "2454.TW",
    "2308": "2308.TW", "2303": "2303.TW", "2382": "2382.TW",
    "2412": "2412.TW", "2881": "2881.TW", "2882": "2882.TW",
    "3711": "3711.TW", "2395": "2395.TW", "2886": "2886.TW",
    "2891": "2891.TW", "1303": "1303.TW", "1301": "1301.TW",
    "2002": "2002.TW", "2207": "2207.TW", "3045": "3045.TW",
    "VALE3": "VALE3.SA", "PETR4": "PETR4.SA", "PETR3": "PETR3.SA",
    "ITUB4": "ITUB4.SA", "BBDC4": "BBDC4.SA", "ABEV3": "ABEV3.SA",
    "WEGE3": "WEGE3.SA", "BBAS3": "BBAS3.SA", "RENT3": "RENT3.SA",
    "EGIE3": "EGIE3.SA", "EMBR3": "EMBR3.SA", "MGLU3": "MGLU3.SA",
    "RADL3": "RADL3.SA", "SUZB3": "SUZB3.SA", "LREN3": "LREN3.SA",
    "HAPV3": "HAPV3.SA", "RAIL3": "RAIL3.SA", "CPLE6": "CPLE6.SA",
    "VIVT3": "VIVT3.SA", "TAEE11": "TAEE11.SA", "TRPL4": "TRPL4.SA",
    "7203": "7203.T", "6758": "6758.T", "9984": "9984.T",
    "6501": "6501.T", "8306": "8306.T", "9432": "9432.T",
    "7974": "7974.T", "4502": "4502.T", "6902": "6902.T",
    "8001": "8001.T", "7267": "7267.T", "6954": "6954.T",
    "4661": "4661.T", "8411": "8411.T", "9983": "9983.T",
    "700":  "0700.HK", "0700": "0700.HK",
    "9988": "9988.HK", "1299": "1299.HK",
    "0005": "0005.HK", "2318": "2318.HK",
    "0388": "0388.HK", "1398": "1398.HK",
    "3690": "3690.HK", "2020": "2020.HK",
    "INFY": "INFY", "WIT": "WIT", "HDB": "HDB", "IBN": "IBN",
}

COUNTRY_SUFFIX: dict[str, str] = {
    "South Korea": ".KS", "Korea": ".KS",
    "Taiwan": ".TW",
    "Japan": ".T",
    "Hong Kong": ".HK",
    "Australia": ".AX",
    "Canada": ".TO",
    "Germany": ".DE",
    "France": ".PA",
    "Sweden": ".ST",
    "Switzerland": ".SW",
    "Netherlands": ".AS",
    "United Kingdom": ".L",
    "Brazil": ".SA",
    "China": ".SS",
    "Mexico": ".MX",
    "Singapore": ".SI",
}

PROBE_SUFFIXES = [".KS", ".TW", ".T", ".HK", ".SS", ".SZ", ".SA", ".NS", ".AX", ".L", ".DE", ".PA"]

_TEMPLATE_DESC_RE = re.compile(r"is a holding tracked across smart-beta ETFs", re.IGNORECASE)


class _SafeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, float) and (math.isnan(o) or math.isinf(o)):
            return None
        return super().default(o)


def _sanitise(obj: Any) -> Any:
    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    if isinstance(obj, dict):
        return {k: _sanitise(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitise(v) for v in obj]
    return obj


def _dumps(obj: Any, **kw) -> str:
    kw.setdefault("cls", _SafeEncoder)
    return json.dumps(_sanitise(obj), **kw)


def _write_json_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def _load_leaderboard_tickers() -> dict[str, str]:
    if not LEADERBOARD_PATH.exists():
        print(f"  ERROR: {LEADERBOARD_PATH} not found.")
        return {}
    try:
        lb = json.loads(LEADERBOARD_PATH.read_text(encoding="utf-8"))
        return {r["ticker"]: r.get("company", "") for r in lb if r.get("ticker")}
    except Exception as e:
        print(f"  ERROR reading leaderboard: {e}")
        return {}


def _load_ticker_metadata() -> dict[str, dict]:
    if not TICKER_META_PATH.exists():
        return {}
    try:
        df = pd.read_csv(TICKER_META_PATH, dtype=str).fillna("")
        result: dict[str, dict] = {}
        for _, row in df.iterrows():
            t = str(row.get("ticker", "")).strip()
            if t:
                result[t] = {
                    "sector":  str(row.get("sector", "Unknown")),
                    "country": str(row.get("country", "Unknown")),
                }
        return result
    except Exception as e:
        print(f"  WARNING: Could not load ticker_metadata: {e}")
        return {}


def _load_coverage_state() -> dict:
    default: dict = {"resolved": [], "pending": [], "unresolved": {}, "last_run_utc": None, "coverage_pct": 0.0}
    if COVERAGE_STATE_PATH.exists():
        try:
            state = json.loads(COVERAGE_STATE_PATH.read_text(encoding="utf-8"))
            for key, val in default.items():
                state.setdefault(key, val)
            return state
        except Exception as e:
            print(f"  WARNING: Bad coverage_state.json ({e}), starting fresh.")
    return dict(default)


def _save_coverage_state(state: dict) -> None:
    _write_json_atomic(COVERAGE_STATE_PATH, _dumps(state, indent=2, sort_keys=True))


def _load_symbol_map() -> dict[str, str]:
    base = dict(KNOWN_SYMBOL_MAP)
    if SYMBOL_MAP_PATH.exists():
        try:
            from_file = json.loads(SYMBOL_MAP_PATH.read_text(encoding="utf-8"))
            base.update(from_file)
        except Exception as e:
            print(f"  WARNING: Bad symbol_map.json ({e})")
    return base


def _save_symbol_map(m: dict[str, str]) -> None:
    _write_json_atomic(SYMBOL_MAP_PATH, _dumps(m, indent=2, sort_keys=True))


def _load_desc_cache() -> dict[str, str]:
    if DESC_CACHE_PATH.exists():
        try:
            return json.loads(DESC_CACHE_PATH.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"  WARNING: Bad description cache ({e})")
    return {}


def _save_desc_cache(cache: dict[str, str]) -> None:
    _write_json_atomic(DESC_CACHE_PATH, _dumps(cache, indent=2, sort_keys=True))


def _is_us_ticker(ticker: str, country: str) -> bool:
    if country and country not in ("Unknown", ""):
        return country == "United States"
    return bool(re.fullmatch(r"[A-Z]{1,5}", ticker))


def _resolve_yf_symbol(ticker: str, meta: dict, symbol_map: dict) -> str:
    if ticker in symbol_map:
        return symbol_map[ticker]
    country = (meta.get("country") or "").strip()
    if _is_us_ticker(ticker, country):
        return ticker
    if country and country in COUNTRY_SUFFIX:
        return ticker + COUNTRY_SUFFIX[country]
    if re.fullmatch(r"A?\d{6}", ticker):
        return ticker.lstrip("A") + ".KS"
    if re.fullmatch(r"\d{4}", ticker):
        return ticker + ".TW"
    if re.fullmatch(r"[A-Z]{4}(3|4|11)", ticker):
        return ticker + ".SA"
    if re.fullmatch(r"\d{1,4}", ticker):
        return ticker.zfill(4) + ".HK"
    return ticker


def _probe_yf_symbol(ticker: str, symbol_map: dict) -> str | None:
    try:
        import yfinance as yf
    except ImportError:
        return None
    base = ticker.lstrip("A") if re.fullmatch(r"A\d{6}", ticker) else ticker
    for suffix in PROBE_SUFFIXES:
        sym = base + suffix
        try:
            fi = yf.Ticker(sym).fast_info
            price = fi.get("last_price") or fi.get("lastPrice") or 0
            if price and float(price) > 0:
                symbol_map[ticker] = sym
                print(f"    PROBE HIT: {ticker} → {sym} (price={float(price):.2f})")
                return sym
        except Exception:
            pass
        time.sleep(PROBE_DELAY)
    return None


def _fetch_price_one(symbol: str) -> list[list] | None:
    try:
        import yfinance as yf
        df = yf.download(
            symbol, period=PRICE_PERIOD, interval="1d",
            auto_adjust=True, progress=False, threads=False,
        )
        if df is None or df.empty:
            return None
        if isinstance(df.columns, pd.MultiIndex):
            level0 = df.columns.get_level_values(0)
            if "Close" in level0:
                close = df["Close"]
                if isinstance(close, pd.DataFrame):
                    close = close.iloc[:, 0]
            else:
                close = df.iloc[:, 0]
        else:
            close = df["Close"] if "Close" in df.columns else df.iloc[:, 0]
        close = close.dropna()
        if close.empty:
            return None
        prices = [
            [pd.Timestamp(dt).strftime("%Y-%m-%d"), round(float(val), 4)]
            for dt, val in close.items()
            if not (isinstance(val, float) and (math.isnan(val) or math.isinf(val)))
        ]
        prices.sort(key=lambda x: x[0])
        return prices if prices else None
    except Exception as e:
        print(f"    Price fetch error ({symbol}): {e}")
        return None


def _fetch_description_one(symbol: str) -> str | None:
    """Fetch real description from yfinance. Returns None if not found. NEVER fabricates."""
    try:
        import yfinance as yf
        info = yf.Ticker(symbol).info
        desc = (info.get("longBusinessSummary") or info.get("description") or "").strip()
        if len(desc) < 20:
            return None
        if len(desc) > 620:
            sentences = desc.split(". ")
            out, length = "", 0
            for s in sentences:
                if length + len(s) > 600:
                    break
                out += s + ". "
                length += len(s) + 2
            desc = out.strip() or desc[:600]
        return desc
    except Exception:
        return None


def _write_detail_file(
    ticker: str, company: str,
    desc: str | None, prices: list[list] | None,
    pending: bool = False,
) -> None:
    generated = datetime.now(timezone.utc).isoformat(timespec="seconds")
    if pending:
        payload: dict = {"ticker": ticker, "company": company, "pending": True, "generated": generated}
    else:
        payload = {
            "ticker": ticker, "company": company,
            "description": desc or "", "prices": prices or [],
            "generated": generated,
        }
    _write_json_atomic(DETAILS_DIR / f"{ticker}.json", _dumps(payload, separators=(",", ":")))


def _write_pending_stubs(pending_tickers: list[str], lb_map: dict[str, str]) -> int:
    count = 0
    for ticker in pending_tickers:
        out_path = DETAILS_DIR / f"{ticker}.json"
        if out_path.exists():
            try:
                existing = json.loads(out_path.read_text(encoding="utf-8"))
                if not existing.get("pending", False) and existing.get("prices"):
                    continue
            except Exception:
                pass
        _write_detail_file(ticker, lb_map.get(ticker, ticker), None, None, pending=True)
        count += 1
    return count


def _prioritize_batch(
    pending: list[str], metadata: dict,
    symbol_map: dict, batch_size: int,
) -> list[str]:
    def priority(t: str) -> int:
        if t in symbol_map:
            return 0
        country = (metadata.get(t) or {}).get("country", "")
        if _is_us_ticker(t, country):
            return 1
        if country and country not in ("Unknown", ""):
            return 2
        return 3
    return sorted(pending, key=priority)[:batch_size]


def build_details(
    batch_size: int = BATCH_SIZE_DEFAULT,
    tickers_filter: list[str] | None = None,
    fill_pending_only: bool = False,
    dry_run: bool = False,
    refresh_descriptions: bool = False,
) -> None:
    print("\n" + "═" * 62)
    print("  Predator Protocol — Stock Details (Self-Healing Coverage)")
    print("═" * 62)

    DETAILS_DIR.mkdir(parents=True, exist_ok=True)

    lb_map     = _load_leaderboard_tickers()
    if not lb_map:
        print("  No tickers found — run predator.build first.")
        return

    metadata   = _load_ticker_metadata()
    state      = _load_coverage_state()
    symbol_map = _load_symbol_map()
    desc_cache = _load_desc_cache() if not refresh_descriptions else {}

    total_universe = set(lb_map.keys())
    resolved       = set(state.get("resolved") or []) & total_universe
    unresolved     = {k: v for k, v in (state.get("unresolved") or {}).items() if k in total_universe}
    pending_set    = (set(state.get("pending") or []) | (total_universe - resolved - set(unresolved.keys()))) & total_universe

    prev_coverage = len(resolved) / max(len(total_universe), 1) * 100
    print(f"\n  Universe:   {len(total_universe)} tickers")
    print(f"  Resolved:   {len(resolved)} ({prev_coverage:.1f}%)")
    print(f"  Pending:    {len(pending_set)}")
    print(f"  Unresolved: {len(unresolved)}")

    if fill_pending_only:
        print("\n  --fill-pending: writing stub files only…")
        if not dry_run:
            n = _write_pending_stubs(sorted(pending_set | set(unresolved.keys())), lb_map)
            print(f"  Wrote {n} pending stub files.")
        return

    if tickers_filter:
        batch = [t for t in tickers_filter if t in total_universe]
        print(f"\n  Filter mode: {len(batch)} tickers")
    else:
        batch = _prioritize_batch(sorted(pending_set), metadata, symbol_map, batch_size)
        print(f"\n  Batch: {len(batch)} of {len(pending_set)} pending")

    if dry_run:
        print("\n  --dry-run plan:")
        for t in batch[:30]:
            sym = _resolve_yf_symbol(t, metadata.get(t, {}), symbol_map)
            country = (metadata.get(t) or {}).get("country", "?")
            print(f"    {t:15s} → {sym:20s}  [{country}]")
        if len(batch) > 30:
            print(f"    … and {len(batch)-30} more")
        return

    newly_resolved = 0
    errors = 0
    newly_unresolved: list[str] = []

    for i, ticker in enumerate(batch, 1):
        company = lb_map.get(ticker, ticker)
        meta    = metadata.get(ticker, {})
        print(f"\n  [{i}/{len(batch)}] {ticker} ({company[:40]})")

        symbol = _resolve_yf_symbol(ticker, meta, symbol_map)
        prices = _fetch_price_one(symbol)

        if not prices and re.search(r"\d", ticker):
            print(f"    No data for {symbol} — probing…")
            probed = _probe_yf_symbol(ticker, symbol_map)
            if probed and probed != symbol:
                symbol = probed
                prices = _fetch_price_one(symbol)

        current_desc = desc_cache.get(ticker, "")
        needs_desc = not current_desc or _TEMPLATE_DESC_RE.search(current_desc)
        if needs_desc and symbol:
            time.sleep(DESC_DELAY)
            desc = _fetch_description_one(symbol)
            if desc:
                desc_cache[ticker] = desc
                print(f"    Description: {len(desc)} chars")
            else:
                print(f"    Description: not found")
        else:
            desc = current_desc or None

        if prices:
            _write_detail_file(ticker, company, desc, prices, pending=False)
            resolved.add(ticker)
            pending_set.discard(ticker)
            unresolved.pop(ticker, None)
            newly_resolved += 1
            print(f"    ✓ {len(prices)} price points")
        else:
            fail_count = unresolved.get(ticker, 0) + 1
            unresolved[ticker] = fail_count
            if fail_count >= MAX_RETRY:
                pending_set.discard(ticker)
                newly_unresolved.append(ticker)
                print(f"    ✗ Unresolved after {fail_count} attempts")
            else:
                print(f"    ✗ Failed (attempt {fail_count}/{MAX_RETRY})")
            errors += 1

    all_pending = sorted(pending_set | {t for t in unresolved if t not in resolved})
    print(f"\n  Writing pending stubs for {len(all_pending)} tickers…")
    stubs = _write_pending_stubs(all_pending, lb_map)
    print(f"  Stubs written: {stubs}")

    new_coverage = len(resolved) / max(len(total_universe), 1) * 100
    state["resolved"]     = sorted(resolved)
    state["pending"]      = sorted(pending_set)
    state["unresolved"]   = unresolved
    state["last_run_utc"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    state["coverage_pct"] = round(new_coverage, 2)
    _save_coverage_state(state)
    _save_symbol_map(symbol_map)
    _save_desc_cache(desc_cache)

    unresolved_report = {
        "generated_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "count": len(unresolved),
        "tickers": {t: {"attempts": c, "company": lb_map.get(t, "")} for t, c in unresolved.items()},
    }
    _write_json_atomic(UNRESOLVED_PATH, _dumps(unresolved_report, indent=2))

    print(f"\n{'═'*62}")
    print(f"  Coverage: {prev_coverage:.1f}% → {new_coverage:.1f}% (+{newly_resolved} this run)")
    print(f"  Resolved: {len(resolved)} / {len(total_universe)}")
    print(f"  Pending:  {len(pending_set)}  |  Unresolved: {len(unresolved)}")
    if newly_unresolved:
        print(f"  Newly unresolved: {', '.join(newly_unresolved[:10])}")
    remaining = math.ceil(len(pending_set) / batch_size) if batch_size > 0 else "?"
    print(f"  Projected runs to full coverage: ~{remaining}")
    print(f"{'═'*62}\n")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="predator.fetch_stock_details")
    p.add_argument("--batch-size",   type=int, default=BATCH_SIZE_DEFAULT)
    p.add_argument("--fill-pending", action="store_true")
    p.add_argument("--dry-run",      action="store_true")
    p.add_argument("--tickers",      type=str, default=None)
    p.add_argument("--refresh-desc", action="store_true")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    tickers_filter = None
    if args.tickers:
        tickers_filter = [t.strip().upper() for t in args.tickers.split(",")]
    build_details(
        batch_size=args.batch_size,
        tickers_filter=tickers_filter,
        fill_pending_only=args.fill_pending,
        dry_run=args.dry_run,
        refresh_descriptions=args.refresh_desc,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
