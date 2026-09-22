"""
Trackinsight Fund Flow Scraper & Processor for Conviction Labs Markets.
Fetches full daily fund flow history and derived metrics for any ETF.
Uses Playwright to navigate AWS WAF and same-origin browser session.
"""
import argparse
import json
import logging
import os
import random
import sys
import time
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
from dateutil.relativedelta import relativedelta
import numpy as np
import pandas as pd
from playwright.sync_api import sync_playwright

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("flow_scraper")

ENDPOINT = "https://www.trackinsight.com/search-api/snapshot/get_snapshots"
OUT_DIR = Path("docs/data/flows")
INDEX_PATH = Path("docs/data/etf_search_index.json")

# Verified known ticker-to-key mappings
KNOWN_KEYS = {
    "BDRY": "BDRY",
    # Leveraged Tech, AI & Semis
    "TECL": "TECL",
    "FNGU": "FNGU",
    "BULZ": "BULZ",
    "USD": "ARCX:USD",
    "ROM": "ROM",
    "WEBL": "WEBL",
    "SOXL": "ARCX:SOXL",
    "SOXS": "ARCX:SOXS",
    # Leveraged Financials & Banks
    "FAS": "FAS",
    "DPST": "DPST",
    "UYG": "UYG",
    # Leveraged Biotech & Healthcare
    "LABU": "LABU",
    "CURE": "ARCX:CURE",
    # Leveraged Energy, Commodities & Metals
    "ERX": "ERX",
    "GUSH": "GUSH",
    "UCO": "ARCX:UCO",
    "SCO": "ARCX:SCO",
    "BOIL": "BOIL",
    "KOLD": "KOLD",
    "AGQ": "AGQ",
    "UGL": "UGL",
    "NUGT": "ARCX:NUGT",
    "JNUG": "JNUG",
    "CPER": "ARCX:CPER",
    "GLD": "GLD",
    "SLV": "SLV",
    "USO": "USO",
    "UNG": "UNG",
    # Leveraged Industrials, Defense & Transport
    "DFEN": "ARCX:DFEN",
    "TPOR": "TPOR",
    "DUSL": "DUSL",
    "NAIL": "NAIL",
    # Volatility & FX
    "UVXY": "BATS:UVXY",
    "SVIX": "SVIX",
    "SVXY": "SVXY",
    "UVIX": "UVIX",
    "UUP": "UUP",
    "EUO": "EUO",
    "YCS": "YCS",
    # Single-Stock Leveraged (AI & High Beta)
    "NVDL": "NVDL",
    "NVDU": "XNMS:NVDU",
    "NVDS": "XNMS:NVDS",
    "TSLL": "TSLL",
    "TSLQ": "TSLQ",
    "MSTU": "BATS:MSTU",
    "MSTX": "MSTX",
    "CONL": "CONL",
    "PLTU": "XNMS:PLTU",
    "PTIR": "PTIR",
    "METU": "XNMS:METU",
    "AMUU": "AMUU",
    "GGLL": "GGLL",
    "GOOX": "GOOX",
    "AAPU": "XNMS:AAPU",
    "MSFU": "XNMS:MSFU",
    "AMZU": "XNMS:AMZU",
    # Broad Benchmark Leverage
    "TQQQ": "TQQQ",
    "SQQQ": "XNMS:SQQQ",
    "UPRO": "UPRO",
    "SPXL": "SPXL",
    "SPXU": "ARCX:SPXU",
    "SSO": "ARCX:SSO",
    "SH": "ARCX:SH",
    "QID": "ARCX:QID",
    "TNA": "TNA",
    "TZA": "TZA",
    "TLT": "TLT",
    "BDRY": "BDRY",
    "BWET": "BWET"
}

CURATED_PRESETS = {
    "leveraged_tech_ai": ["TECL", "SOXL", "SOXS", "USD", "FNGU", "BULZ", "ROM", "WEBL"],
    "single_stock_leveraged": ["NVDL", "NVDU", "TSLL", "TSLQ", "MSTU", "MSTX", "CONL", "PLTU", "PTIR", "METU", "AMUU", "GGLL", "GOOX", "AAPU", "MSFU", "AMZU"],
    "leveraged_financials": ["FAS", "DPST", "UYG"],
    "leveraged_biotech_health": ["LABU", "CURE"],
    "leveraged_energy_commodities": ["ERX", "GUSH", "UCO", "BOIL", "KOLD", "AGQ", "UGL", "NUGT", "JNUG", "CPER"],
    "leveraged_defense_transport": ["DFEN", "TPOR", "DUSL", "NAIL"],
    "volatility_fx": ["UVXY", "SVIX", "SVXY", "UVIX", "UUP", "EUO", "YCS"],
    "broad_market_leverage": ["TQQQ", "SQQQ", "UPRO", "SPXU", "TNA", "TZA"]
}

_FETCH_JS = """
async (payload) => {
    try {
        const resp = await fetch('%s', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-Requested-With': 'XMLHttpRequest'
            },
            body: JSON.stringify(payload)
        });
        if (!resp.ok) {
            return {__error: true, status: resp.status, statusText: resp.statusText};
        }
        return await resp.json();
    } catch (e) {
        return {__error: true, status: 0, statusText: String(e)};
    }
}
""" % ENDPOINT


def stamp_to_date(d):
    try:
        return datetime.fromtimestamp(int(d) * 86400, tz=timezone.utc).strftime("%Y-%m-%d")
    except Exception:
        return str(d)


def unpack(field_data):
    if not isinstance(field_data, dict):
        return []
    scale = field_data.get("scale", 1) or 1
    raw = field_data.get("data", [])
    return [v / scale if v is not None else None for v in raw]


def resolve_key(ticker: str) -> str:
    ticker_up = ticker.strip().upper()
    if ticker_up in KNOWN_KEYS:
        return KNOWN_KEYS[ticker_up]
    
    # Try search index if available
    if INDEX_PATH.exists():
        try:
            with open(INDEX_PATH, "r", encoding="utf-8") as f:
                idx = json.load(f)
                for row in idx.get("rows", []):
                    if row[0].upper() == ticker_up:
                        return row[1]
        except Exception:
            pass
    return ticker_up


def apply_derived_metrics(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    flows_filled = df["usd_flow"].fillna(0.0)
    df["cumulative_flow"] = flows_filled.cumsum()
    df["daily_inflow"] = df["usd_flow"].clip(lower=0)
    df["daily_outflow"] = df["usd_flow"].clip(upper=0)

    # 30-day rolling Z-Score
    window = 30
    df["mean_30d"] = df["usd_flow"].rolling(window, min_periods=5).mean()
    df["std_30d"] = df["usd_flow"].rolling(window, min_periods=5).std()
    df["flow_zscore"] = np.where(df["std_30d"] > 0, (df["usd_flow"] - df["mean_30d"]) / df["std_30d"], 0.0)

    # Momentum
    df["flow_5d"] = df["usd_flow"].rolling(5, min_periods=1).sum()
    df["flow_20d"] = df["usd_flow"].rolling(20, min_periods=1).sum()

    # Flow Regime
    def get_regime(z):
        if pd.isna(z):
            return "BALANCED"
        if z > 1.5:
            return "ACCUMULATION"
        if z < -1.5:
            return "DISTRIBUTION"
        return "BALANCED"

    df["regime"] = df["flow_zscore"].apply(get_regime)

    # Streak
    sign = np.sign(flows_filled)
    streak = sign.groupby((sign != sign.shift()).cumsum()).cumsum()

    # Pressure calculation (-100 to +100)
    def compute_pressure(row, s):
        if pd.isna(row["flow_zscore"]):
            return 0.0
        mom_factor = 10 if row["flow_5d"] > 0 else (-10 if row["flow_5d"] < 0 else 0)
        streak_bonus = min(abs(s) * 2, 20) * (1 if s > 0 else -1)
        raw = (row["flow_zscore"] * 25) + mom_factor + streak_bonus
        return max(-100.0, min(100.0, raw))

    pressures = []
    for idx, row in df.iterrows():
        pressures.append(compute_pressure(row, streak.loc[idx]))
    df["pressure"] = pressures

    df = df.drop(columns=["mean_30d", "std_30d"], errors="ignore")

    for col in ["flow_zscore", "flow_5d", "flow_20d", "pressure"]:
        df[col] = df[col].fillna(0.0)

    return df


def parse_snapshots(raw_response, ticker: str) -> pd.DataFrame:
    chunks = raw_response if isinstance(raw_response, list) else ([raw_response] if raw_response else [])
    rows = {}

    for item in chunks:
        if not isinstance(item, dict):
            continue
        stamps = item.get("stamp", {}).get("data", [])
        if not stamps:
            continue
        flows = unpack(item.get("USD:flow", {}))
        navs = unpack(item.get("nav", {}))
        perfs = unpack(item.get("perf", {}))

        for i, s in enumerate(stamps):
            d_str = stamp_to_date(s)
            if not d_str:
                continue
            rows[d_str] = {
                "date": d_str,
                "usd_flow": flows[i] if i < len(flows) and flows[i] is not None else 0.0,
                "nav": navs[i] if i < len(navs) and navs[i] is not None else None,
                "perf_pct": perfs[i] if i < len(perfs) and perfs[i] is not None else None
            }

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(list(rows.values())).sort_values("date").reset_index(drop=True)
    df["nav"] = df["nav"].ffill().bfill()
    return apply_derived_metrics(df)


def fetch_ticker_data(page, ticker: str, key: str, start_date: str = None, start_year: int = 2016, fail_fast: bool = False) -> pd.DataFrame:
    today = date.today()
    if start_date:
        try:
            d = datetime.strptime(start_date, "%Y-%m-%d").date()
            logger.info(f"Fetching incremental delta for {ticker} (key: {key}) from {start_date} to {today}...")
        except Exception:
            d = date(start_year, 1, 1)
            logger.info(f"Fetching full history for {ticker} (key: {key}) from {start_year} to {today}...")
    else:
        d = date(start_year, 1, 1)
        logger.info(f"Fetching full history for {ticker} (key: {key}) from {start_year} to {today}...")

    reqs = []
    while d <= today:
        q_end = min(d + relativedelta(months=3) - relativedelta(days=1), today)
        reqs.append({
            "fund": key,
            "startDate": d.strftime("%Y-%m-%d"),
            "endDate": q_end.strftime("%Y-%m-%d"),
            "columns": ["stamp", "USD:flow", "nav", "perf"]
        })
        d += relativedelta(months=3)

    result = page.evaluate(_FETCH_JS, {"requests": reqs})
    if isinstance(result, dict) and result.get("__error"):
        err_msg = f"HTTP Error fetching {ticker} (key: {key}): status={result.get('status')} {result.get('statusText')}"
        if fail_fast:
            logger.error(f"[FAIL-FAST] {err_msg}")
            raise RuntimeError(err_msg)
        logger.warning(err_msg)
        return pd.DataFrame()

    return parse_snapshots(result, ticker)


def save_ticker_json(ticker: str, key: str, df: pd.DataFrame):
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_file = OUT_DIR / f"{ticker.upper()}.json"
    old_count = 0

    # Merge with existing file if present, deduplicating on 'date'
    if out_file.exists():
        try:
            with open(out_file, "r", encoding="utf-8") as f:
                old = json.load(f)
                old_df = pd.DataFrame(old.get("data", []))
                if not old_df.empty:
                    old_count = len(old_df)
                    if not df.empty:
                        # Combine old history with new delta, keeping latest data on date collision
                        combined = pd.concat([old_df, df], ignore_index=True)
                        combined = combined.drop_duplicates(subset=["date"], keep="last").sort_values("date").reset_index(drop=True)
                        df = apply_derived_metrics(combined)
                    else:
                        df = old_df
        except Exception as e:
            logger.warning(f"Could not merge existing file for {ticker}: {e}")

    records = []
    for _, row in df.iterrows():
        records.append({
            "date": row["date"],
            "usd_flow": round(float(row["usd_flow"]), 2) if pd.notna(row["usd_flow"]) else 0.0,
            "nav": round(float(row["nav"]), 4) if pd.notna(row.get("nav")) else None,
            "perf_pct": round(float(row["perf_pct"]), 4) if pd.notna(row.get("perf_pct")) else None,
            "cumulative_flow": round(float(row["cumulative_flow"]), 2) if pd.notna(row.get("cumulative_flow")) else 0.0,
            "daily_inflow": round(float(row["daily_inflow"]), 2) if pd.notna(row.get("daily_inflow")) else 0.0,
            "daily_outflow": round(float(row["daily_outflow"]), 2) if pd.notna(row.get("daily_outflow")) else 0.0,
            "flow_zscore": round(float(row["flow_zscore"]), 3) if pd.notna(row.get("flow_zscore")) else 0.0,
            "flow_5d": round(float(row["flow_5d"]), 2) if pd.notna(row.get("flow_5d")) else 0.0,
            "flow_20d": round(float(row["flow_20d"]), 2) if pd.notna(row.get("flow_20d")) else 0.0,
            "regime": str(row.get("regime", "BALANCED")),
            "pressure": round(float(row.get("pressure", 0.0)), 1)
        })

    payload = {
        "ticker": ticker.upper(),
        "key": key,
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "count": len(records),
        "data": records
    }

    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(payload, f, separators=(",", ":"))

    new_appended = len(records) - old_count
    logger.info(f"Saved {ticker} -> {out_file} ({len(records)} total rows, appended {new_appended} new rows)")


def build_manifest():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    manifest_items = {}

    for p in OUT_DIR.glob("*.json"):
        if p.stem == "curated_manifest":
            continue
        try:
            with open(p, "r", encoding="utf-8") as f:
                content = json.load(f)
                data = content.get("data", [])
                if not data:
                    continue
                last_rec = data[-1]
                net_30d = sum(r["usd_flow"] for r in data[-30:]) if len(data) >= 30 else sum(r["usd_flow"] for r in data)
                
                manifest_items[content["ticker"]] = {
                    "ticker": content["ticker"],
                    "key": content.get("key", content["ticker"]),
                    "asof": last_rec.get("date"),
                    "nav": last_rec.get("nav"),
                    "flow_30d": net_30d,
                    "flow_zscore": last_rec.get("flow_zscore", 0.0),
                    "regime": last_rec.get("regime", "BALANCED"),
                    "pressure": last_rec.get("pressure", 0.0),
                    "start_date": data[0].get("date"),
                    "records": len(data)
                }
        except Exception as e:
            logger.warning(f"Error reading {p}: {e}")

    manifest_file = OUT_DIR / "curated_manifest.json"
    manifest_payload = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "total_etfs": len(manifest_items),
        "presets": CURATED_PRESETS,
        "etfs": manifest_items
    }

    with open(manifest_file, "w", encoding="utf-8") as f:
        json.dump(manifest_payload, f, indent=2)

    logger.info(f"Saved manifest -> {manifest_file} ({len(manifest_items)} ETFs)")


def run(tickers: list[str], force: bool = False, fail_fast: bool = False):
    pw = sync_playwright().start()
    browser = pw.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled", "--no-sandbox"])
    context = browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        viewport={"width": 1920, "height": 1080}
    )
    page = context.new_page()
    page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

    # Warm-up session to settle WAF cookies (fail fast if site is unreachable)
    logger.info("Warming up browser session on Trackinsight...")
    try:
        page.goto("https://www.trackinsight.com/en", wait_until="domcontentloaded", timeout=20000)
        time.sleep(2)
    except Exception as e:
        logger.error(f"Warm-up navigation failed: {e}")
        if fail_fast:
            browser.close()
            pw.stop()
            logger.error("[FAIL-FAST] Exiting immediately due to warm-up failure.")
            sys.exit(1)

    for i, t in enumerate(tickers):
        ticker_clean = t.split(":")[-1].upper()
        key = resolve_key(ticker_clean)
        
        # Check if already present and determine if full download or incremental delta
        out_file = OUT_DIR / f"{ticker_clean}.json"
        start_date = None
        start_year = 2016

        if out_file.exists():
            try:
                with open(out_file, "r", encoding="utf-8") as f:
                    c = json.load(f)
                    d = c.get("data", [])
                    if d:
                        last_d = d[-1]["date"]
                        last_dt = datetime.strptime(last_d, "%Y-%m-%d").date()
                        today = date.today()

                        # If already updated today and not forced, skip
                        if last_dt >= today and not force:
                            logger.info(f"[{i+1}/{len(tickers)}] {ticker_clean} is already up-to-date ({last_d}). Skipping.")
                            continue

                        # Overlap by 5 calendar days to capture any custodian T+1/T+2 restatements
                        delta_start = max(date(2016, 1, 1), last_dt - timedelta(days=5))
                        start_date = delta_start.strftime("%Y-%m-%d")
                        logger.info(f"[{i+1}/{len(tickers)}] {ticker_clean}: Existing history found up to {last_d}. Requesting incremental delta from {start_date}...")
            except Exception as e:
                logger.warning(f"Error checking cached file for {ticker_clean}: {e}")

        if not start_date:
            logger.info(f"[{i+1}/{len(tickers)}] {ticker_clean}: No local cache found. Initiating full historical fetch...")

        try:
            df = fetch_ticker_data(page, ticker_clean, key, start_date=start_date, start_year=start_year, fail_fast=fail_fast)
            if not df.empty:
                save_ticker_json(ticker_clean, key, df)
            else:
                msg = f"No new data returned for {ticker_clean} (key: {key})"
                if fail_fast:
                    logger.error(f"[FAIL-FAST] {msg}")
                    browser.close()
                    pw.stop()
                    sys.exit(1)
                logger.warning(msg)
        except Exception as e:
            logger.error(f"Failed processing {ticker_clean}: {e}")
            if fail_fast:
                browser.close()
                pw.stop()
                sys.exit(1)

        # Polite jitter
        time.sleep(1.5 + random.uniform(0.5, 1.5))

    browser.close()
    pw.stop()
    build_manifest()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch ETF fund flow data from Trackinsight")
    parser.add_argument("--ticker", type=str, help="Single ETF ticker (e.g. TQQQ)")
    parser.add_argument("--tickers", type=str, help="Comma-separated tickers (e.g. TQQQ,SOXL,SQQQ)")
    parser.add_argument("--curated", action="store_true", help="Fetch all curated preset ETFs")
    parser.add_argument("--batch-top", type=int, help="Fetch next N uncached ETFs ranked by AUM from search index (e.g. 50)")
    parser.add_argument("--force", action="store_true", help="Force re-fetch even if ETF was updated today")
    parser.add_argument("--fail-fast", action="store_true", help="Exit immediately with error code on any failure")
    parser.add_argument("--manifest-only", action="store_true", help="Only rebuild curated_manifest.json")
    args = parser.parse_args()

    if args.manifest_only:
        build_manifest()
        sys.exit(0)

    target_tickers = []
    if args.ticker:
        target_tickers = [t.strip().upper() for t in args.ticker.split(",") if t.strip()]
    elif args.tickers:
        target_tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    elif args.curated:
        all_curated = set()
        for group in CURATED_PRESETS.values():
            all_curated.update(group)
        target_tickers = sorted(all_curated)
    elif args.batch_top:
        # Load search index, find uncached ETFs, sort by AUM descending
        if not INDEX_PATH.exists():
            logger.error(f"Index file {INDEX_PATH} not found. Run scripts/build_etf_search_index.py first.")
            sys.exit(1)
        with open(INDEX_PATH, "r", encoding="utf-8") as f:
            idx_data = json.load(f)
        candidates = []
        for r in idx_data.get("rows", []):
            t = r[0].upper()
            aum = r[4] or 0
            out_file = OUT_DIR / f"{t}.json"
            if not out_file.exists():
                candidates.append((t, aum))
        candidates.sort(key=lambda x: x[1], reverse=True)
        target_tickers = [t[0] for t in candidates[:args.batch_top]]
        logger.info(f"Selected top {len(target_tickers)} uncached ETFs by AUM: {target_tickers}")
    else:
        # Default: target top priorities
        target_tickers = ["TQQQ", "SOXL", "SQQQ", "SPY", "QQQ", "NVDL", "AGQ", "UGL"]

    logger.info(f"Target tickers ({len(target_tickers)}): {target_tickers}")
    run(target_tickers, force=args.force, fail_fast=args.fail_fast)
