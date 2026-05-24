"""
Ingest Mega Markets Historical Excel → docs/data/market_returns.json

Reads Mega_Markets_Historical.xlsx from the repo root and merges its data
into the existing market_returns.json. Excel data takes priority for
overlapping months (it has longer history).

Usage:
    python -m predator.ingest_mega_xl                    # process all series
    python -m predator.ingest_mega_xl --dry-run          # print what would be written
    python -m predator.ingest_mega_xl --series sp500     # process only sp500
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

# ─── Platform encoding fix (Windows CI) ──────────────────────────────────────
if sys.platform.startswith("win"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except AttributeError:
        pass


# ─── Paths ───────────────────────────────────────────────────────────────────

REPO_ROOT   = Path(__file__).resolve().parent.parent
EXCEL_PATH  = REPO_ROOT / "Mega_Markets_Historical.xlsx"
OUTPUT_PATH = REPO_ROOT / "docs" / "data" / "market_returns.json"


# ─── Series mapping ───────────────────────────────────────────────────────────
# Excel Index_Name → (asset_key, category, native_ccy)

SERIES_MAP: dict[str, tuple[str, str, str]] = {
    # Equities sheet
    "S&P 500":                              ("sp500",       "equity",          "USD"),
    "Dow Jones Industrial Average":         ("djia",        "equity",          "USD"),
    "NASDAQ Composite":                     ("nasdaq",      "equity",          "USD"),
    "NASDAQ-100":                           ("nasdaq100",   "equity",          "USD"),
    "Nikkei 225":                           ("nikkei",      "equity",          "JPY"),
    "BSE Sensex":                           ("sensex",      "equity",          "INR"),
    "DAX (Curvo synthetic TR, base 10000)": ("dax",         "equity",          "EUR"),
    # International_Equities sheet
    "FTSE 100 (GBP)":                       ("ftse100",     "equity",          "GBP"),
    "Hang Seng (HKD)":                      ("hang_seng",   "equity",          "HKD"),
    "ASX 200 (AUD)":                        ("asx200",      "equity",          "AUD"),
    "Bovespa (BRL)":                        ("bovespa",     "equity",          "BRL"),
    "Shanghai Composite (CNY)":             ("shanghai",    "equity",          "CNY"),
    # Precious_Metals sheet
    "Gold (USD/oz)":                        ("gold",        "precious_metals", "USD"),
    "Silver (USD/oz)":                      ("silver",      "precious_metals", "USD"),
    "Platinum (USD/oz)":                    ("platinum",    "precious_metals", "USD"),
    "Palladium (USD/oz)":                   ("palladium",   "precious_metals", "USD"),
    # Commodities sheet
    "Copper (USD/lb)":                      ("copper_lb",   "base_metals",     "USD"),
    "Corn (USD/bushel)":                    ("corn",        "agriculture",     "USD"),
    "Wheat (USD/bushel)":                   ("wheat",       "agriculture",     "USD"),
    "Soybeans (USD/bushel)":                ("soybeans",    "agriculture",     "USD"),
    "Sugar (USD/lb)":                       ("sugar",       "agriculture",     "USD"),
    "Coffee Arabica (USD/lb)":              ("coffee",      "agriculture",     "USD"),
    "Cotton (USD/lb)":                      ("cotton",      "agriculture",     "USD"),
    # Energy sheet
    "WTI Crude Oil (USD/bbl)":              ("wti_crude",   "energy",          "USD"),
    "Brent Crude Oil (USD/bbl)":            ("brent_crude", "energy",          "USD"),
}

# Sheet names in the Excel file
SHEETS = [
    "Equities",
    "International_Equities",
    "Precious_Metals",
    "Commodities",
    "Energy",
]


# ─── Safe JSON encoder (NaN → null) ─────────────────────────────────────────

class _SafeEncoder(json.JSONEncoder):
    """Encode NaN / ±Inf floats as JSON null instead of invalid bare NaN."""

    def iterencode(self, o, _one_shot=False):
        return super().iterencode(self._sanitise(o), _one_shot)

    def _sanitise(self, obj):
        if isinstance(obj, float):
            return None if (math.isnan(obj) or math.isinf(obj)) else obj
        if isinstance(obj, dict):
            return {k: self._sanitise(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [self._sanitise(v) for v in obj]
        return obj


def _dumps(obj: Any, **kwargs) -> str:
    kwargs.setdefault("cls", _SafeEncoder)
    return json.dumps(obj, **kwargs)


# ─── Excel reader ─────────────────────────────────────────────────────────────

def _read_excel_series(
    excel_path: Path,
    series_filter: set[str] | None = None,
) -> dict[str, pd.DataFrame]:
    """
    Read all sheets from the Excel file.

    Returns a dict mapping Index_Name → DataFrame with columns [Date, Close]
    (daily rows, sorted ascending).

    Only includes series present in SERIES_MAP (and optionally filtered by
    series_filter which contains asset keys like 'sp500').
    """
    print(f"\nReading: {excel_path}")

    # Build reverse map: asset_key → index_name (for filtering)
    key_to_name = {v[0]: k for k, v in SERIES_MAP.items()}
    if series_filter:
        wanted_names = {key_to_name[k] for k in series_filter if k in key_to_name}
    else:
        wanted_names = set(SERIES_MAP.keys())

    results: dict[str, pd.DataFrame] = {}

    for sheet in SHEETS:
        try:
            df = pd.read_excel(excel_path, sheet_name=sheet)
        except Exception as e:
            print(f"  WARNING: Could not read sheet '{sheet}': {e}")
            continue

        if "Index_Name" not in df.columns or "Date" not in df.columns or "Close" not in df.columns:
            print(f"  WARNING: Sheet '{sheet}' missing expected columns. Got: {list(df.columns)}")
            continue

        # Filter to only the series we care about
        for index_name in df["Index_Name"].unique():
            if index_name not in SERIES_MAP:
                continue
            if index_name not in wanted_names:
                continue

            sub = df[df["Index_Name"] == index_name][["Date", "Close"]].copy()
            sub["Date"] = pd.to_datetime(sub["Date"], errors="coerce")
            sub = sub.dropna(subset=["Date", "Close"])
            sub = sub.sort_values("Date")

            asset_key = SERIES_MAP[index_name][0]
            results[asset_key] = sub
            print(f"  {sheet:25s} | {index_name:45s} → {asset_key} ({len(sub)} rows)")

    return results


# ─── Resample to monthly EOP ──────────────────────────────────────────────────

def _to_monthly_close(df: pd.DataFrame) -> dict[str, float]:
    """
    Resample daily data to monthly end-of-period close.

    Returns {YYYY-MM: value} dict, rounded to 4 dp.
    """
    monthly = df.resample("ME", on="Date")["Close"].last().dropna()
    out: dict[str, float] = {}
    for dt, val in monthly.items():
        ym = pd.Timestamp(dt).strftime("%Y-%m")
        out[ym] = round(float(val), 4)
    return out


def _to_daily_2y(df: pd.DataFrame) -> dict[str, float]:
    """
    Extract daily close for the trailing 24 months.

    Returns {YYYY-MM-DD: value} dict, rounded to 4 dp.
    """
    if df.empty:
        return {}
    cutoff = df["Date"].max() - pd.DateOffset(months=24)
    recent = df[df["Date"] >= cutoff].copy()
    out: dict[str, float] = {}
    for _, row in recent.iterrows():
        day = pd.Timestamp(row["Date"]).strftime("%Y-%m-%d")
        val = row["Close"]
        if pd.notna(val):
            out[day] = round(float(val), 4)
    return out


# ─── Load / save JSON ─────────────────────────────────────────────────────────

def _load_existing(path: Path) -> dict[str, Any]:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"  WARNING: Could not parse existing JSON ({e}). Starting fresh.")
    return {"generated_utc": "", "assets": {}}


def _write_output(output: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_dumps(output, indent=2), encoding="utf-8")
    size_kb = path.stat().st_size / 1024
    print(f"\nWrote: {path} ({size_kb:.1f} KB)")


# ─── Merge logic ─────────────────────────────────────────────────────────────

def _merge_close(existing_close: dict[str, float], excel_close: dict[str, float]) -> dict[str, float]:
    """
    Merge existing monthly close dict with Excel monthly close dict.

    Excel data takes priority for overlapping months.
    Returns a new dict sorted by YYYY-MM key.
    """
    merged = {**existing_close, **excel_close}   # Excel overwrites existing
    return dict(sorted(merged.items()))


# ─── Main processing ──────────────────────────────────────────────────────────

def process(
    series_filter: set[str] | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """
    Read Excel, merge into existing JSON, return the updated output dict.

    Args:
        series_filter: Set of asset keys to process. None = all.
        dry_run: If True, print what would be written but don't write.
    """
    if not EXCEL_PATH.exists():
        print(f"ERROR: Excel file not found: {EXCEL_PATH}")
        sys.exit(1)

    # Read Excel
    raw_series = _read_excel_series(EXCEL_PATH, series_filter)

    if not raw_series:
        print("WARNING: No matching series found in Excel.")
        return {}

    # Load existing JSON
    existing = _load_existing(OUTPUT_PATH)
    assets_out: dict[str, Any] = existing.get("assets", {})

    print(f"\nProcessing {len(raw_series)} series...")

    for asset_key, df in raw_series.items():
        index_name = next(k for k, v in SERIES_MAP.items() if v[0] == asset_key)
        _, category, native_ccy = SERIES_MAP[index_name]

        # Compute monthly EOP close
        excel_monthly = _to_monthly_close(df)
        if not excel_monthly:
            print(f"  SKIP {asset_key}: no monthly data after resampling")
            continue

        # Compute daily 2Y
        daily_2y = _to_daily_2y(df)

        sorted_months = sorted(excel_monthly.keys())
        excel_first = sorted_months[0]
        excel_last  = sorted_months[-1]

        if asset_key in assets_out:
            # Merge: keep existing, overwrite with Excel
            existing_close = assets_out[asset_key].get("close", {})
            merged_close = _merge_close(existing_close, excel_monthly)
            all_months = sorted(merged_close.keys())
            merged_first = all_months[0]
            merged_last  = all_months[-1]

            old_count = len(existing_close)
            new_count = len(merged_close)
            added = new_count - old_count
            print(f"  MERGE  {asset_key:15s}: {old_count} → {new_count} months (+{added} new, Excel: {excel_first}→{excel_last})")

            assets_out[asset_key]["close"] = merged_close
            assets_out[asset_key]["daily_2y"] = daily_2y
            # Update meta fields
            assets_out[asset_key]["meta"]["first"] = merged_first
            assets_out[asset_key]["meta"]["last"]  = merged_last
            assets_out[asset_key]["meta"]["source"] = "Mega_Markets_Historical.xlsx"
        else:
            # New entry
            print(f"  NEW    {asset_key:15s}: {len(excel_monthly)} months ({excel_first}→{excel_last})")
            assets_out[asset_key] = {
                "meta": {
                    "name":        index_name,
                    "category":    category,
                    "native_ccy":  native_ccy,
                    "source":      "Mega_Markets_Historical.xlsx",
                    "return_type": "price",
                    "first":       excel_first,
                    "last":        excel_last,
                },
                "close":    excel_monthly,
                "daily_2y": daily_2y,
            }

    output = {
        **existing,
        "generated_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "assets": assets_out,
    }

    if dry_run:
        print("\n--dry-run: output NOT written.")
        # Print a sample of what would be written
        for key in list(raw_series.keys())[:3]:
            if key in assets_out:
                close = assets_out[key]["close"]
                months = sorted(close.keys())
                print(f"\n  {key}: {len(close)} months, {months[0]} → {months[-1]}")
                # Show first 3 and last 3
                sample = {m: close[m] for m in months[:3]}
                sample.update({m: close[m] for m in months[-3:]})
                print(f"    sample: {sample}")
    else:
        _write_output(output, OUTPUT_PATH)

    return output


# ─── CLI ─────────────────────────────────────────────────────────────────────

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="predator.ingest_mega_xl",
        description="Ingest Mega_Markets_Historical.xlsx into market_returns.json",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be written without modifying the JSON.",
    )
    parser.add_argument(
        "--series",
        type=str,
        default=None,
        help="Comma-separated asset keys to process (e.g. sp500,gold). Default: all.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    print("═" * 60)
    print("  Predator Protocol — Mega Markets Historical Ingest")
    print("═" * 60)

    series_filter: set[str] | None = None
    if args.series:
        series_filter = {s.strip() for s in args.series.split(",")}
        print(f"\nFiltering to series: {', '.join(sorted(series_filter))}")

    process(series_filter=series_filter, dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
