#!/usr/bin/env python3
"""Vendor World Bank CMO ("Pink Sheet") monthly commodity prices into the repo.

Writes data/reference/wb_cmo_monthly.json: {series_key: {"label", "unit", "monthly": [[YYYY-MM, v]]}}
for every commodity the Markets tab shows. The file is an independent reference used to
(1) supply verified pre-FRED history for commodities and (2) verify every published month in CI
(scripts/verify_market_returns.py) without depending on the World Bank URL at build time.

Run when the World Bank publishes a new Pink Sheet (monthly) or to refresh:
  python scripts/vendor_wb_cmo.py [--xlsx path | --url URL]
"""
from __future__ import annotations

import argparse
import io
import json
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "data" / "reference" / "wb_cmo_monthly.json"
DEFAULT_URL = ("https://thedocs.worldbank.org/en/doc/18675f1d1639c7a34d463f59263ba0a2-0050012025/"
               "related/CMO-Historical-Data-Monthly.xlsx")

# Markets asset key -> World Bank "Monthly Prices" column header
WB_COLUMNS = {
    "gold": "Gold", "silver": "Silver", "platinum": "Platinum",
    "wti_crude": "Crude oil, WTI", "brent_crude": "Crude oil, Brent",
    "natural_gas": "Natural gas, US", "coal": "Coal, Australian",
    "copper": "Copper", "aluminum": "Aluminum", "nickel": "Nickel", "zinc": "Zinc",
    "iron_ore": "Iron ore, cfr spot", "tin": "Tin", "lead": "Lead",
    "wheat": "Wheat, US HRW", "corn": "Maize", "soybeans": "Soybeans",
    "cotton": "Cotton, A Index", "sugar": "Sugar, world", "coffee": "Coffee, Arabica",
    "cocoa": "Cocoa", "rice": "Rice, Thai 5%", "palm_oil": "Palm oil",
}


def parse(xlsx_bytes: bytes) -> dict:
    import openpyxl  # noqa: PLC0415

    wb = openpyxl.load_workbook(io.BytesIO(xlsx_bytes), read_only=True, data_only=True)
    ws = wb["Monthly Prices"]
    header = units = None
    out: dict[str, dict] = {}
    for row in ws.iter_rows(values_only=True):
        if header is None:
            if row and any(isinstance(c, str) and c.strip() == "Gold" for c in row):
                header = [c.strip() if isinstance(c, str) else c for c in row]
            continue
        if units is None:
            units = list(row)
            continue
        label = row[0]
        if not (isinstance(label, str) and len(label) == 7 and label[4] == "M"):
            continue
        ym = f"{label[:4]}-{label[5:]}"
        for key, col in WB_COLUMNS.items():
            if col not in header:
                continue
            idx = header.index(col)
            v = row[idx]
            if isinstance(v, (int, float)) and v > 0:
                entry = out.setdefault(key, {"label": col, "unit": str(units[idx] or "").strip("() "), "monthly": []})
                entry["monthly"].append([ym, round(float(v), 6)])
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--xlsx")
    ap.add_argument("--url", default=DEFAULT_URL)
    args = ap.parse_args()
    if args.xlsx:
        raw = Path(args.xlsx).read_bytes()
    else:
        import requests  # noqa: PLC0415
        resp = requests.get(args.url, timeout=180)
        resp.raise_for_status()
        raw = resp.content
    series = parse(raw)
    last = max(s["monthly"][-1][0] for s in series.values())
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps({
        "source": "World Bank Commodity Price Data (The Pink Sheet), CMO-Historical-Data-Monthly",
        "basis": "monthly average prices",
        "through": last,
        "vendored_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "series": series,
    }, separators=(",", ":"), sort_keys=True), encoding="utf-8")
    print(f"wrote {OUT} · {len(series)} series · through {last} · {OUT.stat().st_size // 1024} KB")


if __name__ == "__main__":
    main()
