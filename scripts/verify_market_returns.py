#!/usr/bin/env python3
"""Verify Markets returns against independent sources: catch wrong values, never flag real ones.

For every commodity the World Bank "Pink Sheet" (CMO monthly prices) covers, and for every
FRED-backed asset, this compares the published monthly series in market_returns.json with the
reference series month by month and year by year:

* level check  - the ratio site/reference should be one constant per asset (the unit conversion,
                 e.g. $/mt -> $/lb). A month whose ratio departs from that constant by more than
                 LEVEL_TOL is a splice/unit error or a wrong value, whatever its size.
* annual check - the calendar-year return (Dec -> Dec) from the site vs the reference. Real
                 outliers (cocoa +178% in 2024) match the reference and pass; garbage (-100%) fails.

Exit status is non-zero when any level break or any annual difference >= ANNUAL_FAIL_PP is found.
Unreachable reference sources are skipped with a warning so a network blip never blocks a build.

Usage:
  python scripts/verify_market_returns.py [--file docs/data/market_returns.json | --url URL]
                                          [--report docs/data/market_verification.json]
"""
from __future__ import annotations

import argparse
import io
import json
import statistics
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
WB_URL = ("https://thedocs.worldbank.org/en/doc/18675f1d1639c7a34d463f59263ba0a2-0050012025/"
          "related/CMO-Historical-Data-Monthly.xlsx")
FRED_CSV = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={sid}"

# site asset key -> World Bank "Monthly Prices" column header
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
REFERENCE_PATH = ROOT / "data" / "reference" / "wb_cmo_monthly.json"
# Tolerances by price basis. IMF/FRED monthly averages track the World Bank closely (grade and
# delivery-point differences stay within about +/-15%); month-end series (daily oil/gas, futures)
# legitimately diverge from monthly averages in violent months, so they get wider bands. Unit or
# splice errors are x2 to x1000 and fail either way.
# Unit/splice errors are x2.2 (lb vs kg) up to x1000 (+120% .. -99.9%); genuine source differences
# seen across all 23 series stay within about +/-35% (IMF vs US HRW wheat since 2023, Brent month-end
# in March 2020 at -55%). These bands separate the two.
LEVEL_TOL = {"average": 0.40, "month_end": 0.60}
ANNUAL_FAIL_PP = {"average": 25.0, "month_end": 60.0}
ANNUAL_WARN_PP = 8.0
MONTH_END_ASSETS = {"gold", "silver", "platinum", "palladium", "wti_crude", "brent_crude", "natural_gas"}
# Before 2009 the IMF iron-ore series is the annual benchmark contract and the World Bank series is
# spot: different prices by design, so compare only from 2009.
COMPARE_FROM = {"iron_ore": "2009-01"}


def _get(url: str, timeout: int = 90) -> bytes:
    try:  # requests handles FRED's server reliably; plain urllib gets disconnected there
        import requests  # noqa: PLC0415
        resp = requests.get(url, timeout=timeout, headers={"User-Agent": "market-returns-verifier/1.0"})
        resp.raise_for_status()
        return resp.content
    except ImportError:
        req = urllib.request.Request(url, headers={"User-Agent": "market-returns-verifier/1.0"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read()


def load_world_bank() -> dict[str, dict[str, float]]:
    import openpyxl  # noqa: PLC0415 - optional dependency, only needed here
    wb = openpyxl.load_workbook(io.BytesIO(_get(WB_URL, timeout=180)), read_only=True, data_only=True)
    ws = wb["Monthly Prices"]
    header = None
    series: dict[str, dict[str, float]] = {}
    for row in ws.iter_rows(values_only=True):
        if header is None:
            if row and any(isinstance(c, str) and c.strip() == "Gold" for c in row):
                header = [c.strip() if isinstance(c, str) else c for c in row]
            continue
        label = row[0]
        if not (isinstance(label, str) and len(label) == 7 and label[4] == "M"):
            continue
        ym = f"{label[:4]}-{label[5:]}"
        for key, col in WB_COLUMNS.items():
            if col in header:
                v = row[header.index(col)]
                if isinstance(v, (int, float)) and v > 0:
                    series.setdefault(key, {})[ym] = float(v)
    return series


def load_fred(series_id: str) -> dict[str, float]:
    text = _get(FRED_CSV.format(sid=series_id)).decode("utf-8")
    by_month: dict[str, list[float]] = {}
    for line in text.splitlines()[1:]:
        parts = line.split(",")
        if len(parts) != 2 or parts[1] in ("", "."):
            continue
        try:
            by_month.setdefault(parts[0][:7], []).append(float(parts[1]))
        except ValueError:
            continue
    # daily FRED series (oil, gas) -> month-end value, monthly series -> the value itself
    return {ym: vals[-1] for ym, vals in by_month.items() if vals[-1] > 0}


def annual_returns(monthly: dict[str, float]) -> dict[int, float]:
    out = {}
    years = sorted({int(ym[:4]) for ym in monthly})
    for y in years:
        a, b = monthly.get(f"{y - 1}-12"), monthly.get(f"{y}-12")
        if a and b:
            out[y] = b / a - 1
    return out


def compare(key: str, site: dict[str, float], ref: dict[str, float], ref_name: str) -> dict:
    basis = "month_end" if key in MONTH_END_ASSETS else "average"
    start = COMPARE_FROM.get(key, "")
    common = sorted(ym for ym in set(site) & set(ref) if ym >= start)
    result = {"asset": key, "reference": ref_name, "basis": basis, "months_compared": len(common),
              "level_breaks": [], "annual_fail": [], "annual_warn": []}
    if len(common) < 12:
        result["skipped"] = "too little overlap"
        return result
    ratios = {ym: site[ym] / ref[ym] for ym in common}
    k = statistics.median(ratios.values())
    for ym in common:
        dev = ratios[ym] / k - 1
        if abs(dev) > LEVEL_TOL[basis]:
            result["level_breaks"].append({"month": ym, "site": site[ym], "reference": ref[ym],
                                           "off_by_pct": round(dev * 100, 1)})
    result["unit_factor"] = k
    sa, ra = annual_returns(site), annual_returns(ref)
    for y in sorted(y for y in set(sa) & set(ra) if f"{y - 1}-12" >= start):
        d = (sa[y] - ra[y]) * 100
        row = {"year": y, "site_pct": round(sa[y] * 100, 1), "reference_pct": round(ra[y] * 100, 1),
               "diff_pp": round(d, 1)}
        if abs(d) >= ANNUAL_FAIL_PP[basis]:
            result["annual_fail"].append(row)
        elif abs(d) >= ANNUAL_WARN_PP:
            result["annual_warn"].append(row)
    return result


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    src = ap.add_mutually_exclusive_group()
    src.add_argument("--file", default=str(ROOT / "docs" / "data" / "market_returns.json"))
    src.add_argument("--url")
    ap.add_argument("--report", default="")
    ap.add_argument("--fred", action="store_true", help="also re-download each FRED series and compare")
    ap.add_argument("--live-world-bank", action="store_true",
                    help="download the World Bank file instead of using the vendored reference")
    args = ap.parse_args()

    data = json.loads(_get(args.url) if args.url else Path(args.file).read_bytes())
    assets = data.get("assets", {})

    checks: list[dict] = []
    warnings: list[str] = []
    wb: dict[str, dict[str, float]] = {}
    try:
        if args.live_world_bank:
            wb = load_world_bank()
        else:
            ref = json.loads(REFERENCE_PATH.read_text(encoding="utf-8"))["series"]
            wb = {k: {ym: float(v) for ym, v in s_["monthly"]} for k, s_ in ref.items()}
    except Exception as exc:  # never block a build on a missing reference; report it instead
        warnings.append(f"World Bank reference unavailable: {type(exc).__name__}: {exc}")

    for key, asset in assets.items():
        site = {ym: float(v) for ym, v in asset.get("monthly", []) if v and float(v) > 0}
        meta = asset.get("meta", {})
        if key in wb:
            checks.append(compare(key, site, wb[key], "World Bank CMO monthly"))
        source = str(meta.get("source", ""))
        if source.startswith("fred:") and args.fred:
            sid = source.split(":", 1)[1]
            try:
                checks.append(compare(key, site, load_fred(sid), f"FRED {sid}"))
            except Exception as exc:
                warnings.append(f"FRED {sid} unavailable for {key}: {type(exc).__name__}")

    breaks = sum(len(c["level_breaks"]) for c in checks)
    fails = sum(len(c["annual_fail"]) for c in checks)
    report = {"asof": data.get("asof"), "checks": checks, "warnings": warnings,
              "summary": {"assets_checked": len({c["asset"] for c in checks}),
                          "comparisons": len(checks), "level_breaks": breaks, "annual_fail": fails}}
    if args.report:
        Path(args.report).write_text(json.dumps(report, indent=1), encoding="utf-8")

    for w in warnings:
        print(f"WARN {w}")
    for c in checks:
        if c["level_breaks"] or c["annual_fail"]:
            yrs = ", ".join(f"{r['year']}: site {r['site_pct']:+}% vs ref {r['reference_pct']:+}%" for r in c["annual_fail"][:6])
            mons = [b["month"] for b in c["level_breaks"]]
            print(f"FAIL {c['asset']:12} vs {c['reference']:24} level-breaks={len(mons)} "
                  f"({mons[0] if mons else ''}..{mons[-1] if mons else ''})  annual: {yrs}")
    print(f"SUMMARY {report['summary']}")
    return 1 if (breaks or fails) else 0


if __name__ == "__main__":
    sys.exit(main())
