#!/usr/bin/env python3
"""Build docs/data/etf_returns.json: month-end NAV series for the 160 Fund Flows ETFs.

The file uses the same asset schema as docs/data/market_returns.json
(``{"assets": {key: {"meta": {...}, "monthly": [[YYYY-MM, value], ...]}}}``) so the
Markets tabs (Return Matrix, Periodic Table, Hold Lab, Drawdowns, Seasonality) and the
Portfolio Lab can use the leveraged-ETF universe without special cases.

Values are the source NAV exactly as stored (split-adjusted by the flow pipeline); the last
month may be partial and is marked by ``asof``.

Run: python scripts/build_etf_returns.py [--flows-dir docs/data/flows] [--out docs/data/etf_returns.json]
"""
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _group_label(category: str) -> str:
    """'4. Single-Stock Leveraged (Bull) — AI, Semis' -> 'Single-Stock Leveraged (Bull) — AI, Semis'."""
    return re.sub(r"^\s*\d+\.\s*", "", category or "").strip() or "Other"


def build(flows_dir: Path) -> dict:
    catalog = json.loads((flows_dir / "catalog.json").read_text(encoding="utf-8"))
    assets: dict[str, dict] = {}
    asof = ""
    for inst in catalog["instruments"]:
        ticker = inst["ticker"]
        path = flows_dir / f"{ticker}.json"
        if not path.exists():
            continue
        rows = json.loads(path.read_text(encoding="utf-8")).get("data") or []
        month_end: dict[str, float] = {}
        for row in rows:
            nav = row.get("nav")
            date = str(row.get("date") or "")
            if not date or nav is None:
                continue
            try:
                value = float(nav)
            except (TypeError, ValueError):
                continue
            if value <= 0:
                continue
            month_end[date[:7]] = value  # rows are date-ascending, so the last write is month-end
            asof = max(asof, date)
        if len(month_end) < 2:
            continue
        monthly = [[ym, round(v, 6)] for ym, v in sorted(month_end.items())]
        group = _group_label(inst.get("category", ""))
        order_match = re.match(r"^\s*(\d+)\.", inst.get("category", "") or "")
        group_order = int(order_match.group(1)) if order_match else 99
        assets[f"etf_{ticker.lower()}"] = {
            "meta": {
                "name": f"{ticker} · {inst.get('fund_name', ticker)}",
                "ticker": ticker,
                "fund_name": inst.get("fund_name", ticker),
                "category": "lev_etf",
                "group": group,
                "group_order": group_order,
                "leverage": inst.get("leverage", ""),
                "underlying": inst.get("underlying", ""),
                "issuer": inst.get("issuer", ""),
                "aum_m": inst.get("aum_m"),
                "native_ccy": "USD",
                "source": "fund flows NAV",
                "return_type": "nav",
                "first": monthly[0][0],
                "last": monthly[-1][0],
            },
            "monthly": monthly,
        }
    return {
        "asof": asof,
        "generated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "count": len(assets),
        "assets": assets,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--flows-dir", default=str(ROOT / "docs" / "data" / "flows"))
    parser.add_argument("--out", default=str(ROOT / "docs" / "data" / "etf_returns.json"))
    args = parser.parse_args()
    data = build(Path(args.flows_dir))
    out = Path(args.out)
    out.write_text(json.dumps(data, separators=(",", ":"), sort_keys=True), encoding="utf-8")
    print(f"wrote {out} · {data['count']} ETFs · asof {data['asof']} · {out.stat().st_size // 1024} KB")


if __name__ == "__main__":
    main()
