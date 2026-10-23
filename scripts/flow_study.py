"""Do leveraged-ETF fund flows predict returns? (2026-10)

Pools every long-side fund in data/flows/all_leveraged_etf_flows.csv and asks
what a fund's 5-day flow adds beyond its own 20-day price move:

    price z = 20-day NAV return / (60-day daily-return std × √20)
    flow z  = 5-day net flow   / (60-day daily-flow std × √5)
    outcome = NAV return over the next 20 days, entered the day after

Each fund-day's outcome is compared with the average of all funds in the same
price state (down ≤ −1σ / middle / up ≥ +1σ) on the same date, so the table
isolates the flow's contribution. Results by era; docs/SIGNALS_STUDY.md,
"Fund flows". Run: python scripts/flow_study.py
"""
from __future__ import annotations

import numpy as np
import pandas as pd

SRC = "data/flows/all_leveraged_etf_flows.csv"


def main() -> None:
    d = pd.read_csv(SRC, usecols=["date", "ticker", "usd_flow", "nav", "category"], parse_dates=["date"])
    d = d[~d.category.str.contains("Short")].sort_values(["ticker", "date"])
    parts = []
    for _, g in d.groupby("ticker"):
        g = g[g.nav > 0].copy()
        if len(g) < 200:
            continue
        r = g.nav.pct_change()
        g["zf5"] = g.usd_flow.rolling(5).sum() / (g.usd_flow.rolling(60).std() * np.sqrt(5))
        g["zp"] = (g.nav / g.nav.shift(20) - 1) / (r.rolling(60).std() * np.sqrt(20))
        g["f20"] = g.nav.shift(-21) / g.nav.shift(-1) - 1
        g["flow_vs_prior_ret"] = r.shift(1)
        g["zf1"] = g.usd_flow / g.usd_flow.rolling(60).std()
        parts.append(g)
    a = pd.concat(parts).replace([np.inf, -np.inf], np.nan).dropna(subset=["zf5", "zp", "f20"])
    print(f"{len(a):,} fund-days, {a.ticker.nunique()} funds, {a.date.min():%Y-%m} to {a.date.max():%Y-%m}")
    print("corr(1-day flow z, prior-day return):", round(a.zf1.corr(a.flow_vs_prior_ret), 3))

    a["price"] = pd.cut(a.zp, [-np.inf, -1, 1, np.inf], labels=["down", "mid", "up"])
    a["flow"] = pd.cut(a.zf5, [-np.inf, -0.8, 0.8, np.inf], labels=["out", "flat", "in"])
    a["era"] = pd.cut(a.date.dt.year, [2015, 2020, 2023, 2026], labels=["2016-20", "2021-23", "2024-26"])
    a["excess"] = a.f20 - a.groupby(["date", "price"], observed=True).f20.transform("mean")
    t = a.groupby(["price", "flow", "era"], observed=True).agg(n=("excess", "size"), excess_pct=("excess", "mean"))
    t["excess_pct"] *= 100
    print("\n20-day return vs same-date funds in the same price state (%):")
    print(t.round(2).unstack("era").to_string())


if __name__ == "__main__":
    main()
