"""
Audit of every secondary leaderboard signal — reproduces docs/SIGNALS_STUDY.md §4.

Read-only; needs a replay cache from scripts/scoring_study.py:

    python scripts/scoring_study.py cache --config config.yaml --out /tmp/study/v4g
    python scripts/signal_audit.py /tmp/study/v4g

Each signal is replayed on every snapshot date with the definition the site
uses, then scored on weekly sample dates: the members' mean 20-day forward
return demeaned within currency (vs peers), with a t-stat over the weekly
series, and the share of members that gained / lost a holding fund within
~30 days. Two regimes: 16-ETF (Mar–mid-May, momentum-friendly) and 30-ETF
(Jun–Oct, momentum unwind).
"""
from __future__ import annotations

import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")
sys.path.insert(0, str(Path(__file__).resolve().parent))
from scoring_study import Study  # noqa: E402

REGIMES = {"30-ETF": ("2026-06-05", "2026-10-21"), "16-ETF": ("2026-03-05", "2026-05-15")}


class Panels:
    def __init__(self, st: Study):
        self.st = st
        self.dates = [pd.Timestamp(d) for d in st.dates]
        L = st.L
        self.E = L.pivot_table(index="ticker", columns="asof", values="etf_count").fillna(0)
        self.R = L.pivot_table(index="ticker", columns="asof", values="leaderboard_rank")
        self.SC = L.pivot_table(index="ticker", columns="asof", values="final_score")
        self.tiers = {d: g.set_index("ticker")["tiers"] for d, g in L.groupby("asof")}
        H = st.H
        self.H_by = st.H_by
        # first sighting of each (ETF, ticker) pair and each ETF (NEW + maturity)
        S = pd.read_parquet(Path(sys.argv[1]) / "history_sanitized.parquet")
        S["Holdings_As_Of"] = pd.to_datetime(S["Holdings_As_Of"])
        self.pair_first = S.groupby(["ETF_Ticker", "ticker"])["Holdings_As_Of"].min()
        self.etf_first = S.groupby("ETF_Ticker")["Holdings_As_Of"].min()
        self.tier_of = H.drop_duplicates("ETF_Ticker").set_index("ETF_Ticker")["tier"]
        # score streak: consecutive day-over-day moves of the same sign
        diff = np.sign(self.SC.diff(axis=1)).fillna(0)
        streak = pd.DataFrame(0, index=diff.index, columns=diff.columns, dtype=float)
        prev = pd.Series(0.0, index=diff.index)
        for c in diff.columns:
            s = diff[c]
            cur = np.where(s > 0, np.where(prev > 0, prev + 1, 1), np.where(s < 0, np.where(prev < 0, prev - 1, -1), 0))
            prev = pd.Series(cur, index=diff.index)
            streak[c] = prev
        self.streak = streak

    def back(self, d, days):
        t = d - pd.Timedelta(days=days)
        c = [x for x in self.dates if x <= t]
        return c[-1] if c else None

    def fwd_date(self, d, days):
        t = d + pd.Timedelta(days=days)
        c = [x for x in self.dates if x >= t]
        return c[0] if c else None

    def per_holding_7d(self, d):
        p = self.back(d, 7)
        if p is None:
            return pd.Series(dtype=float), pd.Series(dtype=float)
        a = self.H_by[d][["ETF_Ticker", "ticker", "rank", "weight"]]
        b = self.H_by[p][["ETF_Ticker", "ticker", "rank", "weight"]]
        m = a.merge(b, on=["ETF_Ticker", "ticker"], suffixes=("", "_p"))
        m["rd"] = m["rank_p"] - m["rank"]
        m["wf"] = (m["weight"] - m["weight_p"]) / m["weight_p"]
        return m.groupby("ticker")["rd"].mean(), m.groupby("ticker")["wf"].mean()

    def state(self, d) -> dict[str, set]:
        st, lb, h = self.st, self.st.L_by[d], self.H_by[d]
        ec = lb["etf_count"]
        nc = h[h.tier != "Core"]
        conv = nc[nc.conviction >= 0.6].groupby("ticker")["ETF_Ticker"].nunique().reindex(lb.index).fillna(0)
        tnc = nc.groupby("ticker")["tier"].nunique().reindex(lb.index).fillna(0)
        hc = (ec >= 4) & (conv >= 2) & (tnc >= 2)
        rd7, wf7 = self.per_holding_7d(d)
        rd7 = rd7.reindex(lb.index).fillna(0); wf7 = wf7.reindex(lb.index).fillna(0)
        streak = self.streak[d].reindex(lb.index).fillna(0)
        p30 = self.back(d, 30)
        d30 = (ec - self.E[p30].reindex(lb.index).fillna(0)) if p30 is not None else ec * 0
        tiers = lb["tiers"].fillna("")
        tset = tiers.str.split(r" \+ ")
        has = lambda t: tset.apply(lambda x: t in x)
        # linear-mass dominance for CONCENTRATED
        h2 = h.assign(lm=h.tier_points * h.conviction)
        tot = h2.groupby("ticker")["lm"].transform("sum")
        smax = (h2["lm"] / tot).groupby(h2["ticker"]).max().reindex(lb.index).fillna(1)
        owmax = (h2["weight"] * h2["n_holdings"]).groupby(h2["ticker"]).max().reindex(lb.index).fillna(0)
        conc = (~hc) & (((smax >= 0.85) & (ec >= 2)) | ((ec == 1) & (owmax >= 4)))
        # NEW: pair first seen within 14 days, ETF tracked ≥ 30 days
        cutoff = d - pd.Timedelta(days=14)
        pf = self.pair_first.reindex(list(zip(h.ETF_Ticker, h.ticker))).to_numpy()
        mature = h.ETF_Ticker.map(self.etf_first).le(d - pd.Timedelta(days=30)).to_numpy()
        is_new = (pf > np.datetime64(cutoff)) & mature
        new = set(h.ticker[is_new])
        # Quality+: gained a Quality-tier fund vs ~30d ago
        qual = set(self.tier_of.index[self.tier_of == "Quality"])
        def held(dd):
            hh = self.H_by[dd]; hh = hh[hh.ETF_Ticker.isin(qual)]
            return hh.groupby("ticker")["ETF_Ticker"].apply(set)
        qnow = held(d); qthen = held(p30) if p30 is not None else pd.Series(dtype=object)
        qadd = {t for t, s in qnow.items() if s - qthen.get(t, set())}
        out = {
            "HC (new)": hc,
            "Pre-HC": (~hc) & (ec >= 3) & (conv >= 1) & (tnc >= 2) & (rd7 > 0) & (streak >= 2),
            "Pre-HC (old: exactly 3 funds)": (ec == 3) & (rd7 > 0) & (streak >= 2),
            "Stealth-Buy": (ec <= 2) & (wf7 > 0.10) & (rd7 >= 0),
            "CONCENTRATED": conc,
            "SPECULATIVE_BETA": has("Trend") & ~(has("Quality") | has("Scout")) & (ec < 4),
            "Quality+": lb.index.isin(qadd),
            "Q+Q chip": has("Quant") & has("Quality") & (d30 > 0),
            "ACCUM badge (Δ30 ≥ 2)": d30 >= 2,
            "EARLY badge": (ec >= 2) & (ec <= 4) & ((wf7 > 0.005) | (d30 > 0)),
            "CROWDED badge": (ec >= 9) & (wf7.abs() < 0.02) & (d30 == 0),
            "Funds ↑ (Δ30 > 0)": d30 > 0,
            "Funds ↓ (Δ30 < 0)": d30 < 0,
            "NEW": lb.index.isin(new),
            "regime: accelerating": (streak > 3) & (rd7 >= 2),
            "regime: rising": (streak > 0) & (rd7 > 0) & ~((streak > 3) & (rd7 >= 2)),
            "regime: weakening": (streak < 0) & (rd7 < 0) & ~((streak < -3) & (rd7 <= -2)),
            "regime: declining": (streak < -3) & (rd7 <= -2),
        }
        # Extreme-moves ribbon (positive and negative members, ≥ 3 funds, top-8 by |z|)
        drivers = {"d30": d30, "rdelta": (self.R[p30].reindex(lb.index) - lb["leaderboard_rank"]) if p30 is not None else ec * 0,
                   "wf7": wf7.where(wf7.abs() >= 0.05)}
        z = pd.DataFrame({k: (v - v.mean()) / v.std() for k, v in drivers.items()})
        best = z.abs().max(axis=1).where(ec >= 3)
        top = best[best >= 2].sort_values(ascending=False).index[:8]
        sign = pd.Series({t: np.sign(z.loc[t, z.loc[t].abs().idxmax()]) for t in top}, dtype=float)
        out["ribbon (up)"] = lb.index.isin(sign.index[sign > 0])
        out["ribbon (down)"] = lb.index.isin(sign.index[sign < 0])
        return {k: set(lb.index[np.asarray(v, dtype=bool)]) for k, v in out.items()}


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__); return 2
    st = Study(Path(sys.argv[1]))
    P = Panels(st)
    pd.set_option("display.width", 250)
    for lab, (a, b) in REGIMES.items():
        rows = {}
        for d in st.weekly(a, b):
            fr = st.fwd_ret(d, 20)
            d30 = P.fwd_date(d, 30)
            if not len(fr) or d30 is None:
                continue
            fr = st.demean(fr.reindex(st.L_by[d].index))
            e0, e30 = P.E[d], P.E[d30].reindex(P.E.index).fillna(0)
            for k, s in P.state(d).items():
                s = list(s)
                r = rows.setdefault(k, {"n": [], "fwd20": [], "gain": [], "lose": []})
                r["n"].append(len(s))
                if s:
                    r["fwd20"].append(fr.reindex(s).mean())
                    r["gain"].append((e30.reindex(s) > e0.reindex(s)).mean())
                    r["lose"].append((e30.reindex(s) < e0.reindex(s)).mean())
        base_lose = None
        out = {}
        for k, r in rows.items():
            f = np.array([x for x in r["fwd20"] if pd.notna(x)])
            se = f.std(ddof=1) / np.sqrt(len(f)) if len(f) > 2 else np.nan
            out[k] = {"avg names": np.mean(r["n"]), "fwd20 vs peers": f.mean() if len(f) else np.nan,
                      "t": f.mean() / se if se else np.nan, "gained fund 30d": np.nanmean(r["gain"]) if r["gain"] else np.nan,
                      "lost fund 30d": np.nanmean(r["lose"]) if r["lose"] else np.nan}
        print(f"\n=== {lab}")
        print(pd.DataFrame(out).T.round(4).to_string())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
