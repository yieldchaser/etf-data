"""
Walk-forward scoring study — reproduces docs/SCORING_V4_STUDY.md.

READ-ONLY with respect to the repo: reads the parquet holdings store and the
per-stock price files, writes only into the --cache directory you pass.

Subcommands
    cache     Replay the REAL engine (compute_leaderboard) on every snapshot
              date for a config; store per-date per-holding frames,
              leaderboards, the sanitized history and a price panel.
                python scripts/scoring_study.py cache --config config.yaml --out /tmp/study/v4
    variants  Score variants as pure functions of a cache's per-holding frames
              (V0 is a vectorized re-implementation of the pre-v4 apex
              final_score and is checked to match the engine exactly first).
                python scripts/scoring_study.py variants --cache /tmp/study/v3
    engine    Compare engine caches directly (e.g. pre-v4 vs v4 config).
                python scripts/scoring_study.py engine --cache /tmp/study/v3 /tmp/study/v4

Method
    • Weekly sample dates; two regimes: the 30-ETF universe (2026-05-25 →)
      as the main sample and the 16-ETF universe (Feb → mid-May) as an
      out-of-sample robustness check.
    • Entry at the T+2 close after the holdings as-of date (median
      publication lag is 1 day); forward 5/10/20/40 trading days; returns
      winsorized 1/99% and demeaned within currency groups so the study
      measures stock selection, not which country's market rallied.
    • Metrics: cross-sectional Spearman IC (Newey-West t on the weekly
      series), top-50 demeaned return, and internal metrics — top-50
      breadth-filler share, forward fund-accumulation IC (score vs 30-day
      change in holder count), top-50 forward exit rate, top-50 → top-100
      retention after ~28 days, daily top-100 rank churn.
"""
from __future__ import annotations

import argparse
import glob
import json
import math
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from conviction.scoring import Config, compute_leaderboard  # noqa: E402
from conviction.history import accumulation_velocity        # noqa: E402

P30 = ("2026-05-25", "2026-10-21")
P16 = ("2026-02-20", "2026-05-17")
HORIZONS = (5, 10, 20, 40)
NW_LAGS = {5: 1, 10: 2, 20: 4, 40: 8}


# ═══════════════════════════════════════════════════════════════════════════
# cache
# ═══════════════════════════════════════════════════════════════════════════
def build_cache(config: Path, out: Path) -> None:
    out.mkdir(parents=True, exist_ok=True)
    h = pd.read_parquet(REPO / "data/history_parquet/year=2026/holdings.parquet")
    h["Holdings_As_Of"] = pd.to_datetime(h["Holdings_As_Of"])
    cfg = Config.from_yaml(config)
    holds, lbs, t0 = [], [], time.time()
    for i, d in enumerate(sorted(h["Holdings_As_Of"].unique())):
        d = pd.Timestamp(d)
        lb, lat = compute_leaderboard(h, cfg, as_of=d)
        lat = lat[["ETF_Ticker", "ticker", "weight", "rank", "n_holdings", "tier",
                   "tier_points", "conviction", "score", "Holdings_As_Of"]].assign(asof=d)
        lb = lb[["ticker", "final_score", "avg_conviction", "etf_count", "flag",
                 "leaderboard_rank", "tiers", "total_weight"]].assign(asof=d)
        holds.append(lat); lbs.append(lb)
        if i % 25 == 0:
            print(f"  {i:3d} {d.date()} {len(lb)} names  {time.time() - t0:.0f}s", flush=True)
    pd.concat(holds).to_parquet(out / "holdings_by_date.parquet")
    pd.concat(lbs).to_parquet(out / "leaderboard_by_date.parquet")
    san = cfg.sanitizer.apply(h[h["ETF_Ticker"].isin(cfg.etf_lookup())].dropna(subset=["ticker", "weight"]))
    san[san["weight"] > 0].to_parquet(out / "history_sanitized.parquet")

    cols, ccy = [], {}
    for f in glob.glob(str(REPO / "docs/data/details/*.json")):
        d = json.load(open(f, encoding="utf-8"))
        if d.get("prices"):
            ccy[d["ticker"]] = d.get("currency") or "UNK"
            cols.append(pd.Series({p[0]: p[1] for p in d["prices"]}, name=d["ticker"], dtype=float))
    px = pd.concat(cols, axis=1)
    px.index = pd.to_datetime(px.index)
    px.sort_index().to_parquet(out / "prices.parquet")
    pd.Series(ccy, name="currency").to_frame().to_parquet(out / "currency.parquet")
    print(f"cache written to {out}  ({time.time() - t0:.0f}s)")


# ═══════════════════════════════════════════════════════════════════════════
# shared evaluation machinery
# ═══════════════════════════════════════════════════════════════════════════
class Study:
    def __init__(self, cache: Path):
        self.H = pd.read_parquet(cache / "holdings_by_date.parquet")
        self.L = pd.read_parquet(cache / "leaderboard_by_date.parquet")
        S = pd.read_parquet(cache / "history_sanitized.parquet")
        self.ccy = pd.read_parquet(cache / "currency.parquet")["currency"]
        self.dates = sorted(self.H["asof"].unique())
        self.H_by = {pd.Timestamp(d): g for d, g in self.H.groupby("asof")}
        self.L_by = {pd.Timestamp(d): g.set_index("ticker") for d, g in self.L.groupby("asof")}
        S["Holdings_As_Of"] = pd.to_datetime(S["Holdings_As_Of"])
        self.first = S.groupby(["ticker", "ETF_Ticker"])["Holdings_As_Of"].min().reset_index()
        self.pts = self.H.drop_duplicates("ETF_Ticker").set_index("ETF_Ticker")["tier_points"]
        self.first["pts"] = self.first["ETF_Ticker"].map(self.pts)
        px = pd.read_parquet(cache / "prices.parquet")
        px = px[px.index >= "2025-01-01"].where(lambda x: x > 0)
        px = px.mask(px.pct_change(fill_method=None).abs() > 0.6)     # bad ticks / unadjusted splits
        self.cal = px.index[px.notna().sum(axis=1) > 500]
        self.px = px.reindex(self.cal).ffill(limit=5)
        self.panel = self.L.pivot_table(index="ticker", columns="asof", values="final_score")
        self._kick: dict = {}

    # prices ----------------------------------------------------------------
    def _pos(self, d) -> int:
        return int(self.cal.searchsorted(pd.Timestamp(d), side="right") - 1)

    def fwd_ret(self, d, h: int, lag: int = 2) -> pd.Series:
        i = self._pos(d) + lag
        if i + h >= len(self.cal):
            return pd.Series(dtype=float)
        return self.px.iloc[i + h] / self.px.iloc[i] - 1

    def past_ret(self, d, n: int, skip: int = 0) -> pd.Series:
        i = self._pos(d) - skip
        return self.px.iloc[i] / self.px.iloc[max(0, i - n)] - 1

    def demean(self, r: pd.Series) -> pd.Series:
        r = r.dropna()
        lo, hi = r.quantile([0.01, 0.99])
        r = r.clip(lo, hi)
        g = pd.Series(r.index, index=r.index).map(self.ccy).fillna("UNK")
        g = g.where(g.map(g.value_counts()) >= 15, "OTHER")
        return r - r.groupby(g).transform("mean")

    def weekly(self, start, end) -> list[pd.Timestamp]:
        out, last = [], None
        for d in (pd.Timestamp(x) for x in self.dates):
            if pd.Timestamp(start) <= d <= pd.Timestamp(end) and (last is None or (d - last).days >= 7):
                out.append(d); last = d
        return out

    def nearest(self, target) -> pd.Timestamp:
        arr = np.array(self.dates, dtype="datetime64[ns]")
        return pd.Timestamp(self.dates[min(np.searchsorted(arr, np.datetime64(target)), len(arr) - 1)])

    def kicker(self, d):
        if d not in self._kick:
            cols = [c for c in self.panel.columns if c <= d][-10:]
            v = accumulation_velocity(self.panel[cols]).set_index("ticker")
            self._kick[d] = (1 + 0.25 * np.tanh(v["velocity_z"].fillna(0) / 2)
                             + 0.10 * np.tanh(v["ignition_z"].fillna(0) / 2))
        return self._kick[d]

    # metrics ---------------------------------------------------------------
    def evaluate(self, scorers: dict, dates: list, top: int = 50) -> pd.DataFrame:
        """scorers: name → f(date) -> Series(score by ticker), plus the name's
        universe frame lookup via self.L_by (leaderboard of that cache)."""
        rows = []
        for d in dates:
            lb = self.L_by[d]
            rets = {h: self.fwd_ret(d, h) for h in HORIZONS}
            rets = {h: self.demean(r.reindex(lb.index)) for h, r in rets.items() if len(r)}
            d30 = self.nearest(d + pd.Timedelta(days=28))
            later = self.L_by[d30] if (d30 - d).days >= 21 else None
            multi = set(lb.index[lb["etf_count"] >= 2])
            for name, f in scorers.items():
                s = f(d).reindex(lb.index).fillna(0)
                topn = s.sort_values(ascending=False).index[:top]
                rec = {"date": d, "variant": name}
                for h, r in rets.items():
                    x = pd.concat([s, r], axis=1, keys=["s", "r"]).dropna()
                    rec[f"ic{h}"] = _spearman(x.s, x.r)
                    xm = x[x.index.isin(multi)]
                    rec[f"icM{h}"] = _spearman(xm.s, xm.r)
                    rec[f"top{h}"] = r.reindex(topn).mean()
                rec["filler_top50"] = float((lb.loc[topn, "avg_conviction"] < 0.5).mean())
                rec["single_fund_top50"] = float((lb.loc[topn, "etf_count"] == 1).mean())
                rec["nonUSD_top50"] = float((pd.Series(topn).map(self.ccy).fillna("UNK") != "USD").mean())
                if later is not None:
                    dec = later["etf_count"].reindex(lb.index).fillna(0) - lb["etf_count"]
                    rec["ic_detf"] = _spearman(s, dec)
                    rec["exit_top50"] = float((dec.reindex(topn) < 0).mean())
                    top100 = set(f(d30).sort_values(ascending=False).index[:100])
                    rec["retain50in100"] = float(np.mean([t in top100 for t in topn]))
                rows.append(rec)
        return pd.DataFrame(rows)

    def churn(self, f, dates, top: int = 100) -> float:
        out = []
        for a, b in zip(dates[:-1], dates[1:]):
            ra, rb = f(a).rank(ascending=False), f(b).rank(ascending=False)
            t = ra[ra <= top].index
            out.append((ra.reindex(t) - rb.reindex(t)).abs().median())
        return float(np.nanmean(out))


def _spearman(a, b) -> float:
    x = pd.concat([pd.Series(a), pd.Series(b)], axis=1).dropna()
    if len(x) < 20:
        return float("nan")
    r = x.rank()
    return float(np.corrcoef(r.iloc[:, 0], r.iloc[:, 1])[0, 1])


def _nw_se(x, lags: int) -> float:
    x = np.asarray(x, float); x = x[~np.isnan(x)]
    n = len(x)
    if n < 3:
        return float("nan")
    e = x - x.mean()
    s = (e @ e) / n
    for lag in range(1, min(lags, n - 1) + 1):
        s += 2 * (1 - lag / (lags + 1)) * (e[lag:] @ e[:-lag]) / n
    return math.sqrt(max(s, 0) / n)


def summarize(df: pd.DataFrame) -> pd.DataFrame:
    out = []
    for name, g in df.groupby("variant", sort=False):
        r = {"variant": name}
        for h in HORIZONS:
            for k in (f"ic{h}", f"icM{h}", f"top{h}"):
                if k in g:
                    r[k] = g[k].mean()
        if "ic20" in g:
            r["ic20_t"] = r["ic20"] / _nw_se(g["ic20"], NW_LAGS[20])
        for k in ("filler_top50", "single_fund_top50", "nonUSD_top50", "ic_detf", "exit_top50", "retain50in100"):
            if k in g:
                r[k] = g[k].mean()
        out.append(r)
    return pd.DataFrame(out).set_index("variant")


# ═══════════════════════════════════════════════════════════════════════════
# variants (pure functions of the per-holding frame) — run on a PRE-v4 cache
# ═══════════════════════════════════════════════════════════════════════════
def _jaccard(hd):
    sets = {e: set(g["ticker"]) for e, g in hd.groupby("ETF_Ticker")}
    return {a: {b: len(sets[a] & sets[b]) / len(sets[a] | sets[b]) for b in sets} for a in sets}


def _overlap_sum(hd, col):
    J = _jaccard(hd)
    sub = hd[["ticker", "ETF_Ticker", col]].sort_values(["ticker", col], ascending=[True, False], kind="stable")
    out, cur, inc, acc = {}, None, [], 0.0
    for t, e, s in sub.itertuples(index=False, name=None):
        if t != cur:
            if cur is not None:
                out[cur] = acc
            cur, inc, acc = t, [], 0.0
        m = 1.0 if not inc else max(0.2, 1.0 - 1.5 * max(J[e][p] for p in inc))
        inc.append(e); acc += s * m
    if cur is not None:
        out[cur] = acc
    return pd.Series(out)


def make_variant(st: Study, *, synergy=0.15, kappa=1.0, intl_points=None, overlap=True,
                 multipliers=True, conv_once=False, kicker=False, apex_prod=False,
                 drawdown=None, n_tiers=5):
    """Pre-v4 apex final_score with switchable changes (defaults = pre-v4)."""
    intl60 = [e for e, p in st.pts.items() if p == 60]

    def f(d):
        hd = st.H_by[d].copy()
        if intl_points is not None:
            m = hd["ETF_Ticker"].isin(intl60)
            hd.loc[m, "tier_points"] = intl_points
        hd["s"] = hd["tier_points"] * hd["conviction"] ** kappa
        b = _overlap_sum(hd, "s") if overlap else hd.groupby("ticker")["s"].sum()
        b = b * (1 + (hd.groupby("ticker")["tier"].nunique().reindex(b.index) - 1) * synergy)
        tot = hd.groupby("ticker")["s"].sum()
        sh = hd["s"] / hd["ticker"].map(tot)
        neff = 1 / (sh ** 2).groupby(hd["ticker"]).sum()
        m_conc = (0.6 + 0.4 * ((neff - 1) / 2).clip(0, 1)).clip(0.6, 1)
        ts = hd.groupby(["ticker", "tier"])["s"].sum()
        p = (ts / ts.groupby(level=0).transform("sum"))
        p = p[p > 0]
        m_breadth = (1 + 0.35 * (-(p * np.log(p))).groupby(level=0).sum() / math.log(n_tiers)).reindex(b.index).fillna(1)
        fs = st.first[st.first["Holdings_As_Of"] <= d]
        pts = fs["ETF_Ticker"].map(st.pts if intl_points is None else st.pts.where(~st.pts.index.isin(intl60), intl_points))
        om = pts.groupby(fs["ticker"]).sum().reindex(b.index).fillna(0)
        pen = b / (om + 40.0)
        m_uni = (pen / pen[b > 0].median()).pow(0.5).clip(0.8, 1.25)
        out = b * m_conc * m_breadth * m_uni if multipliers else b
        ac = hd.groupby("ticker")["conviction"].mean().reindex(out.index)
        if conv_once:
            pw = ((hd["tier_points"] * hd["conviction"]).groupby(hd["ticker"]).sum()
                  / hd.groupby("ticker")["tier_points"].sum()).reindex(out.index)
            out = out * pw.clip(0.5, 1.25)
        if drawdown is not None:
            r = st.past_ret(d, 21).reindex(out.index)
            out = out.where(~(r < drawdown), out / m_breadth)
        if kicker or apex_prod:
            out = out * st.kicker(d).reindex(out.index).fillna(1.0)
        if apex_prod:
            g = ac.clip(0.4, 1.1)
            med = hd.groupby("ticker")["rank"].median().reindex(out.index)
            out = out * ac.clip(0.5, 1.25) * g.where(med > 10, g.clip(lower=1.0))
        return out
    return f


def run_variants(cache: Path) -> None:
    st = Study(cache)
    v0 = make_variant(st)
    d = pd.Timestamp(st.dates[-1])
    chk = pd.concat([v0(d).astype("int64").rename("v"), st.L_by[d]["final_score"]], axis=1).dropna()
    diff = int((chk["v"] - chk["final_score"]).abs().max())
    print(f"V0 re-implementation vs engine final_score on {d.date()}: max |diff| = {diff}")
    if diff != 0:
        print("  (expected 0 on a pre-v4 cache; on a v4 cache use the `engine` subcommand)")
    mom = lambda n, skip=0: (lambda dd: st.past_ret(dd, n, skip).reindex(st.H_by[dd]["ticker"].unique()))
    V = {
        "V0 pre-v4 final_score": v0,
        "V0a pre-v4 apex_score": make_variant(st, apex_prod=True),
        "V1 no tier synergy": make_variant(st, synergy=0),
        "V3 conviction once (pw)": make_variant(st, synergy=0, conv_once=True),
        "V5 + drawdown dampener": make_variant(st, synergy=0, conv_once=True, drawdown=-0.20),
        "V7 + APEX kicker": make_variant(st, synergy=0, conv_once=True, kicker=True),
        "K2 kappa=2": make_variant(st, synergy=0, kappa=2.0),
        "K2 + intl 40 pts (v4)": make_variant(st, synergy=0, kappa=2.0, intl_points=40),
        "X 12-1 momentum": mom(252, 21),
        "X 1m reversal": lambda dd: -st.past_ret(dd, 21).reindex(st.H_by[dd]["ticker"].unique()),
    }
    pd.set_option("display.width", 250); pd.set_option("display.max_columns", 40)
    cols = ["ic5", "ic20", "ic20_t", "icM20", "top20", "ic40", "filler_top50", "single_fund_top50",
            "nonUSD_top50", "ic_detf", "exit_top50", "retain50in100", "churn"]
    for label, (a, b) in (("30-ETF universe", P30), ("16-ETF universe", P16)):
        ds = st.weekly(a, b)
        sm = summarize(st.evaluate(V, ds))
        daily = [pd.Timestamp(x) for x in st.dates if ds[0] <= pd.Timestamp(x) <= ds[-1]][::2]
        sm["churn"] = [st.churn(V[k], daily) for k in sm.index]
        print(f"\n{label}: {len(ds)} weekly dates {ds[0].date()} → {ds[-1].date()}")
        print(sm[cols].round(4).to_string())


def run_engine(caches: list[Path]) -> None:
    pd.set_option("display.width", 250)
    res = {}
    for c in caches:
        st = Study(c)
        f = lambda d, st=st: st.L_by[d]["final_score"].astype(float)
        for label, (a, b) in (("30-ETF", P30), ("16-ETF", P16)):
            sm = summarize(st.evaluate({c.name: f}, st.weekly(a, b)))
            sm["hc_count"] = np.mean([(st.L_by[d]["flag"] == "HIGH_CONVICTION").sum() for d in st.weekly(a, b)])
            sm["conc_count"] = np.mean([(st.L_by[d]["flag"] == "CONCENTRATED").sum() for d in st.weekly(a, b)])
            res[(c.name, label)] = sm.iloc[0]
    print(pd.DataFrame(res).T.round(4).to_string())


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("cache"); c.add_argument("--config", type=Path, default=REPO / "config.yaml")
    c.add_argument("--out", type=Path, required=True)
    v = sub.add_parser("variants"); v.add_argument("--cache", type=Path, required=True)
    e = sub.add_parser("engine"); e.add_argument("--cache", type=Path, nargs="+", required=True)
    a = ap.parse_args(argv)
    if a.cmd == "cache":
        build_cache(a.config, a.out)
    elif a.cmd == "variants":
        run_variants(a.cache)
    else:
        run_engine(a.cache)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
