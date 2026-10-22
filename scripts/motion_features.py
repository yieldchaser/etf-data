"""Per (ticker, date) fund-level motion features over k snapshots (docs/SIGNALS_STUDY.md §3b).

    python scripts/motion_features.py <study cache> <out.parquet> 10


For each fund f holding ticker i at both d[t-k] and d[t] (book dates h0, h1):
  own_rank = log(rank_h0 / rank_h1)              (+ = moved up inside the fund's own book)
  wlog     = log(w_h1 / w_h0)                    (raw weight change)
  active   = wlog - [log(1+r_i) - log(1+r_f)]    (weight change NOT explained by price drift;
             r_f = fund return = Σ w_h0 · r over priced constituents)
Aggregated per ticker: holders, max/min active, # holders active ≥ +0.2 / ≤ -0.2,
median own_rank, share of holders whose own rank improved ≥ 20%.
"""
import sys
from pathlib import Path
import numpy as np, pandas as pd
sys.path.insert(0, str(Path(__file__).resolve().parent))
from scoring_study import Study

cache, out, k = Path(sys.argv[1]), Path(sys.argv[2]), int(sys.argv[3])
st = Study(cache)
H = st.H[["ETF_Ticker", "ticker", "weight", "rank", "Holdings_As_Of", "asof"]].copy()
H["Holdings_As_Of"] = pd.to_datetime(H["Holdings_As_Of"]); H["asof"] = pd.to_datetime(H["asof"])
dates = sorted(H["asof"].unique())
px = st.px
cal = px.index

LAG1 = {"CALF", "COWZ", "GRIN", "IVAL", "JHSC", "JOET"}      # weights published one trading day stale
STATIC = {"AVSC", "JHEM", "JHMM", "MFEM"}                    # weights never drift: any change is deliberate
def prices_at(day, lag):
    j = cal.searchsorted(day, side="right") - 1 - lag
    return px.iloc[j] if j >= 0 else None

pcache = {}
def P(day, lag=0):
    if (day, lag) not in pcache: pcache[(day, lag)] = prices_at(day, lag)
    return pcache[(day, lag)]

byd = {d: g for d, g in H.groupby("asof")}
rows = []
for t in range(k, len(dates)):
    d0, d1 = dates[t - k], dates[t]
    a, b = byd[d0], byd[d1]
    m = b.merge(a, on=["ETF_Ticker", "ticker"], suffixes=("", "_0"))
    m = m[m["Holdings_As_Of"] != m["Holdings_As_Of_0"]]            # fund's book actually updated
    if m.empty: continue
    out_parts = []
    for (f, h0, h1), g in m.groupby(["ETF_Ticker", "Holdings_As_Of_0", "Holdings_As_Of"]):
        lag = 1 if f in LAG1 else 0
        p0, p1 = P(h0, lag), P(h1, lag)
        if p0 is None or p1 is None: continue
        r = (p1.reindex(g["ticker"]).to_numpy() / p0.reindex(g["ticker"]).to_numpy()) - 1
        # fund return from its full h0 book
        book0 = a[a["ETF_Ticker"] == f]
        rb = (p1.reindex(book0["ticker"]).to_numpy() / p0.reindex(book0["ticker"]).to_numpy()) - 1
        ok = np.isfinite(rb)
        if ok.sum() < 5 or book0["weight"].to_numpy()[ok].sum() < 0.5: continue
        rf = np.nansum(book0["weight"].to_numpy()[ok] * rb[ok]) / book0["weight"].to_numpy()[ok].sum()
        gg = g.assign(r=r, rf=rf, static=f in STATIC)
        out_parts.append(gg)
    if not out_parts: continue
    m = pd.concat(out_parts)
    m = m[(m["weight"] > 0) & (m["weight_0"] > 0)]
    m["own_rank"] = np.log(m["rank_0"] / m["rank"])
    m["wlog"] = np.log(m["weight"] / m["weight_0"])
    m["active"] = np.where(m["static"], m["wlog"], m["wlog"] - (np.log1p(m["r"]) - np.log1p(m["rf"])))
    agg = m.groupby("ticker").agg(
        held_both=("ETF_Ticker", "nunique"),
        act_max=("active", "max"), act_min=("active", "min"), act_med=("active", "median"),
        act_up=("active", lambda s: int((s >= 0.2).sum())), act_dn=("active", lambda s: int((s <= -0.2).sum())),
        own_med=("own_rank", "median"),
        own_up=("own_rank", lambda s: float((s >= 0.2).mean())), own_dn=("own_rank", lambda s: float((s <= -0.2).mean())),
        wlog_med=("wlog", "median"))
    agg["asof"] = d1
    rows.append(agg.reset_index())
pd.concat(rows).to_parquet(out)
print("wrote", out)
