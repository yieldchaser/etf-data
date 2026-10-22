"""
BURST / CRATER / SURGE / SLIDE motion study — reproduces docs/SIGNALS_STUDY.md §3b.

    python scripts/scoring_study.py cache --config config.yaml --out /tmp/study/v4g
    python scripts/motion_features.py /tmp/study/v4g /tmp/study/feat_k10.parquet 10
    python scripts/motion_study.py /tmp/study/v4g /tmp/study/feat_k10.parquet all|rules|regime|confirmed

rules     : 20 rule families (rank, own-book rank, active weight net of price,
            price momentum, fund-membership confirmation), onset events, 10/20d
            forward return vs currency peers, coverage, by regime.
regime    : can a momentum BURST be timed with its own trailing realized record?
confirmed : wider / looser variants that keep fund-membership confirmation.
"""
import sys
MODE = sys.argv[3] if len(sys.argv) > 3 else "all"
sys.argv = ["x", sys.argv[1], sys.argv[2]]
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent))
import numpy as np, pandas as pd
import signals_study as ss
from signals_study import st, DATES, R, E, RS, REGIMES, fwd, onsets
F = pd.read_parquet(sys.argv[2])
FB = {pd.Timestamp(d): g.set_index("ticker") for d, g in F.groupby("asof")}
DI = {d: i for i, d in enumerate(DATES)}
TOP = 300

def past(d, n=10):
    r = st.past_ret(d, n)
    return st.demean(r.reindex(st.L_by[d].index)) if len(r) else None
PASTC = {}
def pastc(d):
    if d not in PASTC: PASTC[d] = past(d)
    return PASTC[d]

def base(i, k):
    d, p = DATES[i], DATES[i - k]
    return d, RS[d], RS[p]

def feat(d, col, default=0.0):
    f = FB.get(d)
    return f[col] if f is not None else pd.Series(dtype=float)

def S(mask):
    return set(mask.index[mask.fillna(False).astype(bool)])

def burst(i, kind):
    if i < 20: return set()
    d, now, then = base(i, 10)
    lr = np.log(then / now)
    rel = now <= TOP
    best = RS[DATES[i - 10:i + 1]].min(axis=1); inforce = np.log(now / best) <= 0.15
    add = (E[d] - E[DATES[i - 10]]).reindex(now.index).fillna(0) >= 1
    au = feat(d, "act_up").reindex(now.index).fillna(0)
    am = feat(d, "act_max").reindex(now.index)
    own = feat(d, "own_up").reindex(now.index).fillna(0); hb = feat(d, "held_both").reindex(now.index).fillna(0)
    lean = (au >= 1)
    if kind == "B0 shipped: x1.65 + fund add": return S((lr >= .5) & rel & inforce & add)
    if kind == "B1 x1.65 rank only":           return S((lr >= .5) & rel & inforce)
    if kind == "B1b x1.35 rank only":          return S((lr >= .3) & rel & inforce)
    if kind == "B2 1+ fund leans in":          return S(lean & rel)
    if kind == "B3 2+ funds lean in":          return S((au >= 2) & rel)
    if kind == "B4 x1.35 + (add or lean)":     return S((lr >= .3) & rel & inforce & (add | lean))
    if kind == "B5 x1.65 + (add or lean)":     return S((lr >= .5) & rel & inforce & (add | lean))
    if kind == "B6 own-rank up in 2/3 holders": return S((hb >= 2) & (own >= 2/3) & rel)
    if kind == "B7 price top-5% (top300)":
        pr = pastc(d)
        if pr is None: return set()
        pr = pr.reindex(now.index)[rel]; return set(pr[pr >= pr.quantile(.95)].index)
    if kind == "B8 x1.35 + lean (no add)":     return S((lr >= .3) & rel & inforce & lean)
    raise KeyError(kind)

def crater(i, kind):
    if i < 20: return set()
    d, now, then = base(i, 10)
    lr = np.log(now / then)
    rel = then <= TOP
    worst = RS[DATES[i - 10:i + 1]].max(axis=1); inforce = np.log(worst / now) <= 0.15
    d20, now20, then20 = base(i, 20)
    lr20 = np.log(now20 / then20); rel20 = then20 <= TOP
    worst20 = RS[DATES[i - 20:i + 1]].max(axis=1); inforce20 = np.log(worst20 / now20) <= 0.15
    ex20 = (E[d] - E[DATES[i - 20]]).reindex(now.index).fillna(0) <= -1
    ad = feat(d, "act_dn").reindex(now.index).fillna(0)
    own = feat(d, "own_dn").reindex(now.index).fillna(0); hb = feat(d, "held_both").reindex(now.index).fillna(0)
    cut = ad >= 1
    if kind == "C0 shipped: k20 x1.65 + exit": return S((lr20 >= .5) & rel20 & inforce20 & ex20)
    if kind == "C1 x1.65 rank only":          return S((lr >= .5) & rel & inforce)
    if kind == "C2 1+ fund cuts":             return S(cut & rel)
    if kind == "C3 2+ funds cut":             return S((ad >= 2) & rel)
    if kind == "C4 x1.35 + (exit20 or cut)":  return S((lr >= .3) & rel & inforce & (ex20 | cut))
    if kind == "C5 x1.65 + (exit20 or cut)":  return S((lr >= .5) & rel & inforce & (ex20 | cut))
    if kind == "C6 own-rank down in 2/3":     return S((hb >= 2) & (own >= 2/3) & rel)
    if kind == "C7 price bottom-5% (top300)":
        pr = pastc(d)
        if pr is None: return set()
        pr = pr.reindex(now.index)[rel]; return set(pr[pr <= pr.quantile(.05)].index)
    if kind == "C8 x1.35 + cut (no exit)":    return S((lr >= .3) & rel & inforce & cut)
    if kind == "C9 k20 x1.65 + (exit20 or cut)": return S((lr20 >= .5) & rel20 & inforce20 & (ex20 | cut))
    raise KeyError(kind)

def evaluate(fn, a, b):
    ev, _ = onsets(fn, a, b)
    idx = [i for i, d in enumerate(DATES) if pd.Timestamp(a) <= d <= pd.Timestamp(b)]
    counts = pd.Series([len(fn(i)) for i in idx])
    rows = []
    for d, t in ev:
        f20, f10, pr = fwd(d, 20), fwd(d, 10), pastc(d)
        if f20 is None: continue
        rows.append({"f20": f20.get(t), "f10": f10.get(t) if f10 is not None else np.nan,
                     "p10": pr.get(t) if pr is not None else np.nan})
    x = pd.DataFrame(rows)
    f = x["f20"].dropna()
    return {"flags/day": counts.mean(), "zero days": (counts == 0).mean(), "events": len(f),
            "past10": x["p10"].mean(), "next10": x["f10"].mean(), "next20": f.mean(),
            "t20": f.mean() / (f.std(ddof=1) / np.sqrt(len(f))) if len(f) > 2 else np.nan, "hit20": (f > 0).mean()}



def study_rules():
    B = ["B0 shipped: x1.65 + fund add", "B1 x1.65 rank only", "B1b x1.35 rank only", "B2 1+ fund leans in", "B3 2+ funds lean in",
         "B4 x1.35 + (add or lean)", "B5 x1.65 + (add or lean)", "B6 own-rank up in 2/3 holders", "B7 price top-5% (top300)", "B8 x1.35 + lean (no add)"]
    C = ["C0 shipped: k20 x1.65 + exit", "C1 x1.65 rank only", "C2 1+ fund cuts", "C3 2+ funds cut", "C4 x1.35 + (exit20 or cut)",
         "C5 x1.65 + (exit20 or cut)", "C6 own-rank down in 2/3", "C7 price bottom-5% (top300)", "C8 x1.35 + cut (no exit)", "C9 k20 x1.65 + (exit20 or cut)"]
    pd.set_option("display.width", 250)
    which = "both"
    for title, names, fn in [("BURST", B, burst), ("CRATER", C, crater)]:
        if which not in ("both", title): continue
        for lab, (a, b) in REGIMES.items():
            out = {n: evaluate(lambda i, n=n: fn(i, n), a, b) for n in names}
            print(f"\n=== {title} — {lab}")
            print(pd.DataFrame(out).T.round(3).to_string(), flush=True)

def study_regime():
    import numpy as np, pandas as pd


    def realized(rule, a="2026-02-20", b="2026-10-21"):
        ev, _ = onsets(lambda i: burst(i, rule), a, b)
        rows = []
        for d, t in ev:
            f20 = fwd(d, 20)
            if f20 is None: continue
            rows.append((d, t, f20.get(t)))
        return pd.DataFrame(rows, columns=["d", "t", "f20"]).dropna()

    pd.set_option("display.width", 200)
    for rule in ["B1 x1.35 rank only".replace("x1.35", "x1.65"), "B1b x1.35 rank only", "B7 price top-5% (top300)", "B6 own-rank up in 2/3 holders", "B0 shipped: x1.65 + fund add"]:
        ev = realized(rule)
        out = []
        for lo, hi in [(30, 60), (30, 90)]:
            ind = []
            for d in ev["d"]:
                w = ev[(ev["d"] >= d - pd.Timedelta(days=hi)) & (ev["d"] <= d - pd.Timedelta(days=lo))]
                ind.append(w["f20"].mean() if len(w) >= 8 else np.nan)
            e = ev.assign(ind=ind).dropna(subset=["ind"])
            on, off = e[e["ind"] > 0]["f20"], e[e["ind"] <= 0]["f20"]
            t = lambda x: x.mean() / (x.std(ddof=1) / np.sqrt(len(x))) if len(x) > 2 else np.nan
            out.append({"window": f"{lo}-{hi}d back", "events": len(e), "all": e["f20"].mean(),
                        "when trailing>0: n": len(on), "mean": on.mean(), "t": t(on),
                        "when trailing<=0: n": len(off), "mean ": off.mean(), "t ": t(off),
                        "corr(ind,f20)": e[["ind", "f20"]].corr().iloc[0, 1]})
        print(f"\n=== {rule}")
        print(pd.DataFrame(out).round(3).to_string(index=False), flush=True)

def study_confirmed():
    import numpy as np, pandas as pd


    def b2(i, kind):
        if i < 20: return set()
        d, now, then = base(i, 10)
        lr = np.log(then / now); rel = now <= TOP
        best = RS[DATES[i - 10:i + 1]].min(axis=1); inforce = np.log(now / best) <= 0.15
        add10 = (E[d] - E[DATES[i - 10]]).reindex(now.index).fillna(0) >= 1
        if kind == "B0 shipped x1.65 +add10":       return S((lr >= .5) & rel & inforce & add10)
        if kind == "B9 x1.35 +add10":               return S((lr >= .3) & rel & inforce & add10)
        if kind == "B10 x1.2 +add10":               return S((lr >= np.log(1.2)) & rel & inforce & add10)
        if kind == "B11 any rise +add10":           return S((lr > 0) & rel & add10)
        if kind == "B12 x1.35 +add10, top500":      return S((lr >= .3) & (now <= 500) & inforce & add10)
        raise KeyError(kind)

    def c2(i, kind):
        if i < 20: return set()
        d, now, then = base(i, 20)
        lr = np.log(now / then); rel = then <= TOP
        worst = RS[DATES[i - 20:i + 1]].max(axis=1); inforce = np.log(worst / now) <= 0.15
        ex20 = (E[d] - E[DATES[i - 20]]).reindex(now.index).fillna(0) <= -1
        if kind == "C0 shipped k20 x1.65 +exit20": return S((lr >= .5) & rel & inforce & ex20)
        if kind == "C10 k20 x1.35 +exit20":        return S((lr >= .3) & rel & inforce & ex20)
        if kind == "C11 k20 x1.2 +exit20":         return S((lr >= np.log(1.2)) & rel & inforce & ex20)
        if kind == "C12 k20 any fall +exit20":     return S((lr > 0) & rel & ex20)
        if kind == "C13 k20 x1.35 +exit20, from top500": return S((lr >= .3) & (then <= 500) & inforce & ex20)
        raise KeyError(kind)

    pd.set_option("display.width", 250)
    for title, names, fn in [("BURST", ["B0 shipped x1.65 +add10", "B9 x1.35 +add10", "B10 x1.2 +add10", "B11 any rise +add10", "B12 x1.35 +add10, top500"], b2),
                             ("CRATER", ["C0 shipped k20 x1.65 +exit20", "C10 k20 x1.35 +exit20", "C11 k20 x1.2 +exit20", "C12 k20 any fall +exit20", "C13 k20 x1.35 +exit20, from top500"], c2)]:
        for lab, (a, b) in REGIMES.items():
            out = {n: evaluate(lambda i, n=n: fn(i, n), a, b) for n in names}
            print(f"\n=== {title} — {lab}")
            print(pd.DataFrame(out).T.round(3).to_string(), flush=True)


if __name__ == "__main__":
    if MODE in ("all", "rules"): study_rules()
    if MODE in ("all", "regime"): study_regime()
    if MODE in ("all", "confirmed"): study_confirmed()
