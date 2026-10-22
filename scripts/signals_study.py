"""
HIGH_CONVICTION + BURST/CRATER redesign study — reproduces docs/SIGNALS_STUDY.md.

Read-only. Needs a replay cache from scripts/scoring_study.py (with the
partial-snapshot guard on, i.e. the current config):

    python scripts/scoring_study.py cache --config config.yaml --out /tmp/study/v4g
    python scripts/signals_study.py /tmp/study/v4g          # both studies
    python scripts/signals_study.py /tmp/study/v4g hc       # HIGH_CONVICTION only
    python scripts/signals_study.py /tmp/study/v4g bc       # BURST / CRATER only

Candidate rules are re-implemented here as pure functions of the replayed
per-date leaderboards so variants can be compared; the shipped rule lives in
conviction/history.burst_crater_panels (BURST = "smoothed k10 tau.5 floor300
+funds", CRATER = "smoothed k20 tau.5 floor300 +funds" below) and conviction/scoring.py (HIGH_CONVICTION).
"""
import sys, warnings
from pathlib import Path
warnings.filterwarnings("ignore")
sys.path.insert(0, str(Path(__file__).resolve().parent))
import numpy as np, pandas as pd
from scoring_study import Study, _nw_se

pd.set_option("display.width", 250); pd.set_option("display.max_columns", 30)
if len(sys.argv) < 2:
    sys.exit(__doc__)
st = Study(Path(sys.argv[1]))
DATES = [pd.Timestamp(d) for d in st.dates]
R = st.L.pivot_table(index="ticker", columns="asof", values="leaderboard_rank")
SC = st.L.pivot_table(index="ticker", columns="asof", values="final_score")
E = st.L.pivot_table(index="ticker", columns="asof", values="etf_count").fillna(0)
N = st.L.groupby("asof").size()
REGIMES = {"30-ETF": ("2026-06-05", "2026-10-21"), "16-ETF": ("2026-03-05", "2026-05-15")}

def fwd(d, h=20):
    r = st.fwd_ret(d, h)
    return st.demean(r.reindex(st.L_by[d].index)) if len(r) else None

def after(d, days):
    t = d + pd.Timedelta(days=days)
    c = [x for x in DATES if x >= t]
    return c[0] if c else None

# ═══ HIGH_CONVICTION candidates ═══════════════════════════════════════════════
def conv_holders(d, theta):
    h = st.H_by[d]
    return h[h["conviction"] >= theta].groupby("ticker")["ETF_Ticker"].nunique()

def hc_sets(d):
    """Candidate HIGH_CONVICTION sets. 'nonCore' variants exclude the Core
    (cap-weighted benchmark) tier from conviction holders and tier counts —
    the shipped rule is "N >=4 funds & >=2 nonCore C>=0.6 & >=2 nonCore tiers"."""
    lb = st.L_by[d]; h = st.H_by[d]; nc = h[h.tier != "Core"]
    ec = lb["etf_count"]; tiers = lb["tiers"].str.count(r"\+") + 1
    tiers_nc = nc.groupby("ticker")["tier"].nunique().reindex(lb.index).fillna(0)
    out = {"H0 >=4 funds (current)": set(lb.index[ec >= 4]),
           "H1 >=5 funds": set(lb.index[ec >= 5]),
           "H1 >=6 funds": set(lb.index[ec >= 6])}
    for th in (0.5, 0.6, 0.7):
        ch = conv_holders(d, th).reindex(lb.index).fillna(0)
        for k in (2, 3):
            out[f"H2 >={k} funds C>={th}"] = set(lb.index[ch >= k])
        out[f"H4 >=4 funds & >=2 C>={th} & >=2 tiers"] = set(lb.index[(ec >= 4) & (ch >= 2) & (tiers >= 2)])
        out[f"H5 >=4 funds & >=3 C>={th}"] = set(lb.index[(ec >= 4) & (ch >= 3)])
        chn = nc[nc.conviction >= th].groupby("ticker")["ETF_Ticker"].nunique().reindex(lb.index).fillna(0)
        out[f"N >=4 funds & >=2 nonCore C>={th} & >=2 nonCore tiers"] = set(lb.index[(ec >= 4) & (chn >= 2) & (tiers_nc >= 2)])
    top = lb["final_score"].sort_values(ascending=False)
    out["H3 top-50 score & >=4 funds"] = set([t for t in top.index if ec[t] >= 4][:50])
    return out

def hc_study():
    for lab, (a, b) in REGIMES.items():
        rows = []
        for d in st.weekly(a, b):
            fr = fwd(d); d30 = after(d, 28)
            if fr is None or d30 is None: continue
            sets = hc_sets(d); later = hc_sets(d30)
            lb, lb30 = st.L_by[d], st.L_by[d30]
            base = fr.reindex(lb.index[lb["etf_count"] >= 2]).mean()
            for k, s in sets.items():
                s = list(s)
                lost = (lb30["etf_count"].reindex(s).fillna(0) < lb["etf_count"].reindex(s)).mean() if s else np.nan
                rows.append({"def": k, "n": len(s), "fwd20": fr.reindex(s).mean(), "vs_multi": fr.reindex(s).mean() - base,
                             "lost_fund_30d": lost, "still_hc_28d": np.mean([t in later[k] for t in s]) if s else np.nan})
        df = pd.DataFrame(rows).groupby("def", sort=False).mean()
        print(f"\n=== HIGH_CONVICTION candidates — {lab}"); print(df.round(4).to_string())

# ═══ BURST / CRATER candidates ═══════════════════════════════════════════════
def smooth(P, w=3):
    return P.T.rolling(w, min_periods=w).median().T

RS = smooth(R)

def cur_flags(i, burst=True):
    """Current production rule replayed at date index i (30-day window)."""
    d = DATES[i]; w = [x for x in DATES if d - pd.Timedelta(days=30) <= x <= d]
    if len(w) < 5: return set()
    P = R[w]
    cov = P.notna().sum(axis=1)
    rng = P.max(axis=1) - P.min(axis=1)
    rec = P.iloc[:, -10:]; med = P.median(axis=1)
    sus = (rec.lt(med, axis=0) if burst else rec.gt(med, axis=0)).sum(axis=1)
    ok = (cov / len(w) >= 0.8) | (cov < 15)
    return set(P.index[(rng >= 40) & ok & (sus >= 8)])

def new_flags(i, burst=True, k=10, tau=0.5, floor=300, funds=False, persist=True, kf=None):
    """Smoothed, directional, rank-relative rule.
    BURST : log(R̃[t-k] / R̃[t]) ≥ τ, R̃[t] ≤ floor (relevant now), move still
            in force (R̃[t] at/near its window best), optional fund confirmation.
    CRATER: mirror — log(R̃[t] / R̃[t-k]) ≥ τ, R̃[t-k] ≤ floor (it WAS relevant)."""
    if i - max(k, kf or 0) < 0: return set()
    d, p = DATES[i], DATES[i - k]
    now, then = RS[d], RS[p]
    if burst:
        lr = np.log(then / now); rel = now <= floor
        m = (lr >= tau) & rel
        if persist:   # not a spent move: within 15% (log) of the best smoothed rank in the window
            best = RS[DATES[i - k:i + 1]].min(axis=1)
            m &= np.log(now / best) <= 0.15
        if funds and kf:      # held more funds than its low in the last kf snapshots
            m &= (E[d] - E[DATES[i - kf:i + 1]].min(axis=1)).reindex(m.index).fillna(0) >= 1
        elif funds:
            m &= (E[d] - E[p]).reindex(m.index).fillna(0) >= 1
    else:
        lr = np.log(now / then); rel = then <= floor
        m = (lr >= tau) & rel
        if persist:
            worst = RS[DATES[i - k:i + 1]].max(axis=1)
            m &= np.log(worst / now) <= 0.15
        if funds and kf:      # fewer funds than its high in the last kf snapshots 
            m &= (E[d] - E[DATES[i - kf:i + 1]].max(axis=1)).reindex(m.index).fillna(0) <= -1
        elif funds:
            m &= (E[d] - E[p]).reindex(m.index).fillna(0) <= -1
    return set(m.index[m.fillna(False)])

def onsets(flagger, a, b, gap=20):
    """First-day events: ticker flagged at t but not at t-1; no re-onset within `gap` snapshots."""
    idx = [i for i, d in enumerate(DATES) if pd.Timestamp(a) <= d <= pd.Timestamp(b)]
    prev, last = set(), {}
    ev, counts = [], []
    for i in idx:
        cur = flagger(i); counts.append(len(cur))
        for t in cur - prev:
            if i - last.get(t, -999) > gap:
                ev.append((DATES[i], t)); last[t] = i
        prev = cur
    return ev, np.mean(counts)

def eval_events(ev, burst=True):
    rows = []
    for d, t in ev:
        fr = fwd(d, 20); f10 = fwd(d, 10)
        d14, d30 = after(d, 14), after(d, 30)
        if fr is None or d30 is None: continue
        e0 = E[d].get(t, 0)
        hc14 = any(E[x].get(t, 0) >= 4 for x in DATES if d < x <= d14) if e0 < 4 else np.nan
        rows.append({"fwd20": fr.get(t), "fwd10": f10.get(t) if f10 is not None else np.nan,
                     "gain_fund_30d": E[d30].get(t, 0) > e0, "lose_fund_30d": E[d30].get(t, 0) < e0,
                     "enter_hc14": hc14, "rank_now": R[d].get(t)})
    df = pd.DataFrame(rows)
    if df.empty: return {}
    x = df["fwd20"].dropna()
    return {"events": len(df), "fwd10": df["fwd10"].mean(), "fwd20": x.mean(),
            "t20": x.mean() / (x.std(ddof=1) / np.sqrt(len(x))) if len(x) > 2 else np.nan,
            "hit20": (x > 0).mean(), "gain_fund": df["gain_fund_30d"].mean(), "lose_fund": df["lose_fund_30d"].mean(),
            "enter_hc14": df["enter_hc14"].mean(), "med_rank": df["rank_now"].median()}

def bc_study():
    variants = {
        "current (range>=40, 30d)": dict(kind="cur"),
        "smoothed k10 tau.5 floor300": dict(k=10, tau=0.5, floor=300),
        "smoothed k10 tau.7 floor300": dict(k=10, tau=0.7, floor=300),
        "smoothed k20 tau.7 floor300": dict(k=20, tau=0.7, floor=300),
        "smoothed k10 tau.5 floor150": dict(k=10, tau=0.5, floor=150),
        "smoothed k10 tau.5 floor300 +funds": dict(k=10, tau=0.5, floor=300, funds=True),
        "smoothed k20 tau.5 floor300 +funds": dict(k=20, tau=0.5, floor=300, funds=True),
        "smoothed k10 tau.5 floor300 +funds within 20": dict(k=10, tau=0.5, floor=300, funds=True, kf=20),
        "smoothed k10 tau.5 floor300 +funds within 30": dict(k=10, tau=0.5, floor=300, funds=True, kf=30),
        "no-smoothing k10 tau.5 floor300": dict(k=10, tau=0.5, floor=300, raw=True),
    }
    global RS
    smoothed = RS
    for burst in (True, False):
        for lab, (a, b) in REGIMES.items():
            out = {}
            for name, v in variants.items():
                RS = R if v.get("raw") else smoothed
                if v.get("kind") == "cur":
                    f = lambda i, burst=burst: cur_flags(i, burst)
                else:
                    kw = {k: v[k] for k in ("k", "tau", "floor", "funds", "kf") if k in v}
                    f = lambda i, burst=burst, kw=kw: new_flags(i, burst, **kw)
                ev, avg_n = onsets(f, a, b)
                r = eval_events(ev, burst); r["flagged/day"] = avg_n
                out[name] = r
            RS = smoothed
            print(f"\n=== {'BURST' if burst else 'CRATER'} — {lab} (onset events, 20d currency-demeaned fwd return)")
            print(pd.DataFrame(out).T.round(4).to_string())

if __name__ == "__main__":
    which = sys.argv[2] if len(sys.argv) > 2 else "all"
    if which in ("all", "hc"): hc_study()
    if which in ("all", "bc"): bc_study()
