"""Full 30-ETF signal-trigger -> forward-performance study (LONG & SHORT).

- Universe: ALL 30 ETFs from config.
- History starts at the all-30-introduced date (2026-05-22) so the 30d lookback
  is valid; triggers are only counted from ~3 weeks after (past the 30d maturity
  gate) through late July, so each trigger has >=~3 weeks of forward price data.
- Trigger date = first date the condition holds (from the real rank panel).
  burst  : peak rank improvement >=40, coverage ok, sustained>=8 of last 10
  climb  : peak rank improvement >=80 (strong-velocity proxy), sustained>=6
  crater : peak rank drop     >=40, coverage ok, sustained>=8 of last 10 (short)
- Forward return measured from docs/data/details/<ticker>.json daily prices.
No repo changes.
"""
import sys, json, time
from pathlib import Path
sys.path.insert(0, str(Path.cwd()))
import pandas as pd, numpy as np
from predator import scoring as sc
from predator.build import fetch_history

ALL30 = pd.Timestamp("2026-05-22")          # all 30 ETFs introduced
HIST_START = pd.Timestamp("2026-05-16")      # a little before, for 30d lookback
TRIGGER_FROM = pd.Timestamp("2026-06-15")    # ~3wk after all-30 (past maturity gate)
TRIGGER_TO   = pd.Timestamp("2026-07-22")    # so each has >=~3wk forward price
DETAILS = Path("docs/data/details")
STRIDE = 1                                   # daily (faithful to production burst gate)

cfg = sc.Config.from_yaml(Path("config.yaml"))
etfs = list(cfg.etf_lookup().keys())
print(f"full universe: {len(etfs)} ETFs")

raw = fetch_history("data/all_history.csv")
raw["d"] = pd.to_datetime(raw["Holdings_As_Of"]).dt.normalize()
raw = raw[(raw["d"] >= HIST_START) & (raw["ETF_Ticker"].isin(etfs))].copy()
print(f"filtered rows {len(raw):,} · {raw['ETF_Ticker'].nunique()} ETFs · {raw['d'].min().date()}→{raw['d'].max().date()}")

alld = sorted(raw["d"].unique())
sampled = alld[::STRIDE]
print(f"snapshot dates: {len(alld)} total · sampling every {STRIDE} → {len(sampled)} leaderboard computations")

t0 = time.time()
historical = {}
for i, d in enumerate(sampled):
    lb, _ = sc.compute_leaderboard(raw, cfg, as_of=d)
    historical[d] = lb
    if (i+1) % 5 == 0:
        print(f"  {i+1}/{len(sampled)} leaderboards  [{time.time()-t0:.0f}s]", flush=True)
print(f"historical built: {len(historical)} [{time.time()-t0:.0f}s]")

panel = pd.DataFrame({d: historical[d].set_index("ticker")["leaderboard_rank"] for d in sampled}).sort_index(axis=1)
panel_dates = list(panel.columns)
print(f"rank panel: {panel.shape[0]} tickers x {panel.shape[1]} dates")

def per_date_states():
    burst_first, climb_first, crater_first = {}, {}, {}
    for j, t in enumerate(panel_dates):
        if t < TRIGGER_FROM:   # only record triggers in our window of interest
            pass
        wstart = t - pd.Timedelta(days=30)
        cols = [c for c in panel_dates if wstart <= c <= t]
        if len(cols) < 5:
            continue
        subp = panel[cols]
        worst = subp.max(axis=1); best = subp.min(axis=1); peak = (worst - best)
        obs = subp.notna().sum(axis=1); coverage = obs/len(cols)
        recent = subp.iloc[:, -10:] if subp.shape[1] >= 10 else subp
        med = subp.median(axis=1)
        sust_up = (recent.lt(med, axis=0)).sum(axis=1)
        sust_dn = (recent.gt(med, axis=0)).sum(axis=1)
        cov_ok = (coverage >= 0.80) | (obs < 15)
        burst  = (peak >= 40) & cov_ok & (sust_up  >= 8)
        crater = (peak >= 40) & cov_ok & (sust_dn  >= 8)
        climb  = (peak >= 80) & cov_ok & (sust_up  >= 6)
        for tkr, v in burst.items():
            if v and tkr not in burst_first:  burst_first[tkr]  = t
        for tkr, v in climb.items():
            if v and tkr not in climb_first:  climb_first[tkr]  = t
        for tkr, v in crater.items():
            if v and tkr not in crater_first: crater_first[tkr] = t
    # keep only triggers inside [TRIGGER_FROM, TRIGGER_TO]
    f = lambda d: TRIGGER_FROM <= d <= TRIGGER_TO
    return ({k:v for k,v in burst_first.items()  if f(v)},
            {k:v for k,v in climb_first.items()  if f(v)},
            {k:v for k,v in crater_first.items() if f(v)})

burst_first, climb_first, crater_first = per_date_states()
print(f"RECENT triggers [{TRIGGER_FROM.date()}..{TRIGGER_TO.date()}]: burst={len(burst_first)} climb={len(climb_first)} crater={len(crater_first)}")

def load_prices(tkr):
    p = DETAILS / f"{tkr}.json"
    if not p.exists(): return None
    try: return json.load(open(p)).get("prices")
    except Exception: return None

def fwd_return(tkr, trig):
    pr = load_prices(tkr)
    if not pr: return None
    idx = None
    ts = trig.strftime("%Y-%m-%d")
    for i,(dt,_) in enumerate(pr):
        if dt >= ts: idx = i; break
    if idx is None: return None
    p0 = pr[idx][1]
    if not p0: return None
    out = []
    for h in (0, 20, 40):
        j = idx + h
        if j < len(pr) and pr[j][1]:
            out.append((pr[j][0], pr[j][1]/p0 - 1))
        else:
            out.append((None, None))
    return out

def analyze(name, trig, expect_up):
    print(f"\n{'='*72}\n{name}: {len(trig)} triggers  (expect {'UP' if expect_up else 'DOWN'} after signal)\n{'='*72}")
    rows = []
    for tkr, d in trig.items():
        lb = historical[d]; s = lb.set_index("ticker")
        company = s.loc[tkr,"company"] if tkr in s.index else ""
        rank = int(s.loc[tkr,"leaderboard_rank"]) if tkr in s.index else None
        fr = fwd_return(tkr, d)
        if not fr: continue
        rows.append((tkr, company, d.strftime("%Y-%m-%d"), rank, fr[0][1], fr[1][1], fr[2][1]))
    if not rows:
        print("  (no price data)"); return
    df = pd.DataFrame(rows, columns=["ticker","company","trig","rank@trig","ret_now","ret20","ret40"])
    df["ok"] = (df["ret_now"] > 0) if expect_up else (df["ret_now"] < 0)
    print(f"  direction-correct: {df['ok'].sum()}/{len(df)} = {100*df['ok'].mean():.0f}%")
    print(f"  median ret_now {df['ret_now'].median()*100:+.1f}% | ret20 {df['ret20'].median()*100:+.1f}% | ret40 {df['ret40'].median()*100:+.1f}%")
    fmt = {"ret_now":lambda x:f"{x*100:+.1f}%","ret20":lambda x:(f"{x*100:+.1f}%" if x is not None else "-"),
           "ret40":lambda x:(f"{x*100:+.1f}%" if x is not None else "-")}
    print("\n  --- TOP 15 by forward return (genius cases) ---")
    top = df.reindex(df["ret_now"].abs().sort_values(ascending=False).index).head(15)
    print(top.to_string(index=False, formatters=fmt))

analyze("LONG — BURST triggers", burst_first, True)
analyze("LONG — STRONG CLIMB (velocity proxy)", climb_first, True)
analyze("SHORT — CRATER triggers", crater_first, False)
print("\nDONE")
