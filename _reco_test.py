"""Walk-forward recommendation test (NO look-ahead).

At each historical recommendation date T, using ONLY holdings data <= T:
  1. build historical leaderboards (dates <= T)
  2. compute the verbatim _attach_velocity signal state at T
  3. RECOMMEND the top-N highest-conviction LONG names = burst_30d==True
     ranked by velocity_score (signal only, no price/outcome used)
Then measure ACTUAL forward return from T using details/ prices.
Repo unchanged.
"""
import sys, json, time
from pathlib import Path
sys.path.insert(0, str(Path.cwd()))
import pandas as pd, numpy as np
from predator import scoring as sc
from predator import history as hist
from predator.build import fetch_history

DETAILS = Path("docs/data/details")
RECO_DATES = [pd.Timestamp("2026-06-15"), pd.Timestamp("2026-07-01")]
TOP_N = 5

cfg = sc.Config.from_yaml(Path("config.yaml"))
etfs = list(cfg.etf_lookup().keys())
raw_all = fetch_history("data/all_history.csv")
raw_all["d"] = pd.to_datetime(raw_all["Holdings_As_Of"]).dt.normalize()

# ---- verbatim _attach_velocity (build.py:308-416), operates at a single date T ----
def attach_velocity(leaderboard, deltas_by_period, historical):
    d7 = deltas_by_period.get(7); d30 = deltas_by_period.get(30)
    rank_avg_7 = d7.groupby("ticker")["rank_delta"].mean() if d7 is not None and not d7.empty else pd.Series(dtype=float)
    flow_avg_7 = d7.groupby("ticker")["weight_flow"].mean() if d7 is not None and not d7.empty else pd.Series(dtype=float)
    rank_avg_30 = d30.groupby("ticker")["rank_delta"].mean() if d30 is not None and not d30.empty else pd.Series(dtype=float)
    dates_sorted = sorted(historical.keys()); today_date = dates_sorted[-1]
    window_start = today_date - pd.Timedelta(days=30)
    window_cols = [c for c in dates_sorted if c >= window_start]
    gdelta = pd.Series(dtype=float); peak = pd.Series(dtype=float); best = pd.Series(dtype=float); is_burst = pd.Series(dtype=bool)
    if len(window_cols) >= 5:
        rows = {d: historical[d].set_index("ticker")["leaderboard_rank"] for d in window_cols if "leaderboard_rank" in historical[d].columns}
        if rows:
            panel = pd.DataFrame(rows)
            cov = (len(window_cols) - panel.isna().sum(axis=1)) / len(window_cols)
            first_col = panel.iloc[:, 0]; last_col = panel.iloc[:, -1]
            worst = panel.max(axis=1); best = panel.min(axis=1)
            gdelta = (first_col - last_col).round(0)
            peak = (worst - best).round(0)
            recent10 = panel.iloc[:, -10:] if panel.shape[1] >= 10 else panel
            med = panel.median(axis=1)
            sust_up = (recent10.lt(med, axis=0)).sum(axis=1)
            obs = panel.notna().sum(axis=1)
            cov_ok = (cov >= 0.80) | (obs < 15)
            is_burst = (peak >= 40) & cov_ok & (sust_up >= 8)
    past = pd.Series(dtype=float)
    if len(dates_sorted) >= 2:
        td = today_date - pd.Timedelta(days=30)
        pd_ = min(dates_sorted, key=lambda d: abs((d - td).total_seconds()))
        if "etf_count" in historical[pd_].columns:
            past = historical[pd_].set_index("ticker")["etf_count"]
    leaderboard["avg_rank_delta_7d"] = leaderboard["ticker"].map(rank_avg_7).fillna(0).round(2)
    leaderboard["avg_weight_flow_7d"] = leaderboard["ticker"].map(flow_avg_7).fillna(0).round(4)
    leaderboard["avg_rank_delta_30d"] = leaderboard["ticker"].map(rank_avg_30).fillna(0).round(2)
    leaderboard["global_rank_delta_30d"] = leaderboard["ticker"].map(gdelta).fillna(0).astype(int)
    leaderboard["global_rank_peak_30d"] = leaderboard["ticker"].map(peak).fillna(0).astype(int)
    leaderboard["global_rank_best_30d"] = leaderboard["ticker"].map(best).fillna(leaderboard["leaderboard_rank"]).astype(int)
    leaderboard["etf_count_30d_ago"] = leaderboard["ticker"].map(past).fillna(leaderboard["etf_count"]).astype(int)
    leaderboard["etf_count_delta_30d"] = (leaderboard["etf_count"] - leaderboard["etf_count_30d_ago"]).astype(int)
    leaderboard["burst_30d"] = leaderboard["ticker"].map(is_burst).fillna(False)
    leaderboard["velocity_score"] = (
        leaderboard["global_rank_delta_30d"].fillna(0).clip(-200, 200) * 0.5 +
        leaderboard["global_rank_peak_30d"].fillna(0).clip(0, 200) * 0.25 +
        leaderboard["avg_rank_delta_7d"].fillna(0) * 1.0 +
        leaderboard["avg_weight_flow_7d"].fillna(0) * 20.0 +
        leaderboard["etf_count_delta_30d"].fillna(0) * 5.0 +
        leaderboard["score_streak"].fillna(0).clip(-10, 10) * 1.0).round(2)
    return leaderboard

def load_prices(tkr):
    p = DETAILS / f"{tkr}.json"
    if not p.exists(): return None
    try: return json.load(open(p)).get("prices")
    except Exception: return None

def fwd(tkr, T):
    pr = load_prices(tkr)
    if not pr: return None
    idx = None; ts = T.strftime("%Y-%m-%d")
    for i,(dt,_) in enumerate(pr):
        if dt >= ts: idx = i; break
    if idx is None: return None
    p0 = pr[idx][1]
    if not p0: return None
    last = len(pr)-1
    r_now = pr[last][1]/p0 - 1
    r20 = pr[idx+20][1]/p0 - 1 if idx+20 < len(pr) and pr[idx+20][1] else None
    r40 = pr[idx+40][1]/p0 - 1 if idx+40 < len(pr) and pr[idx+40][1] else None
    return r_now, r20, r40

for T in RECO_DATES:
    print(f"\n{'#'*78}\n# RECOMMENDATION DATE (as-if): {T.date()}  — selection uses ONLY data <= {T.date()}\n{'#'*78}")
    lo = T - pd.Timedelta(days=60)
    raw = raw_all[(raw_all["d"] >= lo) & (raw_all["d"] <= T) & (raw_all["ETF_Ticker"].isin(etfs))].copy()
    dates = sorted(raw["d"].unique())
    historical = {d: lb for d, (lb, _) in ((d, sc.compute_leaderboard(raw, cfg, as_of=d)) for d in dates)}
    deltas = {7: sc.compute_rank_deltas(raw, cfg, lookback_days=7),
              30: sc.compute_rank_deltas(raw, cfg, lookback_days=30)}
    lb = historical[T].copy()
    # merge score_streak (signal input, derived from holdings history <= T — NOT price)
    sp = hist.score_panel(historical); fp = hist.flag_panel(historical)
    st = hist.streaks_and_deltas(sp, fp)
    if not st.empty:
        lb = lb.merge(st[["ticker", "score_streak"]], on="ticker", how="left")
    else:
        lb["score_streak"] = None
    lb = attach_velocity(lb, deltas, historical)
    burst = lb[lb["burst_30d"] == True].copy()
    # highest conviction long = top velocity_score among burst (signal only)
    burst["is_hc"] = burst["flag"] == "HIGH_CONVICTION"
    # genuine high conviction = burst AND ranked by apex final_score (the system's own
    # conviction mass), not by velocity (which surfaces low-score tail noise)
    rec = burst.sort_values(["final_score", "velocity_score"], ascending=False).head(TOP_N)
    print(f"universe at T: {len(lb)} tickers · burst names: {len(burst)} "
          f"({int(burst['is_hc'].sum())} also HIGH_CONVICTION) · recommending TOP {TOP_N} by apex final_score\n")
    print(f"{'ticker':10} {'company':34} {'vel':>7} {'peak':>5} {'gΔ30':>6} {'eΔ30':>5} {'score':>7} {'rank':>6}")
    out = []
    for _, r in rec.iterrows():
        print(f"{r['ticker']:10} {str(r['company'])[:34]:34} {r['velocity_score']:7.1f} {r['global_rank_peak_30d']:5.0f} "
              f"{r['global_rank_delta_30d']:6.0f} {r['etf_count_delta_30d']:5.0f} {r['final_score']:7.1f} {r['leaderboard_rank']:6.0f}")
        out.append((r["ticker"], r["company"], r["velocity_score"]))
    print("\n  -> ACTUAL forward returns from recommendation date (measured now, not used for selection):")
    wins = 0
    for tkr, comp, vel in out:
        fr = fwd(tkr, T)
        if fr is None:
            print(f"   {tkr:10} {str(comp)[:30]:30}  (no price data)")
            continue
        rn, r20, r40 = fr
        ok = "UP ✓" if rn > 0 else "DOWN ✗"
        if rn > 0: wins += 1
        print(f"   {tkr:10} {str(comp)[:30]:30}  fwd_to_now {rn*100:+.1f}%  +20d {('' if r20 is None else f'{r20*100:+.1f}%')}  +40d {('' if r40 is None else f'{r40*100:+.1f}%')}   {ok}")
    print(f"   => {wins}/{len(out)} recommended names went UP after the signal")
print("\nDONE")
