"""
Patch build.py: replace old _attach_velocity with the new full formula
(global rank trajectory, burst_30d, peak_improvement_30d, etc.)
Also check that 'leaderboard_rank' column exists in historical snapshots.
"""
import pathlib

bp = pathlib.Path("predator/build.py")
txt = bp.read_text(encoding="utf-8")
orig = len(txt)

# The old _attach_velocity function + its call site
OLD_VEL = """\
    # ── VELOCITY signal — aggregates per-ETF rank/weight motion ──────────────────
    def _attach_velocity(lb, d7, d30, hist_lb):
        \"\"\"Add 6 velocity columns to the leaderboard DataFrame.\"\"\"
        if d7 is not None and not d7.empty:
            rank_avg_7  = d7.groupby('ticker')['rank_delta'].mean()
            flow_avg_7  = d7.groupby('ticker')['weight_flow'].mean()
        else:
            rank_avg_7 = flow_avg_7 = pd.Series(dtype=float)

        if d30 is not None and not d30.empty:
            rank_avg_30 = d30.groupby('ticker')['rank_delta'].mean()
        else:
            rank_avg_30 = pd.Series(dtype=float)

        # ETF count change vs ~30d ago
        if hist_lb:
            dates_sorted = sorted(hist_lb.keys())
            if len(dates_sorted) >= 2:
                target = dates_sorted[-1] - pd.Timedelta(days=30)
                past_date = min(dates_sorted, key=lambda d: abs((d - target).total_seconds()))
                past_counts = hist_lb[past_date].set_index('ticker')['etf_count'] if 'etf_count' in hist_lb[past_date].columns else pd.Series(dtype=float)
            else:
                past_counts = pd.Series(dtype=float)
        else:
            past_counts = pd.Series(dtype=float)

        lb['avg_rank_delta_7d']   = lb['ticker'].map(rank_avg_7).fillna(0).round(2)
        lb['avg_weight_flow_7d']  = lb['ticker'].map(flow_avg_7).fillna(0).round(4)
        lb['avg_rank_delta_30d']  = lb['ticker'].map(rank_avg_30).fillna(0).round(2)
        lb['etf_count_30d_ago']   = lb['ticker'].map(past_counts).fillna(lb['etf_count']).astype(int)
        lb['etf_count_delta_30d'] = (lb['etf_count'] - lb['etf_count_30d_ago']).astype(int)

        lb['velocity_score'] = (
            lb['avg_rank_delta_7d'].fillna(0) * 1.0 +
            lb['avg_weight_flow_7d'].fillna(0) * 20.0 +
            lb['etf_count_delta_30d'].fillna(0) * 5.0 +
            lb.get('score_streak', pd.Series(0, index=lb.index)).fillna(0).clip(-10, 10) * 1.0
        ).round(2)
        return lb

    leaderboard = _attach_velocity(
        leaderboard,
        deltas_by_period.get(7),
        deltas_by_period.get(30),
        historical,
    )
    print(f\"  velocity_score: range [{leaderboard['velocity_score'].min():.1f}, {leaderboard['velocity_score'].max():.1f}]\")
"""

NEW_VEL = """\
    # ── VELOCITY signal — captures both steady accumulation AND burst moves ─────
    def _attach_velocity(leaderboard: pd.DataFrame,
                         deltas_by_period: dict,
                         historical: dict) -> pd.DataFrame:
        \"\"\"Add velocity columns. Catches STX-style +55-ranks-in-12-days bursts
        that a naive 7d-only delta would miss.\"\"\"

        # 1. Per-ETF rank/weight motion
        d7  = deltas_by_period.get(7)
        d30 = deltas_by_period.get(30)
        rank_avg_7  = d7.groupby("ticker")["rank_delta"].mean()  if d7  is not None and not d7.empty  else pd.Series(dtype=float)
        flow_avg_7  = d7.groupby("ticker")["weight_flow"].mean() if d7  is not None and not d7.empty  else pd.Series(dtype=float)
        rank_avg_30 = d30.groupby("ticker")["rank_delta"].mean() if d30 is not None and not d30.empty else pd.Series(dtype=float)

        # 2. Global leaderboard rank trajectory (requires leaderboard_rank col in historical snapshots)
        dates_sorted = sorted(historical.keys())
        today_date = dates_sorted[-1]
        window_start = today_date - pd.Timedelta(days=30)
        window_cols = [c for c in dates_sorted if c >= window_start]

        global_rank_delta_30 = pd.Series(dtype=float)
        peak_improvement_30  = pd.Series(dtype=float)
        best_in_window       = pd.Series(dtype=float)

        if len(window_cols) >= 2:
            # Check historical snapshots have leaderboard_rank
            sample = historical[window_cols[0]]
            if "leaderboard_rank" in sample.columns:
                rank_panel_rows = {}
                for d in window_cols:
                    rank_panel_rows[d] = historical[d].set_index("ticker")["leaderboard_rank"]
                rank_panel = pd.DataFrame(rank_panel_rows)
                first_col        = rank_panel.iloc[:, 0]
                worst_in_window  = rank_panel.max(axis=1)
                best_in_window   = rank_panel.min(axis=1)
                current          = rank_panel.iloc[:, -1]
                global_rank_delta_30 = (first_col - current).round(0)        # positive = improved
                peak_improvement_30  = (worst_in_window - best_in_window).round(0)

        # 3. ETF count change vs ~30d ago
        past_counts = pd.Series(dtype=float)
        if len(dates_sorted) >= 2:
            target    = today_date - pd.Timedelta(days=30)
            past_date = min(dates_sorted, key=lambda d: abs((d - target).total_seconds()))
            if "etf_count" in historical[past_date].columns:
                past_counts = historical[past_date].set_index("ticker")["etf_count"]

        # 4. Attach all raw signals
        leaderboard["avg_rank_delta_7d"]     = leaderboard["ticker"].map(rank_avg_7).fillna(0).round(2)
        leaderboard["avg_weight_flow_7d"]    = leaderboard["ticker"].map(flow_avg_7).fillna(0).round(4)
        leaderboard["avg_rank_delta_30d"]    = leaderboard["ticker"].map(rank_avg_30).fillna(0).round(2)
        leaderboard["global_rank_delta_30d"] = leaderboard["ticker"].map(global_rank_delta_30).fillna(0).astype(int)
        leaderboard["global_rank_peak_30d"]  = leaderboard["ticker"].map(peak_improvement_30).fillna(0).astype(int)
        leaderboard["global_rank_best_30d"]  = leaderboard["ticker"].map(best_in_window).fillna(leaderboard["leaderboard_rank"]).astype(int)
        leaderboard["etf_count_30d_ago"]     = leaderboard["ticker"].map(past_counts).fillna(leaderboard["etf_count"]).astype(int)
        leaderboard["etf_count_delta_30d"]   = (leaderboard["etf_count"] - leaderboard["etf_count_30d_ago"]).astype(int)
        # Burst: peak improvement of >=40 global ranks at any point in last 30d
        leaderboard["burst_30d"]             = leaderboard["global_rank_peak_30d"] >= 40

        # 5. Composite velocity score
        # Tuning: global rank Δ30d of +50 → +25; peak +50 → +12.5;
        #         per-ETF avg Δ7d of +5 → +5; weight flow +20% → +4;
        #         ETFs added 30d +1 → +5; score streak +2d → +2
        leaderboard["velocity_score"] = (
            leaderboard["global_rank_delta_30d"].fillna(0).clip(-200, 200) * 0.5 +
            leaderboard["global_rank_peak_30d"].fillna(0).clip(0, 200) * 0.25 +
            leaderboard["avg_rank_delta_7d"].fillna(0) * 1.0 +
            leaderboard["avg_weight_flow_7d"].fillna(0) * 20.0 +
            leaderboard["etf_count_delta_30d"].fillna(0) * 5.0 +
            leaderboard["score_streak"].fillna(0).clip(-10, 10) * 1.0
        ).round(2)
        return leaderboard

    leaderboard = _attach_velocity(leaderboard, deltas_by_period, historical)
    print(f"  velocity_score: range [{leaderboard['velocity_score'].min():.1f}, {leaderboard['velocity_score'].max():.1f}]")
    burst_count = int(leaderboard['burst_30d'].sum())
    print(f"  burst_30d:      {burst_count} tickers with >=40 peak rank improvement")
"""

if OLD_VEL in txt:
    txt = txt.replace(OLD_VEL, NEW_VEL, 1)
    print("build.py: _attach_velocity replaced OK")
else:
    print("build.py: OLD_VEL not found verbatim — searching for anchor...")
    idx = txt.find("# ── VELOCITY signal")
    print(f"  Found at char {idx}")
    print(repr(txt[idx:idx+200]))

# Also update top_velocity block to include new fields
OLD_TV = """\
    # Top velocity movers (15 names, held by 2+ ETFs)
    if 'velocity_score' in leaderboard.columns:
        top_vel = leaderboard[leaderboard['etf_count'] >= 2].sort_values('velocity_score', ascending=False).head(15)
        chg['top_velocity'] = [
            {
                'ticker':            str(r['ticker']),
                'company':           str(r.get('company', '')),
                'velocity_score':    float(r['velocity_score']),
                'avg_rank_delta_7d': float(r['avg_rank_delta_7d']),
                'etf_count_delta_30d': int(r['etf_count_delta_30d']),
                'final_score':       int(r['final_score']),
                'etf_count':         int(r['etf_count']),
                'tiers':             str(r.get('tiers', '')),
            }
            for _, r in top_vel.iterrows()
        ]
"""
NEW_TV = """\
    # Top velocity movers (15 names, held by 2+ ETFs)
    if 'velocity_score' in leaderboard.columns:
        top_vel = leaderboard[leaderboard['etf_count'] >= 2].sort_values('velocity_score', ascending=False).head(15)
        chg['top_velocity'] = [
            {
                'ticker':               str(r['ticker']),
                'company':              str(r.get('company', '')),
                'velocity_score':       float(r['velocity_score']),
                'avg_rank_delta_7d':    float(r['avg_rank_delta_7d']),
                'global_rank_delta_30d': int(r.get('global_rank_delta_30d', 0)),
                'global_rank_peak_30d': int(r.get('global_rank_peak_30d', 0)),
                'etf_count_delta_30d':  int(r['etf_count_delta_30d']),
                'burst_30d':            bool(r.get('burst_30d', False)),
                'final_score':          int(r['final_score']),
                'etf_count':            int(r['etf_count']),
                'tiers':                str(r.get('tiers', '')),
            }
            for _, r in top_vel.iterrows()
        ]
"""

if OLD_TV in txt:
    txt = txt.replace(OLD_TV, NEW_TV, 1)
    print("build.py: top_velocity block updated OK")
else:
    print("build.py: OLD_TV not found")

bp.write_text(txt, encoding="utf-8")
print(f"build.py done. {len(txt)} bytes (was {orig})")
