"""Patch build.py and index.html for Phase 2.6 Part B: Velocity signal."""
import pathlib, re

# ═══════════════════════════════════════════════════════════════
# 1. build.py — add _attach_velocity + top_velocity to changelog
# ═══════════════════════════════════════════════════════════════
bp = pathlib.Path("predator/build.py")
txt = bp.read_text(encoding="utf-8")
orig = len(txt)

# Insert _attach_velocity function + call right before "# ── leaderboard.json"
# which comes after the multi-period score delta block (line ~196)
anchor_lb = "    # ── leaderboard.json — main payload for the site ──────────────────────────\n"
velocity_block = (
    "    # ── VELOCITY signal — aggregates per-ETF rank/weight motion ──────────────────\n"
    "    def _attach_velocity(lb, d7, d30, hist_lb):\n"
    "        \"\"\"Add 6 velocity columns to the leaderboard DataFrame.\"\"\"\n"
    "        if d7 is not None and not d7.empty:\n"
    "            rank_avg_7  = d7.groupby('ticker')['rank_delta'].mean()\n"
    "            flow_avg_7  = d7.groupby('ticker')['weight_flow'].mean()\n"
    "        else:\n"
    "            rank_avg_7 = flow_avg_7 = pd.Series(dtype=float)\n"
    "\n"
    "        if d30 is not None and not d30.empty:\n"
    "            rank_avg_30 = d30.groupby('ticker')['rank_delta'].mean()\n"
    "        else:\n"
    "            rank_avg_30 = pd.Series(dtype=float)\n"
    "\n"
    "        # ETF count change vs ~30d ago\n"
    "        if hist_lb:\n"
    "            dates_sorted = sorted(hist_lb.keys())\n"
    "            if len(dates_sorted) >= 2:\n"
    "                target = dates_sorted[-1] - pd.Timedelta(days=30)\n"
    "                past_date = min(dates_sorted, key=lambda d: abs((d - target).total_seconds()))\n"
    "                past_counts = hist_lb[past_date].set_index('ticker')['etf_count'] if 'etf_count' in hist_lb[past_date].columns else pd.Series(dtype=float)\n"
    "            else:\n"
    "                past_counts = pd.Series(dtype=float)\n"
    "        else:\n"
    "            past_counts = pd.Series(dtype=float)\n"
    "\n"
    "        lb['avg_rank_delta_7d']   = lb['ticker'].map(rank_avg_7).fillna(0).round(2)\n"
    "        lb['avg_weight_flow_7d']  = lb['ticker'].map(flow_avg_7).fillna(0).round(4)\n"
    "        lb['avg_rank_delta_30d']  = lb['ticker'].map(rank_avg_30).fillna(0).round(2)\n"
    "        lb['etf_count_30d_ago']   = lb['ticker'].map(past_counts).fillna(lb['etf_count']).astype(int)\n"
    "        lb['etf_count_delta_30d'] = (lb['etf_count'] - lb['etf_count_30d_ago']).astype(int)\n"
    "\n"
    "        lb['velocity_score'] = (\n"
    "            lb['avg_rank_delta_7d'].fillna(0) * 1.0 +\n"
    "            lb['avg_weight_flow_7d'].fillna(0) * 20.0 +\n"
    "            lb['etf_count_delta_30d'].fillna(0) * 5.0 +\n"
    "            lb.get('score_streak', pd.Series(0, index=lb.index)).fillna(0).clip(-10, 10) * 1.0\n"
    "        ).round(2)\n"
    "        return lb\n"
    "\n"
    "    leaderboard = _attach_velocity(\n"
    "        leaderboard,\n"
    "        deltas_by_period.get(7),\n"
    "        deltas_by_period.get(30),\n"
    "        historical,\n"
    "    )\n"
    "    print(f\"  velocity_score: range [{leaderboard['velocity_score'].min():.1f}, {leaderboard['velocity_score'].max():.1f}]\")\n"
    "\n"
)

if anchor_lb in txt:
    txt = txt.replace(anchor_lb, velocity_block + anchor_lb, 1)
    print("build.py fix 1 (velocity): OK")
else:
    print("build.py fix 1: NOT FOUND")

# Add top_velocity to changelog dict right before the JSON write
anchor_chg = "    # ── changelog.json — entries / exits / movers ─────────────────────────────\n"
    
top_vel_block = (
    "    # Top velocity movers (15 names, held by 2+ ETFs)\n"
    "    if 'velocity_score' in leaderboard.columns:\n"
    "        top_vel = leaderboard[leaderboard['etf_count'] >= 2].sort_values('velocity_score', ascending=False).head(15)\n"
    "        chg['top_velocity'] = [\n"
    "            {\n"
    "                'ticker':            str(r['ticker']),\n"
    "                'company':           str(r.get('company', '')),\n"
    "                'velocity_score':    float(r['velocity_score']),\n"
    "                'avg_rank_delta_7d': float(r['avg_rank_delta_7d']),\n"
    "                'etf_count_delta_30d': int(r['etf_count_delta_30d']),\n"
    "                'final_score':       int(r['final_score']),\n"
    "                'etf_count':         int(r['etf_count']),\n"
    "                'tiers':             str(r.get('tiers', '')),\n"
    "            }\n"
    "            for _, r in top_vel.iterrows()\n"
    "        ]\n"
    "\n"
)

if anchor_chg in txt:
    txt = txt.replace(anchor_chg, top_vel_block + anchor_chg, 1)
    print("build.py fix 2 (top_velocity): OK")
else:
    print("build.py fix 2: NOT FOUND")

bp.write_text(txt, encoding="utf-8")
print(f"build.py done. {len(txt)} bytes (was {orig})")
