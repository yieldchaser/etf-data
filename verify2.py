import json

lb = json.loads(open('docs/data/leaderboard.json').read())

# Check STX
stx = next((r for r in lb if r['ticker'] == 'STX'), None)
if stx:
    print("STX velocity fields:")
    for k in ['velocity_score','global_rank_delta_30d','global_rank_peak_30d','global_rank_best_30d',
              'avg_rank_delta_7d','etf_count_delta_30d','burst_30d','leaderboard_rank']:
        print(f"  {k}: {stx.get(k)}")
else:
    print("STX not in leaderboard")

# Top 10 by velocity
print("\nTop 10 by velocity_score:")
lb.sort(key=lambda x: x.get('velocity_score', 0), reverse=True)
for r in lb[:10]:
    print(f"  #{r.get('leaderboard_rank','?')} {r['ticker']:6s}  vel={r.get('velocity_score'):7.1f}  burst={r.get('burst_30d')}  grd30={r.get('global_rank_delta_30d')}  peak={r.get('global_rank_peak_30d')}")

# Validate changelog
chg = json.loads(open('docs/data/changelog.json').read())
tv = chg.get('top_velocity', [])
print(f"\ntop_velocity: {len(tv)} entries")
for x in tv[:3]:
    print(f"  {x['ticker']:6s} vel={x['velocity_score']:.1f} burst={x.get('burst_30d')} grd30Δ={x.get('global_rank_delta_30d')} peak={x.get('global_rank_peak_30d')}")
