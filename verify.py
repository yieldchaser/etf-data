import json
lb = json.loads(open('docs/data/leaderboard.json').read())
r0 = lb[0]
keys = [k for k in r0 if 'velocity' in k or 'rank_delta' in k or 'etf_count_delta' in k or 'weight_flow_7d' in k]
print('Velocity keys:', keys)
print('Top 5 by velocity:')
lb.sort(key=lambda x: x.get('velocity_score', 0), reverse=True)
for r in lb[:5]:
    print(f"  {r['ticker']}: vel={r.get('velocity_score')}, rd7={r.get('avg_rank_delta_7d')}, etfD30={r.get('etf_count_delta_30d')}")
chg = json.loads(open('docs/data/changelog.json').read())
print(f'top_velocity in changelog: {len(chg.get("top_velocity", []))} entries')
if chg.get('top_velocity'):
    print('  First:', chg['top_velocity'][0])
