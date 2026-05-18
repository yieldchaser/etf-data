import json

lb = json.load(open('docs/data/leaderboard.json'))
burst = [r for r in lb if r.get('burst_30d')]

print(f'Total BURST count: {len(burst)}')
print('\nSample BURST tickers (first 10):')
for r in burst[:10]:
    print(f"{r['ticker']:8} peak={r.get('global_rank_peak_30d'):3} delta={r.get('global_rank_delta_30d'):4} best=#{r.get('global_rank_best_30d'):3} current=#{r['leaderboard_rank']:3}")

print('\nChecking for false positives (CNY, EUR, JPY, HII, FGXXX):')
for ticker in ['CNY', 'EUR', 'JPY', 'HII', 'FGXXX']:
    r = next((x for x in lb if x['ticker'] == ticker), None)
    if r:
        print(f"{ticker:8} burst={r.get('burst_30d')} peak={r.get('global_rank_peak_30d')} etf_count={r.get('etf_count')}")
