"""Captured baselines for preservation tests.

These JSON files (and the pytest pass-set txt) are checked into the repo so
the fix can later assert "no change vs. baseline". They are captured ONCE
on unfixed code; they should NOT be regenerated automatically.

Files:
- annual_cache.json     → {asset_id: {year: {ret, partial, count}}} for every asset
- metals_2026.json      → 2026 cells for every metals asset
- meta_sources.json     → {asset_id: source_label} for every asset
- non_dual_meta_sources.json → meta sources excluding sp500/nasdaq/nasdaq100
- markets_html_tabs.json → DOM-text snapshots of non-Return-Matrix tab blocks
- pytest_pass_set.txt   → list of tests passing on unfixed code (one per line)
"""
