"""
Patch index.html for Phase 2.6 Part B:
1. Add velocityOnly + burstOnly state variables
2. Update counts getter with VELO + BURST
3. Update reset() to clear new flags
4. Update filtered getter with velocity/burst filters
5. Add velocity, global_rank_delta_30d, global_rank_peak_30d etc. to sortableCols
6. Add VELO + BURST filter chips to filter bar
7. Add Velocity <th> to thead
8. Add Velocity <td> to tbody row
9. Add BURST and VELO chips to flag cell
10. Add fmtVelocity, velocityColor, velocityTip helpers
11. Add Top Velocity Movers panel to Changes tab
"""
import pathlib

p = pathlib.Path("docs/index.html")
txt = p.read_text(encoding="utf-8")
orig = len(txt)

# ── 1. Add velocityOnly + burstOnly to state ──────────────────────────────────
old1 = "    flag: '', selectedTier: '', newOnly: false, q: '',"
new1 = "    flag: '', selectedTier: '', newOnly: false, velocityOnly: false, burstOnly: false, q: '',"
if old1 in txt:
    txt = txt.replace(old1, new1, 1)
    print("Fix 1 (state): OK")
else:
    print("Fix 1 (state): NOT FOUND")

# ── 2. Update counts getter ────────────────────────────────────────────────────
old2 = (
    "    get counts() {\n"
    "      return {\n"
    "        HIGH_CONVICTION: this.leaderboard.filter(r => r.flag === 'HIGH_CONVICTION').length,\n"
    "        SPECULATIVE_BETA: this.leaderboard.filter(r => r.flag === 'SPECULATIVE_BETA').length,\n"
    "        NEW: this.leaderboard.filter(r => r.any_new).length,\n"
    "      };\n"
    "    },\n"
)
new2 = (
    "    get counts() {\n"
    "      return {\n"
    "        HIGH_CONVICTION: this.leaderboard.filter(r => r.flag === 'HIGH_CONVICTION').length,\n"
    "        SPECULATIVE_BETA: this.leaderboard.filter(r => r.flag === 'SPECULATIVE_BETA').length,\n"
    "        NEW: this.leaderboard.filter(r => r.any_new).length,\n"
    "        VELO: this.leaderboard.filter(r => (r.velocity_score || 0) >= 25 && (r.etf_count || 0) >= 2 && !r.burst_30d).length,\n"
    "        BURST: this.leaderboard.filter(r => r.burst_30d).length,\n"
    "      };\n"
    "    },\n"
)
if old2 in txt:
    txt = txt.replace(old2, new2, 1)
    print("Fix 2 (counts): OK")
else:
    print("Fix 2 (counts): NOT FOUND")

# ── 3. Update reset() ─────────────────────────────────────────────────────────
old3 = "    reset() { this.flag = ''; this.newOnly = false; this.selectedTier = ''; this.q = ''; this.page = 1; },"
new3 = "    reset() { this.flag = ''; this.newOnly = false; this.velocityOnly = false; this.burstOnly = false; this.selectedTier = ''; this.q = ''; this.page = 1; },"
if old3 in txt:
    txt = txt.replace(old3, new3, 1)
    print("Fix 3 (reset): OK")
else:
    print("Fix 3 (reset): NOT FOUND")

# ── 4. Update filtered getter — add velocity/burst filters ────────────────────
old4 = (
    "      if (this.flag)    r = r.filter(x => x.flag === this.flag);\n"
    "      if (this.newOnly) r = r.filter(x => x.any_new);\n"
    "      if (tier)         r = r.filter(x => (x.tiers || '').split(' + ').includes(tier));\n"
    "      if (q)            r = r.filter(x => (x.ticker || '').toLowerCase().includes(q) || (x.company || '').toLowerCase().includes(q));\n"
)
new4 = (
    "      if (this.flag)         r = r.filter(x => x.flag === this.flag);\n"
    "      if (this.newOnly)      r = r.filter(x => x.any_new);\n"
    "      if (this.velocityOnly) r = r.filter(x => (x.velocity_score || 0) >= 25 && (x.etf_count || 0) >= 2 && !x.burst_30d);\n"
    "      if (this.burstOnly)    r = r.filter(x => x.burst_30d);\n"
    "      if (tier)              r = r.filter(x => (x.tiers || '').split(' + ').includes(tier));\n"
    "      if (q)                 r = r.filter(x => (x.ticker || '').toLowerCase().includes(q) || (x.company || '').toLowerCase().includes(q));\n"
)
if old4 in txt:
    txt = txt.replace(old4, new4, 1)
    print("Fix 4 (filtered): OK")
else:
    print("Fix 4 (filtered): NOT FOUND")

# ── 5. Add velocity cols to sortableCols ──────────────────────────────────────
old5 = (
    "      { key: 'score_percentile', label: 'Percentile (level)' },\n"
    "    ],"
)
new5 = (
    "      { key: 'score_percentile',      label: 'Percentile (level)' },\n"
    "      { key: 'velocity_score',         label: 'Velocity (composite)' },\n"
    "      { key: 'global_rank_delta_30d',  label: 'Global rank \u0394 (30d)' },\n"
    "      { key: 'global_rank_peak_30d',   label: 'Peak rank improvement (30d)' },\n"
    "      { key: 'avg_rank_delta_7d',      label: 'Avg per-ETF rank \u0394 (7d)' },\n"
    "      { key: 'etf_count_delta_30d',    label: 'ETFs added (30d)' },\n"
    "    ],"
)
if old5 in txt:
    txt = txt.replace(old5, new5, 1)
    print("Fix 5 (sortableCols): OK")
else:
    print("Fix 5 (sortableCols): NOT FOUND")

# ── 6. Add VELO + BURST filter chips (after NEW button, before tier select) ───
old6 = (
    '        <button @click="newOnly = !newOnly"\n'
    '                :class="newOnly ? \'border-blue-500/40 text-blue-300\' : \'\'"\n'
    '                class="chip border px-2 py-1 transition hover:border-blue-500/30"\n'
    '                style="border-color: var(--border-2)"\n'
    '                x-text="`NEW ${counts.NEW || 0}`"></button>\n'
    '        <select x-model="selectedTier"'
)
new6 = (
    '        <button @click="newOnly = !newOnly"\n'
    '                :class="newOnly ? \'border-blue-500/40 text-blue-300\' : \'\'"\n'
    '                class="chip border px-2 py-1 transition hover:border-blue-500/30"\n'
    '                style="border-color: var(--border-2)"\n'
    '                x-text="`NEW ${counts.NEW || 0}`"></button>\n'
    '        <button @click="velocityOnly = !velocityOnly"\n'
    '                :class="velocityOnly ? \'border-cyan-500/40 text-cyan-300\' : \'\'"\n'
    '                class="chip border px-2 py-1 transition hover:border-cyan-500/30"\n'
    '                style="border-color: var(--border-2)"\n'
    '                x-tooltip="\'Show only top-velocity names (velocity \u2265 25, held by 2+ ETFs, no burst)\'"\n'
    '                x-text="`VELO ${counts.VELO || 0}`"></button>\n'
    '        <button @click="burstOnly = !burstOnly"\n'
    '                :class="burstOnly ? \'border-purple-500/40 text-purple-300\' : \'\'"\n'
    '                class="chip border px-2 py-1 transition hover:border-purple-500/30"\n'
    '                style="border-color: var(--border-2)"\n'
    '                x-tooltip="\'Show only burst movers — stocks that improved global rank by 40+ in the last 30 days\'"\n'
    '                x-text="`BURST ${counts.BURST || 0}`"></button>\n'
    '        <select x-model="selectedTier"'
)
if old6 in txt:
    txt = txt.replace(old6, new6, 1)
    print("Fix 6 (VELO+BURST chips): OK")
else:
    print("Fix 6 (VELO+BURST chips): NOT FOUND")

# ── 7. Add Velocity <th> after the SC.Δ% header ───────────────────────────────
old7 = (
    '              <th class="text-right px-3 py-2 cursor-pointer hover:text-zinc-300 hidden md:table-cell" @click="sort(\'etf_count\')" :class="sortClass(\'etf_count\')" x-tooltip="\'Count of distinct ETFs currently holding this ticker.\'">ETFs</th>\n'
)
new7 = (
    '              <th @click="sort(\'velocity_score\')" :class="sortClass(\'velocity_score\')"\n'
    '                  class="text-right px-3 py-2 cursor-pointer hover:text-zinc-300 hidden lg:table-cell"\n'
    '                  x-tooltip="\'Velocity \u2014 composite rate-of-change signal across all ETFs. Sums: global rank \u0394 (30d) + avg per-ETF rank \u0394 (7d) + weight flow (7d) + ETF count change (30d) + score streak. High positive = institutional accumulation accelerating.\'">Velocity</th>\n'
    '              <th class="text-right px-3 py-2 cursor-pointer hover:text-zinc-300 hidden md:table-cell" @click="sort(\'etf_count\')" :class="sortClass(\'etf_count\')" x-tooltip="\'Count of distinct ETFs currently holding this ticker.\'">ETFs</th>\n'
)
if old7 in txt:
    txt = txt.replace(old7, new7, 1)
    print("Fix 7 (velocity th): OK")
else:
    print("Fix 7 (velocity th): NOT FOUND")

# ── 8. Add Velocity <td> after the SC.Δ% cell ─────────────────────────────────
old8 = (
    '                  <td class="px-3 py-2 text-right num font-mono hidden lg:table-cell" :style="`color: ${flowColor(activeDeltaValue(row))}`" x-text="fmtPct(activeDeltaValue(row))"></td>\n'
    '                  <td class="px-3 py-2 text-right num font-mono hidden md:table-cell" x-text="row.etf_count"></td>\n'
)
new8 = (
    '                  <td class="px-3 py-2 text-right num font-mono hidden lg:table-cell" :style="`color: ${flowColor(activeDeltaValue(row))}`" x-text="fmtPct(activeDeltaValue(row))"></td>\n'
    '                  <td class="px-3 py-2 text-right num font-mono hidden lg:table-cell"\n'
    '                      :style="`color: ${velocityColor(row.velocity_score)}`"\n'
    '                      x-tooltip="velocityTip(row)"\n'
    '                      x-text="fmtVelocity(row.velocity_score)"></td>\n'
    '                  <td class="px-3 py-2 text-right num font-mono hidden md:table-cell" x-text="row.etf_count"></td>\n'
)
if old8 in txt:
    txt = txt.replace(old8, new8, 1)
    print("Fix 8 (velocity td): OK")
else:
    print("Fix 8 (velocity td): NOT FOUND")

# ── 9. Add BURST + VELO chips to flag cell ────────────────────────────────────
old9 = (
    '                      <span x-show="row.any_new" class="chip"\n'
    '                            x-tooltip="\'NEW — first appearance in any ETF within the last 14 days\'"\n'
    '                            style="background: rgba(96, 165, 250, 0.10); color: var(--blue); border: 1px solid rgba(96, 165, 250, 0.25)">NEW</span>\n'
    '                    </div>\n'
    '                  </td>\n'
    '                </tr>\n'
)
new9 = (
    '                      <span x-show="row.any_new" class="chip"\n'
    '                            x-tooltip="\'NEW — first appearance in any ETF within the last 14 days\'"\n'
    '                            style="background: rgba(96, 165, 250, 0.10); color: var(--blue); border: 1px solid rgba(96, 165, 250, 0.25)">NEW</span>\n'
    '                      <span x-show="row.burst_30d" class="chip"\n'
    '                            :x-tooltip="`\'BURST move \u2014 global rank improved by \'+ row.global_rank_peak_30d +\'+ positions in last 30d. Currently #\'+ row.leaderboard_rank +\' (best in window: #\'+ row.global_rank_best_30d +\')\u2019`"\n'
    '                            style="background: rgba(168, 85, 247, 0.12); color: #c084fc; border: 1px solid rgba(168, 85, 247, 0.30)">BURST</span>\n'
    '                      <span x-show="(row.velocity_score || 0) >= 25 && (row.etf_count || 0) >= 2 && !row.burst_30d" class="chip"\n'
    '                            :x-tooltip="`\'Top-velocity composite (\'+ row.velocity_score +\'). Conviction is concentrating.\u2019`"\n'
    '                            style="background: rgba(34, 211, 238, 0.10); color: var(--cyan); border: 1px solid rgba(34, 211, 238, 0.25)">VELO</span>\n'
    '                    </div>\n'
    '                  </td>\n'
    '                </tr>\n'
)
if old9 in txt:
    txt = txt.replace(old9, new9, 1)
    print("Fix 9 (BURST+VELO chips): OK")
else:
    print("Fix 9 (BURST+VELO chips): NOT FOUND")

# ── 10. Add fmtVelocity, velocityColor, velocityTip helpers ──────────────────
old10 = "    formatBuilt(iso) {"
new10 = (
    "    fmtVelocity(v) {\n"
    "      if (v == null || !isFinite(v)) return '\u2014';\n"
    "      const n = Number(v);\n"
    "      if (Math.abs(n) < 0.5) return '0';\n"
    "      return (n > 0 ? '+' : '') + n.toFixed(1);\n"
    "    },\n"
    "\n"
    "    velocityColor(v) {\n"
    "      if (v == null || !isFinite(v)) return 'var(--text-3)';\n"
    "      const n = Number(v);\n"
    "      if (n >= 15) return 'var(--up)';\n"
    "      if (n >= 5)  return '#86efac';\n"
    "      if (n <= -15) return 'var(--down)';\n"
    "      if (n <= -5)  return '#fda4af';\n"
    "      return 'var(--text-2)';\n"
    "    },\n"
    "\n"
    "    velocityTip(row) {\n"
    "      const rd7  = row.avg_rank_delta_7d ?? 0;\n"
    "      const wf7  = (row.avg_weight_flow_7d ?? 0) * 100;\n"
    "      const ed30 = row.etf_count_delta_30d ?? 0;\n"
    "      const sst  = row.score_streak ?? 0;\n"
    "      const grd30 = row.global_rank_delta_30d ?? 0;\n"
    "      const grp30 = row.global_rank_peak_30d ?? 0;\n"
    "      return `<div class=\"font-mono text-[10px]\">`\n"
    "        + `<div style=\"color: var(--cyan)\">VELOCITY ${this.fmtVelocity(row.velocity_score)}</div>`\n"
    "        + `<div>global rank \u0394 (30d): ${grd30 >= 0 ? '+' : ''}${grd30} positions</div>`\n"
    "        + `<div>peak improvement (30d): +${grp30} positions</div>`\n"
    "        + `<div>avg per-ETF rank \u0394 (7d): ${rd7 >= 0 ? '+' : ''}${rd7.toFixed(1)}</div>`\n"
    "        + `<div>avg weight flow (7d): ${wf7 >= 0 ? '+' : ''}${wf7.toFixed(1)}%</div>`\n"
    "        + `<div>ETFs added (30d): ${ed30 >= 0 ? '+' : ''}${ed30}</div>`\n"
    "        + `<div>score streak: ${sst >= 0 ? '+' : ''}${sst} days</div>`\n"
    "        + (row.burst_30d ? '<div style=\"color: #c084fc; margin-top: 4px\">\u26a1 BURST detected</div>' : '')\n"
    "        + '</div>';\n"
    "    },\n"
    "\n"
    "    formatBuilt(iso) {"
)
if "    formatBuilt(iso) {" in txt:
    txt = txt.replace("    formatBuilt(iso) {", new10, 1)
    print("Fix 10 (velocity helpers): OK")
else:
    print("Fix 10 (velocity helpers): NOT FOUND")

# ── 11. Add Top Velocity Movers panel to Changes tab ─────────────────────────
old11 = (
    "      <!-- New entrants this week -->\n"
    "      <div class=\"lg:col-span-2 rounded-lg border\" style=\"background: var(--surface); border-color: var(--border)\">\n"
    "        <div class=\"px-3 py-2 flex items-center justify-between\" style=\"border-bottom: 1px solid var(--border)\">\n"
    "          <div class=\"label-cyan label\">New tickers this week</div>\n"
    "          <div class=\"text-xs num font-mono\" style=\"color: var(--text-3)\" x-text=\"`${changelog.new_entrants?.length || 0} names · not seen 7+ days ago`\"></div>\n"
    "        </div>"
)
new11 = (
    "      <!-- Top Velocity Movers -->\n"
    "      <div class=\"lg:col-span-2 rounded-lg border\" style=\"background: var(--surface); border-color: var(--border)\">\n"
    "        <div class=\"px-3 py-2 flex items-center justify-between\" style=\"border-bottom: 1px solid var(--border)\">\n"
    "          <div class=\"label\" style=\"color: var(--cyan)\">Top velocity movers &mdash; last 30 days</div>\n"
    "          <div class=\"text-xs num font-mono\" style=\"color: var(--text-3)\">\u26a1 highest acceleration of institutional conviction</div>\n"
    "        </div>\n"
    "        <div class=\"grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3\">\n"
    "          <template x-for=\"r in (changelog.top_velocity || [])\" :key=\"r.ticker\">\n"
    "            <div class=\"px-3 py-2 text-xs hover:bg-white/5 transition\" style=\"border-bottom: 1px solid rgba(255,255,255,0.03)\">\n"
    "              <div class=\"flex items-center justify-between\">\n"
    "                <div class=\"flex flex-col\">\n"
    "                  <a :href=\"`stock.html?t=${r.ticker}`\" class=\"font-mono font-medium hover:text-cyan-300 transition\" style=\"color: inherit; text-decoration: none;\" x-text=\"r.ticker\"></a>\n"
    "                  <span style=\"color: var(--text-2)\" class=\"truncate max-w-[180px]\" x-text=\"r.company\"></span>\n"
    "                </div>\n"
    "                <div class=\"num font-mono text-right\">\n"
    "                  <div :style=\"`color: ${velocityColor(r.velocity_score)}`\" x-text=\"fmtVelocity(r.velocity_score)\"></div>\n"
    "                  <div class=\"text-[10px]\" style=\"color: var(--text-3)\">\n"
    "                    <span x-show=\"r.burst_30d\" style=\"color: #c084fc\">\u26a1BURST </span>\n"
    "                    <span x-text=\"`${r.etf_count} ETF${r.etf_count===1?'':'s'}`\"></span>\n"
    "                  </div>\n"
    "                </div>\n"
    "              </div>\n"
    "              <div class=\"flex gap-3 mt-1\" style=\"color: var(--text-3)\">\n"
    "                <span x-text=\"`\u0394rank7d: ${(r.avg_rank_delta_7d||0)>=0?'+':''}${(r.avg_rank_delta_7d||0).toFixed(1)}`\"></span>\n"
    "                <span x-show=\"r.etf_count_delta_30d != 0\" :style=\"`color: ${(r.etf_count_delta_30d||0)>0?'var(--up)':'var(--down)'}`\"\n"
    "                      x-text=\"`ETFs: ${(r.etf_count_delta_30d||0)>0?'+':''}${r.etf_count_delta_30d}`\"></span>\n"
    "              </div>\n"
    "            </div>\n"
    "          </template>\n"
    "          <div x-show=\"!changelog.top_velocity?.length\" class=\"px-3 py-4 text-xs\" style=\"color: var(--text-3)\">No data — rebuild required.</div>\n"
    "        </div>\n"
    "      </div>\n"
    "\n"
    "      <!-- New entrants this week -->\n"
    "      <div class=\"lg:col-span-2 rounded-lg border\" style=\"background: var(--surface); border-color: var(--border)\">\n"
    "        <div class=\"px-3 py-2 flex items-center justify-between\" style=\"border-bottom: 1px solid var(--border)\">\n"
    "          <div class=\"label-cyan label\">New tickers this week</div>\n"
    "          <div class=\"text-xs num font-mono\" style=\"color: var(--text-3)\" x-text=\"`${changelog.new_entrants?.length || 0} names · not seen 7+ days ago`\"></div>\n"
    "        </div>"
)
if old11 in txt:
    txt = txt.replace(old11, new11, 1)
    print("Fix 11 (top velocity panel): OK")
else:
    print("Fix 11 (top velocity panel): NOT FOUND")

p.write_text(txt, encoding="utf-8")
print(f"\nindex.html done. {len(txt)} bytes (was {orig})")
