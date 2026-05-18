"""
Patch stock.html for Phase 2.6 Part B:
1. Expand KPI strip to 6 cols, add VELOCITY and ETFs ADDED (30d) cards
2. Add BURST chip to the flag/status banner
3. Add fmtVelocity + velocityColor helpers to stockApp()
"""
import pathlib

p = pathlib.Path("docs/stock.html")
txt = p.read_text(encoding="utf-8")
orig = len(txt)

# ── 1. Expand KPI grid cols + add 2 new cards after the template loop ─────────
old1 = (
    '    <!-- KPI strip -->\n'
    '    <section class="grid grid-cols-2 sm:grid-cols-4 gap-2 sm:gap-3 mb-3">\n'
    '      <template x-for="kpi in kpis" :key="kpi.label">\n'
    '        <div class="rounded-lg px-3 py-2.5 border" style="background: var(--surface); border-color: var(--border)">\n'
    '          <div class="label" x-text="kpi.label"></div>\n'
    '          <div class="flex items-baseline gap-2 mt-0.5">\n'
    '            <div class="text-xl sm:text-2xl font-medium num font-mono" :style="`color: ${kpi.color || \'var(--text)\'}`" x-text="kpi.value"></div>\n'
    '            <div class="text-xs num font-mono" :style="`color: ${kpi.deltaColor}`" x-show="kpi.delta !== undefined && kpi.delta !== null" x-text="kpi.delta"></div>\n'
    '          </div>\n'
    '        </div>\n'
    '      </template>\n'
    '    </section>\n'
)
new1 = (
    '    <!-- KPI strip -->\n'
    '    <section class="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-2 sm:gap-3 mb-3">\n'
    '      <template x-for="kpi in kpis" :key="kpi.label">\n'
    '        <div class="rounded-lg px-3 py-2.5 border" style="background: var(--surface); border-color: var(--border)">\n'
    '          <div class="label" x-text="kpi.label"></div>\n'
    '          <div class="flex items-baseline gap-2 mt-0.5">\n'
    '            <div class="text-xl sm:text-2xl font-medium num font-mono" :style="`color: ${kpi.color || \'var(--text)\'}`" x-text="kpi.value"></div>\n'
    '            <div class="text-xs num font-mono" :style="`color: ${kpi.deltaColor}`" x-show="kpi.delta !== undefined && kpi.delta !== null" x-text="kpi.delta"></div>\n'
    '          </div>\n'
    '        </div>\n'
    '      </template>\n'
    '      <!-- VELOCITY KPI -->\n'
    '      <div class="rounded-lg px-3 py-2.5 border" style="background: var(--surface); border-color: var(--border)"\n'
    '           x-tooltip="\'Composite velocity score across all ETFs holding this name. High positive = institutional accumulation accelerating.\'">\n'
    '        <div class="label">Velocity</div>\n'
    '        <div class="num font-mono text-xl sm:text-2xl mt-0.5"\n'
    '             :style="`color: ${velocityColor(leaderboardRow?.velocity_score)}`"\n'
    '             x-text="fmtVelocity(leaderboardRow?.velocity_score)"></div>\n'
    '        <div class="text-[10px] mt-0.5" style="color: var(--text-3)"\n'
    '             x-show="leaderboardRow?.burst_30d" style="color: #c084fc">\u26a1 BURST</div>\n'
    '      </div>\n'
    '      <!-- ETFs ADDED 30d KPI -->\n'
    '      <div class="rounded-lg px-3 py-2.5 border" style="background: var(--surface); border-color: var(--border)"\n'
    '           x-tooltip="\'Net change in number of ETFs holding this name over the last 30 days.\'">\n'
    '        <div class="label">ETFs Added (30d)</div>\n'
    '        <div class="num font-mono text-xl sm:text-2xl mt-0.5"\n'
    '             :style="(leaderboardRow?.etf_count_delta_30d || 0) > 0 ? \'color: var(--up)\' : ((leaderboardRow?.etf_count_delta_30d || 0) < 0 ? \'color: var(--down)\' : \'\')"\n'
    '             x-text="(leaderboardRow?.etf_count_delta_30d || 0) > 0 ? \`+${leaderboardRow.etf_count_delta_30d}\` : (leaderboardRow?.etf_count_delta_30d ?? 0)"></div>\n'
    '      </div>\n'
    '    </section>\n'
)
if old1 in txt:
    txt = txt.replace(old1, new1, 1)
    print("Fix 1 (KPI strip): OK")
else:
    print("Fix 1 (KPI strip): NOT FOUND")

# ── 2. Add BURST chip to the flag banner ──────────────────────────────────────
old2 = (
    '      <!-- Any-new badge -->\n'
    '      <span x-show="leaderboardRow?.any_new" class="chip"\n'
    '            style="background: rgba(96,165,250,0.12); color: #60a5fa; border: 1px solid rgba(96,165,250,0.3)">NEW</span>\n'
    '    </div>\n'
)
new2 = (
    '      <!-- Any-new badge -->\n'
    '      <span x-show="leaderboardRow?.any_new" class="chip"\n'
    '            style="background: rgba(96,165,250,0.12); color: #60a5fa; border: 1px solid rgba(96,165,250,0.3)">NEW</span>\n'
    '      <!-- BURST badge -->\n'
    '      <span x-show="leaderboardRow?.burst_30d" class="chip"\n'
    '            :x-tooltip="`\'BURST move \u2014 global rank improved by \'+ leaderboardRow.global_rank_peak_30d +\'+ positions in last 30d. Best rank: #\'+ leaderboardRow.global_rank_best_30d +\'\u2019`"\n'
    '            style="background: rgba(168,85,247,0.12); color: #c084fc; border: 1px solid rgba(168,85,247,0.30)">\u26a1 BURST</span>\n'
    '      <!-- VELO badge -->\n'
    '      <span x-show="(leaderboardRow?.velocity_score || 0) >= 25 && !leaderboardRow?.burst_30d" class="chip"\n'
    '            style="background: rgba(34,211,238,0.10); color: var(--cyan); border: 1px solid rgba(34,211,238,0.25)">VELO</span>\n'
    '    </div>\n'
)
if old2 in txt:
    txt = txt.replace(old2, new2, 1)
    print("Fix 2 (BURST/VELO badge): OK")
else:
    print("Fix 2 (BURST/VELO badge): NOT FOUND")

# ── 3. Add fmtVelocity + velocityColor helpers to stockApp() ──────────────────
old3 = (
    '    // Formatting\n'
    '    fmtWeight(n) {'
)
new3 = (
    '    // Velocity helpers (mirrored from index.html)\n'
    '    fmtVelocity(v) {\n'
    '      if (v == null || !isFinite(v)) return \'\u2014\';\n'
    '      const n = Number(v);\n'
    '      if (Math.abs(n) < 0.5) return \'0\';\n'
    '      return (n > 0 ? \'+\' : \'\') + n.toFixed(1);\n'
    '    },\n'
    '    velocityColor(v) {\n'
    '      if (v == null || !isFinite(v)) return \'var(--text-3)\';\n'
    '      const n = Number(v);\n'
    '      if (n >= 15) return \'var(--up)\';\n'
    '      if (n >= 5)  return \'#86efac\';\n'
    '      if (n <= -15) return \'var(--down)\';\n'
    '      if (n <= -5)  return \'#fda4af\';\n'
    '      return \'var(--text-2)\';\n'
    '    },\n'
    '\n'
    '    // Formatting\n'
    '    fmtWeight(n) {'
)
if old3 in txt:
    txt = txt.replace(old3, new3, 1)
    print("Fix 3 (velocity helpers): OK")
else:
    print("Fix 3 (velocity helpers): NOT FOUND")

p.write_text(txt, encoding="utf-8")
print(f"\nstock.html done. {len(txt)} bytes (was {orig})")
