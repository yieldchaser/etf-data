"""Patch stock.html for Phase 2.6 Part A: shared time axis on rank history chart."""
import re, pathlib

p = pathlib.Path("docs/stock.html")
txt = p.read_text(encoding="utf-8")
orig_len = len(txt)

# ── Fix 1: @mousemove handler ────────────────────────────────────────────────
old1 = (
    "             const svg = $el.querySelector('svg');\n"
    "             if (!svg || !rankLines.length) return;\n"
    "             const rect = svg.getBoundingClientRect();\n"
    "             const rx = $event.clientX - rect.left;\n"
    "             const maxLen = Math.max(...rankLines.map(l => l.series.length));\n"
    "             if (maxLen < 2) return;\n"
    "             const idx = Math.round((rx / rect.width) * (maxLen - 1));\n"
    "             rci = Math.max(0, Math.min(maxLen - 1, idx));\n"
    "             rcx = rect.width > 0 ? (rci / (maxLen - 1)) * rect.width : null;\n"
)
new1 = (
    "             const svg = $el.querySelector('svg');\n"
    "             if (!svg || !rankSharedDates.length) return;\n"
    "             const rect = svg.getBoundingClientRect();\n"
    "             const rx = $event.clientX - rect.left;\n"
    "             const n = rankSharedDates.length;\n"
    "             if (n < 2) return;\n"
    "             rci = Math.max(0, Math.min(n - 1, Math.round((rx / rect.width) * (n - 1))));\n"
    "             rcx = (rci / (n - 1)) * rect.width;\n"
)
if old1 in txt:
    txt = txt.replace(old1, new1, 1)
    print("Fix 1 (mousemove): OK")
else:
    print("Fix 1 (mousemove): NOT FOUND")

# ── Fix 2: replace the old crosshair-dot template + old crosshair-vertical ───
old2 = (
    "              <!-- Crosshair dot if hovered -->\n"
    "              <template x-if=\"rci !== null && rci < line.series.length\">\n"
    "                <circle :cx=\"(rci / Math.max(1, line.series.length - 1)) * 1000\"\n"
    "                        :cy=\"line.series[rci].y\"\n"
    "                        r=\"3\" :fill=\"line.color\" stroke=\"#000\" stroke-width=\"1.5\"/>\n"
    "              </template>\n"
    "            </g>\n"
    "          </template>\n"
    "          <!-- Crosshair vertical -->\n"
    "          <template x-if=\"rci !== null\">\n"
    "            <line class=\"crosshair-line\"\n"
    "                  :x1=\"(rci / Math.max(1, Math.max(...rankLines.map(l => l.series.length)) - 1)) * 1000\"\n"
    "                  y1=\"0\"\n"
    "                  :x2=\"(rci / Math.max(1, Math.max(...rankLines.map(l => l.series.length)) - 1)) * 1000\"\n"
    "                  y2=\"220\"/>\n"
    "          </template>\n"
    "        </svg>\n"
)
new2 = (
    "            </g>\n"
    "          </template>\n"
    "          <!-- Crosshair dots — one per ETF, shown only if ETF has data on the hovered date -->\n"
    "          <template x-if=\"rci !== null\">\n"
    "            <g>\n"
    "              <template x-for=\"line in rankLines\" :key=\"line.etf\">\n"
    "                <template x-for=\"pt in [line.series.find(p => p.d === rankSharedDates[rci])]\" :key=\"pt?.d\">\n"
    "                  <circle x-show=\"pt\"\n"
    "                          :cx=\"(rci / Math.max(1, rankSharedDates.length - 1)) * 1000\"\n"
    "                          :cy=\"pt?.y\" r=\"3\" :fill=\"line.color\" stroke=\"#000\" stroke-width=\"1.5\"/>\n"
    "                </template>\n"
    "              </template>\n"
    "            </g>\n"
    "          </template>\n"
    "          <!-- Crosshair vertical -->\n"
    "          <template x-if=\"rci !== null\">\n"
    "            <line class=\"crosshair-line\"\n"
    "                  :x1=\"(rci / Math.max(1, rankSharedDates.length - 1)) * 1000\"\n"
    "                  y1=\"0\"\n"
    "                  :x2=\"(rci / Math.max(1, rankSharedDates.length - 1)) * 1000\"\n"
    "                  y2=\"220\"/>\n"
    "          </template>\n"
    "          <!-- X-axis date labels -->\n"
    "          <template x-if=\"rankSharedDates.length >= 2\">\n"
    "            <g style=\"font-family: ui-monospace; font-size: 9px\" fill=\"var(--text-3)\">\n"
    "              <text x=\"0\"    y=\"232\" text-anchor=\"start\"  x-text=\"rankSharedDates[0]\"></text>\n"
    "              <text x=\"500\"  y=\"232\" text-anchor=\"middle\" x-text=\"rankSharedDates[Math.floor(rankSharedDates.length / 2)]\"></text>\n"
    "              <text x=\"1000\" y=\"232\" text-anchor=\"end\"   x-text=\"rankSharedDates[rankSharedDates.length - 1]\"></text>\n"
    "            </g>\n"
    "          </template>\n"
    "        </svg>\n"
)
if old2 in txt:
    txt = txt.replace(old2, new2, 1)
    print("Fix 2 (crosshair dots + vertical + x-axis labels): OK")
else:
    print("Fix 2: NOT FOUND")
    # Debug: find where the crosshair dot comment is
    idx = txt.find("Crosshair dot if hovered")
    if idx >= 0:
        print("  (found 'Crosshair dot' at char", idx, "- showing context)")
        print(repr(txt[idx:idx+600]))

# ── Fix 3: Hover tooltip — use shared date + dash for missing ETFs ──────────
old3 = (
    "        <!-- Hover tooltip: show all ETF ranks at hovered index -->\n"
    "        <div x-show=\"rci !== null\" class=\"chart-tooltip\" :style=\"`top: 4px; left: ${Math.min((rcx || 0) + 8, 300)}px`\">\n"
    "          <div class=\"font-mono text-[10px] mb-1\" style=\"color: var(--text-3)\"\n"
    "               x-text=\"rankLines[0]?.series[rci]?.d || ''\"\n"
    "          </div>\n"
    "          <template x-for=\"line in rankLines\" :key=\"line.etf\">\n"
    "            <div x-show=\"rci < line.series.length\" class=\"flex items-center gap-2 text-[10px]\">\n"
    "              <span class=\"font-mono\" :style=\"`color: ${line.color}`\" x-text=\"line.etf\"></span>\n"
    "              <span class=\"num font-mono\" style=\"color: var(--text-2)\" x-text=\"'#' + line.series[rci]?.r\"></span>\n"
    "            </div>\n"
    "          </template>\n"
    "        </div>\n"
)
# The original had a slightly different format; let's search flexibly
idx3 = txt.find("<!-- Hover tooltip: show all ETF ranks at hovered index -->")
if idx3 >= 0:
    end3 = txt.find("<!-- Legend row -->", idx3)
    old3_actual = txt[idx3:end3]
    new3 = (
        "        <!-- Hover tooltip: shared date + per-ETF rank or dash -->\n"
        "        <div x-show=\"rci !== null\" class=\"chart-tooltip\" :style=\"`top: 4px; left: ${Math.min((rcx || 0) + 8, 300)}px`\">\n"
        "          <div class=\"font-mono text-[10px] mb-1\" style=\"color: var(--text-3)\"\n"
        "               x-text=\"rankSharedDates[rci] || ''\"></div>\n"
        "          <template x-for=\"line in rankLines\" :key=\"line.etf\">\n"
        "            <div class=\"flex items-center gap-2 text-[10px]\">\n"
        "              <span class=\"font-mono\" :style=\"`color: ${line.color}`\" x-text=\"line.etf\"></span>\n"
        "              <span class=\"num font-mono\" style=\"color: var(--text-2)\"\n"
        "                    x-text=\"(() => { const p = line.series.find(p => p.d === rankSharedDates[rci]); return p ? '#' + p.r : '\\u2014'; })()\"></span>\n"
        "            </div>\n"
        "          </template>\n"
        "        </div>\n"
    )
    txt = txt[:idx3] + new3 + txt[end3:]
    print("Fix 3 (tooltip): OK")
else:
    print("Fix 3 (tooltip): NOT FOUND")

# ── Fix 4: Replace rankLines getter + add rankSharedDates getter ──────────────
old4_start = "    // Rank History chart\n    get rankLines() {"
old4_end = "      return lines;\n    },\n"
idx4s = txt.find(old4_start)
if idx4s >= 0:
    idx4e = txt.find(old4_end, idx4s) + len(old4_end)
    new4 = (
        "    // Rank History chart — shared time axis so every ETF's vertices are calendar-aligned\n"
        "    get rankLines() {\n"
        "      const hh = this.holdingsHistory;\n"
        "      if (!hh || !Object.keys(hh).length) return [];\n"
        "\n"
        "      // 1. Build shared time axis — union of all dates, sorted ascending\n"
        "      const dateSet = new Set();\n"
        "      for (const series of Object.values(hh)) {\n"
        "        for (const p of series) dateSet.add(p.d);\n"
        "      }\n"
        "      const allDates = Array.from(dateSet).sort();\n"
        "      if (allDates.length < 2) return [];\n"
        "      const dateToX = {};\n"
        "      allDates.forEach((d, i) => { dateToX[d] = (i / (allDates.length - 1)) * 1000; });\n"
        "\n"
        "      // 2. Y scale from global rank range across all ETFs\n"
        "      const allRanks = Object.values(hh).flat().map(p => p.r).filter(Boolean);\n"
        "      if (!allRanks.length) return [];\n"
        "      const minRank = Math.min(...allRanks);\n"
        "      const maxRank = Math.max(...allRanks);\n"
        "      const rangeFloor = Math.max(maxRank, minRank + 4); // min 4-rank spread\n"
        "      const yRange = Math.max(1, rangeFloor - minRank);\n"
        "      const yTop = 10, yBot = 210;\n"
        "      const rankToY = r => yTop + ((r - minRank) / yRange) * (yBot - yTop);\n"
        "\n"
        "      // 3. Build one polyline per ETF, vertices on the shared axis\n"
        "      const lines = [];\n"
        "      for (const etf of Object.keys(hh).sort()) {\n"
        "        const rows = hh[etf];\n"
        "        if (!rows || !rows.length) continue;\n"
        "        const tier = this.tierMap[etf] || 'Blob';\n"
        "        const color = this.tierLineColor(tier);\n"
        "        const series = rows\n"
        "          .filter(p => dateToX[p.d] !== undefined)\n"
        "          .map(p => ({ r: p.r, d: p.d, x: dateToX[p.d], y: rankToY(p.r) }));\n"
        "        if (!series.length) continue;\n"
        "        const path = series\n"
        "          .map((p, i) => `${i === 0 ? 'M' : 'L'}${p.x.toFixed(1)},${p.y.toFixed(1)}`)\n"
        "          .join(' ');\n"
        "        lines.push({\n"
        "          etf, path, color, tier, series,\n"
        "          endX: series[series.length - 1].x,\n"
        "          endY: series[series.length - 1].y,\n"
        "        });\n"
        "      }\n"
        "      // Attach shared axis to first line for template access\n"
        "      if (lines.length) lines[0].sharedDates = allDates;\n"
        "      return lines;\n"
        "    },\n"
        "\n"
        "    get rankSharedDates() {\n"
        "      return this.rankLines[0]?.sharedDates || [];\n"
        "    },\n"
    )
    txt = txt[:idx4s] + new4 + txt[idx4e:]
    print("Fix 4 (rankLines + rankSharedDates): OK")
else:
    print("Fix 4: NOT FOUND, searching for context...")
    idx_ctx = txt.find("// Rank History chart")
    print(repr(txt[idx_ctx:idx_ctx+200]))

p.write_text(txt, encoding="utf-8")
print(f"\nDone. File size: {len(txt)} bytes (was {orig_len})")
