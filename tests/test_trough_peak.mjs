/**
 * Headless Node.js Mathematical & Contract Verification Suite for TroughPeak
 */

import assert from 'node:assert';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');

const TroughPeak = require(path.join(ROOT, 'docs', 'trough-peak.js'));
const catalog = JSON.parse(fs.readFileSync(path.join(ROOT, 'docs', 'data', 'flows', 'catalog.json'), 'utf8'));
const etfHistory = JSON.parse(fs.readFileSync(path.join(ROOT, 'docs', 'data', 'flows', 'etf_history.json'), 'utf8')).history;

console.log('Testing TroughPeak ZigZag Cycle & Underwater Drawdown Engine...');

// 1. ZigZag detectCycles synthetic & threshold monotonicity
{
  const dates = [
    '2025-01-01', '2025-01-02', '2025-01-03', '2025-01-06',
    '2025-01-07', '2025-01-08', '2025-01-09', '2025-01-10'
  ];
  const prices = [100, 60, 120, 70, 210, 100, 150, 90];
  const cycles30 = TroughPeak.detectCycles(dates, prices, 30);
  // Trough 60 -> Peak 120 (+100%), Trough 70 -> Peak 210 (+200%), Trough 100 -> Peak 150 (+50%)
  assert.strictEqual(cycles30.length, 3);
  assert.strictEqual(Math.round(cycles30[0].gain), 100);
  assert.strictEqual(Math.round(cycles30[1].gain), 200);
  assert.strictEqual(Math.round(cycles30[2].gain), 50);

  // Real ETF series monotonicity: higher threshold yields <= cycle count
  const longInst = catalog.instruments.find(x => (etfHistory[x.ticker] || []).length >= 1000);
  assert.ok(longInst, 'Expected at least one long-history ETF in catalog');
  const bars = etfHistory[longInst.ticker];
  const dArr = bars.map(b => b.d);
  const cArr = bars.map(b => b.c);
  const c15 = TroughPeak.detectCycles(dArr, cArr, 15);
  const c30 = TroughPeak.detectCycles(dArr, cArr, 30);
  const c100 = TroughPeak.detectCycles(dArr, cArr, 100);
  assert.ok(c15.length >= c30.length && c30.length >= c100.length);
  console.log('  [PASS] ZigZag detectCycles precision & threshold monotonicity verified');
}

// 2. computeRollingLookbackDD (364d & 730d) vs brute-force O(N^2) reference + leap year
{
  const baseMs = Date.UTC(2023, 0, 1);
  const dayMs = 86400 * 1000;
  const dates = [];
  const closes = [];
  for (let i = 0; i < 500; i++) {
    dates.push(new Date(baseMs + i * dayMs).toISOString().slice(0, 10));
    if (i === 100) closes.push(100);
    else if (i < 100) closes.push(10 + i * 0.1);
    else if (i <= 464) closes.push(40);
    else closes.push(50);
  }

  const res = TroughPeak.computeRolling52WDD(dates, closes);
  assert.strictEqual(res.peaks[100], 100);
  assert.strictEqual(res.dd[100], 0);
  assert.strictEqual(Math.round(res.dd[200]), -60);
  assert.strictEqual(res.peaks[464], 100);
  assert.strictEqual(res.peaks[466], 50);
  assert.strictEqual(res.dd[466], 0);

  // Brute-force O(N^2) parity on real ETF series
  const sampleInst = catalog.instruments.find(x => (etfHistory[x.ticker] || []).length >= 800);
  const realBars = etfHistory[sampleInst.ticker].slice(-800);
  const rDates = realBars.map(b => b.d);
  const rCloses = realBars.map(b => b.c);
  for (const lookback of [364, 730]) {
    const fast = TroughPeak.computeRollingLookbackDD(rDates, rCloses, lookback);
    const WIN = lookback * 86400 * 1000;
    for (let i = 0; i < rCloses.length; i++) {
      let naivePeak = -Infinity;
      let naiveIdx = -1;
      for (let j = 0; j <= i; j++) {
        if (fast.ts[i] - fast.ts[j] <= WIN && rCloses[j] >= naivePeak) {
          naivePeak = rCloses[j];
          naiveIdx = j;
        }
      }
      assert.strictEqual(fast.peaks[i], naivePeak, `Peak mismatch at ${i} (lookback=${lookback})`);
      assert.strictEqual(fast.peakIndices[i], naiveIdx, `Index mismatch at ${i} (lookback=${lookback})`);
      assert.ok(fast.dd[i] <= 1e-9, `Drawdown must be non-positive at ${i}`);
    }
  }
  console.log('  [PASS] O(N) monotonic deque 52W & 2Y drawdown matches O(N^2) oracle exactly');
}

// 3. computeATHDD & computeUnderwaterMetrics
{
  const dates = ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05'];
  const closes = [100, 80, 150, 75, 150];
  const ath = TroughPeak.computeATHDD(dates, closes);
  assert.deepStrictEqual(ath.peaks, [100, 100, 150, 150, 150]);
  assert.deepStrictEqual(ath.dd, [0, -20, 0, -50, 0]);

  const pts = [
    { t: 1000000, dd: 0, close: 100, peak: 100, peakTs: 1000000 },
    { t: 1086400000, dd: -20, close: 80, peak: 100, peakTs: 1000000 },
    { t: 1259200000, dd: -80, close: 20, peak: 100, peakTs: 1000000 },
    { t: 1345600000, dd: -40, close: 60, peak: 100, peakTs: 1000000 }
  ];
  const m = TroughPeak.computeUnderwaterMetrics(pts);
  assert.strictEqual(m.currentDD, -40);
  assert.strictEqual(m.maxDD, -80);
  assert.strictEqual(m.daysUnderwater, 16);
  assert.ok(Math.abs(m.breakEvenGain - (40 / 60 * 100)) < 1e-6);
  assert.strictEqual(m.maxDDBreakEvenGain, 400);
  assert.strictEqual(m.recoveryFromTrough, 200);
  console.log('  [PASS] ATH drawdown and underwater break-even metrics verified');
}

// 4. decimateDDPtsPrecise trough preservation
{
  const pts = [];
  for (let i = 0; i < 1200; i++) {
    pts.push({
      t: 1000 + i * 10,
      dd: -Math.abs(Math.sin(i / 11) * 75),
      close: 50 + Math.sin(i / 11) * 20
    });
  }
  const chartW = 300;
  const xOf = t => ((t - 1000) / (pts[pts.length - 1].t - 1000)) * chartW;
  const dec = TroughPeak.decimateDDPtsPrecise(pts, xOf, chartW);
  assert.ok(dec.length < pts.length);
  assert.strictEqual(dec[0].t, pts[0].t);
  assert.strictEqual(dec[dec.length - 1].t, pts[pts.length - 1].t);
  const origMin = Math.min(...pts.map(p => p.dd));
  const decMin = Math.min(...dec.map(p => p.dd));
  assert.ok(Math.abs(origMin - decMin) < 1e-9, 'Extrema decimation must preserve worst drawdown trough');
  console.log('  [PASS] Extrema-preserving drawdown decimation verified');
}

// 5. 2-Point Drag Measurement Math (forward & reverse index ordering)
{
  const dates = ['2026-01-01', '2026-01-11', '2026-02-01'];
  const closes = [20, 25, 30];
  const vols = [500, 1200, 800];
  const fwd = TroughPeak.computeDragMeasurement(dates, closes, vols, 0, 2);
  const rev = TroughPeak.computeDragMeasurement(dates, closes, vols, 2, 0);
  assert.deepStrictEqual(fwd, rev);
  assert.strictEqual(fwd.diff, 10);
  assert.strictEqual(fwd.pct, 50);
  assert.strictEqual(fwd.bars, 2);
  assert.strictEqual(fwd.calDays, 31);
  assert.strictEqual(fwd.volMin, 500);
  assert.strictEqual(fwd.volMax, 1200);
  assert.ok(fwd.annPct > 0);
  assert.strictEqual(TroughPeak.computeDragMeasurement(dates, closes, vols, 1, 1), null);
  console.log('  [PASS] 2-point drag measurement math verified');
}

// 6. Dynamic Comparison Scope Resolution, Normalized Complexes, Threshold Presets & Color Assignment
{
  // Verify threshold presets include 5%, 10%, and canonical 30% default
  assert.ok(TroughPeak.THRESHOLD_PRESETS.includes(5), 'THRESHOLD_PRESETS must include 5%');
  assert.ok(TroughPeak.THRESHOLD_PRESETS.includes(10), 'THRESHOLD_PRESETS must include 10%');
  assert.strictEqual(TroughPeak.detectCycles(['2025-01-01', '2025-01-02', '2025-01-03'], [100, 100, 100], 0).length, 0,
    'Flat price run at threshold=0 must produce 0 cycles');

  const byUnderlying = new Map();
  for (const inst of catalog.instruments) {
    const key = TroughPeak.normalizeUnderlyingKey(inst.underlying);
    if (!key) continue;
    if (!byUnderlying.has(key)) byUnderlying.set(key, []);
    byUnderlying.get(key).push(inst);
  }
  const pairedGroup = Array.from(byUnderlying.values()).find(
    arr => arr.some(x => TroughPeak.isInverseInstrument(x)) && arr.some(x => !TroughPeak.isInverseInstrument(x))
  );
  const multiComplex = Array.from(byUnderlying.values()).find(arr => arr.length >= 3);
  const unpairedInst = Array.from(byUnderlying.values()).find(arr => arr.length === 1)[0];

  assert.ok(pairedGroup && pairedGroup.length >= 2, 'Normalized catalog must resolve bull/bear pair');
  const pairList = TroughPeak.resolveComparisonSet(pairedGroup[0].ticker, catalog.instruments, 'pair');
  assert.strictEqual(pairList.length, 2, 'Paired underlying must resolve to 2 instruments');

  const complexList = TroughPeak.resolveComparisonSet(multiComplex[0].ticker, catalog.instruments, 'underlying');
  assert.strictEqual(complexList.length, multiComplex.length, 'Underlying complex must resolve all normalized siblings');

  const unpairedPair = TroughPeak.resolveComparisonSet(unpairedInst.ticker, catalog.instruments, 'pair');
  assert.deepStrictEqual(unpairedPair, [unpairedInst.ticker], 'Unpaired instrument must fall back cleanly to 1 card');

  // Verify category scope resolves >6 members cleanly (eliminated legacy MAX_COMPARE_PEERS=6 cap)
  const byCat = new Map();
  for (const inst of catalog.instruments) {
    if (!inst.category) continue;
    if (!byCat.has(inst.category)) byCat.set(inst.category, []);
    byCat.get(inst.category).push(inst);
  }
  const largeCat = Array.from(byCat.values()).find(arr => arr.length > 6);
  assert.ok(largeCat && largeCat.length > 6, 'Catalog must have at least one category with >6 peers');
  const catList = TroughPeak.resolveComparisonSet(largeCat[0].ticker, catalog.instruments, 'category');
  assert.strictEqual(catList.length, largeCat.length,
    `Category peers must not be truncated at 6 (got ${catList.length}, expected ${largeCat.length})`);

  const manyColors = TroughPeak.assignSeriesColors(catList, catalog.instruments);
  const uniqueColors = new Set(Object.values(manyColors));
  assert.strictEqual(uniqueColors.size, catList.length, 'All peers in >6 category must get distinct hex colors');
  for (const hex of uniqueColors) {
    assert.match(hex, /^#[0-9a-f]{6}$/i, `Color ${hex} must be valid 6-digit hex`);
  }

  const pairColors = TroughPeak.assignSeriesColors(pairList, catalog.instruments);
  assert.notStrictEqual(pairColors[pairList[0]], pairColors[pairList[1]], 'Long and inverse pair colors must contrast');
  console.log(`  [PASS] Dynamic comparison scope (${catList.length} category peers, normalized complexes, distinct hex palette) verified`);
}

// 7. Zero Emojis & Zero Banned Words
{
  const emojiRegex = /[\u{1F300}-\u{1F9FF}]|[\u{2600}-\u{26FF}]|[\u{2700}-\u{27BF}]/u;
  const banned = ['institutional', 'retail', 'fomo', 'capitulation', 'verified', 'creations', 'statistically significant', 'localstorage', 'cloud', '15,000', '15K'];
  for (const rel of ['docs/trough-peak.js', 'docs/trough-peak.css']) {
    const content = fs.readFileSync(path.join(ROOT, rel), 'utf8');
    assert.strictEqual(emojiRegex.test(content), false, `Emoji found in ${rel}`);
    const lower = content.toLowerCase();
    for (const word of banned) {
      assert.strictEqual(lower.includes(word.toLowerCase()), false, `Banned term "${word}" found in ${rel}`);
    }
  }
  console.log('  [PASS] Zero emojis and zero banned terminology verified');
}

console.log('\nAll TroughPeak tests PASSED successfully.');
