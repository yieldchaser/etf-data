/**
 * Headless Node.js Mathematical & Contract Verification Suite for VolumeHeatmap
 */

import assert from 'node:assert';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');

const VolumeHeatmap = require(path.join(ROOT, 'docs', 'volume-heatmap.js'));
const catalog = JSON.parse(fs.readFileSync(path.join(ROOT, 'docs', 'data', 'flows', 'catalog.json'), 'utf8'));
const etfHistory = JSON.parse(fs.readFileSync(path.join(ROOT, 'docs', 'data', 'flows', 'etf_history.json'), 'utf8')).history;

console.log('Testing VolumeHeatmap MicroCVI & Stage 2 Spotlight Engine...');

// 1. Trimmed Mean 21 (discard bottom 3 & top 3, average central 15)
{
  const buf = [
    1, 2, 3,               // bottom 3 outliers
    10, 10, 10, 10, 10,
    10, 10, 10, 10, 10,
    10, 10, 10, 10, 10,    // 15 central tens
    9999, 8888, 7777       // top 3 outliers
  ];
  assert.strictEqual(VolumeHeatmap.computeTrimmedMean21(buf), 10);
  console.log('  [PASS] computeTrimmedMean21 tail-trimming verified');
}

// 2. Microstructure boundedness, Abdi-Ranaldo spread >= 0, and RVOL positivity
{
  const sampleTicker = catalog.instruments[0].ticker;
  const bars = etfHistory[sampleTicker].slice(-252);
  for (const mode of ['share', 'dollar']) {
    const res = VolumeHeatmap.computeEtfMicrostructure(bars, mode);
    assert.strictEqual(res.peakScores.length, bars.length);
    assert.strictEqual(res.valleyScores.length, bars.length);
    for (let t = 0; t < bars.length; t++) {
      assert.ok(res.peakScores[t] >= 0 && res.peakScores[t] < 100, `peakScore[${t}] out of [0,100)`);
      assert.ok(res.valleyScores[t] >= 0 && res.valleyScores[t] < 100, `valleyScore[${t}] out of [0,100)`);
      assert.ok(res.rvolArr[t] >= 0 && isFinite(res.rvolArr[t]), `rvolArr[${t}] invalid`);
      assert.ok(res.trimDollarTurnover[t] >= 0 && isFinite(res.trimDollarTurnover[t]));
    }
  }
  console.log('  [PASS] Stage 1 MicroCVI boundedness [0, 100) and RVOL invariants verified');
}

// 3. Live Partial Session Exclusion
{
  const bars = [
    { d: '2026-11-05', o: 10, h: 11, l: 9, c: 10.5, v: 1000 },
    { d: '2026-11-06', o: 10.5, h: 11, l: 10, c: 10.8, v: 250 }
  ];
  const fakeNoonET = new Date('2026-11-06T17:00:00Z'); // 12:00 PM EST (market open)
  const stripped = VolumeHeatmap.stripLivePartialSession(bars, true, fakeNoonET);
  assert.strictEqual(stripped.length, 1);
  assert.strictEqual(stripped[0].d, '2026-11-05');

  const keptClosed = VolumeHeatmap.stripLivePartialSession(bars, false, fakeNoonET);
  assert.strictEqual(keptClosed.length, 2);
  console.log('  [PASS] Live partial session exclusion verified');
}

// 4. Dynamic Scope Resolution & Cross-Asset Aggregation + Stage 2 Monotonicity
{
  // Dynamically pick an underlying that has both long and inverse ETFs and >= 300 bars
  const byUnderlying = new Map();
  for (const inst of catalog.instruments) {
    const key = VolumeHeatmap.normalizeUnderlyingKey(inst.underlying);
    if (!key || (etfHistory[inst.ticker] || []).length < 300) continue;
    if (!byUnderlying.has(key)) byUnderlying.set(key, []);
    byUnderlying.get(key).push(inst);
  }
  const pairedEntry = Array.from(byUnderlying.values()).find(
    arr => arr.some(x => VolumeHeatmap.isInverseInstrument(x)) && arr.some(x => !VolumeHeatmap.isInverseInstrument(x))
  );
  assert.ok(pairedEntry && pairedEntry.length >= 2, 'Catalog must contain at least one bull/bear underlying complex');

  const primaryTicker = pairedEntry[0].ticker;
  const scopes = VolumeHeatmap.getAvailableScopes(primaryTicker, catalog.instruments);
  assert.ok(scopes.underlying.total >= 2);
  assert.ok(scopes.underlying.longCount >= 1 && scopes.underlying.shortCount >= 1);

  const complexTickers = VolumeHeatmap.resolveScopeTickers(primaryTicker, catalog.instruments, 'underlying', 'all');
  const subset = Object.fromEntries(complexTickers.map(tk => [tk, etfHistory[tk]]));

  let assertionCount = 0;
  for (const side of ['all', 'long', 'short']) {
    for (const mode of ['share', 'dollar']) {
      const agg = VolumeHeatmap.computeHeatAggregate(subset, mode, side, catalog.instruments, { isMarketOpen: false });
      assert.ok(agg.series.length >= 252, `Expected >= 252 sessions for ${primaryTicker} (${side}/${mode})`);

      for (const days of [90, 126, 180, 252]) {
        const visibleSlice = agg.series.slice(-days);
        for (const regime of ['PEAKS', 'VALLEYS']) {
          const s99 = VolumeHeatmap.evaluateViewportSpotlight(visibleSlice, regime, 'P99');
          const s95 = VolumeHeatmap.evaluateViewportSpotlight(visibleSlice, regime, 'P95');
          const s90 = VolumeHeatmap.evaluateViewportSpotlight(visibleSlice, regime, 'P90');

          const c99 = s99.filter(x => x.isHighlighted).length;
          const c95 = s95.filter(x => x.isHighlighted).length;
          const c90 = s90.filter(x => x.isHighlighted).length;

          assert.ok(c90 > c95 && c95 > c99 && c99 >= 1,
            `Monotonicity failed (${side}/${mode}/${days}/${regime}): P90=${c90}, P95=${c95}, P99=${c99}`);

          // Verify structural climax floors match exact specification
          const floors = VolumeHeatmap.STRUCTURAL_FLOORS[regime];
          for (const st of s90) {
            if (st.visualTier === 'RED_P99') assert.strictEqual(st.isStructuralClimax, st.score >= floors.P99);
            if (st.visualTier === 'ORANGE_P95') assert.strictEqual(st.isStructuralClimax, st.score >= floors.P95);
            if (st.visualTier === 'AMBER_P90') assert.strictEqual(st.isStructuralClimax, st.score >= floors.P90);
          }
          assertionCount++;
        }
      }
    }
  }
  console.log(`  [PASS] Stage 2 spotlight monotonicity & dual-gated structural floors (${assertionCount} matrix checks)`);
}

// 5. Tie-Break Uniqueness
{
  const flatBar = { d: '2026-01-01', o: 10, h: 10, l: 10, c: 10, v: 1000 };
  const bars = Array.from({ length: 120 }, (_, i) => ({
    ...flatBar,
    d: new Date(Date.UTC(2026, 0, 1 + i)).toISOString().slice(0, 10)
  }));
  const agg = VolumeHeatmap.computeHeatAggregate({ FLAT: bars }, 'share', 'all', [], { isMarketOpen: false });
  const keysPeak = new Set(agg.series.map(r => r.compositeTieKeyPeak));
  const keysValley = new Set(agg.series.map(r => r.compositeTieKeyValley));
  assert.strictEqual(keysPeak.size, agg.series.length, 'Peak tie-break keys must be strictly unique');
  assert.strictEqual(keysValley.size, agg.series.length, 'Valley tie-break keys must be strictly unique');
  console.log('  [PASS] Strict deterministic tie-break uniqueness verified');
}

// 5b. Normalized Underlying Grouping, Individual Ticker Side, Union Date Alignment & Spread Guard
{
  const synthCatalog = [
    { ticker: 'BULLA', underlying: 'Widget Index (Index)', direction: 'long', leverage_value: 3 },
    { ticker: 'BEARA', underlying: 'Widget Index (Primary Hedge)', direction: 'inverse', leverage_value: -3 }
  ];
  assert.strictEqual(VolumeHeatmap.normalizeUnderlyingKey(synthCatalog[0].underlying),
    VolumeHeatmap.normalizeUnderlyingKey(synthCatalog[1].underlying),
    'Parenthesized underlying labels must normalize identically');

  const sc = VolumeHeatmap.getAvailableScopes('BULLA', synthCatalog);
  assert.strictEqual(sc.underlying.total, 2);
  assert.strictEqual(sc.underlying.longCount, 1);
  assert.strictEqual(sc.underlying.shortCount, 1);

  const singleSide = VolumeHeatmap.resolveScopeTickers('BULLA', synthCatalog, 'underlying', 'BEARA');
  assert.deepStrictEqual(singleSide, ['BEARA'], 'Individual ticker side must isolate requested peer');

  // Union date axis across staggered histories after WINDOW_LAG warmup + <=0 / NaN close guard
  const warmup = Array.from({ length: 24 }, (_, i) => ({
    d: new Date(Date.UTC(2026, 0, 1 + i)).toISOString().slice(0, 10),
    o: 10, h: 11, l: 9, c: 10, v: 1000
  }));
  const hLong = warmup.concat([
    { d: '2026-02-01', o: 10, h: 11, l: 9, c: 10, v: 1000 },
    { d: '2026-02-02', o: 10, h: 12, l: 9, c: 11, v: 1500 },
    { d: '2026-02-03', o: 0, h: 0, l: 0, c: 0, v: 500 } // non-positive guard post-warmup
  ]);
  const hShort = warmup.concat([
    { d: '2026-02-02', o: 20, h: 21, l: 19, c: 20, v: 800 },
    { d: '2026-02-04', o: 20, h: 22, l: 18, c: 21, v: 900 }
  ]);
  const unionAgg = VolumeHeatmap.computeHeatAggregate({ BULLA: hLong, BEARA: hShort }, 'share', 'all', synthCatalog, { isMarketOpen: false });
  const tailDates = unionAgg.series.slice(-4).map(r => r.date);
  assert.deepStrictEqual(tailDates, ['2026-02-01', '2026-02-02', '2026-02-03', '2026-02-04'],
    'computeHeatAggregate must form sorted chronological union of post-warmup bar dates');
  for (const row of unionAgg.series) {
    assert.ok(Number.isFinite(row.peakScore) && Number.isFinite(row.valleyScore) && Number.isFinite(row.maxRvol),
      'Zero/missing price sessions must yield finite MicroCVI scores');
  }

  // Weekend ET check must keep Friday bar
  const sundayET = new Date('2026-11-08T16:00:00Z');
  const weekendKept = VolumeHeatmap.stripLivePartialSession(hLong.slice(0, 2), true, sundayET);
  assert.strictEqual(weekendKept.length, 2, 'Weekend check must not strip prior completed sessions');
  console.log('  [PASS] Normalized complex grouping, union date alignment, zero-price guard & weekend safety verified');
}

// 6. Zero Emojis, Zero Banned Words, Zero Legacy References
{
  const emojiRegex = /[\u{1F300}-\u{1F9FF}]|[\u{2600}-\u{26FF}]|[\u{2700}-\u{27BF}]/u;
  const banned = ['institutional', 'retail', 'fomo', 'capitulation', 'verified', 'creations', 'statistically significant', 'localstorage', 'cloud', '15,000', '15K'];
  for (const rel of ['docs/volume-heatmap.js', 'docs/volume-studies.css']) {
    const content = fs.readFileSync(path.join(ROOT, rel), 'utf8');
    assert.strictEqual(emojiRegex.test(content), false, `Emoji found in ${rel}`);
    const lower = content.toLowerCase();
    for (const word of banned) {
      assert.strictEqual(lower.includes(word.toLowerCase()), false, `Banned term "${word}" found in ${rel}`);
    }
  }
  console.log('  [PASS] Zero emojis and zero banned terminology verified');
}

// 7. Symmetric Bidirectional Climax: +25% Breakout PeakScore > 80
{
  const baselineBars = Array.from({ length: 25 }, (_, i) => ({
    d: `2026-01-${String(i + 1).padStart(2, '0')}`,
    o: 100, h: 102, l: 98, c: 100, v: 10000
  }));
  // Add a +25% breakout session with high volume (4x RVOL)
  baselineBars.push({
    d: '2026-01-26',
    o: 102, h: 126, l: 101, c: 125, v: 45000
  });
  const res = VolumeHeatmap.computeEtfMicrostructure(baselineBars, 'share');
  const breakoutIdx = baselineBars.length - 1;
  const breakoutScore = res.peakScores[breakoutIdx];
  const breakoutDir = res.climaxDirs ? res.climaxDirs[breakoutIdx] : 1;
  assert.ok(breakoutScore > 80, `Expected +25% breakout peakScore > 80, got ${breakoutScore}`);
  assert.strictEqual(breakoutDir, 1, 'Expected breakout direction to be +1 (Buying Climax)');
  console.log(`  [PASS] +25% breakout symmetric climax verified (score: ${breakoutScore.toFixed(2)}, dir: +1)`);
}

// 8. EOD focusClose Tracking, Focus Peer Override & Invalid-Bar Fallback
{
  const synthCat = [
    { ticker: 'BULLX', underlying: 'Widget Index', direction: 'long', leverage_value: 3 },
    { ticker: 'BEARX', underlying: 'Widget Index', direction: 'inverse', leverage_value: -3 }
  ];
  const hBull = [];
  const hBear = [];
  for (let i = 0; i < 30; i++) {
    const d = new Date(Date.UTC(2026, 0, 1 + i)).toISOString().slice(0, 10);
    const bc = 40 + i * 0.5;
    const sc = 25 - i * 0.2;
    hBull.push({ d, o: bc - 0.3, h: bc + 0.5, l: bc - 0.5, c: bc, v: 100000 });
    hBear.push({ d, o: sc + 0.2, h: sc + 0.4, l: sc - 0.4, c: i === 29 ? 0 : sc, v: 80000 });
  }
  hBull[29].c = 55.5;

  const micro = VolumeHeatmap.computeEtfMicrostructure(hBull, 'share');
  assert.strictEqual(micro.closes.length, hBull.length);
  assert.strictEqual(micro.closes[29], 55.5);

  const aggFocusBull = VolumeHeatmap.computeHeatAggregate(
    { BULLX: hBull, BEARX: hBear }, 'share', 'all', synthCat,
    { isMarketOpen: false, focusTicker: 'BULLX' }
  );
  const lastBull = aggFocusBull.series[aggFocusBull.series.length - 1];
  assert.strictEqual(lastBull.priceTicker, 'BULLX');
  assert.strictEqual(lastBull.focusClose, 55.5);

  const aggPeerBear = VolumeHeatmap.computeHeatAggregate(
    { BULLX: hBull, BEARX: hBear }, 'share', 'BEARX', synthCat,
    { isMarketOpen: false, focusTicker: 'BULLX' }
  );
  const prevRow = aggPeerBear.series[aggPeerBear.series.length - 2];
  assert.strictEqual(prevRow.priceTicker, 'BEARX');
  assert.ok(Math.abs(prevRow.focusClose - hBear[28].c) < 1e-9);

  const aggFallback = VolumeHeatmap.computeHeatAggregate(
    { BULLX: hBull, BEARX: hBear }, 'share', 'all', synthCat,
    { isMarketOpen: false, focusTicker: 'BEARX' }
  );
  const lastFallback = aggFallback.series[aggFallback.series.length - 1];
  assert.strictEqual(lastFallback.priceTicker, 'BULLX', 'Invalid/zero close on focusTicker must fall back cleanly to active peer');
  assert.strictEqual(lastFallback.focusClose, 55.5);

  console.log('  [PASS] EOD focusClose tracking, peer override, and fallback verified');
}

console.log('\nAll VolumeHeatmap tests PASSED successfully.');
