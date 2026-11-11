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
          const s99 = VolumeHeatmap.evaluateCausalSpotlight(visibleSlice, regime, 'P99');
          const s95 = VolumeHeatmap.evaluateCausalSpotlight(visibleSlice, regime, 'P95');
          const s90 = VolumeHeatmap.evaluateCausalSpotlight(visibleSlice, regime, 'P90');

          const c99 = s99.filter(x => x.isHighlighted).length;
          const c95 = s95.filter(x => x.isHighlighted).length;
          const c90 = s90.filter(x => x.isHighlighted).length;

          assert.ok(c90 >= c95 && c95 >= c99 && c99 >= 0,
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
      // Backwards-compatible alias check + 90D vs 252D horizon invariance on overlapping dates
      assert.strictEqual(VolumeHeatmap.evaluateViewportSpotlight, VolumeHeatmap.evaluateCausalSpotlight);
      for (const regime of ['PEAKS', 'VALLEYS']) {
        const st90 = VolumeHeatmap.evaluateCausalSpotlight(agg.series.slice(-90), regime, 'P95');
        const st252Tail = VolumeHeatmap.evaluateCausalSpotlight(agg.series.slice(-252), regime, 'P95').slice(-90);
        assert.deepStrictEqual(st90, st252Tail, `90D vs 252D horizon toggle altered historical classifications (${side}/${mode}/${regime})`);
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
  const banned = ['institutional', 'retail', 'fomo', 'capitulation', 'verified', 'creations', 'statistically significant', 'localstorage', 'cloud', '15,000', '15K', 'bloodbath'];
  for (const rel of ['docs/volume-heatmap.js', 'docs/volume-studies.css']) {
    const content = fs.readFileSync(path.join(ROOT, rel), 'utf8');
    assert.strictEqual(emojiRegex.test(content), false, `Emoji found in ${rel}`);
    const lower = content.toLowerCase();
    for (const word of banned) {
      assert.strictEqual(lower.includes(word.toLowerCase()), false, `Banned term "${word}" found in ${rel}`);
    }
  }
  const marketsHtml = fs.readFileSync(path.join(ROOT, 'docs/markets.html'), 'utf8');
  const hmStart = marketsHtml.indexOf('id="vs-heatmap-container"');
  const hmEnd = marketsHtml.indexOf('</details>', hmStart);
  assert.ok(hmStart > 0 && hmEnd > hmStart, 'markets.html must contain vs-heatmap-container');
  const hmSnippet = marketsHtml.slice(hmStart, hmEnd);
  assert.strictEqual(emojiRegex.test(hmSnippet), false, 'Emoji found in heatmap HTML snippet');
  for (const word of banned) {
    assert.strictEqual(hmSnippet.toLowerCase().includes(word.toLowerCase()), false, `Banned term "${word}" found in heatmap HTML`);
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

// 9. Monotonic Deque rollingWindowMax & rollingWindowMin vs O(N*W) Brute-Force Oracle
{
  const oracleMax = (arr, w) => arr.map((_, i) => {
    let best = -Infinity;
    for (let j = Math.max(0, i - w + 1); j <= i; j++) {
      const v = Number(arr[j]);
      if (Number.isFinite(v) && v > 0 && v > best) best = v;
    }
    return best === -Infinity ? null : best;
  });
  const oracleMin = (arr, w) => arr.map((_, i) => {
    let best = Infinity;
    for (let j = Math.max(0, i - w + 1); j <= i; j++) {
      const v = Number(arr[j]);
      if (Number.isFinite(v) && v > 0 && v < best) best = v;
    }
    return best === Infinity ? null : best;
  });

  // Deterministic pseudo-random wave with invalid 0 / NaN gaps
  let seed = 42;
  const randSeries = Array.from({ length: 320 }, (_, i) => {
    if (i === 0 || i === 77 || i === 190) return 0;
    if (i === 191) return NaN;
    seed = (seed * 1664525 + 1013904223) >>> 0;
    return 10 + (seed % 9000) / 100;
  });

  for (const w of [1, 5, 21, 63, 252]) {
    assert.deepStrictEqual(VolumeHeatmap.rollingWindowMax(randSeries, w), oracleMax(randSeries, w), `rollingWindowMax mismatch at w=${w}`);
    assert.deepStrictEqual(VolumeHeatmap.rollingWindowMin(randSeries, w), oracleMin(randSeries, w), `rollingWindowMin mismatch at w=${w}`);
  }
  assert.deepStrictEqual(VolumeHeatmap.rollingWindowMax([], 63), []);
  console.log('  [PASS] Monotonic deque rollingWindowMax/Min exact O(N*W) oracle parity verified');
}

// 10. Pre-Slice Causal 252D Context, TRI tc Drawdown Immunity & Short-History Labels
{
  // Build 1050 bars: maxBars=756 starts at global index 294; index 294..314 warmup -> first series row is global 315.
  // Place a major peak at global index 94 (within 252 bars BEFORE global 315, i.e., before the 756-bar cut!)
  const nTotal = 1050;
  const paddedBars = [];
  for (let i = 0; i < nTotal; i++) {
    const d = new Date(Date.UTC(2022, 0, 1 + i)).toISOString().slice(0, 10);
    const px = i === 94 ? 200 : 100;
    paddedBars.push({ d, o: px, h: px * 1.01, l: px * 0.99, c: px, tc: px, v: 50000 });
  }
  const agg = VolumeHeatmap.computeHeatAggregate({ PADX: paddedBars }, 'share', 'all', [], { isMarketOpen: false });
  assert.ok(agg.series.length > 0);
  const firstRec = agg.series[0]; // corresponds to global index 1050 - 756 + 21 = 315 (distance from 94 is 221 <= 252)
  assert.ok(firstRec.focusCtx && firstRec.focusCtx.dd252Pct != null, 'First heatmap row must carry focusCtx.dd252Pct');
  assert.ok(Math.abs(firstRec.focusCtx.dd252Pct - (-50.0)) < 1e-6, `Expected -50.0% pre-slice 252D DD, got ${firstRec.focusCtx.dd252Pct}`);
  assert.strictEqual(firstRec.focusCtx.win252, 252);

  // Real dividend ETF (TLT): headline close tracks c, drawdowns track tc (no phantom dividend drop)
  const tltBars = etfHistory.TLT || etfHistory[Object.keys(etfHistory)[0]];
  const tltCtxMap = VolumeHeatmap.computeTickerCloseContext(tltBars);
  const lastTltBar = tltBars[tltBars.length - 1];
  const lastTltCtx = tltCtxMap.get(lastTltBar.d);
  assert.strictEqual(lastTltCtx.close, lastTltBar.c);
  assert.strictEqual(lastTltCtx.trClose, lastTltBar.tc);
  assert.ok(lastTltCtx.dd63Pct <= 0 && lastTltCtx.dd252Pct <= 0);
  assert.ok(lastTltCtx.ru63Pct >= 0 && lastTltCtx.ru252Pct >= 0);

  // Dynamically pick any short-history ETF (< 220 bars) to check win252 < 252 accuracy
  const shortEntry = Object.entries(etfHistory).find(([, b]) => Array.isArray(b) && b.length >= 63 && b.length < 220);
  assert.ok(shortEntry, 'Dataset should contain at least one short-history ETF for window-length guard test');
  const shortMap = VolumeHeatmap.computeTickerCloseContext(shortEntry[1]);
  const lastShortCtx = shortMap.get(shortEntry[1][shortEntry[1].length - 1].d);
  assert.strictEqual(lastShortCtx.win252, shortEntry[1].length);
  console.log('  [PASS] Pre-slice 252D causal padding, TRI context & short-history win252 verified');
}

// 11. Causal No-Lookahead Invariance for computeCausalDailySigmaSeries
{
  const makeBars = () => Array.from({ length: 50 }, (_, i) => {
    const px = 50 + Math.sin(i * 0.4) * 2;
    return {
      d: new Date(Date.UTC(2026, 0, 1 + i)).toISOString().slice(0, 10),
      o: px * 0.995, h: px * 1.015, l: px * 0.985, c: px, tc: px, v: 10000
    };
  });
  const barsA = makeBars();
  const barsB = makeBars();
  // Mutate bar t=35 dramatically in barsB
  barsB[35].o = 70;
  barsB[35].h = 75;
  barsB[35].l = 65;
  barsB[35].c = 68;
  barsB[35].tc = 68;

  const sigA = VolumeHeatmap.computeCausalDailySigmaSeries(barsA, barsA.map(b => b.tc));
  const sigB = VolumeHeatmap.computeCausalDailySigmaSeries(barsB, barsB.map(b => b.tc));
  assert.strictEqual(sigA[35], sigB[35], 'Causal sigma[t] must NOT be altered by mutating bar t');
  assert.notStrictEqual(sigA[36], sigB[36], 'Causal sigma[t+1] MUST incorporate bar t');
  console.log('  [PASS] Strict causal no-lookahead invariance on prior-21D YZ sigma verified');
}

// 12. Dynamic DD Threshold Scaling, Monotonicity, 252D Bear Guard & Price Formatting
{
  for (const sig of [null, 0, 0.0001, 0.5, 0.77, 1.5, 2.5, 4.6, 8.0, 12.0, 20.0]) {
    const t = VolumeHeatmap.getDdThresholds(sig);
    assert.ok(t.pullPct > 0 && t.pullPct < t.deepPct && t.deepPct < t.severePct, `Non-monotonic DD cutoffs for sig=${sig}`);
    assert.ok(t.pullPct < t.bear252Pct, `bear252Pct must exceed pullPct for sig=${sig}`);
    assert.ok(t.drop1dPct > 0 && t.drop1dPct < t.drop1dSevPct, `Non-monotonic 1D cutoffs for sig=${sig}`);
  }

  const lo = VolumeHeatmap.getDdThresholds(0.8);
  const mid = VolumeHeatmap.getDdThresholds(2.5);
  const hi = VolumeHeatmap.getDdThresholds(5.0);
  assert.ok(lo.pullPct < mid.pullPct && mid.pullPct < hi.pullPct);
  assert.ok(lo.drop1dPct < mid.drop1dPct && mid.drop1dPct < hi.drop1dPct);

  assert.strictEqual(VolumeHeatmap.getDdSeverity(-2.0, -5.0, 2.5, 252).cls, 'dd-near-high');
  assert.strictEqual(VolumeHeatmap.getDdSeverity(-2.0, -5.0, 2.5, 252).label, 'NEAR HIGH');

  const bearGuard = VolumeHeatmap.getDdSeverity(-1.5, -42.0, 2.5, 252);
  assert.strictEqual(bearGuard.cls, 'dd-bear-high');
  assert.strictEqual(bearGuard.label, '63D HI · 252D BEAR');

  const shortBearGuard = VolumeHeatmap.getDdSeverity(-1.5, -42.0, 2.5, 139);
  assert.strictEqual(shortBearGuard.label, '63D HI · 139D BEAR');

  assert.strictEqual(VolumeHeatmap.getDdSeverity(-10.0, -10.0, 2.5, 252).cls, 'dd-pullback');
  assert.strictEqual(VolumeHeatmap.getDdSeverity(-25.0, -25.0, 2.5, 252).cls, 'dd-deep');
  assert.strictEqual(VolumeHeatmap.getDdSeverity(-45.0, -45.0, 2.5, 252).cls, 'dd-severe');
  assert.strictEqual(VolumeHeatmap.getDdSeverity(-45.0, -45.0, 2.5, 252).label, 'SEVERE DD');
  assert.strictEqual(VolumeHeatmap.getDdSeverity(null, null, 2.5, 252).cls, 'dd-unknown');

  assert.strictEqual(VolumeHeatmap.formatHeatPrice(12345.6), '$12,346');
  assert.strictEqual(VolumeHeatmap.formatHeatPrice(1234.5), '$1,234.50');
  assert.strictEqual(VolumeHeatmap.formatHeatPrice(25.14), '$25.14');
  assert.strictEqual(VolumeHeatmap.formatHeatPrice(0.4521), '$0.4521');
  assert.strictEqual(VolumeHeatmap.formatHeatPrice(0), '');
  assert.strictEqual(VolumeHeatmap.formatHeatPrice(null), '');
  console.log('  [PASS] Volatility-calibrated DD thresholds, Bear Guard & price formatting verified');
}

// 13. Scope Switching Contract: Exiting Isolated Ticker Pill Into Multi-ETF Basket Resets Side to 'all'
{
  const byUnd = new Map();
  for (const inst of catalog.instruments) {
    const key = VolumeHeatmap.normalizeUnderlyingKey(inst.underlying);
    if (!key) continue;
    if (!byUnd.has(key)) byUnd.set(key, []);
    byUnd.get(key).push(inst);
  }
  const paired = Array.from(byUnd.values()).find(arr => arr.length >= 2);
  const startTk = paired[0].ticker;
  VolumeHeatmap.mount({ ticker: startTk, allHistory: etfHistory, catalogInstruments: catalog.instruments });
  assert.strictEqual(VolumeHeatmap.state.scope, 'underlying');
  assert.strictEqual(VolumeHeatmap.state.side, startTk);

  // Switching to All 160 / Category / Featured from isolated ticker pill must show full basket ('all')
  VolumeHeatmap.setScope('universe');
  assert.strictEqual(VolumeHeatmap.state.scope, 'universe');
  assert.strictEqual(VolumeHeatmap.state.side, 'all', 'Switching to universe basket from isolated ticker must reset side to all');

  // Directional filter ('long') persists across macro basket transitions
  VolumeHeatmap.setSide('long');
  VolumeHeatmap.setScope('featured');
  assert.strictEqual(VolumeHeatmap.state.scope, 'featured');
  assert.strictEqual(VolumeHeatmap.state.side, 'long');
  VolumeHeatmap._userPinnedMacroScope = false;
  console.log('  [PASS] Scope switching resets isolated ticker focus to full target basket while preserving directional filter');
}

// 14. Zero-Lookahead Future Truncation & Mutation Invariance (Stage 1 + Stage 2 End-to-End)
{
  const pairedEntry = Array.from(
    catalog.instruments.reduce((map, inst) => {
      const k = VolumeHeatmap.normalizeUnderlyingKey(inst.underlying);
      if (k && Array.isArray(etfHistory[inst.ticker]) && etfHistory[inst.ticker].length >= 1100) {
        if (!map.has(k)) map.set(k, []);
        map.get(k).push(inst.ticker);
      }
      return map;
    }, new Map()).values()
  ).find(list => list.length >= 2);
  assert.ok(pairedEntry && pairedEntry.length >= 2, 'Expected paired complex with >= 1100 bars for causal truncation check');

  const fullSubset = Object.fromEntries(pairedEntry.map(tk => [tk, etfHistory[tk]]));
  const truncSubset = Object.fromEntries(pairedEntry.map(tk => [tk, etfHistory[tk].slice(0, -40)]));

  for (const mode of ['share', 'dollar']) {
    const aggFull = VolumeHeatmap.computeHeatAggregate(fullSubset, mode, 'all', catalog.instruments, { isMarketOpen: false });
    const aggTrunc = VolumeHeatmap.computeHeatAggregate(truncSubset, mode, 'all', catalog.instruments, { isMarketOpen: false });
    const truncDateSet = new Set(aggTrunc.series.map(r => r.date));
    const truncByDate = new Map(aggTrunc.series.map(r => [r.date, r]));

    for (const fullRow of aggFull.series) {
      if (!truncDateSet.has(fullRow.date)) continue;
      const trRow = truncByDate.get(fullRow.date);
      assert.strictEqual(fullRow.compositeTieKeyPeak, trRow.compositeTieKeyPeak, `TieKeyPeak shifted on ${fullRow.date}`);
      assert.strictEqual(fullRow.compositeTieKeyValley, trRow.compositeTieKeyValley, `TieKeyValley shifted on ${fullRow.date}`);
    }

    for (const regime of ['PEAKS', 'VALLEYS']) {
      for (const flt of ['P90', 'P95', 'P99']) {
        const fullStates = new Map(
          VolumeHeatmap.evaluateCausalSpotlight(aggFull.series, regime, flt).map(s => [s.date, s])
        );
        const truncStates = VolumeHeatmap.evaluateCausalSpotlight(aggTrunc.series, regime, flt);
        for (const ts of truncStates) {
          const fsState = fullStates.get(ts.date);
          if (!fsState) continue;
          assert.deepStrictEqual(ts, fsState, `Look-ahead leak detected (${mode}/${regime}/${flt}) on ${ts.date}`);
        }
      }
    }
  }
  console.log('  [PASS] Zero-lookahead future truncation & tie-key invariance verified across Stage 1 and Stage 2');
}

// 15. 1050-Bar Pre-Slice Stage 2 Causal Padding & Decoupled 50 / 20 / 20 Warm-Up Gates
{
  const nTotal = 1050;
  const paddedBars = [];
  for (let i = 0; i < nTotal; i++) {
    const d = new Date(Date.UTC(2022, 0, 1 + i)).toISOString().slice(0, 10);
    const v = 50000 + (i % 17) * 500 + (i === 180 ? 250000 : 0);
    paddedBars.push({ d, o: 100, h: 101, l: 99, c: 100 + (i % 5) * 0.2, v });
  }
  const agg = VolumeHeatmap.computeHeatAggregate({ PADSTAGE2: paddedBars }, 'share', 'all', [], { isMarketOpen: false });
  assert.strictEqual(agg.series.length, VolumeHeatmap.HEATMAP_MAX_BARS);
  const st = VolumeHeatmap.evaluateCausalSpotlight(agg.series, 'PEAKS', 'P95');
  assert.strictEqual(st[0].windowLen, VolumeHeatmap.CAUSAL_SPOTLIGHT_WINDOW,
    'Oldest display cell series[0] must inherit full 252D causal Stage 2 lookback from pre-slice padding');

  const makeSpikeSeries = len => Array.from({ length: len }, (_, i) => ({
    date: new Date(Date.UTC(2026, 0, 1 + i)).toISOString().slice(0, 10),
    peakScore: i === len - 1 ? 85.0 : 12.0 + (i % 5),
    valleyScore: 0,
    maxRvol: i === len - 1 ? 4.5 : 1.0,
    minRvol: 1.0,
    climaxDir: 1
  }));

  const s19 = makeSpikeSeries(19);
  assert.strictEqual(VolumeHeatmap.evaluateCausalSpotlight(s19, 'PEAKS', 'P90')[18].isHighlighted, false, 'wLen=19 must remain in warm-up');
  assert.strictEqual(VolumeHeatmap.evaluateCausalSpotlight(s19, 'PEAKS', 'P99')[18].visualTier, 'NONE');

  const s20 = makeSpikeSeries(20);
  const res20_95 = VolumeHeatmap.evaluateCausalSpotlight(s20, 'PEAKS', 'P95');
  const res20_99 = VolumeHeatmap.evaluateCausalSpotlight(s20, 'PEAKS', 'P99');
  assert.strictEqual(res20_95[19].isHighlighted, true, 'wLen=20 must unlock P95');
  assert.strictEqual(res20_95[19].visualTier, 'ORANGE_P95', 'wLen=20 must cap tier at P95 rather than inflating P99');
  assert.strictEqual(res20_99[19].isHighlighted, false, 'wLen=20 must gate P99 until >= 50 sessions');

  const s49 = makeSpikeSeries(49);
  assert.strictEqual(VolumeHeatmap.evaluateCausalSpotlight(s49, 'PEAKS', 'P99')[48].isHighlighted, false, 'wLen=49 must gate P99');

  const s50 = makeSpikeSeries(50);
  const res50_99 = VolumeHeatmap.evaluateCausalSpotlight(s50, 'PEAKS', 'P99');
  assert.strictEqual(res50_99[49].isHighlighted, true, 'wLen=50 must unlock P99');
  assert.strictEqual(res50_99[49].visualTier, 'RED_P99');
  console.log('  [PASS] 252D pre-display Stage 2 padding and decoupled 50 / 20 / 20 warm-up gates verified');
}

// 16. Pessimistic Tie-Breaking, Score <= 0 Suppression & minRvol === 0 Guard
{
  for (const vol of [0, 1000]) {
    const flatBars = Array.from({ length: 120 }, (_, i) => ({
      d: new Date(Date.UTC(2026, 0, 1 + i)).toISOString().slice(0, 10),
      o: 10, h: 10, l: 10, c: 10, v: vol
    }));
    const aggFlat = VolumeHeatmap.computeHeatAggregate({ FLATZ: flatBars }, 'share', 'all', [], { isMarketOpen: false });
    for (const regime of ['PEAKS', 'VALLEYS']) {
      for (const flt of ['P90', 'P95', 'P99']) {
        const res = VolumeHeatmap.evaluateCausalSpotlight(aggFlat.series, regime, flt);
        const hiCount = res.filter(x => x.isHighlighted).length;
        assert.strictEqual(hiCount, 0, `Flat series (v=${vol}) must produce 0 ${regime} ${flt} highlights`);
        const last = res[res.length - 1];
        assert.strictEqual(last.windowRank, last.windowLen, `Flat tail rank must tie pessimistically at windowLen (${vol}/${regime})`);
      }
    }
  }

  // minRvol === 0 must not fall back to 1.0 via falsy ||
  const synthTies = Array.from({ length: 60 }, (_, i) => ({
    date: new Date(Date.UTC(2026, 2, 1 + i)).toISOString().slice(0, 10),
    peakScore: 0,
    valleyScore: i === 58 ? 50.0 : (i === 59 ? 50.0 : 10.0),
    maxRvol: 1.0,
    minRvol: i === 58 ? 0.0 : (i === 59 ? 0.4 : 0.8),
    climaxDir: 1
  }));
  const stMin0 = VolumeHeatmap.evaluateCausalSpotlight(synthTies, 'VALLEYS', 'P95');
  assert.strictEqual(stMin0[58].windowRank, 1, 'minRvol === 0 (extreme drought) must outrank minRvol = 0.4');
  assert.strictEqual(stMin0[59].windowRank, 2, 'Second identical valleyScore with higher minRvol must rank #2');
  console.log('  [PASS] Pessimistic ties, score <= 0 suppression, and minRvol === 0 safety verified');
}

console.log('\nAll VolumeHeatmap tests PASSED successfully.');
