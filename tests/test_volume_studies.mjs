/**
 * Headless Node.js Mathematical Verification Suite for VolumeStudies
 */

import assert from 'node:assert';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');

const VolumeStudies = require(path.join(ROOT, 'docs', 'volume-studies.js'));

console.log('Testing VolumeStudies Mathematical Engine...');

// 1. SMA calculation
{
  const values = [10, 20, 30, 40, 50];
  const sma3 = VolumeStudies.sma(values, 3);
  assert.strictEqual(sma3[0], null);
  assert.strictEqual(sma3[1], null);
  assert.strictEqual(sma3[2], 20); // (10+20+30)/3
  assert.strictEqual(sma3[3], 30); // (20+30+40)/3
  assert.strictEqual(sma3[4], 40); // (30+40+50)/3
  console.log('✔ SMA precision validated');
}

// 2. Rolling Sum
{
  const values = [5, 10, 15, 20];
  const sum2 = VolumeStudies.rollingSum(values, 2);
  assert.strictEqual(sum2[0], null);
  assert.strictEqual(sum2[1], 15);
  assert.strictEqual(sum2[2], 25);
  assert.strictEqual(sum2[3], 35);
  console.log('✔ Rolling sum precision validated');
}

// 3. Directional Sign Series
{
  const closes = [100, 105, 103, 103, 110];
  const signs = VolumeStudies.signSeries(closes);
  assert.deepStrictEqual(signs, [0, 1, -1, 0, 1]);
  console.log('✔ Sign series directionality validated');
}

// 4. Signed Volume
{
  const closes = [50, 52, 49, 49, 51];
  const volumes = [1000, 2000, 3000, 1500, 4000];
  const signed = VolumeStudies.signedVolume(closes, volumes);
  assert.deepStrictEqual(signed, [0, 2000, -3000, 0, 4000]);
  console.log('✔ Signed volume validated');
}

// 5. Volume Oscillator
{
  const volumes = [100, 100, 100, 100, 100, 200, 200, 200, 200, 200];
  const osc = VolumeStudies.volumeOscillator(volumes, 2, 4);
  // At index 3: SMA2 = (100+100)/2 = 100; SMA4 = (100*4)/4 = 100 -> ((100-100)/100)*100 = 0%
  assert.strictEqual(osc[3], 0);
  // At index 5: volumes[4]=100, volumes[5]=200 -> SMA2 = 150; SMA4 = (100+100+100+200)/4 = 125 -> ((150-125)/125)*100 = 20%
  assert.strictEqual(osc[5], 20);
  console.log('✔ Volume oscillator fast/slow expansion validated');
}

// 6. Chaikin Money Flow (CMF) with CLV bounds & flat price handling
{
  // Normal session
  const highs = [110, 115, 112, 118, 120];
  const lows = [100, 105, 102, 108, 110];
  const closes = [105, 112, 104, 117, 119];
  const volumes = [1000, 1000, 1000, 1000, 1000];

  const cmf3 = VolumeStudies.cmf(highs, lows, closes, volumes, 3);
  assert.strictEqual(cmf3[0], null);
  assert.strictEqual(cmf3[1], null);
  assert.ok(cmf3[2] >= -1.0 && cmf3[2] <= 1.0);
  assert.ok(cmf3[3] >= -1.0 && cmf3[3] <= 1.0);
  assert.ok(cmf3[4] >= -1.0 && cmf3[4] <= 1.0);

  // Flat price session where High === Low (zero division guard)
  const flatHighs = [100, 100, 100];
  const flatLows = [100, 100, 100];
  const flatCloses = [100, 100, 100];
  const flatVols = [500, 500, 500];
  const flatCmf = VolumeStudies.cmf(flatHighs, flatLows, flatCloses, flatVols, 2);
  assert.strictEqual(flatCmf[1], 0);
  assert.strictEqual(flatCmf[2], 0);
  console.log('✔ CMF bounded invariance and zero-division protection validated');
}

// 7. On-Balance Volume (OBV)
{
  const closes = [10, 12, 11, 13];
  const volumes = [100, 200, 150, 300];
  const obv = VolumeStudies.obv(closes, volumes);
  assert.strictEqual(obv[0], 0);
  assert.strictEqual(obv[1], 200);      // 0 + 200
  assert.strictEqual(obv[2], 50);       // 200 - 150
  assert.strictEqual(obv[3], 350);      // 50 + 300
  console.log('✔ OBV cumulative series validated');
}

// 8. Price-Volume Trend (PVT)
{
  const closes = [10, 11, 10]; // +10%, then -9.09%
  const volumes = [1000, 1000, 1100];
  const pvt = VolumeStudies.pvt(closes, volumes);
  assert.strictEqual(pvt[0], 0);
  assert.strictEqual(Math.round(pvt[1]), 100); // 1000 * (1/10) = 100
  assert.strictEqual(Math.round(pvt[2]), 0);   // 100 + 1100 * (-1/11) = 100 - 100 = 0
  console.log('✔ PVT proportional price weighting validated');
}

// 9. Window Re-basing (OBV & PVT relative to start index)
{
  const history = [
    { d: '2024-01-02', o: 10, h: 12, l: 9, c: 11, v: 1000 },
    { d: '2024-01-03', o: 11, h: 13, l: 10, c: 12, v: 2000 },
    { d: '2024-01-04', o: 12, h: 14, l: 11, c: 10, v: 1500 },
    { d: '2024-01-05', o: 10, h: 12, l: 9, c: 11, v: 2500 },
    { d: '2024-01-08', o: 11, h: 13, l: 10, c: 13, v: 3000 }
  ];

  // Custom window start at index 2
  const obvPayload = VolumeStudies.compute(history, 'obv', 'custom', {
    customRange: { startIdx: 2, endIdx: 4 }
  });

  assert.strictEqual(obvPayload.line[0], 0, 'Re-based OBV must equal 0 at visible start');
  assert.strictEqual(obvPayload.dates.length, 3);
  assert.strictEqual(obvPayload.dates[0], '2024-01-04');

  const pvtPayload = VolumeStudies.compute(history, 'pvt', 'custom', {
    customRange: { startIdx: 2, endIdx: 4 }
  });

  assert.strictEqual(pvtPayload.line[0], 0, 'Re-based PVT must equal 0 at visible start');
  console.log('✔ Visible window zero-rebasing validated');
}

// 10. Metric Unit Scaling & Suffix Formatting
{
  assert.strictEqual(VolumeStudies.fmtVol(1240000, 'shares', { withSuffix: true }), '1.24M shs');
  assert.strictEqual(VolumeStudies.fmtVol(1240000, 'dollar'), '$1.24M');
  assert.strictEqual(VolumeStudies.fmtVol(500, 'shares', { withSuffix: true }), '500 shs');
  assert.strictEqual(VolumeStudies.fmtSigned(1240000, 'shares', { withSuffix: true }), '+1.24M shs');
  assert.strictEqual(VolumeStudies.fmtSigned(-1240000, 'dollar'), '-$1.24M');
  assert.strictEqual(VolumeStudies.fmtSigned(0, 'dollar'), '$0');
  console.log('✔ Metric unit scaling and shares/dollar formatting validated');
}

// 11. Range Indices Resolution Safety
{
  const dates = ['2023-11-01', '2023-12-01', '2024-01-05', '2024-06-01', '2024-10-31'];
  // Year 2024 selection
  const r2024 = VolumeStudies._resolveRangeIndices(dates, 'year_2024');
  assert.strictEqual(r2024.startIdx, 2);
  assert.strictEqual(r2024.endIdx, 4);

  // Year 2023 selection
  const r2023 = VolumeStudies._resolveRangeIndices(dates, 'year_2023');
  assert.strictEqual(r2023.startIdx, 0);
  assert.strictEqual(r2023.endIdx, 1);

  // Custom range clamping
  const rCustom = VolumeStudies._resolveRangeIndices(dates, 'custom', { startIdx: 1, endIdx: 3 });
  assert.strictEqual(rCustom.startIdx, 1);
  assert.strictEqual(rCustom.endIdx, 3);
  console.log('✔ Year and custom range indices resolution validated');
}

// 12. 21D Price Return & CMF Divergence Analysis
{
  const closes = [100, 102, 105, 110];
  const pret2 = VolumeStudies.priceReturn(closes, 2);
  assert.strictEqual(pret2[0], null);
  assert.strictEqual(pret2[1], null);
  assert.strictEqual(Math.round(pret2[2] * 100) / 100, 5.0); // (105-100)/100 = +5%
  assert.strictEqual(Math.round(pret2[3] * 100) / 100, 7.84); // (110-102)/102 = +7.84%

  // Divergence scenarios
  // 1. Price up (+5%), CMF negative (-0.12) -> Bearish Divergence
  // 2. Price down (-4%), CMF positive (+0.15) -> Bullish Divergence
  // 3. Price up (+2%), CMF positive (+0.08) -> In-Sync Accumulation
  // 4. Price down (-3%), CMF negative (-0.10) -> In-Sync Distribution
  // 5. Price flat (0%), CMF neutral (0.01) -> Neutral
  const pReturns = [5, -4, 2, -3, 0];
  const cmfVals = [-0.12, 0.15, 0.08, -0.10, 0.01];
  const divs = VolumeStudies.cmfDivergence(pReturns, cmfVals);

  assert.strictEqual(divs[0], 'BEARISH DIVERGENCE');
  assert.strictEqual(divs[1], 'BULLISH DIVERGENCE');
  assert.strictEqual(divs[2], 'IN-SYNC ACCUMULATION');
  assert.strictEqual(divs[3], 'IN-SYNC DISTRIBUTION');
  assert.strictEqual(divs[4], 'NEUTRAL');
  console.log('✔ 21D Price return and CMF divergence regime detection validated');
}

// 13. CMF Integration with Divergence in Compute Payload
{
  const history = [];
  for (let i = 0; i < 30; i++) {
    // Generate simulated sessions
    history.push({
      d: `2024-01-${String(i + 1).padStart(2, '0')}`,
      o: 100 + i * 0.5,
      h: 102 + i * 0.5,
      l: 99 + i * 0.5,
      c: 101 + i * 0.5,
      v: 10000 + (i % 3) * 2000
    });
  }

  const payload = VolumeStudies.compute(history, 'cmf', 'all');
  assert.strictEqual(payload.study, 'cmf');
  assert.strictEqual(payload.dates.length, 30);
  assert.strictEqual(payload.line.length, 30);
  assert.strictEqual(payload.priceRet.length, 30);
  assert.strictEqual(payload.divergence.length, 30);
  assert.ok(payload.priceRet[25] !== null, 'Day 25 should have valid 21D price return');
  assert.ok(payload.divergence[25] !== null, 'Day 25 should have valid divergence evaluation');
  assert.ok(payload.cmfZScore !== undefined, 'Payload should include cmfZScore');
  console.log('✔ CMF compute payload contract including divergence validated');
}

// 14. BVC, Yang-Zhang Volatility & Regression Divergence
{
  const history = [];
  for (let i = 0; i < 40; i++) {
    history.push({
      d: `2024-01-${String(i + 1).padStart(2, '0')}`,
      o: 100 + i * 0.2,
      h: 102 + i * 0.2,
      l: 99 + i * 0.2,
      c: 101 + i * 0.2,
      v: 25000 + (i % 5) * 5000
    });
  }
  const yz = VolumeStudies.yangZhangVol(history, 21);
  assert.strictEqual(yz.length, 40);
  assert.strictEqual(yz[0], null);
  assert.ok(yz[25] > 0, 'Yang-Zhang volatility should be strictly positive');

  const bvcFlow = VolumeStudies.bvcFlow(history, yz);
  assert.strictEqual(bvcFlow.length, 40);
  assert.strictEqual(bvcFlow[0], 0);

  const bvcObv = VolumeStudies.bvcObv(history, yz);
  assert.strictEqual(bvcObv.length, 40);

  const smaP = VolumeStudies.smaPrior([10, 20, 30, 40, 50], 3);
  assert.strictEqual(smaP[3], 20); // (10+20+30)/3
  assert.strictEqual(smaP[4], 30); // (20+30+40)/3

  const dates = history.map(b => b.d);
  const vols = history.map(b => b.v);
  const rDow = VolumeStudies.rvolDow(dates, vols, 20);
  assert.strictEqual(rDow.length, 40);

  const amihud = VolumeStudies.amihudIlliq21(history, 2.0);
  assert.strictEqual(amihud.length, 40);

  const roll = VolumeStudies.rollSpread21(history, 1.5);
  assert.strictEqual(roll.length, 40);

  const regDiv = VolumeStudies.detectRegressionDivergence(history.map(b => b.c), history.map(b => b.v), 25);
  assert.strictEqual(regDiv.length, 40);
  console.log('✔ BVC, Yang-Zhang Vol, Amihud, Roll, and Regression Divergence validated');
}

// 15. Upgraded Econometric Pipeline Contracts (causal Prior-20, Osc, Leverage-Neutral ILLIQ, TR-CMF, BVC-OBV/vPVT, 63D OLS Divergence)
{
  const history = [];
  for (let i = 0; i < 80; i++) {
    const base = 100 * Math.exp(0.002 * i + 0.015 * Math.sin(i * 0.6));
    const prev = i > 0 ? history[i - 1].c : base;
    history.push({
      d: new Date(Date.UTC(2025, 0, 2 + i)).toISOString().slice(0, 10),
      o: prev * (i % 7 === 0 ? 0.97 : 1.001),
      h: base * 1.02,
      l: base * 0.985,
      c: base,
      v: 10000
    });
  }
  // Bar 25 spikes 10x in volume while closing down mildly
  history[25].v = 100000;
  history[25].c = history[24].c * 0.99;

  // vol: causal smaPrior must NOT include bar 25 in ma[25]
  const vol1x = VolumeStudies.compute(history, 'vol', 'all', { leverage: 1 });
  const vol3x = VolumeStudies.compute(history, 'vol', 'all', { leverage: 3 });
  assert.strictEqual(vol1x.ma[25], 10000, 'Causal Prior SMA-20 at bar 25 must exclude bar 25 spike');
  assert.strictEqual(vol1x.ma[26], 14500, 'Causal Prior SMA-20 at bar 26 must include bar 25');
  assert.ok(vol1x.illiq[40] > 0 && vol3x.illiq[40] > 0);
  assert.ok(Math.abs((vol1x.illiq[40] / vol3x.illiq[40]) - 3) < 1e-9, 'Amihud illiquidity must scale inversely by |leverage|');

  // osc: compute('osc') compares SMA(5) vs causal Prior SMA(20)
  const oscPayload = VolumeStudies.compute(history, 'osc', 'all', { leverage: 1 });
  // At bar 25: SMA(5) = (4*10000 + 100000)/5 = 28000, Prior20 = 10000 -> +180%
  assert.strictEqual(Math.round(oscPayload.line[25]), 180);

  // obv & pvt: no lagging MA overlay
  const obvPayload = VolumeStudies.compute(history, 'obv', 'all', { leverage: 1 });
  const pvtPayload = VolumeStudies.compute(history, 'pvt', 'all', { leverage: 1 });
  assert.strictEqual(obvPayload.ma, null, 'BVC-OBV must omit lagging SMA overlay');
  assert.strictEqual(pvtPayload.ma, null, 'vPVT must omit lagging SMA overlay');
  assert.strictEqual(obvPayload.line[0], 0);
  assert.strictEqual(pvtPayload.line[0], 0);
  assert.ok(obvPayload.buyPct.every(p => p >= 0 && p <= 100));

  // cmf: TR-CMF accounts for overnight gaps & produces CMF Z-score and divergence
  const cmfPayload = VolumeStudies.compute(history, 'cmf', 'all', { leverage: 1 });
  assert.ok(cmfPayload.cmfZScore.some(v => v != null && isFinite(v)), 'TR-CMF must populate Z_CMF when valid window >= 10');
  assert.strictEqual(cmfPayload.divergence.length, 80);

  // detectRegressionDivergence sign contract: positive flow surprise on positive-beta pair => BULLISH DIVERGENCE
  const nDiv = 65;
  const pSeries = [];
  const fBull = [];
  let cum = 0;
  for (let i = 0; i < nDiv; i++) {
    const p = i === 0 ? 100 : pSeries[i - 1] * Math.exp(i === nDiv - 1 ? -0.02 : (i % 2 === 0 ? 0.015 : -0.01));
    pSeries.push(p);
    const step = i === nDiv - 1
      ? 5000 // strong positive absorption flow despite negative return!
      : (i > 0 ? 1000 * Math.log(p / pSeries[i - 1]) / 0.015 + ((i % 3) - 1) * 5 : 0);
    cum += step;
    fBull.push(cum);
  }
  const bullRes = VolumeStudies.detectRegressionDivergence(pSeries, fBull, 63);
  assert.strictEqual(bullRes[nDiv - 1], 'BULLISH DIVERGENCE', 'Unexpected positive flow surprise must classify as BULLISH DIVERGENCE');

  // Negative flow surprise on an up day => BEARISH DIVERGENCE
  const fBear = fBull.slice(0, -1);
  pSeries[nDiv - 1] = pSeries[nDiv - 2] * Math.exp(0.02);
  fBear.push(fBear[fBear.length - 1] - 5000);
  const bearRes = VolumeStudies.detectRegressionDivergence(pSeries, fBear, 63);
  assert.strictEqual(bearRes[nDiv - 1], 'BEARISH DIVERGENCE', 'Unexpected negative flow deficit must classify as BEARISH DIVERGENCE');

  console.log('✔ Upgraded econometric compute() contracts and OLS divergence signs validated');
}

// 16. Zero Emojis & Zero Banned Terminology in Upgraded Assets
{
  const emojiRegex = /[\u{1F300}-\u{1F9FF}]|[\u{2600}-\u{26FF}]|[\u{2700}-\u{27BF}]/u;
  const banned = ['institutional', 'retail', 'fomo', 'capitulation', 'verified', 'creations', 'statistically significant', 'localstorage', 'watch_tier', 'watchTier', '15,000', '15K', 'TQQQ'];
  for (const rel of ['docs/volume-studies.js', 'docs/macd-strategy.css']) {
    const content = fs.readFileSync(path.join(ROOT, rel), 'utf8');
    assert.strictEqual(emojiRegex.test(content), false, `Emoji found in ${rel}`);
    const lower = content.toLowerCase();
    for (const word of banned) {
      assert.strictEqual(lower.includes(word.toLowerCase()), false, `Banned term "${word}" found in ${rel}`);
    }
  }
  console.log('✔ Zero emojis and zero banned terminology in VolumeStudies & MACD CSS validated');
}

console.log('\nAll 16 mathematical contract tests PASSED successfully.');
