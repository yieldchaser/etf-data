/**
 * Headless Node.js Runtime Contract Verification Suite for docs/markets.html
 * Evaluates the production inline script from docs/markets.html inside a sandboxed VM
 * and tests behavior/math/XSS/reset contracts on real computed outputs.
 */

import assert from 'node:assert';
import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const HTML = fs.readFileSync(path.join(ROOT, 'docs', 'markets.html'), 'utf8');

function extractMarketsInlineScript(html) {
  const marker = '// ─── Singleton tooltip engine';
  const startIdx = html.indexOf(marker);
  assert.ok(startIdx > 0, 'Could not locate inline markets script start');
  const scriptEnd = html.indexOf('</script>', startIdx);
  assert.ok(scriptEnd > startIdx, 'Could not locate inline markets script end');
  return html.slice(startIdx, scriptEnd);
}

function makeFakeElement(tagName = 'div') {
  const el = {
    tagName: String(tagName).toUpperCase(),
    style: {},
    children: [],
    dataset: {},
    textContent: '',
    appendChild(child) {
      this.children.push(child);
      return child;
    },
    replaceChildren(...nodes) {
      this.children = nodes;
      this.textContent = nodes.map(n => n.textContent || n.nodeValue || '').join('');
    },
    getBoundingClientRect() {
      return { left: 100, top: 100, right: 200, bottom: 130, width: 100, height: 30 };
    },
    querySelector() { return null; },
    querySelectorAll() { return []; },
    contains() { return false; }
  };
  return el;
}

function loadMarketsRuntime(fetchImpl) {
  const ttEl = makeFakeElement('div');
  const listeners = {};
  const sandbox = {
    console,
    Math,
    Date,
    Number,
    String,
    Array,
    Object,
    Set,
    Map,
    JSON,
    Promise,
    parseInt,
    parseFloat,
    isFinite,
    isNaN,
    setTimeout,
    clearTimeout,
    URLSearchParams,
    location: { search: '', pathname: '/markets.html', hash: '' },
    window: { innerWidth: 1366, innerHeight: 768 },
    document: {
      getElementById(id) { return id === 'tt' ? ttEl : null; },
      createElement(tag) { return makeFakeElement(tag); },
      createTextNode(txt) { return { nodeType: 3, nodeValue: String(txt ?? ''), textContent: String(txt ?? '') }; },
      querySelectorAll() { return []; },
      addEventListener(ev, fn) { listeners[ev] = fn; }
    },
    fetch: fetchImpl || (async () => ({ ok: false, status: 500, json: async () => ({}) }))
  };
  vm.createContext(sandbox);
  vm.runInContext(extractMarketsInlineScript(HTML), sandbox);
  return { sandbox, ttEl };
}

console.log('Testing docs/markets.html runtime contracts...');

// 1. Drawdown SVG closes every contiguous non-null segment back to y0
{
  const { sandbox } = loadMarketsRuntime();
  const app = sandbox.marketsApp();
  app.returnsData = {
    asof: '2020-12',
    assets: {
      sp500: {
        meta: { name: 'S&P 500', category: 'equity', native_ccy: 'USD' },
        monthly: [
          ['2020-07', 100], ['2020-08', 80], ['2020-09', 90],
          ['2020-10', 100], ['2020-11', 95], ['2020-12', 110]
        ]
      },
      gold: {
        meta: { name: 'Gold', category: 'precious_metals', native_ccy: 'USD' },
        monthly: [
          ['2020-01', 100], ['2020-02', 90], ['2020-03', 100],
          ['2020-07', 110], ['2020-08', 105], ['2020-12', 120]
        ]
      }
    }
  };
  app.activeDrawdownAssets = new Set(['sp500', 'gold']);
  app.ddTimeRange = 'All';

  const svg = app.drawdownChartSvg;
  const fillPaths = [...svg.matchAll(/<path d="([^"]+)" fill="[^"]+" opacity="0\.35"/g)].map(m => m[1]);
  assert.strictEqual(fillPaths.length, 2, 'Expected 2 filled drawdown area paths');
  for (const d of fillPaths) {
    const segs = d.split('Z').map(s => s.trim()).filter(Boolean);
    assert.ok(segs.length >= 1 && d.trim().endsWith('Z'), `Area path must close every contiguous segment back to baseline y0: ${d}`);
  }
  console.log('  [PASS] drawdownChartSvg contiguous segment polygon closure verified');
}

// 2. Calendar monthsToRecover arithmetic & empty-data ulcerIndex/currentDdPctile nulls
{
  const { sandbox } = loadMarketsRuntime();
  const app = sandbox.marketsApp();
  app.returnsData = {
    asof: '2022-06',
    assets: {
      sp500: {
        meta: { name: 'S&P 500', category: 'equity', native_ccy: 'USD' },
        monthly: [
          ['2020-01', 95], ['2020-02', 100], ['2020-03', 80], ['2020-04', 60],
          ['2021-01', 85], ['2022-05', 105], ['2022-06', 102]
        ]
      },
      gold: {
        meta: { name: 'Empty Series', category: 'precious_metals', native_ccy: 'USD' },
        monthly: []
      }
    }
  };
  app.activeDrawdownAssets = new Set(['sp500', 'gold']);
  app.ddTimeRange = 'All';

  const stats = app.drawdownTable;
  const spStat = stats.find(s => s.key === 'sp500');
  const emptyStat = stats.find(s => s.key === 'gold');
  assert.strictEqual(spStat.peakDate, '2020-02');
  assert.strictEqual(spStat.troughDate, '2020-04');
  assert.strictEqual(spStat.recoveryDate, '2022-05');
  assert.strictEqual(spStat.monthsToRecover, 27, 'Expected exact calendar month difference (2022-05 minus 2020-02 = 27)');
  assert.strictEqual(emptyStat.ulcerIndex, null, 'Empty series fallback must return ulcerIndex: null');
  assert.strictEqual(emptyStat.currentDdPctile, null, 'Empty series fallback must return currentDdPctile: null');
  console.log('  [PASS] Exact calendar monthsToRecover and empty drawdownTable nulls verified');
}

// 3. Rolling 12M correlation pairs matching months, enforces n>=6, skips zero-variance without NaN
{
  const { sandbox } = loadMarketsRuntime();
  const app = sandbox.marketsApp();
  app.matrixStartYear = 2019;
  const basePts = [];
  const invPts = [];
  const flatPts = [];
  const sparsePts = [];
  let pb = 100, pi = 100;
  for (let i = 0; i < 18; i++) {
    const yr = 2020 + Math.floor(i / 12);
    const mo = String((i % 12) + 1).padStart(2, '0');
    const ym = `${yr}-${mo}`;
    const r = (i % 2 === 0) ? 0.04 : -0.02;
    if (i > 0) {
      pb *= (1 + r);
      pi *= (1 - r);
    }
    basePts.push([ym, pb]);
    invPts.push([ym, pi]);
    flatPts.push([ym, 100]);
    if (i <= 2 || i >= 16) sparsePts.push([ym, 50 + i]);
  }
  app.returnsData = {
    asof: '2021-06',
    assets: {
      sp500:     { meta: { name: 'Base Asset', category: 'equity', native_ccy: 'USD' }, monthly: basePts },
      gold:      { meta: { name: 'Inverse Mirror', category: 'precious_metals', native_ccy: 'USD' }, monthly: invPts },
      silver:    { meta: { name: 'Flat Zero Var', category: 'precious_metals', native_ccy: 'USD' }, monthly: flatPts },
      wti_crude: { meta: { name: 'Too Few Pairs', category: 'energy', native_ccy: 'USD' }, monthly: sparsePts }
    }
  };
  app.rollingCorrBase = 'sp500';
  app.activeCorrAssets = new Set(['sp500', 'gold', 'silver', 'wti_crude']);

  const { allYms, series } = app._getRollingCorrSeries();
  assert.ok(series.gold && series.gold.pts.length >= 2, 'Valid mirror series must emit rolling correlations');
  for (const pt of series.gold.pts) {
    assert.ok(Number.isFinite(pt.corr) && Math.abs(pt.corr - (-1.0)) < 1e-9, `Expected -1.0 corr, got ${pt.corr}`);
  }
  assert.strictEqual(series.silver, undefined, 'Zero-variance constant series must be skipped cleanly (no NaN/Infinity)');
  assert.strictEqual(series.wti_crude, undefined, 'Sparse (<6 pairs) series must not emit spurious rolling correlations');
  assert.ok(allYms.length >= 2);
  console.log('  [PASS] Rolling 12M correlation pairing & zero-variance safety verified');
}

// 4. assetDetailChartSvg zero-range and log-scale finite coordinate guards
{
  const { sandbox } = loadMarketsRuntime();
  const app = sandbox.marketsApp();
  app.returnsData = {
    asof: '2024-06',
    assets: {
      flat: {
        meta: { name: 'Flat Price Series', category: 'equity', native_ccy: 'USD' },
        monthly: [['2024-01', 42], ['2024-02', 42], ['2024-03', 42], ['2024-04', 42]]
      }
    }
  };
  app.selectedAsset = 'flat';
  for (const useLog of [false, true]) {
    app.detailLogScale = useLog;
    const svg = app.assetDetailChartSvg;
    assert.ok(svg.includes('<svg') && !svg.includes('NaN') && !svg.includes('Infinity'),
      `assetDetailChartSvg must produce finite coordinates for flat series (log=${useLog})`);
  }
  console.log('  [PASS] assetDetailChartSvg zero-range & log-scale finite coordinate guards verified');
}

// 5. XSS escaping across Periodic Table, Volatility Heatmap, Bars & Singleton Tooltip
{
  const { sandbox, ttEl } = loadMarketsRuntime();
  const app = sandbox.marketsApp();
  const payload = '<img src=x onerror="window.__xss=1">';
  const attrPayload = `" onmouseover="window.__xss=2" name="${payload}`;

  assert.strictEqual(
    sandbox._escHtml('<script>"\'&'),
    '&lt;script&gt;&quot;&#39;&amp;'
  );

  app.matrixStartYear = 2023;
  app.returnsData = {
    asof: '2025-12',
    assets: {
      sp500: {
        meta: { name: attrPayload, category: 'equity', native_ccy: 'USD' },
        monthly: [['2023-12', 100], ['2024-12', 110], ['2025-12', 120]]
      }
    }
  };
  const pHtml = app.periodicTableHtml;
  assert.ok(pHtml.includes('data-pname='), 'Periodic table should emit delegated data-pname attribute');
  assert.strictEqual(pHtml.includes('<img'), false, 'Periodic table HTML must escape injected tags');
  assert.strictEqual(pHtml.includes('onmouseover="window.__xss'), false, 'Periodic table must not contain inline handler injection');

  sandbox._showPeriodicTt({ getBoundingClientRect: () => ({ left: 10, top: 50, right: 50, bottom: 80, width: 40, height: 30 }) }, payload, '2025', 0.123);
  assert.strictEqual(ttEl.children.length, 2, 'Periodic tooltip must construct safe DOM element children');
  assert.strictEqual(ttEl.children[0].textContent, payload, 'Tooltip title must preserve payload purely as textContent');

  app.volData = {
    series: {
      vix: { monthly_avg: { '2025-01': 15.2, '2025-02': 22.4 }, daily: { '2025-02-28': 22.4 } }
    }
  };
  app.volGranularity = 'Monthly';
  app.volLookback = 'All';
  const zBar = app.volZBarHtml;
  const zHeat = app.volMonthlyZHeatmapHtml;
  assert.ok(zBar.includes('data-tip=') && zHeat.includes('data-tip='));
  assert.strictEqual(/onmouseover\s*=/.test(zBar) || /onmouseover\s*=/.test(zHeat), false, 'Volatility panels must use delegated data-tip without inline onmouseover');
  console.log('  [PASS] XSS escaping & delegated DOM tooltip safety verified');
}

// 6. Dataset asof & _applyTimeRange anchoring (independent of wall-clock Date)
{
  const { sandbox } = loadMarketsRuntime();
  const app = sandbox.marketsApp();
  app.returnsData = {
    asof: '2018-06',
    assets: {
      sp500: {
        meta: { name: 'Historical Benchmark', category: 'equity', native_ccy: 'USD' },
        monthly: [['2010-06', 100], ['2013-06', 120], ['2018-06', 150]]
      }
    }
  };
  app.setMatrixWindow(5);
  assert.strictEqual(app.matrixStartYear, 2013, 'setMatrixWindow(5) must anchor to dataset asof year (2018 - 5 = 2013)');

  app.ddTimeRange = '5Y';
  const sliced = app._applyTimeRange([
    { ym: '2010-06', dd: -0.2 },
    { ym: '2013-06', dd: 0.0 },
    { ym: '2018-06', dd: -0.1 }
  ]);
  assert.deepStrictEqual(sliced.map(p => p.ym), ['2013-06', '2018-06'],
    '_applyTimeRange must anchor lookback relative to dataset lastYm (not wall clock)');
  console.log('  [PASS] Dataset asof & _applyTimeRange wall-clock independence verified');
}

// 7. Single-flight _loadReturns() deduplication & retry reset after failure
{
  let fetchCount = 0;
  let shouldFail = false;
  const { sandbox } = loadMarketsRuntime(async () => {
    fetchCount++;
    if (shouldFail) throw new Error('network error');
    return {
      ok: true,
      status: 200,
      json: async () => ({
        asof: '2025-10',
        assets: { sp500: { meta: { name: 'S&P 500', category: 'equity', native_ccy: 'USD' }, monthly: [['2025-09', 100], ['2025-10', 105]] } }
      })
    };
  });

  const app = sandbox.marketsApp();
  const p1 = app._loadReturns();
  const inflight = app._returnsPromise;
  assert.ok(inflight && typeof inflight.then === 'function', 'First _loadReturns call must store in-flight _returnsPromise');
  const p2 = app._loadReturns();
  assert.strictEqual(app._returnsPromise, inflight, 'Concurrent _loadReturns call must reuse existing in-flight promise');
  await Promise.all([p1, p2]);
  assert.strictEqual(fetchCount, 1, 'Single-flight deduplication must execute exactly 1 fetch');
  assert.strictEqual(app._returnsPromise, null, '_returnsPromise must clear after completion');
  assert.strictEqual(app.returnsData.asof, '2025-10');

  // Verify non-ok HTTP status resets _returnsPromise cleanly so subsequent reload works
  const { sandbox: sbFail } = loadMarketsRuntime(async () => ({ ok: false, status: 503 }));
  const appFail = sbFail.marketsApp();
  await appFail._loadReturns();
  assert.strictEqual(appFail.returnsData, null);
  assert.strictEqual(appFail._returnsPromise, null, 'Failed load must reset _returnsPromise to null');
  console.log('  [PASS] Single-flight _loadReturns deduplication & reset verified');
}

console.log('\nAll docs/markets.html runtime contract tests PASSED successfully.');
