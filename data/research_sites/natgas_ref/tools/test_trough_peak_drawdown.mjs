import assert from 'assert';
import fs from 'fs';

console.log('Running Institutional Drawdown Analysis Unit Tests...');

// 1. Extract functions from docs/trough-peak.html or test their exact logic
const html = fs.readFileSync('docs/trough-peak.html', 'utf8');

// Helper to extract function body from HTML
function extractFn(fnName) {
    const regex = new RegExp(`function\\s+${fnName}\\s*\\([^)]*\\)\\s*\\{`);
    const match = html.match(regex);
    if (!match) {
        throw new Error(`Function ${fnName} not found in docs/trough-peak.html`);
    }
    const startIdx = match.index;
    let openBraces = 0;
    let endIdx = -1;
    let started = false;
    for (let i = startIdx; i < html.length; i++) {
        if (html[i] === '{') {
            openBraces++;
            started = true;
        } else if (html[i] === '}') {
            openBraces--;
            if (started && openBraces === 0) {
                endIdx = i + 1;
                break;
            }
        }
    }
    if (endIdx === -1) {
        throw new Error(`Could not find closing brace for ${fnName}`);
    }
    const code = html.slice(startIdx, endIdx);
    return new Function(`return (${code})`)();
}

const computeRolling52WDD = extractFn('computeRolling52WDD');
const computeATHDD = extractFn('computeATHDD');
const computeUnderwaterMetrics = extractFn('computeUnderwaterMetrics');
const decimateDDPtsPrecise = extractFn('decimateDDPtsPrecise');
const ddHexToRgba = extractFn('ddHexToRgba');

// Test Suite 1: computeRolling52WDD
console.log('Testing computeRolling52WDD...');
{
    // Generate dates spanning 500 days
    const baseMs = Date.UTC(2023, 0, 1);
    const dayMs = 86400 * 1000;
    const dates = [];
    const closes = [];

    for (let i = 0; i < 500; i++) {
        dates.push(new Date(baseMs + i * dayMs).toISOString().slice(0, 10));
        // Day 100 has peak of 100.
        // Day 470 has price 50 (370 days later -> Day 100 peak of 100 must have expired by 364 days lookback)
        if (i === 100) closes.push(100);
        else if (i < 100) closes.push(10 + i * 0.1);
        else if (i <= 464) closes.push(40); // 100 to 464 is 364 days.
        else closes.push(50); // after 464 days, peak 100 is evicted; current close 50 should be new peak!
    }

    const res = computeRolling52WDD(dates, closes);
    assert.strictEqual(res.ts.length, 500);
    assert.strictEqual(res.dd.length, 500);
    assert.strictEqual(res.peaks.length, 500);

    // At day 100, close is 100, peak is 100, dd is 0
    assert.strictEqual(res.peaks[100], 100);
    assert.strictEqual(res.dd[100], 0);

    // At day 200, close is 40, peak is 100, dd is -60%
    assert.strictEqual(res.peaks[200], 100);
    assert.strictEqual(Math.round(res.dd[200]), -60);

    // At day 464: time diff from day 100 is exactly 364 days -> still within window
    assert.strictEqual(res.peaks[464], 100);

    // At day 466: time diff from day 100 is 366 days (> 364 days = 31,449,600,000 ms)
    // Day 100 has been evicted! New maximum is 50.
    assert.strictEqual(res.peaks[466], 50);
    assert.strictEqual(res.dd[466], 0);

    // Naive verification: compare against naive sliding window over all 500 days
    const WIN = 52 * 7 * 86400 * 1000;
    for (let i = 0; i < 500; i++) {
        let naivePeak = -Infinity;
        let naiveIdx = -1;
        for (let j = 0; j <= i; j++) {
            if (res.ts[i] - res.ts[j] <= WIN) {
                if (closes[j] >= naivePeak) {
                    naivePeak = closes[j];
                    naiveIdx = j;
                }
            }
        }
        assert.strictEqual(res.peaks[i], naivePeak, `Mismatch at day ${i}`);
        assert.strictEqual(res.peakIndices[i], naiveIdx, `Index mismatch at day ${i}`);
        assert(res.dd[i] <= 0.000001, `Drawdown must be non-positive at day ${i}`);
    }
}
console.log('Passed computeRolling52WDD verification.');

// Test Suite 2: Leap Year Lookback Verification
console.log('Testing Leap Year 364-day eviction lookback...');
{
    // Leap year 2024 has 366 days in calendar year. 364 days lookback should cleanly evict exactly 52 weeks ago.
    const dates = [];
    const closes = [];
    const startMs = Date.UTC(2024, 0, 1); // Jan 1, 2024
    for (let i = 0; i < 400; i++) {
        dates.push(new Date(startMs + i * 86400 * 1000).toISOString().slice(0, 10));
        closes.push(i === 0 ? 120 : 60);
    }
    const res = computeRolling52WDD(dates, closes);
    // At day 363 (363 days after Jan 1), Jan 1 peak 120 is active
    assert.strictEqual(res.peaks[363], 120);
    assert.strictEqual(res.dd[363], -50);
    // At day 365 (365 days after Jan 1), Jan 1 is evicted (365 > 364 days)
    assert.strictEqual(res.peaks[365], 60);
    assert.strictEqual(res.dd[365], 0);
}
console.log('Passed Leap Year lookback verification.');

// Test Suite 3: computeATHDD
console.log('Testing computeATHDD...');
{
    const dates = ['2020-01-01', '2020-01-02', '2020-01-03', '2020-01-04', '2020-01-05'];
    const closes = [100, 80, 150, 75, 150];
    const res = computeATHDD(dates, closes);

    assert.deepStrictEqual(res.peaks, [100, 100, 150, 150, 150]);
    assert.deepStrictEqual(res.peakIndices, [0, 0, 2, 2, 4]); // 150 on day 4 sets/matches peak
    assert.strictEqual(res.dd[0], 0);
    assert.strictEqual(res.dd[1], -20);
    assert.strictEqual(res.dd[2], 0);
    assert.strictEqual(res.dd[3], -50);
    assert.strictEqual(res.dd[4], 0);
}
console.log('Passed computeATHDD verification.');

// Test Suite 4: computeUnderwaterMetrics
console.log('Testing computeUnderwaterMetrics...');
{
    const pts = [
        { t: 1000000, dd: 0, close: 100, peak: 100, peakTs: 1000000, peakIdx: 0 },
        { t: 1086400000, dd: -20, close: 80, peak: 100, peakTs: 1000000, peakIdx: 0 },
        { t: 1172800000, dd: -50, close: 50, peak: 100, peakTs: 1000000, peakIdx: 0 },
        { t: 1259200000, dd: -80, close: 20, peak: 100, peakTs: 1000000, peakIdx: 0 }, // Trough
        { t: 1345600000, dd: -40, close: 60, peak: 100, peakTs: 1000000, peakIdx: 0 }  // Current
    ];

    const m = computeUnderwaterMetrics(pts);
    assert.strictEqual(m.currentDD, -40);
    assert.strictEqual(m.maxDD, -80);
    assert.strictEqual(m.troughPrice, 20);
    assert.strictEqual(m.troughDate, 1259200000);
    assert.strictEqual(m.peakPrice, 100);
    assert.strictEqual(m.currentPrice, 60);

    // Days underwater: (1345600000 - 1000000) / 86400000 = 15.56 => 16 days
    assert.strictEqual(m.daysUnderwater, 16);

    // Break-even gain required from current DD (-40%):
    // (-(-40) / (100 - 40)) * 100 = 40 / 60 * 100 = 66.666...%
    assert(Math.abs(m.breakEvenGain - (40 / 60 * 100)) < 1e-6);

    // Break-even gain from max DD (-80%):
    // 80 / 20 * 100 = 400%
    assert.strictEqual(m.maxDDBreakEvenGain, 400);

    // Recovery from trough: (60 - 20) / 20 * 100 = 200%
    assert.strictEqual(m.recoveryFromTrough, 200);

    // Test at ATH (0% DD):
    const ptsATH = [{ t: 2000000, dd: 0, close: 100, peak: 100, peakTs: 2000000, peakIdx: 0 }];
    const mATH = computeUnderwaterMetrics(ptsATH);
    assert.strictEqual(mATH.currentDD, 0);
    assert.strictEqual(mATH.daysUnderwater, 0);
    assert.strictEqual(mATH.breakEvenGain, 0);
}
console.log('Passed computeUnderwaterMetrics verification.');

// Test Suite 5: decimateDDPtsPrecise (Boundary-Preserving Decimation)
console.log('Testing decimateDDPtsPrecise...');
{
    const pts = [];
    for (let i = 0; i < 1000; i++) {
        pts.push({
            t: 1000 + i * 10,
            dd: -Math.abs(Math.sin(i / 10) * 80),
            close: 50 + Math.sin(i / 10) * 20
        });
    }

    const chartW = 300;
    const xOf = t => ((t - 1000) / (pts[pts.length - 1].t - 1000)) * chartW;

    const decimated = decimateDDPtsPrecise(pts, xOf, chartW);
    assert(decimated.length < pts.length, 'Should reduce points for performance');
    assert.strictEqual(decimated[0].t, pts[0].t, 'First boundary point must be preserved');
    assert.strictEqual(decimated[decimated.length - 1].t, pts[pts.length - 1].t, 'Last boundary point must be preserved');

    // Chronological order invariant
    for (let i = 0; i < decimated.length - 1; i++) {
        assert(decimated[i].t <= decimated[i + 1].t, 'Decimated points must remain in non-decreasing chronological order');
    }

    // Minimum DD must be preserved
    const origMinDD = Math.min(...pts.map(p => p.dd));
    const decMinDD = Math.min(...decimated.map(p => p.dd));
    assert(Math.abs(origMinDD - decMinDD) < 1e-6, 'Trough minimum drawdown must be preserved in decimation');
}
console.log('Passed decimateDDPtsPrecise verification.');

// Test Suite 6: ddHexToRgba
console.log('Testing ddHexToRgba...');
{
    assert.strictEqual(ddHexToRgba('#ffffff', 0.5), 'rgba(255, 255, 255, 0.5)');
    assert.strictEqual(ddHexToRgba('#c07828', 0.28), 'rgba(192, 120, 40, 0.28)');
    assert.strictEqual(ddHexToRgba('#fff', 1), 'rgba(255, 255, 255, 1)');
}
console.log('Passed ddHexToRgba verification.');

// Test Suite 7: HTML Section Order & DOM Invariants
console.log('Testing HTML Section Order & DOM Invariants in docs/trough-peak.html...');
{
    const chartSectionIdx = html.indexOf('<!-- ENHANCED CHART SECTION -->');
    const ddSectionIdx = html.indexOf('<!-- ETF DRAWDOWN ANALYSIS -->');
    const vpSectionIdx = html.indexOf('<!-- VOLUME PROFILE -->');
    const tableSectionIdx = html.indexOf('<!-- CYCLE DETAIL TABLE -->');
    const matrixSectionIdx = html.indexOf('<!-- YEARLY FREQUENCY MATRIX -->');
    const calcSectionIdx = html.indexOf('<!-- ETF CYCLE TAX & COMPOUNDING CALCULATOR -->');

    assert(chartSectionIdx !== -1, 'ENHANCED CHART SECTION found');
    assert(ddSectionIdx !== -1, 'ETF DRAWDOWN ANALYSIS found');
    assert.strictEqual(vpSectionIdx, -1, 'VOLUME PROFILE must be cleanly removed from trough-peak.html');
    assert(tableSectionIdx !== -1, 'CYCLE DETAIL TABLE found');
    assert(matrixSectionIdx !== -1, 'YEARLY FREQUENCY MATRIX found');
    assert(calcSectionIdx !== -1, 'CALCULATOR found');

    // Order assertions
    assert(chartSectionIdx < ddSectionIdx, 'Price & Cycle Map must precede ETF Drawdown Analysis');
    assert(ddSectionIdx < tableSectionIdx, 'ETF Drawdown Analysis must directly precede Cycle Detail Table');
    assert(tableSectionIdx < matrixSectionIdx, 'Cycle Detail Table must precede Yearly Frequency Matrix');
    assert(matrixSectionIdx < calcSectionIdx, 'Yearly Frequency Matrix must precede Calculator');

    // Old block must NOT exist
    assert(!html.includes('<!-- ETF DRAWDOWN CHART -->'), 'Old ETF Drawdown Chart comment must be removed');
    assert(!html.includes('id="vp-panel"'), '#vp-panel must be removed');
    assert(!html.includes('borderTopColor'), 'No borderTopColor manipulation allowed in DD KPI cards');

    // Required Elements
    const requiredIds = [
        'dd-panel',
        'dd-badge',
        'dd-mode-tabs',
        'dd-horizon-group',
        'download-dd-csv',
        'dd-kpi-grid',
        'dd-kpi-cur',
        'dd-kpi-mdd',
        'dd-kpi-peak',
        'dd-kpi-underwater',
        'drawdown-canvas',
        'dd-tooltip',
        'dd-etf-chips',
        'dd-footer-desc'
    ];

    for (const id of requiredIds) {
        assert(html.includes(`id="${id}"`), `Required DOM element id="${id}" must exist`);
    }

    assert(html.includes('data-mode="rolling_52w"'), 'data-mode="rolling_52w" must exist');
    assert(html.includes('data-mode="ath"'), 'data-mode="ath" must exist');
}
console.log('Passed HTML structure and layout order verification.');

// Test Suite 8: Zero Emojis and Zero Forbidden Double-Dashes
console.log('Testing Zero Emojis and Zero Forbidden Double-Dashes constraints...');
{
    const emojiRegex = /[\u{1F300}-\u{1F9FF}]|[\u{2600}-\u{26FF}]|[\u{2700}-\u{27BF}]/u;
    const lines = html.split('\n');

    let ddSectionActive = false;
    let emojiCount = 0;
    let doubleDashCount = 0;

    lines.forEach((line, idx) => {
        const lineNum = idx + 1;
        if (line.includes('ETF DRAWDOWN ANALYSIS') || line.includes('Drawdown Chart Renderer')) {
            ddSectionActive = true;
        }

        // Check for emojis in modified/added drawdown code
        if (line.includes('dd-') || line.includes('Drawdown') || line.includes('computeRolling52WDD')) {
            if (emojiRegex.test(line)) {
                console.error(`Emoji violation at line ${lineNum}: ${line.trim()}`);
                emojiCount++;
            }

            // Check forbidden double-dash
            if (line.includes('--')) {
                let cleaned = line;
                cleaned = cleaned.replace(/var\(--[a-zA-Z0-9_-]+\)/g, '');
                cleaned = cleaned.replace(/--[a-zA-Z0-9_-]+\s*:/g, '');
                cleaned = cleaned.replace(/[a-zA-Z0-9_]\s*--/g, '');
                cleaned = cleaned.replace(/--\s*[a-zA-Z0-9_]/g, '');
                if (cleaned.includes('--')) {
                    console.error(`Forbidden double-dash at line ${lineNum}: ${line.trim()}`);
                    doubleDashCount++;
                }
            }
        }
    });

    assert.strictEqual(emojiCount, 0, 'No emojis allowed in Drawdown implementation');
    assert.strictEqual(doubleDashCount, 0, 'No forbidden double-dashes allowed in Drawdown implementation');
}
console.log('Passed constraint verification (Zero emojis, Zero forbidden double-dashes).');

console.log('ALL UNIT TESTS PASSED SUCCESSFULLY!');
