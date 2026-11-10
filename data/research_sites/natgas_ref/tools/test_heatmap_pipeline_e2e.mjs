import fs from 'fs';

// Mock DOM environment
global.document = {
    getElementById(id) {
        if (!this._elements) this._elements = {};
        if (!this._elements[id]) {
            this._elements[id] = {
                id,
                classList: {
                    _classes: new Set(),
                    add(c) { this._classes.add(c); },
                    remove(c) { this._classes.delete(c); },
                    toggle(c, val) { if (val) this._classes.add(c); else this._classes.delete(c); },
                    contains(c) { return this._classes.has(c); }
                },
                style: {},
                attributes: {},
                children: [],
                setAttribute(k, v) { this.attributes[k] = v; },
                appendChild(c) { this.children.push(c); },
                innerHTML: ''
            };
        }
        return this._elements[id];
    },
    createElement(tag) {
        return {
            tag,
            classList: {
                _classes: new Set(),
                add(c) { this._classes.add(c); },
                remove(c) { this._classes.delete(c); },
                toggle(c, val) { if (val) this._classes.add(c); else this._classes.delete(c); },
                contains(c) { return this._classes.has(c); }
            },
            style: {},
            attributes: {},
            children: [],
            setAttribute(k, v) { this.attributes[k] = v; },
            appendChild(c) { this.children.push(c); }
        };
    }
};

global.window = {
    App: {
        heatMode: 'share',
        heatSide: 'all',
        heatSpotlightMode: 'peaks',
        heatPeaksThreshold: 95,
        heatDays: 90,
        heatOffset: 0,
        heatGlobalPercentiles: { share: {}, dollar: {} }
    }
};

global.CONFIG = {
    heatmapDays: 756,
    etfs: {
        'BOIL': { side: 'long', exchange: 'NYSE', leverage: '2x' },
        'HNU.TO': { side: 'long', exchange: 'TSX', leverage: '2x' },
        '3NGL.L': { side: 'long', exchange: 'LSE', leverage: '3x' },
        'KOLD': { side: 'short', exchange: 'NYSE', leverage: '2x' },
        'HND.TO': { side: 'short', exchange: 'TSX', leverage: '2x' },
        '3NGS.L': { side: 'short', exchange: 'LSE', leverage: '3x' }
    }
};

global.formatDisplayDate = d => d;

// Evaluate signals.js and charts.js into global
const chartsCode = fs.readFileSync('docs/js/charts.js', 'utf8').replace('const Charts =', 'global.Charts =');
const signalsCode = fs.readFileSync('docs/js/signals.js', 'utf8').replace('const Signals =', 'global.Signals =');

eval(chartsCode);
eval(signalsCode);

const rawData = JSON.parse(fs.readFileSync('docs/data/dashboard_data.json', 'utf8'));

// Build allMetrics with mapBar matching app.js
const mapBar = h => ({
    date: h.date || h[0],
    open: h.open != null ? h.open : (h[3] != null ? h[3] : (h.close != null ? h.close : h[1])),
    high: h.high != null ? h.high : (h[4] != null ? h[4] : (h.close != null ? h.close : h[1])),
    low: h.low != null ? h.low : (h[5] != null ? h[5] : (h.close != null ? h.close : h[1])),
    close: h.close != null ? h.close : h[1],
    volume: h.volume != null ? h.volume : h[2],
    dollarVolume: h.dollar_volume != null ? h.dollar_volume : ((h.close != null && h.volume != null) ? h.close * h.volume : 0)
});

const allMetrics = {};
for (const [ticker, etfData] of Object.entries(rawData.etfs)) {
    allMetrics[ticker] = {
        heatmapData: (etfData.history || []).slice(-756).map(mapBar)
    };
}

console.log('Testing App.computeAllGlobalPercentiles simulation...');
const filters = ['all', 'long', 'short', ...Object.keys(CONFIG.etfs)];
for (const f of filters) {
    window.App.heatGlobalPercentiles.share[f] = Signals.computeHeatGlobalPercentiles(allMetrics, 'share', f);
    window.App.heatGlobalPercentiles.dollar[f] = Signals.computeHeatGlobalPercentiles(allMetrics, 'dollar', f);
}

console.log('Precomputed series count for ALL (share):', window.App.heatGlobalPercentiles.share.all.series.length);
console.log('Precomputed series count for SHORT (share):', window.App.heatGlobalPercentiles.share.short.series.length);
console.log('Precomputed series count for LONG (share):', window.App.heatGlobalPercentiles.share.long.series.length);

// Test interactive clicks on P90, P95, P99 across all viewports
const daysToTest = [90, 126, 180, 252];
const sidesToTest = ['all', 'short', 'long', 'BOIL', 'KOLD'];
const modesToTest = ['share', 'dollar'];
const spotlightModesToTest = ['peaks', 'valleys'];

let assertions = 0;
for (const side of sidesToTest) {
    for (const m of modesToTest) {
        for (const sm of spotlightModesToTest) {
            for (const d of daysToTest) {
                // Call renderHeatCalendar for P99
                Signals.renderHeatCalendar(allMetrics, 0, m, side, sm, 99, window.App.heatGlobalPercentiles[m][side], d);
                const countBadge99 = document.getElementById('heat-peak-count').textContent;
                const count99 = parseInt(countBadge99);

                // Call renderHeatCalendar for P95
                Signals.renderHeatCalendar(allMetrics, 0, m, side, sm, 95, window.App.heatGlobalPercentiles[m][side], d);
                const countBadge95 = document.getElementById('heat-peak-count').textContent;
                const count95 = parseInt(countBadge95);

                // Call renderHeatCalendar for P90
                Signals.renderHeatCalendar(allMetrics, 0, m, side, sm, 90, window.App.heatGlobalPercentiles[m][side], d);
                const countBadge90 = document.getElementById('heat-peak-count').textContent;
                const count90 = parseInt(countBadge90);

                if (!(count90 > count95 && count95 > count99 && count99 >= 1)) {
                    throw new Error(`Monotonicity failed for side=${side}, mode=${m}, spotlight=${sm}, days=${d}: P90=${count90}, P95=${count95}, P99=${count99}`);
                }
                assertions += 3;
            }
        }
    }
}

console.log(`ALL ${assertions} MONOTONICITY ASSERTIONS PASSED WITH FLYING COLORS!`);

// Specifically inspect the user's exact case from the screenshot:
console.log('\n--- VERIFYING USER SCENARIO (SHORT ETFs, 90 DAYS, PEAKS) ---');
Signals.renderHeatCalendar(allMetrics, 0, 'share', 'short', 'peaks', 99, window.App.heatGlobalPercentiles.share.short, 90);
console.log('P99 badge:', document.getElementById('heat-peak-count').textContent);

Signals.renderHeatCalendar(allMetrics, 0, 'share', 'short', 'peaks', 95, window.App.heatGlobalPercentiles.share.short, 90);
console.log('P95 badge:', document.getElementById('heat-peak-count').textContent);

Signals.renderHeatCalendar(allMetrics, 0, 'share', 'short', 'peaks', 90, window.App.heatGlobalPercentiles.share.short, 90);
console.log('P90 badge:', document.getElementById('heat-peak-count').textContent);

console.log('\n--- VERIFYING ALL ETFs, 90 DAYS, PEAKS ---');
Signals.renderHeatCalendar(allMetrics, 0, 'share', 'all', 'peaks', 99, window.App.heatGlobalPercentiles.share.all, 90);
console.log('P99 badge:', document.getElementById('heat-peak-count').textContent);

Signals.renderHeatCalendar(allMetrics, 0, 'share', 'all', 'peaks', 95, window.App.heatGlobalPercentiles.share.all, 90);
console.log('P95 badge:', document.getElementById('heat-peak-count').textContent);

Signals.renderHeatCalendar(allMetrics, 0, 'share', 'all', 'peaks', 90, window.App.heatGlobalPercentiles.share.all, 90);
console.log('P90 badge:', document.getElementById('heat-peak-count').textContent);
