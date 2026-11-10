/* ============================================
   Configuration
   ============================================ */

const CONFIG = {
    // ETF definitions
    etfs: {
        BOIL: {
            ticker: 'BOIL',
            yahoo: 'BOIL',
            name: 'ProShares Ultra Bloomberg NG',
            side: 'long',
            pair: 'KOLD',
            leverage: '2x',
            exchange: 'NYSE'
        },
        'HNU.TO': {
            ticker: 'HNU.TO',
            yahoo: 'HNU.TO',
            name: 'BetaPro Natural Gas 2x Bull',
            side: 'long',
            pair: 'HND.TO',
            leverage: '2x',
            exchange: 'TSX'
        },
        '3NGL.L': {
            ticker: '3NGL.L',
            yahoo: '3NGL.L',
            name: 'WisdomTree NG 3x Daily Long',
            side: 'long',
            pair: '3NGS.L',
            leverage: '3x',
            exchange: 'LSE'
        },
        KOLD: {
            ticker: 'KOLD',
            yahoo: 'KOLD',
            name: 'ProShares UltraShort Bloomberg NG',
            side: 'short',
            pair: 'BOIL',
            leverage: '2x',
            exchange: 'NYSE'
        },
        'HND.TO': {
            ticker: 'HND.TO',
            yahoo: 'HND.TO',
            name: 'BetaPro Natural Gas 2x Bear',
            side: 'short',
            pair: 'HNU.TO',
            leverage: '2x',
            exchange: 'TSX'
        },
        '3NGS.L': {
            ticker: '3NGS.L',
            yahoo: '3NGS.L',
            name: 'WisdomTree NG 3x Daily Short',
            side: 'short',
            pair: '3NGL.L',
            leverage: '3x',
            exchange: 'LSE'
        }
    },

    // Pairs for IPSI calculation
    pairs: [
        { long: 'BOIL', short: 'KOLD', label: 'BOIL / KOLD' },
        { long: 'HNU.TO', short: 'HND.TO', label: 'HNU / HND' },
        { long: '3NGL.L', short: '3NGS.L', label: '3NGL / 3NGS' }
    ],

    // Lookback windows (trading days) — 5d added for fast-window layer
    windows: {
        percentile: [5, 10, 21, 63, 126, 252],
        rvol: [5, 10, 21, 63, 126, 252],
        zScore: [5, 10, 21, 63, 126, 252],
        vroc: [5, 10, 21],
        ma: [10, 21, 50, 200],
        correlation: 30
    },

    // Price-Volume Studies panel (classic volume indicators)
    volumeStudies: {
        oscillator: { fast: 5, slow: 21 },
        ma: 21,
        cmf: 21,
        ranges: [
            { key: '3m', label: '3M', days: 90 },
            { key: '6m', label: '6M', days: 180 },
            { key: '1y', label: '1Y', days: 365 },
            { key: '2y', label: '2Y', days: 730 },
            { key: '5y', label: '5Y', days: 1826 },
            { key: '10y', label: '10Y', days: 3653 },
            { key: 'all', label: 'ALL', days: null }
        ]
    },

    // Alert thresholds
    thresholds: {
        cvi:  { elevated: 60, high: 75, critical: 90, extreme: 95 },
        vcvi: { elevated: 55, high: 72, critical: 88, extreme: 95 },  // vol-adjusted CVI
        vps:  { elevated: 50, high: 70, critical: 85, extreme: 95 },
        rvol: { elevated: 1.5, high: 2.5, critical: 3.5, extreme: 5.0 },
        zScore: { elevated: 1.5, high: 2.0, critical: 2.5, extreme: 3.0 },
        percentile: { elevated: 50, high: 75, critical: 90, extreme: 95 },
        ipsi: { elevated: 1.5, high: 2.0, stress: 2.5, critical: 3.5 },
        mwca_threshold: 90,  // Percentile threshold for each window to trigger MWCA
        // Volatility modelling thresholds
        volRegime: { low: 30, normal: 60, high: 80, extreme: 92 },
        // HV term structure: HV10 / HV63
        // < 0.65 = calming strongly  |  > 1.35 = accelerating strongly
        hvTermStructure: { calming: 0.65, stable_low: 0.85, stable_high: 1.15, accelerating: 1.35 },
        vov: { elevated: 40, high: 60, critical: 80, extreme: 100 },   // vol-of-vol (%)
        atrBreakout: { elevated: 1.2, high: 1.5, critical: 2.0, extreme: 3.0 },  // × ATR

        // Fast spike detection (5d VCVI window)
        fastVcvi: { threshold: 45, atrMult: 2.0 },
        // Gas price level gate — uses seasonal z-score (not raw percentile)
        // Because nat gas is so volatile, "high vs 2yr" is meaningless.
        // Gate fires when gas is ≥1.5σ above/below its own seasonal monthly norm.
        ngPriceGate: { zThreshold: 1.5 },

        // NG=F Volatility Regime — 3-tier classification
        // Natural gas can enter "ultra-volatile" outlier regimes where typical
        // signal patterns may not hold (2022 ~$9/MMBtu; Jan 2026 >$7/MMBtu).
        ngRegime: {
            extreme:  { priceMin: 7.0, zMin: 2.5, hvPctMin: 90,
                         color: 'var(--purple)', label: 'EXTREME REGIME',
                         note: 'Ultra-volatile outlier — signal patterns may not behave as expected' },
            elevated: { priceMin: 4.5, zMin: 1.5, hvPctMin: 70,
                         color: 'var(--red)',    label: 'ELEVATED STRESS',
                         note: 'Heightened conditions — interpret signals with added caution' },
            normal:   { color: 'rgba(255, 255, 255, 0.85)', label: 'NORMAL',
                         note: 'Typical conditions — signals behave as expected' },
        }
    },

    // VPS weights — 5-component including inverse vol regime
    vpsWeights: { rvol: 0.25, zScore: 0.20, percentile: 0.25, vroc: 0.10, volRegime: 0.20 },

    // Leveraged ETF annual decay rates for decay-correction display
    etfDecayRates: {
        'BOIL': 0.35, 'KOLD': 0.35,
        'HNU.TO': 0.40, 'HND.TO': 0.40,
        '3NGL.L': 0.55, '3NGS.L': 0.55
    },

    // Season display config
    seasonDisplay: {
        winter: { emoji: '❄', label: 'WINTER', color: '#60a8f8' },
        spring: { emoji: '✿', label: 'SPRING', color: '#6ddc8b' },
        summer: { emoji: '☀', label: 'SUMMER', color: '#f5c542' },
        fall:   { emoji: '◈', label: 'FALL',   color: '#f5a742' }
    },

    // Data source
    dataUrl: 'data/dashboard_data.json',
    proxyUrl: 'https://query1.finance.yahoo.com/v8/finance/chart/',

    // Refresh interval (milliseconds)
    refreshInterval: 60000, // 1 minute when market open
    refreshIntervalPrePost: 300000, // 5 minutes during pre-market and after-hours
    refreshIntervalClosed: 1800000, // 30 minutes when closed, weekend, or holiday

    // History length for charts
    sparklineDays: 30,
    volumeBarDays: 30,
    heatmapDays: 756,

    // Yahoo Finance chart API period
    fetchPeriod: '2y',
    fetchInterval: '1d',

    // Format ISO date (YYYY-MM-DD) into readable ordinal string (e.g. 19th May, 2026)
    formatDateOrdinal(iso) {
        if (!iso || typeof iso !== 'string') return iso || '—';
        const parts = iso.split('-');
        if (parts.length < 3) return iso;
        const y = parts[0];
        const mIdx = parseInt(parts[1], 10) - 1;
        const dNum = parseInt(parts[2].slice(0, 2), 10);
        if (isNaN(dNum) || isNaN(mIdx) || mIdx < 0 || mIdx > 11) return iso;
        const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
        const m = months[mIdx];
        const ordSuffix = (dNum >= 11 && dNum <= 13) ? 'th' : ['th', 'st', 'nd', 'rd', 'th', 'th', 'th', 'th', 'th', 'th'][dNum % 10] || 'th';
        return `${dNum}${ordSuffix} ${m}, ${y}`;
    }
};

function formatDisplayDate(iso) {
    if (typeof CONFIG !== 'undefined' && typeof CONFIG.formatDateOrdinal === 'function') {
        return CONFIG.formatDateOrdinal(iso);
    }
    if (!iso || typeof iso !== 'string') return iso || '—';
    const parts = iso.split('-');
    if (parts.length < 3) return iso;
    const y = parts[0];
    const mIdx = parseInt(parts[1], 10) - 1;
    const dNum = parseInt(parts[2].slice(0, 2), 10);
    if (isNaN(dNum) || isNaN(mIdx) || mIdx < 0 || mIdx > 11) return iso;
    const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    const m = months[mIdx];
    const ordSuffix = (dNum >= 11 && dNum <= 13) ? 'th' : ['th', 'st', 'nd', 'rd', 'th', 'th', 'th', 'th', 'th', 'th'][dNum % 10] || 'th';
    return `${dNum}${ordSuffix} ${m}, ${y}`;
}
