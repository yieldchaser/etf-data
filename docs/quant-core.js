/**
 * QuantCore - Mathematical and Taxonomy Engine for Leveraged Fund Flows
 *
 * Implements high-precision numerical algorithms, time-versioned taxonomy,
 * and microstructure estimators across browser and Node.js runtimes.
 *
 * Architecture Compliance:
 * - Pure algebraic and numerical routines with zero external runtime dependencies.
 * - Time-versioned leverage schedules and signed market beta attribution.
 * - Reconstructs Total Return series from authoritatively reported yields and NAVs.
 * - Zero emojis, zero banned terminology, strictly factual financial copy.
 */

(function (root, factory) {
  if (typeof define === 'function' && define.amd) {
    define([], factory);
  } else if (typeof module === 'object' && module.exports) {
    module.exports = factory();
  } else {
    root.QuantCore = factory();
  }
}(typeof self !== 'undefined' ? self : this, function () {
  'use strict';

  // ── Canonical Underlying Identification & Taxonomy ──────────────────────

  const UNDERLYING_CANONICAL_MAP = [
    { id: 'NVDA', patterns: [/nvidia/i, /\bnvda\b/i], label: 'NVIDIA (NVDA)' },
    { id: 'TSLA', patterns: [/tesla/i, /\btsla\b/i], label: 'Tesla (TSLA)' },
    { id: 'NDX', patterns: [/nasdaq[- ]*100/i, /\bndx\b/i, /\bqqq\b/i, /\bqqqm\b/i, /\bsqqq\b/i, /\bqld\b/i], label: 'NASDAQ-100' },
    { id: 'SPX_QUALITY', patterns: [/s&p\s*500\s*quality/i, /\bsphq\b/i], label: 'S&P 500 Quality' },
    { id: 'SPX_MOMENTUM', patterns: [/s&p\s*500\s*momentum/i, /\bspmo\b/i], label: 'S&P 500 Momentum' },
    { id: 'SPX', patterns: [/s&p\s*500(?!\s*vix)/i, /\bspx\b/i, /\bspy\b/i, /\bspxl\b/i, /\bupro\b/i, /\bsso\b/i, /\bspxs\b/i], label: 'S&P 500' },
    { id: 'RUT', patterns: [/russell\s*2000/i, /\brut\b/i, /\btna\b/i, /\btza\b/i, /\burty\b/i], label: 'Russell 2000' },
    { id: 'UST_20Y', patterns: [/20\+\s*year\s*treasury/i, /ice\s*u\.s\.\s*treasury\s*20\+/i, /\btlt\b/i, /\btmf\b/i, /\bubt\b/i, /\btmv\b/i], label: '20+ Year Treasury' },
    { id: 'UST_7_10Y', patterns: [/7[- ]10\s*year\s*treasury/i, /\btyd\b/i], label: '7-10 Year Treasury' },
    { id: 'SEMIS', patterns: [/semiconductor/i, /\bmvis\b.*semi/i, /\bsoxl\b/i, /\bsoxs\b/i, /\bsmh\b/i, /\bsmhx\b/i, /\busd\b/i], label: 'Semiconductors' },
    { id: 'VIX_ST_FUTURES', patterns: [/vix/i, /\buvix\b/i, /\buvxy\b/i, /\bsvix\b/i, /\bsvxy\b/i], label: 'VIX Short-Term Futures' },
    { id: 'MSTR', patterns: [/microstrategy/i, /\bmstr\b/i, /\bmstu\b/i, /\bmstx\b/i, /\bmstz\b/i, /\bsmst\b/i], label: 'MicroStrategy (MSTR)' },
    { id: 'PLTR', patterns: [/palantir/i, /\bpltr\b/i, /\bpltu\b/i, /\bpltd\b/i, /\bptir\b/i], label: 'Palantir (PLTR)' },
    { id: 'BTC', patterns: [/bitcoin/i, /\bbtc\b/i, /\bbitu\b/i, /\bbitx\b/i, /\bibit\b/i], label: 'Bitcoin (BTC)' },
    { id: 'ETH', patterns: [/ether/i, /\beth\b/i, /\bethu\b/i], label: 'Ether (ETH)' },
    { id: 'CRUDE_OIL', patterns: [/crude\s*oil/i, /\buco\b/i, /\bsco\b/i], label: 'Crude Oil' },
    { id: 'CHINA_50', patterns: [/china\s*50/i, /\byinn\b/i, /\byang\b/i], label: 'China 50' },
    { id: 'CHINA_A_SHARES', patterns: [/china\s*a[- ]*shares/i, /\bchau\b/i], label: 'China A-Shares' },
    { id: 'CHINA_INTERNET', patterns: [/china\s*internet/i, /\bcweb\b/i], label: 'China Internet' },
    { id: 'GOLD_MINERS', patterns: [/gold\s*miners(?!\s*junior)/i, /\bgdxu\b/i, /\bnugt\b/i], label: 'Gold Miners' },
    { id: 'JR_GOLD_MINERS', patterns: [/junior\s*gold\s*miners/i, /\bjnug\b/i], label: 'Junior Gold Miners' },
    { id: 'GOLD', patterns: [/\bgold\b/i, /\bugl\b/i], label: 'Gold' },
    { id: 'SILVER', patterns: [/silver/i, /\bagq\b/i], label: 'Silver' },
    { id: 'ARKK_INNOV', patterns: [/disruptive\s*innovation/i, /arkk/i, /\btark\b/i], label: 'ARKK Innovation' },
    { id: 'COIN', patterns: [/coinbase/i, /\bcoin\b/i, /\bconi\b/i, /\bconl\b/i], label: 'Coinbase (COIN)' },
    { id: 'MU', patterns: [/micron/i, /\bmu\b/i, /\bmull\b/i, /\bmuu\b/i], label: 'Micron Technology (MU)' },
    { id: 'BIOTECH_NDX', patterns: [/nasdaq\s*biotechnology/i, /\bbib\b/i], label: 'Nasdaq Biotechnology' },
    { id: 'BIOTECH_SP', patterns: [/s&p\s*biotechnology/i, /\blabu\b/i, /\bxbi\b/i], label: 'S&P Biotechnology' },
    { id: 'BIOTECH', patterns: [/biotech/i], label: 'Biotech' },
    { id: 'US_FIN', patterns: [/financials/i, /\bfas\b/i, /\buyg\b/i], label: 'Financials' },
    { id: 'US_TECH', patterns: [/technology\s*select/i, /u\.s\.\s*technology/i, /\btecl\b/i, /\brom\b/i], label: 'Technology' },
    { id: 'US_REIT', patterns: [/reit/i, /real\s*estate/i, /\bdrn\b/i, /\bure\b/i], label: 'Real Estate' },
    { id: 'ENERGY', patterns: [/energy\s*select/i, /\berx\b/i], label: 'Energy' },
    { id: 'OIL_GAS', patterns: [/oil\s*&\s*gas/i, /\bdig\b/i], label: 'Oil & Gas' },
    { id: 'HEALTHCARE', patterns: [/health\s*care/i, /\bcure\b/i], label: 'Health Care' },
    { id: 'CONSUMER_DISC', patterns: [/consumer\s*discretionary/i, /\bwant\b/i], label: 'Consumer Discretionary' },
    { id: 'COMMERCE_SECTOR', patterns: [/\bretl\b/i], label: 'Equal Weight Commerce' },
    { id: 'AEROSPACE_DEFENSE', patterns: [/aerospace/i, /\bdfen\b/i], label: 'Aerospace & Defense' },
    { id: 'HOMEBUILDERS', patterns: [/homebuilders/i, /\bnail\b/i], label: 'Homebuilders' },
    { id: 'REGIONAL_BANKING', patterns: [/regional\s*banking/i, /\bdpst\b/i], label: 'Regional Banking' },
    { id: 'DJ30', patterns: [/dow\s*jones\s*30/i, /\budow\b/i], label: 'Dow Jones 30' },
    { id: 'DJ_INTERNET', patterns: [/dow\s*jones\s*internet/i, /\bwebl\b/i], label: 'Dow Jones Internet' },
    { id: 'MAG7', patterns: [/magnificent\s*seven/i, /\bmags\b/i, /\bmagx\b/i, /\bqqqu\b/i], label: 'Magnificent Seven' },
    { id: 'AAPL', patterns: [/apple/i, /\baapl\b/i, /\baapu\b/i], label: 'Apple (AAPL)' },
    { id: 'AMZN', patterns: [/amazon/i, /\bamzn\b/i, /\bamzu\b/i], label: 'Amazon (AMZN)' },
    { id: 'GOOGL', patterns: [/alphabet/i, /\bgoogl\b/i, /\bggll\b/i, /\bgoox\b/i], label: 'Alphabet (GOOGL)' },
    { id: 'MSFT', patterns: [/microsoft/i, /\bmsft\b/i, /\bmsfl\b/i, /\bmsfu\b/i], label: 'Microsoft (MSFT)' },
    { id: 'META', patterns: [/meta\s*platforms/i, /\bmeta\b/i, /\bfbl\b/i, /\bmetu\b/i], label: 'Meta Platforms (META)' }
  ];

  function canonicalUnderlyingId(rawUnderlying, ticker) {
    const raw = String(rawUnderlying || '').trim();
    const tk = String(ticker || '').trim().toUpperCase();

    // Specific ticker overrides for ambiguous categories
    if (tk === 'RETL') return 'COMMERCE_SECTOR';

    for (let i = 0; i < UNDERLYING_CANONICAL_MAP.length; i++) {
      const entry = UNDERLYING_CANONICAL_MAP[i];
      for (let p = 0; p < entry.patterns.length; p++) {
        if (entry.patterns[p].test(raw) || (tk && entry.patterns[p].test(tk))) {
          return entry.id;
        }
      }
    }

    // Default: strip non-alphanumeric trailing descriptions
    const cleaned = raw.replace(/\s*\([^)]*\)\s*$/, '').trim().toUpperCase().replace(/[^A-Z0-9]+/g, '_');
    return cleaned || (tk || 'UNKNOWN');
  }

  function canonicalUnderlyingLabel(id, raw) {
    const match = UNDERLYING_CANONICAL_MAP.find(e => e.id === id);
    if (match) return match.label;
    if (raw) {
      return String(raw).replace(/\s*\([^)]*\)\s*$/, '').trim();
    }
    return String(id || 'Unknown');
  }

  // ── Time-Versioned Leverage & Beta Attribution ──────────────────────────

  function leverageAtDate(ticker, dateStr, defaultLev) {
    const tk = String(ticker || '').trim().toUpperCase();
    const dt = String(dateStr || '').slice(0, 10);
    const def = Number(defaultLev) || 1.0;

    if (tk === 'UVXY') {
      return (!dt || dt < '2018-02-28') ? 2.0 : 1.5;
    }
    if (tk === 'SVXY') {
      return (!dt || dt < '2018-02-28') ? -1.0 : -0.5;
    }
    return def;
  }

  function marketBetaPolarity(ticker, leverageValue) {
    const tk = String(ticker || '').trim().toUpperCase();
    const lev = Number(leverageValue) || 1.0;

    if (tk === 'UVIX' || tk === 'UVXY') return -1;
    if (tk === 'SVIX' || tk === 'SVXY') return 1;
    if (lev > 0) return 1;
    if (lev < 0) return -1;
    return 1;
  }

  function classifyPairMatch(instA, instB, dateStr) {
    if (!instA || !instB) return null;
    const undA = canonicalUnderlyingId(instA.underlying, instA.ticker);
    const undB = canonicalUnderlyingId(instB.underlying, instB.ticker);
    if (!undA || undA !== undB) return null;

    const levA = leverageAtDate(instA.ticker, dateStr, instA.leverage_value);
    const levB = leverageAtDate(instB.ticker, dateStr, instB.leverage_value);

    // Must be opposite directions
    if (levA * levB >= 0) return null;

    if (Math.abs(Math.abs(levA) - Math.abs(levB)) < 1e-6) {
      return 'symmetric';
    }
    return 'asymmetric_hedge';
  }

  function areSymmetricPair(instA, instB, dateStr) {
    return classifyPairMatch(instA, instB, dateStr) === 'symmetric';
  }

  // ── Total Return Index Series Reconstruction ────────────────────────────

  function reconstructTotalReturnSeries(bars, flowRecords) {
    if (!Array.isArray(bars) || bars.length === 0) return [];
    const N = bars.length;

    // Check if bars already carry valid Total Return Close (tc)
    const hasPrecomputedTC = bars.every(b => typeof b.tc === 'number' && Number.isFinite(b.tc) && b.tc > 0);
    if (hasPrecomputedTC) {
      return bars.map(b => b.tc);
    }

    // Build date-indexed map of flow records
    const flowMap = new Map();
    if (Array.isArray(flowRecords)) {
      for (let i = 0; i < flowRecords.length; i++) {
        const r = flowRecords[i];
        if (r && r.date) flowMap.set(r.date, r);
      }
    } else if (flowRecords && typeof flowRecords === 'object') {
      for (const k of Object.keys(flowRecords)) {
        flowMap.set(k, flowRecords[k]);
      }
    }

    // Reconstruct backwards from latest bar N-1 with A_TR[N-1] = 1.0
    const A = new Float64Array(N);
    A[N - 1] = 1.0;

    for (let t = N - 1; t >= 1; t--) {
      const bCurr = bars[t];
      const bPrev = bars[t - 1];
      const dCurr = bCurr.d || bCurr.date;
      const dPrev = bPrev.d || bPrev.date;

      const fCurr = flowMap.get(dCurr);
      const fPrev = flowMap.get(dPrev);

      let dYield = 0.0;

      if (fCurr && fPrev && typeof fCurr.performance === 'number' && typeof fCurr.nav === 'number' && typeof fPrev.nav === 'number') {
        const perf = fCurr.performance;
        const navCurr = fCurr.nav;
        const navPrev = fPrev.nav;

        if (perf > -1.0 && navCurr > 0 && navPrev > 0) {
          const rPerf = Math.log1p(perf);
          const rNav = Math.log(navCurr / navPrev);
          const rawYield = rPerf - rNav;

          // Gate against split jumps and extreme outliers
          if (rawYield > 0.0 && rawYield <= 0.15 && Math.abs(rawYield) <= 0.15) {
            dYield = rawYield;
          }
        }
      }

      A[t - 1] = A[t] * Math.exp(-dYield);
    }

    const triSeries = new Float64Array(N);
    for (let t = 0; t < N; t++) {
      const close = Number(bars[t].c ?? bars[t].close) || 1.0;
      triSeries[t] = close * A[t];
    }

    return Array.from(triSeries);
  }

  // ── Numerical Algorithms & Microstructure Estimators ────────────────────

  function yangZhangVolatility(bars, windowSize = 21, annualized = false) {
    const N = Array.isArray(bars) ? bars.length : 0;
    const n = Math.max(5, Math.trunc(windowSize || 21));
    const result = new Array(N).fill(null);
    if (N < n + 1) return result;

    const k = 0.34 / (1.34 + (n + 1) / (n - 1));
    const annualFactor = annualized ? Math.sqrt(252) : 1.0;

    for (let t = n; t < N; t++) {
      let sumO = 0;
      let sumC = 0;
      let sumRS = 0;
      const oArr = new Float64Array(n);
      const cArr = new Float64Array(n);

      let validWindow = true;
      for (let j = 0; j < n; j++) {
        const idx = t - n + 1 + j;
        const curr = bars[idx];
        const prev = bars[idx - 1];

        const o = Number(curr.o ?? curr.open);
        const h = Number(curr.h ?? curr.high);
        const l = Number(curr.l ?? curr.low);
        const c = Number(curr.c ?? curr.close);
        const cPrev = Number(prev.c ?? prev.close);

        if (o <= 0 || h <= 0 || l <= 0 || c <= 0 || cPrev <= 0) {
          validWindow = false;
          break;
        }

        const logO = Math.log(o / cPrev);
        const logC = Math.log(c / o);
        const logU = Math.log(h / o);
        const logD = Math.log(l / o);

        const rs = logU * (logU - logC) + logD * (logD - logC);

        oArr[j] = logO;
        cArr[j] = logC;
        sumO += logO;
        sumC += logC;
        sumRS += rs;
      }

      if (!validWindow) continue;

      const meanO = sumO / n;
      const meanC = sumC / n;

      let varO = 0;
      let varC = 0;
      for (let j = 0; j < n; j++) {
        varO += (oArr[j] - meanO) ** 2;
        varC += (cArr[j] - meanC) ** 2;
      }
      varO /= (n - 1);
      varC /= (n - 1);

      const meanRS = sumRS / n;
      const yzVar = varO + k * varC + (1 - k) * meanRS;
      const yzVol = Math.sqrt(Math.max(1e-12, yzVar)) * annualFactor;
      result[t] = Number.isFinite(yzVol) ? yzVol : null;
    }

    return result;
  }

  function studentT4CDF(z) {
    if (!Number.isFinite(z)) return z < 0 ? 0.0 : 1.0;
    const z2 = z * z;
    return 0.5 + (z * (6 + z2)) / (2 * Math.pow(4 + z2, 1.5));
  }

  function studentT5CDF(t) {
    if (!Number.isFinite(t)) return t < 0 ? 0.0 : 1.0;
    const theta = Math.atan(t / Math.sqrt(5));
    const cos = Math.cos(theta);
    const sin = Math.sin(theta);
    return 0.5 + (1 / Math.PI) * (theta + sin * cos * (1 + (2 / 3) * cos * cos));
  }

  function normalQuantile(p) {
    if (p <= 0.0) return -Infinity;
    if (p >= 1.0) return Infinity;

    const a = [
      -3.969683028665376e+01,  2.209460984245205e+02,
      -2.759285104469687e+02,  1.383577518672690e+02,
      -3.066479806614716e+01,  2.506628277459239e+00
    ];
    const b = [
      -5.447609879822406e+01,  1.615858368580409e+02,
      -1.556989798598866e+02,  6.680131188771972e+01,
      -1.328068155288572e+01
    ];
    const c = [
      -7.784894002430293e-03, -3.223964580411365e-01,
      -2.400758277161838e+00, -2.549732539343734e+00,
       4.374664141464968e+00,  2.938163982698783e+00
    ];
    const d = [
       7.784695709041462e-03,  3.224671290700398e-01,
       2.445134137142996e+00,  3.754408661907416e+00
    ];

    const pLow = 0.02425;
    const pHigh = 1 - pLow;

    if (p < pLow) {
      const q = Math.sqrt(-2 * Math.log(p));
      return (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) /
             ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1);
    }
    if (p <= pHigh) {
      const q = p - 0.5;
      const r = q * q;
      return (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q /
             (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1);
    }
    const q = Math.sqrt(-2 * Math.log(1 - p));
    return -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) /
            ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1);
  }

  function normalCDF(x) {
    if (!Number.isFinite(x)) return x < 0 ? 0.0 : 1.0;
    const a1 =  0.254829592;
    const a2 = -0.284496736;
    const a3 =  1.421413741;
    const a4 = -1.453152027;
    const a5 =  1.061405429;
    const p  =  0.3275911;

    const sign = x < 0 ? -1 : 1;
    const absX = Math.abs(x) / Math.SQRT2;
    const t = 1.0 / (1.0 + p * absX);
    const y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * Math.exp(-absX * absX);
    return 0.5 * (1.0 + sign * y);
  }

  function rollingAbdiRanaldo21(bars, absLev = 1.0, windowSize = 21) {
    const N = Array.isArray(bars) ? bars.length : 0;
    const w = Math.max(5, Math.trunc(windowSize || 21));
    const lev = Math.max(0.1, Math.abs(Number(absLev) || 1.0));
    const result = new Array(N).fill(null);
    if (N < w + 1) return result;

    const terms = new Float64Array(N);
    const valid = new Uint8Array(N);

    for (let t = 1; t < N; t++) {
      const prev = bars[t - 1];
      const curr = bars[t];

      const hp = Math.max(Number(prev.h ?? prev.high) || 0, 1e-9);
      const lp = Math.max(Number(prev.l ?? prev.low) || 0, 1e-9);
      const cp = Math.max(Number(prev.c ?? prev.close) || 0, 1e-9);

      const hc = Math.max(Number(curr.h ?? curr.high) || 0, 1e-9);
      const lc = Math.max(Number(curr.l ?? curr.low) || 0, 1e-9);

      if (hp <= lp || hc <= lc) continue;

      const etaPrev = 0.5 * (Math.log(hp) + Math.log(lp));
      const etaCurr = 0.5 * (Math.log(hc) + Math.log(lc));
      const logCp = Math.log(cp);

      terms[t] = 4.0 * (logCp - etaPrev) * (logCp - etaCurr);
      valid[t] = 1;
    }

    for (let t = w; t < N; t++) {
      let sum = 0.0;
      let count = 0;
      for (let j = t - w + 1; j <= t; j++) {
        if (valid[j]) {
          sum += terms[j];
          count++;
        }
      }
      if (count >= Math.floor(w * 0.6)) {
        const meanTerm = sum / count;
        const spread = (1.0 / lev) * Math.sqrt(Math.max(0.0, meanTerm));
        result[t] = Number.isFinite(spread) ? spread : null;
      }
    }

    return result;
  }

  function rollingCorwinSchultz21(bars, absLev = 1.0, windowSize = 21) {
    const N = Array.isArray(bars) ? bars.length : 0;
    const w = Math.max(5, Math.trunc(windowSize || 21));
    const lev = Math.max(0.1, Math.abs(Number(absLev) || 1.0));
    const result = new Array(N).fill(null);
    if (N < w + 1) return result;

    const dailySpreads = new Float64Array(N);
    const valid = new Uint8Array(N);
    const denom = 3.0 - 2.0 * Math.SQRT2;

    for (let t = 1; t < N; t++) {
      const prev = bars[t - 1];
      const curr = bars[t];

      const hp = Math.max(Number(prev.h ?? prev.high) || 0, 1e-9);
      const lp = Math.max(Number(prev.l ?? prev.low) || 0, 1e-9);
      const hc = Math.max(Number(curr.h ?? curr.high) || 0, 1e-9);
      const lc = Math.max(Number(curr.l ?? curr.low) || 0, 1e-9);

      if (hp <= lp || hc <= lc) continue;

      const beta = (Math.log(hp / lp) ** 2) + (Math.log(hc / lc) ** 2);
      const gamma = (Math.log(Math.max(hp, hc) / Math.min(lp, lc)) ** 2);

      const alpha = (Math.sqrt(2.0 * beta) - Math.sqrt(beta)) / denom - Math.sqrt(gamma / denom);
      const expA = Math.exp(alpha);
      const s = (1.0 / lev) * Math.max(0.0, 2.0 * (expA - 1.0) / (1.0 + expA));

      dailySpreads[t] = s;
      valid[t] = 1;
    }

    for (let t = w; t < N; t++) {
      let sum = 0.0;
      let count = 0;
      for (let j = t - w + 1; j <= t; j++) {
        if (valid[j]) {
          sum += dailySpreads[j];
          count++;
        }
      }
      if (count >= Math.floor(w * 0.6)) {
        result[t] = sum / count;
      }
    }

    return result;
  }

  function holderPowerMean(values, weights, p = 3.0) {
    if (!Array.isArray(values) || values.length === 0) return 0.0;
    const N = values.length;
    const power = Number(p) || 3.0;

    let wSum = 0.0;
    const wArr = new Float64Array(N);
    for (let i = 0; i < N; i++) {
      const w = Array.isArray(weights) && weights[i] !== undefined ? Math.max(0.0, Number(weights[i]) || 0.0) : 1.0;
      wArr[i] = w;
      wSum += w;
    }
    if (wSum <= 0.0) return 0.0;

    let acc = 0.0;
    for (let i = 0; i < N; i++) {
      const v = Math.max(0.0, Number(values[i]) || 0.0);
      const wNorm = wArr[i] / wSum;
      acc += wNorm * Math.pow(v, power);
    }

    return Math.pow(Math.max(0.0, acc), 1.0 / power);
  }

  function benjaminiHochbergFDR(pValues, q = 0.05) {
    if (!Array.isArray(pValues) || pValues.length === 0) return [];
    const m = pValues.length;
    const thresholdQ = Math.max(0.001, Math.min(0.5, Number(q) || 0.05));

    const indexed = new Array(m);
    for (let i = 0; i < m; i++) {
      const pv = Number(pValues[i]);
      indexed[i] = { p: Number.isFinite(pv) ? Math.max(0.0, Math.min(1.0, pv)) : 1.0, origIdx: i };
    }

    indexed.sort((a, b) => a.p - b.p);

    let maxK = -1;
    for (let k = m - 1; k >= 0; k--) {
      const criticalVal = ((k + 1) / m) * thresholdQ;
      if (indexed[k].p <= criticalVal) {
        maxK = k;
        break;
      }
    }

    const isSignificant = new Array(m).fill(false);
    for (let k = 0; k <= maxK; k++) {
      isSignificant[indexed[k].origIdx] = true;
    }

    return isSignificant;
  }

  // ── Public UMD Module Exports ───────────────────────────────────────────

  return Object.freeze({
    canonicalUnderlyingId,
    canonicalUnderlyingLabel,
    leverageAtDate,
    marketBetaPolarity,
    classifyPairMatch,
    areSymmetricPair,
    reconstructTotalReturnSeries,
    yangZhangVolatility,
    studentT4CDF,
    studentT5CDF,
    normalQuantile,
    normalCDF,
    rollingAbdiRanaldo21,
    rollingCorwinSchultz21,
    holderPowerMean,
    benjaminiHochbergFDR
  });
}));
