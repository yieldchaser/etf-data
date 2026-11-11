/**
 * Trough-to-Peak ZigZag Cycle & Underwater Drawdown Studio — etf-data
 * Native High-DPI Canvas 2D Engine + O(N) Monotonic Deque Drawdown Engine.
 * Zero external dependencies. Zero hardcoded tickers. Factual quantitative terminology.
 */

(function (root, factory) {
  if (typeof module === 'object' && module.exports) {
    let qc = null;
    try { qc = require('./quant-core.js'); } catch (_) {}
    module.exports = factory(qc);
  } else {
    root.TroughPeak = factory(root.QuantCore);
  }
})(typeof self !== 'undefined' ? self : this, function (QuantCore) {
  'use strict';

  const LONG_PALETTE = ['#22d3ee', '#34d399', '#60a5fa', '#a78bfa', '#f59e0b', '#2dd4bf'];
  const SHORT_PALETTE = ['#fb7185', '#f97316', '#f43f5e', '#e879f9', '#fbbf24', '#ec4899'];
  const THRESHOLD_PRESETS = [5, 10, 15, 25, 30, 50, 100, 150, 200];
  const DD_WIN_DAYS = { '1m': 30, '3m': 90, '6m': 180, '1y': 365, '3y': 1095, '5y': 1826 };
  const MONTHS_SHORT = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  const DD_TIP_GAP_MS = 10 * 24 * 3600 * 1000;
  const DEFAULT_DD_VISIBLE_CAP = 8;

  // ── Formatting & Date Utilities ──

  function formatDisplayDate(iso) {
    if (!iso || typeof iso !== 'string') return iso || '—';
    const parts = iso.split('-');
    if (parts.length < 3) return iso;
    const y = parts[0];
    const mIdx = parseInt(parts[1], 10) - 1;
    const dNum = parseInt(parts[2].slice(0, 2), 10);
    if (isNaN(dNum) || isNaN(mIdx) || mIdx < 0 || mIdx > 11) return iso;
    return `${MONTHS_SHORT[mIdx]} ${dNum}, ${y}`;
  }

  function fmtPrice(n) {
    if (n == null || !isFinite(n)) return '—';
    if (n >= 1000) return '$' + n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    if (n >= 1) return '$' + n.toFixed(2);
    return '$' + n.toFixed(4);
  }

  function fmtNum(n, d = 2) {
    return (n != null && isFinite(n)) ? n.toFixed(d) : '—';
  }

  function fmtPct(n, d = 1) {
    if (n == null || !isFinite(n)) return '—';
    return (n >= 0 ? '+' : '') + n.toFixed(d) + '%';
  }

  function fmtVol(v) {
    if (v == null || !isFinite(v)) return '—';
    const a = Math.abs(v);
    if (a >= 1e9) return (a / 1e9).toFixed(2) + 'B';
    if (a >= 1e6) return (a / 1e6).toFixed(2) + 'M';
    if (a >= 1e3) return (a / 1e3).toFixed(0) + 'K';
    return Math.round(a).toLocaleString('en-US');
  }

  function hexToRgba(hex, alpha) {
    if (!hex || typeof hex !== 'string' || hex[0] !== '#') return `rgba(34, 211, 238, ${alpha})`;
    let r = 0, g = 0, b = 0;
    if (hex.length === 4) {
      r = parseInt(hex[1] + hex[1], 16);
      g = parseInt(hex[2] + hex[2], 16);
      b = parseInt(hex[3] + hex[3], 16);
    } else if (hex.length >= 7) {
      r = parseInt(hex.slice(1, 3), 16);
      g = parseInt(hex.slice(3, 5), 16);
      b = parseInt(hex.slice(5, 7), 16);
    }
    return `rgba(${r}, ${g}, ${b}, ${alpha})`;
  }

  function _escapeHtml(s) {
    return String(s == null ? '' : s)
      .replaceAll('&', '&amp;')
      .replaceAll('<', '&lt;')
      .replaceAll('>', '&gt;')
      .replaceAll('"', '&quot;')
      .replaceAll("'", '&#039;');
  }

  function _hslToHex(h, s, l) {
    const sat = s / 100;
    const lig = l / 100;
    const a = sat * Math.min(lig, 1 - lig);
    const f = n => {
      const k = (n + h / 30) % 12;
      const col = lig - a * Math.max(Math.min(k - 3, 9 - k, 1), -1);
      return Math.round(255 * col).toString(16).padStart(2, '0');
    };
    return `#${f(0)}${f(8)}${f(4)}`;
  }

  // ── Catalog & Dynamic Comparison Helpers ──

  function isInverseInstrument(inst) {
    if (!inst) return false;
    if (inst.market_beta_polarity != null && Number.isFinite(Number(inst.market_beta_polarity))) {
      return Number(inst.market_beta_polarity) < 0;
    }
    if (QuantCore && typeof QuantCore.marketBetaPolarity === 'function') {
      return QuantCore.marketBetaPolarity(inst.ticker, inst.leverage_value) < 0;
    }
    return (inst.leverage_value != null && Number(inst.leverage_value) < 0) || inst.direction === 'inverse';
  }

  function normalizeUnderlyingKey(raw, ticker) {
    if (QuantCore && typeof QuantCore.canonicalUnderlyingId === 'function') {
      const canon = QuantCore.canonicalUnderlyingId(raw, ticker);
      if (canon) return canon;
    }
    if (!raw) return '';
    return String(raw)
      .replace(/\s*\([^)]*\)\s*$/g, '')
      .replace(/\s+(Technologies|Global|Index)$/i, '')
      .trim()
      .toLowerCase();
  }

  function _buildCatalogMap(catalogInstruments) {
    if (!catalogInstruments) return {};
    if (!Array.isArray(catalogInstruments)) return catalogInstruments;
    const map = {};
    for (const item of catalogInstruments) {
      if (item && item.ticker) map[item.ticker] = item;
    }
    return map;
  }

  function resolveComparisonSet(activeTicker, catalogInstruments, mode = 'pair', customTickers = [], maxPeers = null) {
    const list = Array.isArray(catalogInstruments) ? catalogInstruments : Object.values(catalogInstruments || {});
    const map = _buildCatalogMap(list);
    const active = map[activeTicker] || null;
    if (!activeTicker) return [];
    const cap = (typeof maxPeers === 'number' && maxPeers > 0) ? maxPeers : Infinity;

    if (mode === 'single') {
      return [activeTicker];
    }

    if (mode === 'custom') {
      const seen = new Set([activeTicker]);
      const out = [activeTicker];
      for (const tk of (customTickers || [])) {
        if (tk && !seen.has(tk) && map[tk] && out.length < cap) {
          seen.add(tk);
          out.push(tk);
        }
      }
      return out;
    }

    if (!active) return [activeTicker];
    const activeInv = isInverseInstrument(active);
    const activeUndKey = active.underlying_id || normalizeUnderlyingKey(active.underlying, activeTicker);
    const activeLev = Math.abs(active.leverage_value || 1);

    const sortPeers = (a, b) => {
      if (QuantCore && typeof QuantCore.classifyPairMatch === 'function') {
        const matchA = QuantCore.classifyPairMatch(active, a);
        const matchB = QuantCore.classifyPairMatch(active, b);
        if (matchA === 'symmetric' && matchB !== 'symmetric') return -1;
        if (matchB === 'symmetric' && matchA !== 'symmetric') return 1;
      }
      const levDiffA = Math.abs(Math.abs(a.leverage_value || 1) - activeLev);
      const levDiffB = Math.abs(Math.abs(b.leverage_value || 1) - activeLev);
      if (levDiffA !== levDiffB) return levDiffA - levDiffB;
      return (b.aum_m || 0) - (a.aum_m || 0);
    };

    if (mode === 'pair') {
      if (!activeUndKey) return [activeTicker];
      const oppPeers = list
        .filter(x => x.ticker !== activeTicker && (x.underlying_id || normalizeUnderlyingKey(x.underlying, x.ticker)) === activeUndKey && isInverseInstrument(x) !== activeInv)
        .sort(sortPeers);
      if (!oppPeers.length) return [activeTicker];
      return [activeTicker, oppPeers[0].ticker];
    }

    if (mode === 'underlying') {
      if (!activeUndKey) return [activeTicker];
      const peers = list
        .filter(x => x.ticker !== activeTicker && (x.underlying_id || normalizeUnderlyingKey(x.underlying, x.ticker)) === activeUndKey)
        .sort((a, b) => (b.aum_m || 0) - (a.aum_m || 0));
      return [activeTicker, ...peers.map(x => x.ticker)].slice(0, cap);
    }

    if (mode === 'category') {
      if (!active.category) return [activeTicker];
      const peers = list
        .filter(x => x.ticker !== activeTicker && x.category === active.category)
        .sort((a, b) => (b.aum_m || 0) - (a.aum_m || 0));
      return [activeTicker, ...peers.map(x => x.ticker)].slice(0, cap);
    }

    return [activeTicker];
  }

  function assignSeriesColors(tickers, catalogMap) {
    const map = _buildCatalogMap(catalogMap);
    const colors = {};
    let longIdx = 0;
    let shortIdx = 0;
    for (const tk of (tickers || [])) {
      const inv = isInverseInstrument(map[tk]);
      if (inv) {
        if (shortIdx < SHORT_PALETTE.length) {
          colors[tk] = SHORT_PALETTE[shortIdx];
        } else {
          const offset = shortIdx - SHORT_PALETTE.length;
          const hue = (325 + offset * 47) % 360;
          const warmHue = (hue > 55 && hue < 300) ? ((hue % 80) + 325) % 360 : hue;
          colors[tk] = _hslToHex(warmHue, 78, 66);
        }
        shortIdx++;
      } else {
        if (longIdx < LONG_PALETTE.length) {
          colors[tk] = LONG_PALETTE[longIdx];
        } else {
          const offset = longIdx - LONG_PALETTE.length;
          const coolHue = 145 + ((offset * 43) % 145);
          colors[tk] = _hslToHex(coolHue, 76, 64);
        }
        longIdx++;
      }
    }
    return colors;
  }

  // ── Pure Quantitative Math Engines ──

  function detectCycles(dates, prices, pct, options = {}) {
    const rawThr = Number(pct) || 0;
    let thr = Math.max(0, rawThr) / 100;
    if (options && options.volAdaptive && options.yzVol) {
      const kappa = options.kappa || 3.0;
      const tau = options.tau || 10;
      const adaptivePct = Math.max(2.0, kappa * options.yzVol * Math.sqrt(tau) * 100);
      thr = adaptivePct / 100;
    }
    const h = Math.log1p(thr);
    if (!prices || prices.length < 2 || !dates || dates.length !== prices.length || h <= 0) return [];

    const x = new Float64Array(prices.length);
    let firstValid = -1;
    for (let i = 0; i < prices.length; i++) {
      const p = Number(prices[i]);
      if (Number.isFinite(p) && p > 0) {
        x[i] = Math.log(p);
        if (firstValid === -1) firstValid = i;
      } else {
        x[i] = NaN;
      }
    }
    if (firstValid === -1) return [];

    const pivots = [];
    let dir = null;
    let iMin = firstValid;
    let iMax = firstValid;
    let pIdx = firstValid;
    let pVal = x[firstValid];

    for (let i = firstValid + 1; i < prices.length; i++) {
      if (!Number.isFinite(x[i])) continue;

      if (dir === null) {
        if (x[i] > x[iMax]) iMax = i;
        if (x[i] < x[iMin]) iMin = i;

        const upHit = (x[i] - x[iMin]) >= h;
        const dnHit = (x[iMax] - x[i]) >= h;

        if (upHit && (!dnHit || iMin < iMax)) {
          pivots.push({ i: iMin, price: prices[iMin], date: dates[iMin], type: 'trough', status: 'CONFIRMED' });
          dir = 'up';
          pIdx = i;
          pVal = x[i];
        } else if (dnHit) {
          pivots.push({ i: iMax, price: prices[iMax], date: dates[iMax], type: 'peak', status: 'CONFIRMED' });
          dir = 'down';
          pIdx = i;
          pVal = x[i];
        }
      } else if (dir === 'up') {
        if (x[i] > pVal) {
          pIdx = i;
          pVal = x[i];
        } else if ((pVal - x[i]) >= h) {
          pivots.push({ i: pIdx, price: prices[pIdx], date: dates[pIdx], type: 'peak', status: 'CONFIRMED' });
          dir = 'down';
          pIdx = i;
          pVal = x[i];
        }
      } else {
        if (x[i] < pVal) {
          pIdx = i;
          pVal = x[i];
        } else if ((x[i] - pVal) >= h) {
          pivots.push({ i: pIdx, price: prices[pIdx], date: dates[pIdx], type: 'trough', status: 'CONFIRMED' });
          dir = 'up';
          pIdx = i;
          pVal = x[i];
        }
      }
    }

    if (dir !== null) {
      pivots.push({
        i: pIdx,
        price: prices[pIdx],
        date: dates[pIdx],
        type: dir === 'up' ? 'peak' : 'trough',
        status: 'PROVISIONAL'
      });
    }

    const cycles = [];
    for (let i = 0; i < pivots.length - 1; i++) {
      if (pivots[i].type === 'trough' && pivots[i + 1].type === 'peak') {
        const gain = ((pivots[i + 1].price - pivots[i].price) / pivots[i].price) * 100;
        const days = Math.max(0, Math.round((Date.parse(pivots[i + 1].date) - Date.parse(pivots[i].date)) / 86400000));
        const isProvisional = pivots[i + 1].status === 'PROVISIONAL' || pivots[i].status === 'PROVISIONAL';
        cycles.push({
          num: cycles.length + 1,
          troughDate: pivots[i].date,
          troughPrice: pivots[i].price,
          troughIdx: pivots[i].i,
          peakDate: pivots[i + 1].date,
          peakPrice: pivots[i + 1].price,
          peakIdx: pivots[i + 1].i,
          gain,
          days,
          isProvisional,
          status: isProvisional ? 'PROVISIONAL' : 'CONFIRMED'
        });
      }
    }
    return cycles;
  }

  function gainBandColor(g, a) {
    if (g >= 200) return `rgba(61,184,122,${a})`;
    if (g >= 100) return `rgba(80,144,160,${a})`;
    if (g >= 50) return `rgba(212,168,48,${a})`;
    return `rgba(192,120,40,${a})`;
  }

  function computeUlcerIndex(ddArray) {
    if (!ddArray || !ddArray.length) return 0;
    let sumSq = 0;
    for (let i = 0; i < ddArray.length; i++) {
      const d = Number(ddArray[i]) || 0;
      sumSq += d * d;
    }
    return Math.sqrt(sumSq / ddArray.length);
  }

  function computeCDaR(ddArray, alpha = 0.95) {
    if (!ddArray || !ddArray.length) return 0;
    const sorted = [...ddArray].map(x => Number(x) || 0).sort((a, b) => a - b);
    const k = Math.max(1, Math.floor((1 - alpha) * sorted.length));
    let sumTail = 0;
    for (let i = 0; i < k; i++) {
      sumTail += Math.abs(sorted[i]);
    }
    return sumTail / k;
  }

  function compute1xEquivalentDD(dd, leverage) {
    const lev = Math.abs(Number(leverage)) || 1.0;
    if (dd == null || !isFinite(dd) || lev <= 0) return dd;
    const ratio = 1 + dd / 100;
    if (ratio <= 0) return -100;
    return (Math.pow(ratio, 1 / lev) - 1) * 100;
  }

  function computeVolScaledDD(dd, yzAnnVol) {
    if (dd == null || !isFinite(dd) || !yzAnnVol || yzAnnVol <= 0) return null;
    const annVolPct = yzAnnVol > 1.0 ? yzAnnVol : yzAnnVol * 100;
    return dd / annVolPct;
  }

  function computeDragAdjustedReturn(prices, leverage, startIdx, endIdx) {
    if (!prices || startIdx == null || endIdx == null) return null;
    const i1 = Math.min(startIdx, endIdx);
    const i2 = Math.max(startIdx, endIdx);
    if (i2 <= i1 || i1 < 0 || i2 >= prices.length) return null;
    const p1 = prices[i1];
    const p2 = prices[i2];
    if (p1 <= 0 || p2 <= 0) return null;
    const lev = Number(leverage) || 1.0;
    if (Math.abs(lev) < 1e-6) return null;

    let sumRetSq = 0;
    for (let i = i1 + 1; i <= i2; i++) {
      if (prices[i] > 0 && prices[i - 1] > 0) {
        const r = Math.log(prices[i] / prices[i - 1]);
        sumRetSq += r * r;
      }
    }
    const rvU = sumRetSq / (lev * lev);
    const varianceDragFactor = 0.5 * lev * (lev - 1) * rvU;
    const grossReturn = (p2 / p1) * Math.exp(varianceDragFactor);
    if (grossReturn <= 0) return -100;
    return (Math.pow(grossReturn, 1 / lev) - 1) * 100;
  }

  function computeCardAnalytics(dates, closes, volumes, cycles, options = {}) {
    if (!closes || !closes.length) {
      return null;
    }
    const confirmedCycles = (cycles || []).filter(c => !c.isProvisional);
    const n = confirmedCycles.length;
    const totalCycles = (cycles || []).length;
    const avg = arr => arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : 0;
    const maxArr = arr => { let m = -Infinity; for (let i = 0; i < arr.length; i++) if (arr[i] > m) m = arr[i]; return m; };
    const minArr = arr => { let m = Infinity; for (let i = 0; i < arr.length; i++) if (arr[i] < m) m = arr[i]; return m; };
    const median = arr => {
      if (!arr.length) return 0;
      const s = [...arr].sort((a, b) => a - b);
      const mid = Math.floor(s.length / 2);
      return s.length % 2 ? s[mid] : (s[mid - 1] + s[mid]) / 2;
    };

    const gains = confirmedCycles.map(c => c.gain);
    const days = confirmedCycles.map(c => c.days);
    const gaps = [];
    for (let i = 0; i < n - 1; i++) {
      gaps.push(Math.round((Date.parse(confirmedCycles[i + 1].troughDate) - Date.parse(confirmedCycles[i].peakDate)) / 86400000));
    }

    const lastPrice = closes[closes.length - 1];

    // Cycle Retracement & Maturity
    let cycRetracement = null;
    let cycPosRaw = null;
    let cycPosPct = 0;
    let cycLabel = '';
    if (n > 0) {
      const lastConf = confirmedCycles[n - 1];
      const logGains = confirmedCycles.map(c => (c.peakPrice > 0 && c.troughPrice > 0) ? Math.log(c.peakPrice / c.troughPrice) : 0).filter(g => g > 0);
      const medianHistLogGain = logGains.length ? median(logGains) : Math.log(1.30);
      
      const activeTrough = (cycles && cycles.length && cycles[cycles.length - 1].troughPrice > 0)
        ? cycles[cycles.length - 1].troughPrice
        : lastConf.troughPrice;

      if (lastConf.peakPrice > lastConf.troughPrice && lastConf.troughPrice > 0) {
        const denomRetracement = Math.log(lastConf.peakPrice / lastConf.troughPrice);
        if (denomRetracement > 1e-6 && lastPrice > 0) {
          cycRetracement = Math.round((Math.log(lastPrice / lastConf.troughPrice) / denomRetracement) * 100);
        }
      }

      if (medianHistLogGain > 1e-6 && activeTrough > 0 && lastPrice > 0) {
        const rawMaturity = (Math.log(lastPrice / activeTrough) / medianHistLogGain) * 100;
        cycPosRaw = Math.round(rawMaturity);
        cycPosPct = Math.min(Math.max(0, cycPosRaw), 100);
        cycLabel = cycPosRaw <= 30 ? 'Early' : (cycPosRaw <= 70 ? 'Mid' : (cycPosRaw <= 100 ? 'Late' : 'Extreme'));
      }
    }

    // 3D Stretch Z-Score over causal prior 60 periods (excluding current session from baseline)
    let stretchZ = null;
    if (closes.length > 63 && closes[closes.length - 1] > 0 && closes[closes.length - 4] > 0) {
      const current3d = (closes[closes.length - 1] / closes[closes.length - 4]) - 1;
      const prior3d = [];
      for (let i = closes.length - 61; i < closes.length - 1; i++) {
        if (Number.isFinite(closes[i]) && Number.isFinite(closes[i - 3]) && closes[i] > 0 && closes[i - 3] > 0) {
          prior3d.push((closes[i] / closes[i - 3]) - 1);
        }
      }
      if (prior3d.length > 2 && Number.isFinite(current3d)) {
        const mean = avg(prior3d);
        const variance = prior3d.reduce((acc, v) => acc + Math.pow(v - mean, 2), 0) / (prior3d.length - 1);
        const std = Math.sqrt(variance);
        stretchZ = std > 1e-12 ? (current3d - mean) / std : 0;
      }
    }

    const getRangePos = len => {
      if (closes.length < len) return null;
      const slice = closes.slice(-len);
      const lo = minArr(slice);
      const hi = maxArr(slice);
      if (hi === lo) return 50;
      return Math.round(((lastPrice - lo) / (hi - lo)) * 100);
    };

    let distFromPeakPct = null;
    let distFromTroughPct = null;
    if (n > 0) {
      const lastC = confirmedCycles[n - 1];
      if (lastC.peakPrice > 0) distFromPeakPct = Math.round(((lastPrice / lastC.peakPrice) - 1) * 100);
      if (lastC.troughPrice > 0) distFromTroughPct = Math.round(((lastPrice / lastC.troughPrice) - 1) * 100);
    }

    let range30dPct = null;
    if (closes.length > 1) {
      const slice30 = closes.slice(-30);
      const rMin = minArr(slice30);
      const rMax = maxArr(slice30);
      if (rMin > 0) range30dPct = Math.round(((rMax - rMin) / rMin) * 100);
    }

    const lev = options.leverage != null ? Number(options.leverage) : 1.0;
    const ddAth = computeATHDD(dates, closes);
    const ulcerIndex = computeUlcerIndex(ddAth.dd);
    const cdar95 = computeCDaR(ddAth.dd, 0.95);
    const dd1xEq = compute1xEquivalentDD(ddAth.dd[ddAth.dd.length - 1], lev);
    const maxDD1xEq = compute1xEquivalentDD(minArr(ddAth.dd), lev);

    let underwaterSessions = 0;
    for (let i = ddAth.dd.length - 1; i >= 0; i--) {
      if (ddAth.dd[i] < -0.01) underwaterSessions++;
      else break;
    }
    const dragAdjReturn = computeDragAdjustedReturn(closes, lev, 0, closes.length - 1);

    return {
      cyclesCount: n,
      totalCyclesCount: totalCycles,
      avgGain: n ? avg(gains) : 0,
      medianGain: n ? median(gains) : 0,
      avgDays: n ? avg(days) : 0,
      avgGap: gaps.length ? Math.round(avg(gaps)) : null,
      maxGain: n ? maxArr(gains) : null,
      minGain: n ? minArr(gains) : null,
      lastPrice,
      cycRetracement,
      cycPosRaw,
      cycPosPct,
      cycLabel,
      stretchZ,
      range1M: getRangePos(21),
      range3M: getRangePos(63),
      range6M: getRangePos(126),
      distFromPeakPct,
      distFromTroughPct,
      range30dPct,
      ulcerIndex,
      cdar95,
      underwaterSessions,
      dd1xEq,
      maxDD1xEq,
      dragAdjReturn
    };
  }

  function computeRollingLookbackDD(dates, closes, lookbackDays = 364) {
    const n = closes ? closes.length : 0;
    const ts = new Array(n);
    for (let i = 0; i < n; i++) {
      ts[i] = typeof dates[i] === 'number' ? dates[i] : Date.parse(dates[i]);
    }
    const days = lookbackDays || 364;
    const WIN = days * 86400 * 1000;
    const dd = new Array(n);
    const peaks = new Array(n);
    const peakIndices = new Array(n);
    const peakTimestamps = new Array(n);
    const deque = new Array(n);
    let head = 0;
    let tail = 0;
    let left = 0;

    for (let i = 0; i < n; i++) {
      const curTs = ts[i];
      const curClose = closes[i];
      while (left < i && curTs - ts[left] > WIN) {
        left++;
      }
      while (head < tail && deque[head] < left) {
        head++;
      }
      while (tail > head && closes[deque[tail - 1]] <= curClose) {
        tail--;
      }
      deque[tail++] = i;

      const pIdx = deque[head];
      const pVal = closes[pIdx];
      peaks[i] = pVal;
      peakIndices[i] = pIdx;
      peakTimestamps[i] = ts[pIdx];

      let d = pVal > 0 ? ((curClose - pVal) / pVal) * 100 : 0;
      if (d > 0 || Math.abs(d) < 1e-9) d = 0;
      dd[i] = d;
    }

    return { ts, dd, peaks, peakIndices, peakTimestamps };
  }

  function computeRolling52WDD(dates, closes) {
    return computeRollingLookbackDD(dates, closes, 364);
  }

  function computeRolling2YDD(dates, closes) {
    return computeRollingLookbackDD(dates, closes, 730);
  }

  function computeATHDD(dates, closes) {
    const n = closes ? closes.length : 0;
    const ts = new Array(n);
    for (let i = 0; i < n; i++) {
      ts[i] = typeof dates[i] === 'number' ? dates[i] : Date.parse(dates[i]);
    }
    const dd = new Array(n);
    const peaks = new Array(n);
    const peakIndices = new Array(n);
    const peakTimestamps = new Array(n);

    let maxVal = -Infinity;
    let maxIdx = -1;

    for (let i = 0; i < n; i++) {
      const curClose = closes[i];
      if (curClose >= maxVal || maxIdx === -1) {
        maxVal = curClose;
        maxIdx = i;
      }
      peaks[i] = maxVal;
      peakIndices[i] = maxIdx;
      peakTimestamps[i] = ts[maxIdx];

      let d = maxVal > 0 ? ((curClose - maxVal) / maxVal) * 100 : 0;
      if (d > 0 || Math.abs(d) < 1e-9) d = 0;
      dd[i] = d;
    }

    return { ts, dd, peaks, peakIndices, peakTimestamps };
  }

  function computeUnderwaterMetrics(pts, closes, peaks, ts) {
    if (!pts || !pts.length) {
      return {
        currentDD: 0,
        maxDD: 0,
        troughPrice: 0,
        troughDate: 0,
        troughIdx: -1,
        peakPrice: 0,
        peakDate: 0,
        currentPrice: 0,
        daysUnderwater: 0,
        breakEvenGain: 0,
        maxDDBreakEvenGain: 0,
        recoveryFromTrough: 0,
        ulcerIndex: 0,
        cdar95: 0,
        tradingSessionsUnderwater: 0
      };
    }

    const lastPt = pts[pts.length - 1];
    const currentDD = lastPt.dd;
    const currentPrice = lastPt.close != null ? lastPt.close : (closes ? closes[closes.length - 1] : 0);
    const peakPrice = lastPt.peak != null ? lastPt.peak : (peaks ? peaks[peaks.length - 1] : currentPrice);
    const peakDate = lastPt.peakTs != null ? lastPt.peakTs : (ts ? ts[ts.length - 1] : lastPt.t);

    let maxDD = 0;
    let troughIdx = 0;
    for (let i = 0; i < pts.length; i++) {
      if (pts[i].dd < maxDD) {
        maxDD = pts[i].dd;
        troughIdx = i;
      }
    }

    const troughPt = pts[troughIdx];
    const troughPrice = troughPt.close != null ? troughPt.close : 0;
    const troughDate = troughPt.t;

    let daysUnderwater = 0;
    if (currentDD < -0.01) {
      daysUnderwater = Math.max(0, Math.round((lastPt.t - peakDate) / 86400000));
    }

    let breakEvenGain = 0;
    if (currentDD < 0 && (100 + currentDD) > 0.0001) {
      breakEvenGain = (-currentDD / (100 + currentDD)) * 100;
    }

    let maxDDBreakEvenGain = 0;
    if (maxDD < 0 && (100 + maxDD) > 0.0001) {
      maxDDBreakEvenGain = (-maxDD / (100 + maxDD)) * 100;
    }

    let recoveryFromTrough = 0;
    if (troughPrice > 0 && currentPrice >= troughPrice) {
      recoveryFromTrough = ((currentPrice - troughPrice) / troughPrice) * 100;
    }

    const ddArr = pts.map(p => p.dd);
    const ulcerIndex = computeUlcerIndex(ddArr);
    const cdar95 = computeCDaR(ddArr, 0.95);

    let tradingSessionsUnderwater = 0;
    for (let i = pts.length - 1; i >= 0; i--) {
      if (pts[i].dd < -0.01) {
        tradingSessionsUnderwater++;
      } else {
        break;
      }
    }

    return {
      currentDD,
      maxDD,
      troughPrice,
      troughDate,
      troughIdx,
      peakPrice,
      peakDate,
      currentPrice,
      daysUnderwater,
      breakEvenGain,
      maxDDBreakEvenGain,
      recoveryFromTrough,
      ulcerIndex,
      cdar95,
      tradingSessionsUnderwater
    };
  }

  function decimateDDPtsPrecise(pts, xOf, chartW) {
    if (!pts || pts.length <= Math.max(40, chartW * 2)) return pts || [];
    const cols = new Map();
    for (let i = 0; i < pts.length; i++) {
      const c = Math.round(xOf(pts[i].t));
      let b = cols.get(c);
      if (!b) {
        b = { lo: i, hi: i, first: i, last: i };
        cols.set(c, b);
      } else {
        b.last = i;
        if (pts[i].dd < pts[b.lo].dd) b.lo = i;
        if (pts[i].dd > pts[b.hi].dd) b.hi = i;
      }
    }
    const out = [];
    const sortedCols = Array.from(cols.keys()).sort((a, b) => a - b);
    for (const c of sortedCols) {
      const b = cols.get(c);
      const idxs = [b.first];
      if (b.lo !== b.first && !idxs.includes(b.lo)) idxs.push(b.lo);
      if (b.hi !== b.first && !idxs.includes(b.hi)) idxs.push(b.hi);
      if (b.last !== b.first && !idxs.includes(b.last)) idxs.push(b.last);
      idxs.sort((a, b) => a - b);
      for (const idx of idxs) {
        out.push(pts[idx]);
      }
    }
    if (out.length > 0) {
      if (out[0] !== pts[0]) out.unshift(pts[0]);
      if (out[out.length - 1] !== pts[pts.length - 1]) out.push(pts[pts.length - 1]);
    }
    return out;
  }

  function computeDragMeasurement(dates, closes, volumes, startIdx, endIdx) {
    if (!dates || !closes || startIdx == null || endIdx == null || startIdx === endIdx) return null;
    const i1 = Math.max(0, Math.min(startIdx, endIdx));
    const i2 = Math.min(dates.length - 1, Math.max(startIdx, endIdx));
    if (i2 <= i1) return null;

    const p1 = closes[i1];
    const p2 = closes[i2];
    if (p1 == null || p1 <= 0 || p2 == null) return null;

    const diff = p2 - p1;
    const pct = (diff / p1) * 100;
    const bars = i2 - i1;
    const calDays = Math.max(0, Math.round((Date.parse(dates[i2]) - Date.parse(dates[i1])) / 86400000));
    const annPct = (calDays >= 30 && p2 > 0) ? (Math.pow(p2 / p1, 365 / calDays) - 1) * 100 : null;

    let volMin = Infinity;
    let volMax = -Infinity;
    let cumDollarVol = 0;
    for (let i = i1; i <= i2; i++) {
      const v = volumes ? (volumes[i] || 0) : 0;
      if (v < volMin) volMin = v;
      if (v > volMax) volMax = v;
      cumDollarVol += closes[i] * v;
    }
    if (!isFinite(volMin)) volMin = 0;
    if (!isFinite(volMax)) volMax = 0;

    return {
      i1,
      i2,
      p1,
      p2,
      diff,
      pct,
      startDate: dates[i1],
      endDate: dates[i2],
      bars,
      calDays,
      annPct,
      volMin,
      volMax,
      cumDollarVol
    };
  }

  // ── Helper: Extract Compact OHLCV Series ──

  function _normalizeSeries(bars) {
    if (!Array.isArray(bars) || !bars.length) return null;
    const dates = [];
    const closes = [];
    const volumes = [];
    const tri = [];
    const validRaw = [];
    const hasTC = bars.some(b => b && b.tc != null && Number.isFinite(Number(b.tc)) && Number(b.tc) > 0);
    let lastTriScale = 1.0;
    for (let i = 0; i < bars.length; i++) {
      const b = bars[i];
      if (!b) continue;
      const d = b.d || b.date;
      const c = Number(b.c != null ? b.c : b.close);
      const vNum = Number(b.v != null ? b.v : (b.volume != null ? b.volume : 0));
      if (!d || !Number.isFinite(c) || c <= 0) continue;
      const v = Number.isFinite(vNum) && vNum > 0 ? vNum : 0;
      let tcVal = c;
      if (hasTC) {
        const rawTc = b.tc != null ? Number(b.tc) : NaN;
        if (Number.isFinite(rawTc) && rawTc > 0) {
          tcVal = rawTc;
          lastTriScale = tcVal / c;
        } else {
          tcVal = c * lastTriScale;
        }
      }
      dates.push(d);
      closes.push(c);
      volumes.push(v);
      tri.push(tcVal);
      validRaw.push(b);
    }
    return dates.length >= 2 ? { dates, closes, volumes, tri, rawBars: validRaw } : null;
  }

  function _getWeekNumber(d) {
    const d2 = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
    d2.setUTCDate(d2.getUTCDate() + 4 - (d2.getUTCDay() || 7));
    const yearStart = new Date(Date.UTC(d2.getUTCFullYear(), 0, 1));
    return Math.ceil((((d2 - yearStart) / 86400000) + 1) / 7);
  }

  function _nearestDDPoint(pts, t) {
    let lo = 0;
    let hi = pts.length - 1;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (pts[mid].t < t) lo = mid + 1;
      else hi = mid;
    }
    let best = pts[lo];
    if (lo > 0 && Math.abs(pts[lo - 1].t - t) < Math.abs(best.t - t)) best = pts[lo - 1];
    return best;
  }

  // ── PointerEvents Custom Dual-Handle Brush & Slider Helper ──

  function _bindCustomSingleSlider(railEl, thumbEl, getBounds, onChange) {
    if (!railEl || !thumbEl || railEl._tpSingleBound) return;
    railEl._tpSingleBound = true;

    const valueFromClientX = clientX => {
      const rect = railEl.getBoundingClientRect();
      if (rect.width <= 0) return getBounds().min;
      const ratio = Math.max(0, Math.min(1, (clientX - rect.left) / rect.width));
      const { min, max, step } = getBounds();
      const raw = min + ratio * (max - min);
      return Math.max(min, Math.min(max, Math.round(raw / step) * step));
    };

    let dragging = false;
    const onDown = e => {
      e.preventDefault();
      dragging = true;
      if (railEl.setPointerCapture && e.pointerId != null) {
        try { railEl.setPointerCapture(e.pointerId); } catch (_) {}
      }
      onChange(valueFromClientX(e.clientX));
    };
    const onMove = e => {
      if (!dragging) return;
      onChange(valueFromClientX(e.clientX));
    };
    const onUp = e => {
      if (!dragging) return;
      dragging = false;
      if (railEl.releasePointerCapture && e.pointerId != null) {
        try { railEl.releasePointerCapture(e.pointerId); } catch (_) {}
      }
    };

    railEl.addEventListener('pointerdown', onDown);
    window.addEventListener('pointermove', onMove);
    window.addEventListener('pointerup', onUp);
    window.addEventListener('pointercancel', onUp);

    thumbEl.addEventListener('keydown', e => {
      const { min, max, step, val } = getBounds();
      let next = val;
      if (e.key === 'ArrowLeft' || e.key === 'ArrowDown') next = Math.max(min, val - step);
      else if (e.key === 'ArrowRight' || e.key === 'ArrowUp') next = Math.min(max, val + step);
      else if (e.key === 'PageDown') next = Math.max(min, val - step * 10);
      else if (e.key === 'PageUp') next = Math.min(max, val + step * 10);
      else if (e.key === 'Home') next = min;
      else if (e.key === 'End') next = max;
      else return;
      e.preventDefault();
      onChange(next);
    });
  }

  function _bindCustomDualBrush(railEl, thumbStart, thumbEnd, highlightEl, getState, onUpdate) {
    if (!railEl || !thumbStart || !thumbEnd || railEl._tpBrushBound) return;
    railEl._tpBrushBound = true;

    const MIN_GAP = 2;
    let activeMode = null; // 'start' | 'end' | 'pan'
    let panAnchorRatio = 0;
    let panInitStart = 0;
    let panInitEnd = 0;

    const ratioFromClientX = clientX => {
      const rect = railEl.getBoundingClientRect();
      if (rect.width <= 0) return 0;
      return Math.max(0, Math.min(1, (clientX - rect.left) / rect.width));
    };

    const onDown = e => {
      const st = getState();
      if (!st || st.maxIdx < MIN_GAP) return;
      e.preventDefault();

      const ratio = ratioFromClientX(e.clientX);
      const idx = Math.round(ratio * st.maxIdx);

      if (e.target === thumbStart) {
        activeMode = 'start';
      } else if (e.target === thumbEnd) {
        activeMode = 'end';
      } else if (e.target === highlightEl && st.endIdx > st.startIdx + MIN_GAP) {
        activeMode = 'pan';
        panAnchorRatio = ratio;
        panInitStart = st.startIdx;
        panInitEnd = st.endIdx;
      } else {
        // Click directly on track: move nearest thumb
        activeMode = Math.abs(idx - st.startIdx) <= Math.abs(idx - st.endIdx) ? 'start' : 'end';
      }

      if (railEl.setPointerCapture && e.pointerId != null) {
        try { railEl.setPointerCapture(e.pointerId); } catch (_) {}
      }

      if (activeMode === 'start') {
        const nextStart = Math.max(0, Math.min(st.endIdx - MIN_GAP, idx));
        onUpdate(nextStart, st.endIdx);
      } else if (activeMode === 'end') {
        const nextEnd = Math.min(st.maxIdx, Math.max(st.startIdx + MIN_GAP, idx));
        onUpdate(st.startIdx, nextEnd);
      }
    };

    const onMove = e => {
      if (!activeMode) return;
      const st = getState();
      if (!st || st.maxIdx < MIN_GAP) return;
      const ratio = ratioFromClientX(e.clientX);
      const idx = Math.round(ratio * st.maxIdx);

      if (activeMode === 'start') {
        const nextStart = Math.max(0, Math.min(st.endIdx - MIN_GAP, idx));
        onUpdate(nextStart, st.endIdx);
      } else if (activeMode === 'end') {
        const nextEnd = Math.min(st.maxIdx, Math.max(st.startIdx + MIN_GAP, idx));
        onUpdate(st.startIdx, nextEnd);
      } else if (activeMode === 'pan') {
        const span = panInitEnd - panInitStart;
        const deltaIdx = Math.round((ratio - panAnchorRatio) * st.maxIdx);
        let nextStart = Math.max(0, Math.min(st.maxIdx - span, panInitStart + deltaIdx));
        let nextEnd = nextStart + span;
        onUpdate(nextStart, nextEnd);
      }
    };

    const onUp = e => {
      if (!activeMode) return;
      activeMode = null;
      if (railEl.releasePointerCapture && e.pointerId != null) {
        try { railEl.releasePointerCapture(e.pointerId); } catch (_) {}
      }
    };

    railEl.addEventListener('pointerdown', onDown);
    window.addEventListener('pointermove', onMove);
    window.addEventListener('pointerup', onUp);
    window.addEventListener('pointercancel', onUp);

    const bindThumbKey = (thumb, isStart) => {
      thumb.addEventListener('keydown', e => {
        const st = getState();
        if (!st || st.maxIdx < MIN_GAP) return;
        const step = Math.max(1, Math.round(st.maxIdx * 0.02));
        let s = st.startIdx;
        let en = st.endIdx;
        if (e.key === 'ArrowLeft' || e.key === 'ArrowDown') {
          if (isStart) s = Math.max(0, s - step);
          else en = Math.max(s + MIN_GAP, en - step);
        } else if (e.key === 'ArrowRight' || e.key === 'ArrowUp') {
          if (isStart) s = Math.min(en - MIN_GAP, s + step);
          else en = Math.min(st.maxIdx, en + step);
        } else {
          return;
        }
        e.preventDefault();
        onUpdate(s, en);
      });
    };
    bindThumbKey(thumbStart, true);
    bindThumbKey(thumbEnd, false);
  }

  // ── Main TroughPeak Controller & Canvas Engine ──

  const TroughPeak = {
    LONG_PALETTE,
    SHORT_PALETTE,
    THRESHOLD_PRESETS,
    DD_WIN_DAYS,
    DEFAULT_DD_VISIBLE_CAP,

    // Pure math exports
    detectCycles,
    computeCardAnalytics,
    computeRollingLookbackDD,
    computeRolling52WDD,
    computeRolling2YDD,
    computeATHDD,
    computeUnderwaterMetrics,
    decimateDDPtsPrecise,
    computeDragMeasurement,
    resolveComparisonSet,
    assignSeriesColors,
    normalizeUnderlyingKey,
    gainBandColor,
    hexToRgba,
    formatDisplayDate,
    isInverseInstrument,
    computeUlcerIndex,
    computeCDaR,
    compute1xEquivalentDD,
    computeVolScaledDD,
    computeDragAdjustedReturn,

    state: {
      activeTicker: '',
      threshold: 30,
      zigzagMode: 'fixed',    // 'fixed' | 'vol_adaptive'
      compareMode: 'pair',    // 'single' | 'pair' | 'underlying' | 'category' | 'custom'
      customPeers: [],        // user-selected comparison tickers in 'custom' mode
      ddVisible: {},          // { ticker: boolean } for drawdown line visibility
      timeRange: 'all',       // '3m' | '6m' | '1y' | '2y' | 'all' | 'year_YYYY' | 'custom'
      zoom: { startIdx: 0, endIdx: 0 },
      ddMode: 'rolling_52w',  // 'rolling_52w' | 'rolling_2y' | 'ath'
      ddHorizon: 'all',       // '3m' | '6m' | '1y' | '3y' | '5y' | 'all' | 'custom'
      ddCustomRange: { startIdx: 0, endIdx: 0, tMin: null, tMax: null },
      tableFilter: 'all',     // 'all' | '1y' | '6m'
      matrixMode: 'basket',   // 'active' | 'basket'
      hover: { x: null, index: null },
      drag: { active: false, locked: false, startIdx: null, currentIdx: null }
    },

    _etfHistoryCache: null,
    _historyPromise: null,
    _catalog: [],
    _catalogMap: {},
    _seriesCache: {},
    _cyclesCache: {},
    _chartCache: null,
    _chartCacheKey: null,
    _ddBaseCanvas: null,
    _ddBaseKey: null,
    _ddViewCache: { key: null, view: null },
    _ddMap: { ready: false, tMin: 0, tMax: 1, padL: 56, chartW: 1, top: 20, chartH: 1, minDD: -10, xOf: null, yOf: null, legendHits: [] },
    _ddHover: { t: null },
    _ddTipT: null,
    _ddLastXY: null,
    _ddRafId: 0,
    _chartRafId: 0,
    _thrRafId: 0,
    _bound: false,
    _resizeObserved: false,

    async fetchETFHistory() {
      if (this._etfHistoryCache) return this._etfHistoryCache;
      if (this._historyPromise) return this._historyPromise;

      this._historyPromise = (async () => {
        const candidates = ['data/flows/etf_history.json', 'docs/data/flows/etf_history.json'];
        for (const url of candidates) {
          try {
            const resp = await fetch(url);
            if (resp.ok) {
              const data = await resp.json();
              if (data && data.history && Object.keys(data.history).length > 0) {
                this._etfHistoryCache = data.history;
                this._seriesCache = {};
                this._cyclesCache = {};
                this._chartCache = null;
                this._chartCacheKey = null;
                this._ddBaseKey = null;
                this._ddViewCache.key = null;
                return this._etfHistoryCache;
              }
            }
          } catch (_) {}
        }
        return null;
      })();

      const result = await this._historyPromise;
      if (!result) {
        this._historyPromise = null;
        return {};
      }
      return result;
    },

    setCatalog(catalogInstruments) {
      this._catalog = Array.isArray(catalogInstruments) ? catalogInstruments : [];
      this._catalogMap = _buildCatalogMap(this._catalog);
    },

    _getSeries(ticker) {
      if (!ticker) return null;
      if (this._seriesCache[ticker]) return this._seriesCache[ticker];
      const raw = this._etfHistoryCache ? this._etfHistoryCache[ticker] : null;
      const norm = _normalizeSeries(raw);
      if (norm) this._seriesCache[ticker] = norm;
      return norm;
    },

    _getComparisonTickers() {
      return resolveComparisonSet(
        this.state.activeTicker,
        this._catalog,
        this.state.compareMode,
        this.state.customPeers,
        null
      );
    },

    _initDDVisibility(peers, reset = false) {
      if (reset) this.state.ddVisible = {};
      const visibleLimit = peers.length > 10 ? DEFAULT_DD_VISIBLE_CAP : peers.length;
      for (let i = 0; i < peers.length; i++) {
        const tk = peers[i];
        if (reset || this.state.ddVisible[tk] === undefined) {
          this.state.ddVisible[tk] = (tk === this.state.activeTicker) || (i < visibleLimit);
        }
      }
      if (this.state.activeTicker) {
        this.state.ddVisible[this.state.activeTicker] = true;
      }
    },

    _getDDTimelineDates() {
      const peers = this._getComparisonTickers();
      const dateSet = new Set();
      for (const tk of peers) {
        if (this.state.ddVisible[tk] === false) continue;
        const ser = this._getSeries(tk);
        if (!ser) continue;
        for (const d of ser.dates) dateSet.add(d);
      }
      if (!dateSet.size) {
        for (const tk of peers) {
          const ser = this._getSeries(tk);
          if (!ser) continue;
          for (const d of ser.dates) dateSet.add(d);
        }
      }
      return Array.from(dateSet).sort();
    },

    _activePriceSeries(ser) {
      if (!ser) return [];
      return (ser.tri && ser.tri.length === ser.closes.length) ? ser.tri : (ser.closes || []);
    },

    _getLatestYangZhangVol(ticker) {
      const ser = this._getSeries(ticker);
      if (!ser || !QuantCore || typeof QuantCore.yangZhangVolatility !== 'function') return null;
      const rawBars = ser.rawBars || (this._etfHistoryCache ? this._etfHistoryCache[ticker] : null);
      if (!rawBars || rawBars.length < 22) return null;
      const yzSeries = QuantCore.yangZhangVolatility(rawBars, 21, false);
      for (let i = yzSeries.length - 1; i >= 0; i--) {
        const v = yzSeries[i];
        if (v != null && isFinite(v) && v > 0) return v;
      }
      return null;
    },

    _getEffectiveThresholdInfo(ticker) {
      const mode = this.state.zigzagMode || 'fixed';
      const fixedPct = this.state.threshold;
      if (mode === 'vol_adaptive') {
        const yzVol = this._getLatestYangZhangVol(ticker);
        if (yzVol != null && yzVol > 0) {
          const adaptivePct = Math.max(2.0, 3.0 * yzVol * Math.sqrt(10) * 100);
          return {
            mode,
            fixedPct,
            effectivePct: adaptivePct,
            yzVol,
            label: `~${adaptivePct.toFixed(1)}% (YZ)`,
            shortTag: `${adaptivePct.toFixed(1)}pct_yz`
          };
        }
      }
      return {
        mode: 'fixed',
        fixedPct,
        effectivePct: fixedPct,
        yzVol: null,
        label: `${fixedPct}%`,
        shortTag: `${fixedPct}pct`
      };
    },

    _getCycles(ticker) {
      const mode = this.state.zigzagMode || 'fixed';
      const key = `${ticker}|${this.state.threshold}|${mode}`;
      if (this._cyclesCache[key]) return this._cyclesCache[key];
      const ser = this._getSeries(ticker);
      if (!ser) return [];

      const yz = mode === 'vol_adaptive' ? this._getLatestYangZhangVol(ticker) : null;
      const prices = this._activePriceSeries(ser);
      const cyc = detectCycles(ser.dates, prices, this.state.threshold, {
        volAdaptive: mode === 'vol_adaptive' && yz != null,
        yzVol: yz
      });
      this._cyclesCache[key] = cyc;
      return cyc;
    },

    async mount(ticker, options = {}) {
      if (options.catalogInstruments) {
        this.setCatalog(options.catalogInstruments);
      }
      if (options.allHistory) {
        this._etfHistoryCache = options.allHistory;
        this._seriesCache = {};
        this._cyclesCache = {};
        this._chartCache = null;
        this._chartCacheKey = null;
        this._ddBaseKey = null;
        this._ddViewCache.key = null;
      }
      await this.fetchETFHistory();

      const targetTicker = ticker || this.state.activeTicker || (this._catalog[0]?.ticker || '');
      if (!targetTicker) return;

      const tickerChanged = this.state.activeTicker !== targetTicker;
      this.state.activeTicker = targetTicker;

      if (tickerChanged) {
        this.state.drag = { active: false, locked: false, startIdx: null, currentIdx: null };
        this.state.hover = { x: null, index: null };
        if (this.state.timeRange === 'custom') this.state.timeRange = '1y';
        if (this.state.ddHorizon === 'custom') this.state.ddHorizon = 'all';
        this._chartCache = null;
        this._chartCacheKey = null;
        this._ddBaseKey = null;
        this._ddViewCache.key = null;
      }

      const activeSer = this._getSeries(targetTicker);
      if (activeSer && this.state.timeRange && this.state.timeRange.startsWith('year_')) {
        const yr = this.state.timeRange.replace('year_', '');
        if (!activeSer.dates.some(d => d && d.startsWith(yr))) {
          this.state.timeRange = '1y';
        }
      }

      const peers = this._getComparisonTickers();
      this._initDDVisibility(peers, tickerChanged);

      this._bindEvents();
      this._initResizeObserver();
      this._syncTimeRangeDefaults();
      this._syncDDRangeDefaults();

      this.renderAll();

      // Guarantee crisp render after Alpine x-show transition reveals container
      if (typeof requestAnimationFrame !== 'undefined') {
        requestAnimationFrame(() => {
          this._chartCache = null;
          this._chartCacheKey = null;
          this._ddBaseKey = null;
          this.renderChart();
          this.renderDrawdownChart();
        });
      }
    },

    setTicker(ticker, options = {}) {
      if (!ticker) return;
      if (options.switchToTicker !== false && typeof window !== 'undefined' && options.notifyApp !== false) {
        window.dispatchEvent(new CustomEvent('trough-peak-ticker', { detail: { ticker } }));
      }
      this.mount(ticker, { catalogInstruments: options.catalogInstruments });
    },

    setCompareMode(mode) {
      if (!['single', 'pair', 'underlying', 'category', 'custom'].includes(mode)) return;
      this.state.compareMode = mode;
      const peers = this._getComparisonTickers();
      this._initDDVisibility(peers, true);
      this._ddBaseKey = null;
      this._ddViewCache.key = null;
      this._syncDDRangeDefaults();
      this.renderAll();
    },

    addCustomPeer(ticker) {
      if (!ticker || !this._catalogMap[ticker]) return;
      this.state.compareMode = 'custom';
      if (ticker !== this.state.activeTicker && !this.state.customPeers.includes(ticker)) {
        this.state.customPeers.push(ticker);
      }
      this.state.ddVisible[ticker] = true;
      this._ddBaseKey = null;
      this._ddViewCache.key = null;
      this._syncDDRangeDefaults();
      this.renderAll();
    },

    removeCustomPeer(ticker) {
      if (!ticker || ticker === this.state.activeTicker) return;
      this.state.customPeers = this.state.customPeers.filter(x => x !== ticker);
      this._ddBaseKey = null;
      this._ddViewCache.key = null;
      this._syncDDRangeDefaults();
      this.renderAll();
    },

    setThreshold(pct) {
      const val = Math.max(0, Math.min(300, Math.round(Number(pct) || 0)));
      if (this.state.threshold === val) return;
      this.state.threshold = val;
      this.state.drag = { active: false, locked: false, startIdx: null, currentIdx: null };
      this._chartCache = null;
      this._chartCacheKey = null;
      this._syncThresholdUI();
      this._scheduleThresholdRender();
    },

    setZigzagMode(mode) {
      if (!['fixed', 'vol_adaptive'].includes(mode)) return;
      if (this.state.zigzagMode === mode) return;
      this.state.zigzagMode = mode;
      this._cyclesCache = {};
      this._chartCache = null;
      this._chartCacheKey = null;
      this._syncThresholdUI();
      this._scheduleThresholdRender();
    },

    setTimeRange(rangeKey) {
      this.state.timeRange = rangeKey;
      this.state.drag = { active: false, locked: false, startIdx: null, currentIdx: null };
      this._syncTimeRangeDefaults();
      this._chartCache = null;
      this._chartCacheKey = null;
      this._renderTimePresets();
      this.renderChart();
    },

    setDDMode(mode) {
      if (!['rolling_52w', 'rolling_2y', 'ath'].includes(mode)) return;
      this.state.ddMode = mode;
      this._ddViewCache.key = null;
      this._ddBaseKey = null;
      this._syncDDControlsUI();
      this.renderDrawdownKPIs();
      this.renderDrawdownChart();
    },

    setDDHorizon(horizon) {
      this.state.ddHorizon = horizon;
      this._syncDDRangeDefaults();
      this._ddViewCache.key = null;
      this._ddBaseKey = null;
      this._syncDDControlsUI();
      this.renderDrawdownKPIs();
      this.renderDrawdownChart();
    },

    setTableFilter(filterKey) {
      if (!['all', '1y', '6m'].includes(filterKey)) return;
      this.state.tableFilter = filterKey;
      this.renderTable();
    },

    setMatrixMode(mode) {
      if (!['active', 'basket'].includes(mode)) return;
      this.state.matrixMode = mode;
      this.renderYearlyMatrix();
    },

    clearMeasurement() {
      if (this.state.drag.startIdx != null || this.state.drag.active || this.state.drag.locked) {
        this.state.drag = { active: false, locked: false, startIdx: null, currentIdx: null };
        this.renderChart(true);
      }
    },

    _scheduleThresholdRender() {
      if (typeof requestAnimationFrame === 'undefined') {
        this.renderCards();
        this.renderChart();
        this.renderTable();
        this.renderYearlyMatrix();
        return;
      }
      if (this._thrRafId) return;
      this._thrRafId = requestAnimationFrame(() => {
        this._thrRafId = 0;
        this.renderCards();
        this.renderChart();
        this.renderTable();
        this.renderYearlyMatrix();
      });
    },

    _getVisibleRange(ser) {
      if (!ser || !ser.dates || !ser.dates.length) return { startIndex: 0, endIndex: 0 };
      const n = ser.dates.length;
      const maxIdx = n - 1;
      let startIndex = 0;
      let endIndex = maxIdx;

      if (this.state.timeRange === 'custom') {
        startIndex = Math.max(0, Math.min(this.state.zoom.startIdx || 0, Math.max(0, maxIdx - 1)));
        endIndex = Math.max(startIndex + 1, Math.min(this.state.zoom.endIdx || maxIdx, maxIdx));
      } else if (this.state.timeRange && this.state.timeRange.startsWith('year_')) {
        const yr = this.state.timeRange.replace('year_', '');
        const idx = ser.dates.findIndex(d => d.slice(0, 4) === yr);
        if (idx === -1) {
          startIndex = 0;
          endIndex = maxIdx;
        } else {
          startIndex = idx;
          let e = idx;
          while (e < n && ser.dates[e].slice(0, 4) <= yr) e++;
          endIndex = Math.max(startIndex + 1, Math.min(maxIdx, e - 1));
        }
      } else if (this.state.timeRange !== 'all') {
        const daysMap = { '3m': 90, '6m': 180, '1y': 365, '2y': 730 };
        const days = daysMap[this.state.timeRange];
        if (days) {
          const lastMs = Date.parse(ser.dates[maxIdx]);
          const cutoff = new Date(lastMs - days * 86400000).toISOString().slice(0, 10);
          startIndex = ser.dates.findIndex(d => d >= cutoff);
          if (startIndex === -1 || startIndex >= maxIdx) startIndex = Math.max(0, maxIdx - 5);
        }
      }

      if (endIndex - startIndex < 1 && maxIdx >= 1) {
        startIndex = Math.max(0, maxIdx - 1);
        endIndex = maxIdx;
      }
      return { startIndex, endIndex };
    },

    _syncTimeRangeDefaults() {
      const ser = this._getSeries(this.state.activeTicker);
      if (!ser) return;
      const { startIndex, endIndex } = this._getVisibleRange(ser);
      this.state.zoom.startIdx = startIndex;
      this.state.zoom.endIdx = endIndex;
      this._syncPriceBrushUI();
    },

    _syncDDRangeDefaults() {
      const timeline = this._getDDTimelineDates();
      if (!timeline.length) return;
      const n = timeline.length;
      const maxIdx = n - 1;
      const tEnd = Date.parse(timeline[maxIdx]);
      let startIdx = 0;

      if (this.state.ddHorizon !== 'all' && this.state.ddHorizon !== 'custom') {
        const days = DD_WIN_DAYS[this.state.ddHorizon];
        if (days) {
          const cutoff = tEnd - days * 86400000;
          while (startIdx < maxIdx && Date.parse(timeline[startIdx]) < cutoff) {
            startIdx++;
          }
        }
      }

      if (this.state.ddHorizon !== 'custom') {
        this.state.ddCustomRange = {
          startIdx,
          endIdx: maxIdx,
          tMin: Date.parse(timeline[startIdx]),
          tMax: tEnd
        };
      }
      this._syncDDBrushUI();
    },

    // ── Rendering Pipeline ──

    renderAll() {
      this._syncThresholdUI();
      this._syncHeaderAndScopeUI();
      this.renderCards();
      this._renderTimePresets();
      this._syncPriceBrushUI();
      this.renderChart();
      this._syncDDControlsUI();
      this._syncDDBrushUI();
      this.renderDDChips();
      this.renderDrawdownKPIs();
      this.renderDrawdownChart();
      this.renderTable();
      this.renderYearlyMatrix();
    },

    _syncThresholdUI() {
      if (typeof document === 'undefined') return;
      const val = this.state.threshold;
      const pct = (val / 300) * 100;
      const info = this._getEffectiveThresholdInfo(this.state.activeTicker);
      const isVol = info.mode === 'vol_adaptive';
      const disp = document.getElementById('tp-threshold-display');
      const fill = document.getElementById('tp-threshold-fill');
      const thumb = document.getElementById('tp-threshold-thumb');
      const rail = document.getElementById('tp-threshold-rail');
      if (disp) disp.textContent = info.label;
      if (fill) fill.style.width = `${pct.toFixed(2)}%`;
      if (thumb) {
        thumb.style.left = `${pct.toFixed(2)}%`;
        thumb.setAttribute('aria-valuenow', String(val));
        thumb.setAttribute('aria-disabled', isVol ? 'true' : 'false');
        thumb.tabIndex = isVol ? -1 : 0;
      }
      if (rail) {
        rail.style.opacity = isVol ? '0.45' : '';
        rail.style.pointerEvents = isVol ? 'none' : '';
        rail.setAttribute('aria-disabled', isVol ? 'true' : 'false');
      }
      document.querySelectorAll('[data-tp-thr]').forEach(btn => {
        btn.classList.toggle('active', !isVol && parseInt(btn.dataset.tpThr, 10) === val);
        btn.disabled = isVol;
        btn.style.opacity = isVol ? '0.45' : '';
      });
      document.querySelectorAll('[data-tp-zz-mode]').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.tpZzMode === (this.state.zigzagMode || 'fixed'));
      });
    },

    _syncZigzagModeUI() {
      if (typeof document === 'undefined') return;
      this._syncThresholdUI();
    },

    _syncHeaderAndScopeUI() {
      if (typeof document === 'undefined') return;
      const tk = this.state.activeTicker;
      const inst = this._catalogMap[tk] || {};
      const ser = this._getSeries(tk);
      const colors = assignSeriesColors(this._getComparisonTickers(), this._catalogMap);
      const activeColor = colors[tk] || '#22d3ee';

      const symEl = document.getElementById('tp-active-symbol');
      const levEl = document.getElementById('tp-active-leverage');
      const nameEl = document.getElementById('tp-active-name');
      const catEl = document.getElementById('tp-active-category');
      const priceEl = document.getElementById('tp-active-price');
      const chgEl = document.getElementById('tp-active-chg');
      const selectEl = document.getElementById('tp-ticker-select');

      if (symEl) {
        symEl.textContent = tk;
        symEl.style.color = activeColor;
      }
      if (levEl) {
        levEl.textContent = inst.leverage || '1x';
        levEl.setAttribute('data-state', isInverseInstrument(inst) ? 'DISTRIBUTION' : 'ACCUMULATION');
      }
      if (nameEl) nameEl.textContent = inst.fund_name || tk;
      if (catEl) {
        const parts = [];
        if (inst.category) parts.push(inst.category);
        if (inst.underlying) parts.push(`Underlying: ${inst.underlying}`);
        catEl.textContent = parts.join(' · ') || 'Catalog Instrument';
      }
      if (selectEl && selectEl.value !== tk) {
        selectEl.value = tk;
      }

      if (ser && ser.closes.length && priceEl && chgEl) {
        const activePrices = this._activePriceSeries(ser);
        const last = ser.closes[ser.closes.length - 1];
        const trLast = activePrices[activePrices.length - 1];
        const trPrev = activePrices.length > 1 ? activePrices[activePrices.length - 2] : trLast;
        const pct = trPrev > 0 ? ((trLast - trPrev) / trPrev) * 100 : 0;
        priceEl.textContent = fmtPrice(last);
        chgEl.textContent = `${pct >= 0 ? '+' : ''}${pct.toFixed(2)}%`;
        chgEl.style.color = pct >= 0 ? '#34d399' : '#fb7185';
      }

      // Dynamic Comparison Scope buttons
      const list = this._catalog;
      const activeUndKey = inst.underlying_id || normalizeUnderlyingKey(inst.underlying, tk);
      const hasUnderlyingPeers = Boolean(activeUndKey && list.some(x => x.ticker !== tk && (x.underlying_id || normalizeUnderlyingKey(x.underlying, x.ticker)) === activeUndKey));
      const hasOppositePair = Boolean(activeUndKey && list.some(x => x.ticker !== tk && (x.underlying_id || normalizeUnderlyingKey(x.underlying, x.ticker)) === activeUndKey && isInverseInstrument(x) !== isInverseInstrument(inst)));

      document.querySelectorAll('[data-tp-compare]').forEach(btn => {
        const mode = btn.dataset.tpCompare;
        btn.classList.toggle('active', mode === this.state.compareMode);
        if (mode === 'pair') {
          btn.title = hasOppositePair
            ? `Head-to-head Bull vs Inverse Bear pair on ${inst.underlying}`
            : 'Shows opposite-direction peer on same underlying when available (single card if unpaired)';
        } else if (mode === 'underlying') {
          btn.disabled = !hasUnderlyingPeers;
          btn.title = hasUnderlyingPeers ? `Compare all ETFs tracking ${inst.underlying}` : 'No other ETFs share this underlying';
        }
      });

      const addPicker = document.getElementById('tp-compare-add');
      if (addPicker && addPicker.options.length <= 1 && this._catalog.length) {
        this.refreshPicker(this._pickerSort || 'aum');
      }
    },

    refreshPicker(sortMode = 'aum') {
      this._pickerSort = ['aum', 'alpha', 'zscore'].includes(sortMode) ? sortMode : 'aum';
      if (typeof document === 'undefined') return;
      const addPicker = document.getElementById('tp-compare-add');
      if (!addPicker || !this._catalog.length) return;
      const sorted = [...this._catalog].sort((a, b) => {
        if (this._pickerSort === 'alpha') return String(a.ticker || '').localeCompare(String(b.ticker || ''));
        if (this._pickerSort === 'zscore') {
          const za = Math.abs(Number(a.flow_zscore) || 0);
          const zb = Math.abs(Number(b.flow_zscore) || 0);
          if (zb !== za) return zb - za;
        }
        const aa = Number(a.aum_m) || 0;
        const ab = Number(b.aum_m) || 0;
        if (ab !== aa) return ab - aa;
        return String(a.ticker || '').localeCompare(String(b.ticker || ''));
      });
      addPicker.innerHTML = '<option value="">Add ETF to Compare</option>' +
        sorted.map(x => {
          const aumM = Number(x.aum_m);
          const aumTag = Number.isFinite(aumM) && aumM > 0
            ? (aumM >= 1000 ? ` · $${(aumM / 1000).toFixed(1)}B` : ` · $${Math.round(aumM)}M`)
            : '';
          return `<option value="${_escapeHtml(x.ticker)}">${_escapeHtml(x.ticker)} (${_escapeHtml(x.leverage || '1x')}${_escapeHtml(aumTag)}) — ${_escapeHtml(x.fund_name)}</option>`;
        }).join('');
    },

    renderCards() {
      if (typeof document === 'undefined') return;
      const grid = document.getElementById('tp-kpi-grid');
      const scopeHint = document.getElementById('tp-compare-hint');
      if (!grid) return;

      const tickers = this._getComparisonTickers();
      const colors = assignSeriesColors(tickers, this._catalogMap);

      if (scopeHint) {
        const effInfo = this._getEffectiveThresholdInfo(this.state.activeTicker);
        const thrTxt = effInfo.mode === 'vol_adaptive'
          ? `Vol-Adaptive YZ (~${effInfo.effectivePct.toFixed(1)}% active)`
          : `>= ${this.state.threshold}% fixed ZigZag threshold`;
        if (this.state.compareMode === 'pair' && tickers.length === 1) {
          scopeHint.textContent = `Showing ${tickers[0]} (no opposite-direction peer on same underlying) · ${thrTxt}`;
        } else {
          scopeHint.textContent = `Comparing ${tickers.length} ${tickers.length === 1 ? 'instrument' : 'instruments'} at ${thrTxt}`;
        }
      }

      grid.innerHTML = tickers.map(tk => {
        const color = colors[tk] || '#22d3ee';
        const inst = this._catalogMap[tk] || {};
        const ser = this._getSeries(tk);
        const isActive = tk === this.state.activeTicker;
        const safeTk = _escapeHtml(tk);

        if (!ser) {
          return `<div class="tp-kpi-card ${isActive ? 'is-active' : ''}" data-tp-card="${safeTk}">
            <div class="tp-kpi-header">
              <span class="tp-kpi-ticker" style="color:${color}">${safeTk}</span>
              <span class="tp-kpi-cycles">NO HISTORY</span>
            </div>
            <div class="tp-kpi-empty">OHLCV history unavailable</div>
          </div>`;
        }

        const cycles = this._getCycles(tk);
        const activePrices = this._activePriceSeries(ser);
        const effLev = (QuantCore && typeof QuantCore.leverageAtDate === 'function' && ser.dates.length)
          ? QuantCore.leverageAtDate(tk, ser.dates[ser.dates.length - 1], inst.leverage_value)
          : inst.leverage_value;
        const stats = computeCardAnalytics(ser.dates, activePrices, ser.volumes, cycles, {
          leverage: effLev
        });
        const levLabel = _escapeHtml(inst.leverage || '');
        const removeBtn = (this.state.compareMode === 'custom' && !isActive)
          ? `<button type="button" class="tp-chip-remove" data-tp-remove-peer="${safeTk}" title="Remove ${safeTk}">&times;</button>`
          : '';

        if (!stats || stats.cyclesCount === 0) {
          return `<div class="tp-kpi-card ${isActive ? 'is-active' : ''}" data-tp-card="${safeTk}">
            <div class="tp-kpi-header">
              <div class="tp-kpi-title-wrap">
                <span class="tp-kpi-ticker" style="color:${color}">${safeTk}</span>
                <span class="tp-kpi-lev">${levLabel}</span>
              </div>
              <div class="tp-kpi-right">
                <span class="tp-kpi-cycles">0 CYCLES</span>
                ${removeBtn}
              </div>
            </div>
            <div class="tp-kpi-hero-row">
              <div class="tp-kpi-hero-left">
                <span class="tp-kpi-lbl">AVG GAIN</span>
                <span class="tp-kpi-val tp-muted">0%</span>
              </div>
              <span class="tp-kpi-median">MED: 0%</span>
            </div>
            <div class="tp-kpi-stats-4">
              <div class="tp-stat-item"><div class="tp-stat-lbl">AVG DAYS</div><div class="tp-stat-val">—</div></div>
              <div class="tp-stat-item"><div class="tp-stat-lbl">AVG GAP</div><div class="tp-stat-val">—</div></div>
              <div class="tp-stat-item"><div class="tp-stat-lbl">MAX GAIN</div><div class="tp-stat-val">—</div></div>
              <div class="tp-stat-item"><div class="tp-stat-lbl">MIN GAIN</div><div class="tp-stat-val">—</div></div>
            </div>
          </div>`;
        }

        const avgGapStr = stats.avgGap != null ? `${stats.avgGap}D` : '—';
        let cycColor = '#e4e4e7';
        if (stats.cycPosRaw != null) {
          if (stats.cycPosRaw >= 100) cycColor = '#22d3ee';
          else if (stats.cycPosRaw > 70) cycColor = '#fb7185';
          else if (stats.cycPosRaw > 30) cycColor = '#fbbf24';
          else cycColor = '#34d399';
        }
        const strStr = stats.stretchZ != null ? `${stats.stretchZ >= 0 ? '+' : ''}${stats.stretchZ.toFixed(1)}&sigma;` : '—';
        const strColor = stats.stretchZ != null ? (stats.stretchZ > 2 ? '#fb7185' : (stats.stretchZ < -2 ? '#34d399' : '#e4e4e7')) : '#8d9aaa';

        const fmtRng = (lbl, v) => {
          if (v == null) return `<span>${lbl}: —</span>`;
          const cls = v <= 30 ? 'tp-soft-green' : (v > 70 ? 'tp-soft-red' : '');
          return `<span>${lbl}: <span class="${cls}">${v}%</span></span>`;
        };

        const pkStr = stats.distFromPeakPct != null
          ? `&darr;Pk: <span class="${stats.distFromPeakPct > -10 ? 'tp-soft-red' : ''}">${stats.distFromPeakPct}%</span>`
          : '&darr;Pk: —';
        const trStr = stats.distFromTroughPct != null
          ? `&uarr;Tr: <span class="${stats.distFromTroughPct < 10 ? 'tp-soft-green' : ''}">${stats.distFromTroughPct >= 0 ? '+' : ''}${stats.distFromTroughPct}%</span>`
          : '&uarr;Tr: —';
        const rng30Str = stats.range30dPct != null ? `Rng: ${stats.range30dPct}%` : 'Rng: —';

        return `<div class="tp-kpi-card ${isActive ? 'is-active' : ''}" data-tp-card="${safeTk}">
          <div class="tp-kpi-header">
            <div class="tp-kpi-title-wrap">
              <span class="tp-kpi-ticker" style="color:${color}">${safeTk}</span>
              <span class="tp-kpi-lev">${levLabel}</span>
            </div>
            <div class="tp-kpi-right">
              <span class="tp-kpi-cycles">${stats.cyclesCount} CYCLES</span>
              ${removeBtn}
            </div>
          </div>
          <div class="tp-kpi-hero-row" title="Mean and median % gain across detected trough-to-peak cycles">
            <div class="tp-kpi-hero-left">
              <span class="tp-kpi-lbl">AVG GAIN</span>
              <span class="tp-kpi-val" style="color:${color}">${Math.round(stats.avgGain)}%</span>
            </div>
            <span class="tp-kpi-median">MED: ${Math.round(stats.medianGain)}%</span>
          </div>
          <div class="tp-kpi-stats-4">
            <div class="tp-stat-item"><div class="tp-stat-lbl">AVG DAYS</div><div class="tp-stat-val">${Math.round(stats.avgDays)}D</div></div>
            <div class="tp-stat-item"><div class="tp-stat-lbl">AVG GAP</div><div class="tp-stat-val">${avgGapStr}</div></div>
            <div class="tp-stat-item"><div class="tp-stat-lbl">MAX GAIN</div><div class="tp-stat-val" style="color:${color}">${Math.round(stats.maxGain)}%</div></div>
            <div class="tp-stat-item"><div class="tp-stat-lbl">MIN GAIN</div><div class="tp-stat-val" style="color:${color}">${Math.round(stats.minGain)}%</div></div>
          </div>
          <div class="tp-micro-analytics">
            <div class="tp-micro-line">
              <span>Cyc: <strong style="color:${cycColor}">${stats.cycPosRaw != null ? stats.cycPosRaw + '%' : '—'}</strong></span>
              <span class="tp-regime-label">${stats.cycLabel || '—'}</span>
              <span>Str: <strong style="color:${strColor}">${strStr}</strong></span>
            </div>
            <div class="tp-cyc-bar-track"><div class="tp-cyc-bar-fill" style="width:${stats.cycPosPct}%;background:${color}"></div></div>
            <div class="tp-micro-line tp-micro-ctx">
              ${fmtRng('1M', stats.range1M)}
              ${fmtRng('3M', stats.range3M)}
              ${fmtRng('6M', stats.range6M)}
            </div>
            <div class="tp-micro-line tp-micro-ctx">
              <span>${pkStr}</span>
              <span>${trStr}</span>
              <span>${rng30Str}</span>
            </div>
          </div>
        </div>`;
      }).join('');
    },

    _renderTimePresets() {
      if (typeof document === 'undefined') return;
      const ser = this._getSeries(this.state.activeTicker);
      const yearsWrap = document.getElementById('tp-years-group');
      const totalBars = ser ? ser.dates.length : 0;

      document.querySelectorAll('[data-tp-range]').forEach(btn => {
        const key = btn.dataset.tpRange;
        btn.classList.toggle('flow-chip-active', this.state.timeRange === key);
        if (key === '6m') btn.disabled = totalBars < 65;
        else if (key === '1y') btn.disabled = totalBars < 130;
        else if (key === '2y') btn.disabled = totalBars < 260;
        else btn.disabled = false;
      });

      if (yearsWrap && ser) {
        const seen = new Set();
        const years = [];
        for (const d of ser.dates) {
          const yr = d.slice(0, 4);
          if (!seen.has(yr)) {
            seen.add(yr);
            years.push(yr);
          }
        }
        years.sort();
        yearsWrap.innerHTML = years.map(yr =>
          `<button type="button" class="flow-chip ${this.state.timeRange === 'year_' + yr ? 'flow-chip-active' : ''}" data-tp-year="${yr}">'${yr.slice(2)}</button>`
        ).join('');
      }
    },

    _syncPriceBrushUI() {
      if (typeof document === 'undefined') return;
      const ser = this._getSeries(this.state.activeTicker);
      const thumbStart = document.getElementById('tp-brush-thumb-start');
      const thumbEnd = document.getElementById('tp-brush-thumb-end');
      const highlight = document.getElementById('tp-brush-highlight');
      const datesLbl = document.getElementById('tp-window-dates');

      if (!ser || !ser.dates.length || !thumbStart || !thumbEnd) return;
      const maxIdx = Math.max(1, ser.dates.length - 1);
      const s = Math.max(0, Math.min(maxIdx - 1, this.state.zoom.startIdx));
      const e = Math.max(s + 1, Math.min(maxIdx, this.state.zoom.endIdx));

      const p1 = (s / maxIdx) * 100;
      const p2 = (e / maxIdx) * 100;

      thumbStart.style.left = `${p1.toFixed(3)}%`;
      thumbEnd.style.left = `${p2.toFixed(3)}%`;
      thumbStart.setAttribute('aria-valuenow', String(s));
      thumbEnd.setAttribute('aria-valuenow', String(e));
      if (highlight) {
        highlight.style.left = `${p1.toFixed(3)}%`;
        highlight.style.width = `${Math.max(0.5, p2 - p1).toFixed(3)}%`;
      }
      if (datesLbl) {
        datesLbl.textContent = `${ser.dates[s]} -> ${ser.dates[e]} (${e - s + 1} sessions)`;
      }
    },

    // ── High-DPI Price + Volume + Cycle Canvas with ImageData Cache ──

    renderChart(overlayOnly = false) {
      if (typeof document === 'undefined') return;
      const canvas = document.getElementById('tp-canvas');
      if (!canvas) return;
      const ser = this._getSeries(this.state.activeTicker);
      const cycles = this._getCycles(this.state.activeTicker);
      const colors = assignSeriesColors(this._getComparisonTickers(), this._catalogMap);
      const tickerColor = colors[this.state.activeTicker] || '#22d3ee';

      const rect = canvas.getBoundingClientRect();
      if (rect.width < 40 || rect.height < 40) return;

      if (!ser) {
        const ctx = canvas.getContext('2d');
        const dpr = (typeof window !== 'undefined' && window.devicePixelRatio) || 1;
        canvas.width = Math.round(rect.width * dpr);
        canvas.height = Math.round(rect.height * dpr);
        ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
        ctx.clearRect(0, 0, rect.width, rect.height);
        return;
      }

      const dpr = (typeof window !== 'undefined' && window.devicePixelRatio) || 1;
      const cacheKey = `${this.state.activeTicker}|${this.state.timeRange}|${this.state.zoom.startIdx}|${this.state.zoom.endIdx}|${this.state.threshold}|${this.state.zigzagMode || 'fixed'}|${Math.round(rect.width)}|${Math.round(rect.height)}|${dpr}`;

      if (overlayOnly && this._chartCache && this._chartCacheKey === cacheKey) {
        const ctx = canvas.getContext('2d');
        ctx.putImageData(this._chartCache, 0, 0);
        this._drawChartOverlay(canvas, ser, tickerColor);
        return;
      }

      this._drawTroughPeakBase(canvas, ser, cycles, tickerColor);
      const ctx = canvas.getContext('2d');
      if (canvas.width > 0 && canvas.height > 0) {
        this._chartCache = ctx.getImageData(0, 0, canvas.width, canvas.height);
        this._chartCacheKey = cacheKey;
      }
      this._drawChartOverlay(canvas, ser, tickerColor);
    },

    _drawTroughPeakBase(canvas, ser, cycles, tickerColor) {
      const ctx = canvas.getContext('2d');
      const dpr = (typeof window !== 'undefined' && window.devicePixelRatio) || 1;
      const rect = canvas.getBoundingClientRect();
      const w = rect.width;
      const h = rect.height;
      const needW = Math.round(w * dpr);
      const needH = Math.round(h * dpr);
      if (canvas.width !== needW || canvas.height !== needH) {
        canvas.width = needW;
        canvas.height = needH;
      }
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx.clearRect(0, 0, w, h);

      const pad = { top: 36, right: 64, bottom: 38, left: 24 };
      const chartW = w - pad.left - pad.right;
      const chartH = h - pad.top - pad.bottom;
      if (chartW < 20 || chartH < 20) return;

      const { startIndex, endIndex } = this._getVisibleRange(ser);
      const activePrices = this._activePriceSeries(ser);
      const visDates = ser.dates.slice(startIndex, endIndex + 1);
      const visCloses = activePrices.slice(startIndex, endIndex + 1);
      const visVols = ser.volumes.slice(startIndex, endIndex + 1);
      const n = visDates.length;

      if (n < 2) {
        ctx.fillStyle = '#8d9aaa';
        ctx.font = '12px ui-monospace, monospace';
        ctx.textAlign = 'center';
        ctx.fillText('Insufficient bars in selected window — expand brush or click ALL', w / 2, h / 2);
        return;
      }

      const dateIdx = new Map();
      for (let i = 0; i < n; i++) dateIdx.set(visDates[i], i);

      let minVal = Infinity, maxVal = -Infinity, maxVol = 1;
      for (let i = 0; i < n; i++) {
        if (visCloses[i] < minVal) minVal = visCloses[i];
        if (visCloses[i] > maxVal) maxVal = visCloses[i];
        if (visVols[i] > maxVol) maxVol = visVols[i];
      }
      const minPrice = (minVal === maxVal) ? minVal * 0.9 : minVal * 0.95;
      const maxPrice = (minVal === maxVal) ? maxVal * 1.1 : maxVal * 1.05;

      const getX = i => pad.left + (i / Math.max(1, n - 1)) * chartW;
      const getY = p => {
        const rng = maxPrice - minPrice;
        if (rng === 0) return pad.top + chartH / 2;
        return pad.top + chartH - ((p - minPrice) / rng) * chartH;
      };
      const getVolY = v => pad.top + chartH - (v / maxVol) * (chartH * 0.28);

      // 1. Cycle Shaded Bands
      for (const c of cycles) {
        if (c.peakDate < visDates[0] || c.troughDate > visDates[n - 1]) continue;
        const sIdx = dateIdx.get(c.troughDate);
        const eIdx = dateIdx.get(c.peakDate);
        const xStart = sIdx != null ? getX(sIdx) : pad.left;
        const xEnd = eIdx != null ? getX(eIdx) : pad.left + chartW;
        if (xEnd > xStart) {
          ctx.fillStyle = gainBandColor(c.gain, 0.09);
          ctx.fillRect(xStart, pad.top, xEnd - xStart, chartH);
        }
      }

      // 2. Band Gain Labels (42px horizontal collision guard)
      ctx.save();
      ctx.font = 'bold 10px ui-monospace, monospace';
      ctx.textAlign = 'center';
      let lastBandLabelX = -999;
      for (const c of cycles) {
        if (c.peakDate < visDates[0] || c.troughDate > visDates[n - 1]) continue;
        const sIdx = dateIdx.get(c.troughDate);
        const eIdx = dateIdx.get(c.peakDate);
        const xStart = sIdx != null ? getX(sIdx) : pad.left;
        const xEnd = eIdx != null ? getX(eIdx) : pad.left + chartW;
        const bandW = xEnd - xStart;
        if (bandW < 42) continue;
        const midX = xStart + bandW / 2;
        if (Math.abs(midX - lastBandLabelX) < 42) continue;
        ctx.fillStyle = gainBandColor(c.gain, 0.88);
        ctx.fillText(`+${Math.round(c.gain)}%`, midX, pad.top + 14);
        lastBandLabelX = midX;
      }
      ctx.restore();

      // 3. Horizontal Price Grid & Right Y-Axis
      ctx.strokeStyle = 'rgba(255, 255, 255, 0.06)';
      ctx.lineWidth = 1;
      ctx.font = '11px ui-monospace, monospace';
      ctx.textAlign = 'left';
      for (let i = 0; i <= 5; i++) {
        const y = pad.top + (i / 5) * chartH;
        ctx.beginPath();
        ctx.moveTo(pad.left, y);
        ctx.lineTo(pad.left + chartW, y);
        ctx.stroke();
        if (i < 5) {
          const price = maxPrice - (i / 5) * (maxPrice - minPrice);
          const lbl = price >= 100
            ? '$' + Math.round(price).toLocaleString('en-US')
            : fmtPrice(price);
          ctx.fillStyle = '#8d9aaa';
          ctx.fillText(lbl, pad.left + chartW + 8, y + 4);
        }
      }

      // 4. Bottom 28% Volume Bars + Trough/Peak Accents
      const barW = Math.max(1, (chartW / n) * 0.78);
      ctx.fillStyle = 'rgba(255, 255, 255, 0.08)';
      for (let i = 0; i < n; i++) {
        const x = getX(i);
        const y = getVolY(visVols[i]);
        ctx.fillRect(x - barW / 2, y, barW, (pad.top + chartH) - y);
      }

      for (const c of cycles) {
        const tIdx = dateIdx.get(c.troughDate);
        const pIdx = dateIdx.get(c.peakDate);
        if (tIdx != null) {
          const x = getX(tIdx), y = getVolY(visVols[tIdx]);
          ctx.fillStyle = 'rgba(251, 113, 133, 0.35)';
          ctx.fillRect(x - barW / 2, y, barW, (pad.top + chartH) - y);
        }
        if (pIdx != null) {
          const x = getX(pIdx), y = getVolY(visVols[pIdx]);
          ctx.fillStyle = 'rgba(52, 211, 153, 0.35)';
          ctx.fillRect(x - barW / 2, y, barW, (pad.top + chartH) - y);
        }
      }

      // 5. Gradient Area Fill & Price Line
      const grad = ctx.createLinearGradient(0, pad.top, 0, pad.top + chartH);
      grad.addColorStop(0, hexToRgba(tickerColor, 0.16));
      grad.addColorStop(1, 'rgba(0, 0, 0, 0)');
      ctx.beginPath();
      ctx.fillStyle = grad;
      for (let i = 0; i < n; i++) {
        const x = getX(i), y = getY(visCloses[i]);
        if (i === 0) ctx.moveTo(x, y);
        else ctx.lineTo(x, y);
      }
      ctx.lineTo(getX(n - 1), pad.top + chartH);
      ctx.lineTo(getX(0), pad.top + chartH);
      ctx.closePath();
      ctx.fill();

      ctx.beginPath();
      ctx.strokeStyle = tickerColor;
      ctx.lineWidth = 2;
      for (let i = 0; i < n; i++) {
        const x = getX(i), y = getY(visCloses[i]);
        if (i === 0) ctx.moveTo(x, y);
        else ctx.lineTo(x, y);
      }
      ctx.stroke();

      // 6. Glowing Pivot Nodes
      for (const c of cycles) {
        const tIdx = dateIdx.get(c.troughDate);
        const pIdx = dateIdx.get(c.peakDate);
        if (tIdx != null) {
          const x = getX(tIdx), y = getY(visCloses[tIdx]);
          ctx.save();
          ctx.shadowColor = '#ef4444';
          ctx.shadowBlur = 9;
          ctx.beginPath();
          ctx.arc(x, y, 4, 0, Math.PI * 2);
          ctx.fillStyle = '#ef4444';
          ctx.fill();
          ctx.shadowBlur = 0;
          ctx.strokeStyle = 'rgba(0,0,0,0.45)';
          ctx.lineWidth = 1;
          ctx.stroke();
          ctx.restore();
        }
        if (pIdx != null) {
          const x = getX(pIdx), y = getY(visCloses[pIdx]);
          ctx.save();
          ctx.shadowColor = '#10b981';
          ctx.shadowBlur = 9;
          ctx.beginPath();
          ctx.arc(x, y, 4, 0, Math.PI * 2);
          ctx.fillStyle = '#10b981';
          ctx.fill();
          ctx.shadowBlur = 0;
          ctx.strokeStyle = 'rgba(0,0,0,0.45)';
          ctx.lineWidth = 1;
          ctx.stroke();
          ctx.restore();
        }
      }

      // Terminal pulse marker
      const lastX = getX(n - 1);
      const lastY = getY(visCloses[n - 1]);
      ctx.save();
      ctx.shadowColor = tickerColor;
      ctx.shadowBlur = 12;
      ctx.beginPath();
      ctx.arc(lastX, lastY, 4.5, 0, Math.PI * 2);
      ctx.fillStyle = tickerColor;
      ctx.fill();
      ctx.shadowBlur = 0;
      ctx.strokeStyle = '#ffffff';
      ctx.lineWidth = 1.5;
      ctx.stroke();
      ctx.restore();

      // 7. Adaptive X-Axis Ticks
      ctx.fillStyle = '#8d9aaa';
      ctx.font = '10.5px ui-monospace, monospace';
      ctx.textAlign = 'center';

      let ticks = [];
      let isYearBoundaryMode = false;
      if (n <= 14) {
        for (let i = 0; i < n; i++) ticks.push(i);
      } else if (n <= 35) {
        ticks.push(0, n - 1);
        let lastWY = '';
        for (let i = 0; i < n; i++) {
          const dt = new Date(visDates[i] + 'T00:00:00Z');
          const wy = `${dt.getUTCFullYear()}-${_getWeekNumber(dt)}`;
          if (wy !== lastWY) {
            if (i !== 0 && i !== n - 1) ticks.push(i);
            lastWY = wy;
          }
        }
      } else if (n <= 65) {
        ticks.push(0, n - 1);
        let lastWY = '';
        let wkIdx = 0;
        for (let i = 0; i < n; i++) {
          const dt = new Date(visDates[i] + 'T00:00:00Z');
          const wy = `${dt.getUTCFullYear()}-${_getWeekNumber(dt)}`;
          if (wy !== lastWY) {
            lastWY = wy;
            wkIdx++;
            if (wkIdx % 2 === 0 && i !== 0 && i !== n - 1) ticks.push(i);
          }
        }
      } else {
        const monthsRange = (Date.parse(visDates[n - 1]) - Date.parse(visDates[0])) / (30 * 86400000);
        if (monthsRange > 36) {
          isYearBoundaryMode = true;
          const yearsRange = monthsRange / 12;
          const maxYearLabels = Math.max(2, Math.floor(chartW / 140));
          let yearInterval = Math.max(1, Math.round(yearsRange / maxYearLabels));
          yearInterval = [1, 2, 3, 5, 10].find(c => c >= yearInterval) || yearInterval;
          const sY = parseInt(visDates[0].slice(0, 4), 10);
          const eY = parseInt(visDates[n - 1].slice(0, 4), 10);
          const firstTickYear = Math.ceil(sY / yearInterval) * yearInterval;
          for (let yr = firstTickYear; yr <= eY; yr += yearInterval) {
            const target = `${yr}-01-01`;
            for (let i = 0; i < n; i++) {
              if (visDates[i] >= target) {
                if (i !== 0 && i !== n - 1) ticks.push(i);
                break;
              }
            }
          }
        } else {
          ticks.push(0, n - 1);
          const maxLabels = Math.max(2, Math.floor(chartW / 140));
          let monthInterval = Math.max(1, Math.round(monthsRange / maxLabels));
          monthInterval = [1, 2, 3, 4, 6, 12, 24].find(c => c >= monthInterval) || monthInterval;
          let lastMonthTotal = -1;
          let lastLoggedMonth = -1;
          for (let i = 0; i < n; i++) {
            const [y, m] = visDates[i].split('-').map(Number);
            const totalMonths = y * 12 + m;
            if (totalMonths !== lastMonthTotal) {
              if (lastLoggedMonth === -1 || (totalMonths - lastLoggedMonth) >= monthInterval) {
                if (i !== 0 && i !== n - 1) ticks.push(i);
                lastLoggedMonth = totalMonths;
              }
              lastMonthTotal = totalMonths;
            }
          }
        }
      }

      ticks = [...new Set(ticks)].sort((a, b) => a - b);
      const spansMultipleYears = visDates[0].slice(0, 4) !== visDates[n - 1].slice(0, 4);
      let lastLabelMonth = -1;
      let lastLabelYear = -1;
      let lastLabelX = -100;
      let lastMonthYearRendered = -1;

      for (let tIdx = 0; tIdx < ticks.length; tIdx++) {
        const idx = ticks[tIdx];
        const d = visDates[idx];
        const [y, m, day] = d.split('-').map(Number);
        const x = getX(idx);
        if (idx !== 0 && idx !== n - 1 && (x < pad.left + 32 || x > pad.left + chartW - 32)) continue;

        let txt = '';
        const mName = MONTHS_SHORT[m - 1] || '';
        if (n <= 14) {
          txt = (m !== lastLabelMonth || tIdx === 0 || tIdx === ticks.length - 1) ? `${mName} ${day}` : String(day);
        } else if (n <= 65) {
          txt = `${mName} ${day}`;
          if (lastLabelYear !== -1 && y !== lastLabelYear) txt += ` ${y}`;
        } else if (isYearBoundaryMode) {
          txt = String(y);
        } else {
          const my = y * 12 + m;
          if (my === lastMonthYearRendered) continue;
          txt = mName + (spansMultipleYears ? ` '${String(y).slice(2)}` : '');
        }

        if (txt) {
          if (lastLabelX > -1 && Math.abs(x - lastLabelX) < 46) continue;
          ctx.fillText(txt, x, pad.top + chartH + 18);
          lastLabelMonth = m;
          lastLabelYear = y;
          lastLabelX = x;
          lastMonthYearRendered = y * 12 + m;

          ctx.beginPath();
          ctx.strokeStyle = 'rgba(255, 255, 255, 0.04)';
          ctx.moveTo(x, pad.top);
          ctx.lineTo(x, pad.top + chartH);
          ctx.stroke();
        }
      }
    },

    _drawChartOverlay(canvas, ser, tickerColor) {
      const ctx = canvas.getContext('2d');
      const dpr = (typeof window !== 'undefined' && window.devicePixelRatio) || 1;
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      const rect = canvas.getBoundingClientRect();
      const w = rect.width;
      const h = rect.height;
      const pad = { top: 36, right: 64, bottom: 38, left: 24 };
      const chartW = w - pad.left - pad.right;
      const chartH = h - pad.top - pad.bottom;

      const { startIndex, endIndex } = this._getVisibleRange(ser);
      const visDates = ser.dates.slice(startIndex, endIndex + 1);
      const visCloses = this._activePriceSeries(ser).slice(startIndex, endIndex + 1);
      const visVols = ser.volumes.slice(startIndex, endIndex + 1);
      const n = visDates.length;
      if (n < 2) return;

      let minVal = Infinity, maxVal = -Infinity;
      for (let i = 0; i < n; i++) {
        if (visCloses[i] < minVal) minVal = visCloses[i];
        if (visCloses[i] > maxVal) maxVal = visCloses[i];
      }
      const minPrice = (minVal === maxVal) ? minVal * 0.9 : minVal * 0.95;
      const maxPrice = (minVal === maxVal) ? maxVal * 1.1 : maxVal * 1.05;
      const getX = i => pad.left + (i / Math.max(1, n - 1)) * chartW;
      const getY = p => {
        const rng = maxPrice - minPrice;
        if (rng === 0) return pad.top + chartH / 2;
        return pad.top + chartH - ((p - minPrice) / rng) * chartH;
      };

      // 1. Direct 2-Point Drag Percentage Measurement Overlay
      const drag = this.state.drag;
      if (drag.startIdx != null && drag.currentIdx != null && drag.startIdx !== drag.currentIdx) {
        const meas = computeDragMeasurement(visDates, visCloses, visVols, drag.startIdx, drag.currentIdx);
        if (meas) {
          const x1 = getX(meas.i1);
          const x2 = getX(meas.i2);
          const y1 = getY(meas.p1);
          const y2 = getY(meas.p2);
          const isPos = meas.diff >= 0;
          const accent = isPos ? '#34d399' : '#fb7185';

          ctx.save();
          ctx.fillStyle = hexToRgba(tickerColor, 0.11);
          ctx.fillRect(x1, pad.top, x2 - x1, chartH);

          ctx.strokeStyle = 'rgba(255, 255, 255, 0.55)';
          ctx.lineWidth = 1;
          ctx.setLineDash([4, 4]);
          ctx.beginPath();
          ctx.moveTo(x1, pad.top);
          ctx.lineTo(x1, pad.top + chartH);
          ctx.moveTo(x2, pad.top);
          ctx.lineTo(x2, pad.top + chartH);
          ctx.stroke();
          ctx.setLineDash([]);

          // Diagonal anchor vector connecting endpoints
          ctx.strokeStyle = accent;
          ctx.lineWidth = 1.6;
          ctx.setLineDash([3, 3]);
          ctx.beginPath();
          ctx.moveTo(x1, y1);
          ctx.lineTo(x2, y2);
          ctx.stroke();
          ctx.setLineDash([]);

          for (const [ex, ey] of [[x1, y1], [x2, y2]]) {
            ctx.beginPath();
            ctx.arc(ex, ey, 4.2, 0, Math.PI * 2);
            ctx.fillStyle = accent;
            ctx.fill();
            ctx.strokeStyle = '#ffffff';
            ctx.lineWidth = 1.4;
            ctx.stroke();
          }

          const sign = isPos ? '+' : '-';
          const pctSign = isPos ? '+' : '';
          const annStr = meas.annPct != null ? ` · Ann ${meas.annPct >= 0 ? '+' : ''}${meas.annPct.toFixed(1)}%` : '';
          const line1 = `${isPos ? 'UP' : 'DOWN'} ${sign}${fmtPrice(Math.abs(meas.diff))} (${pctSign}${meas.pct.toFixed(2)}%)`;
          const line2 = `${formatDisplayDate(meas.startDate)} - ${formatDisplayDate(meas.endDate)} (${meas.bars} bars · ${meas.calDays}D)`;
          const line3 = `Vol: ${fmtVol(meas.volMin)} - ${fmtVol(meas.volMax)}${annStr}`;
          const line4 = drag.locked ? '[Esc / Click to clear]' : 'Release to lock measurement';

          ctx.font = 'bold 12.5px ui-monospace, monospace';
          const w1 = ctx.measureText(line1).width;
          ctx.font = '10.5px ui-monospace, monospace';
          const w2 = ctx.measureText(line2).width;
          const w3 = ctx.measureText(line3).width;
          const w4 = ctx.measureText(line4).width;

          const cardW = Math.min(Math.max(120, chartW - 8), Math.max(w1, w2, w3, w4) + 24);
          const cardH = 74;
          const cardR = 6;
          let cardX = x1 + (x2 - x1) / 2 - cardW / 2;
          cardX = Math.max(pad.left + 4, Math.min(pad.left + chartW - cardW - 4, cardX));
          const cardY = pad.top + 10;

          ctx.save();
          ctx.beginPath();
          ctx.moveTo(cardX + cardR, cardY);
          ctx.lineTo(cardX + cardW - cardR, cardY);
          ctx.quadraticCurveTo(cardX + cardW, cardY, cardX + cardW, cardY + cardR);
          ctx.lineTo(cardX + cardW, cardY + cardH - cardR);
          ctx.quadraticCurveTo(cardX + cardW, cardY + cardH, cardX + cardW - cardR, cardY + cardH);
          ctx.lineTo(cardX + cardR, cardY + cardH);
          ctx.quadraticCurveTo(cardX, cardY + cardH, cardX, cardY + cardH - cardR);
          ctx.lineTo(cardX, cardY + cardR);
          ctx.quadraticCurveTo(cardX, cardY, cardX + cardR, cardY);
          ctx.closePath();
          ctx.fillStyle = 'rgba(13, 17, 28, 0.97)';
          ctx.fill();
          ctx.strokeStyle = 'rgba(255, 255, 255, 0.16)';
          ctx.lineWidth = 1;
          ctx.stroke();
          ctx.clip();

          const maxTextW = Math.max(80, cardW - 20);
          ctx.textAlign = 'left';
          ctx.font = 'bold 12.5px ui-monospace, monospace';
          ctx.fillStyle = accent;
          ctx.fillText(line1, cardX + 12, cardY + 18, maxTextW);
          ctx.font = '10.5px ui-monospace, monospace';
          ctx.fillStyle = '#cbd5e1';
          ctx.fillText(line2, cardX + 12, cardY + 35, maxTextW);
          ctx.fillStyle = '#94a3b8';
          ctx.fillText(line3, cardX + 12, cardY + 50, maxTextW);
          ctx.fillStyle = '#22d3ee';
          ctx.fillText(line4, cardX + 12, cardY + 65, maxTextW);
          ctx.restore();
        }
      }

      // 2. Hover Point Marker (Zero Crosshairs)
      if (this.state.hover.index != null && this.state.hover.index >= 0 && this.state.hover.index < n) {
        const x = getX(this.state.hover.index);
        const py = getY(visCloses[this.state.hover.index]);
        ctx.save();
        ctx.beginPath();
        ctx.arc(x, py, 4.5, 0, Math.PI * 2);
        ctx.fillStyle = tickerColor;
        ctx.fill();
        ctx.strokeStyle = '#ffffff';
        ctx.lineWidth = 1.5;
        ctx.stroke();
        ctx.restore();
      }
    },

    // ── Underwater Drawdown Engine ──

    _buildDDView() {
      const peers = this._getComparisonTickers();
      const full = [];
      let lastTsVis = -Infinity;
      let lastTsAll = -Infinity;

      for (const tk of peers) {
        const ser = this._getSeries(tk);
        if (!ser || !ser.closes.length) continue;
        const activePrices = this._activePriceSeries(ser);
        let res;
        if (this.state.ddMode === 'rolling_2y') {
          res = computeRolling2YDD(ser.dates, activePrices);
        } else if (this.state.ddMode === 'ath') {
          res = computeATHDD(ser.dates, activePrices);
        } else {
          res = computeRolling52WDD(ser.dates, activePrices);
        }
        if (res.ts.length) {
          const endTs = res.ts[res.ts.length - 1];
          if (endTs > lastTsAll) lastTsAll = endTs;
          if (this.state.ddVisible[tk] !== false && endTs > lastTsVis) lastTsVis = endTs;
        }
        full.push({ ticker: tk, ...res, closes: activePrices });
      }

      const lastTs = isFinite(lastTsVis) ? lastTsVis : lastTsAll;
      if (!full.length || !isFinite(lastTs)) return null;

      let tMinCutoff = -Infinity;
      let tMaxCutoff = Infinity;
      if (this.state.ddHorizon === 'custom') {
        if (this.state.ddCustomRange.tMin != null) tMinCutoff = this.state.ddCustomRange.tMin;
        if (this.state.ddCustomRange.tMax != null) tMaxCutoff = this.state.ddCustomRange.tMax;
      } else {
        const days = DD_WIN_DAYS[this.state.ddHorizon];
        if (days) tMinCutoff = lastTs - days * 86400 * 1000;
      }

      const series = [];
      let tMinVis = Infinity;
      let tMaxVis = -Infinity;
      let tMinAll = Infinity;
      let tMaxAll = -Infinity;

      for (const s of full) {
        const pts = [];
        let sWorst = 0;
        const isVis = this.state.ddVisible[s.ticker] !== false;
        for (let i = 0; i < s.ts.length; i++) {
          const t = s.ts[i];
          if (t < tMinCutoff || t > tMaxCutoff) continue;
          const p = {
            t,
            dd: s.dd[i],
            close: s.closes[i],
            peak: s.peaks[i],
            peakTs: s.peakTimestamps[i],
            peakIdx: s.peakIndices[i]
          };
          if (p.t < tMinAll) tMinAll = p.t;
          if (p.t > tMaxAll) tMaxAll = p.t;
          if (isVis) {
            if (p.t < tMinVis) tMinVis = p.t;
            if (p.t > tMaxVis) tMaxVis = p.t;
          }
          if (p.dd < sWorst) sWorst = p.dd;
          pts.push(p);
        }
        if (pts.length) {
          const metrics = computeUnderwaterMetrics(pts, s.closes, s.peaks, s.ts);
          series.push({ ticker: s.ticker, pts, worst: sWorst, metrics });
        }
      }

      if (!series.length) return null;
      let tMin = isFinite(tMinVis) ? tMinVis : tMinAll;
      let tMax = isFinite(tMaxVis) ? tMaxVis : tMaxAll;
      if (this.state.ddHorizon === 'custom' && this.state.ddCustomRange.tMin != null && this.state.ddCustomRange.tMax != null && this.state.ddCustomRange.tMax > this.state.ddCustomRange.tMin) {
        tMin = this.state.ddCustomRange.tMin;
        tMax = this.state.ddCustomRange.tMax;
      }
      if (tMax <= tMin) tMax = tMin + 86400 * 1000;
      return { series, tMin, tMax };
    },

    _getDDView() {
      const peers = this._getComparisonTickers();
      const visSig = peers.filter(t => this.state.ddVisible[t] !== false).join(',');
      const customSig = this.state.ddHorizon === 'custom'
        ? `${this.state.ddCustomRange.tMin}_${this.state.ddCustomRange.tMax}`
        : this.state.ddHorizon;
      const key = `${this.state.ddMode}|${customSig}|${this.state.activeTicker}|${peers.join(',')}|${visSig}`;
      if (this._ddViewCache.key !== key) {
        this._ddViewCache.key = key;
        this._ddViewCache.view = this._buildDDView();
      }
      return this._ddViewCache.view;
    },

    _syncDDControlsUI() {
      if (typeof document === 'undefined') return;
      document.querySelectorAll('[data-tp-dd-mode]').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.tpDdMode === this.state.ddMode);
      });
      document.querySelectorAll('[data-tp-dd-horizon]').forEach(btn => {
        btn.classList.toggle('flow-chip-active', btn.dataset.tpDdHorizon === this.state.ddHorizon);
      });
      const badge = document.getElementById('tp-dd-badge');
      if (badge) {
        const modeLabel = this.state.ddMode === 'rolling_2y' ? 'ROLLING 2Y' : (this.state.ddMode === 'ath' ? 'ALL-TIME HIGH' : 'ROLLING 52W');
        const hLabel = this.state.ddHorizon === 'custom' ? 'CUSTOM' : this.state.ddHorizon.toUpperCase();
        badge.textContent = `${modeLabel} · ${hLabel}`;
      }
      const descEl = document.getElementById('tp-dd-footer-desc');
      if (descEl) {
        if (this.state.ddMode === 'rolling_2y') {
          descEl.textContent = 'Tracks percentage decline from each instrument\'s trailing 730-day rolling high.';
        } else if (this.state.ddMode === 'ath') {
          descEl.textContent = 'Tracks percentage decline from each instrument\'s expanding all-time high across loaded history.';
        } else {
          descEl.textContent = 'Tracks percentage decline from each instrument\'s trailing 52-week (364-day) rolling high.';
        }
      }
    },

    _syncDDBrushUI() {
      if (typeof document === 'undefined') return;
      const timeline = this._getDDTimelineDates();
      const thumbStart = document.getElementById('tp-dd-brush-thumb-start');
      const thumbEnd = document.getElementById('tp-dd-brush-thumb-end');
      const highlight = document.getElementById('tp-dd-brush-highlight');
      const datesLbl = document.getElementById('tp-dd-window-dates');

      if (!timeline.length || !thumbStart || !thumbEnd) return;
      const maxIdx = Math.max(1, timeline.length - 1);
      const s = Math.max(0, Math.min(maxIdx - 1, this.state.ddCustomRange.startIdx));
      const e = Math.max(s + 1, Math.min(maxIdx, this.state.ddCustomRange.endIdx));

      const p1 = (s / maxIdx) * 100;
      const p2 = (e / maxIdx) * 100;

      thumbStart.style.left = `${p1.toFixed(3)}%`;
      thumbEnd.style.left = `${p2.toFixed(3)}%`;
      thumbStart.setAttribute('aria-valuenow', String(s));
      thumbEnd.setAttribute('aria-valuenow', String(e));
      if (highlight) {
        highlight.style.left = `${p1.toFixed(3)}%`;
        highlight.style.width = `${Math.max(0.5, p2 - p1).toFixed(3)}%`;
      }
      if (datesLbl) {
        datesLbl.textContent = `${timeline[s]} -> ${timeline[e]} (${e - s + 1} sessions)`;
      }
    },

    renderDDChips() {
      if (typeof document === 'undefined') return;
      const container = document.getElementById('tp-dd-chips');
      if (!container) return;

      const peers = this._getComparisonTickers();
      const colors = assignSeriesColors(peers, this._catalogMap);
      container.innerHTML = '';

      for (const tk of peers) {
        const isActive = tk === this.state.activeTicker;
        const isVis = this.state.ddVisible[tk] !== false;
        const c = colors[tk] || '#22d3ee';
        const inst = this._catalogMap[tk];
        const chip = document.createElement('button');
        chip.type = 'button';
        chip.className = `tp-etf-chip ${isActive ? 'active' : ''}`;
        chip.textContent = inst && inst.leverage ? `${tk} (${inst.leverage})` : tk;
        chip.title = `${inst ? inst.fund_name : tk} · Click to show/hide curve · Double-click to focus ETF`;
        chip.style.color = c;
        chip.style.borderColor = c;
        chip.style.background = isVis ? (isActive ? hexToRgba(c, 0.20) : hexToRgba(c, 0.08)) : 'transparent';
        chip.style.opacity = isVis ? '1' : '0.42';
        chip.addEventListener('click', () => {
          const visCount = peers.reduce((n, k) => n + (this.state.ddVisible[k] !== false ? 1 : 0), 0);
          if (isVis && visCount > 1) this.state.ddVisible[tk] = false;
          else this.state.ddVisible[tk] = true;
          this._ddViewCache.key = null;
          this._ddBaseKey = null;
          this._syncDDRangeDefaults();
          this.renderDDChips();
          this.renderDrawdownKPIs();
          this.renderDrawdownChart();
        });
        chip.addEventListener('dblclick', () => {
          if (tk !== this.state.activeTicker) {
            this.state.ddVisible[tk] = true;
            this.setTicker(tk);
          }
        });
        container.appendChild(chip);
      }

      if (peers.length > 10) {
        const allBtn = document.createElement('button');
        allBtn.type = 'button';
        allBtn.className = 'tp-pill-btn';
        allBtn.textContent = 'Show All';
        allBtn.title = `Overlay all ${peers.length} peers simultaneously`;
        allBtn.addEventListener('click', () => {
          for (const tk of peers) this.state.ddVisible[tk] = true;
          this._ddViewCache.key = null;
          this._ddBaseKey = null;
          this._syncDDRangeDefaults();
          this.renderDDChips();
          this.renderDrawdownKPIs();
          this.renderDrawdownChart();
        });
        container.appendChild(allBtn);

        const topBtn = document.createElement('button');
        topBtn.type = 'button';
        topBtn.className = 'tp-pill-btn';
        topBtn.textContent = `Top ${DEFAULT_DD_VISIBLE_CAP}`;
        topBtn.title = `Show active ETF + top ${DEFAULT_DD_VISIBLE_CAP - 1} peers by AUM`;
        topBtn.addEventListener('click', () => {
          this._initDDVisibility(peers, true);
          this._ddViewCache.key = null;
          this._ddBaseKey = null;
          this._syncDDRangeDefaults();
          this.renderDDChips();
          this.renderDrawdownKPIs();
          this.renderDrawdownChart();
        });
        container.appendChild(topBtn);
      }
    },

    renderDrawdownKPIs() {
      if (typeof document === 'undefined') return;
      const curEl = document.getElementById('tp-dd-kpi-cur');
      const mddEl = document.getElementById('tp-dd-kpi-mdd');
      const peakEl = document.getElementById('tp-dd-kpi-peak');
      const uwEl = document.getElementById('tp-dd-kpi-underwater');
      if (!curEl || !mddEl || !peakEl || !uwEl) return;

      const view = this._getDDView();
      const tk = this.state.activeTicker;
      const peers = this._getComparisonTickers();
      const colors = assignSeriesColors(peers, this._catalogMap);
      const tickerColor = colors[tk] || '#22d3ee';
      const s = view && view.series ? view.series.find(x => x.ticker === tk) : null;

      const curTitle = this.state.ddHorizon === 'custom' ? 'WINDOW END DRAWDOWN' : 'CURRENT DRAWDOWN';
      const mddTitle = this.state.ddHorizon === 'custom' ? 'MAX DRAWDOWN (WINDOW)' : `MAX DRAWDOWN (${this.state.ddHorizon.toUpperCase()})`;
      const peakTitle = this.state.ddMode === 'rolling_2y' ? '2Y PEAK REFERENCE' : (this.state.ddMode === 'ath' ? 'ALL-TIME HIGH' : '52W PEAK REFERENCE');

      if (!s || !s.pts || !s.pts.length) {
        const setEmpty = (el, lbl) => {
          el.querySelector('.tp-dd-kpi-lbl').textContent = lbl;
          el.querySelector('.tp-dd-kpi-val').textContent = '—';
          el.querySelector('.tp-dd-kpi-sub').textContent = 'Awaiting history';
        };
        setEmpty(curEl, curTitle);
        setEmpty(mddEl, mddTitle);
        setEmpty(peakEl, peakTitle);
        setEmpty(uwEl, 'DAYS UNDERWATER');
        return;
      }

      const m = s.metrics;
      curEl.querySelector('.tp-dd-kpi-lbl').innerHTML = `<span style="color:${tickerColor}">${_escapeHtml(tk)}</span> &middot; ${curTitle}`;
      const curValEl = curEl.querySelector('.tp-dd-kpi-val');
      curValEl.textContent = `${m.currentDD.toFixed(1)}%`;
      curValEl.style.color = m.currentDD < -0.01 ? '#fb7185' : '#34d399';
      curEl.querySelector('.tp-dd-kpi-sub').textContent = m.currentDD < -0.01
        ? `Req. Gain: +${m.breakEvenGain.toFixed(1)}%`
        : (this.state.ddMode === 'rolling_2y' ? 'Trading at 2Y High' : (this.state.ddMode === 'ath' ? 'Trading at ATH' : 'Trading at 52W High'));

      mddEl.querySelector('.tp-dd-kpi-lbl').textContent = mddTitle;
      const mddValEl = mddEl.querySelector('.tp-dd-kpi-val');
      mddValEl.textContent = `${m.maxDD.toFixed(1)}%`;
      mddValEl.style.color = '#fb7185';
      const trgIso = new Date(m.troughDate).toISOString().slice(0, 10);
      mddEl.querySelector('.tp-dd-kpi-sub').textContent = `Trough: ${fmtPrice(m.troughPrice)} (${formatDisplayDate(trgIso)})`;

      peakEl.querySelector('.tp-dd-kpi-lbl').textContent = peakTitle;
      const peakValEl = peakEl.querySelector('.tp-dd-kpi-val');
      peakValEl.textContent = fmtPrice(m.peakPrice);
      peakValEl.style.color = tickerColor;
      const pkIso = new Date(m.peakDate).toISOString().slice(0, 10);
      peakEl.querySelector('.tp-dd-kpi-sub').textContent = `Set on ${formatDisplayDate(pkIso)}`;

      uwEl.querySelector('.tp-dd-kpi-lbl').textContent = 'DAYS UNDERWATER';
      const uwValEl = uwEl.querySelector('.tp-dd-kpi-val');
      uwValEl.textContent = m.daysUnderwater === 0 ? '0d' : `${m.daysUnderwater}d`;
      uwValEl.style.color = m.daysUnderwater > 180 ? '#fbbf24' : '#e4e4e7';
      uwEl.querySelector('.tp-dd-kpi-sub').textContent = m.daysUnderwater === 0
        ? 'Currently at Peak'
        : `Since ${formatDisplayDate(pkIso)}`;
    },

    _buildDDBase(view, w, h, dpr) {
      if (!this._ddBaseCanvas && typeof document !== 'undefined') {
        this._ddBaseCanvas = document.createElement('canvas');
      }
      const base = this._ddBaseCanvas;
      if (!base) return;
      base.width = Math.round(w * dpr);
      base.height = Math.round(h * dpr);
      const ctx = base.getContext('2d');
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx.clearRect(0, 0, w, h);

      this._ddMap.ready = false;
      this._ddMap.legendHits = [];
      if (!view || !view.series.length) return;

      const peers = this._getComparisonTickers();
      const colors = assignSeriesColors(peers, this._catalogMap);
      const visibleSeries = view.series.filter(s => this.state.ddVisible[s.ticker] !== false);
      ctx.font = '11px ui-monospace, monospace';
      let maxLabelW = 0;
      for (const s of (visibleSeries.length ? visibleSeries : view.series)) {
        const lw = ctx.measureText(s.ticker + ' *').width;
        if (lw > maxLabelW) maxLabelW = lw;
      }

      const estChartH = Math.max(40, h - 58);
      const maxPerCol = Math.max(1, Math.floor((estChartH - 8) / 18));
      const legCols = Math.max(1, Math.min(3, Math.ceil((visibleSeries.length || 1) / maxPerCol)));
      const colStride = 26 + maxLabelW;
      const rightPad = Math.min(w * 0.48, 22 + legCols * colStride);
      const pad = { top: 20, right: rightPad, bottom: 38, left: 52 };
      const chartW = w - pad.left - pad.right;
      const chartH = h - pad.top - pad.bottom;
      if (chartW < 20 || chartH < 20) return;

      const { tMin, tMax, series } = view;
      const xOf = t => pad.left + ((t - tMin) / (tMax - tMin)) * chartW;
      let worst = 0;
      for (const s of series) {
        if (this.state.ddVisible[s.ticker] !== false && s.worst < worst) worst = s.worst;
      }
      let minDD = Math.floor(worst / 10) * 10;
      if (minDD > -10) minDD = -10;
      const yOf = val => pad.top + (val / minDD) * chartH;

      Object.assign(this._ddMap, {
        ready: true, tMin, tMax, padL: pad.left, chartW, top: pad.top, chartH, minDD, xOf, yOf
      });

      // Grid & Y labels
      ctx.strokeStyle = 'rgba(255, 255, 255, 0.06)';
      ctx.fillStyle = '#8d9aaa';
      ctx.lineWidth = 1;
      ctx.textAlign = 'right';
      ctx.textBaseline = 'middle';
      const stepY = minDD <= -60 ? 20 : 10;
      for (let v = 0; v >= minDD; v -= stepY) {
        const y = yOf(v);
        ctx.beginPath();
        ctx.moveTo(pad.left, y);
        ctx.lineTo(pad.left + chartW, y);
        ctx.stroke();
        ctx.fillText(`${v}%`, pad.left - 8, y);
      }

      // Zero Line
      ctx.strokeStyle = 'rgba(251, 113, 133, 0.45)';
      ctx.lineWidth = 1.5;
      ctx.beginPath();
      ctx.moveTo(pad.left, yOf(0));
      ctx.lineTo(pad.left + chartW, yOf(0));
      ctx.stroke();

      // Date X ticks
      const spanDays = (tMax - tMin) / 86400000;
      const stepMonths = spanDays <= 400 ? 1 : (spanDays <= 1100 ? 3 : 12);
      const d0 = new Date(tMin);
      let tickY = d0.getUTCFullYear();
      let tickM = d0.getUTCMonth();
      if (stepMonths === 12) { tickY += 1; tickM = 0; }
      else { tickM = (Math.floor(tickM / stepMonths) + 1) * stepMonths; }
      const ticks = [];
      for (let k = 0; ; k++) {
        const t = Date.UTC(tickY, tickM + k * stepMonths, 1);
        if (t > tMax) break;
        if (t >= tMin) ticks.push(t);
      }
      const stride = Math.max(1, Math.ceil(ticks.length / 8));
      ctx.fillStyle = '#8d9aaa';
      ctx.font = '10.5px ui-monospace, monospace';
      ctx.textAlign = 'center';
      ctx.textBaseline = 'top';
      let lastRight = -Infinity;
      for (let i = 0; i < ticks.length; i += stride) {
        const t = ticks[i];
        const d = new Date(t);
        const label = `${MONTHS_SHORT[d.getUTCMonth()]} '${String(d.getUTCFullYear()).slice(2)}`;
        const lw = ctx.measureText(label).width;
        const x = xOf(t);
        if (x - lw / 2 < lastRight + 8) continue;
        ctx.fillText(label, x, pad.top + chartH + 10);
        lastRight = x + lw / 2;
      }

      const activeSym = this.state.activeTicker;
      const inactiveSeries = series.filter(s => this.state.ddVisible[s.ticker] !== false && s.ticker !== activeSym);
      const activeSeries = series.find(s => this.state.ddVisible[s.ticker] !== false && s.ticker === activeSym);

      for (const s of inactiveSeries) {
        const pts = decimateDDPtsPrecise(s.pts, xOf, chartW);
        const c = colors[s.ticker] || '#94a3b8';
        const isInv = isInverseInstrument(this._catalogMap[s.ticker]);
        ctx.save();
        ctx.strokeStyle = c;
        ctx.lineWidth = 1.25;
        ctx.globalAlpha = 0.72;
        if (isInv) ctx.setLineDash([6, 4]);
        ctx.beginPath();
        pts.forEach((p, i) => {
          const x = xOf(p.t), y = yOf(p.dd);
          if (i === 0) ctx.moveTo(x, y);
          else ctx.lineTo(x, y);
        });
        ctx.stroke();
        ctx.restore();
      }

      if (activeSeries) {
        const pts = decimateDDPtsPrecise(activeSeries.pts, xOf, chartW);
        const hex = colors[activeSeries.ticker] || '#22d3ee';
        const isInv = isInverseInstrument(this._catalogMap[activeSeries.ticker]);

        ctx.save();
        const grad = ctx.createLinearGradient(0, yOf(0), 0, yOf(minDD));
        grad.addColorStop(0, hexToRgba(hex, 0.28));
        grad.addColorStop(1, hexToRgba(hex, 0.03));
        ctx.fillStyle = grad;
        ctx.beginPath();
        pts.forEach((p, i) => {
          const x = xOf(p.t), y = yOf(p.dd);
          if (i === 0) ctx.moveTo(x, y);
          else ctx.lineTo(x, y);
        });
        ctx.lineTo(xOf(pts[pts.length - 1].t), yOf(0));
        ctx.lineTo(xOf(pts[0].t), yOf(0));
        ctx.closePath();
        ctx.fill();
        ctx.restore();

        ctx.save();
        ctx.strokeStyle = hex;
        ctx.lineWidth = 2.4;
        if (isInv) ctx.setLineDash([6, 4]);
        ctx.beginPath();
        pts.forEach((p, i) => {
          const x = xOf(p.t), y = yOf(p.dd);
          if (i === 0) ctx.moveTo(x, y);
          else ctx.lineTo(x, y);
        });
        ctx.stroke();
        ctx.restore();

        let worstPt = pts[0];
        for (let i = 1; i < pts.length; i++) {
          if (pts[i].dd < worstPt.dd) worstPt = pts[i];
        }
        if (worstPt && worstPt.dd < -0.01) {
          const wx = xOf(worstPt.t), wy = yOf(worstPt.dd);
          ctx.save();
          ctx.fillStyle = hexToRgba(hex, 0.25);
          ctx.beginPath(); ctx.arc(wx, wy, 8, 0, Math.PI * 2); ctx.fill();
          ctx.strokeStyle = hex; ctx.lineWidth = 1.8;
          ctx.beginPath(); ctx.arc(wx, wy, 4.5, 0, Math.PI * 2); ctx.stroke();
          ctx.fillStyle = '#ffffff';
          ctx.beginPath(); ctx.arc(wx, wy, 2.5, 0, Math.PI * 2); ctx.fill();
          ctx.restore();
        }
      }

      // Right Legend with hitboxes (multi-column safe for large baskets)
      ctx.font = '10.5px ui-monospace, monospace';
      ctx.textAlign = 'left';
      ctx.textBaseline = 'middle';
      const legStartX = pad.left + chartW + 14;
      const legStartY = pad.top + 8;
      let visIdx = 0;
      for (const s of visibleSeries) {
        const col = Math.floor(visIdx / maxPerCol);
        if (col >= legCols) break;
        const row = visIdx % maxPerCol;
        const legX = legStartX + col * colStride;
        const legY = legStartY + row * 18;
        const isAct = s.ticker === activeSym;
        const c = colors[s.ticker] || '#22d3ee';
        ctx.save();
        ctx.fillStyle = c;
        ctx.fillRect(legX, legY - 4, 12, 8);
        if (isAct) {
          ctx.strokeStyle = '#ffffff';
          ctx.lineWidth = 1;
          ctx.strokeRect(legX - 1, legY - 5, 14, 10);
          ctx.fillStyle = '#ffffff';
          ctx.font = 'bold 10.5px ui-monospace, monospace';
        } else {
          ctx.fillStyle = 'rgba(255, 255, 255, 0.78)';
        }
        const label = s.ticker + (isAct ? ' *' : '');
        ctx.fillText(label, legX + 17, legY);
        const lw = ctx.measureText(label).width;
        this._ddMap.legendHits.push({ ticker: s.ticker, x: legX, y: legY - 9, w: 17 + lw, h: 18 });
        ctx.restore();
        visIdx++;
      }
    },

    renderDrawdownChart() {
      if (typeof document === 'undefined') return;
      const canvas = document.getElementById('tp-dd-canvas');
      if (!canvas) return;
      const rect = canvas.getBoundingClientRect();
      if (rect.width < 40 || rect.height < 40) {
        this._ddMap.ready = false;
        this._ddBaseKey = null;
        return;
      }

      const view = this._getDDView();
      const dpr = (typeof window !== 'undefined' && window.devicePixelRatio) || 1;
      const w = rect.width;
      const h = rect.height;
      const needW = Math.round(w * dpr);
      const needH = Math.round(h * dpr);
      if (canvas.width !== needW || canvas.height !== needH) {
        canvas.width = needW;
        canvas.height = needH;
      }

      const peers = this._getComparisonTickers();
      const visSig = peers.filter(t => this.state.ddVisible[t] !== false).join(',');
      const customSig = this.state.ddHorizon === 'custom'
        ? `${this.state.ddCustomRange.tMin}_${this.state.ddCustomRange.tMax}`
        : this.state.ddHorizon;
      const key = `${Math.round(w)}|${Math.round(h)}|${dpr}|${this.state.ddMode}|${customSig}|${this.state.activeTicker}|${visSig}`;
      if (key !== this._ddBaseKey) {
        this._buildDDBase(view, w, h, dpr);
        this._ddBaseKey = this._ddMap.ready ? key : null;
      }

      const ctx = canvas.getContext('2d');
      ctx.setTransform(1, 0, 0, 1, 0, 0);
      ctx.clearRect(0, 0, canvas.width, canvas.height);
      if (this._ddBaseCanvas && this._ddBaseCanvas.width > 0 && this._ddBaseCanvas.height > 0) {
        ctx.drawImage(this._ddBaseCanvas, 0, 0);
      }
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

      if (!view || !this._ddMap.ready || this._ddHover.t == null) return;
      const colors = assignSeriesColors(peers, this._catalogMap);

      ctx.save();
      for (const s of view.series) {
        if (this.state.ddVisible[s.ticker] === false) continue;
        const p = _nearestDDPoint(s.pts, this._ddHover.t);
        if (Math.abs(p.t - this._ddHover.t) > DD_TIP_GAP_MS) continue;
        ctx.fillStyle = colors[s.ticker] || '#22d3ee';
        ctx.strokeStyle = '#ffffff';
        ctx.lineWidth = 1.25;
        ctx.beginPath();
        ctx.arc(this._ddMap.xOf(p.t), this._ddMap.yOf(p.dd), 4, 0, Math.PI * 2);
        ctx.fill();
        ctx.stroke();
      }
      ctx.restore();
    },

    // ── Cycle Detail Table, CSV Export & Yearly Frequency Matrix ──

    _getFilteredCycles(ticker) {
      const cycles = this._getCycles(ticker);
      const ser = this._getSeries(ticker);
      if (!ser || !ser.dates.length || this.state.tableFilter === 'all') {
        return { cycles, filtered: cycles };
      }
      const lastDateMs = Date.parse(ser.dates[ser.dates.length - 1]);
      const daysBack = this.state.tableFilter === '6m' ? 180 : 365;
      const cutoffStr = new Date(lastDateMs - daysBack * 86400000).toISOString().slice(0, 10);
      const filtered = cycles.filter(c => c.troughDate >= cutoffStr);
      return { cycles, filtered };
    },

    renderTable() {
      if (typeof document === 'undefined') return;
      const tk = this.state.activeTicker;
      const effInfo = this._getEffectiveThresholdInfo(tk);
      const titleEl = document.getElementById('tp-table-title');
      const container = document.getElementById('tp-table-container');
      const thrLabel = effInfo.mode === 'vol_adaptive'
        ? `Vol-Adaptive YZ (~${effInfo.effectivePct.toFixed(1)}%)`
        : `>= ${this.state.threshold}%`;
      if (titleEl) titleEl.textContent = `${tk} Cycle Detail (${thrLabel})`;
      if (!container) return;

      document.querySelectorAll('[data-tp-table-filter]').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.tpTableFilter === this.state.tableFilter);
      });

      const ser = this._getSeries(tk);
      if (!ser) {
        container.innerHTML = '<div class="tp-empty-state">Waiting for OHLCV history...</div>';
        return;
      }

      const { cycles, filtered } = this._getFilteredCycles(tk);
      if (!filtered.length) {
        container.innerHTML = '<div class="tp-empty-state">No cycles detected at this threshold in selected period.</div>';
        return;
      }

      const gains = filtered.map(c => c.gain);
      const minG = Math.min(...gains);
      const maxG = Math.max(...gains);
      const maxD = Math.max(1, ...filtered.map(c => c.days));

      const rowsHtml = filtered.slice().reverse().map(c => {
        const idx = cycles.indexOf(c);
        let gapDays = null;
        let gapStr = '—';
        if (idx > 0) {
          const prev = cycles[idx - 1];
          gapDays = Math.round((Date.parse(c.troughDate) - Date.parse(prev.peakDate)) / 86400000);
          gapStr = `${gapDays}D`;
        }

        const ratio = maxG === minG ? 0.5 : (c.gain - minG) / (maxG - minG);
        const [br, bg, bb] = c.gain >= 200 ? [52, 211, 153] : c.gain >= 100 ? [34, 211, 238] : c.gain >= 50 ? [251, 191, 36] : [249, 115, 22];
        const gainBg = `linear-gradient(90deg, rgba(${br},${bg},${bb},0.18) ${Math.round(ratio * 100)}%, transparent ${Math.round(ratio * 100)}%)`;
        const dayRatio = maxD > 0 ? c.days / maxD : 0;
        const dayBg = `linear-gradient(90deg, rgba(148,163,184,0.14) ${Math.round(dayRatio * 100)}%, transparent ${Math.round(dayRatio * 100)}%)`;
        const gapColor = gapDays === null ? '#8d9aaa' : (gapDays < 30 ? '#34d399' : (gapDays < 90 ? '#fbbf24' : '#cbd5e1'));

        return `<tr>
          <td>${c.num}</td>
          <td style="color:#22d3ee">${formatDisplayDate(c.troughDate)}</td>
          <td style="color:#fb7185">${fmtPrice(c.troughPrice)}</td>
          <td style="color:#34d399">${formatDisplayDate(c.peakDate)}</td>
          <td style="color:#34d399">${fmtPrice(c.peakPrice)}</td>
          <td style="color:#34d399;font-weight:700;background:${gainBg}">${fmtPct(c.gain)}</td>
          <td style="background:${dayBg}">${c.days}D</td>
          <td style="color:${gapColor};font-weight:700">${gapStr}</td>
        </tr>`;
      }).join('');

      const n = filtered.length;
      const avgG = gains.reduce((a, b) => a + b, 0) / n;
      const sortedG = [...gains].sort((a, b) => a - b);
      const medG = n % 2 ? sortedG[Math.floor(n / 2)] : (sortedG[n / 2 - 1] + sortedG[n / 2]) / 2;
      const avgD = filtered.reduce((a, c) => a + c.days, 0) / n;
      const allGaps = [];
      for (let i = 0; i < filtered.length; i++) {
        const idx = cycles.indexOf(filtered[i]);
        if (idx > 0) {
          allGaps.push(Math.round((Date.parse(filtered[i].troughDate) - Date.parse(cycles[idx - 1].peakDate)) / 86400000));
        }
      }
      const avgGap = allGaps.length ? Math.round(allGaps.reduce((a, b) => a + b, 0) / allGaps.length) : null;

      container.innerHTML = `<table class="tp-cycle-table">
        <thead><tr>
          <th>#</th>
          <th>TROUGH DATE</th>
          <th>TROUGH $</th>
          <th>PEAK DATE</th>
          <th>PEAK $</th>
          <th>% GAIN</th>
          <th>DAYS</th>
          <th>GAP</th>
        </tr></thead>
        <tbody>${rowsHtml}</tbody>
        <tfoot><tr>
          <td colspan="5" style="text-align:left;font-weight:700">${n} CYCLES (MEDIAN GAIN: ${Math.round(medG)}%)</td>
          <td style="color:#34d399;font-weight:700">~${Math.round(avgG)}%</td>
          <td style="font-weight:700">${Math.round(avgD)}D</td>
          <td style="color:#b1bdca;font-weight:700">${avgGap != null ? avgGap + 'D' : '—'}</td>
        </tr></tfoot>
      </table>`;
    },

    renderYearlyMatrix() {
      if (typeof document === 'undefined') return;
      const container = document.getElementById('tp-matrix-container');
      if (!container) return;

      document.querySelectorAll('[data-tp-matrix-mode]').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.tpMatrixMode === this.state.matrixMode);
      });

      const activeTk = this.state.activeTicker;
      if (this.state.matrixMode === 'active') {
        const cycles = this._getCycles(activeTk);
        if (!cycles.length) {
          container.innerHTML = '<div class="tp-empty-state">No cycles detected at this threshold.</div>';
          return;
        }
        const byYear = {};
        for (const c of cycles) {
          const yr = c.troughDate.slice(0, 4);
          if (!byYear[yr]) byYear[yr] = [];
          byYear[yr].push(c);
        }
        const years = Object.keys(byYear).sort((a, b) => Number(b) - Number(a));
        const rowsHtml = years.map(yr => {
          const list = byYear[yr];
          const gs = list.map(c => c.gain).sort((a, b) => a - b);
          const avgG = gs.reduce((a, b) => a + b, 0) / gs.length;
          const medG = gs.length % 2 ? gs[Math.floor(gs.length / 2)] : (gs[gs.length / 2 - 1] + gs[gs.length / 2]) / 2;
          const avgD = list.reduce((a, c) => a + c.days, 0) / list.length;
          const maxG = gs[gs.length - 1];
          return `<tr>
            <td class="tp-matrix-year">${yr}</td>
            <td>${list.length}</td>
            <td style="color:#34d399;font-weight:700">+${Math.round(avgG)}%</td>
            <td>+${Math.round(medG)}%</td>
            <td>${Math.round(avgD)}D</td>
            <td style="color:#22d3ee;font-weight:700">+${Math.round(maxG)}%</td>
          </tr>`;
        }).join('');

        container.innerHTML = `<table class="tp-matrix-table">
          <thead><tr>
            <th class="tp-matrix-year">YEAR</th>
            <th>CYCLES</th>
            <th>AVG GAIN</th>
            <th>MEDIAN GAIN</th>
            <th>AVG DAYS</th>
            <th>MAX GAIN</th>
          </tr></thead>
          <tbody>${rowsHtml}</tbody>
        </table>`;
        return;
      }

      // Peer Basket matrix (Year x Active Comparison Tickers)
      const tickers = this._getComparisonTickers();
      const colors = assignSeriesColors(tickers, this._catalogMap);
      const allYears = new Set();
      const freq = {};
      const activeYearsByTicker = {};

      for (const tk of tickers) {
        freq[tk] = {};
        const yrSet = new Set();
        const ser = this._getSeries(tk);
        if (ser && ser.dates) {
          for (const d of ser.dates) {
            if (d && d.length >= 4) yrSet.add(d.slice(0, 4));
          }
        }
        activeYearsByTicker[tk] = yrSet;
        for (const c of this._getCycles(tk)) {
          const yr = c.troughDate.slice(0, 4);
          allYears.add(yr);
          if (!freq[tk][yr]) freq[tk][yr] = { count: 0, sumGain: 0 };
          freq[tk][yr].count++;
          freq[tk][yr].sumGain += c.gain;
        }
      }

      const sortedYears = Array.from(allYears).sort((a, b) => Number(b) - Number(a));
      if (!sortedYears.length) {
        container.innerHTML = '<div class="tp-empty-state">No cycles detected across active basket at this threshold.</div>';
        return;
      }

      const allAvgGains = [];
      for (const yr of sortedYears) {
        for (const tk of tickers) {
          const s = freq[tk][yr];
          if (s && s.count > 0) allAvgGains.push(s.sumGain / s.count);
        }
      }
      const minAvg = allAvgGains.length ? Math.min(...allAvgGains) : 0;
      const maxAvg = allAvgGains.length ? Math.max(...allAvgGains) : 0;

      const bodyHtml = sortedYears.map(yr => {
        const cells = tickers.map(tk => {
          const safeTk = _escapeHtml(tk);
          const hadBarsInYear = activeYearsByTicker[tk] && activeYearsByTicker[tk].has(yr);
          if (!hadBarsInYear) {
            return `<td class="tp-matrix-no-data" title="${safeTk} ${yr}: Pre-inception (0 trading bars)">—</td>`;
          }
          const st = freq[tk][yr];
          if (!st || !st.count) {
            const eff = this._getEffectiveThresholdInfo(tk);
            const thrDesc = eff.mode === 'vol_adaptive' ? `~${eff.effectivePct.toFixed(1)}% YZ` : `>= ${this.state.threshold}%`;
            return `<td class="tp-matrix-no-data" title="${safeTk} ${yr}: 0 cycles (${thrDesc})">0</td>`;
          }
          const avgG = st.sumGain / st.count;
          const ratio = maxAvg === minAvg ? 0.5 : (avgG - minAvg) / (maxAvg - minAvg);
          const bg = hexToRgba(colors[tk] || '#22d3ee', 0.08 + ratio * 0.24);
          return `<td style="background:${bg}" title="${safeTk} ${yr}: ${st.count} cycles, avg +${Math.round(avgG)}%">
            <strong>${st.count}</strong> <span class="tp-matrix-pct">(+${Math.round(avgG)}%)</span>
          </td>`;
        }).join('');
        return `<tr><td class="tp-matrix-year">${yr}</td>${cells}</tr>`;
      }).join('');

      const totalsCells = tickers.map(tk => {
        const vals = Object.values(freq[tk]);
        const tot = vals.reduce((a, b) => a + b.count, 0);
        const avgAll = tot > 0 ? vals.reduce((a, b) => a + b.sumGain, 0) / tot : 0;
        return `<td style="color:${colors[tk] || '#22d3ee'};font-weight:700">${tot} <span class="tp-matrix-pct">(+${Math.round(avgAll)}%)</span></td>`;
      }).join('');

      container.innerHTML = `<table class="tp-matrix-table">
        <thead><tr>
          <th class="tp-matrix-year">YEAR</th>
          ${tickers.map(tk => `<th style="color:${colors[tk] || '#22d3ee'}">${_escapeHtml(tk)}</th>`).join('')}
        </tr></thead>
        <tbody>${bodyHtml}</tbody>
        <tfoot><tr class="tp-matrix-totals"><td class="tp-matrix-year">TOTAL</td>${totalsCells}</tr></tfoot>
      </table>`;
    },

    // ── CSV Export Functions ──

    _triggerDownload(filename, csvContent) {
      if (typeof document === 'undefined') return;
      const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = filename;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      URL.revokeObjectURL(url);
    },

    downloadCyclesCSV() {
      const tk = this.state.activeTicker;
      const ser = this._getSeries(tk);
      const { cycles, filtered } = this._getFilteredCycles(tk);
      if (!ser || !filtered.length) return;

      const vols = ser.volumes;
      const dates = ser.dates;
      const getVolStats = dateStr => {
        const idx = dates.indexOf(dateStr);
        if (idx === -1) return Array(10).fill('');
        const avg = arr => arr.length ? (arr.reduce((a, b) => a + b, 0) / arr.length).toFixed(0) : '';
        const trEnd = Math.max(0, idx - 3);
        const trail = n => avg(vols.slice(Math.max(0, trEnd - n), trEnd));
        return [
          vols[idx].toFixed(0),
          avg(vols.slice(Math.max(0, idx - 3), Math.min(vols.length, idx + 4))),
          avg(vols.slice(Math.max(0, idx - 3), idx)),
          avg(vols.slice(idx + 1, Math.min(vols.length, idx + 4))),
          trail(30), trail(60), trail(90), trail(120), trail(150), trail(180)
        ];
      };

      const headers = [
        '#', 'Trough Date', 'Trough Price', 'Peak Date', 'Peak Price', '% Gain', 'Days', 'Gap',
        'Trough Exact Vol', 'Trough +/-3d Avg Vol', 'Trough -3d Avg Vol', 'Trough +3d Avg Vol',
        'Trough Trail 30d Vol', 'Trough Trail 60d Vol', 'Trough Trail 90d Vol',
        'Trough Trail 120d Vol', 'Trough Trail 150d Vol', 'Trough Trail 180d Vol',
        'Peak Exact Vol', 'Peak +/-3d Avg Vol', 'Peak -3d Avg Vol', 'Peak +3d Avg Vol',
        'Peak Trail 30d Vol', 'Peak Trail 60d Vol', 'Peak Trail 90d Vol',
        'Peak Trail 120d Vol', 'Peak Trail 150d Vol', 'Peak Trail 180d Vol'
      ];

      const rows = filtered.map(c => {
        const idx = cycles.indexOf(c);
        const gap = idx > 0
          ? Math.round((Date.parse(c.troughDate) - Date.parse(cycles[idx - 1].peakDate)) / 86400000)
          : '';
        return [
          c.num, c.troughDate, c.troughPrice.toFixed(4), c.peakDate, c.peakPrice.toFixed(4),
          c.gain.toFixed(2), c.days, gap, ...getVolStats(c.troughDate), ...getVolStats(c.peakDate)
        ].join(',');
      });

      const effInfo = this._getEffectiveThresholdInfo(tk);
      this._triggerDownload(`${tk}_cycles_${effInfo.shortTag}.csv`, [headers.join(','), ...rows].join('\n'));
    },

    downloadOHLCVCSV() {
      const tk = this.state.activeTicker;
      const raw = this._etfHistoryCache ? this._etfHistoryCache[tk] : null;
      if (!raw || !raw.length) return;
      const headers = ['Date', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Adj_Close_TR', 'Volume', 'Dollar_Volume'];
      const rows = [];
      for (const b of raw) {
        if (!b || !b.d) continue;
        const c = Number(b.c != null ? b.c : b.close);
        if (!Number.isFinite(c) || c <= 0) continue;
        const o = Number.isFinite(Number(b.o)) ? Number(b.o) : c;
        const h = Number.isFinite(Number(b.h)) ? Number(b.h) : c;
        const l = Number.isFinite(Number(b.l)) ? Number(b.l) : c;
        const tc = (b.tc != null && Number.isFinite(Number(b.tc)) && Number(b.tc) > 0) ? Number(b.tc) : c;
        const vRaw = Number(b.v != null ? b.v : (b.volume || 0));
        const v = Number.isFinite(vRaw) && vRaw > 0 ? vRaw : 0;
        rows.push([
          b.d, tk, o.toFixed(4), h.toFixed(4), l.toFixed(4),
          c.toFixed(4), tc.toFixed(4), Math.round(v), (c * v).toFixed(2)
        ].join(','));
      }
      if (!rows.length) return;
      this._triggerDownload(`${tk}_ohlcv_history.csv`, [headers.join(','), ...rows].join('\n'));
    },

    downloadDrawdownCSV() {
      const view = this._getDDView();
      if (!view || !view.series.length) return;
      const visible = view.series.filter(s => this.state.ddVisible[s.ticker] !== false);
      const records = [];
      for (const s of visible) {
        for (const p of s.pts) {
          const daysUnder = p.dd < -0.01 ? Math.max(0, Math.round((p.t - p.peakTs) / 86400000)) : 0;
          records.push({
            t: p.t,
            date: new Date(p.t).toISOString().slice(0, 10),
            ticker: s.ticker,
            close: p.close.toFixed(4),
            peak: p.peak.toFixed(4),
            dd: p.dd.toFixed(2),
            daysUnder
          });
        }
      }
      records.sort((a, b) => a.t - b.t || a.ticker.localeCompare(b.ticker));
      const headers = ['Date', 'Ticker', 'Adj_Close_TR', 'Peak_TR', 'Drawdown_Pct', 'Days_Underwater'];
      const rows = records.map(r => [r.date, r.ticker, r.close, r.peak, r.dd, r.daysUnder].join(','));
      this._triggerDownload(`${this.state.activeTicker}_drawdown_${this.state.ddMode}_${this.state.ddHorizon}.csv`, [headers.join(','), ...rows].join('\n'));
    },

    // ── Event Binding & ResizeObserver ──

    _initResizeObserver() {
      if (typeof window === 'undefined' || typeof ResizeObserver === 'undefined' || this._resizeObserved) return;
      const pWrap = document.getElementById('tp-canvas-wrap');
      const dWrap = document.getElementById('tp-dd-canvas-wrap');
      if (!pWrap && !dWrap) return;

      this._resizeObserver = new ResizeObserver(entries => {
        let shouldRedraw = false;
        for (const entry of entries) {
          const cr = entry.contentRect;
          if (!cr || cr.width < 40 || cr.height < 40) continue;
          const key = entry.target.id;
          const w = Math.round(cr.width);
          const h = Math.round(cr.height);
          if (!this._lastWrapSizes) this._lastWrapSizes = {};
          const prev = this._lastWrapSizes[key];
          if (!prev || prev.w !== w || prev.h !== h) {
            this._lastWrapSizes[key] = { w, h };
            shouldRedraw = true;
          }
        }
        if (!shouldRedraw) return;
        this._chartCache = null;
        this._chartCacheKey = null;
        this._ddBaseKey = null;
        this.renderChart();
        this.renderDrawdownChart();
      });
      if (pWrap) this._resizeObserver.observe(pWrap);
      if (dWrap) this._resizeObserver.observe(dWrap);
      this._resizeObserved = true;
    },

    _bindEvents() {
      if (typeof document === 'undefined' || this._bound) return;
      const rootEl = document.getElementById('tp-studio-root');
      if (!rootEl) return;

      // 1. Delegated click handler for buttons & cards
      rootEl.addEventListener('click', e => {
        const rmBtn = e.target.closest('[data-tp-remove-peer]');
        if (rmBtn) {
          e.stopPropagation();
          this.removeCustomPeer(rmBtn.dataset.tpRemovePeer);
          return;
        }
        const card = e.target.closest('[data-tp-card]');
        if (card) {
          const tk = card.dataset.tpCard;
          if (tk && tk !== this.state.activeTicker) this.setTicker(tk);
          return;
        }
        const btn = e.target.closest('button');
        if (!btn) return;
        if (btn.dataset.tpThr) this.setThreshold(btn.dataset.tpThr);
        else if (btn.dataset.tpZzMode) this.setZigzagMode(btn.dataset.tpZzMode);
        else if (btn.dataset.tpCompare) this.setCompareMode(btn.dataset.tpCompare);
        else if (btn.dataset.tpRange) this.setTimeRange(btn.dataset.tpRange);
        else if (btn.dataset.tpYear) this.setTimeRange(`year_${btn.dataset.tpYear}`);
        else if (btn.dataset.tpDdMode) this.setDDMode(btn.dataset.tpDdMode);
        else if (btn.dataset.tpDdHorizon) this.setDDHorizon(btn.dataset.tpDdHorizon);
        else if (btn.dataset.tpTableFilter) this.setTableFilter(btn.dataset.tpTableFilter);
        else if (btn.dataset.tpMatrixMode) this.setMatrixMode(btn.dataset.tpMatrixMode);
        else if (btn.id === 'tp-download-cycles-csv') this.downloadCyclesCSV();
        else if (btn.id === 'tp-download-ohlcv-csv') this.downloadOHLCVCSV();
        else if (btn.id === 'tp-download-dd-csv') this.downloadDrawdownCSV();
      });

      const addPicker = document.getElementById('tp-compare-add');
      if (addPicker) {
        addPicker.addEventListener('change', e => {
          if (e.target.value) {
            this.addCustomPeer(e.target.value);
            e.target.value = '';
          }
        });
      }

      // 2. Custom Threshold Slider (PointerEvents)
      _bindCustomSingleSlider(
        document.getElementById('tp-threshold-rail'),
        document.getElementById('tp-threshold-thumb'),
        () => ({ min: 0, max: 300, step: 1, val: this.state.threshold }),
        val => {
          if (this.state.zigzagMode !== 'vol_adaptive') this.setThreshold(val);
        }
      );

      // 3. Custom Price Chart Dual-Handle Brush
      _bindCustomDualBrush(
        document.getElementById('tp-brush-rail'),
        document.getElementById('tp-brush-thumb-start'),
        document.getElementById('tp-brush-thumb-end'),
        document.getElementById('tp-brush-highlight'),
        () => {
          const ser = this._getSeries(this.state.activeTicker);
          if (!ser) return null;
          return { startIdx: this.state.zoom.startIdx, endIdx: this.state.zoom.endIdx, maxIdx: Math.max(1, ser.dates.length - 1) };
        },
        (nextStart, nextEnd) => {
          this.state.timeRange = 'custom';
          this.state.zoom.startIdx = nextStart;
          this.state.zoom.endIdx = nextEnd;
          this.state.drag = { active: false, locked: false, startIdx: null, currentIdx: null };
          this._chartCache = null;
          this._chartCacheKey = null;
          this._renderTimePresets();
          this._syncPriceBrushUI();
          this.renderChart();
        }
      );

      // 4. Custom Drawdown Dual-Handle Brush (union timeline across active comparison peers)
      _bindCustomDualBrush(
        document.getElementById('tp-dd-brush-rail'),
        document.getElementById('tp-dd-brush-thumb-start'),
        document.getElementById('tp-dd-brush-thumb-end'),
        document.getElementById('tp-dd-brush-highlight'),
        () => {
          const timeline = this._getDDTimelineDates();
          if (!timeline.length) return null;
          return { startIdx: this.state.ddCustomRange.startIdx, endIdx: this.state.ddCustomRange.endIdx, maxIdx: Math.max(1, timeline.length - 1) };
        },
        (nextStart, nextEnd) => {
          const timeline = this._getDDTimelineDates();
          if (!timeline.length) return;
          this.state.ddHorizon = 'custom';
          this.state.ddCustomRange = {
            startIdx: nextStart,
            endIdx: nextEnd,
            tMin: Date.parse(timeline[nextStart]),
            tMax: Date.parse(timeline[nextEnd])
          };
          this._ddViewCache.key = null;
          this._ddBaseKey = null;
          this._syncDDControlsUI();
          this._syncDDBrushUI();
          this.renderDrawdownKPIs();
          this.renderDrawdownChart();
        }
      );

      // 5. Price Canvas Hover & Direct 2-Point Drag Measurement (Mouse + Touch)
      const tpCanvas = document.getElementById('tp-canvas');
      const tpTooltip = document.getElementById('tp-tooltip');
      if (tpCanvas && tpTooltip) {
        const idxFromClientX = (clientX, clampBounds = false) => {
          const ser = this._getSeries(this.state.activeTicker);
          if (!ser) return null;
          const rect = tpCanvas.getBoundingClientRect();
          const padL = 24;
          const padR = 64;
          const chartW = rect.width - padL - padR;
          const x = clientX - rect.left;
          if (chartW <= 0) return null;
          if (!clampBounds && (x < padL - 6 || x > rect.width - padR + 6)) return null;
          const { startIndex, endIndex } = this._getVisibleRange(ser);
          const count = endIndex - startIndex + 1;
          if (count < 2) return null;
          const pct = Math.max(0, Math.min(1, (x - padL) / chartW));
          return {
            idx: Math.round(pct * (count - 1)),
            startIndex,
            x: Math.max(padL, Math.min(rect.width - padR, x)),
            rect
          };
        };

        const handleHoverMove = (clientX, clientY) => {
          const ser = this._getSeries(this.state.activeTicker);
          const hit = idxFromClientX(clientX, this.state.drag.active);
          if (!ser || !hit) {
            this.state.hover = { x: null, index: null };
            tpTooltip.style.display = 'none';
            this.renderChart(true);
            return;
          }

          const { idx, startIndex, x, rect } = hit;
          const absIdx = startIndex + idx;
          this.state.hover = { x, index: idx };

          if (this.state.drag.active) {
            this.state.drag.currentIdx = idx;
            tpTooltip.style.display = 'none';
          } else {
            const date = ser.dates[absIdx];
            const activePrices = this._activePriceSeries(ser);
            const price = activePrices[absIdx];
            const rawClose = ser.closes[absIdx];
            const vol = ser.volumes[absIdx] || 0;
            const prev = absIdx > 0 ? activePrices[absIdx - 1] : null;
            const diff = prev != null ? price - prev : null;
            const pct = prev != null && prev > 0 ? (diff / prev) * 100 : null;
            const cyc = this._getCycles(this.state.activeTicker);
            let pivotBadge = '';
            for (const c of cyc) {
              if (c.troughDate === date) { pivotBadge = `<div class="tp-tt-badge tp-tt-trough">CYCLE #${c.num} TROUGH</div>`; break; }
              if (c.peakDate === date) { pivotBadge = `<div class="tp-tt-badge tp-tt-peak">CYCLE #${c.num} PEAK (+${c.gain.toFixed(1)}%)</div>`; break; }
            }
            const rawRow = (rawClose != null && Math.abs(rawClose - price) > 0.01)
              ? `<div class="tp-tt-row"><span class="tp-tt-lbl">RAW CLOSE</span><span class="tp-tt-val">${fmtPrice(rawClose)}</span></div>`
              : '';
            const priceLbl = rawRow ? 'TRI CLOSE' : 'CLOSE';
            const chgRow = diff != null
              ? `<div class="tp-tt-row"><span class="tp-tt-lbl">CHG</span><span class="tp-tt-val" style="color:${diff >= 0 ? '#34d399' : '#fb7185'}">${diff >= 0 ? '+' : '-'}${fmtPrice(Math.abs(diff))} (${pct >= 0 ? '+' : ''}${pct.toFixed(2)}%)</span></div>`
              : '';
            tpTooltip.innerHTML = `
              <div class="tp-tt-date">${formatDisplayDate(date)}</div>
              ${pivotBadge}
              <div class="tp-tt-row"><span class="tp-tt-lbl">${priceLbl}</span><span class="tp-tt-val">${fmtPrice(price)}</span></div>
              ${rawRow}
              ${chgRow}
              <div class="tp-tt-row"><span class="tp-tt-lbl">VOL</span><span class="tp-tt-val">${fmtVol(vol)}</span></div>
            `;
            tpTooltip.style.display = 'block';
            const tw = tpTooltip.offsetWidth || 180;
            const th = tpTooltip.offsetHeight || 90;
            const y = clientY - rect.top;
            const leftPos = (x + 14 + tw > rect.width - 8) ? Math.max(8, x - tw - 12) : x + 14;
            const topPos = Math.max(6, Math.min(rect.height - th - 6, y - 16));
            tpTooltip.style.left = `${Math.round(leftPos)}px`;
            tpTooltip.style.top = `${Math.round(topPos)}px`;
          }

          if (!this._chartRafId && typeof requestAnimationFrame !== 'undefined') {
            this._chartRafId = requestAnimationFrame(() => {
              this._chartRafId = 0;
              this.renderChart(true);
            });
          } else if (typeof requestAnimationFrame === 'undefined') {
            this.renderChart(true);
          }
        };

        tpCanvas.addEventListener('mousemove', e => {
          if (!this.state.drag.active) handleHoverMove(e.clientX, e.clientY);
        });

        window.addEventListener('mousemove', e => {
          if (this.state.drag.active) handleHoverMove(e.clientX, e.clientY);
        });

        tpCanvas.addEventListener('mousedown', e => {
          if (e.button !== 0) return;
          const hit = idxFromClientX(e.clientX, true);
          if (!hit) return;
          this.state.drag = { active: true, locked: false, startIdx: hit.idx, currentIdx: hit.idx };
          this.state.hover = { x: hit.x, index: hit.idx };
          this.renderChart(true);
        });

        window.addEventListener('mouseup', () => {
          if (!this.state.drag.active) return;
          this.state.drag.active = false;
          if (this.state.drag.startIdx != null && this.state.drag.currentIdx != null && this.state.drag.startIdx !== this.state.drag.currentIdx) {
            this.state.drag.locked = true;
          } else {
            this.state.drag = { active: false, locked: false, startIdx: null, currentIdx: null };
          }
          this.renderChart(true);
        });

        tpCanvas.addEventListener('mouseleave', () => {
          if (this.state.drag.active) return;
          this.state.hover = { x: null, index: null };
          tpTooltip.style.display = 'none';
          this.renderChart(true);
        });

        // Passive touch inspection without trapping mobile vertical scroll
        tpCanvas.addEventListener('touchstart', e => {
          if (!e.touches || !e.touches.length) return;
          handleHoverMove(e.touches[0].clientX, e.touches[0].clientY);
        }, { passive: true });

        tpCanvas.addEventListener('touchmove', e => {
          if (!e.touches || !e.touches.length) return;
          handleHoverMove(e.touches[0].clientX, e.touches[0].clientY);
        }, { passive: true });

        tpCanvas.addEventListener('touchend', () => {
          this.state.hover = { x: null, index: null };
          tpTooltip.style.display = 'none';
          this.renderChart(true);
        }, { passive: true });
      }

      // Escape key clears locked 2-point measurement
      if (typeof window !== 'undefined') {
        window.addEventListener('keydown', e => {
          if (e.key === 'Escape') this.clearMeasurement();
        });
        window.addEventListener('resize', () => {
          this._chartCache = null;
          this._chartCacheKey = null;
          this._ddBaseKey = null;
          this.renderChart();
          this.renderDrawdownChart();
        });
      }

      // 6. Drawdown Canvas Hover & Legend Click
      const ddCanvas = document.getElementById('tp-dd-canvas');
      const ddTooltip = document.getElementById('tp-dd-tooltip');
      if (ddCanvas && ddTooltip) {
        const updateDDHover = () => {
          const view = this._getDDView();
          if (!view || !this._ddMap.ready || !this._ddLastXY) return;
          const rect = ddCanvas.getBoundingClientRect();
          const { x, y } = this._ddLastXY;
          const t = this._ddMap.tMin + ((x - this._ddMap.padL) / this._ddMap.chartW) * (this._ddMap.tMax - this._ddMap.tMin);
          let snap = null;
          let bestDt = Infinity;
          for (const s of view.series) {
            if (this.state.ddVisible[s.ticker] === false) continue;
            const p = _nearestDDPoint(s.pts, t);
            const dt = Math.abs(p.t - t);
            if (dt < bestDt) { bestDt = dt; snap = p; }
          }
          if (!snap) return;
          this._ddHover.t = snap.t;
          if (this._ddTipT !== snap.t) {
            this._ddTipT = snap.t;
            const peers = this._getComparisonTickers();
            const colors = assignSeriesColors(peers, this._catalogMap);
            const visRows = view.series
              .filter(s => this.state.ddVisible[s.ticker] !== false)
              .map(s => {
                const p = _nearestDDPoint(s.pts, snap.t);
                const near = Math.abs(p.t - snap.t) <= DD_TIP_GAP_MS;
                return { s, p, near };
              });
            visRows.sort((a, b) => {
              if (a.s.ticker === this.state.activeTicker) return -1;
              if (b.s.ticker === this.state.activeTicker) return 1;
              return (a.near ? a.p.dd : 1) - (b.near ? b.p.dd : 1);
            });
            const cappedRows = visRows.slice(0, 10);
            const rows = cappedRows.map(({ s, p, near }) => {
                const isAct = s.ticker === this.state.activeTicker;
                const c = colors[s.ticker] || '#22d3ee';
                return `<div class="tp-tt-row" style="${isAct ? 'font-weight:700;' : ''}">
                  <span class="tp-tt-lbl" style="color:${c}">${_escapeHtml(s.ticker)}${isAct ? ' *' : ''}</span>
                  <span class="tp-tt-val" style="color:${c}">${near ? p.dd.toFixed(2) + '%' : '—'}</span>
                </div>`;
              }).join('') + (visRows.length > 10 ? `<div class="tp-tt-row"><span class="tp-tt-lbl">+${visRows.length - 10} more peers</span><span class="tp-tt-val"></span></div>` : '');
            const iso = new Date(snap.t).toISOString().slice(0, 10);
            ddTooltip.innerHTML = `<div class="tp-tt-date">${formatDisplayDate(iso)}</div>${rows}`;
          }
          ddTooltip.style.display = 'block';
          const tw = ddTooltip.offsetWidth || 160;
          const th = ddTooltip.offsetHeight || 70;
          ddTooltip.style.left = `${Math.round((x + 14 + tw > rect.width - 8) ? Math.max(8, x - tw - 12) : x + 14)}px`;
          const topPos = th <= rect.height - 12 ? Math.max(6, Math.min(rect.height - th - 6, y - 16)) : 6;
          ddTooltip.style.top = `${Math.round(topPos)}px`;
          this.renderDrawdownChart();
        };

        ddCanvas.addEventListener('mousemove', e => {
          const rect = ddCanvas.getBoundingClientRect();
          this._ddLastXY = { x: e.clientX - rect.left, y: e.clientY - rect.top };
          if (!this._ddRafId && typeof requestAnimationFrame !== 'undefined') {
            this._ddRafId = requestAnimationFrame(() => {
              this._ddRafId = 0;
              updateDDHover();
            });
          } else if (typeof requestAnimationFrame === 'undefined') {
            updateDDHover();
          }
        });

        const handleDDTouch = e => {
          if (!e.touches || !e.touches.length) return;
          const rect = ddCanvas.getBoundingClientRect();
          this._ddLastXY = { x: e.touches[0].clientX - rect.left, y: e.touches[0].clientY - rect.top };
          updateDDHover();
        };

        ddCanvas.addEventListener('touchstart', handleDDTouch, { passive: true });
        ddCanvas.addEventListener('touchmove', handleDDTouch, { passive: true });
        ddCanvas.addEventListener('touchend', () => {
          this._ddHover.t = null;
          this._ddTipT = null;
          ddTooltip.style.display = 'none';
          this.renderDrawdownChart();
        }, { passive: true });

        ddCanvas.addEventListener('mouseleave', () => {
          this._ddHover.t = null;
          this._ddTipT = null;
          ddTooltip.style.display = 'none';
          this.renderDrawdownChart();
        });

        ddCanvas.addEventListener('click', e => {
          const rect = ddCanvas.getBoundingClientRect();
          const x = e.clientX - rect.left;
          const y = e.clientY - rect.top;
          for (const hit of this._ddMap.legendHits) {
            if (x >= hit.x && x <= hit.x + hit.w && y >= hit.y && y <= hit.y + hit.h) {
              const peers = this._getComparisonTickers();
              const visCount = peers.reduce((n, k) => n + (this.state.ddVisible[k] !== false ? 1 : 0), 0);
              if (this.state.ddVisible[hit.ticker] !== false && visCount > 1) {
                this.state.ddVisible[hit.ticker] = false;
              } else {
                this.state.ddVisible[hit.ticker] = true;
              }
              this._ddViewCache.key = null;
              this._ddBaseKey = null;
              this._syncDDRangeDefaults();
              this.renderDDChips();
              this.renderDrawdownKPIs();
              this.renderDrawdownChart();
              return;
            }
          }
        });
      }

      this._bound = true;
    }
  };

  return TroughPeak;
});
