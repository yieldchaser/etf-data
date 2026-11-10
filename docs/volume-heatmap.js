/**
 * Volume Heatmap — MicroCVI Stage 1 Microstructure & Stage 2 Viewport Spotlight
 * Dual-gated 18-column OHLCV climax/drought calendar engine for etf-data.
 * Zero external dependencies. Zero hardcoded ticker names. Factual quantitative terminology.
 */

(function (root, factory) {
  if (typeof module === 'object' && module.exports) {
    let qc = null;
    try { qc = require('./quant-core.js'); } catch (_) {}
    module.exports = factory(qc);
  } else {
    root.VolumeHeatmap = factory(root.QuantCore);
  }
})(typeof self !== 'undefined' ? self : this, function (QuantCore) {
  'use strict';

  const EPS = 1e-9;
  const WINDOW_LAG = 21;
  const KAPPA_PEAK = 1.35;
  const KAPPA_VALLEY = 1.60;
  const LAMBDA = 0.75;
  const HEATMAP_MAX_BARS = 756;

  const STRUCTURAL_FLOORS = {
    PEAKS: { P99: 68.0, P95: 52.0, P90: 38.0 },
    VALLEYS: { P99: 64.0, P95: 48.0, P90: 34.0 }
  };

  const BASELINE_COLORS = [
    '#14141e', '#1a1a28', '#202838', '#283848',
    '#385060', '#4a6878', '#d4a830',
    '#c07828', '#c04040', '#a03838', '#8855bb'
  ];

  const HORIZONS = [90, 126, 180, 252];

  // ── Date & Session Helpers ──

  function getEasternDateParts(nowDate = new Date()) {
    try {
      const fmt = new Intl.DateTimeFormat('en-US', {
        timeZone: 'America/New_York',
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        weekday: 'short',
        hour: '2-digit',
        minute: '2-digit',
        hour12: false,
        hourCycle: 'h23'
      });
      const parts = Object.fromEntries(fmt.formatToParts(nowDate).map(p => [p.type, p.value]));
      const dateStr = `${parts.year}-${parts.month}-${parts.day}`;
      const hour = parseInt(parts.hour, 10) % 24;
      const minute = parseInt(parts.minute, 10);
      const mins = hour * 60 + minute;
      const isWeekday = parts.weekday !== 'Sat' && parts.weekday !== 'Sun';
      // US regular + pre-market window: 04:00 to 16:05 ET
      const isMarketOpen = isWeekday && mins >= 240 && mins <= 965;
      return { dateStr, isMarketOpen };
    } catch (_) {
      return {
        dateStr: nowDate.toISOString().slice(0, 10),
        isMarketOpen: false
      };
    }
  }

  function stripLivePartialSession(bars, isMarketOpenOverride, nowDate = new Date()) {
    if (!Array.isArray(bars) || !bars.length) return [];
    const et = getEasternDateParts(nowDate);
    const isMarketOpen = isMarketOpenOverride !== undefined ? Boolean(isMarketOpenOverride) : et.isMarketOpen;
    if (!isMarketOpen) return bars;
    const last = bars[bars.length - 1];
    const lastDate = last ? (last.d || last.date) : null;
    const utcToday = nowDate.toISOString().slice(0, 10);
    if (lastDate && (lastDate === et.dateStr || lastDate === utcToday)) {
      return bars.slice(0, -1);
    }
    return bars;
  }

  function formatDisplayDate(iso) {
    if (!iso || typeof iso !== 'string') return iso || '—';
    const parts = iso.split('-');
    if (parts.length < 3) return iso;
    const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    const mIdx = parseInt(parts[1], 10) - 1;
    const dNum = parseInt(parts[2].slice(0, 2), 10);
    if (isNaN(mIdx) || isNaN(dNum) || mIdx < 0 || mIdx > 11) return iso;
    return `${months[mIdx]} ${dNum}, ${parts[0]}`;
  }

  // ── Stage 1: Trimmed Baseline & Single-ETF Microstructure ──

  function computeTrimmedMean21(buf) {
    if (!buf || buf.length < 21) {
      const arr = Array.from(buf || []);
      if (!arr.length) return 0.0;
      return arr.reduce((a, b) => a + b, 0) / arr.length;
    }
    const sorted = Array.from(buf).sort((a, b) => a - b);
    let sum = 0.0;
    for (let i = 3; i < 18; i++) {
      sum += sorted[i];
    }
    return sum / 15.0;
  }

  function _normalizeMode(mode) {
    return (mode === 'dollar' || mode === 'dollars') ? 'dollar' : 'share';
  }

  function _barDate(b) {
    return b.d || b.date || '';
  }

  function _barOpen(b, c) {
    const o = b.o != null ? Number(b.o) : (b.open != null ? Number(b.open) : NaN);
    return (Number.isFinite(o) && o > 0) ? o : c;
  }

  function _barHigh(b, c) {
    const h = b.h != null ? Number(b.h) : (b.high != null ? Number(b.high) : NaN);
    return (Number.isFinite(h) && h > 0) ? h : c;
  }

  function _barLow(b, c) {
    const l = b.l != null ? Number(b.l) : (b.low != null ? Number(b.low) : NaN);
    return (Number.isFinite(l) && l > 0) ? l : c;
  }

  function _barClose(b) {
    const c = b.c != null ? Number(b.c) : (b.close != null ? Number(b.close) : NaN);
    return (Number.isFinite(c) && c > 0) ? c : 0;
  }

  function _barVol(b) {
    const v = b.v != null ? Number(b.v) : (b.volume != null ? Number(b.volume) : 0);
    return Number.isFinite(v) ? Math.max(0, v) : 0;
  }

  function computeEtfMicrostructure(bars, mode = 'share') {
    const normMode = _normalizeMode(mode);
    const T = Array.isArray(bars) ? bars.length : 0;
    const dates = new Array(T);
    const closes = new Float64Array(T);
    const turnover = new Float64Array(T);
    const dollarTurnover = new Float64Array(T);
    const trueRange = new Float64Array(T);
    const typPrice = new Float64Array(T);
    const abdiSpread = new Float64Array(T);

    for (let t = 0; t < T; t++) {
      const b = bars[t];
      dates[t] = _barDate(b);
      const c = _barClose(b);
      closes[t] = c;
      const h = _barHigh(b, c);
      const l = _barLow(b, c);
      const v = _barVol(b);
      const tp = (h + l + 2.0 * c) * 0.25;
      typPrice[t] = tp;
      dollarTurnover[t] = v * tp;
      turnover[t] = normMode === 'dollar' ? dollarTurnover[t] : v;

      const prevClose = t > 0 ? _barClose(bars[t - 1]) : _barOpen(b, c);
      const trHigh = Math.max(h, prevClose);
      const trLow = Math.min(l, prevClose);
      trueRange[t] = Math.max(0.0, trHigh - trLow);

      if (t > 0) {
        const prevB = bars[t - 1];
        const prevC = _barClose(prevB);
        const prevH = _barHigh(prevB, prevC);
        const prevL = _barLow(prevB, prevC);
        if (prevC > 0 && prevH > 0 && prevL > 0 && c > 0 && h > 0 && l > 0) {
          const cPrev = Math.log(prevC);
          const etaPrev = 0.5 * (Math.log(prevH) + Math.log(prevL));
          const etaCurr = 0.5 * (Math.log(h) + Math.log(l));
          const s2 = 4.0 * (cPrev - etaPrev) * (cPrev - etaCurr);
          abdiSpread[t] = s2 > 0.0 ? Math.sqrt(s2) : 0.0;
        } else {
          abdiSpread[t] = 0.0;
        }
      }
    }

    const peakScores = new Float64Array(T);
    const valleyScores = new Float64Array(T);
    const rvolArr = new Float64Array(T);
    const trimDollarTurnover = new Float64Array(T);
    const climaxDirs = new Int8Array(T);

    const scratchTurnover = new Float64Array(WINDOW_LAG);
    const scratchDollar = new Float64Array(WINDOW_LAG);
    const scratchTR = new Float64Array(WINDOW_LAG);
    const scratchSpread = new Float64Array(WINDOW_LAG);

    for (let t = 0; t < T; t++) {
      if (t < WINDOW_LAG) {
        let sumDollar = 0.0;
        for (let i = 0; i <= t; i++) {
          sumDollar += dollarTurnover[i];
        }
        trimDollarTurnover[t] = sumDollar / (t + 1);
        rvolArr[t] = 1.0;
        climaxDirs[t] = 1;
        continue;
      }

      for (let i = 0; i < WINDOW_LAG; i++) {
        const idx = t - WINDOW_LAG + i;
        scratchTurnover[i] = turnover[idx];
        scratchDollar[i] = dollarTurnover[idx];
        scratchTR[i] = trueRange[idx];
        scratchSpread[i] = abdiSpread[idx];
      }

      const trimMeanD = computeTrimmedMean21(scratchTurnover);
      const trimMeanDollar = computeTrimmedMean21(scratchDollar);
      const trimMeanTR = computeTrimmedMean21(scratchTR);
      const trimMeanSpread = computeTrimmedMean21(scratchSpread);

      trimDollarTurnover[t] = trimMeanDollar;
      const rvol = turnover[t] / (trimMeanD + EPS);
      rvolArr[t] = rvol;

      let maxH21 = -Infinity;
      let minL21 = Infinity;
      for (let j = t - WINDOW_LAG; j < t; j++) {
        const jc = _barClose(bars[j]);
        const jh = _barHigh(bars[j], jc);
        const jl = _barLow(bars[j], jc);
        if (jh > maxH21) maxH21 = jh;
        if (jl < minL21) minL21 = jl;
      }

      const curC = _barClose(bars[t]);
      const curH = _barHigh(bars[t], curC);
      const curL = _barLow(bars[t], curC);
      const rangeSpan = maxH21 - minL21 + EPS;

      // 1. Peak (Volume Climax & Exhaustion) Intensity - Symmetric Bidirectional
      const volElasticity = Math.pow(1.0 + rvol, 0.55) - 1.0;
      let climaxDir = 1;
      let displacement = 0.0;
      let isNewExtreme = 0.0;
      let wickRatio = 0.0;
      let rawShock = 0.0;

      const mid21 = 0.5 * (maxH21 + minL21);
      if (curC >= mid21) {
        climaxDir = 1;
        displacement = (curC - minL21) / rangeSpan;
        isNewExtreme = curH >= maxH21 - EPS ? 1.0 : 0.0;
        wickRatio = (curH - curC) / (trueRange[t] + EPS);
        rawShock = Math.max(0.0, curH - typPrice[t - 1]) / (trimMeanTR + EPS);
      } else {
        climaxDir = -1;
        displacement = (maxH21 - curC) / rangeSpan;
        isNewExtreme = curL <= minL21 + EPS ? 1.0 : 0.0;
        wickRatio = (curC - curL) / (trueRange[t] + EPS);
        rawShock = Math.max(0.0, typPrice[t - 1] - curL) / (trimMeanTR + EPS);
      }

      const shockTerm = Math.tanh(0.35 * rawShock);
      const absorptionMult = 1.0 + 0.20 * Math.max(0, Math.min(1, wickRatio)) * isNewExtreme;
      const boundedDisplacement = Math.max(0.0, Math.min(1.25, displacement));
      const priceFactor = (0.70 * Math.pow(boundedDisplacement, 1.25) + 0.30 * shockTerm) * absorptionMult;
      const peakIntensity = volElasticity * priceFactor;
      peakScores[t] = (100.0 * peakIntensity) / Math.sqrt(peakIntensity * peakIntensity + KAPPA_PEAK * KAPPA_PEAK);
      climaxDirs[t] = climaxDir;

      // 2. Valley (Volume Drought & Low-Turnover Compression) Intensity
      const rDrought = trimMeanD / (turnover[t] + EPS);
      const droughtBase = Math.max(0.0, Math.pow(rDrought, 0.60) - 1.00);
      const rangeComp = Math.exp(-trueRange[t] / (2.0 * trimMeanTR + EPS));
      const rawSpreadRatio = abdiSpread[t] / (trimMeanSpread + EPS);
      const spreadMult = Math.min(2.0, Math.max(0.75, rawSpreadRatio));

      const valleyIntensity = droughtBase * rangeComp * spreadMult;
      valleyScores[t] = (100.0 * valleyIntensity) / Math.sqrt(valleyIntensity * valleyIntensity + KAPPA_VALLEY * KAPPA_VALLEY);
    }

    return {
      dates,
      closes,
      peakScores,
      valleyScores,
      rvolArr,
      trimDollarTurnover,
      climaxDirs
    };
  }

  // ── Catalog & Dynamic Scope Resolution (Zero Hardcoded Tickers) ──

  function isInverseInstrument(inst) {
    if (!inst) return false;
    return (inst.leverage_value != null && Number(inst.leverage_value) < 0) || inst.direction === 'inverse';
  }

  function normalizeUnderlyingKey(raw) {
    if (!raw) return '';
    return String(raw)
      .replace(/\s*\([^)]*\)\s*$/g, '')
      .replace(/\s+(Technologies|Global|Index)$/i, '')
      .trim()
      .toLowerCase();
  }

  function _cleanUnderlyingLabel(raw) {
    if (!raw) return 'Underlying';
    return String(raw)
      .replace(/\s*\([^)]*\)\s*$/g, '')
      .replace(/\s+(Technologies|Global|Index)$/i, '')
      .trim() || String(raw);
  }

  function _buildCatalogMap(catalogInstruments) {
    const map = {};
    if (Array.isArray(catalogInstruments)) {
      for (const item of catalogInstruments) {
        if (item && item.ticker) map[item.ticker] = item;
      }
    } else if (catalogInstruments && typeof catalogInstruments === 'object') {
      return catalogInstruments;
    }
    return map;
  }

  function getAvailableScopes(activeTicker, catalogInstruments) {
    const list = Array.isArray(catalogInstruments) ? catalogInstruments : Object.values(catalogInstruments || {});
    const map = _buildCatalogMap(list);
    const active = map[activeTicker] || null;
    const activeUndKey = active ? normalizeUnderlyingKey(active.underlying) : '';
    const underlyingLabel = active ? _cleanUnderlyingLabel(active.underlying) : 'Underlying';
    const category = active ? active.category : null;

    const underlyingPeers = activeUndKey
      ? list.filter(x => normalizeUnderlyingKey(x.underlying) === activeUndKey)
      : (active ? [active] : []);
    const categoryPeers = category ? list.filter(x => x.category === category) : (active ? [active] : []);
    const featuredPeers = list.filter(x => x.featured);

    const countSides = arr => {
      let longCount = 0;
      let shortCount = 0;
      for (const x of arr) {
        if (isInverseInstrument(x)) shortCount++;
        else longCount++;
      }
      return { total: arr.length, longCount, shortCount };
    };

    return {
      active: { count: active ? 1 : 0, hasShort: isInverseInstrument(active), hasLong: !isInverseInstrument(active) },
      underlying: { label: underlyingLabel, ...countSides(underlyingPeers) },
      category: { label: category || 'Category', ...countSides(categoryPeers) },
      featured: countSides(featuredPeers),
      universe: countSides(list)
    };
  }

  function resolveScopeTickers(activeTicker, catalogInstruments, scope = 'active', side = 'all') {
    const list = Array.isArray(catalogInstruments) ? catalogInstruments : Object.values(catalogInstruments || {});
    const map = _buildCatalogMap(list);
    const active = map[activeTicker] || null;
    const activeUndKey = active ? normalizeUnderlyingKey(active.underlying) : '';

    let base = [];
    if (scope === 'underlying' && active && activeUndKey) {
      base = list.filter(x => normalizeUnderlyingKey(x.underlying) === activeUndKey);
    } else if (scope === 'category' && active && active.category) {
      base = list.filter(x => x.category === active.category);
    } else if (scope === 'featured') {
      base = list.filter(x => x.featured);
    } else if (scope === 'universe') {
      base = list;
    } else {
      base = active ? [active] : (activeTicker ? [{ ticker: activeTicker }] : []);
    }

    if (scope !== 'active') {
      if (side === 'long' || side === 'short') {
        base = base.filter(x => (side === 'short' ? isInverseInstrument(x) : !isInverseInstrument(x)));
      } else if (side && side !== 'all') {
        const match = base.find(x => x.ticker === side);
        if (match) return [match.ticker];
        if (map[side]) return [side];
      }
    }

    return base.map(x => x.ticker).filter(Boolean);
  }

  // ── Cross-Asset Liquidity-Weighted Composite Aggregation ──

  function computeHeatAggregate(historiesByTicker, mode = 'share', side = 'all', catalogInstruments = null, opts = {}) {
    const normMode = _normalizeMode(mode);
    const catalogMap = _buildCatalogMap(catalogInstruments);
    const allTickers = Object.keys(historiesByTicker || {}).filter(
      t => Array.isArray(historiesByTicker[t]) && historiesByTicker[t].length > 0
    );

    if (!allTickers.length) {
      return {
        p90: STRUCTURAL_FLOORS.PEAKS.P90,
        p95: STRUCTURAL_FLOORS.PEAKS.P95,
        p99: STRUCTURAL_FLOORS.PEAKS.P99,
        peaks: STRUCTURAL_FLOORS.PEAKS,
        valleys: STRUCTURAL_FLOORS.VALLEYS,
        series: [],
        rawDateCount: 0
      };
    }

    let activeTickers = allTickers;
    if (side === 'long') {
      activeTickers = allTickers.filter(t => !isInverseInstrument(catalogMap[t]));
    } else if (side === 'short') {
      activeTickers = allTickers.filter(t => isInverseInstrument(catalogMap[t]));
    } else if (allTickers.includes(side)) {
      activeTickers = [side];
    }

    if (!activeTickers.length) {
      return {
        p90: STRUCTURAL_FLOORS.PEAKS.P90,
        p95: STRUCTURAL_FLOORS.PEAKS.P95,
        p99: STRUCTURAL_FLOORS.PEAKS.P99,
        peaks: STRUCTURAL_FLOORS.PEAKS,
        valleys: STRUCTURAL_FLOORS.VALLEYS,
        series: [],
        rawDateCount: 0
      };
    }

    const etfMetrics = {};
    const indexMaps = {};
    const unionDateSet = new Set();
    const maxBars = opts.maxBars || HEATMAP_MAX_BARS;

    for (const t of activeTickers) {
      const stripped = stripLivePartialSession(historiesByTicker[t], opts.isMarketOpen, opts.nowDate);
      const hist = stripped.length > maxBars ? stripped.slice(-maxBars) : stripped;
      if (!hist.length) continue;

      etfMetrics[t] = computeEtfMicrostructure(hist, normMode);
      const map = new Map();
      for (let i = 0; i < hist.length; i++) {
        const d = _barDate(hist[i]);
        if (d) {
          map.set(d, i);
          unionDateSet.add(d);
        }
      }
      indexMaps[t] = map;
    }

    const sortedUnion = Array.from(unionDateSet).sort();
    const allDates = sortedUnion.length > maxBars ? sortedUnion.slice(-maxBars) : sortedUnion;
    const T = allDates.length;
    const records = [];

    for (let t = 0; t < T; t++) {
      const date = allDates[t];
      let maxPeak = 0.0;
      let maxValley = 0.0;
      let maxRvol = 0.0;
      let minRvol = Infinity;
      let weightedPeakSum = 0.0;
      let weightedValleySum = 0.0;
      let weightSum = 0.0;
      let activeCount = 0;
      let leadPeakTicker = '';
      let leadValleyTicker = '';

      for (const ticker of activeTickers) {
        const map = indexMaps[ticker];
        if (!map) continue;
        const idx = map.get(date);
        if (idx === undefined || idx < WINDOW_LAG) continue;

        const m = etfMetrics[ticker];
        if (!m) continue;
        const pScore = m.peakScores[idx];
        const vScore = m.valleyScores[idx];
        const rv = m.rvolArr[idx];
        const w = Math.sqrt(Math.max(0.0, m.trimDollarTurnover[idx]));

        if (pScore > maxPeak || !leadPeakTicker) {
          maxPeak = pScore;
          leadPeakTicker = ticker;
        }
        if (vScore > maxValley || !leadValleyTicker) {
          maxValley = vScore;
          leadValleyTicker = ticker;
        }
        if (rv > maxRvol) maxRvol = rv;
        if (rv < minRvol) minRvol = rv;

        weightedPeakSum += w * pScore;
        weightedValleySum += w * vScore;
        weightSum += w;
        activeCount++;
      }

      if (activeCount === 0) continue;

      const meanPeak = weightSum > 0.0 ? (weightedPeakSum / weightSum) : maxPeak;
      const meanValley = weightSum > 0.0 ? (weightedValleySum / weightSum) : maxValley;

      const compPeak = LAMBDA * maxPeak + (1.0 - LAMBDA) * meanPeak;
      const compValley = LAMBDA * maxValley + (1.0 - LAMBDA) * meanValley;

      const safeMinRvol = isFinite(minRvol) ? minRvol : maxRvol;
      const tieKeyPeak = compPeak + 1e-7 * maxRvol + 1e-11 * t;
      const tieKeyValley = compValley + 1e-7 * (1.0 / (safeMinRvol + EPS)) + 1e-11 * t;

      const leadM = leadPeakTicker ? etfMetrics[leadPeakTicker] : null;
      const leadMap = leadPeakTicker ? indexMaps[leadPeakTicker] : null;
      const leadIdx = leadMap ? leadMap.get(date) : undefined;
      const climaxDir = (leadM && leadM.climaxDirs && leadIdx !== undefined) ? leadM.climaxDirs[leadIdx] : 1;

      let priceTicker = allTickers.includes(side)
        ? side
        : ((opts.focusTicker && activeTickers.includes(opts.focusTicker)) ? opts.focusTicker : leadPeakTicker);
      let priceM = priceTicker ? etfMetrics[priceTicker] : null;
      let priceMap = priceTicker ? indexMaps[priceTicker] : null;
      let priceIdx = priceMap ? priceMap.get(date) : undefined;
      if (priceIdx === undefined || priceIdx < 0 || !priceM || !priceM.closes || !(priceM.closes[priceIdx] > 0)) {
        const candidates = [leadPeakTicker, leadValleyTicker, ...activeTickers];
        for (const cand of candidates) {
          if (!cand) continue;
          const cm = etfMetrics[cand];
          const cmap = indexMaps[cand];
          const cidx = cmap ? cmap.get(date) : undefined;
          if (cidx !== undefined && cidx >= 0 && cm && cm.closes && cm.closes[cidx] > 0) {
            priceTicker = cand;
            priceM = cm;
            priceIdx = cidx;
            break;
          }
        }
      }
      const focusClose = (priceM && priceM.closes && priceIdx !== undefined && priceM.closes[priceIdx] > 0)
        ? priceM.closes[priceIdx]
        : null;

      records.push({
        index: t,
        date,
        focusClose,
        priceTicker: priceTicker || '',
        peakScore: compPeak,
        valleyScore: compValley,
        maxRvol,
        minRvol: safeMinRvol,
        activeCount,
        leadPeakTicker,
        leadValleyTicker,
        climaxDir,
        compositeTieKeyPeak: tieKeyPeak,
        compositeTieKeyValley: tieKeyValley
      });
    }

    return {
      p90: STRUCTURAL_FLOORS.PEAKS.P90,
      p95: STRUCTURAL_FLOORS.PEAKS.P95,
      p99: STRUCTURAL_FLOORS.PEAKS.P99,
      peaks: STRUCTURAL_FLOORS.PEAKS,
      valleys: STRUCTURAL_FLOORS.VALLEYS,
      series: records,
      rawDateCount: T
    };
  }

  // ── Stage 2: Adaptive Viewport Spotlight ──

  function evaluateViewportSpotlight(visibleSlice, mode = 'PEAKS', activeFilter = 'NONE') {
    const N = Array.isArray(visibleSlice) ? visibleSlice.length : 0;
    if (N === 0) return [];

    const k99 = Math.max(1, Math.round(0.01 * N));
    const k95 = Math.max(k99 + 1, Math.round(0.05 * N));
    const k90 = Math.max(k95 + 1, Math.round(0.10 * N));

    const sortedIndices = Array.from({ length: N }, (_, i) => i);
    sortedIndices.sort((a, b) => {
      const keyA = mode === 'PEAKS' ? visibleSlice[a].compositeTieKeyPeak : visibleSlice[a].compositeTieKeyValley;
      const keyB = mode === 'PEAKS' ? visibleSlice[b].compositeTieKeyPeak : visibleSlice[b].compositeTieKeyValley;
      return keyB - keyA;
    });

    const ranks = new Int32Array(N);
    const baselineBins = new Uint8Array(N);
    for (let rankZero = 0; rankZero < N; rankZero++) {
      const origIdx = sortedIndices[rankZero];
      ranks[origIdx] = rankZero + 1;
      baselineBins[origIdx] = Math.min(4, Math.floor(((N - 1 - rankZero) / N) * 5));
    }

    const floors = STRUCTURAL_FLOORS[mode] || STRUCTURAL_FLOORS.PEAKS;
    const output = new Array(N);

    for (let i = 0; i < N; i++) {
      const rec = visibleSlice[i];
      const score = mode === 'PEAKS' ? rec.peakScore : rec.valleyScore;
      const r = ranks[i];

      let visualTier = 'NONE';
      let isStructuralClimax = false;

      if (r <= k99) {
        visualTier = 'RED_P99';
        isStructuralClimax = score >= floors.P99;
      } else if (r <= k95) {
        visualTier = 'ORANGE_P95';
        isStructuralClimax = score >= floors.P95;
      } else if (r <= k90) {
        visualTier = 'AMBER_P90';
        isStructuralClimax = score >= floors.P90;
      }

      let isHighlighted = true;
      if (activeFilter === 'P99') {
        isHighlighted = r <= k99;
      } else if (activeFilter === 'P95') {
        isHighlighted = r <= k95;
      } else if (activeFilter === 'P90') {
        isHighlighted = r <= k90;
      }

      output[i] = {
        date: rec.date,
        score,
        maxRvol: rec.maxRvol,
        windowRank: r,
        visualTier,
        isHighlighted,
        isStructuralClimax,
        climaxDir: rec.climaxDir || 1,
        baselineQuantileBin: baselineBins[i]
      };
    }

    return output;
  }

  // ── DOM Controller & 18-Column Grid Renderer ──

  const VolumeHeatmap = {
    EPS,
    WINDOW_LAG,
    KAPPA_PEAK,
    KAPPA_VALLEY,
    LAMBDA,
    HEATMAP_MAX_BARS,
    STRUCTURAL_FLOORS,
    BASELINE_COLORS,
    HORIZONS,

    computeTrimmedMean21,
    computeEtfMicrostructure,
    computeHeatAggregate,
    evaluateViewportSpotlight,
    stripLivePartialSession,
    resolveScopeTickers,
    getAvailableScopes,
    isInverseInstrument,
    normalizeUnderlyingKey,
    formatDisplayDate,

    state: {
      ticker: '',
      scope: 'active',      // 'active' | 'underlying' | 'category' | 'featured' | 'universe'
      side: 'all',          // 'all' | 'long' | 'short' | individual ticker
      unit: 'shares',       // 'shares' | 'dollar'
      days: 90,             // 90 | 126 | 180 | 252
      offset: 0,
      spotlight: false,     // false | 'peaks' | 'valleys'
      threshold: 95         // 90 | 95 | 99
    },

    _allHistory: null,
    _catalog: [],
    _catalogMap: {},
    _aggCache: new Map(),
    _userPinnedMacroScope: false,
    _bound: false,

    _invalidateCacheIfHistoryChanged(allHistory) {
      if (this._allHistory !== allHistory) {
        this._allHistory = allHistory;
        this._aggCache.clear();
      }
    },

    setCatalog(catalogInstruments) {
      this._catalog = Array.isArray(catalogInstruments) ? catalogInstruments : [];
      this._catalogMap = _buildCatalogMap(this._catalog);
    },

    _getAggregatePayload() {
      const { ticker, scope, side, unit } = this.state;
      const allHistory = this._allHistory || {};
      const activeInst = this._catalogMap[ticker] || null;

      let scopeId = ticker;
      if (scope === 'underlying' && activeInst?.underlying) scopeId = normalizeUnderlyingKey(activeInst.underlying) || activeInst.underlying;
      else if (scope === 'category' && activeInst?.category) scopeId = activeInst.category;
      else if (scope === 'featured') scopeId = 'featured';
      else if (scope === 'universe') scopeId = 'universe';

      const normSide = scope === 'active' ? 'all' : side;
      const cacheKey = `${scope}|${scopeId}|${unit}|${normSide}|${ticker}`;
      if (this._aggCache.has(cacheKey)) {
        return this._aggCache.get(cacheKey);
      }

      const targetTickers = resolveScopeTickers(ticker, this._catalog, scope, normSide);
      const subset = {};
      for (const tk of targetTickers) {
        if (Array.isArray(allHistory[tk]) && allHistory[tk].length) {
          subset[tk] = allHistory[tk];
        }
      }

      const payload = computeHeatAggregate(subset, unit === 'dollar' ? 'dollar' : 'share', normSide, this._catalogMap, { focusTicker: ticker });
      this._aggCache.set(cacheKey, payload);
      return payload;
    },

    mount({ ticker, allHistory, catalogInstruments, unit } = {}) {
      if (catalogInstruments) this.setCatalog(catalogInstruments);
      if (allHistory) this._invalidateCacheIfHistoryChanged(allHistory);
      if (ticker) {
        const tickerChanged = this.state.ticker !== ticker;
        this.state.ticker = ticker;
        if (tickerChanged) {
          this.state.offset = 0;
          const scopes = getAvailableScopes(ticker, this._catalog);
          if (!this._userPinnedMacroScope) {
            if (scopes.underlying.total >= 2) {
              this.state.scope = 'underlying';
              this.state.side = ticker;
            } else {
              this.state.scope = 'active';
              this.state.side = 'all';
            }
          } else {
            // Preserve macro scope ('category'|'featured'|'universe'), keep directional side valid
            if (!['all', 'long', 'short'].includes(this.state.side)) {
              const scopeTickers = resolveScopeTickers(ticker, this._catalog, this.state.scope, 'all');
              this.state.side = scopeTickers.includes(ticker) ? ticker : 'all';
            } else if (this.state.side === 'short' && (scopes[this.state.scope]?.shortCount || 0) === 0) {
              this.state.side = 'all';
            } else if (this.state.side === 'long' && (scopes[this.state.scope]?.longCount || 0) === 0) {
              this.state.side = 'all';
            }
          }
        }
      }
      if (unit === 'shares' || unit === 'dollar') {
        this.state.unit = unit;
      }
      this._bindDOMEvents();
      this.render();
    },

    setScope(scopeKey) {
      const valid = ['active', 'underlying', 'category', 'featured', 'universe'];
      if (!valid.includes(scopeKey)) return;
      const scopes = getAvailableScopes(this.state.ticker, this._catalog);
      if (scopeKey === 'underlying' && scopes.underlying.total < 2) return;
      this.state.scope = scopeKey;
      this._userPinnedMacroScope = ['category', 'featured', 'universe'].includes(scopeKey);
      this.state.offset = 0;

      if (scopeKey === 'active') {
        this.state.side = 'all';
      } else {
        const scopeTickers = resolveScopeTickers(this.state.ticker, this._catalog, scopeKey, 'all');
        const activeScopeMeta = scopes[scopeKey] || scopes.active;
        if (this.state.side === 'short' && activeScopeMeta.shortCount === 0) {
          this.state.side = 'all';
        } else if (this.state.side === 'long' && activeScopeMeta.longCount === 0) {
          this.state.side = 'all';
        } else if (!['all', 'long', 'short'].includes(this.state.side) && !scopeTickers.includes(this.state.side)) {
          this.state.side = scopeTickers.includes(this.state.ticker) ? this.state.ticker : 'all';
        }
      }
      this.render();
    },

    setSide(sideKey) {
      if (!sideKey) return;
      const key = String(sideKey);
      if (!['all', 'long', 'short'].includes(key)) {
        const known = Boolean(this._catalogMap[key] || (this._allHistory && this._allHistory[key]));
        if (!known) return;
      }
      this.state.side = key;
      this.state.offset = 0;
      this.render();
    },

    setDays(daysVal) {
      const d = parseInt(daysVal, 10);
      if (!HORIZONS.includes(d)) return;
      this.state.days = d;
      this.state.offset = 0;
      this.render();
    },

    setUnit(unitKey) {
      if (unitKey !== 'shares' && unitKey !== 'dollar') return;
      this.state.unit = unitKey;
      this.render();
    },

    prevPage() {
      const payload = this._getAggregatePayload();
      const series = payload.series || [];
      const maxOffset = Math.max(0, series.length - this.state.days);
      this.state.offset = Math.min(maxOffset, this.state.offset + this.state.days);
      this.render();
    },

    nextPage() {
      this.state.offset = Math.max(0, this.state.offset - this.state.days);
      this.render();
    },

    toggleSpotlight(mode) {
      if (mode !== 'peaks' && mode !== 'valleys') return;
      this.state.spotlight = (this.state.spotlight === mode) ? false : mode;
      this.render();
    },

    setThreshold(pct) {
      const p = parseInt(pct, 10);
      if (![90, 95, 99].includes(p)) return;
      this.state.threshold = p;
      if (!this.state.spotlight) this.state.spotlight = 'peaks';
      this.render();
    },

    render() {
      if (typeof document === 'undefined') return;
      const grid = document.getElementById('vs-heatmap-grid');
      if (!grid) return;

      const scopes = getAvailableScopes(this.state.ticker, this._catalog);
      const payload = this._getAggregatePayload();
      const series = payload.series || [];

      // Short-history guard: disable horizons > available series.length
      const availHorizons = HORIZONS.filter(h => series.length >= h);
      if (availHorizons.length && !availHorizons.includes(this.state.days)) {
        this.state.days = availHorizons[availHorizons.length - 1];
      }

      const daySpan = series.length < HORIZONS[0] ? series.length : this.state.days;
      const maxOffset = Math.max(0, series.length - daySpan);
      if (this.state.offset > maxOffset) this.state.offset = maxOffset;

      const endIdx = series.length - this.state.offset;
      const startIdx = Math.max(0, endIdx - daySpan);
      const visibleSlice = series.slice(startIdx, endIdx);

      // Update scope, side, horizon, and spotlight controls UI
      this._syncControlsUI(scopes, series.length, startIdx, endIdx, visibleSlice);

      const count = this.renderCalendar(grid, visibleSlice);
      this._updateSummaryAndLegend(visibleSlice, series.length, count, scopes);
    },

    renderCalendar(container, visibleSlice) {
      if (!container) return 0;
      container.innerHTML = '';

      const spotlight = this.state.spotlight;
      const isPeaks = spotlight === 'peaks';
      const isValleys = spotlight === 'valleys';
      const isSpotlight = isPeaks || isValleys;

      container.classList.toggle('vs-heat-peaks-mode', isPeaks);
      container.classList.toggle('vs-heat-valleys-mode', isValleys);

      if (!visibleSlice || !visibleSlice.length) {
        const empty = document.createElement('div');
        empty.className = 'vs-heat-empty';
        empty.textContent = 'Insufficient confirmed sessions (requires > 21 trading bars).';
        container.appendChild(empty);
        return 0;
      }

      const label = this.state.unit === 'dollar' ? 'DVCVI' : 'CVI';
      const N = visibleSlice.length;
      const regimeMode = isValleys ? 'VALLEYS' : 'PEAKS';
      const activeFilter = isSpotlight ? ('P' + this.state.threshold) : 'NONE';
      const cellStates = evaluateViewportSpotlight(visibleSlice, regimeMode, activeFilter);

      let highlightCount = 0;
      const frag = document.createDocumentFragment();

      for (let i = 0; i < N; i++) {
        const day = visibleSlice[i];
        const st = cellStates[i];
        const cell = document.createElement('div');
        cell.className = 'vs-heat-cell';

        const score = st.score;
        const colorIdx = Math.min(BASELINE_COLORS.length - 1, Math.max(0, Math.floor((score / 100) * BASELINE_COLORS.length)));
        cell.style.background = BASELINE_COLORS[colorIdx];

        const dateFmt = formatDisplayDate(day.date);
        const priceStr = (day.focusClose != null && day.focusClose > 0) ? `$${day.focusClose.toFixed(1)}` : '';
        const rvolVal = isValleys
          ? (day.minRvol != null ? day.minRvol : (day.maxRvol != null ? day.maxRvol : 1.0))
          : (day.maxRvol != null ? day.maxRvol : 1.0);
        const leadTk = isValleys ? day.leadValleyTicker : day.leadPeakTicker;
        const basketLead = (day.activeCount > 1 && leadTk) ? `Lead: ${leadTk} (${day.activeCount} ETFs)` : '';
        const rvolText = `RVOL ${rvolVal.toFixed(2)}x`;
        const rankText = `Rank #${st.windowRank} / ${N}`;

        cell.dataset.price = priceStr;
        cell.dataset.date = dateFmt;
        cell.dataset.rank = rankText;
        cell.dataset.rvol = rvolText;

        if (isSpotlight) {
          if (!st.isHighlighted) {
            cell.classList.add('vs-heat-dim');
          } else {
            highlightCount++;
            if (isPeaks) {
              if (st.visualTier === 'RED_P99') cell.classList.add('vs-heat-peak-extreme');
              else if (st.visualTier === 'ORANGE_P95') cell.classList.add('vs-heat-peak-significant');
              else cell.classList.add('vs-heat-peak-notable');
              if (!st.isStructuralClimax) cell.classList.add('vs-heat-local-max');
            } else {
              if (st.visualTier === 'RED_P99') cell.classList.add('vs-heat-valley-extreme');
              else if (st.visualTier === 'ORANGE_P95') cell.classList.add('vs-heat-valley-significant');
              else cell.classList.add('vs-heat-valley-notable');
              if (!st.isStructuralClimax) cell.classList.add('vs-heat-local-min');
            }
          }

          const dirLabel = st.climaxDir === -1 ? 'SELLING EXHAUSTION' : 'BUYING CLIMAX';
          const climaxTag = st.isStructuralClimax
            ? (isPeaks ? ('STRUCTURAL ' + dirLabel) : 'STRUCTURAL DROUGHT')
            : (isPeaks ? ('LOCAL ' + dirLabel) : 'LOW-TURNOVER COMPRESSION');
          const filterTag = st.visualTier === 'RED_P99' ? 'P99' : (st.visualTier === 'ORANGE_P95' ? 'P95' : 'P90');
          const metricName = isValleys ? 'VOLUME DROUGHT' : label;

          cell.dataset.metric = metricName;
          cell.dataset.score = score.toFixed(1);
          cell.dataset.tag = `${filterTag} · ${climaxTag}${basketLead ? ' · ' + basketLead : ''}`;
          cell.dataset.tier = st.isHighlighted ? (st.isStructuralClimax ? 'structural' : 'local') : 'dim';
        } else {
          cell.dataset.metric = label;
          cell.dataset.score = score.toFixed(1);
          cell.dataset.tag = basketLead;
          cell.dataset.tier = 'baseline';
        }

        frag.appendChild(cell);
      }

      container.appendChild(frag);
      return highlightCount;
    },

    _syncControlsUI(scopes, totalLen, startIdx, endIdx, visibleSlice) {
      if (typeof document === 'undefined') return;

      // Scope buttons (with fully dynamic catalog counts)
      const scopeBtns = document.querySelectorAll('[data-heat-scope]');
      scopeBtns.forEach(btn => {
        const key = btn.getAttribute('data-heat-scope');
        btn.classList.toggle('active', key === this.state.scope);
        if (key === 'underlying') {
          const disabled = scopes.underlying.total < 2;
          btn.disabled = disabled;
          btn.textContent = scopes.underlying.total >= 2
            ? `Underlying (${scopes.underlying.total})`
            : 'Underlying Complex';
          btn.title = disabled
            ? 'No peer ETFs share this underlying instrument'
            : `Aggregate ${scopes.underlying.total} ETFs on ${scopes.underlying.label}`;
        } else if (key === 'category') {
          btn.textContent = `Category (${scopes.category.total})`;
          btn.title = `Aggregate ${scopes.category.total} ETFs in ${scopes.category.label}`;
        } else if (key === 'featured') {
          btn.textContent = `Featured (${scopes.featured.total})`;
          btn.title = `Liquidity-weighted aggregate of ${scopes.featured.total} featured ETFs`;
        } else if (key === 'universe') {
          btn.textContent = `All ${scopes.universe.total}`;
          btn.title = `Liquidity-weighted aggregate of all ${scopes.universe.total} ETFs`;
        }
      });

      // Dynamic [All] [Long] [Short] [ETF1] [ETF2]... bar
      const activeScopeMeta = scopes[this.state.scope] || scopes.active;
      const sideGroup = document.getElementById('vs-heat-side-group');
      if (sideGroup) {
        if (this.state.scope === 'active') {
          sideGroup.style.display = 'none';
        } else {
          sideGroup.style.display = 'flex';
          const allTickersInScope = resolveScopeTickers(this.state.ticker, this._catalog, this.state.scope, 'all');
          const sortedConstituents = allTickersInScope.slice().sort((a, b) => {
            if (a === this.state.ticker) return -1;
            if (b === this.state.ticker) return 1;
            const invA = isInverseInstrument(this._catalogMap[a]) ? 1 : 0;
            const invB = isInverseInstrument(this._catalogMap[b]) ? 1 : 0;
            if (invA !== invB) return invA - invB;
            return (this._catalogMap[b]?.aum_m || 0) - (this._catalogMap[a]?.aum_m || 0);
          });

          const maxPills = sortedConstituents.length <= 16 ? sortedConstituents.length : 8;
          const visibleTickers = sortedConstituents.slice(0, maxPills);
          const extraCount = sortedConstituents.length - visibleTickers.length;

          const dirDisabledLong = activeScopeMeta.longCount === 0 || activeScopeMeta.hasLong === false;
          const dirDisabledShort = activeScopeMeta.shortCount === 0 || activeScopeMeta.hasShort === false;

          let html = `
            <span class="flow-eyebrow vs-heat-row-lbl">Focus:</span>
            <button type="button" class="vs-heat-btn ${this.state.side === 'all' ? 'active' : ''}" data-heat-side="all" title="Aggregate all ${allTickersInScope.length} ETFs in basket">All (${allTickersInScope.length})</button>
            <button type="button" class="vs-heat-btn ${this.state.side === 'long' ? 'active' : ''}" data-heat-side="long" ${dirDisabledLong ? 'disabled' : ''} title="${dirDisabledLong ? 'No long ETFs in selected basket' : 'Filter to long ETFs'}">Long (${activeScopeMeta.longCount || 0})</button>
            <button type="button" class="vs-heat-btn ${this.state.side === 'short' ? 'active' : ''}" data-heat-side="short" ${dirDisabledShort ? 'disabled' : ''} title="${dirDisabledShort ? 'No inverse ETFs in selected basket' : 'Filter to inverse/short ETFs'}">Short (${activeScopeMeta.shortCount || 0})</button>
          `;

          for (const tk of visibleTickers) {
            const inst = this._catalogMap[tk] || {};
            const inv = isInverseInstrument(inst);
            const isSel = this.state.side === tk;
            const levTag = inst.leverage ? ` ${inst.leverage}` : '';
            html += `<button type="button" class="vs-heat-btn vs-heat-etf-pill ${inv ? 'is-inverse' : 'is-long'} ${isSel ? 'active' : ''}" data-heat-side="${tk}" title="Isolate ${tk}${inst.fund_name ? ' — ' + inst.fund_name : ''}">${tk}${levTag}</button>`;
          }
          if (extraCount > 0) {
            html += `<span class="vs-heat-more-badge" title="${sortedConstituents.length} total ETFs in this basket">+${extraCount} more</span>`;
          }
          sideGroup.innerHTML = html;
        }
      }

      // Horizon buttons with short-history guard
      document.querySelectorAll('[data-heat-days]').forEach(btn => {
        const d = parseInt(btn.getAttribute('data-heat-days'), 10);
        const disabled = totalLen < d;
        btn.disabled = disabled;
        btn.classList.toggle('flow-chip-active', d === this.state.days && !disabled);
        btn.title = disabled
          ? `Requires >= ${d} confirmed sessions (${totalLen} available after 21D warm-up)`
          : `Show ${d} trading sessions (${Math.ceil(d / 18)}x18 grid)`;
      });

      // Pagination buttons
      const prevBtn = document.getElementById('vs-heat-prev');
      const nextBtn = document.getElementById('vs-heat-next');
      const maxOffset = Math.max(0, totalLen - (totalLen < HORIZONS[0] ? totalLen : this.state.days));
      if (prevBtn) prevBtn.disabled = this.state.offset >= maxOffset || !visibleSlice.length;
      if (nextBtn) nextBtn.disabled = this.state.offset <= 0 || !visibleSlice.length;

      // Spotlight toggles
      const peaksBtn = document.getElementById('vs-heat-peaks-btn');
      const valleysBtn = document.getElementById('vs-heat-valleys-btn');
      const subGroup = document.getElementById('vs-heat-threshold-group');
      if (peaksBtn) peaksBtn.classList.toggle('active', this.state.spotlight === 'peaks');
      if (valleysBtn) valleysBtn.classList.toggle('active', this.state.spotlight === 'valleys');
      if (subGroup) subGroup.classList.toggle('visible', Boolean(this.state.spotlight));

      document.querySelectorAll('[data-heat-threshold]').forEach(btn => {
        const thr = parseInt(btn.getAttribute('data-heat-threshold'), 10);
        btn.classList.toggle('active', thr === this.state.threshold);
      });
    },

    _updateSummaryAndLegend(visibleSlice, totalLen, highlightCount, scopes) {
      if (typeof document === 'undefined') return;
      const scopeEl = document.getElementById('vs-heat-scope-label');
      const rangeEl = document.getElementById('vs-heat-range-label');
      const countEl = document.getElementById('vs-heat-peak-count');
      const legendEl = document.getElementById('vs-heat-legend');

      if (scopeEl) {
        const allInScope = resolveScopeTickers(this.state.ticker, this._catalog, this.state.scope, 'all');
        const tickersInScope = resolveScopeTickers(this.state.ticker, this._catalog, this.state.scope, this.state.side);
        const sideSuffix = !['all', 'long', 'short'].includes(this.state.side)
          ? ` · Focusing ${this.state.side} (1 of ${allInScope.length} ETFs)`
          : (this.state.side === 'long' ? ` · Long (${tickersInScope.length} ETFs)` : (this.state.side === 'short' ? ` · Short (${tickersInScope.length} ETFs)` : `: ${tickersInScope.length} ETFs`));
        if (this.state.scope === 'active') {
          scopeEl.textContent = `Single ETF: ${this.state.ticker}`;
        } else if (this.state.scope === 'underlying') {
          scopeEl.textContent = `Underlying Complex (${scopes.underlying.label})${sideSuffix}`;
        } else if (this.state.scope === 'category') {
          scopeEl.textContent = `Category Basket${sideSuffix}`;
        } else if (this.state.scope === 'featured') {
          scopeEl.textContent = `Featured Basket${sideSuffix}`;
        } else {
          scopeEl.textContent = `Full Universe${sideSuffix}`;
        }
      }

      if (rangeEl) {
        if (visibleSlice.length) {
          rangeEl.textContent = `${visibleSlice[0].date} -> ${visibleSlice[visibleSlice.length - 1].date} (${visibleSlice.length} of ${totalLen} sessions)`;
        } else {
          rangeEl.textContent = 'No sessions available';
        }
      }

      if (countEl) {
        if (this.state.spotlight === 'peaks') {
          countEl.style.display = 'inline-flex';
          countEl.className = 'vs-heat-peak-count peak-style';
          countEl.textContent = `${highlightCount} climax ${highlightCount === 1 ? 'session' : 'sessions'} (P${this.state.threshold})`;
        } else if (this.state.spotlight === 'valleys') {
          countEl.style.display = 'inline-flex';
          countEl.className = 'vs-heat-peak-count valley-style';
          countEl.textContent = `${highlightCount} drought ${highlightCount === 1 ? 'session' : 'sessions'} (P${this.state.threshold})`;
        } else {
          countEl.style.display = 'none';
        }
      }

      if (legendEl) {
        if (this.state.spotlight === 'peaks') {
          legendEl.innerHTML = `
            <span class="vs-heat-legend-lbl">Selloff Exhaustion Tiers:</span>
            <span class="vs-heat-swatch" style="background:#c8922a"></span><span class="vs-heat-legend-lbl">P90</span>
            <span class="vs-heat-swatch" style="background:#d96030"></span><span class="vs-heat-legend-lbl">P95</span>
            <span class="vs-heat-swatch" style="background:#ff4d4d"></span><span class="vs-heat-legend-lbl">P99 Climax</span>
            <span class="vs-heat-swatch vs-heat-swatch-ring-peak"></span><span class="vs-heat-legend-lbl">Hollow = Local Peak (&lt; Floor)</span>
          `;
        } else if (this.state.spotlight === 'valleys') {
          legendEl.innerHTML = `
            <span class="vs-heat-legend-lbl">Low-Turnover Compression Tiers:</span>
            <span class="vs-heat-swatch" style="background:#0ea5e9"></span><span class="vs-heat-legend-lbl">P90</span>
            <span class="vs-heat-swatch" style="background:#06b6d4"></span><span class="vs-heat-legend-lbl">P95</span>
            <span class="vs-heat-swatch" style="background:#e0f2fe"></span><span class="vs-heat-legend-lbl">P99 Drought</span>
            <span class="vs-heat-swatch vs-heat-swatch-ring-valley"></span><span class="vs-heat-legend-lbl">Hollow = Local Drought (&lt; Floor)</span>
          `;
        } else {
          const swatches = BASELINE_COLORS.map(c => `<span class="vs-heat-swatch" style="background:${c}"></span>`).join('');
          legendEl.innerHTML = `
            <span class="vs-heat-legend-lbl">Quiet</span>
            ${swatches}
            <span class="vs-heat-legend-lbl">Structural Volume Climax</span>
          `;
        }
      }
    },

    _bindDOMEvents() {
      if (typeof document === 'undefined' || this._bound) return;
      const rootEl = document.getElementById('vs-heatmap-container');
      if (!rootEl) return;

      rootEl.addEventListener('click', e => {
        const target = e.target.closest('button');
        if (!target) return;
        if (target.dataset.heatScope) {
          this.setScope(target.dataset.heatScope);
        } else if (target.dataset.heatSide) {
          this.setSide(target.dataset.heatSide);
        } else if (target.dataset.heatDays) {
          this.setDays(target.dataset.heatDays);
        } else if (target.id === 'vs-heat-prev') {
          this.prevPage();
        } else if (target.id === 'vs-heat-next') {
          this.nextPage();
        } else if (target.id === 'vs-heat-peaks-btn') {
          this.toggleSpotlight('peaks');
        } else if (target.id === 'vs-heat-valleys-btn') {
          this.toggleSpotlight('valleys');
        } else if (target.dataset.heatThreshold) {
          this.setThreshold(target.dataset.heatThreshold);
        }
      });

      const grid = document.getElementById('vs-heatmap-grid');
      const tooltip = document.getElementById('vs-heatmap-tooltip');
      const viewport = document.getElementById('vs-heat-viewport');
      if (grid && tooltip && viewport) {
        const showCellTip = cell => {
          if (!cell || !cell.classList.contains('vs-heat-cell')) {
            tooltip.style.display = 'none';
            return;
          }
          const priceHtml = cell.dataset.price ? `<div class="vs-heat-tt-price">${cell.dataset.price}</div>` : '';
          const rankHtml = cell.dataset.rank ? `<div class="vs-heat-tt-rank">${cell.dataset.rank}</div>` : '';
          const tagHtml = cell.dataset.tag ? `<div class="vs-heat-tt-tag">${cell.dataset.tag}</div>` : '';
          tooltip.innerHTML = `
            ${priceHtml}
            <div class="vs-heat-tt-date">${cell.dataset.date || ''}</div>
            ${rankHtml}
            <div class="vs-heat-tt-main">${cell.dataset.metric || 'CVI'}: <strong>${cell.dataset.score || '0.0'}</strong> &middot; ${cell.dataset.rvol || ''}</div>
            ${tagHtml}
          `;
          tooltip.style.display = 'flex';

          const vpRect = viewport.getBoundingClientRect();
          const cRect = cell.getBoundingClientRect();
          const tw = tooltip.offsetWidth || 220;
          const th = tooltip.offsetHeight || 64;

          let left = (cRect.left - vpRect.left) + (cRect.width / 2) - (tw / 2);
          left = Math.max(4, Math.min(vpRect.width - tw - 4, left));

          let top = (cRect.top - vpRect.top) - th - 8;
          if (top < 4) {
            top = (cRect.bottom - vpRect.top) + 8;
          }
          tooltip.style.left = `${Math.round(left)}px`;
          tooltip.style.top = `${Math.round(top)}px`;
        };

        grid.addEventListener('mouseover', e => showCellTip(e.target.closest('.vs-heat-cell')));
        grid.addEventListener('mouseleave', () => {
          tooltip.style.display = 'none';
        });
        grid.addEventListener('click', e => showCellTip(e.target.closest('.vs-heat-cell')));
      }

      this._bound = true;
    }
  };

  return VolumeHeatmap;
});
