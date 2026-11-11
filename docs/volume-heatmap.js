/**
 * Volume Heatmap — MicroCVI Stage 1 Microstructure & Stage 2 Causal 252D Spotlight
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
  const CAUSAL_SPOTLIGHT_WINDOW = 252;

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

  function _dateToEpochDays(dateStr) {
    if (!dateStr || typeof dateStr !== 'string') return 0;
    const ms = Date.parse(`${dateStr.slice(0, 10)}T00:00:00Z`);
    return Number.isFinite(ms) ? Math.round(ms / 86400000) : 0;
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

  function _barTotalReturnClose(b) {
    if (!b) return 0;
    const tc = b.tc != null ? Number(b.tc) : NaN;
    if (Number.isFinite(tc) && tc > 0) return tc;
    return _barClose(b);
  }

  function _barVol(b) {
    const v = b.v != null ? Number(b.v) : (b.volume != null ? Number(b.volume) : 0);
    return Number.isFinite(v) ? Math.max(0, v) : 0;
  }

  function _escapeHtml(str) {
    if (str == null) return '';
    return String(str)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function _clamp(x, lo, hi) {
    return Math.max(lo, Math.min(hi, x));
  }

  function formatHeatPrice(price) {
    if (price == null || !Number.isFinite(Number(price)) || Number(price) <= 0) return '';
    const p = Number(price);
    if (p >= 10000) return `$${Math.round(p).toLocaleString('en-US')}`;
    if (p >= 1000) return `$${p.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
    if (p >= 1) return `$${p.toFixed(2)}`;
    return `$${p.toFixed(4)}`;
  }

  // ── Causal Rolling Peak/Trough, Volatility Calibration & DD Classification ──

  function rollingWindowMax(values, windowLen) {
    const n = (Array.isArray(values) || ArrayBuffer.isView(values)) ? values.length : 0;
    const w = Math.max(1, Math.trunc(windowLen || 1));
    const out = new Array(n).fill(null);
    const deque = [];
    let head = 0;
    for (let i = 0; i < n; i++) {
      const v = Number(values[i]);
      while (head < deque.length && deque[head] <= i - w) {
        head++;
      }
      if (Number.isFinite(v) && v > 0) {
        while (deque.length > head && Number(values[deque[deque.length - 1]]) <= v) {
          deque.pop();
        }
        deque.push(i);
      }
      if (head > 64 && head * 2 >= deque.length) {
        deque.splice(0, head);
        head = 0;
      }
      out[i] = deque.length > head ? Number(values[deque[head]]) : null;
    }
    return out;
  }

  function rollingWindowMin(values, windowLen) {
    const n = (Array.isArray(values) || ArrayBuffer.isView(values)) ? values.length : 0;
    const w = Math.max(1, Math.trunc(windowLen || 1));
    const out = new Array(n).fill(null);
    const deque = [];
    let head = 0;
    for (let i = 0; i < n; i++) {
      const v = Number(values[i]);
      while (head < deque.length && deque[head] <= i - w) {
        head++;
      }
      if (Number.isFinite(v) && v > 0) {
        while (deque.length > head && Number(values[deque[deque.length - 1]]) >= v) {
          deque.pop();
        }
        deque.push(i);
      }
      if (head > 64 && head * 2 >= deque.length) {
        deque.splice(0, head);
        head = 0;
      }
      out[i] = deque.length > head ? Number(values[deque[head]]) : null;
    }
    return out;
  }

  function _localYangZhangDaily(bars, windowSize = 21) {
    const N = Array.isArray(bars) ? bars.length : 0;
    const n = Math.max(5, Math.trunc(windowSize || 21));
    const result = new Array(N).fill(null);
    if (N < n + 1) return result;
    const k = 0.34 / (1.34 + (n + 1) / (n - 1));
    const oArr = new Float64Array(n);
    const cArr = new Float64Array(n);

    for (let t = n; t < N; t++) {
      let sumO = 0;
      let sumC = 0;
      let sumRS = 0;
      let ok = true;
      for (let j = 0; j < n; j++) {
        const idx = t - n + 1 + j;
        const curr = bars[idx];
        const prev = bars[idx - 1];
        const o = _barOpen(curr, _barClose(curr));
        const c = _barClose(curr);
        const h = _barHigh(curr, c);
        const l = _barLow(curr, c);
        const cPrev = _barClose(prev);
        if (o <= 0 || h <= 0 || l <= 0 || c <= 0 || cPrev <= 0) {
          ok = false;
          break;
        }
        const logO = Math.log(o / cPrev);
        const logC = Math.log(c / o);
        const logU = Math.log(h / o);
        const logD = Math.log(l / o);
        oArr[j] = logO;
        cArr[j] = logC;
        sumO += logO;
        sumC += logC;
        sumRS += logU * (logU - logC) + logD * (logD - logC);
      }
      if (!ok) continue;
      const meanO = sumO / n;
      const meanC = sumC / n;
      let varO = 0;
      let varC = 0;
      for (let j = 0; j < n; j++) {
        const dO = oArr[j] - meanO;
        const dC = cArr[j] - meanC;
        varO += dO * dO;
        varC += dC * dC;
      }
      varO /= (n - 1);
      varC /= (n - 1);
      const yzVar = varO + k * varC + (1 - k) * (sumRS / n);
      const vol = Math.sqrt(Math.max(1e-12, yzVar));
      result[t] = Number.isFinite(vol) ? vol : null;
    }
    return result;
  }

  function computeCausalDailySigmaSeries(bars, trCloses) {
    const N = Array.isArray(bars) ? bars.length : 0;
    const out = new Array(N).fill(null);
    if (N < 3) return out;
    const yzWin = WINDOW_LAG;
    const yzRaw = (QuantCore && typeof QuantCore.yangZhangVolatility === 'function')
      ? QuantCore.yangZhangVolatility(bars, yzWin, false)
      : _localYangZhangDaily(bars, yzWin);

    for (let t = 1; t < N; t++) {
      const prevYz = yzRaw ? yzRaw[t - 1] : null;
      if (prevYz != null && Number.isFinite(prevYz) && prevYz > 0) {
        out[t] = prevYz * 100.0;
        continue;
      }
      const sStart = Math.max(1, t - yzWin);
      const rets = [];
      for (let s = sStart; s < t; s++) {
        const c0 = trCloses[s - 1];
        const c1 = trCloses[s];
        if (c0 > 0 && c1 > 0) {
          rets.push(Math.log(c1 / c0));
        }
      }
      if (rets.length >= 2) {
        let mean = 0;
        for (let i = 0; i < rets.length; i++) mean += rets[i];
        mean /= rets.length;
        let ss = 0;
        for (let i = 0; i < rets.length; i++) {
          const d = rets[i] - mean;
          ss += d * d;
        }
        out[t] = Math.sqrt(Math.max(1e-12, ss / (rets.length - 1))) * 100.0;
      }
    }
    return out;
  }

  function getDdThresholds(sigmaDailyPct) {
    const sig = (sigmaDailyPct != null && Number.isFinite(Number(sigmaDailyPct)) && Number(sigmaDailyPct) > 0)
      ? Number(sigmaDailyPct)
      : 2.0;
    const scale = Math.pow(Math.max(0.25, sig) / 2.5, 0.65);
    const pullPct = _clamp(8.0 * scale, 4.5, 12.0);
    const deepPct = _clamp(20.0 * scale, 11.0, 26.0);
    const severePct = _clamp(35.0 * scale, 20.0, 42.0);
    const bear252Pct = _clamp(28.0 * scale, 15.0, 40.0);
    const drop1dPct = _clamp(2.0 * sig, 1.5, 22.0);
    const drop1dSevPct = _clamp(3.25 * sig, 2.5, 35.0);
    return {
      sigmaDailyPct: sig,
      pullPct,
      deepPct,
      severePct,
      bear252Pct,
      drop1dPct,
      drop1dSevPct
    };
  }

  function getDdSeverity(dd63Pct, dd252Pct = null, sigmaDailyPct = null, win252 = 252) {
    const thr = getDdThresholds(sigmaDailyPct);
    if (dd63Pct == null || !Number.isFinite(dd63Pct)) {
      return { cls: 'dd-unknown', label: 'N/A', fillPct: 0, thresholds: thr };
    }
    const mag = Math.max(0, -dd63Pct);
    if (mag < thr.pullPct) {
      if (dd252Pct != null && Number.isFinite(dd252Pct) && dd252Pct <= -thr.bear252Pct) {
        const wLbl = (win252 && win252 < 252) ? `${win252}D` : '252D';
        return { cls: 'dd-bear-high', label: `63D HI \u00b7 ${wLbl} BEAR`, fillPct: 0, thresholds: thr };
      }
      return { cls: 'dd-near-high', label: 'NEAR HIGH', fillPct: 0, thresholds: thr };
    }
    if (mag < thr.deepPct) {
      const pct = Math.round(25 + ((mag - thr.pullPct) / (thr.deepPct - thr.pullPct)) * 30);
      return { cls: 'dd-pullback', label: 'PULLBACK', fillPct: pct, thresholds: thr };
    }
    if (mag < thr.severePct) {
      const pct = Math.round(55 + ((mag - thr.deepPct) / (thr.severePct - thr.deepPct)) * 30);
      return { cls: 'dd-deep', label: 'DEEP DD', fillPct: pct, thresholds: thr };
    }
    const rampSpan = Math.max(10.0, thr.severePct * 0.6);
    const pct = Math.min(100, Math.round(85 + ((mag - thr.severePct) / rampSpan) * 15));
    return { cls: 'dd-severe', label: 'SEVERE DD', fillPct: pct, thresholds: thr };
  }

  function computeTickerCloseContext(bars) {
    const map = new Map();
    if (!Array.isArray(bars) || !bars.length) return map;
    const N = bars.length;
    const trCloses = new Float64Array(N);
    const closes = new Float64Array(N);
    const validPrefix = new Int32Array(N + 1);

    for (let i = 0; i < N; i++) {
      const c = _barClose(bars[i]);
      const tr = _barTotalReturnClose(bars[i]);
      closes[i] = c;
      trCloses[i] = tr;
      validPrefix[i + 1] = validPrefix[i] + (tr > 0 ? 1 : 0);
    }

    const peak63 = rollingWindowMax(trCloses, 63);
    const peak252 = rollingWindowMax(trCloses, 252);
    const trough63 = rollingWindowMin(trCloses, 63);
    const trough252 = rollingWindowMin(trCloses, 252);
    const sigmaDaily = computeCausalDailySigmaSeries(bars, trCloses);

    for (let i = 0; i < N; i++) {
      const d = _barDate(bars[i]);
      if (!d) continue;
      const c = closes[i] > 0 ? closes[i] : null;
      const tr = trCloses[i] > 0 ? trCloses[i] : null;
      const prevTr = (i > 0 && trCloses[i - 1] > 0) ? trCloses[i - 1] : null;
      const chg1dPct = (tr != null && prevTr != null) ? ((tr - prevTr) / prevTr) * 100.0 : null;

      const p63 = peak63[i];
      const p252 = peak252[i];
      const t63 = trough63[i];
      const t252 = trough252[i];

      const dd63Pct = (tr != null && p63 != null && p63 > 0) ? Math.min(0.0, ((tr - p63) / p63) * 100.0) : null;
      const dd252Pct = (tr != null && p252 != null && p252 > 0) ? Math.min(0.0, ((tr - p252) / p252) * 100.0) : null;
      const ru63Pct = (tr != null && t63 != null && t63 > 0) ? Math.max(0.0, ((tr - t63) / t63) * 100.0) : null;
      const ru252Pct = (tr != null && t252 != null && t252 > 0) ? Math.max(0.0, ((tr - t252) / t252) * 100.0) : null;

      const win63 = validPrefix[i + 1] - validPrefix[Math.max(0, i + 1 - 63)];
      const win252 = validPrefix[i + 1] - validPrefix[Math.max(0, i + 1 - 252)];

      map.set(d, {
        close: c,
        trClose: tr,
        chg1dPct,
        dd63Pct,
        dd252Pct,
        ru63Pct,
        ru252Pct,
        win63,
        win252,
        sigmaDailyPct: sigmaDaily[i]
      });
    }
    return map;
  }

  function computeEtfMicrostructure(bars, mode = 'share') {
    const normMode = _normalizeMode(mode);
    const T = Array.isArray(bars) ? bars.length : 0;
    const dates = new Array(T);
    const closes = new Float64Array(T);
    const adjOpens = new Float64Array(T);
    const adjHighs = new Float64Array(T);
    const adjLows = new Float64Array(T);
    const adjCloses = new Float64Array(T);
    const turnover = new Float64Array(T);
    const dollarTurnover = new Float64Array(T);
    const trueRange = new Float64Array(T);
    const typPrice = new Float64Array(T);
    const adjTypPrice = new Float64Array(T);
    const abdiSpread = new Float64Array(T);

    for (let t = 0; t < T; t++) {
      const b = bars[t];
      dates[t] = _barDate(b);
      const c = _barClose(b);
      closes[t] = c;
      const o = _barOpen(b, c);
      const h = _barHigh(b, c);
      const l = _barLow(b, c);
      const v = _barVol(b);
      const tp = (h + l + 2.0 * c) * 0.25;
      typPrice[t] = tp;
      dollarTurnover[t] = v * tp;
      turnover[t] = normMode === 'dollar' ? dollarTurnover[t] : v;

      // Scale microstructure OHLC/TR/Abdi-Ranaldo by TRI factor (tc / c) so ex-dividend drops do not inflate selling climax scores
      const tc = _barTotalReturnClose(b);
      const trScale = (c > 0 && tc > 0) ? (tc / c) : 1.0;
      const aO = o * trScale;
      const aH = h * trScale;
      const aL = l * trScale;
      const aC = c * trScale;
      adjOpens[t] = aO;
      adjHighs[t] = aH;
      adjLows[t] = aL;
      adjCloses[t] = aC;
      adjTypPrice[t] = (aH + aL + 2.0 * aC) * 0.25;

      const prevAdjC = t > 0 ? adjCloses[t - 1] : aO;
      const trHigh = Math.max(aH, prevAdjC);
      const trLow = Math.min(aL, prevAdjC);
      trueRange[t] = Math.max(0.0, trHigh - trLow);

      if (t > 0) {
        const prevC = adjCloses[t - 1];
        const prevH = adjHighs[t - 1];
        const prevL = adjLows[t - 1];
        if (prevC > 0 && prevH > 0 && prevL > 0 && aC > 0 && aH > 0 && aL > 0) {
          const cPrev = Math.log(prevC);
          const etaPrev = 0.5 * (Math.log(prevH) + Math.log(prevL));
          const etaCurr = 0.5 * (Math.log(aH) + Math.log(aL));
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
        const jh = adjHighs[j];
        const jl = adjLows[j];
        if (jh > maxH21) maxH21 = jh;
        if (jl < minL21) minL21 = jl;
      }

      const curC = adjCloses[t];
      const curH = adjHighs[t];
      const curL = adjLows[t];
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
        rawShock = Math.max(0.0, curH - adjTypPrice[t - 1]) / (trimMeanTR + EPS);
      } else {
        climaxDir = -1;
        displacement = (maxH21 - curC) / rangeSpan;
        isNewExtreme = curL <= minL21 + EPS ? 1.0 : 0.0;
        wickRatio = (curC - curL) / (trueRange[t] + EPS);
        rawShock = Math.max(0.0, adjTypPrice[t - 1] - curL) / (trimMeanTR + EPS);
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
    if (inst.market_beta_polarity != null && Number.isFinite(Number(inst.market_beta_polarity))) {
      return Number(inst.market_beta_polarity) < 0;
    }
    const lev = (inst.leverage_value != null && Number.isFinite(Number(inst.leverage_value)))
      ? Number(inst.leverage_value)
      : (inst.direction === 'inverse' ? -1.0 : 1.0);
    if (QuantCore && typeof QuantCore.marketBetaPolarity === 'function') {
      return QuantCore.marketBetaPolarity(inst.ticker, lev) < 0;
    }
    return lev < 0 || inst.direction === 'inverse';
  }

  function normalizeUnderlyingKey(raw, instOrTicker) {
    if (instOrTicker && typeof instOrTicker === 'object' && instOrTicker.underlying_id) {
      return String(instOrTicker.underlying_id);
    }
    const tk = typeof instOrTicker === 'string'
      ? instOrTicker
      : (instOrTicker && typeof instOrTicker === 'object' ? instOrTicker.ticker : '');
    if (QuantCore && typeof QuantCore.canonicalUnderlyingId === 'function') {
      const cid = QuantCore.canonicalUnderlyingId(raw, tk);
      if (cid && cid !== 'UNKNOWN') return cid;
    }
    if (!raw) return '';
    return String(raw)
      .replace(/\s*\([^)]*\)\s*$/g, '')
      .replace(/\s+(Technologies|Global|Index)$/i, '')
      .trim()
      .toLowerCase();
  }

  function _cleanUnderlyingLabel(raw, inst) {
    if (inst && inst.underlying_label) return String(inst.underlying_label);
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
    const activeUndKey = active ? normalizeUnderlyingKey(active.underlying, active) : '';
    const underlyingLabel = active ? _cleanUnderlyingLabel(active.underlying, active) : 'Underlying';
    const category = active ? active.category : null;

    const underlyingPeers = activeUndKey
      ? list.filter(x => normalizeUnderlyingKey(x.underlying, x) === activeUndKey)
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
    const activeUndKey = active ? normalizeUnderlyingKey(active.underlying, active) : '';

    let base = [];
    if (scope === 'underlying' && active && activeUndKey) {
      base = list.filter(x => normalizeUnderlyingKey(x.underlying, x) === activeUndKey);
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
    const ctxSlicesByTicker = {};
    const closeCtxMaps = {};
    const getCloseCtxMap = tk => {
      if (!tk) return null;
      if (Object.prototype.hasOwnProperty.call(closeCtxMaps, tk)) return closeCtxMaps[tk];
      const sl = ctxSlicesByTicker[tk];
      const computed = sl ? computeTickerCloseContext(sl) : null;
      closeCtxMaps[tk] = computed;
      return computed;
    };
    const indexMaps = {};
    const unionDateSet = new Set();
    const maxBars = Math.max(1, Math.trunc(opts.maxBars || HEATMAP_MAX_BARS));
    const evalMaxBars = maxBars + CAUSAL_SPOTLIGHT_WINDOW;
    const ctxPadBars = 260;

    for (const t of activeTickers) {
      const stripped = stripLivePartialSession(historiesByTicker[t], opts.isMarketOpen, opts.nowDate);
      if (!stripped.length) continue;
      // Preserve >= 260 pre-slice bars so 252D peak/trough and 21D YZ vol stay strictly causal from bar 0 of visible series
      ctxSlicesByTicker[t] = stripped.length > (maxBars + ctxPadBars) ? stripped.slice(-(maxBars + ctxPadBars)) : stripped;

      // Preserve >= CAUSAL_SPOTLIGHT_WINDOW (252) pre-display Stage 1 bars so bar 0 of series has full 252D causal lookback
      const hist = stripped.length > (evalMaxBars + WINDOW_LAG) ? stripped.slice(-(evalMaxBars + WINDOW_LAG)) : stripped;
      if (!hist.length) continue;

      etfMetrics[t] = computeEtfMicrostructure(hist, normMode);
      const map = new Map();
      for (let i = 0; i < hist.length; i++) {
        const d = _barDate(hist[i]);
        if (d) {
          map.set(d, i);
          if (i >= WINDOW_LAG) unionDateSet.add(d);
        }
      }
      indexMaps[t] = map;
    }

    const sortedUnion = Array.from(unionDateSet).sort();
    const allDates = sortedUnion.length > evalMaxBars ? sortedUnion.slice(-evalMaxBars) : sortedUnion;
    const T = allDates.length;
    const dispDateStart = Math.max(0, T - maxBars);
    const paddedRecords = [];

    const buildLeadObj = (tk, pScore, vScore, rv, date) => {
      if (!tk) return null;
      const m = etfMetrics[tk];
      const map = indexMaps[tk];
      const idx = map ? map.get(date) : undefined;
      const ctxMap = getCloseCtxMap(tk);
      const ctx = ctxMap ? ctxMap.get(date) : null;
      const candClose = (ctx && ctx.close != null && ctx.close > 0)
        ? ctx.close
        : ((m && m.closes && idx !== undefined && m.closes[idx] > 0) ? m.closes[idx] : null);
      return {
        ticker: tk,
        side: isInverseInstrument(catalogMap[tk]) ? 'short' : 'long',
        close: candClose,
        chg1dPct: ctx ? ctx.chg1dPct : null,
        dd63Pct: ctx ? ctx.dd63Pct : null,
        dd252Pct: ctx ? ctx.dd252Pct : null,
        ru63Pct: ctx ? ctx.ru63Pct : null,
        ru252Pct: ctx ? ctx.ru252Pct : null,
        win63: ctx ? ctx.win63 : null,
        win252: ctx ? ctx.win252 : null,
        sigmaDailyPct: ctx ? ctx.sigmaDailyPct : null,
        peakScore: pScore,
        valleyScore: vScore,
        rvol: rv
      };
    };

    for (let t = 0; t < T; t++) {
      const date = allDates[t];
      const isPadRow = t < dispDateStart;
      let maxPeak = 0.0;
      let maxValley = 0.0;
      let maxRvol = 0.0;
      let minRvol = Infinity;
      let weightedPeakSum = 0.0;
      let weightedValleySum = 0.0;
      let weightSum = 0.0;
      let activeCount = 0;
      let bestPeak = null;
      let bestValley = null;

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

        if (pScore > maxPeak) maxPeak = pScore;
        if (vScore > maxValley) maxValley = vScore;
        if (rv > maxRvol) maxRvol = rv;
        if (rv < minRvol) minRvol = rv;

        if (!isPadRow) {
          if (
            !bestPeak ||
            pScore > bestPeak.peakScore + 1e-12 ||
            (Math.abs(pScore - bestPeak.peakScore) <= 1e-12 && (
              rv > bestPeak.rvol + 1e-12 ||
              (Math.abs(rv - bestPeak.rvol) <= 1e-12 && w > bestPeak.weight)
            ))
          ) {
            bestPeak = { ticker, peakScore: pScore, valleyScore: vScore, rvol: rv, weight: w };
          }

          if (
            !bestValley ||
            vScore > bestValley.valleyScore + 1e-12 ||
            (Math.abs(vScore - bestValley.valleyScore) <= 1e-12 && (
              rv < bestValley.rvol - 1e-12 ||
              (Math.abs(rv - bestValley.rvol) <= 1e-12 && w > bestValley.weight)
            ))
          ) {
            bestValley = { ticker, peakScore: pScore, valleyScore: vScore, rvol: rv, weight: w };
          }
        }

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

      if (isPadRow) {
        paddedRecords.push({
          date,
          peakScore: compPeak,
          valleyScore: compValley,
          maxRvol,
          minRvol: safeMinRvol
        });
        continue;
      }

      const leadPeakTicker = bestPeak ? bestPeak.ticker : '';
      const leadValleyTicker = bestValley ? bestValley.ticker : '';

      // Immutable date-based tie-breaker preserves strict per-date uniqueness while remaining 100% truncation invariant
      const epochOffset = _dateToEpochDays(date) * 1e-12;
      const tieKeyPeak = compPeak + 1e-7 * Math.min(100, Math.max(0, maxRvol)) + epochOffset;
      const tieKeyValley = compValley + 1e-7 * Math.min(20, 1.0 / Math.max(safeMinRvol, 0.05)) + epochOffset;

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

      let focusCtx = null;
      if (priceTicker && priceIdx !== undefined && priceIdx >= WINDOW_LAG && priceM) {
        focusCtx = buildLeadObj(
          priceTicker,
          priceM.peakScores[priceIdx],
          priceM.valleyScores[priceIdx],
          priceM.rvolArr[priceIdx],
          date
        );
      }

      paddedRecords.push({
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
        leadPeak: bestPeak ? buildLeadObj(bestPeak.ticker, bestPeak.peakScore, bestPeak.valleyScore, bestPeak.rvol, date) : null,
        leadValley: bestValley ? buildLeadObj(bestValley.ticker, bestValley.peakScore, bestValley.valleyScore, bestValley.rvol, date) : null,
        focusCtx,
        climaxDir,
        compositeTieKeyPeak: tieKeyPeak,
        compositeTieKeyValley: tieKeyValley
      });
    }

    const peakCausal = _computeCausalWindowRanksForSeries(paddedRecords, 'PEAKS', CAUSAL_SPOTLIGHT_WINDOW);
    const valleyCausal = _computeCausalWindowRanksForSeries(paddedRecords, 'VALLEYS', CAUSAL_SPOTLIGHT_WINDOW);
    const dispStart = Math.max(0, paddedRecords.length - maxBars);
    const records = paddedRecords.slice(dispStart).map((rec, idx) => {
      const pIdx = dispStart + idx;
      return {
        ...rec,
        index: idx,
        causalPeakRank: peakCausal.ranks[pIdx],
        causalPeakN: peakCausal.windowLens[pIdx],
        causalValleyRank: valleyCausal.ranks[pIdx],
        causalValleyN: valleyCausal.windowLens[pIdx]
      };
    });

    return {
      p90: STRUCTURAL_FLOORS.PEAKS.P90,
      p95: STRUCTURAL_FLOORS.PEAKS.P95,
      p99: STRUCTURAL_FLOORS.PEAKS.P99,
      peaks: STRUCTURAL_FLOORS.PEAKS,
      valleys: STRUCTURAL_FLOORS.VALLEYS,
      series: records,
      rawDateCount: records.length,
      paddedDateCount: paddedRecords.length
    };
  }

  // ── Stage 2: Strictly Causal Trailing 252D Spotlight ──

  function _getCausalSpotlightCutoffs(wLen) {
    const n = Math.max(0, Math.trunc(wLen || 0));
    const k99 = n >= 50 ? Math.max(1, Math.round(0.01 * n)) : 0;
    const k95 = n >= 20 ? Math.max(1, Math.round(0.05 * n)) : 0;
    const k90 = n >= 20 ? Math.max(k95 + 1, Math.round(0.10 * n)) : 0;
    return { k99, k95, k90 };
  }

  function _secondaryTieMetric(rec, mode) {
    if (!rec) return 0;
    if (mode === 'PEAKS') {
      const rv = rec.maxRvol;
      return (rv != null && Number.isFinite(rv)) ? Math.min(100, Math.max(0, rv)) : 0;
    }
    const rv = rec.minRvol;
    const safeRv = (rv != null && Number.isFinite(rv)) ? rv : 1.0;
    return Math.min(20, 1.0 / Math.max(safeRv, 0.05));
  }

  function _computeCausalWindowRanksForSeries(series, mode, windowLen = CAUSAL_SPOTLIGHT_WINDOW) {
    const N = Array.isArray(series) ? series.length : 0;
    const W = Math.max(1, Math.trunc(windowLen || CAUSAL_SPOTLIGHT_WINDOW));
    const ranks = new Int32Array(N);
    const windowLens = new Int32Array(N);

    for (let i = 0; i < N; i++) {
      const start = Math.max(0, i - W + 1);
      const n = i - start + 1;
      windowLens[i] = n;
      const rec = series[i];
      const score = rec ? (mode === 'PEAKS' ? Number(rec.peakScore) : Number(rec.valleyScore)) : 0;
      if (!(Number.isFinite(score) && score > 0)) {
        ranks[i] = n;
        continue;
      }
      const sec = _secondaryTieMetric(rec, mode);
      let gt = 0;
      let eq = 0;
      for (let s = start; s <= i; s++) {
        const sr = series[s];
        const scS = sr ? (mode === 'PEAKS' ? Number(sr.peakScore) : Number(sr.valleyScore)) : 0;
        if (!Number.isFinite(scS)) continue;
        if (scS > score) {
          gt++;
        } else if (scS === score) {
          const secS = _secondaryTieMetric(sr, mode);
          if (secS > sec) gt++;
          else if (secS === sec) eq++;
        }
      }
      ranks[i] = gt + eq;
    }

    return { ranks, windowLens };
  }

  function evaluateCausalSpotlight(seriesOrSlice, mode = 'PEAKS', activeFilter = 'NONE', opts = {}) {
    const N = Array.isArray(seriesOrSlice) ? seriesOrSlice.length : 0;
    if (N === 0) return [];

    const normMode = mode === 'VALLEYS' ? 'VALLEYS' : 'PEAKS';
    const windowLenOpt = (opts && opts.windowLen != null && Number.isFinite(Number(opts.windowLen)))
      ? Math.max(1, Math.trunc(Number(opts.windowLen)))
      : CAUSAL_SPOTLIGHT_WINDOW;
    const forceRecompute = Boolean(opts && opts.forceRecompute);
    const rankKey = normMode === 'PEAKS' ? 'causalPeakRank' : 'causalValleyRank';
    const nKey = normMode === 'PEAKS' ? 'causalPeakN' : 'causalValleyN';

    let fallbackCausal = null;
    const ensureFallback = () => {
      if (!fallbackCausal) {
        fallbackCausal = _computeCausalWindowRanksForSeries(seriesOrSlice, normMode, windowLenOpt);
      }
      return fallbackCausal;
    };

    const floors = STRUCTURAL_FLOORS[normMode] || STRUCTURAL_FLOORS.PEAKS;
    const output = new Array(N);

    for (let i = 0; i < N; i++) {
      const rec = seriesOrSlice[i];
      const rawScore = rec ? (normMode === 'PEAKS' ? Number(rec.peakScore) : Number(rec.valleyScore)) : 0;
      const score = Number.isFinite(rawScore) ? rawScore : 0;

      const hasPrecomputed = !forceRecompute
        && windowLenOpt === CAUSAL_SPOTLIGHT_WINDOW
        && rec
        && rec[rankKey] != null
        && rec[nKey] != null
        && Number.isFinite(Number(rec[rankKey]))
        && Number.isFinite(Number(rec[nKey]));

      let r;
      let wLen;
      if (hasPrecomputed) {
        r = Math.max(1, Math.trunc(Number(rec[rankKey])));
        wLen = Math.max(1, Math.trunc(Number(rec[nKey])));
      } else {
        const fb = ensureFallback();
        r = fb.ranks[i];
        wLen = fb.windowLens[i];
      }

      const { k99, k95, k90 } = _getCausalSpotlightCutoffs(wLen);

      let visualTier = 'NONE';
      let isStructuralClimax = false;

      if (score > 0) {
        if (k99 > 0 && r <= k99) {
          visualTier = 'RED_P99';
          isStructuralClimax = score >= floors.P99;
        } else if (k95 > 0 && r <= k95) {
          visualTier = 'ORANGE_P95';
          isStructuralClimax = score >= floors.P95;
        } else if (k90 > 0 && r <= k90) {
          visualTier = 'AMBER_P90';
          isStructuralClimax = score >= floors.P90;
        }
      }

      let isHighlighted = true;
      if (activeFilter === 'P99') {
        isHighlighted = score > 0 && k99 > 0 && r <= k99;
      } else if (activeFilter === 'P95') {
        isHighlighted = score > 0 && k95 > 0 && r <= k95;
      } else if (activeFilter === 'P90') {
        isHighlighted = score > 0 && k90 > 0 && r <= k90;
      }

      const baselineQuantileBin = score > 0
        ? Math.min(4, Math.max(0, Math.floor(((wLen - r) / wLen) * 5)))
        : 0;

      output[i] = {
        date: rec ? rec.date : '',
        score,
        maxRvol: rec ? rec.maxRvol : 0,
        windowRank: r,
        windowLen: wLen,
        visualTier,
        isHighlighted,
        isStructuralClimax,
        climaxDir: (rec && rec.climaxDir) || 1,
        baselineQuantileBin
      };
    }

    return output;
  }

  const evaluateViewportSpotlight = evaluateCausalSpotlight;

  // ── DOM Controller & 18-Column Grid Renderer ──

  const VolumeHeatmap = {
    EPS,
    WINDOW_LAG,
    KAPPA_PEAK,
    KAPPA_VALLEY,
    LAMBDA,
    HEATMAP_MAX_BARS,
    CAUSAL_SPOTLIGHT_WINDOW,
    STRUCTURAL_FLOORS,
    BASELINE_COLORS,
    HORIZONS,

    computeTrimmedMean21,
    computeEtfMicrostructure,
    computeHeatAggregate,
    evaluateCausalSpotlight,
    evaluateViewportSpotlight,
    stripLivePartialSession,
    resolveScopeTickers,
    getAvailableScopes,
    isInverseInstrument,
    normalizeUnderlyingKey,
    formatDisplayDate,
    formatHeatPrice,
    rollingWindowMax,
    rollingWindowMin,
    computeCausalDailySigmaSeries,
    computeTickerCloseContext,
    getDdThresholds,
    getDdSeverity,

    state: {
      ticker: '',
      scope: 'active',      // 'active' | 'underlying' | 'category' | 'featured' | 'universe'
      side: 'all',          // 'all' | 'long' | 'short' | individual ticker
      unit: 'shares',       // 'shares' | 'dollar'
      days: 90,             // 90 | 126 | 180 | 252
      offset: 0,
      spotlight: false,     // false | 'peaks' | 'valleys'
      threshold: 95,        // 90 | 95 | 99
      ddOverlay: false      // boolean toggle for DD63 severity & 1D selloff overlay
    },

    _allHistory: null,
    _catalog: [],
    _catalogMap: {},
    _catalogVersion: 0,
    _aggCache: new Map(),
    _userPinnedMacroScope: false,
    _bound: false,
    _globalBound: false,
    _activeHeatCell: null,
    _lastTouchAt: 0,

    _invalidateCacheIfHistoryChanged(allHistory) {
      if (this._allHistory !== allHistory) {
        this._allHistory = allHistory;
        this._aggCache.clear();
      }
    },

    setCatalog(catalogInstruments) {
      this._catalog = Array.isArray(catalogInstruments) ? catalogInstruments : [];
      this._catalogMap = _buildCatalogMap(this._catalog);
      this._catalogVersion += 1;
      this._aggCache.clear();
    },

    _getAggregatePayload() {
      const { ticker, scope, side, unit } = this.state;
      const allHistory = this._allHistory || {};
      const activeInst = this._catalogMap[ticker] || null;
      const et = getEasternDateParts();

      let scopeId = ticker;
      if (scope === 'underlying' && activeInst?.underlying) {
        scopeId = normalizeUnderlyingKey(activeInst.underlying, activeInst) || activeInst.underlying;
      } else if (scope === 'category' && activeInst?.category) {
        scopeId = activeInst.category;
      } else if (scope === 'featured') {
        scopeId = 'featured';
      } else if (scope === 'universe') {
        scopeId = 'universe';
      }

      const normSide = scope === 'active' ? 'all' : side;
      const histCount = Array.isArray(allHistory[ticker]) ? allHistory[ticker].length : 0;
      const cacheKey = `${this._catalogVersion}|${et.dateStr}|${et.isMarketOpen ? 1 : 0}|${scope}|${scopeId}|${unit}|${normSide}|${ticker}|${histCount}`;
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
      const scopeChanged = this.state.scope !== scopeKey;
      this.state.scope = scopeKey;
      this._userPinnedMacroScope = ['category', 'featured', 'universe'].includes(scopeKey);
      this.state.offset = 0;

      if (scopeKey === 'active') {
        this.state.side = 'all';
      } else {
        const activeScopeMeta = scopes[scopeKey] || scopes.active;
        if (!['all', 'long', 'short'].includes(this.state.side)) {
          if (scopeChanged) {
            // Switching baskets (e.g. Underlying -> Category / Featured / All 160) shows the full target basket
            this.state.side = 'all';
          } else {
            const scopeTickers = resolveScopeTickers(this.state.ticker, this._catalog, scopeKey, 'all');
            if (!scopeTickers.includes(this.state.side)) {
              this.state.side = scopeTickers.includes(this.state.ticker) ? this.state.ticker : 'all';
            }
          }
        } else if (this.state.side === 'short' && activeScopeMeta.shortCount === 0) {
          this.state.side = 'all';
        } else if (this.state.side === 'long' && activeScopeMeta.longCount === 0) {
          this.state.side = 'all';
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

    toggleDdOverlay() {
      this.state.ddOverlay = !this.state.ddOverlay;
      this.render();
    },

    _hideHeatTooltip() {
      if (typeof document === 'undefined') return;
      const tooltip = document.getElementById('vs-heatmap-tooltip');
      if (tooltip) {
        tooltip.classList.remove('visible');
        tooltip.setAttribute('aria-hidden', 'true');
        tooltip.style.display = 'none';
      }
      this._activeHeatCell = null;
    },

    _showHeatTooltip(cell) {
      if (!cell || !cell._heatMeta || typeof document === 'undefined') {
        this._hideHeatTooltip();
        return;
      }
      const tooltip = document.getElementById('vs-heatmap-tooltip');
      if (!tooltip) return;

      const m = cell._heatMeta;
      this._activeHeatCell = cell;

      const leadLabel = m.leadTicker
        ? (m.isBasket ? `Lead ${m.leadTicker}` : m.leadTicker)
        : '';

      let chgHtml = '';
      if (m.chg1dPct != null && Number.isFinite(m.chg1dPct)) {
        const dispChg = (m.chg1dPct > -0.05 && m.chg1dPct < 0.05) ? 0.0 : m.chg1dPct;
        const sign = dispChg > 0 ? '+' : '';
        const chgCls = dispChg > 0.05 ? 'pos' : (dispChg < -0.05 ? 'neg' : 'flat');
        chgHtml = `<span class="vs-hm-tt-chg ${chgCls}">${sign}${dispChg.toFixed(1)}%</span>`;
      }

      let priceRowHtml = '';
      if (leadLabel || m.priceStr || chgHtml) {
        priceRowHtml = `<div class="vs-hm-tt-price-row">`
          + (leadLabel ? `<span class="vs-hm-tt-asset">${_escapeHtml(leadLabel)}</span>` : '')
          + (m.priceStr ? `<span class="vs-hm-tt-price">${_escapeHtml(m.priceStr)}</span>` : '')
          + chgHtml
          + `</div>`;
      }

      const pctTop = m.totalN > 0 ? Math.max(1, Math.min(100, Math.round((m.windowRank / m.totalN) * 100))) : 100;
      const fmtDd = v => (v > -0.05 && v <= 0) ? '0.0%' : `${v.toFixed(1)}%`;
      const fmtRu = v => (v >= 0 && v < 0.05) ? '0.0%' : `+${v.toFixed(1)}%`;

      const buildDdRow = (winLabel, ddVal, ruVal) => {
        if (ddVal == null || !Number.isFinite(ddVal)) return '';
        const hasRu = ruVal != null && Number.isFinite(ruVal);
        return `<span class="vs-hm-tt-dd-win">${_escapeHtml(winLabel)}</span>`
          + `<span class="vs-hm-tt-dd-pk">${fmtDd(ddVal)} <span class="vs-hm-tt-dd-unit">pk</span></span>`
          + (hasRu
            ? `<span class="vs-hm-tt-dd-sep">&middot;</span><span class="vs-hm-tt-dd-low">${fmtRu(ruVal)} <span class="vs-hm-tt-dd-unit">low</span></span>`
            : `<span class="vs-hm-tt-dd-sep"></span><span class="vs-hm-tt-dd-low"></span>`);
      };

      let ddRowHtml = '';
      if (m.dd63Pct != null && Number.isFinite(m.dd63Pct)) {
        const w63Label = (m.win63 && m.win63 < 63) ? `${m.win63}D` : '63D';
        const row63 = buildDdRow(w63Label, m.dd63Pct, m.ru63Pct);
        const showLongWin = m.dd252Pct != null && Number.isFinite(m.dd252Pct) && (!m.win252 || !m.win63 || m.win252 > m.win63);
        const w252Label = (m.win252 && m.win252 < 252) ? `${m.win252}D` : '252D';
        const row252 = showLongWin ? buildDdRow(w252Label, m.dd252Pct, m.ru252Pct) : '';
        const thr = m.thresholds;
        const scaleNote = (m.sigmaDailyPct != null && Number.isFinite(m.sigmaDailyPct) && thr)
          ? `<div class="vs-hm-tt-dd-scale">YZ &sigma;<sub>21</sub> ${m.sigmaDailyPct.toFixed(1)}% &middot; Pull &le;-${thr.pullPct.toFixed(1)}% &middot; 1D &le;-${thr.drop1dPct.toFixed(1)}%</div>`
          : '';

        ddRowHtml = `<div class="vs-hm-tt-dd ${_escapeHtml(m.ddTierCls)}${m.ddOverlay ? ' overlay-active' : ''}">`
          + `<div class="vs-hm-tt-dd-grid">${row63}${row252}</div>`
          + `<span class="vs-hm-tt-dd-pill ${_escapeHtml(m.ddTierCls)}">${_escapeHtml(m.ddSeverityLabel)}</span>`
          + scaleNote
          + `</div>`;
      }

      let tagHtml = '';
      if (m.isSpotlight) {
        const basketSuffix = (m.isBasket && m.activeCount > 1) ? ` &middot; ${m.activeCount} ETFs` : '';
        if (m.isHighlighted) {
          const regimeCls = m.isValleys ? 'is-valley' : 'is-peak';
          tagHtml = `<div class="vs-heat-tt-tag ${regimeCls}">${_escapeHtml(m.filterTag)} &middot; ${_escapeHtml(m.climaxTag)}${basketSuffix}</div>`;
        } else {
          const reqN = m.threshold === 99 ? 50 : 20;
          if (m.totalN < reqN) {
            tagHtml = `<div class="vs-heat-tt-tag is-dim">DIMMED &middot; Warm-up (${m.totalN}/${reqN}D)${basketSuffix}</div>`;
          } else if (!(m.rawScore > 0)) {
            const zeroReason = m.isValleys ? 'No drought signal (0.0)' : 'No climax signal (0.0)';
            tagHtml = `<div class="vs-heat-tt-tag is-dim">DIMMED &middot; ${zeroReason}${basketSuffix}</div>`;
          } else {
            const tierNote = (m.filterTag && m.filterTag !== 'BASE') ? ` &middot; Tier ${_escapeHtml(m.filterTag)}` : '';
            tagHtml = `<div class="vs-heat-tt-tag is-dim">DIMMED &middot; Below P${m.threshold} cutoff${tierNote}${basketSuffix}</div>`;
          }
        }
      } else if (m.isBasket && m.activeCount > 1) {
        tagHtml = `<div class="vs-heat-tt-tag">Basket: ${m.activeCount} ETFs &middot; ${_escapeHtml(m.modeWord)}</div>`;
      }

      const rankScopeLbl = m.totalN > 0 ? `Causal ${m.totalN}D` : 'Causal';
      tooltip.innerHTML = priceRowHtml
        + `<div class="vs-heat-tt-date">${_escapeHtml(m.dateFormatted)}</div>`
        + `<div class="vs-heat-tt-rank">${rankScopeLbl} Rank #${m.windowRank} / ${m.totalN} &middot; Top ${pctTop}%</div>`
        + `<div class="vs-heat-tt-main"><span class="vs-hm-tt-lbl">${_escapeHtml(m.metricLabel)}:</span> <strong>${_escapeHtml(m.scoreStr)}</strong> &middot; <span class="vs-hm-tt-rvol">RVOL ${_escapeHtml(m.rvolStr)}</span></div>`
        + ddRowHtml
        + tagHtml;

      tooltip.style.visibility = 'hidden';
      tooltip.style.display = 'flex';

      const cRect = cell.getBoundingClientRect();
      const viewport = document.getElementById('vs-heat-viewport');
      const panelEl = cell.closest('.flow-volume-studies-card') || viewport;
      const panelRect = panelEl ? panelEl.getBoundingClientRect() : null;
      const tw = tooltip.offsetWidth || 226;
      const th = tooltip.offsetHeight || 136;
      const pad = 8;
      const GAP = 8;
      const vw = typeof window !== 'undefined' ? window.innerWidth : 1200;
      const vh = typeof window !== 'undefined' ? window.innerHeight : 800;

      const minLeft = panelRect ? Math.max(pad, panelRect.left + 6) : pad;
      const maxRight = panelRect ? Math.min(vw - pad, panelRect.right - 6) : (vw - pad);
      const maxLeft = Math.max(minLeft, maxRight - tw);

      let left = cRect.left + (cRect.width / 2) - (tw / 2);
      left = Math.max(minLeft, Math.min(maxLeft, left));

      let top = cRect.top - th - GAP;
      if (top < pad) {
        top = cRect.bottom + GAP;
      }
      top = Math.max(pad, Math.min(vh - th - pad, top));

      tooltip.style.left = `${Math.round(left)}px`;
      tooltip.style.top = `${Math.round(top)}px`;
      tooltip.style.visibility = 'visible';
      tooltip.classList.add('visible');
      tooltip.setAttribute('aria-hidden', 'false');
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
      this._hideHeatTooltip();
      container.innerHTML = '';

      const spotlight = this.state.spotlight;
      const isPeaks = spotlight === 'peaks';
      const isValleys = spotlight === 'valleys';
      const isSpotlight = isPeaks || isValleys;
      const ddOverlay = Boolean(this.state.ddOverlay);

      container.classList.toggle('vs-heat-peaks-mode', isPeaks);
      container.classList.toggle('vs-heat-valleys-mode', isValleys);
      container.classList.toggle('vs-heat-dd-mode', ddOverlay);

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
      const cellStates = evaluateCausalSpotlight(visibleSlice, regimeMode, activeFilter);

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

        const lead = isValleys ? (day.leadValley || day.leadPeak) : (day.leadPeak || day.leadValley);
        const isBasket = day.activeCount > 1;
        const activeCtx = isBasket ? lead : (day.focusCtx || lead);
        const leadTicker = activeCtx && activeCtx.ticker
          ? activeCtx.ticker
          : (day.priceTicker || (isValleys ? day.leadValleyTicker : day.leadPeakTicker) || '');

        const dateFormatted = formatDisplayDate(day.date);
        const rawClose = (activeCtx && activeCtx.close != null) ? activeCtx.close : day.focusClose;
        const priceStr = formatHeatPrice(rawClose);
        const chg1dPct = activeCtx ? activeCtx.chg1dPct : null;
        const dd63Pct = activeCtx ? activeCtx.dd63Pct : null;
        const dd252Pct = activeCtx ? activeCtx.dd252Pct : null;
        const ru63Pct = activeCtx ? activeCtx.ru63Pct : null;
        const ru252Pct = activeCtx ? activeCtx.ru252Pct : null;
        const win63 = activeCtx ? activeCtx.win63 : null;
        const win252 = activeCtx ? activeCtx.win252 : null;
        const sigmaDailyPct = activeCtx ? activeCtx.sigmaDailyPct : null;
        const ddInfo = getDdSeverity(dd63Pct, dd252Pct, sigmaDailyPct, win252);

        const rvolVal = isValleys
          ? ((activeCtx && activeCtx.rvol != null) ? activeCtx.rvol : (day.minRvol != null ? day.minRvol : (day.maxRvol != null ? day.maxRvol : 1.0)))
          : ((activeCtx && activeCtx.rvol != null) ? activeCtx.rvol : (day.maxRvol != null ? day.maxRvol : 1.0));
        const rvolStr = `${rvolVal.toFixed(2)}x`;

        let climaxTag = '';
        let filterTag = 'BASE';

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
          climaxTag = st.isStructuralClimax
            ? (isPeaks ? `STRUCTURAL ${dirLabel}` : 'STRUCTURAL DROUGHT')
            : (isPeaks ? `LOCAL ${dirLabel}` : 'LOW-TURNOVER COMPRESSION');
          filterTag = st.visualTier === 'RED_P99' ? 'P99' : (st.visualTier === 'ORANGE_P95' ? 'P95' : (st.visualTier === 'AMBER_P90' ? 'P90' : 'BASE'));
        }

        const metricLabel = isValleys ? 'VOLUME DROUGHT' : label;
        cell._heatMeta = {
          dateFormatted,
          leadTicker,
          isBasket,
          activeCount: day.activeCount || 1,
          priceStr,
          chg1dPct,
          metricLabel,
          modeWord: this.state.unit === 'dollar' ? 'Dollar Vol' : 'Share Vol',
          rawScore: score,
          scoreStr: score.toFixed(1),
          rvolStr,
          windowRank: st.windowRank,
          totalN: st.windowLen || N,
          dd63Pct,
          dd252Pct,
          ru63Pct,
          ru252Pct,
          win63,
          win252,
          sigmaDailyPct,
          thresholds: ddInfo.thresholds,
          ddSeverityLabel: ddInfo.label,
          ddTierCls: ddInfo.cls,
          ddOverlay,
          isSpotlight,
          isPeaks,
          isValleys,
          isHighlighted: st.isHighlighted,
          threshold: this.state.threshold,
          climaxTag,
          filterTag
        };

        if (ddOverlay) {
          if (ddInfo.cls) cell.classList.add(ddInfo.cls);
          const track = document.createElement('span');
          track.className = 'vs-heat-dd-track';
          if (ddInfo.fillPct > 0) {
            const fill = document.createElement('span');
            fill.className = `vs-heat-dd-fill ${ddInfo.cls}`;
            fill.style.width = `${ddInfo.fillPct}%`;
            track.appendChild(fill);
          }
          cell.appendChild(track);

          if (chg1dPct != null && Number.isFinite(chg1dPct) && chg1dPct <= -ddInfo.thresholds.drop1dPct) {
            const pip = document.createElement('span');
            pip.className = chg1dPct <= -ddInfo.thresholds.drop1dSevPct
              ? 'vs-heat-dd-drop severe'
              : 'vs-heat-dd-drop';
            cell.appendChild(pip);
          }
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
            const safeTk = _escapeHtml(tk);
            const safeTitle = _escapeHtml(`Isolate ${tk}${inst.fund_name ? ' — ' + inst.fund_name : ''}`);
            html += `<button type="button" class="vs-heat-btn vs-heat-etf-pill ${inv ? 'is-inverse' : 'is-long'} ${isSel ? 'active' : ''}" data-heat-side="${safeTk}" title="${safeTitle}">${safeTk}${_escapeHtml(levTag)}</button>`;
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

      // DD% overlay toggle button & collapsible legend visibility
      const ddBtn = document.getElementById('vs-heat-dd-toggle');
      const ddLegend = document.getElementById('vs-heat-dd-legend');
      const ddActive = Boolean(this.state.ddOverlay);
      if (ddBtn) {
        ddBtn.classList.toggle('active', ddActive);
        ddBtn.setAttribute('aria-pressed', String(ddActive));
      }
      if (ddLegend) {
        ddLegend.classList.toggle('visible', ddActive);
        ddLegend.setAttribute('aria-hidden', String(!ddActive));
      }
    },

    _updateDdLegend(visibleSlice) {
      if (typeof document === 'undefined') return;
      const ddLegend = document.getElementById('vs-heat-dd-legend');
      if (!ddLegend) return;

      const isValleys = this.state.spotlight === 'valleys';
      const tickersSeen = new Set();
      let maxWin252 = 0;
      let latestCtx = null;
      let pullMin = Infinity, pullMax = -Infinity;
      let deepMin = Infinity, deepMax = -Infinity;
      let sevMin = Infinity, sevMax = -Infinity;
      let bearMin = Infinity, bearMax = -Infinity;
      let dropMin = Infinity, dropMax = -Infinity;

      for (const day of (visibleSlice || [])) {
        const lead = isValleys ? (day.leadValley || day.leadPeak) : (day.leadPeak || day.leadValley);
        const ctx = day.activeCount > 1 ? lead : (day.focusCtx || lead);
        if (!ctx) continue;
        if (ctx.ticker) tickersSeen.add(ctx.ticker);
        if (ctx.win252 && ctx.win252 > maxWin252) maxWin252 = ctx.win252;
        const thr = getDdThresholds(ctx.sigmaDailyPct);
        pullMin = Math.min(pullMin, thr.pullPct);
        pullMax = Math.max(pullMax, thr.pullPct);
        deepMin = Math.min(deepMin, thr.deepPct);
        deepMax = Math.max(deepMax, thr.deepPct);
        sevMin = Math.min(sevMin, thr.severePct);
        sevMax = Math.max(sevMax, thr.severePct);
        bearMin = Math.min(bearMin, thr.bear252Pct);
        bearMax = Math.max(bearMax, thr.bear252Pct);
        dropMin = Math.min(dropMin, thr.drop1dPct);
        dropMax = Math.max(dropMax, thr.drop1dPct);
        latestCtx = ctx;
      }

      if (!latestCtx || !isFinite(pullMin)) {
        const defThr = getDdThresholds(null);
        pullMin = pullMax = defThr.pullPct;
        deepMin = deepMax = defThr.deepPct;
        sevMin = sevMax = defThr.severePct;
        bearMin = bearMax = defThr.bear252Pct;
        dropMin = dropMax = defThr.drop1dPct;
      }

      const isSingle = tickersSeen.size <= 1;
      const refThr = (isSingle && latestCtx) ? getDdThresholds(latestCtx.sigmaDailyPct) : null;
      const winLbl = (maxWin252 > 0 && maxWin252 < 252) ? `${maxWin252}D` : '252D';

      const fmtNum = v => v.toFixed(1);
      const fmtRng = (lo, hi) => (Math.abs(hi - lo) < 0.25) ? `${fmtNum(hi)}%` : `${fmtNum(lo)}..${fmtNum(hi)}%`;

      const titleText = (isSingle && latestCtx && latestCtx.ticker && latestCtx.sigmaDailyPct != null)
        ? `DD63 (${_escapeHtml(latestCtx.ticker)} &middot; &sigma;<sub>21</sub> ${latestCtx.sigmaDailyPct.toFixed(1)}%):`
        : 'DD63 Overlay (YZ &sigma;-Scaled):';

      const nearLbl = refThr
        ? `Near High (&lt;${fmtNum(refThr.pullPct)}% &middot; 63D &amp; ${winLbl})`
        : `Near High (63D &amp; ${winLbl})`;
      const bearLbl = refThr
        ? `63D Hi &middot; ${winLbl} Bear (&le;-${fmtNum(refThr.bear252Pct)}%)`
        : `63D Hi &middot; ${winLbl} Bear (&le;-${fmtRng(bearMin, bearMax)})`;
      const pullLbl = refThr
        ? `Pullback (-${fmtNum(refThr.pullPct)}% to -${fmtNum(refThr.deepPct)}%)`
        : `Pullback (-${fmtRng(pullMin, pullMax)} to -${fmtRng(deepMin, deepMax)})`;
      const deepLbl = refThr
        ? `Deep DD (-${fmtNum(refThr.deepPct)}% to -${fmtNum(refThr.severePct)}%)`
        : `Deep DD (-${fmtRng(deepMin, deepMax)} to -${fmtRng(sevMin, sevMax)})`;
      const sevLbl = refThr
        ? `Severe DD (-${fmtNum(refThr.severePct)}%+)`
        : `Severe DD (-${fmtRng(sevMin, sevMax)}+)`;
      const dropLbl = refThr
        ? `-${fmtNum(refThr.drop1dPct)}%+ 1D Drop`
        : `&ge;2&sigma; 1D Drop (-${fmtRng(dropMin, dropMax)}+)`;

      ddLegend.innerHTML = `
        <span class="vs-heat-dd-legend-title">${titleText}</span>
        <span class="vs-heat-dd-legend-item"><span class="vs-heat-dd-swatch dd-near-high"></span>${nearLbl}</span>
        <span class="vs-heat-dd-legend-item"><span class="vs-heat-dd-swatch dd-bear-high"></span>${bearLbl}</span>
        <span class="vs-heat-dd-legend-item"><span class="vs-heat-dd-swatch dd-pullback"></span>${pullLbl}</span>
        <span class="vs-heat-dd-legend-item"><span class="vs-heat-dd-swatch dd-deep"></span>${deepLbl}</span>
        <span class="vs-heat-dd-legend-item"><span class="vs-heat-dd-swatch dd-severe"></span>${sevLbl}</span>
        <span class="vs-heat-dd-legend-item"><span class="vs-heat-dd-pip-sample"></span>${dropLbl}</span>
      `;
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
            <span class="vs-heat-legend-lbl">Climax &amp; Exhaustion Tiers:</span>
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

      this._updateDdLegend(visibleSlice);
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
        } else if (target.id === 'vs-heat-dd-toggle') {
          this.toggleDdOverlay();
        }
      });

      const grid = document.getElementById('vs-heatmap-grid');
      if (grid) {
        grid.addEventListener('mouseover', e => {
          if (Date.now() - (this._lastTouchAt || 0) < 450) return;
          const cell = e.target.closest('.vs-heat-cell');
          if (cell && grid.contains(cell)) {
            this._showHeatTooltip(cell);
          }
        });
        grid.addEventListener('mouseleave', () => {
          this._hideHeatTooltip();
        });
        grid.addEventListener('click', e => {
          if (Date.now() - (this._lastTouchAt || 0) < 450) return;
          const cell = e.target.closest('.vs-heat-cell');
          if (cell && grid.contains(cell)) {
            this._showHeatTooltip(cell);
          }
        });
      }

      if (!this._globalBound && typeof document.addEventListener === 'function') {
        document.addEventListener('touchstart', e => {
          this._lastTouchAt = Date.now();
          const curGrid = document.getElementById('vs-heatmap-grid');
          const cell = (curGrid && e.target && typeof e.target.closest === 'function')
            ? e.target.closest('.vs-heat-cell')
            : null;
          if (cell && curGrid.contains(cell)) {
            if (this._activeHeatCell === cell) {
              this._hideHeatTooltip();
            } else {
              this._showHeatTooltip(cell);
            }
          } else {
            this._hideHeatTooltip();
          }
        }, { passive: true });
        if (typeof window !== 'undefined' && typeof window.addEventListener === 'function') {
          window.addEventListener('scroll', () => this._hideHeatTooltip(), { passive: true });
          window.addEventListener('resize', () => this._hideHeatTooltip(), { passive: true });
        }
        this._globalBound = true;
      }

      this._bound = true;
    }
  };

  return VolumeHeatmap;
});
