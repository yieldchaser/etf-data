/**
 * Price-Volume Studies — Canvas 2D Engine & Mathematical Module
 * Native, high-DPI secondary exchange tape and positioning analytics for etf-data.
 * Zero external dependencies. Zero AI slop. 100% factual terminology.
 */

(function (root, factory) {
  if (typeof module === 'object' && module.exports) {
    module.exports = factory();
  } else {
    root.VolumeStudies = factory();
  }
})(typeof self !== 'undefined' ? self : this, function () {
  'use strict';

  const STUDY_METAS = {
    vol: {
      key: 'vol',
      label: 'Daily Volume',
      short: 'VOL',
      desc: 'Daily trading volume with 21-day SMA baseline and RVOL surge classification.',
      methodology: '<p><strong>Daily Volume (<code>vol</code>)</strong> displays daily trading volume (in shares or USD equivalent) overlaid with a 21-day simple moving average (<code>SMA-21</code>) baseline.</p><p>Relative Volume (<code>RVOL-21 = Volume / SMA-21</code>) highlights anomalous liquidity: sessions with <code>RVOL &ge; 2.0&times;</code> are flagged as <em>Climax Surge</em>; sessions with <code>1.2&times; &le; RVOL &lt; 2.0&times;</code> indicate <em>Elevated Volume</em>; sessions with <code>RVOL &lt; 0.8&times;</code> reflect <em>Muted Volume</em>.</p>'
    },
    osc: {
      key: 'osc',
      label: 'Volume Oscillator',
      short: 'VOL OSC',
      desc: 'Percentage spread between 5-day fast SMA and 21-day slow SMA of volume.',
      methodology: '<p><strong>Volume Oscillator (<code>osc</code>)</strong> calculates the percentage difference between a fast 5-day and slow 21-day simple moving average of volume:</p><p><code>VolOsc = ((SMA_5 - SMA_21) / SMA_21) &times; 100%</code></p><p>Readings above zero indicate <em>Pace Expansion</em> (increasing volume velocity), shaded in green. Readings below zero reflect <em>Pace Contraction</em> (drying volume momentum), shaded in rose.</p>'
    },
    net: {
      key: 'net',
      label: 'Net Volume',
      short: 'NET VOL',
      desc: 'Signed daily volume with 21-day rolling cumulative accumulation balance curve.',
      methodology: '<p><strong>Net Volume (<code>net</code>)</strong> assigns directional sign to daily volume based on price close relative to prior close (positive for green sessions, negative for red sessions).</p><p>A solid continuous curve tracks the 21-day cumulative rolling sum (<code>NetVol-21</code>), illustrating whether recent exchange tape turnover has been dominated by <em>Net Inflow</em> or <em>Net Outflow</em>.</p>'
    },
    cmf: {
      key: 'cmf',
      label: 'Chaikin Money Flow',
      short: 'CMF-21',
      desc: 'Volume-weighted accumulation/distribution using Close Location Value over 21 sessions.',
      methodology: '<p><strong>Chaikin Money Flow (<code>cmf</code>)</strong> measures volume-weighted accumulation and distribution pressure over a 21-day lookback window.</p><p>1. <code>CLV = ((Close - Low) - (High - Close)) / (High - Low) = (2 &times; Close - (High + Low)) / (High - Low)</code><br/>2. <code>Money Flow Volume (MFV) = CLV &times; Volume</code><br/>3. <code>CMF-21 = &sum;(MFV, 21) / &sum;(Volume, 21)</code></p><p>Factual regime thresholds: <code>CMF &ge; +0.10</code> indicates <em>Strong Accumulation</em>; <code>0 &lt; CMF &lt; +0.10</code> indicates <em>Accumulation</em>; <code>-0.10 &lt; CMF &lt; 0</code> reflects <em>Distribution</em>; <code>CMF &le; -0.10</code> reflects <em>Strong Distribution</em>. Contrasting 21-day price return against CMF direction detects bullish and bearish divergences.</p>'
    },
    obv: {
      key: 'obv',
      label: 'On-Balance Volume',
      short: 'OBV',
      desc: 'Running cumulative signed volume re-based to zero at visible window start with 21D MA.',
      methodology: '<p><strong>On-Balance Volume (<code>obv</code>)</strong> calculates running cumulative signed volume: adding daily volume on up sessions and subtracting volume on down sessions.</p><p>To provide high-resolution local tape analysis regardless of inception age, the series is re-based to 0 at the visible window start (<code>OBV*_t = OBV_t - OBV_start</code>). A 21-day moving average overlay confirms whether local accumulation trend remains intact.</p>'
    },
    pvt: {
      key: 'pvt',
      label: 'Price-Volume Trend',
      short: 'PVT',
      desc: 'Volume scaled by proportional price moves, re-based to zero at visible window start.',
      methodology: '<p><strong>Price-Volume Trend (<code>pvt</code>)</strong> weights volume by the proportional percentage magnitude of price changes:</p><p><code>PVT_t = PVT_{t-1} + Volume_t &times; ((Close_t - Close_{t-1}) / Close_{t-1})</code></p><p>Unlike OBV which adds flat volume regardless of whether price moved 0.05% or 5.0%, PVT scales linearly with the severity of price swings, capturing high-conviction tape momentum.</p>'
    },
    heatmap: {
      key: 'heatmap',
      label: 'Volume Heatmap',
      short: 'MICROCVI',
      desc: 'MicroCVI Volume Climax & Drought 18-Column Calendar Grid.',
      methodology: '<p><strong>MicroCVI Volume Heatmap (<code>heatmap</code>)</strong> evaluates daily tape microstructure across a trailing 756-session buffer using a 21-day trimmed baseline (central 15 of 21 sorted sessions) and Abdi-Ranaldo effective spreads.</p><p><strong>Stage 1 (Smooth Bounded Intensity 0&ndash;100):</strong> <code>PeakScore</code> blends volume elasticity <code>(1 + RVOL)^0.55 - 1</code> with 22-bar range position, downside shock, and lower-wick absorption (<code>&kappa;=1.35</code>). <code>ValleyScore</code> combines low-turnover compression <code>(1/RVOL)^0.60 - 0.85</code>, true-range contraction, and spread widening (<code>&kappa;=1.60</code>). Multi-ETF scopes aggregate with <code>&lambda;=0.75</code> max + <code>0.25</code> &radic;dollar-turnover weighted breadth.</p><p><strong>Stage 2 (Dual-Gated Viewport Spotlight):</strong> Top <code>P99 / P95 / P90</code> sessions in the visible window are ranked deterministically. Cells exceeding structural floors (<code>Peaks: 68 / 52 / 38</code>; <code>Valleys: 64 / 48 / 34</code>) glow solid as <em>Structural Volume Climax</em> or <em>Structural Drought</em>; cells below floors render as hollow rings (<em>Local Peak / Low-Turnover Compression</em>).</p>'
    }
  };

  const VolumeStudies = {
    STUDY_METAS,
    state: {
      ticker: '',
      study: 'vol',       // 'vol' | 'osc' | 'net' | 'cmf' | 'obv' | 'pvt' | 'heatmap'
      unit: 'shares',     // 'shares' | 'dollar'
      range: '1y',        // '3m' | '6m' | '1y' | '2y' | 'all' | 'year_YYYY' | 'custom'
      customRange: null,  // { startIdx, endIdx }
      hoverIdx: null,
      lastMouse: { x: 0, y: 0 }
    },

    COLORS: {
      up: '#34d399',          // --flow-positive
      down: '#fb7185',        // --flow-negative
      flat: 'rgba(255, 255, 255, 0.30)',
      line: '#22d3ee',        // --flow-cyan
      ma: '#fbbf24',          // --flow-warning (amber)
      grid: 'rgba(255, 255, 255, 0.06)',
      label: '#8d9aaa',       // --flow-subtle
      crosshair: 'rgba(255, 255, 255, 0.35)',
      climax: '#22d3ee'       // cyan highlight for RVOL >= 2.0x
    },

    PAD: { top: 16, right: 16, bottom: 28, left: 68 },

    _etfHistoryCache: null,
    _historyPromise: null,
    _payload: null,
    _cache: null,
    _bound: false,
    _sliderBound: false,
    _rafId: null,

    // ── Mathematical Primitives ──

    sma(values, n) {
      if (!values || n <= 0) return [];
      const out = new Array(values.length).fill(null);
      let sum = 0;
      let count = 0;
      for (let i = 0; i < values.length; i++) {
        const v = values[i];
        if (v != null && isFinite(v)) {
          sum += v;
          count++;
        }
        if (i >= n) {
          const old = values[i - n];
          if (old != null && isFinite(old)) {
            sum -= old;
            count--;
          }
        }
        if (i >= n - 1 && count === n) {
          out[i] = sum / n;
        }
      }
      return out;
    },

    rollingSum(values, n) {
      if (!values || n <= 0) return [];
      const out = new Array(values.length).fill(null);
      let sum = 0;
      let count = 0;
      for (let i = 0; i < values.length; i++) {
        const v = values[i];
        if (v != null && isFinite(v)) {
          sum += v;
          count++;
        }
        if (i >= n) {
          const old = values[i - n];
          if (old != null && isFinite(old)) {
            sum -= old;
            count--;
          }
        }
        if (i >= n - 1 && count === n) {
          out[i] = sum;
        }
      }
      return out;
    },

    signSeries(closes) {
      if (!closes || !closes.length) return [];
      const out = new Array(closes.length).fill(0);
      for (let i = 1; i < closes.length; i++) {
        const curr = closes[i];
        const prev = closes[i - 1];
        if (curr != null && prev != null) {
          out[i] = curr > prev ? 1 : (curr < prev ? -1 : 0);
        }
      }
      return out;
    },

    signedVolume(closes, volumes) {
      const signs = this.signSeries(closes);
      return (volumes || []).map((v, i) => signs[i] * (v || 0));
    },

    volumeOscillator(volumes, fast = 5, slow = 21) {
      const f = this.sma(volumes, fast);
      const s = this.sma(volumes, slow);
      return (volumes || []).map((_, i) => {
        if (f[i] == null || s[i] == null) return null;
        if (s[i] === 0) return f[i] === 0 ? 0 : 100;
        return ((f[i] - s[i]) / s[i]) * 100;
      });
    },

    cmf(highs, lows, closes, volumes, len = 21) {
      const n = (closes || []).length;
      const out = new Array(n).fill(null);
      if (n < len || len <= 0) return out;

      const mfv = new Array(n);
      for (let i = 0; i < n; i++) {
        const h = highs[i], l = lows[i], c = closes[i], v = volumes[i] || 0;
        const range = h - l;
        const clv = (range > 0) ? ((2 * c - (h + l)) / range) : 0;
        // Strict boundary clamp
        const clampedClv = Math.max(-1, Math.min(1, clv));
        mfv[i] = clampedClv * v;
      }

      let sumMfv = 0;
      let sumVol = 0;
      for (let i = 0; i < len; i++) {
        sumMfv += mfv[i];
        sumVol += volumes[i] || 0;
      }
      out[len - 1] = sumVol > 0 ? Math.max(-1, Math.min(1, sumMfv / sumVol)) : 0;

      for (let i = len; i < n; i++) {
        sumMfv += mfv[i] - mfv[i - len];
        sumVol += (volumes[i] || 0) - (volumes[i - len] || 0);
        out[i] = sumVol > 0 ? Math.max(-1, Math.min(1, sumMfv / sumVol)) : 0;
      }
      return out;
    },

    obv(closes, volumes) {
      const signs = this.signSeries(closes);
      const n = (closes || []).length;
      const out = new Array(n).fill(0);
      let acc = 0;
      for (let i = 1; i < n; i++) {
        acc += signs[i] * (volumes[i] || 0);
        out[i] = acc;
      }
      return out;
    },

    pvt(closes, volumes) {
      const n = (closes || []).length;
      const out = new Array(n).fill(0);
      let acc = 0;
      for (let i = 1; i < n; i++) {
        const pc = closes[i - 1];
        const c = closes[i];
        if (pc != null && pc > 0 && c != null) {
          acc += (volumes[i] || 0) * ((c - pc) / pc);
        }
        out[i] = acc;
      }
      return out;
    },

    priceReturn(closes, len = 21) {
      if (!closes || len <= 0) return [];
      const out = new Array(closes.length).fill(null);
      for (let i = len; i < closes.length; i++) {
        const prev = closes[i - len];
        const curr = closes[i];
        if (prev != null && prev > 0 && curr != null && isFinite(curr)) {
          out[i] = ((curr - prev) / prev) * 100;
        }
      }
      return out;
    },

    cmfDivergence(priceReturns, cmfValues) {
      if (!priceReturns || !cmfValues) return [];
      const n = Math.min(priceReturns.length, cmfValues.length);
      const out = new Array(n).fill(null);
      for (let i = 0; i < n; i++) {
        const pr = priceReturns[i];
        const c = cmfValues[i];
        if (pr == null || c == null || !isFinite(pr) || !isFinite(c)) continue;
        if (pr > 0 && c <= -0.03) {
          out[i] = 'BEARISH DIVERGENCE';
        } else if (pr < 0 && c >= 0.03) {
          out[i] = 'BULLISH DIVERGENCE';
        } else if (pr > 0 && c > 0) {
          out[i] = 'IN-SYNC ACCUMULATION';
        } else if (pr < 0 && c < 0) {
          out[i] = 'IN-SYNC DISTRIBUTION';
        } else {
          out[i] = 'NEUTRAL';
        }
      }
      return out;
    },

    // ── Formatting Utilities ──

    fmtVol(v, unit, opts = {}) {
      if (v == null || !isFinite(v)) return '—';
      const isDollar = unit === 'dollar';
      const prefix = isDollar ? '$' : '';
      const suffix = (!isDollar && opts.withSuffix) ? ' shs' : '';
      const a = Math.abs(v);
      const sign = v < 0 ? '-' : '';
      if (a >= 1e12) return `${sign}${prefix}${(a / 1e12).toFixed(2)}T${suffix}`;
      if (a >= 1e9) return `${sign}${prefix}${(a / 1e9).toFixed(2)}B${suffix}`;
      if (a >= 1e6) return `${sign}${prefix}${(a / 1e6).toFixed(2)}M${suffix}`;
      if (a >= 1e3) return `${sign}${prefix}${(a / 1e3).toFixed(0)}K${suffix}`;
      return `${sign}${prefix}${a.toFixed(0)}${suffix}`;
    },

    fmtSigned(v, unit, opts = {}) {
      if (v == null || !isFinite(v)) return '—';
      const sign = v > 0 ? '+' : (v < 0 ? '-' : '');
      const isDollar = unit === 'dollar';
      const prefix = isDollar ? '$' : '';
      const suffix = (!isDollar && opts.withSuffix) ? ' shs' : '';
      const a = Math.abs(v);
      if (a >= 1e12) return `${sign}${prefix}${(a / 1e12).toFixed(2)}T${suffix}`;
      if (a >= 1e9) return `${sign}${prefix}${(a / 1e9).toFixed(2)}B${suffix}`;
      if (a >= 1e6) return `${sign}${prefix}${(a / 1e6).toFixed(2)}M${suffix}`;
      if (a >= 1e3) return `${sign}${prefix}${(a / 1e3).toFixed(0)}K${suffix}`;
      return `${sign}${prefix}${a.toFixed(0)}${suffix}`;
    },

    fmtPct(v) {
      if (v == null || !isFinite(v)) return '—';
      return (v >= 0 ? '+' : '') + v.toFixed(2) + '%';
    },

    fmtPrice(p) {
      if (p == null || !isFinite(p)) return '—';
      if (p >= 1000) return '$' + p.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
      if (p >= 1) return '$' + p.toFixed(2);
      return '$' + p.toFixed(4);
    },

    fmtDate(d) {
      if (!d) return '—';
      const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
      const m = parseInt(d.slice(5, 7), 10);
      const year = d.slice(2, 4);
      return `${months[m - 1] || '?'} '${year}`;
    },

    fmtFullDate(d) {
      if (!d) return '—';
      const days = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
      const dt = new Date(d + 'T00:00:00Z');
      const dayName = isNaN(dt.getUTCDay()) ? '' : days[dt.getUTCDay()];
      return dayName ? `${dayName}, ${d}` : d;
    },

    // ── Range & Window Calculations ──

    sliceStartForDays(dates, days, nowMs) {
      if (!dates || !dates.length) return 0;
      if (days == null) return 0;
      const now = (nowMs == null) ? Date.now() : nowMs;
      const cutoff = new Date(now - days * 864e5).toISOString().slice(0, 10);
      let idx = 0;
      while (idx < dates.length && dates[idx] < cutoff) idx++;
      return Math.min(idx, Math.max(0, dates.length - 2));
    },

    _resolveRangeIndices(dates, rangeKey, customRange) {
      if (!dates || !dates.length) return { startIdx: 0, endIdx: 0 };
      const n = dates.length;

      if (customRange && customRange.startIdx != null && customRange.endIdx != null) {
        const start = Math.max(0, Math.min(n - 2, customRange.startIdx));
        const end = Math.max(start + 1, Math.min(n - 1, customRange.endIdx));
        return { startIdx: start, endIdx: end };
      }

      if (rangeKey && rangeKey.startsWith('year_')) {
        const yr = rangeKey.replace('year_', '');
        let start = 0;
        while (start < n && dates[start].slice(0, 4) < yr) start++;
        let end = start;
        while (end < n && dates[end].slice(0, 4) <= yr) end++;
        end = Math.max(start + 1, Math.min(n - 1, end - 1));
        start = Math.max(0, Math.min(end - 1, start));
        return { startIdx: start, endIdx: end };
      }

      let days = null;
      if (rangeKey === '3m') days = 90;
      else if (rangeKey === '6m') days = 180;
      else if (rangeKey === '1y') days = 365;
      else if (rangeKey === '2y') days = 730;
      else if (rangeKey === 'all') days = null;
      else days = 365;

      const start = this.sliceStartForDays(dates, days);
      return { startIdx: start, endIdx: n - 1 };
    },

    // ── Core Data Pipeline & Computation ──

    compute(history, studyKey = 'vol', rangeKey = '1y', opts = {}) {
      if (!history || !history.length) return null;
      const unit = opts.unit || this.state.unit || 'shares';

      const dates = [];
      const closes = [];
      const highs = [];
      const lows = [];
      const rawVolumes = [];
      const dollarVolumes = [];

      for (let i = 0; i < history.length; i++) {
        const b = history[i];
        if (!b) continue;
        const d = b.d || b.date;
        const c = b.c != null ? b.c : b.close;
        const h = b.h != null ? b.h : (b.high != null ? b.high : c);
        const l = b.l != null ? b.l : (b.low != null ? b.low : c);
        const v = b.v != null ? b.v : (b.volume != null ? b.volume : 0);

        if (!d || c == null || c <= 0) continue;
        dates.push(d);
        closes.push(c);
        highs.push(h);
        lows.push(l);
        rawVolumes.push(v);
        dollarVolumes.push(c * v);
      }

      if (dates.length < 2) return null;

      const volumes = (unit === 'dollar') ? dollarVolumes : rawVolumes;
      const range = this._resolveRangeIndices(dates, rangeKey, opts.customRange || this.state.customRange);
      const startIdx = range.startIdx;
      const endIdx = range.endIdx;

      const maLen = 21;
      const sliceArr = arr => arr ? arr.slice(startIdx, endIdx + 1) : null;

      const basePayload = {
        study: studyKey,
        unit,
        startIdx,
        endIdx,
        fullLength: dates.length,
        dates: dates.slice(startIdx, endIdx + 1),
        closes: closes.slice(startIdx, endIdx + 1),
        volumes: volumes.slice(startIdx, endIdx + 1),
        rawVolumes: rawVolumes.slice(startIdx, endIdx + 1),
        dollarVolumes: dollarVolumes.slice(startIdx, endIdx + 1),
        bars: null,
        line: null,
        ma: null,
        colorSigns: null,
        zeroCentered: false,
        rvol: null
      };

      if (studyKey === 'vol') {
        const ma = this.sma(volumes, maLen);
        const signs = this.signSeries(closes);
        const rvolFull = volumes.map((v, i) => (ma[i] && ma[i] > 0) ? (v / ma[i]) : null);

        return {
          ...basePayload,
          bars: sliceArr(volumes),
          ma: sliceArr(ma),
          colorSigns: sliceArr(signs),
          rvol: sliceArr(rvolFull),
          zeroCentered: false
        };
      }

      if (studyKey === 'osc') {
        const line = this.volumeOscillator(volumes, 5, 21);
        return {
          ...basePayload,
          line: sliceArr(line),
          zeroCentered: true
        };
      }

      if (studyKey === 'net') {
        const signed = this.signedVolume(closes, volumes);
        const line = this.rollingSum(signed, maLen);
        const signs = this.signSeries(closes);
        return {
          ...basePayload,
          bars: sliceArr(signed),
          line: sliceArr(line),
          colorSigns: sliceArr(signs),
          zeroCentered: true
        };
      }

      if (studyKey === 'cmf') {
        const line = this.cmf(highs, lows, closes, volumes, 21);
        const priceRet = this.priceReturn(closes, 21);
        const divergence = this.cmfDivergence(priceRet, line);
        return {
          ...basePayload,
          line: sliceArr(line),
          priceRet: sliceArr(priceRet),
          divergence: sliceArr(divergence),
          zeroCentered: true
        };
      }

      if (studyKey === 'obv' || studyKey === 'pvt') {
        const full = studyKey === 'obv' ? this.obv(closes, volumes) : this.pvt(closes, volumes);
        const baseVal = full[startIdx] || 0;
        const rebased = full.map(v => v - baseVal);
        const maFull = this.sma(full, maLen);
        const maRebased = maFull.map(v => v == null ? null : v - baseVal);

        return {
          ...basePayload,
          line: sliceArr(rebased),
          ma: sliceArr(maRebased),
          zeroCentered: false
        };
      }

      return basePayload;
    },

    // ── High-DPI Canvas 2D Rendering Engine ──

    _draw() {
      if (typeof document === 'undefined') return;
      const canvas = document.getElementById('vs-canvas');
      const payload = this._payload;
      if (!canvas) return;

      const dpr = (typeof window !== 'undefined' && window.devicePixelRatio) || 1;
      const rect = canvas.getBoundingClientRect();
      const w = Math.floor(rect.width);
      const h = Math.floor(rect.height);

      if (w < 40 || h < 40) return;

      const targetW = Math.round(w * dpr);
      const targetH = Math.round(h * dpr);

      if (canvas.width !== targetW || canvas.height !== targetH) {
        canvas.width = targetW;
        canvas.height = targetH;
      }

      const ctx = canvas.getContext('2d');
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx.clearRect(0, 0, w, h);

      if (!payload || !payload.dates || payload.dates.length < 2) {
        this._drawMessage(ctx, w, h, 'No tape observations in selected window');
        return;
      }

      const pad = this.PAD;
      const chartW = w - pad.left - pad.right;
      const chartH = h - pad.top - pad.bottom;
      if (chartW < 20 || chartH < 20) return;

      const n = payload.dates.length;
      const bars = payload.bars;
      const line = payload.line;
      const ma = payload.ma;

      // Calculate vertical scale
      let yMin = Infinity;
      let yMax = -Infinity;

      const scan = arr => {
        if (!arr) return;
        for (let i = 0; i < arr.length; i++) {
          const v = arr[i];
          if (v != null && isFinite(v)) {
            if (v < yMin) yMin = v;
            if (v > yMax) yMax = v;
          }
        }
      };

      scan(bars);
      scan(line);
      scan(ma);

      if (!isFinite(yMin) || !isFinite(yMax)) {
        this._drawMessage(ctx, w, h, 'No numeric observations available');
        return;
      }

      if (bars && !payload.zeroCentered) {
        yMin = Math.min(0, yMin);
      }
      if (payload.zeroCentered || (yMin < 0 && yMax > 0)) {
        yMin = Math.min(0, yMin);
        yMax = Math.max(0, yMax);
      }

      if (payload.study === 'cmf') {
        yMin = Math.min(yMin, -0.15);
        yMax = Math.max(yMax, 0.15);
      }

      if (yMin === yMax) {
        yMin -= 1;
        yMax += 1;
      }

      const padY = (yMax - yMin) * 0.06;
      yMin -= padY;
      yMax += padY;
      if (bars && !payload.zeroCentered) yMin = Math.max(0, yMin);

      const getX = i => pad.left + (n <= 1 ? 0 : (i / (n - 1)) * chartW);
      const mapY = v => pad.top + chartH - ((v - yMin) / (yMax - yMin)) * chartH;
      const clampY = v => Math.max(pad.top, Math.min(pad.top + chartH, mapY(v)));

      // ── Gridlines & Y-Axis Labels ──
      ctx.font = '11px ui-monospace, "SF Mono", "JetBrains Mono", monospace';
      ctx.fillStyle = this.COLORS.label;
      ctx.textAlign = 'right';

      const yFormatter = this._getYFormatter(payload.study, payload.unit);
      const yTicks = 4;
      for (let i = 0; i <= yTicks; i++) {
        const val = yMax - (i / yTicks) * (yMax - yMin);
        const y = pad.top + (i / yTicks) * chartH;
        ctx.strokeStyle = this.COLORS.grid;
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(pad.left, y);
        ctx.lineTo(pad.left + chartW, y);
        ctx.stroke();

        ctx.fillText(yFormatter(val), pad.left - 8, y + 4);
      }

      // ── Zero Baseline (if applicable) ──
      if (yMin < 0 && yMax > 0) {
        const zy = mapY(0);
        ctx.strokeStyle = 'rgba(255, 255, 255, 0.22)';
        ctx.lineWidth = 1;
        ctx.setLineDash([4, 4]);
        ctx.beginPath();
        ctx.moveTo(pad.left, zy);
        ctx.lineTo(pad.left + chartW, zy);
        ctx.stroke();
        ctx.setLineDash([]);
      }

      // ── CMF Horizontal Accumulation / Distribution Bands ──
      if (payload.study === 'cmf') {
        const yPlus10 = clampY(0.10);
        const yMinus10 = clampY(-0.10);

        ctx.strokeStyle = 'rgba(52, 211, 153, 0.35)'; // Positive emerald tint
        ctx.lineWidth = 1;
        ctx.setLineDash([3, 3]);
        ctx.beginPath();
        ctx.moveTo(pad.left, yPlus10);
        ctx.lineTo(pad.left + chartW, yPlus10);
        ctx.stroke();

        ctx.strokeStyle = 'rgba(251, 113, 133, 0.35)'; // Negative rose tint
        ctx.beginPath();
        ctx.moveTo(pad.left, yMinus10);
        ctx.lineTo(pad.left + chartW, yMinus10);
        ctx.stroke();
        ctx.setLineDash([]);

        // Small indicator labels on right edge
        ctx.fillStyle = 'rgba(52, 211, 153, 0.6)';
        ctx.textAlign = 'left';
        ctx.font = '9px ui-monospace, "SF Mono", "JetBrains Mono", monospace';
        ctx.fillText('+0.10 ACCUM', pad.left + chartW - 74, yPlus10 - 3);

        ctx.fillStyle = 'rgba(251, 113, 133, 0.6)';
        ctx.fillText('-0.10 DISTRIB', pad.left + chartW - 78, yMinus10 + 10);
      }

      // ── X-Axis Date Labels ──
      ctx.font = '11px ui-monospace, "SF Mono", "JetBrains Mono", monospace';
      ctx.fillStyle = this.COLORS.label;
      const xTicks = Math.min(6, n);
      for (let k = 0; k < xTicks; k++) {
        const i = Math.round((k / Math.max(1, xTicks - 1)) * (n - 1));
        ctx.textAlign = k === 0 ? 'left' : (k === xTicks - 1 ? 'right' : 'center');
        ctx.fillText(this.fmtDate(payload.dates[i]), getX(i), pad.top + chartH + 16);
      }

      // ── Render Shaded Fills (Oscillator & CMF) ──
      if (line && (payload.study === 'osc' || payload.study === 'cmf')) {
        const zeroY = clampY(0);
        const colW = chartW / n;
        for (let i = 0; i < n; i++) {
          const v = line[i];
          if (v == null || !isFinite(v)) continue;
          ctx.fillStyle = v >= 0 ? 'rgba(52, 211, 153, 0.14)' : 'rgba(251, 113, 133, 0.14)';
          const x = pad.left + (n <= 1 ? 0 : (i / (n - 1)) * chartW) - (colW / 2);
          const yv = clampY(v);
          ctx.fillRect(x, Math.min(yv, zeroY), Math.max(1, colW), Math.max(1, Math.abs(zeroY - yv)));
        }
      }

      // ── Render Bars (Daily Volume & Net Volume) ──
      if (bars) {
        const colW = chartW / n;
        const barW = Math.max(1, colW * 0.72);
        const zeroY = (yMin < 0 && yMax > 0) ? clampY(0) : pad.top + chartH;

        if (colW < 1) {
          // Pixel-bucket aggregation for massive historical windows (2Y, ALL)
          const perBucket = Math.ceil(n / Math.max(1, Math.floor(chartW)));
          const nBuckets = Math.ceil(n / perBucket);
          const stepW = chartW / nBuckets;
          const pixelBarW = Math.max(1, stepW * 0.75);

          for (let b = 0; b < nBuckets; b++) {
            let sum = 0, count = 0, netSign = 0, hasClimax = false;
            const i0 = b * perBucket;
            const i1 = Math.min(n, i0 + perBucket);

            for (let i = i0; i < i1; i++) {
              const v = bars[i];
              if (v == null || !isFinite(v)) continue;
              sum += v;
              count++;
              netSign += payload.colorSigns ? payload.colorSigns[i] : (v > 0 ? 1 : (v < 0 ? -1 : 0));
              if (payload.rvol && payload.rvol[i] >= 2.0) hasClimax = true;
            }
            if (!count) continue;

            const avgVal = sum / count;
            const sign = netSign > 0 ? 1 : (netSign < 0 ? -1 : 0);
            const color = hasClimax ? this.COLORS.climax : (sign > 0 ? this.COLORS.up : (sign < 0 ? this.COLORS.down : this.COLORS.flat));
            const yv = clampY(avgVal);
            const bx = pad.left + b * stepW + (stepW - pixelBarW) / 2;

            ctx.fillStyle = color;
            ctx.globalAlpha = 0.8;
            ctx.fillRect(bx, Math.min(yv, zeroY), pixelBarW, Math.max(1.5, Math.abs(zeroY - yv)));
            ctx.globalAlpha = 1;
          }
        } else {
          // Standard distinct bar rendering
          for (let i = 0; i < n; i++) {
            const v = bars[i];
            if (v == null || !isFinite(v)) continue;
            const sign = payload.colorSigns ? payload.colorSigns[i] : (v > 0 ? 1 : (v < 0 ? -1 : 0));
            const isClimax = payload.rvol && payload.rvol[i] >= 2.0;
            const color = isClimax ? this.COLORS.climax : (sign > 0 ? this.COLORS.up : (sign < 0 ? this.COLORS.down : this.COLORS.flat));

            const yv = clampY(v);
            const bx = pad.left + (n <= 1 ? 0 : (i / (n - 1)) * chartW) - (barW / 2);

            ctx.fillStyle = color;
            ctx.globalAlpha = isClimax ? 0.95 : 0.78;
            ctx.fillRect(bx, Math.min(yv, zeroY), barW, Math.max(1.5, Math.abs(zeroY - yv)));
            ctx.globalAlpha = 1;
          }
        }
      }

      // ── Path Drawing Helper ──
      const drawPolyline = (arr, strokeColor, lineWidth, dash) => {
        if (!arr) return;
        ctx.strokeStyle = strokeColor;
        ctx.lineWidth = lineWidth;
        if (dash) ctx.setLineDash(dash);
        ctx.beginPath();
        let started = false;
        for (let i = 0; i < n; i++) {
          const v = arr[i];
          if (v == null || !isFinite(v)) { started = false; continue; }
          const x = getX(i);
          const y = clampY(v);
          if (!started) { ctx.moveTo(x, y); started = true; }
          else { ctx.lineTo(x, y); }
        }
        ctx.stroke();
        if (dash) ctx.setLineDash([]);
      };

      // ── Render Indicator Curves ──
      if (line) {
        drawPolyline(line, this.COLORS.line, 1.8, null);
      }
      if (ma) {
        drawPolyline(ma, this.COLORS.ma, 1.4, [4, 3]);
      }

      // ── Render Hover Markers (NO CROSSHAIR) ──
      if (this.state.hoverIdx != null && this.state.hoverIdx >= 0 && this.state.hoverIdx < n) {
        const i = this.state.hoverIdx;
        const hx = getX(i);

        if (bars) {
          const v = bars[i];
          if (v != null && isFinite(v)) {
            const colW = chartW / n;
            const barW = Math.max(2, colW * 0.72);
            const bx = hx - (barW / 2);
            const yv = clampY(v);
            const zeroY = (yMin < 0 && yMax > 0) ? clampY(0) : pad.top + chartH;

            ctx.strokeStyle = '#ffffff';
            ctx.lineWidth = 1.5;
            ctx.strokeRect(bx - 0.5, Math.min(yv, zeroY) - 0.5, barW + 1, Math.max(1.5, Math.abs(zeroY - yv)) + 1);
          }
        }

        if (line) {
          const v = line[i];
          if (v != null && isFinite(v)) {
            const hy = clampY(v);
            ctx.beginPath();
            ctx.arc(hx, hy, 4, 0, Math.PI * 2);
            ctx.fillStyle = this.COLORS.line;
            ctx.fill();
            ctx.strokeStyle = '#ffffff';
            ctx.lineWidth = 1.5;
            ctx.stroke();
          }
        }
      }
    },

    _drawMessage(ctx, w, h, text) {
      ctx.clearRect(0, 0, w, h);
      ctx.font = '12px ui-monospace, "SF Mono", "JetBrains Mono", monospace';
      ctx.fillStyle = this.COLORS.label;
      ctx.textAlign = 'center';
      ctx.fillText(text, w / 2, h / 2);
    },

    _getYFormatter(studyKey, unit) {
      if (studyKey === 'osc') return v => (v >= 0 ? '+' : '') + v.toFixed(0) + '%';
      if (studyKey === 'cmf') return v => (v >= 0 ? '+' : '') + v.toFixed(2);
      if (unit === 'dollar') return v => (v < 0 ? '-' : '') + '$' + this.fmtVol(Math.abs(v));
      return v => this.fmtVol(v);
    },

    // ── Interaction: Pointer Events & Tooltip ──

    _bindEvents() {
      if (typeof document === 'undefined' || this._bound) return;
      const canvas = document.getElementById('vs-canvas');
      if (!canvas) return;

      canvas.addEventListener('mousemove', e => this._onPointerMove(e));
      canvas.addEventListener('mouseleave', () => this._onPointerLeave());

      const handleTouch = e => {
        if (!e.touches || !e.touches.length) return;
        const rect = canvas.getBoundingClientRect();
        this._onPointerMove({
          clientX: e.touches[0].clientX,
          clientY: e.touches[0].clientY,
          targetRect: rect
        });
      };

      canvas.addEventListener('touchstart', handleTouch, { passive: true });
      canvas.addEventListener('touchmove', handleTouch, { passive: true });
      canvas.addEventListener('touchend', () => this._onPointerLeave(), { passive: true });
      canvas.addEventListener('touchcancel', () => this._onPointerLeave(), { passive: true });

      this._initSlider();
      this._bound = true;
    },

    _initSlider() {
      if (this._sliderBound) return;
      const sStart = document.getElementById('vs-range-start');
      const sEnd = document.getElementById('vs-range-end');
      if (!sStart || !sEnd) return;

      const MIN_GAP = 5;

      const setActive = activeThumb => {
        if (activeThumb === sStart) {
          sStart.style.zIndex = '5';
          sEnd.style.zIndex = '4';
        } else {
          sEnd.style.zIndex = '5';
          sStart.style.zIndex = '4';
        }
      };

      const onSliderChange = e => {
        const payload = this._payload;
        if (!payload || !payload.fullLength) return;
        const maxVal = payload.fullLength - 1;

        let v1 = parseInt(sStart.value, 10);
        let v2 = parseInt(sEnd.value, 10);

        if (e.target.id === 'vs-range-start') {
          setActive(sStart);
          if (v1 > v2 - MIN_GAP) {
            v1 = Math.max(0, v2 - MIN_GAP);
            sStart.value = v1;
          }
        } else {
          setActive(sEnd);
          if (v2 < v1 + MIN_GAP) {
            v2 = Math.min(maxVal, v1 + MIN_GAP);
            sEnd.value = v2;
          }
        }

        this.state.range = 'custom';
        this.state.customRange = { startIdx: v1, endIdx: v2 };
        this.state.hoverIdx = null;

        if (typeof window !== 'undefined') {
          window.dispatchEvent(new CustomEvent('volume-studies-range', { detail: { range: 'custom' } }));
        }

        this._recomputeAndDraw();
      };

      const elevateThumb = e => {
        if (e.target) setActive(e.target);
      };

      ['pointerdown', 'mousedown', 'touchstart', 'focus'].forEach(evt => {
        sStart.addEventListener(evt, elevateThumb, { passive: true });
        sEnd.addEventListener(evt, elevateThumb, { passive: true });
      });

      sStart.addEventListener('input', onSliderChange);
      sEnd.addEventListener('input', onSliderChange);

      this._sliderBound = true;
    },

    _syncSlider(totalBars, startIdx, endIdx) {
      const sStart = document.getElementById('vs-range-start');
      const sEnd = document.getElementById('vs-range-end');
      const highlight = document.getElementById('vs-brush-highlight');
      const datesLabel = document.getElementById('vs-window-dates');

      if (!sStart || !sEnd || totalBars < 2) return;

      const maxVal = totalBars - 1;
      sStart.max = maxVal;
      sEnd.max = maxVal;

      if (typeof document !== 'undefined') {
        if (document.activeElement !== sStart && sStart.value != startIdx) {
          sStart.value = startIdx;
        }
        if (document.activeElement !== sEnd && sEnd.value != endIdx) {
          sEnd.value = endIdx;
        }
      }

      if (highlight && maxVal > 0) {
        const p1 = Math.max(0, Math.min(1, startIdx / maxVal));
        const p2 = Math.max(p1, Math.min(1, endIdx / maxVal));
        const thumbRadius = 9; // Half of 18px thumb width
        highlight.style.left = `calc(${thumbRadius}px + (100% - ${thumbRadius * 2}px) * ${p1.toFixed(4)})`;
        highlight.style.width = `calc((100% - ${thumbRadius * 2}px) * ${(p2 - p1).toFixed(4)})`;
      }

      if (datesLabel && this._payload && this._payload.dates) {
        const d = this._payload.dates;
        const sessionCount = d.length;
        datesLabel.textContent = `${d[0]} → ${d[d.length - 1]} (${sessionCount} sessions)`;
      }
    },

    _onPointerMove(e) {
      if (!this._payload || !this._payload.dates || this._payload.dates.length < 2) return;
      const canvas = document.getElementById('vs-canvas');
      if (!canvas) return;

      const rect = e.targetRect || canvas.getBoundingClientRect();
      const x = e.clientX - rect.left;
      const y = e.clientY - rect.top;

      const pad = this.PAD;
      const chartW = rect.width - pad.left - pad.right;
      const n = this._payload.dates.length;
      if (chartW <= 0 || n < 2) return;

      if (x < pad.left - 4 || x > rect.width - pad.right + 4) {
        this._onPointerLeave();
        return;
      }

      let idx = Math.round(((x - pad.left) / chartW) * (n - 1));
      idx = Math.max(0, Math.min(n - 1, idx));

      const idxChanged = (idx !== this.state.hoverIdx);
      this.state.hoverIdx = idx;
      this.state.lastMouse = { x, y };

      this._positionTooltip(x, y);

      if (idxChanged) {
        this._renderTooltipContent(idx);
        this._updateHUD(idx);
        this._scheduleDraw();
      }
    },

    _positionTooltip(x, y) {
      const tooltip = document.getElementById('vs-tooltip');
      const viewport = document.getElementById('vs-canvas-container');
      if (!tooltip || !viewport) return;

      const vw = viewport.clientWidth;
      const vh = viewport.clientHeight;
      const tw = tooltip._cachedWidth || 210;
      const th = tooltip._cachedHeight || 140;

      let left = x + 16;
      if (left + tw > vw - 12) left = x - tw - 16;
      left = Math.max(10, Math.min(vw - tw - 10, left));

      let top = y + 12;
      if (top + th > vh - 10) top = y - th - 12;
      top = Math.max(10, Math.min(vh - th - 10, top));

      tooltip.style.left = left + 'px';
      tooltip.style.top = top + 'px';
      tooltip.style.display = 'block';
    },

    _onPointerLeave() {
      this.state.hoverIdx = null;
      const tooltip = document.getElementById('vs-tooltip');
      if (tooltip) tooltip.style.display = 'none';

      this._scheduleDraw();
      this._updateHUD(null);
    },

    _scheduleDraw() {
      if (this._rafId) return;
      if (typeof requestAnimationFrame !== 'undefined') {
        this._rafId = requestAnimationFrame(() => {
          this._rafId = null;
          this._draw();
        });
      } else {
        this._draw();
      }
    },

    _showTooltip(idx) {
      this._renderTooltipContent(idx);
      if (this.state.lastMouse) {
        this._positionTooltip(this.state.lastMouse.x, this.state.lastMouse.y);
      }
    },

    _renderTooltipContent(idx) {
      const tooltip = document.getElementById('vs-tooltip');
      const viewport = document.getElementById('vs-canvas-container');
      const payload = this._payload;
      if (!tooltip || !viewport || !payload) return;

      const date = payload.dates[idx];
      const close = payload.closes[idx];
      const prevClose = idx > 0 ? payload.closes[idx - 1] : close;
      const priceChg = close - prevClose;
      const pricePct = prevClose > 0 ? (priceChg / prevClose) * 100 : 0;
      const chgColor = priceChg > 0 ? this.COLORS.up : (priceChg < 0 ? this.COLORS.down : this.COLORS.flat);
      const chgSign = priceChg > 0 ? '+' : '';

      const rawVol = payload.rawVolumes ? payload.rawVolumes[idx] : payload.volumes[idx];
      const dollarVol = payload.dollarVolumes ? payload.dollarVolumes[idx] : (close * rawVol);

      let studyValue = null;
      if (payload.bars) studyValue = payload.bars[idx];
      else if (payload.line) studyValue = payload.line[idx];

      const meta = STUDY_METAS[payload.study] || { short: payload.study.toUpperCase() };

      // Regime Classification (Strict Zero Banned Words)
      let regimeText = 'NEUTRAL';
      let regimeColor = this.COLORS.label;

      if (payload.study === 'cmf') {
        const div = payload.divergence ? payload.divergence[idx] : null;
        if (div && (div === 'BULLISH DIVERGENCE' || div === 'BEARISH DIVERGENCE')) {
          regimeText = div;
          regimeColor = div === 'BULLISH DIVERGENCE' ? this.COLORS.up : this.COLORS.down;
        } else if (studyValue != null) {
          if (studyValue >= 0.10) {
            regimeText = 'STRONG ACCUMULATION';
            regimeColor = this.COLORS.up;
          } else if (studyValue > 0) {
            regimeText = 'ACCUMULATION';
            regimeColor = this.COLORS.up;
          } else if (studyValue <= -0.10) {
            regimeText = 'STRONG DISTRIBUTION';
            regimeColor = this.COLORS.down;
          } else if (studyValue < 0) {
            regimeText = 'DISTRIBUTION';
            regimeColor = this.COLORS.down;
          }
        }
      } else if (payload.study === 'osc') {
        if (studyValue != null) {
          if (studyValue > 0) {
            regimeText = 'PACE EXPANSION';
            regimeColor = this.COLORS.up;
          } else if (studyValue < 0) {
            regimeText = 'PACE CONTRACTION';
            regimeColor = this.COLORS.down;
          }
        }
      } else if (payload.study === 'vol') {
        const rvol = payload.rvol ? payload.rvol[idx] : null;
        if (rvol != null) {
          if (rvol >= 2.0) {
            regimeText = 'CLIMAX SURGE';
            regimeColor = this.COLORS.climax;
          } else if (rvol >= 1.2) {
            regimeText = 'ELEVATED VOLUME';
            regimeColor = this.COLORS.line;
          } else if (rvol < 0.8) {
            regimeText = 'MUTED VOLUME';
            regimeColor = this.COLORS.label;
          } else {
            regimeText = 'NORMAL VOLUME';
          }
        }
      } else if (payload.study === 'net') {
        if (studyValue != null) {
          regimeText = studyValue > 0 ? 'NET INFLOW' : (studyValue < 0 ? 'NET OUTFLOW' : 'FLAT');
          regimeColor = studyValue > 0 ? this.COLORS.up : (studyValue < 0 ? this.COLORS.down : this.COLORS.flat);
        }
      } else {
        const maVal = payload.ma ? payload.ma[idx] : null;
        if (studyValue != null && maVal != null) {
          regimeText = studyValue >= maVal ? 'ACCUMULATION' : 'DISTRIBUTION';
          regimeColor = studyValue >= maVal ? this.COLORS.up : this.COLORS.down;
        }
      }

      let studyFormatted = '—';
      if (studyValue != null) {
        if (payload.study === 'osc') studyFormatted = (studyValue >= 0 ? '+' : '') + studyValue.toFixed(1) + '%';
        else if (payload.study === 'cmf') studyFormatted = (studyValue >= 0 ? '+' : '') + studyValue.toFixed(3);
        else if (payload.unit === 'dollar') studyFormatted = this.fmtSigned(studyValue, 'dollar');
        else studyFormatted = this.fmtSigned(studyValue, 'shares', { withSuffix: true });
      }

      let extraRows = '';
      if (payload.ma && payload.ma[idx] != null) {
        const maVal = payload.ma[idx];
        const maFmt = payload.study === 'vol'
          ? (payload.unit === 'dollar' ? this.fmtVol(maVal, 'dollar') : this.fmtVol(maVal, 'shares', { withSuffix: true }))
          : (payload.unit === 'dollar' ? this.fmtSigned(maVal, 'dollar') : this.fmtSigned(maVal, 'shares', { withSuffix: true }));
        extraRows += `
          <div class="vs-tt-row">
            <span class="vs-tt-lbl">21D SMA</span>
            <span class="vs-tt-val" style="color:${this.COLORS.ma}">${maFmt}</span>
          </div>`;
      }

      if (payload.study === 'vol' && payload.rvol && payload.rvol[idx] != null) {
        const rvol = payload.rvol[idx];
        const rColor = rvol >= 2.0 ? this.COLORS.climax : (rvol >= 1.2 ? this.COLORS.line : 'inherit');
        extraRows += `
          <div class="vs-tt-row">
            <span class="vs-tt-lbl">RVOL-21</span>
            <span class="vs-tt-val" style="color:${rColor}">${rvol.toFixed(2)}&times;</span>
          </div>`;
      }

      if (payload.study === 'net' && payload.line && payload.line[idx] != null) {
        const r21 = payload.line[idx];
        const r21Fmt = payload.unit === 'dollar'
          ? this.fmtSigned(r21, 'dollar')
          : this.fmtSigned(r21, 'shares', { withSuffix: true });
        extraRows += `
          <div class="vs-tt-row">
            <span class="vs-tt-lbl">NET 21D SUM</span>
            <span class="vs-tt-val">${r21Fmt}</span>
          </div>`;
      }

      if (payload.study === 'cmf') {
        const pr = payload.priceRet ? payload.priceRet[idx] : null;
        const div = payload.divergence ? payload.divergence[idx] : null;
        if (pr != null && isFinite(pr)) {
          const prSign = pr > 0 ? '+' : '';
          const prColor = pr > 0 ? this.COLORS.up : (pr < 0 ? this.COLORS.down : this.COLORS.flat);
          extraRows += `
          <div class="vs-tt-row">
            <span class="vs-tt-lbl">21D PRICE RET</span>
            <span class="vs-tt-val" style="color:${prColor}">${prSign}${pr.toFixed(2)}%</span>
          </div>`;
        }
        if (div && div !== 'NEUTRAL') {
          const divColor = div.includes('BULLISH') ? this.COLORS.up : (div.includes('BEARISH') ? this.COLORS.down : this.COLORS.line);
          extraRows += `
          <div class="vs-tt-row">
            <span class="vs-tt-lbl">DIVERGENCE</span>
            <span class="vs-tt-val" style="color:${divColor}">${div}</span>
          </div>`;
        }
      }

      tooltip.innerHTML = `
        <div class="vs-tt-header">
          <span class="vs-tt-date">${this.fmtFullDate(date)}</span>
          <span class="vs-tt-badge" style="background:${regimeColor}22;color:${regimeColor};border:1px solid ${regimeColor}44">${regimeText}</span>
        </div>
        <div class="vs-tt-row">
          <span class="vs-tt-lbl">CLOSE</span>
          <span class="vs-tt-val">${this.fmtPrice(close)} <span style="color:${chgColor};font-size:0.625rem">(${chgSign}${pricePct.toFixed(1)}%)</span></span>
        </div>
        <div class="vs-tt-row">
          <span class="vs-tt-lbl">${meta.short}</span>
          <span class="vs-tt-val" style="color:var(--flow-cyan)">${studyFormatted}</span>
        </div>
        ${extraRows}
        <div class="vs-tt-divider"></div>
        <div class="vs-tt-row">
          <span class="vs-tt-lbl">SHARES</span>
          <span class="vs-tt-val">${this.fmtVol(rawVol, 'shares', { withSuffix: true })}</span>
        </div>
        <div class="vs-tt-row">
          <span class="vs-tt-lbl">USD VALUE</span>
          <span class="vs-tt-val">${this.fmtVol(dollarVol, 'dollar')}</span>
        </div>
      `;

      tooltip.style.display = 'block';
      tooltip._cachedWidth = tooltip.offsetWidth;
      tooltip._cachedHeight = tooltip.offsetHeight;
      if (this.state.lastMouse) {
        this._positionTooltip(this.state.lastMouse.x, this.state.lastMouse.y);
      }
    },

    _updateHUD(hoverIdx) {
      const payload = this._payload;
      if (!payload || !payload.dates || !payload.dates.length) return;

      const idx = (hoverIdx != null && hoverIdx >= 0 && hoverIdx < payload.dates.length)
        ? hoverIdx
        : payload.dates.length - 1;

      const close = payload.closes[idx];
      const prevClose = idx > 0 ? payload.closes[idx - 1] : close;
      const priceChg = close - prevClose;
      const pricePct = prevClose > 0 ? (priceChg / prevClose) * 100 : 0;
      const chgColor = priceChg > 0 ? this.COLORS.up : (priceChg < 0 ? this.COLORS.down : this.COLORS.flat);
      const chgSign = priceChg > 0 ? '+' : '';

      const rawVol = payload.rawVolumes ? payload.rawVolumes[idx] : payload.volumes[idx];
      const dollarVol = payload.dollarVolumes ? payload.dollarVolumes[idx] : (close * rawVol);

      let studyValue = null;
      if (payload.bars) studyValue = payload.bars[idx];
      else if (payload.line) studyValue = payload.line[idx];

      let rvolStr = '—';
      if (payload.rvol && payload.rvol[idx] != null) {
        rvolStr = payload.rvol[idx].toFixed(2) + '×';
      } else if (payload.study === 'vol' && payload.ma && payload.ma[idx] != null && payload.ma[idx] > 0) {
        rvolStr = (rawVol / payload.ma[idx]).toFixed(2) + '×';
      }

      let studyFormatted = '—';
      if (studyValue != null) {
        if (payload.study === 'osc') studyFormatted = (studyValue >= 0 ? '+' : '') + studyValue.toFixed(1) + '%';
        else if (payload.study === 'cmf') studyFormatted = (studyValue >= 0 ? '+' : '') + studyValue.toFixed(3);
        else if (payload.unit === 'dollar') studyFormatted = this.fmtSigned(studyValue, 'dollar');
        else studyFormatted = this.fmtSigned(studyValue, 'shares', { withSuffix: true });
      }

      let regimeText = 'NEUTRAL';
      let regimeColor = 'inherit';

      if (payload.study === 'cmf') {
        const div = payload.divergence ? payload.divergence[idx] : null;
        if (div && (div === 'BULLISH DIVERGENCE' || div === 'BEARISH DIVERGENCE')) {
          regimeText = div;
          regimeColor = div === 'BULLISH DIVERGENCE' ? this.COLORS.up : this.COLORS.down;
        } else if (studyValue != null) {
          if (studyValue >= 0.10) { regimeText = 'STRONG ACCUMULATION'; regimeColor = this.COLORS.up; }
          else if (studyValue > 0) { regimeText = 'ACCUMULATION'; regimeColor = this.COLORS.up; }
          else if (studyValue <= -0.10) { regimeText = 'STRONG DISTRIBUTION'; regimeColor = this.COLORS.down; }
          else if (studyValue < 0) { regimeText = 'DISTRIBUTION'; regimeColor = this.COLORS.down; }
        }
      } else if (payload.study === 'osc') {
        if (studyValue != null) {
          if (studyValue > 0) { regimeText = 'PACE EXPANSION'; regimeColor = this.COLORS.up; }
          else if (studyValue < 0) { regimeText = 'PACE CONTRACTION'; regimeColor = this.COLORS.down; }
        }
      } else if (payload.study === 'vol') {
        const r = payload.rvol ? payload.rvol[idx] : null;
        if (r != null) {
          if (r >= 2.0) { regimeText = 'CLIMAX SURGE'; regimeColor = this.COLORS.climax; }
          else if (r >= 1.2) { regimeText = 'ELEVATED VOLUME'; regimeColor = this.COLORS.line; }
          else if (r < 0.8) { regimeText = 'MUTED VOLUME'; regimeColor = this.COLORS.label; }
          else { regimeText = 'NORMAL VOLUME'; }
        }
      } else if (payload.study === 'net') {
        if (studyValue != null) {
          regimeText = studyValue > 0 ? 'NET INFLOW' : (studyValue < 0 ? 'NET OUTFLOW' : 'FLAT');
          regimeColor = studyValue > 0 ? this.COLORS.up : (studyValue < 0 ? this.COLORS.down : this.COLORS.flat);
        }
      } else {
        const maVal = payload.ma ? payload.ma[idx] : null;
        if (studyValue != null && maVal != null) {
          regimeText = studyValue >= maVal ? 'ACCUMULATION' : 'DISTRIBUTION';
          regimeColor = studyValue >= maVal ? this.COLORS.up : this.COLORS.down;
        }
      }

      const hudPrice = document.getElementById('vs-hud-price');
      const hudChg = document.getElementById('vs-hud-chg');
      const hudVol = document.getElementById('vs-hud-vol');
      const hudRvol = document.getElementById('vs-hud-rvol');
      const hudVal = document.getElementById('vs-hud-val');
      const hudRegime = document.getElementById('vs-hud-regime');
      const hudStudyLabel = document.getElementById('vs-hud-study-label');

      if (hudStudyLabel) {
        const meta = STUDY_METAS[payload.study];
        hudStudyLabel.textContent = meta ? meta.short : 'STUDY VALUE';
      }

      if (hudPrice) hudPrice.textContent = this.fmtPrice(close);
      if (hudChg) {
        hudChg.textContent = `${chgSign}${pricePct.toFixed(2)}%`;
        hudChg.style.color = chgColor;
      }
      if (hudVol) {
        hudVol.textContent = payload.unit === 'dollar' ? this.fmtVol(dollarVol, 'dollar') : this.fmtVol(rawVol, 'shares', { withSuffix: true });
      }
      if (hudRvol) hudRvol.textContent = rvolStr;
      if (hudVal) hudVal.textContent = studyFormatted;
      if (hudRegime) {
        hudRegime.textContent = regimeText;
        hudRegime.style.color = regimeColor;
        hudRegime.style.background = (regimeColor && regimeColor !== 'inherit') ? `${regimeColor}1a` : 'transparent';
        hudRegime.style.borderColor = (regimeColor && regimeColor !== 'inherit') ? `${regimeColor}44` : 'transparent';
      }

      // Symmetrical top header context updates matching MACD Engine
      const hdrPrice = document.getElementById('vs-header-price');
      const hdrChg = document.getElementById('vs-header-chg');
      const hdrRegime = document.getElementById('vs-header-regime-badge');

      if (hdrPrice) hdrPrice.textContent = this.fmtPrice(close);
      if (hdrChg) {
        hdrChg.textContent = `${chgSign}${pricePct.toFixed(2)}%`;
        hdrChg.style.color = chgColor;
      }
      if (hdrRegime) {
        hdrRegime.style.display = 'inline-block';
        hdrRegime.textContent = regimeText;
        hdrRegime.style.color = regimeColor;
        hdrRegime.style.background = (regimeColor && regimeColor !== 'inherit') ? `${regimeColor}1a` : 'transparent';
        hdrRegime.style.borderColor = (regimeColor && regimeColor !== 'inherit') ? `${regimeColor}44` : 'transparent';
      }
    },

    _updateMethodology(studyKey) {
      if (typeof document === 'undefined') return;
      const el = document.getElementById('vs-methodology-content');
      if (!el) return;
      const meta = STUDY_METAS[studyKey];
      if (meta && meta.methodology) {
        el.innerHTML = meta.methodology;
      }
    },

    // ── Data Loading & State Management ──

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
              this._etfHistoryCache = data.history || {};
              return this._etfHistoryCache;
            }
          } catch (e) {
            // try next
          }
        }
        return {};
      })();

      return this._historyPromise;
    },

    async loadTicker(ticker, options = {}) {
      if (!ticker) return;
      this.state.ticker = ticker;
      if (options.catalogInstruments) {
        this._catalogInstruments = options.catalogInstruments;
      }
      this._bindEvents();
      this._initResizeObserver();

      const allHistory = await this.fetchETFHistory();
      const history = allHistory[ticker] || [];

      // Extract unique years present in this ticker's history
      const years = [];
      const seenYears = new Set();
      for (let i = 0; i < history.length; i++) {
        const d = history[i].d || history[i].date;
        if (d && d.length >= 4) {
          const yr = d.slice(0, 4);
          if (!seenYears.has(yr)) {
            seenYears.add(yr);
            years.push(yr);
          }
        }
      }
      years.sort();

      // If active range is a specific calendar year not available in this ticker, smoothly fallback to 1Y
      if (this.state.range && this.state.range.startsWith('year_')) {
        const yr = this.state.range.replace('year_', '');
        if (!seenYears.has(yr)) {
          this.state.range = '1y';
          if (typeof window !== 'undefined') {
            window.dispatchEvent(new CustomEvent('volume-studies-range', { detail: { range: '1y' } }));
          }
        }
      }

      // Dispatch or update Alpine state if present
      if (typeof window !== 'undefined') {
        window.dispatchEvent(new CustomEvent('volume-studies-years', { detail: { ticker, years } }));
      }

      this._recomputeAndDraw(history);
      return years;
    },

    _initResizeObserver() {
      if (typeof window === 'undefined' || typeof ResizeObserver === 'undefined' || this._resizeObserved) return;
      const container = document.getElementById('vs-canvas-container');
      if (!container) return;

      this._resizeObserver = new ResizeObserver(entries => {
        for (const entry of entries) {
          const cr = entry.contentRect;
          if (!cr || cr.width < 40 || cr.height < 40) continue;
          const w = Math.round(cr.width);
          const h = Math.round(cr.height);
          if (this._lastObservedW === w && this._lastObservedH === h) continue;
          this._lastObservedW = w;
          this._lastObservedH = h;

          if (!this._bound) this._bindEvents();
          if (!this._sliderBound) this._initSlider();
          this._draw();
          if (this._payload) {
            this._syncSlider(this._payload.fullLength, this._payload.startIdx, this._payload.endIdx);
          }
        }
      });
      this._resizeObserver.observe(container);
      this._resizeObserved = true;
    },

    _recomputeAndDraw(historyParam) {
      const history = historyParam || (this._etfHistoryCache && this.state.ticker ? this._etfHistoryCache[this.state.ticker] : []);
      if (!history || !history.length) {
        this._payload = null;
        if (typeof document !== 'undefined') {
          const canvas = document.getElementById('vs-canvas');
          if (canvas) {
            const ctx = canvas.getContext('2d');
            this._drawMessage(ctx, canvas.width, canvas.height, 'Waiting for ticker tape history...');
          }
        }
        return;
      }

      const studyForCompute = this.state.study === 'heatmap' ? 'vol' : this.state.study;
      const payload = this.compute(history, studyForCompute, this.state.range, {
        unit: this.state.unit,
        customRange: this.state.range === 'custom' ? this.state.customRange : null
      });

      this._payload = payload;
      if (payload) {
        this._updateHUD(null);
        this._updateMethodology(this.state.study);
      }

      if (this.state.study === 'heatmap') {
        const hm = typeof VolumeHeatmap !== 'undefined' ? VolumeHeatmap : (typeof window !== 'undefined' ? window.VolumeHeatmap : null);
        if (hm) {
          hm.mount({
            ticker: this.state.ticker,
            allHistory: this._etfHistoryCache || { [this.state.ticker]: history },
            catalogInstruments: this._catalogInstruments || [],
            unit: this.state.unit
          });
        }
        return;
      }

      if (payload) {
        this._syncSlider(payload.fullLength, payload.startIdx, payload.endIdx);
        this._draw();
        if (typeof requestAnimationFrame !== 'undefined') {
          requestAnimationFrame(() => {
            this._draw();
            if (this._payload) {
              this._syncSlider(this._payload.fullLength, this._payload.startIdx, this._payload.endIdx);
            }
          });
        }
      }
    },

    setStudy(studyKey) {
      if (!STUDY_METAS[studyKey]) return;
      this.state.study = studyKey;
      this.state.hoverIdx = null;
      this._recomputeAndDraw();
    },

    setRange(rangeKey) {
      this.state.range = rangeKey;
      this.state.customRange = null;
      this.state.hoverIdx = null;
      this._recomputeAndDraw();
    },

    setYear(yearStr) {
      this.state.range = `year_${yearStr}`;
      this.state.customRange = null;
      this.state.hoverIdx = null;
      this._recomputeAndDraw();
    },

    setUnit(unitKey) {
      if (unitKey !== 'shares' && unitKey !== 'dollar') return;
      this.state.unit = unitKey;
      this.state.hoverIdx = null;
      this._recomputeAndDraw();
    },

    resize() {
      if (this.state.study !== 'heatmap') {
        this._draw();
      }
    }
  };

  if (typeof window !== 'undefined') {
    window.addEventListener('resize', () => VolumeStudies.resize());
  }

  return VolumeStudies;
});
