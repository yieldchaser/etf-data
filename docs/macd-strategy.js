/**
 * MACD Momentum Engine & Tactical Quant Radar — Canvas 2D Engine
 * Standard MACD (12, 26, 9 EMA EMA) on 1-Hour & 1-Day Timeframes
 * Floor Pivots (P, R1, S1) + Sub-Zero Reversal Confluence
 * Zero external dependencies · Mathematical Quantitative Execution Architecture
 */

(function (root, factory) {
  if (typeof module === 'object' && module.exports) {
    let qc = null;
    try { qc = require('./quant-core.js'); } catch (_) {}
    module.exports = factory(qc);
  } else {
    root.MACDStrategy = factory(root.QuantCore);
  }
})(typeof self !== 'undefined' ? self : this, function (QuantCore) {
  'use strict';

  const CANONICAL_OVERRIDES = {
    SMST: {
      fund_name: 'Defiance Daily Target 2X Short MSTR ETF',
      issuer: 'Defiance ETFs',
      leverage: '-2x',
      leverage_value: -2.0,
      direction: 'inverse',
      category: '9. Selective Benchmark Hedging / Tactical Shorts (Pruned to Key Anchors Only)'
    }
  };

  function _escapeHtml(s) {
    return String(s == null ? '' : s)
      .replaceAll('&', '&amp;')
      .replaceAll('<', '&lt;')
      .replaceAll('>', '&gt;')
      .replaceAll('"', '&quot;')
      .replaceAll("'", '&#039;');
  }

  const MACDStrategy = {
    state: {
      signalsData: null,
      activeTicker: '',
      activeTimeframe: '1h', // '1h' | '1d'
      range1H: '1M',         // '1W' | '2W' | '1M' | '3M' | '1Y' | 'ALL'
      range1D: 'ALL',        // '1M' | '3M' | '6M' | '1Y' | '2Y' | '5Y' | 'ALL'
      customRange: null,     // { startIdx, endIdx }
      historyCache: {},      // Per-ticker deep LOD history cache
      activeView: 'chart',   // 'chart' | 'screener'
      showPivots: false,     // Default OFF to prevent clutter
      filter: 'all',         // 'all' | 'subzero' | 'bull' | 'imminent' | 'bear' | 'macro_bull'
      searchQuery: '',
      sortKey: 'confluence_score',
      sortAsc: false,
      hoverIndex: null,
      lastMouse: { x: 0, y: 0 },
      isHovering: false,
      isMounted: false
    },

    /** Initialize and mount UI */
    async mount(defaultTicker) {
      let initTicker = defaultTicker;
      if (!initTicker) {
        try {
          const urlParams = new URLSearchParams(window.location.search);
          initTicker = urlParams.get('flow') || urlParams.get('ticker');
        } catch (_) {}
      }
      if (initTicker) {
        this.state.activeTicker = initTicker.toUpperCase();
      }
      await this.loadData();
      if (!this.state.activeTicker && this.state.signalsData && Array.isArray(this.state.signalsData.signals) && this.state.signalsData.signals.length) {
        const featuredSig = this.state.signalsData.signals.find(s => s && s.featured);
        this.state.activeTicker = (featuredSig || this.state.signalsData.signals[0]).ticker || '';
      }
      this.loadHistory(this.state.activeTicker);
      this.bindEvents();
      this.render();
      this.state.isMounted = true;
    },

    /** Fetch pre-computed 160-instrument dataset */
    async loadData(force = false) {
      if (this.state.signalsData && !force) return;
      try {
        const fetchWithFallback = async (primary, fallback) => {
          try {
            const res = await fetch(primary);
            if (res && res.ok) return res;
          } catch (_) {}
          try {
            const res = await fetch(fallback);
            if (res && res.ok) return res;
          } catch (_) {}
          return null;
        };

        const sigRes = await fetchWithFallback('data/macd/signals.json', 'data/flows/macd_signals.json');
        if (sigRes && sigRes.ok) {
          this.state.signalsData = await sigRes.json();
          if (this.state.signalsData && Array.isArray(this.state.signalsData.signals)) {
            for (const sig of this.state.signalsData.signals) {
              if (sig && CANONICAL_OVERRIDES[sig.ticker]) {
                Object.assign(sig, CANONICAL_OVERRIDES[sig.ticker]);
              }
            }
          }
        }
      } catch (err) {
        console.warn('[MACDStrategy] Error loading signals data:', err);
      }
    },

    /** Asynchronously fetch and cache Tier 1 LOD deep history for ticker */
    async loadHistory(ticker) {
      if (!ticker) return;
      const t = ticker.toUpperCase();
      if (!this.state.historyCache) this.state.historyCache = {};
      if (this.state.historyCache[t]) {
        if (this.state.activeTicker === t) {
          this.drawChart();
          this.updateHUD(null);
        }
        return;
      }

      try {
        const fetchWithFallback = async (primary, fallback) => {
          try {
            const res = await fetch(primary);
            if (res && res.ok) return res;
          } catch (_) {}
          try {
            const res = await fetch(fallback);
            if (res && res.ok) return res;
          } catch (_) {}
          return null;
        };

        const res = await fetchWithFallback(
          `data/macd/history/${t}.json`,
          `data/flows/history/${t}.json`
        );
        if (res && res.ok) {
          const hist = await res.json();
          this.state.historyCache[t] = hist;
          if (this.state.activeTicker === t) {
            this.drawChart();
            this.updateHUD(null);
          }
        }
      } catch (err) {
        console.warn(`[MACDStrategy] Deep history unavailable for ${t}:`, err);
      }
    },

    /** Strip leading numeric indices and internal maintenance tags like '(Pruned to Key Anchors Only)' */
    cleanCategory(cat) {
      if (!cat) return '';
      return String(cat)
        .replace(/^\d+\.\s*/, '')
        .replace(/\s*\([^)]*Pruned[^)]*\)/gi, '')
        .trim();
    },

    /** Get active instrument data */
    getActiveSignal() {
      if (!this.state.signalsData || !this.state.signalsData.signals) return null;
      const t = this.state.activeTicker;
      return this.state.signalsData.signals.find(s => s.ticker === t) || this.state.signalsData.signals[0] || null;
    },

    /** Get all available bars for active timeframe */
    getAllBarsForActiveTimeframe() {
      const t = this.state.activeTicker;
      const hist = this.state.historyCache && this.state.historyCache[t];
      if (this.state.activeTimeframe === '1h') {
        return (hist && hist.bars_1h && hist.bars_1h.length)
          ? hist.bars_1h
          : ((this.getActiveSignal() && this.getActiveSignal().chart_bars_1h) || []);
      }
      return (hist && hist.bars_1d && hist.bars_1d.length)
        ? hist.bars_1d
        : ((this.getActiveSignal() && this.getActiveSignal().chart_bars_1d) || []);
    },

    /** Get bars for active timeframe & selected range */
    getActiveBars() {
      const allBars = this.getAllBarsForActiveTimeframe();
      if (!allBars || !allBars.length) return [];

      if (this.state.customRange) {
        const s = Math.max(0, Math.min(allBars.length - 1, this.state.customRange.startIdx));
        const e = Math.max(s, Math.min(allBars.length - 1, this.state.customRange.endIdx));
        return allBars.slice(s, e + 1);
      }

      let count = allBars.length;
      if (this.state.activeTimeframe === '1h') {
        const r = this.state.range1H || '1M';
        if (r === '1W') count = 35;
        else if (r === '2W') count = 70;
        else if (r === '1M') count = 140;
        else if (r === '3M') count = 420;
        else if (r === '1Y') count = 1750;
        else count = allBars.length; // ALL (2Y / ~5,000 bars)
      } else {
        const r = this.state.range1D || 'ALL';
        if (r === '1M') count = 22;
        else if (r === '3M') count = 65;
        else if (r === '6M') count = 130;
        else if (r === '1Y') count = 252;
        else if (r === '2Y') count = 504;
        else if (r === '5Y') count = 1260;
        else count = allBars.length; // ALL (up to 5Y / Inception)
      }

      const startIdx = Math.max(0, allBars.length - count);
      return allBars.slice(startIdx);
    },

    /** Set active ETF instrument */
    setTicker(ticker) {
      if (!ticker) return;
      this.state.activeTicker = ticker.toUpperCase();
      this.state.customRange = null;
      this.loadHistory(this.state.activeTicker);
      this.state.hoverIndex = null;
      try {
        const url = new URL(window.location.href);
        if (url.searchParams.get('flow') !== this.state.activeTicker) {
          url.searchParams.set('flow', this.state.activeTicker);
          window.history.replaceState({}, '', url.toString());
        }
      } catch (_) {}
      this.render();
      this.updateHUD(null);
    },

    /** Set active timeframe (1H vs 1D) */
    setTimeframe(tf) {
      if (tf !== '1h' && tf !== '1d') return;
      this.state.activeTimeframe = tf;
      this.state.customRange = null;
      this.state.hoverIndex = null;
      this.updateControlsUI();
      this.updateHUD(null);
      this.drawChart();
    },

    /** Set 1H historical range filter (1W, 2W, 1M, 3M, 1Y, ALL) */
    setRange1H(range) {
      if (!range) return;
      this.state.range1H = range;
      this.state.customRange = null;
      const buttons = document.querySelectorAll('.macd-range-btn-1h');
      buttons.forEach(b => {
        if (b.dataset.range === range) b.classList.add('active');
        else b.classList.remove('active');
      });
      this.state.hoverIndex = null;
      this.drawChart();
      this.updateHUD(null);
    },

    /** Set 1D historical range filter (1M, 3M, 6M, 1Y, 2Y, 5Y, ALL) */
    setRange(range) {
      if (!range) return;
      this.state.range1D = range;
      this.state.customRange = null;
      const buttons = document.querySelectorAll('.macd-range-btn');
      buttons.forEach(b => {
        if (b.dataset.range === range) b.classList.add('active');
        else b.classList.remove('active');
      });
      this.state.hoverIndex = null;
      this.drawChart();
      this.updateHUD(null);
    },

    /** Toggle between Chart View and Screener View */
    setView(view) {
      this.state.activeView = view;
      const chartContainer = document.getElementById('macd-chart-view-panel');
      const screenerContainer = document.getElementById('macd-screener-view-panel');
      const btnChart = document.getElementById('macd-nav-chart');
      const btnScreener = document.getElementById('macd-nav-screener');

      if (chartContainer && screenerContainer) {
        if (view === 'chart') {
          chartContainer.style.display = 'flex';
          screenerContainer.style.display = 'none';
          if (btnChart) btnChart.classList.add('active');
          if (btnScreener) btnScreener.classList.remove('active');
          this.drawChart();
          requestAnimationFrame(() => this.drawChart());
        } else {
          chartContainer.style.display = 'none';
          screenerContainer.style.display = 'flex';
          if (btnChart) btnChart.classList.remove('active');
          if (btnScreener) btnScreener.classList.add('active');
          this.renderScreenerTable();
        }
      }
    },

    /** Toggle floor pivot visibility (P, R1, S1 only) */
    togglePivots() {
      this.state.showPivots = !this.state.showPivots;
      const btn = document.getElementById('macd-toggle-pivots');
      if (btn) {
        if (this.state.showPivots) btn.classList.add('active');
        else btn.classList.remove('active');
      }
      this.drawChart();
    },

    /** Set Screener Filter */
    setFilter(filterKey) {
      this.state.filter = filterKey;
      const buttons = document.querySelectorAll('.macd-filter-btn');
      buttons.forEach(b => {
        if (b.dataset.filter === filterKey) b.classList.add('active');
        else b.classList.remove('active');
      });
      this.renderScreenerTable();
    },

    /** Search filter */
    handleSearch(query) {
      this.state.searchQuery = (query || '').trim().toLowerCase();
      this.renderScreenerTable();
    },

    /** Table column sorter */
    sortBy(key) {
      if (this.state.sortKey === key) {
        this.state.sortAsc = !this.state.sortAsc;
      } else {
        this.state.sortKey = key;
        this.state.sortAsc = false;
      }
      this.renderScreenerTable();
    },

    /** Bind DOM events */
    bindEvents() {
      if (!this._windowEventsBound) {
        this._windowEventsBound = true;
        window.addEventListener('resize', () => {
          if (this.state.activeView === 'chart') {
            this.drawChart();
          }
        });
      }

      if (window.ResizeObserver && !this._resizeObserver) {
        const container = document.getElementById('macd-canvas-container');
        if (container) {
          this._resizeObserver = new ResizeObserver((entries) => {
            for (const entry of entries) {
              if (entry.contentRect && entry.contentRect.width > 0) {
                this.drawChart();
              }
            }
          });
          this._resizeObserver.observe(container);
        }
      }

      const canvas = document.getElementById('macd-canvas');
      if (canvas && !this._canvasEventsBound) {
        this._canvasEventsBound = true;
        canvas.addEventListener('mousemove', (e) => this.handleMouseMove(e));
        canvas.addEventListener('mouseleave', () => this.handleMouseLeave());
        canvas.addEventListener('touchstart', (e) => this.handleTouch(e), { passive: false });
        canvas.addEventListener('touchmove', (e) => this.handleTouch(e), { passive: false });
        canvas.addEventListener('touchend', () => this.handleMouseLeave());
      }

      this._initSlider();
    },

    /** Initialize dual-handle interactive range scrubber */
    _initSlider() {
      if (this._sliderBound) return;
      const sStart = document.getElementById('macd-range-start');
      const sEnd = document.getElementById('macd-range-end');
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
        const allBars = this.getAllBarsForActiveTimeframe();
        if (!allBars || allBars.length < 2) return;
        const maxVal = allBars.length - 1;

        let v1 = parseInt(sStart.value, 10);
        let v2 = parseInt(sEnd.value, 10);

        if (e.target.id === 'macd-range-start') {
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

        this.state.customRange = { startIdx: v1, endIdx: v2 };
        this.state.hoverIndex = null;

        // Deselect preset range buttons to indicate custom scrubber window
        document.querySelectorAll('.macd-range-btn, .macd-range-btn-1h').forEach(b => b.classList.remove('active'));

        this.drawChart();
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

    /** Synchronize dual-handle slider thumbs, highlight track, and date label */
    _syncSlider(totalBars, startIdx, endIdx, activeBars) {
      const sStart = document.getElementById('macd-range-start');
      const sEnd = document.getElementById('macd-range-end');
      const highlight = document.getElementById('macd-brush-highlight');
      const datesLabel = document.getElementById('macd-window-dates');

      if (!sStart || !sEnd || totalBars < 2) return;

      const maxVal = totalBars - 1;
      sStart.max = maxVal;
      sEnd.max = maxVal;

      if (typeof document !== 'undefined') {
        if (document.activeElement !== sStart && Number(sStart.value) !== startIdx) {
          sStart.value = startIdx;
        }
        if (document.activeElement !== sEnd && Number(sEnd.value) !== endIdx) {
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

      if (datesLabel && activeBars && activeBars.length) {
        const first = activeBars[0].dt;
        const last = activeBars[activeBars.length - 1].dt;
        const tfLabel = this.state.activeTimeframe === '1h' ? 'hours' : 'days';
        datesLabel.textContent = `${first} → ${last} (${activeBars.length} ${tfLabel})`;
      }
    },

    /** Mouse movement over Canvas */
    handleMouseMove(e) {
      const canvas = document.getElementById('macd-canvas');
      if (!canvas) return;
      const rect = canvas.getBoundingClientRect();
      const x = e.clientX - rect.left;
      const y = e.clientY - rect.top;

      this.state.lastMouse = { x, y };
      this.state.isHovering = true;
      this.drawChart();
    },

    /** Touch movement over Canvas for Mobile Devices */
    handleTouch(e) {
      if (!e.touches || e.touches.length === 0) return;
      e.preventDefault();
      const touch = e.touches[0];
      const canvas = document.getElementById('macd-canvas');
      if (!canvas) return;
      const rect = canvas.getBoundingClientRect();
      const x = touch.clientX - rect.left;
      const y = touch.clientY - rect.top;

      this.state.lastMouse = { x, y };
      this.state.isHovering = true;
      this.drawChart();
    },

    handleMouseLeave() {
      this.state.isHovering = false;
      this.state.hoverIndex = null;
      const tooltip = document.getElementById('macd-tooltip');
      if (tooltip) tooltip.style.display = 'none';
      this.drawChart();
      this.updateHUD(null);
    },

    /** Update HUD readings */
    updateHUD(bar) {
      const signal = this.getActiveSignal();
      if (!signal) return;

      const allBars = this.getAllBarsForActiveTimeframe();
      const bars = this.getActiveBars();
      const targetBar = bar || (bars && bars[bars.length - 1]);
      if (!targetBar) return;

      let barIdx = -1;
      if (bars && bars.length) {
        barIdx = bar ? bars.indexOf(bar) : bars.length - 1;
        if (barIdx < 0 && this.state.hoverIndex != null && this.state.hoverIndex >= 0 && this.state.hoverIndex < bars.length) {
          barIdx = this.state.hoverIndex;
        }
        if (barIdx < 0 && targetBar.dt) {
          barIdx = bars.findIndex(x => x && x.dt === targetBar.dt);
        }
      }
      const isLatest = !bar || barIdx === bars.length - 1;

      const elPrice = document.getElementById('macd-hud-price');
      const elChg = document.getElementById('macd-hud-chg');
      const elMacd = document.getElementById('macd-hud-macd');
      const elSig = document.getElementById('macd-hud-sig');
      const elHist = document.getElementById('macd-hud-hist');
      const elRvol = document.getElementById('macd-hud-rvol');
      const elPivots = document.getElementById('macd-hud-pivot');
      const elCandle = document.getElementById('macd-hud-candle');
      const elCandleLabel = document.getElementById('macd-hud-candle-label');
      const elDate = document.getElementById('macd-hud-date');

      if (elPrice) elPrice.textContent = `$${Number(targetBar.c || 0).toFixed(2)}`;
      if (elDate) elDate.textContent = targetBar.dt || '—';

      if (elCandleLabel) {
        elCandleLabel.textContent = `${this.state.activeTimeframe.toUpperCase()} Candle`;
      }

      if (elCandle) {
        const isUp = targetBar.c >= targetBar.o;
        elCandle.textContent = isUp ? 'GREEN (Bullish)' : 'RED (Bearish)';
        elCandle.style.color = isUp ? '#10b981' : '#f43f5e';
      }

      if (elChg) {
        let numChg;
        if (isLatest) {
          const rawChg = this.state.activeTimeframe === '1h' ? signal.chg_1h_pct : signal.chg_1d_pct;
          numChg = Number(rawChg || 0);
        } else {
          const prevRef = (barIdx > 0 && bars[barIdx - 1]) ? Number(bars[barIdx - 1].c) : Number(targetBar.o);
          numChg = prevRef > 0 ? ((Number(targetBar.c) - prevRef) / prevRef) * 100 : 0;
        }
        elChg.textContent = `${numChg >= 0 ? '+' : ''}${numChg.toFixed(2)}%`;
        elChg.style.color = numChg >= 0 ? '#10b981' : '#f43f5e';
      }

      if (elMacd) {
        elMacd.textContent = Number(targetBar.macd || 0).toFixed(4);
        elMacd.style.color = '#38bdf8';
      }
      if (elSig) {
        elSig.textContent = Number(targetBar.sig || 0).toFixed(4);
        elSig.style.color = '#f59e0b';
      }
      if (elHist) {
        const h = Number(targetBar.hist || 0);
        elHist.textContent = `${h >= 0 ? '+' : ''}${h.toFixed(4)}`;
        elHist.style.color = h >= 0 ? '#10b981' : '#f43f5e';
      }
      if (elRvol) {
        let rv = null;
        if (isLatest && this.state.activeTimeframe === '1h') {
          rv = Number(signal.rvol_1h || 1.0);
        } else if (allBars && allBars.length) {
          let allIdx = allBars.indexOf(targetBar);
          if (allIdx < 0 && targetBar.dt) allIdx = allBars.findIndex(x => x && x.dt === targetBar.dt);
          if (allIdx < 0 && barIdx >= 0) allIdx = Math.max(0, allBars.length - bars.length + barIdx);
          const curVol = Number(targetBar.v || 0);
          if (allIdx >= 5 && curVol > 0) {
            const priorVols = [];
            for (let j = Math.max(0, allIdx - 20); j < allIdx; j++) {
              const v = Number(allBars[j] && allBars[j].v);
              if (Number.isFinite(v) && v > 0) priorVols.push(v);
            }
            if (priorVols.length >= 5) {
              const meanVol = priorVols.reduce((a, b) => a + b, 0) / priorVols.length;
              if (meanVol > 0) rv = curVol / meanVol;
            }
          }
        }
        if (rv != null && Number.isFinite(rv)) {
          elRvol.textContent = `${rv.toFixed(2)}x`;
          elRvol.style.color = rv >= 1.5 ? '#22d3ee' : '#9ca3af';
        } else {
          elRvol.textContent = '—';
          elRvol.style.color = '#9ca3af';
        }
      }
      if (elPivots && signal.pivots) {
        const p = targetBar.c;
        // Key structural pivots: P, R1, S1
        const keyPivots = {
          'P': signal.pivots.P,
          'R1': signal.pivots.R1,
          'S1': signal.pivots.S1
        };
        let closestKey = 'P';
        let minDist = Infinity;
        for (const [k, v] of Object.entries(keyPivots)) {
          if (!v) continue;
          const dist = Math.abs(p - v);
          if (dist < minDist) {
            minDist = dist;
            closestKey = k;
          }
        }
        const refVal = keyPivots[closestKey];
        const pctDist = refVal ? ((p - refVal) / refVal * 100).toFixed(1) : '0.0';
        elPivots.textContent = `${closestKey} ($${refVal}) · ${Number(pctDist) >= 0 ? '+' : ''}${pctDist}%`;
      }
    },

    /** Render main UI components */
    render() {
      const signal = this.getActiveSignal();
      if (!signal) return;

      // Update Header context strip
      const elSymbol = document.getElementById('macd-active-symbol');
      const elFundName = document.getElementById('macd-active-name');
      const elLeverage = document.getElementById('macd-active-leverage');
      const elCategory = document.getElementById('macd-active-category');
      const elPrice = document.getElementById('macd-active-price');
      const elChg = document.getElementById('macd-active-chg');

      if (elSymbol) elSymbol.textContent = signal.ticker;
      if (elFundName) elFundName.textContent = signal.fund_name;
      if (elLeverage) {
        elLeverage.textContent = signal.leverage;
        elLeverage.dataset.state = String(signal.leverage || '').includes('-') ? 'DISTRIBUTION' : 'ACCUMULATION';
      }
      if (elCategory) {
        const cat = this.cleanCategory(signal.category);
        elCategory.textContent = `${cat} · Underlying: ${signal.underlying}`;
      }
      if (elPrice) {
        if (signal.live_premarket_price) {
          const pmChg = signal.live_premarket_chg_pct != null ? signal.live_premarket_chg_pct : 0;
          const pmColor = pmChg >= 0 ? '#10b981' : '#f43f5e';
          elPrice.innerHTML = `$${Number(signal.latest_price).toFixed(2)} <span style="font-size:0.75rem;color:${pmColor};font-weight:700;margin-left:6px;">Pre: $${Number(signal.live_premarket_price).toFixed(2)} (${pmChg >= 0 ? '+' : ''}${pmChg.toFixed(2)}%)</span>`;
        } else {
          elPrice.textContent = `$${Number(signal.latest_price).toFixed(2)}`;
        }
      }
      if (elChg) {
        const c1h = Number(signal.chg_1h_pct || 0);
        const c1d = Number(signal.chg_1d_pct || 0);
        elChg.innerHTML = `1H: <span style="color:${c1h >= 0 ? '#10b981' : '#f43f5e'}">${c1h >= 0 ? '+' : ''}${c1h.toFixed(2)}%</span> · 24H: <span style="color:${c1d >= 0 ? '#10b981' : '#f43f5e'}">${c1d >= 0 ? '+' : ''}${c1d.toFixed(2)}%</span>`;
      }

      // Update Dynamic Regime Header Badge
      const elRegimeBadge = document.getElementById('macd-active-regime-badge');
      if (elRegimeBadge) {
        if (signal.signal_state === 'SUB_ZERO_BOUNCE' || (signal.bull_cross_fresh && signal.is_subzero)) {
          elRegimeBadge.textContent = 'SUB-ZERO REVERSAL (CONFIRMED GREEN)';
          elRegimeBadge.className = 'flow-badge macd-badge-subzero';
        } else if (signal.signal_state === 'UNCONFIRMED_CROSS') {
          elRegimeBadge.textContent = 'UNCONFIRMED CROSS (AWAITING GREEN CANDLE)';
          elRegimeBadge.className = 'flow-badge macd-badge-unconfirmed';
        } else if (signal.signal_state === 'BULLISH_CROSS' || signal.bull_cross_fresh) {
          elRegimeBadge.textContent = 'BULLISH MOMENTUM CROSS (CONFIRMED GREEN)';
          elRegimeBadge.className = 'flow-badge macd-badge-bull';
        } else if (signal.signal_state === 'BEARISH_CROSS' || signal.bear_cross_fresh) {
          elRegimeBadge.textContent = 'BEARISH EXIT TO CASH (CONFIRMED RED)';
          elRegimeBadge.className = 'flow-badge macd-badge-bear';
        } else if (signal.signal_state === 'UNCONFIRMED_BEAR_CROSS') {
          elRegimeBadge.textContent = 'UNCONFIRMED BEAR CROSS (GREEN CANDLE)';
          elRegimeBadge.className = 'flow-badge macd-badge-neutral';
        } else if (signal.signal_state === 'IMMINENT_SQUEEZE') {
          elRegimeBadge.textContent = 'PRE-CLOSE SQUEEZE WATCH';
          elRegimeBadge.className = 'flow-badge macd-badge-imminent';
        } else if (signal.signal_direction === 'BEARISH') {
          elRegimeBadge.textContent = 'BEARISH MOMENTUM ACTIVE';
          elRegimeBadge.className = 'flow-badge macd-badge-bear';
        } else {
          elRegimeBadge.textContent = signal.signal_label || 'MOMENTUM REGIME ACTIVE';
          elRegimeBadge.className = 'flow-badge macd-badge-neutral';
        }
      }

      // Update Ticker Selector Dropdown
      const select = document.getElementById('macd-ticker-select');
      if (select) {
        if (select.options.length <= 1 && this.state.signalsData?.signals) {
          select.innerHTML = this.state.signalsData.signals.map(s =>
            `<option value="${_escapeHtml(s.ticker)}">${_escapeHtml(s.ticker)} — ${_escapeHtml(s.fund_name)} (${_escapeHtml(s.leverage)})</option>`
          ).join('');
        }
        select.value = signal.ticker;
      }

      // Populate Dynamic Flagship Pills
      this.renderFlagshipPills();

      // Update quick pill highlights
      document.querySelectorAll('.macd-ticker-pill').forEach(btn => {
        if (btn.dataset.ticker === signal.ticker) btn.classList.add('active');
        else btn.classList.remove('active');
      });

      this.updateControlsUI();

      if (this.state.activeView === 'chart') {
        this.drawChart();
        requestAnimationFrame(() => this.drawChart());
      } else {
        this.renderScreenerTable();
      }
    },

    /** Render Dynamic Flagship Instrument Pills (Top Momentum, Biggest Movers & RVOL) */
    renderFlagshipPills() {
      const container = document.getElementById('macd-flagship-pills');
      if (!container) return;
      const signals = this.state.signalsData?.signals;
      if (!signals || signals.length === 0) return;

      const pillsMap = new Map();

      // 1. Top 2 Bullish Reversals (highest confluence score)
      const topReversals = [...signals]
        .filter(s => (s.confluence_score || 0) > 0 || s.signal_direction === 'BULLISH' || s.bull_cross_fresh)
        .sort((a, b) => (b.confluence_score || 0) - (a.confluence_score || 0))
        .slice(0, 2);

      topReversals.forEach(s => {
        pillsMap.set(s.ticker, {
          ticker: s.ticker,
          badge: `Rev ${s.confluence_score > 0 ? '+' : ''}${s.confluence_score}`,
          metricClass: 'pill-reversal',
          title: `Momentum Reversal: Score ${s.confluence_score}`
        });
      });

      // 2. Top 2 Gainers (highest 24H return)
      const topGainers = [...signals]
        .sort((a, b) => (b.chg_1d_pct || 0) - (a.chg_1d_pct || 0))
        .slice(0, 2);

      topGainers.forEach(s => {
        if (!pillsMap.has(s.ticker)) {
          const chg = Number(s.chg_1d_pct || 0);
          pillsMap.set(s.ticker, {
            ticker: s.ticker,
            badge: `${chg >= 0 ? '+' : ''}${chg.toFixed(1)}%`,
            metricClass: chg >= 0 ? 'pill-pos' : 'pill-neg',
            title: `Top 24H Gainer: ${chg.toFixed(2)}%`
          });
        }
      });

      // 3. Top 2 Volume Surges (highest RVOL)
      const topRvol = [...signals]
        .sort((a, b) => (b.rvol_1h || 0) - (a.rvol_1h || 0))
        .slice(0, 2);

      topRvol.forEach(s => {
        if (!pillsMap.has(s.ticker)) {
          const rv = Number(s.rvol_1h || 1.0);
          pillsMap.set(s.ticker, {
            ticker: s.ticker,
            badge: `${rv.toFixed(1)}x Vol`,
            metricClass: 'pill-vol',
            title: `Relative Volume Surge: ${rv.toFixed(2)}x`
          });
        }
      });

      // 4. Dynamic Benchmark Anchors
      const coreAnchors = signals
        .filter(s => s && (s.featured || (s.aum_m && s.aum_m >= 1000)))
        .slice(0, 3)
        .map(s => s.ticker);
      coreAnchors.forEach(t => {
        if (!pillsMap.has(t)) {
          const s = signals.find(item => item.ticker === t);
          if (s) {
            const chg = Number(s.chg_1d_pct || 0);
            pillsMap.set(t, {
              ticker: t,
              badge: `${chg >= 0 ? '+' : ''}${chg.toFixed(1)}%`,
              metricClass: chg >= 0 ? 'pill-pos' : 'pill-neg',
              title: `Core Benchmark: ${s.fund_name}`
            });
          }
        }
      });

      const pillsList = Array.from(pillsMap.values());
      let html = '<span class="flow-eyebrow macd-flagship-label">Leaders:</span>';
      pillsList.forEach(item => {
        const isActive = this.state.activeTicker === item.ticker;
        const safeTk = _escapeHtml(item.ticker);
        html += `<button type="button" class="macd-ticker-pill ${isActive ? 'active' : ''}" data-ticker="${safeTk}" onclick="window.MACDStrategy.setTicker(this.dataset.ticker)" title="${_escapeHtml(item.title)}">
          <span class="macd-pill-ticker">${safeTk}</span>
          <span class="macd-pill-badge ${item.metricClass}">${_escapeHtml(item.badge)}</span>
        </button>`;
      });

      container.innerHTML = html;
    },

    /** Update button states and controls */
    updateControlsUI() {
      const btn1h = document.getElementById('macd-tf-1h');
      const btn1d = document.getElementById('macd-tf-1d');
      if (btn1h && btn1d) {
        if (this.state.activeTimeframe === '1h') {
          btn1h.classList.add('active');
          btn1d.classList.remove('active');
        } else {
          btn1h.classList.remove('active');
          btn1d.classList.add('active');
        }
      }

      // Toggle Range Selector visibility (1H vs 1D)
      const rangeGroup1H = document.getElementById('macd-range-group-1h');
      const rangeGroup1D = document.getElementById('macd-range-group');
      if (rangeGroup1H) {
        rangeGroup1H.style.display = this.state.activeTimeframe === '1h' ? 'flex' : 'none';
      }
      if (rangeGroup1D) {
        rangeGroup1D.style.display = this.state.activeTimeframe === '1d' ? 'flex' : 'none';
      }

      // Update active 1H range button
      if (this.state.activeTimeframe === '1h') {
        const buttons = document.querySelectorAll('.macd-range-btn-1h');
        buttons.forEach(b => {
          if (b.dataset.range === this.state.range1H) b.classList.add('active');
          else b.classList.remove('active');
        });
      }

      // Update active 1D range button
      if (this.state.activeTimeframe === '1d') {
        const buttons = document.querySelectorAll('.macd-range-btn');
        buttons.forEach(b => {
          if (b.dataset.range === this.state.range1D) b.classList.add('active');
          else b.classList.remove('active');
        });
      }

      // Update Pivot button active state
      const pivotBtn = document.getElementById('macd-toggle-pivots');
      if (pivotBtn) {
        if (this.state.showPivots) pivotBtn.classList.add('active');
        else pivotBtn.classList.remove('active');
      }
    },

    /** Draw Dual-Pane Canvas Chart */
    drawChart() {
      const canvas = document.getElementById('macd-canvas');
      if (!canvas) return;
      const ctx = canvas.getContext('2d');
      if (!ctx) return;

      const signal = this.getActiveSignal();
      if (!signal) return;

      const bars = this.getActiveBars();

      if (!bars || bars.length < 2) {
        ctx.fillStyle = '#090a0f';
        ctx.fillRect(0, 0, canvas.width, canvas.height);
        ctx.fillStyle = '#9ca3af';
        ctx.font = '12px ui-monospace, "SF Mono", Menlo, monospace';
        const msg = (this.state.historyCache && this.state.historyCache[this.state.activeTicker])
          ? 'Insufficient bar history for selected timeframe / range'
          : `Loading deep historical candlestick data for ${this.state.activeTicker}...`;
        ctx.fillText(msg, 24, 40);
        return;
      }

      // Synchronize dual-handle range scrubber with active window
      const allBars = this.getAllBarsForActiveTimeframe();
      if (allBars && allBars.length >= 2) {
        let sIdx, eIdx;
        if (this.state.customRange) {
          sIdx = Math.max(0, Math.min(allBars.length - 1, this.state.customRange.startIdx));
          eIdx = Math.max(sIdx, Math.min(allBars.length - 1, this.state.customRange.endIdx));
        } else {
          sIdx = Math.max(0, allBars.length - bars.length);
          eIdx = allBars.length - 1;
        }
        this._syncSlider(allBars.length, sIdx, eIdx, bars);
      }

      // Setup High-DPI
      const dpr = window.devicePixelRatio || 1;
      const rect = canvas.getBoundingClientRect();
      if (!rect || rect.width <= 0 || rect.height <= 0) {
        if (!this._retryRaf) {
          this._retryRaf = requestAnimationFrame(() => {
            this._retryRaf = null;
            this.drawChart();
          });
        }
        return;
      }
      const w = rect.width;
      const h = rect.height;

      if (canvas.width !== Math.floor(w * dpr) || canvas.height !== Math.floor(h * dpr)) {
        canvas.width = Math.floor(w * dpr);
        canvas.height = Math.floor(h * dpr);
      }

      ctx.save();
      ctx.scale(dpr, dpr);

      // Background
      ctx.fillStyle = '#090a0f';
      ctx.fillRect(0, 0, w, h);

      // Layout geometry
      const paddingLeft = 12;
      const paddingRight = 72; // Clean dedicated right Y-axis margin
      const paddingTop = 28;
      const paddingBottom = 24;
      const splitGap = 22;

      const availableHeight = h - paddingTop - paddingBottom - splitGap;
      const topPaneHeight = Math.floor(availableHeight * 0.62);
      const bottomPaneHeight = availableHeight - topPaneHeight;

      const topY0 = paddingTop;
      const topY1 = topY0 + topPaneHeight;
      const bottomY0 = topY1 + splitGap;
      const bottomY1 = bottomY0 + bottomPaneHeight;

      const chartW = w - paddingLeft - paddingRight;

      // ── TOP PANE: CANDLESTICKS + PIVOTS ──
      let minP = Math.min(...bars.map(b => b.l));
      let maxP = Math.max(...bars.map(b => b.h));

      // Include structural pivots (P, R1, S1) in bounds only if enabled
      if (this.state.showPivots && signal.pivots) {
        const keyPivots = [signal.pivots.P, signal.pivots.R1, signal.pivots.S1].filter(v => v > 0);
        const nearPivots = keyPivots.filter(v => v >= minP * 0.85 && v <= maxP * 1.15);
        if (nearPivots.length > 0) {
          minP = Math.min(minP, ...nearPivots);
          maxP = Math.max(maxP, ...nearPivots);
        }
      }

      // Pad bounds
      const pRange = (maxP - minP) || 1;
      minP -= pRange * 0.05;
      maxP += pRange * 0.05;

      const getYTop = (val) => {
        return topY1 - ((val - minP) / (maxP - minP)) * topPaneHeight;
      };

      // Draw Top Pane Grid & Clean Right Y-Axis Price Ticks
      ctx.strokeStyle = 'rgba(255, 255, 255, 0.04)';
      ctx.lineWidth = 1;

      // Compute number of ticks with minTickSpacing of ~36px to prevent collisions
      const yTicks = Math.max(3, Math.min(6, Math.floor(topPaneHeight / 36)));
      ctx.font = '10px ui-monospace, "SF Mono", Menlo, monospace';
      ctx.fillStyle = '#71717a';
      ctx.textAlign = 'left';

      for (let i = 0; i <= yTicks; i++) {
        const val = minP + (i / yTicks) * (maxP - minP);
        const y = Math.round(getYTop(val)) + 0.5;
        ctx.beginPath();
        ctx.moveTo(paddingLeft, y);
        ctx.lineTo(w - paddingRight, y);
        ctx.stroke();

        // Right Y-axis price label: clean, round, never collides with pivots
        ctx.fillText(`$${val.toFixed(2)}`, w - paddingRight + 8, y + 3.5);
      }

      // Draw Structural Floor Pivots (P, R1, S1 only)
      // Labels are drawn on the LEFT side of canvas to avoid Y-axis collisions
      if (this.state.showPivots && signal.pivots) {
        const piv = signal.pivots;
        const pivotDefs = [
          { key: 'R1', val: piv.R1, color: '#34d399', dash: [4, 4] },
          { key: 'P',  val: piv.P,  color: '#f59e0b', dash: [] },
          { key: 'S1', val: piv.S1, color: '#fb7185', dash: [4, 4] },
        ];

        pivotDefs.forEach(pDef => {
          if (pDef.val && pDef.val >= minP && pDef.val <= maxP) {
            const py = Math.round(getYTop(pDef.val)) + 0.5;
            ctx.save();
            ctx.strokeStyle = pDef.color;
            ctx.setLineDash(pDef.dash);
            ctx.lineWidth = pDef.key === 'P' ? 1.5 : 1;
            ctx.beginPath();
            ctx.moveTo(paddingLeft, py);
            ctx.lineTo(w - paddingRight, py);
            ctx.stroke();

            // Left-aligned subtle badge inside plot area (no collision with right price ticks)
            ctx.setLineDash([]);
            const labelText = `${pDef.key} $${pDef.val.toFixed(2)}`;
            ctx.font = '9px ui-monospace, "SF Mono", Menlo, monospace';
            const badgeW = ctx.measureText(labelText).width + 8;
            const badgeH = 13;
            ctx.fillStyle = 'rgba(9, 10, 15, 0.88)';
            ctx.fillRect(paddingLeft + 6, py - 7, badgeW, badgeH);
            ctx.strokeStyle = pDef.color;
            ctx.lineWidth = 0.75;
            ctx.strokeRect(paddingLeft + 6, py - 7, badgeW, badgeH);

            ctx.fillStyle = pDef.color;
            ctx.textAlign = 'left';
            ctx.fillText(labelText, paddingLeft + 10, py + 3);
            ctx.restore();
          }
        });
      }

      // Draw Candlesticks with dynamic sizing
      const nBars = bars.length;
      const barSpacing = chartW / nBars;
      const candleWidth = Math.max(1, Math.min(18, Math.floor(barSpacing * 0.72)));

      let hoverBar = null;
      let hoverX = 0;

      for (let i = 0; i < nBars; i++) {
        const b = bars[i];
        const cx = paddingLeft + i * barSpacing + barSpacing / 2;
        const isUp = b.c >= b.o;
        const candleColor = isUp ? '#10b981' : '#f43f5e';

        const yOpen = getYTop(b.o);
        const yClose = getYTop(b.c);
        const yHigh = getYTop(b.h);
        const yLow = getYTop(b.l);

        // Wick
        ctx.strokeStyle = candleColor;
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(Math.round(cx) + 0.5, Math.round(yHigh));
        ctx.lineTo(Math.round(cx) + 0.5, Math.round(yLow));
        ctx.stroke();

        // Body
        ctx.fillStyle = candleColor;
        const bodyTop = Math.min(yOpen, yClose);
        const bodyHeight = Math.max(1.5, Math.abs(yClose - yOpen));
        ctx.fillRect(Math.round(cx - candleWidth / 2), Math.round(bodyTop), candleWidth, Math.round(bodyHeight));

        // Directional Crossover Markers on candles (only on 1H interval or when spacing allows)
        if (i >= 1 && barSpacing >= 6) {
          const prev = bars[i - 1];
          const isBullCross = (b.macd > b.sig) && (prev.macd <= prev.sig);
          const isBearCross = (b.macd < b.sig) && (prev.macd >= prev.sig);

          if (isBullCross) {
            const my = yLow + 8;
            if (isUp) {
              // Confirmed Green Candle Crossover
              ctx.fillStyle = b.macd < 0 ? '#22d3ee' : '#10b981';
              ctx.beginPath();
              ctx.moveTo(cx, my);
              ctx.lineTo(cx - 4, my + 7);
              ctx.lineTo(cx + 4, my + 7);
              ctx.closePath();
              ctx.fill();

              if (b.macd < 0) {
                // Sub-Zero halo ring
                ctx.strokeStyle = 'rgba(34, 211, 238, 0.45)';
                ctx.lineWidth = 1;
                ctx.beginPath();
                ctx.arc(cx, my + 3.5, 6, 0, Math.PI * 2);
                ctx.stroke();
              }
            } else {
              // Unconfirmed: Bullish cross with Red Candle
              ctx.strokeStyle = '#f59e0b';
              ctx.lineWidth = 1.25;
              ctx.beginPath();
              ctx.moveTo(cx, my);
              ctx.lineTo(cx - 4, my + 7);
              ctx.lineTo(cx + 4, my + 7);
              ctx.closePath();
              ctx.stroke();
            }
          } else if (isBearCross) {
            const my = yHigh - 8;
            if (!isUp) {
              // Confirmed Red Candle Crossover
              ctx.fillStyle = '#f43f5e';
              ctx.beginPath();
              ctx.moveTo(cx, my);
              ctx.lineTo(cx - 4, my - 7);
              ctx.lineTo(cx + 4, my - 7);
              ctx.closePath();
              ctx.fill();
            } else {
              // Unconfirmed: Bearish cross with Green Candle
              ctx.strokeStyle = '#f59e0b';
              ctx.lineWidth = 1.25;
              ctx.beginPath();
              ctx.moveTo(cx, my);
              ctx.lineTo(cx - 4, my - 7);
              ctx.lineTo(cx + 4, my - 7);
              ctx.closePath();
              ctx.stroke();
            }
          }
        }
      }

      if (this.state.isHovering && nBars > 0 && this.state.lastMouse.x >= paddingLeft && this.state.lastMouse.x <= w - paddingRight) {
        const hIdx = Math.max(0, Math.min(nBars - 1, Math.floor((this.state.lastMouse.x - paddingLeft) / barSpacing)));
        this.state.hoverIndex = hIdx;
        hoverBar = bars[hIdx];
        hoverX = paddingLeft + hIdx * barSpacing + barSpacing / 2;
      } else {
        this.state.hoverIndex = null;
      }

      // Pane Top Header label
      ctx.fillStyle = '#9ca3af';
      ctx.font = '11px -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif';
      ctx.textAlign = 'left';
      const rangeTag = this.state.activeTimeframe === '1d' ? ` · ${this.state.range1D}` : '';
      ctx.fillText(`${signal.ticker} · ${this.state.activeTimeframe.toUpperCase()}${rangeTag} Price Candles${this.state.showPivots ? ' & Structural Pivots' : ''}`, paddingLeft + 4, topY0 - 9);

      // ── BOTTOM PANE: MACD (12, 26, 9) + 4-COLOR HISTOGRAM ──
      let minM = Math.min(...bars.map(b => Math.min(b.macd, b.sig, b.hist)));
      let maxM = Math.max(...bars.map(b => Math.max(b.macd, b.sig, b.hist)));
      const mRange = (maxM - minM) || 0.1;
      minM -= mRange * 0.1;
      maxM += mRange * 0.1;

      // Ensure zero line is comfortably visible
      if (minM > 0) minM = -mRange * 0.1;
      if (maxM < 0) maxM = mRange * 0.1;

      const getYBottom = (val) => {
        return bottomY1 - ((val - minM) / (maxM - minM)) * bottomPaneHeight;
      };

      const yZero = getYBottom(0);

      // Draw Bottom Pane Grid & Zero line
      ctx.strokeStyle = 'rgba(255, 255, 255, 0.04)';
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(paddingLeft, Math.round(bottomY0) + 0.5);
      ctx.lineTo(w - paddingRight, Math.round(bottomY0) + 0.5);
      ctx.moveTo(paddingLeft, Math.round(bottomY1) + 0.5);
      ctx.lineTo(w - paddingRight, Math.round(bottomY1) + 0.5);
      ctx.stroke();

      // Zero Axis
      ctx.strokeStyle = '#475569';
      ctx.lineWidth = 1;
      ctx.setLineDash([3, 3]);
      ctx.beginPath();
      ctx.moveTo(paddingLeft, Math.round(yZero) + 0.5);
      ctx.lineTo(w - paddingRight, Math.round(yZero) + 0.5);
      ctx.stroke();
      ctx.setLineDash([]);

      ctx.fillStyle = '#64748b';
      ctx.font = '10px ui-monospace, "SF Mono", Menlo, monospace';
      ctx.fillText('0.00', w - paddingRight + 8, Math.round(yZero) + 3.5);

      // Draw 4-Color Histogram Bars
      for (let i = 0; i < nBars; i++) {
        const b = bars[i];
        const cx = paddingLeft + i * barSpacing + barSpacing / 2;
        const hVal = b.hist || 0;
        const prevH = i > 0 ? (bars[i - 1].hist || 0) : hVal;
        const yH = getYBottom(hVal);

        let hColor;
        if (hVal >= 0) {
          hColor = hVal >= prevH ? '#10b981' : '#34d399';
        } else {
          hColor = hVal <= prevH ? '#f43f5e' : '#fb7185';
        }

        ctx.fillStyle = hColor;
        const top = Math.min(yZero, yH);
        const barH = Math.max(1, Math.abs(yH - yZero));
        ctx.fillRect(Math.round(cx - candleWidth / 2), Math.round(top), candleWidth, Math.round(barH));
      }

      // Draw MACD Line (#38bdf8 sky blue)
      ctx.strokeStyle = '#38bdf8';
      ctx.lineWidth = 1.75;
      ctx.beginPath();
      for (let i = 0; i < nBars; i++) {
        const cx = paddingLeft + i * barSpacing + barSpacing / 2;
        const my = getYBottom(bars[i].macd);
        if (i === 0) ctx.moveTo(cx, my);
        else ctx.lineTo(cx, my);
      }
      ctx.stroke();

      // Draw Signal Line (#f59e0b gold)
      ctx.strokeStyle = '#f59e0b';
      ctx.lineWidth = 1.5;
      ctx.beginPath();
      for (let i = 0; i < nBars; i++) {
        const cx = paddingLeft + i * barSpacing + barSpacing / 2;
        const sy = getYBottom(bars[i].sig);
        if (i === 0) ctx.moveTo(cx, sy);
        else ctx.lineTo(cx, sy);
      }
      ctx.stroke();

      // Bottom Pane Header label
      ctx.fillStyle = '#9ca3af';
      ctx.font = '11px -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif';
      ctx.textAlign = 'left';
      const bottomLabel = w < 640
        ? 'MACD (12, 26, 9) · Momentum Histogram'
        : 'MACD (12, 26, close, 9 EMA) · Sky: MACD | Amber: Signal | 4-Color Histogram';
      ctx.fillText(bottomLabel, paddingLeft + 4, bottomY0 - 7);

      // Draw Date/Time X-Axis Ticks (clamped within plot bounds)
      ctx.fillStyle = '#64748b';
      ctx.font = '9px ui-monospace, "SF Mono", Menlo, monospace';
      ctx.textAlign = 'center';
      const tickStep = Math.max(1, Math.floor(nBars / 6));
      for (let i = 0; i < nBars; i += tickStep) {
        const b = bars[i];
        const cx = paddingLeft + i * barSpacing + barSpacing / 2;
        const rawDt = b.dt || '';
        let dtLabel = rawDt;
        if (this.state.activeTimeframe === '1d') {
          // If viewing multi-year, display YYYY-MM
          dtLabel = nBars > 300 ? rawDt.slice(0, 7) : rawDt;
        } else {
          // 1H view: if >= 1000 bars show YYYY-MM-DD, else MM-DD HH:MM
          dtLabel = nBars > 1000 ? rawDt.slice(0, 10) : (rawDt.length > 10 ? rawDt.slice(5) : rawDt);
        }
        const halfTickW = ctx.measureText(dtLabel).width / 2;
        const clampedX = Math.max(paddingLeft + halfTickW, Math.min(w - paddingRight - halfTickW, cx));
        ctx.fillText(dtLabel, clampedX, bottomY1 + 15);
      }

      // ── SUBTLE HOVER & TOOLTIP (NO CROSSHAIRS) ──
      if (this.state.isHovering && hoverBar && hoverX > 0) {
        // Subtle vertical column highlight over active bar (without crosshair hairlines)
        ctx.fillStyle = 'rgba(255, 255, 255, 0.035)';
        ctx.fillRect(Math.round(hoverX - barSpacing / 2), topY0, Math.max(1, Math.round(barSpacing)), bottomY1 - topY0);

        // Active Date Tag on X-Axis (clamped within plot bounds)
        const hDt = hoverBar.dt || '';
        const hDtLabel = hDt.length > 10 ? hDt.slice(5) : hDt;
        ctx.font = '9px ui-monospace, "SF Mono", Menlo, monospace';
        const hDtW = ctx.measureText(hDtLabel).width + 8;
        const badgeCenterX = Math.max(paddingLeft + hDtW / 2, Math.min(w - paddingRight - hDtW / 2, hoverX));
        ctx.fillStyle = '#181b24';
        ctx.fillRect(badgeCenterX - hDtW / 2, bottomY1 + 5, hDtW, 14);
        ctx.strokeStyle = 'rgba(255, 255, 255, 0.2)';
        ctx.lineWidth = 1;
        ctx.strokeRect(badgeCenterX - hDtW / 2, bottomY1 + 5, hDtW, 14);
        ctx.fillStyle = '#f3f4f6';
        ctx.textAlign = 'center';
        ctx.fillText(hDtLabel, badgeCenterX, bottomY1 + 15);

        // Update Floating Tooltip & HUD
        this.renderTooltip(hoverBar, hoverX, rect.top);
        this.updateHUD(hoverBar);
      } else {
        const tooltip = document.getElementById('macd-tooltip');
        if (tooltip && tooltip.style.display !== 'none') {
          tooltip.style.display = 'none';
          this.updateHUD(null);
        }
      }

      ctx.restore();
    },

    /** Render Floating Tooltip */
    renderTooltip(bar, hoverX, rectTop) {
      const tooltip = document.getElementById('macd-tooltip');
      if (!tooltip) return;

      const isSubZero = bar.macd < 0;
      const spread = (bar.macd || 0) - (bar.sig || 0);
      const isUp = bar.c >= bar.o;
      const candleLabel = isUp ? '<strong style="color:#10b981">GREEN (Bullish)</strong>' : '<strong style="color:#f43f5e">RED (Bearish)</strong>';

      tooltip.innerHTML = `
        <div style="font-weight:700;color:#22d3ee;margin-bottom:6px;border-bottom:1px solid rgba(255,255,255,0.08);padding-bottom:4px;display:flex;justify-content:space-between">
          <span>${_escapeHtml(bar.dt || 'Bar Data')}</span>
          <span style="color:#a1a1aa;font-weight:500;font-size:0.65rem">${this.state.activeTimeframe.toUpperCase()} Interval</span>
        </div>
        <div style="display:grid;grid-template-columns:auto auto;gap:4px 14px;font-size:0.72rem;font-variant-numeric:tabular-nums">
          <span style="color:#9ca3af">Price:</span> <strong style="color:#ffffff">$${Number(bar.c || 0).toFixed(2)}</strong>
          <span style="color:#9ca3af">O / H / L:</span> <span>$${Number(bar.o || 0).toFixed(2)} / $${Number(bar.h || 0).toFixed(2)} / $${Number(bar.l || 0).toFixed(2)}</span>
          <span style="color:#9ca3af">Candle:</span> <span>${candleLabel}</span>
          ${bar.v ? `<span style="color:#9ca3af">Volume:</span> <span>${Number(bar.v || 0).toLocaleString()}</span>` : ''}
          <span style="color:#38bdf8">MACD (12,26):</span> <strong style="color:#38bdf8">${Number(bar.macd || 0).toFixed(4)}</strong>
          <span style="color:#f59e0b">Signal (9):</span> <strong style="color:#f59e0b">${Number(bar.sig || 0).toFixed(4)}</strong>
          <span style="color:#34d399">Histogram:</span> <strong style="color:${(bar.hist || 0) >= 0 ? '#10b981' : '#f43f5e'}">${(bar.hist || 0) >= 0 ? '+' : ''}${Number(bar.hist || 0).toFixed(4)}</strong>
          <span style="color:#9ca3af">Spread:</span> <span>${spread >= 0 ? '+' : ''}${Number(spread).toFixed(4)}</span>
          <span style="color:#9ca3af">Regime:</span> <span style="color:${isSubZero ? '#34d399' : '#38bdf8'}">${isSubZero ? 'Sub-Zero (Convex)' : 'Supra-Zero'}</span>
        </div>
      `;

      tooltip.style.display = 'block';
      const container = document.getElementById('macd-canvas-container');
      const containerW = container ? container.clientWidth : 800;
      const tooltipW = tooltip.offsetWidth || 230;

      let left = hoverX + 16;
      if (left + tooltipW > containerW - 14) {
        left = hoverX - tooltipW - 16;
      }
      if (left < 10) left = 10;
      tooltip.style.left = `${Math.round(left)}px`;
      tooltip.style.top = '38px';
    },

    /** Render 160-Instrument Screener Table */
    renderScreenerTable() {
      const tbody = document.getElementById('macd-screener-tbody');
      const countEl = document.getElementById('macd-screener-count');
      if (!tbody || !this.state.signalsData || !this.state.signalsData.signals) return;

      let list = [...this.state.signalsData.signals];

      // Filter by category / tier
      const f = this.state.filter;
      if (f === 'subzero') {
        list = list.filter(s => (s.signal_state === 'SUB_ZERO_BOUNCE' || (s.bull_cross_fresh && s.is_subzero)) && (s.candle_color_1h === 'GREEN' || s.is_green_candle_1h));
      } else if (f === 'bull') {
        list = list.filter(s => (s.signal_direction === 'BULLISH' || s.bull_cross_fresh) && s.signal_state !== 'UNCONFIRMED_CROSS');
      } else if (f === 'imminent') {
        list = list.filter(s => s.signal_state === 'IMMINENT_SQUEEZE' || s.signal_state === 'UNCONFIRMED_CROSS');
      } else if (f === 'bear') {
        list = list.filter(s => (s.signal_direction === 'BEARISH' || s.bear_cross_fresh) && s.signal_state !== 'UNCONFIRMED_BEAR_CROSS');
      } else if (f === 'macro_bull') {
        list = list.filter(s => s.daily_trend_bullish);
      }

      // Search query filter
      if (this.state.searchQuery) {
        const q = this.state.searchQuery;
        list = list.filter(s =>
          (s.ticker || '').toLowerCase().includes(q) ||
          (s.fund_name || '').toLowerCase().includes(q) ||
          (s.underlying || '').toLowerCase().includes(q) ||
          (s.leverage || '').toLowerCase().includes(q)
        );
      }

      // Sort
      const key = this.state.sortKey;
      const asc = this.state.sortAsc;
      list.sort((a, b) => {
        let va = a[key] !== undefined ? a[key] : 0;
        let vb = b[key] !== undefined ? b[key] : 0;
        if (key === 'leverage') {
          va = parseFloat(String(va).replace('x', '')) || 0;
          vb = parseFloat(String(vb).replace('x', '')) || 0;
        }
        if (typeof va === 'string' || typeof vb === 'string') {
          return asc ? String(va || '').localeCompare(String(vb || '')) : String(vb || '').localeCompare(String(va || ''));
        }
        return asc ? va - vb : vb - va;
      });

      // Update table header sort indicators
      document.querySelectorAll('.macd-table th[data-sort-col]').forEach(th => {
        const col = th.dataset.sortCol;
        const baseLabel = th.dataset.label || th.textContent.replace(/[\s▲▼↑↓↕].*$/, '').trim();
        th.dataset.label = baseLabel;
        if (col === key) {
          th.classList.add('sorted-col');
          th.innerHTML = `${baseLabel} <span class="macd-sort-arrow">${asc ? '▲' : '▼'}</span>`;
        } else {
          th.classList.remove('sorted-col');
          th.innerHTML = `${baseLabel} <span class="macd-sort-arrow-idle">↕</span>`;
        }
      });

      if (countEl) countEl.textContent = `Showing ${list.length} of ${this.state.signalsData.signals.length} Instruments`;

      // Build HTML rows
      if (list.length === 0) {
        tbody.innerHTML = `<tr><td colspan="14" style="text-align:center;padding:2.5rem 1rem;color:#71717a;font-family:-apple-system,BlinkMacSystemFont,sans-serif">No instruments match the active query or filter criteria.</td></tr>`;
        return;
      }

      let html = '';
      list.forEach(s => {
        const isActive = s.ticker === this.state.activeTicker;
        const candleColor = s.candle_color_1h || (s.chg_1h_pct >= 0 ? 'GREEN' : 'RED');
        const isGreenCandle = candleColor === 'GREEN';
        const candlePill = `<span class="macd-candle-pill ${isGreenCandle ? 'green' : 'red'}">${candleColor}</span>`;

        let badgeHtml = '';
        if (s.signal_state === 'SUB_ZERO_BOUNCE' || (s.bull_cross_fresh && s.is_subzero)) {
          badgeHtml = '<span class="macd-badge macd-badge-subzero">Sub-Zero (Confirmed)</span>';
        } else if (s.signal_state === 'UNCONFIRMED_CROSS') {
          badgeHtml = '<span class="macd-badge macd-badge-unconfirmed">Cross (Unconfirmed)</span>';
        } else if (s.signal_state === 'BULLISH_CROSS' || s.bull_cross_fresh) {
          badgeHtml = '<span class="macd-badge macd-badge-bull">Bull Cross (Confirmed)</span>';
        } else if (s.signal_state === 'BEARISH_CROSS' || s.bear_cross_fresh) {
          badgeHtml = '<span class="macd-badge macd-badge-bear">Bear Exit (Confirmed)</span>';
        } else if (s.signal_state === 'UNCONFIRMED_BEAR_CROSS') {
          badgeHtml = '<span class="macd-badge macd-badge-neutral">Bear Cross (Unconf)</span>';
        } else if (s.signal_state === 'IMMINENT_SQUEEZE') {
          badgeHtml = '<span class="macd-badge macd-badge-imminent">Imminent Squeeze</span>';
        } else if (s.signal_direction === 'BEARISH') {
          badgeHtml = '<span class="macd-badge macd-badge-bear">Bearish Active</span>';
        } else {
          badgeHtml = '<span class="macd-badge macd-badge-neutral">Neutral</span>';
        }

        const score = Number(s.confluence_score || 0);
        const scoreColor = score > 30 ? '#10b981' : (score < -30 ? '#f43f5e' : '#9ca3af');
        const c1h = Number(s.chg_1h_pct || 0);
        const c1d = Number(s.chg_1d_pct || 0);
        const m1h = Number(s.macd_1h || 0);
        const s1h = Number(s.signal_1h || 0);
        const h1h = Number(s.hist_1h || 0);
        const rv1h = Number(s.rvol_1h || 1.0);
        const safeTk = _escapeHtml(s.ticker);
        const safeName = _escapeHtml(s.fund_name);
        const safeLev = _escapeHtml(s.leverage);

        html += `
          <tr class="${isActive ? 'active-row' : ''}" data-ticker="${safeTk}" onclick="window.MACDStrategy.setTicker(this.dataset.ticker); window.MACDStrategy.setView('chart'); const p = document.getElementById('macd-chart-view-panel'); if (p) p.scrollIntoView({behavior:'smooth',block:'nearest'});">
            <td><strong style="color:#22d3ee">${safeTk}</strong></td>
            <td style="color:#f3f4f6;max-width:240px;overflow:hidden;text-overflow:ellipsis">${safeName}</td>
            <td><span class="macd-badge macd-badge-neutral">${safeLev}</span></td>
            <td style="text-align:right">
              $${Number(s.latest_price || 0).toFixed(2)}
              ${s.live_premarket_price ? `<div style="font-size:9.5px;color:${(s.live_premarket_chg_pct || 0) >= 0 ? '#10b981' : '#f43f5e'};font-weight:700">Pre: $${Number(s.live_premarket_price).toFixed(2)} (${(s.live_premarket_chg_pct || 0) >= 0 ? '+' : ''}${Number(s.live_premarket_chg_pct || 0).toFixed(1)}%)</div>` : ''}
            </td>
            <td style="text-align:center">${candlePill}</td>
            <td style="text-align:right;color:${c1h >= 0 ? '#10b981' : '#f43f5e'}">${c1h >= 0 ? '+' : ''}${c1h.toFixed(2)}%</td>
            <td style="text-align:right;color:${c1d >= 0 ? '#10b981' : '#f43f5e'}">${c1d >= 0 ? '+' : ''}${c1d.toFixed(2)}%</td>
            <td style="text-align:right;color:#38bdf8">${m1h.toFixed(3)}</td>
            <td style="text-align:right;color:#f59e0b">${s1h.toFixed(3)}</td>
            <td style="text-align:right;color:${h1h >= 0 ? '#10b981' : '#f43f5e'}">${h1h >= 0 ? '+' : ''}${h1h.toFixed(3)}</td>
            <td style="text-align:right;color:${rv1h >= 1.5 ? '#22d3ee' : '#9ca3af'}">${rv1h.toFixed(2)}x</td>
            <td style="text-align:center">${s.daily_trend_bullish ? '<span style="color:#10b981">UP</span>' : '<span style="color:#f43f5e">DOWN</span>'}</td>
            <td style="text-align:right"><strong style="color:${scoreColor}">${score > 0 ? '+' : ''}${score}</strong></td>
            <td style="text-align:center">${badgeHtml}</td>
          </tr>
        `;
      });

      tbody.innerHTML = html;
    }
  };

  return MACDStrategy;
});
