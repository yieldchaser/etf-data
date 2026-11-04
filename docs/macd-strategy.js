/**
 * MACD Momentum Engine & Tactical Quant Radar — Canvas 2D Engine
 * Standard MACD (12, 26, 9 EMA EMA) on 1-Hour & 1-Day Timeframes
 * Traditional Floor Pivots (P, S1-S5, R1-R5) + Sub-Zero Reversal Confluence
 * Zero external dependencies · Institutional Quantitative Execution Architecture
 */

(function (root, factory) {
  if (typeof module === 'object' && module.exports) {
    module.exports = factory();
  } else {
    root.MACDStrategy = factory();
  }
})(typeof self !== 'undefined' ? self : this, function () {
  'use strict';

  const MACDStrategy = {
    state: {
      signalsData: null,
      alertData: null,
      activeTicker: 'INTW',
      activeTimeframe: '1h', // '1h' | '1d'
      activeView: 'chart',   // 'chart' | 'screener'
      showPivots: true,
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

        const [sigRes, alertRes] = await Promise.all([
          fetchWithFallback('data/macd/signals.json', 'data/flows/macd_signals.json'),
          fetchWithFallback('data/macd/latest_alert.json', 'data/flows/latest_macd_alert.json')
        ]);

        if (sigRes && sigRes.ok) {
          this.state.signalsData = await sigRes.json();
        }
        if (alertRes && alertRes.ok) {
          this.state.alertData = await alertRes.json();
        }
      } catch (err) {
        console.warn('[MACDStrategy] Error loading signals data:', err);
      }
    },

    /** Get active instrument data */
    getActiveSignal() {
      if (!this.state.signalsData || !this.state.signalsData.signals) return null;
      const t = this.state.activeTicker;
      return this.state.signalsData.signals.find(s => s.ticker === t) || this.state.signalsData.signals[0] || null;
    },

    /** Set active ETF instrument */
    setTicker(ticker) {
      if (!ticker) return;
      this.state.activeTicker = ticker.toUpperCase();
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

    /** Set active timeframe */
    setTimeframe(tf) {
      if (tf !== '1h' && tf !== '1d') return;
      this.state.activeTimeframe = tf;
      this.state.hoverIndex = null;
      this.updateControlsUI();
      this.updateHUD(null);
      this.drawChart();
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

    /** Toggle floor pivot visibility */
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

      const latestBar = bar || (this.state.activeTimeframe === '1h'
        ? (signal.chart_bars_1h && signal.chart_bars_1h[signal.chart_bars_1h.length - 1])
        : (signal.chart_bars_1d && signal.chart_bars_1d[signal.chart_bars_1d.length - 1]));

      if (!latestBar) return;

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

      if (elPrice) elPrice.textContent = `$${Number(latestBar.c || 0).toFixed(2)}`;
      if (elDate) elDate.textContent = latestBar.dt || '—';

      if (elCandleLabel) {
        elCandleLabel.textContent = `${this.state.activeTimeframe.toUpperCase()} Candle`;
      }

      if (elCandle) {
        const isUp = latestBar.c >= latestBar.o;
        elCandle.textContent = isUp ? 'GREEN (Bullish)' : 'RED (Bearish)';
        elCandle.style.color = isUp ? '#10b981' : '#f43f5e';
      }

      if (elChg) {
        const rawChg = this.state.activeTimeframe === '1h' ? signal.chg_1h_pct : signal.chg_1d_pct;
        const numChg = Number(rawChg || 0);
        elChg.textContent = `${numChg >= 0 ? '+' : ''}${numChg.toFixed(2)}%`;
        elChg.style.color = numChg >= 0 ? '#10b981' : '#f43f5e';
      }

      if (elMacd) {
        elMacd.textContent = Number(latestBar.macd || 0).toFixed(4);
        elMacd.style.color = '#38bdf8';
      }
      if (elSig) {
        elSig.textContent = Number(latestBar.sig || 0).toFixed(4);
        elSig.style.color = '#f59e0b';
      }
      if (elHist) {
        const h = Number(latestBar.hist || 0);
        elHist.textContent = `${h >= 0 ? '+' : ''}${h.toFixed(4)}`;
        elHist.style.color = h >= 0 ? '#10b981' : '#f43f5e';
      }
      if (elRvol) {
        const rv = Number(signal.rvol_1h || 1.0);
        elRvol.textContent = `${rv.toFixed(2)}x`;
        elRvol.style.color = rv >= 1.5 ? '#22d3ee' : '#9ca3af';
      }
      if (elPivots && signal.pivots) {
        const p = latestBar.c;
        let closestKey = 'P';
        let minDist = Infinity;
        for (const [k, v] of Object.entries(signal.pivots)) {
          const dist = Math.abs(p - v);
          if (dist < minDist) {
            minDist = dist;
            closestKey = k;
          }
        }
        const refVal = signal.pivots[closestKey];
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
        elLeverage.dataset.state = signal.leverage.includes('-') ? 'DISTRIBUTION' : 'ACCUMULATION';
      }
      if (elCategory) {
        elCategory.textContent = `${signal.category} · Underlying: ${signal.underlying}`;
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
            `<option value="${s.ticker}">${s.ticker} — ${s.fund_name} (${s.leverage})</option>`
          ).join('');
        }
        select.value = signal.ticker;
      }

      // Populate Flagship Pills dynamically if not yet rendered
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

      this.renderAlertPreview();
    },

    /** Render Flagship Instrument Pills */
    renderFlagshipPills() {
      const container = document.getElementById('macd-flagship-pills');
      if (!container) return;
      if (container.querySelectorAll('.macd-ticker-pill').length > 0) return;

      const availableTickers = new Set((this.state.signalsData?.signals || []).map(s => s.ticker));
      const preferred = [
        { ticker: 'INTW', label: 'INTW (2x INTC)' },
        { ticker: 'SOXL', label: 'SOXL (3x Semis)' },
        { ticker: 'TQQQ', label: 'TQQQ (3x NDX)' },
        { ticker: 'TSLL', label: 'TSLL (2x TSLA)' },
        { ticker: 'NVDL', label: 'NVDL (2x NVDA)' },
        { ticker: 'CONL', label: 'CONL (2x COIN)' },
        { ticker: 'SOXS', label: 'SOXS (-3x Semis)' },
        { ticker: 'SQQQ', label: 'SQQQ (-3x NDX)' }
      ];
      const flagshipList = preferred.filter(p => availableTickers.has(p.ticker));
      if (flagshipList.length === 0) return;

      let html = '<span class="flow-eyebrow" style="color:var(--flow-cyan,#22d3ee);font-weight:600;margin-right:0.25rem">Flagship:</span>';
      flagshipList.forEach(item => {
        const isActive = this.state.activeTicker === item.ticker;
        html += `<button type="button" class="macd-ticker-pill ${isActive ? 'active' : ''}" data-ticker="${item.ticker}" onclick="window.MACDStrategy.setTicker('${item.ticker}')">${item.label}</button>`;
      });
      container.innerHTML = html;
    },

    /** Update button state */
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
    },

    /** Draw Dual-Pane Canvas Chart */
    drawChart() {
      const canvas = document.getElementById('macd-canvas');
      if (!canvas) return;
      const ctx = canvas.getContext('2d');
      if (!ctx) return;

      const signal = this.getActiveSignal();
      if (!signal) return;

      const bars = this.state.activeTimeframe === '1h'
        ? (signal.chart_bars_1h || [])
        : (signal.chart_bars_1d || []);

      if (!bars || bars.length < 2) {
        ctx.fillStyle = '#090a0f';
        ctx.fillRect(0, 0, canvas.width, canvas.height);
        ctx.fillStyle = '#9ca3af';
        ctx.font = '13px -apple-system, sans-serif';
        ctx.fillText('Insufficient bar history for selected timeframe', 20, 40);
        return;
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
      const paddingRight = 78;
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

      // Include nearby pivots if enabled
      if (this.state.showPivots && signal.pivots) {
        const pVals = Object.values(signal.pivots).filter(v => v > 0);
        const nearPivots = pVals.filter(v => v >= minP * 0.85 && v <= maxP * 1.15);
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

      // Draw Top Pane Grid & Axes
      ctx.strokeStyle = 'rgba(255, 255, 255, 0.05)';
      ctx.lineWidth = 1;
      const yTicks = 5;
      ctx.font = '10px ui-monospace, "SF Mono", Menlo, monospace';
      ctx.fillStyle = '#6b7280';
      ctx.textAlign = 'left';

      for (let i = 0; i <= yTicks; i++) {
        const val = minP + (i / yTicks) * (maxP - minP);
        const y = Math.round(getYTop(val)) + 0.5;
        ctx.beginPath();
        ctx.moveTo(paddingLeft, y);
        ctx.lineTo(w - paddingRight, y);
        ctx.stroke();
        ctx.fillText(`$${val.toFixed(2)}`, w - paddingRight + 6, y + 3.5);
      }

      // Draw Floor Pivots (P, R1-R5, S1-S5)
      if (this.state.showPivots && signal.pivots) {
        const piv = signal.pivots;
        const pivotDefs = [
          { key: 'R5', val: piv.R5, color: 'rgba(52, 211, 153, 0.65)', dash: [4, 4] },
          { key: 'R4', val: piv.R4, color: 'rgba(52, 211, 153, 0.70)', dash: [4, 4] },
          { key: 'R3', val: piv.R3, color: 'rgba(52, 211, 153, 0.75)', dash: [4, 4] },
          { key: 'R2', val: piv.R2, color: 'rgba(52, 211, 153, 0.85)', dash: [4, 4] },
          { key: 'R1', val: piv.R1, color: 'rgba(52, 211, 153, 0.95)', dash: [4, 4] },
          { key: 'P',  val: piv.P,  color: '#f59e0b', dash: [] },
          { key: 'S1', val: piv.S1, color: 'rgba(251, 113, 133, 0.95)', dash: [4, 4] },
          { key: 'S2', val: piv.S2, color: 'rgba(251, 113, 133, 0.85)', dash: [4, 4] },
          { key: 'S3', val: piv.S3, color: 'rgba(251, 113, 133, 0.75)', dash: [4, 4] },
          { key: 'S4', val: piv.S4, color: 'rgba(251, 113, 133, 0.70)', dash: [4, 4] },
          { key: 'S5', val: piv.S5, color: 'rgba(251, 113, 133, 0.65)', dash: [4, 4] },
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

            // Right label
            ctx.fillStyle = pDef.color;
            ctx.font = '9px ui-monospace, "SF Mono", Menlo, monospace';
            ctx.fillText(`${pDef.key}: ${pDef.val.toFixed(2)}`, w - paddingRight + 6, py - 2);
            ctx.restore();
          }
        });
      }

      // Draw Candlesticks
      const nBars = bars.length;
      const barSpacing = chartW / nBars;
      const candleWidth = Math.max(2, Math.floor(barSpacing * 0.7));

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

        // Check if mouse hovers this bar
        if (this.state.isHovering && Math.abs(this.state.lastMouse.x - cx) <= barSpacing / 2) {
          hoverBar = b;
          hoverX = cx;
          this.state.hoverIndex = i;
        }

        // Directional Crossover Markers on candles
        if (i >= 1) {
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
              ctx.lineTo(cx - 5, my + 8);
              ctx.lineTo(cx + 5, my + 8);
              ctx.closePath();
              ctx.fill();

              if (b.macd < 0) {
                // Sub-Zero halo ring
                ctx.strokeStyle = 'rgba(34, 211, 238, 0.45)';
                ctx.lineWidth = 1;
                ctx.beginPath();
                ctx.arc(cx, my + 4, 8, 0, Math.PI * 2);
                ctx.stroke();
              }
            } else {
              // Unconfirmed: Bullish cross with Red Candle
              ctx.strokeStyle = '#f59e0b';
              ctx.lineWidth = 1.25;
              ctx.beginPath();
              ctx.moveTo(cx, my);
              ctx.lineTo(cx - 5, my + 8);
              ctx.lineTo(cx + 5, my + 8);
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
              ctx.lineTo(cx - 5, my - 8);
              ctx.lineTo(cx + 5, my - 8);
              ctx.closePath();
              ctx.fill();
            } else {
              // Unconfirmed: Bearish cross with Green Candle
              ctx.strokeStyle = '#f59e0b';
              ctx.lineWidth = 1.25;
              ctx.beginPath();
              ctx.moveTo(cx, my);
              ctx.lineTo(cx - 5, my - 8);
              ctx.lineTo(cx + 5, my - 8);
              ctx.closePath();
              ctx.stroke();
            }
          }
        }
      }

      // Pane Top Header label
      ctx.fillStyle = '#9ca3af';
      ctx.font = '11px -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif';
      ctx.textAlign = 'left';
      ctx.fillText(`${signal.ticker} · ${this.state.activeTimeframe.toUpperCase()} Price Candles & Floor Pivots`, paddingLeft + 4, topY0 - 9);

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
      ctx.strokeStyle = 'rgba(255, 255, 255, 0.05)';
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
      ctx.fillText('0.00', w - paddingRight + 6, Math.round(yZero) + 3.5);

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

      // Bottom Pane Header label (Responsive formatting)
      ctx.fillStyle = '#9ca3af';
      ctx.font = '11px -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif';
      ctx.textAlign = 'left';
      const bottomLabel = w < 640
        ? 'MACD (12, 26, 9) · Momentum Histogram'
        : 'MACD (12, 26, close, 9 EMA) · Sky: MACD | Amber: Signal | 4-Color Histogram';
      ctx.fillText(bottomLabel, paddingLeft + 4, bottomY0 - 7);

      // Draw Date/Time X-Axis Ticks
      ctx.fillStyle = '#64748b';
      ctx.font = '9px ui-monospace, "SF Mono", Menlo, monospace';
      ctx.textAlign = 'center';
      const tickStep = Math.max(1, Math.floor(nBars / 6));
      for (let i = 0; i < nBars; i += tickStep) {
        const b = bars[i];
        const cx = paddingLeft + i * barSpacing + barSpacing / 2;
        const rawDt = b.dt || '';
        const dtLabel = rawDt.length > 10 ? rawDt.slice(5) : rawDt;
        ctx.fillText(dtLabel, cx, bottomY1 + 15);
      }

      // ── CROSSHAIR & TOOLTIP ──
      if (this.state.isHovering && hoverBar && hoverX > 0) {
        const my = this.state.lastMouse.y;

        // Vertical crosshair line
        ctx.strokeStyle = 'rgba(255, 255, 255, 0.22)';
        ctx.lineWidth = 1;
        ctx.setLineDash([2, 2]);
        ctx.beginPath();
        ctx.moveTo(Math.round(hoverX) + 0.5, topY0);
        ctx.lineTo(Math.round(hoverX) + 0.5, bottomY1);
        ctx.stroke();

        // Horizontal crosshair line in active pane
        if (my >= topY0 && my <= topY1) {
          ctx.beginPath();
          ctx.moveTo(paddingLeft, Math.round(my) + 0.5);
          ctx.lineTo(w - paddingRight, Math.round(my) + 0.5);
          ctx.stroke();

          // Active Price Tag on Right Axis
          const hoverPrice = maxP - ((my - topY0) / topPaneHeight) * (maxP - minP);
          ctx.setLineDash([]);
          ctx.fillStyle = '#1e2230';
          ctx.fillRect(w - paddingRight + 1, my - 8, paddingRight - 4, 16);
          ctx.strokeStyle = '#22d3ee';
          ctx.strokeRect(w - paddingRight + 1, my - 8, paddingRight - 4, 16);
          ctx.fillStyle = '#22d3ee';
          ctx.font = '9.5px ui-monospace, "SF Mono", Menlo, monospace';
          ctx.textAlign = 'left';
          ctx.fillText(`$${hoverPrice.toFixed(2)}`, w - paddingRight + 5, my + 3.5);
        } else if (my >= bottomY0 && my <= bottomY1) {
          ctx.beginPath();
          ctx.moveTo(paddingLeft, Math.round(my) + 0.5);
          ctx.lineTo(w - paddingRight, Math.round(my) + 0.5);
          ctx.stroke();

          // Active MACD Tag on Right Axis
          const hoverMacd = maxM - ((my - bottomY0) / bottomPaneHeight) * (maxM - minM);
          ctx.setLineDash([]);
          ctx.fillStyle = '#1e2230';
          ctx.fillRect(w - paddingRight + 1, my - 8, paddingRight - 4, 16);
          ctx.strokeStyle = '#38bdf8';
          ctx.strokeRect(w - paddingRight + 1, my - 8, paddingRight - 4, 16);
          ctx.fillStyle = '#38bdf8';
          ctx.font = '9.5px ui-monospace, "SF Mono", Menlo, monospace';
          ctx.textAlign = 'left';
          ctx.fillText(hoverMacd.toFixed(3), w - paddingRight + 5, my + 3.5);
        }
        ctx.setLineDash([]);

        // Active Date Tag on X-Axis
        const hDt = hoverBar.dt || '';
        const hDtLabel = hDt.length > 10 ? hDt.slice(5) : hDt;
        ctx.fillStyle = '#1e2230';
        ctx.font = '9px ui-monospace, "SF Mono", Menlo, monospace';
        const hDtW = ctx.measureText(hDtLabel).width + 8;
        ctx.fillRect(hoverX - hDtW / 2, bottomY1 + 5, hDtW, 14);
        ctx.strokeStyle = '#a1a1aa';
        ctx.lineWidth = 1;
        ctx.strokeRect(hoverX - hDtW / 2, bottomY1 + 5, hDtW, 14);
        ctx.fillStyle = '#f3f4f6';
        ctx.textAlign = 'center';
        ctx.fillText(hDtLabel, hoverX, bottomY1 + 15);

        // Update Tooltip DOM & HUD
        this.renderTooltip(hoverBar, hoverX, rect.top);
        this.updateHUD(hoverBar);
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
          <span>${bar.dt || 'Bar Data'}</span>
          <span style="color:#a1a1aa;font-weight:500;font-size:0.65rem">${this.state.activeTimeframe.toUpperCase()} Interval</span>
        </div>
        <div style="display:grid;grid-template-columns:auto auto;gap:4px 14px;font-size:0.72rem;font-variant-numeric:tabular-nums">
          <span style="color:#9ca3af">Price:</span> <strong style="color:#ffffff">$${Number(bar.c || 0).toFixed(2)}</strong>
          <span style="color:#9ca3af">O / H / L:</span> <span>$${Number(bar.o || 0).toFixed(2)} / $${Number(bar.h || 0).toFixed(2)} / $${Number(bar.l || 0).toFixed(2)}</span>
          <span style="color:#9ca3af">Candle:</span> <span>${candleLabel}</span>
          <span style="color:#9ca3af">Volume:</span> <span>${Number(bar.v || 0).toLocaleString()}</span>
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

        html += `
          <tr class="${isActive ? 'active-row' : ''}" onclick="window.MACDStrategy.setTicker('${s.ticker}'); window.MACDStrategy.setView('chart'); const p = document.getElementById('macd-chart-view-panel'); if (p) p.scrollIntoView({behavior:'smooth',block:'nearest'});">
            <td><strong style="color:#22d3ee">${s.ticker}</strong></td>
            <td style="color:#f3f4f6;max-width:240px;overflow:hidden;text-overflow:ellipsis">${s.fund_name}</td>
            <td><span class="macd-badge macd-badge-neutral">${s.leverage}</span></td>
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
    },

    /** Render Alert Drawer Preview */
    renderAlertPreview() {
      const container = document.getElementById('macd-alert-text-preview');
      const timeEl = document.getElementById('macd-alert-timestamp');
      if (!container) return;

      if (this.state.alertData && this.state.alertData.text_content) {
        let text = this.state.alertData.text_content;
        text = text.replace(/\ufffd/g, '·');
        container.textContent = text;
        if (timeEl) timeEl.textContent = `As of ${this.state.alertData.timestamp_utc || 'Latest scan'}`;
      } else {
        container.textContent = 'Hourly scan operational. Monitoring 160 leveraged/inverse ETFs for 15:00 EST pre-close rebalance triggers and sub-zero reversals.';
      }
    }
  };

  return MACDStrategy;
});
