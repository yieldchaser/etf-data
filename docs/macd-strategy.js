/**
 * MACD Momentum Engine & Tactical Quant Radar — Canvas 2D Engine
 * Standard MACD (12, 26, 9 EMA EMA) on 1-Hour & 1-Day Timeframes
 * Traditional Floor Pivots (P, S1-S5, R1-R5) + Sub-Zero Reversal Confluence
 * Zero external dependencies. Zero AI slop. 100% Institutional Precision.
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
      if (defaultTicker) {
        this.state.activeTicker = defaultTicker.toUpperCase();
      }
      await this.loadData();
      this.bindEvents();
      this.render();
      this.state.isMounted = true;
    },

    /** Fetch pre-computed 160-instrument dataset */
    async loadData() {
      try {
        const [sigRes, alertRes] = await Promise.all([
          fetch('data/flows/macd_signals.json'),
          fetch('data/flows/latest_macd_alert.json').catch(() => null)
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
      this.render();
    },

    /** Set active timeframe */
    setTimeframe(tf) {
      if (tf !== '1h' && tf !== '1d') return;
      this.state.activeTimeframe = tf;
      this.state.hoverIndex = null;
      this.updateControlsUI();
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
      window.addEventListener('resize', () => {
        if (this.state.activeView === 'chart') {
          this.drawChart();
        }
      });

      const canvas = document.getElementById('macd-canvas');
      if (canvas) {
        canvas.addEventListener('mousemove', (e) => this.handleMouseMove(e));
        canvas.addEventListener('mouseleave', () => this.handleMouseLeave());
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
      const elDate = document.getElementById('macd-hud-date');

      if (elPrice) elPrice.textContent = `$${Number(latestBar.c).toFixed(2)}`;
      if (elDate) elDate.textContent = latestBar.dt || '—';

      if (elChg) {
        const chg = this.state.activeTimeframe === '1h' ? signal.chg_1h_pct : signal.chg_1d_pct;
        elChg.textContent = `${chg >= 0 ? '+' : ''}${chg}%`;
        elChg.style.color = chg >= 0 ? '#10b981' : '#f43f5e';
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
        elRvol.textContent = `${signal.rvol_1h || 1.0}x`;
        elRvol.style.color = (signal.rvol_1h >= 1.5) ? '#22d3ee' : '#9ca3af';
      }
      if (elPivots && signal.pivots) {
        // Find closest pivot
        const p = latestBar.c;
        let closestKey = 'P';
        let minDist = 999999;
        for (const [k, v] of Object.entries(signal.pivots)) {
          const dist = Math.abs(p - v);
          if (dist < minDist) {
            minDist = dist;
            closestKey = k;
          }
        }
        const pctDist = ((p - signal.pivots[closestKey]) / signal.pivots[closestKey] * 100).toFixed(1);
        elPivots.textContent = `${closestKey} ($${signal.pivots[closestKey]}) · ${pctDist >= 0 ? '+' : ''}${pctDist}%`;
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
      if (elPrice) elPrice.textContent = `$${Number(signal.latest_price).toFixed(2)}`;
      if (elChg) {
        const c1h = signal.chg_1h_pct || 0;
        const c1d = signal.chg_1d_pct || 0;
        elChg.innerHTML = `1H: <span style="color:${c1h >= 0 ? '#10b981' : '#f43f5e'}">${c1h >= 0 ? '+' : ''}${c1h}%</span> · 24H: <span style="color:${c1d >= 0 ? '#10b981' : '#f43f5e'}">${c1d >= 0 ? '+' : ''}${c1d}%</span>`;
      }

      // Update Ticker Selector Dropdown
      const select = document.getElementById('macd-ticker-select');
      if (select && select.value !== signal.ticker) {
        select.value = signal.ticker;
      }

      // Update quick pill highlights
      document.querySelectorAll('.macd-ticker-pill').forEach(btn => {
        if (btn.dataset.ticker === signal.ticker) btn.classList.add('active');
        else btn.classList.remove('active');
      });

      this.updateControlsUI();

      if (this.state.activeView === 'chart') {
        this.drawChart();
      } else {
        this.renderScreenerTable();
      }

      this.renderAlertPreview();
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
        ctx.font = '14px sans-serif';
        ctx.fillText('Insufficient bar history for selected timeframe', 20, 40);
        return;
      }

      // Setup High-DPI
      const dpr = window.devicePixelRatio || 1;
      const rect = canvas.getBoundingClientRect();
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
      const paddingLeft = 10;
      const paddingRight = 72;
      const paddingTop = 26;
      const paddingBottom = 22;
      const splitGap = 16;

      const availableHeight = h - paddingTop - paddingBottom - splitGap;
      const topPaneHeight = Math.floor(availableHeight * 0.62);
      const bottomPaneHeight = availableHeight - topPaneHeight;

      const topY0 = paddingTop;
      const topY1 = topY0 + topPaneHeight;
      const bottomY0 = topY1 + splitGap;
      const bottomY1 = bottomY0 + bottomPaneHeight;

      const chartW = w - paddingLeft - paddingRight;

      // ── TOP PANE: CANDLESTICKS + PIVOTS ──
      // Calculate Price Y-Bounds
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
      ctx.font = '10px ui-monospace, "SF Mono", monospace';
      ctx.fillStyle = '#6b7280';
      ctx.textAlign = 'left';

      for (let i = 0; i <= yTicks; i++) {
        const val = minP + (i / yTicks) * (maxP - minP);
        const y = getYTop(val);
        ctx.beginPath();
        ctx.moveTo(paddingLeft, y);
        ctx.lineTo(w - paddingRight, y);
        ctx.stroke();
        ctx.fillText(`$${val.toFixed(2)}`, w - paddingRight + 6, y + 3);
      }

      // Draw Floor Pivots (P, R1-R5, S1-S5)
      if (this.state.showPivots && signal.pivots) {
        const piv = signal.pivots;
        const pivotDefs = [
          { key: 'R5', val: piv.R5, color: 'rgba(52, 211, 153, 0.65)', dash: [4, 4] },
          { key: 'R4', val: piv.R4, color: 'rgba(52, 211, 153, 0.7)', dash: [4, 4] },
          { key: 'R3', val: piv.R3, color: 'rgba(52, 211, 153, 0.75)', dash: [4, 4] },
          { key: 'R2', val: piv.R2, color: 'rgba(52, 211, 153, 0.85)', dash: [4, 4] },
          { key: 'R1', val: piv.R1, color: 'rgba(52, 211, 153, 0.95)', dash: [4, 4] },
          { key: 'P',  val: piv.P,  color: '#f59e0b', dash: [] },
          { key: 'S1', val: piv.S1, color: 'rgba(251, 113, 133, 0.95)', dash: [4, 4] },
          { key: 'S2', val: piv.S2, color: 'rgba(251, 113, 133, 0.85)', dash: [4, 4] },
          { key: 'S3', val: piv.S3, color: 'rgba(251, 113, 133, 0.75)', dash: [4, 4] },
          { key: 'S4', val: piv.S4, color: 'rgba(251, 113, 133, 0.7)', dash: [4, 4] },
          { key: 'S5', val: piv.S5, color: 'rgba(251, 113, 133, 0.65)', dash: [4, 4] },
        ];

        pivotDefs.forEach(pDef => {
          if (pDef.val && pDef.val >= minP && pDef.val <= maxP) {
            const py = getYTop(pDef.val);
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
            ctx.font = '9px ui-monospace, "SF Mono", monospace';
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
        ctx.moveTo(cx, yHigh);
        ctx.lineTo(cx, yLow);
        ctx.stroke();

        // Body
        ctx.fillStyle = candleColor;
        const bodyTop = Math.min(yOpen, yClose);
        const bodyHeight = Math.max(1.5, Math.abs(yClose - yOpen));
        ctx.fillRect(cx - candleWidth / 2, bodyTop, candleWidth, bodyHeight);

        // Check if mouse hovers this bar
        if (this.state.isHovering && Math.abs(this.state.lastMouse.x - cx) <= barSpacing / 2) {
          hoverBar = b;
          hoverX = cx;
          this.state.hoverIndex = i;
        }

        // Crossover Markers on candles
        if (i >= 1) {
          const prev = bars[i - 1];
          const isBullCross = (b.macd > b.sig) && (prev.macd <= prev.sig);
          const isBearCross = (b.macd < b.sig) && (prev.macd >= prev.sig);

          if (isBullCross) {
            // Upward triangle under low
            const my = yLow + 8;
            ctx.fillStyle = b.macd < 0 ? '#22d3ee' : '#10b981';
            ctx.beginPath();
            ctx.moveTo(cx, my);
            ctx.lineTo(cx - 5, my + 8);
            ctx.lineTo(cx + 5, my + 8);
            ctx.closePath();
            ctx.fill();

            if (b.macd < 0) {
              // Sub-Zero halo
              ctx.strokeStyle = 'rgba(34, 211, 238, 0.4)';
              ctx.beginPath();
              ctx.arc(cx, my + 4, 8, 0, Math.PI * 2);
              ctx.stroke();
            }
          } else if (isBearCross) {
            // Downward triangle above high
            const my = yHigh - 8;
            ctx.fillStyle = '#f43f5e';
            ctx.beginPath();
            ctx.moveTo(cx, my);
            ctx.lineTo(cx - 5, my - 8);
            ctx.lineTo(cx + 5, my - 8);
            ctx.closePath();
            ctx.fill();
          }
        }
      }

      // Pane Top Header label
      ctx.fillStyle = '#9ca3af';
      ctx.font = '11px -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif';
      ctx.textAlign = 'left';
      ctx.fillText(`${signal.ticker} · ${this.state.activeTimeframe.toUpperCase()} Price Candles & Traditional Floor Pivots`, paddingLeft + 4, topY0 - 8);

      // ── BOTTOM PANE: MACD (12, 26, 9) + 4-COLOR HISTOGRAM ──
      // Calculate MACD Y-Bounds
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
      ctx.moveTo(paddingLeft, bottomY0);
      ctx.lineTo(w - paddingRight, bottomY0);
      ctx.moveTo(paddingLeft, bottomY1);
      ctx.lineTo(w - paddingRight, bottomY1);
      ctx.stroke();

      // Zero Axis
      ctx.strokeStyle = '#475569';
      ctx.lineWidth = 1;
      ctx.setLineDash([3, 3]);
      ctx.beginPath();
      ctx.moveTo(paddingLeft, yZero);
      ctx.lineTo(w - paddingRight, yZero);
      ctx.stroke();
      ctx.setLineDash([]);

      ctx.fillStyle = '#64748b';
      ctx.font = '10px ui-monospace, "SF Mono", monospace';
      ctx.fillText('0.00', w - paddingRight + 6, yZero + 3);

      // Draw 4-Color Histogram Bars
      for (let i = 0; i < nBars; i++) {
        const b = bars[i];
        const cx = paddingLeft + i * barSpacing + barSpacing / 2;
        const hVal = b.hist || 0;
        const prevH = i > 0 ? (bars[i - 1].hist || 0) : hVal;
        const yH = getYBottom(hVal);

        let hColor;
        if (hVal >= 0) {
          hColor = hVal >= prevH ? '#10b981' : '#34d399'; // Expanding green vs contracting mint
        } else {
          hColor = hVal <= prevH ? '#f43f5e' : '#fb7185'; // Expanding rose vs contracting light rose
        }

        ctx.fillStyle = hColor;
        const top = Math.min(yZero, yH);
        const barH = Math.max(1, Math.abs(yH - yZero));
        ctx.fillRect(cx - candleWidth / 2, top, candleWidth, barH);
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
      ctx.fillText('MACD (12, 26, close, 9 EMA EMA) · Sky: MACD | Amber: Signal | 4-Color Momentum Histogram', paddingLeft + 4, bottomY0 - 5);

      // ── CROSSHAIR & TOOLTIP ──
      if (this.state.isHovering && hoverBar && hoverX > 0) {
        // Vertical crosshair
        ctx.strokeStyle = 'rgba(255, 255, 255, 0.25)';
        ctx.lineWidth = 1;
        ctx.setLineDash([2, 2]);
        ctx.beginPath();
        ctx.moveTo(hoverX, topY0);
        ctx.lineTo(hoverX, bottomY1);
        ctx.stroke();
        ctx.setLineDash([]);

        // Horizontal crosshair in pane under mouse
        const my = this.state.lastMouse.y;
        if (my >= topY0 && my <= topY1) {
          ctx.beginPath();
          ctx.moveTo(paddingLeft, my);
          ctx.lineTo(w - paddingRight, my);
          ctx.stroke();
        } else if (my >= bottomY0 && my <= bottomY1) {
          ctx.beginPath();
          ctx.moveTo(paddingLeft, my);
          ctx.lineTo(w - paddingRight, my);
          ctx.stroke();
        }

        // Update Tooltip DOM
        this.renderTooltip(hoverBar, hoverX, rect.top);
        this.updateHUD(hoverBar);
      }

      ctx.restore();
    },

    /** Render Floating Tooltip */
    renderTooltip(bar, clientX, rectTop) {
      const tooltip = document.getElementById('macd-tooltip');
      if (!tooltip) return;

      const isSubZero = bar.macd < 0;
      const spread = bar.macd - bar.sig;

      tooltip.innerHTML = `
        <div style="font-weight:700;color:#22d3ee;margin-bottom:4px;border-bottom:1px solid rgba(255,255,255,0.1);padding-bottom:2px">
          ${bar.dt}
        </div>
        <div style="display:grid;grid-template-columns:auto auto;gap:4px 12px;font-size:0.7rem">
          <span style="color:#9ca3af">Price:</span> <strong style="color:#ffffff">$${bar.c.toFixed(2)}</strong>
          <span style="color:#9ca3af">O/H/L:</span> <span>$${bar.o.toFixed(2)} / $${bar.h.toFixed(2)} / $${bar.l.toFixed(2)}</span>
          <span style="color:#9ca3af">Volume:</span> <span>${bar.v.toLocaleString()}</span>
          <span style="color:#38bdf8">MACD (12,26):</span> <strong style="color:#38bdf8">${bar.macd.toFixed(4)}</strong>
          <span style="color:#f59e0b">Signal (9):</span> <strong style="color:#f59e0b">${bar.sig.toFixed(4)}</strong>
          <span style="color:#34d399">Histogram:</span> <strong style="color:${bar.hist >= 0 ? '#10b981' : '#f43f5e'}">${bar.hist >= 0 ? '+' : ''}${bar.hist.toFixed(4)}</strong>
          <span style="color:#9ca3af">Spread:</span> <span>${spread >= 0 ? '+' : ''}${spread.toFixed(4)}</span>
          <span style="color:#9ca3af">Regime:</span> <span style="color:${isSubZero ? '#34d399' : '#38bdf8'}">${isSubZero ? 'Sub-Zero (Convex)' : 'Supra-Zero'}</span>
        </div>
      `;

      tooltip.style.display = 'block';
      const tooltipW = tooltip.offsetWidth || 200;
      let left = clientX + 12;
      if (left + tooltipW > window.innerWidth - 60) {
        left = clientX - tooltipW - 12;
      }
      tooltip.style.left = `${left}px`;
      tooltip.style.top = '40px';
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
        list = list.filter(s => s.signal_state === 'SUB_ZERO_BOUNCE' || (s.bull_cross_fresh && s.is_subzero));
      } else if (f === 'bull') {
        list = list.filter(s => s.signal_direction === 'BULLISH' || s.bull_cross_fresh);
      } else if (f === 'imminent') {
        list = list.filter(s => s.signal_state === 'IMMINENT_SQUEEZE');
      } else if (f === 'bear') {
        list = list.filter(s => s.signal_direction === 'BEARISH' || s.bear_cross_fresh);
      } else if (f === 'macro_bull') {
        list = list.filter(s => s.daily_trend_bullish);
      }

      // Search query filter
      if (this.state.searchQuery) {
        const q = this.state.searchQuery;
        list = list.filter(s =>
          s.ticker.toLowerCase().includes(q) ||
          s.fund_name.toLowerCase().includes(q) ||
          s.underlying.toLowerCase().includes(q)
        );
      }

      // Sort
      const key = this.state.sortKey;
      const asc = this.state.sortAsc;
      list.sort((a, b) => {
        let va = a[key] !== undefined ? a[key] : 0;
        let vb = b[key] !== undefined ? b[key] : 0;
        if (typeof va === 'string') {
          return asc ? va.localeCompare(vb) : vb.localeCompare(va);
        }
        return asc ? va - vb : vb - va;
      });

      if (countEl) countEl.textContent = `Showing ${list.length} of ${this.state.signalsData.signals.length} Instruments`;

      // Build HTML rows
      let html = '';
      list.forEach(s => {
        const isActive = s.ticker === this.state.activeTicker;
        let badgeHtml = '';
        if (s.signal_state === 'SUB_ZERO_BOUNCE' || (s.bull_cross_fresh && s.is_subzero)) {
          badgeHtml = '<span class="macd-badge macd-badge-subzero">Sub-Zero Cross</span>';
        } else if (s.signal_state === 'BULLISH_CROSS' || s.bull_cross_fresh) {
          badgeHtml = '<span class="macd-badge macd-badge-bull">Bull Cross</span>';
        } else if (s.signal_state === 'IMMINENT_SQUEEZE') {
          badgeHtml = '<span class="macd-badge macd-badge-imminent">Imminent Squeeze</span>';
        } else if (s.signal_direction === 'BEARISH' || s.bear_cross_fresh) {
          badgeHtml = '<span class="macd-badge macd-badge-bear">Bearish Exit</span>';
        } else {
          badgeHtml = '<span class="macd-badge macd-badge-neutral">Neutral</span>';
        }

        const scoreColor = s.confluence_score > 30 ? '#10b981' : (s.confluence_score < -30 ? '#f43f5e' : '#9ca3af');

        html += `
          <tr class="${isActive ? 'active-row' : ''}" onclick="window.MACDStrategy.setTicker('${s.ticker}'); window.MACDStrategy.setView('chart');">
            <td><strong style="color:#22d3ee">${s.ticker}</strong></td>
            <td style="color:#f3f4f6;max-width:240px;overflow:hidden;text-overflow:ellipsis">${s.fund_name}</td>
            <td><span class="macd-badge macd-badge-neutral">${s.leverage}</span></td>
            <td style="text-align:right">$${Number(s.latest_price).toFixed(2)}</td>
            <td style="text-align:right;color:${s.chg_1h_pct >= 0 ? '#10b981' : '#f43f5e'}">${s.chg_1h_pct >= 0 ? '+' : ''}${s.chg_1h_pct}%</td>
            <td style="text-align:right;color:${s.chg_1d_pct >= 0 ? '#10b981' : '#f43f5e'}">${s.chg_1d_pct >= 0 ? '+' : ''}${s.chg_1d_pct}%</td>
            <td style="text-align:right;color:#38bdf8">${s.macd_1h.toFixed(3)}</td>
            <td style="text-align:right;color:#f59e0b">${s.signal_1h.toFixed(3)}</td>
            <td style="text-align:right;color:${s.hist_1h >= 0 ? '#10b981' : '#f43f5e'}">${s.hist_1h >= 0 ? '+' : ''}${s.hist_1h.toFixed(3)}</td>
            <td style="text-align:right;color:${s.rvol_1h >= 1.5 ? '#22d3ee' : '#9ca3af'}">${s.rvol_1h}x</td>
            <td style="text-align:center">${s.daily_trend_bullish ? '<span style="color:#10b981">UP</span>' : '<span style="color:#f43f5e">DOWN</span>'}</td>
            <td style="text-align:right"><strong style="color:${scoreColor}">${s.confluence_score > 0 ? '+' : ''}${s.confluence_score}</strong></td>
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
        container.textContent = this.state.alertData.text_content;
        if (timeEl) timeEl.textContent = `As of ${this.state.alertData.timestamp_utc || 'Latest scan'}`;
      } else {
        container.textContent = 'Hourly scan operational. Monitoring 160 leveraged/inverse ETFs for 15:00 EST Golden Hour triggers and sub-zero reversals.';
      }
    }
  };

  return MACDStrategy;
});
