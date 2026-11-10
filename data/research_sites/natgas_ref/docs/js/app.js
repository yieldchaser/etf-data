/* ============================================
   Main Application Controller
   ============================================ */

const App = {
    allMetrics: {},
    ngPriceContext: null,
    refreshTimer: null,
    refreshIntervalMs: null, // Interval currently applied to refreshTimer
    isLoading: false,
    lastLiveFetchTime: null,
    lastLiveTimeStr: null,
    liveOverlayState: 'idle', // 'idle' | 'pending' | 'live' | 'failed'
    _hasWarmCache: false,
    _warmCacheTime: null,
    heatOffset: 0,        // Offset for volume heat map pagination
    heatDays: 90,         // Lookback days for volume heat map (90, 126, 180, 252)
    heatMode: 'share',    // Mode for heat map: 'share' or 'dollar'
    heatSide: 'all',      // Side for heat map: 'all', 'long', or 'short'
    vddsMode: 'vol',      // Mode for VDDS panel: 'vol' (S-RVOL-21d) or 'dv' (VDDS ratio)
    heatSpotlightMode: 'off', // Spotlight mode: 'off', 'peaks', or 'valleys'
    heatPeaksMode: false, // Backwards compatibility mirror
    heatPeaksThreshold: 95, // Spotlight threshold percentile (90, 95, or 99)
    heatGlobalPercentiles: {}, // Keyed by {mode, side}
    _initialized: false,
    _eventsBound: false,


    async init() {
        if (this._initialized) {
            console.log('[MONITOR] App already initialized, skipping duplicate init.');
            return;
        }
        this._initialized = true;
        console.log('[MONITOR] Initializing...');
        this.bindEvents();
        if (typeof VolumeProfile !== 'undefined' && typeof VolumeProfile.init === 'function') {
            VolumeProfile.init();
        }
        if (typeof TailRiskDrawer !== 'undefined' && typeof TailRiskDrawer.init === 'function') {
            TailRiskDrawer.init();
        }
        await this.refresh(false);
        console.log('[MONITOR] Dashboard ready.');
    },

    bindEvents() {
        if (this._eventsBound) return;
        this._eventsBound = true;
        document.getElementById('refresh-btn').addEventListener('click', () => this.refresh(true));
        window.addEventListener('resize', () => this.handleResize());

        // Tab visibility wake-up: triggers live refresh if tab returns to focus
        document.addEventListener('visibilitychange', () => {
            if (document.visibilityState === 'visible') {
                const session = this.getMarketSessionInfo();
                const now = Date.now();
                const lastFetchTime = this.lastLiveFetchTime || 0;
                if (session.isMarketActive && (now - lastFetchTime > 60000)) {
                    console.log('[MONITOR] Tab became visible and market is active — triggering refresh');
                    this.refresh(false);
                }
            }
        });
        
        const hp = document.getElementById('heat-prev');
        const hn = document.getElementById('heat-next');
        if (hp) hp.addEventListener('click', () => { this.heatOffset++; this.renderHeatCalendarOnly(); });
        if (hn) hn.addEventListener('click', () => { this.heatOffset = Math.max(0, this.heatOffset - 1); this.renderHeatCalendarOnly(); });

        // Heat Map Side Selector
        const sideGroup = document.getElementById('heat-side-group');
        if (sideGroup) {
            sideGroup.addEventListener('click', (e) => {
                const btn = e.target.closest('button');
                if (!btn) return;
                sideGroup.querySelectorAll('button').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                this.heatSide = btn.getAttribute('data-side');
                this.renderHeatCalendarOnly();
            });
        }

        // Heat Map Days Selector
        const daysGroup = document.getElementById('heat-days-group');
        if (daysGroup) {
            daysGroup.addEventListener('click', (e) => {
                const btn = e.target.closest('button');
                if (!btn) return;
                daysGroup.querySelectorAll('button').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                this.heatDays = parseInt(btn.getAttribute('data-days'), 10) || 90;
                this.heatOffset = 0;
                this.renderHeatCalendarOnly();
            });
        }

        // Heat Map Mode Selector
        const modeGroup = document.getElementById('heat-mode-group');
        if (modeGroup) {
            modeGroup.addEventListener('click', (e) => {
                const btn = e.target.closest('button');
                if (!btn) return;
                modeGroup.querySelectorAll('button').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                this.heatMode = btn.getAttribute('data-mode');
                this.renderHeatCalendarOnly();
            });
        }

        // Spotlight (PEAKS / VALLEYS) Toggles
        const peaksToggle = document.getElementById('heat-peaks-toggle');
        const valleysToggle = document.getElementById('heat-valleys-toggle');
        const peaksThresholdGroup = document.getElementById('heat-peaks-threshold-group');

        const updateSpotlightUI = () => {
            const isPeaks = (this.heatSpotlightMode === 'peaks');
            const isValleys = (this.heatSpotlightMode === 'valleys');
            this.heatPeaksMode = isPeaks;

            if (peaksToggle) peaksToggle.classList.toggle('active', isPeaks);
            if (valleysToggle) valleysToggle.classList.toggle('active', isValleys);

            if (peaksThresholdGroup) {
                peaksThresholdGroup.style.display = (isPeaks || isValleys) ? 'flex' : 'none';
            }
            this.renderHeatCalendarOnly();
        };

        if (peaksToggle) {
            peaksToggle.addEventListener('click', () => {
                this.heatSpotlightMode = (this.heatSpotlightMode === 'peaks') ? 'off' : 'peaks';
                updateSpotlightUI();
            });
        }

        if (valleysToggle) {
            valleysToggle.addEventListener('click', () => {
                this.heatSpotlightMode = (this.heatSpotlightMode === 'valleys') ? 'off' : 'valleys';
                updateSpotlightUI();
            });
        }

        // Spotlight Threshold sub-picker
        if (peaksThresholdGroup) {
            peaksThresholdGroup.addEventListener('click', (e) => {
                const btn = e.target.closest('button');
                if (!btn) return;
                peaksThresholdGroup.querySelectorAll('button').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                this.heatPeaksThreshold = parseInt(btn.getAttribute('data-threshold'));
                this.renderHeatCalendarOnly();
            });
        }


        // VDDS VOL / $ Mode Toggle
        const vddsModeGroup = document.getElementById('vdds-mode-group');
        if (vddsModeGroup) {
            vddsModeGroup.addEventListener('click', (e) => {
                const btn = e.target.closest('button');
                if (!btn) return;
                vddsModeGroup.querySelectorAll('button').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                this.vddsMode = btn.getAttribute('data-vdds-mode');
                // Update subtitle label
                const subtitle = document.getElementById('vdds-subtitle');
                if (subtitle) {
                    subtitle.textContent = this.vddsMode === 'vol'
                        ? '\u00B7 SHARE VOLUME RELATIVE (RVOL-21D)'
                        : '\u00B7 VOLUME-DOLLAR DIVERGENCE';
                }
                Signals.renderVddsBar(this.allMetrics, this.vddsMode);
            });
        }
    },

    async refresh(isManual = false) {
        if (this.isLoading) return;
        this.isLoading = true;
        this.setLoading(true);
        const startTime = Date.now();

        try {
            // Try pre-computed data first (from GitHub Actions pipeline)
            const precomputed = await DataService.fetchPrecomputed();
            const session = this.getMarketSessionInfo();

            if (precomputed && precomputed.etfs) {
                const stamp = precomputed.last_updated || null;
                if (stamp !== this._dataStamp || isManual) {
                    console.log('[MONITOR] Using pre-computed data');
                    this.processPrecomputed(precomputed, isManual);
                    this._dataStamp = stamp;
                } else {
                    console.log('[MONITOR] Pipeline data unchanged — live overlay only');
                }
                // Overlay real-time prices on top of pre-computed signals/metrics.
                // Awaited so refresh button spinner animation stays active while fetching.
                await this._overlayLivePrices(0, isManual);
            } else {
                // Fall back to live Yahoo Finance fetch
                console.log('[MONITOR] Fetching live data from Yahoo Finance...');
                const raw = await DataService.fetchAll();
                this.processLiveData(raw, isManual);
                this.lastLiveFetchTime = Date.now();
                this.updateTimestamp(null, isManual);
            }
        } catch (err) {
            console.error('[MONITOR] Refresh failed:', err);
            this.showError('Failed to fetch data. Will retry...');
        } finally {
            this.isLoading = false;
            const elapsed = Date.now() - startTime;
            if (elapsed < 750) await new Promise(r => setTimeout(r, 750 - elapsed));
            this.setLoading(false);
            this.startAutoRefresh();
        }
    },

    // Fetch regularMarketPrice from Yahoo Finance (via CORS proxy) and overlay
    // onto already-rendered cards. Returns Promise that resolves true or false.
    async _overlayLivePrices(retryCount = 0, isManual = false) {
        const session = this.getMarketSessionInfo();
        const today = session.dateStr;

        if (session.isMarketActive && this.liveOverlayState !== 'live' && !this._hasWarmCache) {
            this.liveOverlayState = 'pending';
            this.updateTimestamp(null, isManual);
        }

        try {
            const liveData = await DataService.fetchQuotesLight();
            if (!liveData || !liveData.etfs) {
                throw new Error('No live quote data returned');
            }

            let updated = false;
            const quotesToCache = {};

            for (const [ticker, liveETF] of Object.entries(liveData.etfs)) {
                if (!liveETF || !this.allMetrics[ticker]) continue;
                const livePrice = liveETF.regularMarketPrice;
                if (livePrice == null) continue;

                this.allMetrics[ticker].current.price = livePrice;
                if (liveETF.regularMarketTime) {
                    this.allMetrics[ticker].current.regularMarketTime = liveETF.regularMarketTime;
                }
                if (liveETF.marketState) {
                    this.allMetrics[ticker].current.marketState = liveETF.marketState;
                }

                // Derive change% from bar series — meta fields are unreliable:
                // when market is closed meta.previousClose == regularMarketPrice
                // (both = last official close) giving 0%, and chartPreviousClose
                // is the base price of the full chart period (years-old price).
                //
                // Yahoo ALWAYS inserts a preliminary bar for today with close = yesterday's
                // close, even when the market is open and no trades have occurred yet.
                // This makes bars[-1] and bars[-2] identical -> 0% change.
                // Solution: always strip today's bar, then compare livePrice (when open)
                // or bars[-1].close (when closed) vs bars[-2] (confirmed previous session).
                //
                // Use per-ETF market state — liveData.marketState is 'open' if ANY
                // exchange is open (e.g. London ETFs open while NYSE is pre-market).
                const marketOpen = liveETF.marketState === 'open';
                // Deduplicate live bars by date — Yahoo sometimes returns two rows for the
                // same calendar day (intraday snapshot + confirmed session bar). Same root
                // cause as processPrecomputed dedup: without this, after stripping today's
                // preliminary bar, bars[-1] and bars[-2] are both yesterday -> 0% change.
                const seenDates = new Map();
                for (const b of liveETF.data || []) { seenDates.set(b.date, b); }
                // Sort ascending by date string after dedup — guards against Yahoo
                // returning bars in descending order, which would make Map.values()
                // return dates old->new (last insert wins = oldest bar per date).
                const allBars = [...seenDates.values()].sort((a, b) => a.date < b.date ? -1 : a.date > b.date ? 1 : 0);
                if (allBars.length >= 2) {
                    let prevClose, currentForChange;

                    const lastBar = allBars[allBars.length - 1];
                    const lastBarIsToday = lastBar.date === today;

                    if (marketOpen) {
                        // Market is open: current is live intraday price.
                        currentForChange = livePrice;
                        // Yahoo creates a preliminary bar for 'today'. If present, the baseline
                        // is the bar before it (yesterday). If absent, the last bar IS yesterday.
                        prevClose = lastBarIsToday
                            ? allBars[allBars.length - 2].close
                            : lastBar.close;
                    } else {
                        // Market is closed (e.g. LSE closed while US is open, or after hours / weekend).
                        // If livePrice differs from lastBar.close, livePrice is a newer session close not yet in allBars.
                        const liveMatchesLastBar = lastBar.close != null && Math.abs(livePrice - lastBar.close) / Math.max(lastBar.close, 1e-6) < 1e-3;
                        if (liveMatchesLastBar) {
                            currentForChange = livePrice;
                            prevClose = allBars[allBars.length - 2].close;
                        } else {
                            currentForChange = livePrice;
                            prevClose = lastBar.close;
                        }
                    }

                    if (prevClose && prevClose > 0) {
                        this.allMetrics[ticker].current.prevClose = prevClose;
                        this.allMetrics[ticker].current.changePct =
                            (currentForChange - prevClose) / prevClose * 100;
                    }
                }
                // Volume update: use Yahoo's live intraday quote volume directly.
                // This bypasses "ghost" chart bars and tracks the official live feed.
                if (marketOpen && liveETF.regularMarketVolume != null) {
                    this.allMetrics[ticker].current.volume = liveETF.regularMarketVolume;
                    this.allMetrics[ticker].current.dollarVolume =
                        livePrice * liveETF.regularMarketVolume;
                }

                // Client-side incremental live recalculations of RVOL, Z-score, and percentiles
                const m = this.allMetrics[ticker];
                const hist = m.history || [];
                const liveVol = m.current.volume;
                const liveDVol = m.current.dollarVolume;
                if (hist.length >= 21 && typeof Metrics !== 'undefined') {
                    const prevVols = hist.map(h => (h.volume != null ? h.volume : h[2])).filter(v => v != null && v > 0);
                    if (liveVol != null && liveVol > 0 && prevVols.length >= 21) {
                        const lookback21 = prevVols.slice(-21);
                        const mean21 = Metrics.mean(lookback21);
                        const std21 = Metrics.std(lookback21);
                        if (m.rvol) m.rvol['21d'] = mean21 > 0 ? liveVol / mean21 : null;
                        if (m.zScore) m.zScore['21d'] = std21 > 0 ? (liveVol - mean21) / std21 : 0;
                        const windows = [5, 10, 21, 63, 126, 252];
                        for (const w of windows) {
                            if (m.volPercentile) m.volPercentile[`${w}d`] = Metrics.percentileRank(liveVol, prevVols.slice(-w));
                        }
                    }

                    const prevDVols = hist.map(h => {
                        const cl = h.close != null ? h.close : h[1];
                        const vo = h.volume != null ? h.volume : h[2];
                        return (cl != null && vo != null) ? cl * vo : null;
                    }).filter(v => v != null && v > 0);
                    if (liveDVol != null && liveDVol > 0 && prevDVols.length >= 21) {
                        const lookbackDv21 = prevDVols.slice(-21);
                        const meanDv21 = Metrics.mean(lookbackDv21);
                        const stdDv21 = Metrics.std(lookbackDv21);
                        if (m.dvRvol) m.dvRvol['21d'] = meanDv21 > 0 ? liveDVol / meanDv21 : null;
                        if (m.dvZScore) m.dvZScore['21d'] = stdDv21 > 0 ? (liveDVol - meanDv21) / stdDv21 : 0;
                        const windows = [5, 10, 21, 63, 126, 252];
                        for (const w of windows) {
                            if (m.dvPercentile) m.dvPercentile[`${w}d`] = Metrics.percentileRank(liveDVol, prevDVols.slice(-w));
                        }
                    }
                }

                // Synchronize sparkData with live price and volume
                if (!m.sparkData) {
                    m.sparkData = [];
                }
                const lastBar = m.sparkData.length > 0 ? m.sparkData[m.sparkData.length - 1] : null;
                if (lastBar && lastBar.date === today) {
                    lastBar.close = livePrice;
                    lastBar.volume = m.current.volume;
                    lastBar.dollarVolume = m.current.dollarVolume;
                } else {
                    const prevBarClose = lastBar ? lastBar.close : (m.current.prevClose ?? null);
                    m.sparkData.push({
                        date: today,
                        close: livePrice,
                        volume: m.current.volume,
                        dollarVolume: m.current.dollarVolume,
                        prevClose: prevBarClose
                    });
                    if (m.sparkData.length > CONFIG.sparklineDays) {
                        m.sparkData = m.sparkData.slice(-CONFIG.sparklineDays);
                    }
                }

                quotesToCache[ticker] = {
                    price: m.current.price,
                    volume: m.current.volume,
                    dollarVolume: m.current.dollarVolume,
                    changePct: m.current.changePct,
                    regularMarketTime: m.current.regularMarketTime,
                    marketState: m.current.marketState,
                    etTimeStr: session.etTimeStr,
                    timestamp: Date.now()
                };

                updated = true;
            }

            if (updated) {
                this.updateLiveCards();
                this.liveOverlayState = 'live';
                this._consecutiveLiveFailures = 0;
                this.lastLiveTimeStr = session.etTimeStr;
                this.lastLiveFetchTime = Date.now();
                this._hasWarmCache = true;
                this._warmCacheTime = session.etTimeStr;

                // Save fresh quotes to client-side localStorage cache
                DataService.saveLiveQuotesToStorage(quotesToCache, session);

                this.updateTimestamp(null, isManual);
                console.log('[MONITOR] Live price overlay applied');
                return true;
            }

            this._consecutiveLiveFailures = (this._consecutiveLiveFailures || 0) + 1;
            this.liveOverlayState = 'failed';
            this.updateTimestamp(null, isManual);
            return false;
        } catch (err) {
            console.warn('[MONITOR] Live price overlay failed (non-fatal):', err);
            if (session.isMarketActive && retryCount < 2) {
                const backoff = (retryCount + 1) * 1200;
                console.log(`[MONITOR] Retrying live overlay in ${backoff}ms...`);
                await new Promise(r => setTimeout(r, backoff));
                return await this._overlayLivePrices(retryCount + 1, isManual);
            }

            this._consecutiveLiveFailures = (this._consecutiveLiveFailures || 0) + 1;
            this.liveOverlayState = 'failed';
            this.updateTimestamp(null, isManual);
            return false;
        }
    },

    processLiveData(raw, isManual = false) {
        // Compute metrics for each ETF
        this.allMetrics = {};
        for (const [ticker, etfData] of Object.entries(raw.etfs)) {
            if (etfData) {
                this.allMetrics[ticker] = Metrics.computeAllMetrics(etfData);
            }
        }

        this.updateMarketStatus(this.getMarketStatusNow());
        this.computeAllGlobalPercentiles();
        this.render();
        this.updateTimestamp(null, isManual);
    },

    processPrecomputed(data, isManual = false) {
        this.allMetrics = {};
        const session = this.getMarketSessionInfo();

        // Warm live quotes hydration from localStorage
        const warmCache = DataService.loadLiveQuotesFromStorage(session);
        const hasWarmCache = !!(warmCache && warmCache.quotes && Object.keys(warmCache.quotes).length > 0);
        this._hasWarmCache = hasWarmCache;
        this._warmCacheTime = warmCache?.etTimeStr || null;

        for (const [ticker, etfData] of Object.entries(data.etfs)) {
            if (!etfData || !etfData.current) continue;

            // Normalise current snapshot: pipeline uses snake_case, renderer expects camelCase
            const raw = etfData.current || {};
            // Compute changePct from the two most recent CONFIRMED history bars.
            // Strip any same-date duplicates (Yahoo sometimes returns two rows for
            // the same calendar day with different volumes) — they produce 0% change.
            let changePctFromHistory = raw.change_pct ?? raw.changePct ?? 0;
            const histRaw = etfData.history || [];
            // Deduplicate by date, keeping last entry per date; sort ascending
            // so hist[-1] is always the most recent bar regardless of source order.
            const seen = new Map();
            for (const h of histRaw) { seen.set(h.date ?? h[0], h); }
            const hist = [...seen.values()].sort((a, b) => {
                const da = a.date ?? a[0], db = b.date ?? b[0];
                return da < db ? -1 : da > db ? 1 : 0;
            });
            let prevCloseForCurrent = null;
            let isSnapshotNewerSession = false;
            if (hist.length >= 2) {
                const c1 = hist[hist.length - 1].close ?? hist[hist.length - 1][1];
                const c0 = hist[hist.length - 2].close ?? hist[hist.length - 2][1];
                const priceMatchesLastBar = (raw.price == null) || (c1 != null && Math.abs(raw.price - c1) / Math.max(c1, 1e-6) < 1e-3);
                if (priceMatchesLastBar) {
                    if (c1 != null && c0 && c0 > 0) {
                        changePctFromHistory = (c1 - c0) / c0 * 100;
                        prevCloseForCurrent = c0;
                    }
                } else if (c1 != null && c1 > 0 && raw.price != null) {
                    // Snapshot price represents a newer session not yet appended to hist (e.g. LSE 3NGL.L / 3NGS.L)
                    isSnapshotNewerSession = true;
                    prevCloseForCurrent = c1;
                    changePctFromHistory = (raw.change_pct != null && !isNaN(raw.change_pct))
                        ? raw.change_pct
                        : ((raw.price - c1) / c1 * 100);
                }
            }
            // For some TSX tickers (HNU.TO / HND.TO) Yahoo doesn't return
            // regularMarketPrice / regularMarketVolume in the meta, so the pipeline
            // stores null. Fall back to the last confirmed history bar in that case.
            const lastBar = hist.length > 0 ? hist[hist.length - 1] : null;
            const current = {
                price:        raw.price       ?? lastBar?.close  ?? lastBar?.[1] ?? null,
                volume:       raw.volume      ?? lastBar?.volume ?? lastBar?.[2] ?? 0,
                changePct:    changePctFromHistory,
                dollarVolume: raw.dollar_volume ?? raw.dollarVolume ?? 0,
                prevClose:    prevCloseForCurrent
            };

            // Warm hydration: restore fresh live price, volume, changePct if cached for today
            if (hasWarmCache && warmCache.quotes[ticker]) {
                const wq = warmCache.quotes[ticker];
                if (wq.price != null) current.price = wq.price;
                if (wq.volume != null) current.volume = wq.volume;
                if (wq.dollarVolume != null) current.dollarVolume = wq.dollarVolume;
                if (wq.changePct != null) current.changePct = wq.changePct;
                if (wq.regularMarketTime != null) current.regularMarketTime = wq.regularMarketTime;
                if (wq.marketState != null) current.marketState = wq.marketState;
            }

            // Always declare sparkData locally — avoids implicit global that would bleed
            // stale values from a previous ticker into the next when history is empty.
            const mapBar = (h, idx, arr) => {
                const cl = h.close != null ? h.close : h[1];
                const vo = h.volume != null ? h.volume : h[2];
                const prevH = (arr && idx > 0) ? arr[idx - 1] : null;
                const prevCl = h.prevClose != null ? h.prevClose : (prevH ? (prevH.close != null ? prevH.close : prevH[1]) : null);
                return {
                    date: h.date || h[0],
                    open: h.open != null ? h.open : (h[3] != null ? h[3] : cl),
                    high: h.high != null ? h.high : (h[4] != null ? h[4] : cl),
                    low: h.low != null ? h.low : (h[5] != null ? h[5] : cl),
                    close: cl,
                    volume: vo,
                    dollarVolume: h.dollar_volume != null ? h.dollar_volume : ((cl != null && vo != null) ? cl * vo : 0),
                    prevClose: prevCl
                };
            };
            // Slicing heatmapData:
            // When market is open or in pre-market, today's bar in hist is incomplete/partial.
            // Using a partial session in the Heat Map causes an artificial 0th-percentile volume drop (black box).
            // Therefore, if the market is open and the last bar represents today, slice up to yesterday's confirmed close.
            const isMarketOpen = (this.getMarketStatusNow() === 'open' || this.getMarketStatusNow() === 'pre_market' || data.market_status === 'open');
            const todayET = session.dateStr;
            const lastBarDate = (hist && hist.length > 0) ? (hist[hist.length - 1].date || hist[hist.length - 1][0]) : null;
            const isTodayIncomplete = isMarketOpen && (lastBarDate === todayET || lastBarDate === new Date().toISOString().slice(0, 10));

            const confirmedHist = isTodayIncomplete ? hist.slice(0, -1) : hist;

            const sparkStartIdx = Math.max(0, hist.length - CONFIG.sparklineDays);
            let sparkData = (hist && hist.length > 0)
                ? hist.slice(sparkStartIdx).map((h, i) => {
                    const mapped = mapBar(h, sparkStartIdx + i, hist);
                    return mapped;
                })
                : [];

            // If current snapshot is a newer session not yet in hist (e.g. 3NGL.L / 3NGS.L), append it to sparkData
            if (isSnapshotNewerSession && current.price != null && current.volume != null) {
                const snapDate = (data.last_updated && typeof data.last_updated === 'string' && data.last_updated.length >= 10)
                    ? data.last_updated.slice(0, 10)
                    : (session.priorClose?.dateStr || session.dateStr);
                const lastSpark = sparkData.length > 0 ? sparkData[sparkData.length - 1] : null;
                if (!lastSpark || lastSpark.date < snapDate) {
                    sparkData.push({
                        date: snapDate,
                        open: current.price,
                        high: current.price,
                        low: current.price,
                        close: current.price,
                        volume: current.volume,
                        dollarVolume: current.dollarVolume || (current.price * current.volume),
                        prevClose: lastSpark ? lastSpark.close : prevCloseForCurrent
                    });
                    if (sparkData.length > CONFIG.sparklineDays) {
                        sparkData = sparkData.slice(-CONFIG.sparklineDays);
                    }
                }
            }

            if (hasWarmCache && warmCache.quotes[ticker]) {
                const wq = warmCache.quotes[ticker];
                if (wq.price != null && wq.volume != null) {
                    const lastBar = sparkData.length > 0 ? sparkData[sparkData.length - 1] : null;
                    if (lastBar && lastBar.date === session.dateStr) {
                        lastBar.close = wq.price;
                        lastBar.volume = wq.volume;
                        lastBar.dollarVolume = wq.dollarVolume || (wq.price * wq.volume);
                    } else if (!lastBar || lastBar.date < session.dateStr) {
                        sparkData.push({
                            date: session.dateStr,
                            close: wq.price,
                            volume: wq.volume,
                            dollarVolume: wq.dollarVolume || (wq.price * wq.volume)
                        });
                        if (sparkData.length > CONFIG.sparklineDays) {
                            sparkData = sparkData.slice(-CONFIG.sparklineDays);
                        }
                    }
                }
            }
            const heatmapData = (confirmedHist && confirmedHist.length > 0)
                ? confirmedHist.slice(-CONFIG.heatmapDays).map(mapBar)
                : [];

            // Build metrics object from pre-computed data
            // Alert level uses VCVI when available (vol-adjusted is primary signal)
            const vcviData  = etfData.vcvi  || etfData.cvi || {};
            const maxVcvi = Math.max(...Object.values(vcviData).filter(v => v != null && !isNaN(v)), 0);
            let alertLevel = 'none';
            const tv = CONFIG.thresholds.vcvi;
            if (maxVcvi >= tv.extreme) alertLevel = 'extreme';
            else if (maxVcvi >= tv.critical) alertLevel = 'critical';
            else if (maxVcvi >= tv.high)     alertLevel = 'high';
            else if (maxVcvi >= tv.elevated) alertLevel = 'elevated';

            // Count MWCA windows
            const mwcaThreshold = CONFIG.thresholds.mwca_threshold;
            let mwcaCount = 0;
            for (const w of CONFIG.windows.percentile) {
                const key = `${w}d`;
                if (etfData.vol_percentile && etfData.vol_percentile[key] >= mwcaThreshold) mwcaCount++;
            }

            // Map alerts from signals
            const alerts = (data.signals || [])
                .filter(s => s.ticker === ticker)
                .map(s => {
                    const tp = s.type;
                    let type = 'rvol';
                    if (tp.includes('vcvi'))       type = 'vcvi';
                    else if (tp.includes('cvi'))   type = 'cvi';
                    else if (tp.includes('mwca'))  type = 'mwca';
                    else if (tp.includes('vov'))   type = 'vov';
                    else if (tp.includes('atr'))   type = 'atr_breakout';
                    else if (tp.includes('vol_regime')) type = 'vol_regime';
                    const level = tp.includes('critical') ? 'critical'
                        : tp.includes('extreme') ? 'extreme'
                        : tp.includes('warning') ? 'elevated'
                        : 'elevated';
                    return {
                        type, level,
                        ticker,
                        time: new Date(s.timestamp).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' }),
                        message: s.message
                    };
                });

            // Normalise volatility block (pipeline uses snake_case)
            const vol = etfData.volatility || {};

            // Compute vol series / percentiles from stored history if not in pipeline JSON
            const histCloses = (etfData.history || []).map(h => h.close ?? h[1]).filter(v => v != null);
            const computedHvSeries21   = histCloses.length >= 22 ? Metrics.computeHVSeries(histCloses, 21, histCloses.length) : [];
            const computedHvPercentiles = histCloses.length >= 6 ? {
                '5d':  Metrics.computeHVPercentile(histCloses, 5),
                '21d': Metrics.computeHVPercentile(histCloses, 21),
                '63d': Metrics.computeHVPercentile(histCloses, 63),
                '252d': Metrics.computeHVPercentile(histCloses, 252),
            } : {};

            // Merge pipeline hv with computed 5d (pipeline only has 10d/21d/63d/252d)
            const hvBase = vol.hv || {};
            const hv5d   = histCloses.length >= 6 ? Metrics.computeHV(histCloses, 5) : null;
            const mergedHv = { '5d': hv5d, ...hvBase };

            const volatility = {
                hv:              mergedHv,
                hvPercentiles:   vol.hv_percentiles  || vol.hvPercentiles  || computedHvPercentiles,
                hvSeries21:      vol.hv_series21     || vol.hvSeries21     || computedHvSeries21,
                volRegimePct:    vol.vol_regime_pct  != null ? vol.vol_regime_pct  : (vol.volRegimePct    ?? null),
                hvTermStructure: vol.hv_term_structure != null ? vol.hv_term_structure : (vol.hvTermStructure ?? null),
                atr14Pct:        vol.atr14_pct       != null ? vol.atr14_pct       : (vol.atr14Pct        ?? null),
                vov21:           vol.vov21           ?? null,
            };

            // Normalise alert level to also consider sharp_spike
            if (etfData.sharp_spike && alertLevel === 'none') alertLevel = 'elevated';

            const rvol = { ...(etfData.rvol || {}) };
            const zScore = { ...(etfData.z_score || etfData.zScore || {}) };
            const vroc = { ...(etfData.vroc || {}) };
            const volPercentile = { ...(etfData.vol_percentile || etfData.volPercentile || {}) };
            const pricePercentile = { ...(etfData.price_percentile || etfData.pricePercentile || {}) };
            const cvi = { ...(etfData.cvi || {}) };
            const vcvi = { ...(etfData.vcvi || etfData.cvi || {}) };
            const dvRvol = { ...(etfData.dv_rvol || {}) };
            const dvZScore = { ...(etfData.dv_zscore || {}) };
            const dvVroc = { ...(etfData.dv_vroc || {}) };
            const dvPercentile = { ...(etfData.dv_percentile || {}) };
            const dvcvi = { ...(etfData.dvcvi || {}) };
            let vps = etfData.vps || 0;
            let dvVps = etfData.dv_vps ?? null;
            let mwca = etfData.mwca || false;
            let decayBlock = etfData.decay ? { ...etfData.decay } : null;
            let sharpSpike = etfData.sharp_spike || false;

            // Ensure DVCVI has volatility regime scaling applied across all windows
            if (typeof Metrics !== 'undefined' && volatility.volRegimePct != null) {
                for (const w of CONFIG.windows.percentile) {
                    const key = `${w}d`;
                    if (dvPercentile[key] != null && pricePercentile[key] != null) {
                        const rawDvCvi = Metrics.computeCVI(dvPercentile[key], pricePercentile[key]);
                        dvcvi[key] = Metrics.computeVCVI(rawDvCvi, volatility.volRegimePct);
                    }
                }
            }

            // Recompute decay-adjusted 21d VCVI using correct exp(-dailyDecay * age) over 21d window
            const annualDecay = (typeof CONFIG !== 'undefined' && CONFIG.etfDecayRates && CONFIG.etfDecayRates[ticker] != null)
                ? CONFIG.etfDecayRates[ticker]
                : (decayBlock?.annual_rate || 0.35);
            if (typeof Metrics !== 'undefined' && typeof Metrics.computeDecayAdjustedVCVI21 === 'function' && histCloses.length >= 21) {
                const decayCalc = Metrics.computeDecayAdjustedVCVI21(histCloses, volPercentile['21d'], volatility.volRegimePct, annualDecay, 21);
                if (decayCalc) {
                    decayBlock = {
                        annual_rate: annualDecay,
                        adj_price_pct: decayCalc.adjPricePct,
                        adj_vcvi_21d: decayCalc.adjVcvi21,
                        correction_active: annualDecay > 0
                    };
                }
            }

            // If warm cached volume exists OR current snapshot is a newer session than hist[-1],
            // perform full incremental recalculation so all card metrics match current price & volume
            if ((hasWarmCache && warmCache.quotes[ticker]) || isSnapshotNewerSession) {
                const liveVol = current.volume;
                const liveDVol = current.dollarVolume;
                const livePrice = current.price;
                if (hist.length >= 21 && typeof Metrics !== 'undefined') {
                    const prevVols = hist.map(h => (h.volume != null ? h.volume : h[2])).filter(v => v != null && v > 0);
                    if (liveVol != null && liveVol > 0 && prevVols.length >= 21) {
                        const lookback21 = prevVols.slice(-21);
                        const mean21 = Metrics.mean(lookback21);
                        const std21 = Metrics.std(lookback21);
                        if (mean21 > 0) rvol['21d'] = liveVol / mean21;
                        if (std21 > 0) zScore['21d'] = (liveVol - mean21) / std21;
                        if (prevVols.length >= 10 && prevVols[prevVols.length - 10] > 0) {
                            vroc['10d'] = ((liveVol - prevVols[prevVols.length - 10]) / prevVols[prevVols.length - 10]) * 100;
                        }
                        const windows = [5, 10, 21, 63, 126, 252];
                        mwcaCount = 0;
                        for (const w of windows) {
                            const pct = Metrics.percentileRank(liveVol, prevVols.slice(-w));
                            volPercentile[`${w}d`] = pct;
                            if (pct >= mwcaThreshold) mwcaCount++;
                        }
                        mwca = (mwcaCount === windows.length);
                    }

                    const prevDVols = hist.map(h => {
                        const cl = h.close != null ? h.close : h[1];
                        const vo = h.volume != null ? h.volume : h[2];
                        return (cl != null && vo != null) ? cl * vo : null;
                    }).filter(v => v != null && v > 0);
                    if (liveDVol != null && liveDVol > 0 && prevDVols.length >= 21) {
                        const lookbackDv21 = prevDVols.slice(-21);
                        const meanDv21 = Metrics.mean(lookbackDv21);
                        const stdDv21 = Metrics.std(lookbackDv21);
                        if (meanDv21 > 0) dvRvol['21d'] = liveDVol / meanDv21;
                        if (stdDv21 > 0) dvZScore['21d'] = (liveDVol - meanDv21) / stdDv21;
                        if (prevDVols.length >= 10 && prevDVols[prevDVols.length - 10] > 0) {
                            dvVroc['10d'] = ((liveDVol - prevDVols[prevDVols.length - 10]) / prevDVols[prevDVols.length - 10]) * 100;
                        }
                        const windows = [5, 10, 21, 63, 126, 252];
                        for (const w of windows) {
                            dvPercentile[`${w}d`] = Metrics.percentileRank(liveDVol, prevDVols.slice(-w));
                        }
                    }

                    const prevCloses = hist.map(h => (h.close != null ? h.close : h[1])).filter(v => v != null && v > 0);
                    if (livePrice != null && livePrice > 0 && prevCloses.length >= 21) {
                        const windows = [5, 10, 21, 63, 126, 252];
                        for (const w of windows) {
                            const key = `${w}d`;
                            const pPct = Metrics.percentileRank(livePrice, prevCloses.slice(-w));
                            pricePercentile[key] = pPct;
                            if (volPercentile[key] != null) {
                                cvi[key] = Metrics.computeCVI(volPercentile[key], pPct);
                                vcvi[key] = Metrics.computeVCVI(cvi[key], volatility.volRegimePct);
                            }
                            if (dvPercentile[key] != null) {
                                const rawDvCvi = Metrics.computeCVI(dvPercentile[key], pPct);
                                dvcvi[key] = Metrics.computeVCVI(rawDvCvi, volatility.volRegimePct);
                            }
                        }

                        if (typeof Metrics.computeDecayAdjustedVCVI21 === 'function') {
                            const closesWithLive = [...prevCloses.slice(-20), livePrice];
                            const decayCalc = Metrics.computeDecayAdjustedVCVI21(closesWithLive, volPercentile['21d'], volatility.volRegimePct, annualDecay, 21);
                            if (decayCalc) {
                                decayBlock = {
                                    annual_rate: annualDecay,
                                    adj_price_pct: decayCalc.adjPricePct,
                                    adj_vcvi_21d: decayCalc.adjVcvi21,
                                    correction_active: annualDecay > 0
                                };
                            }
                        }

                        const vw = CONFIG.vpsWeights || { rvol: 0.25, zScore: 0.20, percentile: 0.25, vroc: 0.10, volRegime: 0.20 };
                        const invVolReg = volatility.volRegimePct != null ? (100 - volatility.volRegimePct) : 50;
                        if (rvol['21d'] != null) {
                            const rvolNorm = Math.min(100, Math.max(0, (Math.log2(Math.max(0.5, rvol['21d'])) + 1) * 33.3));
                            const zNorm = zScore['21d'] != null ? Math.min(100, Math.max(0, (zScore['21d'] + 2) * 12.5)) : 0;
                            const pctNorm = volPercentile['21d'] != null ? volPercentile['21d'] : 0;
                            const vrocNorm = vroc['10d'] != null ? Math.min(100, Math.max(0, (vroc['10d'] + 100) / 5)) : 0;
                            vps = vw.rvol * rvolNorm + vw.zScore * zNorm + vw.percentile * pctNorm + vw.vroc * vrocNorm + vw.volRegime * invVolReg;
                        }
                        if (dvRvol['21d'] != null) {
                            const dvRvolNorm = Math.min(100, Math.max(0, (Math.log2(Math.max(0.5, dvRvol['21d'])) + 1) * 33.3));
                            const dvZNorm = dvZScore['21d'] != null ? Math.min(100, Math.max(0, (dvZScore['21d'] + 2) * 12.5)) : 0;
                            const dvPctNorm = dvPercentile['21d'] != null ? dvPercentile['21d'] : 0;
                            const dvVrocNorm = dvVroc['10d'] != null ? Math.min(100, Math.max(0, (dvVroc['10d'] + 100) / 5)) : 0;
                            dvVps = vw.rvol * dvRvolNorm + vw.zScore * dvZNorm + vw.percentile * dvPctNorm + vw.vroc * dvVrocNorm + vw.volRegime * invVolReg;
                        }
                    }
                }
            }

            // Compute Upgraded VDDS* and Notional Dollar RVOL (drvol) from bar history
            let vddsVal = etfData.vdds ?? null;
            let drvolVal = dvRvol['21d'] ?? null;
            let latestVddsRecord = null;
            if (typeof Metrics !== 'undefined' && typeof Metrics.computeUpgradedVDDS === 'function' && hist.length > 0) {
                const effectiveHist = isSnapshotNewerSession && current.price != null
                    ? [...hist, {
                        date: sparkData[sparkData.length - 1]?.date || session.dateStr,
                        open: current.price,
                        high: current.price,
                        low: current.price,
                        close: current.price,
                        volume: current.volume || 0
                    }]
                    : hist;
                const upgradedVddsSeries = Metrics.computeUpgradedVDDS(effectiveHist);
                if (upgradedVddsSeries && upgradedVddsSeries.length > 0) {
                    latestVddsRecord = upgradedVddsSeries[upgradedVddsSeries.length - 1];
                    vddsVal = latestVddsRecord.vddsStar;
                    drvolVal = latestVddsRecord.drvol;
                }
            }

            // Re-evaluate card alertLevel using active displayed windows & volume participation gate
            if (typeof Metrics !== 'undefined' && typeof Metrics.computeCardAlertLevel === 'function') {
                alertLevel = Metrics.computeCardAlertLevel(vcvi, mwca, sharpSpike, rvol['21d'], volPercentile);
            }

            this.allMetrics[ticker] = {
                ticker: ticker,
                current: current,
                rvol: rvol,
                zScore: zScore,
                vroc: vroc,
                volPercentile: volPercentile,
                pricePercentile: pricePercentile,
                cvi:  cvi,
                vcvi: vcvi,
                volatility,
                decay:             decayBlock,                      // Feature 6
                seasonality:       etfData.seasonality       || null,   // Feature 3
                sharpSpike:        sharpSpike,                      // Feature 1
                fastSignal:        etfData.fast_signal       || null,   // Feature 1
                vps: vps,
                mwca: mwca,
                mwcaCount: mwcaCount,
                priceMAs: (etfData.moving_averages || {}).price || etfData.priceMAs || {},
                volumeMAs: (etfData.moving_averages || {}).volume || etfData.volumeMAs || {},
                rollingCorr: etfData.rolling_correlation != null ? etfData.rolling_correlation : (etfData.rollingCorr || null),
                // Dollar Volume Metrics
                dvRvol:      dvRvol,
                dvZScore:    dvZScore,
                dvPercentile: dvPercentile,
                dvVroc:      dvVroc,
                dvcvi:       dvcvi,
                dvVps:       dvVps,
                vdds:        vddsVal,
                drvol:       drvolVal,
                vddsRecord:  latestVddsRecord,
                alerts: alerts,
                alertLevel: alertLevel,
                sparkData: sparkData,
                heatmapData: heatmapData,
                history: hist,
                historyLength: (etfData.history || []).length
            };
        }

        this.ngPriceContext = data.ng_price_context || null;   // Feature 2
        this.sideConvergence = data.side_convergence || null;
        // Always compute market status from current wall-clock time so the
        // label is correct even when the pre-computed JSON is stale.
        this.updateMarketStatus(this.getMarketStatusNow());
        this.computeAllGlobalPercentiles();
        this.render();
        if (typeof TailRiskDrawer !== 'undefined' && typeof TailRiskDrawer.seedFromPrecomputed === 'function') {
            TailRiskDrawer.seedFromPrecomputed(this.allMetrics);
        }
        this.updateTimestamp(data.last_updated, isManual);
    },

    render() {
        // Section A: Long side cards
        Cards.renderAllCards(this.allMetrics, document.getElementById('long-cards'), 'long');

        // Section B: Short side cards
        Cards.renderAllCards(this.allMetrics, document.getElementById('short-cards'), 'short');

        // Section C: Signal Command Center
        Signals.renderAll(this.allMetrics, this.sideConvergence || null, this.ngPriceContext || null);

        // Price-Volume Studies panel (classic volume indicators)
        if (typeof VolumeStudies !== 'undefined') VolumeStudies.render();

        // Volume Profile panel (volume at price)
        if (typeof VolumeProfile !== 'undefined') VolumeProfile.render();
    },

    // Surgical live-quote update: refreshes only price/change/volume fields in the
    // cards (called on every overlay tick) instead of re-rendering every panel.
    updateLiveCards() {
        const longEl = document.getElementById('long-cards');
        const shortEl = document.getElementById('short-cards');
        if (longEl && shortEl && typeof Cards !== 'undefined' && Cards.updateLive) {
            Cards.updateLive(this.allMetrics, [longEl, shortEl]);
            if (typeof Signals !== 'undefined') {
                if (typeof Signals.renderConvergenceGauges === 'function') {
                    Signals.renderConvergenceGauges(this.allMetrics);
                }
                if (typeof Signals.renderVddsBar === 'function') {
                    Signals.renderVddsBar(this.allMetrics, this.vddsMode || 'vol');
                }
            }
        } else {
            this.render();
        }
        if (typeof TailRiskDrawer !== 'undefined' && TailRiskDrawer.state && TailRiskDrawer.state.isOpen) {
            const activeTicker = TailRiskDrawer.state.ticker;
            const m = this.allMetrics ? this.allMetrics[activeTicker] : null;
            if (m && m.current) {
                const c = m.current;
                const prevClose = c.prevClose || (c.price && c.changePct != null ? (c.price / (1 + c.changePct / 100)) : c.price);
                TailRiskDrawer.updateLive(activeTicker, c.price, prevClose);
            }
        }
    },

    renderHeatCalendarOnly() {
        const globalObj = this.heatGlobalPercentiles[this.heatMode];
        const percentiles = globalObj ? globalObj[this.heatSide] : null;
        Signals.renderHeatCalendar(this.allMetrics, this.heatOffset, this.heatMode, this.heatSide, this.heatSpotlightMode, this.heatPeaksThreshold, percentiles, this.heatDays);
    },

    computeAllGlobalPercentiles() {
        const filters = ['all', 'long', 'short', ...Object.keys(CONFIG.etfs)];
        this.heatGlobalPercentiles = {
            share: {},
            dollar: {}
        };
        for (const f of filters) {
            this.heatGlobalPercentiles.share[f]  = Signals.computeHeatGlobalPercentiles(this.allMetrics, 'share', f);
            this.heatGlobalPercentiles.dollar[f] = Signals.computeHeatGlobalPercentiles(this.allMetrics, 'dollar', f);
        }
    },

    // NYSE holidays (observed full closures) for 2025, 2026, 2027 (YYYY-MM-DD in ET)
    nyseHolidays: new Set([
        // 2025
        '2025-01-01', '2025-01-20', '2025-02-17', '2025-04-18', '2025-05-26',
        '2025-06-19', '2025-07-04', '2025-09-01', '2025-11-27', '2025-12-25',
        // 2026
        '2026-01-01', '2026-01-19', '2026-02-16', '2026-04-03', '2026-05-25',
        '2026-06-19', '2026-07-03', '2026-09-07', '2026-11-26', '2026-12-25',
        // 2027
        '2027-01-01', '2027-01-18', '2027-02-15', '2027-03-26', '2027-05-31',
        '2027-06-18', '2027-07-05', '2027-09-06', '2027-11-25', '2027-12-24'
    ]),

    // Extract ET components from date using Intl.DateTimeFormat
    _getETParts(d) {
        const formatter = new Intl.DateTimeFormat('en-US', {
            timeZone: 'America/New_York',
            weekday: 'short',
            year: 'numeric',
            month: '2-digit',
            day: '2-digit',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
            hourCycle: 'h23'
        });
        const parts = Object.fromEntries(formatter.formatToParts(d).map(p => [p.type, p.value]));
        return {
            weekday: parts.weekday,
            year: parts.year,
            month: parts.month,
            day: parts.day,
            hour: parseInt(parts.hour, 10),
            minute: parseInt(parts.minute, 10),
            second: parseInt(parts.second, 10),
            dateStr: `${parts.year}-${parts.month}-${parts.day}`
        };
    },

    // Walks backwards to find the most recent completed regular trading day
    getPriorTradingDay(date) {
        const d = date ? (date instanceof Date ? date : new Date(date)) : new Date();
        let check = new Date(d.getTime() - 24 * 3600 * 1000);
        for (let i = 0; i < 10; i++) {
            const p = this._getETParts(check);
            if (p.weekday !== 'Sat' && p.weekday !== 'Sun' && !this.nyseHolidays.has(p.dateStr)) {
                const monthDayFmt = new Intl.DateTimeFormat('en-US', {
                    timeZone: 'America/New_York',
                    month: 'short',
                    day: 'numeric'
                });
                return {
                    dateStr: p.dateStr,
                    weekday: p.weekday,
                    label: monthDayFmt.format(check)
                };
            }
            check = new Date(check.getTime() - 24 * 3600 * 1000);
        }
        return null;
    },

    // Returns comprehensive NYSE session metadata based on Eastern Time (ET).
    // Automatically handles US DST transitions via Intl.DateTimeFormat.
    getMarketSessionInfo(date) {
        const d = date ? (date instanceof Date ? date : new Date(date)) : new Date();
        const et = this._getETParts(d);
        const { weekday, year, month, day, hour, minute, second, dateStr } = et;
        const etMinutes = hour * 60 + minute;

        // 12-hour formatted ET time string (e.g. '03:10:01 PM ET')
        const timeFmt = new Intl.DateTimeFormat('en-US', {
            timeZone: 'America/New_York',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
            hour12: true
        });
        const etTimeStr = timeFmt.format(d) + ' ET';

        const isWeekend = (weekday === 'Sat' || weekday === 'Sun');
        const isHoliday = this.nyseHolidays.has(dateStr);

        let status = 'closed';
        let isMarketActive = false;
        let label = 'MARKET CLOSED';

        if (isWeekend) {
            status = 'weekend';
            isMarketActive = false;
            label = 'WEEKEND';
        } else if (isHoliday) {
            status = 'holiday';
            isMarketActive = false;
            label = 'HOLIDAY CLOSED';
        } else if (etMinutes >= 570 && etMinutes < 960) {
            // 09:30 – 16:00 ET
            status = 'open';
            isMarketActive = true;
            label = 'MARKET OPEN';
        } else if (etMinutes >= 240 && etMinutes < 570) {
            // 04:00 – 09:30 ET
            status = 'pre_market';
            isMarketActive = false;
            label = 'PRE-MARKET';
        } else if (etMinutes >= 960 && etMinutes < 1200) {
            // 16:00 – 20:00 ET
            status = 'after_hours';
            isMarketActive = false;
            label = 'AFTER-HOURS';
        } else {
            // 20:00 – 04:00 ET
            status = 'closed';
            isMarketActive = false;
            label = 'MARKET CLOSED';
        }

        const minutesSinceOpen = isMarketActive ? (etMinutes - 570) : -1;
        const marketOpenTimeMs = isMarketActive ? (d.getTime() - (etMinutes - 570) * 60 * 1000) : null;
        const priorClose = this.getPriorTradingDay(d);

        return {
            status,
            isMarketActive,
            label,
            weekday,
            year,
            month,
            day,
            hour,
            minute,
            second,
            dateStr,
            etTimeStr,
            minutesSinceOpen,
            marketOpenTimeMs,
            priorClose
        };
    },

    // Returns NYSE session state based on current wall-clock time in ET.
    getMarketStatusNow(date) {
        return this.getMarketSessionInfo(date).status;
    },

    updateMarketStatus(state) {
        const el = document.getElementById('market-status');
        if (!el) return;

        el.className = 'market-status';
        const text = el.querySelector('.status-text');

        switch (state) {
            case 'open':
                el.classList.add('open');
                if (text) text.textContent = 'MARKET OPEN';
                break;
            case 'pre_market':
                el.classList.add('pre');
                if (text) text.textContent = 'PRE-MARKET';
                break;
            case 'after_hours':
                el.classList.add('pre');
                if (text) text.textContent = 'AFTER-HOURS';
                break;
            case 'weekend':
                el.classList.add('closed');
                if (text) text.textContent = 'WEEKEND';
                break;
            case 'holiday':
                el.classList.add('closed');
                if (text) text.textContent = 'HOLIDAY CLOSED';
                break;
            case 'closed':
            default:
                el.classList.add('closed');
                if (text) text.textContent = 'MARKET CLOSED';
                break;
        }
    },

    updateTimestamp(iso, isManual = false) {
        const el = document.getElementById('last-updated');
        if (!el) return;

        const session = this.getMarketSessionInfo();
        const isMarketActive = session.isMarketActive;

        if (isMarketActive) {
            // Truthful Header State Machine during active market
            if (this.liveOverlayState === 'live') {
                el.textContent = `Prices: LIVE ${this.lastLiveTimeStr || session.etTimeStr}`;
                el.classList.remove('stale');
            } else if (this._hasWarmCache && (!this.lastLiveFetchTime || (Date.now() - this.lastLiveFetchTime) < 600000)) {
                // Keep recent warm cached live data active and resilient during temporary proxy blips
                el.textContent = `Prices: LIVE ${this._warmCacheTime || session.etTimeStr} (Cached) \u2022 Updating...`;
                el.classList.remove('stale');
            } else if (this.liveOverlayState === 'failed' && (this._consecutiveLiveFailures || 0) >= 3) {
                el.textContent = 'Data: Prior Close \u2022 Live feed paused';
                el.classList.add('stale');
            } else {
                // Cold cache on boot / transient proxy retry
                el.textContent = 'Prices: Updating live feed...';
                el.classList.remove('stale');
            }
            return;
        }

        // Market is closed, weekend, holiday, after-hours, or pre-market
        const refreshSuffix = isManual ? ` (Refreshed ${session.etTimeStr})` : '';
        const prior = session.priorClose;
        const dayLabel = (prior && prior.weekday) ? prior.weekday : 'Fri';
        const dateLabel = prior ? prior.label : (session.status === 'holiday' ? session.dateStr : '');

        if (session.status === 'weekend') {
            el.textContent = `Settled Close: ${dayLabel} ${dateLabel}, 16:00 ET${refreshSuffix}`;
        } else if (session.status === 'holiday') {
            el.textContent = `Settled Close: ${dayLabel} ${dateLabel}, 16:00 ET${refreshSuffix}`;
        } else if (session.status === 'after_hours') {
            el.textContent = `Session Closed: 16:00 ET${refreshSuffix}`;
        } else if (session.status === 'pre_market') {
            el.textContent = `Pre-Market (Opens 09:30 ET)${refreshSuffix}`;
        } else {
            // Overnight closed
            el.textContent = `Settled Close: ${dayLabel} ${dateLabel}, 16:00 ET${refreshSuffix}`;
        }
        el.classList.remove('stale');
    },

    setLoading(on) {
        const btn = document.getElementById('refresh-btn');
        if (on) btn.classList.add('spinning');
        else btn.classList.remove('spinning');
    },

    showError(msg) {
        console.warn('[MONITOR]', msg);
        // Could show a toast notification here
    },

    startAutoRefresh() {
        const state = this.getMarketStatusNow();
        let interval = 1800000; // 30 min default outside market hours
        if (state === 'open') {
            interval = (CONFIG && CONFIG.refreshInterval) || 60000; // 60 s
        } else if (state === 'pre_market' || state === 'after_hours') {
            interval = (CONFIG && CONFIG.refreshIntervalPrePost) || 300000; // 5 min
        } else {
            interval = (CONFIG && CONFIG.refreshIntervalClosed) || 1800000; // 30 min
        }

        // Re-evaluate on every refresh so the cadence tracks session transitions
        // (open → closed → next open) without a page reload.
        if (this.refreshTimer && this.refreshIntervalMs === interval) return;
        if (this.refreshTimer) clearInterval(this.refreshTimer);
        this.refreshIntervalMs = interval;
        this.refreshTimer = setInterval(() => this.refresh(false), interval);
        console.log(`[MONITOR] Auto-refresh every ${interval / 1000}s (market: ${state})`);
    },

    handleResize() {
        // Redraw charts on resize
        if (this.resizeTimeout) clearTimeout(this.resizeTimeout);
        this.resizeTimeout = setTimeout(() => {
            this.render();
            if (typeof VolumeProfile !== 'undefined') VolumeProfile.render();
        }, 200);
    }
};

// Banner toggle
function toggleBanner() {
    const banner = document.getElementById('hypothesis-banner');
    if (banner) {
        banner.classList.toggle('collapsed');
    }
}

// Boot
window.App = App;
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => App.init());
} else {
    App.init();
}

// ============================================================
// Universal JS Tooltip Singleton
// Uses position:fixed + viewport clamping — never overflows
// panels regardless of parent overflow or layout constraints.
// ============================================================
(function () {
    const tt = document.createElement('div');
    tt.id = 'tt';
    document.body.appendChild(tt);

    let active = null;
    let hideTimer = null;

    function show(el, evt) {
        if (el === active && tt.classList.contains('visible')) return;
        active = el;
        tt.innerHTML = el.getAttribute('data-tooltip');
        
        if (hideTimer) { clearTimeout(hideTimer); hideTimer = null; }
        
        tt.classList.add('visible');

        // Position after paint so offsetWidth/Height are correct
        requestAnimationFrame(function () {
            const r = el.getBoundingClientRect();
            const tw = tt.offsetWidth;
            const th = tt.offsetHeight;
            const pad = 12;
            const GAP = 10;

            // Preferred Y: above element/cursor
            const anchorTop = (evt && evt.clientY) ? Math.min(r.top, evt.clientY) : r.top;
            const anchorBottom = (evt && evt.clientY) ? Math.max(r.bottom, evt.clientY) : r.bottom;

            let y = anchorTop - th - GAP;
            // If not enough room above viewport, position below
            if (y < pad) {
                y = anchorBottom + GAP;
            }

            // X positioning:
            // If mouse coordinate is present, offset to the right of cursor by 14px so mouse point is never obscured
            let x;
            if (evt && evt.clientX) {
                x = evt.clientX + 14;
                // If it overflows right viewport boundary, flip to left of cursor
                if (x + tw > window.innerWidth - pad) {
                    x = evt.clientX - tw - 14;
                }
            } else {
                x = r.left + r.width / 2 - tw / 2;
            }

            // Viewport clamping
            x = Math.max(pad, Math.min(x, window.innerWidth - tw - pad));
            y = Math.max(pad, Math.min(y, window.innerHeight - th - pad));

            // Safety collision check: if cursor point falls inside tooltip box after clamping,
            // push tooltip away so the mouse pointer and hovered text are never covered
            if (evt && evt.clientX && evt.clientY) {
                const insideX = evt.clientX >= x - 2 && evt.clientX <= x + tw + 2;
                const insideY = evt.clientY >= y - 2 && evt.clientY <= y + th + 2;
                if (insideX && insideY) {
                    if (y >= evt.clientY) {
                        y = Math.min(window.innerHeight - th - pad, evt.clientY + 22);
                    } else {
                        y = Math.max(pad, evt.clientY - th - 22);
                    }
                }
            }

            tt.style.left = Math.round(x) + 'px';
            tt.style.top = Math.round(y) + 'px';
        });
    }

    function hide() {
        hideTimer = setTimeout(() => {
            tt.classList.remove('visible');
            active = null;
        }, 80); // Slight delay for smoother feel
    }

    function hideImmediate() {
        if (hideTimer) { clearTimeout(hideTimer); hideTimer = null; }
        tt.classList.remove('visible');
        active = null;
    }

    document.addEventListener('mouseover', function (e) {
        const el = e.target.closest('[data-tooltip]');
        if (el) show(el, e);
    });

    document.addEventListener('mouseout', function (e) {
        if (e.target.closest('[data-tooltip]')) {
            hide();
        }
    });

    // Touch: tap to show tooltip, auto-dismiss after 2.5s or on next tap elsewhere
    let touchTimer = null;
    document.addEventListener('touchstart', function (e) {
        const el = e.target.closest('[data-tooltip]');
        clearTimeout(touchTimer);
        if (el) {
            if (active === el) { hideImmediate(); return; }
            show(el);
            touchTimer = setTimeout(hideImmediate, 2500);
        } else {
            hideImmediate();
        }
    }, { passive: true });
}());