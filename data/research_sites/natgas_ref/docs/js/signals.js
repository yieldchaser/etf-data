/* ============================================
   Signal Command Center Rendering
   ============================================ */

const Signals = {

    // Icon/prefix for each alert type
    _alertIcon(type) {
        const icons = {
            vcvi: '⚡', cvi: '🔥', mwca: '💥', rvol: '📈', vps: '📊',
            atr_breakout: '📐', vov: '🌀', vol_regime: '🌡', ipsi_stress: '⚠',
            fast_spike: '⚡', weather: '⛈', flow_accel: '▲'
        };
        return icons[type] || '●';
    },

    computeTrimmedMean21(buf) {
        const sorted = Array.from(buf).sort((a, b) => a - b);
        let sum = 0.0;
        const start = 3;
        const end = 18;
        for (let i = start; i < end; i++) {
            sum += sorted[i];
        }
        return sum / 15;
    },

    computeEtfMicrostructure(history, mode = 'share') {
        const EPS = 1e-9;
        const WINDOW_LAG = 21;
        const KAPPA_PEAK = 1.35;
        const KAPPA_VALLEY = 1.60;
        const T = history.length;

        const turnover = new Float64Array(T);
        const dollarTurnover = new Float64Array(T);
        const trueRange = new Float64Array(T);
        const typPrice = new Float64Array(T);
        const abdiSpread = new Float64Array(T);

        for (let t = 0; t < T; t++) {
            const b = history[t];
            const h = b.high != null ? b.high : b.close;
            const l = b.low != null ? b.low : b.close;
            const c = b.close;
            const v = b.volume || 0;
            const tp = (h + l + 2.0 * c) * 0.25;
            typPrice[t] = tp;
            dollarTurnover[t] = v * tp;
            turnover[t] = mode === 'dollar' ? (v * tp) : v;

            const prevClose = t > 0 ? history[t - 1].close : (b.open != null ? b.open : c);
            const trHigh = Math.max(h, prevClose);
            const trLow = Math.min(l, prevClose);
            trueRange[t] = trHigh - trLow;

            if (t > 0) {
                const prevB = history[t - 1];
                const prevH = prevB.high != null ? prevB.high : prevB.close;
                const prevL = prevB.low != null ? prevB.low : prevB.close;
                const prevC = prevB.close;
                const cPrev = Math.log(Math.max(prevC, EPS));
                const etaPrev = 0.5 * (Math.log(Math.max(prevH, EPS)) + Math.log(Math.max(prevL, EPS)));
                const etaCurr = 0.5 * (Math.log(Math.max(h, EPS)) + Math.log(Math.max(l, EPS)));
                const s2 = 4.0 * (cPrev - etaPrev) * (cPrev - etaCurr);
                abdiSpread[t] = s2 > 0.0 ? Math.sqrt(s2) : 0.0;
            }
        }

        const peakScores = new Float64Array(T);
        const valleyScores = new Float64Array(T);
        const rvolArr = new Float64Array(T);
        const trimDollarTurnover = new Float64Array(T);

        const scratchTurnover = new Float64Array(WINDOW_LAG);
        const scratchDollar = new Float64Array(WINDOW_LAG);
        const scratchTR = new Float64Array(WINDOW_LAG);
        const scratchSpread = new Float64Array(WINDOW_LAG);

        for (let t = 0; t < T; t++) {
            if (t < WINDOW_LAG) {
                let sumD = 0.0;
                let sumDollar = 0.0;
                for (let i = 0; i <= t; i++) {
                    sumD += turnover[i];
                    sumDollar += dollarTurnover[i];
                }
                trimDollarTurnover[t] = sumDollar / (t + 1);
                rvolArr[t] = 1.0;
                continue;
            }

            for (let i = 0; i < WINDOW_LAG; i++) {
                const idx = t - WINDOW_LAG + i;
                scratchTurnover[i] = turnover[idx];
                scratchDollar[i] = dollarTurnover[idx];
                scratchTR[i] = trueRange[idx];
                scratchSpread[i] = abdiSpread[idx];
            }

            const trimMeanD = this.computeTrimmedMean21(scratchTurnover);
            const trimMeanDollar = this.computeTrimmedMean21(scratchDollar);
            const trimMeanTR = this.computeTrimmedMean21(scratchTR);
            const trimMeanSpread = this.computeTrimmedMean21(scratchSpread);

            trimDollarTurnover[t] = trimMeanDollar;
            const rvol = turnover[t] / (trimMeanD + EPS);
            rvolArr[t] = rvol;

            let maxH22 = -Infinity;
            let minL22 = Infinity;
            for (let j = t - WINDOW_LAG; j <= t; j++) {
                const barH = history[j].high != null ? history[j].high : history[j].close;
                const barL = history[j].low != null ? history[j].low : history[j].close;
                if (barH > maxH22) maxH22 = barH;
                if (barL < minL22) minL22 = barL;
            }

            // 1. Peak (Capitulation Climax) Intensity & Algebraic Sigmoid
            const curL = history[t].low != null ? history[t].low : history[t].close;
            const curC = history[t].close;
            const volElasticity = Math.pow(1.0 + rvol, 0.55) - 1.0;
            const rangePos = (maxH22 - curC) / (maxH22 - minL22 + EPS);
            const rawDownShock = Math.max(0.0, typPrice[t - 1] - curL) / (trimMeanTR + EPS);
            const shockTerm = Math.tanh(0.35 * rawDownShock);

            const isNewLow = curL <= minL22 + EPS ? 1.0 : 0.0;
            const lowerWickRatio = (curC - curL) / (trueRange[t] + EPS);
            const absorptionMult = 1.0 + 0.20 * lowerWickRatio * isNewLow;

            const priceDrawdownFactor = (0.70 * Math.pow(rangePos, 1.25) + 0.30 * shockTerm) * absorptionMult;
            const peakIntensity = volElasticity * priceDrawdownFactor;
            peakScores[t] = (100.0 * peakIntensity) / Math.sqrt(peakIntensity * peakIntensity + KAPPA_PEAK * KAPPA_PEAK);

            // 2. Valley (Liquidity Drought) Intensity & Algebraic Sigmoid
            const rDrought = trimMeanD / (turnover[t] + EPS);
            const droughtBase = Math.max(0.0, Math.pow(rDrought, 0.60) - 0.85);
            const rangeComp = Math.exp(-trueRange[t] / (2.0 * trimMeanTR + EPS));
            const rawSpreadRatio = abdiSpread[t] / (trimMeanSpread + EPS);
            const spreadMult = Math.min(2.0, Math.max(0.75, rawSpreadRatio));

            const valleyIntensity = droughtBase * rangeComp * spreadMult;
            valleyScores[t] = (100.0 * valleyIntensity) / Math.sqrt(valleyIntensity * valleyIntensity + KAPPA_VALLEY * KAPPA_VALLEY);
        }

        return {
            peakScores,
            valleyScores,
            rvolArr,
            trimDollarTurnover
        };
    },

    computeHeatGlobalPercentiles(allMetrics, mode = 'share', side = 'all') {
        const EPS = 1e-9;
        const WINDOW_LAG = 21;
        const STRUCTURAL_FLOORS = {
            PEAKS: { P99: 68.0, P95: 52.0, P90: 38.0 },
            VALLEYS: { P99: 64.0, P95: 48.0, P90: 34.0 }
        };

        const allTickers = Object.keys(allMetrics).filter(t => allMetrics[t] != null);
        if (allTickers.length === 0) {
            return {
                p90: STRUCTURAL_FLOORS.PEAKS.P90,
                p95: STRUCTURAL_FLOORS.PEAKS.P95,
                p99: STRUCTURAL_FLOORS.PEAKS.P99,
                peaks: STRUCTURAL_FLOORS.PEAKS,
                valleys: STRUCTURAL_FLOORS.VALLEYS,
                series: []
            };
        }

        let activeTickers = allTickers;
        if (side === 'long') {
            activeTickers = allTickers.filter(t => CONFIG.etfs[t] && CONFIG.etfs[t].side === 'long');
        } else if (side === 'short') {
            activeTickers = allTickers.filter(t => CONFIG.etfs[t] && CONFIG.etfs[t].side === 'short');
        } else if (allTickers.includes(side)) {
            activeTickers = [side];
        }

        if (activeTickers.length === 0) {
            return {
                p90: STRUCTURAL_FLOORS.PEAKS.P90,
                p95: STRUCTURAL_FLOORS.PEAKS.P95,
                p99: STRUCTURAL_FLOORS.PEAKS.P99,
                peaks: STRUCTURAL_FLOORS.PEAKS,
                valleys: STRUCTURAL_FLOORS.VALLEYS,
                series: []
            };
        }

        const etfMetrics = {};
        const indexMaps = {};
        let longestDates = [];

        for (const t of activeTickers) {
            const hist = allMetrics[t].heatmapData;
            if (!hist || hist.length === 0) continue;
            etfMetrics[t] = this.computeEtfMicrostructure(hist, mode);
            const map = new Map();
            const dates = [];
            for (let i = 0; i < hist.length; i++) {
                if (hist[i] && hist[i].date) {
                    map.set(hist[i].date, i);
                    dates.push(hist[i].date);
                }
            }
            indexMaps[t] = map;
            if (dates.length > longestDates.length) {
                longestDates = dates;
            }
        }

        const T = longestDates.length;
        const LAMBDA = 0.75;
        const records = [];

        for (let t = 0; t < T; t++) {
            const date = longestDates[t];
            let maxPeak = 0.0;
            let maxValley = 0.0;
            let maxRvol = 0.0;
            let weightedPeakSum = 0.0;
            let weightedValleySum = 0.0;
            let weightSum = 0.0;
            let activeCount = 0;

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

            const tieKeyPeak = compPeak + 1e-7 * maxRvol + 1e-11 * t;
            const tieKeyValley = compValley + 1e-7 * (1.0 / (maxRvol + EPS)) + 1e-11 * t;

            records.push({
                index: t,
                date,
                peakScore: compPeak,
                valleyScore: compValley,
                maxRvol,
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
            series: records
        };
    },

    renderAlertFeed(allMetrics) {
        const feed = document.getElementById('alert-feed');
        if (!feed) return;

        // Collect all alerts from all ETFs
        const allAlerts = [];
        for (const [ticker, m] of Object.entries(allMetrics)) {
            if (m && m.alerts) {
                allAlerts.push(...m.alerts);
            }
        }

        // Sort by severity
        const severityOrder = { extreme: 0, critical: 1, high: 2, elevated: 3 };
        allAlerts.sort((a, b) => (severityOrder[a.level] || 4) - (severityOrder[b.level] || 4));

        if (allAlerts.length === 0) {
            feed.innerHTML = '<div class="no-alerts">No active alerts — all volumes within normal ranges</div>';
            return;
        }

        feed.innerHTML = allAlerts.slice(0, 15).map(a => {
            const side = a.side || CONFIG.etfs[a.ticker]?.side;
            const tickerColor = side === 'long' ? 'var(--green)' : 'var(--red)';
            const icon = this._alertIcon(a.type);
            // Only show the setup badge on VCVI/CVI alerts — those are the directional ones
            const isCapAlert = a.type?.startsWith('vcvi') || a.type?.startsWith('cvi');
            const setupBadge = isCapAlert
                ? (side === 'long'
                    ? '<span class="setup-badge setup-bottom">↑ BOTTOM</span>'
                    : '<span class="setup-badge setup-top">↓ TOP</span>')
                : '';
            return `
                <div class="alert-item alert-${a.type} alert-level-${a.level}">
                    <span class="alert-time">${a.time}</span>
                    <span class="alert-ticker" style="color:${tickerColor}">${a.ticker}</span>
                    ${setupBadge}
                    <span class="alert-icon">${icon}</span>
                    <span class="alert-message">${a.message}</span>
                </div>`;
        }).join('');
    },

    renderStressMatrix(allMetrics) {
        const tbody = document.getElementById('stress-matrix-body');
        if (!tbody) return;

        tbody.innerHTML = CONFIG.pairs.map(pair => {
            const longM  = allMetrics[pair.long];
            const shortM = allMetrics[pair.short];
            const ipsi   = Metrics.computeIPSI(longM, shortM);
            const statusInfo = Metrics.computePairStatus(ipsi, longM, shortM);

            const longRvol  = longM?.drvol ?? longM?.rvol?.['21d'];
            const shortRvol = shortM?.drvol ?? shortM?.rvol?.['21d'];

            // Keep long and short VCVI separate — they signal OPPOSITE directions:
            // Long-side VCVI spike  = long ETF price low + high vol = gas BOTTOM signal
            // Short-side VCVI spike = short ETF price low + high vol = gas TOP signal
            let shortVcvi = shortM?.vcvi?.['63d'] ?? shortM?.cvi?.['63d'];
            let longVcvi  = longM?.vcvi?.['63d']  ?? longM?.cvi?.['63d'];
            let longVcvi5  = longM?.vcvi?.['5d']  ?? longM?.cvi?.['5d'];
            let shortVcvi5 = shortM?.vcvi?.['5d'] ?? shortM?.cvi?.['5d'];
            let anySpike   = longM?.sharpSpike || shortM?.sharpSpike;
            let spikeTicker = longM?.sharpSpike ? pair.long : (shortM?.sharpSpike ? pair.short : null);

            // Use enriched microstructure metrics from upgraded IPSI record if available
            if (ipsi && ipsi.latestRecord) {
                const rec = ipsi.latestRecord;
                longVcvi = rec.longMetrics.vcvi63;
                shortVcvi = rec.shortMetrics.vcvi63;
                longVcvi5 = rec.longMetrics.vcvi5;
                shortVcvi5 = rec.shortMetrics.vcvi5;
                anySpike = rec.longMetrics.sharpSpikeFlag || rec.shortMetrics.sharpSpikeFlag;
                spikeTicker = rec.longMetrics.sharpSpikeFlag ? pair.long : (rec.shortMetrics.sharpSpikeFlag ? pair.short : null);
            }

            // Vol regime: show the pair's long side vol regime (both should be similar)
            const volReg = longM?.volatility?.volRegimePct ?? shortM?.volatility?.volRegimePct;
            const regInfo = Metrics.getVolRegimeLabel(volReg);

            const rvolColor  = v => v != null ? Metrics.getValueColor(v, CONFIG.thresholds.rvol) : 'rgba(255, 255, 255, 0.85)';
            const lCapColor  = longVcvi  != null ? Metrics.getValueColor(longVcvi,  CONFIG.thresholds.vcvi) : 'rgba(255, 255, 255, 0.85)';
            const sCapColor  = shortVcvi != null ? Metrics.getValueColor(shortVcvi, CONFIG.thresholds.vcvi) : 'rgba(255, 255, 255, 0.85)';
            const ipsiColor  = ipsi != null ? Metrics.getValueColor(typeof ipsi === 'number' ? ipsi : ipsi.valueOf(), CONFIG.thresholds.ipsi) : 'rgba(255, 255, 255, 0.85)';

            const fastColor = v => {
                if (v == null) return 'rgba(255, 255, 255, 0.4)';
                if (v >= 65) return 'var(--purple)';
                if (v >= 45) return 'var(--orange)';
                if (v >= 30) return 'var(--yellow)';
                return 'rgba(255, 255, 255, 0.7)';
            };

            const ipsiVal = ipsi != null ? (typeof ipsi === 'number' ? ipsi : ipsi.valueOf()) : null;
            const ipsiTip = ipsi != null
                ? (ipsi.signed != null
                    ? `IPSI ${ipsiVal.toFixed(1)}x (Signed: ${ipsi.signed > 0 ? '+' : ''}${ipsi.signed.toFixed(1)}, Scale: ${ipsi.scale10.toFixed(1)}/10) — Regularized Split-Invariant Cross-Pair Stress Index. Centered at 1.0x (5.0). >1.5x indicates short-side volume dominance.`
                    : `IPSI ${ipsiVal.toFixed(2)}x — short RVOL ÷ long RVOL for this pair. >1.5x indicates short-side volume dominance.`)
                : 'IPSI unavailable';

            // Season badge for this pair (use long side)
            const season = longM?.seasonality?.season;
            const sw = longM?.seasonality?.weight;
            const seasonCfg = season ? (CONFIG.seasonDisplay[season] || {}) : null;
            const seasonTag = seasonCfg
                ? ` <span class="matrix-season" style="color:${seasonCfg.color}" data-tooltip="Season: ${season} ×${sw?.toFixed(2)||'1.00'}">${seasonCfg.emoji}</span>`
                : '';

            const fastBadge = `<span style="color:${fastColor(longVcvi5)}">${longVcvi5 != null ? longVcvi5.toFixed(0) : '—'}</span><span style="opacity:0.35">/</span><span style="color:${fastColor(shortVcvi5)}">${shortVcvi5 != null ? shortVcvi5.toFixed(0) : '—'}</span>`;

            const spikeBadge = anySpike
                ? `<span class="spike-badge" data-tooltip="Sharp Spike on ${spikeTicker}: Fast 5d VCVI spike with daily move >2×ATR">SPIKE</span>`
                : '—';

            return `
                <tr>
                    <td class="pair-name">${pair.label}${seasonTag}</td>
                    <td style="color:${rvolColor(longRvol)}" data-tooltip="Long ETF 21d Notional Dollar RVOL: ${longRvol != null ? longRvol.toFixed(2) + 'x' : '—'} vs 21-day EWMA baseline. Split-invariant.">${longRvol != null ? longRvol.toFixed(1) + 'x' : '—'}</td>
                    <td style="color:${rvolColor(shortRvol)}" data-tooltip="Short/Inverse ETF 21d Notional Dollar RVOL: ${shortRvol != null ? shortRvol.toFixed(2) + 'x' : '—'} vs 21-day EWMA baseline. Split-invariant.">${shortRvol != null ? shortRvol.toFixed(1) + 'x' : '—'}</td>
                    <td style="color:${lCapColor}" data-tooltip="Long-side VCVI-63d (${pair.long}) — GAS BOTTOM signal. Participation-gated. Threshold: 55 watch, 72 critical.">${longVcvi != null ? longVcvi.toFixed(0) : '—'}</td>
                    <td style="color:${sCapColor}" data-tooltip="Short-side VCVI-63d (${pair.short}) — GAS TOP signal. Participation-gated. Threshold: 55 watch, 72 critical.">${shortVcvi != null ? shortVcvi.toFixed(0) : '—'}</td>
                    <td data-tooltip="5d fast-window VCVI — Long: ${longVcvi5!=null?longVcvi5.toFixed(0):'—'} | Short: ${shortVcvi5!=null?shortVcvi5.toFixed(0):'—'}. Threshold: 45.">${fastBadge}</td>
                    <td>${spikeBadge}</td>
                    <td style="color:${ipsiColor}" data-tooltip="${ipsiTip}">${ipsiVal != null ? ipsiVal.toFixed(1) + 'x' : '—'}</td>
                    <td data-tooltip="Volatility regime: ${regInfo.label}"><span class="vol-regime-badge ${regInfo.cls}" style="font-size:0.6rem">${regInfo.label}</span></td>
                    <td data-tooltip="${statusInfo.tooltip}"><span class="stress-status ${statusInfo.cls}">${statusInfo.label}</span></td>
                </tr>`;
        }).join('');
    },

    renderHeatCalendar(allMetrics, offset = 0, mode = 'share', side = 'all', spotlightMode = 'off', peaksThreshold = 95, globalPercentiles = null, days = 90) {
        const container = document.getElementById('heat-calendar');
        if (!container) return;

        // If mode/side/days not explicitly passed, fallback to window.App values
        if (window.App) {
            if (arguments.length < 3) mode = window.App.heatMode || 'share';
            if (arguments.length < 4) side = window.App.heatSide || 'all';
            if (arguments.length < 5) spotlightMode = window.App.heatSpotlightMode || (window.App.heatPeaksMode ? 'peaks' : 'off');
            if (arguments.length < 6) peaksThreshold = window.App.heatPeaksThreshold || 95;
            if (arguments.length < 7 || !globalPercentiles) {
                const globalObj = window.App.heatGlobalPercentiles;
                globalPercentiles = (globalObj && globalObj[mode]) ? globalObj[mode][side] : null;
            }
            if (arguments.length < 8 && window.App) days = window.App.heatDays || 90;
        }
        if (spotlightMode === true) spotlightMode = 'peaks';
        else if (!spotlightMode) spotlightMode = 'off';

        if (!globalPercentiles || !globalPercentiles.series || globalPercentiles.series.length === 0) {
            globalPercentiles = this.computeHeatGlobalPercentiles(allMetrics, mode, side);
        }

        const series = globalPercentiles?.series || [];
        if (series.length === 0) {
            container.innerHTML = '<div style="grid-column: 1 / -1; padding: 24px; text-align: center; color: var(--text-muted); font-size: 0.85rem; font-style: italic;">No active data for the selected filter.</div>';
            return;
        }

        // Pagination controls
        const maxOffset = Math.max(0, Math.floor((series.length - 1) / days));
        offset = Math.max(0, Math.min(offset, maxOffset));

        const hp = document.getElementById('heat-prev');
        const hn = document.getElementById('heat-next');
        const titleEl = document.getElementById('heat-calendar-title');

        const startIndex = Math.max(0, series.length - (offset + 1) * days);
        const endIndex = series.length - offset * days;
        const visibleSlice = series.slice(startIndex, endIndex);

        if (titleEl) {
            const startDay = offset * days + 1;
            const endDay = Math.min((offset + 1) * days, series.length);
            const modeLabel = mode === 'dollar' ? 'DOLLAR VOL' : 'VOLUME';
            const subjectLabel = (side === 'all') ? '' : ' [' + side.replace('.TO', '').replace('.L', '') + ']';
            let dateRangeStr = '';
            if (visibleSlice.length > 0) {
                const d1 = visibleSlice[0].date;
                const d2 = visibleSlice[visibleSlice.length - 1].date;
                const fmt1 = typeof formatDisplayDate === 'function' ? formatDisplayDate(d1) : d1;
                const fmt2 = typeof formatDisplayDate === 'function' ? formatDisplayDate(d2) : d2;
                dateRangeStr = ' · ' + fmt1 + ' – ' + fmt2;
            }
            titleEl.innerHTML = modeLabel + ' HEAT MAP' + subjectLabel + ' <span style="font-size:0.54rem; font-weight:normal; color:rgba(255,255,255,0.45); text-transform:none; margin-left:2px;">(' + startDay + '–' + endDay + 'd' + dateRangeStr + ')</span>';
        }

        if (hp) {
            const isPrevDisabled = (offset >= maxOffset);
            hp.disabled = isPrevDisabled;
            if (isPrevDisabled) {
                hp.setAttribute('data-tooltip', 'No older data available');
            } else {
                const prevStart = (offset + 1) * days + 1;
                const prevEnd = Math.min((offset + 2) * days, series.length);
                const prevStartIdx = Math.max(0, series.length - (offset + 2) * days);
                const prevEndIdx = series.length - (offset + 1) * days;
                const prevSlice = series.slice(prevStartIdx, prevEndIdx);
                let prevRange = '';
                if (prevSlice.length > 0) {
                    const pd1 = typeof formatDisplayDate === 'function' ? formatDisplayDate(prevSlice[0].date) : prevSlice[0].date;
                    const pd2 = typeof formatDisplayDate === 'function' ? formatDisplayDate(prevSlice[prevSlice.length - 1].date) : prevSlice[prevSlice.length - 1].date;
                    prevRange = ' (' + pd1 + ' – ' + pd2 + ')';
                }
                hp.setAttribute('data-tooltip', 'View older ' + days + ' days: Days ' + prevStart + '–' + prevEnd + prevRange);
            }
        }
        if (hn) {
            const isNextDisabled = (offset === 0);
            hn.disabled = isNextDisabled;
            if (isNextDisabled) {
                hn.setAttribute('data-tooltip', 'Currently viewing most recent ' + days + ' days (Days 1–' + days + ')');
            } else {
                const nextStart = (offset - 1) * days + 1;
                const nextEnd = offset * days;
                const nextStartIdx = Math.max(0, series.length - offset * days);
                const nextEndIdx = series.length - (offset - 1) * days;
                const nextSlice = series.slice(nextStartIdx, nextEndIdx);
                let nextRange = '';
                if (nextSlice.length > 0) {
                    const nd1 = typeof formatDisplayDate === 'function' ? formatDisplayDate(nextSlice[0].date) : nextSlice[0].date;
                    const nd2 = typeof formatDisplayDate === 'function' ? formatDisplayDate(nextSlice[nextSlice.length - 1].date) : nextSlice[nextSlice.length - 1].date;
                    nextRange = ' (' + nd1 + ' – ' + nd2 + ')';
                }
                hn.setAttribute('data-tooltip', 'View newer ' + days + ' days: Days ' + nextStart + '–' + nextEnd + nextRange);
            }
        }

        const spotlightCount = Charts.drawHeatCalendar(container, visibleSlice, mode, spotlightMode, peaksThreshold, globalPercentiles, side);
        const peakBadge = document.getElementById('heat-peak-count');
        if (peakBadge) {
            if (spotlightMode === 'peaks') {
                peakBadge.textContent = spotlightCount + ' ' + (spotlightCount === 1 ? 'PEAK' : 'PEAKS');
                peakBadge.className = 'heat-peak-count visible peak-style';
            } else if (spotlightMode === 'valleys') {
                peakBadge.textContent = spotlightCount + ' ' + (spotlightCount === 1 ? 'VALLEY' : 'VALLEYS');
                peakBadge.className = 'heat-peak-count visible valley-style';
            } else {
                peakBadge.className = 'heat-peak-count';
            }
        }
    },

    renderConvergenceGauges(allMetrics) {
        const container = document.getElementById('convergence-gauges');
        if (!container) return;

        const windows = typeof CONFIG !== 'undefined' ? CONFIG.windows.percentile : [5, 10, 21, 63, 126, 252];
        const total = windows.length;
        const threshold = typeof CONFIG !== 'undefined' ? CONFIG.thresholds.mwca_threshold : 90;
        const tickers = typeof CONFIG !== 'undefined' ? Object.keys(CONFIG.etfs) : Object.keys(allMetrics || {});

        container.innerHTML = tickers.map(t => {
            const m = allMetrics[t];
            const cfg = typeof CONFIG !== 'undefined' ? (CONFIG.etfs[t] || {}) : {};
            const side = cfg.side || 'long';
            const count = m ? (m.mwcaCount || 0) : 0;
            const volPct = m ? (m.volPercentile || {}) : {};

            const winDetails = windows.map(w => {
                const val = volPct[`${w}d`];
                const isAlert = val != null && val >= threshold;
                return `${w}d: ${val != null ? val.toFixed(1) + '%' : '—'}${isAlert ? ' [ALERT ≥90%]' : ''}`;
            });

            const statusSummary = count === total
                ? 'ALARM — all 6 windows converging! Structural capitulation/accumulation.'
                : count >= total - 1
                    ? 'Near-alarm — 1 window short of full MWCA convergence.'
                    : count >= 3
                        ? 'Elevated multi-window volume elevation.'
                        : count > 0
                            ? 'Partial window elevation.'
                            : 'Quiet — all windows below 90th percentile.';

            const tip = `${t} (${side.toUpperCase()} ${cfg.leverage || ''} · ${cfg.exchange || ''}): ${count}/${total} windows in alert (≥${threshold}th vol pct). ${statusSummary} [${winDetails.join(' | ')}]`;

            return Charts.createGaugeRing(count, total, t, tip, {
                side,
                cfg,
                volPercentile: volPct,
                windows,
                threshold
            });
        }).join('');
    },

    renderCorrelationBars(allMetrics) {
        const container = document.getElementById('correlation-bars');
        if (!container) return;

        // Order: long first, then short
        const orderedTickers = Object.keys(CONFIG.etfs).sort((a, b) => {
            const sideA = CONFIG.etfs[a].side === 'long' ? 0 : 1;
            const sideB = CONFIG.etfs[b].side === 'long' ? 0 : 1;
            return sideA - sideB;
        });

        container.innerHTML = orderedTickers.map(t => {
            const m = allMetrics[t];
            const corr = m ? m.rollingCorr : null;
            return Charts.createCorrelationBar(t, corr, CONFIG.etfs[t].side);
        }).join('');
    },

    renderValidationBanner(allMetrics) {
        const grid = document.getElementById('validation-grid');
        if (!grid) return;

        grid.innerHTML = Object.keys(CONFIG.etfs).map(t => {
            const m = allMetrics[t];
            const corr = m ? m.rollingCorr : null;
            const side = CONFIG.etfs[t].side;
            // Negative correlation = inverse relationship active (green); positive = co-directional (red)
            const color = corr != null
                ? (corr < -0.2 ? 'var(--green)' : corr < -0.1 ? 'rgba(255, 255, 255, 0.85)' : corr > 0.1 ? 'var(--red)' : 'rgba(255,255,255,0.85)')
                : 'rgba(255,255,255,0.85)';
            const tickerColor = side === 'long' ? 'var(--green)' : 'var(--red)';
            const tipText = `${t} tracking validation card. 30-day Spearman correlation to NG spot is ${corr != null ? corr.toFixed(3) : 'N/A'}. ${side === 'short' ? 'Inverse ETF requires negative correlation to be effective.' : 'Long ETF requires positive correlation to be effective.'}`;

            return `
                <div class="validation-card" data-tooltip="${tipText}">
                    <div class="ticker" style="color:${tickerColor}">${t}</div>
                    <div class="corr-value" style="color:${color}">${corr != null ? corr.toFixed(3) : '—'}</div>
                    <div class="corr-label">30d Spearman</div>
                    <div class="pvalue ${corr != null && corr < -0.2 ? 'pv-confirmed' : corr != null && corr < 0 ? 'pv-weak' : 'pv-none'}">${corr != null && corr < -0.2 ? 'INVERSE' : corr != null && corr < 0 ? 'WEAK' : '—'}</div>
                </div>`;
        }).join('');
    },

    // ---- SIDE-WIDE VOLUME CONVERGENCE (SWVC) ----
    renderSideConvergence(sideConvergence) {
        const container = document.getElementById('swvc-container');
        if (!container) return;

        if (!sideConvergence) {
            container.innerHTML = '<div class="swvc-empty">No convergence data available.</div>';
            return;
        }

        const sides = [
            { key: 'long',  label: 'LONG SIDE',  tickers: ['BOIL', 'HNU.TO', '3NGL.L'] },
            { key: 'short', label: 'SHORT SIDE', tickers: ['KOLD', 'HND.TO', '3NGS.L'] },
        ];

        const html = sides.map(({ key, label, tickers }) => {
            const sc = sideConvergence[key];
            if (!sc) return '';

            const status     = sc.status || 'quiet';
            const score      = sc.score  || 0;
            const total      = sc.total  || 3;
            const lookback   = sc.lookback_days  || 15;
            const windowDays = sc.window_days || 10;
            const spread     = sc.window_spread_days;
            const threshold  = sc.threshold_rvol  || 2.0;

            const statusColors = {
                converged: 'var(--red)',
                partial:   'var(--orange)',
                single:    'var(--yellow)',
                quiet:     'rgba(255,255,255,0.85)',
            };
            const statusLabels = {
                converged: 'CONVERGED',
                partial:   'PARTIAL',
                single:    'SINGLE',
                quiet:     'QUIET',
            };
            const statusColor = statusColors[status] || 'rgba(255,255,255,0.85)';
            const statusLabel = statusLabels[status] || 'QUIET';

            // Score pills: one card per ETF
            const pills = tickers.map(t => {
                const etf = (sc.etfs || {})[t] || {};
                const spiked = etf.spiked;
                const daysAgo = etf.days_ago != null ? etf.days_ago : null;
                const rvol = etf.peak_rvol;
                const date = etf.date || '—';
                const cfg = (typeof CONFIG !== 'undefined' ? CONFIG.etfs[t] : (window.CONFIG?.etfs?.[t] || {})) || {};
                const region = cfg.exchange === 'NYSE' ? 'US' : cfg.exchange === 'TSX' ? 'CA' : cfg.exchange === 'LSE' ? 'UK' : (cfg.exchange || '');
                const lev = cfg.leverage || '';
                const regBadge = region && lev ? `${region} · ${lev}` : (region || lev || '');

                const pillBg = spiked
                    ? (key === 'long' ? 'rgba(34, 197, 94, 0.16)' : 'rgba(239, 68, 68, 0.16)')
                    : 'var(--bg-panel)';
                const pillBorder = spiked
                    ? (key === 'long' ? 'rgba(34, 197, 94, 0.6)' : 'rgba(239, 68, 68, 0.6)')
                    : 'var(--border-primary)';
                const pillTickerColor = spiked
                    ? (key === 'long' ? 'var(--green)' : 'var(--red)')
                    : 'rgba(255, 255, 255, 0.85)';
                const pillDaysColor = spiked
                    ? (key === 'long' ? 'var(--green)' : 'var(--red)')
                    : 'rgba(255, 255, 255, 0.45)';

                const tip = spiked
                    ? `${t} (${region} · ${lev}): Spiked to ${rvol?.toFixed(2)}× RVOL-21d on ${typeof formatDisplayDate === 'function' ? formatDisplayDate(date) : date} (${daysAgo === 0 ? 'today' : `${daysAgo}d ago`}). Active within ${lookback}-day scan lookback.`
                    : `${t} (${region} · ${lev}): Quiet. No volume spike ≥${threshold}× in last ${lookback} trading days.`;

                const daysTxt = spiked
                    ? `${daysAgo === 0 ? 'Today' : `${daysAgo}d ago`} · ${rvol != null ? rvol.toFixed(1) + '×' : '≥2×'}`
                    : 'quiet (<2×)';

                return `<div class="swvc-pill ${spiked ? 'spiked' : ''}"
                             style="background:${pillBg};border-color:${pillBorder}"
                             data-tooltip="${tip}">
                    <div class="swvc-pill-top">
                        <span class="swvc-pill-ticker" style="color:${pillTickerColor}">${t}</span>
                        <span class="swvc-pill-region">${regBadge}</span>
                    </div>
                    <span class="swvc-pill-days" style="color:${pillDaysColor}">${daysTxt}</span>
                </div>`;
            }).join('');

            // Timeline strip: last `lookback` trading days, mark spike positions
            const timelineCells = Array.from({ length: lookback }, (_, i) => {
                const dayOffset = lookback - 1 - i;  // 0 = today, lookback-1 = oldest
                const spikeTickers = tickers.filter(t => {
                    const etf = (sc.etfs || {})[t] || {};
                    return etf.spiked && etf.days_ago === dayOffset;
                });
                const hasSpike = spikeTickers.length > 0;
                const isToday  = dayOffset === 0;
                const cellColor = hasSpike
                    ? (key === 'short' ? 'var(--red)' : 'var(--green)')
                    : isToday ? 'rgba(255,255,255,0.06)' : 'transparent';

                const spikeDetails = spikeTickers.map(t => {
                    const e = (sc.etfs || {})[t] || {};
                    return `${t} (${e.peak_rvol ? e.peak_rvol.toFixed(2) + '×' : '≥2×'} RVOL on ${typeof formatDisplayDate === 'function' ? formatDisplayDate(e.date) : (e.date || '—')})`;
                }).join(', ');

                const tip = hasSpike
                    ? `${dayOffset === 0 ? 'Today' : `Trading day −${dayOffset}d`}: ${spikeDetails}`
                    : isToday ? 'Today (session in progress — no spike ≥2.0× RVOL)' : `Trading day −${dayOffset}d (quiet — no spike ≥2.0× RVOL)`;

                return `<div class="swvc-cell ${hasSpike ? 'spike' : ''} ${isToday ? 'today' : ''}"
                             style="background:${cellColor}"
                             data-tooltip="${tip}"></div>`;
            }).join('');

            let spreadTxt = '';
            let spreadTip = '';
            if (status === 'converged') {
                spreadTxt = `3/3 CONVERGED within ${spread}d (window: ${windowDays}d)`;
                spreadTip = `All 3 ETFs on this side spiked within ${spread} calendar days (window limit: ${windowDays}d). Independent US, Canadian, and UK volume capitulation confirmed.`;
            } else if (score === 2) {
                spreadTxt = `2/3 spiked · ${spread}d spread (window: ${windowDays}d)`;
                spreadTip = `2 of 3 ETFs spiked within ${spread} calendar days of each other. Awaiting the 3rd ETF to confirm within a ${windowDays}-day window for full convergence.`;
            } else if (score === 1) {
                spreadTxt = `1/3 spiked (awaiting 2 within ${windowDays}d)`;
                spreadTip = `Only 1 ETF has spiked in the last ${lookback} trading days. Full convergence requires all 3 ETFs (US/CA/UK) within ${windowDays} days.`;
            } else if (score === 3) {
                spreadTxt = `3/3 spiked · ${spread}d spread (exceeds ${windowDays}d window)`;
                spreadTip = `All 3 ETFs spiked in the last ${lookback} trading days, but the spread of ${spread} calendar days exceeds the ${windowDays}-day convergence window.`;
            } else {
                spreadTxt = `Quiet · no spikes in ${lookback}d lookback`;
                spreadTip = `No ETF on this side has exceeded the ${threshold}× RVOL-21d threshold in the last ${lookback} trading days.`;
            }

            const midOffset = Math.round((lookback - 1) / 2);

            return `
                <div class="swvc-side swvc-${key}">
                    <div class="swvc-side-header">
                        <span class="swvc-side-label">${label}</span>
                        <span class="swvc-status-badge" style="color:${statusColor}">${statusLabel}</span>
                        <span class="swvc-spread-txt" data-tooltip="${spreadTip}">${spreadTxt}</span>
                    </div>
                    <div class="swvc-pills">${pills}</div>
                    <div class="swvc-timeline-wrap">
                        <div class="swvc-timeline">${timelineCells}</div>
                        <div class="swvc-timeline-labels">
                            <span class="swvc-lbl-oldest">−${lookback - 1}d</span>
                            <span class="swvc-lbl-mid">−${midOffset}d</span>
                            <span class="swvc-lbl-today">TODAY</span>
                        </div>
                    </div>
                    ${status === 'converged' ? `
                    <div class="swvc-alert-banner" style="border-color:${statusColor}">
                        All 3 ${key}-side ETFs (US / CA / UK) spiked within ${spread} calendar days
                        — independent cross-market capitulation signal
                    </div>` : ''}
                </div>`;
        }).join('');

        container.innerHTML = html ||
            '<div class="swvc-empty">No data available.</div>';
    },

    // ---- NG PRICE CONTEXT BAR (Feature 2) ----
    renderNgPriceBar(ngPriceContext) {
        const bar = document.getElementById('ng-price-bar');
        if (!bar) return;

        if (!ngPriceContext || ngPriceContext.price == null) {
            bar.innerHTML = '<span class="ng-bar-label" data-tooltip="NYMEX Henry Hub Natural Gas Front-Month Prompt Futures ($/MMBtu). Physical benchmark for all natural gas derivative pricing.">NG=F</span><span class="ng-bar-na">— price data unavailable</span>';
            return;
        }

        const p = ngPriceContext.price;
        const sz  = ngPriceContext.seasonal_zscore;
        const tier = ngPriceContext.tier || 'seasonal_mid';
        const gateShort = ngPriceContext.gate_short;
        const gateLong  = ngPriceContext.gate_long;
        const note = ngPriceContext.seasonal_note || '';

        // Tier color: red = seasonally high, green = seasonally low, muted = mid
        const tierColors = {
            extreme_high: 'var(--purple)', seasonal_high: 'var(--red)',
            extreme_low: 'var(--green)',   seasonal_low:  'var(--blue)',
            seasonal_mid: 'rgba(255, 255, 255, 0.85)'
        };
        const tierColor = tierColors[tier] || 'rgba(255, 255, 255, 0.85)';

        // Seasonal commodity phase
        let seasonKey = ngPriceContext.season;
        if (!seasonKey && note) {
            const m = note.match(/\b(winter|spring|summer|fall)\b/i);
            if (m) seasonKey = m[1].toLowerCase();
        }
        if (!seasonKey && typeof window !== 'undefined' && window.App && window.App.data && window.App.data.etfs) {
            for (const t of Object.keys(window.App.data.etfs)) {
                const s = window.App.data.etfs[t]?.seasonality?.season;
                if (s) { seasonKey = s.toLowerCase(); break; }
            }
        }
        if (!seasonKey) {
            const m = new Date().getUTCMonth() + 1;
            seasonKey = (m === 11 || m === 12 || m === 1 || m === 2) ? 'winter'
                      : (m >= 3 && m <= 5) ? 'spring'
                      : (m >= 6 && m <= 8) ? 'summer' : 'fall';
        }

        const seasonPhases = {
            winter: {
                label: 'WINTER (Withdrawal Phase)',
                color: '#60a8f8',
                tip: 'Physical Seasonal Phase: Winter peak withdrawal period (Nov–Feb). Space heating demand draws down storage inventories, driving physical cash price volatility.'
            },
            spring: {
                label: 'SPRING (Shoulder Phase)',
                color: '#6ddc8b',
                tip: 'Physical Seasonal Phase: Spring shoulder transition period (Mar–May). Mild weather transitions the gas grid from net storage withdrawals to early injections.'
            },
            summer: {
                label: 'SUMMER (Injection Phase)',
                color: '#f5c542',
                tip: 'Physical Seasonal Phase: Summer storage injection and peak electric power burn for air conditioning (Jun–Aug).'
            },
            fall: {
                label: 'FALL (Injection Phase)',
                color: '#f5a742',
                tip: 'Physical Seasonal Phase: Fall storage injection period (Sep–Oct). Final storage build prior to the winter withdrawal season.'
            }
        };
        const phaseInfo = seasonPhases[seasonKey] || seasonPhases.fall;
        const phaseHtml = `<span class="ng-season-phase" style="color:${phaseInfo.color}" data-tooltip="${phaseInfo.tip}">${phaseInfo.label}</span>`;

        // Z-score bar: center=0, ±3σ fills the track. Clamp to -3..+3.
        const zClamped   = Math.max(-3, Math.min(3, sz ?? 0));
        const fillLeft   = sz != null ? ((zClamped / 3) * 50 + 50) : 50;  // pct from left
        const fillWidth  = sz != null ? Math.abs(zClamped / 3 * 50) : 0;
        const fillStart  = sz != null ? (sz >= 0 ? 50 : fillLeft) : 50;

        // Z-score label: +1.8σ above seasonal norm
        const zLabel = sz != null ? `${sz >= 0 ? '+' : ''}${sz.toFixed(1)}σ` : '—σ';

        // Parse metadata from seasonal_note
        const gMeanMatch = note.match(/gMean=\$([0-9.]+)/);
        const normPriceStr = gMeanMatch ? `$${gMeanMatch[1]}` : '$3.30';
        const obsMatch = note.match(/vs\s+(\d+)\s+([a-zA-Z]+)-month/);
        const obsCount = obsMatch ? obsMatch[1] : '237';
        const monthNum = ngPriceContext.month || (new Date().getUTCMonth() + 1);
        const monthName = new Date(Date.UTC(2026, monthNum - 1, 1)).toLocaleString('en-US', { month: 'long', timeZone: 'UTC' });

        const zTip = `Seasonal Z-Score vs 10-year same-month historical norm (${normPriceStr}/MMBtu). Log-normal distribution of ~${obsCount} ${monthName} sessions.`;

        // Macro Gates resolution
        const neutralGateTip = `Macro Gate: NEUTRAL (-1.5σ < z < +1.5σ). Physical gas is in fair seasonal valuation range (${normPriceStr} ${monthName} norm). Volume capitulation signals operate without directional macro bias.`;
        const longGateTip = 'Macro Gate: ACTIVE LONG (z <= -1.5σ). Physical natural gas is deeply oversold vs seasonal norm, fundamentally supporting Long ETF mean-reversion setups.';
        const shortGateTip = 'Macro Gate: ACTIVE SHORT (z >= +1.5σ). Physical natural gas is deeply overbought vs seasonal norm, fundamentally supporting Short ETF mean-reversion setups.';
        const shortInactiveTip = 'Macro Gate: Short gate INACTIVE. Physical gas is not seasonally overbought; short mean-reversion setups lack macro tailwind.';
        const longInactiveTip = 'Macro Gate: Long gate INACTIVE. Physical gas is not seasonally oversold; long mean-reversion setups lack macro tailwind.';

        let gatesHtml = '';
        if (gateLong === true) {
            gatesHtml = `<span class="ng-gate ng-gate-active-long" data-tooltip="${longGateTip}">LONG GATE: ACTIVE</span><span class="ng-gate ng-gate-inactive" data-tooltip="${shortInactiveTip}">SHORT: INACTIVE</span>`;
        } else if (gateShort === true) {
            gatesHtml = `<span class="ng-gate ng-gate-active-short" data-tooltip="${shortGateTip}">SHORT GATE: ACTIVE</span><span class="ng-gate ng-gate-inactive" data-tooltip="${longInactiveTip}">LONG: INACTIVE</span>`;
        } else {
            // Neutral state (-1.5σ < z < +1.5σ or both false)
            gatesHtml = `<span class="ng-gate ng-gate-neutral" data-tooltip="${neutralGateTip}">GATES: NEUTRAL</span>`;
        }

        // Regime badge
        const regime = ngPriceContext.regime || 'normal';
        const regimeCfg = (CONFIG.thresholds.ngRegime || {})[regime] || CONFIG.thresholds.ngRegime?.normal || {};
        const regimeColor = regimeCfg.color || 'rgba(255,255,255,0.85)';
        const regimeLabel = regimeCfg.label || regime.toUpperCase();
        const regimeNote  = regimeCfg.note  || '';
        const regimeTip = `Physical Gas Volatility Regime (NORMAL / ELEVATED STRESS / ULTRA-VOLATILE) based on absolute price, seasonal z-score, and 30-day realized volatility. Current: ${regimeLabel}. ${regimeNote}`;
        const regimeBadge = `<span class="ng-regime-badge ng-regime-${regime}" style="color:${regimeColor};border-color:${regimeColor}" data-tooltip="${regimeTip}">${regimeLabel}</span>`;

        // Extreme regime warning strip
        const regimeWarning = regime === 'extreme'
            ? `<div class="ng-regime-warning" style="color:${regimeColor}">EXTREME REGIME — ${regimeNote}. Historical signal outcomes may not reflect behavior in this environment.</div>`
            : regime === 'elevated'
            ? `<div class="ng-regime-warning ng-regime-warning-dim" style="color:${regimeColor}">${regimeNote}</div>`
            : '';

        bar.innerHTML = `
            <span class="ng-bar-label" data-tooltip="NYMEX Henry Hub Natural Gas Front-Month Prompt Futures ($/MMBtu). Physical benchmark for all natural gas derivative pricing.">NG=F</span>
            ${phaseHtml}
            <span class="ng-bar-price" style="color:${tierColor}" data-tooltip="Current NYMEX prompt month settlement price.">$${p.toFixed(3)}</span>
            <div class="ng-bar-track ng-bar-zscore" data-tooltip="${zTip}">
                <div class="ng-bar-center-mark"></div>
                ${sz != null ? `<div class="ng-bar-fill" style="left:${fillStart}%;width:${fillWidth}%;background:${tierColor}"></div>` : ''}
                <div class="ng-bar-z-neg15" style="left:25%"></div>
                <div class="ng-bar-z-pos15" style="left:75%"></div>
            </div>
            <span class="ng-bar-pct" style="color:${tierColor}" data-tooltip="${zTip}">${zLabel}</span>
            <span class="ng-gates">${gatesHtml}</span>
            ${regimeBadge}
            ${regimeWarning}`;
    },

    // ---- TOP-OF-PAGE CONVERGENCE FLASH BANNER ----
    // Shown when any side reaches CONVERGED status — hard to miss regardless
    // of where the user is scrolled to.
    // Handles Bilateral Climax Resolution if BOTH sides converge simultaneously.
    renderConvergenceFlash(sideConvergence, ngPriceContext, allMetrics) {
        const el = document.getElementById('convergence-flash');
        if (!el) return;

        if (!sideConvergence) { el.style.display = 'none'; return; }

        const shortConverged = (sideConvergence.short || {}).status === 'converged';
        const longConverged  = (sideConvergence.long  || {}).status === 'converged';

        if (!shortConverged && !longConverged) {
            el.style.display = 'none';
            return;
        }

        // Case 1: BILATERAL CLIMAX (Both Long and Short ETFs spiked simultaneously)
        if (shortConverged && longConverged) {
            const scShort = sideConvergence.short;
            const scLong  = sideConvergence.long;
            const shortSpread = scShort.window_spread_days ?? 7;
            const longSpread  = scLong.window_spread_days ?? 7;
            const maxSpread   = Math.max(shortSpread, longSpread);

            const sz = ngPriceContext?.seasonal_zscore;
            const tier = ngPriceContext?.tier;
            const pct2yr = ngPriceContext?.percentile_2yr;
            const gateShort = ngPriceContext?.gate_short;
            const gateLong  = ngPriceContext?.gate_long;

            // Trend determination:
            // 15-year empirical backtest (2011-2026, N=74):
            // Dual spikes at cycle highs mark a Parabolic Blow-Off Top (BOIL -19.7% avg 63d, KOLD +13.9% win rate 63%).
            // Dual spikes at cycle lows mark a Liquidation Trap / False Bottom (BOIL drops -31.7% with 85.7% loss rate).
            const isHigh = (gateShort === true) || tier === 'extreme_high' || tier === 'seasonal_high' || (sz != null && sz >= 0.5) || (pct2yr != null && pct2yr >= 60);
            const isLow  = (gateLong === true)  || tier === 'extreme_low'  || tier === 'seasonal_low'  || (sz != null && sz <= -0.5) || (pct2yr != null && pct2yr <= 40);

            if (isHigh) {
                el.innerHTML = `
                    <div class="convergence-flash-inner flash-dual-top">
                        <span class="flash-icon">[CLIMAX]</span>
                        <span class="flash-body">
                            <strong>DUAL LEVERAGED CLIMAX: PARABOLIC BLOW-OFF TOP WATCH</strong> — Both Long & Short ETFs Spiked Across All Exchanges (Spread: ${maxSpread}d)
                            <span class="flash-etfs">Empirical 15-yr edge: Long ETFs drop −19.7% over 63d (win rate 27.4%), Short ETFs (KOLD) gain +13.9% (t=+2.36, p=0.02). Natural Gas blow-off climax in progress.</span>
                        </span>
                        <span class="flash-setup">EXTREME GAS TOP PROBABILITY</span>
                    </div>`;
            } else if (isLow) {
                el.innerHTML = `
                    <div class="convergence-flash-inner flash-dual-waterfall">
                        <span class="flash-icon">[CRITICAL]</span>
                        <span class="flash-body">
                            <strong>DUAL VOLUME TURNOVER: LIQUIDATION WATERFALL (FALSE BOTTOM WARNING)</strong> — High Churn at Cycle Lows Across All Exchanges (Spread: ${maxSpread}d)
                            <span class="flash-etfs">Empirical 15-yr edge: Dual climaxes at lows have an 85.7% failure rate for Longs (−31.7% avg 63d drop). Downside cascade ongoing; do NOT buy bottom dip.</span>
                        </span>
                        <span class="flash-setup">CASCADE WARNING — AVOID LONGS</span>
                    </div>`;
            } else {
                el.innerHTML = `
                    <div class="convergence-flash-inner flash-neutral">
                        <span class="flash-icon">[WATCH]</span>
                        <span class="flash-body">
                            <strong>BILATERAL LIQUIDITY EXPANSION: TWO-SIDED CHURN</strong> — Long and Short ETFs active simultaneously (Spread: ${maxSpread}d)
                            <span class="flash-etfs">Institutional position rebalancing / calendar roll turnover. Two-sided volume without directional consensus. Await confirmed directional break.</span>
                        </span>
                        <span class="flash-setup">BALANCED TURNOVER — NO BIAS</span>
                    </div>`;
            }
            el.style.display = 'block';
            return;
        }

        // Case 2: SINGLE SIDE CONVERGED
        const sides = [
            { key: 'short', label: 'SHORT SIDE', direction: 'GAS TOP', setupLabel: 'SHORT / INVERSE SETUP', cls: 'flash-short' },
            { key: 'long',  label: 'LONG SIDE',  direction: 'GAS BOTTOM', setupLabel: 'LONG / LEVERAGED SETUP', cls: 'flash-long' },
        ];

        const banners = sides
            .filter(s => (sideConvergence[s.key] || {}).status === 'converged')
            .map(s => {
                const sc = sideConvergence[s.key];
                const spread = sc.window_spread_days;
                const etfList = Object.entries(sc.etfs || {})
                    .filter(([, v]) => v.spiked)
                    .map(([t, v]) => `${t} (${v.days_ago}d ago, ${v.peak_rvol?.toFixed(1)}×)`)
                    .join(' · ');
                return `<div class="convergence-flash-inner ${s.cls}">
                    <span class="flash-icon">[CONVERGED]</span>
                    <span class="flash-body">
                        <strong>${s.label} CONVERGED</strong> — All 3 ETFs spiked within ${spread} calendar days
                        <span class="flash-etfs">${etfList}</span>
                    </span>
                    <span class="flash-setup">${s.setupLabel} — ${s.direction} CANDIDATE</span>
                </div>`;
            });

        el.innerHTML = banners.join('');
        el.style.display = 'block';
    },

    // ---- VDDS CROSS-ETF COMPARISON BAR ----
    // mode: 'vol' = show S-RVOL-21d (pure share volume bar, centre=1.0, range 0.5–3x)
    //        'dv'  = show VDDS ratio DV-RVOL-21d / S-RVOL-21d (original mode)
    renderVddsBar(allMetrics, mode) {
        const el = document.getElementById('vdds-bar');
        if (!el) return;
        const tickers = Object.keys(allMetrics);
        if (!tickers.length) { el.innerHTML = '<span class="vdds-na">No data</span>'; return; }

        const isVolMode = (mode !== 'dv');

        // Update scale legend header above the rows
        const legendEl = document.getElementById('vdds-scale-legend');
        if (legendEl) {
            if (isVolMode) {
                legendEl.innerHTML = `
                    <span>ETF</span>
                    <div class="vdds-scale-track">
                        <span>0x</span>
                        <span class="vdds-scale-mark" style="left:33.33%;">1.0x NORMAL</span>
                        <span>3.0x+</span>
                    </div>
                    <span style="text-align:right;">RVOL</span>`;
            } else {
                legendEl.innerHTML = `
                    <span>ETF</span>
                    <div class="vdds-scale-track">
                        <span>◀ 0.5x CAPITULATION</span>
                        <span class="vdds-scale-mark" style="left:50%;">1.0x BALANCED</span>
                        <span>MOMENTUM 1.5x ▶</span>
                    </div>
                    <span style="text-align:right;">VDDS</span>`;
            }
        }

        const rows = tickers.map(ticker => {
            const m = allMetrics[ticker];
            if (!m) return '';
            const side = (CONFIG.etfs[ticker] || {}).side || 'long';
            const sideColor = side === 'long' ? 'var(--long-accent)' : 'var(--short-accent)';

            let displayVal, barColor, tip, fillStart, fillWidth;

            if (isVolMode) {
                // --- VOL MODE: Notional Dollar RVOL (21d) with Soft-Winsorized Track ---
                // Scale: 0.0x → 0% | 1.0x → 33.33% (baseline tick) | 3.0x+ → 100%
                const val = m.drvol ?? (m.rvol || {})['21d'] ?? 0;
                const clamped = Metrics._softWinsorizeVolTrack(val, 2.25, 3.0, 1.5);
                fillStart = 0;
                fillWidth = (clamped / 3.0) * 100;
                barColor = Metrics._resolveVolModeColorBand(val);
                displayVal = val != null ? val.toFixed(2) + 'x' : '—';
                tip = `Notional Dollar RVOL (21d) for ${ticker}: ${displayVal} vs 21-day EWMA baseline. Split-invariant. 1.0x is normal baseline. >1.5x = notable institutional participation; >2.5x = extreme volume event.`;
            } else {
                // --- DV MODE: Upgraded VDDS* (Volume-Dollar Flow Divergence) ---
                // Scale: 0.50x (Capitulation) ← 1.00x (Center Anchor) → 1.50x (Momentum)
                const vdds = m.vdds != null ? m.vdds : 1.0;
                const clamped = Math.max(0.5, Math.min(1.5, vdds));
                const signedOffset = (clamped - 1.0) * 100; // in [-50%, +50%]
                fillStart = signedOffset < 0 ? 50 + signedOffset : 50;
                fillWidth = Math.abs(signedOffset);
                barColor = Metrics._resolveDollarModeColorBand(vdds);
                displayVal = vdds != null ? vdds.toFixed(2) + 'x' : '—';
                tip = `VDDS for ${ticker}: Upgraded Volume-Dollar Flow Divergence (${displayVal}). Centered at 1.00x Balanced. ${vdds < 0.97 ? 'Capitulation pattern — selling pressure and price compression under active volume.' : (vdds > 1.03 ? 'Accumulation/momentum pattern — buying pressure and price expansion under active volume.' : 'Balanced neutral flow (dormant volume dampens noise toward 1.0x).')}`;
            }

            // Baseline marker: sits at 1.0x in VOL mode (33.33%), or at ratio centre in DV mode (50%)
            const markPct = isVolMode ? 33.333 : 50;

            return `
                <div class="vdds-row">
                    <span class="vdds-ticker" style="color:${sideColor}" data-tooltip="${tip}">${ticker}</span>
                    <div class="vdds-track" data-tooltip="${isVolMode ? '1.0x baseline tick line' : '1.0x balanced center line'}">
                        <div class="vdds-center-mark" style="left:${markPct}%"></div>
                        <div class="vdds-fill" style="left:${fillStart}%;width:${fillWidth}%;background:${barColor}"></div>
                    </div>
                    <span class="vdds-value" style="color:${barColor}">${displayVal}</span>
                </div>`;
        }).join('');
        el.innerHTML = rows || '<span class="vdds-na">No VDDS data available</span>';
    },

    renderAll(allMetrics, sideConvergence, ngPriceContext) {
        const vddsMode = (window.App && window.App.vddsMode) ? window.App.vddsMode : 'vol';
        this.renderConvergenceFlash(sideConvergence, ngPriceContext, allMetrics);
        this.renderNgPriceBar(ngPriceContext);
        this.renderVddsBar(allMetrics, vddsMode);
        this.renderAlertFeed(allMetrics);
        this.renderStressMatrix(allMetrics);
        this.renderSideConvergence(sideConvergence);
        this.renderHeatCalendar(allMetrics, window.App ? window.App.heatOffset : 0, window.App ? window.App.heatMode : 'share', window.App ? window.App.heatSide : 'all', window.App ? window.App.heatSpotlightMode : 'off', window.App ? window.App.heatPeaksThreshold : 95, null, window.App ? window.App.heatDays : 90);
        this.renderConvergenceGauges(allMetrics);
        this.renderCorrelationBars(allMetrics);
        this.renderValidationBanner(allMetrics);
    }
};
