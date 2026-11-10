/* ============================================
   Lightweight Canvas Charts
   No external dependencies
   ============================================ */

const Charts = {

    /* SPARKLINE (Price) */
    drawSparkline(canvas, data, color = '#4a80b8', fillAlpha = 0.1, hoverIndex = null) {
        if (!data || data.length < 2) return;
        const dpr = window.devicePixelRatio || 1;
        const w = canvas.parentElement ? canvas.parentElement.clientWidth : (canvas.clientWidth || 200);
        const h = canvas.parentElement ? (canvas.parentElement.clientHeight || 50) : (canvas.clientHeight || 50);
        if (w <= 0 || h <= 0) return;

        canvas.width = Math.round(w * dpr);
        canvas.height = Math.round(h * dpr);
        canvas.style.width = w + 'px';
        canvas.style.height = h + 'px';

        const ctx = canvas.getContext('2d');
        ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
        ctx.clearRect(0, 0, w, h);

        const closes = data.map(d => d.close);
        const min = Math.min(...closes);
        const max = Math.max(...closes);
        const range = max - min || 1;
        const padding = 3;

        const xStep = (w - padding * 2) / (closes.length - 1);
        const yScale = (h - padding * 2) / range;

        // Fill
        ctx.beginPath();
        ctx.moveTo(padding, h - padding);
        for (let i = 0; i < closes.length; i++) {
            const x = padding + i * xStep;
            const y = h - padding - (closes[i] - min) * yScale;
            ctx.lineTo(x, y);
        }
        ctx.lineTo(padding + (closes.length - 1) * xStep, h - padding);
        ctx.closePath();
        // Support both hex (#rrggbb) and rgb() color formats
        if (color.startsWith('#')) {
            const r = parseInt(color.slice(1,3), 16);
            const g = parseInt(color.slice(3,5), 16);
            const b = parseInt(color.slice(5,7), 16);
            ctx.fillStyle = `rgba(${r},${g},${b},${fillAlpha})`;
        } else {
            ctx.fillStyle = color.replace(')', `, ${fillAlpha})`).replace('rgb', 'rgba');
        }
        ctx.fill();

        // Line
        ctx.beginPath();
        for (let i = 0; i < closes.length; i++) {
            const x = padding + i * xStep;
            const y = h - padding - (closes[i] - min) * yScale;
            if (i === 0) ctx.moveTo(x, y);
            else ctx.lineTo(x, y);
        }
        ctx.strokeStyle = color;
        ctx.lineWidth = 1.5;
        ctx.stroke();

        // Crosshair & Highlight dot when hovering
        if (hoverIndex != null && hoverIndex >= 0 && hoverIndex < closes.length) {
            const hx = padding + hoverIndex * xStep;
            const hy = h - padding - (closes[hoverIndex] - min) * yScale;

            // Dashed vertical crosshair
            ctx.save();
            ctx.setLineDash([3, 3]);
            ctx.strokeStyle = 'rgba(255, 255, 255, 0.45)';
            ctx.lineWidth = 1;
            ctx.beginPath();
            ctx.moveTo(hx, 0);
            ctx.lineTo(hx, h);
            ctx.stroke();
            ctx.restore();

            // Highlight dot
            ctx.beginPath();
            ctx.arc(hx, hy, 4.5, 0, Math.PI * 2);
            ctx.fillStyle = color;
            ctx.fill();
            ctx.strokeStyle = '#ffffff';
            ctx.lineWidth = 1.5;
            ctx.stroke();
        } else {
            // Default current price dot on last point
            const lastX = padding + (closes.length - 1) * xStep;
            const lastY = h - padding - (closes[closes.length - 1] - min) * yScale;
            ctx.beginPath();
            ctx.arc(lastX, lastY, 3, 0, Math.PI * 2);
            ctx.fillStyle = color;
            ctx.fill();
        }
    },

    /* VOLUME BARS */
    drawVolumeBars(canvas, data, percentiles = null, hoverIndex = null, mode = 'share') {
        if (!data || data.length < 2) return;
        const dpr = window.devicePixelRatio || 1;
        const w = canvas.parentElement ? canvas.parentElement.clientWidth : (canvas.clientWidth || 200);
        const h = canvas.parentElement ? (canvas.parentElement.clientHeight || 24) : (canvas.clientHeight || 24);
        if (w <= 0 || h <= 0) return;

        canvas.width = Math.round(w * dpr);
        canvas.height = Math.round(h * dpr);
        canvas.style.width = w + 'px';
        canvas.style.height = h + 'px';

        const ctx = canvas.getContext('2d');
        ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
        ctx.clearRect(0, 0, w, h);

        const volumes = data.map(d => {
            if (mode === 'dollar') {
                return (d.dollarVolume != null) ? d.dollarVolume : ((d.close || 0) * (d.volume || 0));
            }
            return d.volume != null ? d.volume : 0;
        });
        const max = Math.max(...volumes) || 1;
        const validVols = volumes.filter(v => v > 0);
        const mean = validVols.length ? (validVols.reduce((a, b) => a + b, 0) / validVols.length) : max;
        // Anchor color scale to at least 2.2x of the 30d mean so quiet windows without a spike
        // do not falsely color a normal 1.2x bar as an extreme purple climax (#8855bb).
        const colorMax = Math.max(max, mean * 2.2) || 1;
        const barW = Math.max(1, (w / volumes.length) - 1);

        for (let i = 0; i < volumes.length; i++) {
            const x = i * (w / volumes.length);
            const barH = Math.max(2, (volumes[i] / max) * h);

            // Color by regime-anchored magnitude
            const ratio = volumes[i] / colorMax;
            let color;
            if (ratio > 0.9) color = '#8855bb';
            else if (ratio > 0.75) color = '#c04040';
            else if (ratio > 0.5) color = '#c07828';
            else if (ratio > 0.3) color = '#385e88';
            else if (ratio > 0.15) color = '#1e2438';
            else color = '#121422';

            const isHovered = (hoverIndex != null && i === hoverIndex);

            ctx.save();
            if (hoverIndex != null) {
                // Dim non-hovered bars slightly to spotlight the hovered day
                ctx.globalAlpha = isHovered ? 1.0 : 0.40;
            }

            ctx.fillStyle = color;
            ctx.fillRect(x, h - barH, barW, barH);

            if (isHovered) {
                // White accent stroke on hovered bar
                ctx.strokeStyle = '#ffffff';
                ctx.lineWidth = 1;
                ctx.strokeRect(x, h - barH, barW, barH);

                // Dashed vertical crosshair continuation
                ctx.setLineDash([2, 2]);
                ctx.strokeStyle = 'rgba(255, 255, 255, 0.45)';
                ctx.beginPath();
                ctx.moveTo(x + barW / 2, 0);
                ctx.lineTo(x + barW / 2, h);
                ctx.stroke();
            }
            ctx.restore();
        }
    },

    evaluateViewportSpotlight(visibleSlice, mode, activeFilter) {
        const N = visibleSlice.length;
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

        const STRUCTURAL_FLOORS = {
            PEAKS: { P99: 68.0, P95: 52.0, P90: 38.0 },
            VALLEYS: { P99: 64.0, P95: 48.0, P90: 34.0 }
        };
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
                baselineQuantileBin: baselineBins[i]
            };
        }

        return output;
    },

    /* HEAT CALENDAR */
    drawHeatCalendar(container, visibleSlice, mode = 'share', spotlightMode = false, peaksThreshold = 95, globalPercentiles = null, side = 'all') {
        container.innerHTML = '';
        const isPeaks = (spotlightMode === 'peaks' || spotlightMode === true);
        const isValleys = (spotlightMode === 'valleys');
        const isSpotlight = isPeaks || isValleys;
        container.classList.toggle('peaks-mode', isPeaks);
        container.classList.toggle('valleys-mode', isValleys);
        if (!visibleSlice || !visibleSlice.length) return 0;

        const colors = [
            '#14141e', '#1a1a28', '#202838', '#283848',
            '#385060', '#4a6878', '#d4a830',
            '#c07828', '#c04040', '#a03838', '#8855bb'
        ];

        const label = mode === 'dollar' ? 'DVCVI' : 'CVI';
        const subject = (side && side !== 'all') ? (side + ' ') : '';
        const N = visibleSlice.length;
        const regimeMode = isValleys ? 'VALLEYS' : 'PEAKS';
        const activeFilter = isSpotlight ? ('P' + peaksThreshold) : 'NONE';

        const cellStates = this.evaluateViewportSpotlight(visibleSlice, regimeMode, activeFilter);
        let count = 0;

        for (let i = 0; i < N; i++) {
            const day = visibleSlice[i];
            const state = cellStates[i];
            const cell = document.createElement('div');
            cell.className = 'heat-cell';

            const score = state.score;
            const idx = Math.min(colors.length - 1, Math.floor((score / 100) * colors.length));
            cell.style.background = colors[idx];

            const dateFormatted = typeof formatDisplayDate === 'function' ? formatDisplayDate(day.date) : day.date;
            const rvolVal = day.maxRvol != null ? day.maxRvol : 1.0;
            const rvolText = ' · RVOL ' + rvolVal.toFixed(1) + 'x';

            if (isSpotlight) {
                if (!state.isHighlighted) {
                    cell.classList.add('heat-dim');
                } else {
                    count++;
                    if (isPeaks) {
                        if (state.visualTier === 'RED_P99') {
                            cell.classList.add('heat-peak-extreme');
                        } else if (state.visualTier === 'ORANGE_P95') {
                            cell.classList.add('heat-peak-significant');
                        } else {
                            cell.classList.add('heat-peak-notable');
                        }
                        if (!state.isStructuralClimax) {
                            cell.classList.add('local-max');
                        }
                    } else if (isValleys) {
                        if (state.visualTier === 'RED_P99') {
                            cell.classList.add('heat-valley-extreme');
                        } else if (state.visualTier === 'ORANGE_P95') {
                            cell.classList.add('heat-valley-significant');
                        } else {
                            cell.classList.add('heat-valley-notable');
                        }
                        if (!state.isStructuralClimax) {
                            cell.classList.add('local-min');
                        }
                    }
                }

                const climaxTag = state.isStructuralClimax
                    ? (isPeaks ? 'MACRO CLIMAX' : 'MACRO DROUGHT')
                    : (isPeaks ? 'LOCAL PEAK' : 'LOCAL DROUGHT');
                const filterTag = state.visualTier === 'RED_P99' ? 'P99' : state.visualTier === 'ORANGE_P95' ? 'P95' : 'P90';

                if (isValleys) {
                    cell.setAttribute('data-tooltip', dateFormatted + ': ' + subject + 'VOL DROUGHT ' + score.toFixed(1) + rvolText + ' [' + climaxTag + ' · ' + filterTag + ' #' + state.windowRank + '/' + N + ']');
                } else {
                    cell.setAttribute('data-tooltip', dateFormatted + ': ' + subject + label + ' ' + score.toFixed(1) + rvolText + ' [' + climaxTag + ' · ' + filterTag + ' #' + state.windowRank + '/' + N + ']');
                }
            } else {
                const regimeLabel = isValleys ? (subject + 'VOL DROUGHT') : (subject + label);
                cell.setAttribute('data-tooltip', dateFormatted + ': ' + regimeLabel + ' ' + score.toFixed(1) + rvolText + ' (Rank #' + state.windowRank + '/' + N + ')');
            }

            container.appendChild(cell);
        }

        return count;
    },

    /* CONVERGENCE GAUGE (SVG Ring) */
    createGaugeRing(count, total, ticker, tooltip = '', meta = {}) {
        const circumference = 2 * Math.PI * 22; // radius = 22
        const progress = total > 0 ? count / total : 0;
        const dashOffset = circumference * (1 - progress);

        const side = meta.side || (typeof CONFIG !== 'undefined' ? CONFIG.etfs[ticker]?.side : 'long');
        const isLong = side === 'long';
        const cfg = meta.cfg || (typeof CONFIG !== 'undefined' ? CONFIG.etfs[ticker] : {}) || {};
        const region = cfg.exchange === 'NYSE' ? 'US' : cfg.exchange === 'TSX' ? 'CA' : cfg.exchange === 'LSE' ? 'UK' : (cfg.exchange || '');
        const lev = cfg.leverage || '';
        const regBadge = region && lev ? `${region} · ${lev}` : (region || lev || '');

        let strokeColor;
        if (count === total && total > 0) strokeColor = 'var(--purple)';
        else if (count >= total - 1 && count > 0) strokeColor = 'var(--red)';
        else if (count >= 3) strokeColor = 'var(--orange)';
        else if (count > 0) strokeColor = isLong ? 'var(--green)' : 'var(--red)';
        else strokeColor = 'rgba(255, 255, 255, 0.12)';

        const sideColor = isLong ? 'var(--long-accent)' : 'var(--short-accent)';
        const countColor = count === total && total > 0 ? 'var(--purple)'
            : count >= total - 1 && count > 0 ? 'var(--red)'
            : count >= 3 ? 'var(--orange)'
            : count > 0 ? (isLong ? 'var(--green)' : 'var(--red)')
            : 'rgba(255, 255, 255, 0.85)';

        const windows = meta.windows || (typeof CONFIG !== 'undefined' ? CONFIG.windows.percentile : [5, 10, 21, 63, 126, 252]);
        const volPct = meta.volPercentile || {};
        const threshold = meta.threshold || (typeof CONFIG !== 'undefined' ? CONFIG.thresholds.mwca_threshold : 90);

        const pipsHtml = windows.map(w => {
            const val = volPct[`${w}d`];
            const isAlert = val != null && val >= threshold;
            const pipCls = isAlert ? (isLong ? 'gauge-pip alert-long' : 'gauge-pip alert-short') : 'gauge-pip';
            const pipTip = `${ticker} ${w}d: ${val != null ? val.toFixed(1) + '%' : '—'} volume percentile${isAlert ? ` (ALERT ≥${threshold}%)` : ''}`;
            return `<span class="${pipCls}" data-tooltip="${pipTip}">${w}d</span>`;
        }).join('');

        const ttAttr = tooltip ? `data-tooltip="${tooltip}"` : '';

        // Clean uniform card: no left-side colored border
        return `
            <div class="convergence-gauge ${count >= 3 ? 'gauge-in-alert' : ''}" ${ttAttr}>
                <div class="gauge-header">
                    <span class="gauge-ticker" style="color: ${sideColor}">${ticker}</span>
                    <span class="gauge-sub">${regBadge}</span>
                </div>
                <div class="gauge-ring">
                    <svg viewBox="0 0 50 50">
                        <circle class="ring-bg" cx="25" cy="25" r="22"/>
                        <circle class="ring-fill" cx="25" cy="25" r="22"
                            stroke="${strokeColor}"
                            stroke-dasharray="${circumference}"
                            stroke-dashoffset="${dashOffset}"/>
                    </svg>
                    <span class="gauge-count" style="color: ${countColor}">${count}/${total}</span>
                </div>
                <div class="gauge-label">windows in alert</div>
                <div class="gauge-pips">${pipsHtml}</div>
            </div>
        `;
    },

    /* CORRELATION BAR */
    createCorrelationBar(ticker, value, side) {
        if (value == null) value = 0;
        const absVal = Math.abs(value);
        const widthPct = Math.min(absVal * 100, 50); // max 50% of bar width each side
        const isNeg = value < 0;
        const colorClass = isNeg ? 'negative' : 'positive';
        const valueColor = isNeg ? 'var(--red)' : 'var(--green)';
        const tickerColor = side === 'long' ? 'var(--green)' : 'var(--red)';

        let barStyle;
        if (isNeg) {
            barStyle = `right: 50%; width: ${widthPct}%;`;
        } else {
            barStyle = `left: 50%; width: ${widthPct}%;`;
        }

        const tip = `30-day Spearman correlation between ${ticker} daily log returns and NG=F spot returns. Current: ${value.toFixed(3)}.`;

        return `
            <div class="corr-row" data-tooltip="${tip}">
                <span class="corr-ticker" style="color: ${tickerColor}">${ticker}</span>
                <div class="corr-bar-container">
                    <div class="corr-bar-center"></div>
                    <div class="corr-bar-fill ${colorClass}" style="${barStyle}"></div>
                </div>
                <span class="corr-value" style="color: ${valueColor}">${value.toFixed(3)}</span>
            </div>
        `;
    }
};