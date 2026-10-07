(function (root) {
  'use strict';

  const MARKET_TABS = new Set(['matrix', 'pricelog', 'yields', 'periodic', 'drawdown', 'holdingperiod', 'seasonality', 'correlation', 'volatility', 'flows']);

  const RANGE_PRESETS = [
    { key: '1m', label: '1M', count: 22 },
    { key: '3m', label: '3M', count: 66 },
    { key: '6m', label: '6M', count: 130 },
    { key: '1y', label: '1Y', count: 252 },
    { key: '3y', label: '3Y', count: 756 },
    { key: '5y', label: '5Y', count: 1260 },
    { key: 'max', label: 'Max', count: null }
  ];

  const CHART_TABS = [
    { key: 'daily', label: 'Daily flow' },
    { key: 'cumulative', label: 'Cumulative' },
    { key: 'percentile', label: 'Percentile' },
    { key: 'intensity', label: 'Z-score' },
    { key: 'price', label: 'Price + flow' }
  ];

  const CATEGORY_LABELS = {
    long_only: 'Unleveraged',
    daily_2x: 'Daily 2x',
    inverse: 'Targeted inverse',
    index_leverage: 'Index leverage'
  };

  const LOCAL_SOURCE_LABEL = 'Local dataset · Trackinsight-reported historical fields';

  const COLORS = {
    text: '#e8edf3',
    muted: '#b1bdca',
    subtle: '#8d9aaa',
    grid: '#161b22',
    axis: '#334155',
    positive: '#34d399',
    negative: '#fb7185',
    accent: '#8bc7e3',
    cyan: '#22d3ee',
    accentSoft: '#38bdf8',
    neutral: '#64748b',
    warning: '#fbbf24'
  };

  const FLOW_CACHE = new Map();
  const FLOW_METRIC_CACHE = new Map();

  function finiteNumber(value) {
    if (value === null || value === undefined || value === '' || typeof value === 'boolean') return null;
    const number = Number(value);
    return Number.isFinite(number) ? number : null;
  }

  function integerValue(value) {
    const number = finiteNumber(value);
    return number === null ? null : Math.trunc(number);
  }

  function marketHistoryTab(search) {
    const tab = new URLSearchParams(search || '').get('tab');
    return MARKET_TABS.has(tab) ? tab : 'matrix';
  }

  function flowHistoryState(search) {
    const params = new URLSearchParams(search || '');
    const chart = params.get('chart') || 'daily';
    return {
      ticker: String(params.get('flow') || params.get('ticker') || '').trim().toUpperCase(),
      range: params.get('range') || '1y',
      priceEnabled: params.get('price') === '1',
      chart: CHART_TABS.some(tab => tab.key === chart) ? chart : 'daily'
    };
  }

  function reconcileFlowHistory(app, search) {
    const next = flowHistoryState(search);
    app.flowTicker = next.ticker;
    app.flowRangePreset = next.range;
    app.flowPriceEnabled = next.priceEnabled;
    app.flowChartTab = next.chart;
    app.flowChartMessage = '';
    const chartChanged = typeof app.flowEnsureChartTab === 'function' ? app.flowEnsureChartTab() : false;
    if (chartChanged && app.flowData?.ticker === next.ticker && typeof app._flowWriteUrl === 'function') app._flowWriteUrl(false);
    app.flowSearchOpen = false;
    app.flowSearchActiveIndex = -1;
    app._flowMetricCacheKey = '';
    app._flowMetricCache = null;
    if (!next.ticker) {
      if (app.flowAbortController) app.flowAbortController.abort();
      app.flowRequestId += 1;
      app.flowAbortController = null;
      app.flowData = null;
      app.flowDataState = 'pending';
      app.flowDataError = '';
      return next;
    }
    if (app.flowData?.ticker === next.ticker) {
      app._flowApplyUrlRange();
      return next;
    }
    app.selectFlowTicker(next.ticker, { writeUrl: false, push: false });
    return next;
  }

  function escapeHtml(value) {
    return String(value ?? '')
      .replaceAll('&', '&amp;')
      .replaceAll('<', '&lt;')
      .replaceAll('>', '&gt;')
      .replaceAll('"', '&quot;')
      .replaceAll("'", '&#039;');
  }

  function titleCaseCategory(value) {
    if (CATEGORY_LABELS[value]) return CATEGORY_LABELS[value];
    return String(value || 'Catalog')
      .replaceAll('_', ' ')
      .replace(/\b\w/g, character => character.toUpperCase());
  }

  function shortCategoryName(value) {
    const raw = String(value || 'Catalog');
    return raw.replace(/^\d+\.\s*/, '').replace(/\s*\(Pruned.*\)$/i, '');
  }

  function formatMoney(value, options) {
    const number = finiteNumber(value);
    if (number === null) return '—';
    const settings = options || {};
    const sign = settings.signed === false ? '' : number > 0 ? '+' : number < 0 ? '−' : '';
    const absolute = Math.abs(number);
    const prefix = number < 0 ? '−$' : '$';
    if (absolute >= 1e12) return `${sign === '−' ? '' : sign}${prefix}${(absolute / 1e12).toFixed(2)}T`;
    if (absolute >= 1e9) return `${sign === '−' ? '' : sign}${prefix}${(absolute / 1e9).toFixed(2)}B`;
    if (absolute >= 1e6) return `${sign === '−' ? '' : sign}${prefix}${(absolute / 1e6).toFixed(1)}M`;
    if (absolute >= 1e3) return `${sign === '−' ? '' : sign}${prefix}${(absolute / 1e3).toFixed(0)}K`;
    return `${sign === '−' ? '' : sign}${prefix}${absolute.toFixed(0)}`;
  }

  function formatNumber(value, digits) {
    const number = finiteNumber(value);
    if (number === null) return '—';
    return number.toFixed(digits === undefined ? 2 : digits);
  }

  function formatPercent(value) {
    const number = finiteNumber(value);
    return number === null ? '—' : `${number.toFixed(1)}%`;
  }

  function formatSignedPercent(value) {
    const number = finiteNumber(value);
    if (number === null) return '—';
    return `${number > 0 ? '+' : number < 0 ? '−' : ''}${Math.abs(number).toFixed(1)}%`;
  }

  function formatPrice(value) {
    const number = finiteNumber(value);
    if (number === null) return '—';
    return `$${number.toFixed(number >= 100 ? 2 : 4)}`;
  }

  function daysSince(value) {
    if (!value) return null;
    const date = new Date(`${String(value).slice(0, 10)}T00:00:00Z`);
    if (Number.isNaN(date.getTime())) return null;
    const now = new Date();
    const today = Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate());
    return Math.round((today - date.getTime()) / 86400000);
  }

  function completeRollingSums(records, windowSize) {
    const size = Math.max(1, Math.trunc(windowSize || 10));
    const output = new Array(records.length).fill(null);
    for (let index = size - 1; index < records.length; index += 1) {
      let total = 0;
      let complete = true;
      for (let offset = index - size + 1; offset <= index; offset += 1) {
        const value = finiteNumber(records[offset] && records[offset].flow);
        if (value === null) {
          complete = false;
          break;
        }
        total += value;
      }
      if (complete) output[index] = total;
    }
    return output;
  }

  function completeRollingMean(records, index, windowSize) {
    const size = Math.max(1, Math.trunc(windowSize || 20));
    if (index < size - 1) return null;
    let total = 0;
    for (let offset = index - size + 1; offset <= index; offset += 1) {
      const value = finiteNumber(records[offset] && records[offset].flow);
      if (value === null) return null;
      total += value;
    }
    return total / size;
  }

  function percentileAgainstSorted(sorted, value) {
    const target = finiteNumber(value);
    if (!sorted.length || target === null) return null;
    let lower = 0;
    let upper = sorted.length;
    while (lower < upper) {
      const middle = Math.floor((lower + upper) / 2);
      if (sorted[middle] < target) lower = middle + 1;
      else upper = middle;
    }
    const firstEqual = lower;
    let afterEqual = lower;
    while (afterEqual < sorted.length && sorted[afterEqual] === target) afterEqual += 1;
    const equal = afterEqual - firstEqual;
    const midrank = firstEqual + equal / 2;
    return (midrank / sorted.length) * 100;
  }

  function empiricalPercentile(distribution, value) {
    const sorted = (distribution || []).filter(Number.isFinite).slice().sort((left, right) => left - right);
    return percentileAgainstSorted(sorted, value);
  }

  function priorOnlyZScore(records, index, windowSize) {
    const size = Math.max(5, Math.trunc(windowSize || 30));
    const current = finiteNumber(records[index] && records[index].flow);
    const start = Math.max(0, index - size);
    if (current === null || index - start < 5) return null;
    const prior = [];
    for (let offset = start; offset < index; offset += 1) {
      const value = finiteNumber(records[offset] && records[offset].flow);
      if (value === null) return null;
      prior.push(value);
    }
    const mean = prior.reduce((sum, value) => sum + value, 0) / prior.length;
    const variance = prior.reduce((sum, value) => sum + ((value - mean) ** 2), 0) / prior.length;
    const standardDeviation = Math.sqrt(variance);
    if (!Number.isFinite(standardDeviation) || standardDeviation <= 0) return null;
    return (current - mean) / standardDeviation;
  }

  function flowMetricRows(records) {
    const source = Array.isArray(records) ? records : [];
    const rollingTwenty = source.map((record, index) => completeRollingMean(source, index, 20));
    const rollingFive = completeRollingSums(source, 5);
    const rollingTen = completeRollingSums(source, 10);
    const rollingTwentySum = completeRollingSums(source, 20);
    const rollingSixtySum = completeRollingSums(source, 60);
    const sortedTen = rollingTen.filter(Number.isFinite).sort((left, right) => left - right);
    return source.map((record, index) => ({
      date: record.date,
      flow: finiteNumber(record.flow),
      nav: finiteNumber(record.nav),
      performance: finiteNumber(record.performance),
      rollingMean20: rollingTwenty[index],
      rollingSum5: rollingFive[index],
      rollingSum10: rollingTen[index],
      rollingSum20: rollingTwentySum[index],
      rollingSum60: rollingSixtySum[index],
      percentile10: percentileAgainstSorted(sortedTen, rollingTen[index]),
      priorOnlyZScore: priorOnlyZScore(source, index, 30),
      selectedCumulative: null
    }));
  }

  function selectWindowMetrics(rows, startIndex, endIndex) {
    if (!rows.length) return { selected: [], latest: null, numericSelected: 0 };
    const safeStart = Math.max(0, Math.min(Math.trunc(startIndex || 0), rows.length - 1));
    const safeEnd = Math.max(safeStart, Math.min(Math.trunc(endIndex === undefined ? rows.length - 1 : endIndex), rows.length - 1));
    let cumulative = null;
    const selected = rows.slice(safeStart, safeEnd + 1).map(row => {
      const value = finiteNumber(row.flow);
      if (value !== null) cumulative = (cumulative === null ? 0 : cumulative) + value;
      return { ...row, selectedCumulative: cumulative };
    });
    return {
      selected,
      latest: selected.length ? selected[selected.length - 1] : null,
      numericSelected: selected.filter(row => row.flow !== null).length
    };
  }

  function buildWindowMetrics(records, startIndex, endIndex) {
    return selectWindowMetrics(flowMetricRows(records), startIndex, endIndex);
  }

  function cachedFlowMetricRows(revision, records) {
    const key = String(revision || `${records.length}:unknown`);
    const cached = FLOW_METRIC_CACHE.get(key);
    if (cached) return cached;
    const rows = flowMetricRows(records);
    FLOW_METRIC_CACHE.set(key, rows);
    if (FLOW_METRIC_CACHE.size > 8) FLOW_METRIC_CACHE.delete(FLOW_METRIC_CACHE.keys().next().value);
    return rows;
  }

  function expectedRevision(manifestEntry, catalogVersion) {
    if (!manifestEntry) return `pending:${catalogVersion || 'unknown'}`;
    return String(
      manifestEntry.revision
      || manifestEntry.content_sha256
      || [
        catalogVersion || 'unknown',
        manifestEntry.source_asof || manifestEntry.asof || manifestEntry.updated || 'unknown',
        manifestEntry.retrieved_at || manifestEntry.updated || 'unknown',
        manifestEntry.records ?? 'unknown'
      ].join(':')
    );
  }

  function payloadRevision(payload, item) {
    return String(
      payload.revision
      || [
        payload.ticker || item?.ticker || 'unknown',
        payload.catalog_version || item?.catalog_version || 'unknown',
        payload.schema_version || 1,
        payload.source_asof || payload.updated || payload.retrieved_at || 'unknown',
        payload.retrieved_at || payload.updated || 'unknown',
        Array.isArray(payload.data) ? payload.data.length : 0
      ].join(':')
    );
  }

  function normalizeFlowPayload(payload, item, manifestEntry) {
    if (!payload || typeof payload !== 'object') throw new Error('The flow payload is not a JSON object.');
    const expectedTicker = String(item?.ticker || '').toUpperCase();
    const payloadTicker = String(payload.ticker || '').toUpperCase();
    if (expectedTicker && payloadTicker && payloadTicker !== expectedTicker) throw new Error('Ticker does not match the curated catalog.');
    const source = payload.source || manifestEntry?.source || item?.source || 'Local dataset';
    const sourceProvider = payload.source_provider || manifestEntry?.source_provider || item?.source_provider || '';
    const flowCurrency = payload.flow_currency || manifestEntry?.flow_currency || item?.flow_currency || 'USD';
    const navCurrency = payload.nav_currency || manifestEntry?.nav_currency || item?.nav_currency || 'USD';
    const acceptedSources = new Set(['Local dataset', 'local dataset', 'Trackinsight', LOCAL_SOURCE_LABEL]);
    if (!acceptedSources.has(source) && sourceProvider !== 'Trackinsight') throw new Error('Unexpected local flow source.');
    if (flowCurrency !== 'USD' || navCurrency !== 'USD') throw new Error('Flow and NAV currency must be USD.');
    if (!Array.isArray(payload.data)) throw new Error('Flow history data must be an array.');
    if (payload.count !== undefined && integerValue(payload.count) !== payload.data.length) throw new Error('Flow history count does not match its data.');
    const seenDates = new Set();
    const records = payload.data.map((record, index) => {
      if (!record || typeof record !== 'object') throw new Error(`Flow history row ${index + 1} is invalid.`);
      const day = String(record.date || '').slice(0, 10);
      const parsed = new Date(`${day}T00:00:00Z`);
      if (!/^\d{4}-\d{2}-\d{2}$/.test(day) || Number.isNaN(parsed.getTime()) || parsed.toISOString().slice(0, 10) !== day) {
        throw new Error(`Flow history row ${index + 1} has an invalid date.`);
      }
      if (seenDates.has(day)) throw new Error(`Flow history has a duplicate date for ${day}.`);
      seenDates.add(day);
      const nav = finiteNumber(record.nav);
      if (nav !== null && nav <= 0) throw new Error(`Flow history has a non-positive NAV on ${day}.`);
      return {
        date: day,
        flow: finiteNumber(record.usd_flow),
        nav,
        performance: finiteNumber(record.perf_pct),
        sourceCumulative: finiteNumber(record.cumulative_flow)
      };
    }).sort((left, right) => left.date.localeCompare(right.date));
    const schemaVersion = integerValue(payload.schema_version) || 1;
    const sourceAsOf = String(payload.source_asof || payload.asof || payload.updated || (records.length ? records[records.length - 1].date : '') || '');
    if (schemaVersion >= 2 && records.length && sourceAsOf !== records[records.length - 1].date) {
      throw new Error('Source date does not match the latest flow observation.');
    }
    const declaredStatus = String(payload.data_status || payload.data_quality?.status || '').toLowerCase();
    let state = schemaVersion >= 2
      ? (declaredStatus === 'stale' || declaredStatus === 'degraded' ? 'stale' : 'available')
      : 'legacy';
    const age = daysSince(sourceAsOf);
    if (sourceAsOf && age === null) throw new Error('Source date is invalid.');
    if (age !== null && age < 0) throw new Error('Source date is in the future.');
    if (schemaVersion >= 2 && state === 'available' && age !== null && age > 7) state = 'stale';
    if (!records.length) state = 'empty';
    return Object.freeze({
      schemaVersion,
      revision: payloadRevision(payload, item),
      ticker: item?.ticker || payload.ticker || '',
      fundName: item?.fund_name || payload.fund_name || item?.ticker || payload.ticker || 'Catalog instrument',
      underlyingTicker: item?.underlying_ticker || payload.underlying_ticker || '',
      underlyingName: item?.underlying || item?.underlying_name || payload.underlying || payload.underlying_name || '',
      category: item?.category || payload.category || '',
      subgroup: item?.subgroup || payload.subgroup || '',
      issuer: item?.issuer || payload.issuer || '',
      leverageTarget: finiteNumber(item?.leverage_value ?? item?.leverage_target ?? payload.leverage_value ?? payload.leverage_target),
      direction: item?.direction || payload.direction || '',
      resetCadence: item?.reset_cadence || payload.reset_cadence || payload.reset_frequency || '',
      riskTier: item?.risk_tier || payload.risk_tier || '',
      alternatives: Array.isArray(item?.alternatives) ? item.alternatives.slice() : [],
      source,
      sourceLabel: LOCAL_SOURCE_LABEL,
      sourceProvider: sourceProvider || 'Trackinsight',
      sourceMode: payload.source_mode || manifestEntry?.source_mode || 'historical_local',
      qualityStatus: declaredStatus || 'available',
      dataQuality: payload.data_quality || manifestEntry?.data_quality || null,
      flowCurrency,
      navCurrency,
      sourceAsOf,
      retrievedAt: payload.retrieved_at || manifestEntry?.retrieved_at || null,
      updated: payload.updated || manifestEntry?.updated || null,
      state,
      ageDays: age,
      records: Object.freeze(records.map(record => Object.freeze(record))),
      count: records.length
    });
  }

  function cacheKey(ticker, revision) {
    return `${String(ticker || '').toUpperCase()}@${String(revision || 'unknown')}`;
  }

  function statusFromManifest(manifest, manifestEntry) {
    if (!manifestEntry) return 'pending';
    if (Number(manifest?.schema_version || 1) < 2) return Number(manifestEntry.records || 0) > 0 ? 'legacy' : 'pending';
    const availability = String(manifestEntry.availability || manifestEntry.status || '').toLowerCase();
    const qualityValue = typeof manifestEntry.data_quality === 'object'
      ? manifestEntry.data_quality?.status
      : manifestEntry.data_quality;
    const quality = String(manifestEntry.data_status || qualityValue || '').toLowerCase();
    if (availability === 'missing') return 'pending';
    if (availability === 'unavailable') return 'unavailable';
    if (quality === 'pending' || quality === 'missing') return 'pending';
    if (quality === 'stale' || quality === 'degraded' || manifestEntry.fresh === false) return 'stale';
    if (availability === 'available') return 'available';
    return 'pending';
  }

  function statusLabel(state) {
    const labels = {
      available: 'Available',
      legacy: 'Legacy history',
      stale: 'Stale',
      pending: 'Pending',
      unavailable: 'Unavailable',
      empty: 'No observations',
      error: 'Load error',
      loading: 'Loading'
    };
    return labels[state] || 'Unavailable';
  }

  function finiteScale(scale, value) {
    const number = finiteNumber(value);
    return number === null ? null : scale(number);
  }

  function linePath(points) {
    let path = '';
    let drawing = false;
    for (const point of points) {
      const x = finiteNumber(point.x);
      const y = finiteNumber(point.y);
      if (x === null || y === null) {
        drawing = false;
        continue;
      }
      path += `${drawing ? ' L' : ' M'} ${x.toFixed(1)} ${y.toFixed(1)}`;
      drawing = true;
    }
    return path.trim();
  }

  function areaPath(points, baselineY) {
    const valid = [];
    const segments = [];
    let current = [];
    for (const point of points) {
      const x = finiteNumber(point.x);
      const y = finiteNumber(point.y);
      if (x === null || y === null) {
        if (current.length > 1) segments.push(current);
        current = [];
        continue;
      }
      current.push({ x, y });
      valid.push({ x, y });
    }
    if (current.length > 1) segments.push(current);
    if (!segments.length) return '';
    return segments.map(seg => {
      const first = seg[0];
      const last = seg[seg.length - 1];
      const top = seg.map((pt, idx) => `${idx === 0 ? 'M' : 'L'} ${pt.x.toFixed(1)} ${pt.y.toFixed(1)}`).join(' ');
      return `${top} L ${last.x.toFixed(1)} ${baselineY.toFixed(1)} L ${first.x.toFixed(1)} ${baselineY.toFixed(1)} Z`;
    }).join(' ');
  }

  function miniFlowSparklineSvg(values, width, height) {
    const w = width || 96;
    const h = height || 22;
    const series = Array.isArray(values) ? values.map(finiteNumber).filter(v => v !== null) : [];
    if (!series.length) return `<svg viewBox="0 0 ${w} ${h}" width="${w}" height="${h}" aria-hidden="true"></svg>`;
    const maxAbs = Math.max(0.01, ...series.map(v => Math.abs(v)));
    const midY = h / 2;
    const slot = w / series.length;
    const barW = Math.max(1.2, Math.min(4.2, slot * 0.72));
    let rects = `<line x1="0" y1="${midY.toFixed(1)}" x2="${w}" y2="${midY.toFixed(1)}" stroke="rgba(255,255,255,0.08)" stroke-width="1"/>`;
    series.forEach((val, idx) => {
      const x = idx * slot + (slot - barW) / 2;
      const barH = Math.max(1, (Math.abs(val) / maxAbs) * (midY - 2));
      const y = val >= 0 ? midY - barH : midY;
      const fill = val > 0 ? COLORS.positive : val < 0 ? COLORS.negative : COLORS.neutral;
      rects += `<rect x="${x.toFixed(1)}" y="${y.toFixed(1)}" width="${barW.toFixed(1)}" height="${barH.toFixed(1)}" rx="0.5" fill="${fill}" opacity="0.88"/>`;
    });
    return `<svg viewBox="0 0 ${w} ${h}" width="${w}" height="${h}" aria-hidden="true" style="display:block">${rects}</svg>`;
  }

  function axisNumber(value) {
    const number = finiteNumber(value);
    if (number === null) return '—';
    const absolute = Math.abs(number);
    if (absolute >= 1e9) return `${number < 0 ? '−' : ''}$${(absolute / 1e9).toFixed(1)}B`;
    if (absolute >= 1e6) return `${number < 0 ? '−' : ''}$${(absolute / 1e6).toFixed(0)}M`;
    if (absolute >= 1e3) return `${number < 0 ? '−' : ''}$${(absolute / 1e3).toFixed(0)}K`;
    return `${number < 0 ? '−' : ''}$${absolute.toFixed(0)}`;
  }

  function dateTickIndices(count, width) {
    if (count <= 1) return [0];
    const desired = width < 400 ? 2 : width < 500 ? 3 : width < 850 ? 4 : 6;
    return Array.from(new Set(Array.from({ length: desired }, (_, index) => Math.round((count - 1) * index / (desired - 1)))));
  }

  function axisDate(value) {
    if (!value) return '—';
    const [year, month] = String(value).slice(0, 7).split('-');
    return `${year}-${month}`;
  }

  function emptyChart(width, height, message) {
    return `<svg viewBox="0 0 ${width} ${height}" role="img" aria-label="${escapeHtml(message)}"><title>${escapeHtml(message)}</title><rect width="${width}" height="${height}" fill="#050505"/><text x="${width / 2}" y="${height / 2}" text-anchor="middle" fill="${COLORS.muted}" font-family="ui-monospace, SFMono-Regular, monospace" font-size="12">${escapeHtml(message)}</text></svg>`;
  }

  function chartFrame(options) {
    const width = options.width;
    const height = options.height;
    const padding = options.padding;
    const chartWidth = width - padding.left - padding.right;
    const chartHeight = height - padding.top - padding.bottom;
    const xScale = index => padding.left + (options.records.length < 2 ? chartWidth / 2 : index * chartWidth / (options.records.length - 1));
    const dateTicks = dateTickIndices(options.records.length, width).map(index => {
      const x = xScale(index);
      return `<text x="${x.toFixed(1)}" y="${height - 8}" text-anchor="${index === 0 ? 'start' : index === options.records.length - 1 ? 'end' : 'middle'}" fill="${COLORS.subtle}" font-family="ui-monospace, SFMono-Regular, monospace" font-size="11">${escapeHtml(axisDate(options.records[index].date))}</text>`;
    }).join('');
    return { width, height, padding, chartWidth, chartHeight, xScale, dateTicks };
  }

  function chartCrosshairOverlay(frame, rows, hoverIndex, targetY, yLabel) {
    if (hoverIndex === null || hoverIndex === undefined || hoverIndex < 0 || hoverIndex >= rows.length) return '';
    const row = rows[hoverIndex];
    if (!row) return '';
    const x = frame.xScale(hoverIndex);
    const top = frame.padding.top;
    const bottom = frame.height - frame.padding.bottom;
    const dateStr = escapeHtml(row.date || '');
    let out = `<g class="flow-crosshair-group" pointer-events="none">`;
    out += `<line x1="${x.toFixed(1)}" y1="${top.toFixed(1)}" x2="${x.toFixed(1)}" y2="${bottom.toFixed(1)}" stroke="#22d3ee" stroke-width="1.3" stroke-dasharray="3 3" opacity="0.9"/>`;
    const numY = finiteNumber(targetY);
    if (numY !== null) {
      out += `<circle cx="${x.toFixed(1)}" cy="${numY.toFixed(1)}" r="7" fill="none" stroke="#22d3ee" stroke-width="1.2" opacity="0.55"/>`;
      out += `<circle cx="${x.toFixed(1)}" cy="${numY.toFixed(1)}" r="4.5" fill="#22d3ee" stroke="#050505" stroke-width="2"/>`;
      if (yLabel) {
        const textW = Math.max(48, String(yLabel).length * 7 + 10);
        const tagX = frame.padding.left - textW - 4;
        out += `<rect x="${tagX.toFixed(1)}" y="${(numY - 9).toFixed(1)}" width="${textW}" height="18" rx="3" fill="#090d16" stroke="#22d3ee" stroke-width="1"/>`;
        out += `<text x="${(tagX + textW - 5).toFixed(1)}" y="${(numY + 4).toFixed(1)}" text-anchor="end" fill="#22d3ee" font-family="ui-monospace, SFMono-Regular, monospace" font-size="10" font-weight="600">${escapeHtml(yLabel)}</text>`;
      }
    }
    const tagW = 76;
    const tagLeft = Math.max(4, Math.min(frame.width - tagW - 4, x - tagW / 2));
    const tagY = bottom + 3;
    out += `<rect x="${tagLeft.toFixed(1)}" y="${tagY.toFixed(1)}" width="${tagW}" height="19" rx="3" fill="#090d16" stroke="#22d3ee" stroke-width="1.2"/>`;
    out += `<text x="${(tagLeft + tagW / 2).toFixed(1)}" y="${(tagY + 13).toFixed(1)}" text-anchor="middle" fill="#22d3ee" font-family="ui-monospace, SFMono-Regular, monospace" font-size="10.5" font-weight="700">${dateStr}</text>`;
    out += `</g>`;
    return out;
  }

  function flowResearchApp() {
    return {
      flowCatalog: null,
      flowManifest: null,
      flowInitialLoading: true,
      flowCatalogError: '',
      flowManifestError: '',
      flowTicker: '',
      flowData: null,
      flowDataState: 'loading',
      flowDataError: '',
      flowResolvedStates: {},
      flowSearchQuery: '',
      flowSearchOpen: false,
      flowSearchActiveIndex: -1,
      flowTierFilter: 'all',
      flowCategoryFilter: 'all',
      flowStatusFilter: 'all',
      flowActiveCategoryTab: 'all',
      flowUniverseDrawerOpen: false,
      flowAlphaSignalsData: null,
      flowAlphaFilter: 'all',
      flowHoverIndex: null,
      flowViewMode: 'studio',
      flowShowMarketPulse: false,
      flowShowOutliers: true,
      flowScannerSort: 'abs_z',
      flowScannerAsc: false,
      flowScannerRegime: 'all',
      flowScannerDirection: 'all',
      flowChartTab: 'daily',
      flowChartMessage: '',
      flowChartTooltip: { visible: false, index: 0 },
      flowStartIndex: 0,
      flowEndIndex: 0,
      flowRangePreset: '1y',
      flowPriceEnabled: false,
      flowViewportWidth: typeof window === 'undefined' ? 1000 : window.innerWidth,
      flowAbortController: null,
      flowBootstrapControllers: [],
      flowRequestId: 0,
      flowDestroyed: false,
      flowHistoryHandler: null,
      _flowEntryMap: {},
      _flowMetricCacheKey: '',
      _flowMetricCache: null,

      get flowRangePresets() {
        return RANGE_PRESETS;
      },

      get flowChartTabs() {
        return CHART_TABS;
      },

      get flowPrimaryInstruments() {
        return Array.isArray(this.flowCatalog?.instruments) ? this.flowCatalog.instruments : [];
      },

      get flowFeaturedInstruments() {
        return this.flowPrimaryInstruments.filter(item => item.featured);
      },

      get flowAllEntries() {
        return this.flowPrimaryInstruments.map(item => ({ ...item, tier: 'primary' }));
      },

      get flowSelectedInstrument() {
        return this._flowEntryMap[this.flowTicker] || null;
      },

      get flowCategories() {
        const values = new Set(this.flowPrimaryInstruments.map(item => item.category));
        return Array.from(values).sort();
      },

      get flowCategoryTabs() {
        const counts = {};
        for (const item of this.flowPrimaryInstruments) {
          counts[item.category] = (counts[item.category] || 0) + 1;
        }
        return [
          { key: 'all', label: 'All (145)', count: this.flowPrimaryInstruments.length, title: 'All 145 Leveraged & Inverse ETFs' },
          { key: 'featured', label: 'Featured (24)', count: this.flowFeaturedInstruments.length, title: '24 Featured Anchor ETFs' },
          { key: 'ai_semis', label: 'AI & Semis 2X (48)', raw: '4. Single-Stock Leveraged (Bull) — AI, Semis & High-Beta Tech', count: counts['4. Single-Stock Leveraged (Bull) — AI, Semis & High-Beta Tech'] || 48 },
          { key: 'mega_crypto', label: 'Mega & Crypto 2X (29)', raw: '5. Single-Stock Leveraged (Bull) — Mega-Cap Giants, Crypto & Consumer', count: counts['5. Single-Stock Leveraged (Bull) — Mega-Cap Giants, Crypto & Consumer'] || 29 },
          { key: 'tech_3x', label: 'Tech & Semis 3X (16)', raw: '2. Technology, Semiconductor & Thematic (Bull)', count: counts['2. Technology, Semiconductor & Thematic (Bull)'] || 16 },
          { key: 'sectors_3x', label: 'Sectors 3X (12)', raw: '3. Sector Specific Leveraged (Bull)', count: counts['3. Sector Specific Leveraged (Bull)'] || 12 },
          { key: 'shorts', label: 'Shorts & Hedges (10)', raw: '9. Selective Benchmark Hedging / Tactical Shorts (Pruned to Key Anchors Only)', count: counts['9. Selective Benchmark Hedging / Tactical Shorts (Pruned to Key Anchors Only)'] || 10 },
          { key: 'commodities', label: 'Commodities & Vol (9)', raw: '6. Commodities, Energy & Volatility (Bull)', count: counts['6. Commodities, Energy & Volatility (Bull)'] || 9 },
          { key: 'broad_index', label: 'Broad Index (8)', raw: '1. Broad Market Equity Index (Bull)', count: counts['1. Broad Market Equity Index (Bull)'] || 8 },
          { key: 'rates_fixed', label: 'Rates & Crypto (8)', raw: '7. Fixed Income, Currencies & Crypto (Bull)', count: counts['7. Fixed Income, Currencies & Crypto (Bull)'] || 8 },
          { key: 'intl', label: 'International (5)', raw: '8. International / Country (Bull)', count: counts['8. International / Country (Bull)'] || 5 }
        ];
      },

      get flowActiveCategoryInstruments() {
        const key = this.flowActiveCategoryTab;
        if (key === 'featured') return this.flowFeaturedInstruments;
        if (key === 'all') return this.flowPrimaryInstruments;
        const tab = this.flowCategoryTabs.find(t => t.key === key);
        if (!tab || !tab.raw) return this.flowPrimaryInstruments;
        return this.flowPrimaryInstruments.filter(item => item.category === tab.raw);
      },

      get flowGroupedUniverse() {
        const groups = [];
        for (const tab of this.flowCategoryTabs) {
          if (tab.key === 'all' || tab.key === 'featured') continue;
          const items = this.flowPrimaryInstruments.filter(item => item.category === tab.raw);
          if (!items.length) continue;
          const totalAum = items.reduce((sum, item) => sum + (finiteNumber(item.aum_m) || 0), 0);
          groups.push({
            key: tab.key,
            label: tab.label,
            fullName: tab.raw,
            count: items.length,
            totalAumM: totalAum,
            items: items.slice().sort((a, b) => a.ticker.localeCompare(b.ticker))
          });
        }
        return groups;
      },

      get flowActiveHoverRow() {
        const rows = this.flowSelectedRows;
        if (!rows.length) return null;
        if (this.flowHoverIndex !== null && rows[this.flowHoverIndex]) {
          return rows[this.flowHoverIndex];
        }
        return rows[rows.length - 1];
      },

      get flowIsHovering() {
        return this.flowHoverIndex !== null;
      },

      get flowAlphaActiveSignals() {
        return Array.isArray(this.flowAlphaSignalsData?.active_signals) ? this.flowAlphaSignalsData.active_signals : [];
      },

      get flowAlphaTopBasket() {
        return Array.isArray(this.flowAlphaSignalsData?.top5_conviction_basket) ? this.flowAlphaSignalsData.top5_conviction_basket : [];
      },

      get flowAlphaBullBearPairs() {
        return Array.isArray(this.flowAlphaSignalsData?.bull_bear_ecosystems) ? this.flowAlphaSignalsData.bull_bear_ecosystems : [];
      },

      get flowAlphaCategoryRotations() {
        return Array.isArray(this.flowAlphaSignalsData?.category_rotations_top10) ? this.flowAlphaSignalsData.category_rotations_top10 : [];
      },

      get flowAlphaFilteredSignals() {
        const filter = this.flowAlphaFilter;
        const signals = this.flowAlphaActiveSignals;
        if (filter === 'all') return signals;
        if (filter === 'slingshot') return signals.filter(s => String(s.live_signal || '').includes('SLINGSHOT'));
        if (filter === 'ignition') return signals.filter(s => String(s.live_signal || '').includes('IGNITION'));
        if (filter === 'trap') return signals.filter(s => String(s.live_signal || '').includes('TRAP') || String(s.live_signal || '').includes('DEAD_CAT'));
        if (filter === 'squeeze') return signals.filter(s => String(s.live_signal || '').includes('SQUEEZE'));
        return signals;
      },

      get flowUniversePulse() {
        const items = this.flowPrimaryInstruments;
        if (!items.length) {
          return {
            totalFlow1d: 0,
            inflowCount1d: 0,
            outflowCount1d: 0,
            flatCount1d: 0,
            totalFlow20d: 0,
            bullFlow20d: 0,
            bearFlow20d: 0,
            accumulationCount: 0,
            distributionCount: 0,
            balancedCount: 0,
            extremeOutlierCount: 0,
            totalAumM: 0,
            topInflowTicker: '—',
            topInflowVal: 0,
            topOutflowTicker: '—',
            topOutflowVal: 0
          };
        }
        let totalFlow1d = 0;
        let inflowCount1d = 0;
        let outflowCount1d = 0;
        let flatCount1d = 0;
        let totalFlow20d = 0;
        let bullFlow20d = 0;
        let bearFlow20d = 0;
        let accumulationCount = 0;
        let distributionCount = 0;
        let balancedCount = 0;
        let extremeOutlierCount = 0;
        let totalAumM = 0;
        let topInflowTicker = '—';
        let topInflowVal = -Infinity;
        let topOutflowTicker = '—';
        let topOutflowVal = Infinity;

        for (const item of items) {
          const f1 = finiteNumber(item.latest_flow) || 0;
          const f20 = finiteNumber(item.flow_20d) ?? (finiteNumber(item.source_summary?.flow_30d_m) || 0) * 1e6;
          const z = finiteNumber(item.flow_zscore) || 0;
          const lev = finiteNumber(item.leverage_value) || 0;
          const aum = finiteNumber(item.aum_m) || 0;
          totalFlow1d += f1;
          totalFlow20d += f20;
          totalAumM += aum;
          if (lev < 0) bearFlow20d += f20;
          else bullFlow20d += f20;
          if (f1 > 0) inflowCount1d += 1;
          else if (f1 < 0) outflowCount1d += 1;
          else flatCount1d += 1;
          if (f1 > topInflowVal) {
            topInflowVal = f1;
            topInflowTicker = item.ticker;
          }
          if (f1 < topOutflowVal) {
            topOutflowVal = f1;
            topOutflowTicker = item.ticker;
          }
          if (item.regime === 'ACCUMULATION') accumulationCount += 1;
          else if (item.regime === 'DISTRIBUTION') distributionCount += 1;
          else balancedCount += 1;
          if (Math.abs(z) >= 1.5) extremeOutlierCount += 1;
        }
        return {
          totalFlow1d,
          inflowCount1d,
          outflowCount1d,
          flatCount1d,
          totalFlow20d,
          bullFlow20d,
          bearFlow20d,
          accumulationCount,
          distributionCount,
          balancedCount,
          extremeOutlierCount,
          totalAumM,
          topInflowTicker,
          topInflowVal: Number.isFinite(topInflowVal) ? topInflowVal : 0,
          topOutflowTicker,
          topOutflowVal: Number.isFinite(topOutflowVal) ? topOutflowVal : 0
        };
      },

      get flowOutlierCards() {
        const items = this.flowPrimaryInstruments.slice();
        if (!items.length) return [];
        items.sort((a, b) => {
          const za = Math.abs(finiteNumber(a.flow_zscore) || 0);
          const zb = Math.abs(finiteNumber(b.flow_zscore) || 0);
          if (Math.abs(zb - za) > 0.05) return zb - za;
          return Math.abs(finiteNumber(b.latest_flow) || 0) - Math.abs(finiteNumber(a.latest_flow) || 0);
        });
        return items.slice(0, 6).map(item => {
          const z = finiteNumber(item.flow_zscore) || 0;
          const f1 = finiteNumber(item.latest_flow) || 0;
          const f20 = finiteNumber(item.flow_20d) ?? 0;
          return {
            ticker: item.ticker,
            fund_name: item.fund_name,
            underlying: item.underlying || item.fund_name,
            leverage: item.leverage || '+2x',
            issuer: item.issuer || '',
            flow1d: f1,
            flow20d: f20,
            zscore: z,
            regime: item.regime || 'BALANCED',
            aum_m: finiteNumber(item.aum_m) || 0,
            sparklineSvg: miniFlowSparklineSvg(item.sparkline_20d || [], 86, 20)
          };
        });
      },

      get flowPeerComparisonRows() {
        const current = this.flowSelectedInstrument;
        if (!current) return [];
        const items = this.flowPrimaryInstruments;
        const normU = raw => String(raw || '').trim().replace(/\s*\(([^)]+)\)\s*$/, (m, inner) => /^[A-Z.]{1,5}$/.test(inner.trim()) ? ` (${inner.trim()})` : '').toUpperCase();
        const targetUnderlying = normU(current.underlying);
        let peers = items.filter(item => normU(item.underlying) === targetUnderlying);
        if (peers.length < 4) {
          const catPeers = items
            .filter(item => item.category === current.category && item.ticker !== current.ticker)
            .sort((a, b) => Math.abs(finiteNumber(b.flow_zscore) || 0) - Math.abs(finiteNumber(a.flow_zscore) || 0));
          const seen = new Set(peers.map(p => p.ticker));
          for (const cp of catPeers) {
            if (peers.length >= 8) break;
            if (!seen.has(cp.ticker)) {
              peers.push(cp);
              seen.add(cp.ticker);
            }
          }
        }
        return peers.slice(0, 8).map(item => ({
          ...item,
          sparklineSvg: miniFlowSparklineSvg(item.sparkline_20d || [], 72, 18)
        }));
      },

      get flowScannerRows() {
        const query = this.flowSearchQuery.trim().toUpperCase();
        const rows = [];
        for (const item of this.flowAllEntries) {
          if (this.flowTierFilter === 'featured' && !item.featured) continue;
          if (this.flowCategoryFilter !== 'all' && item.category !== this.flowCategoryFilter) continue;
          if (this.flowScannerRegime !== 'all' && item.regime !== this.flowScannerRegime) continue;
          const lev = finiteNumber(item.leverage_value) || 0;
          if (this.flowScannerDirection === 'bull' && lev < 0) continue;
          if (this.flowScannerDirection === 'bear' && lev >= 0) continue;
          if (query) {
            const haystack = [item.ticker, item.fund_name, item.underlying, item.issuer, item.leverage]
              .filter(Boolean)
              .join(' ')
              .toUpperCase();
            if (!haystack.includes(query)) continue;
          }
          rows.push({
            ...item,
            sparklineSvg: miniFlowSparklineSvg(item.sparkline_20d || [], 76, 18)
          });
        }
        const key = this.flowScannerSort;
        const dir = this.flowScannerAsc ? 1 : -1;
        rows.sort((a, b) => {
          let va = 0;
          let vb = 0;
          if (key === 'ticker') return dir * a.ticker.localeCompare(b.ticker);
          if (key === 'abs_z') {
            va = Math.abs(finiteNumber(a.flow_zscore) || 0);
            vb = Math.abs(finiteNumber(b.flow_zscore) || 0);
          } else if (key === 'zscore') {
            va = finiteNumber(a.flow_zscore) || 0;
            vb = finiteNumber(b.flow_zscore) || 0;
          } else if (key === 'flow_1d') {
            va = finiteNumber(a.latest_flow) || 0;
            vb = finiteNumber(b.latest_flow) || 0;
          } else if (key === 'flow_5d') {
            va = finiteNumber(a.flow_5d) || 0;
            vb = finiteNumber(b.flow_5d) || 0;
          } else if (key === 'flow_20d') {
            va = finiteNumber(a.flow_20d) || 0;
            vb = finiteNumber(b.flow_20d) || 0;
          } else if (key === 'flow_60d') {
            va = finiteNumber(a.flow_60d) || 0;
            vb = finiteNumber(b.flow_60d) || 0;
          } else if (key === 'flow_ytd') {
            va = finiteNumber(a.flow_ytd) || 0;
            vb = finiteNumber(b.flow_ytd) || 0;
          } else if (key === 'cumulative') {
            va = finiteNumber(a.latest_cumulative_flow) || 0;
            vb = finiteNumber(b.latest_cumulative_flow) || 0;
          } else if (key === 'pressure') {
            va = finiteNumber(a.pressure) || 0;
            vb = finiteNumber(b.pressure) || 0;
          } else if (key === 'nav_20d') {
            va = finiteNumber(a.nav_return_20d_pct) || 0;
            vb = finiteNumber(b.nav_return_20d_pct) || 0;
          } else if (key === 'aum') {
            va = finiteNumber(a.aum_m) || 0;
            vb = finiteNumber(b.aum_m) || 0;
          }
          if (va === vb) return a.ticker.localeCompare(b.ticker);
          return dir * (va - vb);
        });
        return rows;
      },

      get flowCategoryMatrixRows() {
        const groups = {};
        for (const item of this.flowPrimaryInstruments) {
          const cat = item.category || 'Uncategorized';
          if (!groups[cat]) {
            groups[cat] = {
              category: cat,
              shortName: shortCategoryName(cat),
              count: 0,
              aum_m: 0,
              flow_1d: 0,
              flow_5d: 0,
              flow_20d: 0,
              flow_60d: 0,
              flow_ytd: 0,
              cumulative: 0,
              zSum: 0,
              pressureSum: 0,
              accumulation: 0,
              distribution: 0,
              topTicker: item.ticker,
              topFlow20d: -Infinity
            };
          }
          const g = groups[cat];
          const f20 = finiteNumber(item.flow_20d) || 0;
          g.count += 1;
          g.aum_m += finiteNumber(item.aum_m) || 0;
          g.flow_1d += finiteNumber(item.latest_flow) || 0;
          g.flow_5d += finiteNumber(item.flow_5d) || 0;
          g.flow_20d += f20;
          g.flow_60d += finiteNumber(item.flow_60d) || 0;
          g.flow_ytd += finiteNumber(item.flow_ytd) || 0;
          g.cumulative += finiteNumber(item.latest_cumulative_flow) || 0;
          g.zSum += finiteNumber(item.flow_zscore) || 0;
          g.pressureSum += finiteNumber(item.pressure) || 0;
          if (item.regime === 'ACCUMULATION') g.accumulation += 1;
          if (item.regime === 'DISTRIBUTION') g.distribution += 1;
          if (Math.abs(f20) > g.topFlow20d) {
            g.topFlow20d = Math.abs(f20);
            g.topTicker = item.ticker;
          }
        }
        return Object.values(groups)
          .sort((a, b) => a.category.localeCompare(b.category))
          .map(g => ({
            ...g,
            avgZ: g.count ? g.zSum / g.count : 0,
            avgPressure: g.count ? g.pressureSum / g.count : 0
          }));
      },

      get flowUnderlyingBattleRows() {
        const byUnderlying = {};
        for (const item of this.flowPrimaryInstruments) {
          const rawU = String(item.underlying || '').trim();
          if (!rawU) continue;
          const u = rawU.replace(/\s*\(([^)]+)\)\s*$/, (m, inner) => /^[A-Z.]{1,5}$/.test(inner.trim()) ? ` (${inner.trim()})` : '').trim();
          if (!byUnderlying[u]) {
            byUnderlying[u] = {
              underlying: u,
              tickers: [],
              bullTickers: [],
              bearTickers: [],
              bullFlow20d: 0,
              bearFlow20d: 0,
              totalFlow1d: 0,
              totalFlow20d: 0,
              totalAumM: 0,
              maxAbsZ: 0,
              leadTicker: item.ticker
            };
          }
          const entry = byUnderlying[u];
          const f1 = finiteNumber(item.latest_flow) || 0;
          const f20 = finiteNumber(item.flow_20d) || 0;
          const lev = finiteNumber(item.leverage_value) || 0;
          const z = Math.abs(finiteNumber(item.flow_zscore) || 0);
          entry.tickers.push(item.ticker);
          if (lev < 0) {
            entry.bearTickers.push(item.ticker);
            entry.bearFlow20d += f20;
          } else {
            entry.bullTickers.push(item.ticker);
            entry.bullFlow20d += f20;
          }
          entry.totalFlow1d += f1;
          entry.totalFlow20d += f20;
          entry.totalAumM += finiteNumber(item.aum_m) || 0;
          if (z >= entry.maxAbsZ) {
            entry.maxAbsZ = z;
            entry.leadTicker = item.ticker;
          }
        }
        return Object.values(byUnderlying)
          .filter(row => row.tickers.length >= 2 || row.totalAumM >= 250)
          .sort((a, b) => b.totalAumM - a.totalAumM)
          .slice(0, 18);
      },

      setFlowScannerSort(column) {
        if (this.flowScannerSort === column) {
          this.flowScannerAsc = !this.flowScannerAsc;
        } else {
          this.flowScannerSort = column;
          this.flowScannerAsc = column === 'ticker';
        }
      },

      get flowStatusForTicker() {
        return ticker => {
          const item = this._flowEntryMap[String(ticker || '').toUpperCase()];
          if (!item) return 'unavailable';
          const resolved = this.flowResolvedStates[item.ticker];
          if (resolved) return resolved;
          return statusFromManifest(this.flowManifest, this.flowManifest?.etfs?.[item.ticker]);
        };
      },

      get flowStatusLabelForTicker() {
        return ticker => statusLabel(this.flowStatusForTicker(ticker));
      },

      get flowSearchResults() {
        const query = this.flowSearchQuery.trim().toUpperCase();
        const results = [];
        for (const item of this.flowAllEntries) {
          if (this.flowTierFilter === 'featured' && !item.featured) continue;
          if (this.flowCategoryFilter !== 'all' && item.category !== this.flowCategoryFilter) continue;
          const state = this.flowStatusForTicker(item.ticker);
          if (this.flowStatusFilter !== 'all' && state !== this.flowStatusFilter) continue;
          if (query) {
            const haystack = [
              item.ticker,
              item.fund_name,
              item.underlying,
              item.underlying_name,
              item.issuer,
              item.category,
              item.trackinsight_key,
              item.leverage,
              item.archetype_label,
              item.live_signal_label
            ]
              .filter(Boolean)
              .join(' ')
              .toUpperCase();
            if (!haystack.includes(query)) continue;
          }
          let score = item.featured ? 100 : 0;
          if (item.ticker === query) score += 2000;
          else if (item.ticker.startsWith(query)) score += 800;
          else if ((item.underlying || '').toUpperCase().startsWith(query)) score += 300;
          else if ((item.fund_name || '').toUpperCase().startsWith(query)) score += 200;
          results.push({ item, state, score });
        }
        results.sort((left, right) => right.score - left.score || left.item.ticker.localeCompare(right.item.ticker));
        return results;
      },

      get flowSelectedStateLabel() {
        return statusLabel(this.flowDataState);
      },

      get flowSourceStatusLabel() {
        if (this.flowCatalogError) return 'Catalog unavailable';
        if (this.flowManifestError) return 'Coverage manifest unavailable';
        const manifest = this.flowManifest;
        if (!manifest) return 'Local coverage pending';
        if (manifest.complete === true && manifest.status === 'complete') return 'Complete local historical dataset';
        if (manifest.status === 'stale' || manifest.fresh === false) return 'Stale local dataset';
        return 'Incomplete local dataset';
      },

      get flowCoverage() {
        const primary = this.flowPrimaryInstruments;
        const entries = this.flowManifest?.etfs && typeof this.flowManifest.etfs === 'object' ? this.flowManifest.etfs : {};
        const covered = primary.filter(item => {
          const entry = entries[item.ticker];
          return entry && Number(entry.records || 0) > 0;
        });
        const sourceDates = covered
          .map(item => String(entries[item.ticker].source_asof || entries[item.ticker].asof || ''))
          .filter(Boolean)
          .sort();
        return {
          covered: covered.length,
          primary: primary.length,
          pending: Math.max(0, primary.length - covered.length),
          manifestFiles: integerValue(this.flowManifest?.counts?.files) ?? null,
          generated: this.flowManifest?.source_asof || null,
          sourceAsOf: this.flowManifest?.source_asof || (sourceDates.length ? sourceDates[sourceDates.length - 1] : null)
        };
      },

      get flowRangeCount() {
        return Math.max(0, this.flowEndIndex - this.flowStartIndex + 1);
      },

      get flowMaxRecordIndex() {
        return Math.max(0, (this.flowData?.records?.length || 0) - 1);
      },

      get flowRangeStartDate() {
        return this.flowData?.records?.[this.flowStartIndex]?.date || '';
      },

      get flowRangeEndDate() {
        return this.flowData?.records?.[this.flowEndIndex]?.date || '';
      },

      get flowWindowMetrics() {
        const records = this.flowData?.records || [];
        const key = `${this.flowData?.revision || 'none'}:${this.flowStartIndex}:${this.flowEndIndex}`;
        if (this._flowMetricCacheKey === key && this._flowMetricCache) return this._flowMetricCache;
        const rows = cachedFlowMetricRows(this.flowData?.revision || 'none', records);
        const metrics = selectWindowMetrics(rows, this.flowStartIndex, this.flowEndIndex);
        this._flowMetricCacheKey = key;
        this._flowMetricCache = metrics;
        return metrics;
      },

      get flowSelectedRows() {
        return this.flowWindowMetrics.selected;
      },

      get flowKpis() {
        const selected = this.flowSelectedRows;
        if (!selected.length) {
          return {
            latest: null,
            trailingTwenty: null,
            cumulative: null,
            percentile: null,
            zScore: null,
            coverage: '0 of 0 sessions'
          };
        }
        const latest = selected[selected.length - 1];
        const trailing = selected.slice(-20);
        const trailingComplete = trailing.length === 20 && trailing.every(row => row.flow !== null);
        return {
          latest: latest.flow,
          latestDate: latest.date,
          trailingTwenty: trailingComplete ? trailing.reduce((sum, row) => sum + row.flow, 0) : null,
          cumulative: latest.selectedCumulative,
          percentile: latest.percentile10,
          zScore: latest.priorOnlyZScore,
          coverage: `${this.flowWindowMetrics.numericSelected} of ${selected.length} sessions with numeric flow`
        };
      },

      get flowPriceAvailable() {
        const values = this.flowSelectedRows.map(row => row.nav).filter(Number.isFinite);
        const flows = this.flowSelectedRows.map(row => row.flow).filter(Number.isFinite);
        return values.length >= 2 && flows.length >= 1;
      },

      get flowChartWidth() {
        const viewport = finiteNumber(this.flowViewportWidth) || 1000;
        return Math.max(280, Math.min(1180, Math.round(viewport - 48)));
      },

      get flowDailyChartSvg() {
        const rows = this.flowSelectedRows;
        const width = this.flowChartWidth;
        const height = width < 500 ? 285 : 330;
        if (rows.length < 2) return emptyChart(width, height, 'Select at least two source observations.');
        const frame = chartFrame({ records: rows, width, height, padding: { left: width < 500 ? 54 : 68, right: 18, top: 24, bottom: 34 } });
        const values = rows.flatMap(row => [finiteNumber(row.flow), finiteNumber(row.rollingMean20)]).filter(value => value !== null);
        if (!values.length) return emptyChart(width, height, 'Daily net flow is unavailable for this window.');
        const maximum = Math.max(1, ...values.map(value => Math.abs(value))) * 1.08;
        const yScale = value => frame.padding.top + frame.chartHeight / 2 - value / maximum * frame.chartHeight / 2;
        const zeroY = yScale(0);
        const slot = frame.chartWidth / Math.max(1, rows.length);
        const barWidth = Math.max(0.8, Math.min(11, slot * 0.72));
        let bars = '';
        rows.forEach((row, index) => {
          const flow = finiteNumber(row.flow);
          if (flow === null) return;
          const center = frame.xScale(index);
          const x = center - barWidth / 2;
          const y = yScale(flow);
          const barHeight = Math.max(1.4, Math.abs(y - zeroY));
          const top = flow >= 0 ? y : zeroY;
          const fill = flow === 0 ? COLORS.neutral : flow > 0 ? COLORS.positive : COLORS.negative;
          bars += `<rect x="${x.toFixed(1)}" y="${top.toFixed(1)}" width="${barWidth.toFixed(1)}" height="${barHeight.toFixed(1)}" rx="0.6" fill="${fill}" opacity="0.88"/>`;
        });
        const meanPoints = rows.map((row, index) => ({ x: frame.xScale(index), y: finiteScale(yScale, row.rollingMean20) }));
        const grid = [maximum, maximum / 2, 0, -maximum / 2, -maximum].map(value => {
          const y = yScale(value);
          const isZero = value === 0;
          const isOuter = Math.abs(value) === maximum;
          const dash = isZero ? '' : ' stroke-dasharray="3 3"';
          const label = (isZero || isOuter)
            ? `<text x="${frame.padding.left - 7}" y="${(y + 4).toFixed(1)}" text-anchor="end" fill="${COLORS.subtle}" font-family="ui-monospace, SFMono-Regular, monospace" font-size="11">${escapeHtml(axisNumber(value))}</text>`
            : '';
          return `<line x1="${frame.padding.left}" y1="${y.toFixed(1)}" x2="${width - frame.padding.right}" y2="${y.toFixed(1)}" stroke="${isZero ? COLORS.axis : COLORS.grid}" stroke-width="1"${dash}/>${label}`;
        }).join('');
        const description = `Daily ETF estimated net flow in US dollars for ${rows.length} selected sessions. Positive and negative bars diverge from a neutral zero line. A line shows the complete 20-observation rolling mean; incomplete windows are gaps.`;
        let overlay = '';
        if (this.flowHoverIndex !== null && this.flowHoverIndex >= 0 && this.flowHoverIndex < rows.length) {
          const hRow = rows[this.flowHoverIndex];
          const hFlow = finiteNumber(hRow.flow);
          const hY = hFlow !== null ? yScale(hFlow) : zeroY;
          overlay = chartCrosshairOverlay(frame, rows, this.flowHoverIndex, hY, axisNumber(hFlow));
        }
        return `<svg viewBox="0 0 ${width} ${height}" role="img" aria-labelledby="flow-daily-title flow-daily-desc"><title id="flow-daily-title">Daily ETF estimated net flow</title><desc id="flow-daily-desc">${escapeHtml(description)}</desc>${grid}${bars}<path d="${linePath(meanPoints)}" fill="none" stroke="${COLORS.cyan}" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>${overlay}${frame.dateTicks}</svg>`;
      },

      get flowCumulativeChartSvg() {
        const rows = this.flowSelectedRows;
        const width = this.flowChartWidth;
        const height = width < 500 ? 265 : 310;
        if (rows.length < 2) return emptyChart(width, height, 'Select at least two source observations.');
        const frame = chartFrame({ records: rows, width, height, padding: { left: width < 500 ? 58 : 72, right: 18, top: 22, bottom: 34 } });
        const values = rows.map(row => finiteNumber(row.selectedCumulative)).filter(value => value !== null);
        if (!values.length) return emptyChart(width, height, 'Selected-window cumulative flow is unavailable for this window.');
        const minimum = Math.min(0, ...values);
        const maximum = Math.max(0, ...values);
        const paddingValue = Math.max(1, (maximum - minimum) * 0.08);
        const low = minimum - paddingValue;
        const high = maximum + paddingValue;
        const yScale = value => frame.padding.top + (high - value) / (high - low) * frame.chartHeight;
        const zeroY = yScale(0);
        const linePoints = rows.map((row, index) => ({ x: frame.xScale(index), y: finiteScale(yScale, row.selectedCumulative) }));
        const area = areaPath(linePoints, zeroY);
        const grid = [high, 0, low].map(value => {
          const y = yScale(value);
          return `<line x1="${frame.padding.left}" y1="${y.toFixed(1)}" x2="${width - frame.padding.right}" y2="${y.toFixed(1)}" stroke="${value === 0 ? COLORS.axis : COLORS.grid}" stroke-width="1"/><text x="${frame.padding.left - 7}" y="${(y + 4).toFixed(1)}" text-anchor="end" fill="${COLORS.subtle}" font-family="ui-monospace, SFMono-Regular, monospace" font-size="11">${escapeHtml(axisNumber(value))}</text>`;
        }).join('');
        const last = rows[rows.length - 1];
        const lastY = finiteScale(yScale, last.selectedCumulative);
        const lastColor = (last.selectedCumulative || 0) >= 0 ? COLORS.cyan : COLORS.negative;
        const lastPoint = lastY === null ? '' : `<circle cx="${frame.xScale(rows.length - 1).toFixed(1)}" cy="${lastY.toFixed(1)}" r="3.5" fill="${lastColor}"/>`;
        const description = `Selected-window cumulative source-reported aggregate net flow, summed from a zero baseline before ${rows[0].date}. Missing daily values carry the prior cumulative value and are not converted to zero.`;
        let overlay = '';
        if (this.flowHoverIndex !== null && this.flowHoverIndex >= 0 && this.flowHoverIndex < rows.length) {
          const hRow = rows[this.flowHoverIndex];
          const hVal = finiteNumber(hRow.selectedCumulative);
          const hY = hVal !== null ? yScale(hVal) : zeroY;
          overlay = chartCrosshairOverlay(frame, rows, this.flowHoverIndex, hY, axisNumber(hVal));
        }
        return `<svg viewBox="0 0 ${width} ${height}" role="img" aria-labelledby="flow-cumulative-title flow-cumulative-desc"><title id="flow-cumulative-title">Selected-window cumulative source-reported aggregate net flow</title><desc id="flow-cumulative-desc">${escapeHtml(description)}</desc><defs><linearGradient id="flow-cum-grad" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="${COLORS.cyan}" stop-opacity="0.22"/><stop offset="100%" stop-color="${COLORS.cyan}" stop-opacity="0.0"/></linearGradient></defs>${grid}${area ? `<path d="${area}" fill="url(#flow-cum-grad)"/>` : ''}<path d="${linePath(linePoints)}" fill="none" stroke="${COLORS.cyan}" stroke-width="2.2" stroke-linejoin="round" stroke-linecap="round"/>${lastPoint}${overlay}${frame.dateTicks}</svg>`;
      },

      get flowPercentileChartSvg() {
        const rows = this.flowSelectedRows;
        const width = this.flowChartWidth;
        const height = width < 500 ? 270 : 310;
        if (rows.length < 2) return emptyChart(width, height, 'Select at least two source observations.');
        const frame = chartFrame({ records: rows, width, height, padding: { left: width < 500 ? 52 : 66, right: width < 500 ? 48 : 62, top: 22, bottom: 34 } });
        const flowValues = rows.map(row => finiteNumber(row.rollingSum10)).filter(value => value !== null);
        const percentiles = rows.map(row => finiteNumber(row.percentile10)).filter(value => value !== null);
        if (!flowValues.length || !percentiles.length) return emptyChart(width, height, 'A complete 10-observation flow/percentile window is unavailable for this window.');
        const maximum = Math.max(1, ...flowValues.map(value => Math.abs(value))) * 1.08;
        const yFlow = value => frame.padding.top + frame.chartHeight / 2 - value / maximum * frame.chartHeight / 2;
        const yPercentile = value => frame.padding.top + frame.chartHeight - value / 100 * frame.chartHeight;
        const zeroY = yFlow(0);
        const slot = frame.chartWidth / Math.max(1, rows.length);
        const barWidth = Math.max(0.8, Math.min(9, slot * 0.58));
        let bars = '';
        rows.forEach((row, index) => {
          const value = finiteNumber(row.rollingSum10);
          if (value === null) return;
          const center = frame.xScale(index);
          const y = yFlow(value);
          const top = value >= 0 ? y : zeroY;
          const barHeight = Math.max(1.2, Math.abs(y - zeroY));
          const fill = value === 0 ? COLORS.neutral : value > 0 ? COLORS.positive : COLORS.negative;
          bars += `<rect x="${(center - barWidth / 2).toFixed(1)}" y="${top.toFixed(1)}" width="${barWidth.toFixed(1)}" height="${barHeight.toFixed(1)}" fill="${fill}" opacity="0.65"/>`;
        });
        const percentilePoints = rows.map((row, index) => ({ x: frame.xScale(index), y: finiteScale(yPercentile, row.percentile10) }));
        const leftAxis = [maximum, 0, -maximum].map(value => {
          const y = yFlow(value);
          return `<text x="${frame.padding.left - 7}" y="${(y + 4).toFixed(1)}" text-anchor="end" fill="${COLORS.accent}" font-family="ui-monospace, SFMono-Regular, monospace" font-size="11">${escapeHtml(axisNumber(value))}</text>`;
        }).join('');
        const rightAxis = [100, 50, 0].map(value => {
          const y = yPercentile(value);
          return `<line x1="${frame.padding.left}" y1="${y.toFixed(1)}" x2="${width - frame.padding.right}" y2="${y.toFixed(1)}" stroke="${COLORS.grid}" stroke-width="1"/><text x="${width - frame.padding.right + 7}" y="${(y + 4).toFixed(1)}" text-anchor="start" fill="${COLORS.subtle}" font-family="ui-monospace, SFMono-Regular, monospace" font-size="11">${value}%</text>`;
        }).join('');
        const description = 'The left axis shows complete trailing 10-observation source-reported aggregate net flow in US dollars. The right axis shows the tie-aware empirical percentile of that flow against all complete 10-observation windows in available source history. Missing values remain gaps.';
        let overlay = '';
        if (this.flowHoverIndex !== null && this.flowHoverIndex >= 0 && this.flowHoverIndex < rows.length) {
          const hRow = rows[this.flowHoverIndex];
          const hPct = finiteNumber(hRow.percentile10);
          const hY = hPct !== null ? yPercentile(hPct) : null;
          overlay = chartCrosshairOverlay(frame, rows, this.flowHoverIndex, hY, hPct !== null ? `${hPct.toFixed(0)}%` : null);
        }
        return `<svg viewBox="0 0 ${width} ${height}" role="img" aria-labelledby="flow-percentile-title flow-percentile-desc"><title id="flow-percentile-title">Ten-observation flow percentile</title><desc id="flow-percentile-desc">${escapeHtml(description)}</desc>${leftAxis}${rightAxis}${bars}<path d="${linePath(percentilePoints)}" fill="none" stroke="${COLORS.cyan}" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>${overlay}${frame.dateTicks}</svg>`;
      },

      get flowIntensityChartSvg() {
        const rows = this.flowSelectedRows;
        const width = this.flowChartWidth;
        const height = width < 500 ? 270 : 310;
        if (rows.length < 2) return emptyChart(width, height, 'Select at least two source observations.');
        const frame = chartFrame({ records: rows, width, height, padding: { left: width < 500 ? 46 : 58, right: 18, top: 22, bottom: 34 } });
        const values = rows.map(row => finiteNumber(row.priorOnlyZScore)).filter(value => value !== null);
        if (!values.length) return emptyChart(width, height, 'A prior-only z-score is unavailable for this window.');
        const extent = Math.max(3, ...values.map(value => Math.abs(value)));
        const maximum = Math.ceil(extent * 1.05 * 2) / 2;
        const yScale = value => frame.padding.top + frame.chartHeight / 2 - value / maximum * frame.chartHeight / 2;
        const zeroY = yScale(0);
        const slot = frame.chartWidth / Math.max(1, rows.length);
        const barWidth = Math.max(0.8, Math.min(9, slot * 0.62));
        let bars = '';
        rows.forEach((row, index) => {
          const value = finiteNumber(row.priorOnlyZScore);
          if (value === null) return;
          const center = frame.xScale(index);
          const y = yScale(value);
          const top = value >= 0 ? y : zeroY;
          const barHeight = Math.max(1.2, Math.abs(y - zeroY));
          const fill = value === 0 ? COLORS.neutral : value >= 2 ? COLORS.cyan : value > 0 ? COLORS.positive : value <= -2 ? COLORS.warning : COLORS.negative;
          bars += `<rect x="${(center - barWidth / 2).toFixed(1)}" y="${top.toFixed(1)}" width="${barWidth.toFixed(1)}" height="${barHeight.toFixed(1)}" fill="${fill}"/>`;
        });
        const grid = [maximum, 2, 0, -2, -maximum].filter((v, i, a) => a.indexOf(v) === i).map(value => {
          const y = yScale(value);
          const isThreshold = Math.abs(value) === 2;
          const stroke = value === 0 ? COLORS.axis : isThreshold ? 'rgba(34,211,238,0.22)' : COLORS.grid;
          const dash = isThreshold ? ' stroke-dasharray="3 3"' : '';
          return `<line x1="${frame.padding.left}" y1="${y.toFixed(1)}" x2="${width - frame.padding.right}" y2="${y.toFixed(1)}" stroke="${stroke}" stroke-width="1"${dash}/><text x="${frame.padding.left - 6}" y="${(y + 4).toFixed(1)}" text-anchor="end" fill="${COLORS.subtle}" font-family="ui-monospace, SFMono-Regular, monospace" font-size="11">${value > 0 ? '+' : ''}${value.toFixed(1)}</text>`;
        }).join('');
        const description = 'Prior-only z-score for daily ETF estimated net flow. Each value uses the preceding 30 available sessions and excludes the current observation from its mean and standard deviation. Missing values remain gaps.';
        let overlay = '';
        if (this.flowHoverIndex !== null && this.flowHoverIndex >= 0 && this.flowHoverIndex < rows.length) {
          const hRow = rows[this.flowHoverIndex];
          const hZ = finiteNumber(hRow.priorOnlyZScore);
          const hY = hZ !== null ? yScale(hZ) : zeroY;
          overlay = chartCrosshairOverlay(frame, rows, this.flowHoverIndex, hY, hZ !== null ? `${hZ >= 0 ? '+' : ''}${hZ.toFixed(2)}σ` : null);
        }
        return `<svg viewBox="0 0 ${width} ${height}" role="img" aria-labelledby="flow-intensity-title flow-intensity-desc"><title id="flow-intensity-title">Prior-only daily flow z-score</title><desc id="flow-intensity-desc">${escapeHtml(description)}</desc>${grid}${bars}${overlay}${frame.dateTicks}</svg>`;
      },

      get flowPriceChartSvg() {
        const rows = this.flowSelectedRows;
        const width = this.flowChartWidth;
        const height = width < 500 ? 285 : 330;
        const prices = rows.map(row => finiteNumber(row.nav)).filter(value => value !== null);
        const flows = rows.map(row => finiteNumber(row.flow)).filter(value => value !== null);
        if (!this.flowPriceEnabled) return '';
        if (prices.length < 2 || !flows.length) return emptyChart(width, height, 'Price and flow are unavailable for this window.');
        const frame = chartFrame({ records: rows, width, height, padding: { left: width < 500 ? 52 : 68, right: width < 500 ? 52 : 68, top: 22, bottom: 34 } });
        const minimumPrice = Math.min(...prices);
        const maximumPrice = Math.max(...prices);
        const pricePadding = Math.max((maximumPrice - minimumPrice) * 0.08, maximumPrice * 0.005);
        const lowPrice = Math.max(0, minimumPrice - pricePadding);
        const highPrice = maximumPrice + pricePadding;
        const maximumFlow = Math.max(1, ...flows.map(value => Math.abs(value))) * 1.08;
        const yPrice = value => frame.padding.top + (highPrice - value) / (highPrice - lowPrice || 1) * frame.chartHeight;
        const yFlow = value => frame.padding.top + frame.chartHeight / 2 - value / maximumFlow * frame.chartHeight / 2;
        const zeroFlowY = yFlow(0);
        const slot = frame.chartWidth / Math.max(1, rows.length);
        const barWidth = Math.max(0.8, Math.min(7, slot * 0.45));
        let bars = '';
        rows.forEach((row, index) => {
          const value = finiteNumber(row.flow);
          if (value === null) return;
          const center = frame.xScale(index);
          const y = yFlow(value);
          const top = value >= 0 ? y : zeroFlowY;
          const barHeight = Math.max(1.2, Math.abs(y - zeroFlowY));
          const fill = value === 0 ? COLORS.neutral : value > 0 ? COLORS.positive : COLORS.negative;
          bars += `<rect x="${(center - barWidth / 2).toFixed(1)}" y="${top.toFixed(1)}" width="${barWidth.toFixed(1)}" height="${barHeight.toFixed(1)}" fill="${fill}" opacity="0.68"/>`;
        });
        const pricePoints = rows.map((row, index) => ({ x: frame.xScale(index), y: finiteScale(yPrice, row.nav) }));
        const leftTicks = [highPrice, (highPrice + lowPrice) / 2, lowPrice].map(value => {
          const y = yPrice(value);
          return `<text x="${frame.padding.left - 7}" y="${(y + 4).toFixed(1)}" text-anchor="end" fill="${COLORS.cyan}" font-family="ui-monospace, SFMono-Regular, monospace" font-size="11">${escapeHtml(formatPrice(value))}</text>`;
        }).join('');
        const rightTicks = [maximumFlow, 0, -maximumFlow].map(value => {
          const y = yFlow(value);
          return `<text x="${width - frame.padding.right + 7}" y="${(y + 4).toFixed(1)}" text-anchor="start" fill="${COLORS.muted}" font-family="ui-monospace, SFMono-Regular, monospace" font-size="11">${escapeHtml(axisNumber(value))}</text>`;
        }).join('');
        const description = 'Source-reported NAV or share price on the left axis and daily net flow on the right axis. Each series uses an independent scale and missing observations are not connected.';
        let overlay = '';
        if (this.flowHoverIndex !== null && this.flowHoverIndex >= 0 && this.flowHoverIndex < rows.length) {
          const hRow = rows[this.flowHoverIndex];
          const hPrice = finiteNumber(hRow.nav);
          const hY = hPrice !== null ? yPrice(hPrice) : null;
          overlay = chartCrosshairOverlay(frame, rows, this.flowHoverIndex, hY, formatPrice(hPrice));
        }
        return `<svg viewBox="0 0 ${width} ${height}" role="img" aria-labelledby="flow-price-title flow-price-desc"><title id="flow-price-title">Price and daily ETF estimated net flow</title><desc id="flow-price-desc">${escapeHtml(description)}</desc>${leftTicks}${rightTicks}${bars}<path d="${linePath(pricePoints)}" fill="none" stroke="${COLORS.cyan}" stroke-width="2.2" stroke-linejoin="round" stroke-linecap="round"/>${overlay}${frame.dateTicks}</svg>`;
      },

      get flowMultiHorizonImpulseChartSvg() {
        const rows = this.flowSelectedRows;
        const width = Math.max(280, Math.min(680, Math.round(this.flowChartWidth * 0.58)));
        const height = 215;
        if (rows.length < 5) return emptyChart(width, height, 'Select at least five sessions for multi-horizon impulse.');
        const frame = chartFrame({ records: rows, width, height, padding: { left: 60, right: 16, top: 18, bottom: 28 } });
        const values = rows.flatMap(r => [finiteNumber(r.rollingSum5), finiteNumber(r.rollingSum20), finiteNumber(r.rollingSum60)]).filter(v => v !== null);
        if (!values.length) return emptyChart(width, height, 'Rolling multi-horizon flow sums unavailable.');
        const maxAbs = Math.max(1, ...values.map(v => Math.abs(v))) * 1.08;
        const yScale = value => frame.padding.top + frame.chartHeight / 2 - (value / maxAbs) * (frame.chartHeight / 2);
        const pts5 = rows.map((r, idx) => ({ x: frame.xScale(idx), y: finiteScale(yScale, r.rollingSum5) }));
        const pts20 = rows.map((r, idx) => ({ x: frame.xScale(idx), y: finiteScale(yScale, r.rollingSum20) }));
        const pts60 = rows.map((r, idx) => ({ x: frame.xScale(idx), y: finiteScale(yScale, r.rollingSum60) }));
        const grid = [maxAbs, 0, -maxAbs].map(value => {
          const y = yScale(value);
          return `<line x1="${frame.padding.left}" y1="${y.toFixed(1)}" x2="${width - frame.padding.right}" y2="${y.toFixed(1)}" stroke="${value === 0 ? COLORS.axis : COLORS.grid}" stroke-width="1"/><text x="${frame.padding.left - 6}" y="${(y + 4).toFixed(1)}" text-anchor="end" fill="${COLORS.subtle}" font-family="ui-monospace, SFMono-Regular, monospace" font-size="10.5">${escapeHtml(axisNumber(value))}</text>`;
        }).join('');

        let overlay = '';
        if (this.flowHoverIndex !== null && this.flowHoverIndex >= 0 && this.flowHoverIndex < rows.length) {
          const hRow = rows[this.flowHoverIndex];
          const x = frame.xScale(this.flowHoverIndex);
          const top = frame.padding.top;
          const bottom = frame.height - frame.padding.bottom;
          const dateStr = escapeHtml(hRow.date || '');
          const s5 = finiteNumber(hRow.rollingSum5);
          const s20 = finiteNumber(hRow.rollingSum20);
          const s60 = finiteNumber(hRow.rollingSum60);

          let dots = '';
          if (s5 !== null) {
            const y5 = yScale(s5);
            dots += `<circle cx="${x.toFixed(1)}" cy="${y5.toFixed(1)}" r="4.5" fill="#22d3ee" stroke="#050505" stroke-width="1.8"/>`;
          }
          if (s20 !== null) {
            const y20 = yScale(s20);
            dots += `<circle cx="${x.toFixed(1)}" cy="${y20.toFixed(1)}" r="4.5" fill="#34d399" stroke="#050505" stroke-width="1.8"/>`;
          }
          if (s60 !== null) {
            const y60 = yScale(s60);
            dots += `<circle cx="${x.toFixed(1)}" cy="${y60.toFixed(1)}" r="4.5" fill="#fbbf24" stroke="#050505" stroke-width="1.8"/>`;
          }

          const tagW = 76;
          const tagLeft = Math.max(4, Math.min(width - tagW - 4, x - tagW / 2));
          const tagY = bottom + 3;

          const cardW = 168;
          const cardH = 68;
          let cardX = x + 12;
          if (cardX + cardW > width - 6) {
            cardX = x - cardW - 12;
          }
          if (cardX < 4) cardX = 4;
          const cardY = top + 2;

          const card = `<g class="flow-impulse-tooltip" opacity="0.96">`
            + `<rect x="${cardX.toFixed(1)}" y="${cardY.toFixed(1)}" width="${cardW}" height="${cardH}" rx="4" fill="#090d16" stroke="rgba(34,211,238,0.4)" stroke-width="1"/>`
            + `<text x="${(cardX + 8).toFixed(1)}" y="${(cardY + 14).toFixed(1)}" fill="#94a3b8" font-family="ui-monospace, SFMono-Regular, monospace" font-size="9.5" font-weight="600">${dateStr}</text>`
            + `<text x="${(cardX + 8).toFixed(1)}" y="${(cardY + 29).toFixed(1)}" fill="#22d3ee" font-family="ui-monospace, SFMono-Regular, monospace" font-size="10" font-weight="600">5D sum:  ${escapeHtml(formatMoney(s5))}</text>`
            + `<text x="${(cardX + 8).toFixed(1)}" y="${(cardY + 44).toFixed(1)}" fill="#34d399" font-family="ui-monospace, SFMono-Regular, monospace" font-size="10" font-weight="600">20D sum: ${escapeHtml(formatMoney(s20))}</text>`
            + `<text x="${(cardX + 8).toFixed(1)}" y="${(cardY + 59).toFixed(1)}" fill="#fbbf24" font-family="ui-monospace, SFMono-Regular, monospace" font-size="10" font-weight="600">60D sum: ${escapeHtml(formatMoney(s60))}</text>`
            + `</g>`;

          overlay = `<g class="flow-crosshair-group" pointer-events="none">`
            + `<line x1="${x.toFixed(1)}" y1="${top.toFixed(1)}" x2="${x.toFixed(1)}" y2="${bottom.toFixed(1)}" stroke="#22d3ee" stroke-width="1.2" stroke-dasharray="3 3" opacity="0.85"/>`
            + dots
            + `<rect x="${tagLeft.toFixed(1)}" y="${tagY.toFixed(1)}" width="${tagW}" height="19" rx="3" fill="#090d16" stroke="#22d3ee" stroke-width="1.2"/>`
            + `<text x="${(tagLeft + tagW / 2).toFixed(1)}" y="${(tagY + 13).toFixed(1)}" text-anchor="middle" fill="#22d3ee" font-family="ui-monospace, SFMono-Regular, monospace" font-size="10.5" font-weight="700">${dateStr}</text>`
            + card
            + `</g>`;
        }

        return `<svg viewBox="0 0 ${width} ${height}" role="img" aria-label="Multi-horizon 5D, 20D, and 60D rolling net flow impulse">${grid}<path d="${linePath(pts60)}" fill="none" stroke="${COLORS.warning}" stroke-width="1.6" stroke-dasharray="3 2" opacity="0.85"/><path d="${linePath(pts20)}" fill="none" stroke="${COLORS.positive}" stroke-width="2"/><path d="${linePath(pts5)}" fill="none" stroke="${COLORS.cyan}" stroke-width="1.6" opacity="0.92"/>${overlay}${frame.dateTicks}</svg>`;
      },

      async init() {
        this.flowDestroyed = false;
        this._flowReadUrl();
        this.flowHistoryHandler = event => reconcileFlowHistory(this, event.detail?.search || window.location.search);
        window.addEventListener('flow-historychange', this.flowHistoryHandler);
        this._flowBootstrap();
      },

      destroy() {
        this.flowDestroyed = true;
        if (this.flowHistoryHandler) window.removeEventListener('flow-historychange', this.flowHistoryHandler);
        if (this.flowAbortController) this.flowAbortController.abort();
        for (const controller of this.flowBootstrapControllers) controller.abort();
        this.flowBootstrapControllers = [];
        this.flowRequestId += 1;
        this.flowAbortController = null;
        this.flowData = null;
        this.flowDataState = 'pending';
        this.flowDataError = '';
        this.flowSearchOpen = false;
        this.flowSearchActiveIndex = -1;
        this._flowMetricCacheKey = '';
        this._flowMetricCache = null;
      },

      async _flowBootstrap() {
        this.flowInitialLoading = true;
        this.flowCatalogError = '';
        this.flowManifestError = '';
        const catalogController = new AbortController();
        const manifestController = new AbortController();
        this.flowBootstrapControllers.push(catalogController, manifestController);
        const catalogPromise = fetch('data/flows/catalog.json', { signal: catalogController.signal })
          .then(async response => {
            if (!response.ok) throw new Error(`Catalog request returned ${response.status}.`);
            const payload = await response.json();
            const primary = Array.isArray(payload?.instruments) ? payload.instruments : [];
            const featured = primary.filter(item => item?.featured).length;
            const tickers = new Set(primary.map(item => String(item?.ticker || '').toUpperCase()));
            if (primary.length !== 145 || featured !== 24 || tickers.size !== 145) {
              throw new Error('Local catalog export failed its 145-instrument, 24-featured integrity checks.');
            }
            return payload;
          });
        const manifestPromise = fetch('data/flows/manifest.json', { signal: manifestController.signal })
          .then(async response => {
            if (!response.ok) throw new Error(`Local coverage manifest request returned ${response.status}.`);
            const payload = await response.json();
            const entries = payload?.etfs && typeof payload.etfs === 'object' ? payload.etfs : {};
            if (payload?.complete !== true || payload?.status !== 'complete' || payload?.counts?.instruments !== 145 || Object.keys(entries).length !== 145 || payload?.source?.network_fetch !== false) {
              throw new Error('Local coverage manifest failed its 145-instrument completeness checks.');
            }
            return payload;
          });
        const alphaPromise = fetch('data/alpha_signals.json')
          .then(async response => response.ok ? response.json() : null)
          .catch(() => null);
        const [catalogResult, manifestResult, alphaResult] = await Promise.allSettled([catalogPromise, manifestPromise, alphaPromise]);
        this.flowBootstrapControllers = [];
        if (this.flowDestroyed) return;
        if (catalogResult.status === 'fulfilled' && catalogResult.value && Array.isArray(catalogResult.value.instruments)) {
          this.flowCatalog = catalogResult.value;
          this._flowEntryMap = Object.fromEntries(this.flowAllEntries.map(item => [item.ticker, item]));
        } else {
          this.flowCatalogError = catalogResult.status === 'rejected' ? String(catalogResult.reason?.message || catalogResult.reason) : 'The curated catalog is empty.';
        }
        if (alphaResult.status === 'fulfilled' && alphaResult.value) {
          this.flowAlphaSignalsData = alphaResult.value;
          if (Array.isArray(alphaResult.value.active_signals) && this.flowCatalog?.instruments) {
            const enrichmentMap = new Map(alphaResult.value.active_signals.map(s => [s.ticker, s]));
            for (const inst of this.flowCatalog.instruments) {
              const enrichment = enrichmentMap.get(inst.ticker);
              if (enrichment) {
                inst.archetype = enrichment.archetype;
                inst.archetype_label = enrichment.archetype_label;
                inst.live_signal = enrichment.live_signal;
                inst.live_signal_label = enrichment.live_signal_label;
                inst.paired_bear = enrichment.paired_bear;
                inst.paired_bull = enrichment.paired_bull;
                inst.smart_twin = enrichment.smart_twin;
                inst.conviction = enrichment.conviction;
                inst.dd_from_60d_high_pct = enrichment.dd_from_60d_high_pct;
                inst.rally_from_60d_low_pct = enrichment.rally_from_60d_low_pct;
              }
            }
          }
        }
        if (manifestResult.status === 'fulfilled' && manifestResult.value && typeof manifestResult.value === 'object') {
          this.flowManifest = manifestResult.value;
        } else {
          this.flowManifestError = manifestResult.status === 'rejected' ? String(manifestResult.reason?.message || manifestResult.reason) : 'The coverage manifest is empty.';
        }
        this.flowInitialLoading = false;
        if (this.flowCatalogError) return;
        const requested = this.flowTicker;
        const initialTicker = this._flowEntryMap[requested] ? requested : (this.flowFeaturedInstruments[0]?.ticker || this.flowPrimaryInstruments[0]?.ticker || '');
        await this.selectFlowTicker(initialTicker, { writeUrl: false, push: false });
      },

      flowChartTabAvailable(key) {
        return CHART_TABS.some(tab => tab.key === key) && (key !== 'price' || this.flowPriceAvailable);
      },

      flowEnsureChartTab() {
        if (!this.flowData || this.flowChartTabAvailable(this.flowChartTab)) return false;
        const unavailable = this.flowChartTab;
        this.flowChartTab = 'daily';
        this.flowChartMessage = unavailable === 'price' ? 'Price and flow is unavailable for this selected window.' : '';
        return true;
      },

      flowSetChartTab(key, focusButton, writeUrl) {
        const known = CHART_TABS.some(tab => tab.key === key);
        if (!known) return false;
        if (!this.flowChartTabAvailable(key)) {
          this.flowChartMessage = key === 'price' ? 'Price and flow is unavailable for this selected window.' : 'This analysis view is unavailable for the selected window.';
          if (focusButton && typeof this.$nextTick === 'function') {
            this.$nextTick(() => document.getElementById(`flow-chart-tab-${key}`)?.focus());
          }
          return false;
        }
        this.flowChartTab = key;
        if (key === 'price' && this.flowPriceAvailable) {
          this.flowPriceEnabled = true;
        }
        this.flowChartMessage = '';
        this.flowChartTooltip = { visible: false, index: this.flowChartTooltip.index };
        if (focusButton && typeof this.$nextTick === 'function') {
          this.$nextTick(() => document.getElementById(`flow-chart-tab-${key}`)?.focus());
        }
        if (writeUrl !== false) this._flowWriteUrl(false);
        return true;
      },

      flowChartTabKeydown(event, index) {
        const tabs = CHART_TABS;
        let currentIndex = tabs.findIndex(tab => tab.key === this.flowChartTab);
        if (currentIndex < 0) currentIndex = Math.max(0, Math.min(index, tabs.length - 1));
        let nextIndex = currentIndex;
        if (event.key === 'ArrowRight' || event.key === 'ArrowDown') nextIndex = (currentIndex + 1) % tabs.length;
        else if (event.key === 'ArrowLeft' || event.key === 'ArrowUp') nextIndex = (currentIndex - 1 + tabs.length) % tabs.length;
        else if (event.key === 'Home') nextIndex = 0;
        else if (event.key === 'End') nextIndex = tabs.length - 1;
        else return;
        event.preventDefault();
        this.flowSetChartTab(tabs[nextIndex].key, true, true);
      },

      flowChartPointerMove(event) {
        const rows = this.flowSelectedRows;
        if (!rows.length) return;
        let index = rows.length - 1;
        const clientX = Number.isFinite(event?.clientX) ? event.clientX : null;
        const clientY = Number.isFinite(event?.clientY) ? event.clientY : null;
        if (event?.type === 'mousemove' && clientX !== null) {
          const target = event.currentTarget;
          const svg = target?.querySelector?.('svg') || target;
          const rect = svg?.getBoundingClientRect?.();
          if (rect?.width) index = Math.round(((clientX - rect.left) / rect.width) * (rows.length - 1));
        }
        const clamped = Math.max(0, Math.min(rows.length - 1, index));
        this.flowHoverIndex = clamped;
        this.flowChartTooltip = {
          visible: true,
          index: clamped,
          x: clientX,
          y: clientY
        };
      },

      flowChartPointerLeave() {
        this.flowHoverIndex = null;
        this.flowChartTooltip = { ...this.flowChartTooltip, visible: false };
      },

      flowChartFocus() {
        if (!this.flowSelectedRows.length) return;
        const clamped = this.flowSelectedRows.length - 1;
        this.flowHoverIndex = clamped;
        this.flowChartTooltip = {
          visible: true,
          index: clamped
        };
      },

      get flowChartTooltipStyle() {
        if (!this.flowChartTooltip || !this.flowChartTooltip.visible) {
          return 'display: none;';
        }
        const offsetX = 16;
        const offsetY = 16;
        const boxWidth = 320;
        const boxHeight = 60;
        const viewportWidth = typeof window !== 'undefined' ? window.innerWidth : 1024;
        const viewportHeight = typeof window !== 'undefined' ? window.innerHeight : 768;
        let left = (this.flowChartTooltip.x || 0) + offsetX;
        let top = (this.flowChartTooltip.y || 0) + offsetY;
        if (left + boxWidth > viewportWidth - 8) {
          left = Math.max(8, (this.flowChartTooltip.x || 0) - boxWidth - offsetX);
        }
        if (top + boxHeight > viewportHeight - 8) {
          top = Math.max(8, (this.flowChartTooltip.y || 0) - boxHeight - offsetY);
        }
        return `left: ${Math.round(left)}px; top: ${Math.round(top)}px;`;
      },

      get flowChartTooltipText() {
        if (!this.flowChartTooltip.visible) return '';
        const row = this.flowSelectedRows[this.flowChartTooltip.index];
        if (!row) return '';
        return [
          row.date,
          `Daily flow ${formatMoney(row.flow)}`,
          `20-session mean ${formatMoney(row.rollingMean20)}`,
          `10-session percentile ${formatPercent(row.percentile10)}`,
          `Prior-only z ${row.priorOnlyZScore === null ? '—' : row.priorOnlyZScore.toFixed(2)}`
        ].join(' · ');
      },

      flowSearch() {
        this.flowSearchOpen = true;
        this.flowSearchActiveIndex = -1;
      },

      flowSearchDown() {
        this.flowSearchOpen = true;
        const count = this.flowSearchResults.length;
        if (!count) return;
        this.flowSearchActiveIndex = (this.flowSearchActiveIndex + 1) % count;
      },

      flowSearchUp() {
        this.flowSearchOpen = true;
        const count = this.flowSearchResults.length;
        if (!count) return;
        this.flowSearchActiveIndex = this.flowSearchActiveIndex <= 0 ? count - 1 : this.flowSearchActiveIndex - 1;
      },

      flowSearchEnter() {
        const result = this.flowSearchResults[Math.max(0, this.flowSearchActiveIndex)];
        if (!result) return;
        this.selectFlowTicker(result.item.ticker, { writeUrl: true, push: true });
      },

      chooseFlowResult(result) {
        if (!result?.item?.ticker) return;
        this.selectFlowTicker(result.item.ticker, { writeUrl: true, push: true });
      },

      clearFlowSearch() {
        this.flowSearchQuery = '';
        this.flowSearchActiveIndex = -1;
        this.flowSearchOpen = true;
        this.$nextTick(() => document.getElementById('flow-catalog-search')?.focus());
      },

      async selectFlowTicker(ticker, options) {
        const normalized = String(ticker || '').trim().toUpperCase();
        const settings = options || {};
        if (settings.switchToStudio) this.flowViewMode = 'studio';
        if (!normalized || !this._flowEntryMap[normalized]) {
          if (this.flowAbortController) this.flowAbortController.abort();
          this.flowRequestId += 1;
          this.flowAbortController = null;
          this.flowTicker = normalized;
          this.flowData = null;
          this.flowDataState = 'unavailable';
          this.flowDataError = 'This ticker is not in the 145-instrument local leveraged/inverse catalog.';
          this._flowMetricCacheKey = '';
          this._flowMetricCache = null;
          if (settings.writeUrl !== false) this._flowWriteUrl(Boolean(settings.push));
          return;
        }
        this.flowTicker = normalized;
        this.flowSearchOpen = false;
        this.flowSearchQuery = '';
        this.flowSearchActiveIndex = -1;
        await this._flowLoadTicker(normalized, settings.writeUrl !== false, Boolean(settings.push));
      },

      async _flowLoadTicker(ticker, writeUrl, push) {
        const item = this._flowEntryMap[ticker];
        if (!item) return;
        if (this.flowAbortController) this.flowAbortController.abort();
        const requestId = ++this.flowRequestId;
        const controller = new AbortController();
        let timedOut = false;
        let timeoutId = null;
        this.flowAbortController = controller;
        this.flowDataError = '';
        this.flowData = null;
        this.flowDataState = 'loading';
        this._flowMetricCacheKey = '';
        this._flowMetricCache = null;
        const manifestEntry = this.flowManifest?.etfs?.[ticker] || null;
        const revision = expectedRevision(manifestEntry, this.flowCatalog?.catalog_version);
        const cached = FLOW_CACHE.get(cacheKey(ticker, revision));
        if (cached) {
          this.flowData = cached;
          this.flowDataState = cached.state;
          this.flowAbortController = null;
          this.flowResolvedStates = { ...this.flowResolvedStates, [ticker]: cached.state };
          this._flowApplyUrlRange();
          this.flowPriceEnabled = this.flowPriceEnabled && this.flowPriceAvailable;
          const chartChanged = this.flowEnsureChartTab();
          if (writeUrl || chartChanged) this._flowWriteUrl(push);
          return;
        }
        timeoutId = setTimeout(() => {
          timedOut = true;
          controller.abort();
        }, 15000);
        try {
          const response = await fetch(`data/flows/${encodeURIComponent(ticker)}.json?revision=${encodeURIComponent(revision)}`, { signal: controller.signal });
          if (requestId !== this.flowRequestId) return;
          if (response.status === 404) {
            this.flowDataState = 'pending';
            this.flowDataError = 'No checked-in source history is available yet. Analytics remain unavailable.';
            this.flowResolvedStates = { ...this.flowResolvedStates, [ticker]: 'pending' };
            this._flowSetDefaultRange();
            if (writeUrl) this._flowWriteUrl(push);
            return;
          }
          if (response.status === 410) {
            this.flowDataState = 'unavailable';
            this.flowDataError = 'Source history is unavailable for this catalog row.';
            this.flowResolvedStates = { ...this.flowResolvedStates, [ticker]: 'unavailable' };
            if (writeUrl) this._flowWriteUrl(push);
            return;
          }
          if (!response.ok) throw new Error(`Source history request returned ${response.status}.`);
          const payload = await response.json();
          if (requestId !== this.flowRequestId) return;
          const normalized = normalizeFlowPayload(payload, item, manifestEntry);
          FLOW_CACHE.set(cacheKey(ticker, revision), normalized);
          FLOW_CACHE.set(cacheKey(ticker, normalized.revision), normalized);
          this.flowData = normalized;
          this.flowDataState = normalized.state;
          this.flowResolvedStates = { ...this.flowResolvedStates, [ticker]: normalized.state };
          this._flowApplyUrlRange();
          this.flowPriceEnabled = this.flowPriceEnabled && this.flowPriceAvailable;
          const chartChanged = this.flowEnsureChartTab();
          if (writeUrl || chartChanged) this._flowWriteUrl(push);
        } catch (error) {
          if (requestId !== this.flowRequestId) return;
          if (error?.name === 'AbortError' && !timedOut) return;
          this.flowDataState = 'error';
          this.flowDataError = timedOut ? 'Static file request timed out.' : String(error?.message || error);
          this.flowResolvedStates = { ...this.flowResolvedStates, [ticker]: 'error' };
          if (writeUrl) this._flowWriteUrl(push);
        } finally {
          if (timeoutId !== null) clearTimeout(timeoutId);
          if (requestId === this.flowRequestId) this.flowAbortController = null;
        }
      },

      retryFlowData() {
        if (this.flowTicker) this._flowLoadTicker(this.flowTicker, true, false);
      },

      _flowSetDefaultRange() {
        const count = this.flowData?.records?.length || 0;
        this.flowEndIndex = Math.max(0, count - 1);
        this.flowStartIndex = 0;
        this.flowRangePreset = 'max';
      },

      _flowApplyUrlRange() {
        const records = this.flowData?.records || [];
        if (!records.length) {
          this.flowStartIndex = 0;
          this.flowEndIndex = 0;
          return;
        }
        const params = new URLSearchParams(window.location.search);
        const startDate = params.get('start');
        const endDate = params.get('end');
        if (startDate && endDate) {
          const startIndex = records.findIndex(record => record.date >= startDate);
          let endIndex = -1;
          for (let index = records.length - 1; index >= 0; index -= 1) {
            if (records[index].date <= endDate) {
              endIndex = index;
              break;
            }
          }
          if (startIndex >= 0 && endIndex >= startIndex) {
            this.flowStartIndex = startIndex;
            this.flowEndIndex = endIndex;
            this.flowRangePreset = 'custom';
            return;
          }
        }
        const preset = params.get('range');
        this.flowSetRangePreset(RANGE_PRESETS.some(item => item.key === preset) ? preset : '1y', false);
      },

      flowSetRangePreset(preset, writeUrl) {
        const records = this.flowData?.records || [];
        const count = records.length;
        if (!count) return;
        const definition = RANGE_PRESETS.find(item => item.key === preset) || RANGE_PRESETS.find(item => item.key === 'max');
        const selectedCount = definition.count === null ? count : Math.min(count, definition.count);
        this.flowStartIndex = Math.max(0, count - selectedCount);
        this.flowEndIndex = count - 1;
        this.flowRangePreset = preset;
        this.flowEnsureChartTab();
        if (writeUrl !== false) this._flowWriteUrl(false);
      },

      flowSetStartIndex(value) {
        const count = this.flowData?.records?.length || 0;
        if (!count) return;
        const minimumWindow = count > 1 ? 2 : 1;
        const next = Math.max(0, Math.min(Math.trunc(Number(value) || 0), this.flowEndIndex - minimumWindow + 1));
        this.flowStartIndex = next;
        this.flowRangePreset = 'custom';
        this.flowEnsureChartTab();
        this._flowWriteUrl(false);
      },

      flowSetEndIndex(value) {
        const count = this.flowData?.records?.length || 0;
        if (!count) return;
        const minimumWindow = count > 1 ? 2 : 1;
        const next = Math.min(count - 1, Math.max(Math.trunc(Number(value) || 0), this.flowStartIndex + minimumWindow - 1));
        this.flowEndIndex = next;
        this.flowRangePreset = 'custom';
        this.flowEnsureChartTab();
        this._flowWriteUrl(false);
      },

      flowSetPriceEnabled(value) {
        this.flowPriceEnabled = Boolean(value) && this.flowPriceAvailable;
        if (!this.flowPriceEnabled && this.flowChartTab === 'price') this.flowChartTab = 'daily';
        this.flowEnsureChartTab();
        this._flowWriteUrl(false);
      },

      _flowReadUrl() {
        const state = flowHistoryState(window.location.search);
        this.flowTicker = state.ticker;
        this.flowRangePreset = state.range;
        this.flowPriceEnabled = state.priceEnabled;
        this.flowChartTab = state.chart;
      },

      _flowWriteUrl(push) {
        if (typeof window === 'undefined') return;
        const url = new URL(window.location.href);
        url.searchParams.set('tab', 'flows');
        if (this.flowTicker) url.searchParams.set('flow', this.flowTicker);
        else url.searchParams.delete('flow');
        url.searchParams.delete('ticker');
        if (CHART_TABS.some(tab => tab.key === this.flowChartTab)) url.searchParams.set('chart', this.flowChartTab);
        else url.searchParams.delete('chart');
        if (this.flowData?.records?.length) {
          if (this.flowRangePreset === 'custom') {
            url.searchParams.set('start', this.flowRangeStartDate);
            url.searchParams.set('end', this.flowRangeEndDate);
            url.searchParams.delete('range');
          } else {
            url.searchParams.set('range', this.flowRangePreset);
            url.searchParams.delete('start');
            url.searchParams.delete('end');
          }
        }
        if (this.flowPriceEnabled && this.flowPriceAvailable) url.searchParams.set('price', '1');
        else url.searchParams.delete('price');
        const method = push ? 'pushState' : 'replaceState';
        window.history[method]({}, '', url);
      },

      _flowHandlePopState() {
        reconcileFlowHistory(this, window.location.search);
      },

      flowCloseResults() {
        this.flowSearchOpen = false;
        this.flowSearchActiveIndex = -1;
      },

      flowResetFilters() {
        this.flowTierFilter = 'all';
        this.flowCategoryFilter = 'all';
        this.flowStatusFilter = 'all';
        this.flowScannerRegime = 'all';
        this.flowScannerDirection = 'all';
        this.flowSearchQuery = '';
      },

      titleCaseCategory(value) {
        return titleCaseCategory(value);
      },

      shortCategoryName(value) {
        return shortCategoryName(value);
      },

      formatMoney(value) {
        return formatMoney(value);
      },

      formatPercent(value) {
        return formatPercent(value);
      },

      formatSignedPercent(value) {
        return formatSignedPercent(value);
      },

      formatPrice(value) {
        return formatPrice(value);
      },

      flowAnnouncement() {
        if (this.flowInitialLoading) return 'Loading the curated fund flow catalog.';
        if (this.flowDataState === 'loading') return `Loading ${this.flowTicker} source history.`;
        if (!this.flowData) return `${this.flowTicker} source history is ${statusLabel(this.flowDataState).toLowerCase()}.`;
        return `${this.flowTicker} loaded with ${this.flowData.count} available source observations.`;
      }
    };
  }

  const api = {
    CHART_TABS,
    RANGE_PRESETS,
    buildWindowMetrics,
    cacheKey,
    completeRollingMeans: (records, windowSize) => (records || []).map((record, index) => completeRollingMean(records, index, windowSize)),
    completeRollingSums,
    empiricalPercentile,
    flowHistoryState,
    flowMetricRows,
    flowResearchApp,
    formatMoney,
    finiteScale,
    linePath,
    marketHistoryTab,
    normalizeFlowPayload,
    priorOnlyZScore,
    reconcileFlowHistory,
    statusFromManifest,
    statusLabel
  };

  root.FlowResearch = api;
  root.flowResearchApp = flowResearchApp;
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
})(typeof window === 'undefined' ? globalThis : window);
