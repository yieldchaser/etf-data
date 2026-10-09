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
    { key: 'workbench', label: 'Workbench' },
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
    const chart = params.get('chart') || 'workbench';
    const view = params.get('view');
    const ticker = String(params.get('flow') || params.get('ticker') || '').trim().toUpperCase();
    return {
      ticker,
      range: params.get('range') || '1y',
      priceEnabled: params.get('price') !== '0',
      chart: CHART_TABS.some(tab => tab.key === chart) ? chart : 'workbench',
      view: ['studio', 'scanner', 'matrix', 'alpha'].includes(view) ? view : (ticker ? 'studio' : null)
    };
  }

  function reconcileFlowHistory(app, search) {
    const next = flowHistoryState(search);
    if (next.view) app.flowViewMode = next.view;
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
    const raw = String(value || 'Catalog').replace(/^\d+\.\s*/, '').replace(/\s*\(Pruned.*\)$/i, '');
    return raw
      .replace('Single-Stock Leveraged (Bull) — AI, Semis & High-Beta Tech', 'AI & Semis Single-Stock (Bull)')
      .replace('Single-Stock Leveraged (Bull) — Mega-Cap Giants, Crypto & Consumer', 'Mega-Cap & Crypto (Bull)')
      .replace('Technology, Semiconductor & Thematic (Bull)', 'Tech & Thematic (Bull)')
      .replace('Broad Market Equity Index (Bull)', 'Broad Market Index (Bull)')
      .replace('Sector Specific Leveraged (Bull)', 'Sector Specific (Bull)')
      .replace('Commodities, Energy & Volatility (Bull)', 'Commodities & Vol (Bull)')
      .replace('Fixed Income, Currencies & Crypto (Bull)', 'Fixed Income & Bonds (Bull)')
      .replace('Selective Benchmark Hedging / Tactical Shorts', 'Tactical Benchmark Shorts');
  }

  function formatMoney(value, options) {
    const number = finiteNumber(value);
    if (number === null) return '—';
    const settings = options || {};
    const sign = settings.signed === false ? '' : number > 0 ? '+' : number < 0 ? '−' : '';
    const absolute = Math.abs(number);
    const prefix = number < 0 ? '−$' : '$';
    if (absolute >= 1e12) return `${sign === '−' ? '' : sign}${prefix}${(absolute / 1e12).toFixed(2)}T`;
    if (absolute >= 1e9) {
      const b = absolute / 1e9;
      const formattedB = (b >= 10 && b % 1 === 0) ? b.toFixed(0) : b.toFixed(2);
      return `${sign === '−' ? '' : sign}${prefix}${formattedB}B`;
    }
    if (absolute >= 1e6) {
      const m = absolute / 1e6;
      const formattedM = (m >= 100 && m % 1 === 0) ? m.toFixed(0) : m.toFixed(1);
      return `${sign === '−' ? '' : sign}${prefix}${formattedM}M`;
    }
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

  function flowMetricRows(records, volumeMap) {
    const source = Array.isArray(records) ? records : [];
    const rollingTwenty = source.map((record, index) => completeRollingMean(source, index, 20));
    const rollingFive = completeRollingSums(source, 5);
    const rollingTen = completeRollingSums(source, 10);
    const rollingTwentySum = completeRollingSums(source, 20);
    const rollingSixtySum = completeRollingSums(source, 60);
    const sortedTen = rollingTen.filter(Number.isFinite).sort((left, right) => left - right);
    const volumes = source.map(record => {
      const v = (volumeMap && volumeMap[record.date]) ?? finiteNumber(record.volume);
      if (v !== null && v !== undefined && v > 0) return v;
      if (record.nav && record.flow !== null && record.flow !== undefined) {
        return Math.round(Math.abs(record.flow) / record.nav);
      }
      return null;
    });
    const rollingVolTwenty = volumes.map((_, index) => {
      const slice = volumes.slice(Math.max(0, index - 19), index + 1).filter(v => v !== null);
      return slice.length ? slice.reduce((a, b) => a + b, 0) / slice.length : null;
    });

    return source.map((record, index) => {
      const vol = volumes[index];
      const dollarVol = (vol && record.nav) ? vol * record.nav : null;
      const f = finiteNumber(record.flow);
      const conviction = (f !== null && dollarVol && dollarVol > 0) ? (Math.abs(f) / dollarVol) * 100 : null;
      return {
        date: record.date,
        flow: f,
        nav: finiteNumber(record.nav),
        performance: finiteNumber(record.performance),
        volume: vol,
        dollarVolume: dollarVol,
        convictionRatio: conviction,
        rollingMean20: rollingTwenty[index],
        rollingVolume20: rollingVolTwenty[index],
        rollingSum5: rollingFive[index],
        rollingSum10: rollingTen[index],
        rollingSum20: rollingTwentySum[index],
        rollingSum60: rollingSixtySum[index],
        percentile10: percentileAgainstSorted(sortedTen, rollingTen[index]),
        priorOnlyZScore: priorOnlyZScore(source, index, 30),
        selectedCumulative: null
      };
    });
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

  function cachedFlowMetricRows(revision, records, volumeMap) {
    const key = String(revision || `${records.length}:unknown`) + (volumeMap ? ':vol' : '');
    const cached = FLOW_METRIC_CACHE.get(key);
    if (cached) return cached;
    const rows = flowMetricRows(records, volumeMap);
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
    const w = width || 88;
    const h = height || 22;
    const series = Array.isArray(values) ? values.map(finiteNumber).filter(v => v !== null) : [];
    if (!series.length) return `<svg viewBox="0 0 ${w} ${h}" width="${w}" height="${h}" aria-hidden="true"></svg>`;
    const maxAbs = Math.max(0.01, ...series.map(v => Math.abs(v)));
    const midY = h / 2;
    const slot = w / series.length;
    const barW = Math.max(1.8, Math.min(4.2, slot * 0.76));
    let rects = `<line x1="0" y1="${midY.toFixed(1)}" x2="${w}" y2="${midY.toFixed(1)}" stroke="rgba(255,255,255,0.18)" stroke-width="1"/>`;
    series.forEach((val, idx) => {
      const x = idx * slot + (slot - barW) / 2;
      const barH = Math.max(1.2, (Math.abs(val) / maxAbs) * (midY - 2));
      const y = val >= 0 ? midY - barH : midY;
      const fill = val > 0 ? COLORS.positive : val < 0 ? COLORS.negative : COLORS.neutral;
      rects += `<rect x="${x.toFixed(1)}" y="${y.toFixed(1)}" width="${barW.toFixed(1)}" height="${barH.toFixed(1)}" rx="0.5" fill="${fill}" opacity="0.92"/>`;
    });
    const peakStr = maxAbs >= 1e9 ? `$${(maxAbs / 1e9).toFixed(2)}B` : maxAbs >= 1e6 ? `$${(maxAbs / 1e6).toFixed(1)}M` : maxAbs >= 1e3 ? `$${(maxAbs / 1e3).toFixed(0)}K` : `$${maxAbs.toFixed(0)}`;
    return `<svg viewBox="0 0 ${w} ${h}" width="${w}" height="${h}" role="img" aria-label="20D net flow pulse" style="display:block;margin:0 auto"><title>20D daily flow pulse (peak ±${peakStr})</title>${rects}</svg>`;
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

  function formatShareVolume(value) {
    const number = finiteNumber(value);
    if (number === null) return '—';
    const absolute = Math.abs(number);
    if (absolute >= 1e9) return `${(absolute / 1e9).toFixed(1)}B shs`;
    if (absolute >= 1e6) return `${(absolute / 1e6).toFixed(1)}M shs`;
    if (absolute >= 1e3) return `${(absolute / 1e3).toFixed(0)}k shs`;
    return `${absolute.toLocaleString('en-US')} shs`;
  }

  function cleanCategoryName(value) {
    if (!value) return '';
    return String(value).replace(/^\d+\.\s*/, '').trim();
  }

  function dateTickIndices(count, width) {
    if (count <= 1) return [0];
    const desired = width < 400 ? 2 : width < 500 ? 3 : width < 850 ? 4 : 6;
    return Array.from(new Set(Array.from({ length: desired }, (_, index) => Math.round((count - 1) * index / (desired - 1)))));
  }

  function axisDate(value, spanDays) {
    if (!value) return '—';
    const s = String(value);
    const parts = s.split('-');
    if (parts.length < 3) return s.slice(0, 7);
    const [year, month, day] = parts;
    const mNum = parseInt(month, 10);
    const dNum = parseInt(day, 10);
    const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    const mStr = MONTHS[mNum - 1] || month;

    if (spanDays !== undefined && spanDays !== null) {
      if (spanDays <= 120) {
        return `${mStr} ${dNum}`;
      }
      if (spanDays <= 450) {
        return `${mStr} '${year.slice(2)}`;
      }
      return `${year}-${month}`;
    }

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

    const recs = options.records || [];
    let spanDays = 365;
    if (recs.length > 1 && recs[0]?.date && recs[recs.length - 1]?.date) {
      const d1 = new Date(recs[0].date);
      const d2 = new Date(recs[recs.length - 1].date);
      spanDays = Math.max(1, Math.round((d2 - d1) / 86400000));
    }

    const indices = dateTickIndices(recs.length, width);
    const usedLabels = new Set();
    const dateTicks = indices.map(index => {
      const x = xScale(index);
      let label = axisDate(recs[index]?.date, spanDays);
      if (usedLabels.has(label)) {
        const d = String(recs[index]?.date || '').split('-');
        if (d.length >= 3) {
          const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
          label = `${MONTHS[parseInt(d[1], 10) - 1] || d[1]} ${parseInt(d[2], 10)}`;
        }
      }
      usedLabels.add(label);
      return `<text x="${x.toFixed(1)}" y="${height - 8}" text-anchor="${index === 0 ? 'start' : index === recs.length - 1 ? 'end' : 'middle'}" fill="${COLORS.subtle}" font-family="ui-monospace, SFMono-Regular, monospace" font-size="11">${escapeHtml(label)}</text>`;
    }).join('');
    return { width, height, padding, chartWidth, chartHeight, xScale, dateTicks };
  }

  function chartCrosshairOverlay(frame, rows, hoverIndex, targetY, yLabel, tooltipItems) {
    if (hoverIndex === null || hoverIndex === undefined || hoverIndex < 0 || hoverIndex >= rows.length) return '';
    const row = rows[hoverIndex];
    if (!row) return '';
    const x = frame.xScale(hoverIndex);
    const numY = finiteNumber(targetY);
    if (numY === null) return '';
    return `<g class="flow-crosshair-group" pointer-events="none"><circle cx="${x.toFixed(1)}" cy="${numY.toFixed(1)}" r="5" fill="#22d3ee" stroke="#ffffff" stroke-width="2"/></g>`;
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
      flowActiveCategoryTab: 'featured',
      flowUniverseDrawerOpen: false,
      flowAlphaSignalsData: null,
      flowAlphaFilter: 'all',
      flowHoverIndex: null,
      flowHoverChart: null,
      flowViewMode: 'scanner',
      flowShowMarketPulse: true,
      flowShowOutliers: true,
      flowScannerSort: 'abs_z',
      flowScannerAsc: false,
      flowScannerRegime: 'all',
      flowScannerDirection: 'all',
      flowScatterFilter: 'all',
      flowScatterHoverTicker: null,
      flowShowCatalogDrawer: false,
      flowVolumeCache: null,
      flowWorkbenchPriceEnabled: true,
      flowWorkbenchCumEnabled: true,
      flowWorkbenchDailyEnabled: true,
      flowWorkbenchVolumeEnabled: true,
      flowWorkbenchZScoreEnabled: true,
      flowWorkbenchShocksEnabled: false,
      flowScaleMode: 'usd',
      flowMeasureActive: false,
      flowMeasureDragging: false,
      flowMeasureStartIdx: null,
      flowMeasureCurrentIdx: null,
      flowMeasureResult: null,
      _flowWorkbenchBaseKey: '',
      _flowWorkbenchBaseSvg: '',
      _flowUrlTimer: null,
      _rangeRaf: null,
      flowChartTab: 'workbench',
      flowChartMessage: '',
      flowChartTooltip: { visible: false, index: 0 },
      flowStartIndex: 0,
      flowEndIndex: 0,
      flowActiveThumb: 'end',
      flowRangePreset: '1y',
      flowPriceEnabled: true,
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
        const total = this.flowData?.records?.length || 0;
        if (total < 2) return RANGE_PRESETS;
        const filtered = RANGE_PRESETS.filter(p => {
          if (p.key === 'max') return true;
          return p.count && p.count < total * 0.90;
        });
        return filtered.length ? filtered : RANGE_PRESETS.filter(p => p.key === 'max');
      },

      get flowDisplayCategory() {
        return cleanCategoryName(this.flowSelectedInstrument?.category);
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
        const total = this.flowPrimaryInstruments.length || 150;
        return [
          { key: 'all', label: `All (${total})`, count: total, title: `All ${total} Leveraged & Inverse ETFs` },
          { key: 'featured', label: `Featured (${this.flowFeaturedInstruments.length || 24})`, count: this.flowFeaturedInstruments.length, title: '24 Featured Anchor ETFs' },
          { key: 'ai_semis', label: `AI & Semis 2X (${counts['4. Single-Stock Leveraged (Bull) — AI, Semis & High-Beta Tech'] || 46})`, raw: '4. Single-Stock Leveraged (Bull) — AI, Semis & High-Beta Tech', count: counts['4. Single-Stock Leveraged (Bull) — AI, Semis & High-Beta Tech'] || 46 },
          { key: 'mega_crypto', label: `Mega & Crypto 2X (${counts['5. Single-Stock Leveraged (Bull) — Mega-Cap Giants, Crypto & Consumer'] || 36})`, raw: '5. Single-Stock Leveraged (Bull) — Mega-Cap Giants, Crypto & Consumer', count: counts['5. Single-Stock Leveraged (Bull) — Mega-Cap Giants, Crypto & Consumer'] || 36 },
          { key: 'tech_3x', label: `Tech & Semis 3X (${counts['2. Technology, Semiconductor & Thematic (Bull)'] || 15})`, raw: '2. Technology, Semiconductor & Thematic (Bull)', count: counts['2. Technology, Semiconductor & Thematic (Bull)'] || 15 },
          { key: 'sectors_3x', label: `Sectors 3X (${counts['3. Sector Specific Leveraged (Bull)'] || 9})`, raw: '3. Sector Specific Leveraged (Bull)', count: counts['3. Sector Specific Leveraged (Bull)'] || 9 },
          { key: 'shorts', label: `Shorts & Hedges (${counts['9. Selective Benchmark Hedging / Tactical Shorts (Pruned to Key Anchors Only)'] || 14})`, raw: '9. Selective Benchmark Hedging / Tactical Shorts (Pruned to Key Anchors Only)', count: counts['9. Selective Benchmark Hedging / Tactical Shorts (Pruned to Key Anchors Only)'] || 14 },
          { key: 'commodities', label: `Commodities & Vol (${counts['6. Commodities, Energy & Volatility (Bull)'] || 8})`, raw: '6. Commodities, Energy & Volatility (Bull)', count: counts['6. Commodities, Energy & Volatility (Bull)'] || 8 },
          { key: 'broad_index', label: `Broad Index (${counts['1. Broad Market Equity Index (Bull)'] || 8})`, raw: '1. Broad Market Equity Index (Bull)', count: counts['1. Broad Market Equity Index (Bull)'] || 8 },
          { key: 'rates_fixed', label: `Rates & Crypto (${counts['7. Fixed Income, Currencies & Crypto (Bull)'] || 9})`, raw: '7. Fixed Income, Currencies & Crypto (Bull)', count: counts['7. Fixed Income, Currencies & Crypto (Bull)'] || 9 },
          { key: 'intl', label: `International (${counts['8. International / Country (Bull)'] || 5})`, raw: '8. International / Country (Bull)', count: counts['8. International / Country (Bull)'] || 5 }
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
        const basket = Array.isArray(this.flowAlphaSignalsData?.top5_conviction_basket) ? this.flowAlphaSignalsData.top5_conviction_basket : [];
        return basket.map(item => ({
          ...item,
          nav_return_5d_pct: item.nav_return_5d_pct ?? item.ret_5d_pct ?? null,
          nav_return_20d_pct: item.nav_return_20d_pct ?? item.ret_20d_pct ?? null,
          ret_5d_pct: item.ret_5d_pct ?? item.nav_return_5d_pct ?? null,
          ret_20d_pct: item.ret_20d_pct ?? item.nav_return_20d_pct ?? null
        }));
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
        let bullFlow1d = 0;
        let bearFlow1d = 0;
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
          if (lev < 0) {
            bearFlow1d += f1;
            bearFlow20d += f20;
          } else {
            bullFlow1d += f1;
            bullFlow20d += f20;
          }
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
          bullFlow1d,
          bearFlow1d,
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

      get flowTapeData() {
        const pulse = this.flowUniversePulse;
        const items = this.flowPrimaryInstruments;
        let totalFlow5d = 0;
        let sumSq = 0;
        let totalAbs = 0;
        let bullZSum = 0;
        let bullZCount = 0;
        let bearZSum = 0;
        let bearZCount = 0;
        for (const it of items) {
          const f5 = finiteNumber(it.flow_5d) || 0;
          totalFlow5d += f5;
          const f1Abs = Math.abs(finiteNumber(it.latest_flow) || 0);
          totalAbs += f1Abs;
          const z = finiteNumber(it.flow_zscore) || 0;
          const lev = finiteNumber(it.leverage_value) || 0;
          if (lev < 0) {
            bearZSum += z;
            bearZCount += 1;
          } else {
            bullZSum += z;
            bullZCount += 1;
          }
        }
        if (totalAbs > 0) {
          for (const it of items) {
            const f1Abs = Math.abs(finiteNumber(it.latest_flow) || 0);
            const share = f1Abs / totalAbs;
            sumSq += share * share;
          }
        }
        const hhi = sumSq || 0.136;
        const effFunds = hhi > 0 ? (1 / hhi).toFixed(1) : '7.4';
        const bullAvgZ = bullZCount ? bullZSum / bullZCount : 0;
        const bearAvgZ = bearZCount ? bearZSum / bearZCount : 0;
        const spreadZ = bullAvgZ - bearAvgZ;

        return {
          totalFlow1d: pulse.totalFlow1d,
          inflowCount1d: pulse.inflowCount1d,
          outflowCount1d: pulse.outflowCount1d,
          totalFlow5d,
          totalFlow20d: pulse.totalFlow20d,
          bullFlow20d: pulse.bullFlow20d,
          bearFlow20d: pulse.bearFlow20d,
          accCount: pulse.accumulationCount,
          distCount: pulse.distributionCount,
          spreadZ: `${spreadZ >= 0 ? '+' : ''}${spreadZ.toFixed(2)}σ`,
          hhi: hhi.toFixed(3),
          effFunds
        };
      },

      get flowTopMovers() {
        const items = this.flowPrimaryInstruments;
        if (!items.length) return { inflows: [], outflows: [] };
        const valid = items.filter(it => it.latest_flow !== null && Number.isFinite(it.latest_flow));
        const sortedDesc = [...valid].sort((a, b) => (b.latest_flow || 0) - (a.latest_flow || 0));
        const sortedAsc = [...valid].sort((a, b) => (a.latest_flow || 0) - (b.latest_flow || 0));
        return {
          inflows: sortedDesc.slice(0, 3).map(it => ({
            ticker: it.ticker,
            name: it.underlying || it.fund_name,
            leverage: it.leverage || '+2x',
            flow1d: it.latest_flow,
            flow5d: it.flow_5d,
            zscore: it.flow_zscore,
            regime: it.regime || 'BALANCED'
          })),
          outflows: sortedAsc.slice(0, 3).map(it => ({
            ticker: it.ticker,
            name: it.underlying || it.fund_name,
            leverage: it.leverage || '-2x',
            flow1d: it.latest_flow,
            flow5d: it.flow_5d,
            zscore: it.flow_zscore,
            regime: it.regime || 'BALANCED'
          }))
        };
      },

      get flowScatterCounts() {
        const items = this.flowPrimaryInstruments;
        let washout = 0, momentum = 0, trap = 0, squeeze = 0, neutral = 0;
        for (const item of items) {
          const ret = finiteNumber(item.nav_return_20d_pct) || 0;
          const z = finiteNumber(item.flow_zscore) || 0;
          if (ret < 0 && z <= -0.4) washout += 1;
          else if (ret >= 0 && z >= 0.4) momentum += 1;
          else if (ret < 0 && z >= 0.4) trap += 1;
          else if (ret >= 0 && z <= -0.4) squeeze += 1;
          else neutral += 1;
        }
        return {
          all: items.length,
          washout,
          momentum,
          trap,
          squeeze,
          neutral
        };
      },

      get flowActiveCycleAnomalies() {
        const items = this.flowPrimaryInstruments;
        const dipAccum = [];
        const exhaustionLow = [];
        const climaxInflowTop = [];
        const distributionTop = [];

        for (const it of items) {
          const ret20 = finiteNumber(it.nav_return_20d_pct) || 0;
          const z = finiteNumber(it.flow_zscore) || 0;
          const f1 = finiteNumber(it.latest_flow) || 0;
          const f20 = finiteNumber(it.flow_20d) || 0;
          const aum = finiteNumber(it.aum_m) || 0;
          const name = it.underlying || it.fund_name || it.ticker;
          const entry = {
            ticker: it.ticker,
            name,
            category: it.category,
            leverage: it.leverage,
            ret20,
            z,
            flow1d: f1,
            flow20d: f20,
            aum
          };

          if (ret20 <= -5.0 && z >= 0.8) {
            dipAccum.push(entry);
          } else if (ret20 <= -5.0 && z <= -0.8) {
            exhaustionLow.push(entry);
          } else if (ret20 >= 10.0 && z >= 1.0) {
            climaxInflowTop.push(entry);
          } else if (ret20 >= 10.0 && z <= -0.8) {
            distributionTop.push(entry);
          }
        }

        dipAccum.sort((a, b) => b.z - a.z);
        exhaustionLow.sort((a, b) => a.z - b.z);
        climaxInflowTop.sort((a, b) => b.z - a.z);
        distributionTop.sort((a, b) => a.z - b.z);

        return {
          dipAccum: dipAccum.slice(0, 8),
          exhaustionLow: exhaustionLow.slice(0, 8),
          climaxInflowTop: climaxInflowTop.slice(0, 8),
          distributionTop: distributionTop.slice(0, 8),
          totalAnomalies: dipAccum.length + exhaustionLow.length + climaxInflowTop.length + distributionTop.length
        };
      },

      flowHandleScatterClick(event) {
        const circle = event.target.closest('[data-ticker]');
        if (circle) {
          const ticker = circle.getAttribute('data-ticker');
          if (ticker) {
            this.selectFlowTicker(ticker, { writeUrl: true, push: true, switchToStudio: true });
          }
        }
      },

      flowHandleScatterMouseMove(event) {
        const circle = event.target.closest('[data-ticker]');
        if (circle) {
          const ticker = circle.getAttribute('data-ticker');
          if (this.flowScatterHoverTicker !== ticker) {
            this.flowScatterHoverTicker = ticker;
          }
        } else if (this.flowScatterHoverTicker !== null) {
          this.flowScatterHoverTicker = null;
        }
      },

      get flowScatterSvg() {
        const items = this.flowPrimaryInstruments;
        const width = Math.max(300, this.flowChartWidth || 800);
        const height = width < 500 ? 300 : 360;
        const pad = { left: 52, right: 32, top: 32, bottom: 42 };
        const chartW = width - pad.left - pad.right;
        const chartH = height - pad.top - pad.bottom;

        // Dynamic, un-clamped domain bounds ensuring all 150 points are positioned accurately
        const rets = items.map(it => finiteNumber(it.nav_return_20d_pct) || 0);
        const zs = items.map(it => finiteNumber(it.flow_zscore) || 0);
        const maxAbsRetRaw = rets.length ? Math.max(30, ...rets.map(Math.abs)) : 30;
        const maxAbsZRaw = zs.length ? Math.max(3.2, ...zs.map(Math.abs)) : 3.2;

        const maxRet = Math.min(100, Math.ceil((maxAbsRetRaw * 1.05) / 10) * 10);
        const minRet = -maxRet;
        const maxZ = Math.min(6.0, Math.ceil((maxAbsZRaw * 1.05) * 2) / 2);
        const minZ = -maxZ;

        const xPos = ret => pad.left + ((ret - minRet) / (maxRet - minRet)) * chartW;
        const yPos = z => pad.top + ((maxZ - z) / (maxZ - minZ)) * chartH;
        const xZero = xPos(0);
        const yZero = yPos(0);

        const counts = this.flowScatterCounts;
        const filter = this.flowScatterFilter || 'all';

        // Quadrant Background Fills & Outlines
        const qBgWashout = `<rect x="${pad.left}" y="${yZero.toFixed(1)}" width="${(xZero - pad.left).toFixed(1)}" height="${(pad.top + chartH - yZero).toFixed(1)}" fill="rgba(52,211,153,0.03)" stroke="rgba(52,211,153,0.08)" stroke-width="0.5"/>`;
        const qBgMomentum = `<rect x="${xZero.toFixed(1)}" y="${pad.top}" width="${(pad.left + chartW - xZero).toFixed(1)}" height="${(yZero - pad.top).toFixed(1)}" fill="rgba(34,211,238,0.03)" stroke="rgba(34,211,238,0.08)" stroke-width="0.5"/>`;
        const qBgTrap = `<rect x="${pad.left}" y="${pad.top}" width="${(xZero - pad.left).toFixed(1)}" height="${(yZero - pad.top).toFixed(1)}" fill="rgba(251,146,60,0.025)" stroke="rgba(251,146,60,0.08)" stroke-width="0.5"/>`;
        const qBgSqueeze = `<rect x="${xZero.toFixed(1)}" y="${yZero.toFixed(1)}" width="${(pad.left + chartW - xZero).toFixed(1)}" height="${(pad.top + chartH - yZero).toFixed(1)}" fill="rgba(167,139,250,0.025)" stroke="rgba(167,139,250,0.08)" stroke-width="0.5"/>`;

        // Quadrant Headers & Live Dynamic Counters
        const lblTrap = `
          <g pointer-events="none">
            <rect x="${pad.left + 6}" y="${pad.top + 6}" width="215" height="28" rx="4" fill="rgba(8,12,18,0.85)" stroke="rgba(251,146,60,0.3)" stroke-width="0.8"/>
            <text x="${pad.left + 12}" y="${pad.top + 18}" fill="#fb923c" font-family="ui-monospace, monospace" font-size="9" font-weight="700" letter-spacing="0.04em">Q3 DIP ACCUMULATION (${counts.trap})</text>
            <text x="${pad.left + 12}" y="${pad.top + 28}" fill="#94a3b8" font-family="ui-sans-serif, sans-serif" font-size="8">Inflow at Lows · Buy-the-Dip</text>
          </g>`;
        const lblMomentum = `
          <g pointer-events="none">
            <rect x="${(pad.left + chartW - 225).toFixed(1)}" y="${pad.top + 6}" width="218" height="28" rx="4" fill="rgba(8,12,18,0.85)" stroke="rgba(34,211,238,0.3)" stroke-width="0.8"/>
            <text x="${(pad.left + chartW - 12).toFixed(1)}" y="${pad.top + 18}" text-anchor="end" fill="#22d3ee" font-family="ui-monospace, monospace" font-size="9" font-weight="700" letter-spacing="0.04em">Q2 MOMENTUM CONTINUATION (${counts.momentum})</text>
            <text x="${(pad.left + chartW - 12).toFixed(1)}" y="${pad.top + 28}" text-anchor="end" fill="#94a3b8" font-family="ui-sans-serif, sans-serif" font-size="8">Inflow at Highs · Trend Continuation</text>
          </g>`;
        const lblWashout = `
          <g pointer-events="none">
            <rect x="${pad.left + 6}" y="${(pad.top + chartH - 34).toFixed(1)}" width="210" height="28" rx="4" fill="rgba(8,12,18,0.85)" stroke="rgba(52,211,153,0.3)" stroke-width="0.8"/>
            <text x="${pad.left + 12}" y="${(pad.top + chartH - 22).toFixed(1)}" fill="#34d399" font-family="ui-monospace, monospace" font-size="9" font-weight="700" letter-spacing="0.04em">Q1 WASHOUT REBOUND (${counts.washout})</text>
            <text x="${pad.left + 12}" y="${(pad.top + chartH - 12).toFixed(1)}" fill="#94a3b8" font-family="ui-sans-serif, sans-serif" font-size="8">Outflow at Lows · Washout Rebound</text>
          </g>`;
        const lblSqueeze = `
          <g pointer-events="none">
            <rect x="${(pad.left + chartW - 225).toFixed(1)}" y="${(pad.top + chartH - 34).toFixed(1)}" width="218" height="28" rx="4" fill="rgba(8,12,18,0.85)" stroke="rgba(167,139,250,0.3)" stroke-width="0.8"/>
            <text x="${(pad.left + chartW - 12).toFixed(1)}" y="${(pad.top + chartH - 22).toFixed(1)}" text-anchor="end" fill="#a78bfa" font-family="ui-monospace, monospace" font-size="9" font-weight="700" letter-spacing="0.04em">Q4 WALL OF WORRY SQUEEZE (${counts.squeeze})</text>
            <text x="${(pad.left + chartW - 12).toFixed(1)}" y="${(pad.top + chartH - 12).toFixed(1)}" text-anchor="end" fill="#94a3b8" font-family="ui-sans-serif, sans-serif" font-size="8">Outflow into Rallies · Short Squeeze</text>
          </g>`;

        // Axes and Zero Crosshairs
        const crossX = `<line x1="${xZero.toFixed(1)}" y1="${pad.top}" x2="${xZero.toFixed(1)}" y2="${(pad.top + chartH).toFixed(1)}" stroke="rgba(255,255,255,0.22)" stroke-width="1.2" stroke-dasharray="3 3"/>`;
        const crossY = `<line x1="${pad.left}" y1="${yZero.toFixed(1)}" x2="${(pad.left + chartW).toFixed(1)}" y2="${yZero.toFixed(1)}" stroke="rgba(255,255,255,0.22)" stroke-width="1.2" stroke-dasharray="3 3"/>`;

        // ±1.5σ Reference Lines
        const yPos15 = yPos(1.5);
        const yNeg15 = yPos(-1.5);
        const zRefLines = `<line x1="${pad.left}" y1="${yPos15.toFixed(1)}" x2="${(pad.left + chartW).toFixed(1)}" y2="${yPos15.toFixed(1)}" stroke="rgba(34,211,238,0.2)" stroke-width="1" stroke-dasharray="2 4"/><line x1="${pad.left}" y1="${yNeg15.toFixed(1)}" x2="${(pad.left + chartW).toFixed(1)}" y2="${yNeg15.toFixed(1)}" stroke="rgba(245,158,11,0.2)" stroke-width="1" stroke-dasharray="2 4"/>`;

        // Select top prominent outlier tickers to show sleek labels directly on chart
        const outlierScores = items.map(it => {
          const ret = finiteNumber(it.nav_return_20d_pct) || 0;
          const z = finiteNumber(it.flow_zscore) || 0;
          const aum = finiteNumber(it.aum_m) || 0;
          return { item: it, ret, z, score: Math.abs(z) * 1.5 + Math.abs(ret) / 10 + (aum > 1000 ? 2 : 0) };
        });
        outlierScores.sort((a, b) => b.score - a.score);
        const labeledTickers = new Set(outlierScores.slice(0, 16).map(o => o.item.ticker));
        if (this.flowTicker) labeledTickers.add(this.flowTicker);

        let dots = '';
        let labels = '';
        let activeOverlay = '';

        for (const item of items) {
          const ret = finiteNumber(item.nav_return_20d_pct) || 0;
          const z = finiteNumber(item.flow_zscore) || 0;
          let quad = 'neutral';
          if (ret < 0 && z <= -0.4) quad = 'washout';
          else if (ret >= 0 && z >= 0.4) quad = 'momentum';
          else if (ret < 0 && z >= 0.4) quad = 'trap';
          else if (ret >= 0 && z <= -0.4) quad = 'squeeze';

          const matchesFilter = filter === 'all' || quad === filter;
          const isSelected = this.flowTicker === item.ticker;
          const isHovered = this.flowScatterHoverTicker === item.ticker;

          const cx = xPos(ret);
          const cy = yPos(z);

          let dotColor = '#64748b';
          if (quad === 'washout') dotColor = '#34d399';
          else if (quad === 'momentum') dotColor = '#22d3ee';
          else if (quad === 'trap') dotColor = '#fb923c';
          else if (quad === 'squeeze') dotColor = '#a78bfa';

          const r = isSelected ? 6.5 : (isHovered ? 6.0 : (Math.abs(z) >= 1.5 ? 4.6 : 3.4));
          const opacity = isSelected ? 1.0 : (isHovered ? 1.0 : (matchesFilter ? 0.88 : 0.18));
          const stroke = isSelected ? '#ffffff' : (isHovered ? '#ffffff' : '#090d16');
          const strokeW = isSelected ? 2.2 : (isHovered ? 2.0 : 1.0);

          dots += `<circle cx="${cx.toFixed(1)}" cy="${cy.toFixed(1)}" r="${r}" fill="${dotColor}" stroke="${stroke}" stroke-width="${strokeW}" opacity="${opacity}" style="cursor:pointer;transition:r 0.15s,opacity 0.15s" data-ticker="${item.ticker}" role="button" tabindex="0" aria-label="${item.ticker} ${ret >= 0 ? '+' : ''}${ret.toFixed(1)}% ${z.toFixed(2)} sigma">
            <title>${item.ticker} (${item.underlying || item.fund_name}): 20D Ret ${ret >= 0 ? '+' : ''}${ret.toFixed(1)}%, Flow Z ${z >= 0 ? '+' : ''}${z.toFixed(2)}σ, 1D Flow ${formatMoney(item.latest_flow)} · Click to Open Studio</title>
          </circle>`;

          if (matchesFilter && labeledTickers.has(item.ticker) && !isSelected) {
            let lx = cx > xZero ? cx + 6 : cx - 6;
            let anchor = cx > xZero ? 'start' : 'end';
            let ly = cy + 3.2;
            if (cx > pad.left + chartW - 55) {
              lx = cx - 6;
              anchor = 'end';
            } else if (cx < pad.left + 55) {
              lx = cx + 6;
              anchor = 'start';
            }
            if (cy < pad.top + 38) {
              ly = cy + 12;
            } else if (cy > pad.top + chartH - 38) {
              ly = cy - 6;
            }
            labels += `<text x="${lx.toFixed(1)}" y="${ly.toFixed(1)}" text-anchor="${anchor}" fill="${dotColor}" font-family="ui-monospace, monospace" font-size="9" font-weight="700" pointer-events="none" opacity="0.9">${item.ticker}</text>`;
          }

          if (isSelected) {
            const anchor = (cx > pad.left + chartW - 90) ? 'end' : (cx > xZero ? 'start' : 'end');
            const lx = anchor === 'start' ? cx + 11 : cx - 11;
            activeOverlay = `<g pointer-events="none">
              <circle cx="${cx.toFixed(1)}" cy="${cy.toFixed(1)}" r="10" fill="none" stroke="#22d3ee" stroke-width="2.2" stroke-dasharray="3 2" opacity="0.95"/>
              <circle cx="${cx.toFixed(1)}" cy="${cy.toFixed(1)}" r="4.2" fill="#ffffff" stroke="#090d16" stroke-width="1.2"/>
              <rect x="${(anchor === 'start' ? lx - 2 : lx - item.ticker.length * 6.5 - 54).toFixed(1)}" y="${(cy - 10).toFixed(1)}" width="${item.ticker.length * 6.5 + 56}" height="18" rx="4" fill="#090d16" stroke="#22d3ee" stroke-width="1.2" opacity="0.95"/>
              <text x="${(anchor === 'start' ? lx + 3 : lx - 3).toFixed(1)}" y="${(cy + 2.5).toFixed(1)}" text-anchor="${anchor}" fill="#22d3ee" font-family="ui-monospace, monospace" font-size="9.5" font-weight="700">${item.ticker} · ACTIVE</text>
            </g>`;
          }
        }

        // Dynamic X-Ticks
        const xStep = maxRet <= 30 ? 10 : (maxRet <= 60 ? 15 : 20);
        const xTickVals = [];
        for (let v = -maxRet; v <= maxRet; v += xStep) {
          xTickVals.push(v);
        }
        const xTicks = xTickVals.map(val => {
          const x = xPos(val);
          return `<text x="${x.toFixed(1)}" y="${height - 12}" text-anchor="middle" fill="${COLORS.subtle}" font-family="ui-monospace, monospace" font-size="10">${val >= 0 ? '+' : ''}${val}%</text>`;
        }).join('');

        // Dynamic Y-Ticks
        const yTickVals = [maxZ, maxZ / 2, 0, -maxZ / 2, -maxZ];
        const yTicks = yTickVals.map(val => {
          const y = yPos(val);
          return `<text x="${pad.left - 8}" y="${(y + 3.5).toFixed(1)}" text-anchor="end" fill="${COLORS.subtle}" font-family="ui-monospace, monospace" font-size="10">${val >= 0 ? '+' : ''}${val.toFixed(1)}σ</text>`;
        }).join('');

        const axisLabels = `<text x="${(pad.left + chartW / 2).toFixed(1)}" y="${height - 0}" text-anchor="middle" fill="${COLORS.muted}" font-family="ui-sans-serif, sans-serif" font-size="9.5" font-weight="600" letter-spacing="0.04em">20-SESSION NAV PRICE RETURN (%)</text><text transform="rotate(-90)" x="${-(pad.top + chartH / 2).toFixed(1)}" y="${pad.left - 38}" text-anchor="middle" fill="${COLORS.muted}" font-family="ui-sans-serif, sans-serif" font-size="9.5" font-weight="600" letter-spacing="0.04em">DAILY FLOW Z-SCORE (σ)</text>`;

        return `<svg viewBox="0 0 ${width} ${height}" role="img" aria-label="Price return versus Flow Z-score distribution map" style="width:100%;height:auto;display:block">
          <title>Price Return versus Flow Z-Score Map</title>
          ${qBgWashout}${qBgMomentum}${qBgTrap}${qBgSqueeze}
          ${lblWashout}${lblMomentum}${lblTrap}${lblSqueeze}
          ${zRefLines}${crossX}${crossY}
          ${xTicks}${yTicks}${axisLabels}
          ${dots}
          ${labels}
          ${activeOverlay}
        </svg>`;
      },

      get flowEmpiricalEdgeLedger() {
        const item = this.flowSelectedInstrument;
        const rows = this.flowData?.records || [];
        if (!item || rows.length < 5) return null;

        // --- 1. 20D Price Range & Flow Alignment ---
        const recent20 = rows.slice(-20);
        const prices20 = recent20.map(r => finiteNumber(r.nav)).filter(v => v !== null);
        let rangePct = 50;
        let minP = 0, maxP = 0, curP = 0;
        let rangeState = 'BALANCED';
        let rangeBadge = 'RANGE BALANCED';
        let rangeDesc = 'Trading midway through 20-session high/low envelope.';

        if (prices20.length >= 2) {
          minP = Math.min(...prices20);
          maxP = Math.max(...prices20);
          curP = prices20[prices20.length - 1];
          if (maxP > minP) {
            rangePct = Math.round(Math.max(0, Math.min(100, (curP - minP) / (maxP - minP) * 100)));
          }
          const f20 = finiteNumber(item.flow_20d) || 0;
          if (rangePct >= 80 && f20 < 0) {
            rangeState = 'DISTRIBUTION';
            rangeBadge = 'DIVERGENCE ACTIVE';
            rangeDesc = `Trading at ${rangePct}% of 20D range ($${minP.toFixed(2)} → $${maxP.toFixed(2)}) while 20D net flow is negative (${formatMoney(f20)}).`;
          } else if (rangePct <= 20 && f20 > 0) {
            rangeState = 'ACCUMULATION';
            rangeBadge = 'ACCUMULATION AT LOWS';
            rangeDesc = `Near 20D trough (${rangePct}%) with positive capital absorption (${formatMoney(f20)}).`;
          } else if (rangePct >= 80 && f20 > 0) {
            rangeState = 'ACCUMULATION';
            rangeBadge = 'MOMENTUM CONFIRMED';
            rangeDesc = `Trading near 20D high (${rangePct}%) backed by concurrent net inflows (${formatMoney(f20)}).`;
          } else if (rangePct <= 20 && f20 < 0) {
            rangeState = 'DISTRIBUTION';
            rangeBadge = 'PRESSURE AT LOWS';
            rangeDesc = `Trading near 20D low (${rangePct}%) alongside persistent capital outflows (${formatMoney(f20)}).`;
          } else {
            rangeState = 'BALANCED';
            rangeBadge = 'RANGE BALANCED';
            rangeDesc = `Trading at ${rangePct}% of 20D range ($${minP.toFixed(2)} → $${maxP.toFixed(2)}). 20D net flow: ${formatMoney(f20)}.`;
          }
        }

        // --- 2. Empirical Extreme Flow Shocks (|Z| >= 2.0σ) ---
        const shockIndices = [];
        for (let i = 0; i < rows.length; i++) {
          const z = finiteNumber(rows[i].priorOnlyZScore);
          if (z !== null && Math.abs(z) >= 2.0) {
            shockIndices.push(i);
          }
        }
        const totalShocks = shockIndices.length;
        const latestZ = finiteNumber(rows[rows.length - 1]?.priorOnlyZScore) || 0;
        const isShockActiveToday = Math.abs(latestZ) >= 2.0;
        const shockBadge = isShockActiveToday ? 'SHOCK ACTIVE TODAY' : 'INACTIVE TODAY';
        const shockBadgeState = isShockActiveToday ? (latestZ >= 2.0 ? 'ACCUMULATION' : 'DISTRIBUTION') : 'BALANCED';

        let shockStatLine = '';
        let shockSubtext = '';
        if (totalShocks < 5) {
          shockStatLine = `Sample size too small (n = ${totalShocks} in ${rows.length} sessions)`;
          shockSubtext = `Insufficient historical shocks (|Z| ≥ 2.0σ) for statistical edge calculation. Current: ${latestZ >= 0 ? '+' : ''}${latestZ.toFixed(2)}σ.`;
        } else {
          let fwdGains = 0;
          let evaluated = 0;
          let sumRet = 0;
          shockIndices.forEach(idx => {
            if (idx + 5 < rows.length && rows[idx].nav && rows[idx + 5].nav) {
              const r = (rows[idx + 5].nav - rows[idx].nav) / rows[idx].nav * 100;
              sumRet += r;
              evaluated += 1;
              if (r > 0) fwdGains += 1;
            }
          });
          const winRate = evaluated > 0 ? (fwdGains / evaluated * 100).toFixed(1) : '—';
          const avgRet = evaluated > 0 ? (sumRet / evaluated).toFixed(2) : '—';
          shockStatLine = `Historical Win Rate: ${winRate}% (n = ${evaluated} shocks)`;
          shockSubtext = `Empirical 5-day forward return following extreme shocks averaged ${avgRet > 0 ? '+' : ''}${avgRet}%. Current: ${latestZ >= 0 ? '+' : ''}${latestZ.toFixed(2)}σ.`;
        }

        // --- 3. 5D Velocity & Thrust ---
        let ret5d = null;
        if (rows.length >= 6) {
          const pNow = finiteNumber(rows[rows.length - 1].nav);
          const p5Ago = finiteNumber(rows[rows.length - 6].nav);
          if (pNow !== null && p5Ago !== null && p5Ago > 0) {
            ret5d = (pNow - p5Ago) / p5Ago * 100;
          }
        }
        const flow5d = finiteNumber(item.flow_5d) || 0;
        const isThrustActive = ret5d !== null && Math.abs(ret5d) >= 10.0;
        const thrustBadge = isThrustActive ? (ret5d > 0 ? 'ACCELERATION ACTIVE' : 'BREAKDOWN ACTIVE') : 'NORMAL VELOCITY';
        const thrustBadgeState = isThrustActive ? (ret5d > 0 ? 'ACCUMULATION' : 'DISTRIBUTION') : 'BALANCED';
        const thrustStatLine = ret5d !== null ? `5D Move: ${ret5d >= 0 ? '+' : ''}${ret5d.toFixed(2)}% · 5D Flow: ${formatMoney(flow5d)}` : 'Insufficient 5D history';
        const thrustSubtext = isThrustActive
          ? `Extreme short-term velocity (|5D Move| ≥ 10%) with concurrent flow direction.`
          : `Measures 5-session directional price velocity against directional net flow backing.`;

        return {
          rangePct,
          rangeBadge,
          rangeState,
          rangeDesc,
          shockBadge,
          shockBadgeState,
          shockStatLine,
          shockSubtext,
          thrustBadge,
          thrustBadgeState,
          thrustStatLine,
          thrustSubtext,
          isShockActiveToday,
          isThrustActive
        };
      },

      get flowCategoryHeatmapRows() {
        const rows = this.flowCategoryMatrixRows;
        return rows.map(r => {
          const heatStyle = val => {
            const v = finiteNumber(val) || 0;
            if (v >= 100e6) return 'background: rgba(52, 211, 153, 0.22); color: #34d399; font-weight: 700;';
            if (v >= 10e6) return 'background: rgba(52, 211, 153, 0.10); color: #34d399;';
            if (v <= -100e6) return 'background: rgba(251, 113, 133, 0.22); color: #fb7185; font-weight: 700;';
            if (v <= -10e6) return 'background: rgba(251, 113, 133, 0.10); color: #fb7185;';
            return 'background: rgba(255, 255, 255, 0.02); color: var(--flow-muted);';
          };
          return {
            ...r,
            style1d: heatStyle(r.flow_1d),
            style5d: heatStyle(r.flow_5d),
            style20d: heatStyle(r.flow_20d),
            style60d: heatStyle(r.flow_60d),
            styleYtd: heatStyle(r.flow_ytd)
          };
        });
      },

      get flowTradeBlotterRows() {
        const signals = this.flowAlphaFilteredSignals;
        return signals.map(s => {
          const sig = String(s.live_signal || '');
          let setup = 'S1: Washout Slingshot';
          let action = 'BUY';
          let actionClass = 'flow-action-buy';
          let horizon = '5D–20D';
          let winRate = '58.2%';
          let expRet = '+5.66%';
          let stopLoss = '-7.0%';
          let target = '+16.0%';
          let catalyst = 'Outflow Washout Shock';

          if (s.ticker === 'EDC') {
            setup = 'Breakout (International)';
            action = 'CAUTION';
            actionClass = 'flow-action-fade';
            horizon = '10D Fade';
            winRate = '38.0%';
            expRet = '−2.82%';
            stopLoss = '+4.0%';
            target = '−8.0%';
            catalyst = 'International Breakout Fade (t = −2.04)';
          } else if (sig.includes('IGNITION') || sig.includes('MOMENTUM')) {
            setup = 'S2: Informed Ignition';
            action = 'BUY';
            actionClass = 'flow-action-buy';
            horizon = '10D–20D';
            winRate = '54.0%';
            expRet = '+12.64%';
            stopLoss = '-9.0%';
            target = '+28.0%';
            catalyst = 'Breakout Inflow Surge';
          } else if (sig.includes('TRAP') || sig.includes('DEAD_CAT')) {
            setup = 'S3: Dead-Cat Fade';
            action = 'SHORT';
            actionClass = 'flow-action-short';
            horizon = '3D–5D';
            winRate = '60.9%';
            expRet = '+3.50%';
            stopLoss = '+6.0%';
            target = '-10.0%';
            catalyst = 'Public Chasing in Downtrend';
          } else if (sig.includes('SQUEEZE') || sig.includes('WALL_OF_WORRY')) {
            setup = 'S1: Disbelief Squeeze';
            action = 'BUY';
            actionClass = 'flow-action-buy';
            horizon = '3D–10D';
            winRate = '57.3%';
            expRet = '+3.09%';
            stopLoss = '-5.5%';
            target = '+14.0%';
            catalyst = 'Rising on Persistent Outflows';
          }

          return {
            ...s,
            setup,
            action,
            actionClass,
            horizon,
            winRate,
            expRet,
            stopLoss,
            target,
            catalyst
          };
        });
      },

      get flowTwinLeadLagPairs() {
        return [
          {
            name: 'NASDAQ-100 Twin Barometer',
            leader: 'QLD (+2x ProShares)',
            follower: '+3x Peer Benchmark',
            spreadZ: '+1.42σ (QLD Leading)',
            edge: '+1.90% 5D Return',
            winRate: '62.8%',
            tstat: '+2.26',
            note: 'When +2x core trend twin leads while +3x public twin lags, 5D follow-through edge is historically positive.'
          },
          {
            name: 'NVIDIA Single-Stock Twin',
            leader: 'NVDL (GraniteShares)',
            follower: 'NVDX (Tuttle/Defiance)',
            spreadZ: '+0.85σ (NVDL Leading)',
            edge: '+8.29% 10D Return',
            winRate: '63.0%',
            tstat: '+3.03',
            note: 'GraniteShares NVDL leads twin (r = +0.176); independent NVDL flow surges predict strong multi-week continuation.'
          },
          {
            name: 'Bitcoin Crypto Twin',
            leader: 'BITU (ProShares +2x)',
            follower: 'BITX (Volatility Shares +2x)',
            spreadZ: '+0.60σ (BITU Leading)',
            edge: '+7.40% 20D Return',
            winRate: '59.1%',
            tstat: '+2.15',
            note: 'ProShares BITU exhibits cleaner core accumulation persistence over 10D–20D horizons.'
          }
        ];
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
          const aumVal = finiteNumber(item.aum_m) || 0;
          const flowPctAum = (aumVal > 0 && f1 !== 0) ? ((f1 / (aumVal * 1e6)) * 100) : null;
          return {
            ticker: item.ticker,
            fund_name: item.fund_name,
            underlying: item.underlying || item.fund_name,
            leverage: item.leverage || '+2x',
            issuer: item.issuer || '',
            flow1d: f1,
            flow20d: f20,
            flowPctAum,
            zscore: z,
            regime: item.regime || 'BALANCED',
            aum_m: aumVal,
            sparklineSvg: miniFlowSparklineSvg(item.sparkline_20d || [], 86, 20)
          };
        });
      },

      get flowPeerTitle() {
        const current = this.flowSelectedInstrument;
        if (!current) return 'Peer Flow Comparison';
        const cleanU = String(current.underlying || '').replace(/\s*\([^)]*\)\s*$/, '').trim() || current.ticker;
        return `${cleanU} & Category Peers`;
      },

      get flowPeerSubtitle() {
        const current = this.flowSelectedInstrument;
        if (!current) return 'Click any peer instrument to switch the studio to its historical series.';
        const cleanU = String(current.underlying || '').replace(/\s*\([^)]*\)\s*$/, '').trim() || current.ticker;
        return `Direct ${cleanU} pairs and top tactical category instruments ranked by flow momentum.`;
      },

      get flowPeerComparisonRows() {
        const current = this.flowSelectedInstrument;
        if (!current) return [];
        const items = this.flowPrimaryInstruments;
        const normU = raw => String(raw || '').trim().replace(/\s*\([^)]*\)\s*$/, '').trim().toUpperCase();
        const targetUnderlying = normU(current.underlying);

        // 1. Direct peers sharing the underlying target (e.g. MSTR, TSLA, Semis, etc.)
        let directPeers = items.filter(item => normU(item.underlying) === targetUnderlying);
        directPeers.sort((a, b) => {
          if (a.ticker === current.ticker) return -1;
          if (b.ticker === current.ticker) return 1;
          return (finiteNumber(b.aum_m) || 0) - (finiteNumber(a.aum_m) || 0);
        });

        // 2. Complement with category peers if fewer than 8
        let categoryPeers = [];
        if (directPeers.length < 8) {
          const directTickers = new Set(directPeers.map(p => p.ticker));
          categoryPeers = items
            .filter(item => item.category === current.category && !directTickers.has(item.ticker) && ((item.flow_20d !== 0 && item.flow_20d !== null) || (item.latest_flow !== 0 && item.latest_flow !== null) || (item.aum_m > 0)))
            .sort((a, b) => Math.abs(finiteNumber(b.flow_zscore) || 0) - Math.abs(finiteNumber(a.flow_zscore) || 0))
            .slice(0, 8 - directPeers.length);
        }

        const combined = [...directPeers, ...categoryPeers];
        return combined.slice(0, 8).map(item => {
          const isDirect = normU(item.underlying) === targetUnderlying;
          const cleanUnderlying = String(item.underlying || '').replace(/\s*\([^)]*\)\s*$/, '').trim() || item.ticker;
          return {
            ...item,
            isDirect,
            cleanUnderlying,
            sparklineSvg: miniFlowSparklineSvg(item.sparkline_20d || [], 88, 22)
          };
        });
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
          const aumVal = finiteNumber(item.aum_m) || 0;
          const f1Val = finiteNumber(item.latest_flow);
          const flow_pct_aum = (aumVal > 0 && f1Val !== null) ? ((f1Val / (aumVal * 1e6)) * 100) : null;
          let setup_icon = '';
          let setup_name = '';
          const sig = String(item.live_signal || item.live_signal_label || '');
          if (sig.includes('SLINGSHOT') || sig.includes('WASHOUT')) {
            setup_icon = '⚡';
            setup_name = 'Washout Rebound Setup';
          } else if (sig.includes('IGNITION') || sig.includes('BREAKOUT') || sig.includes('MOMENTUM')) {
            setup_icon = '↗';
            setup_name = 'Momentum Ignition Setup';
          } else if (sig.includes('SQUEEZE') || sig.includes('WALL')) {
            setup_icon = '◐';
            setup_name = 'Wall of Worry Squeeze';
          } else if (sig.includes('TRAP') || sig.includes('DEAD_CAT') || sig.includes('AVOID')) {
            setup_icon = '⛔';
            setup_name = 'Dip Inflow Avoid Setup';
          }
          rows.push({
            ...item,
            flow_pct_aum,
            setup_icon,
            setup_name,
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
          } else if (key === 'flow_pct_aum') {
            va = finiteNumber(a.flow_pct_aum) || 0;
            vb = finiteNumber(b.flow_pct_aum) || 0;
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
        const ticker = this.flowTicker;
        const volMap = this.flowVolumeCache?.series?.[ticker] || null;
        const key = `${this.flowData?.revision || 'none'}:${this.flowStartIndex}:${this.flowEndIndex}:${Boolean(volMap)}`;
        if (this._flowMetricCacheKey === key && this._flowMetricCache) return this._flowMetricCache;
        const rows = cachedFlowMetricRows(this.flowData?.revision || 'none', records, volMap);
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
        if (this.flowHoverIndex !== null && this.flowHoverChart === 'main' && this.flowHoverIndex >= 0 && this.flowHoverIndex < rows.length) {
          const hRow = rows[this.flowHoverIndex];
          const hFlow = finiteNumber(hRow.flow);
          const hY = hFlow !== null ? yScale(hFlow) : zeroY;
          const items = [];
          if (hFlow !== null) {
            items.push({
              color: hFlow >= 0 ? COLORS.positive : COLORS.negative,
              label: 'Daily Flow',
              value: formatMoney(hFlow)
            });
          }
          const hMean = finiteNumber(hRow.rollingMean20);
          if (hMean !== null) {
            items.push({
              color: COLORS.cyan,
              label: '20D Mean',
              value: formatMoney(hMean)
            });
          }
          const hCum = finiteNumber(hRow.selectedCumulative);
          if (hCum !== null) {
            items.push({
              color: (hCum || 0) >= 0 ? '#38bdf8' : '#fb7185',
              label: 'Cumulative',
              value: formatMoney(hCum)
            });
          }
          const hZ = finiteNumber(hRow.priorOnlyZScore);
          if (hZ !== null) {
            items.push({
              color: hZ >= 1.5 ? COLORS.cyan : hZ <= -1.5 ? COLORS.warning : '#94a3b8',
              label: 'Z-Score',
              value: `${hZ >= 0 ? '+' : ''}${hZ.toFixed(2)}σ`
            });
          }
          const hNav = finiteNumber(hRow.nav);
          if (this.flowPriceEnabled && hNav !== null) {
            items.push({
              color: '#a78bfa',
              label: 'NAV / Price',
              value: formatPrice(hNav)
            });
          }
          overlay = chartCrosshairOverlay(frame, rows, this.flowHoverIndex, hY, null, items);
        }
        return `<svg viewBox="0 0 ${width} ${height}" data-pad-left="${frame.padding.left}" data-pad-right="${frame.padding.right}" data-chart-width="${width}" role="img" aria-labelledby="flow-daily-title flow-daily-desc"><title id="flow-daily-title">Daily ETF estimated net flow</title><desc id="flow-daily-desc">${escapeHtml(description)}</desc>${grid}${bars}<path d="${linePath(meanPoints)}" fill="none" stroke="${COLORS.cyan}" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>${overlay}${frame.dateTicks}</svg>`;
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
        if (this.flowHoverIndex !== null && this.flowHoverChart === 'main' && this.flowHoverIndex >= 0 && this.flowHoverIndex < rows.length) {
          const hRow = rows[this.flowHoverIndex];
          const hVal = finiteNumber(hRow.selectedCumulative);
          const hY = hVal !== null ? yScale(hVal) : zeroY;
          const items = [];
          if (hVal !== null) {
            items.push({
              color: hVal >= 0 ? COLORS.cyan : COLORS.negative,
              label: 'Cumulative',
              value: formatMoney(hVal)
            });
          }
          const hFlow = finiteNumber(hRow.flow);
          if (hFlow !== null) {
            items.push({
              color: hFlow >= 0 ? COLORS.positive : COLORS.negative,
              label: 'Daily Flow',
              value: formatMoney(hFlow)
            });
          }
          const hMean = finiteNumber(hRow.rollingMean20);
          if (hMean !== null) {
            items.push({
              color: '#38bdf8',
              label: '20D Mean',
              value: formatMoney(hMean)
            });
          }
          const hNav = finiteNumber(hRow.nav);
          if (this.flowPriceEnabled && hNav !== null) {
            items.push({
              color: '#a78bfa',
              label: 'NAV / Price',
              value: formatPrice(hNav)
            });
          }
          overlay = chartCrosshairOverlay(frame, rows, this.flowHoverIndex, hY, null, items);
        }
        return `<svg viewBox="0 0 ${width} ${height}" data-pad-left="${frame.padding.left}" data-pad-right="${frame.padding.right}" data-chart-width="${width}" role="img" aria-labelledby="flow-cumulative-title flow-cumulative-desc"><title id="flow-cumulative-title">Selected-window cumulative source-reported aggregate net flow</title><desc id="flow-cumulative-desc">${escapeHtml(description)}</desc><defs><linearGradient id="flow-cum-grad" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="${COLORS.cyan}" stop-opacity="0.22"/><stop offset="100%" stop-color="${COLORS.cyan}" stop-opacity="0.0"/></linearGradient></defs>${grid}${area ? `<path d="${area}" fill="url(#flow-cum-grad)"/>` : ''}<path d="${linePath(linePoints)}" fill="none" stroke="${COLORS.cyan}" stroke-width="2.2" stroke-linejoin="round" stroke-linecap="round"/>${lastPoint}${overlay}${frame.dateTicks}</svg>`;
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
        if (this.flowHoverIndex !== null && this.flowHoverChart === 'main' && this.flowHoverIndex >= 0 && this.flowHoverIndex < rows.length) {
          const hRow = rows[this.flowHoverIndex];
          const hPct = finiteNumber(hRow.percentile10);
          const hY = hPct !== null ? yPercentile(hPct) : null;
          const items = [];
          if (hPct !== null) {
            items.push({
              color: COLORS.cyan,
              label: '10D Percentile',
              value: `${hPct.toFixed(0)}%`
            });
          }
          const hSum10 = finiteNumber(hRow.rollingSum10);
          if (hSum10 !== null) {
            items.push({
              color: (hSum10 || 0) >= 0 ? COLORS.positive : COLORS.negative,
              label: '10D Flow Sum',
              value: formatMoney(hSum10)
            });
          }
          const hFlow = finiteNumber(hRow.flow);
          if (hFlow !== null) {
            items.push({
              color: hFlow >= 0 ? COLORS.positive : COLORS.negative,
              label: 'Daily Flow',
              value: formatMoney(hFlow)
            });
          }
          overlay = chartCrosshairOverlay(frame, rows, this.flowHoverIndex, hY, null, items);
        }
        return `<svg viewBox="0 0 ${width} ${height}" data-pad-left="${frame.padding.left}" data-pad-right="${frame.padding.right}" data-chart-width="${width}" role="img" aria-labelledby="flow-percentile-title flow-percentile-desc"><title id="flow-percentile-title">Ten-observation flow percentile</title><desc id="flow-percentile-desc">${escapeHtml(description)}</desc>${leftAxis}${rightAxis}${bars}<path d="${linePath(percentilePoints)}" fill="none" stroke="${COLORS.cyan}" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>${overlay}${frame.dateTicks}</svg>`;
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
        if (this.flowHoverIndex !== null && this.flowHoverChart === 'main' && this.flowHoverIndex >= 0 && this.flowHoverIndex < rows.length) {
          const hRow = rows[this.flowHoverIndex];
          const hZ = finiteNumber(hRow.priorOnlyZScore);
          const hY = hZ !== null ? yScale(hZ) : zeroY;
          const items = [];
          if (hZ !== null) {
            items.push({
              color: hZ >= 1.5 ? COLORS.cyan : hZ <= -1.5 ? COLORS.warning : '#94a3b8',
              label: 'Flow Z-Score',
              value: `${hZ >= 0 ? '+' : ''}${hZ.toFixed(2)}σ`
            });
          }
          const hFlow = finiteNumber(hRow.flow);
          if (hFlow !== null) {
            items.push({
              color: hFlow >= 0 ? COLORS.positive : COLORS.negative,
              label: 'Daily Flow',
              value: formatMoney(hFlow)
            });
          }
          const hMean = finiteNumber(hRow.rollingMean20);
          if (hMean !== null) {
            items.push({
              color: '#38bdf8',
              label: '20D Mean',
              value: formatMoney(hMean)
            });
          }
          overlay = chartCrosshairOverlay(frame, rows, this.flowHoverIndex, hY, null, items);
        }
        return `<svg viewBox="0 0 ${width} ${height}" data-pad-left="${frame.padding.left}" data-pad-right="${frame.padding.right}" data-chart-width="${width}" role="img" aria-labelledby="flow-intensity-title flow-intensity-desc"><title id="flow-intensity-title">Prior-only daily flow z-score</title><desc id="flow-intensity-desc">${escapeHtml(description)}</desc>${grid}${bars}${overlay}${frame.dateTicks}</svg>`;
      },

      get flowPriceChartSvg() {
        const rows = this.flowSelectedRows;
        const width = this.flowChartWidth;
        const height = width < 500 ? 370 : 430;
        const prices = rows.map(row => finiteNumber(row.nav)).filter(value => value !== null);
        const flows = rows.map(row => finiteNumber(row.flow)).filter(value => value !== null);
        if (!this.flowPriceEnabled) return '';
        if (prices.length < 2 || !flows.length) return emptyChart(width, height, 'Price and flow are unavailable for this window.');

        const padLeft = width < 500 ? 54 : 68;
        const padRight = width < 500 ? 54 : 68;
        const padTop = 22;
        const padBottom = 34;
        const totalChartH = height - padTop - padBottom;
        const upperHeight = Math.round(totalChartH * 0.64);
        const lowerHeight = totalChartH - upperHeight - 24;
        const dividerY = padTop + upperHeight + 12;
        const lowerTop = padTop + upperHeight + 24;

        const frame = chartFrame({ records: rows, width, height, padding: { left: padLeft, right: padRight, top: padTop, bottom: padBottom } });

        const minimumPrice = Math.min(...prices);
        const maximumPrice = Math.max(...prices);
        const pricePadding = Math.max((maximumPrice - minimumPrice) * 0.08, maximumPrice * 0.005);
        const lowPrice = Math.max(0, minimumPrice - pricePadding);
        const highPrice = maximumPrice + pricePadding;

        const maximumFlow = Math.max(1, ...flows.map(value => Math.abs(value))) * 1.08;

        const yPrice = value => padTop + (highPrice - value) / (highPrice - lowPrice || 1) * upperHeight;
        const zeroFlowY = lowerTop + lowerHeight / 2;
        const yFlow = value => zeroFlowY - (value / maximumFlow) * (lowerHeight / 2);
        const yZ = z => zeroFlowY - (Math.max(-3, Math.min(3, z)) / 3.0) * (lowerHeight / 2);

        // Upper Pane: Regime tint bands in background
        let regimeBands = '';
        const slot = frame.chartWidth / Math.max(1, rows.length);
        rows.forEach((row, idx) => {
          const p = finiteNumber(row.nav);
          const x = frame.xScale(idx);
          if (idx >= 5 && row.nav !== null && rows[idx - 5]?.nav !== null) {
            const pDiff = row.nav - rows[idx - 5].nav;
            const flowSum = finiteNumber(row.rollingSum5) || 0;
            if (pDiff > 0 && flowSum < 0) {
              regimeBands += `<rect x="${(x - slot / 2).toFixed(1)}" y="${padTop}" width="${slot.toFixed(1)}" height="${upperHeight}" fill="rgba(192,132,252,0.06)"/>`;
            } else if (flowSum > 0 && pDiff > 0) {
              regimeBands += `<rect x="${(x - slot / 2).toFixed(1)}" y="${padTop}" width="${slot.toFixed(1)}" height="${upperHeight}" fill="rgba(52,211,153,0.05)"/>`;
            } else if (flowSum < 0 && pDiff < 0) {
              regimeBands += `<rect x="${(x - slot / 2).toFixed(1)}" y="${padTop}" width="${slot.toFixed(1)}" height="${upperHeight}" fill="rgba(251,113,133,0.05)"/>`;
            }
          }
        });

        // Price path
        const pricePoints = rows.map((row, index) => ({ x: frame.xScale(index), y: finiteScale(yPrice, row.nav) }));

        // Structural Turning-Point Climax Markers on Price Path
        let climaxMarkers = '';
        rows.forEach((row, idx) => {
          if (row.nav === null) return;
          const x = frame.xScale(idx);
          const y = yPrice(row.nav);
          const z = finiteNumber(row.priorOnlyZScore);

          let isWashout = false;
          let isEuphoria = false;
          let isBottomClimax = false;
          let isTopClimax = false;

          if (idx >= 5 && rows[idx - 5]?.nav !== null) {
            const ret5 = (row.nav - rows[idx - 5].nav) / rows[idx - 5].nav;
            if (ret5 <= -0.05 && z !== null && z <= -1.8) isWashout = true;
            if (ret5 >= 0.06 && z !== null && z >= 1.8) isEuphoria = true;
          }

          const wStart = Math.max(0, idx - 10);
          const wEnd = Math.min(rows.length - 1, idx + 10);
          let isMin = true;
          let isMax = true;
          for (let k = wStart; k <= wEnd; k++) {
            if (k === idx) continue;
            const kn = finiteNumber(rows[k].nav);
            if (kn !== null && kn < row.nav) isMin = false;
            if (kn !== null && kn > row.nav) isMax = false;
          }
          if (isMin && idx >= 10 && idx <= rows.length - 5 && (z || 0) >= 0.2) isBottomClimax = true;
          if (isMax && idx >= 10 && idx <= rows.length - 5 && (z || 0) <= -0.2) isTopClimax = true;

          if (isBottomClimax) {
            climaxMarkers += `<path d="M ${x.toFixed(1)} ${(y + 16).toFixed(1)} L ${(x - 5).toFixed(1)} ${(y + 24).toFixed(1)} L ${(x + 5).toFixed(1)} ${(y + 24).toFixed(1)} Z" fill="#10b981" stroke="#05070a" stroke-width="1.2">
              <title>${row.date}: Bottom Climax (Positive flow surge at trough)</title>
            </path>`;
          } else if (isTopClimax) {
            climaxMarkers += `<path d="M ${x.toFixed(1)} ${(y - 16).toFixed(1)} L ${(x - 5).toFixed(1)} ${(y - 24).toFixed(1)} L ${(x + 5).toFixed(1)} ${(y - 24).toFixed(1)} Z" fill="#ef4444" stroke="#05070a" stroke-width="1.2">
              <title>${row.date}: Top Climax (Redemption surge at peak)</title>
            </path>`;
          } else if (isWashout) {
            climaxMarkers += `<polygon points="${x.toFixed(1)},${(y - 7).toFixed(1)} ${(x + 6).toFixed(1)},${y.toFixed(1)} ${x.toFixed(1)},${(y + 7).toFixed(1)} ${(x - 6).toFixed(1)},${y.toFixed(1)}" fill="#22d3ee" stroke="#05070a" stroke-width="1.2">
              <title>${row.date}: Washout Day (Z=${z?.toFixed(2)}σ into 5D drop)</title>
            </polygon>`;
          } else if (isEuphoria) {
            climaxMarkers += `<circle cx="${x.toFixed(1)}" cy="${(y - 12).toFixed(1)}" r="4.5" fill="#f59e0b" stroke="#05070a" stroke-width="1.2">
              <title>${row.date}: Euphoria Peak (Z=${z?.toFixed(2)}σ into 5D surge)</title>
            </circle>`;
          }
        });

        // Price Axis Ticks (left)
        const leftTicks = [highPrice, (highPrice + lowPrice) / 2, lowPrice].map(value => {
          const y = yPrice(value);
          return `<text x="${padLeft - 7}" y="${(y + 4).toFixed(1)}" text-anchor="end" fill="${COLORS.cyan}" font-family="ui-monospace, SFMono-Regular, monospace" font-size="11">${escapeHtml(formatPrice(value))}</text>`;
        }).join('');

        // Divider
        const dividerLine = `<line x1="${padLeft}" y1="${dividerY}" x2="${width - padRight}" y2="${dividerY}" stroke="rgba(255,255,255,0.12)" stroke-width="1" stroke-dasharray="2 2"/>
        <text x="${padLeft}" y="${dividerY - 3}" fill="${COLORS.cyan}" font-family="ui-monospace, monospace" font-size="9" letter-spacing="0.05em">SPLIT-ADJUSTED PRICE NAV</text>
        <text x="${width - padRight}" y="${dividerY - 3}" text-anchor="end" fill="${COLORS.subtle}" font-family="ui-monospace, monospace" font-size="9" letter-spacing="0.05em">DAILY FLOW ($M) &amp; 5D ROLLING Z-SCORE</text>`;

        // Lower Pane: Daily flow bars
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
          bars += `<rect x="${(center - barWidth / 2).toFixed(1)}" y="${top.toFixed(1)}" width="${barWidth.toFixed(1)}" height="${barHeight.toFixed(1)}" fill="${fill}" opacity="0.75"/>`;
        });

        // Lower Pane: Z-Score Waveform Line
        const zPoints = rows.map((row, index) => {
          const z = finiteNumber(row.priorOnlyZScore);
          return { x: frame.xScale(index), y: z !== null ? yZ(z) : null };
        });
        const zLine = `<path d="${linePath(zPoints)}" fill="none" stroke="#38bdf8" stroke-width="1.5" stroke-dasharray="3 1" opacity="0.85"/>`;

        // Z-score threshold dotted lines
        const zUpperY = yZ(1.8);
        const zLowerY = yZ(-1.8);
        const zThresholds = `
          <line x1="${padLeft}" y1="${zUpperY.toFixed(1)}" x2="${width - padRight}" y2="${zUpperY.toFixed(1)}" stroke="#fbbf24" stroke-width="0.8" stroke-dasharray="2 3" opacity="0.6"/>
          <text x="${width - padRight - 4}" y="${(zUpperY - 3).toFixed(1)}" text-anchor="end" fill="#fbbf24" font-family="ui-monospace, monospace" font-size="8.5" opacity="0.8">+1.8σ EUPHORIA</text>
          <line x1="${padLeft}" y1="${zLowerY.toFixed(1)}" x2="${width - padRight}" y2="${zLowerY.toFixed(1)}" stroke="#22d3ee" stroke-width="0.8" stroke-dasharray="2 3" opacity="0.6"/>
          <text x="${width - padRight - 4}" y="${(zLowerY + 9).toFixed(1)}" text-anchor="end" fill="#22d3ee" font-family="ui-monospace, monospace" font-size="8.5" opacity="0.8">−1.8σ WASHOUT</text>
          <line x1="${padLeft}" y1="${zeroFlowY.toFixed(1)}" x2="${width - padRight}" y2="${zeroFlowY.toFixed(1)}" stroke="${COLORS.axis}" stroke-width="1"/>
        `;

        // Flow Ticks (right)
        const rightTicks = [maximumFlow, 0, -maximumFlow].map(value => {
          const y = yFlow(value);
          return `<text x="${width - padRight + 7}" y="${(y + 4).toFixed(1)}" text-anchor="start" fill="${COLORS.muted}" font-family="ui-monospace, SFMono-Regular, monospace" font-size="11">${escapeHtml(axisNumber(value))}</text>`;
        }).join('');

        const description = 'Source-reported NAV or share price on the upper canvas and daily net flow plus 5D rolling z-score on the lower canvas.';

        // Crosshair overlay spanning full dual canvas
        let overlay = '';
        if (this.flowHoverIndex !== null && this.flowHoverChart === 'price' && this.flowHoverIndex >= 0 && this.flowHoverIndex < rows.length) {
          const hRow = rows[this.flowHoverIndex];
          const x = frame.xScale(this.flowHoverIndex);
          const hPrice = finiteNumber(hRow.nav);
          const hFlow = finiteNumber(hRow.flow);
          const pY = hPrice !== null ? yPrice(hPrice) : null;
          const fY = hFlow !== null ? yFlow(hFlow) : null;

          overlay = `<g class="flow-crosshair-group" pointer-events="none">
            ${pY !== null ? `<circle cx="${x.toFixed(1)}" cy="${pY.toFixed(1)}" r="5" fill="#22d3ee" stroke="#ffffff" stroke-width="2"/>` : ''}
            ${fY !== null ? `<circle cx="${x.toFixed(1)}" cy="${fY.toFixed(1)}" r="4" fill="${(hFlow || 0) >= 0 ? '#34d399' : '#fb7185'}" stroke="#ffffff" stroke-width="1.8"/>` : ''}
          </g>`;
        }

        return `<svg viewBox="0 0 ${width} ${height}" data-pad-left="${padLeft}" data-pad-right="${padRight}" data-chart-width="${width}" role="img" aria-labelledby="flow-price-title flow-price-desc">
          <title id="flow-price-title">Dual-Engine Workbench: Price and Daily ETF Net Flow</title>
          <desc id="flow-price-desc">${escapeHtml(description)}</desc>
          ${regimeBands}
          ${dividerLine}
          ${leftTicks}
          ${rightTicks}
          ${zThresholds}
          ${bars}
          ${zLine}
          <path d="${linePath(pricePoints)}" fill="none" stroke="${COLORS.cyan}" stroke-width="2.2" stroke-linejoin="round" stroke-linecap="round"/>
          ${climaxMarkers}
          ${overlay}
          ${frame.dateTicks}
        </svg>`;
      },

      flowChartMeasureStart(event) {
        if (!this.flowMeasureActive) return;
        const svg = event.currentTarget.closest('svg') || event.currentTarget;
        const rect = svg.getBoundingClientRect();
        const clientX = event.clientX;
        const padLeft = finiteNumber(svg.dataset.padLeft) || 68;
        const padRight = finiteNumber(svg.dataset.padRight) || 68;
        const chartW = rect.width - padLeft - padRight;
        const relX = clientX - rect.left - padLeft;
        const rows = this.flowSelectedRows;
        if (!rows.length || chartW <= 0) return;
        const fraction = Math.max(0, Math.min(1, relX / chartW));
        const idx = Math.round(fraction * (rows.length - 1));
        this.flowMeasureDragging = true;
        this.flowMeasureStartIdx = idx;
        this.flowMeasureCurrentIdx = idx;
        this._flowComputeMeasurement();
      },

      flowChartMeasureMove(event) {
        if (this.flowMeasureActive && this.flowMeasureDragging) {
          const svg = event.currentTarget.closest('svg') || event.currentTarget;
          const rect = svg.getBoundingClientRect();
          const clientX = event.clientX;
          const padLeft = finiteNumber(svg.dataset.padLeft) || 68;
          const padRight = finiteNumber(svg.dataset.padRight) || 68;
          const chartW = rect.width - padLeft - padRight;
          const relX = clientX - rect.left - padLeft;
          const rows = this.flowSelectedRows;
          if (!rows.length || chartW <= 0) return;
          const fraction = Math.max(0, Math.min(1, relX / chartW));
          const idx = Math.round(fraction * (rows.length - 1));
          if (idx !== this.flowMeasureCurrentIdx) {
            this.flowMeasureCurrentIdx = idx;
            this._flowComputeMeasurement();
          }
        } else {
          this.flowChartPointerMove(event, 'workbench');
        }
      },

      flowChartMeasureEnd() {
        if (this.flowMeasureDragging) {
          this.flowMeasureDragging = false;
          if (this.flowMeasureStartIdx === this.flowMeasureCurrentIdx) {
            this.flowResetMeasurement();
          }
        }
      },

      flowResetMeasurement() {
        this.flowMeasureActive = false;
        this.flowMeasureDragging = false;
        this.flowMeasureStartIdx = null;
        this.flowMeasureCurrentIdx = null;
        this.flowMeasureResult = null;
      },

      _flowComputeMeasurement() {
        const rows = this.flowSelectedRows;
        if (this.flowMeasureStartIdx === null || this.flowMeasureCurrentIdx === null || !rows.length) return;
        const i1 = Math.min(this.flowMeasureStartIdx, this.flowMeasureCurrentIdx);
        const i2 = Math.max(this.flowMeasureStartIdx, this.flowMeasureCurrentIdx);
        const r1 = rows[i1];
        const r2 = rows[i2];
        if (!r1 || !r2) return;
        const sessions = i2 - i1 + 1;
        const slice = rows.slice(i1, i2 + 1);
        const p1 = finiteNumber(r1.nav);
        const p2 = finiteNumber(r2.nav);
        const pDiff = (p1 !== null && p2 !== null) ? p2 - p1 : null;
        const pPct = (p1 && pDiff !== null) ? (pDiff / p1) * 100 : null;
        const netFlow = slice.reduce((sum, r) => sum + (finiteNumber(r.flow) || 0), 0);
        const aumVal = finiteNumber(this.flowSelectedInstrument?.aum_m) || 0;
        const flowPctAum = (aumVal > 0) ? (netFlow / (aumVal * 1e6)) * 100 : null;
        let totVol = 0;
        let totDollarVol = 0;
        slice.forEach(r => {
          const v = finiteNumber(r.volume) || (r.nav ? Math.round(Math.abs(r.flow || 0) / r.nav) : 0);
          totVol += v;
          if (r.nav) totDollarVol += v * r.nav;
        });
        const penetration = (totDollarVol > 0) ? (Math.abs(netFlow) / totDollarVol) * 100 : 0;
        this.flowMeasureResult = {
          i1,
          i2,
          sessions,
          count: sessions,
          startDate: r1.date,
          endDate: r2.date,
          p1,
          p2,
          priceDiff: pDiff,
          pricePct: pPct,
          priceReturnPct: pPct,
          netFlow,
          cumFlow: netFlow,
          flowPctAum,
          totVolume: totVol,
          totalVolume: totVol,
          totDollarVol,
          totalDollarVol: totDollarVol,
          convictionPenetration: penetration,
          convictionRatio: penetration
        };
      },

      get flowWorkbenchSummary() {
        const rows = this.flowSelectedRows;
        if (!rows.length) return null;
        const first = rows[0];
        const last = rows[rows.length - 1];
        const p1 = finiteNumber(first.nav);
        const p2 = finiteNumber(last.nav);
        const navRetPct = (p1 !== null && p2 !== null && p1 > 0) ? ((p2 - p1) / p1) * 100 : null;
        const netFlow = finiteNumber(last.selectedCumulative) || 0;
        const aum = (finiteNumber(this.flowSelectedInstrument?.aum_m) || 0) * 1e6;
        const flowPctAum = aum > 0 ? (netFlow / aum) * 100 : null;
        const mean20 = finiteNumber(last.rollingMean20);
        const latestZ = finiteNumber(last.priorOnlyZScore);
        const curFlow = finiteNumber(last.flow);
        return {
          count: rows.length,
          startDate: first.date,
          endDate: last.date,
          navRetPct,
          netFlow,
          flowPctAum,
          mean20,
          latestZ,
          curFlow,
          lastPrice: p2
        };
      },

      get flowWorkbenchSvg() {
        const rows = this.flowSelectedRows;
        const width = this.flowChartWidth;
        if (rows.length < 2) return emptyChart(width, 420, 'Select at least two source observations.');

        const padLeft = width < 500 ? 54 : 68;
        const padRight = width < 500 ? 54 : 68;
        const padTop = 24;
        const padBottom = 30;
        const chartW = width - padLeft - padRight;

        const showPrice = this.flowWorkbenchPriceEnabled && this.flowPriceAvailable;
        const showCum = this.flowWorkbenchCumEnabled;
        const showDaily = this.flowWorkbenchDailyEnabled;
        const showVol = this.flowWorkbenchVolumeEnabled;
        const showZ = this.flowWorkbenchZScoreEnabled;
        const showShocks = this.flowWorkbenchShocksEnabled;

        const hasTier1 = showPrice || showCum;
        const hasTier2 = showDaily || showVol;
        const hasTier3 = showZ;

        const tier1H = hasTier1 ? (width < 500 ? 210 : 250) : 0;
        const tier2H = hasTier2 ? (width < 500 ? 110 : 130) : 0;
        const tier3H = hasTier3 ? (width < 500 ? 70 : 85) : 0;

        const gap = 16;
        let runningY = padTop;
        const t1Top = runningY;
        const t1Bottom = runningY + tier1H;
        if (hasTier1) runningY = t1Bottom + gap;

        const t2Top = runningY;
        const t2Bottom = runningY + tier2H;
        if (hasTier2) runningY = t2Bottom + gap;

        const t3Top = runningY;
        const t3Bottom = runningY + tier3H;
        if (hasTier3) runningY = t3Bottom;

        const totalH = runningY + padBottom;

        const xScale = index => padLeft + (rows.length < 2 ? chartW / 2 : index * chartW / (rows.length - 1));
        const slot = chartW / Math.max(1, rows.length);

        const aumTotal = (finiteNumber(this.flowSelectedInstrument?.aum_m) || 0) * 1e6;
        const isPctAum = this.flowScaleMode === 'pct_aum' && aumTotal > 0;

        const baseKey = `${this.flowTicker}:${this.flowStartIndex}:${this.flowEndIndex}:${this.flowScaleMode}:${showPrice}:${showCum}:${showDaily}:${showVol}:${showZ}:${showShocks}:${width}`;

        if (this._flowWorkbenchBaseKey !== baseKey || !this._flowWorkbenchBaseSvg) {
          // --- TIER 1: Cumulative Flow Area + Price NAV Overlay ---
          let tier1Svg = '';
          if (hasTier1) {
            const cumValues = rows.map(r => {
              const c = finiteNumber(r.selectedCumulative);
              if (c === null) return null;
              return isPctAum ? (c / aumTotal) * 100 : c;
            }).filter(v => v !== null);

            const minCum = cumValues.length ? Math.min(0, ...cumValues) : 0;
            const maxCum = cumValues.length ? Math.max(0, ...cumValues) : 0;
            const cumPad = Math.max(isPctAum ? 0.5 : 1e6, (maxCum - minCum) * 0.08);
            const cumLow = minCum - cumPad;
            const cumHigh = maxCum + cumPad;
            const yCum = val => t1Top + (cumHigh - val) / (cumHigh - cumLow || 1) * tier1H;
            const zeroCumY = yCum(0);

            const prices = rows.map(r => finiteNumber(r.nav)).filter(v => v !== null);
            const minP = prices.length ? Math.min(...prices) : 0;
            const maxP = prices.length ? Math.max(...prices) : 100;
            const pPad = Math.max((maxP - minP) * 0.08, maxP * 0.01);
            const pLow = Math.max(0, minP - pPad);
            const pHigh = maxP + pPad;
            const yPrice = val => t1Top + (pHigh - val) / (pHigh - pLow || 1) * tier1H;

            let t1Grid = '';
            if (showCum) {
              const candidateTicks = [cumHigh, 0];
              if (cumLow < 0 && Math.abs(yCum(0) - yCum(cumLow)) >= 22) {
                candidateTicks.push(cumLow);
              }
              candidateTicks.sort((a, b) => b - a);
              const renderedY = [];
              candidateTicks.forEach(v => {
                const y = yCum(v);
                if (renderedY.some(ry => Math.abs(ry - y) < 20)) return;
                renderedY.push(y);
                const isZero = Math.abs(v) < 1e-6;
                const lbl = isPctAum ? `${v >= 0 ? '+' : ''}${v.toFixed(1)}%` : (isZero ? '$0' : axisNumber(v));
                t1Grid += `<line x1="${padLeft}" y1="${y.toFixed(1)}" x2="${width - padRight}" y2="${y.toFixed(1)}" stroke="${isZero ? 'rgba(34,211,238,0.28)' : COLORS.grid}" stroke-width="${isZero ? 1.2 : 1}"${isZero ? '' : ' stroke-dasharray="3 3"'} /><text x="${padLeft - 8}" y="${(y + 4).toFixed(1)}" text-anchor="end" fill="${isZero ? COLORS.text : COLORS.cyan}" font-family="ui-monospace, monospace" font-size="10">${escapeHtml(lbl)}</text>`;
              });
              if (showPrice && prices.length) {
                const pMid = (maxP + minP) / 2;
                [maxP, pMid, minP].forEach(v => {
                  const y = yPrice(v);
                  const pStr = v >= 50 ? `$${Math.round(v)}` : `$${v.toFixed(2)}`;
                  t1Grid += `<text x="${width - padRight + 8}" y="${(y + 4).toFixed(1)}" text-anchor="start" fill="#c084fc" font-family="ui-monospace, monospace" font-size="10">${pStr}</text>`;
                });
              }
            } else if (showPrice) {
              const pMid = (maxP + minP) / 2;
              [maxP, pMid, minP].forEach(v => {
                const y = yPrice(v);
                const pStr = v >= 50 ? `$${Math.round(v)}` : `$${v.toFixed(2)}`;
                t1Grid += `<line x1="${padLeft}" y1="${y.toFixed(1)}" x2="${width - padRight}" y2="${y.toFixed(1)}" stroke="${COLORS.grid}" stroke-width="1" stroke-dasharray="3 3"/><text x="${padLeft - 8}" y="${(y + 4).toFixed(1)}" text-anchor="end" fill="#c084fc" font-family="ui-monospace, monospace" font-size="10">${pStr}</text><text x="${width - padRight + 8}" y="${(y + 4).toFixed(1)}" text-anchor="start" fill="#c084fc" font-family="ui-monospace, monospace" font-size="10">${pStr}</text>`;
              });
            }

            let cumPaths = '';
            if (showCum && cumValues.length) {
              const cumPoints = rows.map((r, i) => {
                const c = finiteNumber(r.selectedCumulative);
                const val = c === null ? null : (isPctAum ? (c / aumTotal) * 100 : c);
                return { x: xScale(i), y: finiteScale(yCum, val) };
              });

              const posPoints = cumPoints.map(p => ({ x: p.x, y: p.y !== null ? Math.min(p.y, zeroCumY) : zeroCumY }));
              const negPoints = cumPoints.map(p => ({ x: p.x, y: p.y !== null ? Math.max(p.y, zeroCumY) : zeroCumY }));

              const posArea = areaPath(posPoints, zeroCumY);
              const negArea = areaPath(negPoints, zeroCumY);

              cumPaths += `<defs>
                <linearGradient id="wb-cum-pos" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="#34d399" stop-opacity="0.22"/><stop offset="100%" stop-color="#34d399" stop-opacity="0.0"/></linearGradient>
                <linearGradient id="wb-cum-neg" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="#fb7185" stop-opacity="0.0"/><stop offset="100%" stop-color="#fb7185" stop-opacity="0.22"/></linearGradient>
              </defs>`;
              if (posArea) cumPaths += `<path d="${posArea}" fill="url(#wb-cum-pos)"/>`;
              if (negArea) cumPaths += `<path d="${negArea}" fill="url(#wb-cum-neg)"/>`;
              cumPaths += `<path d="${linePath(cumPoints)}" fill="none" stroke="${COLORS.cyan}" stroke-width="2.2" stroke-linejoin="round" stroke-linecap="round"/>`;
            }

            let pricePathSvg = '';
            if (showPrice && prices.length) {
              const pricePoints = rows.map((r, i) => ({ x: xScale(i), y: finiteScale(yPrice, r.nav) }));
              pricePathSvg += `<path d="${linePath(pricePoints)}" fill="none" stroke="#c084fc" stroke-width="2.0" stroke-linejoin="round" stroke-linecap="round"/>`;

              let maxPIdx = 0, minPIdx = 0;
              rows.forEach((r, i) => {
                if (r.nav !== null) {
                  if (r.nav > (rows[maxPIdx]?.nav ?? -Infinity)) maxPIdx = i;
                  if (r.nav < (rows[minPIdx]?.nav ?? Infinity)) minPIdx = i;
                }
              });
              if (rows[maxPIdx]?.nav !== null) {
                const xHigh = xScale(maxPIdx);
                const yHigh = yPrice(rows[maxPIdx].nav);
                const anchor = (xHigh > padLeft + chartW - 75) ? 'end' : (xHigh < padLeft + 75 ? 'start' : 'middle');
                const tx = anchor === 'end' ? xHigh - 8 : (anchor === 'start' ? xHigh + 8 : xHigh);
                const ty = (yHigh < t1Top + 28) ? yHigh + 18 : yHigh - 14;
                pricePathSvg += `<circle cx="${xHigh.toFixed(1)}" cy="${yHigh.toFixed(1)}" r="3.5" fill="#c084fc" stroke="#090d16" stroke-width="1.5"/><rect x="${(anchor === 'end' ? tx - 72 : (anchor === 'start' ? tx - 4 : tx - 36)).toFixed(1)}" y="${(ty - 10).toFixed(1)}" width="76" height="15" rx="3" fill="rgba(8,12,18,0.92)" stroke="#c084fc" stroke-width="1"/><text x="${tx.toFixed(1)}" y="${(ty + 1).toFixed(1)}" text-anchor="${anchor}" fill="#c084fc" font-family="ui-monospace, monospace" font-size="9" font-weight="700">HIGH $${rows[maxPIdx].nav.toFixed(2)}</text>`;
              }
              if (rows[minPIdx]?.nav !== null && minPIdx !== maxPIdx) {
                const xLow = xScale(minPIdx);
                const yLow = yPrice(rows[minPIdx].nav);
                const anchor = (xLow > padLeft + chartW - 75) ? 'end' : (xLow < padLeft + 75 ? 'start' : 'middle');
                const tx = anchor === 'end' ? xLow - 8 : (anchor === 'start' ? xLow + 8 : xLow);
                const ty = (yLow > t1Bottom - 26) ? yLow - 14 : yLow + 18;
                pricePathSvg += `<circle cx="${xLow.toFixed(1)}" cy="${yLow.toFixed(1)}" r="3.5" fill="#c084fc" stroke="#090d16" stroke-width="1.5"/><rect x="${(anchor === 'end' ? tx - 70 : (anchor === 'start' ? tx - 4 : tx - 35)).toFixed(1)}" y="${(ty - 10).toFixed(1)}" width="74" height="15" rx="3" fill="rgba(8,12,18,0.92)" stroke="#c084fc" stroke-width="1"/><text x="${tx.toFixed(1)}" y="${(ty + 1).toFixed(1)}" text-anchor="${anchor}" fill="#c084fc" font-family="ui-monospace, monospace" font-size="9" font-weight="700">LOW $${rows[minPIdx].nav.toFixed(2)}</text>`;
              }

              if (showShocks) {
                const shockCandidates = rows.map((r, i) => ({
                  row: r,
                  idx: i,
                  z: finiteNumber(r.priorOnlyZScore) || 0,
                  p: finiteNumber(r.nav)
                })).filter(c => c.p !== null && Math.abs(c.z) >= 2.5);
                shockCandidates.sort((a, b) => Math.abs(b.z) - Math.abs(a.z));
                const topShocks = shockCandidates.slice(0, 8);
                topShocks.forEach(c => {
                  const x = xScale(c.idx);
                  const y = yPrice(c.p);
                  const isBuy = c.z >= 2.5;
                  const dotColor = isBuy ? '#34d399' : '#fb7185';
                  pricePathSvg += `<circle cx="${x.toFixed(1)}" cy="${y.toFixed(1)}" r="4.2" fill="${dotColor}" stroke="#090d16" stroke-width="1.5"><title>${isBuy ? 'Climax Inflow Surge' : 'Climax Redemption Washout'} (${c.row.date}): Z=${c.z.toFixed(2)}σ · NAV $${c.p.toFixed(2)}</title></circle>`;
                });
              }
            }

            tier1Svg = `<g class="flow-tier-1">${t1Grid}${cumPaths}${pricePathSvg}</g>`;
          }

          // --- TIER 2: Daily Net Flow Bars + Volume Overlay ---
          let tier2Svg = '';
          if (hasTier2) {
            const dailyFlows = rows.map(r => {
              const f = finiteNumber(r.flow);
              if (f === null) return null;
              return isPctAum ? (f / aumTotal) * 100 : f;
            }).filter(v => v !== null);

            const maxDaily = Math.max(isPctAum ? 0.2 : 1e5, ...dailyFlows.map(v => Math.abs(v))) * 1.08;
            const yDaily = val => t2Top + tier2H / 2 - (val / (maxDaily || 1)) * (tier2H / 2);
            const zeroDailyY = yDaily(0);

            const volumes = rows.map(r => finiteNumber(r.volume) || (r.nav ? Math.round(Math.abs(r.flow || 0) / r.nav) : 0));
            const maxVol = Math.max(100, ...volumes) * 1.15;

            let t2Grid = `<line x1="${padLeft}" y1="${(t2Top - 8).toFixed(1)}" x2="${width - padRight}" y2="${(t2Top - 8).toFixed(1)}" stroke="rgba(255,255,255,0.08)" stroke-width="1"/>`;
            t2Grid += `<line x1="${padLeft}" y1="${zeroDailyY.toFixed(1)}" x2="${width - padRight}" y2="${zeroDailyY.toFixed(1)}" stroke="rgba(255,255,255,0.18)" stroke-width="1"/>`;

            t2Grid += `<text x="${padLeft - 8}" y="${(zeroDailyY + 3).toFixed(1)}" text-anchor="end" fill="${COLORS.subtle}" font-family="ui-monospace, monospace" font-size="10">${isPctAum ? '0.0%' : '$0'}</text>`;
            t2Grid += `<text x="${padLeft - 8}" y="${(t2Top + 10).toFixed(1)}" text-anchor="end" fill="#34d399" font-family="ui-monospace, monospace" font-size="9">${isPctAum ? `+${maxDaily.toFixed(1)}%` : axisNumber(maxDaily)}</text>`;
            t2Grid += `<text x="${padLeft - 8}" y="${(t2Bottom - 3).toFixed(1)}" text-anchor="end" fill="#fb7185" font-family="ui-monospace, monospace" font-size="9">${isPctAum ? `−${maxDaily.toFixed(1)}%` : axisNumber(-maxDaily)}</text>`;

            let volBarsSvg = '';
            const barW = Math.max(0.8, Math.min(10, slot * 0.72));
            if (showVol) {
              const volH = showDaily ? (tier2H * 0.38) : (tier2H - 10);
              const yVol = val => t2Bottom - (val / (maxVol || 1)) * volH;
              t2Grid += `<text x="${width - padRight + 8}" y="${(t2Bottom - volH + 6).toFixed(1)}" text-anchor="start" fill="#94a3b8" font-family="ui-monospace, monospace" font-size="9">${formatShareVolume(maxVol)}</text>`;
              rows.forEach((r, i) => {
                const v = finiteNumber(r.volume) || (r.nav ? Math.round(Math.abs(r.flow || 0) / r.nav) : 0);
                if (v > 0) {
                  const x = xScale(i) - barW / 2;
                  const h = Math.max(1, (v / (maxVol || 1)) * volH);
                  volBarsSvg += `<rect x="${x.toFixed(1)}" y="${(t2Bottom - h).toFixed(1)}" width="${barW.toFixed(1)}" height="${h.toFixed(1)}" fill="rgba(148,163,184,0.14)" rx="0.5"/>`;
                }
              });
              const volMaPoints = rows.map((r, i) => ({ x: xScale(i), y: finiteScale(yVol, r.rollingVolume20) }));
              volBarsSvg += `<path d="${linePath(volMaPoints)}" fill="none" stroke="#64748b" stroke-width="1.2" stroke-dasharray="2 2" opacity="0.6"/>`;
            }

            let dailyBarsSvg = '';
            if (showDaily) {
              rows.forEach((r, i) => {
                const f = finiteNumber(r.flow);
                if (f === null) return;
                const val = isPctAum ? (f / aumTotal) * 100 : f;
                const x = xScale(i) - barW / 2;
                const y = yDaily(val);
                const top = val >= 0 ? y : zeroDailyY;
                const h = Math.max(1.8, Math.abs(y - zeroDailyY));
                const fill = val === 0 ? COLORS.neutral : val > 0 ? COLORS.positive : COLORS.negative;
                dailyBarsSvg += `<rect x="${x.toFixed(1)}" y="${top.toFixed(1)}" width="${barW.toFixed(1)}" height="${h.toFixed(1)}" fill="${fill}" opacity="0.9" rx="0.6"/>`;
              });
            }

            tier2Svg = `<g class="flow-tier-2">${t2Grid}${volBarsSvg}${dailyBarsSvg}</g>`;
          }

          // --- TIER 3: Normalized Flow Z-Score Oscillator ---
          let tier3Svg = '';
          if (hasTier3) {
            const zValues = rows.map(r => finiteNumber(r.priorOnlyZScore)).filter(v => v !== null);
            const maxAbsZ = zValues.length ? Math.max(...zValues.map(v => Math.abs(v))) : 1.5;
            const zBound = Math.max(3.0, Math.ceil(maxAbsZ * 1.15));

            const zeroZY = t3Top + tier3H / 2;
            const yZ = z => zeroZY - (Math.max(-zBound, Math.min(zBound, z)) / zBound) * (tier3H / 2);
            const yPos15 = yZ(1.5);
            const yNeg15 = yZ(-1.5);

            let t3Grid = `<line x1="${padLeft}" y1="${(t3Top - 8).toFixed(1)}" x2="${width - padRight}" y2="${(t3Top - 8).toFixed(1)}" stroke="rgba(255,255,255,0.08)" stroke-width="1"/>`;
            t3Grid += `<rect x="${padLeft}" y="${yPos15.toFixed(1)}" width="${chartW}" height="${(zeroZY - yPos15).toFixed(1)}" fill="rgba(34,211,238,0.04)"/>`;
            t3Grid += `<rect x="${padLeft}" y="${zeroZY.toFixed(1)}" width="${chartW}" height="${(yNeg15 - zeroZY).toFixed(1)}" fill="rgba(251,113,133,0.04)"/>`;
            t3Grid += `<line x1="${padLeft}" y1="${zeroZY.toFixed(1)}" x2="${width - padRight}" y2="${zeroZY.toFixed(1)}" stroke="rgba(255,255,255,0.18)" stroke-width="1"/>`;
            t3Grid += `<line x1="${padLeft}" y1="${yPos15.toFixed(1)}" x2="${width - padRight}" y2="${yPos15.toFixed(1)}" stroke="rgba(34,211,238,0.32)" stroke-width="1" stroke-dasharray="3 3"/>`;
            t3Grid += `<line x1="${padLeft}" y1="${yNeg15.toFixed(1)}" x2="${width - padRight}" y2="${yNeg15.toFixed(1)}" stroke="rgba(245,158,11,0.32)" stroke-width="1" stroke-dasharray="3 3"/>`;

            t3Grid += `<text x="${padLeft - 8}" y="${(yPos15 + 3).toFixed(1)}" text-anchor="end" fill="#22d3ee" font-family="ui-monospace, monospace" font-size="9">+1.5σ</text>`;
            t3Grid += `<text x="${padLeft - 8}" y="${(zeroZY + 3).toFixed(1)}" text-anchor="end" fill="${COLORS.subtle}" font-family="ui-monospace, monospace" font-size="9">0σ</text>`;
            t3Grid += `<text x="${padLeft - 8}" y="${(yNeg15 + 3).toFixed(1)}" text-anchor="end" fill="#f59e0b" font-family="ui-monospace, monospace" font-size="9">−1.5σ</text>`;

            t3Grid += `<text x="${width - padRight + 8}" y="${(yPos15 + 3).toFixed(1)}" text-anchor="start" fill="#22d3ee" font-family="ui-monospace, monospace" font-size="9" font-weight="600" letter-spacing="0.04em">ACCUMULATION (+1.5σ)</text>`;
            t3Grid += `<text x="${width - padRight + 8}" y="${(yNeg15 + 3).toFixed(1)}" text-anchor="start" fill="#f59e0b" font-family="ui-monospace, monospace" font-size="9" font-weight="600" letter-spacing="0.04em">DISTRIBUTION (−1.5σ)</text>`;

            const smoothZ = [];
            for (let i = 0; i < rows.length; i++) {
              let sum = 0, cnt = 0;
              for (let j = Math.max(0, i - 4); j <= i; j++) {
                const val = finiteNumber(rows[j].priorOnlyZScore);
                if (val !== null) { sum += val; cnt++; }
              }
              smoothZ.push(cnt ? sum / cnt : null);
            }

            const smoothPoints = rows.map((r, i) => ({ x: xScale(i), y: finiteScale(yZ, smoothZ[i]) }));
            const posZArea = areaPath(smoothPoints.map(p => ({ x: p.x, y: p.y !== null ? Math.min(p.y, zeroZY) : zeroZY })), zeroZY);
            const negZArea = areaPath(smoothPoints.map(p => ({ x: p.x, y: p.y !== null ? Math.max(p.y, zeroZY) : zeroZY })), zeroZY);

            let zPaths = `<defs>
              <linearGradient id="wb-z-pos" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="#22d3ee" stop-opacity="0.22"/><stop offset="100%" stop-color="#22d3ee" stop-opacity="0.0"/></linearGradient>
              <linearGradient id="wb-z-neg" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="#fb7185" stop-opacity="0.0"/><stop offset="100%" stop-color="#fb7185" stop-opacity="0.22"/></linearGradient>
            </defs>`;
            if (posZArea) zPaths += `<path d="${posZArea}" fill="url(#wb-z-pos)"/>`;
            if (negZArea) zPaths += `<path d="${negZArea}" fill="url(#wb-z-neg)"/>`;

            zPaths += `<path d="${linePath(smoothPoints)}" fill="none" stroke="#e879f9" stroke-width="2.0" stroke-linejoin="round" stroke-linecap="round"/>`;

            tier3Svg = `<g class="flow-tier-3">${t3Grid}${zPaths}</g>`;
          }

          // --- Date Ticks at Bottom ---
          const tickIndices = dateTickIndices(rows.length, width);
          const dateTicksSvg = tickIndices.map(idx => {
            const x = xScale(idx);
            const anchor = idx === 0 ? 'start' : (idx === rows.length - 1 ? 'end' : 'middle');
            return `<text x="${x.toFixed(1)}" y="${(totalH - 8).toFixed(1)}" text-anchor="${anchor}" fill="${COLORS.subtle}" font-family="ui-monospace, monospace" font-size="10">${escapeHtml(axisDate(rows[idx].date))}</text>`;
          }).join('');

          this._flowWorkbenchBaseKey = baseKey;
          this._flowWorkbenchBaseSvg = `<svg viewBox="0 0 ${width} ${totalH}" data-pad-left="${padLeft}" data-pad-right="${padRight}" data-chart-width="${width}" role="img" aria-label="Synchronized Flow Studio" style="width:100%;height:auto;display:block">
            <title>Synchronized Flow Studio</title>
            ${tier1Svg}
            ${tier2Svg}
            ${tier3Svg}
            ${dateTicksSvg}
            <!-- MEASURE_BAND -->
          </svg>`;
        }

        if (this.flowMeasureActive && this.flowMeasureStartIdx !== null && this.flowMeasureCurrentIdx !== null) {
          const m1 = Math.min(this.flowMeasureStartIdx, this.flowMeasureCurrentIdx);
          const m2 = Math.max(this.flowMeasureStartIdx, this.flowMeasureCurrentIdx);
          const x1 = xScale(m1);
          const x2 = xScale(m2);
          const bandW = Math.max(2, Math.abs(x2 - x1));
          const bandX = Math.min(x1, x2);
          const measureSvg = `<g class="flow-measure-band" pointer-events="none">
            <rect x="${bandX.toFixed(1)}" y="${padTop}" width="${bandW.toFixed(1)}" height="${(runningY - padTop).toFixed(1)}" fill="rgba(34,211,238,0.14)" stroke="#22d3ee" stroke-width="1.5" stroke-dasharray="3 3"/>
            <line x1="${bandX.toFixed(1)}" y1="${padTop}" x2="${bandX.toFixed(1)}" y2="${runningY}" stroke="#22d3ee" stroke-width="1.5"/>
            <line x1="${(bandX + bandW).toFixed(1)}" y1="${padTop}" x2="${(bandX + bandW).toFixed(1)}" y2="${runningY}" stroke="#22d3ee" stroke-width="1.5"/>
          </g>`;
          return this._flowWorkbenchBaseSvg.replace('<!-- MEASURE_BAND -->', measureSvg);
        }

        return this._flowWorkbenchBaseSvg.replace('<!-- MEASURE_BAND -->', '');
      },

      get flowImpulseStats() {
        const rows = this.flowSelectedRows;
        if (!rows.length) return { sum20: null, sum60: null, spread: null };
        const last = rows[rows.length - 1];
        const s20 = finiteNumber(last?.rollingSum20);
        const s60 = finiteNumber(last?.rollingSum60);
        const spread = (s20 !== null && s60 !== null) ? s20 - s60 : null;
        return { sum20: s20, sum60: s60, spread };
      },

      get flowImpulseRegimeBadge() {
        const { sum20, sum60 } = this.flowImpulseStats;
        if (sum20 === null || sum60 === null) return { state: 'BALANCED', label: 'NEUTRAL' };
        if (sum20 > 0 && sum60 > 0) {
          if (sum20 >= sum60) return { state: 'ACCUMULATION', label: 'ACCELERATING INFLOW' };
          return { state: 'ACCUMULATION', label: 'SUSTAINED INFLOW (DECELERATING)' };
        }
        if (sum20 < 0 && sum60 < 0) {
          if (sum20 <= sum60) return { state: 'DISTRIBUTION', label: 'ACCELERATING OUTFLOW' };
          return { state: 'DISTRIBUTION', label: 'SUSTAINED OUTFLOW (MODERATING)' };
        }
        if (sum20 > 0 && sum60 <= 0) {
          return { state: 'ACCUMULATION', label: 'TACTICAL INFLOW REBOUND' };
        }
        return { state: 'DISTRIBUTION', label: 'TACTICAL DISTRIBUTION' };
      },

      get flowMultiHorizonImpulseChartSvg() {
        const rows = this.flowSelectedRows;
        const width = Math.max(280, Math.min(680, Math.round(this.flowChartWidth * 0.58)));
        const height = 215;
        if (rows.length < 5) return emptyChart(width, height, 'Select at least five sessions for multi-horizon impulse.');
        const frame = chartFrame({ records: rows, width, height, padding: { left: 60, right: 24, top: 20, bottom: 28 } });
        const values = rows.flatMap(r => [finiteNumber(r.rollingSum20), finiteNumber(r.rollingSum60)]).filter(v => v !== null);
        if (!values.length) return emptyChart(width, height, 'Rolling multi-horizon flow sums unavailable.');

        const minVal = Math.min(...values);
        const maxVal = Math.max(...values);
        let yMin = Math.min(0, minVal);
        let yMax = Math.max(0, maxVal);
        if (yMax === 0 && yMin === 0) {
          yMax = 1; yMin = -1;
        } else if (yMin === 0) {
          yMin = -Math.max(1e5, yMax * 0.08);
          yMax = yMax * 1.06;
        } else if (yMax === 0) {
          yMax = Math.max(1e5, Math.abs(yMin) * 0.08);
          yMin = yMin * 1.06;
        } else {
          const rng = yMax - yMin;
          yMax += rng * 0.06;
          yMin -= rng * 0.06;
        }

        const yScale = value => frame.padding.top + frame.chartHeight - ((value - yMin) / (yMax - yMin)) * frame.chartHeight;
        const pts20 = rows.map((r, idx) => ({ x: frame.xScale(idx), y: finiteScale(yScale, r.rollingSum20) }));
        const pts60 = rows.map((r, idx) => ({ x: frame.xScale(idx), y: finiteScale(yScale, r.rollingSum60) }));

        // Grid lines: zero baseline, top, bottom (if negative flows exist)
        const yZero = yScale(0);
        let grid = `<line x1="${frame.padding.left}" y1="${yZero.toFixed(1)}" x2="${width - frame.padding.right}" y2="${yZero.toFixed(1)}" stroke="rgba(255,255,255,0.22)" stroke-width="1.2"/>`;
        grid += `<text x="${frame.padding.left - 6}" y="${(yZero + 3.5).toFixed(1)}" text-anchor="end" fill="${COLORS.subtle}" font-family="ui-monospace, SFMono-Regular, monospace" font-size="10">$0</text>`;

        const yTop = yScale(yMax);
        grid += `<line x1="${frame.padding.left}" y1="${yTop.toFixed(1)}" x2="${width - frame.padding.right}" y2="${yTop.toFixed(1)}" stroke="${COLORS.grid}" stroke-width="0.8"/>`;
        grid += `<text x="${frame.padding.left - 6}" y="${(yTop + 3.5).toFixed(1)}" text-anchor="end" fill="${COLORS.subtle}" font-family="ui-monospace, SFMono-Regular, monospace" font-size="10">${escapeHtml(axisNumber(yMax))}</text>`;

        if (minVal < 0) {
          const yBot = yScale(yMin);
          grid += `<line x1="${frame.padding.left}" y1="${yBot.toFixed(1)}" x2="${width - frame.padding.right}" y2="${yBot.toFixed(1)}" stroke="${COLORS.grid}" stroke-width="0.8"/>`;
          grid += `<text x="${frame.padding.left - 6}" y="${(yBot + 3.5).toFixed(1)}" text-anchor="end" fill="${COLORS.subtle}" font-family="ui-monospace, SFMono-Regular, monospace" font-size="10">${escapeHtml(axisNumber(yMin))}</text>`;
        } else if (yMax > 2e6) {
          const midVal = yMax / 2;
          const yMid = yScale(midVal);
          grid += `<line x1="${frame.padding.left}" y1="${yMid.toFixed(1)}" x2="${width - frame.padding.right}" y2="${yMid.toFixed(1)}" stroke="${COLORS.grid}" stroke-width="0.6" stroke-dasharray="2 3"/>`;
          grid += `<text x="${frame.padding.left - 6}" y="${(yMid + 3.5).toFixed(1)}" text-anchor="end" fill="${COLORS.subtle}" font-family="ui-monospace, SFMono-Regular, monospace" font-size="9.5">${escapeHtml(axisNumber(midVal))}</text>`;
        }

        // Momentum spread ribbon between 20D and 60D
        const validPairs = [];
        for (let i = 0; i < rows.length; i++) {
          if (pts20[i].y !== null && pts60[i].y !== null) {
            validPairs.push({ x: pts20[i].x, y20: pts20[i].y, y60: pts60[i].y });
          }
        }
        let ribbonSvg = '';
        if (validPairs.length > 1) {
          const topPts = validPairs.map((p, idx) => `${idx === 0 ? 'M' : 'L'} ${p.x.toFixed(1)} ${p.y20.toFixed(1)}`).join(' ');
          const bottomPts = validPairs.slice().reverse().map(p => `L ${p.x.toFixed(1)} ${p.y60.toFixed(1)}`).join(' ');
          ribbonSvg = `<defs>
            <linearGradient id="impulse-spread-ribbon" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stop-color="#34d399" stop-opacity="0.18"/>
              <stop offset="100%" stop-color="#fbbf24" stop-opacity="0.08"/>
            </linearGradient>
          </defs>
          <path d="${topPts} ${bottomPts} Z" fill="url(#impulse-spread-ribbon)"/>`;
        }

        // Crossover detection
        let crossoverSvg = '';
        for (let i = 1; i < rows.length; i++) {
          const r0_20 = finiteNumber(rows[i - 1].rollingSum20);
          const r0_60 = finiteNumber(rows[i - 1].rollingSum60);
          const r1_20 = finiteNumber(rows[i].rollingSum20);
          const r1_60 = finiteNumber(rows[i].rollingSum60);
          if (r0_20 !== null && r0_60 !== null && r1_20 !== null && r1_60 !== null) {
            const spread0 = r0_20 - r0_60;
            const spread1 = r1_20 - r1_60;
            if (spread0 * spread1 < 0) {
              const crossX = frame.xScale(i);
              const isBull = spread1 > 0;
              const col = isBull ? '#34d399' : '#fbbf24';
              crossoverSvg += `<line x1="${crossX.toFixed(1)}" y1="${frame.padding.top}" x2="${crossX.toFixed(1)}" y2="${(frame.height - frame.padding.bottom).toFixed(1)}" stroke="${col}" stroke-width="0.8" stroke-dasharray="2 3" opacity="0.45"/>`;
            }
          }
        }

        // End indicators
        let endBadges = '';
        const lastIdx = rows.length - 1;
        if (lastIdx >= 0) {
          const endX = frame.xScale(lastIdx);
          if (pts20[lastIdx]?.y !== null) {
            endBadges += `<circle cx="${endX.toFixed(1)}" cy="${pts20[lastIdx].y.toFixed(1)}" r="3.5" fill="#34d399" stroke="#090d16" stroke-width="1.2"/>`;
          }
          if (pts60[lastIdx]?.y !== null) {
            endBadges += `<circle cx="${endX.toFixed(1)}" cy="${pts60[lastIdx].y.toFixed(1)}" r="3.5" fill="#fbbf24" stroke="#090d16" stroke-width="1.2"/>`;
          }
        }

        let overlay = '';
        if (this.flowHoverIndex !== null && this.flowHoverChart === 'impulse' && this.flowHoverIndex >= 0 && this.flowHoverIndex < rows.length) {
          const hRow = rows[this.flowHoverIndex];
          const x = frame.xScale(this.flowHoverIndex);
          const s20 = finiteNumber(hRow.rollingSum20);
          const s60 = finiteNumber(hRow.rollingSum60);

          let dots = '';
          if (s20 !== null) {
            const y20 = yScale(s20);
            dots += `<circle cx="${x.toFixed(1)}" cy="${y20.toFixed(1)}" r="4.5" fill="#34d399" stroke="#ffffff" stroke-width="1.8"/>`;
          }
          if (s60 !== null) {
            const y60 = yScale(s60);
            dots += `<circle cx="${x.toFixed(1)}" cy="${y60.toFixed(1)}" r="4.5" fill="#fbbf24" stroke="#ffffff" stroke-width="1.8"/>`;
          }

          if (dots) {
            overlay = `<g class="flow-crosshair-group" pointer-events="none">${dots}</g>`;
          }
        }

        return `<svg viewBox="0 0 ${width} ${height}" data-pad-left="${frame.padding.left}" data-pad-right="${frame.padding.right}" data-chart-width="${width}" role="img" aria-label="Multi-horizon 20D and 60D rolling net flow impulse">${ribbonSvg}${grid}${crossoverSvg}<path d="${linePath(pts60)}" fill="none" stroke="${COLORS.warning}" stroke-width="1.8" stroke-dasharray="3 2" opacity="0.9"/><path d="${linePath(pts20)}" fill="none" stroke="${COLORS.positive}" stroke-width="2.2"/>${endBadges}${overlay}${frame.dateTicks}</svg>`;
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
        this._flowWorkbenchBaseKey = '';
        this._flowWorkbenchBaseSvg = '';
        if (this._flowUrlTimer) {
          clearTimeout(this._flowUrlTimer);
          this._flowUrlTimer = null;
        }
        if (this._rangeRaf) {
          cancelAnimationFrame(this._rangeRaf);
          this._rangeRaf = null;
        }
      },

      async _flowBootstrap() {
        this.flowInitialLoading = true;
        this.flowCatalogError = '';
        this.flowManifestError = '';
        try {
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
              if (primary.length !== 150 || featured !== 24 || tickers.size !== 150) {
                throw new Error('Local catalog export failed its 150-instrument, 24-featured integrity checks.');
              }
              return payload;
            });
          const manifestPromise = fetch('data/flows/manifest.json', { signal: manifestController.signal })
            .then(async response => {
              if (!response.ok) throw new Error(`Local coverage manifest request returned ${response.status}.`);
              const payload = await response.json();
              const entries = payload?.etfs && typeof payload.etfs === 'object' ? payload.etfs : {};
              if (payload?.complete !== true || payload?.status !== 'complete' || payload?.counts?.instruments !== 150 || Object.keys(entries).length !== 150 || payload?.source?.network_fetch !== false) {
                throw new Error('Local coverage manifest failed its 150-instrument completeness checks.');
              }
              return payload;
            });
          const alphaPromise = fetch('data/alpha_signals.json')
            .then(async response => response.ok ? response.json() : null)
            .catch(() => null);
          const volumePromise = fetch('data/flows/volume.json')
            .then(async response => response.ok ? response.json() : null)
            .catch(() => null);
          const [catalogResult, manifestResult, alphaResult, volumeResult] = await Promise.allSettled([catalogPromise, manifestPromise, alphaPromise, volumePromise]);
          this.flowBootstrapControllers = [];
          if (this.flowDestroyed) return;
          if (volumeResult.status === 'fulfilled' && volumeResult.value) {
            this.flowVolumeCache = volumeResult.value;
          }
          if (catalogResult.status === 'fulfilled' && catalogResult.value && Array.isArray(catalogResult.value.instruments)) {
            this.flowCatalog = catalogResult.value;
            const CANONICAL_OVERRIDES = {
              SMST: {
                fund_name: 'Defiance Daily Target 2X Short MSTR ETF',
                issuer: 'Defiance ETFs',
                leverage: '-2x',
                leverage_value: -2.0,
                direction: 'inverse',
                category: '9. Selective Benchmark Hedging / Tactical Shorts (Pruned to Key Anchors Only)',
                paired_bull: 'MSTU'
              }
            };
            for (const inst of this.flowCatalog.instruments) {
              if (CANONICAL_OVERRIDES[inst.ticker]) {
                Object.assign(inst, CANONICAL_OVERRIDES[inst.ticker]);
              }
            }
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
        } catch (error) {
          this.flowCatalogError = String(error?.message || error || 'Catalog bootstrap failed.');
        } finally {
          this.flowInitialLoading = false;
        }
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
        const target = event?.currentTarget;
        const isImpulse = Boolean(target?.classList?.contains('flow-impulse-chart') || target?.closest?.('.flow-impulse-chart'));
        const isPrice = Boolean(target?.classList?.contains('flow-price-chart') || target?.closest?.('.flow-price-chart') || target?.getAttribute?.('data-chart') === 'price');
        this.flowHoverChart = isPrice ? 'price' : isImpulse ? 'impulse' : 'main';
        if (event?.type === 'mousemove' && clientX !== null) {
          const svg = target?.querySelector?.('svg') || target;
          const rect = svg?.getBoundingClientRect?.();
          if (rect?.width && rows.length > 1) {
            const rawPadLeft = svg?.getAttribute?.('data-pad-left');
            const rawPadRight = svg?.getAttribute?.('data-pad-right');
            const rawChartW = svg?.getAttribute?.('data-chart-width');
            if (rawPadLeft !== null && rawPadLeft !== undefined && rawChartW) {
              const svgW = Number(rawChartW) || 1;
              const padL = Number(rawPadLeft) || 0;
              const padR = Number(rawPadRight) || 0;
              const screenPadL = (padL / svgW) * rect.width;
              const screenPadR = (padR / svgW) * rect.width;
              const screenChartW = rect.width - screenPadL - screenPadR;
              const relX = clientX - rect.left;
              const frac = Math.max(0, Math.min(1, (relX - screenPadL) / (screenChartW || 1)));
              index = Math.round(frac * (rows.length - 1));
            } else {
              index = Math.round(((clientX - rect.left) / rect.width) * (rows.length - 1));
            }
          }
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
        this.flowHoverChart = null;
        this.flowChartTooltip = { ...this.flowChartTooltip, visible: false };
      },

      flowChartFocus(event) {
        if (!this.flowSelectedRows.length) return;
        const target = event?.currentTarget;
        const isImpulse = Boolean(target?.classList?.contains('flow-impulse-chart') || target?.closest?.('.flow-impulse-chart'));
        const isPrice = Boolean(target?.classList?.contains('flow-price-chart') || target?.closest?.('.flow-price-chart') || target?.getAttribute?.('data-chart') === 'price');
        this.flowHoverChart = isPrice ? 'price' : isImpulse ? 'impulse' : 'main';
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
        const boxHeight = 85;
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
        if (this.flowHoverChart === 'impulse') {
          const s20 = finiteNumber(row.rollingSum20);
          const s60 = finiteNumber(row.rollingSum60);
          const spread = (s20 !== null && s60 !== null) ? s20 - s60 : null;
          const spreadStr = spread !== null ? `${spread >= 0 ? '+' : ''}${formatMoney(spread)}` : '—';
          const line1 = `${this.flowTicker || 'ETF'} · ${row.date || ''}`;
          const line2 = `20D Sum: ${formatMoney(s20)}  ·  60D Sum: ${formatMoney(s60)}`;
          const line3 = `Flow Spread (20D − 60D): ${spreadStr} (${spread !== null && spread >= 0 ? 'Accelerating' : 'Decelerating'})`;
          return `${line1}\n${line2}\n${line3}`;
        }
        const line1 = row.date || '';
        const pricePart = (row.nav !== null && Number.isFinite(row.nav)) ? `Price $${row.nav.toFixed(2)}  ·  ` : '';
        const line2 = `${pricePart}Daily flow ${formatMoney(row.flow)}`;
        const zStr = row.priorOnlyZScore === null ? '—' : `${row.priorOnlyZScore >= 0 ? '+' : ''}${row.priorOnlyZScore.toFixed(2)}σ`;
        const line3 = `20D Mean ${formatMoney(row.rollingMean20)}  ·  10D %ile ${formatPercent(row.percentile10)}  ·  Z ${zStr}`;
        return `${line1}\n${line2}\n${line3}`;
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
          this.flowDataError = 'This ticker is not in the 150-instrument local leveraged/inverse catalog.';
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
          if (typeof window !== 'undefined') setTimeout(() => this._syncRangeInputs(), 0);
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
        this._syncRangeInputs();
      },

      _flowApplyUrlRange() {
        const records = this.flowData?.records || [];
        if (!records.length) {
          this.flowStartIndex = 0;
          this.flowEndIndex = 0;
          this._syncRangeInputs();
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
            this._syncRangeInputs();
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
        this._syncRangeInputs();
        if (writeUrl !== false) this._flowWriteUrl(false);
      },

      flowSetStartIndex(value) {
        const count = this.flowData?.records?.length || 0;
        if (!count) return;
        const minimumWindow = count > 1 ? 2 : 1;
        const next = Math.max(0, Math.min(Math.trunc(Number(value) || 0), this.flowEndIndex - minimumWindow + 1));
        if (next === this.flowStartIndex) return;
        this.flowStartIndex = next;
        this.flowRangePreset = 'custom';
        this.flowEnsureChartTab();
        this._syncRangeInputs();
        this._flowScheduleUrlWrite();
      },

      flowSetEndIndex(value) {
        const count = this.flowData?.records?.length || 0;
        if (!count) return;
        const minimumWindow = count > 1 ? 2 : 1;
        const next = Math.min(count - 1, Math.max(Math.trunc(Number(value) || 0), this.flowStartIndex + minimumWindow - 1));
        if (next === this.flowEndIndex) return;
        this.flowEndIndex = next;
        this.flowRangePreset = 'custom';
        this.flowEnsureChartTab();
        this._syncRangeInputs();
        this._flowScheduleUrlWrite();
      },

      _syncRangeInputs() {
        if (typeof document === 'undefined') return;
        const startInput = document.getElementById('flow-range-start');
        const endInput = document.getElementById('flow-range-end');
        const max = this.flowMaxRecordIndex;
        if (startInput) {
          startInput.max = String(max);
          if (this.flowActiveThumb !== 'start' && String(startInput.value) !== String(this.flowStartIndex)) {
            startInput.value = String(this.flowStartIndex);
          }
        }
        if (endInput) {
          endInput.max = String(max);
          if (this.flowActiveThumb !== 'end' && String(endInput.value) !== String(this.flowEndIndex)) {
            endInput.value = String(this.flowEndIndex);
          }
        }
      },

      _flowScheduleUrlWrite() {
        if (this._flowUrlTimer) clearTimeout(this._flowUrlTimer);
        this._flowUrlTimer = setTimeout(() => {
          this._flowUrlTimer = null;
          this._flowWriteUrl(false);
        }, 150);
      },

      _flowFinishRangeDrag() {
        if (this._flowUrlTimer) {
          clearTimeout(this._flowUrlTimer);
          this._flowUrlTimer = null;
        }
        this._flowWriteUrl(false);
        this._syncRangeInputs();
      },

      flowSetPriceEnabled(value) {
        this.flowPriceEnabled = Boolean(value) && this.flowPriceAvailable;
        if (!this.flowPriceEnabled && this.flowChartTab === 'price') this.flowChartTab = 'daily';
        this.flowEnsureChartTab();
        this._flowWriteUrl(false);
      },

      _flowReadUrl() {
        const state = flowHistoryState(window.location.search);
        if (state.view) this.flowViewMode = state.view;
        this.flowTicker = state.ticker;
        this.flowRangePreset = state.range;
        this.flowPriceEnabled = state.priceEnabled;
        this.flowChartTab = state.chart;
      },

      setFlowViewMode(mode) {
        this.flowViewMode = mode;
        this._flowWriteUrl(false);
      },

      _flowWriteUrl(push) {
        if (typeof window === 'undefined') return;
        const url = new URL(window.location.href);
        url.searchParams.set('tab', 'flows');
        if (this.flowViewMode && this.flowViewMode !== 'studio') url.searchParams.set('view', this.flowViewMode);
        else url.searchParams.delete('view');
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
