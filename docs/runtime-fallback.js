(function (window, document) {
  'use strict';

  var root = document.documentElement;
  var app = document.getElementById('markets-app');
  var panel = document.getElementById('flow-runtime-fallback');
  if (!app || !panel) return;

  var status = document.getElementById('flow-runtime-status');
  var summary = document.getElementById('flow-runtime-summary');
  var tickerSelect = document.getElementById('flow-runtime-ticker');
  var rangeSelect = document.getElementById('flow-runtime-range');
  var tableBody = document.getElementById('flow-runtime-table-body');
  var catalog = null;
  var catalogPromise = null;
  var manifest = null;
  var itemMap = {};
  var current = null;
  var currentTicker = '';
  var currentRange = '1y';
  var activeTab = 'matrix';
  var tabsBound = false;
  var controller = null;

  function text(value) {
    return value === null || value === undefined || value === '' ? '—' : String(value);
  }

  function money(value) {
    if (value === null || value === undefined || value === '' || !Number.isFinite(Number(value))) return '—';
    var number = Number(value);
    var sign = number > 0 ? '+' : number < 0 ? '−' : '';
    var absolute = Math.abs(number);
    if (absolute >= 1e9) return sign + '$' + (absolute / 1e9).toFixed(1) + 'B';
    if (absolute >= 1e6) return sign + '$' + (absolute / 1e6).toFixed(1) + 'M';
    if (absolute >= 1e3) return sign + '$' + (absolute / 1e3).toFixed(0) + 'K';
    return sign + '$' + absolute.toFixed(0);
  }

  function setStatus(message) {
    if (status) status.textContent = message;
  }

  function queryState() {
    var params = new URLSearchParams(window.location.search);
    var requestedTab = params.get('tab');
    var tabs = ['matrix', 'pricelog', 'yields', 'periodic', 'drawdown', 'holdingperiod', 'seasonality', 'correlation', 'volatility', 'flows'];
    return {
      tab: tabs.indexOf(requestedTab) >= 0 ? requestedTab : 'matrix',
      ticker: String(params.get('flow') || params.get('ticker') || '').toUpperCase(),
      range: params.get('range') || '1y'
    };
  }

  function writeMainUrl(tab) {
    var url = new URL(window.location.href);
    url.searchParams.set('tab', tab);
    window.history.replaceState({}, '', url);
  }

  function writeFlowUrl(ticker, range, push) {
    var url = new URL(window.location.href);
    url.searchParams.set('tab', 'flows');
    url.searchParams.set('flow', ticker);
    url.searchParams.set('range', range);
    url.searchParams.delete('ticker');
    url.searchParams.delete('start');
    url.searchParams.delete('end');
    url.searchParams.delete('price');
    window.history[push ? 'pushState' : 'replaceState']({}, '', url);
  }

  function applyTab(tab) {
    activeTab = tab;
    document.querySelectorAll('#markets-app [x-show]').forEach(function (node) {
      var expression = node.getAttribute('x-show') || '';
      var match = expression.match(/^activeTab === '([^']+)'/);
      if (match) node.style.display = match[1] === tab ? '' : 'none';
    });
    document.querySelectorAll('[data-markets-tab]').forEach(function (button) {
      var selected = button.getAttribute('data-markets-tab') === tab;
      button.setAttribute('aria-current', selected ? 'page' : 'false');
      button.classList.toggle('tab-active', selected);
    });
    panel.hidden = tab !== 'flows';
  }

  function bindTabs() {
    if (tabsBound) return;
    tabsBound = true;
    document.querySelectorAll('[data-markets-tab]').forEach(function (button) {
      button.addEventListener('click', function () {
        if (alpineInitialized()) return;
        var tab = button.getAttribute('data-markets-tab');
        applyTab(tab);
        writeMainUrl(tab);
        if (tab === 'flows') {
           if (catalog) loadTicker(queryState().ticker || currentTicker || 'TQQQ', false);
          else loadCatalog(queryState()).catch(function () {});
        }
      });
    });
    tickerSelect.addEventListener('change', function () {
      loadTicker(tickerSelect.value, true);
    });
    rangeSelect.addEventListener('change', function () {
      currentRange = rangeSelect.value;
      if (currentTicker) {
        writeFlowUrl(currentTicker, currentRange, false);
        render();
      }
    });
    window.addEventListener('popstate', function () {
      if (window.Alpine) return;
      var state = queryState();
      applyTab(state.tab);
      if (state.tab === 'flows') {
         if (catalog) loadTicker(state.ticker || currentTicker || 'TQQQ', false, state.range);
        else loadCatalog(state).catch(function () {});
      }
    });
  }

  function normalizedPayload(payload, item) {
    if (window.FlowResearch && typeof window.FlowResearch.normalizeFlowPayload === 'function') {
      return window.FlowResearch.normalizeFlowPayload(payload, item, manifest && manifest.etfs && manifest.etfs[item.ticker]);
    }
    return {
      ticker: item.ticker,
       state: payload.data_status === 'stale' ? 'stale' : 'available',
      sourceAsOf: payload.source_asof || payload.updated || '',
      records: (payload.data || []).map(function (row) {
        return {
          date: row.date,
          flow: row.usd_flow === undefined ? null : row.usd_flow,
          nav: row.nav === undefined ? null : row.nav
        };
      })
    };
  }

  function render() {
    if (!current) return;
    var records = current.records || [];
    var count = currentRange === 'max' ? records.length : ({ '3m': 66, '6m': 130, '1y': 252, '3y': 756, '5y': 1260 }[currentRange] || 252);
    var selected = records.slice(Math.max(0, records.length - count));
    var available = manifest && manifest.etfs && manifest.etfs[currentTicker];
    var coverage = available ? (available.records || 0) : records.length;
    if (summary) {
      summary.textContent = currentTicker + ' · ' + records.length + ' available source observations · ' + (available ? available.data_status || 'available' : current.state) + ' · ' + (current.sourceAsOf ? 'source as of ' + current.sourceAsOf : 'source date unavailable') + (coverage ? ' · manifest records ' + coverage : '');
    }
    if (!tableBody) return;
    tableBody.replaceChildren();
    if (!selected.length) {
      var emptyRow = document.createElement('tr');
      var emptyCell = document.createElement('td');
      emptyCell.colSpan = 3;
      emptyCell.textContent = 'No source observations are available for this window.';
      emptyRow.appendChild(emptyCell);
      tableBody.appendChild(emptyRow);
      return;
    }
    selected.forEach(function (row) {
      var tr = document.createElement('tr');
      var date = document.createElement('td');
      var flow = document.createElement('td');
      var nav = document.createElement('td');
      date.textContent = row.date;
      flow.textContent = money(row.flow);
      nav.textContent = text(row.nav);
      tr.appendChild(date);
      tr.appendChild(flow);
      tr.appendChild(nav);
      tableBody.appendChild(tr);
    });
  }

  function loadTicker(ticker, push, requestedRange) {
    if (!catalog) return;
    var normalizedTicker = String(ticker || '').toUpperCase();
    var item = itemMap[normalizedTicker];
    if (!item) normalizedTicker = catalog.instruments[0].ticker;
    item = itemMap[normalizedTicker];
    currentTicker = normalizedTicker;
    currentRange = requestedRange || rangeSelect.value || '1y';
    rangeSelect.value = currentRange;
    tickerSelect.value = normalizedTicker;
    if (controller) controller.abort();
    controller = typeof AbortController === 'function' ? new AbortController() : null;
    setStatus('Loading ' + normalizedTicker + ' from its local static file…');
    var request = fetch('data/flows/' + encodeURIComponent(normalizedTicker) + '.json', controller ? { signal: controller.signal } : undefined)
      .then(function (response) {
        if (!response.ok) throw new Error('Static history returned HTTP ' + response.status + '.');
        return response.json();
      })
      .then(function (payload) {
        current = normalizedPayload(payload, item);
        if (push) writeFlowUrl(normalizedTicker, currentRange, true);
        setStatus('Local Fund Flows fallback ready · no source refresh is triggered.');
        render();
      })
      .catch(function (error) {
        if (error && error.name === 'AbortError') return;
        current = null;
        setStatus(error && error.message ? error.message : 'Local history is unavailable.');
        if (summary) summary.textContent = 'No local history is available for ' + normalizedTicker + '.';
        if (tableBody) tableBody.replaceChildren();
      });
    return request;
  }

  function loadCatalog(state) {
    if (catalog) return Promise.resolve();
    if (catalogPromise) return catalogPromise;
    catalogPromise = Promise.all([
      fetch('data/flows/catalog.json').then(function (response) {
        if (!response.ok) throw new Error('Catalog returned HTTP ' + response.status + '.');
        return response.json();
      }),
       fetch('data/flows/manifest.json').then(function (response) {
        if (!response.ok) throw new Error('Coverage manifest returned HTTP ' + response.status + '.');
        return response.json();
      })
    ]).then(function (results) {
      catalog = results[0];
      manifest = results[1];
       if (!catalog || !Array.isArray(catalog.instruments) || catalog.instruments.length !== 117) throw new Error('Local catalog integrity check failed.');
       if (!manifest || manifest.complete !== true || manifest.status !== 'complete' || !manifest.etfs || Object.keys(manifest.etfs).length !== 117) throw new Error('Local manifest integrity check failed.');
       if (catalog.source && catalog.source.network_fetch !== false) throw new Error('Local catalog is not marked as non-networked.');
       var uniqueTickers = new Set(catalog.instruments.map(function (item) { return item.ticker; }));
       if (uniqueTickers.size !== 117 || catalog.instruments.filter(function (item) { return item.featured; }).length !== 24) throw new Error('Local catalog cardinality check failed.');
       itemMap = Object.fromEntries(catalog.instruments.map(function (item) { return [item.ticker, item]; }));
      tickerSelect.replaceChildren();
      tickerSelect.disabled = false;
      catalog.instruments.forEach(function (item) {
        var option = document.createElement('option');
        option.value = item.ticker;
        option.textContent = item.ticker;
        option.title = item.fund_name;
        option.setAttribute('aria-label', item.ticker + ' ' + item.fund_name);
        tickerSelect.appendChild(option);
      });
       setStatus('117-instrument local catalog loaded · local manifest: ' + (manifest.status || 'unknown') + '.');
       var ticker = state && state.ticker && itemMap[state.ticker] ? state.ticker : 'TQQQ';
      return loadTicker(ticker, false, state && state.range);
    }).catch(function (error) {
      catalogPromise = null;
      setStatus(error && error.message ? error.message : 'Local catalog is unavailable.');
      throw error;
    });
    return catalogPromise;
  }

  function initialize() {
    bindTabs();
    var state = queryState();
    applyTab(state.tab);
    root.setAttribute('data-runtime-fallback', 'ready');
    document.querySelectorAll('[x-cloak]').forEach(function (node) {
      node.removeAttribute('x-cloak');
    });
    if (state.tab !== 'flows') {
      panel.hidden = true;
      return;
    }
    loadCatalog(state).catch(function () {});
  }

  function alpineReady() {
    root.setAttribute('data-runtime-fallback', 'ready');
    panel.hidden = true;
  }

  function alpineInitialized() {
    return Boolean(window.Alpine && app._x_dataStack && app._x_dataStack.length);
  }

  window.addEventListener('alpine:init', alpineReady);
  document.addEventListener('alpine:init', alpineReady);
  window.setTimeout(function () {
    if (alpineInitialized()) alpineReady();
  }, 0);
  var alpinePollAttempts = 0;
  var alpinePoll = window.setInterval(function () {
    alpinePollAttempts += 1;
    if (alpineInitialized()) {
      window.clearInterval(alpinePoll);
      alpineReady();
    } else if (alpinePollAttempts >= 40) {
      window.clearInterval(alpinePoll);
    }
  }, 250);
  if (alpineInitialized()) alpineReady();
  else initialize();
})(window, document);
