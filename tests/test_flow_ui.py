import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CATALOG_JSON = ROOT / "docs" / "data" / "flows" / "catalog.json"
MANIFEST_JSON = ROOT / "docs" / "data" / "flows" / "manifest.json"
MARKET_RETURNS_JSON = ROOT / "docs" / "data" / "market_returns.json"
MARKETS_HTML = ROOT / "docs" / "markets.html"
FLOW_JS = ROOT / "docs" / "flow-ui.js"
FLOW_CSS = ROOT / "docs" / "flow-ui.css"
FALLBACK_JS = ROOT / "docs" / "runtime-fallback.js"
FALLBACK_CSS = ROOT / "docs" / "runtime-fallback.css"


def active_flow_markup() -> str:
    text = MARKETS_HTML.read_text(encoding="utf-8")
    start = text.index('<section class="flow-research"')
    end = text.index('<!-- FOOTER -->', start)
    return text[start:end]


def test_exported_catalog_matches_local_dataset_cardinality():
    exported = json.loads(CATALOG_JSON.read_text(encoding="utf-8"))
    assert exported["schema_version"] == 1
    assert exported["catalog_version"] == "local-authoritative-117-v1"
    assert exported["counts"] == {"featured": 24, "instruments": 117, "rows": 170392}
    assert "watch_tier" not in exported
    assert exported["source"]["network_fetch"] is False
    assert exported["source"]["name"] == "Local authoritative dataset"
    assert exported["source"]["provider"] == "Trackinsight"
    assert exported["source"]["mode"] == "historical_local"
    assert len(exported["instruments"]) == 117
    assert len({row["ticker"] for row in exported["instruments"]}) == 117
    assert len(exported["featured_tickers"]) == 24
    featured = {row["ticker"] for row in exported["instruments"] if row["featured"]}
    assert featured == set(exported["featured_tickers"])
    assert all(row["source"] == "Local dataset" for row in exported["instruments"])
    assert all(row["source_provider"] == "Trackinsight" for row in exported["instruments"])
    assert all(row["source_mode"] == "historical_local" for row in exported["instruments"])
    assert "watch" not in exported["category_counts"]
    assert sum(row["row_count"] for row in exported["instruments"]) == 170392


def test_manifest_agrees_with_catalog_and_forbids_network_fetch():
    catalog = json.loads(CATALOG_JSON.read_text(encoding="utf-8"))
    manifest = json.loads(MANIFEST_JSON.read_text(encoding="utf-8"))
    assert manifest["schema_version"] == 2
    assert manifest["complete"] is True
    assert manifest["status"] == "complete"
    assert manifest["counts"] == {"featured": 24, "files": 117, "instruments": 117, "rows": 170392}
    assert manifest["source"]["network_fetch"] is False
    assert len(manifest["etfs"]) == 117
    assert manifest["counts"]["instruments"] == catalog["counts"]["instruments"]
    assert manifest["counts"]["rows"] == catalog["counts"]["rows"]
    assert manifest["counts"]["featured"] == catalog["counts"]["featured"]
    assert set(manifest["etfs"]) == {row["ticker"] for row in catalog["instruments"]}


def test_flow_ui_uses_only_the_generated_catalog():
    markup = active_flow_markup()
    javascript = FLOW_JS.read_text(encoding="utf-8")
    source = f"{MARKETS_HTML.read_text(encoding='utf-8')}\n{javascript}"
    assert "data/flows/catalog.json" in source
    assert "primary.length !== 117" in javascript
    assert "featured !== 24" in javascript
    assert "tickers.size !== 117" in javascript
    assert "watchTier" not in javascript
    assert "watch_tier" not in source
    assert "etf_search_index" not in source
    assert "15,000" not in source
    assert "15K" not in source
    assert "github_pat" not in source.lower()
    assert "api.github.com" not in source.lower()
    assert "localstorage" not in source.lower()
    assert "workflow_dispatch" not in source
    assert "cloud" not in source.lower()
    assert "TQQQ" not in source


def test_flow_ui_uses_factual_labels_and_explicit_states():
    source = f"{MARKETS_HTML.read_text(encoding='utf-8')}\n{FLOW_JS.read_text(encoding='utf-8')}".lower()
    for phrase in [
        "etf estimated net flow",
        "source-reported aggregate net flow",
        "trailing 20 sessions",
        "available source observations",
        "pending",
        "stale",
        "legacy",
        "unavailable",
        "empty",
    ]:
        assert phrase in source
    for phrase in [
        "institutional",
        "retail",
        "fomo",
        "capitulation",
        "verified",
        "creations",
        "statistically significant",
    ]:
        assert phrase not in source


def test_flow_ui_has_one_shared_native_range_group():
    markup = active_flow_markup()
    assert markup.count("<fieldset") == 1
    assert markup.count('type="range"') == 2
    assert 'id="flow-range-start"' in markup
    assert 'id="flow-range-end"' in markup
    assert "flowSetChartPreset" not in markup
    assert "flowSyncAllCharts" not in markup
    assert "min-width: 600" not in markup
    assert "overflow-x: auto" not in markup


def test_flow_ui_accessibility_contract():
    markup = active_flow_markup()
    javascript = FLOW_JS.read_text(encoding="utf-8")
    assert 'role="combobox"' in markup
    assert 'aria-autocomplete="list"' in markup
    assert 'aria-controls="flow-catalog-results"' in markup
    assert ':aria-expanded=' in markup
    assert ':aria-activedescendant=' in markup
    assert 'role="listbox"' in markup
    assert 'role="option"' in markup
    assert 'aria-live="polite"' in markup
    assert "<table" in markup and "<caption" in markup
    assert "aria-labelledby=" in javascript
    assert "<title id=" in javascript
    assert "<desc id=" in javascript
    assert "flow-window-metrics" not in javascript
    assert "marketHistoryTab" in javascript
    assert "reconcileFlowHistory" in javascript
    assert "flow-historychange" in javascript
    assert "AbortController" in javascript
    assert "flowRequestId" in javascript
    assert "FLOW_CACHE" in javascript
    assert "revision=" in javascript


def test_flow_ui_responsive_surface_uses_readable_type():
    css = FLOW_CSS.read_text(encoding="utf-8")
    assert ".flow-research {" in css
    assert "font-size: 0.8125rem" in css
    assert "font-variant-numeric: tabular-nums" in css
    assert "grid-template-columns: repeat(2, minmax(0, 1fr))" in css
    assert "@media (max-width: 640px)" in css
    assert "box-shadow: 0 0" not in css
    assert "linear-gradient(90deg" not in css


def test_parent_markets_history_listener_reconciles_flow_navigation():
    html = MARKETS_HTML.read_text(encoding="utf-8")
    javascript = FLOW_JS.read_text(encoding="utf-8")
    assert 'id="markets-app"' in html
    assert "_handleMarketsPopState()" in html
    assert "window.addEventListener('popstate', this._marketsPopstateHandler)" in html
    assert "window.removeEventListener('popstate', this._marketsPopstateHandler)" in html
    assert "window.dispatchEvent(new CustomEvent('flow-historychange'" in html
    assert "window.addEventListener('flow-historychange', this.flowHistoryHandler)" in javascript
    assert "window.removeEventListener('flow-historychange', this.flowHistoryHandler)" in javascript
    assert "window.addEventListener('popstate'" not in javascript
    assert "for (const controller of this.flowBootstrapControllers) controller.abort()" in javascript
    assert "if (this.flowDestroyed) return" in javascript


def test_flow_state_is_url_owned_across_tab_remount():
    html = MARKETS_HTML.read_text(encoding="utf-8")
    javascript = FLOW_JS.read_text(encoding="utf-8")
    assert "chart: CHART_TABS.some" in javascript
    assert "app.flowChartTab = next.chart" in javascript
    assert "flowChartTab: state.chart" not in javascript
    assert "url.searchParams.set('chart', this.flowChartTab)" in javascript
    set_tab = html[html.index("setMarketsTab(tab, options)"):html.index("_handleMarketsPopState()")]
    assert "searchParams.delete('flow')" not in set_tab
    assert "searchParams.delete('chart')" not in set_tab


def test_mobile_flow_and_fallback_css_constrain_wide_content():
    flow_css = FLOW_CSS.read_text(encoding="utf-8")
    fallback_css = FALLBACK_CSS.read_text(encoding="utf-8")
    for css in (flow_css, fallback_css):
        assert "min-width: 0" in css
        assert "max-width: 100%" in css
        assert "overflow-x: auto" in css
    assert "box-sizing: border-box" in fallback_css
    assert "#markets-app {" in fallback_css
    assert "width: 100%" in fallback_css
    assert "width: max-content" in flow_css
    assert "width: max-content" in fallback_css
    assert "grid-template-columns: 1fr" in fallback_css


def test_active_pages_no_longer_claim_uncurated_15k_flows():
    for path in (ROOT / "docs").glob("*.html"):
        text = path.read_text(encoding="utf-8")
        assert "15,000+" not in text
        assert "Institutional capital flows" not in text
    index = (ROOT / "docs" / "index.html").read_text(encoding="utf-8")
    assert "markets.html?tab=flows" in index
    assert "117 local leveraged and inverse instruments" in index
    assert "15,000" not in index
    assert "15K" not in index


def test_browser_source_counts_match_current_markets_contracts():
    html = MARKETS_HTML.read_text(encoding="utf-8")
    market_returns = json.loads(MARKET_RETURNS_JSON.read_text(encoding="utf-8"))
    assert html.count("data-markets-tab=") == 10
    assert len(market_returns["assets"]) == 35


def test_local_runtime_fallback_is_available_without_cdn():
    html = MARKETS_HTML.read_text(encoding="utf-8")
    javascript = FALLBACK_JS.read_text(encoding="utf-8")
    css = FALLBACK_CSS.read_text(encoding="utf-8")
    assert 'href="runtime-fallback.css"' in html
    assert 'src="runtime-fallback.js"' in html
    assert 'defer data-runtime-optional="alpine"' in html
    assert 'defer data-runtime-optional="tailwind"' in html
    assert 'id="flow-runtime-fallback"' in html
    assert "data/flows/catalog.json" in javascript
    assert "data/flows/manifest.json" in javascript
    assert "curated_manifest" not in javascript
    assert "removeAttribute('x-cloak')" in javascript
    assert "history.replaceState" in javascript
    assert "history[push ? 'pushState' : 'replaceState']" in javascript
    assert "font-size: 0.875rem" in css
    assert "Trackinsight" not in javascript


def test_flow_chart_tabs_tooltips_and_accessible_table_are_interactive():
    markup = active_flow_markup()
    javascript = FLOW_JS.read_text(encoding="utf-8")
    assert markup.count('role="tab"') == 1
    assert 'role="tablist"' in markup
    assert markup.count('role="tabpanel"') == 5
    assert "@keydown=\"flowChartTabKeydown($event, index)\"" in markup
    assert ':aria-disabled="tab.key' in markup
    assert ':disabled="tab.key' not in markup
    assert "flow-chart-message" in markup
    assert "flowSetChartTab(tab.key, true)" in markup
    assert "flowChartPointerMove($event)" in markup
    assert "flowChartFocus()" in markup
    assert 'class="flow-chart-tooltip"' in markup
    assert 'role="status"' in markup
    assert "Accessible data table for the selected window" in markup
    assert "flowChartTooltipText" in javascript
    assert "flowChartTabKeydown" in javascript
    assert markup.count('class="flow-chart"') == 5


def test_main_tabs_and_flow_handlers_use_safe_history_transitions():
    html = MARKETS_HTML.read_text(encoding="utf-8")
    javascript = FLOW_JS.read_text(encoding="utf-8")
    assert html.count("data-markets-tab=") == 10
    assert "setMarketsTab(tab, options)" in html
    assert "window.history[settings.push ? 'pushState' : 'replaceState']" in html
    assert "marketHistoryTab(window.location.search)" in html
    assert "flow-historychange" in html
    assert "const method = push ? 'pushState' : 'replaceState'" in javascript
    assert "if (writeUrl !== false) this._flowWriteUrl(false)" in javascript


def test_flow_contrast_tokens_and_type_scale_meet_readability_floor():
    css = FLOW_CSS.read_text(encoding="utf-8")
    assert "--flow-muted: #b1bdca" in css
    assert "--flow-subtle: #8d9aaa" in css
    assert "--flow-accent: #8bc7e3" in css
    javascript = FLOW_JS.read_text(encoding="utf-8")
    assert "subtle: '#8d9aaa'" in javascript
    assert "muted: '#b1bdca'" in javascript
    assert "font-size: 9px" not in css
    assert "font-size: 10px" not in css
    assert "font-size: 0.8125rem" in css
    assert "flow-chart-tab:focus-visible" in css
    assert "flow-chart-tooltip" in css


def test_search_and_percentile_ui_expose_complete_primary_scope_and_both_axes():
    html = MARKETS_HTML.read_text(encoding="utf-8")
    javascript = FLOW_JS.read_text(encoding="utf-8")
    active = html[html.index('<section class="flow-research"'):html.index("<!-- FOOTER -->")]
    assert '<option value="all">117 local instruments</option>' in active
    assert '<option value="featured">24 featured instruments</option>' in active
    assert '<option value="primary">All 117 instruments</option>' in active
    assert '<option value="watch"' not in active
    assert "watch_tier" not in active
    assert "results.slice(0, 24)" not in javascript
    assert "else if (!item.featured" not in javascript
    assert "left axis shows complete trailing 10-observation" in javascript
    assert "right axis shows the tie-aware empirical percentile" in javascript
