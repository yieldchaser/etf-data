"""Regression tests for the Markets design regression (2026-09-26).

Symptom: after 8662b57f2 ("switch Fund Flows to authoritative local
dataset"), the Fund Flows work restyled the ENTIRE markets.html page, not
just the flow tab. The Markets tab bar, the app container width and
padding all changed appearance.

Root cause: docs/runtime-fallback.js sets
`document.documentElement.setAttribute('data-runtime-fallback', 'ready')`
on BOTH code paths -- initialize() (Alpine never booted, the genuine
no-CDN case) AND alpineReady() (Alpine booted fine). runtime-fallback.css
then styles `html[data-runtime-fallback="ready"] #markets-app button`,
`#markets-app` padding/max-width, etc. So the degraded skin applied to
100% of normal visitors.

Measured with a real CSS engine: 8 computed properties on the Markets tab
buttons and the app container differ when the attribute is present.

The fallback must stay (tests/test_flow_ui.py pins it as the no-CDN
path), so the fix SCOPES the skin to the real fallback case instead of
deleting it. Tab labels, tab count and Markets CSS selectors are
unchanged by the flow work and are pinned here so they cannot drift.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
FALLBACK_JS = ROOT / "docs" / "runtime-fallback.js"
FALLBACK_CSS = ROOT / "docs" / "runtime-fallback.css"
MARKETS_HTML = ROOT / "docs" / "markets.html"

# The 10 Markets tabs, as shipped. Labels must not drift.
EXPECTED_TABS = {
    "matrix": "Return Matrix",
    "pricelog": "Price Log",
    "yields": "Yield History",
    "periodic": "Periodic Table",
    "drawdown": "Drawdowns",
    "holdingperiod": "Hold Lab",
    "seasonality": "Seasonality",
    "correlation": "Correlation",
    "volatility": "Volatility",
    "flows": "Fund Flows",
}

_BUTTON = re.compile(
    r"<button\b[^>]*?(?:@click=\"(?:activeTab = '(\w+)'|setMarketsTab\('(\w+)'\)))\"[^>]*>"
    r"(.*?)</button>", re.S)
_PANEL = re.compile(r"activeTab === '(\w+)'")


def _tab_map() -> dict[str, str]:
    html = MARKETS_HTML.read_text(encoding="utf-8")
    out = {}
    for m in _BUTTON.finditer(html):
        key = m.group(1) or m.group(2)
        out[key] = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", m.group(3))).strip()
    return out


def _style_block() -> str:
    html = MARKETS_HTML.read_text(encoding="utf-8")
    m = re.search(r"<style>(.*?)</style>", html, re.S)
    return m.group(1) if m else ""


def test_markets_tabs_and_labels_are_unchanged_by_flow_work():
    """The flow work must not have altered the Markets tab set."""
    assert _tab_map() == EXPECTED_TABS


def test_every_tab_has_a_panel():
    panels = set(_PANEL.findall(MARKETS_HTML.read_text(encoding="utf-8")))
    assert set(EXPECTED_TABS) <= panels


# ── The actual regression ──────────────────────────────────────────────

def test_fallback_attribute_only_set_when_alpine_is_absent():
    """The degraded skin must NOT apply to a normal Alpine-booted visit.

    alpineReady() exists to hide the fallback panel once Alpine owns the
    page. It must not leave the styling hook behind, because
    runtime-fallback.css keys the Markets-wide overrides off that same
    attribute.
    """
    js = FALLBACK_JS.read_text(encoding="utf-8")

    # The attribute may be set exactly once, inside initialize() -- the
    # path that only runs when Alpine never initialised.
    occurrences = js.count("setAttribute('data-runtime-fallback', 'ready')")
    assert occurrences == 1, (
        "data-runtime-fallback must be set on exactly one path; the "
        f"Alpine-booted path must not set it (found {occurrences})"
    )

    alpine_ready = re.search(
        r"function alpineReady\(\)\s*\{(.*?)\n  \}", js, re.S)
    assert alpine_ready, "alpineReady() must still exist to hide the fallback panel"
    assert "setAttribute" not in alpine_ready.group(1), (
        "alpineReady() must not set data-runtime-fallback -- that is what "
        "restyles the Markets tabs for every visitor"
    )
    assert "panel.hidden = true" in alpine_ready.group(1), (
        "alpineReady() must still hide the fallback panel when Alpine owns the page"
    )


def test_markets_wide_css_is_gated_on_the_fallback_body_class():
    """The #markets-app overrides must require an explicit fallback marker.

    Today they key off `html[data-runtime-fallback="ready"]`, which is the
    same attribute the degraded path sets. A distinct body-level marker
    (only ever set by the degraded path) makes the intent unambiguous and
    keeps the styles from ever reaching a normal visit.
    """
    css = FALLBACK_CSS.read_text(encoding="utf-8")
    markets_rules = re.findall(r"html\[data-runtime-fallback[^\{]*\{", css)
    assert not markets_rules, (
        "Markets-wide CSS must not key off html[data-runtime-fallback]; "
        f"found {len(markets_rules)} rule(s). Gate on the explicit "
        "body.flow-runtime-fallback-active marker instead."
    )
    assert "flow-runtime-fallback-active" in css, (
        "expected a dedicated body marker for the degraded skin"
    )
    assert "body.flow-runtime-fallback-active #markets-app" in css


def test_fallback_panel_still_present_and_hidden_by_default():
    """The no-CDN fallback itself must keep working."""
    html = MARKETS_HTML.read_text(encoding="utf-8")
    js = FALLBACK_JS.read_text(encoding="utf-8")
    assert 'id="flow-runtime-fallback"' in html
    assert "href=\"runtime-fallback.css\"" in html
    assert "src=\"runtime-fallback.js\"" in html
    assert "removeAttribute('x-cloak')" in js
    assert "history.replaceState" in js


# ── Cumulative flows vs the range slider ───────────────────────────────

def test_range_sliders_do_not_dereference_null_flow_data():
    """`flowData` starts null; the sliders sit outside the x-show guard.

    Binding :max to `Math.max(0, flowData.records.length - 1)` threw
    "Cannot read properties of null (reading 'records')" on every render
    during a ticker switch or reload, which is what left the cumulative
    panel feeling frozen and broke the slider drag.
    """
    html = MARKETS_HTML.read_text(encoding="utf-8")
    offenders = [
        f"line {i}: {line.strip()[:120]}"
        for i, line in enumerate(html.splitlines(), 1)
        if "flowData.records" in line
        and "flowData?.records" not in line
        and not re.search(r"flowData\s*&&", line)   # short-circuit is safe
        and "flowData ?" not in line                # ternary is safe
    ]
    assert not offenders, (
        "unguarded flowData deref in markup:\n" + "\n".join(offenders))


def test_max_record_index_accessor_is_optional_chained():
    js = (ROOT / "docs" / "flow-ui.js").read_text(encoding="utf-8")
    assert "get flowMaxRecordIndex()" in js
    m = re.search(r"get flowMaxRecordIndex\(\)\s*\{(.*?)\n      \}", js, re.S)
    assert m, "flowMaxRecordIndex accessor not found"
    assert "?." in m.group(1), "accessor must use optional chaining"
    assert ':max="flowMaxRecordIndex"' in MARKETS_HTML.read_text(encoding="utf-8")


def test_cumulative_is_window_scoped_not_global():
    """Cumulative must be summed over the SELECTED window, from zero.

    Pinned because the KPI label promises "Selected-window cumulative" and
    the slider is the only thing that changes the window.
    """
    js = (ROOT / "docs" / "flow-ui.js").read_text(encoding="utf-8")
    m = re.search(r"function selectWindowMetrics\(.*?\n  \}", js, re.S)
    assert m, "selectWindowMetrics not found"
    body = m.group(0)
    assert "rows.slice(safeStart, safeEnd + 1)" in body, (
        "cumulative must be computed from the selected slice only")
    assert "cumulative = (cumulative === null ? 0 : cumulative) + value" in body, (
        "cumulative must start from a zero baseline inside the window")
    # The window key must include both indices or the cache never invalidates.
    assert "${this.flowStartIndex}:${this.flowEndIndex}" in js

