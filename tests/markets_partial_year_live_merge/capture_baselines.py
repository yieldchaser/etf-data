"""One-shot baseline capture for preservation tests.

Run this BEFORE the fix lands:
    python -m tests.markets_partial_year_live_merge.capture_baselines

It writes the JSON snapshots used by `test_preservation.py` into
`tests/markets_partial_year_live_merge/baselines/`. The script is idempotent
— rerunning regenerates from the current live JSON / HTML.

The pytest pass-set baseline (`pytest_pass_set.txt`) is captured separately
via the `_capture_pytest_baseline` helper in `test_preservation.py` (it
requires running pytest itself, so it can't live here).
"""
from __future__ import annotations

import json
import re
from pathlib import Path

from .port import build_cache_for_asset

REPO_ROOT = Path(__file__).resolve().parents[2]
MARKET_RETURNS = REPO_ROOT / "docs" / "data" / "market_returns.json"
MARKETS_HTML = REPO_ROOT / "docs" / "markets.html"
BASELINES = Path(__file__).parent / "baselines"

METALS_ASSETS = [
    "copper", "aluminum", "gold", "silver",
    "platinum", "palladium", "nickel", "zinc",
    "iron_ore", "tin", "lead",
]

DUAL_REGISTRY_HOLDOUTS = {"sp500", "nasdaq", "nasdaq100"}


def _load_returns() -> dict:
    return json.loads(MARKET_RETURNS.read_text(encoding="utf-8"))


def _capture_annual_cache(returns: dict) -> dict:
    """Snapshot every {year: {ret, partial, count}} cell for every asset."""
    out: dict[str, dict[str, dict]] = {}
    for aid, blob in returns["assets"].items():
        cache = build_cache_for_asset(blob)
        # JSON keys must be strings — convert int year → str.
        out[aid] = {str(yr): {
            "ret": round(cell["ret"], 8),
            "partial": bool(cell["partial"]),
            "count": int(cell["count"]),
        } for yr, cell in cache.items()}
    return out


def _capture_metals_2026(returns: dict) -> dict:
    """Snapshot the 2026 cell for every metals asset, including the
    dashboard-rendered partial-flag/render-style fields. The render fields
    are derived from the cache the same way `getCellData` in markets.html
    consumes them.
    """
    out: dict[str, dict] = {}
    for aid in METALS_ASSETS:
        if aid not in returns["assets"]:
            continue
        cache = build_cache_for_asset(returns["assets"][aid])
        cell = cache.get(2026)
        if cell is None:
            continue
        # `partial` flag drives `.matrix-cell-partial` class which sets
        # opacity:0.40 + italic. The render style is therefore "dimmed" for
        # any partial cell and "saturated" for a full-year cell.
        render_style = "dimmed" if cell["partial"] else "saturated"
        out[aid] = {
            "ret": round(cell["ret"], 8),
            "partial": bool(cell["partial"]),
            "count": int(cell["count"]),
            "renderStyle": render_style,
            "partialFlag": bool(cell["partial"]),
        }
    return out


def _capture_meta_sources(returns: dict) -> dict[str, str]:
    """Snapshot meta.source for every asset."""
    return {
        aid: blob["meta"]["source"]
        for aid, blob in returns["assets"].items()
    }


def _capture_non_dual_meta_sources(returns: dict) -> dict[str, str]:
    """Snapshot meta.source for every non-dual-registry asset."""
    return {
        aid: src
        for aid, src in _capture_meta_sources(returns).items()
        if aid not in DUAL_REGISTRY_HOLDOUTS
    }


# ── markets.html DOM-text snapshots ─────────────────────────────────────────

# We're operating on a static HTML file (no JS execution), so what we can
# practically pin is the *block of HTML that defines each non-Return-Matrix
# tab*. That HTML is what Alpine.js renders into; any unexpected edit during
# the fix would change the block and the diff would surface.

# Each tab is a top-level `<div x-show="activeTab === '<tab-id>'">…</div>` (see
# markets.html ~lines 290, 804, 944, 1066, 1134, 1306, 1424, 1508, 1657).
NON_MATRIX_TABS = [
    "pricelog",
    "yields",
    "periodic",
    "drawdown",
    "holdingperiod",
    "seasonality",
    "correlation",
    "volatility",
]


def _extract_tab_block(html: str, tab_id: str) -> str:
    """Extract the HTML block for one non-Return-Matrix tab.

    We find the line that starts the block (`<div x-show="activeTab === '<id>'">`)
    and walk forward, tracking <div> open/close depth, until we close the
    outermost div. Returns the block as a single string (whitespace
    normalised).
    """
    needle = f"x-show=\"activeTab === '{tab_id}'\""
    start = html.find(needle)
    if start == -1:
        raise ValueError(f"tab block not found: {tab_id}")
    # back up to the start of the <div ...> tag
    div_start = html.rfind("<div", 0, start)
    if div_start == -1:
        raise ValueError(f"<div for tab {tab_id} not found")

    # walk forward, tracking <div ...> and </div>
    depth = 0
    i = div_start
    open_re = re.compile(r"<div\b", re.IGNORECASE)
    close_re = re.compile(r"</div\s*>", re.IGNORECASE)
    while i < len(html):
        m_open = open_re.search(html, i)
        m_close = close_re.search(html, i)
        if m_close is None:
            raise ValueError(f"unterminated tab block: {tab_id}")
        if m_open is not None and m_open.start() < m_close.start():
            depth += 1
            i = m_open.end()
        else:
            depth -= 1
            i = m_close.end()
            if depth == 0:
                block = html[div_start:i]
                # normalise CRLF + trailing whitespace per line so the
                # snapshot is robust to line-ending churn.
                lines = [ln.rstrip() for ln in block.splitlines()]
                return "\n".join(lines)
    raise ValueError(f"never closed tab block: {tab_id}")


def _capture_markets_html_tabs() -> dict[str, str]:
    html = MARKETS_HTML.read_text(encoding="utf-8")
    return {tab_id: _extract_tab_block(html, tab_id) for tab_id in NON_MATRIX_TABS}


def main() -> None:
    BASELINES.mkdir(parents=True, exist_ok=True)
    returns = _load_returns()

    out = {
        "annual_cache.json": _capture_annual_cache(returns),
        "metals_2026.json": _capture_metals_2026(returns),
        "meta_sources.json": _capture_meta_sources(returns),
        "non_dual_meta_sources.json": _capture_non_dual_meta_sources(returns),
        "markets_html_tabs.json": _capture_markets_html_tabs(),
    }
    for name, content in out.items():
        path = BASELINES / name
        path.write_text(json.dumps(content, indent=2, sort_keys=True), encoding="utf-8")
        print(f"  wrote {path.relative_to(REPO_ROOT)}  ({len(json.dumps(content))} bytes)")


if __name__ == "__main__":
    main()
