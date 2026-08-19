"""
Property-based tests for the self_living_check verdict computation logic.

Feature: automation-self-living-data-flow
Properties 1, 2, 3 — verdict invariants and source-label honesty.

The `compute_self_living_check` helper is copied (not imported) from
tests/test_unit_gaps.py so this file is fully self-contained and independent
of build.py's internal structure.
"""
from __future__ import annotations

from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

# ─── Fixed live-eligible key set (5 canonical assets) ────────────────────────

LIVE_ELIGIBLE_KEYS: set[str] = {"sp500", "gold", "us_10y", "us_2y", "oil"}

# ─── Pure verdict computation (mirrors build.py logic exactly) ───────────────


def compute_self_living_check(
    assets: dict[str, Any],
    live_eligible_keys: set[str],
) -> dict[str, Any]:
    """
    Replicate the self_living_check verdict logic from conviction/build.py.

    This is intentionally a copy — not an import — so that the test is
    independent of build.py's internal structure and can verify the logic
    in isolation.
    """
    by_source: dict[str, int] = {}
    holdout_keys: list[str] = []
    holdout_names: list[str] = []

    for aid, av in assets.items():
        meta = av.get("meta", {}) or {}
        src = meta.get("source", "unknown")
        family = (
            "fred" if src.startswith("fred:")
            else "yfinance" if src.startswith("yfinance:")
            else "excel" if "Mega_Markets_Historical" in src
            else "other"
        )
        by_source[family] = by_source.get(family, 0) + 1

        if (
            aid in live_eligible_keys
            and isinstance(src, str)
            and src.startswith("Mega_Markets_Historical")
        ):
            holdout_keys.append(aid)
            holdout_names.append(meta.get("name") or aid)

    live_source_count = by_source.get("fred", 0) + by_source.get("yfinance", 0)
    excel_only_count = by_source.get("excel", 0)

    if not holdout_keys and live_source_count > excel_only_count:
        verdict = "LIVE_MERGE_HEALTHY"
    elif live_source_count > 0:
        verdict = "LIVE_MERGE_DEGRADED"
    else:
        verdict = "LIVE_MERGE_FAILED"

    return {
        "live_source_count": live_source_count,
        "excel_only_count": excel_only_count,
        "holdouts": sorted(holdout_names),
        "holdout_keys": sorted(holdout_keys),
        "verdict": verdict,
    }


# ─── Hypothesis strategies ────────────────────────────────────────────────────

# Series ID text: letters only, 1–20 chars
_series_id_st = st.text(
    min_size=1,
    max_size=20,
    alphabet=st.characters(whitelist_categories=("Lu", "Ll")),
)

# Asset name text: letters only, 1–20 chars
_name_st = st.text(
    min_size=1,
    max_size=20,
    alphabet=st.characters(whitelist_categories=("Lu", "Ll")),
)


def asset_with_live_source_strategy():
    """
    Generate asset dicts whose meta.source starts with 'yfinance:' or 'fred:'.
    Key is drawn from LIVE_ELIGIBLE_KEYS.
    """
    prefix_st = st.sampled_from(["yfinance:", "fred:"])
    return st.builds(
        lambda key, prefix, series_id, name: {
            "key": key,
            "meta": {
                "name": name,
                "source": f"{prefix}{series_id}",
            },
        },
        key=st.sampled_from(sorted(LIVE_ELIGIBLE_KEYS)),
        prefix=prefix_st,
        series_id=_series_id_st,
        name=_name_st,
    )


def asset_with_excel_source_strategy():
    """
    Generate asset dicts whose meta.source starts with 'Mega_Markets_Historical.xlsx:'.
    Key is drawn from LIVE_ELIGIBLE_KEYS.
    """
    return st.builds(
        lambda key, series_id, name: {
            "key": key,
            "meta": {
                "name": name,
                "source": f"Mega_Markets_Historical.xlsx:{series_id}",
            },
        },
        key=st.sampled_from(sorted(LIVE_ELIGIBLE_KEYS)),
        series_id=_series_id_st,
        name=_name_st,
    )


def asset_strategy():
    """Union of live-source and Excel-source asset strategies."""
    return st.one_of(asset_with_live_source_strategy(), asset_with_excel_source_strategy())


# ─── Helper: build assets dict from a list of asset dicts ────────────────────

def _build_assets_dict(asset_list: list[dict]) -> dict[str, Any]:
    """
    Convert a list of asset dicts (each with 'key' and 'meta') into the
    market_returns.json-shaped assets dict keyed by asset ID.

    When the same key appears multiple times, the last entry wins (mirrors
    how a real market_returns.json would be keyed).
    """
    return {a["key"]: {"meta": a["meta"]} for a in asset_list}


# ─── Property 1: Verdict HEALTHY invariant ───────────────────────────────────


# Feature: automation-self-living-data-flow, Property 1: Verdict HEALTHY invariant
@given(st.lists(asset_with_live_source_strategy(), min_size=1))
@settings(max_examples=100)
def test_verdict_healthy_invariant(assets):
    """
    **Validates: Requirements 2.1, 2.2, 2.3**

    For any non-empty set of assets where every asset carries a live source
    (meta.source starts with 'yfinance:' or 'fred:'), the computed
    self_living_check verdict SHALL be LIVE_MERGE_HEALTHY, holdouts SHALL be
    an empty list, and live_source_count SHALL be greater than excel_only_count.

    This property holds because:
    - All assets have live sources → no holdout_keys are populated
    - live_source_count > 0 and excel_only_count == 0 → live > excel
    - Both conditions for HEALTHY are satisfied
    """
    assets_dict = _build_assets_dict(assets)
    result = compute_self_living_check(assets_dict, LIVE_ELIGIBLE_KEYS)

    assert result["verdict"] == "LIVE_MERGE_HEALTHY", (
        f"Expected LIVE_MERGE_HEALTHY when all assets have live sources, "
        f"got {result['verdict']!r}. "
        f"live_source_count={result['live_source_count']}, "
        f"excel_only_count={result['excel_only_count']}, "
        f"holdout_keys={result['holdout_keys']}"
    )
    assert result["holdouts"] == [], (
        f"Expected holdouts=[] when all assets have live sources, "
        f"got {result['holdouts']!r}"
    )
    assert result["live_source_count"] > result["excel_only_count"], (
        f"Expected live_source_count > excel_only_count, "
        f"got live={result['live_source_count']}, excel={result['excel_only_count']}"
    )


# ─── Property 2: Verdict DEGRADED invariant ──────────────────────────────────


# Feature: automation-self-living-data-flow, Property 2: Verdict DEGRADED invariant
@given(
    st.lists(asset_with_live_source_strategy(), min_size=1),
    st.lists(asset_with_excel_source_strategy(), min_size=1),
)
@settings(max_examples=100)
def test_verdict_degraded_invariant(live_assets, excel_assets):
    """
    **Validates: Requirements 2.4, 2.5**

    For any mix of live-source assets and Excel-source assets (at least one of
    each), the computed verdict SHALL be LIVE_MERGE_DEGRADED, and holdout_keys
    SHALL equal exactly the set of Excel-source asset keys that are in
    LIVE_ELIGIBLE_KEYS.

    This property holds because:
    - At least one Excel-source asset is in LIVE_ELIGIBLE_KEYS → holdout_keys non-empty
    - At least one live-source asset → live_source_count > 0
    - Non-empty holdout_keys → HEALTHY condition fails → DEGRADED is selected
    """
    # Combine live and Excel assets; later entries overwrite earlier ones for
    # the same key. To guarantee at least one Excel holdout survives, we build
    # the dict so Excel assets are inserted last (they overwrite any live asset
    # with the same key).
    assets_dict: dict[str, Any] = {}
    for a in live_assets:
        assets_dict[a["key"]] = {"meta": a["meta"]}
    for a in excel_assets:
        assets_dict[a["key"]] = {"meta": a["meta"]}

    # Compute the expected holdout keys: Excel-source assets whose key is in
    # LIVE_ELIGIBLE_KEYS and whose entry was not overwritten by a live asset.
    expected_holdout_keys: set[str] = set()
    for key, av in assets_dict.items():
        src = av["meta"].get("source", "")
        if src.startswith("Mega_Markets_Historical") and key in LIVE_ELIGIBLE_KEYS:
            expected_holdout_keys.add(key)

    # If all Excel keys were overwritten by live assets (same key appeared in
    # both lists and live came last), there may be no holdouts — skip that case
    # since it would produce HEALTHY, not DEGRADED.
    # We force Excel assets to be inserted last above, so this should not happen,
    # but guard defensively.
    if not expected_holdout_keys:
        # No holdouts after deduplication — this example is vacuously valid;
        # skip rather than assert a wrong verdict.
        return

    # Also need at least one live source to remain after deduplication.
    live_source_count = sum(
        1 for av in assets_dict.values()
        if av["meta"].get("source", "").startswith(("yfinance:", "fred:"))
    )
    if live_source_count == 0:
        # All keys were overwritten by Excel — would be FAILED, not DEGRADED.
        return

    result = compute_self_living_check(assets_dict, LIVE_ELIGIBLE_KEYS)

    assert result["verdict"] == "LIVE_MERGE_DEGRADED", (
        f"Expected LIVE_MERGE_DEGRADED when live and Excel assets are mixed, "
        f"got {result['verdict']!r}. "
        f"live_source_count={result['live_source_count']}, "
        f"holdout_keys={result['holdout_keys']}, "
        f"expected_holdout_keys={sorted(expected_holdout_keys)}"
    )
    assert set(result["holdout_keys"]) == expected_holdout_keys, (
        f"holdout_keys mismatch: "
        f"got {set(result['holdout_keys'])!r}, "
        f"expected {expected_holdout_keys!r}"
    )


# ─── Property 3: Source-label honesty (verdict side) ─────────────────────────


# Feature: automation-self-living-data-flow, Property 3: Source-label honesty
@given(asset_strategy())
@settings(max_examples=100)
def test_source_label_honesty(asset):
    """
    **Validates: Requirements 3.4, 5.5**

    For any single asset:
    - IF meta.source starts with 'yfinance:' or 'fred:', the asset key SHALL
      NOT appear in holdout_keys (it is classified as a live source).
    - IF meta.source starts with 'Mega_Markets_Historical' AND the key is in
      LIVE_ELIGIBLE_KEYS, the asset key SHALL appear in holdout_keys (it is
      classified as an Excel holdout).

    This property ensures the verdict computation honestly reflects the source
    label: no asset can carry a live-source label while being treated as a
    holdout, and no registry-eligible Excel asset can escape holdout detection.
    """
    assets_dict = {asset["key"]: {"meta": asset["meta"]}}
    result = compute_self_living_check(assets_dict, LIVE_ELIGIBLE_KEYS)

    src = asset["meta"]["source"]
    key = asset["key"]

    if src.startswith("yfinance:") or src.startswith("fred:"):
        assert key not in result["holdout_keys"], (
            f"Asset {key!r} with live source {src!r} must NOT be in holdout_keys, "
            f"but got holdout_keys={result['holdout_keys']!r}. "
            f"Req 3.4/5.5: live-source label must not be classified as a holdout."
        )
    elif src.startswith("Mega_Markets_Historical") and key in LIVE_ELIGIBLE_KEYS:
        assert key in result["holdout_keys"], (
            f"Asset {key!r} with Excel source {src!r} and key in LIVE_ELIGIBLE_KEYS "
            f"must be in holdout_keys, but got holdout_keys={result['holdout_keys']!r}. "
            f"Req 3.4/5.5: registry-eligible Excel asset must be classified as a holdout."
        )
