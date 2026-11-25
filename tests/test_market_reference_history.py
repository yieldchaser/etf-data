"""Commodity history is spliced by level, never by guessed unit constants.

Covers conviction.markets_history.apply_reference_history (pre-FRED history from the World Bank
reference, rescaled to the live core's level) and _level_matched_extension (futures months after
FRED's last month, rescaled to FRED's level).
"""
import pandas as pd

import conviction.markets_history as mh


def _months(start: str, n: int) -> list[str]:
    return [p.strftime("%Y-%m") for p in pd.period_range(start, periods=n, freq="M")]


def test_reference_history_is_rescaled_to_core_units(monkeypatch):
    # Reference in $/kg, live core in $/mt (1000x): the old history must land on the core's level,
    # so the year that crosses the seam is a real return, not -99.9%.
    ref_months = _months("1990-01", 48)
    ref = {"cocoa": {"monthly": [[ym, 1.0 + i * 0.01] for i, ym in enumerate(ref_months)]}}
    monkeypatch.setattr(mh, "_REFERENCE_CACHE", ref)
    core = [[ym, (1.0 + (i + 24) * 0.01) * 1000.0] for i, ym in enumerate(ref_months[24:])]
    merged = [[ym, 7.0] for ym in ref_months[:24]] + core   # corrupted old months in another unit
    out = dict(mh.apply_reference_history("cocoa", merged, core))
    assert abs(out["1990-01"] - 1000.0) < 1e-6
    dec_1991, dec_1992 = out["1991-12"], out["1992-12"]
    assert 0.0 < dec_1992 / dec_1991 - 1 < 0.2          # smooth across the seam


def test_reference_history_keeps_real_large_moves(monkeypatch):
    # A genuine tripling before the core (cocoa 2024-style) must survive: it is reference data.
    ref_months = _months("2020-01", 60)
    vals = [2.0] * 30 + [6.0] * 30
    monkeypatch.setattr(mh, "_REFERENCE_CACHE", {"cocoa": {"monthly": [[m, v] for m, v in zip(ref_months, vals)]}})
    core = [[m, v * 1000] for m, v in zip(ref_months[40:], vals[40:])]
    out = dict(mh.apply_reference_history("cocoa", core, core))
    assert abs(out["2022-06"] / out["2022-05"] - 1) < 1e-9
    assert abs(out["2022-07"] / out["2022-06"] - 3.0) < 1e-9


def test_unstable_reference_ratio_drops_history_instead_of_guessing(monkeypatch):
    ref_months = _months("2000-01", 48)
    monkeypatch.setattr(mh, "_REFERENCE_CACHE", {"rice": {"monthly": [[m, 100.0] for m in ref_months]}})
    core = [[m, 100.0 * (1 if i % 2 else 3)] for i, m in enumerate(ref_months[24:])]
    out = mh.apply_reference_history("rice", [[m, 1.0] for m in ref_months[:24]] + core, core)
    assert out[0][0] == ref_months[24]                   # nothing before the core survived


def test_extension_is_level_matched_and_skipped_when_unstable():
    idx = pd.date_range("2024-01-31", periods=12, freq="ME")
    core = pd.Series([9000.0 + i for i in range(10)], index=idx[:10])
    futures_cents = pd.Series([(9000.0 + i) / 22.0462 for i in range(12)], index=idx)  # other unit
    out = mh._level_matched_extension(core, futures_cents, "copper")
    assert len(out) == 12 and abs(out.iloc[-1] - 9011.0) < 0.5
    noisy = pd.Series([1.0, 5.0, 1.0, 5.0, 1.0, 5.0, 1.0, 5.0, 1.0, 5.0, 1.0, 5.0], index=idx)
    assert len(mh._level_matched_extension(core, noisy, "copper")) == 10
