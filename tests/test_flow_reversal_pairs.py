"""Source reversal pairs (a creation undone the next day) are netted out of
the derived flow metrics. Run: pytest tests/test_flow_reversal_pairs.py"""
import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd

_spec = importlib.util.spec_from_file_location(
    "update_daily_flows", Path(__file__).parent.parent / "scripts" / "update_daily_flows.py")
udf = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(udf)


def _flows(tail):
    rng = np.random.default_rng(0)
    base = list(rng.normal(0, 1e6, 60))
    return pd.Series(base + tail, dtype=float)


def test_pair_is_netted_onto_second_day_and_total_kept():
    f = _flows([1.02e9, -0.99e9])
    net = udf._net_reversal_pairs(f)
    assert net.iloc[-2] == 0.0
    assert abs(net.iloc[-1] - 0.03e9) < 1
    assert abs(net.sum() - f.sum()) < 1


def test_genuine_one_sided_spike_is_kept():
    f = _flows([1.0e9, 2e6])
    assert udf._net_reversal_pairs(f).iloc[-2] == 1.0e9


def test_chain_keeps_running_total():
    f = _flows([-301e6, 297e6, -288e6])
    net = udf._net_reversal_pairs(f)
    assert abs(net.sum() - f.sum()) < 1
    assert abs(net.iloc[-1]) < 300e6 and net.iloc[-3] == 0.0


def test_derived_metrics_use_netted_flows_but_keep_raw():
    df = pd.DataFrame({"usd_flow": _flows([1.02e9, -0.99e9])})
    out = udf._apply_derived_metrics(df.copy())
    assert out["usd_flow"].iloc[-1] == -0.99e9          # raw stays as reported
    assert abs(out["flow_zscore"].iloc[-1]) < 40         # not a -1e9 shock
    assert abs(out["flow_5d"].iloc[-1] - out["usd_flow"].iloc[-5:].sum()) < 1
