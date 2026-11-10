"""Unit tests for pure NumPy/Pandas quantitative validation and empirical falsification suite.

Tests mathematical correctness and empirical falsification gates without network access.
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np
import pytest

from scripts.validate_quant_signals import (
    benjamini_hochberg_fdr,
    deflated_sharpe_ratio,
    newey_west_hac_se,
    run_empirical_falsification_gates,
    spearman_rank_corr,
    stationary_bootstrap_indices,
)

ROOT = Path(__file__).resolve().parents[1]
REPORT_PATH = ROOT / "docs" / "data" / "quant_validation_report.json"


def test_spearman_rank_correlation_properties():
    """Verify Spearman Rank Correlation on known vectors."""
    # Perfect positive rank correlation
    x = np.array([1.0, 2.0, 5.0, 10.0, 20.0])
    y = np.array([10.0, 25.0, 30.0, 100.0, 500.0])
    assert abs(spearman_rank_corr(x, y) - 1.0) < 1e-6

    # Perfect inverse rank correlation
    y_inv = np.array([500.0, 100.0, 30.0, 25.0, 10.0])
    assert abs(spearman_rank_corr(x, y_inv) - (-1.0)) < 1e-6

    # Boundary handling: under-length or constant
    assert spearman_rank_corr(np.array([1.0, 2.0]), np.array([1.0, 2.0])) == 0.0
    assert spearman_rank_corr(np.array([5.0, 5.0, 5.0, 5.0]), np.array([1.0, 2.0, 3.0, 4.0])) == 0.0


def test_newey_west_hac_standard_errors():
    """Verify Newey-West HAC covariance produces positive definite standard errors."""
    t_obs = 100
    rng = np.random.RandomState(42)
    # Autocorrelated residuals AR(1) with rho = 0.5
    u = np.empty(t_obs)
    u[0] = rng.randn()
    for t in range(1, t_obs):
        u[t] = 0.5 * u[t - 1] + rng.randn()

    x_mat = np.column_stack([np.ones(t_obs), rng.randn(t_obs)])
    se = newey_west_hac_se(u, x_mat, max_lags=4)
    assert len(se) == 2
    assert all(s > 0 for s in se)


def test_benjamini_hochberg_fdr_control():
    """Verify Benjamini-Hochberg step-up procedure identifies true discoveries."""
    # 5 strong p-values, 5 noise p-values
    p_vals = [0.001, 0.002, 0.005, 0.01, 0.015, 0.45, 0.60, 0.75, 0.85, 0.95]
    sig = benjamini_hochberg_fdr(p_vals, q=0.05)
    assert len(sig) == 10
    # The lowest 5 must be rejected (significant discoveries)
    assert all(sig[:5])
    # The noise p-values must not be rejected
    assert not any(sig[5:])


def test_deflated_sharpe_ratio_behavior():
    """Verify Deflated Sharpe Ratio penalizes multiple trials appropriately."""
    # Strategy with SR = 1.0 tested across 1 trial vs 100 trials
    dsr_single = deflated_sharpe_ratio(est_sr=1.2, num_trials=2, var_sr=0.1, skewness=0.0, kurtosis=3.0, t_obs=500)
    dsr_multiple = deflated_sharpe_ratio(est_sr=1.2, num_trials=100, var_sr=0.1, skewness=0.0, kurtosis=3.0, t_obs=500)
    assert dsr_single > dsr_multiple


def test_stationary_bootstrap_index_generation():
    """Verify Politis-Romano stationary bootstrap path dimensions and properties."""
    paths = stationary_bootstrap_indices(n=100, num_paths=50, mean_block_len=8.0)
    assert len(paths) == 50
    for path in paths:
        assert len(path) == 100
        assert np.all(path >= 0) and np.all(path < 100)


def test_empirical_falsification_gates_pass():
    """Verify that all 5 empirical falsification gates pass cleanly."""
    history_cache = {}
    history_file = ROOT / "docs" / "data" / "flows" / "etf_history.json"
    if history_file.is_file():
        history_cache = json.loads(history_file.read_text(encoding="utf-8")).get("history", {})

    gates = run_empirical_falsification_gates(dfs={}, history=history_cache, catalog={})
    assert gates["phantom_ex_dividend_gate"]["passed"] is True
    assert gates["split_3_2_detection_gate"]["passed"] is True
    assert gates["uvxy_svxy_transition_gate"]["passed"] is True
    assert gates["hurdle_zero_inflation_gate"]["passed"] is True
    assert gates["sub_dollar_spread_finiteness_gate"]["passed"] is True


def test_quant_validation_report_file_integrity():
    """Verify checked-in or generated validation report JSON schema."""
    assert REPORT_PATH.is_file(), f"Missing report at {REPORT_PATH}"
    report = json.loads(REPORT_PATH.read_text(encoding="utf-8"))
    assert report["status"] == "complete"
    assert "fama_macbeth_rank_ic" in report
    assert "jorda_local_projections" in report
    assert "hasbrouck_information_share" in report
    assert "multiple_testing_and_dsr" in report
    assert "stationary_bootstrap_microcvi" in report
    assert "empirical_falsification_gates" in report
    assert report["empirical_falsification_gates"]["phantom_ex_dividend_gate"]["passed"] is True
