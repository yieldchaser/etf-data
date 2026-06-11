"""Tests for the §31.1 conviction gate on apex_score (breadth-inflation guard).

The gate is applied in predator/build.py after predictive_overlay.
These tests exercise the gate logic in isolation using synthetic DataFrames
that mirror the shape of the leaderboard and latest DataFrames at that point.

Run: pytest tests/test_apex_gate.py -v
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Helper: build minimal synthetic leaderboard + latest DataFrames
# ---------------------------------------------------------------------------

def _make_lb_and_latest(specs: list[dict]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    specs: list of dicts with keys:
        ticker, final_score, avg_conviction, etf_count, total_weight,
        per_etf_ranks (list[int])  — per-ETF rank for this ticker

    Returns (leaderboard, latest) that match the shape expected by the gate code.
    """
    lb_rows = []
    latest_rows = []
    etf_names = [f"ETF{i}" for i in range(20)]  # pool of ETF names to pull from

    for spec in specs:
        ticker = spec["ticker"]
        ranks = spec["per_etf_ranks"]
        n_etfs = len(ranks)
        lb_rows.append({
            "ticker": ticker,
            "final_score": int(spec["final_score"]),
            "avg_conviction": float(spec["avg_conviction"]),
            "etf_count": n_etfs,
            "total_weight": float(spec["total_weight"]),
            "leaderboard_rank": 0,  # placeholder; will be overwritten
        })
        for i, rank in enumerate(ranks):
            latest_rows.append({
                "ETF_Ticker": etf_names[i],
                "ticker": ticker,
                "rank": rank,
                "weight": 0.05,
            })

    lb = pd.DataFrame(lb_rows)
    # Assign initial leaderboard_rank by final_score descending
    order = lb.sort_values("final_score", ascending=False).index
    lb["leaderboard_rank"] = pd.Series(
        range(1, len(lb) + 1), index=order
    ).astype("int64")
    lb["apex_score"] = lb["final_score"].astype("int64")
    lb["apex_rank"] = lb["leaderboard_rank"].astype("int64")

    latest = pd.DataFrame(latest_rows)
    return lb, latest


# ---------------------------------------------------------------------------
# Core gate logic extracted for unit testing
# (mirrors the build.py §31.1 block exactly — keep in sync)
# ---------------------------------------------------------------------------

def _apply_conviction_gate(
    leaderboard: pd.DataFrame,
    latest: pd.DataFrame,
) -> pd.DataFrame:
    """Apply the §31.1 conviction gate and return an annotated copy."""
    lb = leaderboard.copy()

    _median_etf_rank = (
        latest.groupby("ticker")["rank"].median()
        .rename("median_etf_rank")
    )
    lb = lb.merge(_median_etf_rank.reset_index(), on="ticker", how="left")
    lb["median_etf_rank"] = lb["median_etf_rank"].fillna(9999.0)

    _conv = lb["avg_conviction"].fillna(0.40)
    _g = _conv.clip(lower=0.40, upper=1.10)

    _top_of_book = lb["median_etf_rank"] <= 10
    _g = _g.where(~_top_of_book, other=_g.clip(lower=1.0))

    lb["conviction_gate"] = _g.round(2)
    lb["apex_score"] = (
        lb["apex_score"] * lb["conviction_gate"]
    ).clip(lower=0.0).round().astype("int64")

    _order = lb.sort_values(
        ["apex_score", "etf_count", "total_weight"],
        ascending=[False, False, False],
    ).index
    lb["apex_rank"] = (
        pd.Series(range(1, len(lb) + 1), index=_order).astype("int64")
    )
    return lb


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestConvictionGateOrdering:
    def test_high_conviction_beats_breadth_filler(self):
        """A high-conviction #1 position beats a breadth-filler held by 5 funds."""
        lb, latest = _make_lb_and_latest([
            # CONV: single fund, top of book, avg_conviction = 1.30
            {
                "ticker": "CONV",
                "final_score": 800,
                "avg_conviction": 1.30,
                "etf_count": 1,
                "total_weight": 0.15,
                "per_etf_ranks": [1],
            },
            # FILLER: 5 funds, mediocre ranks, avg_conviction = 0.60
            {
                "ticker": "FILLER",
                "final_score": 900,   # higher raw score via breadth
                "avg_conviction": 0.60,
                "etf_count": 5,
                "total_weight": 0.25,
                "per_etf_ranks": [30, 40, 50, 35, 45],
            },
        ])
        # Before gate: FILLER has higher apex_score (900 > 800)
        assert lb.loc[lb["ticker"] == "FILLER", "apex_score"].iloc[0] > \
               lb.loc[lb["ticker"] == "CONV", "apex_score"].iloc[0]

        out = _apply_conviction_gate(lb, latest)

        conv_row   = out[out["ticker"] == "CONV"].iloc[0]
        filler_row = out[out["ticker"] == "FILLER"].iloc[0]

        # CONV: median_rank=1 ≤ 10, g clamped ≥ 1.0, g = max(clip(1.30,0.40,1.10), 1.0) = 1.10
        assert conv_row["conviction_gate"] == pytest.approx(1.10)
        # FILLER: median_rank=40 > 10, g = clip(0.60, 0.40, 1.10) = 0.60
        assert filler_row["conviction_gate"] == pytest.approx(0.60)

        # After gate, CONV should rank above FILLER
        assert conv_row["apex_rank"] < filler_row["apex_rank"], (
            f"CONV apex_rank={conv_row['apex_rank']} should be < "
            f"FILLER apex_rank={filler_row['apex_rank']}"
        )
        assert conv_row["apex_score"] > filler_row["apex_score"], (
            f"CONV apex_score={conv_row['apex_score']} should be > "
            f"FILLER apex_score={filler_row['apex_score']}"
        )

    def test_final_score_untouched(self):
        """Gate must NOT modify final_score — byte-identical before and after."""
        lb, latest = _make_lb_and_latest([
            {
                "ticker": "CONV",
                "final_score": 800,
                "avg_conviction": 1.30,
                "etf_count": 2,
                "total_weight": 0.10,
                "per_etf_ranks": [1, 2],
            },
            {
                "ticker": "FILLER",
                "final_score": 900,
                "avg_conviction": 0.50,
                "etf_count": 5,
                "total_weight": 0.25,
                "per_etf_ranks": [30, 40, 50, 35, 45],
            },
        ])
        final_before = lb.set_index("ticker")["final_score"].copy()
        out = _apply_conviction_gate(lb, latest)
        final_after = out.set_index("ticker")["final_score"]

        for ticker in ["CONV", "FILLER"]:
            assert final_before[ticker] == final_after[ticker], (
                f"final_score for {ticker} changed: "
                f"{final_before[ticker]} → {final_after[ticker]}"
            )

    def test_median_rank_le10_escape_hatch(self):
        """A ticker with avg_conviction < 1.0 but median ETF rank ≤ 10 must get g ≥ 1.0."""
        lb, latest = _make_lb_and_latest([
            # Slightly under equal-weight but top-5 in every holder
            {
                "ticker": "TOPBOOK",
                "final_score": 700,
                "avg_conviction": 0.85,   # below 1.0
                "etf_count": 3,
                "total_weight": 0.12,
                "per_etf_ranks": [3, 5, 8],  # median = 5 ≤ 10
            },
        ])
        out = _apply_conviction_gate(lb, latest)
        row = out[out["ticker"] == "TOPBOOK"].iloc[0]

        assert row["median_etf_rank"] == pytest.approx(5.0)
        # g should be max(clip(0.85, 0.40, 1.10), 1.0) = 1.0
        assert row["conviction_gate"] == pytest.approx(1.0)
        # apex_score should equal original (gate multiplier = 1.0)
        assert row["apex_score"] == 700

    def test_low_conviction_filler_gated(self):
        """A breadth-filler with avg_conviction = 0.45 and deep ranks gets g = 0.45."""
        lb, latest = _make_lb_and_latest([
            {
                "ticker": "FILLER2",
                "final_score": 500,
                "avg_conviction": 0.45,
                "etf_count": 4,
                "total_weight": 0.20,
                "per_etf_ranks": [60, 70, 80, 90],  # median = 75 >> 10
            },
        ])
        out = _apply_conviction_gate(lb, latest)
        row = out[out["ticker"] == "FILLER2"].iloc[0]

        assert row["conviction_gate"] == pytest.approx(0.45)
        # apex_score = round(500 * 0.45) = 225
        assert row["apex_score"] == round(500 * 0.45)

    def test_conviction_clipped_at_floor_040(self):
        """avg_conviction below 0.40 is clamped to 0.40 (never punished beyond the floor)."""
        lb, latest = _make_lb_and_latest([
            {
                "ticker": "VERYLOW",
                "final_score": 300,
                "avg_conviction": 0.10,  # below floor
                "etf_count": 3,
                "total_weight": 0.15,
                "per_etf_ranks": [100, 120, 140],
            },
        ])
        out = _apply_conviction_gate(lb, latest)
        row = out[out["ticker"] == "VERYLOW"].iloc[0]

        # Gate floor is 0.40, not 0.10
        assert row["conviction_gate"] == pytest.approx(0.40)

    def test_conviction_clipped_at_ceiling_110(self):
        """avg_conviction above 1.10 is clamped to 1.10 (ceiling)."""
        lb, latest = _make_lb_and_latest([
            {
                "ticker": "SUPER",
                "final_score": 1000,
                "avg_conviction": 1.80,  # above ceiling
                "etf_count": 2,
                "total_weight": 0.20,
                "per_etf_ranks": [1, 1],  # median = 1 ≤ 10 → escape kicks in too
            },
        ])
        out = _apply_conviction_gate(lb, latest)
        row = out[out["ticker"] == "SUPER"].iloc[0]

        # clip(1.80, 0.40, 1.10) = 1.10; escape also gives max(1.10, 1.0) = 1.10
        assert row["conviction_gate"] == pytest.approx(1.10)

    def test_apex_rank_is_dense_1_to_n(self):
        """apex_rank must be a dense 1..n sequence with no gaps."""
        specs = [
            {
                "ticker": f"T{i}",
                "final_score": 1000 - i * 50,
                "avg_conviction": 0.5 + i * 0.1,
                "etf_count": 2,
                "total_weight": 0.10,
                "per_etf_ranks": [i + 1, i + 2],
            }
            for i in range(6)
        ]
        lb, latest = _make_lb_and_latest(specs)
        out = _apply_conviction_gate(lb, latest)
        ranks = sorted(out["apex_rank"].tolist())
        assert ranks == list(range(1, len(specs) + 1))

    def test_conviction_gate_column_present_in_output(self):
        """conviction_gate must always be present in output for UI/tooltip use."""
        lb, latest = _make_lb_and_latest([
            {
                "ticker": "X",
                "final_score": 500,
                "avg_conviction": 0.90,
                "etf_count": 2,
                "total_weight": 0.10,
                "per_etf_ranks": [5, 10],
            },
        ])
        out = _apply_conviction_gate(lb, latest)
        assert "conviction_gate" in out.columns
        assert "median_etf_rank" in out.columns
