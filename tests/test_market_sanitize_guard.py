"""Unit tests for the shock-preserving UNIT SANITIZER in markets_history.

Guards the 2026-08 audit finding: the old candidate-factor repair rescaled
GENUINE >3x commodity shocks (sugar x45.36 etc.) because it only checked that
the repaired point sat within 0.5-2x of its predecessor.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from conviction.markets_history import sanitize_monthly_series


def _series(values: list[float], start_year: int = 2025) -> list[list]:
    return [[f"{start_year + i // 12}-{(i % 12) + 1:02d}", v] for i, v in enumerate(values)]


class TestShockPreservingSanitizer:
    def test_genuine_shock_is_preserved(self):
        """A real extreme spike whose level PERSISTS must NOT be rescaled.
        With prev=20 -> p=550 (27.5x, sugar), the old heuristic found
        550/45.36 = 12.1 within [0.5x, 2x] of prev and 'repaired' the point
        to 12.1 even though the next month confirms the new ~550 level."""
        raw = _series([20.0, 21.0, 550.0, 560.0, 555.0])
        out = sanitize_monthly_series(raw, key="sugar")
        fixed = [v for _, v in out]
        assert fixed == [20.0, 21.0, 550.0, 560.0, 555.0], (
            f"genuine persistent shock was corrupted: {fixed}"
        )

    def test_true_unit_error_is_repaired(self):
        """A one-month unit glitch (level reverts next month) IS repaired:
        candidate factor brings it back in line with both neighbours."""
        # sugar normal level ~20 cents; one month arrives as 20*45.36 ~ 907
        vals = [20.0, 21.0, 907.18, 22.0, 21.5]
        raw = _series(vals)
        out = sanitize_monthly_series(raw, key="sugar")
        fixed = [v for _, v in out]
        assert abs(fixed[2] - 907.18 / 45.359237) < 1e-3, (
            f"unit-error month should be rescaled, got {fixed[2]}"
        )
        # neighbours untouched
        assert fixed[1] == 21.0 and fixed[3] == 22.0

    def test_non_commodity_keys_untouched(self):
        """Assets without a scale_fn are never candidate-rescaled."""
        raw = _series([100.0, 500.0, 505.0])
        out = sanitize_monthly_series(raw, key="nikkei")
        assert [v for _, v in out] == [100.0, 500.0, 505.0]

    def test_trailing_unit_error_still_repaired(self):
        """A series ENDING on a wrong-scaled point has no lookahead; repair is
        allowed on single-point evidence (fallback switches mid-stream often
        land at the end during incremental updates)."""
        raw = _series([20.0, 21.0, 22.0, 997.9])  # last month x45 glitch
        out = sanitize_monthly_series(raw, key="sugar")
        fixed = [v for _, v in out]
        assert abs(fixed[3] - 997.9 / 45.359237) < 1e-2, (
            f"trailing glitch should be repaired, got {fixed[3]}"
        )
