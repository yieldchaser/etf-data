"""Regression tests for P0 bug-hunt fixes (2026-08 audit wave).

Covers:
  * NEW-entrant boundary: seen EXACTLY new_lookback_days ago ⇒ NOT new
  * HIGH_CONVICTION uses raw etf_count in ALL modes (apex gate retired)
  * Sanitizer fingerprint is row-order sensitive (cache-collision fix)
"""
from __future__ import annotations

import datetime
from pathlib import Path

import pandas as pd
import pytest

from conviction.scoring import Config, compute_leaderboard, _df_fingerprint

REPO_ROOT = Path(__file__).resolve().parents[1]


def _h(rows) -> pd.DataFrame:
    """rows of (ETF_Ticker, ticker, name, weight, Holdings_As_Of, Date_Scraped)."""
    return pd.DataFrame(
        rows,
        columns=["ETF_Ticker", "ticker", "name", "weight", "Holdings_As_Of", "Date_Scraped"],
    )


@pytest.fixture(scope="module")
def cfg():
    return Config.from_yaml(REPO_ROOT / "config.yaml")


class TestNewEntrantBoundary:
    @staticmethod
    def _mature_rows(base_yt: str) -> list[tuple]:
        """Seed filler rows so QMOM spans ≥ etf_maturity_days (§28 gate
        otherwise suppresses is_new entirely for young ETFs)."""
        base = datetime.date.fromisoformat(base_yt)
        return [
            ("QMOM", f"F{j:02d}", f"Filler {j}", 0.02,
             (base - datetime.timedelta(days=45 + i)).strftime("%Y-%m-%d"),
             (base - datetime.timedelta(days=45 + i)).strftime("%Y-%m-%d"))
            for i in range(3) for j in range(3)
        ]

    def test_seen_exactly_lookback_days_ago_is_not_new(self, cfg):
        """A pair whose prior appearance falls exactly on the cutoff date has
        been absent for the full window and must NOT be flagged NEW."""
        base = datetime.date(2026, 4, 1)
        d0 = base.strftime("%Y-%m-%d")
        d14 = (base + datetime.timedelta(days=cfg.new_lookback_days)).strftime("%Y-%m-%d")
        rows = self._mature_rows(d14) + [
            ("QMOM", "X", "Ticker X", 0.05, d0, d0),
            ("QMOM", "X", "Ticker X", 0.06, d14, d14),
        ]
        df = _h(rows)
        lb, latest = compute_leaderboard(df, cfg, as_of=pd.Timestamp(d14))
        assert latest["ETF_Ticker"].nunique() >= 1
        x = latest[latest["ticker"] == "X"].iloc[0]
        assert not bool(x["is_new"]), (
            f"seen exactly {cfg.new_lookback_days}d ago must not be NEW "
            f"(is_new={x['is_new']})"
        )

    def test_seen_within_window_is_new(self, cfg):
        """A pair last seen INSIDE the lookback window stays NEW on return."""
        base = datetime.date(2026, 4, 1)
        d1 = (base + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        d14 = (base + datetime.timedelta(days=cfg.new_lookback_days)).strftime("%Y-%m-%d")
        rows = self._mature_rows(d14) + [
            ("QMOM", "X", "Ticker X", 0.05, d1, d1),   # 13 days before d14
            ("QMOM", "X", "Ticker X", 0.06, d14, d14),
        ]
        df = _h(rows)
        lb, latest = compute_leaderboard(df, cfg, as_of=pd.Timestamp(d14))
        x = latest[latest["ticker"] == "X"].iloc[0]
        assert bool(x["is_new"]), "last seen inside the lookback window must stay NEW"


class TestHighConvictionRawCount:
    def test_hc_requires_only_raw_etf_count(self):
        """In apex mode, HIGH_CONVICTION fires on raw etf_count ≥ min_etfs.
        The old conviction-breadth gate (θ≥0.8 in ≥4 funds) fired on ~0.03%
        of rows and contradicted every documented definition."""
        import dataclasses

        yaml_cfg = Config.from_yaml(REPO_ROOT / "config.yaml")
        apex_cfg = dataclasses.replace(yaml_cfg, scoring_mode="apex")

        base = "2026-04-01"
        # 5 distinct ETFs hold X at small weights → raw count 5 ≥ 4 but each
        # fund's conviction share is tiny.
        etfs = ["FPX", "QMOM", "COWZ", "SPMO", "PIE"]
        rows = [(e, "X", "Ticker X", 0.01, base, base) for e in etfs]
        df = _h(rows)
        lb, _ = compute_leaderboard(df, apex_cfg, as_of=pd.Timestamp(base))
        x = lb[lb["ticker"] == "X"].iloc[0]
        assert int(x["etf_count"]) == 5
        assert x["flag"] == "HIGH_CONVICTION", (
            f"raw etf_count=5 must flag HIGH_CONVICTION, got {x['flag']!r} "
            f"(conviction_etf_count={x.get('conviction_etf_count')})"
        )


class TestSanitizerFingerprint:
    def test_fingerprint_is_order_sensitive(self):
        """Same multiset of rows in different order must produce DIFFERENT
        fingerprints — the dedupe keeps name='first', so order changes the
        canonical result and a colliding cache entry would return the wrong
        frame."""
        rows_a = [
            ("FPX", "X", "Name A", 0.10, "2026-04-01", "2026-04-01"),
            ("FPX", "Y", "Name B", 0.20, "2026-04-01", "2026-04-01"),
        ]
        df_a = _h(rows_a)
        df_b = _h(list(reversed(rows_a)))
        assert _df_fingerprint(df_a) != _df_fingerprint(df_b)


class TestQualitySetFromConfig:
    def test_quality_tier_derived_from_config(self):
        """The Quality ETF set must come from config.yaml's Quality tier —
        the old hardcoded {COWZ, CALF, SPHQ} silently ignored five Quality
        funds added later, and an ETF-object-vs-string comparison bug
        (fixed 2026-08) emptied the set entirely, killing qa detection."""
        from conviction.history import _quality_etfs_from_config

        yaml_cfg = Config.from_yaml(REPO_ROOT / "config.yaml")
        expected = {
            t for t, e_info in yaml_cfg.etf_lookup().items()
            if getattr(e_info, "tier", None) == "Quality"
        }
        assert expected, "config.yaml must define at least one Quality-tier ETF"
        assert _quality_etfs_from_config(yaml_cfg) == frozenset(expected)
