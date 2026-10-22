"""Scoring engine v4 (2026-10 study) — regression tests.

Covers the structural fixes in docs/SCORING_V4_STUDY.md:
  • κ-convex per-holding conviction (apex only), monotone in breadth
  • no tier-synergy bonus stacked on M_breadth in apex mode
  • staleness guard for dead ETF feeds
  • name-verified identity resolution (collision split / suffix merge /
    curated overrides) and curated share-class renames
  • apex final_score keeps one decimal (no mass collapse to 0)

Run: pytest tests/test_scoring_v4.py -v
"""
from __future__ import annotations

import dataclasses
from pathlib import Path

import pandas as pd
import pytest

from conviction.scoring import (
    Config, compute_leaderboard, conviction_multiplier, clear_sanitizer_cache,
    _name_tokens, _tokens_overlap,
)

CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"


@pytest.fixture(autouse=True)
def _fresh_cache():
    clear_sanitizer_cache()
    yield
    clear_sanitizer_cache()


@pytest.fixture
def cfg() -> Config:
    return dataclasses.replace(Config.from_yaml(CONFIG_PATH), etf_maturity_days=0)


def _h(rows: list[tuple]) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=["ETF_Ticker", "ticker", "name", "weight",
                                       "Holdings_As_Of", "Date_Scraped"])


def _book(etf: str, positions: list[tuple[str, str, float]], date="2026-05-01") -> list[tuple]:
    return [(etf, t, n, w, date, date) for t, n, w in positions]


def _fillers(n: int, w: float, prefix="F") -> list[tuple[str, str, float]]:
    return [(f"{prefix}{i:03d}", f"Filler {prefix}{i:03d} Corp", w) for i in range(n)]


def _row(lb: pd.DataFrame, ticker: str) -> pd.Series:
    return lb[lb["ticker"] == ticker].iloc[0]


# ─── κ convexity ──────────────────────────────────────────────────────────────
class TestConvexity:
    def test_config_sets_kappa_two(self, cfg):
        assert cfg.scoring_mode == "apex"
        assert cfg.conviction.kappa == 2.0

    def test_apex_holding_score_is_points_times_c_squared(self, cfg):
        rows = _book("QMOM", [("BIG", "Big Corp", 0.30)] + _fillers(9, 0.07))
        _, latest = compute_leaderboard(_h(rows), cfg)
        r = latest[latest["ticker"] == "BIG"].iloc[0]
        c = conviction_multiplier(0.30, 1, 10, cfg.conviction)
        assert r["conviction"] == pytest.approx(c)
        assert r["score"] == pytest.approx(40 * c ** 2)

    def test_conviction_mode_stays_linear(self, cfg):
        conv = dataclasses.replace(cfg, scoring_mode="conviction")
        rows = _book("QMOM", [("BIG", "Big Corp", 0.30)] + _fillers(9, 0.07))
        _, latest = compute_leaderboard(_h(rows), conv)
        r = latest[latest["ticker"] == "BIG"].iloc[0]
        assert r["score"] == pytest.approx(40 * r["conviction"])

    def test_adding_a_filler_holder_never_lowers_the_score(self, cfg):
        """The retired m_conv / gate averaged conviction, so a new low-conviction
        holder could LOWER a name's score. κ-convexity is per holding: monotone."""
        base = (_book("FPX", [("STAR", "Star Corp", 0.20)] + _fillers(20, 0.04))
                + _book("QMOM", [("STAR", "Star Corp", 0.15)] + _fillers(20, 0.0425)))
        extra = _book("SPHQ", _fillers(60, 0.0163) + [("STAR", "Star Corp", 0.002)])
        lb0, _ = compute_leaderboard(_h(base), cfg)
        clear_sanitizer_cache()
        lb1, _ = compute_leaderboard(_h(base + extra), cfg)
        assert _row(lb1, "STAR")["etf_count"] == 3
        assert _row(lb1, "STAR")["final_score"] >= _row(lb0, "STAR")["final_score"]

    def test_top_of_book_pair_outranks_breadth_filler(self, cfg):
        """A name every holder puts at the top of its book (2 funds) must
        outrank a name six funds carry as bottom-of-book filler."""
        rows = []
        for etf in ("FPX", "QMOM"):
            rows += _book(etf, [("STAR", "Star Corp", 0.12)] + _fillers(40, 0.022, prefix=etf))
        for etf in ("COWZ", "CALF", "SPHQ", "XMMO", "SPMO", "JHMM"):
            rows += _book(etf, _fillers(50, 0.0199, prefix=etf) + [("FILL", "Fill Corp", 0.005)])
        lb, _ = compute_leaderboard(_h(rows), cfg)
        star, fill = _row(lb, "STAR"), _row(lb, "FILL")
        assert fill["etf_count"] == 6 and star["etf_count"] == 2
        assert star["leaderboard_rank"] < fill["leaderboard_rank"]

    def test_apex_final_score_keeps_one_decimal(self, cfg):
        rows = _book("SPMO", [("TOP", "Top Corp", 0.30)] + _fillers(99, 0.00707))
        lb, _ = compute_leaderboard(_h(rows), cfg)
        small = lb[lb["ticker"] != "TOP"]["final_score"]
        assert (small > 0).all(), "held fillers must not collapse to a 0 score"
        assert (lb["final_score"] == lb["final_score"].round(1)).all()


# ─── No breadth double count ──────────────────────────────────────────────────
class TestBreadthNotDoubleCounted:
    def test_tier_synergy_bonus_ignored_in_apex(self, cfg):
        rows = []
        for etf in ("FPX", "QMOM", "COWZ", "SPMO", "XLG"):   # all five tiers
            rows += _book(etf, [("WIDE", "Wide Corp", 0.10)] + _fillers(9, 0.10, prefix=etf))
        with_bonus = dataclasses.replace(
            cfg, conviction=dataclasses.replace(cfg.conviction, tier_diversity_bonus=0.15))
        no_bonus = dataclasses.replace(
            cfg, conviction=dataclasses.replace(cfg.conviction, tier_diversity_bonus=0.0))
        a, _ = compute_leaderboard(_h(rows), with_bonus)
        clear_sanitizer_cache()
        b, _ = compute_leaderboard(_h(rows), no_bonus)
        assert _row(a, "WIDE")["final_score"] == _row(b, "WIDE")["final_score"]
        assert _row(a, "WIDE")["m_breadth"] > 1.0   # breadth still rewarded, once


# ─── Staleness guard ──────────────────────────────────────────────────────────
class TestStalenessGuard:
    def test_dead_feed_stops_scoring_but_short_gap_does_not(self, cfg):
        assert cfg.stale_snapshot_days == 30
        rows = (_book("FPX", [("LIVE", "Live Corp", 0.5), ("X1", "X One", 0.5)], "2026-06-01")
                + _book("QMOM", [("DEAD", "Dead Corp", 0.5), ("X2", "X Two", 0.5)], "2026-04-15")
                + _book("COWZ", [("GAP", "Gap Corp", 0.5), ("X3", "X Three", 0.5)], "2026-05-12"))
        lb, _ = compute_leaderboard(_h(rows), cfg)
        assert "LIVE" in set(lb["ticker"])
        assert "GAP" in set(lb["ticker"])        # 20 days behind — transient gap
        assert "DEAD" not in set(lb["ticker"])   # 47 days behind — dead feed

    def test_reference_is_freshest_snapshot_not_wall_clock(self, cfg):
        rows = _book("FPX", [("A", "A Corp", 1.0)], "2026-01-01")
        lb, _ = compute_leaderboard(_h(rows), cfg, as_of=pd.Timestamp("2026-03-01"))
        assert set(lb["ticker"]) == {"A"}


# ─── Identity resolution ──────────────────────────────────────────────────────
class TestIdentityResolution:
    def test_cross_market_collision_is_split(self, cfg):
        """'MTN' = Vail Resorts in US funds and MTN Group in EM funds — must be
        two leaderboard rows, not one fabricated 4-fund HIGH_CONVICTION name."""
        rows = []
        for etf in ("AVSC", "JHSC"):
            rows += _book(etf, [("MTN", "VAIL RESORTS INC", 0.02)] + _fillers(49, 0.02, prefix=etf))
        for etf in ("JHEM", "MFEM"):
            rows += _book(etf, [("MTN", "MTN GROUP LTD SOUTH AFRICA", 0.02)] + _fillers(49, 0.02, prefix=etf))
        lb, _ = compute_leaderboard(_h(rows), cfg)
        assert _row(lb, "MTN")["held_by"] == "AVSC, JHSC"
        assert _row(lb, "MTN.MTN")["held_by"] == "JHEM, MFEM"
        assert _row(lb, "MTN")["flag"] != "HIGH_CONVICTION"

    def test_same_company_across_markets_is_not_split(self, cfg):
        rows = (_book("SPMO", [("AU", "AngloGold Ashanti plc", 0.5), ("Z1", "Zed One", 0.5)])
                + _book("PIZ", [("AU", "ANGLOGOLD ASHANTI PLC", 0.5), ("Z2", "Zed Two", 0.5)]))
        lb, _ = compute_leaderboard(_h(rows), cfg)
        assert _row(lb, "AU")["etf_count"] == 2

    def test_exchange_suffix_variant_merges_when_names_agree(self, cfg):
        rows = (_book("PIZ", [("6857", "Advantest Corp", 0.5), ("Z1", "Zed One", 0.5)])
                + _book("FPXI", [("6857.JP", "ADVANTEST CORPORATION", 0.5), ("Z2", "Zed Two", 0.5)]))
        lb, _ = compute_leaderboard(_h(rows), cfg)
        assert "6857.JP" not in set(lb["ticker"])
        assert _row(lb, "6857")["etf_count"] == 2

    def test_suffix_variant_of_a_different_company_stays_separate(self, cfg):
        rows = (_book("AVSC", [("ENR", "ENERGIZER HOLDINGS INC", 0.5), ("Z1", "Zed One", 0.5)])
                + _book("GRIN", [("ENR GR", "SIEMENS ENERGY AG", 0.5), ("Z2", "Zed Two", 0.5)])
                + _book("JHMM", [("NXT", "NEXTPOWER INC", 0.5), ("Z3", "Zed Three", 0.5)])
                + _book("IVAL", [("NXT.LN", "NEXT PLC", 0.5), ("Z4", "Zed Four", 0.5)]))
        lb, _ = compute_leaderboard(_h(rows), cfg)
        tickers = set(lb["ticker"])
        assert {"ENR", "ENR.GR", "NXT", "NXT.LN"} <= tickers
        assert _row(lb, "ENR")["etf_count"] == 1

    def test_curated_override_splits_international_numeric_collision(self, cfg):
        rows = (_book("EEMO", [("2313", "Compeq Manufacturing Co Ltd", 0.5), ("Z1", "Zed One", 0.5)])
                + _book("JHEM", [("2313", "SHENZHOU INTERNATIONAL GROUP", 0.5), ("Z2", "Zed Two", 0.5)]))
        lb, _ = compute_leaderboard(_h(rows), cfg)
        assert _row(lb, "2313")["held_by"] == "EEMO"
        assert _row(lb, "2313.HK")["held_by"] == "JHEM"

    def test_share_classes_pool_but_energizer_is_not_siemens(self, cfg):
        renames = cfg.sanitizer.ticker_renames
        assert renames["005935"] == "005930"
        assert renames["PETR3"] == "PETR4"
        assert renames["ITUB3"] == "ITUB4"
        assert "ENR" not in renames and "ENR.GR" not in renames
        rows = (_book("EEMO", [("005930", "Samsung Electronics Co Ltd", 0.08),
                               ("005935", "Samsung Electronics Co Ltd", 0.03)] + _fillers(89, 0.01))
                + _book("JHEM", [("A005930", "SAMSUNG ELECTRONICS CO LTD", 0.04)] + _fillers(96, 0.01, "J")))
        lb, _ = compute_leaderboard(_h(rows), cfg)
        assert "005935" not in set(lb["ticker"])
        sam = _row(lb, "005930")
        assert sam["etf_count"] == 2
        assert sam["total_weight"] == pytest.approx(0.15)

    def test_name_token_rules(self):
        assert _tokens_overlap(_name_tokens("IND + COMM BK OF CHINA H"),
                               _name_tokens("Industrial & Commercial Bank of China"))
        assert not _tokens_overlap(_name_tokens("NEXT PLC"), _name_tokens("NEXTPOWER INC"))
        assert _tokens_overlap(_name_tokens("Kohl's Corp"), _name_tokens("KOHLS CORP COMMON STOCK"))
        assert _name_tokens("SAMSUNG ELECTRONICS CO LTD COMMON STOCK KRW100.0") == {"SAMSUNG", "ELECTRONICS"}


class TestCompanyNames:
    """Fund files publish "<ISSUER> COMMON STOCK USD.01" or "<ISSUER> <COUNTRY>"
    in capitals; the leaderboard shows a readable issuer name instead."""

    @pytest.mark.parametrize("raw, clean", [
        ("FORMFACTOR INC UNITED STATES OF AMERICA (THE)", "Formfactor Inc"),
        ("ATKORE INC COMMON STOCK USD.01", "Atkore Inc"),
        ("CHEESECAKE FACTORY INC/THE COMMON STOCK USD.01", "The Cheesecake Factory Inc"),
        ("SAMSUNG SDS CO LTD KOREA (THE REPUBLIC OF)", "Samsung SDS Co Ltd"),
        ("BANK OF CHINA LTD H HONG KONG", "Bank of China Ltd"),
        ("PING AN BANK CO LTD A HONG KONG", "Ping An Bank Co Ltd"),
        ("SONIC AUTOMOTIVE INC CLASS A COMMON STOCK USD.01", "Sonic Automotive Inc"),
        ("Cigna Group/The", "The Cigna Group"),
        ("Apple Inc", "Apple Inc"),
        ("AT&T INC", "AT&T Inc"),
    ])
    def test_clean(self, raw, clean):
        from conviction.scoring import clean_company_name
        assert clean_company_name(raw) == clean

    def test_mixed_case_source_wins(self):
        from conviction.scoring import _first_valid_name
        s = pd.Series(["NATERA INC UNITED STATES OF AMERICA (THE)", "Natera Inc."])
        assert _first_valid_name(s) == "Natera Inc."
