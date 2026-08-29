"""Unit tests for conviction.short_screener guardrails."""

import pytest
from conviction.short_screener import screen_short_candidate


def test_pltr_mandate_rotation_suspect():
    """FPX (IPO index fund) exits due to aging out, while 5 general funds hold/accumulate."""
    funds_prior = {
        "FPX": 0.0233,
        "JOET": 0.0067,
        "QQQM": 0.0134,
        "RPG": 0.0126,
        "SPHB": 0.0097,
        "SPMO": 0.0114,
        "XLG": 0.0081,
    }
    funds_now = {
        "FPX": 0.0,      # Exited (mandate aging out)
        "JOET": 0.0060,
        "QQQM": 0.0135,  # Adding
        "RPG": 0.0133,   # Adding
        "SPHB": 0.0102,  # Adding
        "SPMO": 0.0119,  # Adding
        "XLG": 0.0074,
    }
    res = screen_short_candidate("PLTR", funds_prior, funds_now, score_delta=-61.0, rank_delta=-773.0)
    assert res.verdict == "MANDATE_ROTATION_SUSPECT"
    assert "FPX" in res.funds_exited
    assert "MANDATE_RESTRICTED" not in res.ticker


def test_hl_factor_handoff():
    """QMOM (Momentum) drops position, but sister fund QVAL (Value) enters."""
    funds_prior = {
        "JHMM": 0.0007,
        "QMOM": 0.0200,
        "XMMO": 0.0167,
    }
    funds_now = {
        "JHMM": 0.0007,
        "QMOM": 0.0,     # Exited momentum
        "QVAL": 0.0199,  # Entered value
        "XMMO": 0.0173,  # Added
    }
    res = screen_short_candidate("HL", funds_prior, funds_now, score_delta=-34.0, rank_delta=-512.0)
    assert res.verdict == "FACTOR_HANDOFF"
    assert "QMOM" in res.funds_exited
    assert "QVAL" in res.funds_added_or_new


def test_applovin_unanimous_short():
    """All 6 holding funds actively trim position without any additions."""
    funds_prior = {
        "FPX": 0.0252,
        "JOET": 0.0091,
        "QQQM": 0.0072,
        "RPG": 0.0138,
        "SPHB": 0.0146,
        "SPHQ": 0.0264,
    }
    funds_now = {
        "FPX": 0.0209,
        "JOET": 0.0066,
        "QQQM": 0.0055,
        "RPG": 0.0111,
        "SPHB": 0.0116,
        "SPHQ": 0.0206,
    }
    res = screen_short_candidate("APP", funds_prior, funds_now, score_delta=-42.0, rank_delta=-18.0)
    assert res.verdict == "VALID_SHORT"
    assert len(res.funds_trimmed) == 6
    assert len(res.funds_added_or_new) == 0


def test_vertiv_multi_fund_collapse():
    """Multiple funds trimming and negative multi-week velocity."""
    funds_prior = {
        "JOET": 0.0074,
        "PDP": 0.0080,
        "RPG": 0.0213,
        "SPHB": 0.0150,
    }
    funds_now = {
        "JOET": 0.0068,  # trimmed
        "PDP": 0.0079,
        "RPG": 0.0205,   # trimmed
        "SPHB": 0.0140,  # trimmed
    }
    res = screen_short_candidate("VRT", funds_prior, funds_now, score_delta=-33.0, rank_delta=-943.0)
    assert res.verdict == "VALID_SHORT"
    assert len(res.funds_trimmed) >= 2


def test_passive_noise():
    """No funds exited, small price-driven drift."""
    funds_prior = {"SPMO": 0.0100, "SPHQ": 0.0100}
    funds_now = {"SPMO": 0.0098, "SPHQ": 0.0099}
    res = screen_short_candidate("ABC", funds_prior, funds_now, score_delta=-5.0, rank_delta=-15.0)
    assert res.verdict == "PASSIVE_NOISE"
