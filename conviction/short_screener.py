"""conviction.short_screener — Institutional Short-Side Screening & Guardrails.

Provides high-conviction short candidate evaluation by distinguishing true
institutional consensus abandonment from:
  1. Mechanical mandate aging/rotations (e.g. IPO fund aging out while core funds hold/buy)
  2. Internal factor handoffs (e.g. Momentum fund exiting while Value fund enters)
  3. Passive benchmark weighting noise (price moves shifting index percentages)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


# Known funds with mechanical, non-fundamental mandate rules
MANDATE_RESTRICTED_ETFS: frozenset[str] = frozenset({
    "FPX",   # First Trust US IPO: mechanically exits ~1,000 trading days post-IPO
    "FPXI",  # First Trust Int'l IPO: mechanically exits ~1,000 trading days post-IPO
    "CSD",   # Invesco Spin-off: mechanically holds spin-offs for a fixed calendar window
})

# Sister / related fund pairs known for factor handoffs
FACTOR_FAMILY_PAIRS: list[tuple[str, str]] = [
    ("QMOM", "QVAL"),  # Alpha Architect US Momentum <-> Value
    ("IMOM", "IVAL"),  # Alpha Architect Int'l Momentum <-> Value
    ("SPMO", "SPHQ"),  # Invesco S&P Momentum <-> Quality
]


@dataclass(frozen=True)
class ShortScreeningResult:
    ticker: str
    verdict: Literal["VALID_SHORT", "MANDATE_ROTATION_SUSPECT", "FACTOR_HANDOFF", "PASSIVE_NOISE", "INSUFFICIENT_EVIDENCE"]
    score_delta: float
    rank_delta: float
    funds_prior_count: int
    funds_now_count: int
    funds_exited: list[str]
    funds_trimmed: list[str]
    funds_added_or_new: list[str]
    reason: str


def screen_short_candidate(
    ticker: str,
    funds_prior: dict[str, float],
    funds_now: dict[str, float],
    score_delta: float,
    rank_delta: float,
) -> ShortScreeningResult:
    """Evaluate a potential short candidate against institutional consensus guardrails.

    Args:
        ticker: Ticker symbol.
        funds_prior: Mapping of ETF_Ticker -> weight in prior window (e.g. 30d ago).
        funds_now: Mapping of ETF_Ticker -> weight in current window.
        score_delta: Change in total conviction score (score_now - score_prior).
        rank_delta: Change in leaderboard rank (rank_prior - rank_now; negative = dropped).

    Returns:
        ShortScreeningResult with verdict and explanatory breakdown.
    """
    all_funds = set(funds_prior.keys()) | set(funds_now.keys())
    
    exited = []
    trimmed = []
    added_or_new = []

    for f in sorted(all_funds):
        w_prior = funds_prior.get(f, 0.0)
        w_now = funds_now.get(f, 0.0)
        delta = w_now - w_prior
        
        if w_prior > 0 and w_now == 0:
            exited.append(f)
        elif delta <= -0.0005:  # trimmed by at least 0.05% weight
            trimmed.append(f)
        elif delta >= 0.0005 or (w_prior == 0 and w_now > 0):
            added_or_new.append(f)

    # 1. Check for Factor Handoff Guard (e.g. QMOM -> QVAL)
    for f_from, f_to in FACTOR_FAMILY_PAIRS:
        if (f_from in exited or f_from in trimmed) and (f_to in added_or_new or funds_now.get(f_to, 0.0) > 0.015):
            return ShortScreeningResult(
                ticker=ticker,
                verdict="FACTOR_HANDOFF",
                score_delta=score_delta,
                rank_delta=rank_delta,
                funds_prior_count=len(funds_prior),
                funds_now_count=len(funds_now),
                funds_exited=exited,
                funds_trimmed=trimmed,
                funds_added_or_new=added_or_new,
                reason=f"Factor rotation handoff detected ({f_from} trimmed/exited while sister factor fund {f_to} entered/added)."
            )

    # 2. Check for Mandate-Driven Single-Fund Exit Guard (e.g. FPX IPO aging)
    mandate_exits = [f for f in exited if f in MANDATE_RESTRICTED_ETFS]
    if mandate_exits and len(exited) == len(mandate_exits):
        # Only mandate funds exited, check if core funds maintained or added
        active_holding_funds = [f for f in funds_now.keys() if f not in MANDATE_RESTRICTED_ETFS and funds_now[f] > 0.005]
        if len(active_holding_funds) >= 3 or (len(added_or_new) > 0 and len(trimmed) <= 1):
            return ShortScreeningResult(
                ticker=ticker,
                verdict="MANDATE_ROTATION_SUSPECT",
                score_delta=score_delta,
                rank_delta=rank_delta,
                funds_prior_count=len(funds_prior),
                funds_now_count=len(funds_now),
                funds_exited=exited,
                funds_trimmed=trimmed,
                funds_added_or_new=added_or_new,
                reason=f"Sole liquidation was mandate-restricted fund ({', '.join(mandate_exits)}) while {len(active_holding_funds)} core funds maintained/accumulated exposure."
            )

    # 3. Check for Passive Benchmark Noise (No funds exited, trivial weight shifts, but price drifted)
    if not exited and not added_or_new and len(trimmed) <= 1:
        if score_delta > -30:
            return ShortScreeningResult(
                ticker=ticker,
                verdict="PASSIVE_NOISE",
                score_delta=score_delta,
                rank_delta=rank_delta,
                funds_prior_count=len(funds_prior),
                funds_now_count=len(funds_now),
                funds_exited=exited,
                funds_trimmed=trimmed,
                funds_added_or_new=added_or_new,
                reason="No actual fund departures; score change driven by passive beta drift."
            )

    # 4. Valid High-Conviction Short: True Institutional Consensus Exodus
    selling_consensus = len(exited) + len(trimmed)
    buying_consensus = len(added_or_new)

    if (selling_consensus >= 2 and buying_consensus == 0) or len(exited) >= 2 or (selling_consensus >= 3 and buying_consensus <= 1):
        return ShortScreeningResult(
            ticker=ticker,
            verdict="VALID_SHORT",
            score_delta=score_delta,
            rank_delta=rank_delta,
            funds_prior_count=len(funds_prior),
            funds_now_count=len(funds_now),
            funds_exited=exited,
            funds_trimmed=trimmed,
            funds_added_or_new=added_or_new,
            reason=f"Broad institutional consensus exit: {len(exited)} full liquidations, {len(trimmed)} funds trimming with zero/minimal buying."
        )

    return ShortScreeningResult(
        ticker=ticker,
        verdict="INSUFFICIENT_EVIDENCE",
        score_delta=score_delta,
        rank_delta=rank_delta,
        funds_prior_count=len(funds_prior),
        funds_now_count=len(funds_now),
        funds_exited=exited,
        funds_trimmed=trimmed,
        funds_added_or_new=added_or_new,
        reason=f"Mixed institutional flow ({len(exited)} exits, {len(trimmed)} trims, {len(added_or_new)} adds); does not meet unanimous short threshold."
    )
