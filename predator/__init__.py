"""Predator Protocol — cross-ETF conviction scanner.

Lives inside yieldchaser/etf-data alongside the scraper.
"""
from .scoring import Config, Tier, Sanitizer, compute_leaderboard, compute_rank_deltas, rank_multiplier

__version__ = "0.2.0"
__all__ = ["Config", "Tier", "Sanitizer", "compute_leaderboard", "compute_rank_deltas", "rank_multiplier"]
