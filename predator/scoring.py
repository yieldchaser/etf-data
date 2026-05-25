"""
Predator Protocol — scoring engine.

The algorithm is the documented Predator Protocol v1 from the etf-data README.
Sanitizer mirrors the ArchiveToDatabase_Production VBA sub.

Inputs:
    history: long-form holdings, schema from yieldchaser/etf-data scraper.py.
             Columns: ETF_Ticker, ticker, name, weight, Holdings_As_Of, Date_Scraped

Outputs (from compute_leaderboard):
    leaderboard:  one row per ticker, sorted by final_score desc
    latest:       one row per (ETF, ticker) in latest snapshot, with score components
"""
from __future__ import annotations
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Iterable
import pandas as pd
import yaml


# Schema produced by scraper.py V17.4. Order is the contract.
HOLDINGS_COLUMNS = ["ETF_Ticker", "ticker", "name", "weight", "Holdings_As_Of", "Date_Scraped"]


@dataclass(frozen=True)
class ETF:
    """One row from the ETF universe — ticker, tier name, point weight."""
    ticker: str
    tier: str
    points: int


# Backward-compat alias (some code may still reference Tier)
Tier = ETF


@dataclass(frozen=True)
class Sanitizer:
    """Filters garbage holdings and normalizes ticker symbols.

    Defaults mirror the Power Query `MasterLeaderboard` filter in
    Equity_Indices_.xlsb (exact filter at the bottom of the query):
        ([Ticker] <> null and [Ticker] <> "" and [Ticker] <> "$USD" and [Ticker] <> "USD")
        and [Company] <> "Short-Term Investment Trust - Invesco Government & Agency Portfolio"

    Plus the documented GOOG → GOOGL normalization that merges dual-class
    Alphabet shares (Power Query step `FixGoogle`).

    The legacy VBA sanitizer (more aggressive) is available via
    `blocked_name_patterns` if needed.
    """
    blocked_tickers: tuple[str, ...]
    blocked_name_patterns: tuple[str, ...]          # substring match (case-insensitive)
    blocked_name_exact: tuple[str, ...]             # exact equality match (case-insensitive)
    ticker_replacements: tuple[tuple[str, str], ...] # BRK-B → BRK.B, BF/B → BF.B
    ticker_renames: dict                             # GOOG → GOOGL (after replacements)
    cross_listing_patterns: tuple[tuple[str, str], ...] = ()  # regex (pattern, replacement) — collapses cross-listings (e.g. KRX 'A005930' → '005930')

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop blocked rows, standardize ticker punctuation, then apply renames."""
        if df.empty:
            return df.copy()
        out = df.copy()

        # 1. Ticker blocklist (case-insensitive exact match) + empty/null guard
        ticker_str = out["ticker"].astype(str).str.strip()
        mask = ticker_str.ne("") & ticker_str.str.lower().ne("nan") & ticker_str.notna()
        blocked_lower = {t.lower() for t in self.blocked_tickers}
        if blocked_lower:
            mask &= ~ticker_str.str.lower().isin(blocked_lower)

        # 2. Company name filters
        name_upper = out["name"].astype(str).str.upper().str.strip()
        for pat in self.blocked_name_patterns:
            mask &= ~name_upper.str.contains(pat.upper(), regex=False, na=False)
        blocked_exact_upper = {n.upper() for n in self.blocked_name_exact}
        if blocked_exact_upper:
            mask &= ~name_upper.isin(blocked_exact_upper)

        out = out[mask].copy()
        if out.empty:
            return out

        # 3. Ticker punctuation standardization (BRK-B → BRK.B, "8058 JP" → "8058.JP")
        cleaned = out["ticker"].astype(str).str.strip()
        for old, new in self.ticker_replacements:
            cleaned = cleaned.str.replace(old, new, regex=False)
        # Collapse consecutive dots produced by chained replacements (BF / B → BF..B → BF.B)
        cleaned = cleaned.str.replace(r"\.{2,}", ".", regex=True).str.strip(".")
        out["ticker"] = cleaned

        # 4. Ticker renames (GOOG → GOOGL) — merge dual-class shares
        if self.ticker_renames:
            out["ticker"] = out["ticker"].replace(self.ticker_renames)

        # 5. Cross-listing collapse (regex). E.g. KRX 'A005930' (Bloomberg form) and
        # bare '005930' (KRX exchange form) are the SAME security — collapse to one
        # canonical ticker so they don't fragment the leaderboard. Uses regex with
        # back-references (e.g. ^A(\d{6})$ → \1). High-confidence merges only —
        # patterns must be tight enough that false positives are negligible.
        merged_pairs: list[tuple[str, str]] = []
        if self.cross_listing_patterns:
            before = out["ticker"].copy()
            for pattern, replacement in self.cross_listing_patterns:
                out["ticker"] = out["ticker"].str.replace(pattern, replacement, regex=True)
            changed = before != out["ticker"]
            if changed.any():
                # Record (raw → canonical) pairs for audit; capped to avoid log spam.
                pairs = (
                    pd.DataFrame({"_raw": before[changed], "_canonical": out.loc[changed, "ticker"]})
                    .drop_duplicates()
                    .itertuples(index=False, name=None)
                )
                merged_pairs = list(pairs)

        # 6. Deduplicate rows created by renames OR cross-listing collapse
        # (e.g., GOOG+GOOGL same ETF/date → one row; A005930 + 005930 → one row)
        if self.ticker_renames or self.cross_listing_patterns:
            out = self._dedupe_after_renames(out)

        # 7. Audit log for cross-listing merges (printed once per call). Capped to
        # 20 examples; a summary line always emits when any merge occurs so CI
        # logs surface the magnitude.
        if merged_pairs:
            print(
                f"[sanitizer] cross-listing collapse: {len(merged_pairs)} unique raw→canonical mappings",
                flush=True,
            )
            for raw, canonical in merged_pairs[:20]:
                print(f"  {raw}  →  {canonical}", flush=True)
            if len(merged_pairs) > 20:
                print(f"  … and {len(merged_pairs) - 20} more", flush=True)

        return out

    def _dedupe_after_renames(self, df: pd.DataFrame) -> pd.DataFrame:
        """After GOOG→GOOGL or KRX A-prefix collapse, sum weights for duplicate
        (ETF_Ticker, ticker, Holdings_As_Of) keys.

        Without this, dual-class shares or cross-listings that get renamed to the
        same ticker create Cartesian-product explosions in downstream merges
        (build.py merge on [ETF_Ticker, ticker]).
        """
        if df.empty:
            return df
        if not self.ticker_renames and not self.cross_listing_patterns:
            return df
        key = ["ETF_Ticker", "ticker", "Holdings_As_Of"]
        if not all(c in df.columns for c in key):
            return df
        agg = df.groupby(key, as_index=False).agg(
            weight=("weight", "sum"),
            name=("name", "first"),
            Date_Scraped=("Date_Scraped", "max"),
        )
        # Restore column order (HOLDINGS_COLUMNS contract)
        return agg[["ETF_Ticker", "ticker", "name", "weight", "Holdings_As_Of", "Date_Scraped"]]


@dataclass(frozen=True)
class ConvictionConfig:
    """Parameters for the §24 conviction-weighted scoring formula."""
    ow_star:  float   # OW at which Wovr ≈ 1.0
    c_max:    float   # ceiling on Wovr
    rp_floor: float   # minimum Grank contribution
    hc_theta: float   # conviction threshold for HC breadth count

    def __post_init__(self):
        if self.ow_star <= 0:
            raise ValueError(f"ConvictionConfig.ow_star must be > 0, got {self.ow_star}")


@dataclass(frozen=True)
class HistoryConfig:
    rank_delta_lookback_days: int          # kept for backward compat (= delta_periods_days[1])
    delta_periods_days: tuple[int, ...]    # Phase 2: multi-period deltas
    leaderboard_lookback_days: int
    changelog_top_n: int


@dataclass(frozen=True)
class Config:
    sanitizer: Sanitizer
    etfs: tuple[ETF, ...]                               # was: tiers
    rank_breakpoints: tuple[tuple[int, float], ...]
    new_lookback_days: int
    new_bonus_mult: float
    new_bonus_tiers: tuple[str, ...]
    high_conviction_min_etfs: int
    history: HistoryConfig
    scoring_mode: str = "legacy"                        # 'legacy' | 'conviction'
    conviction: ConvictionConfig | None = None          # §24 params (conviction mode)
    etf_maturity_days: int = 30                         # §28 maturity gate

    @classmethod
    def from_yaml(cls, path: str | Path) -> "Config":
        cfg = yaml.safe_load(Path(path).read_text())
        san = cfg.get("sanitizer", {})
        # cross_listing_patterns: list of {pattern: regex, replacement: str} dicts
        # in YAML; loaded as ((pattern, replacement), ...) tuple here.
        cross_listing = tuple(
            (str(rule["pattern"]), str(rule.get("replacement", "")))
            for rule in san.get("cross_listing_patterns", [])
            if isinstance(rule, dict) and "pattern" in rule
        )
        sanitizer = Sanitizer(
            blocked_tickers=tuple(san.get("blocked_tickers", [])),
            blocked_name_patterns=tuple(san.get("blocked_name_patterns", [])),
            blocked_name_exact=tuple(san.get("blocked_name_exact", [])),
            ticker_replacements=tuple((k, v) for k, v in san.get("ticker_replacements", {}).items()),
            ticker_renames=dict(san.get("ticker_renames", {})),
            cross_listing_patterns=cross_listing,
        )
        etfs = tuple(
            ETF(ticker=e["ticker"], tier=e["tier"], points=int(e["points"]))
            for e in cfg["etfs"]
        )
        h_cfg = cfg.get("history", {})
        _periods_raw = h_cfg.get("delta_periods_days", None)
        _default_lookback = int(h_cfg.get("rank_delta_lookback_days", 7))
        if _periods_raw:
            _periods = tuple(int(p) for p in _periods_raw)
        else:
            _periods = (_default_lookback,)
        history = HistoryConfig(
            rank_delta_lookback_days=_default_lookback,
            delta_periods_days=_periods,
            leaderboard_lookback_days=int(h_cfg.get("leaderboard_lookback_days", 60)),
            changelog_top_n=int(h_cfg.get("changelog_top_n", 15)),
        )

        # §24 conviction config
        scoring_mode = cfg.get("scoring_mode", "legacy")
        cv_cfg = cfg.get("conviction", {})
        conviction = ConvictionConfig(
            ow_star=float(cv_cfg.get("ow_star", 4.0)),
            c_max=float(cv_cfg.get("c_max", 1.5)),
            rp_floor=float(cv_cfg.get("rp_floor", 0.35)),
            hc_theta=float(cv_cfg.get("hc_theta", 0.8)),
        ) if cv_cfg or scoring_mode == "conviction" else None

        return cls(
            sanitizer=sanitizer,
            etfs=etfs,
            rank_breakpoints=tuple((int(b["rank_max"]), float(b["multiplier"]))
                                   for b in cfg["rank_breakpoints"]),
            new_lookback_days=int(cfg["new_lookback_days"]),
            new_bonus_mult=float(cfg["new_bonus_mult"]),
            new_bonus_tiers=tuple(cfg["new_bonus_tiers"]),
            high_conviction_min_etfs=int(cfg["high_conviction_min_etfs"]),
            history=history,
            scoring_mode=scoring_mode,
            conviction=conviction,
            etf_maturity_days=int(cfg.get("etf_maturity_days", 30)),
        )

    def etf_lookup(self) -> dict[str, ETF]:
        """O(1) lookup from ticker to ETF metadata."""
        return {e.ticker: e for e in self.etfs}

    def etfs_in_tier(self, tier: str) -> tuple[str, ...]:
        """All ETF tickers belonging to the given tier."""
        return tuple(e.ticker for e in self.etfs if e.tier == tier)

    def all_tier_names(self) -> tuple[str, ...]:
        """Unique tier names in insertion order."""
        seen: set[str] = set()
        out: list[str] = []
        for e in self.etfs:
            if e.tier not in seen:
                seen.add(e.tier)
                out.append(e.tier)
        return tuple(out)

    def etf_tier_map(self) -> dict[str, ETF]:
        """Backward-compat alias for etf_lookup()."""
        return self.etf_lookup()

    # Backward-compat: some callers may access cfg.tiers to list ETF groups.
    # Return a tuple-of-ETF (same as self.etfs) so existing code doesn't break.
    @property
    def tiers(self) -> tuple[ETF, ...]:
        return self.etfs


def rank_multiplier(rank: int, breakpoints: Iterable[tuple[int, float]]) -> float:
    for rank_max, mult in breakpoints:
        if rank <= rank_max:
            return mult
    return 1.0


def conviction_multiplier(
    weight: float,
    rank: int,
    n_holdings: int,
    cv: "ConvictionConfig",
) -> float:
    """
    §24 conviction multiplier C_i = Wovr(OW_i) × Grank(RP_i).

    OW_i  = weight × n_holdings  (overweight vs equal-weight; 1.0 = equal-weight)
    RP_i  = 1 − (rank − 1) / n_holdings  (rank percentile; 1.0 = top of book)

    Wovr(OW) = clamp( ln(1+OW) / ln(1+OW★) , 0 , C_max )
    Grank(RP) = RP_floor + (1 − RP_floor) × RP

    Returns C_i ∈ [0, C_max × 1.0] (product of two factors, each ≤ their ceiling).
    """
    import math
    n = max(n_holdings, 1)
    ow = weight * n                                          # overweight ratio
    rp = 1.0 - (rank - 1) / n                               # rank percentile [0,1]

    # Wovr: log-compressed overweight, anchored at OW★ → 1.0
    ln_anchor = math.log(1 + cv.ow_star)
    wovr = math.log(1 + ow) / ln_anchor if ln_anchor > 0 else 0.0
    wovr = max(0.0, min(cv.c_max, wovr))

    # Grank: linear rank percentile with a floor so rank never fully zeroes out
    grank = cv.rp_floor + (1.0 - cv.rp_floor) * rp

    return wovr * grank


def _validate_input(df: pd.DataFrame) -> None:
    missing = set(HOLDINGS_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"Input DataFrame missing required columns: {missing}")


def compute_leaderboard(
    history: pd.DataFrame,
    cfg: Config,
    *,
    as_of: pd.Timestamp | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Compute the leaderboard as of a given date (default: latest in data).

    Args:
        history: long-form holdings history
        cfg: scoring config
        as_of: cap history to rows where Holdings_As_Of <= as_of.
               If None, uses the latest date in the data.

    Returns:
        (leaderboard, latest_holdings_with_scores)
    """
    _validate_input(history)
    df = history.copy()
    df["Holdings_As_Of"] = pd.to_datetime(df["Holdings_As_Of"], errors="coerce")
    df = df.dropna(subset=["Holdings_As_Of", "ticker", "weight"])

    # Restrict ETFs to those in config
    etf_lookup = cfg.etf_lookup()
    df = df[df["ETF_Ticker"].isin(etf_lookup)].copy()

    # Apply sanitizer (block list + ticker standardization)
    df = cfg.sanitizer.apply(df)
    if df.empty:
        empty = pd.DataFrame(columns=["ticker", "company", "final_score", "etf_count",
                                       "total_weight", "held_by", "tiers", "any_new",
                                       "best_rank", "flag", "leaderboard_rank"])
        return empty, pd.DataFrame()

    # Cap by as_of (time-travel support)
    if as_of is not None:
        as_of_ts = pd.Timestamp(as_of)
        df = df[df["Holdings_As_Of"] <= as_of_ts]

    if df.empty:
        empty = pd.DataFrame(columns=["ticker", "company", "final_score", "etf_count",
                                       "total_weight", "held_by", "tiers", "any_new",
                                       "best_rank", "flag", "leaderboard_rank"])
        return empty, pd.DataFrame()

    # Latest snapshot per ETF (within capped data)
    latest_dates = df.groupby("ETF_Ticker")["Holdings_As_Of"].transform("max")
    latest = df[df["Holdings_As_Of"] == latest_dates].copy()

    # Filter zero-weight rows before ranking/counting (they inflate etf_count/held_by)
    latest = latest[latest["weight"] > 0].copy()

    # Rank within each ETF by weight desc, deterministic tiebreak by ticker
    latest = latest.sort_values(["ETF_Ticker", "weight", "ticker"], ascending=[True, False, True])
    latest["rank"] = latest.groupby("ETF_Ticker").cumcount() + 1

    # ETF size (n_holdings) — needed for conviction formula and maturity gate
    etf_sizes = latest.groupby("ETF_Ticker")["ticker"].transform("count")
    latest["n_holdings"] = etf_sizes

    # §28 Maturity gate — track first_seen date per ETF in the full history
    # An ETF is "mature" if we have data for it spanning ≥ etf_maturity_days.
    etf_first_seen = df.groupby("ETF_Ticker")["Holdings_As_Of"].min()
    etf_last_seen  = df.groupby("ETF_Ticker")["Holdings_As_Of"].max()
    etf_span_days  = (etf_last_seen - etf_first_seen).dt.days
    mature_etfs = set(etf_span_days[etf_span_days >= cfg.etf_maturity_days].index)

    # NEW detection: not seen in this ETF before the cutoff window
    # §28: only flag NEW for mature ETFs (suppresses the 15→30 migration flood)
    cutoff = df["Holdings_As_Of"].max() - timedelta(days=cfg.new_lookback_days)
    historical_pairs = set(map(tuple, df.loc[df["Holdings_As_Of"] < cutoff,
                                              ["ETF_Ticker", "ticker"]].itertuples(index=False, name=None)))
    pair_index = pd.Series(list(zip(latest["ETF_Ticker"], latest["ticker"])), index=latest.index)
    is_new_raw = ~pair_index.isin(historical_pairs)
    # Suppress NEW flag for immature ETFs
    etf_is_mature = latest["ETF_Ticker"].map(lambda e: e in mature_etfs)
    latest["is_new"] = is_new_raw & etf_is_mature

    # Per-ETF tier and point lookup
    latest["tier"] = latest["ETF_Ticker"].map(lambda e: etf_lookup[e].tier)
    latest["tier_points"] = latest["ETF_Ticker"].map(lambda e: etf_lookup[e].points)

    # ── SCORE FORMULA — two modes ─────────────────────────────────────────────
    if cfg.scoring_mode == "conviction" and cfg.conviction is not None:
        # §24 Conviction-weighted formula:
        #   C_i        = Wovr(OW_i) × Grank(RP_i)        # overweight × rank percentile
        #   Single_i   = TierPoints_i × C_i              # each holding's contribution
        #   Final      = Σ_i Single_i                    # breadth of conviction (additive)
        #
        # The legacy additive `new_bonus = tier_points × 5` is suppressed in this
        # mode by design: it was a flat-per-row term that, applied UN-weighted by
        # conviction, allowed a single filler-rank NEW hit (e.g. PIE #82, conv≈0.12)
        # to inject ~200 raw points and override the conviction signal — see the
        # Samsung-vs-ITUB4 regression. Conviction mode keeps breadth additive
        # (Σ across ETFs) but only breadth *of conviction*. The is_new flag is
        # still computed and surfaced for display/changelog; it just doesn't
        # short-circuit the multiplier.
        cv = cfg.conviction
        latest["conviction"] = latest.apply(
            lambda row: conviction_multiplier(
                row["weight"], row["rank"], row["n_holdings"], cv
            ),
            axis=1,
        )
        latest["rank_mult"] = latest["conviction"]   # expose for downstream compat
        latest["weight_pct"] = latest["weight"] * 100.0
        latest["base_score"] = latest["tier_points"] * latest["conviction"]
        # Column kept (build.py emits it to holdings_latest.json) but always 0
        # in conviction mode — the bonus is folded out, not folded in.
        latest["new_bonus"] = 0.0
        latest["score"] = latest["base_score"]
    else:
        # Legacy Power Query formula:
        #   Single Score = Weight% × Points × Rank_Multiplier × 100 + New_Bonus
        latest["rank_mult"] = latest["rank"].apply(lambda r: rank_multiplier(r, cfg.rank_breakpoints))
        latest["weight_pct"] = latest["weight"] * 100.0
        latest["base_score"] = latest["weight_pct"] * latest["tier_points"] * latest["rank_mult"]
        eligible_new = latest["is_new"] & latest["tier"].isin(cfg.new_bonus_tiers)
        latest["new_bonus"] = (latest["tier_points"] * cfg.new_bonus_mult).where(eligible_new, 0.0)
        latest["score"] = latest["base_score"] + latest["new_bonus"]
        latest["conviction"] = latest["rank_mult"]   # expose for downstream compat

    # Aggregate per ticker
    agg = latest.groupby("ticker").agg(
        company=("name", "first"),
        final_score=("score", "sum"),
        etf_count=("ETF_Ticker", "nunique"),
        total_weight=("weight", "sum"),
        held_by=("ETF_Ticker", lambda s: ", ".join(sorted(s.unique()))),
        tiers=("tier", lambda s: " + ".join(sorted(set(s)))),
        any_new=("is_new", "any"),
        best_rank=("rank", "min"),
        avg_conviction=("conviction", "mean"),
    ).reset_index()

    # §25 Conviction breadth: k_c = count of ETFs where C_i ≥ θ
    if cfg.scoring_mode == "conviction" and cfg.conviction is not None:
        theta = cfg.conviction.hc_theta
        conviction_etfs = latest[latest["conviction"] >= theta].groupby("ticker")["ETF_Ticker"].nunique()
        agg["conviction_etf_count"] = agg["ticker"].map(conviction_etfs).fillna(0).astype(int)
    else:
        agg["conviction_etf_count"] = agg["etf_count"]

    def _flag(row) -> str:
        tier_set = set(row["tiers"].split(" + ")) if row["tiers"] else set()
        # §25: HC uses conviction breadth in conviction mode, raw count in legacy
        hc_count = row["conviction_etf_count"] if cfg.scoring_mode == "conviction" else row["etf_count"]
        if hc_count >= cfg.high_conviction_min_etfs:
            return "HIGH_CONVICTION"
        if "Trend" in tier_set and not (tier_set & {"Quality", "Scout"}):
            return "SPECULATIVE_BETA"
        return ""

    agg["flag"] = agg.apply(_flag, axis=1)
    agg = agg.sort_values(["final_score", "etf_count", "total_weight"], ascending=False).reset_index(drop=True)
    agg["leaderboard_rank"] = agg.index + 1

    # §27 Score normalization — percentile within today's universe (0–100)
    # Keep raw final_score for sorting; add normalized_score for display.
    n = len(agg)
    if n > 1:
        # Percentile rank: fraction of scores strictly below this score × 100
        agg["normalized_score"] = agg["final_score"].rank(method="average", pct=True).mul(100).round(1)
    else:
        agg["normalized_score"] = 50.0

    # Display rounding (matches Excel Int64.Type cast on Final Alpha Score).
    # Sort uses the precise float; final display is truncated to int.
    agg["final_score"] = agg["final_score"].astype("int64")
    agg["total_weight"] = agg["total_weight"].round(6)
    agg["avg_conviction"] = agg["avg_conviction"].round(3)

    return agg, latest


def compute_rank_deltas(
    history: pd.DataFrame,
    cfg: Config,
    lookback_days: int | None = None,
) -> pd.DataFrame:
    """
    For each (ETF, ticker), compute rank and weight change vs ~lookback_days ago.

    Positive rank_delta = rank improved (moved up). Positive weight_flow = weight grew.
    """
    if lookback_days is None:
        lookback_days = cfg.history.rank_delta_lookback_days  # default = delta_periods_days[1] or first

    etf_lookup = cfg.etf_lookup()
    df = history.copy()
    df["Holdings_As_Of"] = pd.to_datetime(df["Holdings_As_Of"], errors="coerce")
    df = df.dropna(subset=["Holdings_As_Of", "ticker", "weight"])
    df = df[df["weight"] > 0]  # exclude zero-weight rows (they inflate rank counts)
    df = df[df["ETF_Ticker"].isin(etf_lookup)]
    df = cfg.sanitizer.apply(df)
    if df.empty:
        return pd.DataFrame(columns=["ETF_Ticker", "ticker", "rank_now", "rank_then",
                                      "rank_delta", "weight_now", "weight_then", "weight_flow"])

    latest_date_per_etf = df.groupby("ETF_Ticker")["Holdings_As_Of"].max()
    latest_date_per_etf.index.name = "ETF_Ticker"

    def _ranked_at(date_per_etf: pd.Series) -> pd.DataFrame:
        d = df.merge(date_per_etf.rename("target_date").reset_index(), on="ETF_Ticker")
        d = d[d["Holdings_As_Of"] == d["target_date"]].copy()
        d = d.sort_values(["ETF_Ticker", "weight", "ticker"], ascending=[True, False, True])
        d["rank"] = d.groupby("ETF_Ticker").cumcount() + 1
        return d[["ETF_Ticker", "ticker", "rank", "weight"]]

    now_df = _ranked_at(latest_date_per_etf)

    # Past: closest snapshot ≤ (latest - lookback) per ETF
    target_dates = latest_date_per_etf - pd.Timedelta(days=lookback_days)
    snapshot_dates = df.groupby("ETF_Ticker")["Holdings_As_Of"].unique()
    chosen = {}
    for etf, dates in snapshot_dates.items():
        target = target_dates[etf]
        dates_le = [d for d in dates if d <= target]
        chosen[etf] = max(dates_le) if dates_le else min(dates)
    chosen_series = pd.Series(chosen, name="target_date")
    chosen_series.index.name = "ETF_Ticker"
    then_df = _ranked_at(chosen_series)

    merged = now_df.merge(then_df, on=["ETF_Ticker", "ticker"], how="left", suffixes=("_now", "_then"))
    merged["rank_delta"] = merged["rank_then"] - merged["rank_now"]   # positive = moved up
    merged["weight_flow"] = (merged["weight_now"] - merged["weight_then"]) / merged["weight_then"].replace(0, float('nan'))
    return merged
