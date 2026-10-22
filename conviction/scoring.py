"""
Conviction Labs — scoring engine.

The algorithm is the documented Conviction Labs scoring model from the etf-data README.
Sanitizer mirrors the ArchiveToDatabase_Production VBA sub.

Scoring modes:
    legacy     — original Power Query formula: weight% × points × rank_mult × 100
                 (+ flat NEW bonus for Scout/Quant entrants).
    conviction — §24 conviction-weighted: C_i = Wovr(OW_i) × Grank(RP_i),
                 Single_i = points_i × C_i, Final = Σ_i Single_i.
    apex       — §30 Conviction v3, revised in v4 (2026-10 study,
                 docs/SCORING_V4_STUDY.md). Per-holding conviction C_i as in
                 conviction mode, but convex: Single_i = points_i × C_i^κ
                 (κ = 2), so filler positions can't add up to a high score and
                 the score stays monotone in breadth. The per-ticker raw mass
                 B = Σ Single_i (Jaccard-overlap discounted; no tier-synergy
                 bonus — M_breadth below is the one breadth reward) is shaped
                 by three multipliers:

                 (a) Concentration (single-fund manipulation guard)
                     s_i    = score_i / Σ score_i      (holding share of mass)
                     HHI    = Σ s_i²                   (Herfindahl index)
                     N_eff  = 1 / HHI                  (effective ETF count)
                     M_conc = floor + (1 − floor) × min(1, (N_eff − 1)/(N_eff★ − 1))
                     clamped to [conc_floor, 1] — single-fund monsters stay
                     visible at ≥ conc_floor of their raw mass.

                 (b) Strategy breadth dispersal
                     p_t       = tier-t share of the ticker's mass
                     E         = −Σ_t p_t·ln(p_t)      (Shannon entropy)
                     M_breadth = 1 + α × E / ln(N_tiers)
                     Independent style co-sign (Scout+Quant+Quality…) earns up
                     to α uplift; mono-tier names get exactly 1.0.

                 (c) Universe / geographic normalization
                     O(ticker) = ETFs that EVER held it (weight > 0, full
                                 as_of-capped history)
                     OM        = Σ points_e over e ∈ O  (opportunity mass)
                     pen       = B / (OM + λ)           (shrunk penetration)
                     M_universe = clip((pen / median pen)^η, clip_lo, clip_hi)
                     Levels intl names with small opportunity sets without
                     false positives — η damps, λ shrinks, clip bounds.

                 Final = B × M_conc × M_breadth × M_universe — the single
                 canonical score (apex_score / apex_rank are aliases since v4).
                 New flag "CONCENTRATED": not HC and (s_max ≥ θ_share with
                 ≥ 2 ETFs, or a single-ETF holding at OW ≥ OW★), where s_max
                 is the max per-ETF share of linear conviction mass Σ points×C.

Inputs:
    history: long-form holdings, schema from yieldchaser/etf-data scraper.py.
             Columns: ETF_Ticker, ticker, name, weight, Holdings_As_Of, Date_Scraped

Outputs (from compute_leaderboard):
    leaderboard:  one row per ticker, sorted by final_score desc
    latest:       one row per (ETF, ticker) in latest snapshot, with score components
"""
from __future__ import annotations
import hashlib
import math
import re
from collections import Counter
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Iterable
import numpy as np
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
    region: str = "US"   # "intl" → reports local-exchange symbols (identity resolution)


# Backward-compat alias (some code may still reference Tier)
Tier = ETF


# ─── Sanitization memoization (build perf) ─────────────────────────────────
# During a single build, cfg.sanitizer.apply() is invoked from 4 call sites
# (compute_leaderboard, compute_rank_deltas ×6 periods, and two history.py
# paths) on what is functionally the SAME input dataframe — the same raw rows
# after the same copy/coerce/dropna/weight>0/etf-lookup pre-filter. Without
# caching, the full 192k-row sanitization (blocklist scan, ticker regex
# replacements, cross-listing collapse, dedupe) runs ~9 times, dominating
# the build at ~19 minutes and flooding CI logs with the 100-line
# cross-listing report on every call.
#
# The cache is keyed on a content fingerprint of (sanitizer config, input df).
# Identical inputs return a deep copy of the cached result; distinct inputs
# (each test case, each genuinely different dataframe) recompute. The
# cross-listing audit log now prints exactly once per DISTINCT input — on
# the cache miss — instead of once per call.
#
# Correctness guarantees:
#   * Cache hits return df.copy() — callers freely mutate `out` without
#     corrupting the cached canonical result.
#   * The fingerprint hashes columns + dtypes + a value hash, so any real
#     data change is a guaranteed miss.
#   * The cache is bounded (FIFO via pop(next(iter)), 16 entries) — no
#     unbounded memory growth.
_SANITIZER_CACHE: dict[tuple[str, str], "pd.DataFrame"] = {}
_SANITIZER_CACHE_MAX: int = 16


def _df_fingerprint(df: pd.DataFrame) -> str:
    """Content hash of a dataframe: shape + dtypes + order-sensitive row hash.

    Includes shape, column dtype signature, and an order-sensitive XOR-mixed
    row hash so that any real data change — including row REORDERING, which a
    plain additive sum cannot detect — produces a different key (cache miss).
    """
    try:
        row_hashes = pd.util.hash_pandas_object(df, index=False).values.astype(np.uint64)
        # Position-WEIGHTED sum: the old plain `sum()` (and any commutative
        # fold like XOR) is order-INSENSITIVE, while _compute's dedupe keeps
        # name="first" (order-sensitive) — two different inputs with the same
        # multiset of rows could alias to one cache entry and return the
        # wrong frame. Multiplying each hash by its 1-based position makes
        # any reorder change the fingerprint.
        weights = np.arange(1, row_hashes.size + 1, dtype=np.uint64)
        val = int((row_hashes * weights).sum() & np.uint64(0xFFFFFFFFFFFFFFFF)) if row_hashes.size else 0
    except TypeError:
        # Fallback for object-dtype columns hash_pandas_object can't handle
        val = hash(df.astype(str).values.tobytes()) & 0xFFFFFFFFFFFFFFFF
    dtypes_sig = "/".join(str(dt) for dt in df.dtypes)
    return f"{df.shape[0]}x{df.shape[1]}|{dtypes_sig}|{val}"


# ─── Security-identity resolution (v4) ─────────────────────────────────────
# Issuers report the same security in different symbol dialects, and bare
# local-exchange codes from international funds collide with unrelated US
# tickers. Both failure modes were measured on the live store (2026-10 study):
#   • collision — JHEM's 'MTN' (MTN Group, JSE) and AVSC's 'MTN' (Vail Resorts)
#     merged into one fabricated 4-fund HIGH_CONVICTION row; likewise KTB, A,
#     TEL, G, KEY, KMB, PRU, SAIL, VAL … (~50 tickers).
#   • fragmentation — '6857' (PIZ) vs '6857.JP' (FPXI, GRIN) split Advantest;
#     Thai 'ADVANC' / 'ADVANC.F' / 'ADVANC.R' split one company three ways.
# Every decision is name-verified: holder-reported company names must share a
# distinctive token, so blind suffix stripping can't merge e.g. Siemens Energy
# ('ENR.GR') into Energizer ('ENR') or Airbus ('AIR.FP') into AAR Corp ('AIR').

# Words that carry no identity: corporate forms, share-class / listing jargon,
# country names, and sector words common enough to create false overlaps.
_NAME_STOPWORDS = frozenset("""
    THE INC CORP CORPORATION COMPANY LTD LIMITED PLC AG NV SE SPA SAB CV PCL BHD TBK
    HOLDING HOLDINGS GROUP GRP COMMON STOCK CLASS SHS SHARES ORD ORDINARY ADR ADS GDR
    SPONSORED NVDR FOREIGN FORGN LOCAL REG UNIT UNITS PREF PREFERRED PREFERENCE PFD SER
    SERIES INTL INTERNATIONAL GLOBAL NEW AND FOR
    UNITED STATES AMERICA KOREA REPUBLIC HONG KONG INDIA BRAZIL CHINA TAIWAN THAILAND
    MEXICO JAPAN CHILE SOUTH AFRICA SAUDI ARABIA INDONESIA MALAYSIA PHILIPPINES POLAND
    TURKEY GREECE ISRAEL CANADA AUSTRALIA GERMANY FRANCE SPAIN ITALY KINGDOM
    BANK BANCORP FINANCIAL ENERGY TECHNOLOGY TECHNOLOGIES INDUSTRIES
    RESOURCES CAPITAL SYSTEMS MINING GOLD PROPERTIES HEALTHCARE SERVICES SOLUTIONS
    PARTNERS TRUST INVESTMENT INVESTMENTS ENTERPRISES MOTOR MOTORS POWER ELECTRIC
""".split())

# Exchange / listing-board suffixes issuers append to a root symbol
# ('6857.JP', '175.HK', 'ADVANC.R'). Share-class letters (A/B) are NOT here —
# '.B' etc. denote a different line of stock and are handled by curated renames.
_EXCHANGE_SUFFIXES = frozenset("""
    JP JT HK TT KS KQ LN AU NO FH SM SS IT IM PW BZ C CT CN US U GR GY FP NA SW DC
    SE SP BB AV ID R F UH QD SJ AB TB MK IJ PM CI TI GA
""".split())


def _name_tokens(name) -> frozenset[str]:
    """Distinctive uppercase tokens of a holder-reported company name."""
    if name is None or (isinstance(name, float) and math.isnan(name)):
        return frozenset()
    s = re.sub(r"['’.]", "", str(name).upper())       # Kohl's → KOHLS, Legalzoom.com → LEGALZOOMCOM
    s = re.sub(r"[^A-Z0-9]+", " ", s)
    return frozenset(
        t for t in s.split()
        if len(t) >= 3 and t not in _NAME_STOPWORDS
        and not t.isdigit() and not re.fullmatch(r"[A-Z]{3}\d[\d.]*", t)   # KRW100, INR10
    )


def _tokens_overlap(a: frozenset[str], b: frozenset[str]) -> bool:
    """True when two names plausibly denote the same company: they share a
    distinctive token exactly, or ≥ 2 tokens match as abbreviation prefixes
    ('IND + COMM BK' ↔ 'Industrial & Commercial Bank'). A single prefix hit is
    not enough — 'NEXT PLC' must not match 'NEXTPOWER INC'."""
    if a & b:
        return True
    prefix_hits = {x for x in a for y in b if x.startswith(y) or y.startswith(x)}
    return len(prefix_hits) >= 2


def clear_sanitizer_cache() -> None:
    """Clear the sanitizer memoization cache.

    Tests that mutate sanitizer config or pass a dataframe, then assert on a
    fresh computation, can call this to bypass the cache. Normal builds never
    need it — the cache is content-keyed so distinct inputs always miss.
    """
    _SANITIZER_CACHE.clear()


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
    resolve_identities: bool = False                 # v4 name-verified collision split + suffix merge
    identity_overrides: tuple[tuple[str, str, str], ...] = ()  # (ticker, name regex, new ticker) — row level
    international_etfs: tuple[str, ...] = ()         # funds reporting local-exchange symbols
    partial_snapshot_min_ratio: float = 0.0          # drop snapshots whose weight sum < ratio × trailing median (0 = off)

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop blocked rows, standardize ticker punctuation, then apply renames.

        Memoized: identical inputs return a deep copy of a cached result so the
        ~9 redundant calls during a build collapse to one computation per
        distinct input. See the module-level `_SANITIZER_CACHE` docstring for
        the correctness contract.
        """
        if df.empty:
            return df.copy()

        key = (self._config_fingerprint(), _df_fingerprint(df))
        cached = _SANITIZER_CACHE.get(key)
        if cached is not None:
            return cached.copy()

        out = self._compute(df)

        # Bounded LRU: evict oldest when at capacity.
        if len(_SANITIZER_CACHE) >= _SANITIZER_CACHE_MAX:
            _SANITIZER_CACHE.pop(next(iter(_SANITIZER_CACHE)))
        _SANITIZER_CACHE[key] = out
        return out.copy()

    def _config_fingerprint(self) -> str:
        """Stable hash of the sanitizer's own config (its frozen dataclass fields)."""
        payload = (
            self.blocked_tickers,
            self.blocked_name_patterns,
            self.blocked_name_exact,
            self.ticker_replacements,
            tuple(sorted(self.ticker_renames.items())) if self.ticker_renames else (),
            self.cross_listing_patterns,
            self.resolve_identities,
            self.international_etfs,
            self.identity_overrides,
            self.partial_snapshot_min_ratio,
        )
        return hashlib.sha256(repr(payload).encode("utf-8")).hexdigest()

    def _compute(self, df: pd.DataFrame) -> pd.DataFrame:
        """Actual sanitization (uncached path). Returns the canonical cleaned df."""
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

        # 5b. Curated row-level overrides for collisions the automatic split
        # can't adjudicate (two international exchanges sharing a numeric code,
        # e.g. '2313' = Compeq (TW) in EEMO/MFEM/PIE but Shenzhou (HK) in JHEM).
        # Matched on the row's company name, so they stay correct even when an
        # issuer's use of the symbol changes over time.
        identity_changes: list[tuple[str, str, str]] = []
        for tkr, name_re, new in self.identity_overrides:
            hit = out["ticker"].eq(tkr) & out["name"].astype(str).str.contains(
                name_re, case=False, regex=True, na=False)
            if hit.any():
                out.loc[hit, "ticker"] = new
                identity_changes.append(("override", tkr, new))

        # 5c. Name-verified identity resolution (collision split + suffix merge)
        if self.resolve_identities:
            out, resolved = self._resolve_identities(out)
            identity_changes += resolved

        # 6. Deduplicate rows created by renames OR cross-listing collapse
        # (e.g., GOOG+GOOGL same ETF/date → one row; A005930 + 005930 → one row)
        if self.ticker_renames or self.cross_listing_patterns or identity_changes:
            out = self._dedupe_after_renames(out)

        # 7. Partial-snapshot guard — runs on SANITIZED rows (cash lines removed)
        if self.partial_snapshot_min_ratio > 0:
            out = self._drop_partial_snapshots(out)

        if identity_changes:
            splits = [c for c in identity_changes if c[0] == "split"]
            merges = [c for c in identity_changes if c[0] == "merge"]
            overrides = [c for c in identity_changes if c[0] == "override"]
            print(f"[sanitizer] identity resolution: {len(splits)} collision splits, "
                  f"{len(merges)} suffix merges, {len(overrides)} curated overrides", flush=True)
            for kind, raw, canonical in (splits[:10] + merges[:10]):
                print(f"  {kind:5s} {raw}  ->  {canonical}", flush=True)

        # 7. Audit log for cross-listing merges. Runs only on the cache-miss path
        # (i.e. once per distinct input), so a build no longer floods CI with the
        # 100-line report on every one of the ~9 sanitizer invocations.
        if merged_pairs:
            print(
                f"[sanitizer] cross-listing collapse: {len(merged_pairs)} unique raw->canonical mappings",
                flush=True,
            )
            for raw, canonical in merged_pairs[:20]:
                print(f"  {raw}  ->  {canonical}", flush=True)
            if len(merged_pairs) > 20:
                print(f"  ... and {len(merged_pairs) - 20} more", flush=True)

        return out

    def _resolve_identities(self, df: pd.DataFrame) -> tuple[pd.DataFrame, list[tuple[str, str, str]]]:
        """Split cross-market ticker collisions, then merge exchange-suffix
        variants of the same security. Returns (df, [(kind, raw, canonical)]).

        Identity is decided per (ETF, ticker) pair over the whole input, so a
        holding keeps one identity across every snapshot date.

        Collision split — a ticker is split only when BOTH hold:
          • its holders' company names fall into ≥ 2 clusters with no
            distinctive token in common, and
          • exactly one cluster contains a US-domestic fund; the others are
            all international funds.
        The US cluster keeps the bare symbol (the US listing is the default
        namespace of a bare ticker); each international cluster becomes
        '<ticker>.<TAG>', TAG = its most common leading name token
        (MTN → 'MTN.MTN' for MTN Group, KTB → 'KTB.KRUNG').

        Suffix merge — 'ROOT.SUF' (SUF an exchange/board code) collapses into
        an identity of ROOT only when exactly one such identity shares a name
        token with it.
        """
        intl = set(self.international_etfs)
        names = df[["ETF_Ticker", "ticker", "name"]].copy()
        names["name"] = names["name"].astype(str).str.strip()
        names = names[~names["name"].str.lower().isin(("", "nan", "none", "null", "n/a"))]
        if names.empty:
            return df, []
        rep = (names.groupby(["ETF_Ticker", "ticker"])["name"]
               .agg(lambda s: s.value_counts().index[0]))
        tokens = {k: _name_tokens(v) for k, v in rep.items()}

        def _lead_token(name: str, distinctive: frozenset[str]) -> str:
            """First word of `name` that is a distinctive token (the tag source)."""
            words = re.sub(r"[^A-Z0-9]+", " ", re.sub(r"['’.]", "", name.upper())).split()
            return next((w for w in words if w in distinctive), "")

        # ── collision split ───────────────────────────────────────────────
        pair_map: dict[tuple[str, str], str] = {}
        identity_tokens: dict[str, set[str]] = {}
        changes: list[tuple[str, str, str]] = []
        by_ticker: dict[str, list[str]] = {}
        for (etf, tkr) in tokens:
            by_ticker.setdefault(tkr, []).append(etf)
        for tkr, etfs in by_ticker.items():
            known = [e for e in sorted(etfs) if tokens[(e, tkr)]]
            # Single-linkage clustering of holders by company-name overlap
            clusters: list[list[str]] = []
            for e in known:
                linked = [c for c in clusters
                          if any(_tokens_overlap(tokens[(e, tkr)], tokens[(o, tkr)]) for o in c)]
                clusters = [c for c in clusters if c not in linked] + [[e] + [x for c in linked for x in c]]
            home = [c for c in clusters if any(e not in intl for e in c)]
            if len(clusters) < 2 or len(home) != 1:
                identity_tokens.setdefault(tkr, set()).update(*(tokens[(e, tkr)] for e in known))
                continue
            identity_tokens.setdefault(tkr, set()).update(*(tokens[(e, tkr)] for e in home[0]))
            for c in clusters:
                if c is home[0]:
                    continue
                distinctive = frozenset().union(*(tokens[(e, tkr)] for e in c))
                leads = Counter(_lead_token(rep[(e, tkr)], distinctive) for e in c)
                tag = (sorted(leads.items(), key=lambda kv: (-kv[1], kv[0]))[0][0] or "X")[:10]
                new = f"{tkr}.{tag}"
                for e in c:
                    pair_map[(e, tkr)] = new
                identity_tokens.setdefault(new, set()).update(distinctive)
                changes.append(("split", f"{tkr}@{'/'.join(sorted(c))}", new))

        # ── suffix merge ──────────────────────────────────────────────────
        ticker_map: dict[str, str] = {}
        split_children: dict[str, list[str]] = {}
        for new in set(pair_map.values()):
            split_children.setdefault(new.rsplit(".", 1)[0], []).append(new)
        for tkr in by_ticker:
            if "." not in tkr:
                continue
            root, suf = tkr.rsplit(".", 1)
            if suf not in _EXCHANGE_SUFFIXES or root not in identity_tokens:
                continue
            own = identity_tokens.get(tkr, set())
            if not own:
                continue
            cands = [root] + split_children.get(root, [])
            hits = [c for c in cands if identity_tokens.get(c) and
                    _tokens_overlap(frozenset(own), frozenset(identity_tokens[c]))]
            if len(hits) == 1:
                ticker_map[tkr] = hits[0]
                changes.append(("merge", tkr, hits[0]))

        if not pair_map and not ticker_map:
            return df, []
        out = df.copy()
        if pair_map:
            keys = pd.Series(list(zip(out["ETF_Ticker"], out["ticker"])), index=out.index)
            mapped = keys.map(pair_map)
            out["ticker"] = mapped.where(mapped.notna(), out["ticker"])
        if ticker_map:
            out["ticker"] = out["ticker"].replace(ticker_map)
        return out, sorted(changes)

    def _drop_partial_snapshots(self, df: pd.DataFrame) -> pd.DataFrame:
        """Drop fund snapshots that were scraped incompletely.

        A partial scrape (e.g. EEMO 2026-08-29: 231 of ~261 rows, weights
        summing to 0.72, its #1 holding SK hynix missing) is otherwise read as
        that fund's current book: names "exit", ranks spike for a day or two,
        and the spike trips BURST/CRATER, exits and score deltas. A snapshot is
        partial when its weight sum is below `partial_snapshot_min_ratio` × the
        median of the fund's previous 10 snapshots (≥ 3 needed). Only PAST
        snapshots form the reference, so the decision is as-of safe; dropping
        a snapshot makes the as-of logic fall back to the fund's last complete
        book. A genuine reconstitution (fewer names, weights still ≈ 100%)
        passes untouched.
        """
        key = ["ETF_Ticker", "Holdings_As_Of"]
        if df.empty or not all(c in df.columns for c in key + ["weight"]):
            return df
        sums = df[df["weight"] > 0].groupby(key, as_index=False)["weight"].sum()
        sums["_t"] = pd.to_datetime(sums["Holdings_As_Of"], errors="coerce")
        sums = sums.sort_values(["ETF_Ticker", "_t"])
        ref = sums.groupby("ETF_Ticker")["weight"].transform(
            lambda s: s.shift(1).rolling(10, min_periods=3).median())
        bad = sums[sums["weight"] < self.partial_snapshot_min_ratio * ref]
        if bad.empty:
            return df
        drop = pd.MultiIndex.from_frame(bad[key])
        mask = pd.MultiIndex.from_frame(df[key]).isin(drop)
        print(f"[sanitizer] partial-snapshot guard: dropped {len(bad)} incomplete fund snapshot(s): "
              + ", ".join(f"{e} {str(d)[:10]}" for e, d in list(bad[key].itertuples(index=False, name=None))[:12]),
              flush=True)
        return df[~mask]

    def _dedupe_after_renames(self, df: pd.DataFrame) -> pd.DataFrame:
        """After GOOG→GOOGL or KRX A-prefix collapse, sum weights for duplicate
        (ETF_Ticker, ticker, Holdings_As_Of) keys.

        Without this, dual-class shares or cross-listings that get renamed to the
        same ticker create Cartesian-product explosions in downstream merges
        (build.py merge on [ETF_Ticker, ticker]).
        """
        if df.empty:
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
    kappa: float = 1.0  # per-holding convexity: Single_i = points × C_i^κ (v4: κ = 2)
    overlap_discount_enabled: bool = False
    overlap_penalty_weight: float = 1.5
    min_overlap_multiplier: float = 0.20
    tier_diversity_bonus: float = 0.15

    def __post_init__(self):
        if self.ow_star <= 0:
            raise ValueError(f"ConvictionConfig.ow_star must be > 0, got {self.ow_star}")



@dataclass(frozen=True)
class ApexConfig:
    """§30 apex-mode parameters (Conviction v3)."""
    conc_floor: float = 0.60        # minimum concentration multiplier (single-fund bets stay visible)
    conc_target_neff: float = 3.0   # effective-ETF count at which concentration penalty fully releases
    breadth_alpha: float = 0.35     # max uplift from full strategy-tier dispersal
    universe_eta: float = 0.50      # exponent damping universe normalization
    universe_lambda: float = 40.0   # shrinkage pseudo-mass (≈ one median fund's points)
    universe_clip_lo: float = 0.80  # floor on universe multiplier
    universe_clip_hi: float = 1.25  # ceiling on universe multiplier
    conc_flag_share: float = 0.85   # max-share threshold for CONCENTRATED flag
                                    # (one fund supplies ≥85% of score mass — above
                                    # this universe's structural median of ~0.81)
    def __post_init__(self):
        if self.conc_target_neff <= 1:
            raise ValueError(f"ApexConfig.conc_target_neff must be > 1, got {self.conc_target_neff}")
        if not 0.0 <= self.conc_floor <= 1.0:
            raise ValueError(f"ApexConfig.conc_floor must be in [0, 1], got {self.conc_floor}")
        if self.universe_clip_lo > self.universe_clip_hi:
            raise ValueError(
                f"ApexConfig.universe_clip_lo ({self.universe_clip_lo}) must be "
                f"<= universe_clip_hi ({self.universe_clip_hi})"
            )


@dataclass(frozen=True)
class MotionConfig:
    """BURST / CRATER (2026-10 redesign, docs/SIGNALS_STUDY.md).

    On the leaderboard-rank series smoothed by a rolling median of
    `smooth_snapshots` (a one-day spike can't move it):
      BURST  — log(R̃[t−k] / R̃[t]) ≥ min_log_rank_move, R̃[t] ≤ relevance_rank,
               R̃[t] within persist_log_tol of the window's best, and the name
               GAINED ≥ 1 holding fund over the k snapshots.
      CRATER — log(R̃[t] / R̃[t−k]) ≥ min_log_rank_move, R̃[t−k] ≤ relevance_rank,
               R̃[t] within persist_log_tol of the window's worst, and the name
               LOST ≥ 1 holding fund.
    """
    smooth_snapshots: int = 3
    lookback_snapshots: int = 10
    min_log_rank_move: float = 0.5      # ≈ rank ×1.65 better/worse (e.g. #250 → #150)
    relevance_rank: int = 300
    persist_log_tol: float = 0.15
    require_fund_confirmation: bool = True


@dataclass(frozen=True)
class SignalsConfig:
    """Screening signals (2026-10 audit, docs/SIGNALS_STUDY.md §4)."""
    funds_rank_max: int = 200       # Funds ↑ / ↓ apply to names inside this rank
    stealth_rank_max: int = 500     # Stealth applies inside this rank
    pre_hc_min_etfs: int = 3        # Pre-HC: holders needed (HC needs high_conviction_min_etfs)


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
    scoring_mode: str = "legacy"                        # 'legacy' | 'conviction' | 'apex'
    conviction: ConvictionConfig | None = None          # §24 params (conviction + apex modes)
    etf_maturity_days: int = 30                         # §28 maturity gate
    apex: ApexConfig | None = None                      # §30 params (apex mode)
    high_conviction_min_conviction_etfs: int = 0         # holders at C ≥ hc_theta (non-benchmark)
    high_conviction_min_tiers: int = 0                   # distinct non-benchmark strategy tiers
    high_conviction_exclude_tiers: tuple[str, ...] = ()  # benchmark tiers (Core) — not conviction
    motion: "MotionConfig | None" = None                 # BURST / CRATER parameters
    signals: "SignalsConfig" = SignalsConfig()           # screening chips
    stale_snapshot_days: int | None = None              # v4: drop ETF snapshots this much older than the freshest

    @classmethod
    def from_yaml(cls, path: str | Path) -> "Config":
        # Explicit UTF-8: config.yaml contains non-ASCII chars (→ arrows, etc.).
        # On Windows the default encoding (CP1252) would raise UnicodeDecodeError.
        cfg = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        san = cfg.get("sanitizer", {})
        # cross_listing_patterns: list of {pattern: regex, replacement: str} dicts
        # in YAML; loaded as ((pattern, replacement), ...) tuple here.
        cross_listing = tuple(
            (str(rule["pattern"]), str(rule.get("replacement", "")))
            for rule in san.get("cross_listing_patterns", [])
            if isinstance(rule, dict) and "pattern" in rule
        )
        etfs = tuple(
            ETF(ticker=e["ticker"], tier=e["tier"], points=int(e["points"]),
                region=str(e.get("region", "US")))
            for e in cfg["etfs"]
        )
        sanitizer = Sanitizer(
            blocked_tickers=tuple(san.get("blocked_tickers", [])),
            blocked_name_patterns=tuple(san.get("blocked_name_patterns", [])),
            blocked_name_exact=tuple(san.get("blocked_name_exact", [])),
            ticker_replacements=tuple((k, v) for k, v in san.get("ticker_replacements", {}).items()),
            ticker_renames=dict(san.get("ticker_renames", {})),
            cross_listing_patterns=cross_listing,
            resolve_identities=bool(san.get("resolve_identities", False)),
            identity_overrides=tuple(
                (str(r["ticker"]), str(r["name"]), str(r["to"]))
                for r in san.get("identity_overrides", [])
                if isinstance(r, dict) and {"ticker", "name", "to"} <= set(r)
            ),
            international_etfs=tuple(e.ticker for e in etfs if e.region == "intl"),
            partial_snapshot_min_ratio=float(san.get("partial_snapshot_min_ratio", 0.0)),
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

        # §24 conviction config — apex mode REQUIRES it (per-holding path is shared)
        scoring_mode = cfg.get("scoring_mode", "legacy")
        cv_cfg = cfg.get("conviction", {})
        conviction = ConvictionConfig(
            ow_star=float(cv_cfg.get("ow_star", 4.0)),
            c_max=float(cv_cfg.get("c_max", 1.5)),
            rp_floor=float(cv_cfg.get("rp_floor", 0.35)),
            hc_theta=float(cv_cfg.get("hc_theta", 0.8)),
            kappa=float(cv_cfg.get("kappa", 1.0)),
            overlap_discount_enabled=bool(cv_cfg.get("overlap_discount_enabled", False)),
            overlap_penalty_weight=float(cv_cfg.get("overlap_penalty_weight", 1.5)),
            min_overlap_multiplier=float(cv_cfg.get("min_overlap_multiplier", 0.20)),
            tier_diversity_bonus=float(cv_cfg.get("tier_diversity_bonus", 0.15)),
        ) if cv_cfg or scoring_mode in ("conviction", "apex") else None

        # §30 apex config — optional block; every key falls back to its default
        ax_cfg = cfg.get("apex", {})
        apex = ApexConfig(
            conc_floor=float(ax_cfg.get("conc_floor", 0.60)),
            conc_target_neff=float(ax_cfg.get("conc_target_neff", 3.0)),
            breadth_alpha=float(ax_cfg.get("breadth_alpha", 0.35)),
            universe_eta=float(ax_cfg.get("universe_eta", 0.50)),
            universe_lambda=float(ax_cfg.get("universe_lambda", 40.0)),
            universe_clip_lo=float(ax_cfg.get("universe_clip_lo", 0.80)),
            universe_clip_hi=float(ax_cfg.get("universe_clip_hi", 1.25)),
            conc_flag_share=float(ax_cfg.get("conc_flag_share", 0.85)),
        ) if ax_cfg or scoring_mode == "apex" else None

        return cls(
            sanitizer=sanitizer,
            etfs=etfs,
            rank_breakpoints=tuple((int(b["rank_max"]), float(b["multiplier"]))
                                   for b in cfg["rank_breakpoints"]),
            new_lookback_days=int(cfg["new_lookback_days"]),
            new_bonus_mult=float(cfg["new_bonus_mult"]),
            new_bonus_tiers=tuple(cfg["new_bonus_tiers"]),
            high_conviction_min_etfs=int(cfg["high_conviction_min_etfs"]),
            high_conviction_min_conviction_etfs=int(cfg.get("high_conviction_min_conviction_etfs", 0)),
            high_conviction_min_tiers=int(cfg.get("high_conviction_min_tiers", 0)),
            high_conviction_exclude_tiers=tuple(cfg.get("high_conviction_exclude_tiers", [])),
            motion=MotionConfig(**cfg["motion"]) if cfg.get("motion") else MotionConfig(),
            signals=SignalsConfig(**cfg["signals"]) if cfg.get("signals") else SignalsConfig(),
            history=history,
            scoring_mode=scoring_mode,
            conviction=conviction,
            etf_maturity_days=int(cfg.get("etf_maturity_days", 30)),
            apex=apex,
            stale_snapshot_days=(int(cfg["stale_snapshot_days"])
                                 if cfg.get("stale_snapshot_days") is not None else None),
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


def compute_etf_overlap_matrix(latest_df: pd.DataFrame) -> dict[str, dict[str, float]]:
    """
    Computes pairwise Jaccard similarity of tickers held across all active ETFs.
    Returns similarity[etf1][etf2] = intersection / union.
    """
    etfs = sorted(latest_df["ETF_Ticker"].unique())
    etf_sets = {e: set(latest_df[latest_df["ETF_Ticker"] == e]["ticker"].unique()) for e in etfs}
    matrix = {}
    for e1 in etfs:
        matrix[e1] = {}
        for e2 in etfs:
            if e1 == e2:
                matrix[e1][e2] = 1.0
            else:
                inter = len(etf_sets[e1] & etf_sets[e2])
                union = len(etf_sets[e1] | etf_sets[e2])
                matrix[e1][e2] = inter / union if union > 0 else 0.0
    return matrix


_NAME_INVALID = ("none", "nan", "null", "n/a", "undefined", "unknown")

# Some fund files publish names as "<ISSUER> COMMON STOCK USD.01" or
# "<ISSUER> <COUNTRY OF DOMICILE>" in capitals. Stripped only from all-caps
# names, which is the format that carries them.
_NAME_COUNTRY_SUFFIX = re.compile(
    r"\s+(UNITED STATES OF AMERICA(\s*\(THE\))?|HONG KONG|INDIA|SOUTH AFRICA|"
    r"KOREA\s*\(THE REPUBLIC OF\)|SAUDI ARABIA|THAILAND|INDONESIA|MALAYSIA|"
    r"TAIWAN\s*\(PROVINCE OF CHINA\)|CHINA|BRAZIL|MEXICO|CHILE|PERU|COLOMBIA|"
    r"PHILIPPINES(\s*\(THE\))?|POLAND|TURKEY|TURKIYE|GREECE|QATAR|KUWAIT|UNITED ARAB EMIRATES|"
    r"EGYPT|HUNGARY|CZECHIA|JAPAN|SINGAPORE|ISRAEL)\s*$")
_NAME_SHARE_SUFFIX = re.compile(
    r"\s+(COMMON STOCK|ORDINARY SHARES?|ORD SHS?|NVDR|(FOREIGN\s+)+SH\.?|(CLASS|CL)\s+[A-Z](\s+SHARES?)?|SHARES)"
    r"(\s+[A-Z]{3}\s*[\d.]+)?(\s+[A-Z])?\s*$")
_NAME_KEEP_LOWER = {"INC": "Inc", "CORP": "Corp", "CO": "Co", "LTD": "Ltd", "THE": "The", "AND": "and",
                    "OF": "of", "GRP": "Grp", "INTL": "Intl", "BHD": "Bhd", "TBK": "Tbk",
                    # short real words that are not acronyms
                    "AN": "An", "ON": "On", "AT": "At", "BY": "By", "FOR": "for", "NEW": "New", "ONE": "One",
                    "AIR": "Air", "OIL": "Oil", "GAS": "Gas", "SEA": "Sea", "SUN": "Sun", "BIG": "Big",
                    "TOP": "Top", "BIO": "Bio", "CAR": "Car", "BAY": "Bay", "DE": "de", "LA": "La"}


def clean_company_name(name: str) -> str:
    """Readable issuer name from a fund-file string. Mixed-case names are
    returned unchanged; all-caps names lose share-class / domicile noise
    and are re-cased (tokens of ≤ 3 letters such as AMD, SK or MP stay
    upper case, since they are usually acronyms)."""
    s = re.sub(r"\s+", " ", str(name)).strip()
    if re.search(r"/the$", s, re.I):          # "Cigna Group/The" -> "The Cigna Group"
        s = "The " + s[:-4] if s != s.upper() else s
    if not s or s != s.upper():
        return s
    for _ in range(2):   # "… INC COMMON STOCK USD.01" or "… CO LTD HONG KONG"
        s = _NAME_SHARE_SUFFIX.sub("", s)
        s = _NAME_COUNTRY_SUFFIX.sub("", s)
    s = re.sub(r"\s+[A-Z]\s*$", "", s) if len(s.split()) > 2 else s   # trailing share-class letter
    if s.endswith("/THE"):
        s = "The " + s[:-4]
    words = []
    for w in s.split(" "):
        core = re.sub(r"[^A-Z]", "", w)
        if w in _NAME_KEEP_LOWER:
            words.append(_NAME_KEEP_LOWER[w])
        elif len(core) <= 3:
            words.append(w)
        else:
            words.append(re.sub(r"[A-Z]+", lambda m: m.group(0).capitalize(), w))
    out = " ".join(words)
    return out[0].upper() + out[1:] if out else str(name).strip()


def _first_valid_name(series: pd.Series) -> str:
    """Best display name among the holders' name strings: a mixed-case name
    (the cleaner fund-file format) wins; otherwise the cleaned capitals."""
    best = ""
    for val in series:
        if val is None or not pd.notna(val):
            continue
        s = str(val).strip()
        if not s or s.lower() in _NAME_INVALID:
            continue
        if s != s.upper():
            return clean_company_name(s)
        best = best or s
    return clean_company_name(best) if best else ""


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

    # v4 staleness guard — an ETF whose newest snapshot is older than
    # stale_snapshot_days relative to the freshest snapshot available at as_of
    # stops scoring instead of contributing its frozen book forever. Sized well
    # above the longest observed scrape outage (21 days, COWZ/CALF) so a
    # transient gap never fakes an exit; it only retires a feed that has died.
    if cfg.stale_snapshot_days is not None and not latest.empty:
        freshest = latest["Holdings_As_Of"].max()
        latest = latest[latest["Holdings_As_Of"] >= freshest - timedelta(days=cfg.stale_snapshot_days)]

    # Filter zero-weight rows before ranking/counting (they inflate etf_count/held_by)
    latest = latest[latest["weight"] > 0].copy()
    if latest.empty:
        empty = pd.DataFrame(columns=["ticker", "company", "final_score", "etf_count",
                                       "total_weight", "held_by", "tiers", "any_new",
                                       "best_rank", "flag", "leaderboard_rank"])
        return empty, pd.DataFrame()

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
    #
    # Per-ETF cutoff: a lagging ETF whose latest snapshot is older than the
    # global max must NOT have its newest holdings silently classified as
    # historical (which would permanently suppress is_new for them). Compute
    # the lookback cutoff relative to each ETF's own latest date instead of
    # the global max. (BUG-11: global cutoff penalized lagging ETFs.)
    etf_cutoff = (etf_last_seen - timedelta(days=cfg.new_lookback_days)).to_dict()
    cutoff_global = df["Holdings_As_Of"].max() - timedelta(days=cfg.new_lookback_days)
    # `<=` boundary: a pair whose most recent prior appearance falls EXACTLY on
    # the cutoff date has been held for the full lookback window and must NOT
    # be flagged NEW (README: "absent from the 14-day lookback window"). The
    # old strict `<` made seen-exactly-14-days-ago count as brand new.
    hist_mask = [
        asof <= etf_cutoff.get(etf, cutoff_global)
        for etf, asof in zip(df["ETF_Ticker"], df["Holdings_As_Of"])
    ]
    historical_pairs = set(map(tuple, df.loc[hist_mask,
                                              ["ETF_Ticker", "ticker"]].itertuples(index=False, name=None)))
    pair_index = pd.Series(list(zip(latest["ETF_Ticker"], latest["ticker"])), index=latest.index)
    is_new_raw = ~pair_index.isin(historical_pairs)
    # Suppress NEW flag for immature ETFs
    etf_is_mature = latest["ETF_Ticker"].map(lambda e: e in mature_etfs)
    latest["is_new"] = is_new_raw & etf_is_mature

    # Per-ETF tier and point lookup
    latest["tier"] = latest["ETF_Ticker"].map(lambda e: etf_lookup[e].tier)
    latest["tier_points"] = latest["ETF_Ticker"].map(lambda e: etf_lookup[e].points)

    # ── SCORE FORMULA — three modes (apex shares the conviction per-holding path)
    if cfg.scoring_mode in ("conviction", "apex") and cfg.conviction is not None:
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
        # v4 convexity: Single_i = points × C_i^κ. With κ = 2 a filler held at
        # C = 0.3 contributes 0.09 of a full position while C = 1.2 contributes
        # 1.44 — breadth of *conviction*, not breadth of filler. Unlike the
        # retired avg-conviction factors this is per holding, so adding a holder
        # can never lower a name's score (monotone in breadth).
        # Apex only — conviction mode stays the exact §24 reference formula.
        kappa = cv.kappa if cfg.scoring_mode == "apex" else 1.0
        latest["base_score"] = latest["tier_points"] * latest["conviction"].pow(kappa)
        # Linear conviction mass Σ points × C — the basis of the dominance
        # diagnostics (max_etf_share, top_etf_share), whose calibrated
        # thresholds (CONCENTRATED at 0.85) predate κ and keep their meaning.
        latest["linear_mass"] = latest["tier_points"] * latest["conviction"]
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
        latest["linear_mass"] = latest["score"]

    # Aggregate per ticker
    agg = latest.groupby("ticker").agg(
        company=("name", _first_valid_name),
        final_score=("score", "sum"),
        etf_count=("ETF_Ticker", "nunique"),
        total_weight=("weight", "sum"),
        held_by=("ETF_Ticker", lambda s: ", ".join(sorted(s.unique()))),
        tiers=("tier", lambda s: " + ".join(sorted(set(s)))),
        any_new=("is_new", "any"),
        best_rank=("rank", "min"),
        avg_conviction=("conviction", "mean"),
    ).reset_index()

    # Backfill missing names from full historical parquet if current latest was blank
    empty_company = agg["company"].isna() | (agg["company"].str.strip() == "")
    if empty_company.any() and "name" in df.columns:
        valid_hist = df[df["name"].notna() & (df["name"].str.strip() != "")]
        if not valid_hist.empty:
            hist_name_map = valid_hist.groupby("ticker")["name"].last().to_dict()
            agg.loc[empty_company, "company"] = agg.loc[empty_company, "ticker"].map(hist_name_map).fillna("")

    # Apply ETF Overlap Discount & Tier Synergy Scaling (Option 1)
    cv = cfg.conviction
    if cfg.scoring_mode in ("conviction", "apex") and cv is not None and getattr(cv, "overlap_discount_enabled", False):
        overlap_matrix = compute_etf_overlap_matrix(latest)
        penalty_weight = getattr(cv, "overlap_penalty_weight", 1.5)
        floor_mult = getattr(cv, "min_overlap_multiplier", 0.20)
        # Apex mode rewards strategy dispersal through M_breadth (entropy); the
        # legacy per-tier synergy bonus on top of it double-counted breadth
        # (up to 1.60 × 1.35 = 2.16× for a 5-tier name), so it is off in apex.
        tier_bonus = 0.0 if cfg.scoring_mode == "apex" else getattr(cv, "tier_diversity_bonus", 0.15)
        
        # Hot path: this block runs inside EVERY historical_leaderboards call
        # (98× per build). The naive per-ticker group.sort_values() + iterrows()
        # + scalar pandas cell lookups cost ~5.3s per call; a single pre-sorted
        # itertuples pass with plain dict access does identical math in ~1/3 of
        # the time (measured on the real 474k-row store).
        ticker_tiers = latest.groupby("ticker")["tier"].nunique().to_dict()
        sub = latest[["ticker", "ETF_Ticker", "score"]].sort_values(
            ["ticker", "score"], ascending=[True, False], kind="stable"
        )
        ticker_scores: dict[str, float] = {}
        cur_ticker: str | None = None
        included_etfs: list[str] = []
        disc_score = 0.0
        for tkr, etf, sc in sub.itertuples(index=False, name=None):
            if tkr != cur_ticker:
                if cur_ticker is not None:
                    n_tiers = ticker_tiers.get(cur_ticker, 1)
                    ticker_scores[cur_ticker] = disc_score * (1.0 + (n_tiers - 1) * tier_bonus)
                cur_ticker = tkr
                included_etfs = []
                disc_score = 0.0
            row_sims = overlap_matrix[etf]
            if not included_etfs:
                mult = 1.0
            else:
                max_sim = max(row_sims[prev_e] for prev_e in included_etfs)
                mult = max(floor_mult, 1.0 - (max_sim * penalty_weight))
            included_etfs.append(etf)
            disc_score += sc * mult
        if cur_ticker is not None:
            n_tiers = ticker_tiers.get(cur_ticker, 1)
            ticker_scores[cur_ticker] = disc_score * (1.0 + (n_tiers - 1) * tier_bonus)

        agg["final_score"] = agg["ticker"].map(ticker_scores).fillna(agg["final_score"])

    # §25 Conviction breadth: k_c = count of ETFs where C_i ≥ θ, and the
    # number of distinct strategy tiers those funds span. Benchmark tiers
    # (Core: cap-weighted XLG / QQQM) are excluded — they "overweight" the
    # largest companies by construction, which is size, not conviction.
    excluded = set(cfg.high_conviction_exclude_tiers)
    signal = latest[~latest["tier"].isin(excluded)]
    agg["strategy_tier_count"] = agg["ticker"].map(
        signal.groupby("ticker")["tier"].nunique()).fillna(0).astype(int)
    if cfg.scoring_mode in ("conviction", "apex") and cfg.conviction is not None:
        theta = cfg.conviction.hc_theta
        conviction_etfs = signal[signal["conviction"] >= theta].groupby("ticker")["ETF_Ticker"].nunique()
        agg["conviction_etf_count"] = agg["ticker"].map(conviction_etfs).fillna(0).astype(int)
    else:
        agg["conviction_etf_count"] = agg["etf_count"]

    # ── §30 Concentration / breadth / universe diagnostics ───────────────────
    # Computed in ALL modes so the JSON schema stays stable. The multipliers
    # only deviate from 1.0 in apex mode; hhi / n_eff / tier_entropy are
    # purely descriptive in legacy/conviction modes.
    #
    #   s_i        = score_i / Σ score_i            (holding share of mass)
    #   HHI        = Σ s_i²          N_eff = 1/HHI  (effective ETF count)
    #   M_conc     = floor + (1−floor) × min(1, (N_eff−1)/(N_eff★−1))
    #   E          = −Σ_t p_t·ln(p_t)               (tier-share entropy)
    #   M_breadth  = 1 + α × E / ln(N_tiers)
    #   pen        = B / (OM + λ)                   (shrunk penetration)
    #   M_universe = clip((pen / median pen)^η, clip_lo, clip_hi)
    #   Final      = B × M_conc × M_breadth × M_universe   (apex only)
    score_totals = latest.groupby("ticker")["score"].sum()
    tot_per_row = latest["ticker"].map(score_totals)
    shares = latest["score"].div(tot_per_row.where(tot_per_row > 0))
    hhi = agg["ticker"].map(shares.pow(2).groupby(latest["ticker"]).sum(min_count=1)).fillna(1.0)
    n_eff = 1.0 / hhi
    # s_max — max per-ETF share of LINEAR conviction mass (dominance: drives
    # the CONCENTRATED flag). Read on linear mass because κ = 2 squares the
    # per-fund ratios: on κ-mass the 0.85 threshold fired 3× as often.
    lin_tot = latest["ticker"].map(latest.groupby("ticker")["linear_mass"].sum())
    lin_shares = latest["linear_mass"].div(lin_tot.where(lin_tot > 0))
    s_max = agg["ticker"].map(lin_shares.groupby(latest["ticker"]).max()).fillna(1.0)

    tier_scores = latest.groupby(["ticker", "tier"])["score"].sum()
    tier_totals = tier_scores.groupby(level="ticker").transform("sum")
    p_t = tier_scores.div(tier_totals.where(tier_totals > 0))
    p_pos = p_t.where(p_t > 0)
    ent_terms = -(p_pos * np.log(p_pos))
    tier_entropy = agg["ticker"].map(ent_terms.groupby(level="ticker").sum()).fillna(0.0)

    n_tiers = len(cfg.all_tier_names())
    e_norm = tier_entropy / math.log(n_tiers) if n_tiers > 1 else tier_entropy * 0.0

    # Σ score = 0 guard — multipliers neutralized (skip, don't penalize)
    zero_mass = agg["ticker"].map(score_totals).fillna(0.0) <= 0

    if cfg.scoring_mode == "apex":
        ax = cfg.apex if cfg.apex is not None else ApexConfig()

        # (a) Concentration — single-fund manipulation guard
        release = ((n_eff - 1.0) / (ax.conc_target_neff - 1.0)).clip(0.0, 1.0)
        m_conc = (ax.conc_floor + (1.0 - ax.conc_floor) * release).clip(ax.conc_floor, 1.0)

        # (b) Strategy breadth dispersal
        m_breadth = 1.0 + ax.breadth_alpha * e_norm

        # (c) Universe normalization — opportunity set over the FULL
        # as_of-capped sanitized history (every ETF that EVER held the name)
        pairs = df.loc[df["weight"] > 0, ["ticker", "ETF_Ticker"]].drop_duplicates()
        opp_mass = pairs["ETF_Ticker"].map(lambda e: etf_lookup[e].points).groupby(pairs["ticker"]).sum()
        om = agg["ticker"].map(opp_mass).fillna(0.0)
        b_raw = agg["final_score"].astype(float)
        penetration = b_raw / (om + ax.universe_lambda)
        med = penetration[b_raw > 0].median()
        if pd.isna(med) or med <= 0:
            m_universe = pd.Series(1.0, index=agg.index)
        else:
            m_universe = penetration.div(med).pow(ax.universe_eta).clip(
                ax.universe_clip_lo, ax.universe_clip_hi)

        m_conc = m_conc.where(~zero_mass, 1.0)
        m_breadth = m_breadth.where(~zero_mass, 1.0)
        m_universe = m_universe.where(~zero_mass, 1.0)

        # (d) Final — before sort/rank/normalize so they see apex scores
        agg["final_score"] = b_raw * m_conc * m_breadth * m_universe

        # Max per-holding OW per ticker — drives the single-ETF CONCENTRATED test
        ow_max = (latest["weight"] * latest["n_holdings"]).groupby(latest["ticker"]).max()
        agg["_max_ow"] = agg["ticker"].map(ow_max).fillna(0.0)
    else:
        m_conc = pd.Series(1.0, index=agg.index)
        m_breadth = pd.Series(1.0, index=agg.index)
        m_universe = pd.Series(1.0, index=agg.index)

    agg["hhi"] = hhi.round(4)
    agg["n_eff"] = n_eff.round(2)
    agg["max_etf_share"] = s_max.round(4)
    agg["m_conc"] = m_conc.round(4)
    agg["m_breadth"] = m_breadth.round(4)
    agg["m_universe"] = m_universe.round(4)
    agg["tier_entropy"] = tier_entropy.round(4)

    def _flag(row) -> str:
        # HIGH_CONVICTION (2026-10) = broad AND convinced AND independent:
        #   ≥ high_conviction_min_etfs holders, of which
        #   ≥ high_conviction_min_conviction_etfs non-benchmark funds hold it
        #     at conviction ≥ hc_theta (overweight, high in the book), across
        #   ≥ high_conviction_min_tiers non-benchmark strategy tiers.
        # The raw "≥ 4 holders" rule flagged ~38 names in the 16-ETF universe
        # but ~200 in the 30-ETF one (four broad funds hold 500–1,300 names
        # each). The conviction rule flags ~20–25, and its names lose a holder
        # within 30 days 15% of the time vs 27% (2026-10 signal study).
        # Legacy mode (no conviction column) keeps the raw-count rule.
        # (raw count, ALL modes). Matches the documented definition everywhere
        # (README, methodology, tooltips). The previous apex-mode gate used
        # conviction breadth (C_i ≥ θ in ≥4 ETFs), which fired on ~0.03% of
        # rows — effectively a dead flag. conviction_etf_count is still
        # emitted as a diagnostic column.
        if row["etf_count"] >= cfg.high_conviction_min_etfs and (
                cfg.scoring_mode == "legacy"
                or (row["conviction_etf_count"] >= cfg.high_conviction_min_conviction_etfs
                    and row["strategy_tier_count"] >= cfg.high_conviction_min_tiers)):
            return "HIGH_CONVICTION"
        if cfg.scoring_mode == "apex":
            # §30 CONCENTRATED — conviction parked in too few books to be HC,
            # but too much mass to ignore. Two triggers (dominance criterion —
            # an equal 2-way split is the normal shape of a 2-book name, not
            # an anomaly, so HHI=0.5 alone must not fire):
            #   • multi-ETF: one fund supplies ≥ θ_share of score mass (≥ 2 books)
            #   • single-ETF: the one holding is a real outsized bet (OW ≥ OW★)
            ax = cfg.apex if cfg.apex is not None else ApexConfig()
            ow_anchor = cfg.conviction.ow_star if cfg.conviction is not None else float("inf")
            if ((row["max_etf_share"] >= ax.conc_flag_share and row["etf_count"] >= 2)
                    or (row["etf_count"] == 1 and row.get("_max_ow", 0.0) >= ow_anchor)):
                return "CONCENTRATED"
        return ""

    agg["flag"] = agg.apply(_flag, axis=1)
    agg = agg.drop(columns=["_max_ow"], errors="ignore")
    agg = agg.sort_values(
        ["final_score", "etf_count", "total_weight", "ticker"],
        ascending=[False, False, False, True]
    ).reset_index(drop=True)
    agg["leaderboard_rank"] = agg.index + 1

    # §27 Score normalization — percentile within today's universe (0–100)
    # Keep raw final_score for sorting; add normalized_score for display.
    n = len(agg)
    if n > 1:
        # Percentile rank: fraction of scores strictly below this score × 100
        agg["normalized_score"] = agg["final_score"].rank(method="average", pct=True).mul(100).round(1)
    else:
        agg["normalized_score"] = 50.0

    # Display rounding. Legacy / conviction keep the Excel Int64.Type cast on
    # Final Alpha Score. Apex keeps one decimal: under κ = 2 convexity a filler
    # holding contributes fractions of a point, and int truncation collapsed
    # ~46% of the board to a 0 score (ties, and "score > 0" logic such as
    # universe exits silently ignoring them). Sort uses the precise float.
    if cfg.scoring_mode == "apex":
        agg["final_score"] = agg["final_score"].round(1)
    else:
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
        if dates_le:
            chosen[etf] = max(dates_le)
    chosen_series = pd.Series(chosen, name="target_date")
    chosen_series.index.name = "ETF_Ticker"
    then_df = _ranked_at(chosen_series)

    merged = now_df.merge(then_df, on=["ETF_Ticker", "ticker"], how="left", suffixes=("_now", "_then"))
    merged["rank_delta"] = merged["rank_then"] - merged["rank_now"]   # positive = moved up
    merged["weight_flow"] = (merged["weight_now"] - merged["weight_then"]) / merged["weight_then"].replace(0, float('nan'))
    return merged
