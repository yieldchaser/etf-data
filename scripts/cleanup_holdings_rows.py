"""One-time cleanup of legacy placeholder rows in the CURRENT-YEAR partition.

Removes, per the 2026-08 scraper-stack audit:
  * rows with weight <= 0 (FX-hedge negatives: IMOM EUR -0.1954 etc.)
  * $-prefixed currency lines ($KRW, $TWD, $AED ...) regardless of weight
  * Cash&Other aggregation lines
Bare 3-letter codes with positive weight are KEPT unless they match an
explicit blocklist — some are real tickers (e.g. PEN = Penumbra).

Locked past-year partitions are contractually immutable and are NOT touched;
the sanitizer blocklist in config.yaml covers any stragglers there at scoring
time. Refreshes CHECKSUMS.json for the rewritten partition.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "scripts"))

from migrate_to_parquet import _load_manifest, _partition_summary, _write_manifest  # noqa: E402

DEST = REPO / "data" / "history_parquet"
MANIFEST_NAME = "CHECKSUMS.json"


def main() -> int:
    manifest = _load_manifest(DEST)
    current_year = datetime.now().year
    year_str = str(current_year)
    parquet_path = DEST / f"year={current_year}" / "holdings.parquet"
    if not parquet_path.exists():
        print(f"ERROR: no current-year partition at {parquet_path}")
        return 1

    df = pd.read_parquet(parquet_path)
    before = len(df)
    w = pd.to_numeric(df["weight"], errors="coerce")
    t = df["ticker"].astype(str)

    m_nonpos = w.fillna(0) <= 0
    m_dollar = t.str.startswith("$")
    m_cash = t == "Cash&Other"
    drop_mask = m_nonpos | m_dollar | m_cash

    print(f"year={year_str}: {before:,} rows before cleanup")
    print(f"  weight<=0          : {int(m_nonpos.sum()):,}")
    print(f"  $-prefixed         : {int((m_dollar & ~m_nonpos).sum()):,}")
    print(f"  Cash&Other         : {int((m_cash & ~m_nonpos).sum()):,}")

    # Positive-weight bare currency codes would be ambiguous with real
    # tickers — surface them instead of silently deleting.
    pos_bare = df[(~drop_mask) & t.str.fullmatch(r"[A-Z]{3}")]
    if len(pos_bare):
        codes = sorted(pos_bare["ticker"].unique())
        print(f"  NOTE: {len(pos_bare)} positive-weight bare-code rows kept "
              f"(verify manually): {codes[:15]}")

    cleaned = df[~drop_mask].copy()
    removed = before - len(cleaned)
    if removed == 0:
        print("Nothing to remove.")
        return 0

    cleaned.to_parquet(parquet_path, index=False)
    manifest.setdefault("partitions", {})[year_str] = _partition_summary(parquet_path, current_year)
    manifest["generated_utc"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    _write_manifest(DEST, manifest)

    print(f"\n✅ Removed {removed:,} rows → {len(cleaned):,} remain in year={year_str}")
    print("   CHECKSUMS.json refreshed; past-year partitions untouched.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
