"""
READ-ONLY data-validation suite (Steps 1-4 of the zero-loss proof).

Proves that every dedup key in the live data/all_history.csv survives a
CSV → year-partitioned-Parquet → reconstructed-CSV round-trip with zero loss,
WITHOUT touching the committed data/history_parquet/ store.

Pipeline:
    Step 1: fingerprint the live CSV  → baseline
    Step 2: surface any unparseable Holdings_As_Of dates (migrate drops them!)
    Step 3: run migrate into a temp sandbox, reconstruct CSV from the partitions
    Step 4: compare key sets, row counts, weight sums

Writes:   _baseline_fingerprint.json  (gitignored via *.json)
Touches:  only a temp working dir under tempfile.gettempdir()
Exit 0:   zero loss proved
Exit 1:   mismatch found — DO NOT proceed with the migration
"""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

if sys.platform.startswith("win"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except AttributeError:
        pass

REPO_ROOT   = Path(__file__).resolve().parent.parent
LIVE_CSV    = REPO_ROOT / "data" / "all_history.csv"
LIVE_STORE  = REPO_ROOT / "data" / "history_parquet"
MIGRATE     = REPO_ROOT / "scripts" / "migrate_to_parquet.py"
FINGERPRINT = REPO_ROOT / "_baseline_fingerprint.json"

KEY_COLS = ["ETF_Ticker", "ticker", "Holdings_As_Of"]


# ─── helpers ─────────────────────────────────────────────────────────────────

def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


def _read_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    # Match migrate's own coercion: drop rows whose Holdings_As_Of won't parse.
    df["Holdings_As_Of"] = pd.to_datetime(df["Holdings_As_Of"], errors="coerce")
    return df


# ─── Step 1: fingerprint the live CSV ────────────────────────────────────────

def step1_fingerprint(df: pd.DataFrame) -> dict:
    print("\n" + "=" * 70)
    print("STEP 1 — Baseline fingerprint of data/all_history.csv")
    print("=" * 70)

    fp = {
        "source_file": str(LIVE_CSV),
        "captured_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "raw_file_size_bytes": LIVE_CSV.stat().st_size,
        "raw_file_sha256": _sha256(LIVE_CSV),
        "row_count_total": int(len(df)),
        "distinct_etfs": int(df["ETF_Ticker"].nunique()),
        "columns": list(df.columns),
    }

    # Per-ETF row counts (top 10 only in the printout; full dict in JSON)
    fp["per_etf_rows"] = {
        k: int(v) for k, v in df["ETF_Ticker"].value_counts().items()
    }

    # Per-Holdings_As_Of row counts (full — small, ≤ a few hundred dates)
    fp["per_asof_rows"] = {
        d.strftime("%Y-%m-%d"): int(n)
        for d, n in df["Holdings_As_Of"].value_counts().items()
    }

    # Weight integrity — exclude any non-numeric weight values quietly
    weights = pd.to_numeric(df["weight"], errors="coerce")
    fp["weight_sum_total"] = float(weights.sum())
    fp["weight_non_numeric_count"] = int(weights.isna().sum())

    print(f"  rows (incl. any pre-dedup duplicates): {fp['row_count_total']:,}")
    print(f"  distinct ETFs: {fp['distinct_etfs']}")
    print(f"  distinct Holdings_As_Of dates: {len(fp['per_asof_rows'])}")
    print(f"  date range: {min(fp['per_asof_rows'])} → {max(fp['per_asof_rows'])}")
    print(f"  weight Σ = {fp['weight_sum_total']:.6f}")
    print(f"  weight Σ across ETFs: {sum(df.groupby('ETF_Ticker')['weight'].apply(lambda s: pd.to_numeric(s, errors='coerce').sum()).tolist()):.6f}")
    print(f"  non-numeric weight values: {fp['weight_non_numeric_count']}")
    print(f"  raw file SHA-256: {fp['raw_file_sha256'][:24]}…")
    print(f"  raw file size: {fp['raw_file_size_bytes']:,} bytes")

    return fp


# ─── Step 2: surface silent data-loss vectors ────────────────────────────────

def step2_detect_loss_vectors(raw_df: pd.DataFrame) -> dict:
    """migrate_to_parquet.py drops rows whose Holdings_As_Of is NaT. Find them."""
    print("\n" + "=" * 70)
    print("STEP 2 — Detect silent data-loss vectors (rows migrate would drop)")
    print("=" * 70)

    # Re-read RAW (before datetime coercion) to see what the migrate script sees.
    raw = pd.read_csv(LIVE_CSV)
    parsed = pd.to_datetime(raw["Holdings_As_Of"], errors="coerce")
    bad_mask = parsed.isna()
    bad_rows = raw[bad_mask]

    report = {
        "unparseable_asof_count": int(bad_mask.sum()),
        "unparseable_asof_examples": [],
    }
    if not bad_rows.empty:
        samples = bad_rows.head(10)[KEY_COLS + ["name"]].to_dict("records")
        report["unparseable_asof_examples"] = samples
        print(f"  ⚠️  {report['unparseable_asof_count']} row(s) have unparseable "
              f"Holdings_As_Of — migrate would DROP these:")
        for s in samples:
            print(f"      {s}")
    else:
        print(f"  ✓  all {len(raw):,} rows have parseable Holdings_As_Of — "
              f"nothing would be silently dropped by migrate")

    return report


# ─── Step 3: sandbox round-trip (never touches the live store) ───────────────

def step3_sandbox_roundtrip(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    print("\n" + "=" * 70)
    print("STEP 3 — Sandbox CSV→Parquet→CSV round-trip (live store untouched)")
    print("=" * 70)

    with tempfile.TemporaryDirectory(prefix="zeroloss_") as tmp:
        tmp_dir = Path(tmp)
        # Write a scratch CSV that exactly mirrors the live one's parseable rows
        # (migrate would drop NaT rows; we already proved in Step 2 there are none,
        #  but we mirror migrate's behaviour precisely so the round-trip is faithful).
        scratch_csv = tmp_dir / "all_history.csv"
        df.to_csv(scratch_csv, index=False)
        sandbox_store = tmp_dir / "history_parquet"

        # Invoke the REAL migrate script against the sandbox.
        r = subprocess.run(
            [sys.executable, str(MIGRATE),
             "--source", str(scratch_csv),
             "--dest", str(sandbox_store)],
            capture_output=True, text=True, encoding="utf-8",
            cwd=str(REPO_ROOT),
        )
        if r.returncode != 0:
            print("  ❌ migrate subprocess FAILED:")
            print(r.stdout)
            print(r.stderr)
            sys.exit(1)

        # Find the partitions it created
        parts = sorted(sandbox_store.glob("year=*/holdings.parquet"))
        meta = {
            "partitions_created": [p.parent.name for p in parts],
            "total_partition_files": len(parts),
        }
        print(f"  sandbox partitions: {meta['partitions_created']}")

        # Reconstruct the dataset by concatenating all partitions
        reconstructed = pd.concat(
            [pd.read_parquet(p) for p in parts], ignore_index=True
        )
        # Parquet stores Holdings_As_Of natively as datetime64; normalise to
        # YYYY-MM-DD strings for the comparison (same as test_zero_loss_reconstruction)
        reconstructed["Holdings_As_Of"] = (
            pd.to_datetime(reconstructed["Holdings_As_Of"]).dt.strftime("%Y-%m-%d")
        )
        df_norm = df.copy()
        df_norm["Holdings_As_Of"] = (
            pd.to_datetime(df_norm["Holdings_As_Of"]).dt.strftime("%Y-%m-%d")
        )

        meta["reconstructed_rows"] = int(len(reconstructed))
        meta["input_rows"] = int(len(df_norm))
        return reconstructed, df_norm, meta


# ─── Step 4: compare key sets, row counts, weight sums ───────────────────────

def step4_compare(df_norm: pd.DataFrame, recon: pd.DataFrame) -> dict:
    print("\n" + "=" * 70)
    print("STEP 4 — Zero-loss comparison")
    print("=" * 70)

    # Dedup both sides with the migrate script's exact rule (keep="last")
    src_dd = df_norm.drop_duplicates(subset=KEY_COLS, keep="last")
    rec_dd = recon.drop_duplicates(subset=KEY_COLS, keep="last")

    src_keys = set(map(tuple, src_dd[KEY_COLS].itertuples(index=False, name=None)))
    rec_keys = set(map(tuple, rec_dd[KEY_COLS].itertuples(index=False, name=None)))

    missing_from_recon = src_keys - rec_keys
    extra_in_recon     = rec_keys - src_keys

    # NaN-ticker rows: `nan != nan` in Python, so set arithmetic on tuples
    # containing NaN is unreliable. Re-key with NaN filled to "" and compare
    # full content (incl. name + weight) — this is the authoritative check.
    def _content_key(d):
        d = d.copy()
        d["ticker"] = d["ticker"].fillna("")
        d["Holdings_As_Of"] = pd.to_datetime(d["Holdings_As_Of"]).dt.strftime("%Y-%m-%d")
        d["name"]   = d["name"].fillna("")
        d["weight"] = pd.to_numeric(d["weight"], errors="coerce")
        return set(d.apply(lambda r: (
            r["ETF_Ticker"], r["ticker"], r["Holdings_As_Of"],
            r["name"], round(float(r["weight"]), 9),
        ), axis=1))

    src_content = _content_key(src_dd)
    rec_content = _content_key(rec_dd)
    content_missing = src_content - rec_content
    content_extra   = rec_content - src_content

    # Weight integrity on the dedup'd sets
    src_w = pd.to_numeric(src_dd["weight"], errors="coerce").sum()
    rec_w = pd.to_numeric(rec_dd["weight"], errors="coerce").sum()

    report = {
        "input_rows": int(len(df_norm)),
        "reconstructed_rows": int(len(recon)),
        "input_dedup_rows": int(len(src_dd)),
        "reconstructed_dedup_rows": int(len(rec_dd)),
        "unique_keys_input": len(src_keys),
        "unique_keys_reconstructed": len(rec_keys),
        "keys_missing_from_reconstruction": len(missing_from_recon),
        "keys_extra_in_reconstruction": len(extra_in_recon),
        "missing_key_examples": list(missing_from_recon)[:20],
        # Authoritative: NaN-tolerant content comparison
        "content_keys_missing": len(content_missing),
        "content_keys_extra": len(content_extra),
        "content_missing_examples": list(content_missing)[:20],
        "nan_ticker_rows_input": int(df_norm["ticker"].isna().sum()),
        "weight_sum_input_dedup": float(src_w),
        "weight_sum_reconstructed_dedup": float(rec_w),
        "weight_abs_diff": float(abs(src_w - rec_w)),
    }

    print(f"  input rows (after migrate-style date coerce): {report['input_rows']:,}")
    print(f"  reconstructed rows from parquet:              {report['reconstructed_rows']:,}")
    print(f"  input dedup rows    (keep=last): {report['input_dedup_rows']:,}")
    print(f"  reconstructed dedup (keep=last): {report['reconstructed_dedup_rows']:,}")
    print(f"  unique dedup keys  input: {report['unique_keys_input']:,}")
    print(f"  unique dedup keys  recon: {report['unique_keys_reconstructed']:,}")
    print(f"  keys missing from reconstruction: {report['keys_missing_from_reconstruction']}")
    print(f"  keys extra in reconstruction:     {report['keys_extra_in_reconstruction']}")
    print(f"  weight Σ input:    {src_w:.6f}")
    print(f"  weight Σ recon:    {rec_w:.6f}")
    print(f"  weight |Δ|:        {report['weight_abs_diff']:.9f}")

    return report


# ─── main ────────────────────────────────────────────────────────────────────

def main() -> int:
    print(f"VALIDATING: {LIVE_CSV}")
    if not LIVE_CSV.exists():
        print("ERROR: live CSV not found")
        return 2

    df = _read_csv(LIVE_CSV)

    fp       = step1_fingerprint(df)
    loss_vec = step2_detect_loss_vectors(df)
    recon, df_norm, rt_meta = step3_sandbox_roundtrip(df)
    cmp      = step4_compare(df_norm, recon)

    # ── Verdict ──────────────────────────────────────────────────────────────
    # Authoritative gate: NaN-tolerant content comparison + row count + weight.
    # The tuple-set comparison can false-positive on NaN-ticker rows (nan != nan);
    # the content-key comparison fills NaN and compares (ETF, ticker, date, name, weight),
    # so it is the source of truth.
    zero_loss = (
        cmp["content_keys_missing"] == 0
        and cmp["content_keys_extra"] == 0
        and cmp["input_dedup_rows"] == cmp["reconstructed_dedup_rows"]
        and cmp["weight_abs_diff"] < 1e-6
    )

    print("\n" + "=" * 70)
    print("VERDICT")
    print("=" * 70)
    if zero_loss:
        print("  ✅ ZERO DATA LOSS PROVED")
        print(f"     {cmp['unique_keys_input']:,} unique dedup keys round-tripped intact")
        print(f"     row count match: {cmp['input_dedup_rows']:,} == {cmp['reconstructed_dedup_rows']:,}")
        print(f"     weight Δ = {cmp['weight_abs_diff']:.2e} (below 1e-6 threshold)")
        print(f"     content keys missing/extra: {cmp['content_keys_missing']}/{cmp['content_keys_extra']}")
        if cmp["nan_ticker_rows_input"]:
            print(f"     note: {cmp['nan_ticker_rows_input']} NaN-ticker rows verified identical "
                  f"via NaN-tolerant content key (not a loss; CSV-side data-quality quirk)")
    else:
        print("  ❌ DATA LOSS DETECTED — DO NOT PROCEED WITH THE MIGRATION")
        print(f"     content keys missing: {cmp['content_keys_missing']}")
        print(f"     content keys extra:   {cmp['content_keys_extra']}")
        print(f"     row delta:    {cmp['reconstructed_dedup_rows'] - cmp['input_dedup_rows']}")
        print(f"     weight Δ:     {cmp['weight_abs_diff']:.2e}")

    payload = {
        "verdict": "ZERO_LOSS_PROVED" if zero_loss else "DATA_LOSS_DETECTED",
        "fingerprint": fp,
        "loss_vectors": loss_vec,
        "roundtrip": rt_meta,
        "comparison": cmp,
    }
    FINGERPRINT.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    print(f"\nFingerprint written: {FINGERPRINT}")
    return 0 if zero_loss else 1


if __name__ == "__main__":
    sys.exit(main())
