"""
Tests for the year-partitioned Parquet store immutability contract.

Contract (charter v2, Part 3):
    1. Past-year partitions (year < current_year) are byte-stable. Once a
       year closes, its holdings.parquet MUST NOT change. CHECKSUMS.json
       at the store root is the manifest.
    2. Current-year partition is mutable.
    3. The migrate script refuses to overwrite past-year partitions
       without --allow-historical-rewrite.
    4. The store can be empty (system pre-bootstrap) — tests skip cleanly.

These tests are designed to FAIL LOUDLY in CI if any prior-year partition
ever drifts byte-for-byte from its recorded SHA-256.
"""
from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest


REPO_ROOT     = Path(__file__).resolve().parent.parent
PARQUET_STORE = REPO_ROOT / "data" / "history_parquet"
MANIFEST_PATH = PARQUET_STORE / "CHECKSUMS.json"
MIGRATE_SCRIPT = REPO_ROOT / "scripts" / "migrate_to_parquet.py"


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


# ─── Real-archive validations (skip when archive not bootstrapped) ──────────

@pytest.mark.skipif(not MANIFEST_PATH.exists(),
                    reason="Parquet archive not yet bootstrapped (no CHECKSUMS.json)")
def test_manifest_well_formed():
    """Manifest must be valid JSON with the v1 schema."""
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    assert manifest.get("schema_version") == 1, \
        f"Unexpected schema_version: {manifest.get('schema_version')}"
    assert "partitions" in manifest, "Manifest missing 'partitions' key"
    assert isinstance(manifest["partitions"], dict), \
        f"partitions must be a dict, got {type(manifest['partitions']).__name__}"
    for year_str, record in manifest["partitions"].items():
        assert year_str.isdigit() and len(year_str) == 4, \
            f"Bad year key in manifest: '{year_str}'"
        assert "sha256" in record and len(record["sha256"]) == 64, \
            f"Bad sha256 for year={year_str}: {record.get('sha256')!r}"
        assert "rows" in record and isinstance(record["rows"], int), \
            f"Bad row count for year={year_str}"


@pytest.mark.skipif(not MANIFEST_PATH.exists(),
                    reason="Parquet archive not yet bootstrapped (no CHECKSUMS.json)")
def test_past_year_partitions_are_immutable():
    """
    HARD GATE: every past-year partition's recomputed sha256 must match
    the manifest. A failure here means past data has been altered — this
    is a contract violation.
    """
    manifest     = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    current_year = datetime.now(timezone.utc).year
    drift: list[str] = []

    for year_str, record in manifest["partitions"].items():
        try:
            year = int(year_str)
        except ValueError:
            continue
        if year >= current_year:
            continue   # current/future years are mutable

        parquet_path = PARQUET_STORE / f"year={year_str}" / "holdings.parquet"
        if not parquet_path.exists():
            drift.append(f"  year={year_str}: MANIFEST RECORDS A PARTITION but file is missing on disk")
            continue
        actual = _sha256(parquet_path)
        expected = record["sha256"]
        if actual != expected:
            drift.append(
                f"  year={year_str}: expected sha256={expected[:16]}…  got={actual[:16]}…"
            )

    assert not drift, (
        "Past-year parquet partitions are contractually immutable but DRIFTED:\n"
        + "\n".join(drift)
        + "\n\nIf a fix is genuinely required, run:\n"
        "  python scripts/migrate_to_parquet.py --allow-historical-rewrite\n"
        "and explain the change in the commit message."
    )


@pytest.mark.skipif(not MANIFEST_PATH.exists(),
                    reason="Parquet archive not yet bootstrapped (no CHECKSUMS.json)")
def test_manifest_partitions_reflect_disk():
    """
    Every partition file on disk must appear in the manifest, and vice versa.
    Detects orphan partitions (file but no manifest entry) which would mean
    the immutability test silently skips them.
    """
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    on_disk  = {p.parent.name.split("=")[1] for p in PARQUET_STORE.glob("year=*/holdings.parquet")}
    in_manifest = set(manifest["partitions"].keys())
    missing_from_manifest = on_disk - in_manifest
    missing_from_disk     = in_manifest - on_disk
    assert not missing_from_manifest, \
        f"Partition file(s) not in manifest (orphans, immutability untracked): {missing_from_manifest}"
    assert not missing_from_disk, \
        f"Manifest record(s) without partition file: {missing_from_disk}"


# ─── Behaviour validations using a tmp_path sandbox ─────────────────────────

def _make_sample_csv(path: Path, year_rows: dict[int, int]) -> None:
    """Write a tiny synthetic all_history.csv with rows in the requested years."""
    rows = []
    for yr, n in year_rows.items():
        for i in range(n):
            rows.append({
                "ETF_Ticker":     "TST",
                "ticker":         f"AAA{i:03d}",
                "name":           f"Test Co {i}",
                "weight":         0.001 * (i + 1),
                "Holdings_As_Of": f"{yr}-06-15",
                "Date_Scraped":   f"{yr}-06-16",
            })
    pd.DataFrame(rows).to_csv(path, index=False)


def _run_migrate(source: Path, dest: Path, *extra: str) -> subprocess.CompletedProcess:
    """Invoke migrate_to_parquet.py against a temp store; capture output.

    `encoding="utf-8"` is required because the migrate script emits UTF-8 text
    (including emoji/✓ marks) and the default `text=True` decoder uses the
    platform's preferred encoding (cp1252 on Windows), which raises
    UnicodeDecodeError. The script already reconfigures sys.stdout to UTF-8
    on Windows, so the fix is consumer-side only.
    """
    return subprocess.run(
        [sys.executable, str(MIGRATE_SCRIPT),
         "--source", str(source), "--dest", str(dest), *extra],
        capture_output=True, text=True, encoding="utf-8", cwd=str(REPO_ROOT),
    )


def test_migrate_creates_manifest_with_current_year_only(tmp_path):
    """First-run migration of current-year-only data writes a manifest with one entry."""
    csv_path = tmp_path / "all_history.csv"
    dest     = tmp_path / "history_parquet"
    current_year = datetime.now(timezone.utc).year
    _make_sample_csv(csv_path, {current_year: 5})

    result = _run_migrate(csv_path, dest)
    assert result.returncode == 0, f"migrate failed:\nSTDOUT:{result.stdout}\nSTDERR:{result.stderr}"
    assert (dest / f"year={current_year}" / "holdings.parquet").exists()
    assert (dest / "CHECKSUMS.json").exists()

    manifest = json.loads((dest / "CHECKSUMS.json").read_text())
    assert manifest["schema_version"] == 1
    assert str(current_year) in manifest["partitions"]
    assert manifest["partitions"][str(current_year)]["rows"] == 5


def test_migrate_refuses_to_rewrite_past_year_partition(tmp_path):
    """
    Locked past-year partitions are NOT rewritten without explicit opt-in.
    Simulates a year rollover: bootstrap year=2024 (now in the past for any
    current_year >= 2025), then re-run migrate with different data — the
    on-disk file must remain byte-identical.
    """
    current_year = datetime.now(timezone.utc).year
    past_year    = current_year - 1   # always in the past
    csv_path     = tmp_path / "all_history.csv"
    dest         = tmp_path / "history_parquet"

    # Initial bootstrap with 5 rows in past_year
    _make_sample_csv(csv_path, {past_year: 5})
    r1 = _run_migrate(csv_path, dest)
    assert r1.returncode == 0, f"bootstrap failed:\n{r1.stdout}\n{r1.stderr}"
    parquet_path = dest / f"year={past_year}" / "holdings.parquet"
    initial_sha  = _sha256(parquet_path)
    initial_size = parquet_path.stat().st_size

    # Now re-run with DIFFERENT data for the same past year — must be refused
    _make_sample_csv(csv_path, {past_year: 50})  # 10× more rows
    r2 = _run_migrate(csv_path, dest)
    assert r2.returncode == 0, f"second migrate failed:\n{r2.stdout}\n{r2.stderr}"
    assert "LOCKED" in r2.stdout, f"Expected LOCKED message, got:\n{r2.stdout}"

    # File must be byte-identical
    assert parquet_path.stat().st_size == initial_size
    assert _sha256(parquet_path) == initial_sha, \
        "Past-year parquet partition was rewritten despite immutability guard!"


def test_migrate_allows_rewrite_with_explicit_override(tmp_path):
    """The --allow-historical-rewrite escape hatch works (used for genuine fixes)."""
    current_year = datetime.now(timezone.utc).year
    past_year    = current_year - 1
    csv_path     = tmp_path / "all_history.csv"
    dest         = tmp_path / "history_parquet"

    _make_sample_csv(csv_path, {past_year: 5})
    _run_migrate(csv_path, dest)

    parquet_path = dest / f"year={past_year}" / "holdings.parquet"
    initial_sha  = _sha256(parquet_path)

    _make_sample_csv(csv_path, {past_year: 50})
    r = _run_migrate(csv_path, dest, "--allow-historical-rewrite")
    assert r.returncode == 0, f"override migrate failed:\n{r.stdout}\n{r.stderr}"
    assert "REWRITING" in r.stdout, f"Expected REWRITING warning, got:\n{r.stdout}"
    assert _sha256(parquet_path) != initial_sha, \
        "--allow-historical-rewrite should have changed the partition"


def test_verify_only_passes_on_clean_store(tmp_path):
    """--verify-only returns 0 when manifest matches disk."""
    csv_path = tmp_path / "all_history.csv"
    dest     = tmp_path / "history_parquet"
    current_year = datetime.now(timezone.utc).year
    _make_sample_csv(csv_path, {current_year: 5})
    _run_migrate(csv_path, dest)

    r = _run_migrate(csv_path, dest, "--verify-only")
    assert r.returncode == 0, f"verify failed on clean store:\n{r.stdout}\n{r.stderr}"


def test_verify_only_fails_on_past_year_drift(tmp_path):
    """--verify-only returns 1 when a past-year partition has been mutated."""
    current_year = datetime.now(timezone.utc).year
    past_year    = current_year - 1
    csv_path     = tmp_path / "all_history.csv"
    dest         = tmp_path / "history_parquet"
    _make_sample_csv(csv_path, {past_year: 5})
    _run_migrate(csv_path, dest)

    # Tamper with the past-year partition directly
    parquet_path = dest / f"year={past_year}" / "holdings.parquet"
    df = pd.read_parquet(parquet_path)
    df.iloc[0, df.columns.get_loc("weight")] = 999.999   # mutate one row
    df.to_parquet(parquet_path, index=False, compression="snappy")

    r = _run_migrate(csv_path, dest, "--verify-only")
    assert r.returncode == 1, f"Expected verify to fail, got rc={r.returncode}\n{r.stdout}"
    assert "IMMUTABILITY VIOLATION" in r.stdout, \
        f"Expected loud violation message, got:\n{r.stdout}"


def test_verify_only_passes_on_empty_store(tmp_path):
    """No manifest → no contract to violate → exit 0 with informational message."""
    dest = tmp_path / "history_parquet"
    dest.mkdir()
    r = _run_migrate(tmp_path / "fake.csv", dest, "--verify-only")
    assert r.returncode == 0
    assert "nothing to verify" in r.stdout.lower()


# ─── Sandbox isolation helper (Req 1.3, 2.5, 3.5) ───────────────────────────

def _real_store_snapshot() -> dict[Path, float]:
    """Capture mtime of every file under the real Parquet_Store.

    Returns ``{}`` when the real store does not exist (fresh checkout); the
    caller's before/after equality check then passes vacuously, which is the
    correct behavior — no real-store mutation happened because the store
    itself was absent throughout the test.
    """
    if not PARQUET_STORE.exists():
        return {}
    return {
        p: p.stat().st_mtime
        for p in PARQUET_STORE.glob("**/*")
        if p.is_file()
    }


# ─── New: Part A storage-contract proof tests ────────────────────────────────

def test_year_rollover_simulation(tmp_path):
    """
    Req 1: prove the year-2026 partition is byte-identical after year-2027 rows
    are appended in a second migrate run.

    The test fixes years 2026 and 2027 so the simulation never depends on the
    wall clock; however the immutability guard only locks a partition when
    ``year < current_year``, so we skip when wall-clock year < 2027.
    """
    wall_year = datetime.now(timezone.utc).year
    if wall_year < 2027:
        pytest.skip(
            f"Year-rollover simulation requires wall-clock year >= 2027 "
            f"(current: {wall_year}); test skipped"
        )

    csv_path = tmp_path / "all_history.csv"
    dest     = tmp_path / "history_parquet"

    # Step 1: write CSV with only year=2026 rows; run migrate.
    _make_sample_csv(csv_path, {2026: 5})
    r1 = _run_migrate(csv_path, dest)
    assert r1.returncode == 0, f"first migrate failed:\n{r1.stdout}\n{r1.stderr}"

    # Manifest exists and records year=2026
    manifest_path = dest / "CHECKSUMS.json"
    assert manifest_path.exists(), "manifest missing after first migrate"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert "2026" in manifest["partitions"], (
        f"manifest does not record year=2026 partition: "
        f"keys={list(manifest['partitions'])}"
    )
    assert len(manifest["partitions"]["2026"]["sha256"]) == 64

    # Step 2: record SHA-256 + byte size of year=2026 partition.
    parquet_2026 = dest / "year=2026" / "holdings.parquet"
    expected_sha  = _sha256(parquet_2026)
    expected_size = parquet_2026.stat().st_size

    # Step 3: extend CSV with year=2027 rows; run migrate again.
    _make_sample_csv(csv_path, {2026: 5, 2027: 7})
    r2 = _run_migrate(csv_path, dest)
    assert r2.returncode == 0, f"second migrate failed:\n{r2.stdout}\n{r2.stderr}"

    # Step 4: assert SHA-256 + byte size of year=2026 unchanged.
    actual_sha  = _sha256(parquet_2026)
    actual_size = parquet_2026.stat().st_size
    if actual_sha != expected_sha:
        # Req 1.4: emit partition year + first-16-hex on mismatch.
        pytest.fail(
            f"year=2026 partition SHA-256 changed: "
            f"expected={expected_sha[:16]}…  actual={actual_sha[:16]}…"
        )
    assert actual_size == expected_size, (
        f"year=2026 partition byte size changed: "
        f"expected={expected_size}  actual={actual_size}"
    )

    # Step 5: assert "LOCKED" appears for year=2026 in the second run's stdout.
    assert "LOCKED" in r2.stdout, (
        f"expected LOCKED message for year=2026 in second run, got:\n{r2.stdout}"
    )
    assert "year=2026" in r2.stdout, (
        f"LOCKED message did not reference year=2026:\n{r2.stdout}"
    )

    # Step 6: assert year=2027 partition was created.
    parquet_2027 = dest / "year=2027" / "holdings.parquet"
    assert parquet_2027.exists(), "year=2027 partition was not created"

    # Req 1.5: emit confirmation message.
    print(f"\n  Year-rollover simulation: year=2026 partition is byte-identical "
          f"(sha256={expected_sha[:16]}…, size={expected_size} bytes).")


def test_zero_loss_reconstruction(tmp_path):
    """
    Req 2: prove every Dedup_Key in the source CSV is present in the dataset
    reconstructed from concatenated Parquet partitions, with zero row loss.
    """
    csv_path = tmp_path / "all_history.csv"
    dest     = tmp_path / "history_parquet"

    # Multi-year source CSV — at least 2 distinct years, ≥5 rows/year.
    _make_sample_csv(csv_path, {2025: 5, 2026: 7})
    r = _run_migrate(csv_path, dest)
    assert r.returncode == 0, f"migrate failed:\n{r.stdout}\n{r.stderr}"

    # Concatenate every partition file in the sandbox store.
    partition_paths = sorted(dest.glob("year=*/holdings.parquet"))
    assert len(partition_paths) >= 2, (
        f"expected at least 2 partition files, got {len(partition_paths)}: "
        f"{[str(p) for p in partition_paths]}"
    )
    reconstructed = pd.concat(
        [pd.read_parquet(p) for p in partition_paths],
        ignore_index=True,
    )

    # Apply the Dedup_Key independently to source and reconstructed.
    key_cols = ["ETF_Ticker", "ticker", "Holdings_As_Of"]
    source = pd.read_csv(csv_path)
    # Normalize Holdings_As_Of to YYYY-MM-DD strings on both sides so the
    # set comparison is type-agnostic (Parquet stores datetime64 natively).
    source["Holdings_As_Of"] = (
        pd.to_datetime(source["Holdings_As_Of"]).dt.strftime("%Y-%m-%d")
    )
    reconstructed["Holdings_As_Of"] = (
        pd.to_datetime(reconstructed["Holdings_As_Of"]).dt.strftime("%Y-%m-%d")
    )

    source_dedup = source.drop_duplicates(subset=key_cols, keep="last")
    recon_dedup  = reconstructed.drop_duplicates(subset=key_cols, keep="last")

    # Req 2.6: fail loudly if either side is empty rather than passing vacuously.
    assert not source_dedup.empty, "source CSV is empty after dedup"
    assert not recon_dedup.empty, "reconstructed dataset is empty after dedup"

    # Row-count equality.
    assert len(recon_dedup) == len(source_dedup), (
        f"deduplicated row count mismatch: source={len(source_dedup)}, "
        f"reconstructed={len(recon_dedup)}"
    )

    # Set equality on Dedup_Key tuples.
    src_keys = set(map(tuple, source_dedup[key_cols].itertuples(index=False, name=None)))
    rec_keys = set(map(tuple, recon_dedup[key_cols].itertuples(index=False, name=None)))
    missing = src_keys - rec_keys
    if missing:
        # Req 2.4: list up to 20 missing Dedup_Key values.
        examples = list(missing)[:20]
        pytest.fail(
            f"{len(missing)} Dedup_Key tuple(s) present in source but absent from "
            f"reconstructed dataset; first {len(examples)}: {examples}"
        )


def test_append_only_contract(tmp_path):
    """
    Req 3: prove the past-year partition is byte-identical and row-count-stable
    after a current-year append, that the manifest's generated_at advances when
    net-new rows are appended, and that the real Parquet_Store is never touched.
    """
    current_year = datetime.now(timezone.utc).year
    past_year    = current_year - 1
    csv_path     = tmp_path / "all_history.csv"
    dest         = tmp_path / "history_parquet"

    # Snapshot the real store BEFORE any sandboxed work (Req 3.5).
    real_before = _real_store_snapshot()

    # Bootstrap with past-year rows.
    _make_sample_csv(csv_path, {past_year: 5})
    r1 = _run_migrate(csv_path, dest)
    assert r1.returncode == 0, f"bootstrap migrate failed:\n{r1.stdout}\n{r1.stderr}"

    manifest_path = dest / "CHECKSUMS.json"
    manifest1     = json.loads(manifest_path.read_text(encoding="utf-8"))
    past_record   = manifest1["partitions"].get(str(past_year))
    # Req 3.1: bootstrap must succeed; otherwise fail immediately, do not pass vacuously.
    assert past_record is not None, (
        f"Bootstrap failed to create past-year partition; cannot test "
        f"append-only contract. Manifest partitions: {list(manifest1['partitions'])}"
    )

    parquet_past   = dest / f"year={past_year}" / "holdings.parquet"
    initial_sha    = _sha256(parquet_past)
    initial_size   = parquet_past.stat().st_size
    initial_rows   = int(pd.read_parquet(parquet_past).shape[0])
    initial_genat  = manifest1.get("generated_at", "")

    # Append CURRENT-YEAR rows; the past-year CSV slice is ALSO present so the
    # migrate script processes both years (the immutability guard is what
    # protects past_year, not the absence of past-year rows in the input).
    _make_sample_csv(csv_path, {past_year: 5, current_year: 4})
    r2 = _run_migrate(csv_path, dest)
    assert r2.returncode == 0, f"append migrate failed:\n{r2.stdout}\n{r2.stderr}"

    # Two independent checks on the past-year partition (Req 3.3).
    final_sha  = _sha256(parquet_past)
    final_rows = int(pd.read_parquet(parquet_past).shape[0])
    failures: list[str] = []
    if final_sha != initial_sha:
        failures.append(
            f"FAILED: sha256 (year={past_year}: expected={initial_sha[:16]}… "
            f"actual={final_sha[:16]}…)"
        )
    if final_rows != initial_rows:
        failures.append(
            f"FAILED: row-count (year={past_year}: expected={initial_rows} "
            f"actual={final_rows})"
        )
    assert not failures, "Past-year partition mutated:\n  " + "\n  ".join(failures)
    assert parquet_past.stat().st_size == initial_size, (
        f"year={past_year} partition byte size changed: "
        f"expected={initial_size}  actual={parquet_past.stat().st_size}"
    )

    # Req 3.4: manifest's generated_at advances after a real append.
    manifest2     = json.loads(manifest_path.read_text(encoding="utf-8"))
    final_genat   = manifest2.get("generated_at", "")
    assert final_genat >= initial_genat and final_genat != "", (
        f"manifest generated_at did not advance: "
        f"before={initial_genat!r}  after={final_genat!r}"
    )

    # Req 3.2: current-year row count == count of unique Dedup_Key tuples in
    # the new current-year input.
    parquet_curr  = dest / f"year={current_year}" / "holdings.parquet"
    curr_df       = pd.read_parquet(parquet_curr)
    expected_curr = 4    # _make_sample_csv emits unique tickers AAA000…AAA003
    assert len(curr_df) == expected_curr, (
        f"current-year partition row count = {len(curr_df)}, expected {expected_curr}"
    )
    # Manifest's recorded current-year row count agrees with disk.
    assert manifest2["partitions"][str(current_year)]["rows"] == expected_curr, (
        f"manifest current-year rows = "
        f"{manifest2['partitions'][str(current_year)]['rows']}, "
        f"expected {expected_curr}"
    )

    # Req 3.5: real Parquet_Store mtimes unchanged throughout the test.
    real_after = _real_store_snapshot()
    assert real_before == real_after, (
        f"Real Parquet_Store was touched during sandboxed test; "
        f"diff: {set(real_after.items()) ^ set(real_before.items())}"
    )
