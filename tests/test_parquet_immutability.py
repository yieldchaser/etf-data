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
    """Invoke migrate_to_parquet.py against a temp store; capture output."""
    return subprocess.run(
        [sys.executable, str(MIGRATE_SCRIPT),
         "--source", str(source), "--dest", str(dest), *extra],
        capture_output=True, text=True, cwd=str(REPO_ROOT),
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
