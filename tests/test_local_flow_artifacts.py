import json
import re
import shutil
import socket
import subprocess
from pathlib import Path

import pytest

from scripts import build_local_flow_artifacts as build


ROOT = Path(__file__).resolve().parents[1]
DOCS_OUT = ROOT / "docs" / "data" / "flows"
SOURCE_DIR = ROOT / "data" / "flows"
CATALOG_JSON = DOCS_OUT / "catalog.json"
MANIFEST_JSON = DOCS_OUT / "manifest.json"

EXPECTED_COUNTS = {"featured": 24, "instruments": 117, "rows": 170392}


@pytest.fixture(autouse=True)
def _forbid_network(monkeypatch):
    def _blocked(*args, **kwargs):
        raise AssertionError("local artifact tests must not perform network access")

    monkeypatch.setattr(socket, "socket", _blocked)
    monkeypatch.setattr(socket, "create_connection", _blocked)
    monkeypatch.setattr(socket, "getaddrinfo", _blocked)


@pytest.fixture(scope="module")
def report():
    return build.validate_local_dataset()


def test_validate_local_dataset_reports_exact_universe(report):
    assert report["status"] == "ok"
    assert report["expected"] == {
        "universe_count": 117,
        "total_rows": 170392,
        "featured_count": 24,
    }
    actual = report["actual"]
    assert actual["catalog_rows"] == 117
    assert actual["individual_files"] == 117
    assert actual["individual_rows"] == 170392
    assert actual["aggregate_rows"] == 170392
    assert actual["workbook_catalog_rows"] == 117
    assert actual["workbook_latest_rows"] == 117
    assert actual["unique_tickers"] == 117
    assert actual["featured_tickers"] == 24
    assert actual["earliest_date"] == "2016-01-04"
    assert actual["latest_date"] == "2026-09-23"
    assert actual["duplicate_ticker_dates"] == 0
    assert actual["missing_required_values"] == 0
    assert actual["metadata_discrepancy_count"] == 1
    assert actual["metadata_discrepancies"] == [
        "WTIU.underlying raw label differs from summary label"
    ]
    assert report["individual"]["file_count"] == 117
    assert report["individual"]["total_rows"] == 170392
    assert len(report["featured_tickers"]) == 24
    assert len(set(report["featured_tickers"])) == 24
    assert set(report["featured_tickers"]) <= set(report["summary"])


def test_individual_files_expose_required_columns_and_clean_dates(report):
    files = report["files"]
    assert sorted(files) == sorted(report["summary"])
    assert len(files) == 117
    for ticker, info in files.items():
        assert set(build.REQUIRED_COLUMNS) <= set(info["headers"])
        assert set(info["headers"]) <= set(build.AGGREGATE_COLUMNS)
        assert info["row_count"] == len(info["data"])
        assert info["row_count"] == report["summary"][ticker]["rows_count"]
        dates = [row["date"] for row in info["data"]]
        assert dates == sorted(dates)
        assert len(dates) == len(set(dates))
        assert dates[0] >= "2016-01-04"
        assert dates[-1] <= "2026-09-23"
        assert all(row["ticker"] == ticker for row in info["data"])
        assert info["missing_required_values"] == 0
        assert info["duplicate_dates"] == 0
        assert info["relative_path"] == f"data/flows/individual/{ticker}_flows.csv"
    assert sum(info["row_count"] for info in files.values()) == 170392
    assert report["aggregate"]["row_count"] == 170392
    assert report["aggregate"]["unique_ticker_count"] == 117
    assert set(build.FEATURED_TICKERS) <= set(files)
    for ticker in ("TQQQ", "NVDL", sorted(files)[0]):
        info = files[ticker]
        assert build._sha256_file(info["path"]) == info["sha256"]


def test_docs_catalog_and_manifest_are_deterministic_projections(report):
    assert build._json_bytes(build._build_catalog(report)) == CATALOG_JSON.read_bytes()
    assert build._json_bytes(build._build_manifest(report)) == MANIFEST_JSON.read_bytes()
    catalog = json.loads(CATALOG_JSON.read_text(encoding="utf-8"))
    assert catalog["counts"] == EXPECTED_COUNTS
    assert catalog["catalog_version"] == build.CATALOG_VERSION
    assert catalog["source"]["network_fetch"] is False
    assert "watch_tier" not in catalog
    assert catalog["content_sha256"] == build._payload_hash(catalog)
    manifest = json.loads(MANIFEST_JSON.read_text(encoding="utf-8"))
    assert manifest["counts"] == {"featured": 24, "files": 117, "instruments": 117, "rows": 170392}
    assert manifest["content_sha256"] == build._payload_hash(manifest)


def test_manifest_and_ticker_artifacts_match_validated_sources(report):
    manifest = json.loads(MANIFEST_JSON.read_text(encoding="utf-8"))
    assert set(manifest["etfs"]) == set(report["files"])
    assert manifest["complete"] is True
    assert manifest["status"] == "complete"
    assert manifest["source"]["network_fetch"] is False
    assert manifest["quality"]["required_columns"] == list(build.REQUIRED_COLUMNS)
    assert manifest["quality"]["workbook_checked"] is True
    assert manifest["quality"]["duplicate_ticker_dates"] == 0
    assert manifest["quality"]["missing_required_values"] == 0
    for ticker, entry in manifest["etfs"].items():
        info = report["files"][ticker]
        assert entry["path"] == f"{ticker}.json"
        assert entry["records"] == info["row_count"]
        assert entry["earliest_date"] == info["earliest_date"]
        assert entry["latest_date"] == info["latest_date"]
        assert entry["trackinsight_key"] == info["data"][0]["trackinsight_key"]
        artifact = json.loads((DOCS_OUT / f"{ticker}.json").read_text(encoding="utf-8"))
        assert artifact == build._build_ticker_payload(ticker, report)
        assert artifact["source"] == "Local dataset"
        assert artifact["source_provider"] == "Trackinsight"
        assert artifact["source_mode"] == "historical_local"
        assert artifact["count"] == info["row_count"]


def test_build_local_artifacts_writes_only_into_temp_output(tmp_path):
    before = {
        path.name: (path.stat().st_size, path.stat().st_mtime_ns)
        for path in sorted(DOCS_OUT.iterdir())
    }
    output = tmp_path / "flows"
    result = build.build_local_artifacts(
        output_dir=output,
        validate_workbook=False,
        validate_markdown=False,
    )
    assert result["status"] == "ok"
    assert result["ticker_artifacts"] == 117
    assert result["row_count"] == 170392
    assert result["source_asof"] == "2026-09-23"
    assert Path(result["catalog"]) == output / "catalog.json"
    assert Path(result["manifest"]) == output / "manifest.json"
    names = {path.name for path in output.iterdir()}
    catalog = json.loads((output / "catalog.json").read_text(encoding="utf-8"))
    tickers = {row["ticker"] for row in catalog["instruments"]}
    assert names == {"catalog.json", "manifest.json", *(f"{ticker}.json" for ticker in tickers)}
    assert len(tickers) == 117
    assert catalog["counts"] == EXPECTED_COUNTS
    assert "watch_tier" not in catalog
    manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["complete"] is True
    assert manifest["status"] == "complete"
    assert manifest["counts"]["instruments"] == 117
    assert manifest["quality"]["workbook_checked"] is False
    assert not [path for path in tmp_path.iterdir() if path.name != "flows"]
    after = {
        path.name: (path.stat().st_size, path.stat().st_mtime_ns)
        for path in sorted(DOCS_OUT.iterdir())
    }
    assert after == before


def test_assert_output_is_safe_refuses_writing_into_source(tmp_path):
    with pytest.raises(build.LocalFlowValidationError, match="refusing to write"):
        build._assert_output_is_safe(SOURCE_DIR, SOURCE_DIR)
    with pytest.raises(build.LocalFlowValidationError, match="refusing to write"):
        build._assert_output_is_safe(SOURCE_DIR, SOURCE_DIR / "generated")
    build._assert_output_is_safe(SOURCE_DIR, tmp_path)


def test_ui_and_workflows_agree_on_117_local_cardinality():
    flow_js = (ROOT / "docs" / "flow-ui.js").read_text(encoding="utf-8")
    markets = (ROOT / "docs" / "markets.html").read_text(encoding="utf-8")
    fallback = (ROOT / "docs" / "runtime-fallback.js").read_text(encoding="utf-8")
    index = (ROOT / "docs" / "index.html").read_text(encoding="utf-8")
    assert "primary.length !== 117" in flow_js
    assert "featured !== 24" in flow_js
    assert "watchTier" not in flow_js
    assert "etf_search_index" not in flow_js
    assert '<option value="all">117 local instruments</option>' in markets
    assert '<option value="featured">24 featured instruments</option>' in markets
    assert '<option value="primary">All 117 instruments</option>' in markets
    assert '<option value="watch"' not in markets
    assert "watch_tier" not in markets
    assert "data/flows/manifest.json" in fallback
    assert "curated_manifest" not in fallback
    assert "117 local leveraged and inverse instruments" in index
    assert "15,000" not in index and "15K" not in index
    build_site = (ROOT / ".github" / "workflows" / "build_site.yml").read_text(encoding="utf-8")
    assert "--verify-output" in build_site
    assert "workflow_call" in build_site
    assert "['instruments'] == 117" in build_site
    assert "['featured'] == 24" in build_site
    assert "'watch_tier' not in catalog" in build_site
    daily = (ROOT / ".github" / "workflows" / "daily_etf_flows.yml").read_text(encoding="utf-8")
    assert "schedule:" not in daily
    assert "workflow_dispatch" in daily
    assert "exit 1" in daily


def test_no_test_ui_workflow_or_readme_path_reads_legacy_yaml():
    legacy = re.compile(r"(?<!test_)" + "_".join(("etf", "flow", "catalog")))
    targets = [
        *sorted((ROOT / "tests").rglob("*.py")),
        *sorted((ROOT / "docs").glob("*.js")),
        *sorted((ROOT / "docs").glob("*.html")),
        *sorted((ROOT / ".github" / "workflows").glob("*.yml")),
        ROOT / "README.md",
        ROOT / "scripts" / "build_local_flow_artifacts.py",
    ]
    hits = []
    for path in targets:
        if not path.exists():
            continue
        for number, line in enumerate(path.read_text(encoding="utf-8", errors="replace").splitlines(), start=1):
            if legacy.search(line):
                hits.append(f"{path.relative_to(ROOT)}:{number}")
    assert hits == []


def test_authoritative_flow_sources_are_not_git_ignored():
    """Regression guard: data/flows/ source files must stay committable.

    The repo-wide `*.csv` / `*.json` ignore rules must not swallow the
    authoritative local dataset — a fresh clone has to be able to rebuild
    docs/data/flows artifacts in CI. `git check-ignore -q` exits 1 when the
    path is NOT ignored, 0 when it IS ignored.
    """
    git = shutil.which("git")
    if git is None:
        pytest.skip("git executable unavailable")
    probe = subprocess.run(
        [git, "rev-parse", "--is-inside-work-tree"],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    if probe.returncode != 0 or probe.stdout.strip() != "true":
        pytest.skip("not inside a git work tree")
    targets = [
        "data/flows/all_leveraged_etf_flows.csv",
        "data/flows/curated_catalog_summary.json",
        "data/flows/individual/TQQQ_flows.csv",
    ]
    for rel in targets:
        assert (ROOT / rel).is_file(), f"missing authoritative source file {rel}"
        proc = subprocess.run(
            [git, "check-ignore", "-q", rel],
            cwd=ROOT,
            capture_output=True,
            text=True,
        )
        assert proc.returncode == 1, (
            f"{rel} must not be git-ignored; `git check-ignore -q` exited "
            f"{proc.returncode} (1 = not ignored, 0 = ignored). "
            f"stderr={(proc.stderr or '').strip()!r}"
        )
