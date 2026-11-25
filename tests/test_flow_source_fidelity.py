"""Verify, then trust: every daily flow the site shows must equal the source-reported value.

The Fund Flows tab displays flows exactly as the source (Trackinsight) reports them. This test
checks that the published per-ticker artifacts in docs/data/flows/<T>.json carry the same
usd_flow, date for date, as the source copies in data/flows/individual/<T>_flows.csv, so no
step in the pipeline silently changes, nets or drops a reported value.

Run: pytest tests/test_flow_source_fidelity.py
"""
import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SOURCE_DIR = ROOT / "data" / "flows" / "individual"
DOCS_DIR = ROOT / "docs" / "data" / "flows"


def _source_files():
    return sorted(SOURCE_DIR.glob("*_flows.csv"))


def test_every_source_ticker_is_published():
    tickers = [p.name[: -len("_flows.csv")] for p in _source_files()]
    assert len(tickers) >= 150, len(tickers)
    missing = [t for t in tickers if not (DOCS_DIR / f"{t}.json").exists()]
    assert not missing, f"source tickers without a published artifact: {missing[:10]}"


def test_published_flows_equal_source_values():
    mismatches = []
    for src in _source_files():
        ticker = src.name[: -len("_flows.csv")]
        with src.open(newline="", encoding="utf-8") as fh:
            source = {row["date"]: row["usd_flow"] for row in csv.DictReader(fh)}
        published = {
            row["date"]: row.get("usd_flow")
            for row in json.loads((DOCS_DIR / f"{ticker}.json").read_text(encoding="utf-8"))["data"]
        }
        assert set(published) == set(source), f"{ticker}: published dates differ from source"
        for date, raw in source.items():
            want = None if raw in ("", None) else float(raw)
            got = published[date]
            if (want is None) != (got is None) or (want is not None and abs(want - float(got)) > 0.5):
                mismatches.append((ticker, date, raw, got))
    assert not mismatches, f"{len(mismatches)} published flows differ from source, e.g. {mismatches[:5]}"
