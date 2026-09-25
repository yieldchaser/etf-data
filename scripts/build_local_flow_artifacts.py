"""Build deterministic static artifacts from the authoritative local ETF flow dataset."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import re
import shutil
import sys
import tempfile
import uuid
from collections import Counter
from datetime import date
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE_DIR = REPO_ROOT / "data" / "flows"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "docs" / "data" / "flows"
DEFAULT_UNIVERSE_DOC = REPO_ROOT / "FUND_FLOW_ETFS.md"
EXPECTED_UNIVERSE_COUNT = 117
EXPECTED_TOTAL_ROWS = 170392
EXPECTED_WORKBOOK_DATE_ROWS = 2702
ARTIFACT_SCHEMA_VERSION = 1
DATA_SCHEMA_VERSION = 2
CATALOG_VERSION = "local-authoritative-117-v1"
REQUIRED_COLUMNS = (
    "date",
    "ticker",
    "trackinsight_key",
    "usd_flow",
    "nav",
    "perf_pct",
    "cumulative_flow",
    "daily_inflow",
    "daily_outflow",
    "flow_zscore",
    "flow_5d",
    "flow_20d",
    "regime",
    "pressure",
)
OPTIONAL_COLUMNS = ("category", "underlying", "leverage")
AGGREGATE_COLUMNS = REQUIRED_COLUMNS + OPTIONAL_COLUMNS
NUMERIC_COLUMNS = frozenset(
    {
        "usd_flow",
        "nav",
        "perf_pct",
        "cumulative_flow",
        "daily_inflow",
        "daily_outflow",
        "flow_zscore",
        "flow_5d",
        "flow_20d",
        "pressure",
    }
)
TEXT_COLUMNS = frozenset({"date", "ticker", "trackinsight_key", "regime", *OPTIONAL_COLUMNS})
SUMMARY_FIELDS = frozenset(
    {
        "ticker",
        "trackinsight_key",
        "category",
        "fund_name",
        "issuer",
        "underlying",
        "leverage",
        "aum_m",
        "ter",
        "earliest_date",
        "latest_date",
        "rows_count",
        "latest_nav",
        "total_cumulative_flow_m",
        "flow_zscore_latest",
        "regime_latest",
        "pressure_latest",
    }
)
FEATURED_TICKERS = (
    "TQQQ",
    "QLD",
    "UPRO",
    "SPXL",
    "SOXL",
    "TECL",
    "USD",
    "DFEN",
    "FAS",
    "ERX",
    "CURE",
    "NVDL",
    "NVDX",
    "LITX",
    "COHX",
    "PTIR",
    "TSLL",
    "MSTU",
    "CONL",
    "UGL",
    "NUGT",
    "TMF",
    "YINN",
    "SQQQ",
)
TICKER_FILE_PATTERN = re.compile(r"^(?P<ticker>[A-Z0-9]+)_flows\.csv$")
LEVERAGE_PATTERN = re.compile(r"^[+-]?(?:\d+(?:\.\d+)?|\.\d+)x$")


class LocalFlowValidationError(RuntimeError):
    pass


def _error(message: str) -> LocalFlowValidationError:
    return LocalFlowValidationError(message)


def _resolved(path: str | Path) -> Path:
    return Path(path).expanduser().resolve()


def _is_within(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _assert_output_is_safe(source_dir: Path, output_dir: Path) -> None:
    source = _resolved(source_dir)
    output = _resolved(output_dir)
    if output == source or _is_within(output, source):
        raise _error(f"refusing to write generated artifacts inside authoritative source directory: {output}")


def _json_bytes(payload: Any) -> bytes:
    return (
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _payload_hash(payload: dict[str, Any], field: str = "content_sha256") -> str:
    content = {key: value for key, value in payload.items() if key != field}
    return _sha256_bytes(_json_bytes(content))


def _with_content_hash(payload: dict[str, Any]) -> dict[str, Any]:
    result = dict(payload)
    result["content_sha256"] = _payload_hash(result)
    return result


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise _error(f"could not read JSON source {path}: {exc}") from exc


def _parse_date(value: Any, context: str) -> str:
    if hasattr(value, "strftime") and not isinstance(value, str):
        try:
            parsed = value.strftime("%Y-%m-%d")
            date.fromisoformat(parsed)
            return parsed
        except (TypeError, ValueError) as exc:
            raise _error(f"{context} has an invalid date: {value!r}") from exc
    text = str(value or "").strip()[:10]
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise _error(f"{context} has an invalid ISO date: {value!r}") from exc
    if parsed.isoformat() != text:
        raise _error(f"{context} has a non-canonical ISO date: {value!r}")
    return text


def _parse_number(value: Any, field: str, context: str, allow_none: bool = True) -> float | int | None:
    if value is None or (isinstance(value, str) and not value.strip()):
        if allow_none:
            return None
        raise _error(f"{context} is missing required numeric field {field}")
    if isinstance(value, bool):
        raise _error(f"{context} has a boolean in numeric field {field}")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise _error(f"{context} has a non-numeric value in {field}: {value!r}") from exc
    if not math.isfinite(number):
        raise _error(f"{context} has a non-finite value in {field}")
    if field in {"rows_count"}:
        if not number.is_integer():
            raise _error(f"{context} has a non-integer {field}")
        return int(number)
    if number.is_integer() and field in {"usd_flow", "nav", "cumulative_flow", "daily_inflow", "daily_outflow", "flow_5d", "flow_20d"}:
        return int(number)
    return number


def _close(left: Any, right: Any, tolerance: float) -> bool:
    if left is None or right is None:
        return left is right
    try:
        return abs(float(left) - float(right)) <= tolerance
    except (TypeError, ValueError):
        return str(left) == str(right)


def _normalise_leverage(value: Any, context: str) -> tuple[str, float]:
    text = str(value or "").strip()
    if not LEVERAGE_PATTERN.fullmatch(text):
        raise _error(f"{context} has invalid leverage {value!r}")
    try:
        number = float(text[:-1])
    except ValueError as exc:
        raise _error(f"{context} has invalid leverage {value!r}") from exc
    if not math.isfinite(number) or abs(number) < 0.5:
        raise _error(f"{context} is not a leveraged or inverse instrument: {value!r}")
    return text, number


def _read_csv(path: Path, required: tuple[str, ...], allowed: tuple[str, ...]) -> tuple[list[str], list[dict[str, str]]]:
    try:
        handle = path.open(newline="", encoding="utf-8-sig")
    except OSError as exc:
        raise _error(f"could not open CSV source {path}: {exc}") from exc
    with handle:
        reader = csv.DictReader(handle)
        headers = list(reader.fieldnames or [])
        if not headers:
            raise _error(f"CSV source has no header: {path}")
        if len(headers) != len(set(headers)):
            raise _error(f"CSV source has duplicate columns: {path}")
        missing = [column for column in required if column not in headers]
        unknown = [column for column in headers if column not in allowed]
        if missing:
            raise _error(f"CSV source {path} is missing required columns: {', '.join(missing)}")
        if unknown:
            raise _error(f"CSV source {path} has unknown columns: {', '.join(unknown)}")
        rows: list[dict[str, str]] = []
        for index, row in enumerate(reader, start=2):
            if None in row:
                raise _error(f"CSV source {path} has an over-wide row at line {index}")
            rows.append(row)
    return headers, rows


def _normalise_row(raw: dict[str, str], columns: list[str], context: str) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for column in columns:
        value = raw.get(column)
        if column in TEXT_COLUMNS:
            if value is None or not str(value).strip():
                if column in REQUIRED_COLUMNS:
                    raise _error(f"{context} has a missing required field {column}")
                result[column] = None
                continue
            text = str(value).strip()
            result[column] = _parse_date(text, f"{context}.{column}") if column == "date" else text
        elif column in NUMERIC_COLUMNS:
            result[column] = _parse_number(value, column, context, allow_none=column not in REQUIRED_COLUMNS)
        else:
            result[column] = None if value is None or not str(value).strip() else str(value).strip()
    return result


def _load_summary(path: Path, expected_count: int) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    payload = _read_json(path)
    if not isinstance(payload, list):
        raise _error(f"catalog summary must be a JSON list: {path}")
    if len(payload) != expected_count:
        raise _error(f"catalog summary has {len(payload)} rows; expected {expected_count}")
    rows: list[dict[str, Any]] = []
    by_ticker: dict[str, dict[str, Any]] = {}
    for index, raw in enumerate(payload, start=1):
        context = f"catalog summary row {index}"
        if not isinstance(raw, dict):
            raise _error(f"{context} is not an object")
        missing = sorted(SUMMARY_FIELDS - set(raw))
        unknown = sorted(set(raw) - SUMMARY_FIELDS)
        if missing:
            raise _error(f"{context} is missing fields: {', '.join(missing)}")
        if unknown:
            raise _error(f"{context} has unknown fields: {', '.join(unknown)}")
        item = dict(raw)
        item["ticker"] = str(item["ticker"]).strip().upper()
        item["trackinsight_key"] = str(item["trackinsight_key"]).strip()
        for field in ("category", "fund_name", "issuer", "underlying", "ter", "regime_latest"):
            if not str(item[field]).strip():
                raise _error(f"{context} has an empty {field}")
        item["earliest_date"] = _parse_date(item["earliest_date"], f"{context}.earliest_date")
        item["latest_date"] = _parse_date(item["latest_date"], f"{context}.latest_date")
        if item["earliest_date"] > item["latest_date"]:
            raise _error(f"{context} has a reversed date range")
        item["rows_count"] = _parse_number(item["rows_count"], "rows_count", context, allow_none=False)
        for field in ("aum_m", "latest_nav", "total_cumulative_flow_m", "flow_zscore_latest", "pressure_latest"):
            item[field] = _parse_number(item[field], field, context, allow_none=False)
        item["leverage"], _ = _normalise_leverage(item["leverage"], context)
        if item["ticker"] in by_ticker:
            raise _error(f"catalog summary contains duplicate ticker {item['ticker']}")
        by_ticker[item["ticker"]] = item
        rows.append(item)
    return rows, by_ticker


def _parse_markdown_tickers(path: Path) -> set[str]:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise _error(f"could not read universe document {path}: {exc}") from exc
    tickers = set(re.findall(r"^\|\s*\*\*([A-Z0-9]+)\*\*\s*\|", text, re.MULTILINE))
    if "Total Instruments:** 117" not in text:
        raise _error(f"universe document does not declare the authoritative 117-instrument universe: {path}")
    return tickers


def _underlying_matches_summary(value: Any, expected: Any) -> bool:
    text = str(value or "").strip()
    summary = str(expected or "").strip()
    return text == summary or text.startswith(summary + " (")


def _validate_individual_files(
    source_dir: Path,
    summary_by_ticker: dict[str, dict[str, Any]],
    expected_count: int,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    individual_dir = source_dir / "individual"
    if not individual_dir.is_dir():
        raise _error(f"individual source directory is missing: {individual_dir}")
    entries = sorted(individual_dir.iterdir(), key=lambda item: item.name)
    if any(not item.is_file() for item in entries):
        raise _error(f"individual source directory contains a non-file entry: {individual_dir}")
    paths: list[Path] = []
    for path in entries:
        if not TICKER_FILE_PATTERN.fullmatch(path.name):
            raise _error(f"unexpected file in individual source directory: {path.name}")
        paths.append(path)
    if len(paths) != expected_count:
        raise _error(f"individual source has {len(paths)} files; expected {expected_count}")
    tickers = [TICKER_FILE_PATTERN.fullmatch(path.name).group("ticker") for path in paths]
    if len(set(tickers)) != expected_count:
        raise _error("individual source contains duplicate ticker filenames")
    if set(tickers) != set(summary_by_ticker):
        missing = sorted(set(summary_by_ticker) - set(tickers))
        extra = sorted(set(tickers) - set(summary_by_ticker))
        raise _error(f"individual source identity mismatch; missing={missing}, extra={extra}")
    files: dict[str, dict[str, Any]] = {}
    variants: Counter[str] = Counter()
    metadata_discrepancies: list[str] = []
    total_rows = 0
    for path, ticker in zip(paths, tickers):
        context = f"individual source {path.name}"
        headers, raw_rows = _read_csv(path, REQUIRED_COLUMNS, REQUIRED_COLUMNS + OPTIONAL_COLUMNS)
        variants[",".join(headers)] += 1
        if not raw_rows:
            raise _error(f"{context} is empty")
        rows = [_normalise_row(row, headers, f"{context} row {index}") for index, row in enumerate(raw_rows, start=2)]
        dates = [row["date"] for row in rows]
        if dates != sorted(dates):
            raise _error(f"{context} dates are not strictly increasing")
        if len(dates) != len(set(dates)):
            duplicates = sorted(date for date, count in Counter(dates).items() if count > 1)
            raise _error(f"{context} has duplicate dates: {', '.join(duplicates)}")
        summary = summary_by_ticker[ticker]
        expected_key = str(summary["trackinsight_key"])
        for index, row in enumerate(rows, start=2):
            if row["ticker"] != ticker:
                raise _error(f"{context} row {index} ticker is {row['ticker']!r}, expected {ticker!r}")
            if row["trackinsight_key"] != expected_key:
                raise _error(f"{context} row {index} Trackinsight key is {row['trackinsight_key']!r}, expected {expected_key!r}")
            for optional, summary_field in (("category", "category"), ("underlying", "underlying"), ("leverage", "leverage")):
                if optional not in headers:
                    continue
                if optional == "underlying":
                    if not _underlying_matches_summary(row[optional], summary[summary_field]):
                        raise _error(f"{context} row {index} underlying does not match catalog summary")
                    if row[optional] != summary[summary_field]:
                        discrepancy = f"{ticker}.{optional} raw label differs from summary label"
                        if discrepancy not in metadata_discrepancies:
                            metadata_discrepancies.append(discrepancy)
                elif row[optional] != summary[summary_field]:
                    raise _error(f"{context} row {index} {optional} does not match catalog summary")
        if len(rows) != summary["rows_count"]:
            raise _error(f"{context} has {len(rows)} rows; summary declares {summary['rows_count']}")
        first = rows[0]
        last = rows[-1]
        if first["date"] != summary["earliest_date"] or last["date"] != summary["latest_date"]:
            raise _error(f"{context} date range does not match catalog summary")
        if not _close(last["nav"], summary["latest_nav"], 0.001):
            raise _error(f"{context} latest NAV does not match catalog summary")
        if not _close(last["cumulative_flow"] / 1_000_000, summary["total_cumulative_flow_m"], 0.02):
            raise _error(f"{context} cumulative flow does not match catalog summary")
        if not _close(last["flow_zscore"], summary["flow_zscore_latest"], 0.01):
            raise _error(f"{context} latest z-score does not match catalog summary")
        if last["regime"] != summary["regime_latest"]:
            raise _error(f"{context} latest regime does not match catalog summary")
        if not _close(last["pressure"], summary["pressure_latest"], 0.11):
            raise _error(f"{context} latest pressure does not match catalog summary")
        files[ticker] = {
            "ticker": ticker,
            "path": path,
            "relative_path": f"data/flows/individual/{path.name}",
            "sha256": _sha256_file(path),
            "headers": headers,
            "data": rows,
            "row_count": len(rows),
            "earliest_date": first["date"],
            "latest_date": last["date"],
            "missing_required_values": 0,
            "duplicate_dates": 0,
            "latest": last,
            "metadata_discrepancies": [
                discrepancy
                for discrepancy in metadata_discrepancies
                if discrepancy.startswith(f"{ticker}.")
            ],
        }
        total_rows += len(rows)
    return files, {
        "file_count": len(files),
        "total_rows": total_rows,
        "column_variants": dict(variants),
        "metadata_discrepancies": metadata_discrepancies,
    }


def _validate_aggregate(
    source_dir: Path,
    summary_by_ticker: dict[str, dict[str, Any]],
    files: dict[str, dict[str, Any]],
    expected_total_rows: int,
) -> dict[str, Any]:
    path = source_dir / "all_leveraged_etf_flows.csv"
    headers, raw_rows = _read_csv(path, AGGREGATE_COLUMNS, AGGREGATE_COLUMNS)
    if len(raw_rows) != expected_total_rows:
        raise _error(f"aggregate source has {len(raw_rows)} rows; expected {expected_total_rows}")
    if headers != list(AGGREGATE_COLUMNS):
        raise _error(f"aggregate source columns are not in the required order: {headers}")
    source_by_key = {
        (ticker, row["date"]): row
        for ticker, info in files.items()
        for row in info["data"]
    }
    seen: set[tuple[str, str]] = set()
    dates: set[str] = set()
    for index, raw in enumerate(raw_rows, start=2):
        context = f"aggregate source row {index}"
        row = _normalise_row(raw, headers, context)
        ticker = row["ticker"]
        if ticker not in summary_by_ticker:
            raise _error(f"{context} has ticker {ticker!r} outside the catalog summary")
        identity = (ticker, row["date"])
        if identity in seen:
            raise _error(f"{context} duplicates ticker/date {ticker}/{row['date']}")
        seen.add(identity)
        dates.add(row["date"])
        source_row = source_by_key.get(identity)
        if source_row is None:
            raise _error(f"{context} has no matching individual row {ticker}/{row['date']}")
        for field in REQUIRED_COLUMNS:
            if field in {"ticker", "trackinsight_key", "date", "regime"}:
                if row[field] != source_row[field]:
                    raise _error(f"{context} differs from individual source in {field}")
            elif not _close(row[field], source_row[field], 0.000001):
                raise _error(f"{context} differs from individual source in {field}")
        summary = summary_by_ticker[ticker]
        for field in OPTIONAL_COLUMNS:
            if field in source_row and row[field] != source_row[field]:
                raise _error(f"{context} differs from individual source in {field}")
            if field == "underlying":
                if not _underlying_matches_summary(row[field], summary[field]):
                    raise _error(f"{context} differs from catalog summary in {field}")
            elif row[field] != summary[field]:
                raise _error(f"{context} differs from catalog summary in {field}")
    if seen != set(source_by_key):
        missing = sorted(set(source_by_key) - seen)
        raise _error(f"aggregate source is missing {len(missing)} individual rows")
    return {
        "path": path,
        "relative_path": "data/flows/all_leveraged_etf_flows.csv",
        "sha256": _sha256_file(path),
        "headers": headers,
        "row_count": len(raw_rows),
        "unique_ticker_count": len({row[0] for row in seen}),
        "date_count": len(dates),
        "earliest_date": min(dates),
        "latest_date": max(dates),
    }


def _excel_value(value: Any, field: str, context: str) -> Any:
    try:
        import pandas as pd
    except ImportError as exc:
        raise _error("pandas is required to validate the authoritative workbook") from exc
    if pd.isna(value):
        return None
    if field in {"earliest_date", "latest_date"}:
        return _parse_date(value, context)
    if field in {"rows_count"}:
        return _parse_number(value, field, context, allow_none=False)
    if field in {"aum_m", "latest_nav", "total_cumulative_flow_m", "flow_zscore_latest", "pressure_latest"}:
        return _parse_number(value, field, context, allow_none=False)
    return str(value).strip()


def _validate_workbook(
    source_dir: Path,
    summary_by_ticker: dict[str, dict[str, Any]],
    files: dict[str, dict[str, Any]],
    aggregate: dict[str, Any],
) -> dict[str, Any]:
    try:
        import pandas as pd
    except ImportError as exc:
        raise _error("pandas is required to validate the authoritative workbook") from exc
    path = source_dir / "Leveraged_ETF_Flows_Master.xlsx"
    if not path.is_file():
        raise _error(f"authoritative workbook is missing: {path}")
    try:
        workbook = pd.ExcelFile(path)
    except Exception as exc:
        raise _error(f"could not read authoritative workbook: {exc}") from exc
    expected_sheets = {"Universe_Catalog", "Summary_Latest", "Daily_Flows_Wide", "Cumulative_Flows_Wide", "NAV_Wide"}
    if set(workbook.sheet_names) != expected_sheets:
        raise _error(f"workbook sheets changed: {workbook.sheet_names}")
    universe = pd.read_excel(workbook, sheet_name="Universe_Catalog")
    universe_columns = [
        "ticker",
        "trackinsight_key",
        "category",
        "fund_name",
        "issuer",
        "underlying",
        "leverage",
        "aum_m",
        "ter",
        "earliest_date",
        "latest_date",
        "rows_count",
        "latest_nav",
        "total_cumulative_flow_m",
        "flow_zscore_latest",
        "regime_latest",
        "pressure_latest",
    ]
    if list(universe.columns) != universe_columns or len(universe) != len(summary_by_ticker):
        raise _error("workbook Universe_Catalog schema or row count changed")
    universe_tickers = [str(value).strip().upper() for value in universe["ticker"]]
    if len(set(universe_tickers)) != len(universe_tickers) or set(universe_tickers) != set(summary_by_ticker):
        raise _error("workbook Universe_Catalog identity does not match catalog summary")
    for ticker in universe_tickers:
        row = universe.loc[universe["ticker"] == ticker].iloc[0]
        summary = summary_by_ticker[ticker]
        for field in universe_columns:
            actual = _excel_value(row[field], field, f"workbook Universe_Catalog {ticker}.{field}")
            expected = summary[field]
            if field in {"aum_m", "latest_nav", "total_cumulative_flow_m", "flow_zscore_latest", "pressure_latest"}:
                if not _close(actual, expected, 0.001):
                    raise _error(f"workbook Universe_Catalog {ticker} differs in {field}")
            elif str(actual) != str(expected):
                raise _error(f"workbook Universe_Catalog {ticker} differs in {field}")
    latest = pd.read_excel(workbook, sheet_name="Summary_Latest")
    latest_columns = [
        "ticker",
        "category",
        "fund_name",
        "issuer",
        "underlying",
        "leverage",
        "latest_date",
        "latest_nav",
        "total_cumulative_flow_m",
        "flow_zscore_latest",
        "regime_latest",
        "pressure_latest",
    ]
    if list(latest.columns) != latest_columns or len(latest) != len(summary_by_ticker):
        raise _error("workbook Summary_Latest schema or row count changed")
    for ticker in summary_by_ticker:
        row = latest.loc[latest["ticker"] == ticker].iloc[0]
        summary = summary_by_ticker[ticker]
        for field in latest_columns[1:]:
            actual = _excel_value(row[field], field, f"workbook Summary_Latest {ticker}.{field}")
            expected = summary[field]
            if field in {"latest_nav", "total_cumulative_flow_m", "flow_zscore_latest", "pressure_latest"}:
                if not _close(actual, expected, 0.001):
                    raise _error(f"workbook Summary_Latest {ticker} differs in {field}")
            elif str(actual) != str(expected):
                raise _error(f"workbook Summary_Latest {ticker} differs in {field}")
    expected_dates = set()
    for info in files.values():
        expected_dates.update(row["date"] for row in info["data"])
    if len(expected_dates) != EXPECTED_WORKBOOK_DATE_ROWS or len(expected_dates) != aggregate["date_count"]:
        raise _error("authoritative date union is not the expected 2702 observations")
    sheet_specs = {
        "Daily_Flows_Wide": "usd_flow",
        "Cumulative_Flows_Wide": "cumulative_flow",
        "NAV_Wide": "nav",
    }
    workbook_sheets: dict[str, Any] = {}
    for sheet_name, field in sheet_specs.items():
        frame = pd.read_excel(workbook, sheet_name=sheet_name)
        if len(frame) != EXPECTED_WORKBOOK_DATE_ROWS or len(frame.columns) != len(summary_by_ticker) + 1:
            raise _error(f"workbook {sheet_name} dimensions changed")
        if str(frame.columns[0]) != "date" or set(frame.columns[1:]) != set(summary_by_ticker):
            raise _error(f"workbook {sheet_name} identity columns changed")
        date_values = [_parse_date(value, f"workbook {sheet_name}.date") for value in frame["date"]]
        if date_values != sorted(date_values) or len(set(date_values)) != len(date_values) or set(date_values) != expected_dates:
            raise _error(f"workbook {sheet_name} date axis does not match individual sources")
        values_by_ticker = {
            ticker: {row["date"]: row[field] for row in files[ticker]["data"]}
            for ticker in summary_by_ticker
        }
        for ticker in summary_by_ticker:
            actual_by_date: dict[str, float | None] = {}
            for date_value, raw_value in zip(date_values, frame[ticker].tolist()):
                try:
                    actual_by_date[date_value] = None if pd.isna(raw_value) else float(raw_value)
                except (TypeError, ValueError):
                    actual_by_date[date_value] = str(raw_value)
            expected_by_date = values_by_ticker[ticker]
            for date_value in date_values:
                actual = actual_by_date[date_value]
                expected = expected_by_date.get(date_value)
                if expected is None:
                    if actual is not None:
                        raise _error(f"workbook {sheet_name} has an unexpected value for {ticker}/{date_value}")
                elif not _close(actual, expected, 0.000001):
                    raise _error(f"workbook {sheet_name} differs for {ticker}/{date_value}")
        workbook_sheets[sheet_name] = {"rows": len(frame), "columns": len(frame.columns)}
    return {
        "path": path,
        "relative_path": "data/flows/Leveraged_ETF_Flows_Master.xlsx",
        "sha256": _sha256_file(path),
        "sheets": workbook_sheets,
        "catalog_rows": len(universe),
        "latest_rows": len(latest),
    }


def validate_local_dataset(
    source_dir: str | Path = DEFAULT_SOURCE_DIR,
    expected_count: int = EXPECTED_UNIVERSE_COUNT,
    expected_total_rows: int | None = EXPECTED_TOTAL_ROWS,
    validate_workbook: bool = True,
    validate_markdown: bool = True,
    universe_doc_path: str | Path = DEFAULT_UNIVERSE_DOC,
) -> dict[str, Any]:
    """Validate every authoritative local source and return a build report."""
    source = _resolved(source_dir)
    if not source.is_dir():
        raise _error(f"authoritative source directory is missing: {source}")
    if expected_total_rows is None:
        expected_total_rows = 0
    summary_path = source / "curated_catalog_summary.json"
    summary_rows, summary_by_ticker = _load_summary(summary_path, expected_count)
    files, individual_report = _validate_individual_files(source, summary_by_ticker, expected_count)
    actual_total = individual_report["total_rows"]
    if expected_total_rows and actual_total != expected_total_rows:
        raise _error(f"individual source has {actual_total} rows; expected {expected_total_rows}")
    aggregate = _validate_aggregate(source, summary_by_ticker, files, expected_total_rows or actual_total)
    if aggregate["row_count"] != actual_total:
        raise _error("aggregate and individual row totals differ")
    if aggregate["unique_ticker_count"] != expected_count:
        raise _error("aggregate does not contain exactly the expected ticker universe")
    if set(FEATURED_TICKERS) - set(summary_by_ticker) or len(FEATURED_TICKERS) != 24 or len(set(FEATURED_TICKERS)) != 24:
        raise _error("featured ticker subset must contain exactly 24 tickers from the local universe")
    if validate_markdown:
        markdown_tickers = _parse_markdown_tickers(_resolved(universe_doc_path))
        if markdown_tickers != set(summary_by_ticker):
            raise _error(
                "universe document identity mismatch; "
                f"missing={sorted(set(summary_by_ticker) - markdown_tickers)}, "
                f"extra={sorted(markdown_tickers - set(summary_by_ticker))}"
            )
    workbook = _validate_workbook(source, summary_by_ticker, files, aggregate) if validate_workbook else None
    source_hashes = {
        "data/flows/curated_catalog_summary.json": _sha256_file(summary_path),
        "data/flows/all_leveraged_etf_flows.csv": aggregate["sha256"],
        "data/flows/Leveraged_ETF_Flows_Master.xlsx": workbook["sha256"] if workbook else None,
        "data/flows/individual": {ticker: info["sha256"] for ticker, info in sorted(files.items())},
    }
    category_counts = Counter(item["category"] for item in summary_rows)
    return {
        "status": "ok",
        "expected": {
            "universe_count": expected_count,
            "total_rows": expected_total_rows or actual_total,
            "featured_count": 24,
        },
        "actual": {
            "catalog_rows": len(summary_rows),
            "individual_files": individual_report["file_count"],
            "individual_rows": actual_total,
            "aggregate_rows": aggregate["row_count"],
            "workbook_catalog_rows": workbook["catalog_rows"] if workbook else None,
            "workbook_latest_rows": workbook["latest_rows"] if workbook else None,
            "unique_tickers": len(summary_by_ticker),
            "featured_tickers": len(FEATURED_TICKERS),
            "earliest_date": min(info["earliest_date"] for info in files.values()),
            "latest_date": max(info["latest_date"] for info in files.values()),
            "duplicate_ticker_dates": 0,
            "missing_required_values": 0,
            "metadata_discrepancy_count": len(individual_report["metadata_discrepancies"]),
            "metadata_discrepancies": individual_report["metadata_discrepancies"],
        },
        "category_counts": dict(sorted(category_counts.items())),
        "featured_tickers": list(FEATURED_TICKERS),
        "summary": summary_by_ticker,
        "files": files,
        "individual": individual_report,
        "aggregate": aggregate,
        "workbook": workbook,
        "source_hashes": source_hashes,
    }


def _relative_source_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(REPO_ROOT.resolve()).as_posix()
    except ValueError:
        return path.name


def _quality(info: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": "complete",
        "schema": "valid",
        "duplicate_dates": info["duplicate_dates"],
        "missing_required_values": info["missing_required_values"],
        "row_count_consistent": True,
        "identity_consistent": True,
        "date_range_consistent": True,
    }


def _instrument_entry(ticker: str, report: dict[str, Any]) -> dict[str, Any]:
    info = report["files"][ticker]
    summary = report["summary"][ticker]
    last = info["latest"]
    leverage_text, leverage_value = _normalise_leverage(summary["leverage"], f"catalog {ticker}")
    return {
        "ticker": ticker,
        "trackinsight_key": summary["trackinsight_key"],
        "category": summary["category"],
        "fund_name": summary["fund_name"],
        "issuer": summary["issuer"],
        "underlying": summary["underlying"],
        "leverage": leverage_text,
        "leverage_value": leverage_value,
        "aum_m": summary["aum_m"],
        "ter": summary["ter"],
        "earliest_date": summary["earliest_date"],
        "latest_date": summary["latest_date"],
        "row_count": info["row_count"],
        "rows_count": info["row_count"],
        "latest_nav": last["nav"],
        "latest_flow": last["usd_flow"],
        "latest_cumulative_flow": last["cumulative_flow"],
        "flow_zscore": last["flow_zscore"],
        "regime": last["regime"],
        "pressure": last["pressure"],
        "featured": ticker in FEATURED_TICKERS,
        "source": "Local dataset",
        "source_provider": "Trackinsight",
        "source_mode": "historical_local",
        "source_file": info["relative_path"],
        "source_sha256": info["sha256"],
        "data_quality": _quality(info),
        "data_quality_status": "complete",
        "source_summary": summary,
        "latest": {
            "date": last["date"],
            "nav": last["nav"],
            "flow": last["usd_flow"],
            "cumulative_flow": last["cumulative_flow"],
            "flow_zscore": last["flow_zscore"],
            "regime": last["regime"],
            "pressure": last["pressure"],
        },
    }


def _build_catalog(report: dict[str, Any]) -> dict[str, Any]:
    instruments = [
        _instrument_entry(ticker, report)
        for ticker in sorted(report["summary"])
    ]
    payload: dict[str, Any] = {
        "schema_version": ARTIFACT_SCHEMA_VERSION,
        "catalog_version": CATALOG_VERSION,
        "source": {
            "name": "Local authoritative dataset",
            "provider": "Trackinsight",
            "mode": "historical_local",
            "network_fetch": False,
            "description": "Local historical fields reported by Trackinsight; no live fetch is performed.",
        },
        "counts": {
            "instruments": len(instruments),
            "featured": len(FEATURED_TICKERS),
            "rows": report["actual"]["individual_rows"],
        },
        "category_counts": report["category_counts"],
        "featured_tickers": list(FEATURED_TICKERS),
        "instruments": instruments,
    }
    return _with_content_hash(payload)


def _build_ticker_payload(ticker: str, report: dict[str, Any]) -> dict[str, Any]:
    info = report["files"][ticker]
    entry = _instrument_entry(ticker, report)
    payload: dict[str, Any] = {
        "schema_version": DATA_SCHEMA_VERSION,
        "artifact_version": ARTIFACT_SCHEMA_VERSION,
        "catalog_version": CATALOG_VERSION,
        "ticker": ticker,
        "key": info["data"][0]["trackinsight_key"],
        "trackinsight_key": info["data"][0]["trackinsight_key"],
        "source": "Local dataset",
        "source_provider": "Trackinsight",
        "source_mode": "historical_local",
        "source_file": info["relative_path"],
        "source_sha256": info["sha256"],
        "source_asof": info["latest_date"],
        "updated": info["latest_date"],
        "count": info["row_count"],
        "row_count": info["row_count"],
        "earliest_date": info["earliest_date"],
        "latest_date": info["latest_date"],
        "data_status": "complete",
        "data_quality": _quality(info),
        "raw_columns": info["headers"],
        "data": info["data"],
        "catalog_entry": entry,
    }
    return _with_content_hash(payload)


def _build_manifest(report: dict[str, Any]) -> dict[str, Any]:
    entries: dict[str, Any] = {}
    for ticker in sorted(report["files"]):
        info = report["files"][ticker]
        entries[ticker] = {
            "ticker": ticker,
            "path": f"{ticker}.json",
            "trackinsight_key": info["data"][0]["trackinsight_key"],
            "records": info["row_count"],
            "earliest_date": info["earliest_date"],
            "latest_date": info["latest_date"],
            "source_asof": info["latest_date"],
            "availability": "available",
            "data_status": "complete",
            "data_quality": "complete",
            "content_sha256": _payload_hash(_build_ticker_payload(ticker, report)),
        }
    payload: dict[str, Any] = {
        "schema_version": DATA_SCHEMA_VERSION,
        "manifest_version": ARTIFACT_SCHEMA_VERSION,
        "catalog_version": CATALOG_VERSION,
        "status": "complete",
        "complete": True,
        "source": {
            "name": "Local authoritative dataset",
            "provider": "Trackinsight",
            "mode": "historical_local",
            "network_fetch": False,
            "description": "Historical fields are read from local CSV and XLSX sources; no live fetch is performed.",
        },
        "counts": {
            "instruments": len(entries),
            "featured": len(FEATURED_TICKERS),
            "rows": report["actual"]["individual_rows"],
            "files": len(entries),
        },
        "category_counts": report["category_counts"],
        "earliest_date": report["actual"]["earliest_date"],
        "latest_date": report["actual"]["latest_date"],
        "source_asof": report["actual"]["latest_date"],
        "quality": {
            "status": "complete",
            "duplicate_ticker_dates": 0,
            "missing_required_values": 0,
            "required_columns": list(REQUIRED_COLUMNS),
            "optional_columns_seen": sorted({column for info in report["files"].values() for column in info["headers"] if column in OPTIONAL_COLUMNS}),
            "workbook_checked": report["workbook"] is not None,
            "aggregate_checked": True,
            "universe_document_checked": True,
        },
        "source_files": report["source_hashes"],
        "etfs": entries,
    }
    return _with_content_hash(payload)


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    serialized = _json_bytes(payload)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(serialized)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _expected_output_names(report: dict[str, Any]) -> set[str]:
    return {"catalog.json", "manifest.json", *(f"{ticker}.json" for ticker in report["files"])}


def _verify_generated_output(output_dir: Path, report: dict[str, Any]) -> None:
    if not output_dir.is_dir():
        raise _error(f"generated output directory is missing: {output_dir}")
    actual_names = {path.name for path in output_dir.iterdir() if path.is_file()}
    expected_names = _expected_output_names(report)
    if actual_names != expected_names:
        extra = sorted(actual_names - expected_names)
        missing = sorted(expected_names - actual_names)
        raise _error(f"generated output file set mismatch; extra={extra}, missing={missing}")
    if any(path.is_symlink() for path in output_dir.iterdir()):
        raise _error("generated output contains a symbolic link")
    catalog = _read_json(output_dir / "catalog.json")
    manifest = _read_json(output_dir / "manifest.json")
    if not isinstance(catalog, dict) or not isinstance(manifest, dict):
        raise _error("generated catalog and manifest must be JSON objects")
    if catalog.get("content_sha256") != _payload_hash(catalog):
        raise _error("generated catalog content hash is invalid")
    if manifest.get("content_sha256") != _payload_hash(manifest):
        raise _error("generated manifest content hash is invalid")
    if catalog.get("counts", {}).get("instruments") != len(report["files"]):
        raise _error("generated catalog count is inconsistent")
    if catalog.get("counts", {}).get("featured") != 24:
        raise _error("generated catalog featured count is inconsistent")
    catalog_items = catalog.get("instruments")
    manifest_items = manifest.get("etfs")
    if not isinstance(catalog_items, list) or not isinstance(manifest_items, dict):
        raise _error("generated catalog or manifest has an invalid item collection")
    tickers = sorted(report["files"])
    if sorted(item.get("ticker") for item in catalog_items) != tickers:
        raise _error("generated catalog identity does not match validated sources")
    if sorted(manifest_items) != tickers:
        raise _error("generated manifest identity does not match validated sources")
    if manifest.get("complete") is not True or manifest.get("status") != "complete":
        raise _error("generated manifest is not complete")
    if manifest.get("counts", {}).get("rows") != report["actual"]["individual_rows"]:
        raise _error("generated manifest row count is inconsistent")
    expected_catalog = _build_catalog(report)
    if catalog != expected_catalog:
        raise _error("generated catalog does not match deterministic source projection")
    expected_manifest = _build_manifest(report)
    if manifest != expected_manifest:
        raise _error("generated manifest does not match deterministic source projection")
    for ticker in tickers:
        payload = _read_json(output_dir / f"{ticker}.json")
        if not isinstance(payload, dict):
            raise _error(f"generated artifact for {ticker} is not an object")
        if payload.get("content_sha256") != _payload_hash(payload):
            raise _error(f"generated artifact hash is invalid for {ticker}")
        expected_payload = _build_ticker_payload(ticker, report)
        if payload != expected_payload:
            raise _error(f"generated artifact does not match source data for {ticker}")


def _publish_staging(staging: Path, output_dir: Path) -> None:
    backup: Path | None = None
    if output_dir.exists() or output_dir.is_symlink():
        if output_dir.is_symlink() or not output_dir.is_dir():
            raise _error(f"generated output path is not a real directory: {output_dir}")
        backup = output_dir.with_name(f".{output_dir.name}.previous-{uuid.uuid4().hex}")
        os.replace(output_dir, backup)
    try:
        os.replace(staging, output_dir)
    except Exception:
        if backup is not None and backup.exists() and not output_dir.exists():
            os.replace(backup, output_dir)
        raise
    if backup is not None and backup.exists():
        shutil.rmtree(backup)


def build_local_artifacts(
    source_dir: str | Path = DEFAULT_SOURCE_DIR,
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    validate_workbook: bool = True,
    validate_markdown: bool = True,
    universe_doc_path: str | Path = DEFAULT_UNIVERSE_DOC,
) -> dict[str, Any]:
    """Validate sources, build artifacts in staging, verify, and publish atomically."""
    source = _resolved(source_dir)
    output = _resolved(output_dir)
    _assert_output_is_safe(source, output)
    report = validate_local_dataset(
        source,
        validate_workbook=validate_workbook,
        validate_markdown=validate_markdown,
        universe_doc_path=universe_doc_path,
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{output.name}.staging-", dir=output.parent))
    try:
        _atomic_write_json(staging / "catalog.json", _build_catalog(report))
        for ticker in sorted(report["files"]):
            _atomic_write_json(staging / f"{ticker}.json", _build_ticker_payload(ticker, report))
        _atomic_write_json(staging / "manifest.json", _build_manifest(report))
        _verify_generated_output(staging, report)
        _publish_staging(staging, output)
    except Exception:
        if staging.exists():
            shutil.rmtree(staging)
        raise
    return {
        "status": "ok",
        "output_dir": str(output),
        "catalog": str(output / "catalog.json"),
        "manifest": str(output / "manifest.json"),
        "ticker_artifacts": len(report["files"]),
        "row_count": report["actual"]["individual_rows"],
        "source_asof": report["actual"]["latest_date"],
    }


def _json_safe(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    return value


def _public_report(report: dict[str, Any]) -> dict[str, Any]:
    return _json_safe({
        "status": report["status"],
        "expected": report["expected"],
        "actual": report["actual"],
        "category_counts": report["category_counts"],
        "featured_tickers": report["featured_tickers"],
        "individual": report["individual"],
        "aggregate": {
            key: value
            for key, value in report["aggregate"].items()
            if key not in {"headers", "path"}
        },
        "workbook": report["workbook"],
        "source_hashes": report["source_hashes"],
    })


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build static ETF flow artifacts from local sources only")
    parser.add_argument("--source-dir", type=Path, default=DEFAULT_SOURCE_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--universe-doc", type=Path, default=DEFAULT_UNIVERSE_DOC)
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--verify-output", action="store_true")
    parser.add_argument("--skip-workbook", action="store_true")
    parser.add_argument("--skip-universe-doc", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.validate_only or args.verify_output:
            report = validate_local_dataset(
                args.source_dir,
                validate_workbook=not args.skip_workbook,
                validate_markdown=not args.skip_universe_doc,
                universe_doc_path=args.universe_doc,
            )
            if args.verify_output:
                _verify_generated_output(_resolved(args.output_dir), report)
            print(json.dumps(_public_report(report), indent=2, ensure_ascii=False, sort_keys=True))
            return 0
        result = build_local_artifacts(
            args.source_dir,
            args.output_dir,
            validate_workbook=not args.skip_workbook,
            validate_markdown=not args.skip_universe_doc,
            universe_doc_path=args.universe_doc,
        )
        print(json.dumps(result, indent=2, ensure_ascii=False, sort_keys=True))
        return 0
    except (LocalFlowValidationError, OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
