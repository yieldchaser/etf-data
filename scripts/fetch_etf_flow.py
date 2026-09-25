"""Legacy Trackinsight ingestion kept behind an explicit unsafe opt-in; local artifacts are the default."""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import os
import random
import re
import sys
import tempfile
import time
from urllib.parse import quote
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd
import yaml
from dateutil.relativedelta import relativedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("flow_scraper")

REPO_ROOT = Path(__file__).resolve().parents[1]
CATALOG_PATH = REPO_ROOT / "data" / "etf_flow_catalog.yaml"
OUT_DIR = REPO_ROOT / "docs" / "data" / "flows"
MANIFEST_PATH = OUT_DIR / "curated_manifest.json"
UI_CATALOG_PATH = OUT_DIR / "catalog.json"
ENDPOINT = "https://www.trackinsight.com/search-api/snapshot/get_snapshots"
HOMEPAGE = "https://www.trackinsight.com/en"
SCHEMA_VERSION = 2
MANIFEST_CONTENT_HASH_FIELD = "content_sha256"
DEFAULT_OVERLAP_DAYS = 7
DEFAULT_WINDOW_MONTHS = 3
DEFAULT_WINDOWS_PER_REQUEST = 3
MIN_WINDOWS_PER_REQUEST = 1
MAX_WINDOWS_PER_REQUEST = 4
WAF_COOLDOWN_HOURS = 24
SOURCE_CURRENCY = "USD"
DERIVED_FLOW_FIELDS = (
    "cumulative_flow",
    "daily_inflow",
    "daily_outflow",
    "flow_zscore",
    "regime",
    "pressure",
    "flow_5d",
    "flow_20d",
)
DERIVED_NUMERIC_TOLERANCES = {
    "cumulative_flow": 0.01,
    "daily_inflow": 0.01,
    "daily_outflow": 0.01,
    "flow_zscore": 0.001,
    "pressure": 0.1,
    "flow_5d": 0.01,
    "flow_20d": 0.01,
}
LOCAL_INDEX_PATH = REPO_ROOT / "docs" / "data" / "etf_search_index.json"
RAW_FLOW_DIR = REPO_ROOT / "data" / "flows"
EXTERNAL_INGESTION_ENABLED = False
DEFAULT_MAX_TICKERS = 3
DEFAULT_MAX_REQUESTS = 3
DEFAULT_MAX_WINDOWS = 100
DEFAULT_MAX_RUNTIME_SECONDS = 900

_FETCH_JS = """
async ({endpoint, payload, timeoutMs}) => {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
        const response = await fetch(endpoint, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-Requested-With': 'XMLHttpRequest'
            },
            credentials: 'include',
            body: JSON.stringify(payload),
            signal: controller.signal
        });
        const retryAfter = response.headers.get('retry-after');
        const text = await response.text();
        if (!response.ok) {
            return {
                __error: true,
                status: response.status,
                statusText: response.statusText,
                retryAfter
            };
        }
        const lower = text.toLowerCase();
        if (lower.includes('human verification') || lower.includes('aws waf') || lower.includes('captcha') || lower.includes('challenge')) {
            return {
                __error: true,
                status: 200,
                statusText: 'Challenge response',
                retryAfter,
                challenge: true,
                bodySnippet: text.slice(0, 240)
            };
        }
        try {
            return JSON.parse(text);
        } catch (error) {
            return {
                __error: true,
                status: 502,
                statusText: 'Invalid JSON response',
                retryAfter
            };
        }
    } catch (error) {
        return {
            __error: true,
            status: 0,
            statusText: String(error),
            retryAfter: null
        };
    } finally {
        clearTimeout(timer);
    }
}
"""


class FlowError(RuntimeError):
    pass


class CatalogError(FlowError):
    pass


class DataValidationError(FlowError):
    pass


class CircuitOpen(FlowError):
    pass


class BudgetExceeded(FlowError):
    pass


class APIError(FlowError):
    def __init__(
        self,
        message: str,
        status: int = 0,
        retry_after: float | None = None,
        hard_stop: bool = False,
    ):
        super().__init__(message)
        self.status = status
        self.retry_after = retry_after
        self.hard_stop = hard_stop

    @property
    def retryable(self) -> bool:
        if self.hard_stop:
            return False
        return self.status == 0 or self.status in {408, 425, 429, 500, 502, 503, 504}

    @property
    def systemic(self) -> bool:
        return self.hard_stop or self.status in {0, 401, 403, 405, 408, 425, 429, 500, 502, 503, 504}


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 4
    base_delay_seconds: float = 2.0
    max_delay_seconds: float = 120.0
    max_retry_after_seconds: float = 120.0
    request_timeout_ms: int = 20_000

    def delay_for(
        self,
        attempt: int,
        retry_after: float | None = None,
        jitter: float = 0.0,
    ) -> float:
        exponential = min(
            self.max_delay_seconds,
            self.base_delay_seconds * (2 ** max(0, attempt - 1)),
        )
        if retry_after is not None:
            return min(self.max_retry_after_seconds, max(exponential, retry_after))
        return min(self.max_delay_seconds, exponential + max(0.0, jitter))


class RatePacer:
    def __init__(
        self,
        min_interval_seconds: float = 5.0,
        max_interval_seconds: float = 9.0,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
        rng: random.Random | None = None,
    ):
        if min_interval_seconds < 0 or max_interval_seconds < min_interval_seconds:
            raise ValueError("invalid pacing interval")
        self.min_interval_seconds = min_interval_seconds
        self.max_interval_seconds = max_interval_seconds
        self.sleep = sleep
        self.clock = clock
        self.rng = rng or random.SystemRandom()
        self.last_completed_at: float | None = None
        self.total_wait_seconds = 0.0

    def wait(self, max_wait_seconds: float | None = None) -> float:
        now = self.clock()
        if self.last_completed_at is not None:
            interval = self.rng.uniform(self.min_interval_seconds, self.max_interval_seconds)
            remaining = self.last_completed_at + interval - now
            if remaining > 0:
                if max_wait_seconds is not None and remaining > max_wait_seconds:
                    raise BudgetExceeded("pacing wait exceeds remaining runtime budget")
                self.sleep(remaining)
                self.total_wait_seconds += remaining
                return remaining
        return 0.0

    def mark_response_complete(self) -> None:
        self.last_completed_at = self.clock()


class CircuitBreaker:
    def __init__(self, threshold: int = 3):
        if threshold < 1:
            raise ValueError("threshold must be positive")
        self.threshold = threshold
        self.consecutive_systemic_failures = 0
        self.opened = False

    def record_success(self) -> None:
        self.consecutive_systemic_failures = 0

    def record_failure(self, systemic: bool) -> None:
        if not systemic:
            self.consecutive_systemic_failures = 0
            return
        self.consecutive_systemic_failures += 1
        if self.consecutive_systemic_failures >= self.threshold:
            self.opened = True
            raise CircuitOpen(
                f"systemic failure threshold reached ({self.consecutive_systemic_failures})"
            )


@dataclass
class FetchStats:
    endpoint_calls: int = 0
    source_requests: int = 0
    retries: int = 0


@dataclass(frozen=True)
class RunBudget:
    max_tickers: int = 3
    max_requests: int = 3
    max_windows: int = 100
    max_runtime_seconds: float = 900.0

    def validate(self) -> None:
        if self.max_tickers < 1 or self.max_requests < 1 or self.max_windows < 1:
            raise ValueError("run budgets must be positive")
        if self.max_runtime_seconds <= 0:
            raise ValueError("max_runtime_seconds must be positive")


class BudgetTracker:
    def __init__(
        self,
        budget: RunBudget,
        clock: Callable[[], float] = time.monotonic,
    ):
        budget.validate()
        self.budget = budget
        self.clock = clock
        self.started_at = clock()
        self.tickers = 0
        self.requests = 0
        self.windows = 0

    @property
    def elapsed_seconds(self) -> float:
        return max(0.0, self.clock() - self.started_at)

    @property
    def remaining_seconds(self) -> float:
        return max(0.0, self.budget.max_runtime_seconds - self.elapsed_seconds)

    def reserve_ticker(self) -> None:
        if self.tickers >= self.budget.max_tickers:
            raise BudgetExceeded("ticker budget exhausted")
        if self.requests >= self.budget.max_requests or self.remaining_seconds <= 0:
            raise BudgetExceeded("request or runtime budget exhausted before ticker")
        self.tickers += 1

    def ensure_request_capacity(self, window_count: int, required_timeout: float = 0.0) -> None:
        if window_count < 1:
            raise ValueError("window_count must be positive")
        if self.requests + 1 > self.budget.max_requests:
            raise BudgetExceeded("endpoint request budget exhausted")
        if self.windows + window_count > self.budget.max_windows:
            raise BudgetExceeded("source window budget exhausted")
        if self.remaining_seconds <= max(0.0, required_timeout):
            raise BudgetExceeded("runtime budget cannot accommodate request")

    def reserve_request(self, window_count: int, required_timeout: float = 0.0) -> None:
        self.ensure_request_capacity(window_count, required_timeout)
        self.requests += 1
        self.windows += window_count

    def check_deadline(self) -> None:
        if self.remaining_seconds <= 0:
            raise BudgetExceeded("runtime budget exhausted")

    def can_wait(self, seconds: float) -> bool:
        return seconds >= 0 and seconds <= self.remaining_seconds


@dataclass
class RunResult:
    requested: int
    attempted: int = 0
    succeeded: int = 0
    skipped: int = 0
    failures: list[dict[str, str]] = field(default_factory=list)
    systemic_failure: bool = False
    circuit_opened: bool = False
    budget_paused: bool = False
    waf_challenge: bool = False
    complete: bool = False
    stats: FetchStats = field(default_factory=FetchStats)
    manifest_path: Path | None = None
    manifest_status: str | None = None
    budget: RunBudget | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "requested": self.requested,
            "attempted": self.attempted,
            "succeeded": self.succeeded,
            "skipped": self.skipped,
            "failures": self.failures,
            "systemic_failure": self.systemic_failure,
            "circuit_opened": self.circuit_opened,
            "budget_paused": self.budget_paused,
            "waf_challenge": self.waf_challenge,
            "complete": self.complete,
            "endpoint_calls": self.stats.endpoint_calls,
            "source_requests": self.stats.source_requests,
            "retries": self.stats.retries,
            "manifest_path": str(self.manifest_path) if self.manifest_path else None,
            "manifest_status": self.manifest_status,
            "budget": self.budget.__dict__ if self.budget else None,
        }


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _as_utc_iso(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def parse_retry_after(value: Any, now: datetime | None = None) -> float | None:
    if value is None or value == "":
        return None
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        try:
            parsed = parsedate_to_datetime(str(value))
        except (TypeError, ValueError, OverflowError):
            return None
        if parsed is None:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        current = now or _utc_now()
        return max(0.0, (parsed - current).total_seconds())
    if not math.isfinite(seconds) or seconds < 0:
        return None
    return seconds


def _objective_for(item: dict[str, Any]) -> str:
    leverage = item["leverage_target"]
    cadence = item["reset_cadence"]
    underlying = item["underlying_name"]
    ticker = item["underlying_ticker"]
    if item["category"] == "long_only":
        return f"Provide unleveraged long exposure to {underlying}."
    if leverage < 0:
        return f"Seek {leverage}x of the daily performance of {ticker} before fees and expenses."
    if cadence == "quarterly":
        return f"Seek 2x of the quarterly performance of {ticker} before fees and expenses."
    if cadence == "monthly":
        return f"Seek 2x of the monthly performance of {ticker} before fees and expenses."
    return f"Seek 2x of the daily performance of {ticker} before fees and expenses."


def _validate_catalog_row(item: dict[str, Any], context: str) -> None:
    required = {
        "ticker",
        "fund_name",
        "underlying_ticker",
        "underlying_name",
        "category",
        "subgroup",
        "issuer",
        "leverage_target",
        "direction",
        "reset_cadence",
        "risk_tier",
        "featured",
        "canonical",
        "alternatives",
        "trackinsight_key",
    }
    missing = sorted(required - set(item))
    if missing:
        raise CatalogError(f"{context} missing fields: {', '.join(missing)}")
    item["ticker"] = str(item["ticker"]).strip().upper()
    item["underlying_ticker"] = str(item["underlying_ticker"]).strip().upper()
    item["trackinsight_key"] = str(item["trackinsight_key"]).strip()
    item["alternatives"] = [str(value).strip().upper() for value in item["alternatives"]]
    if not item["ticker"] or not item["trackinsight_key"]:
        raise CatalogError(f"{context} has an empty ticker or Trackinsight key")
    if not isinstance(item["leverage_target"], (int, float)):
        raise CatalogError(f"{context} leverage_target must be numeric")
    if not isinstance(item["featured"], bool) or not isinstance(item["canonical"], bool):
        raise CatalogError(f"{context} featured and canonical must be booleans")
    if item["direction"] not in {"long", "inverse"}:
        raise CatalogError(f"{context} has invalid direction")
    if item["reset_cadence"] not in {"daily", "weekly", "monthly", "quarterly"}:
        raise CatalogError(f"{context} has invalid reset_cadence")


def load_catalog(path: str | Path = CATALOG_PATH) -> dict[str, Any]:
    catalog_path = Path(path)
    try:
        payload = yaml.safe_load(catalog_path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise CatalogError(f"could not load catalog {catalog_path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise CatalogError("catalog root must be a mapping")
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise CatalogError(f"catalog schema_version must be {SCHEMA_VERSION}")
    source = payload.get("source")
    if not isinstance(source, dict) or source.get("name") != "Trackinsight":
        raise CatalogError("catalog source must be Trackinsight")
    if source.get("endpoint") != ENDPOINT:
        raise CatalogError("catalog endpoint does not match the ingestion endpoint")
    if source.get("flow_currency") != SOURCE_CURRENCY or source.get("nav_currency") != SOURCE_CURRENCY:
        raise CatalogError("catalog USD flow/NAV metadata is required")
    instruments = payload.get("instruments")
    watch_tier = payload.get("watch_tier")
    if not isinstance(instruments, list) or not isinstance(watch_tier, list):
        raise CatalogError("catalog instruments and watch_tier must be lists")
    expected_count = payload.get("expected_primary_count")
    if expected_count != len(instruments):
        raise CatalogError(
            f"expected_primary_count={expected_count} but instruments has {len(instruments)} rows"
        )
    seen: set[str] = set()
    for item in instruments:
        if not isinstance(item, dict):
            raise CatalogError("every primary catalog row must be a mapping")
        _validate_catalog_row(item, "primary catalog")
        ticker = item["ticker"]
        if ticker in seen:
            raise CatalogError(f"duplicate primary ticker {ticker}")
        seen.add(ticker)
    watch_seen: set[str] = set()
    for item in watch_tier:
        if not isinstance(item, dict):
            raise CatalogError("every watch-tier row must be a mapping")
        _validate_catalog_row(item, "watch-tier catalog")
        ticker = item["ticker"]
        if ticker in watch_seen or ticker in seen:
            raise CatalogError(f"watch-tier ticker {ticker} is duplicated")
        watch_seen.add(ticker)
        if abs(float(item["leverage_target"])) < 2:
            raise CatalogError(f"watch-tier underlying {ticker} lacks a leveraged fund")
    expected_counts = payload.get("expected_counts")
    if not isinstance(expected_counts, dict):
        raise CatalogError("expected_counts is required")
    actual_counts: dict[str, int] = {}
    for item in instruments:
        actual_counts[item["category"]] = actual_counts.get(item["category"], 0) + 1
    for category, expected in expected_counts.items():
        if category == "watch_tier":
            actual = len(watch_tier)
        else:
            actual = actual_counts.get(category, 0)
        if actual != expected:
            raise CatalogError(f"{category} count is {actual}, expected {expected}")
    if sum(bool(item["featured"]) for item in instruments) != 24:
        raise CatalogError("catalog must define exactly 24 featured primary instruments")
    if not all(item["canonical"] for item in instruments):
        raise CatalogError("every primary row must be canonical")
    for item in instruments:
        collisions = sorted(set(item["alternatives"]) & seen)
        if collisions:
            raise CatalogError(
                f"{item['ticker']} alternatives duplicate primary rows: {', '.join(collisions)}"
            )
    history = payload.get("history")
    if not isinstance(history, dict):
        raise CatalogError("catalog history policy is required")
    try:
        datetime.strptime(history["earliest_start"], "%Y-%m-%d")
        overlap = int(history["scheduled_overlap_days"])
        stale_after = int(history["stale_after_days"])
        smoke_lookback = int(history["smoke_lookback_days"])
        window_months = int(history["request_window_months"])
        windows_per_request = int(history.get("windows_per_request", DEFAULT_WINDOWS_PER_REQUEST))
    except (KeyError, TypeError, ValueError) as exc:
        raise CatalogError("catalog history policy is invalid") from exc
    if (
        overlap < 0
        or stale_after < 0
        or smoke_lookback < 1
        or window_months < 1
        or window_months > 12
        or not MIN_WINDOWS_PER_REQUEST <= windows_per_request <= MAX_WINDOWS_PER_REQUEST
    ):
        raise CatalogError("catalog history policy is out of range")
    history["windows_per_request"] = windows_per_request
    source_name = source["name"]
    for item in [*instruments, *watch_tier]:
        item["source"] = source_name
        item["flow_currency"] = source["flow_currency"]
        item["nav_currency"] = source["nav_currency"]
        item["catalog_version"] = payload["catalog_version"]
        item["objective"] = _objective_for(item)
        item["availability"] = "unverified"
        item["data_status"] = "unverified"
    return payload


def _normalise_identity_text(value: Any) -> str:
    text = str(value or "").casefold().replace("&", " and ")
    return "".join(character for character in text if character.isalnum())


def validate_catalog_keys(
    catalog: dict[str, Any],
    index_path: str | Path = LOCAL_INDEX_PATH,
    catalog_path: str | Path = CATALOG_PATH,
) -> dict[str, Any]:
    try:
        index_payload = json.loads(Path(index_path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CatalogError(f"could not read local identity index: {exc}") from exc
    rows = index_payload.get("rows") if isinstance(index_payload, dict) else None
    if not isinstance(rows, list):
        raise CatalogError("local identity index has no rows list")
    by_ticker: dict[str, list[list[Any]]] = {}
    for row in rows:
        if isinstance(row, list) and row and row[0] is not None:
            by_ticker.setdefault(str(row[0]).upper(), []).append(row)
    try:
        canonical_payload = yaml.safe_load(Path(catalog_path).read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise CatalogError(f"could not read canonical catalog: {exc}") from exc
    canonical_items = {
        item["ticker"]: item
        for item in [
            *(canonical_payload.get("instruments", []) if isinstance(canonical_payload, dict) else []),
            *(canonical_payload.get("watch_tier", []) if isinstance(canonical_payload, dict) else []),
        ]
    }
    issues = []
    checked = 0
    identity = catalog.get("identity", {})
    provider_aliases = identity.get("provider_aliases", {})
    name_aliases = identity.get("name_aliases", {})
    for item in [*catalog_items(catalog), *catalog["watch_tier"]]:
        checked += 1
        ticker = item["ticker"]
        canonical = canonical_items.get(ticker)
        if canonical is None:
            issues.append(f"{ticker}: not present in canonical catalog file")
            continue
        for field in ["trackinsight_key", "fund_name", "issuer"]:
            if item.get(field) != canonical.get(field):
                issues.append(f"{ticker}: canonical {field} constraint mismatch")
        if item.get("flow_currency") != SOURCE_CURRENCY or item.get("nav_currency") != SOURCE_CURRENCY:
            issues.append(f"{ticker}: catalog currency is not USD")
        candidates = by_ticker.get(ticker, [])
        exact = [row for row in candidates if str(row[1]) == item["trackinsight_key"]]
        if len(exact) != 1:
            issues.append(f"{ticker}: key {item['trackinsight_key']} has {len(exact)} exact index matches")
            continue
        row = exact[0]
        if str(row[0]).upper() != ticker:
            issues.append(f"{ticker}: index display ticker mismatch")
        if item.get("flow_currency") != SOURCE_CURRENCY or item.get("nav_currency") != SOURCE_CURRENCY:
            issues.append(f"{ticker}: catalog currency is not USD")
        if len(row) > 5 and str(row[5]).upper() != SOURCE_CURRENCY:
            issues.append(f"{ticker}: index currency is {row[5]}")
        index_provider = _normalise_identity_text(row[3] if len(row) > 3 else "")
        accepted_providers = {
            _normalise_identity_text(item["issuer"]),
            *[
                _normalise_identity_text(value)
                for value in provider_aliases.get(item["issuer"], [])
            ],
        }
        if index_provider not in accepted_providers:
            issues.append(f"{ticker}: provider constraint does not match index")
        index_name = _normalise_identity_text(row[2] if len(row) > 2 else "")
        accepted_names = {
            _normalise_identity_text(item["fund_name"]),
            *[
                _normalise_identity_text(value)
                for value in name_aliases.get(ticker, [])
            ],
        }
        if index_name not in accepted_names:
            issues.append(f"{ticker}: fund name constraint does not match index")
    report = {
        "checked": checked,
        "primary": len(catalog_items(catalog)),
        "watch_tier": len(catalog["watch_tier"]),
        "unresolved": issues,
    }
    if issues:
        raise CatalogError("catalog identity validation failed: " + "; ".join(issues))
    return report


def catalog_items(catalog: dict[str, Any]) -> list[dict[str, Any]]:
    return list(catalog["instruments"])


def catalog_by_ticker(catalog: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {item["ticker"]: item for item in catalog_items(catalog)}


def resolve_key(ticker: str, catalog: dict[str, Any] | None = None) -> str:
    normalized = ticker.strip().upper()
    active_catalog = catalog or load_catalog()
    item = catalog_by_ticker(active_catalog).get(normalized)
    if item is None:
        raise CatalogError(f"{normalized} is not in the curated primary catalog")
    return item["trackinsight_key"]


def stamp_to_date(value: Any) -> str:
    if isinstance(value, bool):
        raise DataValidationError(f"invalid date stamp {value!r}")
    if isinstance(value, (int, float, str)):
        try:
            numeric = float(value)
            if not math.isfinite(numeric):
                raise ValueError
            return datetime.fromtimestamp(int(numeric) * 86400, tz=timezone.utc).strftime("%Y-%m-%d")
        except (ValueError, OverflowError, OSError):
            pass
    if isinstance(value, str):
        try:
            return datetime.strptime(value[:10], "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError as exc:
            raise DataValidationError(f"invalid date stamp {value!r}") from exc
    raise DataValidationError(f"invalid date stamp {value!r}")


def _coerce_number(value: Any, field_name: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise DataValidationError(f"{field_name} contains a non-numeric value")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise DataValidationError(f"{field_name} contains a non-numeric value") from exc
    if not math.isfinite(number):
        raise DataValidationError(f"{field_name} contains a non-finite value")
    return number


def unpack(field_data: Any, field_name: str) -> list[float | None]:
    if field_data is None:
        return []
    if not isinstance(field_data, dict):
        raise DataValidationError(f"{field_name} must be an object")
    scale = field_data.get("scale", 1)
    if isinstance(scale, bool):
        raise DataValidationError(f"{field_name} scale must be numeric")
    try:
        scale_value = float(scale if scale is not None else 1)
    except (TypeError, ValueError) as exc:
        raise DataValidationError(f"{field_name} scale must be numeric") from exc
    if not math.isfinite(scale_value) or scale_value <= 0:
        raise DataValidationError(f"{field_name} scale must be finite and positive")
    values = field_data.get("data")
    if values is None:
        return []
    if not isinstance(values, list):
        raise DataValidationError(f"{field_name} data must be a list")
    return [
        None if value is None else _coerce_number(value, field_name) / scale_value
        for value in values
    ]


def _regime(z_score: float) -> str:
    if pd.isna(z_score):
        return "UNAVAILABLE"
    if z_score > 1.5:
        return "ACCUMULATION"
    if z_score < -1.5:
        return "DISTRIBUTION"
    return "BALANCED"


def apply_derived_metrics(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    result = df.copy()
    flow = pd.to_numeric(result["usd_flow"], errors="coerce").astype(float)
    filled = flow.fillna(0.0)
    result["cumulative_flow"] = filled.cumsum()
    result["daily_inflow"] = flow.clip(lower=0)
    result["daily_outflow"] = flow.clip(upper=0)
    mean_30d = flow.rolling(30, min_periods=5).mean()
    std_30d = flow.rolling(30, min_periods=5).std()
    z_score = ((flow - mean_30d) / std_30d).where(std_30d > 0)
    result["flow_zscore"] = z_score
    result["flow_5d"] = flow.rolling(5, min_periods=1).sum()
    result["flow_20d"] = flow.rolling(20, min_periods=1).sum()
    result["regime"] = z_score.map(_regime)
    signs = np.sign(filled)
    streaks = signs.groupby((signs != signs.shift()).cumsum()).cumsum()
    momentum_bonus = np.where(result["flow_5d"] > 0, 10.0, np.where(result["flow_5d"] < 0, -10.0, 0.0))
    streak_bonus = np.minimum(streaks.abs() * 2, 20) * np.sign(streaks)
    pressure = z_score * 25 + momentum_bonus + streak_bonus
    result["pressure"] = pressure.clip(-100, 100)
    return result.drop(columns=["mean_30d", "std_30d"], errors="ignore")


def _history_frame(data: list[dict[str, Any]], context: str) -> pd.DataFrame:
    if not isinstance(data, list):
        raise DataValidationError(f"{context} data must be a list")
    if not data:
        return pd.DataFrame(columns=["date", "usd_flow", "nav", "perf_pct"])
    frame = pd.DataFrame(data)
    required = {"date", "usd_flow"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise DataValidationError(f"{context} missing columns: {', '.join(missing)}")
    normalized_dates = []
    for value in frame["date"]:
        try:
            normalized_dates.append(datetime.strptime(str(value), "%Y-%m-%d").date())
        except ValueError as exc:
            raise DataValidationError(f"{context} has invalid date {value!r}") from exc
    frame["date"] = normalized_dates
    for column in ["usd_flow", "nav", "perf_pct", "cumulative_flow", "daily_inflow", "daily_outflow", "flow_zscore", "flow_5d", "flow_20d", "pressure"]:
        if column not in frame.columns:
            frame[column] = np.nan
        original = frame[column]
        converted = pd.to_numeric(original, errors="coerce")
        invalid = original.notna() & converted.isna()
        if invalid.any():
            raise DataValidationError(f"{context}.{column} contains non-numeric values")
        finite = converted.dropna().map(math.isfinite)
        if not finite.all():
            raise DataValidationError(f"{context}.{column} contains non-finite values")
        frame[column] = converted.astype(float)
    if frame["date"].duplicated().any():
        duplicates = frame.loc[frame["date"].duplicated(keep=False), "date"].astype(str).unique()
        raise DataValidationError(f"{context} has duplicate dates: {', '.join(duplicates[:5])}")
    if (frame["nav"].dropna() <= 0).any():
        raise DataValidationError(f"{context} has a non-positive NAV")
    return frame.sort_values("date").reset_index(drop=True)


def _validate_derived_flow_fields(
    data: list[dict[str, Any]],
    frame: pd.DataFrame,
) -> pd.DataFrame:
    missing = []
    for index, record in enumerate(data):
        absent = [field for field in DERIVED_FLOW_FIELDS if field not in record]
        if absent:
            missing.append(f"data[{index}]:{','.join(absent)}")
    if missing:
        raise DataValidationError("derived fields are missing: " + "; ".join(missing[:5]))
    canonical = apply_derived_metrics(frame)
    canonical_records = canonical.to_dict("records")
    for index, (record, expected) in enumerate(zip(data, canonical_records)):
        for field in DERIVED_FLOW_FIELDS:
            actual = record.get(field)
            if field == "regime":
                if actual != expected[field]:
                    raise DataValidationError(f"data[{index}].{field} does not match raw flow")
                continue
            expected_value = expected[field]
            if pd.isna(expected_value):
                if actual is not None and not pd.isna(actual):
                    raise DataValidationError(f"data[{index}].{field} does not match raw flow")
                continue
            if actual is None:
                raise DataValidationError(f"data[{index}].{field} is missing")
            try:
                actual_number = float(actual)
            except (TypeError, ValueError) as exc:
                raise DataValidationError(f"data[{index}].{field} is not numeric") from exc
            tolerance = DERIVED_NUMERIC_TOLERANCES[field]
            if not math.isfinite(actual_number) or abs(actual_number - float(expected_value)) > tolerance + 1e-9:
                raise DataValidationError(f"data[{index}].{field} does not match raw flow")
    return canonical


def _response_chunks(raw_response: Any) -> list[Any]:
    if isinstance(raw_response, list):
        return raw_response
    if isinstance(raw_response, dict):
        for key in ["snapshots", "results", "items"]:
            if isinstance(raw_response.get(key), list):
                return raw_response[key]
        return [raw_response]
    raise DataValidationError("response must be an object or list")


def parse_snapshots(raw_response: Any, ticker: str) -> pd.DataFrame:
    rows: dict[str, dict[str, Any]] = {}
    chunks = _response_chunks(raw_response)
    for item in chunks:
        if not isinstance(item, dict):
            raise DataValidationError(f"{ticker} response contains a non-object snapshot")
        stamp_data = item.get("stamp")
        if not isinstance(stamp_data, dict):
            continue
        stamps = stamp_data.get("data")
        if stamps is None:
            continue
        if not isinstance(stamps, list):
            raise DataValidationError(f"{ticker} stamp data must be a list")
        flows = unpack(item.get("USD:flow"), f"{ticker}.USD:flow")
        navs = unpack(item.get("nav"), f"{ticker}.nav")
        performances = unpack(item.get("perf"), f"{ticker}.perf")
        for index, stamp in enumerate(stamps):
            day = stamp_to_date(stamp)
            row = {
                "date": day,
                "usd_flow": flows[index] if index < len(flows) else None,
                "nav": navs[index] if index < len(navs) else None,
                "perf_pct": performances[index] if index < len(performances) else None,
            }
            if row["nav"] is not None and row["nav"] <= 0:
                raise DataValidationError(f"{ticker} has non-positive NAV on {day}")
            if day in rows:
                comparable = ("usd_flow", "nav", "perf_pct")
                if any(rows[day].get(column) != row.get(column) for column in comparable):
                    raise DataValidationError(f"{ticker} has conflicting duplicate values for {day}")
                continue
            rows[day] = row
    if not rows:
        raise DataValidationError(f"{ticker} response contained no valid snapshots")
    frame = pd.DataFrame(list(rows.values())).sort_values("date").reset_index(drop=True)
    return apply_derived_metrics(frame)


def _finite_or_none(value: Any, digits: int) -> float | None:
    if value is None or pd.isna(value):
        return None
    number = float(value)
    if not math.isfinite(number):
        raise DataValidationError("derived data contains a non-finite value")
    return round(number, digits)


def frame_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    frame = apply_derived_metrics(_history_frame(frame.to_dict("records"), "history"))
    records = []
    for row in frame.to_dict("records"):
        records.append(
            {
                "date": row["date"].strftime("%Y-%m-%d"),
                "usd_flow": _finite_or_none(row["usd_flow"], 2),
                "nav": _finite_or_none(row["nav"], 4),
                "perf_pct": _finite_or_none(row["perf_pct"], 4),
                "cumulative_flow": _finite_or_none(row["cumulative_flow"], 2),
                "daily_inflow": _finite_or_none(row["daily_inflow"], 2),
                "daily_outflow": _finite_or_none(row["daily_outflow"], 2),
                "flow_zscore": _finite_or_none(row["flow_zscore"], 3),
                "flow_5d": _finite_or_none(row["flow_5d"], 2),
                "flow_20d": _finite_or_none(row["flow_20d"], 2),
                "regime": str(row.get("regime", "UNAVAILABLE")),
                "pressure": _finite_or_none(row.get("pressure"), 1),
            }
        )
    return records


def validate_history_regression(old_frame: pd.DataFrame, merged_frame: pd.DataFrame) -> None:
    old = _history_frame(old_frame.to_dict("records"), "old history") if not old_frame.empty else pd.DataFrame()
    merged = _history_frame(merged_frame.to_dict("records"), "merged history")
    if old.empty:
        return
    if len(merged) < len(old):
        raise DataValidationError(f"merged row count regressed from {len(old)} to {len(merged)}")
    if merged["date"].min() > old["date"].min():
        raise DataValidationError(
            f"history start regressed from {old['date'].min()} to {merged['date'].min()}"
        )


def merge_history(old_frame: pd.DataFrame, new_frame: pd.DataFrame) -> pd.DataFrame:
    old = _history_frame(old_frame.to_dict("records"), "old history") if not old_frame.empty else pd.DataFrame()
    new = _history_frame(new_frame.to_dict("records"), "new history") if not new_frame.empty else pd.DataFrame()
    if new.empty:
        raise DataValidationError("new history is empty")
    if old.empty:
        return apply_derived_metrics(new)
    combined = pd.concat([old, new], ignore_index=True)
    combined = combined.drop_duplicates(subset=["date"], keep="last").sort_values("date").reset_index(drop=True)
    validate_history_regression(old, combined)
    return apply_derived_metrics(combined)


def manifest_content_hash(manifest: dict[str, Any]) -> str:
    content = {
        key: value
        for key, value in manifest.items()
        if key != MANIFEST_CONTENT_HASH_FIELD
    }
    serialized = json.dumps(
        content,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(serialized).hexdigest()


def _assert_output_not_authoritative(path: str | Path) -> None:
    target = Path(path).expanduser().resolve()
    raw_root = RAW_FLOW_DIR.resolve()
    try:
        target.relative_to(raw_root)
    except ValueError:
        return
    raise FlowError(f"refusing to write external-flow output inside authoritative local data: {target}")


def atomic_write_json(path: str | Path, payload: dict[str, Any], indent: int | None = None) -> None:
    _assert_output_not_authoritative(path)
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(
        payload,
        indent=indent,
        separators=(",", ":") if indent is None else None,
        ensure_ascii=False,
        allow_nan=False,
    )
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=target.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(serialized)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
    except Exception:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass
        raise


def _public_catalog_row(item: dict[str, Any]) -> dict[str, Any]:
    fields = (
        "ticker",
        "fund_name",
        "underlying_ticker",
        "underlying_name",
        "category",
        "subgroup",
        "issuer",
        "leverage_target",
        "direction",
        "reset_cadence",
        "risk_tier",
        "featured",
        "canonical",
        "alternatives",
        "source",
        "flow_currency",
        "nav_currency",
        "objective",
    )
    return {field: item[field] for field in fields}


def build_ui_catalog(catalog: dict[str, Any]) -> dict[str, Any]:
    instruments = catalog_items(catalog)
    source = catalog["source"]
    return {
        "schema_version": 1,
        "catalog_version": catalog["catalog_version"],
        "source": {
            "name": source["name"],
            "flow_currency": source["flow_currency"],
            "nav_currency": source["nav_currency"],
        },
        "counts": {
            "primary": len(instruments),
            "featured": sum(bool(item["featured"]) for item in instruments),
            "watch_tier": len(catalog["watch_tier"]),
            "by_category": manifest_category_counts(catalog),
        },
        "instruments": [_public_catalog_row(item) for item in instruments],
        "watch_tier": [_public_catalog_row(item) for item in catalog["watch_tier"]],
    }


def export_ui_catalog(
    catalog_path: str | Path = CATALOG_PATH,
    output_path: str | Path = UI_CATALOG_PATH,
) -> Path:
    target = Path(output_path)
    payload = build_ui_catalog(load_catalog(catalog_path))
    atomic_write_json(target, payload, indent=2)
    return target


def save_ticker_json(
    ticker: str,
    key: str,
    df: pd.DataFrame,
    item: dict[str, Any] | None = None,
    out_dir: str | Path = OUT_DIR,
    retrieved_at: datetime | None = None,
    stale_after_days: int | None = None,
) -> tuple[dict[str, Any], Path]:
    normalized = ticker.strip().upper()
    catalog_item = item
    if catalog_item is None:
        catalog_item = catalog_by_ticker(load_catalog())[normalized]
    if catalog_item["ticker"] != normalized:
        raise CatalogError(f"catalog item {catalog_item['ticker']} does not match {normalized}")
    new_frame = _history_frame(df.to_dict("records"), f"fetched {normalized}")
    if new_frame.empty:
        raise DataValidationError(f"fetched history for {normalized} is empty")
    output_dir = Path(out_dir)
    target = output_dir / f"{normalized}.json"
    old_payload: dict[str, Any] | None = None
    if target.exists():
        try:
            old_payload = json.loads(target.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise DataValidationError(f"existing {normalized} payload is unreadable: {exc}") from exc
        if not isinstance(old_payload, dict):
            raise DataValidationError(f"existing {normalized} payload is not an object")
    old_frame = (
        _history_frame(old_payload.get("data", []), f"existing {normalized}")
        if old_payload is not None
        else pd.DataFrame()
    )
    combined = merge_history(old_frame, new_frame)
    records = frame_records(combined)
    retrieved = retrieved_at or _utc_now()
    if retrieved.tzinfo is None:
        retrieved = retrieved.replace(tzinfo=timezone.utc)
    retrieved = retrieved.astimezone(timezone.utc)
    null_flow_records = sum(record["usd_flow"] is None for record in records)
    null_nav_records = sum(record["nav"] is None for record in records)
    source_asof = records[-1]["date"]
    if stale_after_days is None:
        stale_after_days = int(load_catalog()["history"].get("stale_after_days", 7))
    if stale_after_days < 0:
        raise ValueError("stale_after_days must be non-negative")
    source_date = datetime.strptime(source_asof, "%Y-%m-%d").date()
    quality_status = "ok" if (retrieved.date() - source_date).days <= stale_after_days else "stale"
    catalog_version = catalog_item.get("catalog_version")
    if not catalog_version:
        catalog_version = load_catalog()["catalog_version"]
    payload = {
        "schema_version": SCHEMA_VERSION,
        "catalog_version": catalog_version,
        "ticker": normalized,
        "key": key,
        "fund_name": catalog_item["fund_name"],
        "underlying_ticker": catalog_item["underlying_ticker"],
        "underlying_name": catalog_item["underlying_name"],
        "category": catalog_item["category"],
        "subgroup": catalog_item["subgroup"],
        "issuer": catalog_item["issuer"],
        "leverage_target": catalog_item["leverage_target"],
        "direction": catalog_item["direction"],
        "reset_cadence": catalog_item["reset_cadence"],
        "reset_frequency": catalog_item["reset_cadence"],
        "risk_tier": catalog_item["risk_tier"],
        "featured": catalog_item["featured"],
        "canonical": catalog_item["canonical"],
        "alternatives": catalog_item["alternatives"],
        "objective": catalog_item["objective"],
        "source": catalog_item["source"],
        "flow_currency": catalog_item["flow_currency"],
        "nav_currency": catalog_item["nav_currency"],
        "source_asof": source_asof,
        "retrieved_at": _as_utc_iso(retrieved),
        "updated": retrieved.date().isoformat(),
        "availability": "available",
        "status": "available",
        "data_status": quality_status,
        "data_quality": {
            "status": quality_status,
            "validation": "passed",
            "records": len(records),
            "null_flow_records": null_flow_records,
            "null_nav_records": null_nav_records,
            "duplicate_records": 0,
        },
        "count": len(records),
        "data": records,
    }
    atomic_write_json(target, payload)
    old_count = len(old_frame)
    logger.info(
        "Saved %s -> %s (%d total rows, %d source rows)",
        normalized,
        target,
        len(records),
        len(new_frame),
    )
    logger.debug("Merged %s from %d old rows", normalized, old_count)
    return payload, target


def _load_existing_payload(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"_invalid": True}
    if not isinstance(payload, dict):
        return {"_invalid": True}
    return payload


def _retrieval_date(payload: dict[str, Any]) -> date | None:
    value = payload.get("retrieved_at") or payload.get("updated")
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).date()
    except ValueError:
        try:
            return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
        except ValueError:
            return None


def _parse_timestamp(value: Any, field_name: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise DataValidationError(f"{field_name} is missing")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise DataValidationError(f"{field_name} is invalid") from exc
    if parsed.tzinfo is None:
        raise DataValidationError(f"{field_name} must include a timezone")
    return parsed.astimezone(timezone.utc)


def _validate_manifest_timestamp(
    value: Any,
    field_name: str,
    now: datetime | None = None,
) -> datetime:
    parsed = _parse_timestamp(value, field_name)
    current = now or _utc_now()
    if parsed > current:
        raise FlowError(f"{field_name} is in the future")
    if parsed < datetime(2000, 1, 1, tzinfo=timezone.utc):
        raise FlowError(f"{field_name} is implausibly old")
    return parsed


def _reject_nonfinite(value: Any, path: str = "payload") -> None:
    if isinstance(value, float) and not math.isfinite(value):
        raise DataValidationError(f"{path} contains a non-finite value")
    if isinstance(value, dict):
        for key, nested in value.items():
            _reject_nonfinite(nested, f"{path}.{key}")
    elif isinstance(value, list):
        for index, nested in enumerate(value):
            _reject_nonfinite(nested, f"{path}[{index}]")


def _validate_flow_payload(
    item: dict[str, Any],
    payload: dict[str, Any],
    today: date,
    stale_after_days: int,
) -> tuple[pd.DataFrame, date, datetime, dict[str, Any]]:
    _reject_nonfinite(payload)
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise DataValidationError("schema_version is unsupported")
    expected_catalog_version = item.get("catalog_version")
    if (
        not isinstance(expected_catalog_version, str)
        or not expected_catalog_version
        or payload.get("catalog_version") != expected_catalog_version
    ):
        raise DataValidationError("catalog_version does not match current catalog")
    data = payload.get("data")
    if not isinstance(data, list) or not data:
        raise DataValidationError("data must be a non-empty array")
    if payload.get("count") != len(data):
        raise DataValidationError("count does not equal data length")
    raw_dates = []
    for index, record in enumerate(data):
        if not isinstance(record, dict):
            raise DataValidationError(f"data[{index}] is not an object")
        raw_date = record.get("date")
        try:
            raw_dates.append(datetime.strptime(str(raw_date), "%Y-%m-%d").date())
        except (TypeError, ValueError) as exc:
            raise DataValidationError(f"data[{index}].date is invalid") from exc
    if raw_dates != sorted(raw_dates):
        raise DataValidationError("data dates are not sorted")
    if len(set(raw_dates)) != len(raw_dates):
        raise DataValidationError("data dates are not unique")
    frame = _validate_derived_flow_fields(
        data,
        _history_frame(data, f"catalog file {item['ticker']}"),
    )
    actual_max = frame["date"].max()
    if payload.get("ticker") != item["ticker"]:
        raise DataValidationError("ticker does not match catalog")
    if payload.get("key") != item["trackinsight_key"]:
        raise DataValidationError("key does not match catalog")
    metadata_fields = [
        "fund_name",
        "underlying_ticker",
        "underlying_name",
        "category",
        "subgroup",
        "issuer",
        "leverage_target",
        "direction",
        "risk_tier",
        "featured",
        "canonical",
        "alternatives",
        "objective",
        "source",
        "flow_currency",
        "nav_currency",
    ]
    for field in metadata_fields:
        if payload.get(field) != item.get(field):
            raise DataValidationError(f"{field} does not match catalog")
    if payload.get("reset_cadence") != item["reset_cadence"] or payload.get("reset_frequency") != item["reset_cadence"]:
        raise DataValidationError("reset frequency does not match catalog")
    if payload.get("flow_currency") != "USD" or payload.get("nav_currency") != "USD":
        raise DataValidationError("flow/NAV currency must be USD")
    source_asof = payload.get("source_asof")
    try:
        source_date = datetime.strptime(str(source_asof), "%Y-%m-%d").date()
    except (TypeError, ValueError) as exc:
        raise DataValidationError("source_asof is invalid") from exc
    if source_date > today:
        raise DataValidationError("source_asof is in the future")
    if source_date != actual_max:
        raise DataValidationError("source_asof does not equal actual maximum record date")
    retrieved_at = _parse_timestamp(payload.get("retrieved_at"), "retrieved_at")
    if retrieved_at > _utc_now() + timedelta(minutes=5):
        raise DataValidationError("retrieved_at is in the future")
    if retrieved_at.date() < source_date:
        raise DataValidationError("retrieved_at precedes source_asof")
    expected_updated = retrieved_at.date().isoformat()
    if payload.get("updated") != expected_updated:
        raise DataValidationError("updated does not match retrieved_at")
    if payload.get("availability") != "available" or payload.get("status") != "available":
        raise DataValidationError("availability/status is invalid")
    retrieval_age_days = max(0, (retrieved_at.date() - source_date).days)
    expected_status = "ok" if retrieval_age_days <= stale_after_days else "stale"
    expected_quality = {
        "status": expected_status,
        "validation": "passed",
        "records": len(data),
        "null_flow_records": sum(record.get("usd_flow") is None for record in data),
        "null_nav_records": sum(record.get("nav") is None for record in data),
        "duplicate_records": 0,
    }
    if payload.get("data_status") != expected_status:
        raise DataValidationError("data_status does not match freshness")
    if payload.get("data_quality") != expected_quality:
        raise DataValidationError("data_quality does not match validated data")
    return frame, source_date, retrieved_at, expected_quality


def _manifest_entry(
    item: dict[str, Any],
    out_dir: Path,
    today: date,
    stale_after_days: int,
) -> dict[str, Any]:
    ticker = item["ticker"]
    base = {
        "ticker": ticker,
        "key": item["trackinsight_key"],
        "fund_name": item["fund_name"],
        "underlying_ticker": item["underlying_ticker"],
        "underlying_name": item["underlying_name"],
        "category": item["category"],
        "subgroup": item["subgroup"],
        "issuer": item["issuer"],
        "leverage_target": item["leverage_target"],
        "direction": item["direction"],
        "reset_cadence": item["reset_cadence"],
        "reset_frequency": item["reset_cadence"],
        "risk_tier": item["risk_tier"],
        "featured": item["featured"],
        "canonical": item["canonical"],
        "alternatives": item["alternatives"],
        "objective": item["objective"],
        "source": item["source"],
        "flow_currency": item["flow_currency"],
        "nav_currency": item["nav_currency"],
    }
    payload = _load_existing_payload(out_dir / f"{ticker}.json")
    if payload is None:
        return {
            **base,
            "asof": None,
            "source_asof": None,
            "retrieved_at": None,
            "start_date": None,
            "records": 0,
            "nav": None,
            "flow_30d": None,
            "flow_zscore": None,
            "regime": None,
            "pressure": None,
            "fresh": False,
            "freshness_days": None,
            "availability": "missing",
            "data_status": "missing",
            "data_quality": {"status": "missing", "validation": "not_run"},
        }
    retrieved_at = payload.get("retrieved_at") if not payload.get("_invalid") else None
    if payload.get("_invalid"):
        return {
            **base,
            "asof": None,
            "source_asof": None,
            "retrieved_at": None,
            "start_date": None,
            "records": 0,
            "nav": None,
            "flow_30d": None,
            "flow_zscore": None,
            "regime": None,
            "pressure": None,
            "fresh": False,
            "freshness_days": None,
            "availability": "invalid",
            "data_status": "invalid",
            "data_quality": {"status": "invalid", "validation": "failed"},
        }
    try:
        frame, source_date, parsed_retrieved, quality = _validate_flow_payload(
            item,
            payload,
            today,
            stale_after_days,
        )
    except DataValidationError as exc:
        return {
            **base,
            "asof": None,
            "source_asof": None,
            "retrieved_at": retrieved_at,
            "start_date": None,
            "records": 0,
            "nav": None,
            "flow_30d": None,
            "flow_zscore": None,
            "regime": None,
            "pressure": None,
            "fresh": False,
            "freshness_days": None,
            "availability": "invalid",
            "data_status": "invalid",
            "data_quality": {"status": "invalid", "validation": "failed", "error": str(exc)},
        }
    records = frame.to_dict("records")
    latest = records[-1]
    flows = [
        float(record["usd_flow"])
        for record in records[-30:]
        if record.get("usd_flow") is not None and pd.notna(record.get("usd_flow"))
    ]
    freshness_days = max(0, (today - source_date).days)
    fresh = freshness_days <= stale_after_days
    return {
        **base,
        "asof": source_date.isoformat(),
        "source_asof": source_date.isoformat(),
        "retrieved_at": _as_utc_iso(parsed_retrieved),
        "start_date": records[0]["date"].strftime("%Y-%m-%d"),
        "records": len(records),
        "nav": float(latest["nav"]) if pd.notna(latest.get("nav")) else None,
        "flow_30d": round(sum(flows), 2),
        "flow_zscore": None if pd.isna(latest.get("flow_zscore")) else float(latest.get("flow_zscore")),
        "regime": self_regime(latest),
        "pressure": None if pd.isna(latest.get("pressure")) else float(latest.get("pressure")),
        "fresh": fresh,
        "freshness_days": freshness_days,
        "availability": "available",
        "data_status": quality["status"],
        "data_quality": quality,
    }


def self_regime(record: dict[str, Any]) -> str:
    if "regime" in record and record["regime"]:
        return str(record["regime"])
    return _regime(record.get("usd_flow"))


def compute_manifest(
    catalog: dict[str, Any] | None = None,
    out_dir: str | Path = OUT_DIR,
    today: date | None = None,
    stale_after_days: int = 7,
    generated_at: datetime | None = None,
    budget: BudgetTracker | None = None,
) -> dict[str, Any]:
    active_catalog = catalog or load_catalog()
    output_dir = Path(out_dir)
    current_date = today or date.today()
    generated = generated_at or _utc_now()
    if budget is not None:
        budget.check_deadline()
    entries = {}
    for item in catalog_items(active_catalog):
        if budget is not None:
            budget.check_deadline()
        entries[item["ticker"]] = _manifest_entry(
            item,
            output_dir,
            current_date,
            stale_after_days,
        )
    if budget is not None:
        budget.check_deadline()
    available = [entry for entry in entries.values() if entry["availability"] == "available"]
    valid_retrievals = [
        _parse_timestamp(entry["retrieved_at"], f"{entry['ticker']}.retrieved_at")
        for entry in available
        if entry.get("retrieved_at")
    ]
    if valid_retrievals:
        generated = max(valid_retrievals)
    available_count = len(available)
    total_count = len(entries)
    status = "ok" if available_count == total_count else "partial" if available_count else "empty"
    source_dates = [entry["source_asof"] for entry in available if entry["source_asof"]]
    category_counts: dict[str, int] = {}
    for entry in entries.values():
        category_counts[entry["category"]] = category_counts.get(entry["category"], 0) + 1
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "catalog_version": active_catalog["catalog_version"],
        "generated_utc": _as_utc_iso(generated),
        "source": "Trackinsight",
        "source_endpoint": ENDPOINT,
        "source_asof": max(source_dates) if source_dates else None,
        "retrieved_at": _as_utc_iso(generated),
        "status": status,
        "complete": status == "ok",
        "required_etfs": total_count,
        "unavailable_allowlist": [],
        "fresh": status == "ok" and all(entry["fresh"] for entry in entries.values()),
        "total_etfs": total_count,
        "available_etfs": available_count,
        "missing_etfs": total_count - available_count,
        "categories": category_counts,
        "featured_count": sum(bool(entry["featured"]) for entry in entries.values()),
        "canonical_count": sum(bool(entry["canonical"]) for entry in entries.values()),
        "source_coverage": {
            "expected": total_count,
            "available": available_count,
            "ratio": round(available_count / total_count, 6) if total_count else 0.0,
            "start_date": min(
                (entry["start_date"] for entry in available if entry["start_date"]),
                default=None,
            ),
            "end_date": max(source_dates) if source_dates else None,
        },
        "etfs": entries,
    }
    manifest[MANIFEST_CONTENT_HASH_FIELD] = manifest_content_hash(manifest)
    if budget is not None:
        budget.check_deadline()
    return manifest


def build_manifest(
    catalog: dict[str, Any] | None = None,
    out_dir: str | Path = OUT_DIR,
    today: date | None = None,
    stale_after_days: int = 7,
    generated_at: datetime | None = None,
    budget: BudgetTracker | None = None,
) -> dict[str, Any]:
    output_dir = Path(out_dir)
    manifest = compute_manifest(
        catalog,
        output_dir,
        today,
        stale_after_days,
        generated_at,
        budget,
    )
    if budget is not None:
        budget.check_deadline()
    target = output_dir / "curated_manifest.json"
    atomic_write_json(target, manifest, indent=2)
    if budget is not None:
        budget.check_deadline()
    logger.info(
        "Saved curated manifest -> %s (%d/%d available)",
        target,
        manifest["available_etfs"],
        manifest["total_etfs"],
    )
    return manifest


def validate_manifest(
    path: str | Path = MANIFEST_PATH,
    min_available: int = 118,
    require_complete: bool = True,
    unavailable_allowlist: list[str] | None = None,
    catalog: dict[str, Any] | None = None,
    today: date | None = None,
    stale_after_days: int = 7,
    require_fresh: bool = True,
) -> dict[str, Any]:
    manifest_path = Path(path)
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise FlowError(f"manifest is unreadable: {exc}") from exc
    if manifest.get("schema_version") != SCHEMA_VERSION:
        raise FlowError("manifest schema_version is invalid")
    active_catalog = catalog or load_catalog()
    now = _utc_now()
    current_date = today or date.today()
    generated = _validate_manifest_timestamp(
        manifest.get("generated_utc"),
        "manifest.generated_utc",
        now,
    )
    manifest_retrieved = _validate_manifest_timestamp(
        manifest.get("retrieved_at"),
        "manifest.retrieved_at",
        now,
    )
    if manifest_retrieved > generated:
        raise FlowError("manifest retrieved_at is after generated_utc")
    actual = compute_manifest(
        active_catalog,
        manifest_path.parent,
        today=current_date,
        stale_after_days=stale_after_days,
        generated_at=generated,
    )
    errors = []
    claimed_hash = manifest.get(MANIFEST_CONTENT_HASH_FIELD)
    actual_hash = actual.get(MANIFEST_CONTENT_HASH_FIELD)
    if not isinstance(claimed_hash, str) or claimed_hash != actual_hash:
        errors.append(MANIFEST_CONTENT_HASH_FIELD)
    actual_top_fields = set(actual) - {"etfs"}
    claimed_top_fields = set(manifest) - {"etfs"}
    if claimed_top_fields != actual_top_fields:
        errors.append("manifest_fields")
    top_fields = sorted(actual_top_fields | claimed_top_fields)
    for field in top_fields:
        if manifest.get(field) != actual.get(field):
            errors.append(field)
    claimed_entries = manifest.get("etfs")
    actual_entries = actual.get("etfs", {})
    if not isinstance(claimed_entries, dict) or set(claimed_entries) != set(actual_entries):
        errors.append("etfs_membership")
    else:
        for ticker, actual_entry in actual_entries.items():
            claimed_entry = claimed_entries[ticker]
            if not isinstance(claimed_entry, dict):
                errors.append(f"{ticker}.entry")
                continue
            if set(claimed_entry) != set(actual_entry):
                errors.append(f"{ticker}.fields")
            if claimed_entry.get("ticker") != ticker or actual_entry.get("ticker") != ticker:
                errors.append(f"{ticker}.ticker")
            entry_fields = sorted(set(actual_entry) | set(claimed_entry))
            for field in entry_fields:
                if claimed_entry.get(field) != actual_entry.get(field):
                    errors.append(f"{ticker}.{field}")
    for entry in actual_entries.values():
        if entry.get("availability") != "available":
            continue
        entry_retrieved = _parse_timestamp(entry.get("retrieved_at"), f"{entry['ticker']}.retrieved_at")
        if generated < entry_retrieved:
            errors.append(f"{entry['ticker']}.retrieved_at")
        source_asof = entry.get("source_asof")
        if source_asof and generated.date() < date.fromisoformat(source_asof):
            errors.append(f"{entry['ticker']}.source_asof")
    if manifest.get("fresh") is True:
        for entry in actual_entries.values():
            if entry.get("availability") != "available" or not entry.get("source_asof"):
                continue
            age_days = (current_date - date.fromisoformat(entry["source_asof"])).days
            if age_days > stale_after_days:
                errors.append("fresh")
                break
    if errors:
        raise FlowError("manifest failed recomputation: " + ", ".join(errors[:12]))
    if len(actual_entries) != 118:
        raise FlowError("recomputed catalog must contain exactly 118 primary rows")
    allowlist = set(unavailable_allowlist or manifest.get("unavailable_allowlist", []))
    if not allowlist.issubset(set(actual_entries)):
        raise FlowError("unavailable allowlist contains an unknown ticker")
    available = int(manifest.get("available_etfs", 0))
    if available < min_available:
        raise FlowError(
            f"manifest is incomplete: availability {available} is below {min_available}"
        )
    if require_complete and available + len(allowlist) < len(actual_entries):
        raise FlowError(
            f"manifest is incomplete: {available}/{len(actual_entries)} available"
        )
    if require_complete and manifest.get("status") != "ok":
        raise FlowError(f"manifest status is {manifest.get('status')}, not complete")
    if require_complete and require_fresh and not manifest.get("fresh"):
        raise FlowError("manifest contains stale or unavailable source data")
    return manifest


def select_targets(
    catalog: dict[str, Any],
    tickers: list[str] | None,
    mode: str = "scheduled",
) -> list[str]:
    if not tickers:
        return [item["ticker"] for item in catalog_items(catalog)]
    normalized: list[str] = []
    for raw in tickers:
        for value in str(raw).split(","):
            ticker = value.strip().upper()
            if ticker and ticker not in normalized:
                normalized.append(ticker)
    known = set(catalog_by_ticker(catalog))
    unknown = sorted(set(normalized) - known)
    if unknown:
        raise CatalogError(f"tickers outside the primary catalog: {', '.join(unknown)}")
    if mode == "smoke" and len(normalized) > 3:
        raise CatalogError("smoke mode is limited to 3 primary tickers")
    return normalized


def build_request_windows(
    start_date: str,
    end_date: str,
    key: str,
    window_months: int = DEFAULT_WINDOW_MONTHS,
) -> list[dict[str, Any]]:
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    if start > end:
        raise ValueError("start_date must not be after end_date")
    requests = []
    current = start
    while current <= end:
        window_end = min(current + relativedelta(months=window_months) - relativedelta(days=1), end)
        requests.append(
            {
                "fund": key,
                "startDate": current.strftime("%Y-%m-%d"),
                "endDate": window_end.strftime("%Y-%m-%d"),
                "columns": ["stamp", "USD:flow", "nav", "perf"],
            }
        )
        current += relativedelta(months=window_months)
    return requests


def chunk_request_windows(
    requests: list[dict[str, Any]],
    windows_per_request: int = DEFAULT_WINDOWS_PER_REQUEST,
) -> list[list[dict[str, Any]]]:
    if not MIN_WINDOWS_PER_REQUEST <= windows_per_request <= MAX_WINDOWS_PER_REQUEST:
        raise ValueError("windows_per_request must be between 1 and 4")
    if not requests:
        return []
    return [
        requests[index:index + windows_per_request]
        for index in range(0, len(requests), windows_per_request)
    ]


def apply_plan_budget(
    plans: list[dict[str, Any]],
    budget: RunBudget,
) -> list[dict[str, Any]]:
    budget.validate()
    selected = []
    active_tickers = 0
    used_requests = 0
    used_windows = 0
    for plan in plans:
        if plan["skip"]:
            selected.append(plan)
            continue
        if active_tickers >= budget.max_tickers:
            break
        plan_windows = int(plan.get("source_requests", 0))
        batch_windows = int(plan.get("windows_per_request", DEFAULT_WINDOWS_PER_REQUEST))
        if plan_windows < 1 or batch_windows < 1:
            continue
        available_requests = budget.max_requests - used_requests
        available_windows = budget.max_windows - used_windows
        if available_requests < 1 or available_windows < batch_windows:
            break
        planned_requests = min(
            (plan_windows + batch_windows - 1) // batch_windows,
            available_requests,
            available_windows // batch_windows,
        )
        if planned_requests < 1:
            break
        planned_windows = min(plan_windows, planned_requests * batch_windows)
        budgeted_plan = dict(plan)
        budgeted_plan["budget_source_requests"] = planned_windows
        budgeted_plan["budget_endpoint_requests"] = planned_requests
        budgeted_plan["budget_truncated"] = planned_windows < plan_windows
        selected.append(budgeted_plan)
        active_tickers += 1
        used_requests += planned_requests
        used_windows += planned_windows
    return selected


def build_fetch_plan(
    catalog: dict[str, Any],
    tickers: list[str] | None = None,
    mode: str = "scheduled",
    force: bool = False,
    out_dir: str | Path = OUT_DIR,
    today: date | None = None,
    refresh_history: bool = False,
    budget: RunBudget | None = None,
    include_stale: bool = False,
    windows_per_request: int = DEFAULT_WINDOWS_PER_REQUEST,
) -> list[dict[str, Any]]:
    if mode not in {"scheduled", "backfill", "smoke", "resume"}:
        raise ValueError(f"unsupported mode {mode}")
    if not MIN_WINDOWS_PER_REQUEST <= windows_per_request <= MAX_WINDOWS_PER_REQUEST:
        raise ValueError("windows_per_request must be between 1 and 4")
    targets = select_targets(catalog, tickers, mode)
    known = catalog_by_ticker(catalog)
    current_date = today or date.today()
    earliest = datetime.strptime(catalog["history"]["earliest_start"], "%Y-%m-%d").date()
    overlap_days = int(catalog["history"]["scheduled_overlap_days"])
    stale_after_days = int(catalog["history"].get("stale_after_days", overlap_days))
    smoke_lookback_days = int(catalog["history"]["smoke_lookback_days"])
    window_months = int(catalog["history"]["request_window_months"])
    output_dir = Path(out_dir)
    history_refresh = refresh_history or force
    plans = []
    for ticker in targets:
        item = known[ticker]
        path = output_dir / f"{ticker}.json"
        payload = _load_existing_payload(path)
        cached_records = 0
        cached_source_asof = None
        valid_cached = False
        source_stale = False
        resume_cached = False
        skip = False
        checkpoint_state = "missing"
        if payload is not None and payload.get("_invalid"):
            checkpoint_state = "invalid"
        elif payload is not None:
            try:
                _frame, source_date, _retrieved, _quality = _validate_flow_payload(
                    item,
                    payload,
                    current_date,
                    stale_after_days,
                )
                valid_cached = True
                data = payload.get("data", [])
                cached_records = len(data)
                cached_source_asof = source_date.isoformat()
                source_stale = (current_date - source_date).days > stale_after_days
                checkpoint_state = "stale" if source_stale else "fresh"
            except DataValidationError:
                valid_cached = False
                cached_records = 0
                cached_source_asof = None
                checkpoint_state = "invalid"
            retrieval_day = _retrieval_date(payload)
            if valid_cached:
                same_day_current = retrieval_day == current_date and not source_stale
                resume_cached = bool(
                    mode == "resume"
                    and not history_refresh
                    and not source_stale
                )
                skip = bool(
                    (mode in {"scheduled", "backfill", "resume"} and not history_refresh)
                    and same_day_current
                ) or resume_cached
        if mode == "smoke":
            start_date = max(earliest, current_date - timedelta(days=smoke_lookback_days))
        elif history_refresh or not cached_source_asof:
            start_date = earliest
        else:
            last_date = datetime.strptime(str(cached_source_asof), "%Y-%m-%d").date()
            start_date = max(earliest, last_date - timedelta(days=overlap_days))
        requests = [] if skip else build_request_windows(
            start_date.isoformat(),
            current_date.isoformat(),
            item["trackinsight_key"],
            window_months,
        )
        plans.append(
            {
                "ticker": ticker,
                "key": item["trackinsight_key"],
                "category": item["category"],
                "subgroup": item["subgroup"],
                "mode": mode,
                "start_date": start_date.isoformat(),
                "end_date": current_date.isoformat(),
                "cached_records": cached_records,
                "cached_source_asof": cached_source_asof,
                "source_stale": source_stale,
                "checkpoint_state": checkpoint_state,
                "resolution": "explicit_catalog_key",
                "skip": skip,
                "resume_cached": resume_cached,
                "refresh_history": history_refresh,
                "windows_per_request": windows_per_request,
                "source_requests": len(requests),
                "endpoint_requests": len(chunk_request_windows(requests, windows_per_request)),
            }
        )
    return apply_plan_budget(plans, budget) if budget else plans


def build_checkpoint_plan(
    catalog: dict[str, Any],
    out_dir: str | Path,
    tickers: list[str] | None = None,
    today: date | None = None,
    mode: str = "resume",
    budget: RunBudget | None = None,
    generated_at: datetime | None = None,
    windows_per_request: int = DEFAULT_WINDOWS_PER_REQUEST,
) -> dict[str, Any]:
    active_catalog = catalog or load_catalog()
    active_budget = budget or RunBudget()
    active_budget.validate()
    current_date = today or date.today()
    plans = build_fetch_plan(
        active_catalog,
        tickers=tickers,
        mode=mode,
        out_dir=out_dir,
        today=current_date,
        windows_per_request=windows_per_request,
    )
    selected = apply_plan_budget(plans, active_budget)
    state_counts: dict[str, int] = {}
    for plan in plans:
        state = str(plan["checkpoint_state"])
        state_counts[state] = state_counts.get(state, 0) + 1
    selected_tickers = [plan["ticker"] for plan in selected if not plan["skip"]]
    deferred_tickers = [
        plan["ticker"]
        for plan in plans
        if not plan["skip"] and plan["ticker"] not in selected_tickers
    ]
    return {
        "schema_version": 1,
        "catalog_version": active_catalog["catalog_version"],
        "generated_at": _as_utc_iso(generated_at or _utc_now()),
        "mode": mode,
        "required_tickers": len(plans),
        "state_counts": state_counts,
        "selected_tickers": selected_tickers,
        "selected_count": len(selected_tickers),
        "deferred_tickers": deferred_tickers,
        "deferred_count": len(deferred_tickers),
        "source_requests": sum(
            plan.get("budget_source_requests", plan["source_requests"])
            for plan in selected
            if not plan["skip"]
        ),
        "endpoint_requests": sum(
            plan.get("budget_endpoint_requests", plan.get("endpoint_requests", 0))
            for plan in selected
            if not plan["skip"]
        ),
        "windows_per_request": windows_per_request,
        "plans": selected,
    }


def _redact_checkpoint_text(value: Any, limit: int = 400) -> str:
    text = "".join(character for character in str(value or "") if character.isprintable())
    text = re.sub(
        r"(?i)\b(authorization|password|secret|token|pat|etrade_pat|api[_-]?key|access[_-]?key|cookie)\b\s*[:=]\s*[^\r\n]+",
        r"\1=[redacted]",
        text,
    )
    text = re.sub(r"(?i)\bbearer\s+\S+", "Bearer [redacted]", text)
    return text[:limit]


def _load_optional_json(path: str | Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    candidate = Path(path)
    if not candidate.exists():
        return {}
    try:
        payload = json.loads(candidate.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def waf_cooldown_active(
    out_dir: str | Path,
    now: datetime | None = None,
) -> bool:
    diagnostics = _load_optional_json(Path(out_dir) / "checkpoint_diagnostics.json")
    run_info = diagnostics.get("run")
    challenge_values = [
        diagnostics.get("waf_challenge"),
        run_info.get("waf_challenge") if isinstance(run_info, dict) else None,
    ]
    challenge = any(
        value is True or (isinstance(value, str) and value.lower() == "true")
        for value in challenge_values
    )
    if not challenge:
        return False
    value = diagnostics.get("cooldown_until_utc")
    if not value:
        return True
    current = now or _utc_now()
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    try:
        until = _parse_timestamp(value, "cooldown_until_utc")
    except DataValidationError:
        return True
    return until > current.astimezone(timezone.utc)


def write_checkpoint_diagnostics(
    catalog: dict[str, Any],
    out_dir: str | Path,
    run_result_path: str | Path | None = None,
    run_exit_code: int | None = None,
    github_run_id: str | None = None,
    github_run_attempt: str | None = None,
    today: date | None = None,
    generated_at: datetime | None = None,
) -> Path:
    output_dir = Path(out_dir)
    manifest_path = output_dir / "curated_manifest.json"
    manifest = validate_manifest(
        manifest_path,
        min_available=0,
        require_complete=False,
        catalog=catalog,
        today=today,
        require_fresh=False,
    )
    run_result = _load_optional_json(run_result_path)
    waf_value = run_result.get("waf_challenge")
    waf_challenge = waf_value is True or (
        isinstance(waf_value, str) and waf_value.lower() == "true"
    )
    cooldown_until = None
    if waf_challenge:
        cooldown_base = generated_at or _utc_now()
        if cooldown_base.tzinfo is None:
            cooldown_base = cooldown_base.replace(tzinfo=timezone.utc)
        cooldown_until = _as_utc_iso(
            cooldown_base + timedelta(hours=WAF_COOLDOWN_HOURS)
        )
    failures = []
    failure_values = run_result.get("failures", [])
    if not isinstance(failure_values, list):
        failure_values = []
    for failure in failure_values[:50]:
        if not isinstance(failure, dict):
            continue
        failures.append(
            {
                "scope": _redact_checkpoint_text(failure.get("scope"), 80),
                "ticker": _redact_checkpoint_text(failure.get("ticker"), 16).upper() or None,
                "systemic": str(failure.get("systemic", "false")).lower() == "true",
                "error": _redact_checkpoint_text(failure.get("error")),
            }
        )
    entries = manifest["etfs"]
    availability_counts: dict[str, int] = {}
    for entry in entries.values():
        key = str(entry.get("availability") or "unknown")
        availability_counts[key] = availability_counts.get(key, 0) + 1
    ticker_diagnostics = {
        ticker: {
            "availability": entry.get("availability"),
            "data_status": entry.get("data_status"),
            "fresh": entry.get("fresh"),
            "freshness_days": entry.get("freshness_days"),
            "records": entry.get("records"),
            "source_asof": entry.get("source_asof"),
        }
        for ticker, entry in entries.items()
    }
    required_etfs = len(catalog_items(catalog))
    complete = manifest.get("complete") is True and manifest.get("available_etfs") == required_etfs
    payload = {
        "schema_version": 1,
        "catalog_version": manifest["catalog_version"],
        "generated_at": _as_utc_iso(generated_at or _utc_now()),
        "manifest_status": manifest["status"],
        "waf_challenge": waf_challenge,
        "cooldown_until_utc": cooldown_until,
        "manifest_content_sha256": manifest[MANIFEST_CONTENT_HASH_FIELD],
        "required_etfs": manifest["required_etfs"],
        "available_etfs": manifest["available_etfs"],
        "missing_etfs": manifest["missing_etfs"],
        "fresh": manifest["fresh"],
        "complete": complete,
        "promotion_allowed": complete and run_exit_code == 0,
        "availability_counts": availability_counts,
        "ticker_diagnostics": ticker_diagnostics,
        "run": {
            "github_run_id": _redact_checkpoint_text(github_run_id, 64) or None,
            "github_run_attempt": _redact_checkpoint_text(github_run_attempt, 16) or None,
            "exit_code": run_exit_code,
            "requested": run_result.get("requested"),
            "attempted": run_result.get("attempted"),
            "succeeded": run_result.get("succeeded"),
            "skipped": run_result.get("skipped"),
            "budget_paused": run_result.get("budget_paused"),
            "waf_challenge": waf_challenge,
            "systemic_failure": run_result.get("systemic_failure"),
            "circuit_opened": run_result.get("circuit_opened"),
            "endpoint_calls": run_result.get("endpoint_calls"),
            "source_requests": run_result.get("source_requests"),
            "retries": run_result.get("retries"),
            "manifest_status": run_result.get("manifest_status"),
            "failures": failures,
        },
    }
    target = output_dir / "checkpoint_diagnostics.json"
    atomic_write_json(target, payload, indent=2)
    return target


def plan_checkpoint_promotion(
    catalog: dict[str, Any],
    out_dir: str | Path,
    today: date | None = None,
) -> dict[str, Any]:
    output_dir = Path(out_dir)
    manifest = validate_manifest(
        output_dir / "curated_manifest.json",
        min_available=118,
        require_complete=True,
        catalog=catalog,
        today=today,
    )
    files = [f"docs/data/flows/{item['ticker']}.json" for item in catalog_items(catalog)]
    files.append("docs/data/flows/curated_manifest.json")
    return {
        "schema_version": 1,
        "catalog_version": manifest["catalog_version"],
        "complete": True,
        "fresh": manifest["fresh"],
        "available_etfs": manifest["available_etfs"],
        "content_sha256": manifest[MANIFEST_CONTENT_HASH_FIELD],
        "files": files,
    }


def _execute_with_retry(
    operation: Callable[[], Any],
    policy: RetryPolicy,
    stats: FetchStats,
    sleep: Callable[[float], None] = time.sleep,
    rng: random.Random | None = None,
    can_wait: Callable[[float], bool] | None = None,
) -> Any:
    random_source = rng or random.SystemRandom()
    transient_retry_used = False
    for attempt in range(1, policy.max_attempts + 1):
        try:
            return operation()
        except APIError as exc:
            if exc.status in {429, 503}:
                if transient_retry_used or attempt >= 2:
                    raise
                transient_retry_used = True
            elif transient_retry_used:
                raise
            if not exc.retryable or attempt >= policy.max_attempts:
                raise
            if exc.retry_after is not None and exc.retry_after > policy.max_retry_after_seconds:
                raise APIError(
                    f"Retry-After {exc.retry_after}s exceeds safe cap {policy.max_retry_after_seconds}s",
                    exc.status,
                    exc.retry_after,
                    hard_stop=True,
                )
            delay = policy.delay_for(attempt, exc.retry_after, 0.0)
            if can_wait is not None and not can_wait(delay):
                raise BudgetExceeded(
                    f"retry wait {delay}s exceeds remaining runtime budget"
                )
            stats.retries += 1
            jitter = random_source.uniform(0, min(policy.base_delay_seconds, policy.max_delay_seconds))
            if exc.retry_after is None:
                delay = policy.delay_for(attempt, None, jitter)
                if can_wait is not None and not can_wait(delay):
                    raise BudgetExceeded(
                        f"retry wait {delay}s exceeds remaining runtime budget"
                    )
            sleep(delay)
    raise APIError("retry loop exhausted", 0)


def _is_challenge_text(value: str) -> bool:
    text = str(value or "").lower()
    return any(
        token in text
        for token in (
            "human verification",
            "aws waf",
            "awswaf",
            "captcha",
            "challenge",
            "just a moment",
            "verify you are human",
            "access denied",
            "request blocked",
        )
    )


def fund_flows_url(key: str) -> str:
    normalized = str(key or "").strip()
    if not normalized:
        raise ValueError("warm-up key is required")
    return f"https://www.trackinsight.com/en/fund/{quote(normalized, safe='')}/flows"


def _warmup_page(
    page: Any,
    policy: RetryPolicy,
    budget: BudgetTracker | None,
    key: str = "SPY",
) -> Any:
    if budget is not None:
        budget.check_deadline()
        required = policy.request_timeout_ms / 1000
        if budget.remaining_seconds < required:
            raise BudgetExceeded("runtime budget cannot accommodate warm-up timeout")
        timeout_ms = min(
            policy.request_timeout_ms,
            max(1, int(budget.remaining_seconds * 1000)),
        )
    else:
        timeout_ms = policy.request_timeout_ms
    url = fund_flows_url(key)
    try:
        response = page.goto(
            url,
            wait_until="domcontentloaded",
            timeout=timeout_ms,
        )
    except Exception as exc:
        raise APIError(f"Trackinsight warm-up failed: {exc}", 0, hard_stop=True) from exc
    if budget is not None:
        budget.check_deadline()

    def inspect_response() -> tuple[int, str]:
        status = getattr(response, "status", None)
        if not isinstance(status, int):
            raise APIError("Trackinsight warm-up returned no HTTP response", 0, hard_stop=True)
        try:
            body = response.text()
        except Exception as exc:
            raise APIError("Trackinsight warm-up body could not be read", 0, hard_stop=True) from exc
        return status, str(body or "")

    def reject_blocked(status: int, body: str) -> None:
        if status < 200 or status >= 300 or not body.strip() or _is_challenge_text(body):
            raise APIError(
                f"Trackinsight warm-up blocked: status={status}",
                403 if _is_challenge_text(body) else status,
                hard_stop=True,
            )

    status, body = inspect_response()
    reject_blocked(status, body)
    try:
        page.wait_for_load_state("networkidle", timeout=timeout_ms)
    except Exception as exc:
        raise APIError(
            f"Trackinsight fund page did not settle: {exc}",
            status,
            hard_stop=True,
        ) from exc
    if budget is not None:
        budget.check_deadline()
    status, body = inspect_response()
    reject_blocked(status, body)
    return response


def fetch_ticker_data(
    page: Any,
    ticker: str,
    key: str,
    start_date: str | None = None,
    start_year: int = 2016,
    fail_fast: bool = False,
    policy: RetryPolicy | None = None,
    pacer: RatePacer | None = None,
    stats: FetchStats | None = None,
    today: date | None = None,
    window_months: int = DEFAULT_WINDOW_MONTHS,
    budget: BudgetTracker | None = None,
    windows_per_request: int = DEFAULT_WINDOWS_PER_REQUEST,
    on_batch: Callable[[pd.DataFrame], None] | None = None,
) -> pd.DataFrame:
    active_policy = policy or RetryPolicy()
    active_pacer = pacer or RatePacer()
    active_stats = stats or FetchStats()
    end_date = (today or date.today()).isoformat()
    resolved_start = start_date or f"{start_year:04d}-01-01"
    requests = build_request_windows(resolved_start, end_date, key, window_months)
    batches = chunk_request_windows(requests, windows_per_request)
    logger.info(
        "Fetching %s (key: %s) from %s to %s in %d quarterly windows across %d endpoint requests",
        ticker,
        key,
        resolved_start,
        end_date,
        len(requests),
        len(batches),
    )
    combined = pd.DataFrame()

    for batch_number, batch in enumerate(batches, 1):
        def operation() -> Any:
            required_timeout = active_policy.request_timeout_ms / 1000
            if budget is not None:
                budget.ensure_request_capacity(len(batch), required_timeout)
                timeout_ms = min(
                    active_policy.request_timeout_ms,
                    max(1, int(budget.remaining_seconds * 1000)),
                )
                max_pacing_wait = budget.remaining_seconds
            else:
                timeout_ms = active_policy.request_timeout_ms
                max_pacing_wait = None
            active_pacer.wait(max_pacing_wait)
            if budget is not None:
                budget.check_deadline()
                if budget.remaining_seconds < required_timeout:
                    raise BudgetExceeded("runtime budget cannot accommodate request after pacing")
                timeout_ms = min(
                    active_policy.request_timeout_ms,
                    max(1, int(budget.remaining_seconds * 1000)),
                )
                budget.reserve_request(len(batch), required_timeout)
            active_stats.endpoint_calls += 1
            active_stats.source_requests += len(batch)
            try:
                result = page.evaluate(
                    _FETCH_JS,
                    {
                        "endpoint": ENDPOINT,
                        "payload": {"requests": batch},
                        "timeoutMs": timeout_ms,
                    },
                )
            except Exception as exc:
                raise APIError(f"browser request failed for {ticker}: {exc}", 0) from exc
            finally:
                active_pacer.mark_response_complete()
            if budget is not None:
                budget.check_deadline()
            if isinstance(result, dict) and result.get("__error"):
                status = int(result.get("status") or 0)
                status_text = result.get("statusText") or "unknown error"
                retry_after = parse_retry_after(result.get("retryAfter"))
                challenge = bool(result.get("challenge"))
                hard_stop = challenge or status in {401, 403, 405}
                raise APIError(
                    f"Trackinsight HTTP error for {ticker}: status={status} {status_text}",
                    403 if challenge else status,
                    retry_after,
                    hard_stop=hard_stop,
                )
            return result

        result = _execute_with_retry(
            operation,
            active_policy,
            active_stats,
            can_wait=budget.can_wait if budget is not None else None,
        )
        if budget is not None:
            budget.check_deadline()
        parsed = parse_snapshots(result, ticker)
        if combined.empty:
            combined = parsed
        else:
            combined = merge_history(combined, parsed)
        if on_batch is not None:
            on_batch(combined.copy())
        if budget is not None:
            budget.check_deadline()
        logger.info(
            "Completed %s batch %d/%d with %d cumulative records",
            ticker,
            batch_number,
            len(batches),
            len(combined),
        )
    if combined.empty:
        raise DataValidationError(f"{ticker} returned no history")
    return combined


def run(
    tickers: list[str],
    force: bool = False,
    fail_fast: bool = False,
    mode: str = "scheduled",
    catalog: dict[str, Any] | None = None,
    out_dir: str | Path = OUT_DIR,
    today: date | None = None,
    policy: RetryPolicy | None = None,
    pacer: RatePacer | None = None,
    circuit_breaker: CircuitBreaker | None = None,
    refresh_history: bool = False,
    budget: RunBudget | None = None,
    include_stale: bool = False,
    windows_per_request: int = DEFAULT_WINDOWS_PER_REQUEST,
) -> RunResult:
    active_catalog = catalog or load_catalog()
    output_dir = Path(out_dir)
    current_date = today or date.today()
    active_budget = budget or RunBudget()
    active_budget.validate()
    all_plans = build_fetch_plan(
        active_catalog,
        tickers,
        mode,
        force,
        output_dir,
        current_date,
        refresh_history,
        include_stale=include_stale,
        windows_per_request=windows_per_request,
    )
    plans = apply_plan_budget(all_plans, active_budget)
    result = RunResult(
        requested=len(all_plans),
        budget=active_budget,
        budget_paused=len(plans) < len(all_plans) and any(not plan["skip"] for plan in all_plans),
    )
    active_policy = policy or RetryPolicy()
    active_pacer = pacer or RatePacer()
    breaker = circuit_breaker or CircuitBreaker()
    tracker = BudgetTracker(active_budget)
    run_retrieved_at = _utc_now()
    if not any(not plan["skip"] for plan in plans):
        result.skipped = sum(plan["skip"] for plan in plans)
        try:
            manifest = build_manifest(
                active_catalog,
                output_dir,
                current_date,
                generated_at=_utc_now(),
                budget=tracker,
            )
        except BudgetExceeded:
            result.budget_paused = True
            return result
        result.manifest_path = output_dir / "curated_manifest.json"
        result.manifest_status = manifest["status"]
        result.complete = manifest["status"] == "ok" and manifest["available_etfs"] == 118
        return result
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        result.systemic_failure = True
        result.failures.append({"scope": "browser", "error": str(exc)})
        return result
    playwright = sync_playwright().start()
    browser = None
    try:
        browser = playwright.chromium.launch(headless=True, args=["--no-sandbox"])
        context = browser.new_context()
        page = context.new_page()
        page.set_default_timeout(active_policy.request_timeout_ms)
        logger.info("Warming up one Trackinsight fund page session")
        warmup_key = next(plan["key"] for plan in plans if not plan["skip"])
        try:
            _warmup_page(page, active_policy, tracker, warmup_key)
        except BudgetExceeded:
            result.budget_paused = True
            return result
        except Exception as exc:
            result.systemic_failure = True
            result.waf_challenge = isinstance(exc, APIError) and exc.status in {401, 403, 405}
            result.failures.append({"scope": "session", "error": str(exc)})
            return result
        active_pacer.mark_response_complete()
        items = catalog_by_ticker(active_catalog)
        window_months = int(active_catalog["history"]["request_window_months"])
        for index, plan in enumerate(plans):
            if plan["skip"]:
                result.skipped += 1
                continue
            ticker = plan["ticker"]
            item = items[ticker]
            try:
                tracker.reserve_ticker()
            except BudgetExceeded:
                result.budget_paused = True
                break
            result.attempted += 1
            try:
                tracker.check_deadline()
                def persist_batch(partial_frame: pd.DataFrame) -> None:
                    save_ticker_json(
                        ticker,
                        plan["key"],
                        partial_frame,
                        item=item,
                        out_dir=output_dir,
                        retrieved_at=run_retrieved_at,
                        stale_after_days=int(active_catalog["history"].get("stale_after_days", 7)),
                    )

                frame = fetch_ticker_data(
                    page,
                    ticker,
                    plan["key"],
                    start_date=plan["start_date"],
                    policy=active_policy,
                    pacer=active_pacer,
                    stats=result.stats,
                    today=current_date,
                    window_months=window_months,
                    budget=tracker,
                    windows_per_request=plan.get("windows_per_request", DEFAULT_WINDOWS_PER_REQUEST),
                    on_batch=persist_batch,
                )
                save_ticker_json(
                    ticker,
                    plan["key"],
                    frame,
                    item=item,
                    out_dir=output_dir,
                    retrieved_at=run_retrieved_at,
                    stale_after_days=int(active_catalog["history"].get("stale_after_days", 7)),
                )
                tracker.check_deadline()
                result.succeeded += 1
                breaker.record_success()
                logger.info("[%d/%d] Saved %s", index + 1, len(plans), ticker)
            except BudgetExceeded:
                result.budget_paused = True
                break
            except Exception as exc:
                systemic = isinstance(exc, APIError) and exc.systemic
                immediate_stop = isinstance(exc, APIError) and (
                    exc.hard_stop or exc.status in {401, 403, 405, 429, 503}
                )
                if isinstance(exc, APIError) and exc.status in {401, 403, 405}:
                    result.waf_challenge = True
                if systemic:
                    result.systemic_failure = True
                result.failures.append(
                    {
                        "ticker": ticker,
                        "error": str(exc),
                        "systemic": "true" if systemic else "false",
                    }
                )
                logger.error("[%d/%d] Failed %s: %s", index + 1, len(plans), ticker, exc)
                try:
                    breaker.record_failure(systemic)
                except CircuitOpen as circuit_error:
                    result.systemic_failure = True
                    result.circuit_opened = True
                    result.failures.append({"scope": "circuit", "error": str(circuit_error)})
                    break
                if immediate_stop or fail_fast:
                    break
        if result.systemic_failure:
            return result
        try:
            manifest = build_manifest(
                active_catalog,
                output_dir,
                current_date,
                generated_at=_utc_now(),
                budget=tracker,
            )
        except BudgetExceeded:
            result.budget_paused = True
            return result
        result.manifest_path = output_dir / "curated_manifest.json"
        result.manifest_status = manifest["status"]
        result.complete = manifest["status"] == "ok" and manifest["available_etfs"] == 118
        return result
    finally:
        if browser is not None:
            browser.close()
        playwright.stop()


def parse_ticker_args(values: list[Any] | None) -> list[str]:
    if not values:
        return []
    tickers = []
    for value in values:
        if value is None:
            continue
        parts = value if isinstance(value, list) else [value]
        for part_value in parts:
            if part_value is None:
                continue
            tickers.extend(
                part.strip().upper()
                for part in str(part_value).split(",")
                if part.strip()
            )
    return tickers


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Legacy external ETF flow ingestion; disabled unless explicitly opted in")
    parser.add_argument("--unsafe-external-fetch", action="store_true", help="Explicitly enable legacy external ingestion")
    parser.add_argument("--validate-local", "--verify-local", dest="validate_local", action="store_true", help="Validate the authoritative local dataset without network access")
    parser.add_argument("--catalog", action="store_true", help="Fetch every legacy catalog row")
    parser.add_argument("--curated", action="store_true", help="Alias for --catalog")
    parser.add_argument("--ticker", action="append", help="One or more comma-separated primary tickers")
    parser.add_argument("--tickers", action="append", help="One or more comma-separated primary tickers")
    parser.add_argument("--mode", choices=["scheduled", "backfill", "resume", "smoke"], default="scheduled")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--refresh-history", action="store_true")
    parser.add_argument("--include-stale", action="store_true")
    parser.add_argument("--fail-fast", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--manifest-only", action="store_true")
    parser.add_argument("--checkpoint-plan", action="store_true")
    parser.add_argument("--check-waf-cooldown", action="store_true")
    parser.add_argument("--checkpoint-diagnostics", action="store_true")
    parser.add_argument("--plan-promotion", action="store_true")
    parser.add_argument("--run-result", type=Path)
    parser.add_argument("--run-exit-code", type=int)
    parser.add_argument("--github-run-id")
    parser.add_argument("--github-run-attempt")
    parser.add_argument("--export-catalog", action="store_true")
    parser.add_argument("--verify-manifest", action="store_true")
    parser.add_argument("--validate-keys", action="store_true")
    parser.add_argument("--require-complete", dest="require_complete", action="store_true")
    parser.add_argument("--allow-partial", dest="require_complete", action="store_false")
    parser.set_defaults(require_complete=True)
    parser.add_argument("--min-available", type=int, default=118)
    parser.add_argument("--max-tickers", type=int, default=DEFAULT_MAX_TICKERS)
    parser.add_argument("--max-requests", type=int, default=DEFAULT_MAX_REQUESTS)
    parser.add_argument("--max-windows", type=int, default=DEFAULT_MAX_WINDOWS)
    parser.add_argument("--max-runtime-seconds", type=float, default=DEFAULT_MAX_RUNTIME_SECONDS)
    parser.add_argument("--windows-per-request", type=int, default=DEFAULT_WINDOWS_PER_REQUEST)
    parser.add_argument("--max-retry-after-seconds", type=float, default=120.0)
    parser.add_argument("--out-dir", type=Path, default=OUT_DIR)
    parser.add_argument("--catalog-path", type=Path, default=CATALOG_PATH)
    parser.add_argument("--ui-catalog-path", type=Path, default=UI_CATALOG_PATH)
    parser.add_argument("--index-path", type=Path, default=LOCAL_INDEX_PATH)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.validate_local:
            from build_local_flow_artifacts import _public_report, validate_local_dataset
            report = validate_local_dataset(REPO_ROOT / "data" / "flows")
            print(json.dumps(_public_report(report), indent=2, ensure_ascii=False, sort_keys=True))
            return 0
        if not args.unsafe_external_fetch:
            print(
                "External ETF flow ingestion is disabled. Use scripts/build_local_flow_artifacts.py for local artifacts; "
                "legacy ingestion requires the explicit --unsafe-external-fetch manual opt-in.",
                file=sys.stderr,
            )
            return 2
        _assert_output_not_authoritative(args.out_dir)
        catalog = load_catalog(args.catalog_path)
        if args.export_catalog:
            target = export_ui_catalog(args.catalog_path, args.ui_catalog_path)
            print(json.dumps({"catalog": str(target), "catalog_version": catalog["catalog_version"]}))
            return 0
        if args.validate_keys:
            report = validate_catalog_keys(
                catalog,
                args.index_path,
                args.catalog_path,
            )
            print(json.dumps(report, indent=2))
            return 0
        if args.check_waf_cooldown:
            active = waf_cooldown_active(args.out_dir)
            print(json.dumps({"waf_cooldown_active": active}))
            return 0 if active else 1
        if args.checkpoint_plan:
            checkpoint_budget = RunBudget(
                max_tickers=args.max_tickers,
                max_requests=args.max_requests,
                max_windows=args.max_windows,
                max_runtime_seconds=args.max_runtime_seconds,
            )
            report = build_checkpoint_plan(
                catalog,
                args.out_dir,
                tickers=parse_ticker_args(args.tickers),
                mode=args.mode,
                budget=checkpoint_budget,
                windows_per_request=args.windows_per_request,
            )
            print(json.dumps(report, indent=2))
            return 0
        if args.checkpoint_diagnostics:
            target = write_checkpoint_diagnostics(
                catalog,
                args.out_dir,
                run_result_path=args.run_result,
                run_exit_code=args.run_exit_code,
                github_run_id=args.github_run_id,
                github_run_attempt=args.github_run_attempt,
            )
            print(json.dumps({"checkpoint_diagnostics": str(target)}))
            return 0
        if args.plan_promotion:
            report = plan_checkpoint_promotion(catalog, args.out_dir)
            print(json.dumps(report, indent=2))
            return 0
        if args.manifest_only:
            manifest = build_manifest(catalog, args.out_dir)
            print(json.dumps({"status": manifest["status"], "total_etfs": manifest["total_etfs"]}))
            return 0
        if args.verify_manifest:
            manifest = validate_manifest(
                args.out_dir / "curated_manifest.json",
                args.min_available,
                args.require_complete,
                catalog=catalog,
            )
            print(json.dumps({"status": manifest["status"], "available_etfs": manifest["available_etfs"]}))
            return 0
        tickers = parse_ticker_args([args.ticker, args.tickers])
        if args.mode == "smoke" and not tickers:
            raise CatalogError("smoke mode requires --ticker or --tickers")
        budget = RunBudget(
            max_tickers=args.max_tickers,
            max_requests=args.max_requests,
            max_windows=args.max_windows,
            max_runtime_seconds=args.max_runtime_seconds,
        )
        all_plans = build_fetch_plan(
            catalog,
            tickers,
            args.mode,
            args.force,
            args.out_dir,
            refresh_history=args.refresh_history,
            include_stale=args.include_stale,
            windows_per_request=args.windows_per_request,
        )
        plans = apply_plan_budget(all_plans, budget)
        if args.dry_run:
            report = {
                "catalog_version": catalog["catalog_version"],
                "primary_count": len(catalog_items(catalog)),
                "watch_tier_count": len(catalog["watch_tier"]),
                "category_counts": manifest_category_counts(catalog),
                "requested_tickers": len(all_plans),
                "selected_tickers": len([plan for plan in plans if not plan["skip"]]),
                "source_requests": sum(
                    plan.get("budget_source_requests", plan["source_requests"])
                    for plan in plans
                ),
                "endpoint_requests": sum(
                    plan.get("budget_endpoint_requests", plan.get("endpoint_requests", 0))
                    for plan in plans
                ),
                "windows_per_request": args.windows_per_request,
                "budget": budget.__dict__,
                "pacing_seconds": [5.0, 9.0],
                "request_timeout_seconds": RetryPolicy().request_timeout_ms / 1000,
                "plans": plans,
            }
            print(json.dumps(report, indent=2))
            return 0
        result = run(
            tickers,
            force=args.force,
            fail_fast=args.fail_fast,
            mode=args.mode,
            catalog=catalog,
            out_dir=args.out_dir,
            refresh_history=args.refresh_history,
            budget=budget,
            include_stale=args.include_stale,
            windows_per_request=args.windows_per_request,
            policy=RetryPolicy(max_retry_after_seconds=args.max_retry_after_seconds),
        )
        print(json.dumps(result.to_dict(), indent=2))
        if result.systemic_failure:
            return 1
        if args.mode == "resume" and result.budget_paused:
            return 0
        if result.attempted > 0 and result.succeeded == 0:
            return 1
        if args.fail_fast and result.failures:
            return 1
        if args.require_complete and not result.complete:
            return 1
        return 0
    except (CatalogError, DataValidationError, FlowError, ValueError) as exc:
        logger.error("%s", exc)
        return 2


def manifest_category_counts(catalog: dict[str, Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in catalog_items(catalog):
        counts[item["category"]] = counts.get(item["category"], 0) + 1
    return counts


if __name__ == "__main__":
    sys.exit(main())
