import copy
import json
import random
import socket
import subprocess
import tempfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
import yaml

from scripts import fetch_etf_flow as flow


def raw_response():
    return [
        {
            "stamp": {"data": [20000, 20001, 20002]},
            "USD:flow": {"scale": 100, "data": [100, None, 0]},
            "nav": {"scale": 1, "data": [10, 11, None]},
            "perf": {"scale": 1, "data": [None, 0, 1]},
        }
    ]


def sample_frame():
    return flow.parse_snapshots(raw_response(), "TEST")


def dated_frame(end_date=date(2026, 9, 21)):
    dates = [end_date - timedelta(days=offset) for offset in (3, 2, 1, 0)]
    stamps = [(value - date(1970, 1, 1)).days for value in dates]
    return flow.parse_snapshots(
        [
            {
                "stamp": {"data": stamps},
                "USD:flow": {"scale": 100, "data": [100, None, 0, 10]},
                "nav": {"scale": 1, "data": [10, 11, 12, 13]},
                "perf": {"scale": 1, "data": [None, 0, 1, 2]},
            }
        ],
        "TEST",
    )


SYNTHETIC_CATALOG_VERSION = "test-synthetic-118-v1"
SYNTHETIC_WATCH_TICKERS = ("WATCHA", "WATCHB", "WATCHC", "WATCHD", "WARNE", "WAXON")
SYNTHETIC_LONG_ONLY_SUBGROUPS = (
    "core",
    "factors",
    "sectors",
    "themes",
    "bonds",
    "real_assets",
    "international",
)
SYNTHETIC_EXPECTED_COUNTS = {
    "long_only": 61,
    "daily_2x": 50,
    "inverse": 4,
    "index_leverage": 3,
    "watch_tier": 6,
}


@pytest.fixture(autouse=True)
def _forbid_network(monkeypatch):
    def _blocked(*args, **kwargs):
        raise AssertionError("flow catalog tests must not perform network access")

    monkeypatch.setattr(socket, "socket", _blocked)
    monkeypatch.setattr(socket, "create_connection", _blocked)
    monkeypatch.setattr(socket, "getaddrinfo", _blocked)


def _synthetic_row(ticker, category, subgroup, direction, cadence, leverage, featured, alternatives=()):
    return {
        "ticker": ticker,
        "fund_name": f"{ticker} Synthetic Fund",
        "underlying_ticker": ticker,
        "underlying_name": f"{ticker} Synthetic Underlying",
        "category": category,
        "subgroup": subgroup,
        "issuer": "Synthetic Issuer",
        "leverage_target": leverage,
        "direction": direction,
        "reset_cadence": cadence,
        "risk_tier": "core" if category == "long_only" else "elevated",
        "featured": featured,
        "canonical": True,
        "alternatives": list(alternatives),
        "trackinsight_key": ticker,
    }


def _synthetic_primary_rows():
    tickers = ["SPY", "VTI", "QQQ", "XLK", "MQQQ", "NVDL"] + [
        f"LVRG{index:03d}" for index in range(112)
    ]
    assert len(tickers) == 118
    rows = []
    for index, ticker in enumerate(tickers):
        if index < 61:
            category, direction, cadence, leverage = "long_only", "long", "daily", 2.0
            subgroup = SYNTHETIC_LONG_ONLY_SUBGROUPS[index % len(SYNTHETIC_LONG_ONLY_SUBGROUPS)]
        elif index < 111:
            category, direction, cadence, leverage = "daily_2x", "long", "daily", 2.0
            subgroup = "leveraged"
        elif index < 115:
            category, direction, cadence, leverage = "inverse", "inverse", "daily", -2.0
            subgroup = "inverse"
        else:
            category, direction, cadence, leverage = "index_leverage", "long", "quarterly", 2.0
            subgroup = "index"
        rows.append(
            _synthetic_row(
                ticker,
                category,
                subgroup,
                direction,
                cadence,
                leverage,
                featured=index < 24,
                alternatives=("NVDX", "NVDU", "NVDG") if ticker == "NVDL" else (),
            )
        )
    return rows


def _synthetic_watch_rows():
    return [
        _synthetic_row(ticker, "daily_2x", "watch", "long", "daily", 2.0, False)
        for ticker in SYNTHETIC_WATCH_TICKERS
    ]


def write_synthetic_catalog(directory, mutate=None):
    payload = {
        "schema_version": flow.SCHEMA_VERSION,
        "catalog_version": SYNTHETIC_CATALOG_VERSION,
        "source": {
            "name": "Trackinsight",
            "endpoint": flow.ENDPOINT,
            "flow_currency": "USD",
            "nav_currency": "USD",
            "description": "Synthetic offline catalog used by tests.",
        },
        "expected_primary_count": 118,
        "expected_counts": dict(SYNTHETIC_EXPECTED_COUNTS),
        "history": {
            "earliest_start": "2016-01-01",
            "scheduled_overlap_days": 7,
            "stale_after_days": 7,
            "smoke_lookback_days": 30,
            "request_window_months": 3,
            "windows_per_request": flow.DEFAULT_WINDOWS_PER_REQUEST,
        },
        "instruments": _synthetic_primary_rows(),
        "watch_tier": _synthetic_watch_rows(),
    }
    if mutate is not None:
        mutate(payload)
    path = Path(directory) / "synthetic_catalog.yaml"
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


@pytest.fixture(scope="module")
def synthetic(tmp_path_factory):
    directory = tmp_path_factory.mktemp("synthetic_catalog")
    path = write_synthetic_catalog(directory)
    catalog = flow.load_catalog(path)
    rows = [
        [item["ticker"], item["trackinsight_key"], item["fund_name"], item["issuer"], "synthetic", "USD"]
        for item in [*catalog["instruments"], *catalog["watch_tier"]]
    ]
    index_path = directory / "synthetic_index.json"
    index_path.write_text(json.dumps({"rows": rows}), encoding="utf-8")
    return SimpleNamespace(path=path, index_path=index_path, catalog=catalog, directory=directory)


@pytest.fixture
def catalog(synthetic):
    return copy.deepcopy(synthetic.catalog)


def test_catalog_count_groups_and_watch_tier(catalog):
    items = catalog["instruments"]
    assert len(items) == 118
    assert len({item["ticker"] for item in items}) == 118
    assert flow.manifest_category_counts(catalog) == {
        "long_only": 61,
        "daily_2x": 50,
        "inverse": 4,
        "index_leverage": 3,
    }
    assert len(catalog["watch_tier"]) == 6
    assert {item["ticker"] for item in catalog["watch_tier"]} == set(SYNTHETIC_WATCH_TICKERS)
    assert sum(item["featured"] for item in items) == 24
    assert all(item["canonical"] for item in items)
    assert all(item["source"] == "Trackinsight" for item in items)
    assert all(item["flow_currency"] == item["nav_currency"] == "USD" for item in items)
    assert all(item["objective"] for item in items)
    assert all(abs(float(item["leverage_target"])) >= 2 for item in catalog["watch_tier"])


def test_catalog_contains_requested_subgroups_and_alternatives(catalog):
    items = {item["ticker"]: item for item in catalog["instruments"]}
    assert {item["subgroup"] for item in items.values() if item["category"] == "long_only"} == {
        "core",
        "factors",
        "sectors",
        "themes",
        "bonds",
        "real_assets",
        "international",
    }
    assert items["NVDL"]["alternatives"] == ["NVDX", "NVDU", "NVDG"]
    assert all(not (set(item["alternatives"]) & set(items)) for item in items.values())
    assert not hasattr(flow, "INDEX_PATH")


def test_catalog_keys_validate_against_local_index_offline(synthetic, catalog):
    report = flow.validate_catalog_keys(
        catalog,
        index_path=synthetic.index_path,
        catalog_path=synthetic.path,
    )
    assert report == {
        "checked": 124,
        "primary": 118,
        "watch_tier": 6,
        "unresolved": [],
    }
    items = {item["ticker"]: item for item in catalog["instruments"]}
    watch = {item["ticker"]: item for item in catalog["watch_tier"]}
    assert items["SPY"]["trackinsight_key"] == "SPY"
    assert items["SPY"]["issuer"] == "Synthetic Issuer"
    assert items["NVDL"]["trackinsight_key"] == "NVDL"
    assert items["LVRG000"]["fund_name"] == "LVRG000 Synthetic Fund"
    assert watch[SYNTHETIC_WATCH_TICKERS[0]]["trackinsight_key"] == SYNTHETIC_WATCH_TICKERS[0]


def test_catalog_identity_constraints_reject_wrong_key_ticker_currency_name_provider(synthetic, catalog):
    base = catalog
    mutations = [
        ("trackinsight_key", "SPY", "WRONG:SPY"),
        ("ticker", "SPY", "SPX"),
        ("flow_currency", "SPY", "EUR"),
        ("fund_name", "SPY", "Wrong Fund"),
        ("issuer", "SPY", "Wrong Issuer"),
        ("issuer", "SPY", "State"),
    ]
    for field, ticker, value in mutations:
        tampered = copy.deepcopy(base)
        item = next(item for item in tampered["instruments"] if item["ticker"] == ticker)
        item[field] = value
        with pytest.raises(flow.CatalogError):
            flow.validate_catalog_keys(
                tampered,
                index_path=synthetic.index_path,
                catalog_path=synthetic.path,
            )
    assert flow.validate_catalog_keys(
        base,
        index_path=synthetic.index_path,
        catalog_path=synthetic.path,
    )["unresolved"] == []


def test_load_catalog_rejects_alternatives_that_collide_with_primary_rows(tmp_path):
    def collide(payload):
        payload["instruments"][0]["alternatives"] = [payload["instruments"][1]["ticker"]]

    path = write_synthetic_catalog(tmp_path, mutate=collide)
    with pytest.raises(flow.CatalogError, match="duplicate primary"):
        flow.load_catalog(path)


def test_load_catalog_rejects_expected_count_mismatches(tmp_path):
    path = write_synthetic_catalog(
        tmp_path,
        mutate=lambda payload: payload.__setitem__("expected_primary_count", 117),
    )
    with pytest.raises(flow.CatalogError, match="expected_primary_count"):
        flow.load_catalog(path)


def test_parser_preserves_null_zero_and_scale():
    frame = sample_frame()
    assert len(frame) == 3
    assert frame.loc[0, "usd_flow"] == 1.0
    assert pd.isna(frame.loc[1, "usd_flow"])
    assert frame.loc[2, "usd_flow"] == 0.0
    records = flow.frame_records(frame)
    assert records[1]["usd_flow"] is None
    assert records[2]["usd_flow"] == 0.0
    assert records[2]["nav"] is None


def test_parser_rejects_invalid_shapes_values_and_duplicates():
    with pytest.raises(flow.DataValidationError):
        flow.parse_snapshots({"stamp": {"data": [20000]}, "USD:flow": {"scale": 0, "data": [1]}}, "X")
    with pytest.raises(flow.DataValidationError):
        flow.parse_snapshots({"stamp": {"data": [20000]}, "USD:flow": {"scale": 1, "data": [float("nan")]}}, "X")
    with pytest.raises(flow.DataValidationError):
        flow.parse_snapshots("not-a-response", "X")
    duplicate = [
        {"stamp": {"data": [20000]}, "USD:flow": {"data": [1]}, "nav": {"data": [10]}},
        {"stamp": {"data": [20000]}, "USD:flow": {"data": [2]}, "nav": {"data": [10]}},
    ]
    with pytest.raises(flow.DataValidationError):
        flow.parse_snapshots(duplicate, "X")


def test_retry_after_and_exponential_policy():
    assert flow.parse_retry_after("7") == 7
    assert flow.parse_retry_after(None) is None
    policy = flow.RetryPolicy(base_delay_seconds=2, max_delay_seconds=10)
    assert policy.delay_for(1) == 2
    assert policy.delay_for(2) == 4
    assert policy.delay_for(3) == 8
    assert policy.delay_for(1, retry_after=9) == 9
    assert policy.delay_for(2, jitter=1.5) == 5.5


def test_retry_operation_is_bounded_and_counts_retries():
    calls = []
    sleeps = []

    def operation():
        calls.append(1)
        if len(calls) < 2:
            raise flow.APIError("busy", 503, 2)
        return "ok"

    stats = flow.FetchStats()
    result = flow._execute_with_retry(
        operation,
        flow.RetryPolicy(max_attempts=4, base_delay_seconds=1),
        stats,
        sleep=sleeps.append,
        rng=random.Random(7),
    )
    assert result == "ok"
    assert len(calls) == 2
    assert len(sleeps) == 1
    assert all(value >= 2 for value in sleeps)
    assert stats.retries == 1

    with pytest.raises(flow.APIError):
        flow._execute_with_retry(
            lambda: (_ for _ in ()).throw(flow.APIError("bad identity", 404)),
            flow.RetryPolicy(max_attempts=4),
            flow.FetchStats(),
            sleep=sleeps.append,
        )


def test_rate_limit_retry_stops_after_one_retry():
    calls = []
    sleeps = []

    def operation():
        calls.append(1)
        raise flow.APIError("rate limited", 503, 0)

    with pytest.raises(flow.APIError):
        flow._execute_with_retry(
            operation,
            flow.RetryPolicy(max_attempts=4, base_delay_seconds=0),
            flow.FetchStats(),
            sleep=sleeps.append,
        )
    assert len(calls) == 2
    assert len(sleeps) == 1


def test_retry_after_cap_and_remaining_budget_are_fail_closed():
    sleeps = []
    with pytest.raises(flow.APIError) as cap_error:
        flow._execute_with_retry(
            lambda: (_ for _ in ()).throw(flow.APIError("rate limited", 429, 121)),
            flow.RetryPolicy(max_retry_after_seconds=120),
            flow.FetchStats(),
            sleep=sleeps.append,
        )
    assert cap_error.value.hard_stop is True
    assert sleeps == []

    with pytest.raises(flow.BudgetExceeded):
        flow._execute_with_retry(
            lambda: (_ for _ in ()).throw(flow.APIError("unavailable", 503, 10)),
            flow.RetryPolicy(),
            flow.FetchStats(),
            sleep=sleeps.append,
            can_wait=lambda _: False,
        )
    assert sleeps == []


def test_warmup_inspects_status_and_challenge_without_endpoint_calls():
    class Response:
        def __init__(self, status, body):
            self.status = status
            self.headers = {"content-type": "text/html"}
            self.body = body

        def text(self):
            return self.body

    class Page:
        def __init__(self, response):
            self.response = response
            self.calls = 0
            self.goto_url = None
            self.settled = 0

        def goto(self, url, **kwargs):
            self.calls += 1
            self.goto_url = url
            return self.response

        def wait_for_load_state(self, state, **kwargs):
            self.settled += 1

    for response in [Response(403, "blocked"), Response(405, "blocked"), Response(200, "Human Verification")]:
        page = Page(response)
        with pytest.raises(flow.APIError) as error:
            flow._warmup_page(page, flow.RetryPolicy(), None, "ARCX:SOXL")
        assert error.value.hard_stop is True
        assert page.calls == 1
        assert page.goto_url.endswith("/en/fund/ARCX%3ASOXL/flows")

    success_page = Page(Response(200, "<html>fund page</html>"))
    assert flow._warmup_page(success_page, flow.RetryPolicy(), None, "SPY") is success_page.response
    assert success_page.settled == 1

    budget_page = Page(Response(200, "ok"))
    tracker = flow.BudgetTracker(
        flow.RunBudget(max_tickers=1, max_requests=1, max_windows=1, max_runtime_seconds=1),
        clock=lambda: 0.0,
    )
    with pytest.raises(flow.BudgetExceeded):
        flow._warmup_page(
            budget_page,
            flow.RetryPolicy(request_timeout_ms=20_000),
            tracker,
        )
    assert budget_page.calls == 0


def test_fetch_retries_browser_http_error_objects():
    class FakePage:
        def __init__(self):
            self.calls = 0

        def evaluate(self, script, payload):
            self.calls += 1
            if self.calls == 1:
                return {"__error": True, "status": 503, "statusText": "busy", "retryAfter": "0"}
            return raw_response()

    page = FakePage()
    stats = flow.FetchStats()
    frame = flow.fetch_ticker_data(
        page,
        "TEST",
        "TEST",
        start_date="2024-10-01",
        today=date(2024, 10, 10),
        policy=flow.RetryPolicy(max_attempts=2, base_delay_seconds=0),
        pacer=flow.RatePacer(0, 0, sleep=lambda _: None, rng=random.Random(1)),
        stats=stats,
        window_months=12,
    )
    assert len(frame) == 3
    assert page.calls == 2
    assert stats.retries == 1


def test_quarterly_windows_are_bounded_into_same_ticker_requests():
    requests = flow.build_request_windows("2016-01-01", "2016-12-31", "ARCX:SOXL")
    batches = flow.chunk_request_windows(requests, windows_per_request=2)
    assert [len(batch) for batch in batches] == [2, 2]
    assert all({request["fund"] for request in batch} == {"ARCX:SOXL"} for batch in batches)
    assert requests[0]["startDate"] == "2016-01-01"
    assert requests[1]["startDate"] == "2016-04-01"
    assert requests[-1]["endDate"] == "2016-12-31"


def test_fetch_uses_one_ticker_and_checkpoints_each_batch():
    class FakePage:
        def __init__(self):
            self.payloads = []

        def evaluate(self, script, payload):
            self.payloads.append(payload)
            return raw_response()

    page = FakePage()
    batches = []
    frame = flow.fetch_ticker_data(
        page,
        "TEST",
        "ARCX:SOXL",
        start_date="2016-01-01",
        today=date(2016, 12, 31),
        window_months=3,
        windows_per_request=2,
        pacer=flow.RatePacer(0, 0, sleep=lambda _: None),
        on_batch=batches.append,
    )
    assert len(page.payloads) == 2
    assert all(len(payload["payload"]["requests"]) == 2 for payload in page.payloads)
    assert all(
        {request["fund"] for request in payload["payload"]["requests"]} == {"ARCX:SOXL"}
        for payload in page.payloads
    )
    assert len(batches) == 2
    assert len(frame) == 3


def test_flow_scraper_contains_no_waf_evasion_policy():
    source = (Path(__file__).parent.parent / "scripts" / "fetch_etf_flow.py").read_text(encoding="utf-8")
    forbidden = (
        "navigator.webdriver",
        "AutomationControlled",
        "user_agent=",
        "add_init_script",
        "disable-blink-features",
    )
    assert all(token not in source for token in forbidden)
    assert "browser.new_context()" in source
    assert "chromium.launch" in source


@pytest.mark.parametrize("status", [403, 405])
def test_access_statuses_are_immediate_hard_stops(status):
    class FakePage:
        def __init__(self):
            self.calls = 0

        def evaluate(self, script, payload):
            self.calls += 1
            return {"__error": True, "status": status, "statusText": "blocked"}

    page = FakePage()
    with pytest.raises(flow.APIError) as error:
        flow.fetch_ticker_data(
            page,
            "TEST",
            "TEST",
            start_date="2024-10-01",
            today=date(2024, 10, 10),
            policy=flow.RetryPolicy(max_attempts=4),
            pacer=flow.RatePacer(0, 0, sleep=lambda _: None),
            window_months=12,
        )
    assert error.value.hard_stop is True
    assert error.value.retryable is False
    assert page.calls == 1


def test_challenge_html_is_immediate_hard_stop():
    class FakePage:
        def __init__(self):
            self.calls = 0

        def evaluate(self, script, payload):
            self.calls += 1
            return {
                "__error": True,
                "status": 200,
                "statusText": "Challenge response",
                "challenge": True,
            }

    page = FakePage()
    with pytest.raises(flow.APIError) as error:
        flow.fetch_ticker_data(
            page,
            "TEST",
            "TEST",
            start_date="2024-10-01",
            today=date(2024, 10, 10),
            policy=flow.RetryPolicy(max_attempts=4),
            pacer=flow.RatePacer(0, 0, sleep=lambda _: None),
            window_months=12,
        )
    assert error.value.hard_stop is True
    assert page.calls == 1


def test_rate_pacer_waits_after_response_completion():
    current = [0.0]
    sleeps = []

    def clock():
        return current[0]

    def sleep(seconds):
        sleeps.append(seconds)
        current[0] += seconds

    pacer = flow.RatePacer(2, 3, sleep=sleep, clock=clock, rng=random.Random(1))
    pacer.mark_response_complete()
    pacer.wait()
    pacer.mark_response_complete()
    pacer.wait()
    assert len(sleeps) == 2
    assert all(2 <= value <= 3 for value in sleeps)


def test_circuit_breaker_opens_only_on_systemic_failures():
    breaker = flow.CircuitBreaker(threshold=2)
    breaker.record_failure(False)
    breaker.record_failure(False)
    breaker.record_failure(True)
    with pytest.raises(flow.CircuitOpen):
        breaker.record_failure(True)
    assert breaker.opened


def test_plan_and_runtime_budgets_bound_resumable_work(tmp_path, catalog):
    budget = flow.RunBudget(max_tickers=1, max_requests=1, max_windows=100)
    plans = flow.build_fetch_plan(
        catalog,
        ["SPY", "MQQQ"],
        mode="resume",
        out_dir=tmp_path,
        today=date(2026, 9, 24),
        budget=budget,
    )
    assert len([plan for plan in plans if not plan["skip"]]) == 1
    tracker = flow.BudgetTracker(budget, clock=lambda: 0.0)
    tracker.reserve_ticker()
    tracker.reserve_request(1)
    with pytest.raises(flow.BudgetExceeded):
        tracker.reserve_ticker()
    runtime_clock = [0.0]
    runtime_tracker = flow.BudgetTracker(
        flow.RunBudget(max_tickers=1, max_requests=1, max_windows=1, max_runtime_seconds=1),
        clock=lambda: runtime_clock[0],
    )
    runtime_clock[0] = 2.0
    with pytest.raises(flow.BudgetExceeded):
        runtime_tracker.reserve_request(1)


def test_runtime_budget_blocks_request_and_response_completion():
    class FakePage:
        def __init__(self, clock):
            self.clock = clock
            self.calls = 0

        def evaluate(self, script, payload):
            self.calls += 1
            self.clock[0] += 2
            return raw_response()

    one_second = [0.0]
    short_budget = flow.RunBudget(max_tickers=1, max_requests=1, max_windows=2, max_runtime_seconds=1)
    short_tracker = flow.BudgetTracker(short_budget, clock=lambda: one_second[0])
    short_page = FakePage(one_second)
    with pytest.raises(flow.BudgetExceeded):
        flow.fetch_ticker_data(
            short_page,
            "TEST",
            "TEST",
            start_date="2024-10-01",
            today=date(2024, 10, 10),
            policy=flow.RetryPolicy(request_timeout_ms=20_000),
            pacer=flow.RatePacer(0, 0, sleep=lambda _: None),
            budget=short_tracker,
            window_months=12,
        )
    assert short_page.calls == 0

    two_seconds = [0.0]
    crossing_budget = flow.RunBudget(max_tickers=1, max_requests=1, max_windows=2, max_runtime_seconds=2)
    crossing_tracker = flow.BudgetTracker(crossing_budget, clock=lambda: two_seconds[0])
    crossing_page = FakePage(two_seconds)
    with pytest.raises(flow.BudgetExceeded):
        flow.fetch_ticker_data(
            crossing_page,
            "TEST",
            "TEST",
            start_date="2024-10-01",
            today=date(2024, 10, 10),
            policy=flow.RetryPolicy(request_timeout_ms=1_000),
            pacer=flow.RatePacer(0, 0, sleep=lambda _: None),
            budget=crossing_tracker,
            window_months=12,
        )
    assert crossing_page.calls == 1


def test_pacing_wait_is_clamped_by_runtime_budget():
    current = [0.0]
    sleeps = []
    pacer = flow.RatePacer(5, 9, sleep=lambda value: sleeps.append(value), clock=lambda: current[0], rng=random.Random(1))
    pacer.mark_response_complete()
    with pytest.raises(flow.BudgetExceeded):
        pacer.wait(max_wait_seconds=1)
    assert sleeps == []


def test_chunk_budget_checkpoints_before_stopping():
    class FakePage:
        def __init__(self):
            self.calls = 0

        def evaluate(self, script, payload):
            self.calls += 1
            return raw_response()

    page = FakePage()
    saved = []
    tracker = flow.BudgetTracker(
        flow.RunBudget(max_tickers=1, max_requests=1, max_windows=2, max_runtime_seconds=60),
        clock=lambda: 0.0,
    )
    with pytest.raises(flow.BudgetExceeded):
        flow.fetch_ticker_data(
            page,
            "TEST",
            "TEST",
            start_date="2016-01-01",
            today=date(2016, 12, 31),
            window_months=3,
            windows_per_request=2,
            budget=tracker,
            pacer=flow.RatePacer(0, 0, sleep=lambda _: None),
            on_batch=saved.append,
        )
    assert page.calls == 1
    assert len(saved) == 1


def test_merge_is_idempotent_and_preserves_history():
    old = pd.DataFrame(
        [
            {"date": "2024-01-01", "usd_flow": 1.0},
            {"date": "2024-01-02", "usd_flow": 2.0},
        ]
    )
    delta = pd.DataFrame(
        [
            {"date": "2024-01-02", "usd_flow": 20.0},
            {"date": "2024-01-03", "usd_flow": 3.0},
        ]
    )
    merged = flow.merge_history(old, delta)
    repeated = flow.merge_history(merged, delta)
    assert len(repeated) == 3
    assert repeated.loc[repeated["date"] == pd.Timestamp("2024-01-02").date(), "usd_flow"].iloc[0] == 20.0
    assert repeated["date"].min() == pd.Timestamp("2024-01-01").date()


def test_history_regression_is_rejected():
    old = pd.DataFrame(
        [
            {"date": "2024-01-01", "usd_flow": 1.0},
            {"date": "2024-01-02", "usd_flow": 2.0},
        ]
    )
    later = pd.DataFrame(
        [
            {"date": "2024-01-03", "usd_flow": 3.0},
            {"date": "2024-01-04", "usd_flow": 4.0},
        ]
    )
    with pytest.raises(flow.DataValidationError, match="start regressed"):
        flow.validate_history_regression(old, later)
    with pytest.raises(flow.DataValidationError, match="row count regressed"):
        flow.validate_history_regression(old, old.iloc[:1])


def test_atomic_write_preserves_existing_file_on_replace_failure(tmp_path, monkeypatch):
    target = tmp_path / "data.json"
    original = {"count": 1, "data": [{"value": 1}]}
    target.write_text(json.dumps(original), encoding="utf-8")
    before = target.read_bytes()

    def fail_replace(*args):
        raise OSError("simulated replace failure")

    monkeypatch.setattr(flow.os, "replace", fail_replace)
    with pytest.raises(OSError):
        flow.atomic_write_json(target, {"count": 2})
    assert target.read_bytes() == before
    assert not list(tmp_path.glob(".*.tmp"))


def test_save_normalizes_metadata_and_keeps_null(tmp_path, catalog):
    item = next(item for item in catalog["instruments"] if item["ticker"] == "SPY")
    payload, path = flow.save_ticker_json(
        "SPY",
        "SPY",
        sample_frame(),
        item=item,
        out_dir=tmp_path,
        retrieved_at=datetime(2026, 9, 24, tzinfo=timezone.utc),
        stale_after_days=7,
    )
    stored = json.loads(path.read_text(encoding="utf-8"))
    assert stored["schema_version"] == 2
    assert stored["source"] == "Trackinsight"
    assert stored["source_asof"] == "2024-10-06"
    assert stored["retrieved_at"].startswith("2026-09-24T")
    assert stored["data_quality"]["null_flow_records"] == 1
    assert stored["data"][1]["usd_flow"] is None
    assert stored["data"][2]["usd_flow"] == 0.0
    assert payload["count"] == 3


def test_manifest_file_validation_rejects_tampered_payloads(tmp_path, catalog):
    item = next(item for item in catalog["instruments"] if item["ticker"] == "SPY")
    generated = datetime(2024, 10, 6, tzinfo=timezone.utc)
    flow.save_ticker_json(
        "SPY",
        "SPY",
        sample_frame(),
        item=item,
        out_dir=tmp_path,
        retrieved_at=generated,
        stale_after_days=7,
    )
    original = json.loads((tmp_path / "SPY.json").read_text(encoding="utf-8"))
    mutations = [
        ("ticker", "QQQ"),
        ("key", "WRONG:SPY"),
        ("schema_version", 1),
        ("catalog_version", "2099-01-01.0"),
        ("count", 999),
        ("flow_currency", "EUR"),
        ("nav_currency", "EUR"),
        ("source", "OtherSource"),
        ("issuer", "Wrong Provider"),
        ("source_asof", "2024-10-05"),
        ("source_asof", "2026-01-01"),
        ("retrieved_at", "2099-01-01T00:00:00+00:00"),
        ("updated", "2024-10-05"),
        ("data_status", "stale"),
        ("quality_status", "stale"),
        ("missing_updated", True),
        ("missing_data_status", True),
        ("missing_data_quality", True),
        ("data_order", True),
        ("data_duplicate", True),
        ("nonfinite", True),
        ("quality", True),
        ("derived:cumulative_flow", True),
        ("derived:daily_inflow", True),
        ("derived:daily_outflow", True),
        ("derived:flow_zscore", True),
        ("derived:regime", True),
        ("derived:pressure", True),
        ("derived:flow_5d", True),
        ("derived:flow_20d", True),
        ("derived_missing", True),
        ("null_to_zero", True),
    ]
    for field, value in mutations:
        tampered = copy.deepcopy(original)
        if field == "data_order":
            tampered["data"] = list(reversed(tampered["data"]))
        elif field == "data_duplicate":
            tampered["data"].append(copy.deepcopy(tampered["data"][0]))
        elif field == "nonfinite":
            tampered["data"][0]["usd_flow"] = float("nan")
        elif field == "quality":
            tampered["data_quality"] = {}
        elif field == "quality_status":
            tampered["data_quality"]["status"] = "stale"
        elif field == "missing_updated":
            tampered.pop("updated")
        elif field == "missing_data_status":
            tampered.pop("data_status")
        elif field == "missing_data_quality":
            tampered.pop("data_quality")
        elif field == "derived_missing":
            tampered["data"][0].pop("cumulative_flow")
        elif field == "null_to_zero":
            tampered["data"][1]["usd_flow"] = 0.0
        elif field.startswith("derived:"):
            derived_field = field.split(":", 1)[1]
            tampered["data"][0][derived_field] = (
                "TAMPERED" if derived_field == "regime" else 999.0
            )
        else:
            tampered[field] = value
        (tmp_path / "SPY.json").write_text(json.dumps(tampered), encoding="utf-8")
        actual = flow.compute_manifest(
            catalog,
            out_dir=tmp_path,
            today=date(2024, 10, 6),
            generated_at=generated,
        )
        assert actual["etfs"]["SPY"]["availability"] == "invalid", field
        assert actual["status"] != "ok", field


def test_manifest_is_curated_and_reports_availability(tmp_path, catalog):
    (tmp_path / "UNLISTED.json").write_text(
        json.dumps({"ticker": "UNLISTED", "data": [{"date": "2026-09-23", "usd_flow": 1}]}),
        encoding="utf-8",
    )
    item = next(item for item in catalog["instruments"] if item["ticker"] == "SPY")
    flow.save_ticker_json(
        "SPY",
        "SPY",
        dated_frame(),
        item=item,
        out_dir=tmp_path,
        retrieved_at=datetime(2026, 9, 24, tzinfo=timezone.utc),
        stale_after_days=7,
    )
    manifest = flow.build_manifest(
        catalog,
        out_dir=tmp_path,
        today=date(2026, 9, 24),
        generated_at=datetime(2026, 9, 24, tzinfo=timezone.utc),
    )
    assert manifest["total_etfs"] == 118
    assert len(manifest["etfs"]) == 118
    assert "UNLISTED" not in manifest["etfs"]
    assert manifest["available_etfs"] == 1
    assert manifest["status"] == "partial"
    assert manifest["etfs"]["SPY"]["availability"] == "available"
    assert manifest["etfs"]["NVDL"]["availability"] == "missing"
    assert manifest["source_coverage"]["expected"] == 118
    with pytest.raises(flow.FlowError, match="incomplete"):
        flow.validate_manifest(
            tmp_path / "curated_manifest.json",
            catalog=catalog,
            today=date(2026, 9, 24),
        )
    complete = dict(manifest)
    complete["status"] = "ok"
    complete["complete"] = True
    complete["available_etfs"] = 118
    complete["missing_etfs"] = 0
    (tmp_path / "complete_manifest.json").write_text(json.dumps(complete), encoding="utf-8")
    with pytest.raises(flow.FlowError, match="recomputation"):
        flow.validate_manifest(
            tmp_path / "complete_manifest.json",
            catalog=catalog,
            today=date(2026, 9, 24),
        )


def test_recomputed_complete_manifest_accepts_all_catalog_files(tmp_path, catalog):
    generated = datetime(2024, 10, 6, tzinfo=timezone.utc)
    for item in catalog["instruments"]:
        flow.save_ticker_json(
            item["ticker"],
            item["trackinsight_key"],
            sample_frame(),
            item=item,
            out_dir=tmp_path,
            retrieved_at=generated,
            stale_after_days=7,
        )
    manifest = flow.build_manifest(
        catalog,
        out_dir=tmp_path,
        today=date(2024, 10, 6),
        generated_at=generated,
    )
    assert manifest["status"] == "ok"
    assert manifest["available_etfs"] == 118
    assert len(manifest["content_sha256"]) == 64
    assert manifest["fresh"] is True
    assert flow.validate_manifest(
        tmp_path / "curated_manifest.json",
        catalog=catalog,
        today=date(2024, 10, 6),
    )["available_etfs"] == 118
    promotion = flow.plan_checkpoint_promotion(
        catalog,
        tmp_path,
        today=date(2024, 10, 6),
    )
    assert promotion["complete"] is True
    assert len(promotion["files"]) == 119
    assert promotion["files"][0] == f"docs/data/flows/{catalog['instruments'][0]['ticker']}.json"
    assert promotion["files"][-1] == "docs/data/flows/curated_manifest.json"
    original = json.loads((tmp_path / "SPY.json").read_text(encoding="utf-8"))
    for field in ("updated", "data_status", "data_quality"):
        tampered = copy.deepcopy(original)
        if field == "updated":
            tampered[field] = "2024-10-05"
        elif field == "data_status":
            tampered[field] = "stale"
        else:
            tampered[field]["status"] = "stale"
        (tmp_path / "SPY.json").write_text(json.dumps(tampered), encoding="utf-8")
        with pytest.raises(flow.FlowError):
            flow.validate_manifest(
                tmp_path / "curated_manifest.json",
                catalog=catalog,
                today=date(2024, 10, 6),
            )
    (tmp_path / "SPY.json").write_text(json.dumps(original), encoding="utf-8")


def test_manifest_validation_recomputes_and_rejects_tampered_fields(tmp_path, catalog):
    generated = datetime(2024, 10, 6, tzinfo=timezone.utc)
    for item in catalog["instruments"]:
        flow.save_ticker_json(
            item["ticker"],
            item["trackinsight_key"],
            sample_frame(),
            item=item,
            out_dir=tmp_path,
            retrieved_at=generated,
            stale_after_days=7,
        )
    manifest = flow.build_manifest(
        catalog,
        out_dir=tmp_path,
        today=date(2024, 10, 6),
        generated_at=generated,
    )
    path = tmp_path / "curated_manifest.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(flow.FlowError):
        flow.validate_manifest(
            path,
            catalog=catalog,
            today=date(2025, 1, 1),
            require_fresh=False,
        )

    def reject(mutator):
        tampered = copy.deepcopy(manifest)
        mutator(tampered)
        path.write_text(json.dumps(tampered), encoding="utf-8")
        with pytest.raises(flow.FlowError):
            flow.validate_manifest(path, catalog=catalog, today=date(2024, 10, 6))

    top_values = {
        "schema_version": 1,
        "catalog_version": "tampered",
        "content_sha256": "tampered",
        "generated_utc": "2024-10-06T00:00:01+00:00",
        "source": "tampered",
        "source_endpoint": "tampered",
        "source_asof": "2000-01-01",
        "retrieved_at": "2024-10-06T00:00:01+00:00",
        "status": "empty",
        "complete": False,
        "required_etfs": 1,
        "unavailable_allowlist": ["SPY"],
        "fresh": False,
        "total_etfs": 1,
        "available_etfs": 1,
        "missing_etfs": 117,
        "categories": {"tampered": 1},
        "featured_count": 0,
        "canonical_count": 0,
        "source_coverage": {"tampered": 1},
    }
    for field, value in top_values.items():
        reject(lambda tampered, field=field, value=value: tampered.__setitem__(field, value))
    for field in ("generated_utc", "retrieved_at"):
        reject(
            lambda tampered, field=field: tampered.__setitem__(
                field,
                "2099-01-01T00:00:00+00:00",
            )
        )
    coordinated = copy.deepcopy(manifest)
    coordinated.update(
        generated_utc="2024-10-06T00:00:01+00:00",
        retrieved_at="2024-10-06T00:00:01+00:00",
    )
    coordinated["content_sha256"] = flow.manifest_content_hash(coordinated)
    path.write_text(json.dumps(coordinated), encoding="utf-8")
    with pytest.raises(flow.FlowError):
        flow.validate_manifest(path, catalog=catalog, today=date(2024, 10, 6))

    entry = manifest["etfs"]["SPY"]
    for field, value in entry.items():
        if field == "ticker":
            replacement = "QQQ"
        elif field == "nav":
            replacement = 999.0
        elif field == "data_quality":
            replacement = {}
        elif isinstance(value, bool):
            replacement = not value
        elif isinstance(value, int):
            replacement = value + 1
        elif isinstance(value, float):
            replacement = value + 1.0
        elif isinstance(value, list):
            replacement = ["tampered"]
        elif isinstance(value, dict):
            replacement = {}
        else:
            replacement = "tampered"
        reject(
            lambda tampered, field=field, replacement=replacement: tampered["etfs"]["SPY"].__setitem__(
                field,
                replacement,
            )
        )


def test_fetch_plan_uses_overlap_and_backfill_epoch(tmp_path, catalog):
    item = next(item for item in catalog["instruments"] if item["ticker"] == "SPY")
    flow.save_ticker_json(
        "SPY",
        "SPY",
        dated_frame(),
        item=item,
        out_dir=tmp_path,
        retrieved_at=datetime(2026, 9, 22, tzinfo=timezone.utc),
        stale_after_days=7,
    )
    scheduled = flow.build_fetch_plan(
        catalog,
        ["SPY"],
        mode="scheduled",
        out_dir=tmp_path,
        today=date(2026, 9, 24),
    )[0]
    assert scheduled["start_date"] == "2026-09-14"
    backfill = flow.build_fetch_plan(
        catalog,
        ["SPY"],
        mode="backfill",
        out_dir=tmp_path,
        today=date(2026, 9, 24),
    )[0]
    assert backfill["start_date"] == "2026-09-14"
    assert backfill["source_requests"] == scheduled["source_requests"]
    refreshed = flow.build_fetch_plan(
        catalog,
        ["SPY"],
        mode="backfill",
        out_dir=tmp_path,
        today=date(2026, 9, 24),
        refresh_history=True,
    )[0]
    assert refreshed["start_date"] == "2016-01-01"
    assert refreshed["source_requests"] > backfill["source_requests"]
    forced = flow.build_fetch_plan(
        catalog,
        ["SPY"],
        mode="backfill",
        force=True,
        out_dir=tmp_path,
        today=date(2026, 9, 24),
    )[0]
    assert forced["start_date"] == "2016-01-01"
    smoke = flow.build_fetch_plan(
        catalog,
        ["MQQQ"],
        mode="smoke",
        out_dir=tmp_path,
        today=date(2026, 9, 24),
    )[0]
    assert smoke["start_date"] == "2026-08-25"
    assert smoke["source_requests"] == 1

    (tmp_path / "SPY.json").unlink()
    item = next(item for item in catalog["instruments"] if item["ticker"] == "SPY")
    flow.save_ticker_json(
        "SPY",
        "SPY",
        dated_frame(end_date=date(2026, 9, 1)),
        item=item,
        out_dir=tmp_path,
        retrieved_at=datetime(2026, 9, 24, tzinfo=timezone.utc),
        stale_after_days=7,
    )
    stale_payload = json.loads((tmp_path / "SPY.json").read_text(encoding="utf-8"))
    stale_payload["data_status"] = "stale"
    stale_payload["data_quality"]["status"] = "stale"
    stale_payload["manifest_content_hash"] = flow.manifest_content_hash(stale_payload)
    (tmp_path / "SPY.json").write_text(json.dumps(stale_payload), encoding="utf-8")
    stale_selected = flow.build_fetch_plan(
        catalog,
        ["SPY"],
        mode="resume",
        out_dir=tmp_path,
        today=date(2026, 9, 24),
    )[0]
    assert stale_selected["skip"] is False
    assert stale_selected["source_stale"] is True
    assert stale_selected["checkpoint_state"] == "stale"
    assert stale_selected["source_requests"] > 0
    explicit_stale = flow.build_fetch_plan(
        catalog,
        ["SPY"],
        mode="resume",
        out_dir=tmp_path,
        today=date(2026, 9, 24),
        include_stale=True,
    )[0]
    assert explicit_stale["skip"] is False
    assert explicit_stale["source_requests"] > 0


def test_resume_selects_missing_before_cached(tmp_path, catalog):
    item = next(item for item in catalog["instruments"] if item["ticker"] == "SPY")
    flow.save_ticker_json(
        "SPY",
        "SPY",
        dated_frame(),
        item=item,
        out_dir=tmp_path,
        retrieved_at=datetime(2026, 9, 22, tzinfo=timezone.utc),
        stale_after_days=7,
    )
    plans = flow.build_fetch_plan(
        catalog,
        ["SPY", "VTI"],
        mode="resume",
        out_dir=tmp_path,
        today=date(2026, 9, 24),
    )
    assert [plan["ticker"] for plan in plans if not plan["skip"]] == ["VTI"]
    assert plans[0]["resume_cached"] is True
    assert plans[0]["checkpoint_state"] == "fresh"
    assert plans[1]["checkpoint_state"] == "missing"
    include_stale = flow.build_fetch_plan(
        catalog,
        ["SPY", "VTI"],
        mode="resume",
        out_dir=tmp_path,
        today=date(2026, 9, 24),
        include_stale=True,
    )
    assert [plan["ticker"] for plan in include_stale if not plan["skip"]] == ["VTI"]


def test_partial_historical_checkpoint_remains_resumable(tmp_path, catalog):
    item = next(item for item in catalog["instruments"] if item["ticker"] == "SPY")
    flow.save_ticker_json(
        "SPY",
        "SPY",
        sample_frame(),
        item=item,
        out_dir=tmp_path,
        retrieved_at=datetime(2026, 9, 24, tzinfo=timezone.utc),
        stale_after_days=7,
    )
    plan = flow.build_fetch_plan(
        catalog,
        ["SPY"],
        mode="resume",
        out_dir=tmp_path,
        today=date(2026, 9, 24),
    )[0]
    assert plan["checkpoint_state"] == "stale"
    assert plan["start_date"] == "2024-09-29"
    assert plan["source_requests"] > 0


def test_checkpoint_plan_selects_stale_invalid_and_missing_rows_but_not_fresh_rows(tmp_path, catalog):
    tickers = ["SPY", "VTI", "QQQ", "XLK"]
    items = {item["ticker"]: item for item in catalog["instruments"]}
    flow.save_ticker_json(
        "SPY",
        "SPY",
        dated_frame(),
        item=items["SPY"],
        out_dir=tmp_path,
        retrieved_at=datetime(2026, 9, 24, tzinfo=timezone.utc),
        stale_after_days=7,
    )
    flow.save_ticker_json(
        "VTI",
        "VTI",
        dated_frame(end_date=date(2026, 9, 1)),
        item=items["VTI"],
        out_dir=tmp_path,
        retrieved_at=datetime(2026, 9, 24, tzinfo=timezone.utc),
        stale_after_days=7,
    )
    stale = json.loads((tmp_path / "VTI.json").read_text(encoding="utf-8"))
    stale["data_status"] = "stale"
    stale["data_quality"]["status"] = "stale"
    stale["manifest_content_hash"] = flow.manifest_content_hash(stale)
    (tmp_path / "VTI.json").write_text(json.dumps(stale), encoding="utf-8")
    (tmp_path / "QQQ.json").write_text("{invalid", encoding="utf-8")
    report = flow.build_checkpoint_plan(
        catalog,
        tmp_path,
        tickers=tickers,
        mode="resume",
        budget=flow.RunBudget(
            max_tickers=10,
            max_requests=10000,
            max_windows=10000,
            max_runtime_seconds=900,
        ),
        today=date(2026, 9, 24),
        generated_at=datetime(2026, 9, 24, tzinfo=timezone.utc),
    )
    assert report["state_counts"] == {"fresh": 1, "stale": 1, "invalid": 1, "missing": 1}
    assert report["selected_tickers"] == ["VTI", "QQQ", "XLK"]
    assert report["deferred_count"] == 0
    assert all(plan["checkpoint_state"] == "stale" for plan in report["plans"] if plan["ticker"] == "VTI")


def test_checkpoint_diagnostics_are_bounded_redacted_and_non_promotable(tmp_path, catalog):
    item = next(item for item in catalog["instruments"] if item["ticker"] == "SPY")
    flow.save_ticker_json(
        "SPY",
        "SPY",
        dated_frame(),
        item=item,
        out_dir=tmp_path,
        retrieved_at=datetime(2026, 9, 24, tzinfo=timezone.utc),
        stale_after_days=7,
    )
    flow.build_manifest(catalog, tmp_path, today=date(2026, 9, 24))
    run_result = tmp_path / "run-result.json"
    run_result.write_text(
        json.dumps(
            {
                "requested": 1,
                "attempted": 1,
                "succeeded": 0,
                "failures": [
                    {
                        "scope": "provider",
                        "ticker": "VTI",
                        "systemic": "true",
                        "error": "Authorization: Bearer top-secret-token",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    target = flow.write_checkpoint_diagnostics(
        catalog,
        tmp_path,
        run_result_path=run_result,
        run_exit_code=1,
        github_run_id="12345",
        github_run_attempt="2",
        today=date(2026, 9, 24),
        generated_at=datetime(2026, 9, 24, tzinfo=timezone.utc),
    )
    diagnostics = json.loads(target.read_text(encoding="utf-8"))
    assert diagnostics["available_etfs"] == 1
    assert diagnostics["required_etfs"] == 118
    assert diagnostics["complete"] is False
    assert diagnostics["promotion_allowed"] is False
    assert diagnostics["run"]["exit_code"] == 1
    assert diagnostics["run"]["failures"][0]["ticker"] == "VTI"
    assert "top-secret-token" not in json.dumps(diagnostics)
    assert "[redacted]" in diagnostics["run"]["failures"][0]["error"]
    assert diagnostics["waf_challenge"] is False
    assert diagnostics["cooldown_until_utc"] is None
    with pytest.raises(flow.FlowError):
        flow.plan_checkpoint_promotion(catalog, tmp_path, today=date(2026, 9, 24))


def test_waf_cooldown_is_read_from_checkpoint_diagnostics(tmp_path):
    diagnostics = {
        "waf_challenge": True,
        "cooldown_until_utc": "2026-09-25T00:00:00+00:00",
    }
    (tmp_path / "checkpoint_diagnostics.json").write_text(
        json.dumps(diagnostics),
        encoding="utf-8",
    )
    assert flow.waf_cooldown_active(
        tmp_path,
        now=datetime(2026, 9, 24, 12, 0, tzinfo=timezone.utc),
    )
    assert not flow.waf_cooldown_active(
        tmp_path,
        now=datetime(2026, 9, 26, 12, 0, tzinfo=timezone.utc),
    )


def test_daily_flow_workflow_is_safe_and_explicit():
    root = Path(__file__).parent.parent
    path = root / ".github" / "workflows" / "daily_etf_flows.yml"
    text = path.read_text(encoding="utf-8")
    workflow = yaml.safe_load(text)
    on_block = workflow.get(True) or workflow.get("on")
    assert list(on_block) == ["workflow_dispatch"]
    assert "schedule" not in on_block
    acknowledgement = on_block["workflow_dispatch"]["inputs"]["acknowledgement"]
    assert acknowledgement["required"] is True
    assert acknowledgement["default"] == "External ingestion disabled"
    assert workflow["permissions"] == {"contents": "read"}
    assert workflow["concurrency"] == {"group": "daily-flows", "cancel-in-progress": False}
    assert list(workflow["jobs"]) == ["disabled"]
    checkpoint_job = workflow["jobs"]["disabled"]
    assert checkpoint_job["runs-on"] == "ubuntu-latest"
    assert checkpoint_job["timeout-minutes"] == 5
    assert len(checkpoint_job["steps"]) == 1
    step = checkpoint_job["steps"][0]
    assert "uses" not in step
    run = step["run"]
    assert "exit 1" in run
    assert "External ETF flow ingestion is intentionally disabled." in run
    assert "authoritative data is under data/flows" in run
    assert "scripts/build_local_flow_artifacts.py" in run
    assert "No Trackinsight or other network request is made by this workflow." in run
    assert "curl" not in text.lower()
    assert "wget" not in text.lower()
    assert "etf_search_index" not in text
    assert "ETRADE_PAT" not in text.upper()
    assert "PERSONAL_ACCESS_TOKEN" not in text.upper()
    assert "-X theirs" not in text
    assert "--include-stale" not in text
    assert "--dry-run" not in text
    assert "--unsafe-external-fetch" not in text
    build_workflow = yaml.safe_load(
        (root / ".github" / "workflows" / "build_site.yml").read_text(encoding="utf-8")
    )
    build_on = build_workflow.get(True) or build_workflow.get("on")
    assert "workflow_call" in build_on
    assert build_on["workflow_call"]["secrets"]["FRED_API_KEY"]["required"] is False
    assert build_workflow["permissions"] == {
        "contents": "write",
        "pages": "write",
        "id-token": "write",
    }


def test_external_ingestion_requires_explicit_unsafe_opt_in():
    assert flow.EXTERNAL_INGESTION_ENABLED is False
    help_text = flow._parser().format_help()
    assert "--unsafe-external-fetch" in help_text
    assert "Explicitly enable legacy external ingestion" in help_text
    assert "--validate-local" in help_text


@pytest.mark.parametrize(
    "argv",
    [
        [],
        ["--dry-run"],
        ["--dry-run", "--mode", "resume"],
        ["--verify-manifest", "--min-available", "118", "--require-complete"],
        ["--checkpoint-plan"],
        ["--plan-promotion"],
    ],
)
def test_main_gates_every_ingestion_mode_without_unsafe_opt_in(argv):
    assert flow.main(argv) == 2


def test_checkpoint_branch_rebase_conflict_aborts_without_overwrite(tmp_path):
    def git(*args):
        return subprocess.run(
            ["git", *args],
            cwd=tmp_path,
            capture_output=True,
            text=True,
            encoding="utf-8",
            check=True,
        )

    git("init", "-b", "main")
    git("config", "user.name", "Flow Test")
    git("config", "user.email", "flow-test@example.invalid")
    manifest = tmp_path / "docs" / "data" / "flows" / "curated_manifest.json"
    manifest.parent.mkdir(parents=True)
    manifest.write_text(json.dumps({"status": "base"}), encoding="utf-8")
    git("add", ".")
    git("commit", "-m", "base")
    git("switch", "-c", "etf-flow-checkpoint")
    manifest.write_text(json.dumps({"status": "partial-checkpoint"}), encoding="utf-8")
    git("add", ".")
    git("commit", "-m", "checkpoint")
    git("switch", "main")
    manifest.write_text(json.dumps({"status": "main-conflict"}), encoding="utf-8")
    git("add", ".")
    git("commit", "-m", "main")
    git("switch", "etf-flow-checkpoint")
    rebase = subprocess.run(
        ["git", "rebase", "main"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )
    assert rebase.returncode != 0
    git("rebase", "--abort")
    assert json.loads(manifest.read_text(encoding="utf-8")) == {"status": "partial-checkpoint"}
    assert not (tmp_path / ".git" / "rebase-merge").exists()
    assert not (tmp_path / ".git" / "rebase-apply").exists()


def test_daily_scrape_rebase_conflicts_abort_without_overwrite():
    path = Path(__file__).parent.parent / ".github" / "workflows" / "daily_scrape.yml"
    text = path.read_text(encoding="utf-8")
    workflow = yaml.safe_load(text)
    assert "-X theirs" not in text
    assert "git pull --rebase --autostash origin main" in text
    assert "git rebase --abort" in text
    assert "exit 1" in text
    save_step = next(
        step
        for step in workflow["jobs"]["scrape-and-commit"]["steps"]
        if step.get("id") == "save_data"
    )
    assert "-X theirs" not in save_step["run"]


def test_build_site_gates_complete_flow_data_before_upload():
    path = Path(__file__).parent.parent / ".github" / "workflows" / "build_site.yml"
    text = path.read_text(encoding="utf-8")
    workflow = yaml.safe_load(text)
    steps = workflow["jobs"]["build"]["steps"]
    names = [step.get("name", "") for step in steps]
    build_index = next(
        index for index, name in enumerate(names) if name == "Build local ETF flow artifacts"
    )
    gate_index = next(
        index for index, name in enumerate(names) if name == "Verify complete local ETF flow artifacts"
    )
    upload_index = next(index for index, name in enumerate(names) if name == "Upload Pages artifact")
    assert build_index < gate_index < upload_index
    gate = steps[gate_index]
    run = gate["run"]
    assert "--verify-output --output-dir docs/data/flows" in run
    assert "['instruments'] == 117" in run
    assert "['featured'] == 24" in run
    assert "'watch_tier' not in catalog" in run
    assert "manifest['counts']['instruments'] == 117" in run
    assert "manifest['complete'] is True and manifest['status'] == 'complete'" in run
    assert "manifest['source']['network_fetch'] is False" in run
    assert "catalog['source']['network_fetch'] is False" in run
    assert "117 instruments, 24 featured, no watch tier, no network fetch" in run
    assert gate.get("continue-on-error") is not True
    assert "-X theirs" not in text
    stock_step = next(step for step in steps if "Persist stock-detail coverage" in step.get("name", ""))
    assert stock_step.get("continue-on-error") is not True
    assert "git rebase --abort" in stock_step["run"]


def test_browser_flow_ui_cannot_trigger_ingestion():
    root = Path(__file__).parent.parent
    text = (root / "docs" / "markets.html").read_text(encoding="utf-8")
    start = text.index('<section class="flow-research"')
    end = text.index('<!-- FOOTER -->', start)
    active = text[start:end]
    script = (root / "docs" / "flow-ui.js").read_text(encoding="utf-8")
    source = f"{text}\n{script}".lower()
    assert "api.github.com" not in source
    assert "github_pat" not in source
    assert "personal_access_token" not in source
    assert "workflow_dispatch" not in source
    assert "etf_search_index" not in source
    assert "15,000" not in source
    assert "15k" not in source
    assert "cloud" not in source
    assert "role=\"combobox\"" in active
    assert "aria-live=\"polite\"" in active
