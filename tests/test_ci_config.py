"""
YAML smoke tests for GitHub Actions workflow files.

Validates that the CI configuration matches the requirements for the
automation-self-living-data-flow spec.

Requirements covered: 1.1, 1.2, 1.3, 4.1, 4.2, 4.3, 4.4, 4.5, 7.4, 7.5, 3.7, 3.8, 7.2
"""
import yaml
from pathlib import Path

# ---------------------------------------------------------------------------
# Load workflow files once at module level
# ---------------------------------------------------------------------------
_REPO_ROOT = Path(__file__).parent.parent
# encoding='utf-8': workflow comments may contain non-ASCII em-dashes and the
# Windows default (cp1252) chokes on them.
BUILD_YML = yaml.safe_load((_REPO_ROOT / ".github/workflows/build_site.yml").read_text(encoding="utf-8"))
SCRAPE_YML = yaml.safe_load((_REPO_ROOT / ".github/workflows/daily_scrape.yml").read_text(encoding="utf-8"))
MACD_YML = yaml.safe_load((_REPO_ROOT / ".github/workflows/macd_momentum_alerts.yml").read_text(encoding="utf-8"))


def _build_steps() -> list:
    """Return the list of steps from the build job in build_site.yml."""
    return BUILD_YML["jobs"]["build"]["steps"]


def _scrape_steps() -> list:
    """Return the list of steps from the scrape-and-commit job in daily_scrape.yml."""
    return SCRAPE_YML["jobs"]["scrape-and-commit"]["steps"]


def _find_step(steps: list, name_fragment: str) -> dict | None:
    """Return the first step whose name contains name_fragment (case-insensitive)."""
    fragment_lower = name_fragment.lower()
    for step in steps:
        if step.get("name", "").lower().find(fragment_lower) != -1:
            return step
    return None


# ---------------------------------------------------------------------------
# Tests 1–3: FRED_API_KEY env vars
# ---------------------------------------------------------------------------

def test_markets_history_step_has_fred_key():
    """Req 1.1 — The markets_history --full-refresh step must expose FRED_API_KEY.

    Step name: '2/2 — Live FRED + yfinance merge (current months, fx, cpi, rates)'
    """
    step = _find_step(_build_steps(), "2/2")
    assert step is not None, "Could not find the '2/2 — Live FRED + yfinance merge' step"
    env = step.get("env", {})
    assert "FRED_API_KEY" in env, (
        f"Step '{step['name']}' is missing env.FRED_API_KEY; env block = {env}"
    )


def test_vol_history_step_has_fred_key():
    """Req 1.2 — The vol_history --full-refresh step must expose FRED_API_KEY.

    Step name: 'Fetch vol history (CBOE indices from FRED)'
    """
    step = _find_step(_build_steps(), "Fetch vol history")
    assert step is not None, "Could not find the 'Fetch vol history' step"
    env = step.get("env", {})
    assert "FRED_API_KEY" in env, (
        f"Step '{step['name']}' is missing env.FRED_API_KEY; env block = {env}"
    )


def test_fetch_fred_step_has_fred_key():
    """Req 1.3 — The fetch_fred step must expose FRED_API_KEY.

    Step name: 'Fetch FRED data (price log)'
    """
    step = _find_step(_build_steps(), "Fetch FRED data")
    assert step is not None, "Could not find the 'Fetch FRED data' step"
    env = step.get("env", {})
    assert "FRED_API_KEY" in env, (
        f"Step '{step['name']}' is missing env.FRED_API_KEY; env block = {env}"
    )


# ---------------------------------------------------------------------------
# Tests 4–6: continue-on-error policy
# ---------------------------------------------------------------------------

def test_external_steps_have_continue_on_error():
    """Reqs 4.1–4.5 — All five external-data steps must carry continue-on-error: true.

    The five steps are:
      - 1/2 — Markets Excel deep-history seed (backfill only)
      - 2/2 — Live FRED + yfinance merge (current months, fx, cpi, rates)
      - Fetch market data (yfinance price log)
      - Fetch FRED data (price log)
      - Fetch vol history (CBOE indices from FRED)
    """
    external_step_fragments = [
        "1/2",                  # ingest_markets_xl
        "2/2",                  # markets_history --full-refresh
        "Fetch market data",    # markets.fetch_yf
        "Fetch FRED data",      # markets.fetch_fred
        "Fetch vol history",    # vol_history --full-refresh
    ]
    steps = _build_steps()
    for fragment in external_step_fragments:
        step = _find_step(steps, fragment)
        assert step is not None, f"Could not find external step matching '{fragment}'"
        assert step.get("continue-on-error") is True, (
            f"Step '{step['name']}' is missing continue-on-error: true"
        )


def test_build_step_no_continue_on_error():
    """Reqs 7.4, 7.5 — The 'Build site artifacts' step must NOT carry continue-on-error.

    Step name: 'Build site artifacts (conviction)'
    """
    step = _find_step(_build_steps(), "Build site artifacts")
    assert step is not None, "Could not find the 'Build site artifacts' step"
    # continue-on-error must be absent or explicitly False
    coe = step.get("continue-on-error")
    assert coe is not True, (
        f"Step '{step['name']}' must NOT have continue-on-error: true, but found: {coe}"
    )


def test_run_tests_step_no_continue_on_error():
    """Reqs 7.4, 7.5 — The 'Run tests' step must NOT carry continue-on-error.

    Step name: 'Run tests'
    """
    step = _find_step(_build_steps(), "Run tests")
    assert step is not None, "Could not find the 'Run tests' step"
    coe = step.get("continue-on-error")
    assert coe is not True, (
        f"Step '{step['name']}' must NOT have continue-on-error: true, but found: {coe}"
    )


# ---------------------------------------------------------------------------
# Tests 7–9: Trigger and git configuration
# ---------------------------------------------------------------------------

def test_daily_scrape_has_cron_schedule():
    """Req 3.7 — daily_scrape.yml must have a schedule trigger with at least one cron entry.

    Note: yaml.safe_load parses the YAML 'on:' key as the Python boolean True,
    so we look up SCRAPE_YML[True] rather than SCRAPE_YML["on"].
    """
    # 'on' is a YAML boolean alias — yaml.safe_load maps it to Python True
    on_block = SCRAPE_YML.get(True) or SCRAPE_YML.get("on") or {}
    schedule = on_block.get("schedule")
    assert schedule is not None, "daily_scrape.yml is missing a 'schedule' trigger"
    assert isinstance(schedule, list) and len(schedule) >= 1, (
        f"schedule trigger must have at least one cron entry; got: {schedule}"
    )
    # Each entry must have a 'cron' key
    for entry in schedule:
        assert "cron" in entry, f"Schedule entry is missing 'cron' key: {entry}"


def test_build_site_trigger_and_reusable_contract():
    """Build supports push/manual triggers and the flow promotion workflow_call."""
    on_block = BUILD_YML.get(True) or BUILD_YML.get("on") or {}
    assert "workflow_run" not in on_block, (
        "build_site.yml must not use obsolete workflow_run chaining; "
        f"found triggers: {list(on_block.keys())}"
    )
    push = on_block.get("push")
    assert isinstance(push, dict) and "paths" in push, (
        "build_site.yml must trigger on push with a paths filter"
    )
    assert "workflow_dispatch" in on_block, "manual dispatch must remain for hotfixes"
    assert "workflow_call" in on_block, "flow promotion needs a reusable workflow_call trigger"


def _on_block(yml: dict) -> dict:
    return yml.get(True) or yml.get("on") or {}


def test_workflows_serialize_with_concurrency_groups():
    """2026-08 hardening — both workflows MUST define concurrency groups and
    MUST NOT cancel in progress: overlapping scrapes die on binary-parquet
    rebase conflicts, and cancelled builds lose the stock-detail coverage
    commit-back between commit and push."""
    for name, yml in (("build_site", BUILD_YML), ("daily_scrape", SCRAPE_YML)):
        conc = yml.get("concurrency")
        assert isinstance(conc, dict), f"{name}.yml is missing a concurrency group"
        assert conc.get("group"), f"{name}.yml concurrency group has no name"
        assert conc.get("cancel-in-progress") is False, (
            f"{name}.yml must set cancel-in-progress: false (queue, don't cancel)"
        )
    # Distinct groups — a scrape and a build must be able to run concurrently.
    assert BUILD_YML["concurrency"]["group"] != SCRAPE_YML["concurrency"]["group"]


def test_every_job_has_timeout_minutes():
    """2026-08 hardening — hung fetches must not burn the 360-min default."""
    for name, yml in (("build_site", BUILD_YML), ("daily_scrape", SCRAPE_YML)):
        for job_name, job in yml.get("jobs", {}).items():
            tm = job.get("timeout-minutes")
            assert isinstance(tm, int) and 0 < tm <= 120, (
                f"{name}.yml job '{job_name}' missing sane timeout-minutes"
            )


def test_checkout_enables_lfs_for_markets_workbook():
    """data/Mega_Markets_Historical.xlsx lives in Git LFS; with checkout's
    default lfs:false CI ingests a pointer file and the deep-history seed
    fails silently under continue-on-error."""
    steps = _build_steps()
    checkout = next(s for s in steps if str(s.get("uses", "")).startswith("actions/checkout"))
    assert checkout.get("with", {}).get("lfs") is True, (
        "build_site.yml checkout must set lfs: true"
    )


def test_git_pull_rebase_present():
    """Req 7.2 — daily_scrape.yml must contain 'git pull --rebase' in a run step."""
    steps = _scrape_steps()
    found = False
    for step in steps:
        run_block = step.get("run", "")
        if "git pull --rebase" in run_block:
            found = True
            break
    assert found, (
        "No step in daily_scrape.yml contains 'git pull --rebase'. "
        "This is required to avoid push conflicts when the scraper runs concurrently."
    )


def test_cache_conviction_includes_history_and_artifacts():
    """Ensure build_site.yml cache_conviction caches docs/data/history and key outputs
    so fast builds do not deploy stale history or missing data."""
    step = _find_step(_build_steps(), "Cache conviction artifacts")
    assert step is not None, "Cache conviction artifacts step missing"
    paths = step.get("with", {}).get("path", "")
    assert "docs/data/history" in paths, "docs/data/history must be in cache paths"
    assert "docs/data/leaderboard.json" in paths, "docs/data/leaderboard.json must be in cache paths"
    assert "docs/data/holdings_exits.json" in paths, "docs/data/holdings_exits.json must be in cache paths"


def test_macd_momentum_alerts_workflow_contract():
    """Validate macd_momentum_alerts.yml CI configuration, permissions, and security hygiene."""
    # Permissions: contents: write, actions: write
    perms = MACD_YML.get("permissions", {})
    assert perms.get("contents") == "write", "macd_momentum_alerts.yml must have contents: write"
    assert perms.get("actions") == "write", "macd_momentum_alerts.yml must have actions: write"

    # Concurrency
    conc = MACD_YML.get("concurrency", {})
    assert conc.get("group") == "macd-momentum-alerts", "concurrency group must be macd-momentum-alerts"
    assert conc.get("cancel-in-progress") is False, "cancel-in-progress must be False"

    # Triggers
    on_block = MACD_YML.get(True) or MACD_YML.get("on") or {}
    schedule = on_block.get("schedule", [])
    assert len(schedule) >= 4, f"Expected at least 4 cron schedules, found {len(schedule)}"
    assert "workflow_dispatch" in on_block, "workflow_dispatch trigger must exist"

    # Timeout
    job = MACD_YML["jobs"]["scan-and-alert"]
    tm = job.get("timeout-minutes")
    assert isinstance(tm, int) and 0 < tm <= 30, f"Invalid timeout-minutes: {tm}"

    # Secrets mapping (strictly via secrets context)
    steps = job["steps"]
    scanner_step = next(s for s in steps if "MACD Momentum Scanner" in s.get("name", ""))
    env = scanner_step.get("env", {})
    required_secrets = [
        "ALERT_EMAIL_SENDER",
        "ALERT_EMAIL_PASSWORD",
        "ALERT_EMAIL_RECIPIENT",
        "ALERT_SMTP_HOST",
        "ALERT_SMTP_PORT",
    ]
    for sec in required_secrets:
        assert sec in env, f"Missing secret mapping for {sec}"
        assert "secrets." in env[sec], f"Secret {sec} must reference secrets context"

    # Trigger chaining to build_site.yml
    chain_step = next((s for s in steps if "Trigger site deployment" in s.get("name", "")), None)
    assert chain_step is not None, "Must have step to trigger build_site.yml"
    assert "gh workflow run build_site.yml" in chain_step.get("run", "")
