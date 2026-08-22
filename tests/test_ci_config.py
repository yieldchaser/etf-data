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
BUILD_YML = yaml.safe_load((_REPO_ROOT / ".github/workflows/build_site.yml").read_text())
SCRAPE_YML = yaml.safe_load((_REPO_ROOT / ".github/workflows/daily_scrape.yml").read_text())


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


def test_build_site_single_trigger_chain():
    """Req 3.8 (rev 2026-08) — build_site.yml must have EXACTLY ONE automated
    trigger chain: push-to-main with site/data paths. workflow_run was removed
    (it fired on no-change scrape days and raced the push-path run into the
    concurrency group, cancelling real builds); the explicit dispatch step in
    daily_scrape.yml was removed with it. workflow_dispatch stays for manual
    hotfixes. The coverage commit-back carries [skip ci] so it never re-triggers.

    Note: yaml.safe_load parses the YAML 'on:' key as Python boolean True.
    """
    on_block = BUILD_YML.get(True) or BUILD_YML.get("on") or {}
    assert "workflow_run" not in on_block, (
        "build_site.yml must NOT use a workflow_run trigger anymore "
        f"(single push-paths chain policy); found triggers: {list(on_block.keys())}"
    )
    push = on_block.get("push")
    assert isinstance(push, dict) and "paths" in push, (
        "build_site.yml must trigger on push with a paths filter"
    )
    assert "workflow_dispatch" in on_block, "manual dispatch must remain for hotfixes"


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
