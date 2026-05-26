"""Capture the pytest pass-set baseline.

Run BEFORE the fix:
    python -m tests.markets_partial_year_live_merge.capture_pytest_baseline

This runs the full `tests/` suite and writes the list of nodeids that
PASS into `baselines/pytest_pass_set.txt`. Test 2i later asserts the same
set still passes after the fix (with our own preservation tests excluded
since they didn't exist on unfixed code).

We use --no-header --no-summary -p no:cacheprovider for a stable, parseable
output. Failures are tolerated — we only record passes.
"""
from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

BASELINES = Path(__file__).parent / "baselines"
SELF_PKG = "tests/markets_partial_year_live_merge"  # exclude our own tests


def main() -> None:
    BASELINES.mkdir(parents=True, exist_ok=True)
    out_path = BASELINES / "pytest_pass_set.txt"

    # Run the suite (verbose, no traceback to keep output small) but ignore
    # our own preservation package since it didn't exist on the original
    # code base — it is what we're adding.
    cmd = [
        sys.executable, "-m", "pytest", "tests/",
        "-v", "--tb=no", "--no-header",
        "-p", "no:cacheprovider",
        "--ignore", SELF_PKG,
    ]
    print("running:", " ".join(cmd))
    proc = subprocess.run(cmd, capture_output=True, text=True, cwd=Path(__file__).resolve().parents[2])
    output = proc.stdout + proc.stderr

    # Pytest -v lines look like:  "tests/foo.py::TestX::test_y PASSED  [ 12%]"
    # Capture the nodeid before " PASSED".
    pass_re = re.compile(r"^(tests/\S+) PASSED", re.MULTILINE)
    passes = sorted(set(pass_re.findall(output)))

    out_path.write_text("\n".join(passes) + "\n", encoding="utf-8")
    print(f"  captured {len(passes)} passing tests → {out_path}")
    # also report failures so the operator knows the baseline state
    fail_re = re.compile(r"^(tests/\S+) FAILED", re.MULTILINE)
    failures = sorted(set(fail_re.findall(output)))
    if failures:
        print(f"  baseline failures (will be allowed to stay red): {len(failures)}")
        for f in failures:
            print(f"    {f}")


if __name__ == "__main__":
    main()
