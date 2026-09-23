"""Run the midflight test suite.

    python3 tests/run_tests.py                 # test this checkout
    python3 tests/run_tests.py . ../midflight-1.2.8   # compare two checkouts

Requires nothing but Python 3.11+ — ``tests/core/`` stubs the small part of
KiraAI the plugin touches, so no framework install and no web UI are needed.
Exit code is non-zero if any check fails.
"""
import subprocess
import sys
from pathlib import Path

TESTS = Path(__file__).resolve().parent

SUITES = ["test_phantom_run.py", "test_scenarios.py", "test_stuck_paths.py",
          "test_foreign_event.py", "test_system_event.py", "test_inject_count.py"]


def main():
    dirs = sys.argv[1:] or [str(TESTS.parent)]
    rc = 0
    for suite in SUITES:
        print(f"\n########## {suite} ##########")
        r = subprocess.run([sys.executable, str(TESTS / suite), *dirs])
        rc = rc or r.returncode
    print("\n" + ("ALL TESTS PASSED" if rc == 0 else "SOME TESTS FAILED"))
    return rc


if __name__ == "__main__":
    sys.exit(main())
