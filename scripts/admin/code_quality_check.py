#!/usr/bin/env python
"""
Code quality check and auto-fix script.
"""

import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent
SRC_PATHS = ["src", "tests", "airflow/dags"]


def run_command(cmd: list[str], description: str) -> bool:
    """
    Run command and show result.
    """
    print(f"\n{'=' * 60}")
    print(f"🔍 {description}")
    print(f"{'=' * 60}")
    result = subprocess.run(cmd, cwd=PROJECT_ROOT)
    return result.returncode == 0


def main():
    print("\n" + "=" * 60)
    print("📊 CODE QUALITY & FORMAT CHECK")
    print("=" * 60)

    # 1. Format with black and isort
    print("\n1️⃣  AUTO-FORMAT WITH BLACK & ISORT")
    print("-" * 60)

    for path in SRC_PATHS:
        cmd = ["black", path]
        if run_command(cmd, f"Format {path} with black"):
            print(f"✅ black {path}: OK")
        else:
            print(f"⚠️  black {path}: skipped or failed")

    for path in SRC_PATHS:
        cmd = ["isort", path]
        if run_command(cmd, f"Sort imports in {path}"):
            print(f"✅ isort {path}: OK")
        else:
            print(f"⚠️  isort {path}: skipped or failed")

    # 2. Check format
    print("\n2️⃣  CHECK FORMAT (after auto-format)")
    print("-" * 60)
    format_ok = True
    for path in SRC_PATHS:
        cmd = ["black", "--check", path]
        if not run_command(cmd, f"Check black format in {path}"):
            format_ok = False

    # 3. Lint with ruff
    print("\n3️⃣  LINT WITH RUFF")
    print("-" * 60)
    lint_ok = run_command(["ruff", "check", *SRC_PATHS], "Run ruff")

    # 4. Type check
    print("\n4️⃣  TYPE CHECK WITH MYPY")
    print("-" * 60)
    type_ok = run_command(["mypy", "src", "--ignore-missing-imports"], "Type check")

    # 5. Performance checks
    print("\n5️⃣  PERFORMANCE CHECK WITH RUFF")
    print("-" * 60)
    perf_ok = run_command(["ruff", "check", "--select", "PERF", *SRC_PATHS], "Performance check")

    # 6. Dead code
    print("\n6️⃣  FIND DEAD CODE WITH VULTURE")
    print("-" * 60)
    vulture_ok = run_command(
        ["vulture", "src", "airflow/dags", "--min-confidence", "80"], "Find dead code"
    )

    # Summary
    print("\n" + "=" * 60)
    print("📋 SUMMARY")
    print("=" * 60)
    print(f"✅ Format:       {'PASS' if format_ok else 'FAIL'}")
    print(f"✅ Linting:      {'PASS' if lint_ok else 'FAIL'}")
    print(f"✅ Type check:   {'PASS' if type_ok else 'FAIL'}")
    print(f"✅ Performance:  {'PASS' if perf_ok else 'FAIL'}")
    print(f"✅ Dead code:    {'PASS' if vulture_ok else 'FAIL'}")

    all_pass = format_ok and lint_ok and type_ok and perf_ok and vulture_ok

    if all_pass:
        print("\n" + "=" * 60)
        print("✅ ALL CHECKS PASSED!")
        print("=" * 60)
        return 0
    else:
        print("\n" + "=" * 60)
        print("⚠️  SOME CHECKS FAILED - SEE DETAILS ABOVE")
        print("=" * 60)
        return 1


if __name__ == "__main__":
    sys.exit(main())
