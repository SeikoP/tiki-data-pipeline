"""Pytest collection rules for automated tests.

Manual probes, demos, database smoke checks, and benchmarks live under ``tests/``
for developer convenience, but they are not safe to import during the automated
unit test run because they depend on external services or execute work at import
time.
"""

collect_ignore = [
    "manual",
    "pg",
    "verify.py",
    "test_parallel_benchmark.py",
]

collect_ignore_glob = [
    "manual_*.py",
    "demo_*.py",
    "debug_*.py",
]
