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


INTEGRATION_TEST_FILES = {
    "test_final_redis_cache.py",
    "test_full_redis_flow.py",
    "test_redis_cache_fix.py",
    "test_redis_connection.py",
    "test_redis_priority.py",
    "test_redis_working.py",
    "test_sales_count.py",
}

INTEGRATION_TEST_NAMES = {
    "test_crawl_single_category",
    "test_crawl_multiple_categories",
    "test_get_page",
}

SLOW_TEST_FILES = {
    "test_sales_count.py",
}

SLOW_TEST_NAMES = {
    "test_crawl_single_category",
    "test_crawl_multiple_categories",
    "test_get_page",
}


def pytest_collection_modifyitems(config, items):
    """Separate service/live-network tests from the default unit suite."""
    import pytest

    for item in items:
        filename = item.path.name
        if filename in INTEGRATION_TEST_FILES or item.name in INTEGRATION_TEST_NAMES:
            item.add_marker(pytest.mark.integration)
        if filename in SLOW_TEST_FILES or item.name in SLOW_TEST_NAMES:
            item.add_marker(pytest.mark.slow)
