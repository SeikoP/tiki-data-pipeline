"""Test task startup time để tìm bottleneck"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


def _measure_import_times():
    """Measure import time của các modules chính"""
    print("\n" + "=" * 70)
    print("TEST: Import Times")
    print("=" * 70)

    modules = [
        ("json", "import json"),
        ("logging", "import logging"),
        ("time", "import time"),
        ("requests", "import requests"),
        ("redis", "import redis"),
        ("psycopg2", "import psycopg2"),
        ("aiohttp", "import aiohttp"),
        ("crawl_products", "from pipelines.crawl.crawl_products import crawl_category_products"),
        (
            "crawl_products_detail",
            "from pipelines.crawl.crawl_products_detail import crawl_product_detail_with_selenium",
        ),
        ("redis_cache", "from pipelines.crawl.storage.redis_cache import get_redis_cache"),
        (
            "adaptive_rate_limiter",
            "from pipelines.crawl.storage.adaptive_rate_limiter import get_adaptive_rate_limiter",
        ),
    ]

    results = []
    for name, import_stmt in modules:
        start = time.time()
        try:
            exec(import_stmt)
            duration = time.time() - start
            results.append((name, duration, "OK"))
            print(f"  {name:30s}: {duration:6.3f}s")
        except Exception as e:
            duration = time.time() - start
            results.append((name, duration, f"ERROR: {e}"))
            print(f"  {name:30s}: {duration:6.3f}s (ERROR: {e})")

    # Summary
    print("\n" + "-" * 70)
    total = sum(r[1] for r in results if r[2] == "OK")
    print(f"Total import time: {total:.3f}s")

    # Slowest imports
    print("\nSlowest imports:")
    sorted_results = sorted(results, key=lambda x: x[1], reverse=True)
    for name, duration, status in sorted_results[:5]:
        if status == "OK":
            print(f"  {name:30s}: {duration:6.3f}s")

    return results


def test_import_times():
    """Test import time của các modules chính"""
    results = _measure_import_times()
    assert results


def _measure_bootstrap_import():
    """Measure import time của bootstrap module (DAG file)"""
    print("\n" + "=" * 70)
    print("TEST: Bootstrap Import Time")
    print("=" * 70)

    # Add DAG path
    dag_path = Path(__file__).parent.parent / "airflow" / "dags"
    if str(dag_path) not in sys.path:
        sys.path.insert(0, str(dag_path))

    start = time.time()
    try:
        from tiki_crawl_products_v2 import bootstrap

        duration = time.time() - start
        print(f"  Bootstrap import: {duration:.3f}s")

        # Check what's in bootstrap
        attrs = [a for a in dir(bootstrap) if not a.startswith("_")]
        print(f"  Exported attributes: {len(attrs)}")
        print(f"  Sample: {', '.join(attrs[:10])}")

        return duration
    except Exception as e:
        duration = time.time() - start
        print(f"  Bootstrap import: {duration:.3f}s (ERROR: {e})")
        return duration


def test_bootstrap_import():
    """Test import time của bootstrap module (DAG file)"""
    duration = _measure_bootstrap_import()
    assert duration >= 0


def _measure_task_function_call():
    """Measure thời gian gọi task function"""
    print("\n" + "=" * 70)
    print("TEST: Task Function Call Time")
    print("=" * 70)

    # Simulate task context
    context = {
        "ti": type("TaskInstance", (), {"xcom_pull": lambda *args, **kwargs: None})(),
        "dag_run": type("DagRun", (), {"conf": {}})(),
    }

    # Test load_categories (lightweight task)
    start = time.time()
    try:
        dag_path = Path(__file__).parent.parent / "airflow" / "dags"
        if str(dag_path) not in sys.path:
            sys.path.insert(0, str(dag_path))

        from tiki_crawl_products_v2.tasks.category import load_categories

        # Call function
        result = load_categories(**context)
        duration = time.time() - start

        print(f"  load_categories: {duration:.3f}s")
        if result:
            print(f"  Result: {len(result)} categories")

        return duration
    except Exception as e:
        duration = time.time() - start
        print(f"  load_categories: {duration:.3f}s (ERROR: {e})")
        return duration


def test_task_function_call():
    """Test thời gian gọi task function"""
    duration = _measure_task_function_call()
    assert duration >= 0


if __name__ == "__main__":
    print("\nStarting Task Startup Time Tests...\n")

    # Test 1: Import times
    import_results = _measure_import_times()

    # Test 2: Bootstrap import
    bootstrap_time = _measure_bootstrap_import()

    # Test 3: Task function call
    task_time = _measure_task_function_call()

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)

    total_import = sum(r[1] for r in import_results if r[2] == "OK")
    print(f"Total module imports: {total_import:.3f}s")
    print(f"Bootstrap import: {bootstrap_time:.3f}s")
    print(f"Task function call: {task_time:.3f}s")
    print(f"\nEstimated task startup: {total_import + bootstrap_time:.3f}s")

    # Recommendations
    print("\n" + "=" * 70)
    print("RECOMMENDATIONS")
    print("=" * 70)

    slow_imports = [r for r in import_results if r[2] == "OK" and r[1] > 0.5]
    if slow_imports:
        print("\nSlow imports (>0.5s):")
        for name, duration, _ in slow_imports:
            print(f"  - {name}: {duration:.3f}s")
            print("    -> Consider lazy import in task functions")
    else:
        print("\nNo slow imports detected (all <0.5s)")

    if bootstrap_time > 1.0:
        print(f"\nBootstrap import is slow ({bootstrap_time:.3f}s)")
        print("  -> Check bootstrap.py for heavy imports at module level")
    else:
        print(f"\nBootstrap import is acceptable ({bootstrap_time:.3f}s)")

    print("\n" + "=" * 70)
