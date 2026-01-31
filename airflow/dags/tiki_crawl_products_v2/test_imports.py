#!/usr/bin/env python3
"""Test script to verify all imports work correctly."""

import sys
from pathlib import Path

# Add src to path
src_path = Path(__file__).parent.parent.parent.parent / "src"
if src_path.exists():
    sys.path.insert(0, str(src_path))

print("Testing imports from bootstrap...")

try:
    # Test basic imports - just verify they exist
    from tiki_crawl_products_v2.bootstrap import (
        Path,
        sys,
    )

    print("✅ Basic imports successful")
except ImportError as e:
    print(f"❌ Basic imports failed: {e}")
    sys.exit(1)

try:
    # Test Airflow class imports
    from tiki_crawl_products_v2.bootstrap import (
        DAG,
        DAG_CONFIG,
        PythonOperator,
        TaskGroup,
    )

    print("✅ Airflow class imports successful")
    print(f"   - DAG: {DAG}")
    print(f"   - TaskGroup: {TaskGroup}")
    print(f"   - PythonOperator: {PythonOperator}")
    print(f"   - DAG_CONFIG: {type(DAG_CONFIG)}")

    if DAG is None:
        print("⚠️  WARNING: DAG is None - this will cause issues!")
    if TaskGroup is None:
        print("⚠️  WARNING: TaskGroup is None - this will cause issues!")
    if PythonOperator is None:
        print("⚠️  WARNING: PythonOperator is None - this will cause issues!")
    if DAG_CONFIG is None:
        print("⚠️  WARNING: DAG_CONFIG is None - this will cause issues!")

except ImportError as e:
    print(f"❌ Airflow class imports failed: {e}")
    sys.exit(1)

try:
    # Test resilience pattern imports
    from tiki_crawl_products_v2.bootstrap import (
        CircuitBreakerOpenError,  # noqa: F401
        classify_error,  # noqa: F401
    )

    print("✅ Resilience pattern imports successful")
except ImportError as e:
    print(f"❌ Resilience pattern imports failed: {e}")
    sys.exit(1)

try:
    # Test function imports
    from tiki_crawl_products_v2.bootstrap import (
        atomic_write_file,  # noqa: F401
        ensure_output_dirs,  # noqa: F401
        get_int_variable,  # noqa: F401
        get_logger,  # noqa: F401
        get_tiki_circuit_breaker,  # noqa: F401
        get_tiki_degradation,  # noqa: F401
        get_tiki_dlq,  # noqa: F401
        get_variable,  # noqa: F401
    )

    print("✅ Function imports successful")
except ImportError as e:
    print(f"❌ Function imports failed: {e}")
    sys.exit(1)

try:
    # Test crawl function imports
    from tiki_crawl_products_v2.bootstrap import (
        crawl_category_products,  # noqa: F401
        crawl_product_detail_with_driver,  # noqa: F401
        crawl_product_detail_with_selenium,  # noqa: F401
        extract_product_detail,  # noqa: F401
    )

    print("✅ Crawl function imports successful")
except ImportError as e:
    print(f"❌ Crawl function imports failed: {e}")
    sys.exit(1)

try:
    # Test path imports
    from tiki_crawl_products_v2.bootstrap import (
        CACHE_DIR,  # noqa: F401
        CATEGORIES_FILE,  # noqa: F401
        DETAIL_CACHE_DIR,  # noqa: F401
        OUTPUT_FILE,  # noqa: F401
        OUTPUT_FILE_WITH_DETAIL,  # noqa: F401
        PROGRESS_FILE,  # noqa: F401
    )

    print("✅ Path imports successful")
except ImportError as e:
    print(f"❌ Path imports failed: {e}")
    sys.exit(1)

print("\n🎉 All imports successful!")
