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
    # Test basic imports
    from tiki_crawl_products_v2.bootstrap import (
        Any,
        Path,
        json,
        logging,
        os,
        re,
        shutil,
        sys,
        time,
        datetime,
    )
    print("✅ Basic imports successful")
except ImportError as e:
    print(f"❌ Basic imports failed: {e}")
    sys.exit(1)

try:
    # Test Airflow class imports
    from tiki_crawl_products_v2.bootstrap import (
        DAG,
        TaskGroup,
        PythonOperator,
        DAG_CONFIG,
    )
    print(f"✅ Airflow class imports successful")
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
        CircuitBreakerOpenError,
        classify_error,
    )
    print("✅ Resilience pattern imports successful")
except ImportError as e:
    print(f"❌ Resilience pattern imports failed: {e}")
    sys.exit(1)

try:
    # Test function imports
    from tiki_crawl_products_v2.bootstrap import (
        atomic_write_file,
        ensure_output_dirs,
        get_int_variable,
        get_logger,
        get_tiki_circuit_breaker,
        get_tiki_degradation,
        get_tiki_dlq,
        get_variable,
    )
    print("✅ Function imports successful")
except ImportError as e:
    print(f"❌ Function imports failed: {e}")
    sys.exit(1)

try:
    # Test crawl function imports
    from tiki_crawl_products_v2.bootstrap import (
        crawl_category_products,
        crawl_product_detail_with_driver,
        crawl_product_detail_with_selenium,
        extract_product_detail,
    )
    print("✅ Crawl function imports successful")
except ImportError as e:
    print(f"❌ Crawl function imports failed: {e}")
    sys.exit(1)

try:
    # Test path imports
    from tiki_crawl_products_v2.bootstrap import (
        CACHE_DIR,
        CATEGORIES_FILE,
        DETAIL_CACHE_DIR,
        OUTPUT_FILE,
        OUTPUT_FILE_WITH_DETAIL,
        PROGRESS_FILE,
    )
    print("✅ Path imports successful")
except ImportError as e:
    print(f"❌ Path imports failed: {e}")
    sys.exit(1)

print("\n🎉 All imports successful!")
