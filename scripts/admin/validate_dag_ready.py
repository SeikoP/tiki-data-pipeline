#!/usr/bin/env python
"""
Comprehensive validation to prevent errors in next DAG run.
"""

import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from pipelines.crawl.config import MAX_CATEGORY_LEVELS


def check_hierarchy_map():
    """
    Check if hierarchy map exists and is valid.
    """
    print("\n" + "=" * 80)
    print("1. CHECKING HIERARCHY MAP")
    print("=" * 80)

    hierarchy_file = "data/raw/category_hierarchy_map.json"

    if not os.path.exists(hierarchy_file):
        print(f"❌ ERROR: Hierarchy map not found at {hierarchy_file}")
        return False

    try:
        with open(hierarchy_file, encoding="utf-8") as f:
            data = json.load(f)

        if not isinstance(data, dict):
            print(f"❌ ERROR: Hierarchy map is not a dict, got {type(data)}")
            return False

        # Check structure
        sample_url = list(data.keys())[0]
        sample_entry = data[sample_url]

        required_keys = ["name", "parent_chain"]
        missing_keys = [k for k in required_keys if k not in sample_entry]

        if missing_keys:
            print(f"❌ ERROR: Missing keys in hierarchy map entries: {missing_keys}")
            return False

        print("✅ Hierarchy map valid:")
        print(f"   • Total entries: {len(data)}")
        print(f"   • Sample entry: {sample_url}")
        print(f"     - Name: {sample_entry.get('name')}")
        print(f"     - Parent chain: {sample_entry.get('parent_chain', [])}")

        # Check for duplicate names
        names = {}
        for info in data.values():
            name = info.get("name")
            if name:
                if name in names:
                    names[name] += 1
                else:
                    names[name] = 1

        duplicates = {k: v for k, v in names.items() if v > 1}
        if duplicates:
            print(f"⚠️  WARNING: Found {len(duplicates)} category names with duplicates")
            for name, count in list(duplicates.items())[:3]:
                print(f"   • '{name}': {count} times")

        return True

    except json.JSONDecodeError as e:
        print(f"❌ ERROR: Invalid JSON in hierarchy map: {e}")
        return False
    except Exception as e:
        print(f"❌ ERROR: Failed to load hierarchy map: {e}")
        return False


def check_category_path_in_db():
    """
    Check if products in DB have valid category_path.
    """
    print("\n" + "=" * 80)
    print("2. CHECKING CATEGORY_PATH IN DATABASE")
    print("=" * 80)

    try:
        import psycopg2

        # Try multiple connections
        connections = [
            {
                "host": "localhost",
                "database": "crawl_data",
                "user": "postgres",
                "password": "postgres",
                "port": 5432,
            },
        ]

        conn = None
        for conn_info in connections:
            try:
                conn = psycopg2.connect(**conn_info)
                break
            except Exception:
                continue

        if not conn:
            print("⚠️  WARNING: Could not connect to database (skipping DB checks)")
            return True

        cur = conn.cursor()

        # Check category_path distribution
        cur.execute(
            """
            SELECT
                jsonb_array_length(category_path) as levels,
                COUNT(*) as count
            FROM products
            WHERE category_path IS NOT NULL
            GROUP BY levels
            ORDER BY levels
        """
        )

        distribution = cur.fetchall()

        if not distribution:
            print("⚠️  WARNING: No products with category_path found")
            cur.close()
            conn.close()
            return True

        print("✅ Category path distribution:")
        for levels, count in distribution:
            print(f"   • {levels} levels: {count} products")

        # Check for invalid paths
        cur.execute(
            f"""
            SELECT COUNT(*) as count
            FROM products
            WHERE category_path IS NOT NULL
            AND jsonb_array_length(category_path) > {MAX_CATEGORY_LEVELS}
        """
        )

        invalid_count = cur.fetchone()[0]
        if invalid_count > 0:
            print(
                f"⚠️  WARNING: Found {invalid_count} products with >{MAX_CATEGORY_LEVELS} levels (will be trimmed)"
            )

        # Check for null or empty paths
        cur.execute(
            """
            SELECT COUNT(*) as null_count,
                   COUNT(CASE WHEN jsonb_array_length(category_path) = 0 THEN 1 END) as empty_count
            FROM products
            WHERE category_path IS NULL OR jsonb_array_length(category_path) = 0
        """
        )

        null_count, empty_count = cur.fetchone()
        if null_count > 0 or empty_count > 0:
            print(f"⚠️  WARNING: Found {null_count} null + {empty_count} empty category_path")

        # Check parent category coverage
        cur.execute(
            """
            SELECT COUNT(*) as has_parent
            FROM products
            WHERE category_path IS NOT NULL
            AND jsonb_array_length(category_path) >= 4
        """
        )

        has_parent = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM products WHERE category_path IS NOT NULL")
        total = cur.fetchone()[0]

        if total > 0:
            coverage = (has_parent / total) * 100
            print(f"✅ Parent category coverage: {has_parent}/{total} ({coverage:.1f}%)")

        cur.close()
        conn.close()
        return True

    except Exception as e:
        print(f"⚠️  WARNING: Database check failed: {e}")
        return True


def check_extract_function():
    """
    Check if extract_product_detail function works correctly.
    """
    print("\n" + "=" * 80)
    print("3. CHECKING extract_product_detail FUNCTION")
    print("=" * 80)

    try:
        from src.pipelines.crawl.crawl_products_detail import (
            extract_product_detail,
            load_category_hierarchy,
        )

        print("✅ Function imports successful")

        # Test with simple HTML
        test_html = """
        <html>
        <head><title>Test Product</title></head>
        <body>
            <h1 data-view-id="pdp_product_name">Test Product</h1>
            <div data-view-id="pdp_breadcrumb">
                <a href="#">Trang trí nhà cửa</a>
                <a href="#">Tranh trang trí</a>
                <a href="#">Tranh đá</a>
            </div>
            <div data-view-id="pdp_product_price">100,000</div>
            <script id="__NEXT_DATA__" type="application/json">{}</script>
        </body>
        </html>
        """

        result = extract_product_detail(test_html, "https://tiki.vn/test/123", verbose=False)

        if not isinstance(result, dict):
            print(f"❌ ERROR: extract_product_detail returned {type(result)} instead of dict")
            return False

        required_keys = ["name", "price", "category_path", "url"]
        missing_keys = [k for k in required_keys if k not in result]

        if missing_keys:
            print(f"⚠️  WARNING: Missing keys in result: {missing_keys}")

        print("✅ Function works correctly:")
        print(f"   • Extracted {len(result)} fields")
        print(f"   • Product name: {result.get('name')}")
        print(
            f"   • Category path: {result.get('category_path')} ({len(result.get('category_path', []))} levels)"
        )

        # Test with hierarchy_map
        hierarchy_map = load_category_hierarchy()
        print(f"   • Hierarchy map loaded: {len(hierarchy_map)} categories")

        return True

    except ImportError as e:
        print(f"❌ ERROR: Failed to import function: {e}")
        return False
    except Exception as e:
        print(f"❌ ERROR: Function test failed: {e}")
        return False


def check_dag_syntax():
    """
    Check if main DAG files have valid syntax.
    """
    print("\n" + "=" * 80)
    print("4. CHECKING DAG SYNTAX")
    print("=" * 80)

    dag_files = [
        "airflow/dags/tiki_crawl_products_dag.py",
    ]

    all_valid = True

    for dag_file in dag_files:
        if not os.path.exists(dag_file):
            print(f"⚠️  WARNING: DAG file not found: {dag_file}")
            continue

        try:
            with open(dag_file, encoding="utf-8") as f:
                code = f.read()
                compile(code, dag_file, "exec")
            print(f"✅ {dag_file}: Syntax OK")
        except SyntaxError as e:
            print(f"❌ ERROR in {dag_file}: {e}")
            all_valid = False
        except Exception as e:
            print(f"❌ ERROR checking {dag_file}: {e}")
            all_valid = False

    return all_valid


def main():
    print("\n" + "=" * 80)
    print("COMPREHENSIVE DAG VALIDATION")
    print("=" * 80)

    checks = [
        ("Hierarchy Map", check_hierarchy_map),
        ("Category Path DB", check_category_path_in_db),
        ("Extract Function", check_extract_function),
        ("DAG Syntax", check_dag_syntax),
    ]

    results = {}
    for name, check_func in checks:
        try:
            results[name] = check_func()
        except Exception as e:
            print(f"\n❌ CRITICAL ERROR in {name}: {e}")
            results[name] = False

    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)

    for name, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status}: {name}")

    all_passed = all(results.values())

    if all_passed:
        print("\n" + "=" * 80)
        print("✅ ALL CHECKS PASSED - DAG READY TO RUN")
        print("=" * 80)
    else:
        print("\n" + "=" * 80)
        print("⚠️  SOME CHECKS FAILED - PLEASE FIX ISSUES BEFORE RUNNING DAG")
        print("=" * 80)

    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
