#!/usr/bin/env python
"""
Quick sanity check before DAG run
Run this before triggering DAG to catch early issues
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))


def quick_check():
    print("\n" + "=" * 60)
    print("QUICK SANITY CHECK")
    print("=" * 60)

    errors = []
    warnings = []

    # 1. Check imports
    print("\n1️⃣  Checking imports...")
    try:
        from src.pipelines.crawl.crawl_products_detail import (
            extract_product_detail,
            load_category_hierarchy,
        )

        print("   ✅ crawl_products_detail imports OK")
    except Exception as e:
        errors.append(f"Import error: {e}")

    # 2. Check hierarchy map
    print("2️⃣  Checking hierarchy map...")
    try:
        hierarchy_map = load_category_hierarchy()
        if not hierarchy_map:
            warnings.append("Hierarchy map is empty")
        else:
            print(f"   ✅ Hierarchy map loaded: {len(hierarchy_map)} entries")
    except Exception as e:
        errors.append(f"Hierarchy map error: {e}")

    # 3. Test extract function
    print("3️⃣  Testing extract function...")
    try:
        test_html = """
        <html><body>
            <h1 data-view-id="pdp_product_name">Test</h1>
            <div data-view-id="pdp_breadcrumb">
                <a href="#">Nhà Cửa - Đời Sống</a>
                <a href="#">Trang trí nhà cửa</a>
                <a href="#">Tranh trang trí</a>
            </div>
            <div data-view-id="pdp_product_price">99,999</div>
            <script id="__NEXT_DATA__" type="application/json">{}</script>
        </body></html>
        """
        result = extract_product_detail(test_html, "https://tiki.vn/test/123", verbose=False)

        if not isinstance(result, dict):
            errors.append(f"extract_product_detail returned {type(result)}")
        elif not result.get("category_path"):
            warnings.append("extract_product_detail returned empty category_path")
        else:
            print(
                f"   ✅ Extract function OK (got {len(result.get('category_path', []))} category levels)"
            )
    except Exception as e:
        errors.append(f"Extract test error: {e}")

    # 4. Check DB (optional)
    print("4️⃣  Checking database...")
    try:
        import psycopg2

        try:
            conn = psycopg2.connect(
                host="localhost",
                database="crawl_data",
                user="postgres",
                password="postgres",
                port=5432,
            )
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM products WHERE category_path IS NOT NULL")
            count = cur.fetchone()[0]
            print(f"   ✅ Database OK: {count} products with category_path")
            cur.close()
            conn.close()
        except Exception as db_err:
            warnings.append(f"DB check failed (will work if Airflow can connect): {db_err}")
    except ImportError:
        warnings.append("psycopg2 not available (OK if in Docker)")

    # Summary
    print("\n" + "=" * 60)
    if errors:
        print("❌ ERRORS FOUND:")
        for error in errors:
            print(f"   • {error}")
        return 1
    elif warnings:
        print("⚠️  WARNINGS:")
        for warning in warnings:
            print(f"   • {warning}")
    else:
        print("✅ ALL CHECKS PASSED - READY TO RUN DAG")

    print("=" * 60 + "\n")
    return 0 if not errors else 1


if __name__ == "__main__":
    sys.exit(quick_check())
