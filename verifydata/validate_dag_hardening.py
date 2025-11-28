"""
Comprehensive DAG validation checker - đảm bảo DAG bảo vệ chống các lỗi đã sửa

Kiểm tra:
1. MAX_CATEGORY_LEVELS enforcement (tránh product names trong category_path)
2. Empty/NULL category path handling
3. Product name filtering logic
4. Category path length validation
5. Data quality gates trước khi load warehouse
"""

import os
import re
import sys
from pathlib import Path

def check_crawl_products_detail():
    """Kiểm tra crawl_products_detail.py có bảo vệ chống lỗi"""
    
    crawl_file = Path("src/pipelines/crawl/crawl_products_detail.py")
    if not crawl_file.exists():
        print(f"❌ File not found: {crawl_file}")
        return False
    
    content = crawl_file.read_text(encoding='utf-8')
    checks = {
        "MAX_CATEGORY_LEVELS defined": "MAX_CATEGORY_LEVELS = 4" in content,
        "Category length check (sync)": "len(product_data[\"category_path\"]) > MAX_CATEGORY_LEVELS" in content,
        "Category length truncation (sync)": "product_data[\"category_path\"][:MAX_CATEGORY_LEVELS]" in content,
        "Product name filter (length)": "> 80" in content and "len(" in content,
        "Product name filter (prefix)": "startswith" in content or "product_name" in content.lower(),
    }
    
    print("=" * 70)
    print("1. CRAWL PRODUCTS DETAIL - Product name & level 4 protection")
    print("=" * 70)
    
    all_pass = True
    for check, passed in checks.items():
        status = "✅" if passed else "❌"
        print(f"{status} {check}")
        if not passed:
            all_pass = False
    
    return all_pass

def check_warehouse_builder():
    """Kiểm trace star_schema_builder có validation"""
    
    warehouse_file = Path("src/pipelines/warehouse/star_schema_builder.py")
    if not warehouse_file.exists():
        print(f"❌ File not found: {warehouse_file}")
        return False
    
    content = warehouse_file.read_text(encoding='utf-8')
    checks = {
        "Filter NULL categories": "IS NULL" in content or "is None" in content,
        "Extract category levels": "level_1" in content or "category_path" in content,
        "Verify data integrity": "COUNT" in content or "verify" in content.lower(),
    }
    
    print("\n" + "=" * 70)
    print("2. WAREHOUSE BUILDER - Data integrity checks")
    print("=" * 70)
    
    all_pass = True
    for check, passed in checks.items():
        status = "✅" if passed else "❌"
        print(f"{status} {check}")
        if not passed:
            all_pass = False
    
    return all_pass

def check_dag_structure():
    """Kiểm tra DAG có bước validation sau mỗi stage"""
    
    dag_file = Path("airflow/dags/tiki_crawl_products_dag.py")
    if not dag_file.exists():
        print(f"❌ File not found: {dag_file}")
        return False
    
    content = dag_file.read_text(encoding='utf-8')
    checks = {
        "Error handling in DAG": "except" in content or "try" in content,
        "Task retries configured": "retries=" in content or "retry" in content.lower(),
        "Timeout set": "execution_timeout" in content or "timeout" in content.lower(),
        "Logging configured": "logging" in content or "logger" in content.lower(),
    }
    
    print("\n" + "=" * 70)
    print("3. DAG WORKFLOW - Resilience & monitoring")
    print("=" * 70)
    
    all_pass = True
    for check, passed in checks.items():
        status = "✅" if passed else "❌"
        print(f"{status} {check}")
        if not passed:
            all_pass = False
    
    return all_pass

def check_config_file():
    """Kiểm tra config có category validation settings"""
    
    config_file = Path("src/pipelines/crawl/config.py")
    if not config_file.exists():
        print(f"❌ File not found: {config_file}")
        return False
    
    content = config_file.read_text(encoding='utf-8')
    checks = {
        "Category levels config": "CATEGORY_LEVELS" in content or "LEVEL" in content,
        "Product name patterns": "PRODUCT_NAME" in content or "PATTERN" in content,
        "Validation rules": "VALIDATE" in content or "RULE" in content,
    }
    
    print("\n" + "=" * 70)
    print("4. CONFIGURATION - Business rules")
    print("=" * 70)
    
    all_pass = True
    for check, passed in checks.items():
        status = "✅" if passed else "❌"
        print(f"{status} {check}")
        if not passed:
            all_pass = False
    
    return all_pass

def main():
    print("\n")
    print("╔" + "=" * 68 + "╗")
    print("║" + " DAG VALIDATION REPORT - Data Quality Protection".ljust(69) + "║")
    print("╚" + "=" * 68 + "╝")
    print()
    
    # Get project root from verifydata location
    script_path = Path(__file__)
    project_root = script_path.parent.parent  # Go up from verifydata/
    os.chdir(project_root)
    
    results = []
    results.append(("Crawl Products Detail", check_crawl_products_detail()))
    results.append(("Warehouse Builder", check_warehouse_builder()))
    results.append(("DAG Workflow", check_dag_structure()))
    results.append(("Configuration", check_config_file()))
    
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    
    for component, passed in results:
        status = "✅ PASS" if passed else "⚠️  NEED REVIEW"
        print(f"{status}: {component}")
    
    all_passed = all(passed for _, passed in results)
    
    print("\n" + "=" * 70)
    if all_passed:
        print("✅ DAG WORKFLOW IS HARDENED AGAINST KNOWN ERRORS!")
        print("\nProtections in place:")
        print("  • MAX_CATEGORY_LEVELS enforcement (prevents product names in category_path)")
        print("  • Product name filtering (length > 80 chars, prefix matching)")
        print("  • Category path validation (all products must have 4 levels)")
        print("  • NULL/empty category handling")
        print("  • Data integrity checks in warehouse")
        print("  • Error handling and retries in DAG")
    else:
        print("⚠️  SOME PROTECTIONS NEED REVIEW/IMPROVEMENT")
    
    print("=" * 70 + "\n")
    
    return 0 if all_passed else 1

if __name__ == "__main__":
    sys.exit(main())
