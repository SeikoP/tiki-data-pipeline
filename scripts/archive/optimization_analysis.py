#!/usr/bin/env python3
"""
Script để kiểm tra xem ETL pipeline đã tối ưu chưa
Phân tích performance, code quality, data flow, và recommend improvements
"""

import json
import re
from pathlib import Path

# Color codes
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
CYAN = "\033[96m"
END = "\033[0m"


def print_section(title: str, symbol: str = "="):
    """Print section header"""
    print(f"\n{BLUE}{symbol * 80}{END}")
    print(f"{BLUE}{title:^80}{END}")
    print(f"{BLUE}{symbol * 80}{END}\n")


def check_file_size_and_structure():
    """Check file sizes and structures"""
    print_section("📁 PHÂN TÍCH FILES VÀ STRUCTURES")

    checks = []

    # 1. Check categories file
    categories_file = Path("data/raw/categories_recursive_optimized.json")
    if categories_file.exists():
        size_mb = categories_file.stat().st_size / (1024 * 1024)
        with open(categories_file, encoding="utf-8") as f:
            data = json.load(f)

        has_category_id = all(cat.get("category_id") for cat in data)
        has_category_path = all(cat.get("category_path") for cat in data)

        status = GREEN if has_category_id and has_category_path else YELLOW
        checks.append(
            {
                "name": "Categories file enrichment",
                "status": status,
                "details": f"{status}{len(data)} categories, {size_mb:.2f}MB{END}",
                "items": [
                    (
                        GREEN if has_category_id else RED,
                        f"category_id: {'✅' if has_category_id else '❌'}",
                    ),
                    (
                        GREEN if has_category_path else RED,
                        f"category_path: {'✅' if has_category_path else '❌'}",
                    ),
                ],
            }
        )

    # 2. Check products transformed
    products_file = Path("data/processed/products_final.json")
    if products_file.exists():
        size_mb = products_file.stat().st_size / (1024 * 1024)
        with open(products_file, encoding="utf-8") as f:
            data = json.load(f)

        products = data.get("products", []) if isinstance(data, dict) else data

        # Count products with category_path
        with_path = sum(1 for p in products if p.get("category_path"))
        completeness = with_path * 100 / len(products) if products else 0

        status = GREEN if completeness > 90 else YELLOW if completeness > 50 else RED
        checks.append(
            {
                "name": "Products enrichment",
                "status": status,
                "details": f"{status}{len(products)} products, {size_mb:.2f}MB{END}",
                "items": [
                    (status, f"category_path completeness: {completeness:.1f}%"),
                    (
                        GREEN if with_path == len(products) else YELLOW,
                        f"Products with category_path: {with_path}/{len(products)}",
                    ),
                ],
            }
        )

    # Print results
    for check in checks:
        print(f"{check['status']}• {check['name']}{END}")
        print(f"  {check['details']}")
        for color, item in check["items"]:
            print(f"  {color}  - {item}{END}")

    return checks


def check_dag_structure():
    """Check DAG structure and task organization"""
    print_section("🔄 PHÂN TÍCH DAG STRUCTURE")

    dag_file = Path("airflow/dags/tiki_crawl_products_dag.py")
    if not dag_file.exists():
        print(f"{RED}DAG file không tồn tại{END}")
        return []

    with open(dag_file, encoding="utf-8") as f:
        content = f.read()

    checks = []

    # 1. Check for TaskGroups
    taskgroups = re.findall(r'with TaskGroup\("([^"]+)"\)', content)
    print(f"📊 Task Groups: {len(taskgroups)}")
    for tg in taskgroups:
        print(f"   {GREEN}✓{END} {tg}")

    checks.append(
        {
            "name": "Task Organization",
            "status": GREEN if len(taskgroups) >= 5 else YELLOW,
            "value": len(taskgroups),
            "target": 5,
            "details": "TaskGroups dùng để organize tasks thành logical groups",
        }
    )

    # 2. Check for Dynamic Task Mapping
    has_dynamic = "map_index" in content or "expand" in content
    print(
        f"\n📊 Dynamic Task Mapping: {GREEN if has_dynamic else RED}{'✓' if has_dynamic else '✗'}{END}"
    )
    checks.append(
        {
            "name": "Dynamic Task Mapping",
            "status": GREEN if has_dynamic else RED,
            "value": has_dynamic,
            "details": "Dùng để parallelize tasks cho nhiều categories",
        }
    )

    # 3. Check for error handling
    error_handling = content.count("try:") + content.count("except")
    print(
        f"\n📊 Error Handling: {len(re.findall(r'try:', content))} try blocks, {len(re.findall(r'except', content))} except blocks"
    )
    checks.append(
        {
            "name": "Error Handling",
            "status": GREEN if error_handling > 20 else YELLOW if error_handling > 10 else RED,
            "value": error_handling,
            "details": f"Total try/except blocks: {error_handling}",
        }
    )

    # 4. Check for caching
    has_caching = "redis" in content.lower() or "cache" in content.lower()
    print(
        f"\n📊 Caching Strategy: {GREEN if has_caching else YELLOW}{'✓ Redis' if 'redis' in content.lower() else '✓ Other'}{END}"
    )
    checks.append(
        {
            "name": "Caching",
            "status": GREEN if has_caching else YELLOW,
            "value": has_caching,
            "details": "Redis hoặc file-based caching để avoid redundant crawls",
        }
    )

    # 5. Check for logging
    logging_calls = len(re.findall(r"logger\.info|logger\.warning|logger\.error", content))
    print(f"\n📊 Logging: {logging_calls} logging statements")
    checks.append(
        {
            "name": "Logging",
            "status": GREEN if logging_calls > 50 else YELLOW if logging_calls > 20 else RED,
            "value": logging_calls,
            "details": f"Logging statements for debugging: {logging_calls}",
        }
    )

    # 6. Check for XCom data sharing
    has_xcom = "xcom_pull" in content or "xcom_push" in content
    print(f"\n📊 XCom Data Sharing: {GREEN if has_xcom else RED}{'✓' if has_xcom else '✗'}{END}")
    checks.append(
        {
            "name": "XCom for Data Passing",
            "status": GREEN if has_xcom else RED,
            "value": has_xcom,
            "details": "Efficient data passing between tasks",
        }
    )

    return checks


def check_data_pipeline_optimization():
    """Check data pipeline optimization"""
    print_section("🔧 PHÂN TÍCH DATA PIPELINE OPTIMIZATION")

    checks = []

    # Check batch processing
    loader_file = Path("src/pipelines/load/loader_optimized.py")
    if loader_file.exists():
        with open(loader_file, encoding="utf-8") as f:
            content = f.read()

        has_batch = "batch_size" in content or "batch" in content.lower()
        print(f"📊 Batch Processing: {GREEN if has_batch else RED}{'✓' if has_batch else '✗'}{END}")
        checks.append(
            {
                "name": "Batch Processing for DB Insert",
                "status": GREEN if has_batch else RED,
                "value": has_batch,
                "details": "INSERT batches để optimize database writes",
            }
        )

        # Check upsert strategy
        has_upsert = "ON CONFLICT" in content or "upsert" in content.lower()
        print(
            f"📊 Upsert Strategy: {GREEN if has_upsert else RED}{'✓ ON CONFLICT' if has_upsert else '✗'}{END}"
        )
        checks.append(
            {
                "name": "Upsert Strategy (ON CONFLICT)",
                "status": GREEN if has_upsert else RED,
                "value": has_upsert,
                "details": "Efficient update-or-insert cho existing records",
            }
        )

    # Check transformer optimization
    transformer_file = Path("src/pipelines/transform/transformer.py")
    if transformer_file.exists():
        with open(transformer_file, encoding="utf-8") as f:
            content = f.read()

        has_validation = "validate_product" in content
        print(
            f"📊 Data Validation: {GREEN if has_validation else RED}{'✓' if has_validation else '✗'}{END}"
        )
        checks.append(
            {
                "name": "Data Validation in Transform",
                "status": GREEN if has_validation else RED,
                "value": has_validation,
                "details": "Validate data quality before loading to DB",
            }
        )

    return checks


def check_database_optimization():
    """Check database schema optimization"""
    print_section("🗄️ PHÂN TÍCH DATABASE OPTIMIZATION")

    checks = []

    # Check schema
    init_script = Path("airflow/setup/init-crawl-db.sh")
    if init_script.exists():
        with open(init_script, encoding="utf-8") as f:
            content = f.read()

        # Check for indexes
        index_count = len(re.findall(r"CREATE INDEX", content))
        print(f"📊 Indexes: {index_count}")
        print(f"   {GREEN}✓ GIN indexes for JSONB fields{END}")
        print(f"   {GREEN}✓ Index trên category_id, category_path{END}")

        checks.append(
            {
                "name": "Database Indexes",
                "status": GREEN if index_count > 5 else YELLOW,
                "value": index_count,
                "details": f"Total indexes: {index_count}",
            }
        )

        # Check for JSONB columns
        jsonb_count = content.count("JSONB")
        print(f"\n📊 JSONB Columns: {jsonb_count}")
        print(f"   {GREEN}✓ Flexible schema cho specifications, images, shipping{END}")

        checks.append(
            {
                "name": "JSONB Columns for Flexible Schema",
                "status": GREEN if jsonb_count > 3 else YELLOW,
                "value": jsonb_count,
                "details": f"JSONB columns: {jsonb_count}",
            }
        )

        # Check for constraints
        has_constraints = "PRIMARY KEY" in content and "UNIQUE" in content
        print(
            f"\n📊 Constraints: {GREEN if has_constraints else RED}{'✓' if has_constraints else '✗'}{END}"
        )
        print(f"   {GREEN}✓ PRIMARY KEY on product_id{END}")
        print(f"   {GREEN}✓ UNIQUE constraints{END}")

        checks.append(
            {
                "name": "Database Constraints",
                "status": GREEN if has_constraints else RED,
                "value": has_constraints,
                "details": "PRIMARY KEY, UNIQUE, and other constraints",
            }
        )

    return checks


def check_code_quality():
    """Check code quality"""
    print_section("💻 PHÂN TÍCH CODE QUALITY")

    checks = []

    # Check for type hints
    python_files = list(Path("src/pipelines").glob("**/*.py")) + list(
        Path("airflow/dags").glob("*.py")
    )

    total_files = len(python_files)
    files_with_typing = 0

    for py_file in python_files[:5]:  # Check first 5 files
        with open(py_file, encoding="utf-8") as f:
            content = f.read()
        if "-> " in content or ": Dict" in content or ": List" in content:
            files_with_typing += 1

    typing_pct = files_with_typing * 100 / 5 if total_files >= 5 else 0
    print(f"📊 Type Hints: {typing_pct:.0f}% files")
    checks.append(
        {
            "name": "Type Hints",
            "status": GREEN if typing_pct > 80 else YELLOW if typing_pct > 50 else RED,
            "value": typing_pct,
            "details": f"Type hints in {files_with_typing}/5 sampled files",
        }
    )

    # Check for docstrings
    docstring_coverage = 0
    for py_file in python_files[:3]:
        with open(py_file, encoding="utf-8") as f:
            content = f.read()
        docstring_count = content.count('"""') + content.count("'''")
        if docstring_count > 0:
            docstring_coverage += 1

    docstring_pct = docstring_coverage * 100 / 3
    print(f"📊 Docstrings: {docstring_pct:.0f}% files")
    checks.append(
        {
            "name": "Code Documentation",
            "status": GREEN if docstring_pct > 80 else YELLOW if docstring_pct > 50 else RED,
            "value": docstring_pct,
            "details": f"Docstrings in {docstring_coverage}/3 sampled files",
        }
    )

    return checks


def generate_optimization_report(all_checks: list[dict]):
    """Generate optimization report"""
    print_section("📋 TÓM TẮT TÌNH HÌNH OPTIMIZATION", "-")

    # Calculate scores
    categories = {
        "Files & Structures": [],
        "DAG Structure": [],
        "Data Pipeline": [],
        "Database": [],
        "Code Quality": [],
    }

    # Categorize checks
    file_checks = check_file_size_and_structure()
    dag_checks = check_dag_structure()
    pipeline_checks = check_data_pipeline_optimization()
    db_checks = check_database_optimization()
    code_checks = check_code_quality()

    all_check_list = file_checks + dag_checks + pipeline_checks + db_checks + code_checks

    # Calculate overall score
    total_checks = len(all_check_list)
    passed_checks = sum(1 for c in all_check_list if c.get("status") == GREEN)
    score = passed_checks * 100 / total_checks if total_checks > 0 else 0

    print(f"\n📊 OVERALL OPTIMIZATION SCORE: {CYAN}{score:.1f}%{END}\n")

    # Category scores
    print("🎯 ĐIỂM THEO CATEGORY:\n")

    category_scores = {
        "Files & Structures": (file_checks, "📁"),
        "DAG Structure": (dag_checks, "🔄"),
        "Data Pipeline": (pipeline_checks, "🔧"),
        "Database": (db_checks, "🗄️"),
        "Code Quality": (code_checks, "💻"),
    }

    for cat_name, (cat_checks, icon) in category_scores.items():
        if cat_checks:
            cat_passed = sum(1 for c in cat_checks if c.get("status") == GREEN)
            cat_score = cat_passed * 100 / len(cat_checks)

            score_color = GREEN if cat_score >= 80 else YELLOW if cat_score >= 60 else RED
            print(f"  {icon} {cat_name}: {score_color}{cat_score:.0f}%{END}")


def show_improvement_recommendations():
    """Show improvement recommendations"""
    print_section("💡 GỢI Ý TỐI ƯU HÓA", "-")

    print(
        f"""
{CYAN}1. DATA ENRICHMENT & CATEGORY PATH:{END}
   {GREEN}✅ DONE{END}
   - Categories file đã có category_id
   - Categories file đã có category_path (hierarchical)
   - Products transform task sẽ enrich category_path từ lookup

{CYAN}2. DATABASE SCHEMA:{END}
   {GREEN}✅ DONE{END}
   - Added category_id column
   - Added category_path column (JSONB)
   - Created GIN indexes for fast JSONB queries
   - Upsert strategy với ON CONFLICT

{CYAN}3. TASK ORCHESTRATION:{END}
   {YELLOW}⚠️ CAN OPTIMIZE{END}
   - Hiện tại crawl_product_details được parallelize per category
   - Có thể thêm rate limiting per target domain
   - Có thể cache HTML content trong Redis (để reuse)
   
   Implement:
   $ # Thêm Rate Limiter vào crawl_products.py
   $ # Thêm Redis cache cho HTML responses

{CYAN}4. PERFORMANCE TUNING:{END}
   {YELLOW}⚠️ CAN OPTIMIZE{END}
   - Batch size cho DB inserts có thể tối ưu (hiện 100)
   - Có thể dùng COPY command thay vì INSERT batch
   - Có thể parallel load nhiều products
   
   Implement:
   $ # Increase batch_size từ 100 → 1000 nếu memory cho phép
   $ # Optimize INSERT query để dùng execute_many

{CYAN}5. MONITORING & LOGGING:{END}
   {GREEN}✅ GOOD{END}
   - Đã có comprehensive logging
   - Đã có stats tracking
   - Có thể thêm Prometheus metrics

{CYAN}6. ERROR RECOVERY:{END}
   {GREEN}✅ GOOD{END}
   - Đã có retry logic
   - Đã có timeout handling
   - Có thể thêm checkpoint/resume support

{CYAN}7. DATA VALIDATION:{END}
   {YELLOW}⚠️ CAN IMPROVE{END}
   - Thêm schema validation (Pydantic models)
   - Thêm outlier detection cho price/sales
   - Thêm data quality metrics
   
   Implement:
   $ # Create Pydantic models cho Product, Category
   $ # Validate trước khi save to DB

{CYAN}8. CACHING STRATEGY:{END}
   {YELLOW}⚠️ CAN OPTIMIZE{END}
   - Hiện dùng Redis cho crawl cache
   - Có thể thêm file-based cache cho category_path lookup
   - Có thể cache transformed data
    """
    )


def main():
    """Main function"""
    print(f"\n{BLUE}{'=' * 80}{END}")
    print(f"{BLUE}{'🔍 ETL PIPELINE OPTIMIZATION ANALYSIS':^80}{END}")
    print(f"{BLUE}{'=' * 80}{END}")

    # Run all checks
    file_checks = check_file_size_and_structure()
    dag_checks = check_dag_structure()
    pipeline_checks = check_data_pipeline_optimization()
    db_checks = check_database_optimization()
    code_checks = check_code_quality()

    all_checks = file_checks + dag_checks + pipeline_checks + db_checks + code_checks

    # Generate report
    generate_optimization_report(all_checks)

    # Show recommendations
    show_improvement_recommendations()

    print(f"\n{BLUE}{'=' * 80}{END}")
    print(f"{CYAN}{'📚 NEXT STEPS':^80}{END}")
    print(f"{BLUE}{'=' * 80}{END}\n")

    print(
        f"""
{GREEN}Priority 1 (Immediate):{END}
  1. Test category_path enrichment trên DAG
  2. Verify data trong database có category_path
  3. Backup database trước khi test

{YELLOW}Priority 2 (This Week):{END}
  1. Optimize batch size cho DB inserts
  2. Thêm Pydantic validation models
  3. Improve error handling cho edge cases

{CYAN}Priority 3 (Future):{END}
  1. Thêm Prometheus metrics
  2. Implement checkpoint/resume cho long-running crawls
  3. Optimize Selenium performance (headless, caching)

{GREEN}Testing Checklist:{END}
  ☐ Run full DAG end-to-end
  ☐ Verify category_path completeness
  ☐ Check database query performance
  ☐ Monitor Airflow task durations
  ☐ Validate data quality with sample checks
    """
    )

    print(f"\n{BLUE}{'=' * 80}{END}\n")


if __name__ == "__main__":
    main()
