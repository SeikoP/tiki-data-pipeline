"""
Helper script để đồng bộ logic từ DAG chính sang test DAG

Script này sẽ:
1. Đọc DAG chính (tiki_crawl_products_dag.py)
2. Thay đổi các tham số cho test mode
3. Ghi vào test DAG file (tiki_crawl_products_test_dag.py)

Chạy: python scripts/sync_test_dag.py
"""

import re
from collections.abc import Callable
from pathlib import Path

# Đường dẫn files
SCRIPT_DIR = Path(__file__).parent  # airflow/dags/
PROJECT_ROOT = SCRIPT_DIR.parent.parent  # project root
MAIN_DAG_PATH = SCRIPT_DIR / "tiki_crawl_products_dag.py"  # Cùng thư mục
TEST_DAG_PATH = SCRIPT_DIR / "tiki_crawl_products_test_dag.py"  # Cùng thư mục


def replace_max_products(match: re.Match) -> str:
    """Thay thế max_products với giá trị test"""
    return "max_products = 10  # TEST MODE: Hardcode 10 products cho test  # 0 = không giới hạn"


def replace_execution_timeout(match: re.Match) -> str:
    """Giảm execution_timeout xuống tối đa 5-10 phút cho test"""
    minutes_match = re.search(r"minutes=(\d+)", match.group(0))
    if minutes_match:
        original_minutes = int(minutes_match.group(1))
        # Giảm xuống tối đa 5 phút (hoặc 10 phút nếu > 10)
        if original_minutes > 10:
            new_minutes = 10
        elif original_minutes > 5:
            new_minutes = 5
        else:
            new_minutes = original_minutes
        return f"execution_timeout=timedelta(minutes={new_minutes}),  # TEST MODE: Giảm timeout xuống {new_minutes} phút"
    return match.group(0)


# Các thay đổi cần thiết cho test mode
# Format: (pattern, replacement, description)
# replacement có thể là string hoặc callable function
TEST_REPLACEMENTS: list[tuple[str, str | Callable, str]] = [
    # DAG ID
    (
        r'"dag_id":\s*"tiki_crawl_products"',
        '"dag_id": "tiki_crawl_products_test"',
        "Thay đổi DAG ID",
    ),
    # Description - scheduled
    (
        r'"Crawl sản phẩm Tiki với Dynamic Task Mapping và tối ưu hóa \(Tự động chạy hàng ngày\)"',
        '"TEST - Crawl sản phẩm Tiki với cấu hình tối giản để test E2E (Tự động chạy hàng ngày)"',
        "Thay đổi description (scheduled)",
    ),
    # Description - manual
    (
        r'"Crawl sản phẩm Tiki với Dynamic Task Mapping và tối ưu hóa \(Chạy thủ công - Test mode\)"',
        '"TEST - Crawl sản phẩm Tiki với cấu hình tối giản để test E2E (Chạy thủ công - Test mode)"',
        "Thay đổi description (manual)",
    ),
    # max_active_tasks
    (
        r'"max_active_tasks":\s*10',
        '"max_active_tasks": 3,  # TEST MODE: Giảm xuống 3 tasks song song để test nhanh',
        "Giảm max_active_tasks",
    ),
    # retries trong DEFAULT_ARGS
    (
        r'"retries":\s*\d+,?\s*#.*owner',
        '"retries": 1,  # TEST MODE: Giảm retries xuống 1',
        "Giảm retries trong DEFAULT_ARGS",
    ),
    # max_products trong prepare_products_for_detail
    (
        r"max_products\s*=\s*int\(\s*Variable\.get\([^)]+\)\s*\)",
        replace_max_products,
        "Giới hạn max_products = 10",
    ),
    # Thêm giới hạn max_products trong transform_products (sau khi đọc products từ file)
    (
        r'(logger\.info\(f"📊 Tổng số products trong file: \{len\(products\)\}"\))\s*\n\s*# Log thông tin về crawl detail',
        r'\1\n        \n        # TEST MODE: Giới hạn số lượng products để test\n        max_products = 10  # TEST MODE: Hardcode 10 products cho test\n        if max_products > 0 and len(products) > max_products:\n            logger.info(f"⚠️  TEST MODE: Giới hạn từ {len(products)} xuống {max_products} products")\n            products = products[:max_products]\n            logger.info(f"✅ Đã giới hạn: {len(products)} products để transform")\n        \n        # Log thông tin về crawl detail',
        "Thêm giới hạn max_products trong transform_products",
    ),
    # Thêm giới hạn max_categories trong load_categories (TEST MODE)
    (
        r"(# Giới hạn số danh mục nếu cần \(để test\))\s*\n\s*try:\s*\n\s*max_categories\s*=\s*int\(\s*Variable\.get\([^)]+\)\s*\)",
        r'\1\n        # TEST MODE: Hardcode giới hạn 2 categories cho test\n        max_categories = 2  # TEST MODE: Hardcode 2 categories cho test\n        if max_categories > 0 and len(categories) > max_categories:\n            logger.info(f"⚠️  TEST MODE: Giới hạn từ {len(categories)} xuống {max_categories} categories")\n            categories = categories[:max_categories]\n            logger.info(f"✅ Đã giới hạn: {len(categories)} categories để crawl")\n        \n        # Vẫn kiểm tra Variable nếu có (để override nếu cần)\n        try:\n            var_max_categories = int(Variable.get("TIKI_MAX_CATEGORIES", default="0"))',
        "Thêm giới hạn max_categories trong load_categories",
    ),
    # max_pages
    (
        r"max_pages\s*=\s*\d+\s*#.*Mặc định",
        "max_pages = 2  # TEST MODE: Hardcode 2 pages cho test  # Mặc định 20 trang để tránh timeout",
        "Giảm max_pages = 2",
    ),
    # timeout
    (
        r"timeout\s*=\s*\d+\s*#.*phút mặc định",
        "timeout = 120  # TEST MODE: Giảm timeout xuống 2 phút  # 5 phút mặc định",
        "Giảm timeout = 120",
    ),
    # max_retries
    (
        r"max_retries=\d+",
        "max_retries=2,  # TEST MODE: Giảm retry xuống 2",
        "Giảm max_retries = 2",
    ),
    # execution_timeout
    (
        r"execution_timeout=timedelta\(minutes=\d+\)",
        replace_execution_timeout,
        "Giảm execution_timeout",
    ),
    # Tags - manual
    (
        r'"tags":\s*\["tiki",\s*"crawl",\s*"products",\s*"data-pipeline",\s*"manual"\]',
        '"tags": ["tiki", "crawl", "products", "data-pipeline", "manual", "test"]',
        "Thêm tag test (manual)",
    ),
    # Tags - scheduled
    (
        r'"tags":\s*\["tiki",\s*"crawl",\s*"products",\s*"data-pipeline",\s*"scheduled"\]',
        '"tags": ["tiki", "crawl", "products", "data-pipeline", "scheduled", "test"]',
        "Thêm tag test (scheduled)",
    ),
]


def apply_test_replacements(content: str) -> str:
    """Áp dụng các thay đổi cho test mode"""
    result = content
    changes_made = []

    for pattern, replacement, description in TEST_REPLACEMENTS:
        if re.search(pattern, result):
            if callable(replacement):
                # Nếu replacement là function, dùng nó để thay thế
                result = re.sub(pattern, replacement, result)
                changes_made.append(description)
            else:
                # Nếu replacement là string, thay thế trực tiếp
                result = re.sub(pattern, replacement, result)
                changes_made.append(description)

    if changes_made:
        print(f"\n✓ Đã áp dụng {len(changes_made)} thay đổi:")
        for change in changes_made:
            print(f"   - {change}")
    else:
        print("\n⚠️  Không có thay đổi nào được áp dụng")

    return result


def sync_test_dag():
    """Đồng bộ logic từ DAG chính sang test DAG"""
    print("=" * 70)
    print("🔄 ĐỒNG BỘ TEST DAG TỪ DAG CHÍNH")
    print("=" * 70)

    # Kiểm tra file DAG chính có tồn tại không
    if not MAIN_DAG_PATH.exists():
        print(f"❌ Không tìm thấy DAG chính: {MAIN_DAG_PATH}")
        return False

    print(f"📖 Đọc DAG chính: {MAIN_DAG_PATH}")
    try:
        with open(MAIN_DAG_PATH, encoding="utf-8") as f:
            main_dag_content = f.read()
    except Exception as e:
        print(f"❌ Lỗi khi đọc DAG chính: {e}")
        return False

    print(f"✅ Đã đọc {len(main_dag_content)} ký tự từ DAG chính")

    # Áp dụng các thay đổi cho test mode
    print("\n🔧 Áp dụng các thay đổi cho test mode...")
    test_dag_content = apply_test_replacements(main_dag_content)

    # Đảm bảo thư mục tồn tại
    TEST_DAG_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Ghi vào test DAG file
    print(f"\n💾 Ghi vào test DAG: {TEST_DAG_PATH}")
    try:
        with open(TEST_DAG_PATH, "w", encoding="utf-8") as f:
            f.write(test_dag_content)
    except Exception as e:
        print(f"❌ Lỗi khi ghi test DAG: {e}")
        return False

    print(f"✅ Đã ghi {len(test_dag_content)} ký tự vào test DAG")

    # So sánh số dòng
    main_lines = main_dag_content.count("\n")
    test_lines = test_dag_content.count("\n")
    print("\n📊 Thống kê:")
    print(f"   - DAG chính: {main_lines} dòng")
    print(f"   - Test DAG: {test_lines} dòng")
    print(f"   - Chênh lệch: {abs(main_lines - test_lines)} dòng")

    print("\n" + "=" * 70)
    print("✅ HOÀN TẤT ĐỒNG BỘ TEST DAG")
    print("=" * 70)
    print(f"\n💡 Test DAG đã được cập nhật: {TEST_DAG_PATH}")
    print("   Bạn có thể kiểm tra và chạy test DAG trong Airflow UI")

    return True


if __name__ == "__main__":
    success = sync_test_dag()
    exit(0 if success else 1)
