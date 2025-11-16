"""
Script để tạo DAG test từ DAG chính
Tự động copy và modify các phần cần thiết
"""
import re
from pathlib import Path

# Đường dẫn files
dag_file = Path("airflow/dags/tiki_crawl_products_dag.py")
test_dag_file = Path("airflow/dags/tiki_crawl_products_test_dag.py")

print(f"📖 Đang đọc DAG chính: {dag_file}")
with open(dag_file, "r", encoding="utf-8") as f:
    content = f.read()

print("🔧 Đang modify DAG cho test...")

# 1. Đổi DAG ID
content = content.replace('"dag_id": "tiki_crawl_products"', '"dag_id": "tiki_crawl_products_test"')

# 2. Đổi description
content = content.replace(
    "Crawl sản phẩm Tiki với Dynamic Task Mapping và tối ưu hóa",
    "TEST - Crawl sản phẩm Tiki với cấu hình tối giản để test E2E"
)

# 3. Đổi output paths
content = content.replace(
    'OUTPUT_DIR = DATA_DIR / "raw" / "products"',
    'OUTPUT_DIR = DATA_DIR / "test_output" / "products"'
)

# 4. Giảm max_active_tasks
content = content.replace(
    '"max_active_tasks": 10,',
    '"max_active_tasks": 3,  # Giảm xuống 3 cho test'
)

# 5. Thêm tag "test"
content = content.replace(
    'dag_tags = ["tiki", "crawl", "products", "data-pipeline", "manual"]',
    'dag_tags = ["tiki", "crawl", "products", "data-pipeline", "manual", "test"]'
)
content = content.replace(
    'dag_tags = ["tiki", "crawl", "products", "data-pipeline", "scheduled"]',
    'dag_tags = ["tiki", "crawl", "products", "data-pipeline", "scheduled", "test"]'
)

# 6. Hardcode giới hạn categories trong load_categories function
# Tìm và replace phần giới hạn categories
pattern = r'(max_categories = int\(Variable\.get\("TIKI_MAX_CATEGORIES", default_var="0"\)\)\s+if max_categories > 0:\s+categories = categories\[:max_categories\])'
replacement = r'''# TEST MODE: Hardcode giới hạn 3 categories
            max_categories = 3  # Hardcode cho test
            if max_categories > 0:
                categories = categories[:max_categories]'''
content = re.sub(pattern, replacement, content, flags=re.MULTILINE)

# 7. Hardcode giới hạn pages trong crawl_single_category
pattern = r'(max_pages = int\(\s+Variable\.get\("TIKI_MAX_PAGES_PER_CATEGORY", default_var="20"\)\s+\))'
replacement = r'max_pages = 2  # TEST MODE: Hardcode 2 pages cho test'
content = re.sub(pattern, replacement, content, flags=re.MULTILINE)

# 8. Hardcode giới hạn products cho detail trong prepare_products_for_detail
pattern = r'(max_products = int\(\s+Variable\.get\("TIKI_MAX_PRODUCTS_FOR_DETAIL", default_var="0"\)\s+\))'
replacement = r'max_products = 10  # TEST MODE: Hardcode 10 products cho test'
content = re.sub(pattern, replacement, content, flags=re.MULTILINE)

# 9. Giảm timeout
content = content.replace(
    'timeout = int(Variable.get("TIKI_CRAWL_TIMEOUT", default_var="300"))',
    'timeout = 120  # TEST MODE: Giảm timeout xuống 2 phút'
)

# 10. Giảm retries
content = content.replace(
    '"retries": 3,',
    '"retries": 1,  # TEST MODE: Giảm retries'
)

# 11. Giảm execution timeout cho các tasks
content = content.replace(
    'execution_timeout=timedelta(minutes=10)',
    'execution_timeout=timedelta(minutes=5)  # TEST MODE: Giảm timeout'
)
content = content.replace(
    'execution_timeout=timedelta(minutes=30)',
    'execution_timeout=timedelta(minutes=10)  # TEST MODE: Giảm timeout'
)
content = content.replace(
    'execution_timeout=timedelta(minutes=60)',
    'execution_timeout=timedelta(minutes=15)  # TEST MODE: Giảm timeout'
)
content = content.replace(
    'execution_timeout=timedelta(minutes=15)',
    'execution_timeout=timedelta(minutes=5)  # TEST MODE: Giảm timeout'
)

print(f"💾 Đang ghi DAG test: {test_dag_file}")
with open(test_dag_file, "w", encoding="utf-8") as f:
    f.write(content)

print("✅ Đã tạo DAG test thành công!")
print(f"📁 File: {test_dag_file}")
print("\n💡 Các thay đổi:")
print("   - DAG ID: tiki_crawl_products_test")
print("   - Output: data/test_output/products/")
print("   - max_active_tasks: 3")
print("   - Categories: 3 (hardcode)")
print("   - Pages: 2 (hardcode)")
print("   - Products detail: 10 (hardcode)")
print("   - Timeout: giảm xuống")
print("   - Retries: 1")

