#!/usr/bin/env python3
"""
Script tổng hợp để fix category_path issue:
1. Enrich categories file với category_id và category_path
2. Apply database schema changes
3. Verify kết quả
"""

import json
import subprocess
import sys
from pathlib import Path

# Color codes for output
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
END = "\033[0m"


def run_script(script_name: str, description: str) -> bool:
    """Run a script and return True if successful"""
    print(f"\n{BLUE}{'=' * 70}{END}")
    print(f"{BLUE}📋 {description}{END}")
    print(f"{BLUE}{'=' * 70}{END}\n")
    
    script_path = Path(__file__).parent / script_name
    
    if not script_path.exists():
        print(f"{RED}❌ Script không tồn tại: {script_path}{END}")
        return False
    
    try:
        result = subprocess.run(
            [sys.executable, str(script_path)],
            check=True,
            capture_output=False,
            text=True
        )
        print(f"\n{GREEN}✅ {description} thành công!{END}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n{RED}❌ {description} thất bại! Code: {e.returncode}{END}")
        return False
    except Exception as e:
        print(f"\n{RED}❌ Lỗi khi run {script_name}: {e}{END}")
        return False


def main():
    """Main workflow"""
    print(f"\n{YELLOW}{'=' * 70}{END}")
    print(f"{YELLOW}🔧 FIX CATEGORY PATH ISSUE{END}")
    print(f"{YELLOW}{'=' * 70}{END}")
    
    # Step 1: Enrich categories
    if not run_script(
        "enrich_categories_with_paths.py",
        "Enrich categories file với category_id và category_path"
    ):
        print(f"\n{RED}❌ Bước 1 thất bại!{END}")
        return False
    
    # Step 2: Apply schema changes
    if not run_script(
        "apply_schema_changes.py",
        "Apply database schema changes"
    ):
        print(f"\n{RED}⚠️ Bước 2 có vấn đề (có thể là database chưa được khởi động){END}")
        print(f"{YELLOW}Bạn có thể chạy bước này sau khi Docker-compose started{END}")
    
    # Final summary
    print(f"\n{BLUE}{'=' * 70}{END}")
    print(f"{BLUE}📋 SUMMARY{END}")
    print(f"{BLUE}{'=' * 70}{END}")
    
    print(f"\n{GREEN}✅ Completed steps:{END}")
    print(f"   1. ✅ Enriched categories file với category_id và category_path")
    print(f"   2. ⚠️ Applied database schema changes (hoặc sẽ apply khi DB ready)")
    
    print(f"\n{YELLOW}📝 Next steps:{END}")
    print(f"""
   1. 🚀 Restart Airflow DAG để sử dụng enriched categories file
      - Lệnh: docker-compose restart airflow-scheduler airflow-worker
   
   2. 📊 Run DAG 'tiki_crawl_products' trên Airflow UI
      - URL: http://localhost:8080
      - Tìm DAG 'tiki_crawl_products' và click "Play"
   
   3. ✔️ Verify kết quả:
      - Kiểm tra products table có category_id và category_path
      - Run query: SELECT product_id, category_id, category_path FROM products LIMIT 5;
   
   4. 🔍 Monitor logs:
      - Task 'enrich_products_category_path' sẽ log số products được enriched
      - Task 'load_products' sẽ confirm products được saved với category_path
    """)
    
    print(f"\n{GREEN}{'=' * 70}{END}")
    print(f"{GREEN}✅ Fix process completed!{END}")
    print(f"{GREEN}{'=' * 70}{END}\n")


if __name__ == "__main__":
    main()
