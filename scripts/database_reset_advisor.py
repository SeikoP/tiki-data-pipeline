#!/usr/bin/env python3
"""
Script để quyết định có nên xóa DB để crawl lại từ đầu không
Phân tích tình hình hiện tại và recommend actions
"""

import json
import os
import sys
from pathlib import Path

# Try to import psycopg2
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

# Import config
try:
    sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
    from pipelines.crawl.config import (
        POSTGRES_DB,
        POSTGRES_HOST,
        POSTGRES_PASSWORD,
        POSTGRES_PORT,
        POSTGRES_USER,
    )
except ImportError:
    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", 5432))
    POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
    POSTGRES_DB = os.getenv("POSTGRES_DB", "crawl_data")

# Color codes
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
CYAN = "\033[96m"
END = "\033[0m"


def print_section(title: str):
    """Print section header"""
    print(f"\n{BLUE}{'=' * 80}{END}")
    print(f"{BLUE}{title:^80}{END}")
    print(f"{BLUE}{'=' * 80}{END}\n")


def analyze_current_state():
    """Analyze current state of data"""
    print_section("📊 PHÂN TÍCH TÌNH HÌNH HIỆN TẠI")
    
    # Check file data
    print("📂 DỮ LIỆU FILE:")
    files_to_check = [
        ("Categories", Path("data/raw/categories_recursive_optimized.json")),
        ("Products", Path("data/processed/products_final.json")),
    ]
    
    file_stats = {}
    for file_name, file_path in files_to_check:
        if file_path.exists():
            try:
                with open(file_path, encoding="utf-8") as f:
                    data = json.load(f)
                
                count = 0
                if isinstance(data, dict):
                    if "products" in data:
                        count = len(data["products"])
                    elif "categories" in data:
                        count = len(data["categories"])
                elif isinstance(data, list):
                    count = len(data)
                
                print(f"   ✅ {file_name}: {GREEN}{count}{END} items")
                file_stats[file_name] = count
            except Exception as e:
                print(f"   ⚠️ {file_name}: Error - {e}")
        else:
            print(f"   ❌ {file_name}: File không tồn tại")
            file_stats[file_name] = 0
    
    # Check database
    print("\n🗄️ DỮ LIỆU DATABASE:")
    if HAS_PSYCOPG2:
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                database=POSTGRES_DB,
            )
            cur = conn.cursor(cursor_factory=RealDictCursor)
            
            # Check products
            cur.execute("SELECT COUNT(*) as cnt FROM products;")
            products_count = cur.fetchone()["cnt"]
            print(f"   ✅ Products table: {GREEN}{products_count}{END} rows")
            
            # Check products with category_path
            cur.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN category_path IS NOT NULL THEN 1 ELSE 0 END) as with_path
                FROM products;
            """)
            stats = cur.fetchone()
            with_path = stats["with_path"] or 0
            print(f"      - Với category_path: {GREEN}{with_path}{END}/{products_count}")
            
            # Check categories
            cur.execute("SELECT COUNT(*) as cnt FROM categories;")
            categories_count = cur.fetchone()["cnt"]
            print(f"   ✅ Categories table: {GREEN}{categories_count}{END} rows")
            
            cur.close()
            conn.close()
            
            db_stats = {
                "products": products_count,
                "products_with_path": with_path,
                "categories": categories_count,
            }
        except Exception as e:
            print(f"   ❌ Database error: {e}")
            db_stats = {}
    else:
        print(f"   ⚠️ psycopg2 not installed - cannot check database")
        db_stats = {}
    
    return file_stats, db_stats


def show_analysis(**kwargs):
    """Show analysis and recommendation"""
    print_section("📋 PHÂN TÍCH VÀ KHUYẾN CÁO")
    
    file_stats = kwargs.get("file_stats", {})
    db_stats = kwargs.get("db_stats", {})
    
    categories = file_stats.get("Categories", 0)
    products = file_stats.get("Products", 0)
    db_products = db_stats.get("products", 0)
    db_products_with_path = db_stats.get("products_with_path", 0)
    
    print("🔍 TÌNH HÌNH HIỆN TẠI:\n")
    
    # Analyze issues
    issues = []
    
    # Issue 1: Products without category_path
    if db_products > 0 and db_products_with_path < db_products:
        missing_path = db_products - db_products_with_path
        pct = missing_path * 100 / db_products
        issues.append({
            "severity": "HIGH",
            "issue": f"❌ {missing_path}/{db_products} products ({pct:.1f}%) KHÔNG có category_path",
            "impact": "Breadcrumb navigation sẽ không hoạt động cho những products này",
        })
    
    # Issue 2: File data inconsistency
    if products > 0 and db_products > 0 and abs(products - db_products) > 100:
        issues.append({
            "severity": "MEDIUM",
            "issue": f"⚠️ File có {products} products nhưng DB có {db_products} (khác nhau {abs(products - db_products)})",
            "impact": "Dữ liệu trong file và database không đồng bộ",
        })
    
    # Issue 3: Missing categories
    if categories > 0 and db_stats.get("categories", 0) == 0:
        issues.append({
            "severity": "MEDIUM",
            "issue": f"⚠️ Categories file có {categories} categories nhưng DB không có",
            "impact": "Category_path lookup sẽ không hoạt động",
        })
    
    if issues:
        print("🚨 VẤN ĐỀ PHÁT HIỆN:\n")
        for i, issue in enumerate(issues, 1):
            severity = issue["severity"]
            if severity == "HIGH":
                color = RED
            elif severity == "MEDIUM":
                color = YELLOW
            else:
                color = BLUE
            
            print(f"{color}{i}. [{severity}]{END}")
            print(f"   {issue['issue']}")
            print(f"   Impact: {issue['impact']}\n")
    else:
        print(f"{GREEN}✅ Không phát hiện vấn đề lớn{END}\n")
    
    # Recommendation
    print("\n📝 KHUYẾN CÁO:\n")
    
    if db_products_with_path < db_products:
        print(f"{YELLOW}1. TRƯỜNG HỢP: Dữ liệu đã tồn tại nhưng category_path CHƯA HOÀN CHỈNH{END}")
        print(f"""
   {CYAN}✅ KHÔNG CẦN XÓA DB{END}
   
   {GREEN}Thay vào đó, làm theo các bước:{END}
   
   a) Chạy enrich script để bổ sung category_path:
      $ python scripts/enrich_categories_with_paths.py
      
   b) Restart DAG để enrich products:
      $ docker-compose restart airflow-scheduler airflow-worker
      
   c) Run DAG 'tiki_crawl_products' trên Airflow UI
      - Task 'enrich_products_category_path' sẽ:
        • Load categories file (với category_path)
        • Xây dựng lookup map: category_id -> category_path
        • Enrich products bằng category_path từ lookup
      
   d) Verify kết quả:
      SELECT COUNT(*) as cnt FROM products WHERE category_path IS NOT NULL;
      
   ✨ Lợi ích:
      - Giữ lại dữ liệu crawled cũ (không mất công)
      - Chỉ cập nhật category_path missing
      - Tiết kiệm thời gian crawl
        """)
    
    else:
        print(f"{GREEN}1. TRƯỜNG HỢP: Dữ liệu đã hoàn chỉnh{END}")
        print(f"""
   {GREEN}✅ Dữ liệu đã sẵn sàng!{END}
   
   {CYAN}Bạn có thể:{END}
   - Sử dụng dữ liệu hiện tại cho analysis
   - Chạy lại DAG để update tất cả dữ liệu
   - XÓA DB CHỈ NẾU muốn reset toàn bộ
        """)
    
    print(f"\n{BLUE}{'=' * 80}{END}")
    print(f"{BLUE}2. NẾUVÌ CÓ LÍ DO XÓA DB (ví dụ: test crawl, reset data){END}")
    print(f"""
   {CYAN}Các bước xóa và crawl lại:{END}
   
   a) Backup dữ liệu (optional):
      $ docker-compose exec postgres pg_dump -U postgres crawl_data > backup.sql
      
   b) Xóa dữ liệu trong tables:
      $ python scripts/reset_database.py
      
      hoặc xóa toàn bộ database:
      $ docker-compose exec postgres dropdb -U postgres crawl_data
      
   c) Restart PostgreSQL để reinitialize database:
      $ docker-compose restart postgres
      
   d) Restart Airflow:
      $ docker-compose restart airflow-scheduler airflow-worker
      
   e) Run DAG 'tiki_crawl_products' từ đầu:
      - Sẽ crawl lại tất cả categories
      - Crawl products từ từng category
      - Transform và load vào DB (sạch và đầy đủ)
      
   ⏱️ Thời gian dự kiến: 1-3 giờ (tùy thuộc số categories/products)
        """)
    
    print(f"{BLUE}{'=' * 80}{END}")


def show_menu():
    """Show menu for user to choose action"""
    print_section("🎯 CHỌN HÀNH ĐỘNG")
    
    print("""
1. ✅ Giữ DB, chỉ enrich category_path (RECOMMENDED)
   - Nhanh, không mất dữ liệu
   - Bổ sung category_path cho products thiếu
   
2. 🔄 Reset toàn bộ DB và crawl lại
   - Xóa tất cả dữ liệu
   - Crawl products từ đầu
   - Mất công nên chỉ dùng khi thực sự cần
   
3. 💾 Xem thêm thông tin về backup/restore
   
4. ❌ Thoát
    """)
    
    choice = input(f"\n{CYAN}Chọn (1-4): {END}").strip()
    return choice


def show_reset_warning():
    """Show warning before reset"""
    print_section("⚠️ CẢNH BÁO: RESET DATABASE")
    
    print(f"""
{RED}Bạn chuẩn bị XÓA TẤT CẢ DỮ LIỆU trong database!{END}

{YELLOW}Điều này sẽ:{END}
  ❌ XÓA tất cả products
  ❌ XÓA tất cả categories
  ❌ XÓA crawl history
  ✅ Giữ lại database structure (tables, indexes)

{CYAN}Bạn có thể:{END}
  1. Backup dữ liệu trước khi xóa
  2. Khôi phục sau bằng restore script

{YELLOW}Lưu ý: Hành động này KHÔNG THỂ ĐẢO NGƯỢC!{END}
    """)
    
    confirm = input(f"\n{RED}Bạn chắc chắn muốn tiếp tục? (yes/NO): {END}").strip().lower()
    return confirm == "yes"


def main():
    """Main function"""
    print(f"\n{YELLOW}{'=' * 80}{END}")
    print(f"{YELLOW}{'🗄️ DATABASE RESET ADVISOR':^80}{END}")
    print(f"{YELLOW}{'=' * 80}{END}")
    
    # Analyze current state
    file_stats, db_stats = analyze_current_state()
    
    # Show analysis
    show_analysis(file_stats=file_stats, db_stats=db_stats)
    
    # Show menu
    while True:
        choice = show_menu()
        
        if choice == "1":
            print(f"\n{GREEN}✅ Bạn đã chọn: Giữ DB, enrich category_path{END}")
            print(f"""
{CYAN}Các bước tiếp theo:{END}

1. Chạy enrich script:
   $ cd e:\\Project\\tiki-data-pipeline
   $ python scripts/enrich_categories_with_paths.py
   
2. Restart Airflow (nếu chạy với Docker):
   $ docker-compose restart airflow-scheduler airflow-worker
   
3. Truy cập Airflow UI: http://localhost:8080
   - Tìm DAG 'tiki_crawl_products'
   - Click nút Play (▶️) để trigger DAG
   - Giám sát logs
   
4. Verify kết quả sau khi DAG chạy xong:
   $ python scripts/visualize_final_data.py
            """)
            break
        
        elif choice == "2":
            if show_reset_warning():
                print(f"\n{RED}Bắt đầu reset...{END}")
                print(f"""
{CYAN}Các bước:{END}

1. Stop containers (nếu chạy):
   $ docker-compose stop
   
2. Reset PostgreSQL (xóa database):
   $ docker-compose exec postgres dropdb -U postgres crawl_data
   
   hoặc giữ database structure:
   $ docker-compose exec postgres psql -U postgres -d crawl_data \\
     -c "TRUNCATE TABLE products CASCADE; TRUNCATE TABLE categories CASCADE;"
   
3. Start containers lại:
   $ docker-compose up -d
   
4. Verify database đã reset:
   $ python scripts/visualize_final_data.py
   
5. Run DAG để crawl lại từ đầu:
   - Truy cập http://localhost:8080
   - Trigger 'tiki_crawl_products' DAG
                """)
            break
        
        elif choice == "3":
            print(f"""
{CYAN}📚 THÔNG TIN BACKUP/RESTORE:{END}

{GREEN}BACKUP:{END}
  # Backup toàn bộ database
  $ docker-compose exec postgres pg_dump -U postgres crawl_data > backup.sql
  
  # Backup chỉ products table
  $ docker-compose exec postgres pg_dump -U postgres -t products crawl_data > products_backup.sql

{GREEN}RESTORE:{END}
  # Restore toàn bộ database
  $ docker-compose exec postgres psql -U postgres crawl_data < backup.sql
  
  # Restore chỉ products table
  $ docker-compose exec postgres psql -U postgres crawl_data < products_backup.sql

{GREEN}BACKUP SCRIPTS:{END}
  $ python scripts/backup-postgres.ps1      # Windows PowerShell
  $ bash scripts/backup-postgres.sh         # Linux/Mac
            """)
        
        elif choice == "4":
            print(f"\n{GREEN}Thoát{END}")
            break
        
        else:
            print(f"{RED}Lựa chọn không hợp lệ{END}")
    
    print(f"\n{BLUE}{'=' * 80}{END}\n")


if __name__ == "__main__":
    main()
