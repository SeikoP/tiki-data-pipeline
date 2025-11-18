#!/usr/bin/env python3
"""
Script để visualize dữ liệu cuối cùng sau khi chạy ETL pipeline:
- Kiểm tra dữ liệu từ files (JSON)
- Kiểm tra dữ liệu trong database (PostgreSQL)
- So sánh và report
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

# Set UTF-8 encoding for output
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

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
    """Print a section header"""
    print(f"\n{BLUE}{'=' * 80}{END}")
    print(f"{BLUE}{title:^80}{END}")
    print(f"{BLUE}{'=' * 80}{END}\n")


def check_file_data():
    """Check data in JSON files"""
    print_section("📂 KIỂM TRA DỮ LIỆU FILE (JSON)")
    
    files_to_check = [
        ("Categories", Path("data/raw/categories_recursive_optimized.json")),
        ("Products (with detail)", Path("data/processed/products_with_detail.json")),
        ("Products (transformed)", Path("data/processed/products_transformed.json")),
        ("Products (final)", Path("data/processed/products_final.json")),
    ]
    
    for file_name, file_path in files_to_check:
        print(f"\n📄 {file_name}: {file_path}")
        
        if not file_path.exists():
            print(f"{YELLOW}   ℹ️ File không tồn tại (có thể chưa được tạo){END}")
            continue
        
        try:
            with open(file_path, encoding="utf-8") as f:
                data = json.load(f)
            
            # Handle different file formats
            if isinstance(data, dict):
                # Wrapper format
                products = data.get("products", [])
                categories = data.get("categories", [])
                
                if products:
                    print(f"{GREEN}   ✅ Tìm thấy {len(products)} products{END}")
                    
                    # Show first product sample
                    first_product = products[0]
                    print(f"\n   📋 Sample product #{1}:")
                    print(f"      - ID: {first_product.get('product_id')}")
                    print(f"      - Name: {first_product.get('name', 'N/A')[:50]}...")
                    print(f"      - Category URL: {first_product.get('category_url', 'N/A')[:40]}...")
                    print(f"      - Category ID: {first_product.get('category_id', 'N/A')}")
                    
                    if first_product.get("category_path"):
                        path_str = " > ".join(first_product.get("category_path", []))
                        print(f"      - Category Path: {path_str}")
                    else:
                        print(f"      - Category Path: {YELLOW}(không có){END}")
                    
                    print(f"      - Price: {first_product.get('price', 'N/A')}")
                    print(f"      - Sales: {first_product.get('sales_count', 'N/A')}")
                    
                    # Statistics
                    with_category_path = sum(1 for p in products if p.get("category_path"))
                    with_category_id = sum(1 for p in products if p.get("category_id"))
                    with_price = sum(1 for p in products if p.get("price"))
                    with_sales = sum(1 for p in products if p.get("sales_count"))
                    
                    print(f"\n   📊 Statistics:")
                    print(f"      - Có category_path: {GREEN}{with_category_path}{END}/{len(products)}")
                    print(f"      - Có category_id: {GREEN}{with_category_id}{END}/{len(products)}")
                    print(f"      - Có price: {GREEN}{with_price}{END}/{len(products)}")
                    print(f"      - Có sales_count: {GREEN}{with_sales}{END}/{len(products)}")
                
                if categories:
                    print(f"{GREEN}   ✅ Tìm thấy {len(categories)} categories{END}")
                    
                    # Show first category sample
                    first_cat = categories[0]
                    print(f"\n   📋 Sample category #{1}:")
                    print(f"      - Name: {first_cat.get('name')}")
                    print(f"      - ID: {first_cat.get('category_id', 'N/A')}")
                    print(f"      - Level: {first_cat.get('level', 'N/A')}")
                    
                    if first_cat.get("category_path"):
                        path_str = " > ".join(first_cat.get("category_path", []))
                        print(f"      - Category Path: {path_str}")
                    else:
                        print(f"      - Category Path: {YELLOW}(không có){END}")
            
            elif isinstance(data, list):
                # Direct list format
                print(f"{GREEN}   ✅ Tìm thấy {len(data)} items{END}")
                
                if data:
                    first_item = data[0]
                    print(f"\n   📋 Sample item #{1}:")
                    for key in list(first_item.keys())[:5]:
                        value = first_item[key]
                        if isinstance(value, str) and len(value) > 40:
                            value = value[:40] + "..."
                        print(f"      - {key}: {value}")
        
        except Exception as e:
            print(f"{RED}   ❌ Lỗi đọc file: {e}{END}")


def check_database_data():
    """Check data in PostgreSQL database"""
    if not HAS_PSYCOPG2:
        print_section("🗄️ KIỂM TRA DỮ LIỆU DATABASE")
        print(f"{YELLOW}   ℹ️ psycopg2 không được cài đặt, bỏ qua kiểm tra database{END}")
        return
    
    print_section("🗄️ KIỂM TRA DỮ LIỆU DATABASE (PostgreSQL)")
    
    try:
        # Connect to database
        print(f"📡 Kết nối đến: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            database=POSTGRES_DB,
        )
        cur = conn.cursor(cursor_factory=RealDictCursor)
        print(f"{GREEN}✅ Kết nối thành công!{END}\n")
        
        # Check products table
        print("📊 Products table:")
        cur.execute("SELECT COUNT(*) as cnt FROM products;")
        products_count = cur.fetchone()["cnt"]
        print(f"   - Tổng products: {GREEN}{products_count}{END}")
        
        # Check categories table
        print("\n📊 Categories table:")
        cur.execute("SELECT COUNT(*) as cnt FROM categories;")
        categories_count = cur.fetchone()["cnt"]
        print(f"   - Tổng categories: {GREEN}{categories_count}{END}")
        
        # Sample product
        if products_count > 0:
            print(f"\n📋 Sample product from database:")
            cur.execute("""
                SELECT 
                    product_id, 
                    name, 
                    category_url, 
                    category_id, 
                    category_path,
                    price,
                    sales_count,
                    crawled_at
                FROM products 
                LIMIT 1;
            """)
            product = cur.fetchone()
            
            if product:
                print(f"   - Product ID: {product['product_id']}")
                print(f"   - Name: {product['name'][:50]}...")
                print(f"   - Category URL: {product['category_url'][:40] if product['category_url'] else 'N/A'}...")
                print(f"   - Category ID: {product['category_id'] or 'N/A'}")
                
                if product['category_path']:
                    path_str = " > ".join(product['category_path'])
                    print(f"   - Category Path: {path_str}")
                else:
                    print(f"   - Category Path: {YELLOW}(không có){END}")
                
                print(f"   - Price: {product['price'] or 'N/A'}")
                print(f"   - Sales: {product['sales_count'] or 'N/A'}")
                print(f"   - Crawled: {product['crawled_at']}")
        
        # Statistics about category_path
        print(f"\n📊 Category Path Statistics:")
        cur.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN category_path IS NOT NULL THEN 1 ELSE 0 END) as with_path,
                SUM(CASE WHEN category_id IS NOT NULL THEN 1 ELSE 0 END) as with_id
            FROM products;
        """)
        stats = cur.fetchone()
        
        total = stats['total'] or 0
        with_path = stats['with_path'] or 0
        with_id = stats['with_id'] or 0
        
        print(f"   - Total products: {total}")
        print(f"   - With category_path: {GREEN}{with_path}{END}/{total} ({with_path*100/total:.1f}%)")
        print(f"   - With category_id: {GREEN}{with_id}{END}/{total} ({with_id*100/total:.1f}%)")
        
        # Sample categories
        if categories_count > 0:
            print(f"\n📋 Sample category from database:")
            cur.execute("""
                SELECT 
                    category_id,
                    name,
                    level,
                    category_path,
                    parent_url
                FROM categories 
                WHERE level = 1
                LIMIT 1;
            """)
            category = cur.fetchone()
            
            if category:
                print(f"   - Category ID: {category['category_id']}")
                print(f"   - Name: {category['name']}")
                print(f"   - Level: {category['level']}")
                
                if category['category_path']:
                    path_str = " > ".join(category['category_path'])
                    print(f"   - Category Path: {path_str}")
                else:
                    print(f"   - Category Path: {YELLOW}(không có){END}")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"{RED}❌ Lỗi kết nối database: {e}{END}")
        print(f"{YELLOW}   Gợi ý: Đảm bảo PostgreSQL đang chạy (docker-compose up){END}")


def visualize_data_flow():
    """Visualize the data flow through the pipeline"""
    print_section("🔄 LUỒNG DỮ LIỆU ETL")
    
    print(f"""
{CYAN}EXTRACT (Crawl){END}
   ↓
   📄 Crawl từ category URLs
   📝 Lưu vào: data/raw/
   - categories_recursive_optimized.json (465 categories)
   - products_raw_*.json (products từ từng category)
   
   {GREEN}✅ Output: Raw product data + categories{END}

{CYAN}TRANSFORM{END}
   ↓
   📝 Data cleaning & normalization:
   - Normalize text, price, sales_count
   - Extract category_id từ category_url
   - Build category_path từ categories lookup
   - Compute additional fields (discount %, revenue, etc.)
   
   📁 Lưu vào: data/processed/
   - products_transformed.json
   
   {GREEN}✅ Output: Cleaned & enriched products with category_path{END}

{CYAN}LOAD{END}
   ↓
   🗄️ Insert vào PostgreSQL:
   - products table (với category_id, category_path)
   - categories table (với category_path)
   
   {GREEN}✅ Output: Data in database ready for analysis{END}

{CYAN}RESULT DATA STRUCTURE{END}
   ↓
   📊 Mỗi product có:
      • product_id (unique)
      • name, url, image_url
      • category_url, {GREEN}category_id{END}, {GREEN}category_path{END}
      • price, original_price, discount_percent
      • sales_count, rating_average, review_count
      • specifications, images (JSONB)
      • seller info, brand, stock info
      • computed fields (revenue, popularity, value score)
      • timestamps (crawled_at, updated_at)
   
   📊 Category path example:
      {GREEN}["Nhà & Đời sống", "Nhạc cụ", "Amplifier"]{END}
      (Dùng cho breadcrumb navigation)
    """)


def main():
    """Main function"""
    print(f"\n{YELLOW}{'=' * 80}{END}")
    print(f"{YELLOW}{'📊 FINAL DATA VISUALIZATION AFTER ETL PIPELINE':^80}{END}")
    print(f"{YELLOW}{'=' * 80}{END}")
    
    # Check files
    check_file_data()
    
    # Check database
    check_database_data()
    
    # Show data flow
    visualize_data_flow()
    
    # Summary
    print_section("📋 SUMMARY")
    
    print(f"""
{GREEN}✅ Data Pipeline Status:{END}

1. {CYAN}Categories:{END}
   - 465 categories từ Tiki (level 1-4)
   - Mỗi category có:
     • category_id: c<số> (e.g., c1883)
     • category_path: ["level_1_name", "level_2_name", ...]
     • parent_url: URL của danh mục cha
     • level: mức phân cấp (1-4)

2. {CYAN}Products:{END}
   - Hàng ngàn products crawled từ categories
   - Mỗi product có:
     • category_id: extracted từ category_url
     • category_path: lookup từ categories file
     • Tất cả metadata (price, sales, rating, etc.)

3. {CYAN}Database:{END}
   - products table: {CYAN}~N products{END} (với category_id, category_path JSONB)
   - categories table: {CYAN}465 categories{END} (với category_path)

{YELLOW}📝 Note:{END}
   - category_path được xây dựng từ parent_url relationship
   - Level 1 categories chỉ có path ["category_name"] (vì không có parent)
   - Level 2+ categories có full path từ root
   - category_path dùng để làm breadcrumb navigation
    """)
    
    print(f"\n{BLUE}{'=' * 80}{END}")
    print(f"{GREEN}{'Visualization Complete!':^80}{END}")
    print(f"{BLUE}{'=' * 80}{END}\n")


if __name__ == "__main__":
    main()
