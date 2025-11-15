#!/usr/bin/env python3
"""
Script để kiểm tra và fix category_id trong bảng products
- Kiểm tra xem column category_id đã có chưa
- Nếu chưa có, thêm column
- Update category_id từ category_url cho các records hiện có
"""

import os
import sys
from pathlib import Path

# Thêm src vào path
project_root = Path(__file__).parent.parent
src_path = project_root / "src"
sys.path.insert(0, str(src_path))

try:
    import psycopg2
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
except ImportError:
    print("❌ Cần cài đặt psycopg2: pip install psycopg2-binary")
    sys.exit(1)


def check_and_fix_category_id():
    """Kiểm tra và fix category_id trong database"""
    print("=" * 70)
    print("🔍 KIỂM TRA VÀ FIX category_id TRONG PRODUCTS TABLE")
    print("=" * 70)
    
    # Lấy database config
    db_host = os.getenv("POSTGRES_HOST", "localhost")
    db_port = int(os.getenv("POSTGRES_PORT", "5432"))
    db_name = os.getenv("POSTGRES_DB", "crawl_data")
    db_user = os.getenv("POSTGRES_USER", "postgres")
    db_password = os.getenv("POSTGRES_PASSWORD", "postgres")
    
    # Thử đọc từ .env file
    env_file = project_root / ".env"
    if env_file.exists():
        try:
            from dotenv import load_dotenv
            load_dotenv(env_file, override=True)
            db_host = os.getenv("POSTGRES_HOST", db_host)
            db_port = int(os.getenv("POSTGRES_PORT", db_port))
            db_name = os.getenv("POSTGRES_DB", db_name)
            db_user = os.getenv("POSTGRES_USER", db_user)
            db_password = os.getenv("POSTGRES_PASSWORD", db_password)
        except ImportError:
            # Fallback: đọc thủ công
            with open(env_file, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        key, value = line.split("=", 1)
                        key = key.strip()
                        value = value.strip().strip('"').strip("'")
                        if key == "POSTGRES_PASSWORD":
                            db_password = value
                        elif key == "POSTGRES_HOST":
                            db_host = value
                        elif key == "POSTGRES_USER":
                            db_user = value
                        elif key == "POSTGRES_DB":
                            db_name = value
                        elif key == "POSTGRES_PORT":
                            try:
                                db_port = int(value)
                            except ValueError:
                                pass
    
    print(f"\n📋 Thông tin kết nối:")
    print(f"   - Host: {db_host}")
    print(f"   - Port: {db_port}")
    print(f"   - User: {db_user}")
    print(f"   - Database: {db_name}")
    
    try:
        # Kết nối database
        print(f"\n🔌 Đang kết nối database...")
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password,
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        
        print("✅ Đã kết nối database")
        
        # Bước 1: Kiểm tra xem column category_id đã có chưa
        print("\n🔍 Bước 1: Kiểm tra column category_id...")
        cur.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'products' AND column_name = 'category_id';
        """)
        result = cur.fetchone()
        
        if result:
            print("✅ Column category_id đã tồn tại")
        else:
            print("⚠️  Column category_id chưa có, đang thêm...")
            cur.execute("""
                ALTER TABLE products ADD COLUMN category_id VARCHAR(255);
            """)
            print("✅ Đã thêm column category_id")
        
        # Bước 2: Kiểm tra column category_path
        print("\n🔍 Bước 2: Kiểm tra column category_path...")
        cur.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'products' AND column_name = 'category_path';
        """)
        result = cur.fetchone()
        
        if result:
            print("✅ Column category_path đã tồn tại")
        else:
            print("⚠️  Column category_path chưa có, đang thêm...")
            cur.execute("""
                ALTER TABLE products ADD COLUMN category_path JSONB;
            """)
            print("✅ Đã thêm column category_path")
        
        # Bước 3: Tạo indexes nếu chưa có
        print("\n🔍 Bước 3: Kiểm tra và tạo indexes...")
        cur.execute("""
            SELECT indexname FROM pg_indexes 
            WHERE tablename = 'products' AND indexname = 'idx_products_category_id';
        """)
        if not cur.fetchone():
            print("📝 Đang tạo index cho category_id...")
            cur.execute("""
                CREATE INDEX idx_products_category_id ON products(category_id);
            """)
            print("✅ Đã tạo index cho category_id")
        else:
            print("✅ Index cho category_id đã tồn tại")
        
        cur.execute("""
            SELECT indexname FROM pg_indexes 
            WHERE tablename = 'products' AND indexname = 'idx_products_category_path';
        """)
        if not cur.fetchone():
            print("📝 Đang tạo GIN index cho category_path...")
            cur.execute("""
                CREATE INDEX idx_products_category_path ON products USING GIN (category_path);
            """)
            print("✅ Đã tạo GIN index cho category_path")
        else:
            print("✅ Index cho category_path đã tồn tại")
        
        # Bước 4: Đếm số products hiện có
        print("\n📊 Bước 4: Thống kê dữ liệu...")
        cur.execute("SELECT COUNT(*) FROM products;")
        total_products = cur.fetchone()[0]
        print(f"   - Tổng số products: {total_products}")
        
        cur.execute("SELECT COUNT(*) FROM products WHERE category_url IS NOT NULL;")
        products_with_category_url = cur.fetchone()[0]
        print(f"   - Products có category_url: {products_with_category_url}")
        
        cur.execute("SELECT COUNT(*) FROM products WHERE category_id IS NOT NULL;")
        products_with_category_id = cur.fetchone()[0]
        print(f"   - Products có category_id: {products_with_category_id}")
        
        cur.execute("SELECT COUNT(*) FROM products WHERE category_path IS NOT NULL;")
        products_with_category_path = cur.fetchone()[0]
        print(f"   - Products có category_path: {products_with_category_path}")
        
        # Bước 5: Update category_id từ category_url
        print("\n🔄 Bước 5: Đang update category_id từ category_url...")
        cur.execute("""
            UPDATE products 
            SET category_id = 'c' || substring(category_url from '/c([0-9]+)')
            WHERE category_id IS NULL 
              AND category_url IS NOT NULL 
              AND category_url ~ '/c[0-9]+';
        """)
        updated_count = cur.rowcount
        print(f"✅ Đã update category_id cho {updated_count} products từ category_url")
        
        # Bước 6: Thống kê sau khi update
        print("\n📊 Bước 6: Thống kê sau khi update...")
        cur.execute("SELECT COUNT(*) FROM products WHERE category_id IS NOT NULL;")
        products_with_category_id_after = cur.fetchone()[0]
        print(f"   - Products có category_id (sau update): {products_with_category_id_after}")
        
        if products_with_category_url > products_with_category_id_after:
            missing = products_with_category_url - products_with_category_id_after
            print(f"   ⚠️  Còn {missing} products có category_url nhưng không extract được category_id")
            print(f"      (có thể do format URL không đúng pattern /c[0-9]+)")
        
        cur.close()
        conn.close()
        
        print("\n" + "=" * 70)
        print("✅ HOÀN TẤT!")
        print("=" * 70)
        print(f"\n📈 Tổng kết:")
        print(f"   - Tổng products: {total_products}")
        print(f"   - Có category_id: {products_with_category_id_after}")
        print(f"   - Có category_path: {products_with_category_path}")
        
        return 0
        
    except psycopg2.OperationalError as e:
        print(f"\n❌ Lỗi kết nối database: {e}")
        print("\n💡 Hướng dẫn:")
        print("   1. Đảm bảo PostgreSQL đang chạy")
        print("   2. Set environment variables hoặc tạo file .env")
        return 1
    except Exception as e:
        print(f"\n❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(check_and_fix_category_id())

