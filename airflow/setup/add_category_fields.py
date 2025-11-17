#!/usr/bin/env python3
"""
Script để thêm category_id và category_path columns vào products table
Có thể chạy trực tiếp hoặc qua Python thay vì bash script
"""

import os
import sys
from pathlib import Path

# Thêm src vào path
project_root = Path(__file__).parent.parent.parent
src_path = project_root / "src"
sys.path.insert(0, str(src_path))

try:
    import psycopg2
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
except ImportError:
    print("❌ Cần cài đặt psycopg2: pip install psycopg2-binary")
    sys.exit(1)


def run_migration():
    """Chạy migration để thêm category_id và category_path"""
    print("=" * 70)
    print("📊 MIGRATION: Add category_id and category_path to products table")
    print("=" * 70)
    
    # Lấy database config từ environment variables
    # trufflehog:ignore - Development defaults, production uses .env
    db_host = os.getenv("POSTGRES_HOST", "localhost")
    db_port = int(os.getenv("POSTGRES_PORT", "5432"))
    db_name = os.getenv("POSTGRES_DB", "crawl_data")
    db_user = os.getenv("POSTGRES_USER", "postgres")
    db_password = os.getenv("POSTGRES_PASSWORD", "postgres")  # DEVELOPMENT ONLY
    
    # Thử đọc từ .env file nếu có
    env_file = project_root / ".env"
    if env_file.exists():
        print(f"📄 Đang đọc .env từ: {env_file}")
        try:
            from dotenv import load_dotenv
            load_dotenv(env_file, override=True)
            db_host = os.getenv("POSTGRES_HOST", db_host)
            db_port = int(os.getenv("POSTGRES_PORT", db_port))
            db_name = os.getenv("POSTGRES_DB", db_name)
            db_user = os.getenv("POSTGRES_USER", db_user)
            db_password = os.getenv("POSTGRES_PASSWORD", db_password)
            print("✅ Đã load .env bằng python-dotenv")
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
            print("✅ Đã load .env thủ công")
    
    print(f"\n📋 Thông tin kết nối:")
    print(f"   - Host: {db_host}")
    print(f"   - Port: {db_port}")
    print(f"   - User: {db_user}")
    print(f"   - Database: {db_name}")
    print(f"   - Password: {'***' if db_password else '(chưa set)'}")
    
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
        
        # Thêm category_id column
        print("\n📝 Đang thêm column category_id...")
        cur.execute("""
            ALTER TABLE products ADD COLUMN IF NOT EXISTS category_id VARCHAR(255);
        """)
        print("✅ Đã thêm column category_id")
        
        # Thêm category_path column
        print("📝 Đang thêm column category_path...")
        cur.execute("""
            ALTER TABLE products ADD COLUMN IF NOT EXISTS category_path JSONB;
        """)
        print("✅ Đã thêm column category_path")
        
        # Tạo index cho category_id
        print("📝 Đang tạo index cho category_id...")
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_products_category_id ON products(category_id);
        """)
        print("✅ Đã tạo index cho category_id")
        
        # Tạo GIN index cho category_path
        print("📝 Đang tạo GIN index cho category_path...")
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_products_category_path ON products USING GIN (category_path);
        """)
        print("✅ Đã tạo GIN index cho category_path")
        
        # Update category_id từ category_url nếu có thể extract
        print("📝 Đang update category_id từ category_url...")
        cur.execute("""
            UPDATE products 
            SET category_id = 'c' || substring(category_url from '/c([0-9]+)')
            WHERE category_id IS NULL 
              AND category_url IS NOT NULL 
              AND category_url ~ '/c[0-9]+';
        """)
        updated_count = cur.rowcount
        print(f"✅ Đã update category_id cho {updated_count} products từ category_url")
        
        # Grant privileges
        print("📝 Đang grant privileges...")
        cur.execute(f"""
            GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO {db_user};
            GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO {db_user};
        """)
        print("✅ Đã grant privileges")
        
        cur.close()
        conn.close()
        
        print("\n" + "=" * 70)
        print("✅ MIGRATION HOÀN TẤT!")
        print("=" * 70)
        print("\nCác columns đã được thêm:")
        print("  - category_id VARCHAR(255) - Để link với categories table")
        print("  - category_path JSONB - Để lưu breadcrumb (array)")
        print("\nCác indexes đã được tạo:")
        print("  - idx_products_category_id - Index cho category_id")
        print("  - idx_products_category_path - GIN index cho category_path")
        
        return 0
        
    except psycopg2.OperationalError as e:
        print(f"\n❌ Lỗi kết nối database: {e}")
        print("\n💡 Hướng dẫn:")
        print("   1. Đảm bảo PostgreSQL đang chạy")
        print("   2. Set environment variables hoặc tạo file .env:")
        print("      POSTGRES_HOST=localhost")
        print("      POSTGRES_PORT=5432")
        print("      POSTGRES_USER=postgres")
        print("      POSTGRES_PASSWORD=your_password")
        print("      POSTGRES_DB=crawl_data")
        print("\n   3. Hoặc chạy trong Docker với:")
        print("      docker-compose up -d postgres")
        return 1
    except Exception as e:
        print(f"\n❌ Lỗi khi chạy migration: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(run_migration())

