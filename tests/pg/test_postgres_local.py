#!/usr/bin/env python3
"""
Script test kết nối PostgreSQL từ máy local (không phải trong Docker container)
Sử dụng khi muốn kết nối từ máy local đến PostgreSQL trong Docker
"""

import os
import sys
from pathlib import Path

try:
    import psycopg2
except ImportError:
    print("[ERROR] psycopg2 chua duoc cai dat")
    print("[INFO] Cai dat: pip install psycopg2-binary")
    sys.exit(1)

# Tìm file .env trong project root
project_root = Path(__file__).parent.parent
env_file = project_root / ".env"

# Load thông tin từ .env nếu có
postgres_user = "postgres"
postgres_password = "postgres"
postgres_host = "localhost"
postgres_port = 5432
postgres_db = "crawl_data"

if env_file.exists():
    try:
        from dotenv import load_dotenv

        load_dotenv(env_file, override=True)
        print(f"[OK] Da load .env tu: {env_file}")
    except ImportError:
        print("[WARN] python-dotenv chua duoc cai dat, doc .env thu cong...")
        # Đọc thủ công
        with open(env_file, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line.startswith("POSTGRES_USER="):
                    postgres_user = line.split("=", 1)[1].strip()
                elif line.startswith("POSTGRES_PASSWORD="):
                    postgres_password = line.split("=", 1)[1].strip()
                elif line.startswith("POSTGRES_HOST="):
                    postgres_host = line.split("=", 1)[1].strip()
                elif line.startswith("POSTGRES_PORT="):
                    postgres_port = int(line.split("=", 1)[1].strip())
                elif line.startswith("POSTGRES_DB="):
                    postgres_db = line.split("=", 1)[1].strip()

# Override với environment variables nếu có
postgres_user = os.getenv("POSTGRES_USER", postgres_user)
postgres_password = os.getenv("POSTGRES_PASSWORD", postgres_password)
postgres_host = os.getenv("POSTGRES_HOST", postgres_host)
postgres_port = int(os.getenv("POSTGRES_PORT", postgres_port))
postgres_db = os.getenv("POSTGRES_DB", postgres_db)

print("=" * 50)
print("Test kết nối PostgreSQL từ máy LOCAL")
print("=" * 50)
print(f"Host: {postgres_host}")
print(f"Port: {postgres_port}")
print(f"User: {postgres_user}")
print(f"Database: {postgres_db}")
print(f"Password: {'*' * len(postgres_password)}")
print()

# Test 1: Connection với host và port rõ ràng (TCP/IP)
print("1. Testing TCP/IP connection (host + port)...")
try:
    conn = psycopg2.connect(
        host=postgres_host,
        port=postgres_port,
        database=postgres_db,
        user=postgres_user,
        password=postgres_password,
        connect_timeout=10,
    )
    print("[OK] TCP/IP connection thanh cong!")

    # Test query
    with conn.cursor() as cur:
        cur.execute("SELECT version();")
        version = cur.fetchone()
        print(f"   PostgreSQL version: {version[0][:50]}...")

        # Kiểm tra databases
        cur.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
        databases = [row[0] for row in cur.fetchall()]
        print(f"   Databases available: {', '.join(databases)}")

    conn.close()
except psycopg2.OperationalError as e:
    print(f"[ERROR] TCP/IP connection failed: {e}")
    print()
    print("[INFO] Cac nguyen nhan co the:")
    print("   1. PostgreSQL container chua chay: docker compose ps postgres")
    print("   2. Port chua duoc expose: Kiem tra docker-compose.yaml co 'ports: - \"5432:5432\"'")
    print("   3. Port 5432 da duoc su dung boi PostgreSQL local khac")
    print("   4. Firewall chan ket noi")
    sys.exit(1)
except Exception as e:
    print(f"[ERROR] Loi: {e}")
    sys.exit(1)

# Test 2: Connection string format
print("\n2. Testing connection string format...")
try:
    conn_str = f"postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db}"
    conn = psycopg2.connect(conn_str, connect_timeout=10)
    print("[OK] Connection string format thanh cong!")
    conn.close()
except Exception as e:
    print(f"[ERROR] Connection string format failed: {e}")

# Test 3: Connection với DSN
print("\n3. Testing DSN format...")
try:
    dsn = f"host={postgres_host} port={postgres_port} dbname={postgres_db} user={postgres_user} password={postgres_password} connect_timeout=10"
    conn = psycopg2.connect(dsn)
    print("[OK] DSN format thanh cong!")
    conn.close()
except Exception as e:
    print(f"[ERROR] DSN format failed: {e}")

# Test 4: Test với PostgresStorage
print("\n4. Testing voi PostgresStorage class...")
try:
    sys.path.insert(0, str(project_root / "src"))
    from pipelines.crawl.storage.postgres_storage import PostgresStorage

    storage = PostgresStorage(
        host=postgres_host,
        port=postgres_port,
        database=postgres_db,
        user=postgres_user,
        password=postgres_password,
    )

    with storage.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';"
            )
            table_count = cur.fetchone()[0]
            print("[OK] PostgresStorage connection thanh cong!")
            print(f"   So tables trong database: {table_count}")

    storage.close()
except ImportError as e:
    print(f"[WARN] Khong the import PostgresStorage: {e}")
except Exception as e:
    print(f"[ERROR] PostgresStorage connection failed: {e}")

print("\n" + "=" * 50)
print("[OK] Hoan tat test!")
print("=" * 50)
print()
print("[INFO] Thong tin ket noi:")
print(f"   Host: {postgres_host}")
print(f"   Port: {postgres_port}")
print(f"   Database: {postgres_db}")
print(f"   User: {postgres_user}")
print()
print("[INFO] Su dung trong code:")
print("   storage = PostgresStorage(")
print(f'       host="{postgres_host}",')
print(f"       port={postgres_port},")
print(f'       database="{postgres_db}",')
print(f'       user="{postgres_user}",')
print(f'       password="{postgres_password}"')
print("   )")
