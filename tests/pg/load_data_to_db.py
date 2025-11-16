#!/usr/bin/env python3
"""
Script để load dữ liệu từ file JSON vào PostgreSQL database
Sử dụng khi DAG chưa chạy hoặc muốn load lại dữ liệu
"""

import json
import os
import sys
from pathlib import Path

# Thêm src vào path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

try:
    from pipelines.crawl.storage.postgres_storage import PostgresStorage
    from pipelines.load.loader import DataLoader
except ImportError as e:
    print(f"[ERROR] Khong the import modules: {e}")
    print("[INFO] Dang thu import tu duong dan tuyet doi...")
    sys.exit(1)

# Load .env
env_file = project_root / ".env"
if env_file.exists():
    try:
        from dotenv import load_dotenv

        load_dotenv(env_file, override=True)
        print(f"[OK] Da load .env tu: {env_file}")
    except ImportError:
        print("[WARN] python-dotenv chua duoc cai dat, doc .env thu cong...")
        with open(env_file, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if "=" in line and not line.startswith("#"):
                    key, value = line.split("=", 1)
                    os.environ[key.strip()] = value.strip()

# Database config
db_host = os.getenv("POSTGRES_HOST", "localhost")
db_port = int(os.getenv("POSTGRES_PORT", "5432"))
db_name = os.getenv("POSTGRES_DB", "crawl_data")
db_user = os.getenv("POSTGRES_USER", "postgres")
db_password = os.getenv("POSTGRES_PASSWORD", "postgres")

print("=" * 70)
print("LOAD DU LIEU VAO DATABASE")
print("=" * 70)
print(f"Host: {db_host}")
print(f"Port: {db_port}")
print(f"Database: {db_name}")
print(f"User: {db_user}")
print()

# Tìm file dữ liệu
data_dir = project_root / "data"
possible_files = [
    data_dir / "processed" / "products_transformed.json",
    data_dir / "processed" / "products_final.json",
    data_dir / "raw" / "products" / "products_with_detail.json",
    data_dir / "raw" / "products" / "products.json",
]

input_file = None
for file_path in possible_files:
    if file_path.exists():
        input_file = file_path
        print(f"[OK] Tim thay file: {input_file}")
        break

if not input_file:
    print("[ERROR] Khong tim thay file du lieu!")
    print("[INFO] Cac file co the:")
    for f in possible_files:
        print(f"  - {f}")
    sys.exit(1)

# Đọc dữ liệu
print(f"\n[INFO] Dang doc file: {input_file}")
try:
    with open(input_file, encoding="utf-8") as f:
        data = json.load(f)

    # Lấy products
    if isinstance(data, dict):
        products = data.get("products", [])
    elif isinstance(data, list):
        products = data
    else:
        print(f"[ERROR] File khong dung format: {type(data)}")
        sys.exit(1)

    print(f"[OK] Doc duoc {len(products)} products")

    if len(products) == 0:
        print("[WARN] Khong co products nao trong file!")
        sys.exit(0)

except Exception as e:
    print(f"[ERROR] Loi khi doc file: {e}")
    import traceback

    traceback.print_exc()
    sys.exit(1)

# Load vào database
print("\n[INFO] Dang load vao database...")
try:
    loader = DataLoader(
        host=db_host,
        port=db_port,
        database=db_name,
        user=db_user,
        password=db_password,
        batch_size=100,
        enable_db=True,
    )

    try:
        # Load products
        load_stats = loader.load_products(
            products,
            save_to_file=None,  # Không lưu file, chỉ load vào DB
            upsert=True,
            validate_before_load=True,
        )

        print("\n" + "=" * 70)
        print("KET QUA LOAD")
        print("=" * 70)
        print(f"[OK] DB loaded: {load_stats.get('db_loaded', 0)}")
        print(f"[OK] Success: {load_stats.get('success_count', 0)}")
        print(f"[ERROR] Failed: {load_stats.get('failed_count', 0)}")
        if load_stats.get("errors"):
            print(f"\n[WARN] Co {len(load_stats['errors'])} loi:")
            for error in load_stats["errors"][:5]:  # Chỉ hiển thị 5 lỗi đầu
                print(f"  - {error}")
        print("=" * 70)

        # Kiểm tra lại database
        print("\n[INFO] Kiem tra lai database...")
        storage = PostgresStorage(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password,
        )

        with storage.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM products;")
                count = cur.fetchone()[0]
                print(f"[OK] So products trong database: {count}")

        storage.close()

    finally:
        loader.close()

except Exception as e:
    print(f"[ERROR] Loi khi load vao database: {e}")
    import traceback

    traceback.print_exc()
    sys.exit(1)

print("\n[OK] Hoan tat!")
