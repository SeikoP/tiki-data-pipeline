"""
Demo Step 3: Load Products vào Database

Bước này load dữ liệu đã transform vào PostgreSQL database.
"""

import json
import sys
from pathlib import Path

# Fix encoding cho Windows console
if sys.platform == "win32":
    import io

    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# Thêm src vào path
project_root = Path(__file__).parent.parent
src_path = project_root / "src"
sys.path.insert(0, str(src_path))

# Import loader
try:
    from pipelines.load.loader import DataLoader
except ImportError as e:
    print(f"❌ Lỗi import: {e}")
    print("💡 Đảm bảo bạn đã cài đặt dependencies: pip install -r requirements.txt")
    sys.exit(1)


def main():
    print("=" * 80)
    print("💾 DEMO STEP 3: LOAD PRODUCTS TO DATABASE")
    print("=" * 80)
    print()
    print("Bước này sẽ:")
    print("  1. Đọc dữ liệu đã transform")
    print("  2. Load vào PostgreSQL database (nếu có)")
    print("  3. Lưu vào file JSON (backup)")
    print()

    # Đọc file từ bước 2
    input_file = project_root / "data" / "processed" / "demo_products_transformed.json"

    if not input_file.exists():
        print(f"❌ Không tìm thấy file: {input_file}")
        print("💡 Chạy demo_step2_transform.py trước!")
        sys.exit(1)

    print(f"📂 Đang đọc file: {input_file}")

    try:
        with open(input_file, encoding="utf-8") as f:
            data = json.load(f)

        products = data.get("products", [])
        print(f"📊 Tổng số products: {len(products)}")
        print()

        if not products:
            print("❌ Không có products để load!")
            return

        # Cấu hình database (có thể lấy từ environment variables)
        import os

        db_host = os.getenv("POSTGRES_HOST", "localhost")
        db_port = int(os.getenv("POSTGRES_PORT", "5432"))
        db_name = os.getenv("POSTGRES_DB", "crawl_data")
        db_user = os.getenv("POSTGRES_USER", "airflow")
        db_password = os.getenv("POSTGRES_PASSWORD", "airflow")

        print("🔌 Cấu hình database:")
        print(f"   - Host: {db_host}")
        print(f"   - Port: {db_port}")
        print(f"   - Database: {db_name}")
        print(f"   - User: {db_user}")
        print()

        # Load vào database
        print("⏳ Đang load vào database...")
        loader = DataLoader(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password,
            batch_size=100,
            enable_db=True,  # Thử kết nối database
        )

        try:
            # Lưu vào file processed
            output_dir = project_root / "data" / "processed"
            output_dir.mkdir(parents=True, exist_ok=True)
            output_file = output_dir / "demo_products_final.json"

            load_stats = loader.load_products(
                products,
                save_to_file=str(output_file),
                upsert=True,
                validate_before_load=True,
            )

            print()
            print("=" * 80)
            print("📊 LOAD RESULTS")
            print("=" * 80)
            print(f"✅ DB loaded: {load_stats['db_loaded']}")
            print(f"✅ File loaded: {load_stats['file_loaded']}")
            print(f"❌ Failed: {load_stats['failed_count']}")
            if load_stats.get("errors"):
                print(f"⚠️  Errors: {len(load_stats['errors'])}")
                for error in load_stats["errors"][:3]:  # Hiển thị 3 lỗi đầu
                    print(f"   - {error}")
            print("=" * 80)
            print()

            print(f"💾 Đã lưu vào: {output_file}")
            print()
            print("✅ Bước 3 hoàn thành!")
            print()
            print("📋 Tóm tắt pipeline:")
            print("   1. ✅ Crawl products từ Tiki.vn")
            print("   2. ✅ Transform dữ liệu (normalize, validate, compute)")
            print("   3. ✅ Load vào database và file")
            print()
            print("🎉 Pipeline hoàn thành!")
            print("=" * 80)

        except Exception as e:
            print(f"⚠️  Lỗi khi load vào database: {e}")
            print("💡 Database có thể chưa được khởi động hoặc cấu hình sai.")
            print("   Dữ liệu vẫn được lưu vào file JSON.")
            import traceback

            traceback.print_exc()

        finally:
            loader.close()

    except Exception as e:
        print(f"❌ Lỗi khi load: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
