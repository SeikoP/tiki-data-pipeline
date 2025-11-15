"""
Script để extract categories từ categories_tree.json và load vào database
"""

import os
import sys
from pathlib import Path

# Thêm src vào path
project_root = Path(__file__).parent.parent
src_path = project_root / "src"
sys.path.insert(0, str(src_path))

from pipelines.extract.extract_categories import extract_categories_from_tree_file
from pipelines.load.loader import DataLoader


def main():
    """Main function để extract và load categories"""
    print("=" * 70)
    print("📁 EXTRACT & LOAD CATEGORIES TO DATABASE")
    print("=" * 70)

    # 1. Extract categories từ tree file
    tree_file = project_root / "data" / "raw" / "categories_tree.json"
    print(f"\n📖 Bước 1: Extract categories từ {tree_file}")

    if not tree_file.exists():
        print(f"❌ Không tìm thấy file: {tree_file}")
        return 1

    try:
        categories = extract_categories_from_tree_file(tree_file)
        print(f"✅ Đã extract {len(categories)} categories")
    except Exception as e:
        print(f"❌ Lỗi khi extract categories: {e}")
        import traceback

        traceback.print_exc()
        return 1

    # 2. Load vào database
    print(f"\n💾 Bước 2: Load categories vào database")

    # Lấy credentials từ environment variables
    loader = DataLoader(
        database=os.getenv("POSTGRES_DB", "crawl_data"),
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER", "airflow_user"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
        batch_size=100,
        enable_db=True,
    )

    try:
        stats = loader.load_categories(
            categories,
            save_to_file=None,  # Không lưu file, chỉ load vào DB
            upsert=True,
            validate_before_load=True,
        )

        print(f"\n📊 Kết quả:")
        print(f"  - Tổng số categories: {stats['total_loaded']}")
        print(f"  - Đã load vào DB: {stats['db_loaded']}")
        print(f"  - Thành công: {stats['success_count']}")
        print(f"  - Thất bại: {stats['failed_count']}")

        if stats["errors"]:
            print(f"\n⚠️  Có {len(stats['errors'])} lỗi:")
            for error in stats["errors"][:10]:  # Chỉ hiển thị 10 lỗi đầu
                print(f"  - {error}")
            if len(stats["errors"]) > 10:
                print(f"  ... và {len(stats['errors']) - 10} lỗi khác")

        loader.close()
        print("\n✅ Hoàn thành!")
        return 0

    except Exception as e:
        print(f"❌ Lỗi khi load vào database: {e}")
        import traceback

        traceback.print_exc()
        loader.close()
        return 1


if __name__ == "__main__":
    sys.exit(main())

