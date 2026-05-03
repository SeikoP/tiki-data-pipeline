"""
Clear all cache to start fresh crawl
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import redis
from dotenv import load_dotenv

project_root = Path(__file__).parent.parent
env_file = (
    Path(os.getenv("ENV_FILE_PATH", "")) if os.getenv("ENV_FILE_PATH") else project_root / ".env"
)
load_dotenv(env_file)


def clear_redis_cache():
    """Clear Redis cache"""
    try:
        redis_client = redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            password=os.getenv("REDIS_PASSWORD", None),
            decode_responses=True,
        )

        # Test connection
        redis_client.ping()

        # Get all keys
        all_keys = redis_client.keys("*")
        print(f"📊 Tìm thấy {len(all_keys)} keys trong Redis")

        if all_keys:
            # Delete all keys
            deleted = redis_client.delete(*all_keys)
            print(f"✅ Đã xóa {deleted} keys từ Redis")
        else:
            print("✅ Redis đã sạch (không có keys)")

        return True
    except redis.ConnectionError:
        print("⚠️  Không thể kết nối Redis (có thể chưa chạy)")
        return False
    except Exception as e:
        print(f"❌ Lỗi khi xóa Redis cache: {e}")
        return False


def clear_file_cache():
    """Clear file cache in data directory"""
    import shutil

    data_dir = Path(__file__).parent.parent / "data"

    # Patterns for individual files
    cache_patterns = [
        "data/raw/*.json",
        "data/processed/*.json",
        "data/profiling/*.json",
        "data/test_output/*.json",
    ]

    # Directories to clear completely
    cache_dirs = [
        data_dir / "raw" / "cache",
        data_dir / "raw" / "products",
    ]

    total_deleted = 0

    # Delete individual files
    for pattern in cache_patterns:
        files = list(data_dir.parent.glob(pattern))
        for file in files:
            try:
                file.unlink()
                total_deleted += 1
                print(f"  Đã xóa: {file.name}")
            except Exception as e:
                print(f"  ⚠️  Không thể xóa {file.name}: {e}")

    # Delete cache directories
    for cache_dir in cache_dirs:
        if cache_dir.exists():
            try:
                file_count = sum(1 for _ in cache_dir.rglob("*") if _.is_file())
                shutil.rmtree(cache_dir)
                total_deleted += file_count
                print(
                    f"  Đã xóa thư mục: {cache_dir.relative_to(data_dir.parent)} ({file_count} files)"
                )
            except Exception as e:
                print(f"  ⚠️  Không thể xóa {cache_dir.name}: {e}")

    return total_deleted


def main():
    print("=" * 80)
    print("🧹 XÓA TOÀN BỘ CACHE ĐỂ CRAWL MỚI")
    print("=" * 80)

    # 1. Clear Redis cache
    print("\n1️⃣  Xóa Redis cache...")
    print("-" * 80)
    redis_cleared = clear_redis_cache()

    # 2. Clear file cache
    print("\n2️⃣  Xóa file cache...")
    print("-" * 80)
    deleted_files = clear_file_cache()
    print(f"✅ Đã xóa {deleted_files} files cache")

    # 3. Summary
    print("\n" + "=" * 80)
    print("📊 TỔNG KẾT:")
    print("=" * 80)
    print(f"  Redis cache: {'✅ Đã xóa' if redis_cleared else '⚠️  Không xóa được'}")
    print(f"  File cache: ✅ Đã xóa {deleted_files} files")

    print("\n💡 LƯU Ý:")
    print("  - Database KHÔNG bị xóa (products, categories vẫn còn)")
    print("  - Nếu muốn xóa database, chạy: docker-compose down -v")
    print("  - Lần crawl tiếp theo sẽ UPSERT vào database hiện tại")

    print("\n🚀 SẴN SÀNG CRAWL MỚI!")
    print("=" * 80)


if __name__ == "__main__":
    main()
